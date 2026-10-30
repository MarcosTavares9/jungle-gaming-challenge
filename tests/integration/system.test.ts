import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  test,
} from "bun:test";
import {
  payloadHash,
  type TransactionResult,
  type WagerInput,
} from "../../src/application/contracts";
import { config } from "../../src/infrastructure/config";
import { queueNames } from "../../src/infrastructure/queues";
import { IntegrationContext, eventually } from "./helpers";

const context = new IntegrationContext();
// The supported command provisions isolated resources. Plain `bun test` only runs the unit suite.
describe.skipIf(process.env.INTEGRATION_TEST_RUN !== "true")(
  "PostgreSQL, SQS e múltiplos processos",
  () => {
    beforeAll(async () => {
      await context.setup();
      // A concorrência testada é de processamento; a subida sequencial evita flake no cold start.
      for (let index = 0; index < 3; index += 1) await context.start();
    }, 60_000);
    afterAll(() => context.close(), 15_000);
    afterEach(async () => {
      if (context.db) await context.assertConsistent();
    });

    test("migrations são reversíveis e reaplicáveis", async () => {
      await context.db.migrator.down({ to: 0 });
      const [removed] = await context.sql.query<{ n: string }>(
        "SELECT COUNT(*)::text AS n FROM information_schema.tables WHERE table_schema='public' AND table_name='wallets'",
      );
      expect(removed!.n).toBe("0");
      await context.db.migrator.up();
      const [restored] = await context.sql.query<{ n: string }>(
        "SELECT COUNT(*)::text AS n FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('wallets','wallet_ledger','wager_transactions','inbox','outbox')",
      );
      expect(restored!.n).toBe("5");
    });

    test("abertura positiva gera OPENING, ledger e eventos; zero não gera movimentação", async () => {
      const funded = await context.wallet();
      const empty = await context.wallet("0.00");
      const entries = await context.queries.ledger(funded.id, 50);
      expect(entries.items).toHaveLength(1);
      expect(entries.items[0]).toMatchObject({
        direction: "CREDIT",
        money: { amount: "100.00", currency: "BRL" },
        walletVersion: 1,
      });
      const opening = await context.queries.transaction(
        entries.items[0]!.transactionId,
      );
      expect(opening).toMatchObject({ kind: "OPENING", status: "PROCESSED" });
      expect((await context.queries.ledger(empty.id, 50)).items).toHaveLength(
        0,
      );
      const events = await context.sql.query<{ event_type: string }>(
        "SELECT event_type FROM outbox WHERE aggregate_id=? ORDER BY event_type",
        [funded.id],
      );
      expect(events.map((e) => e.event_type)).toEqual([
        "WagerTransactionProcessed",
        "WalletBalanceChanged",
      ]);
    });

    test("carteira duplicada resulta em conflito e não cria outra abertura", async () => {
      const wallet = await context.wallet();
      const response = await context.services[0]!.post("/wallets", {
        playerId: wallet.playerId,
        initialBalance: wallet.balance,
      });
      expect(response.status).toBe(409);
      const [count] = await context.sql.query<{ n: string }>(
        "SELECT COUNT(*)::text AS n FROM wallets WHERE player_id=?",
        [wallet.playerId],
      );
      expect(count!.n).toBe("1");
    });

    test("BET, WIN e LOSS mantêm ledger e versão; LOSS não publica mudança de saldo", async () => {
      const wallet = await context.wallet();
      const bet = context.input(wallet, "BET", "20.00");
      const betResult = await context.process(bet);
      expect(betResult.balance?.amount).toBe("80.00");
      const win = context.input(
        wallet,
        "WIN",
        "35.00",
        bet.externalTransactionId,
      );
      expect((await context.process(win)).balance?.amount).toBe("115.00");
      const loss = await context.process(context.input(wallet, "LOSS"));
      expect(loss.status).toBe("PROCESSED");
      expect((await context.queries.wallet(wallet.id)).version).toBe(3);
      const rows = await context.sql.query<{ event_type: string }>(
        "SELECT event_type FROM outbox WHERE payload->'data'->>'transactionId'=?",
        [loss.transactionId],
      );
      expect(rows.map((r) => r.event_type)).toEqual([
        "WagerTransactionProcessed",
      ]);
      expect((await context.queries.ledger(wallet.id, 50)).items).toHaveLength(
        3,
      );
    });

    test("REFUND e ROLLBACK revertem valor integral uma vez por tipo", async () => {
      const wallet = await context.wallet();
      const bet = context.input(wallet, "BET", "30.00");
      await context.process(bet);
      const refund = context.input(
        wallet,
        "REFUND",
        "30.00",
        bet.externalTransactionId,
      );
      expect((await context.process(refund)).balance?.amount).toBe("100.00");
      const duplicate = await context.process(
        context.input(wallet, "REFUND", "30.00", bet.externalTransactionId),
      );
      expect(duplicate.failureCode).toBe("ALREADY_REVERSED");
      const rollback = context.input(
        wallet,
        "ROLLBACK",
        "30.00",
        refund.externalTransactionId,
      );
      expect((await context.process(rollback)).balance?.amount).toBe("70.00");
      expect(
        (
          await context.process(
            context.input(
              wallet,
              "ROLLBACK",
              "30.00",
              refund.externalTransactionId,
            ),
          )
        ).failureCode,
      ).toBe("ALREADY_REVERSED");
      const separateBet = context.input(wallet, "BET", "5.00");
      await context.process(separateBet);
      expect(
        (
          await context.process(
            context.input(
              wallet,
              "ROLLBACK",
              "5.00",
              separateBet.externalTransactionId,
            ),
          )
        ).balance?.amount,
      ).toBe("70.00");
    });

    test("reversão de WIN sem saldo tem código distinto e não gera ledger", async () => {
      const wallet = await context.wallet("0.00");
      const win = context.input(wallet, "WIN", "10.00");
      await context.process(win);
      await context.process(context.input(wallet, "BET", "10.00"));
      const reversal = await context.process(
        context.input(wallet, "ROLLBACK", "10.00", win.externalTransactionId),
      );
      expect(reversal).toMatchObject({
        status: "REJECTED",
        failureCode: "REVERSAL_INSUFFICIENT_FUNDS",
      });
      const ordinaryBet = await context.process(context.input(wallet));
      expect(ordinaryBet.failureCode).toBe("INSUFFICIENT_FUNDS");
      expect((await context.queries.ledger(wallet.id, 50)).items).toHaveLength(
        2,
      );
    });

    test("referência de outra rodada, tipo indevido e reversão parcial são rejeitados", async () => {
      const wallet = await context.wallet();
      const bet = context.input(wallet);
      await context.process(bet);
      const mismatch = {
        ...context.input(wallet, "REFUND", "10.00", bet.externalTransactionId),
        roundId: "another",
      };
      expect((await context.process(mismatch)).failureCode).toBe(
        "REFERENCE_MISMATCH",
      );
      expect(
        (
          await context.process(
            context.input(wallet, "REFUND", "9.99", bet.externalTransactionId),
          )
        ).failureCode,
      ).toBe("REVERSAL_AMOUNT_MISMATCH");
      const win = context.input(wallet, "WIN");
      await context.process(win);
      expect(
        (
          await context.process(
            context.input(wallet, "REFUND", "10.00", win.externalTransactionId),
          )
        ).failureCode,
      ).toBe("INVALID_REFERENCE_KIND");
      const rejected = context.input(wallet, "BET", "1000.00");
      await context.process(rejected);
      expect(
        (
          await context.process(
            context.input(
              wallet,
              "REFUND",
              "1000.00",
              rejected.externalTransactionId,
            ),
          )
        ).failureCode,
      ).toBe("REFERENCE_REJECTED");
    });

    test("identidade de carteira, jogador, provedor e moeda é validada", async () => {
      const wallet = await context.wallet();
      const other = await context.wallet();
      const bet = context.input(wallet);
      await context.process(bet);
      expect(
        (
          await context.process({
            ...context.input(wallet),
            walletId: crypto.randomUUID(),
          })
        ).failureCode,
      ).toBe("WALLET_NOT_FOUND");
      expect(
        (
          await context.process({
            ...context.input(wallet),
            playerId: other.playerId,
          })
        ).failureCode,
      ).toBe("PLAYER_MISMATCH");
      const usd = {
        ...context.input(wallet),
        money: { amount: "1.00", currency: "USD" },
      } as unknown as WagerInput;
      expect((await context.process(usd)).failureCode).toBe(
        "CURRENCY_MISMATCH",
      );
      expect(
        (
          await context.process(
            context.input(other, "REFUND", "10.00", bet.externalTransactionId),
          )
        ).failureCode,
      ).toBe("REFERENCE_MISMATCH");
      const foreignProvider = await context.process({
        ...context.input(wallet, "REFUND", "10.00", bet.externalTransactionId),
        providerId: "foreign",
      });
      expect(foreignProvider.status).toBe("PENDING_REFERENCE");
      await context.sql.query(
        "UPDATE wager_transactions SET attempts=?,next_attempt_at=now() WHERE id=?",
        [config.REFERENCE_MAX_ATTEMPTS, foreignProvider.transactionId],
      );
      await context.processor.retryReferences();
      expect(
        (await context.queries.transaction(foreignProvider.transactionId))
          .failureCode,
      ).toBe("REFERENCE_NOT_FOUND");
    });

    test.each(["REFUND", "ROLLBACK"] as const)(
      "%s recebido antes da BET é retomado por workers concorrentes",
      async (kind) => {
        const wallet = await context.wallet();
        const bet = context.input(wallet, "BET", "15.00");
        const dependent = await context.process(
          context.input(wallet, kind, "15.00", bet.externalTransactionId),
        );
        expect(dependent.status).toBe("PENDING_REFERENCE");
        expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
          "100.00",
        );
        await context.process(bet);
        await context.due(dependent.transactionId);
        await Promise.all([
          context.processor.retryReferences(),
          context.processor.retryReferences(),
        ]);
        expect(
          (await context.queries.transaction(dependent.transactionId)).status,
        ).toBe("PROCESSED");
        expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
          "100.00",
        );
        expect(
          (await context.queries.ledger(wallet.id, 50)).items,
        ).toHaveLength(3);
      },
    );

    test("referência ausente usa backoff e termina com rejeição ao esgotar tentativas", async () => {
      const wallet = await context.wallet();
      const result = await context.process(
        context.input(wallet, "REFUND", "1.00", "never-existed"),
      );
      await context.due(result.transactionId);
      await context.processor.retryReferences();
      const [retry] = await context.sql.query<{
        attempts: number;
        next_attempt_at: string;
      }>("SELECT attempts,next_attempt_at FROM wager_transactions WHERE id=?", [
        result.transactionId,
      ]);
      expect(retry!.attempts).toBe(1);
      expect(new Date(retry!.next_attempt_at).getTime()).toBeGreaterThan(
        Date.now(),
      );
      await context.sql.query(
        "UPDATE wager_transactions SET attempts=?,next_attempt_at=now() WHERE id=?",
        [config.REFERENCE_MAX_ATTEMPTS - 1, result.transactionId],
      );
      await context.processor.retryReferences();
      expect(
        await context.queries.transaction(result.transactionId),
      ).toMatchObject({
        status: "REJECTED",
        failureCode: "REFERENCE_NOT_FOUND",
      });
      const events = await context.sql.query<{ event_type: string }>(
        "SELECT event_type FROM outbox WHERE aggregate_id=? AND event_type IN ('WagerTransactionPendingReference','WagerTransactionRejected') ORDER BY event_type",
        [wallet.id],
      );
      expect(events.map((e) => e.event_type)).toEqual([
        "WagerTransactionPendingReference",
        "WagerTransactionRejected",
      ]);
    });

    test("replay conserva saldo original mesmo após outra operação", async () => {
      const wallet = await context.wallet();
      const input = context.input(wallet);
      const result = await context.process(input);
      await context.process(context.input(wallet));
      expect(await context.process(input)).toEqual({
        ...result,
        idempotentReplay: true,
      });
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "80.00",
      );
      await expect(
        context.process({
          ...input,
          money: { amount: "11.00", currency: "BRL" },
        }),
      ).rejects.toThrow("IDEMPOTENCY_CONFLICT");
      await expect(context.process(input, "new-key")).rejects.toThrow(
        "IDEMPOTENCY_CONFLICT",
      );
    });

    test("inbox persiste redelivery e recusa reutilização de messageId com outro conteúdo", async () => {
      const wallet = await context.wallet();
      const input = context.input(wallet);
      const messageId = crypto.randomUUID();
      const ctx = {
        correlationId: messageId,
        inbox: { messageId, hash: payloadHash(input) },
      };
      const result = await context.processor.process(
        input,
        input.externalTransactionId,
        ctx,
      );
      expect(
        (
          await context.processor.process(
            input,
            input.externalTransactionId,
            ctx,
          )
        ).idempotentReplay,
      ).toBe(true);
      const changed = context.input(wallet);
      await expect(
        context.processor.process(changed, changed.externalTransactionId, {
          ...ctx,
          inbox: { messageId, hash: payloadHash(changed) },
        }),
      ).rejects.toThrow("INBOX_PAYLOAD_CONFLICT");
      const inbox = await context.sql.query<{ transaction_id: string }>(
        "SELECT transaction_id FROM inbox WHERE message_id=?",
        [messageId],
      );
      expect(inbox).toEqual([{ transaction_id: result.transactionId }]);
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "90.00",
      );
      const orphan = await context.sql.query(
        "SELECT id FROM wager_transactions WHERE external_transaction_id=?",
        [changed.externalTransactionId],
      );
      expect(orphan).toHaveLength(0);
    });

    test.each(["outbox", "inbox"] as const)(
      "falha tardia na %s desfaz saldo, transação, ledger e eventos",
      async (table) => {
        const wallet = await context.wallet();
        const input = context.input(wallet);
        const messageId = crypto.randomUUID();
        const column = table === "outbox" ? "aggregate_id" : "message_id";
        const value = table === "outbox" ? wallet.id : messageId;
        await context.sql
          .query(`CREATE FUNCTION test_abort_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.${column}::text = '${value}' THEN RAISE EXCEPTION 'injected write failure'; END IF; RETURN NEW; END $$`);
        await context.sql.query(
          `CREATE TRIGGER test_abort BEFORE INSERT ON ${table} FOR EACH ROW EXECUTE FUNCTION test_abort_write()`,
        );
        try {
          await expect(
            context.processor.process(input, input.externalTransactionId, {
              correlationId: messageId,
              inbox: { messageId, hash: payloadHash(input) },
            }),
          ).rejects.toThrow("injected write failure");
        } finally {
          await context.sql.query(`DROP TRIGGER test_abort ON ${table}`);
          await context.sql.query("DROP FUNCTION test_abort_write()");
        }
        expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
          "100.00",
        );
        expect(
          (await context.queries.ledger(wallet.id, 50)).items,
        ).toHaveLength(1);
        expect(
          await context.sql.query(
            "SELECT id FROM wager_transactions WHERE external_transaction_id=?",
            [input.externalTransactionId],
          ),
        ).toHaveLength(0);
        expect(
          await context.sql.query(
            "SELECT message_id FROM inbox WHERE message_id=?",
            [messageId],
          ),
        ).toHaveLength(0);
        const events = await context.sql.query(
          "SELECT id FROM outbox WHERE aggregate_id=?",
          [wallet.id],
        );
        expect(events).toHaveLength(2);
      },
    );

    test("schema bloqueia saldo negativo, alteração sem ledger e mutação de histórico", async () => {
      const wallet = await context.wallet();
      const tx = await context.process(context.input(wallet));
      for (const command of [
        "UPDATE wallets SET balance=-1,version=version+1 WHERE id=?",
        "UPDATE wallets SET balance=balance+1,version=version+1 WHERE id=?",
        "UPDATE wallets SET version=version+1 WHERE id=?",
        "UPDATE wallets SET currency='USD' WHERE id=?",
      ])
        await expect(
          context.sql.query(command, [wallet.id]),
        ).rejects.toMatchObject({ code: "23514" });
      await expect(
        context.sql.query(
          "UPDATE wallet_ledger SET amount=amount+1 WHERE wallet_id=?",
          [wallet.id],
        ),
      ).rejects.toMatchObject({ code: "23514" });
      await expect(
        context.sql.query("DELETE FROM wallet_ledger WHERE wallet_id=?", [
          wallet.id,
        ]),
      ).rejects.toMatchObject({ code: "23514" });
      await expect(
        context.sql.query("TRUNCATE wallet_ledger"),
      ).rejects.toMatchObject({ code: "23514" });
      await expect(
        context.sql.query(
          "UPDATE wager_transactions SET status='PENDING' WHERE id=?",
          [tx.transactionId],
        ),
      ).rejects.toMatchObject({ code: "23514" });
      await expect(
        context.sql.query("DELETE FROM wager_transactions WHERE id=?", [
          tx.transactionId,
        ]),
      ).rejects.toMatchObject({ code: "23514" });
    });

    test("cursor de ledger permanece estável quando chegam novos lançamentos", async () => {
      const wallet = await context.wallet();
      await context.process(context.input(wallet));
      const first = await context.queries.ledger(wallet.id, 1);
      expect(first.nextCursor).toBeString();
      await context.process(context.input(wallet));
      const second = await context.queries.ledger(
        wallet.id,
        50,
        first.nextCursor!,
      );
      expect(second.items.map((e) => e.walletVersion)).toEqual([2, 3]);
      expect(second.nextCursor).toBeNull();
      await expect(
        context.queries.ledger(wallet.id, 50, "invalid"),
      ).rejects.toThrow();
      const other = await context.wallet();
      await expect(
        context.queries.ledger(other.id, 50, first.nextCursor!),
      ).rejects.toThrow();
    });

    test("50 envios paralelos em três processos produzem exatamente um débito", async () => {
      expect(new Set(context.services.map((s) => s.child.pid)).size).toBe(3);
      const wallet = await context.wallet();
      const input = context.input(wallet, "BET", "25.00");
      const results = await Promise.all(
        Array.from({ length: 50 }, (_, i) =>
          context.services[i % 3]!.post(
            "/wagering/transactions",
            input,
            input.externalTransactionId,
          ),
        ),
      );
      expect(
        results.filter(
          (r) => r.status !== 200 || r.body.status !== "PROCESSED",
        ),
      ).toEqual([]);
      expect(results.filter((r) => !r.body.idempotentReplay)).toHaveLength(1);
      expect(results.filter((r) => r.body.idempotentReplay)).toHaveLength(49);
      expect(new Set(results.map((r) => r.body.transactionId)).size).toBe(1);
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "75.00",
      );
      expect(
        (await context.queries.ledger(wallet.id, 50)).items.filter(
          (e) => e.direction === "DEBIT",
        ),
      ).toHaveLength(1);
    });

    test("duas apostas de 80 disputando 100 deixam saldo 20", async () => {
      const wallet = await context.wallet();
      const inputs = [
        context.input(wallet, "BET", "80.00"),
        context.input(wallet, "BET", "80.00"),
      ];
      const results = await Promise.all(
        inputs.map((input, i) =>
          context.services[i]!.post(
            "/wagering/transactions",
            input,
            input.externalTransactionId,
          ),
        ),
      );
      expect(results.map((r) => r.status).sort()).toEqual([200, 422]);
      expect(results.find((r) => r.status === 422)!.body.failureCode).toBe(
        "INSUFFICIENT_FUNDS",
      );
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "20.00",
      );
      expect(
        (await context.queries.ledger(wallet.id, 50)).items.filter(
          (e) => e.direction === "DEBIT",
        ),
      ).toHaveLength(1);
      for (const input of inputs)
        expect(
          (
            await context.services[2]!.post(
              "/wagering/transactions",
              input,
              input.externalTransactionId,
            )
          ).body.idempotentReplay,
        ).toBe(true);
    });

    test("carteiras distintas progridem em paralelo mesmo com uma carteira bloqueada", async () => {
      const locked = await context.wallet();
      const others = await Promise.all(
        Array.from({ length: 6 }, () => context.wallet()),
      );
      const unlock = await context.lock(locked.id);
      try {
        const results = await Promise.all(
          others.map((w, i) => {
            const input = context.input(w);
            return context.services[i % 3]!.post(
              "/wagering/transactions",
              input,
              input.externalTransactionId,
            );
          }),
        );
        expect(results.every((r) => r.status === 200)).toBe(true);
        for (const w of others)
          expect((await context.queries.wallet(w.id)).balance.amount).toBe(
            "90.00",
          );
      } finally {
        await unlock();
      }
    });

    test("conflito da mesma chave entre carteiras distintas não deixa efeitos parciais", async () => {
      const wallets = await Promise.all([context.wallet(), context.wallet()]);
      const key = crypto.randomUUID();
      const inputs = wallets.map((w) => context.input(w));
      const results = await Promise.all(
        inputs.map((input, i) =>
          context.services[i]!.post("/wagering/transactions", input, key),
        ),
      );
      expect(results.map((r) => r.status).sort()).toEqual([200, 409]);
      const balances = await Promise.all(
        wallets.map((w) => context.queries.wallet(w.id)),
      );
      expect(balances.map((w) => w.balance.amount).sort()).toEqual([
        "100.00",
        "90.00",
      ]);
    });

    test("reconciliação usa snapshot consistente enquanto há escrita concorrente", async () => {
      const wallet = await context.wallet();
      const writes = Promise.all(
        Array.from({ length: 12 }, () =>
          context.process(context.input(wallet, "BET", "1.00")),
        ),
      );
      const checks = await Promise.all(
        Array.from({ length: 12 }, () =>
          context.queries.reconcile(wallet.id, "race"),
        ),
      );
      await writes;
      expect(checks.every((r) => r.consistent)).toBe(true);
      expect(
        (await context.queries.reconcile(wallet.id, "final")).difference.amount,
      ).toBe("0.00");
    });

    test("HTTP distingue contrato inválido, conflito, rejeição, pendência e recurso ausente", async () => {
      const service = context.services[0]!;
      const wallet = await context.wallet();
      const input = context.input(wallet);
      expect((await service.post("/wagering/transactions", input)).status).toBe(
        400,
      );
      expect(
        (
          await service.post(
            "/wagering/transactions",
            { ...input, kind: "OPENING" },
            input.externalTransactionId,
          )
        ).status,
      ).toBe(400);
      expect(
        (
          await service.post(
            "/wagering/transactions",
            { ...input, money: { amount: 1, currency: "BRL" } },
            input.externalTransactionId,
          )
        ).status,
      ).toBe(400);
      expect(
        (
          await service.post(
            "/wagering/transactions",
            input,
            input.externalTransactionId,
          )
        ).status,
      ).toBe(200);
      expect(
        (
          await service.post(
            "/wagering/transactions",
            { ...input, gameId: "changed" },
            input.externalTransactionId,
          )
        ).status,
      ).toBe(409);
      const rejected = context.input(wallet, "BET", "1000.00");
      expect(
        (
          await service.post(
            "/wagering/transactions",
            rejected,
            rejected.externalTransactionId,
          )
        ).status,
      ).toBe(422);
      const pending = context.input(wallet, "REFUND", "1.00", "missing");
      expect(
        (
          await service.post(
            "/wagering/transactions",
            pending,
            pending.externalTransactionId,
          )
        ).status,
      ).toBe(202);
      expect(
        (await fetch(service.base + `/wallets/${crypto.randomUUID()}`)).status,
      ).toBe(404);
      const metrics = await (await fetch(service.base + "/metrics")).text();
      for (const name of [
        "wager_transactions_total",
        "wager_duplicates_total",
        "wager_lock_conflicts_total",
        "wager_outbox_lag_seconds",
        "wager_processing_seconds",
      ])
        expect(metrics).toContain(name);
    });

    test("SQS deduplica redelivery com inbox e reutiliza resultado da API", async () => {
      const wallet = await context.wallet();
      const input = context.input(wallet);
      const sent = await context.send(input);
      await context.worker.consume();
      await context.rawSend(sent.body, wallet.id);
      await context.worker.consume();
      expect((await context.process(input)).idempotentReplay).toBe(true);
      const inbox = await context.sql.query(
        "SELECT message_id FROM inbox WHERE message_id=?",
        [sent.envelope.messageId],
      );
      expect(inbox).toHaveLength(1);
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "90.00",
      );
      expect(await context.queueCount(queueNames.requests)).toBe(0);
    });

    test("HTTP retorna 503 com Retry-After em timeout de lock e aceita reenvio da mesma chave", async () => {
      const wallet = await context.wallet();
      const input = context.input(wallet);
      const unlock = await context.lock(wallet.id);
      const service = context.services[0]!;
      try {
        const response = await fetch(service.base + "/wagering/transactions", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "idempotency-key": input.externalTransactionId,
          },
          body: JSON.stringify(input),
        });
        expect(response.status).toBe(503);
        expect(response.headers.get("retry-after")).toBe("1");
        expect(await response.json()).toMatchObject({
          code: "INFRASTRUCTURE_UNAVAILABLE",
        });
        expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
          "100.00",
        );
      } finally {
        await unlock();
      }
      const retried = await service.post(
        "/wagering/transactions",
        input,
        input.externalTransactionId,
      );
      expect(retried.status).toBe(200);
      expect(retried.body.balance?.amount).toBe("90.00");
      expect(retried.body.idempotentReplay).toBe(false);
    }, 15_000);

    test("rejeição de negócio via SQS é terminal e recebe ack", async () => {
      const wallet = await context.wallet("0.00");
      const input = context.input(wallet);
      await context.send(input);
      await context.worker.consume();
      expect(
        await context.queries.providerTransaction(
          input.providerId,
          input.externalTransactionId,
        ),
      ).toMatchObject({
        status: "REJECTED",
        failureCode: "INSUFFICIENT_FUNDS",
      });
      expect(await context.queueCount(queueNames.requests)).toBe(0);
      expect(await context.queueCount(queueNames.dlq)).toBe(0);
    });

    test("payload inválido vai para DLQ preservando corpo original", async () => {
      const body = `invalid-json-${crypto.randomUUID()}`;
      await context.rawSend(body);
      await context.worker.consume();
      const message = await context.receive(queueNames.dlq);
      expect(message.Body).toBe(body);
      await context.ack(queueNames.dlq, message);
      expect(await context.queueCount(queueNames.requests)).toBe(0);
    });

    test("timeout de lock causa retry SQS e recuperação sem débito duplicado", async () => {
      const wallet = await context.wallet();
      const input = context.input(wallet);
      await context.send(input);
      const unlock = await context.lock(wallet.id);
      try {
        await context.worker.consume();
        expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
          "100.00",
        );
        expect(
          await context.sql.query(
            "SELECT id FROM wager_transactions WHERE external_transaction_id=?",
            [input.externalTransactionId],
          ),
        ).toHaveLength(0);
      } finally {
        await unlock();
      }
      const redelivery = await context.receive(queueNames.requests);
      expect(
        Number(redelivery.Attributes?.ApproximateReceiveCount),
      ).toBeGreaterThanOrEqual(2);
      await context.release(queueNames.requests, redelivery);
      await context.worker.consume();
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "90.00",
      );
      expect(await context.queueCount(queueNames.requests)).toBe(0);
    }, 20_000);

    test("falha transitória no quinto recebimento termina na DLQ", async () => {
      const wallet = await context.wallet();
      const input = context.input(wallet);
      const sent = await context.send(input);
      for (let i = 1; i <= 4; i++) {
        const received = await context.receive(queueNames.requests);
        expect(Number(received.Attributes?.ApproximateReceiveCount)).toBe(i);
        await context.release(queueNames.requests, received);
      }
      const unlock = await context.lock(wallet.id);
      try {
        await context.worker.consume();
      } finally {
        await unlock();
      }
      await context.drainOutbox();
      const message = await context.receive(queueNames.dlq);
      expect(message.Body).toBe(sent.body);
      await context.ack(queueNames.dlq, message);
      expect(
        await context.queries.providerTransaction(
          input.providerId,
          input.externalTransactionId,
        ),
      ).toMatchObject({ status: "FAILED", failureCode: "RETRY_EXHAUSTED" });
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "100.00",
      );
      expect(await context.queueCount(queueNames.requests)).toBe(0);
    }, 60_000);

    test("dois processos publicam a mesma outbox sem disputar os mesmos registros", async () => {
      await context.drainOutbox();
      const wallets = await Promise.all(
        Array.from({ length: 12 }, () => context.wallet("1.00")),
      );
      const start = String(Date.now() + 1200);
      const children = Array.from({ length: 2 }, () =>
        Bun.spawn([process.execPath, "tests/fixtures/publish.ts"], {
          env: { ...process.env, PUBLISH_AT: start },
          stdout: "pipe",
          stderr: "pipe",
        }),
      );
      const outputs = children.map((c) => new Response(c.stdout).text());
      const errors = children.map((c) => new Response(c.stderr).text());
      expect(await Promise.all(children.map((c) => c.exited))).toEqual([0, 0]);
      expect(await Promise.all(errors)).toEqual(["", ""]);
      expect(
        (await Promise.all(outputs)).map(
          (s) => (JSON.parse(s) as { count: number }).count,
        ),
      ).toEqual([10, 10]);
      await context.drainOutbox();
      const rows = await context.sql.query<{
        attempts: number;
        published_at: string;
      }>(
        "SELECT attempts,published_at FROM outbox WHERE aggregate_id IN (" +
          wallets.map(() => "?").join(",") +
          ")",
        wallets.map((w) => w.id),
      );
      expect(rows).toHaveLength(24);
      expect(
        rows.every((r) => r.attempts === 1 && r.published_at !== null),
      ).toBe(true);
    }, 20_000);

    test("queda após publish deixa outbox recuperável com o mesmo eventId", async () => {
      await context.drainOutbox();
      const wallet = await context.wallet("1.00");
      const before = await context.sql.query<{ id: string }>(
        "SELECT id FROM outbox WHERE aggregate_id=? ORDER BY id",
        [wallet.id],
      );
      const child = Bun.spawn([process.execPath, "tests/fixtures/publish.ts"], {
        env: { ...process.env, TEST_CRASH_AFTER_PUBLISH: "true" },
        stdout: "pipe",
        stderr: "pipe",
      });
      const output = new Response(child.stdout).text();
      const error = new Response(child.stderr).text();
      expect(await child.exited).toBe(87);
      await output;
      expect(await error).toBe("");
      const unpublished = await context.sql.query(
        "SELECT id FROM outbox WHERE aggregate_id=? AND published_at IS NULL",
        [wallet.id],
      );
      expect(unpublished).toHaveLength(2);
      await context.drainOutbox();
      const after = await context.sql.query<{ id: string }>(
        "SELECT id FROM outbox WHERE aggregate_id=? AND published_at IS NOT NULL ORDER BY id",
        [wallet.id],
      );
      expect(after).toEqual(before);
    }, 20_000);

    test("queda após commit e antes do ack suporta redelivery e reinício", async () => {
      const wallet = await context.wallet();
      const input = context.input(wallet, "BET", "25.00");
      const crashed = await context.start({
        WORKERS_ENABLED: "true",
        TEST_CRASH_AFTER_COMMIT: "true",
      });
      const sent = await context.send(input);
      const exit = await Promise.race([
        crashed.child.exited,
        Bun.sleep(10_000).then(() => "timeout"),
      ]);
      expect(exit).toBe(86);
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "75.00",
      );
      const message = await context.receive(queueNames.requests, 35_000);
      expect(JSON.parse(message.Body!).messageId).toBe(sent.envelope.messageId);
      expect(
        Number(message.Attributes?.ApproximateReceiveCount),
      ).toBeGreaterThanOrEqual(2);
      await context.release(queueNames.requests, message);
      const restarted = await context.start({ WORKERS_ENABLED: "true" });
      try {
        await eventually(
          () => context.queueCount(queueNames.requests),
          (n) => n === 0,
        );
        expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
          "75.00",
        );
        expect(
          (await context.queries.ledger(wallet.id, 50)).items,
        ).toHaveLength(2);
        const inbox = await context.sql.query(
          "SELECT message_id FROM inbox WHERE message_id=?",
          [sent.envelope.messageId],
        );
        expect(inbox).toHaveLength(1);
        const metrics = await (await fetch(restarted.base + "/metrics")).text();
        expect(metrics).toContain("wager_duplicates_total 1");
      } finally {
        await restarted.stop();
      }
    }, 55_000);

    test("shutdown do worker interrompe long polling sem aguardar timeout do broker", async () => {
      const worker = context.worker;
      worker.start();
      await Bun.sleep(150);
      const start = Date.now();
      await worker.stop();
      expect(Date.now() - start).toBeLessThan(2000);
      expect(await context.queueCount(queueNames.requests)).toBe(0);
    });
  },
);
