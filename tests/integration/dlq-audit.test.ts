import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  test,
} from "bun:test";
import { SendMessageCommand } from "@aws-sdk/client-sqs";
import { queueNames } from "../../src/infrastructure/queues";
import { DeadLetterArchive } from "../../src/infrastructure/dead-letter-archive";
import { IntegrationContext, type WalletResponse } from "./helpers";

const context = new IntegrationContext();
async function send(body: string, groupId: string) {
  const result = await context.queues.client.send(
    new SendMessageCommand({
      QueueUrl: await context.queues.url(queueNames.dlq),
      MessageBody: body,
      MessageGroupId: groupId,
      MessageDeduplicationId: crypto.randomUUID(),
    }),
  );
  return result.MessageId!;
}
function envelope(wallet: WalletResponse) {
  const input = context.input(wallet);
  const messageId = crypto.randomUUID();
  return {
    input,
    body: JSON.stringify({
      messageId,
      type: "WagerTransactionRequested",
      occurredAt: new Date().toISOString(),
      data: { ...input, idempotencyKey: input.externalTransactionId },
    }),
  };
}
async function audit() {
  const child = Bun.spawn([process.execPath, "scripts/audit-dlq.ts"], {
    env: process.env,
    stdout: "pipe",
    stderr: "pipe",
  });
  const output = new Response(child.stdout).text();
  const errors = new Response(child.stderr).text();
  const exit = await child.exited;
  return { exit, output: await output, errors: await errors };
}

describe.skipIf(process.env.INTEGRATION_TEST_RUN !== "true")(
  "Regressões do review: auditoria DLQ",
  () => {
    beforeAll(() => context.setup(), 15_000);
    afterAll(() => context.close());
    afterEach(async () => {
      await context.assertConsistent();
      await context.drainOutbox();
      while ((await context.queueCount(queueNames.dlq)) > 0) {
        const message = await context.receive(queueNames.dlq);
        await context.ack(queueNames.dlq, message);
      }
    }, 45_000);

    test("dois lotes avançam sobre 11 mensagens do mesmo grupo FIFO preservando os corpos", async () => {
      const wallet = await context.wallet("0.00");
      const sent: { id: string; body: string }[] = [];
      for (let i = 0; i < 11; i++) {
        const item = envelope(wallet);
        sent.push({ id: await send(item.body, wallet.id), body: item.body });
      }
      expect((await audit()).exit).toBe(0);
      const first = await context.sql.query(
        "SELECT message_id FROM dlq_archive WHERE message_id IN (" +
          sent.map(() => "?").join(",") +
          ")",
        sent.map((m) => m.id),
      );
      expect(first).toHaveLength(10);
      expect(await context.queueCount(queueNames.dlq)).toBe(1);
      expect((await audit()).exit).toBe(0);
      expect(await context.queueCount(queueNames.dlq)).toBe(0);
      const archived = await context.sql.query<{
        message_id: string;
        body: string;
      }>(
        "SELECT message_id,body FROM dlq_archive WHERE message_id IN (" +
          sent.map(() => "?").join(",") +
          ")",
        sent.map((m) => m.id),
      );
      expect(archived).toHaveLength(11);
      for (const item of sent)
        expect(archived.find((r) => r.message_id === item.id)?.body).toBe(
          item.body,
        );
      const [failed] = await context.sql.query<{ n: string }>(
        "SELECT COUNT(*)::text AS n FROM wager_transactions WHERE wallet_id=? AND status='FAILED'",
        [wallet.id],
      );
      expect(failed!.n).toBe("11");
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "0.00",
      );
      expect((await context.queries.ledger(wallet.id, 50)).items).toHaveLength(
        0,
      );
    }, 30_000);

    test("falha de banco retorna exit 1 e mantém todo o lote na DLQ para nova tentativa", async () => {
      const wallet = await context.wallet();
      const first = envelope(wallet);
      const second = envelope(wallet);
      await send(first.body, wallet.id);
      await send(second.body, wallet.id);
      await context.sql
        .query(`CREATE FUNCTION audit_test_deny() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.wallet_id='${wallet.id}'::uuid THEN RAISE EXCEPTION 'audit permission denied' USING ERRCODE='42501';END IF;RETURN NEW;END $$`);
      await context.sql.query(
        "CREATE TRIGGER audit_test_deny BEFORE INSERT ON wager_transactions FOR EACH ROW EXECUTE FUNCTION audit_test_deny()",
      );
      let result: Awaited<ReturnType<typeof audit>>;
      try {
        result = await audit();
      } finally {
        await context.sql.query(
          "DROP TRIGGER audit_test_deny ON wager_transactions",
        );
        await context.sql.query("DROP FUNCTION audit_test_deny()");
      }
      expect(result.exit).toBe(1);
      expect(result.output).toContain("dlq_audit_failed");
      expect(result.output).toContain("42501");
      expect(await context.queueCount(queueNames.dlq)).toBe(2);
      expect(
        await context.sql.query(
          "SELECT id FROM wager_transactions WHERE external_transaction_id=?",
          [first.input.externalTransactionId],
        ),
      ).toHaveLength(0);
      expect((await audit()).exit).toBe(0);
      expect(await context.queueCount(queueNames.dlq)).toBe(0);
      expect(
        (
          await context.queries.providerTransaction(
            second.input.providerId,
            second.input.externalTransactionId,
          )
        ).status,
      ).toBe("FAILED");
    }, 30_000);

    test("falha do arquivo não confirma a mensagem e retry não duplica transação nem evento", async () => {
      const wallet = await context.wallet();
      const item = envelope(wallet);
      const id = await send(item.body, wallet.id);
      await context.sql
        .query(`CREATE FUNCTION audit_archive_deny() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.message_id='${id}' THEN RAISE EXCEPTION 'archive permission denied' USING ERRCODE='42501';END IF;RETURN NEW;END $$`);
      await context.sql.query(
        "CREATE TRIGGER audit_archive_deny BEFORE INSERT ON dlq_archive FOR EACH ROW EXECUTE FUNCTION audit_archive_deny()",
      );
      let result: Awaited<ReturnType<typeof audit>>;
      try {
        result = await audit();
      } finally {
        await context.sql.query(
          "DROP TRIGGER audit_archive_deny ON dlq_archive",
        );
        await context.sql.query("DROP FUNCTION audit_archive_deny()");
      }
      expect(result.exit).toBe(1);
      expect(await context.queueCount(queueNames.dlq)).toBe(1);
      expect(
        (
          await context.queries.providerTransaction(
            item.input.providerId,
            item.input.externalTransactionId,
          )
        ).status,
      ).toBe("FAILED");
      expect(
        await context.sql.query(
          "SELECT message_id FROM dlq_archive WHERE message_id=?",
          [id],
        ),
      ).toHaveLength(0);
      expect((await audit()).exit).toBe(0);
      expect(await context.queueCount(queueNames.dlq)).toBe(0);
      expect(
        await context.sql.query(
          "SELECT id FROM outbox WHERE aggregate_id=? AND event_type='WagerTransactionFailed'",
          [wallet.id],
        ),
      ).toHaveLength(1);
      expect(
        await context.sql.query(
          "SELECT message_id FROM dlq_archive WHERE message_id=?",
          [id],
        ),
      ).toHaveLength(1);
    }, 30_000);

    test("payload inválido é preservado no arquivo e não bloqueia o FIFO", async () => {
      const body = "invalid-json-" + crypto.randomUUID();
      const id = await send(body, crypto.randomUUID());
      const result = await audit();
      expect(result.exit).toBe(0);
      expect(await context.queueCount(queueNames.dlq)).toBe(0);
      const [row] = await context.sql.query<{
        body: string;
        outcome: string;
        failure_code: string;
      }>(
        "SELECT body,outcome,failure_code FROM dlq_archive WHERE message_id=?",
        [id],
      );
      expect(row).toEqual({
        body,
        outcome: "INVALID_PAYLOAD",
        failure_code: "INVALID_PAYLOAD",
      });
    });

    test("conflito é arquivado sem substituir o resultado financeiro original", async () => {
      const wallet = await context.wallet();
      const item = envelope(wallet);
      const processed = await context.process(item.input);
      const changed = JSON.parse(item.body);
      changed.data.money.amount = "11.00";
      const body = JSON.stringify(changed);
      const id = await send(body, wallet.id);
      expect((await audit()).exit).toBe(0);
      const [row] = await context.sql.query<{
        body: string;
        outcome: string;
        failure_code: string;
      }>(
        "SELECT body,outcome,failure_code FROM dlq_archive WHERE message_id=?",
        [id],
      );
      expect(row).toEqual({
        body,
        outcome: "CONFLICT",
        failure_code: "IDEMPOTENCY_CONFLICT",
      });
      expect(await context.process(item.input)).toEqual({
        ...processed,
        idempotentReplay: true,
      });
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "90.00",
      );
    });

    test("arquivo é idempotente, append-only e rejeita alteração do corpo para a mesma entrega", async () => {
      const archive = new DeadLetterArchive(context.db);
      const record = {
        sourceQueue: queueNames.dlq,
        messageId: crypto.randomUUID(),
        body: "original",
        outcome: "INVALID_PAYLOAD" as const,
      };
      await archive.save(record);
      await archive.save(record);
      expect(
        await context.sql.query(
          "SELECT message_id FROM dlq_archive WHERE message_id=?",
          [record.messageId],
        ),
      ).toHaveLength(1);
      await expect(
        archive.save({ ...record, body: "changed" }),
      ).rejects.toThrow("DLQ archive payload conflict");
      await expect(
        context.sql.query("UPDATE dlq_archive SET body=? WHERE message_id=?", [
          "changed",
          record.messageId,
        ]),
      ).rejects.toMatchObject({ code: "23514" });
      await expect(
        context.sql.query("DELETE FROM dlq_archive WHERE message_id=?", [
          record.messageId,
        ]),
      ).rejects.toMatchObject({ code: "23514" });
    });
  },
);
