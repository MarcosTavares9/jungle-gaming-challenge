import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  test,
} from "bun:test";
import { payloadHash } from "../../src/application/contracts";
import { queueNames } from "../../src/infrastructure/queues";
import { IntegrationContext } from "./helpers";

const context = new IntegrationContext();
describe.skipIf(process.env.INTEGRATION_TEST_RUN !== "true")(
  "Falha permanente e entrega durável à DLQ",
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
    }, 15_000);

    test("FAILED, inbox, evento e intenção DLQ são atômicos e idempotentes", async () => {
      const wallet = await context.wallet();
      const input = context.input(wallet);
      const messageId = crypto.randomUUID();
      const ctx = {
        correlationId: messageId,
        causationId: messageId,
        inbox: { messageId, hash: payloadHash(input) },
      };
      const delivery = {
        messageId: crypto.randomUUID(),
        body: JSON.stringify({ test: messageId }),
      };
      const failed = await context.processor.recordPermanentFailure(
        input,
        input.externalTransactionId,
        ctx,
        "INFRASTRUCTURE_PERMANENT",
        delivery,
      );
      expect(failed).toMatchObject({
        status: "FAILED",
        failureCode: "INFRASTRUCTURE_PERMANENT",
        idempotentReplay: false,
      });
      const replay = await context.processor.recordPermanentFailure(
        input,
        input.externalTransactionId,
        ctx,
        "RETRY_EXHAUSTED",
        delivery,
      );
      expect(replay).toEqual({ ...failed, idempotentReplay: true });
      expect(await context.process(input)).toEqual({
        ...failed,
        idempotentReplay: true,
      });
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "100.00",
      );
      const rows = await context.sql.query<{
        destination: string;
        event_type: string;
      }>(
        "SELECT destination,event_type FROM outbox WHERE aggregate_id=? AND event_type IN (?,?)",
        [
          wallet.id,
          "WagerTransactionFailed",
          "WagerMessageDeadLetterRequested",
        ],
      );
      expect(rows).toHaveLength(2);
      expect(rows.filter((r) => r.destination === "dlq")).toHaveLength(1);
      await expect(
        context.db.migrator.down({ to: "Migration202610290001" }),
      ).rejects.toThrow("Publish pending DLQ intents");
      expect(
        await context.sql.query(
          "SELECT message_id FROM inbox WHERE message_id=?",
          [messageId],
        ),
      ).toHaveLength(1);
      expect(
        await context.sql.query(
          "SELECT id FROM wallet_ledger WHERE transaction_id=?",
          [failed.transactionId],
        ),
      ).toHaveLength(0);
      await context.drainOutbox();
      const dead = await context.receive(queueNames.dlq);
      expect(dead.Body).toBe(delivery.body);
      await context.ack(queueNames.dlq, dead);
    });

    test("falha ao gravar intenção DLQ desfaz também FAILED e inbox", async () => {
      const wallet = await context.wallet();
      const input = context.input(wallet);
      const messageId = crypto.randomUUID();
      const ctx = {
        correlationId: messageId,
        inbox: { messageId, hash: payloadHash(input) },
      };
      await context.sql
        .query(`CREATE FUNCTION test_abort_dlq() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.destination='dlq' AND NEW.aggregate_id='${wallet.id}'::uuid THEN RAISE EXCEPTION 'DLQ intent failure'; END IF;RETURN NEW;END $$`);
      await context.sql.query(
        "CREATE TRIGGER test_abort_dlq BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION test_abort_dlq()",
      );
      try {
        await expect(
          context.processor.recordPermanentFailure(
            input,
            input.externalTransactionId,
            ctx,
            "RETRY_EXHAUSTED",
            { body: "original", messageId },
          ),
        ).rejects.toThrow("DLQ intent failure");
      } finally {
        await context.sql.query("DROP TRIGGER test_abort_dlq ON outbox");
        await context.sql.query("DROP FUNCTION test_abort_dlq()");
      }
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
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "100.00",
      );
    });

    test("falha de transporte não sobrescreve transação já processada", async () => {
      const wallet = await context.wallet();
      const input = context.input(wallet);
      const processed = await context.process(input);
      const messageId = crypto.randomUUID();
      const failed = await context.processor.recordPermanentFailure(
        input,
        input.externalTransactionId,
        {
          correlationId: messageId,
          inbox: { messageId, hash: payloadHash(input) },
        },
        "RETRY_EXHAUSTED",
        { body: "original", messageId },
      );
      expect(failed).toEqual({ ...processed, idempotentReplay: true });
      expect(
        await context.sql.query(
          "SELECT id FROM outbox WHERE destination='dlq' AND aggregate_id=?",
          [wallet.id],
        ),
      ).toHaveLength(0);
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "90.00",
      );
    });

    test("erro SQL permanente persiste FAILED antes de confirmar a mensagem SQS", async () => {
      const wallet = await context.wallet();
      const input = context.input(wallet);
      const sent = await context.send(input);
      await context.sql
        .query(`CREATE FUNCTION test_permission_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.wallet_id='${wallet.id}'::uuid THEN RAISE EXCEPTION 'permission denied in test' USING ERRCODE='42501'; END IF;RETURN NEW;END $$`);
      await context.sql.query(
        "CREATE TRIGGER test_permission_failure BEFORE INSERT ON wallet_ledger FOR EACH ROW EXECUTE FUNCTION test_permission_failure()",
      );
      try {
        await context.worker.consume();
      } finally {
        await context.sql.query(
          "DROP TRIGGER test_permission_failure ON wallet_ledger",
        );
        await context.sql.query("DROP FUNCTION test_permission_failure()");
      }
      expect(
        await context.queries.providerTransaction(
          input.providerId,
          input.externalTransactionId,
        ),
      ).toMatchObject({
        status: "FAILED",
        failureCode: "INFRASTRUCTURE_PERMANENT",
      });
      expect(await context.queueCount(queueNames.requests)).toBe(0);
      const pending = await context.sql.query(
        "SELECT id FROM outbox WHERE destination='dlq' AND aggregate_id=? AND published_at IS NULL",
        [wallet.id],
      );
      expect(pending).toHaveLength(1);
      await context.drainOutbox();
      const dead = await context.receive(queueNames.dlq);
      expect(dead.Body).toBe(sent.body);
      await context.ack(queueNames.dlq, dead);
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "100.00",
      );
    });

    test("redrive nativo é arquivado antes de sair da DLQ após retorno do banco", async () => {
      const wallet = await context.wallet();
      const input = context.input(wallet);
      const sent = await context.send(input);
      for (let i = 0; i < 5; i++) {
        const message = await context.receive(queueNames.requests);
        await context.release(queueNames.requests, message);
      }
      const { ReceiveMessageCommand } = await import("@aws-sdk/client-sqs");
      await context.queues.client.send(
        new ReceiveMessageCommand({
          QueueUrl: await context.queues.url(queueNames.requests),
          WaitTimeSeconds: 1,
        }),
      );
      const child = Bun.spawn([process.execPath, "scripts/audit-dlq.ts"], {
        env: process.env,
        stdout: "pipe",
        stderr: "pipe",
      });
      const output = new Response(child.stdout).text();
      const errors = new Response(child.stderr).text();
      expect(await child.exited).toBe(0);
      await output;
      expect(await errors).toBe("");
      expect(
        await context.queries.providerTransaction(
          input.providerId,
          input.externalTransactionId,
        ),
      ).toMatchObject({ status: "FAILED", failureCode: "RETRY_EXHAUSTED" });
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "100.00",
      );
      expect(await context.queueCount(queueNames.dlq)).toBe(0);
      const archived = await context.sql.query<{
        body: string;
        outcome: string;
      }>(
        "SELECT body,outcome FROM dlq_archive WHERE source_queue=? AND body=?",
        [queueNames.dlq, sent.body],
      );
      expect(archived).toEqual([{ body: sent.body, outcome: "AUDITED" }]);
      expect(
        await context.sql.query(
          "SELECT id FROM outbox WHERE destination='dlq' AND aggregate_id=?",
          [wallet.id],
        ),
      ).toHaveLength(0);
    }, 15_000);
  },
);
