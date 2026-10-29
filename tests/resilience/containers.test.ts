import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  test,
} from "bun:test";
import { Processor } from "../../src/application/processor";
import { Queries } from "../../src/application/queries";
import type { TransactionResult } from "../../src/application/contracts";
import { openDatabase } from "../../src/infrastructure/database";
import { queueNames } from "../../src/infrastructure/queues";
import { config } from "../../src/infrastructure/config";
import { IntegrationContext, eventually } from "../integration/helpers";
import {
  containers,
  compose,
  docker,
  logs,
  post,
  ready,
  refreshContainers,
} from "./helpers";

const context = new IntegrationContext();
describe.skipIf(process.env.RESILIENCE_TEST_RUN !== "true")(
  "Compose, indisponibilidade e SIGTERM em Linux",
  () => {
    beforeAll(async () => {
      expect(containers).toHaveLength(3);
      context.db = await openDatabase();
      context.processor = new Processor(context.db);
      context.queries = new Queries(context.db);
      await ready();
    }, 30_000);
    afterAll(() => context.close());
    afterEach(async () => {
      if (context.db) await context.assertConsistent();
    });

    test("três containers atendem em paralelo e mantêm idempotência persistente", async () => {
      expect(new Set(containers.map((c) => c.id)).size).toBe(3);
      const wallet = await context.wallet();
      const input = context.input(wallet, "BET", "25.00");
      const responses = await Promise.all(
        Array.from({ length: 50 }, (_, i) =>
          post<TransactionResult>(
            containers[i % 3]!,
            "/wagering/transactions",
            input,
            input.externalTransactionId,
          ),
        ),
      );
      expect(responses.every((r) => r.status === 200)).toBe(true);
      expect(responses.filter((r) => !r.body.idempotentReplay)).toHaveLength(1);
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "75.00",
      );
      expect((await context.queries.ledger(wallet.id, 50)).items).toHaveLength(
        2,
      );
    });

    test("queda de PostgreSQL preserva comandos e serviço recupera após retorno do banco", async () => {
      const wallet = await context.wallet();
      const input = context.input(wallet, "BET", "15.00");
      // Hold all consumers while the database is stopped, then expose the queued command to the outage.
      await docker(["pause", ...containers.map((c) => c.id)]);
      try {
        await context.send(input);
        await compose(["stop", "-t", "1", "postgres"]);
        await docker(["unpause", ...containers.map((c) => c.id)]);
        const response = await fetch(containers[0]!.base + "/health/ready", {
          signal: AbortSignal.timeout(12_000),
        });
        expect(response.status).toBe(503);
        expect((await fetch(containers[0]!.base + "/health/live")).status).toBe(
          200,
        );
        expect(
          await context.queueCount(queueNames.requests),
        ).toBeGreaterThanOrEqual(1);
      } finally {
        await compose(["start", "postgres"]);
        await eventually(
          async () => {
            try {
              return (
                await compose([
                  "exec",
                  "-T",
                  "postgres",
                  "pg_isready",
                  "-U",
                  "jungle",
                ])
              ).includes("accepting connections");
            } catch {
              return false;
            }
          },
          Boolean,
          15_000,
        );
        const address = (await compose(["port", "postgres", "5432"])).trim();
        const url = new URL(config.DATABASE_URL);
        url.port = address.slice(address.lastIndexOf(":") + 1);
        config.DATABASE_URL = url.toString();
        await context.db.close(true);
        context.db = await openDatabase();
        context.processor = new Processor(context.db);
        context.queries = new Queries(context.db);
        await docker(["unpause", ...containers.map((c) => c.id)]).catch(
          () => {},
        );
      }
      await ready();
      await eventually(
        () =>
          context.queries
            .providerTransaction(input.providerId, input.externalTransactionId)
            .catch(() => null),
        (r) => r?.status === "PROCESSED",
        25_000,
      );
      expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
        "85.00",
      );
      expect(await context.queueCount(queueNames.requests)).toBe(0);
    }, 60_000);

    test("indisponibilidade SQS acumula outbox, aplica backoff e publica após recuperação", async () => {
      await eventually(
        async () => {
          const [r] = await context.sql.query<{ n: string }>(
            "SELECT COUNT(*)::text AS n FROM outbox WHERE published_at IS NULL",
          );
          return r!.n;
        },
        (n) => n === "0",
        20_000,
      );
      await compose(["pause", "localstack"]);
      let walletId = "";
      try {
        const wallet = await context.wallet();
        walletId = wallet.id;
        const input = context.input(wallet, "BET", "10.00");
        expect(
          (
            await post<TransactionResult>(
              containers[0]!,
              "/wagering/transactions",
              input,
              input.externalTransactionId,
            )
          ).status,
        ).toBe(200);
        expect(
          (
            await fetch(containers[0]!.base + "/health/ready", {
              signal: AbortSignal.timeout(7000),
            })
          ).status,
        ).toBe(503);
        const retried = await eventually(
          () =>
            context.sql.query<{
              attempts: number;
              next_attempt_at: string;
              published_at: string | null;
            }>(
              "SELECT attempts,next_attempt_at,published_at FROM outbox WHERE aggregate_id=?",
              [walletId],
            ),
          (rows) => rows.some((r) => r.attempts > 0),
          15_000,
        );
        expect(retried.every((r) => r.published_at === null)).toBe(true);
        expect(
          retried.some(
            (r) =>
              r.attempts > 0 &&
              new Date(r.next_attempt_at).getTime() > Date.now() - 2000,
          ),
        ).toBe(true);
        expect((await context.queries.wallet(walletId)).balance.amount).toBe(
          "90.00",
        );
      } finally {
        await compose(["unpause", "localstack"]);
      }
      await ready();
      await eventually(
        () =>
          context.sql.query<{ published_at: string | null }>(
            "SELECT published_at FROM outbox WHERE aggregate_id=?",
            [walletId],
          ),
        (rows) =>
          rows.length === 4 && rows.every((r) => r.published_at !== null),
        20_000,
      );
      const emitted = (await Promise.all(containers.map((c) => logs(c))))
        .flat()
        .filter(
          (l) => l.event === "outbox_publish_failed" && l.walletId === walletId,
        );
      expect(emitted.length).toBeGreaterThan(0);
      for (const line of emitted) {
        for (const field of [
          "correlationId",
          "messageId",
          "transactionId",
          "walletId",
          "providerId",
        ])
          expect(Object.hasOwn(line, field)).toBe(true);
        expect(line.transactionId).toBeString();
        expect(line.providerId).toBeString();
      }
    }, 60_000);

    test("SIGTERM conclui mensagem em andamento e libera o restante do lote", async () => {
      const wallet = await context.wallet();
      const first = context.input(wallet, "BET", "10.00");
      const second = context.input(wallet, "BET", "10.00");
      await docker(["stop", "--time", "15", ...containers.map((c) => c.id)]);
      // Let server-side long polls from the stopped consumers expire before creating the batch.
      await Bun.sleep(5500);
      const unlock = await context.lock(wallet.id);
      let unlocked = false;
      const active = containers[0]!;
      try {
        const sent = await context.send(first);
        const pending = await context.send(second);
        // Stopped peers have no outstanding long polls that could reserve the released message.
        await docker(["start", active.id]);
        await eventually(
          () => logs(active),
          (lines) =>
            lines.some(
              (l) =>
                l.event === "message_processing_started" &&
                l.messageId === sent.envelope.messageId,
            ),
          4000,
        );
        const prior = await logs(active);
        const priorStopping = prior.filter(
          (l) => l.event === "service_stopping",
        ).length;
        const priorStopped = prior.filter(
          (l) => l.event === "service_stopped",
        ).length;
        await docker(["kill", "--signal=SIGTERM", active.id]);
        await eventually(
          () => logs(active),
          (lines) =>
            lines.filter(
              (l) => l.event === "service_stopping" && l.signal === "SIGTERM",
            ).length > priorStopping,
          3000,
        );
        await unlock();
        unlocked = true;
        const shutdown = await eventually(
          () => logs(active),
          (lines) =>
            lines.filter(
              (l) => l.event === "service_stopped" && l.signal === "SIGTERM",
            ).length > priorStopped &&
            lines.some(
              (l) =>
                l.event === "message_released" &&
                l.messageId === pending.envelope.messageId,
            ),
          10_000,
        );
        expect(
          shutdown.some(
            (l) => l.event === "service_stopped" && l.signal === "SIGTERM",
          ),
        ).toBe(true);
        await eventually(
          async () =>
            JSON.parse(
              await docker([
                "inspect",
                "--format",
                "{{json .State}}",
                active.id,
              ]),
            ) as { Running: boolean; ExitCode: number },
          (s) => !s.Running,
          5000,
        );
        expect(
          (
            await context.queries.providerTransaction(
              first.providerId,
              first.externalTransactionId,
            )
          ).status,
        ).toBe("PROCESSED");
        expect((await context.queries.wallet(wallet.id)).balance.amount).toBe(
          "90.00",
        );
        expect(
          await context.queueCount(queueNames.requests),
        ).toBeGreaterThanOrEqual(1);
        const released = await context.receive(queueNames.requests);
        expect(JSON.parse(released.Body!).data.externalTransactionId).toBe(
          second.externalTransactionId,
        );
        await context.release(queueNames.requests, released);
      } finally {
        if (!unlocked) await unlock();
        for (const c of containers) {
          await docker(["start", c.id]);
        }
        await refreshContainers();
      }
      await ready();
      await eventually(
        () => context.queries.wallet(wallet.id),
        (w) => w.balance.amount === "80.00",
      );
      expect((await context.queries.ledger(wallet.id, 50)).items).toHaveLength(
        3,
      );
    }, 45_000);
  },
);
