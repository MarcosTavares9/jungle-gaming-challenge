import { expect } from "bun:test";
import { createServer } from "node:net";
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type Message,
} from "@aws-sdk/client-sqs";
import { Processor } from "../../src/application/processor";
import { Queries } from "../../src/application/queries";
import type {
  TransactionResult,
  WagerInput,
} from "../../src/application/contracts";
import { Money } from "../../src/domain/money";
import {
  openDatabase,
  Sql,
  type Database,
} from "../../src/infrastructure/database";
import { Queues, queueNames } from "../../src/infrastructure/queues";
import { Workers } from "../../src/infrastructure/workers";

export async function eventually<T>(
  read: () => Promise<T>,
  accepted: (value: T) => boolean,
  timeout = 10_000,
): Promise<T> {
  const deadline = Date.now() + timeout;
  let last: T | undefined;
  while (Date.now() < deadline) {
    last = await read();
    if (accepted(last)) return last;
    await Bun.sleep(100);
  }
  throw new Error(
    `Condição não atingida em ${timeout} ms: ${JSON.stringify(last)}`,
  );
}
export async function freePort(): Promise<number> {
  const server = createServer();
  return new Promise((resolve, reject) => {
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string")
        return reject(new Error("Porta indisponível"));
      server.close((error) => (error ? reject(error) : resolve(address.port)));
    });
  });
}
export class ServiceProcess {
  private constructor(
    readonly child: ReturnType<typeof Bun.spawn<"ignore", "pipe", "pipe">>,
    readonly port: number,
    private readonly output: Promise<string>,
    private readonly errors: Promise<string>,
  ) {}
  get base() {
    return `http://127.0.0.1:${this.port}`;
  }
  static async start(env: Record<string, string> = {}) {
    const port = await freePort();
    const child = Bun.spawn([process.execPath, "src/main.ts"], {
      env: {
        ...process.env,
        PORT: String(port),
        WORKERS_ENABLED: "false",
        ...env,
      },
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    });
    const instance = new ServiceProcess(
      child,
      port,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    );
    try {
      await eventually(
        async () => {
          if (child.exitCode !== null)
            throw new Error(`Serviço encerrou: ${await instance.logs()}`);
          try {
            return (
              await fetch(`${instance.base}/health/ready`, {
                signal: AbortSignal.timeout(1000),
              })
            ).ok;
          } catch {
            return false;
          }
        },
        Boolean,
        30_000,
      );
      return instance;
    } catch (error) {
      await instance.stop();
      const reason = error instanceof Error ? error.message : String(error);
      throw new Error(
        `Falha ao iniciar serviço: ${reason}\n${await instance.logs()}`,
        { cause: error },
      );
    }
  }
  async logs() {
    return `${await this.output}\n${await this.errors}`;
  }
  async stop() {
    if (this.child.exitCode === null) this.child.kill();
    await this.child.exited;
  }
  async post<T = TransactionResult>(
    path: string,
    body?: unknown,
    key?: string,
  ) {
    const response = await fetch(this.base + path, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(key ? { "idempotency-key": key } : {}),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    return { status: response.status, body: (await response.json()) as T };
  }
}
export type WalletResponse = Awaited<ReturnType<Processor["openWallet"]>>;
export class IntegrationContext {
  db!: Database;
  queues = new Queues();
  processor!: Processor;
  queries!: Queries;
  services: ServiceProcess[] = [];
  get sql() {
    return new Sql(this.db.em.fork());
  }
  get worker() {
    return new Workers(this.db, this.processor, this.queues);
  }
  async setup() {
    if (
      process.env.INTEGRATION_TEST_RUN !== "true" ||
      !/\/jungle_test_[a-f0-9]{32}(\?|$)/.test(
        process.env.DATABASE_URL ?? "",
      ) ||
      !/^test-[a-f0-9]{12}-$/.test(process.env.SQS_QUEUE_PREFIX ?? "")
    )
      throw new Error(
        "Use bun run test:integration para criar recursos isolados",
      );
    this.db = await openDatabase();
    await this.db.migrator.up();
    await this.queues.setup();
    this.processor = new Processor(this.db);
    this.queries = new Queries(this.db);
  }
  async start(env: Record<string, string> = {}) {
    const service = await ServiceProcess.start(env);
    this.services.push(service);
    return service;
  }
  async close() {
    await Promise.all(this.services.map((s) => s.stop()));
    this.queues.close();
    if (this.db) await this.db.close(true);
  }
  wallet(amount = "100.00") {
    return this.processor.openWallet(
      crypto.randomUUID(),
      Money.from({ amount, currency: "BRL" }),
    );
  }
  input(
    wallet: WalletResponse,
    kind: WagerInput["kind"] = "BET",
    amount = "10.00",
    reference?: string,
  ): WagerInput {
    return {
      providerId: "integration",
      externalTransactionId: crypto.randomUUID(),
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: "round",
      gameId: "game",
      kind,
      money: { amount: kind === "LOSS" ? "0.00" : amount, currency: "BRL" },
      ...(reference ? { referenceExternalTransactionId: reference } : {}),
    };
  }
  process(input: WagerInput, key = input.externalTransactionId) {
    return this.processor.process(input, key, {
      correlationId: input.externalTransactionId,
    });
  }
  async assertConsistent() {
    const bad = await this.sql
      .query(`SELECT w.id FROM wallets w LEFT JOIN wallet_ledger l ON l.wallet_id=w.id
      GROUP BY w.id HAVING w.balance <> COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END),0) OR w.balance < 0`);
    expect(bad).toEqual([]);
    const invalidTransactions = await this.sql
      .query(`SELECT t.id FROM wager_transactions t LEFT JOIN wallet_ledger l ON l.transaction_id=t.id
      GROUP BY t.id HAVING (t.status='PROCESSED' AND t.kind<>'LOSS' AND COUNT(l.id)<>1)
      OR ((t.status<>'PROCESSED' OR t.kind='LOSS') AND COUNT(l.id)<>0)`);
    expect(invalidTransactions).toEqual([]);
  }
  async due(id: string) {
    await this.sql.query(
      "UPDATE wager_transactions SET next_attempt_at=now() WHERE id=?",
      [id],
    );
  }
  async send(input: WagerInput, messageId: string = crypto.randomUUID()) {
    const envelope = {
      messageId,
      type: "WagerTransactionRequested",
      occurredAt: new Date().toISOString(),
      data: { ...input, idempotencyKey: input.externalTransactionId },
    };
    const body = JSON.stringify(envelope);
    await this.rawSend(body, input.walletId);
    return { envelope, body };
  }
  async rawSend(body: string, walletId: string = crypto.randomUUID()) {
    await this.queues.client.send(
      new SendMessageCommand({
        QueueUrl: await this.queues.url(queueNames.requests),
        MessageBody: body,
        MessageGroupId: walletId,
        MessageDeduplicationId: crypto.randomUUID(),
      }),
    );
  }
  async receive(name: string, timeout = 10_000): Promise<Message> {
    const messages = await eventually(
      async () => {
        const result = await this.queues.client.send(
          new ReceiveMessageCommand({
            QueueUrl: await this.queues.url(name),
            WaitTimeSeconds: 1,
            MaxNumberOfMessages: 1,
            MessageSystemAttributeNames: [
              "ApproximateReceiveCount",
              "MessageGroupId",
            ],
          }),
        );
        return result.Messages ?? [];
      },
      (m) => m.length > 0,
      timeout,
    );
    return messages[0]!;
  }
  async release(name: string, message: Message) {
    await this.queues.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: await this.queues.url(name),
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: 0,
      }),
    );
  }
  async ack(name: string, message: Message) {
    await this.queues.client.send(
      new DeleteMessageCommand({
        QueueUrl: await this.queues.url(name),
        ReceiptHandle: message.ReceiptHandle,
      }),
    );
  }
  async queueCount(name: string) {
    const result = await this.queues.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: await this.queues.url(name),
        AttributeNames: [
          "ApproximateNumberOfMessages",
          "ApproximateNumberOfMessagesNotVisible",
        ],
      }),
    );
    return (
      Number(result.Attributes?.ApproximateNumberOfMessages ?? 0) +
      Number(result.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0)
    );
  }
  async drainOutbox() {
    await eventually(
      async () => {
        await this.worker.publish();
        const [row] = await this.sql.query<{ n: string }>(
          "SELECT COUNT(*)::text AS n FROM outbox WHERE published_at IS NULL",
        );
        return row!.n;
      },
      (n) => n === "0",
      30_000,
    );
  }
  async lock(walletId: string) {
    const em = this.db.em.fork();
    await em.begin();
    await new Sql(em).query("SELECT id FROM wallets WHERE id=? FOR UPDATE", [
      walletId,
    ]);
    return () => em.rollback();
  }
}
