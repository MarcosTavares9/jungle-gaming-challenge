import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type Message,
} from "@aws-sdk/client-sqs";
import { ZodError } from "zod";
import { messageSchema, payloadHash } from "../application/contracts";
import { Processor } from "../application/processor";
import { ConflictError, DomainError } from "../domain/errors";
import { OutboxMessage } from "../domain/events";
import { type Database, Sql } from "./database";
import { errorCode, log, metrics } from "./telemetry";
import { Queues, queueNames } from "./queues";

export class Workers {
  private stopping = false;
  private jobs: Promise<void>[] = [];
  private readonly abortReceive = new AbortController();
  constructor(
    private readonly db: Database,
    private readonly processor: Processor,
    private readonly queues: Queues,
  ) {}
  start() {
    this.jobs = [
      this.loop("sqs", () => this.consume()),
      this.loop("outbox", () => this.publish(), 300),
      this.loop(
        "references",
        () => this.processor.retryReferences(this.abortReceive.signal),
        500,
      ),
    ];
  }
  async stop() {
    this.stopping = true;
    this.abortReceive.abort();
    await Promise.allSettled(this.jobs);
  }
  private async loop(name: string, work: () => Promise<unknown>, delay = 100) {
    while (!this.stopping) {
      try {
        await work();
      } catch (error) {
        if (!this.stopping) {
          metrics.retries.inc({ worker: name });
          log("warn", "worker_retry", { worker: name, code: errorCode(error) });
          await Bun.sleep(1000);
        }
      }
      if (!this.stopping) await Bun.sleep(delay);
    }
  }
  async publish(): Promise<number> {
    const queueUrl = await this.queues.url(queueNames.events);
    return this.db.em.fork().transactional(async (em) => {
      const sql = new Sql(em);
      const rows = await sql.query<{
        id: string;
        aggregate_id: string;
        event_type: string;
        payload: Record<string, unknown>;
        occurred_at: Date;
        attempts: number;
        next_attempt_at: Date;
      }>(
        `SELECT * FROM outbox WHERE published_at IS NULL AND next_attempt_at <= now() ORDER BY occurred_at,id LIMIT 10 FOR UPDATE SKIP LOCKED`,
      );
      let sent = 0;
      for (const row of rows) {
        if (this.stopping) break;
        const event = OutboxMessage.rehydrate({
          id: row.id,
          aggregateId: row.aggregate_id,
          eventType: row.event_type,
          payload: row.payload,
          occurredAt: new Date(row.occurred_at),
          attempts: row.attempts,
          nextAttemptAt: new Date(row.next_attempt_at),
        });
        try {
          await this.queues.client.send(
            new SendMessageCommand({
              QueueUrl: queueUrl,
              MessageBody: JSON.stringify(event.payload),
              MessageGroupId: event.aggregateId,
              MessageDeduplicationId: event.id,
            }),
            { abortSignal: AbortSignal.timeout(5000) },
          );
          if (process.env.TEST_CRASH_AFTER_PUBLISH === "true") process.exit(87);
          event.markPublished(new Date());
          await sql.query(
            "UPDATE outbox SET published_at = ?, attempts = attempts + 1 WHERE id = ?",
            [event.publishedAt, event.id],
          );
          sent += 1;
        } catch (error) {
          event.scheduleRetry(new Date());
          await sql.query(
            "UPDATE outbox SET attempts = ?, next_attempt_at = ? WHERE id = ?",
            [event.attempts, event.nextAttemptAt, event.id],
          );
          metrics.retries.inc({ worker: "outbox" });
          log("warn", "outbox_publish_failed", {
            eventId: event.id,
            walletId: event.aggregateId,
            code: errorCode(error),
          });
        }
      }
      const [lag] = await sql.query<{ seconds: string | null }>(
        `SELECT EXTRACT(EPOCH FROM now() - MIN(occurred_at))::text AS seconds FROM outbox WHERE published_at IS NULL`,
      );
      metrics.outboxLag.set(Math.max(0, Number(lag?.seconds ?? 0)));
      return sent;
    });
  }
  async consume(): Promise<void> {
    const url = await this.queues.url(queueNames.requests);
    const result = await this.queues.client.send(
      new ReceiveMessageCommand({
        QueueUrl: url,
        WaitTimeSeconds: 5,
        MaxNumberOfMessages: 10,
        VisibilityTimeout: 30,
        MessageSystemAttributeNames: [
          "ApproximateReceiveCount",
          "MessageGroupId",
        ],
      }),
      { abortSignal: this.abortReceive.signal },
    );
    // FIFO preserves order inside each wallet group; different groups run concurrently.
    const groups = new Map<string, Message[]>();
    for (const m of result.Messages ?? []) {
      const group = m.Attributes?.MessageGroupId ?? m.MessageId ?? "unknown";
      groups.set(group, [...(groups.get(group) ?? []), m]);
    }
    const groupsResult = await Promise.allSettled(
      [...groups.values()].map(async (messages) => {
        let blocked = false;
        for (const message of messages) {
          if (this.stopping || blocked) {
            await this.release(url, message);
            continue;
          }
          blocked = !(await this.handle(url, message));
        }
      }),
    );
    const failed = groupsResult.find((r) => r.status === "rejected");
    if (failed?.status === "rejected") throw failed.reason;
  }
  private async handle(url: string, message: Message): Promise<boolean> {
    let visibilityBusy = false;
    const heartbeat = setInterval(() => {
      if (visibilityBusy) return;
      visibilityBusy = true;
      void this.queues.client
        .send(
          new ChangeMessageVisibilityCommand({
            QueueUrl: url,
            ReceiptHandle: message.ReceiptHandle,
            VisibilityTimeout: 30,
          }),
        )
        .catch(() =>
          log("warn", "visibility_extension_failed", {
            messageId: message.MessageId,
          }),
        )
        .finally(() => {
          visibilityBusy = false;
        });
    }, 10_000);
    try {
      const envelope = messageSchema.parse(JSON.parse(message.Body ?? ""));
      const { idempotencyKey, ...input } = envelope.data;
      await this.processor.process(input, idempotencyKey, {
        correlationId: envelope.messageId,
        causationId: envelope.messageId,
        inbox: {
          messageId: envelope.messageId,
          hash: payloadHash({ input, idempotencyKey }),
        },
      });
      // Test fault injection is only enabled explicitly by the integration harness.
      if (process.env.TEST_CRASH_AFTER_COMMIT === "true") process.exit(86);
      await this.queues.client.send(
        new DeleteMessageCommand({
          QueueUrl: url,
          ReceiptHandle: message.ReceiptHandle,
        }),
      );
      return true;
    } catch (error) {
      const receives = Number(
        message.Attributes?.ApproximateReceiveCount ?? "1",
      );
      const permanent =
        error instanceof ZodError ||
        error instanceof SyntaxError ||
        error instanceof ConflictError ||
        error instanceof DomainError;
      if (permanent || receives >= 5) {
        await this.queues.client.send(
          new SendMessageCommand({
            QueueUrl: await this.queues.url(queueNames.dlq),
            MessageBody: message.Body ?? "{}",
            MessageGroupId: "dead-letter",
            MessageDeduplicationId: message.MessageId ?? crypto.randomUUID(),
          }),
        );
        await this.queues.client.send(
          new DeleteMessageCommand({
            QueueUrl: url,
            ReceiptHandle: message.ReceiptHandle,
          }),
        );
        metrics.dlq.inc();
        log("error", "message_dead_lettered", {
          messageId: message.MessageId,
          code: error instanceof ConflictError ? error.code : errorCode(error),
          receives,
        });
        return true;
      }
      metrics.retries.inc({ worker: "sqs" });
      await this.queues.client.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: url,
          ReceiptHandle: message.ReceiptHandle,
          VisibilityTimeout: Math.min(60, 2 ** receives),
        }),
      );
      log("warn", "message_retry", {
        messageId: message.MessageId,
        code: errorCode(error),
        receives,
      });
      return false;
    } finally {
      clearInterval(heartbeat);
    }
  }
  private async release(url: string, message: Message) {
    await this.queues.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: url,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: 0,
      }),
    );
  }
}
