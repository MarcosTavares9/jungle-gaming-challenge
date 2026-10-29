import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type Message,
} from "@aws-sdk/client-sqs";
import { ZodError } from "zod";
import {
  messageSchema,
  payloadHash,
  type WagerInput,
} from "../application/contracts";
import { Processor } from "../application/processor";
import { ConflictError, DomainError } from "../domain/errors";
import { OutboxMessage } from "../domain/events";
import { type Database, Sql } from "./database";
import { errorCode, log, metrics } from "./telemetry";
import { Queues, queueNames } from "./queues";
import { isPermanentInfrastructureError } from "./failures";

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
    log("info", "worker_stopping");
    this.abortReceive.abort();
    await Promise.allSettled(this.jobs);
    log("info", "worker_stopped");
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
        destination: "events" | "dlq";
        provider_id: string | null;
      }>(
        `SELECT o.*,t.provider_id FROM outbox o LEFT JOIN wager_transactions t
          ON t.id=COALESCE(o.payload->'data'->>'transactionId',o.payload->>'transactionId')::uuid
         WHERE o.published_at IS NULL AND o.next_attempt_at <= now()
         ORDER BY o.occurred_at,o.id LIMIT 10 FOR UPDATE OF o SKIP LOCKED`,
      );
      let sent = 0;
      for (const row of rows) {
        if (this.stopping) break;
        const data = (row.payload.data ?? {}) as Record<string, unknown>;
        const trace = {
          correlationId: row.payload.correlationId,
          messageId: row.payload.messageId ?? row.payload.causationId,
          transactionId: data.transactionId ?? row.payload.transactionId,
          walletId: row.aggregate_id,
          providerId: row.provider_id,
        };
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
          const queueUrl = await this.queues.url(
            row.destination === "dlq" ? queueNames.dlq : queueNames.events,
          );
          await this.queues.client.send(
            new SendMessageCommand({
              QueueUrl: queueUrl,
              MessageBody:
                row.destination === "dlq"
                  ? String(row.payload.body)
                  : JSON.stringify(event.payload),
              MessageGroupId: event.aggregateId,
              MessageDeduplicationId:
                row.destination === "dlq"
                  ? String(row.payload.deduplicationId)
                  : event.id,
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
          if (row.destination === "dlq") {
            metrics.dlq.inc();
            log("error", "message_dead_lettered", {
              ...trace,
              eventId: event.id,
              code: row.payload.failureCode,
            });
          }
        } catch (error) {
          event.scheduleRetry(new Date());
          await sql.query(
            "UPDATE outbox SET attempts = ?, next_attempt_at = ? WHERE id = ?",
            [event.attempts, event.nextAttemptAt, event.id],
          );
          metrics.retries.inc({ worker: "outbox" });
          log("warn", "outbox_publish_failed", {
            ...trace,
            eventId: event.id,
            walletId: event.aggregateId,
            code: errorCode(error),
          });
          break;
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
    let input: WagerInput | undefined;
    let idempotencyKey: string | undefined;
    let processContext: Parameters<Processor["process"]>[2] | undefined;
    const trace: Record<string, unknown> = {
      messageId: message.MessageId,
      transportMessageId: message.MessageId,
    };
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
            ...trace,
          }),
        )
        .finally(() => {
          visibilityBusy = false;
        });
    }, 10_000);
    try {
      const envelope = messageSchema.parse(JSON.parse(message.Body ?? ""));
      const { idempotencyKey: key, ...business } = envelope.data;
      input = business;
      idempotencyKey = key;
      Object.assign(trace, {
        correlationId: envelope.messageId,
        messageId: envelope.messageId,
        walletId: input.walletId,
        providerId: input.providerId,
        externalTransactionId: input.externalTransactionId,
      });
      processContext = {
        correlationId: envelope.messageId,
        causationId: envelope.messageId,
        inbox: {
          messageId: envelope.messageId,
          hash: payloadHash({ input, idempotencyKey }),
        },
      };
      log("info", "message_processing_started", trace);
      const result = await this.processor.process(
        input,
        idempotencyKey,
        processContext,
      );
      trace.transactionId = result.transactionId;
      if (result.status === "FAILED") {
        await this.processor.recordPermanentFailure(
          input,
          idempotencyKey,
          processContext,
          "INFRASTRUCTURE_PERMANENT",
          {
            body: message.Body!,
            messageId: message.MessageId ?? envelope.messageId,
          },
        );
      }
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
      if (
        !permanent &&
        input &&
        idempotencyKey &&
        processContext &&
        (receives >= 5 || isPermanentInfrastructureError(error))
      ) {
        try {
          const result = await this.processor.recordPermanentFailure(
            input,
            idempotencyKey,
            processContext,
            receives >= 5 ? "RETRY_EXHAUSTED" : "INFRASTRUCTURE_PERMANENT",
            {
              body: message.Body!,
              messageId: message.MessageId ?? String(trace.messageId),
            },
          );
          trace.transactionId = result.transactionId;
          await this.queues.client.send(
            new DeleteMessageCommand({
              QueueUrl: url,
              ReceiptHandle: message.ReceiptHandle,
            }),
          );
          log("warn", "message_failure_finalized", {
            ...trace,
            status: result.status,
            code: result.failureCode,
            receives,
          });
          return true;
        } catch (recordingError) {
          log("error", "failure_recording_retry", {
            ...trace,
            code: errorCode(recordingError),
            receives,
          });
          throw recordingError;
        }
      }
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
          ...trace,
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
        ...trace,
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
    const parsed = messageSchema.safeParse(
      (() => {
        try {
          return JSON.parse(message.Body ?? "");
        } catch {
          return null;
        }
      })(),
    );
    log(
      "info",
      "message_released",
      parsed.success
        ? {
            correlationId: parsed.data.messageId,
            messageId: parsed.data.messageId,
            walletId: parsed.data.data.walletId,
            providerId: parsed.data.data.providerId,
            transportMessageId: message.MessageId,
          }
        : { messageId: message.MessageId },
    );
  }
}
