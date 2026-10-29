import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
} from "@aws-sdk/client-sqs";
import { ZodError } from "zod";
import { messageSchema, payloadHash } from "../src/application/contracts";
import { Processor } from "../src/application/processor";
import { ConflictError } from "../src/domain/errors";
import { openDatabase } from "../src/infrastructure/database";
import {
  DeadLetterArchive,
  type DeadLetterAudit,
} from "../src/infrastructure/dead-letter-archive";
import { Queues, queueNames } from "../src/infrastructure/queues";
import { errorCode, log } from "../src/infrastructure/telemetry";

// Process one batch. Durable archival precedes ack so subsequent runs can advance the FIFO group.
const db = await openDatabase();
const queues = new Queues();
try {
  const url = await queues.url(queueNames.dlq);
  const processor = new Processor(db);
  const archive = new DeadLetterArchive(db);
  const batch = await queues.client.send(
    new ReceiveMessageCommand({
      QueueUrl: url,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 1,
      VisibilityTimeout: 30,
    }),
  );
  const acknowledged = new Set<string>();
  try {
    for (const message of batch.Messages ?? []) {
      const trace: Record<string, unknown> = { messageId: message.MessageId };
      try {
        if (!message.MessageId || !message.ReceiptHandle)
          throw new Error("Incomplete SQS delivery");
        const audit: DeadLetterAudit = {
          sourceQueue: queueNames.dlq,
          messageId: message.MessageId,
          body: message.Body ?? "",
          outcome: "AUDITED",
        };
        try {
          const envelope = messageSchema.parse(JSON.parse(audit.body));
          const { idempotencyKey, ...input } = envelope.data;
          Object.assign(trace, {
            correlationId: envelope.messageId,
            messageId: envelope.messageId,
            transportMessageId: message.MessageId,
            walletId: input.walletId,
            providerId: input.providerId,
          });
          const result = await processor.recordPermanentFailure(
            input,
            idempotencyKey,
            {
              correlationId: envelope.messageId,
              causationId: envelope.messageId,
              inbox: {
                messageId: envelope.messageId,
                hash: payloadHash({ input, idempotencyKey }),
              },
            },
            "RETRY_EXHAUSTED",
            {
              body: audit.body,
              messageId: message.MessageId,
              alreadyDeadLettered: true,
            },
          );
          audit.transactionId = result.transactionId;
          audit.transactionStatus = result.status;
          audit.failureCode = result.failureCode;
          trace.transactionId = result.transactionId;
        } catch (error) {
          if (error instanceof ZodError || error instanceof SyntaxError) {
            audit.outcome = "INVALID_PAYLOAD";
            audit.failureCode = "INVALID_PAYLOAD";
          } else if (error instanceof ConflictError) {
            audit.outcome = "CONFLICT";
            audit.failureCode = error.code;
          } else throw error;
        }
        await archive.save(audit);
        await queues.client.send(
          new DeleteMessageCommand({
            QueueUrl: url,
            ReceiptHandle: message.ReceiptHandle,
          }),
        );
        acknowledged.add(message.ReceiptHandle);
        log("info", "dlq_audit_archived", {
          ...trace,
          outcome: audit.outcome,
          code: audit.failureCode,
        });
      } catch (error) {
        process.exitCode = 1;
        log("error", "dlq_audit_failed", { ...trace, code: errorCode(error) });
        break;
      }
    }
  } finally {
    for (const message of batch.Messages ?? []) {
      if (acknowledged.has(message.ReceiptHandle!)) continue;
      try {
        await queues.client.send(
          new ChangeMessageVisibilityCommand({
            QueueUrl: url,
            ReceiptHandle: message.ReceiptHandle,
            VisibilityTimeout: 0,
          }),
        );
      } catch (error) {
        process.exitCode = 1;
        log("error", "dlq_audit_release_failed", {
          messageId: message.MessageId,
          code: errorCode(error),
        });
      }
    }
  }
} finally {
  queues.close();
  await db.close(true);
}
