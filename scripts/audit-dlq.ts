import {
  ChangeMessageVisibilityCommand,
  ReceiveMessageCommand,
} from "@aws-sdk/client-sqs";
import { messageSchema, payloadHash } from "../src/application/contracts";
import { Processor } from "../src/application/processor";
import { openDatabase } from "../src/infrastructure/database";
import { Queues, queueNames } from "../src/infrastructure/queues";
import { errorCode, log } from "../src/infrastructure/telemetry";

// Audits one batch after database recovery. Messages remain in DLQ for the operator.
const db = await openDatabase();
const queues = new Queues();
try {
  const url = await queues.url(queueNames.dlq);
  const processor = new Processor(db);
  const batch = await queues.client.send(
    new ReceiveMessageCommand({
      QueueUrl: url,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 1,
      VisibilityTimeout: 30,
    }),
  );
  for (const message of batch.Messages ?? []) {
    try {
      const envelope = messageSchema.parse(JSON.parse(message.Body ?? ""));
      const { idempotencyKey, ...input } = envelope.data;
      await processor.recordPermanentFailure(
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
          body: message.Body!,
          messageId: message.MessageId!,
          alreadyDeadLettered: true,
        },
      );
    } catch (error) {
      log("warn", "dlq_audit_skipped", {
        messageId: message.MessageId,
        code: errorCode(error),
      });
    } finally {
      await queues.client.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: url,
          ReceiptHandle: message.ReceiptHandle,
          VisibilityTimeout: 0,
        }),
      );
    }
  }
} finally {
  queues.close();
  await db.close(true);
}
