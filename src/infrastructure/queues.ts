import {
  CreateQueueCommand,
  GetQueueUrlCommand,
  GetQueueAttributesCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { config } from "./config";
export const queueNames = {
  requests: `${config.SQS_QUEUE_PREFIX}wager-transactions.fifo`,
  dlq: `${config.SQS_QUEUE_PREFIX}wager-transactions-dlq.fifo`,
  events: `${config.SQS_QUEUE_PREFIX}wager-events.fifo`,
} as const;
export class Queues {
  readonly client = new SQSClient({
    endpoint: config.SQS_ENDPOINT,
    region: config.AWS_REGION,
    credentials: {
      accessKeyId: config.AWS_ACCESS_KEY_ID,
      secretAccessKey: config.AWS_SECRET_ACCESS_KEY,
    },
    maxAttempts: 2,
    requestHandler: { requestTimeout: 15_000, connectionTimeout: 3000 },
  });
  private urls = new Map<string, string>();
  async url(name: string): Promise<string> {
    const cached = this.urls.get(name);
    if (cached) return cached;
    const result = await this.client.send(
      new GetQueueUrlCommand({ QueueName: name }),
    );
    if (!result.QueueUrl) throw new Error("Queue URL missing");
    this.urls.set(name, result.QueueUrl);
    return result.QueueUrl;
  }
  async ready() {
    for (const name of Object.values(queueNames))
      await this.client.send(
        new GetQueueAttributesCommand({
          QueueUrl: await this.url(name),
          AttributeNames: ["QueueArn"],
        }),
      );
  }
  async setup() {
    const dlq = await this.client.send(
      new CreateQueueCommand({
        QueueName: queueNames.dlq,
        Attributes: { FifoQueue: "true", ContentBasedDeduplication: "false" },
      }),
    );
    const attributes = await this.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: dlq.QueueUrl,
        AttributeNames: ["QueueArn"],
      }),
    );
    await this.client.send(
      new CreateQueueCommand({
        QueueName: queueNames.requests,
        Attributes: {
          FifoQueue: "true",
          ContentBasedDeduplication: "false",
          VisibilityTimeout: "30",
          ReceiveMessageWaitTimeSeconds: "5",
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: attributes.Attributes?.QueueArn,
            maxReceiveCount: "5",
          }),
        },
      }),
    );
    await this.client.send(
      new CreateQueueCommand({
        QueueName: queueNames.events,
        Attributes: { FifoQueue: "true", ContentBasedDeduplication: "false" },
      }),
    );
  }
  close() {
    this.client.destroy();
  }
}
