import type { MoneyProps } from "./money";
import type { LedgerDirection } from "./wallet";

export interface EventContext {
  correlationId: string;
  causationId?: string;
}
export interface IntegrationEventProps<T> extends EventContext {
  eventId: string;
  aggregateId: string;
  occurredAt: Date;
  data: T;
}
export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;
  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly occurredAt: Date;
  readonly data: Readonly<T>;
  protected constructor(p: IntegrationEventProps<T>) {
    this.eventId = p.eventId;
    this.aggregateId = p.aggregateId;
    this.correlationId = p.correlationId;
    this.causationId = p.causationId;
    this.occurredAt = p.occurredAt;
    this.data = Object.freeze(p.data);
  }
  toJSON() {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      version: this.version,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      causationId: this.causationId,
      occurredAt: this.occurredAt.toISOString(),
      data: this.data,
    };
  }
}
export interface TransactionEventData {
  transactionId: string;
  walletId: string;
  providerId: string;
  status: string;
  failureCode?: string;
}
export class WagerTransactionProcessed extends IntegrationEvent<TransactionEventData> {
  readonly eventType = "WagerTransactionProcessed";
  readonly version = 1;
  static from(p: IntegrationEventProps<TransactionEventData>) {
    return new WagerTransactionProcessed(p);
  }
}
export class WagerTransactionRejected extends IntegrationEvent<TransactionEventData> {
  readonly eventType = "WagerTransactionRejected";
  readonly version = 1;
  static from(p: IntegrationEventProps<TransactionEventData>) {
    return new WagerTransactionRejected(p);
  }
}
export class WagerTransactionPendingReference extends IntegrationEvent<TransactionEventData> {
  readonly eventType = "WagerTransactionPendingReference";
  readonly version = 1;
  static from(p: IntegrationEventProps<TransactionEventData>) {
    return new WagerTransactionPendingReference(p);
  }
}
export interface BalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}
export class WalletBalanceChanged extends IntegrationEvent<BalanceChangedData> {
  readonly eventType = "WalletBalanceChanged";
  readonly version = 1;
  static from(p: IntegrationEventProps<BalanceChangedData>) {
    return new WalletBalanceChanged(p);
  }
}

export class InboxMessage {
  private constructor(
    public readonly messageId: string,
    public readonly consumerName: string,
    public readonly payloadHash: string,
    public readonly receivedAt: Date,
    private _processedAt?: Date,
  ) {}
  static receive(p: {
    messageId: string;
    consumerName: string;
    payloadHash: string;
  }) {
    return new InboxMessage(
      p.messageId,
      p.consumerName,
      p.payloadHash,
      new Date(),
    );
  }
  static rehydrate(p: {
    messageId: string;
    consumerName: string;
    payloadHash: string;
    receivedAt: Date;
    processedAt?: Date;
  }) {
    return new InboxMessage(
      p.messageId,
      p.consumerName,
      p.payloadHash,
      p.receivedAt,
      p.processedAt,
    );
  }
  get processedAt() {
    return this._processedAt;
  }
  isProcessed() {
    return this._processedAt !== undefined;
  }
  markProcessed(at: Date) {
    this._processedAt = at;
  }
}
export class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
  ) {}
  static enqueue(event: IntegrationEvent<unknown>) {
    return new OutboxMessage(
      event.eventId,
      event.aggregateId,
      event.eventType,
      Object.freeze(event.toJSON()),
      event.occurredAt,
      0,
    );
  }
  static rehydrate(p: {
    id: string;
    aggregateId: string;
    eventType: string;
    payload: Record<string, unknown>;
    occurredAt: Date;
    attempts: number;
    nextAttemptAt?: Date;
    publishedAt?: Date;
  }) {
    return new OutboxMessage(
      p.id,
      p.aggregateId,
      p.eventType,
      Object.freeze(p.payload),
      p.occurredAt,
      p.attempts,
      p.nextAttemptAt,
      p.publishedAt,
    );
  }
  get attempts() {
    return this._attempts;
  }
  get nextAttemptAt() {
    return this._nextAttemptAt;
  }
  get publishedAt() {
    return this._publishedAt;
  }
  isPending() {
    return !this._publishedAt;
  }
  isDue(now: Date) {
    return (
      this.isPending() && (!this._nextAttemptAt || this._nextAttemptAt <= now)
    );
  }
  markPublished(at: Date) {
    this._publishedAt = at;
  }
  scheduleRetry(now: Date) {
    this._attempts += 1;
    this._nextAttemptAt = new Date(
      now.getTime() +
        Math.min(60_000, 1000 * 2 ** Math.min(this._attempts, 10)),
    );
  }
}
