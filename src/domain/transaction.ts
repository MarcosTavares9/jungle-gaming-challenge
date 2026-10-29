import { DomainError, type FailureCode } from "./errors";
import type { Money } from "./money";
import type { LedgerDirection } from "./wallet";

export type Kind = "OPENING" | "BET" | "WIN" | "LOSS" | "REFUND" | "ROLLBACK";
export type Status =
  | "PENDING"
  | "PENDING_REFERENCE"
  | "PROCESSED"
  | "REJECTED"
  | "FAILED";
export interface TransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: Kind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
}
export interface TransactionState extends TransactionProps {
  status: Status;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
}

export class WagerTransaction {
  private constructor(
    public readonly props: Readonly<TransactionProps>,
    private state: Omit<TransactionState, keyof TransactionProps>,
  ) {}
  static create(p: TransactionProps): WagerTransaction {
    if (
      (p.kind === "REFUND" || p.kind === "ROLLBACK") &&
      !p.referenceExternalTransactionId
    )
      throw new DomainError("REFERENCE_NOT_FOUND");
    if (p.kind === "LOSS" ? !p.money.isZero() : !p.money.isPositive())
      throw new DomainError("INVALID_MONEY");
    return new WagerTransaction(Object.freeze({ ...p }), { status: "PENDING" });
  }
  static rehydrate(p: TransactionState): WagerTransaction {
    const {
      status,
      referenceTransactionId,
      failureCode,
      processedAt,
      ...props
    } = p;
    return new WagerTransaction(Object.freeze(props), {
      status,
      referenceTransactionId,
      failureCode,
      processedAt,
    });
  }
  get status(): Status {
    return this.state.status;
  }
  get referenceTransactionId(): string | undefined {
    return this.state.referenceTransactionId;
  }
  get failureCode(): FailureCode | undefined {
    return this.state.failureCode;
  }
  get processedAt(): Date | undefined {
    return this.state.processedAt;
  }
  isTerminal(): boolean {
    return ["PROCESSED", "REJECTED", "FAILED"].includes(this.status);
  }
  affectsBalance(): boolean {
    return this.props.kind !== "LOSS";
  }
  requiresReference(): boolean {
    return this.props.kind === "REFUND" || this.props.kind === "ROLLBACK";
  }
  matchesPayload(hash: string): boolean {
    return this.props.payloadHash === hash;
  }
  markProcessed(referenceTransactionId: string | undefined, at: Date): void {
    this.assertMutable();
    this.state = {
      status: "PROCESSED",
      referenceTransactionId,
      processedAt: at,
    };
  }
  markPendingReference(): void {
    this.assertMutable();
    this.state.status = "PENDING_REFERENCE";
  }
  reject(code: FailureCode): void {
    this.assertMutable();
    this.state = {
      status: "REJECTED",
      failureCode: code,
      processedAt: new Date(),
    };
  }
  fail(code: FailureCode): void {
    this.assertMutable();
    this.state = {
      status: "FAILED",
      failureCode: code,
      processedAt: new Date(),
    };
  }
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    if (this.props.kind === "BET") return "DEBIT";
    if (this.props.kind === "ROLLBACK") {
      if (!reference) throw new DomainError("REFERENCE_NOT_FOUND");
      return reference.props.kind === "BET" ? "CREDIT" : "DEBIT";
    }
    return "CREDIT";
  }
  private assertMutable(): void {
    if (this.isTerminal()) throw new DomainError("INVALID_STATE");
  }
}
