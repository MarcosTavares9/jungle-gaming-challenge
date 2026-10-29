export type FailureCode =
  | "INSUFFICIENT_FUNDS"
  | "REVERSAL_INSUFFICIENT_FUNDS"
  | "CURRENCY_MISMATCH"
  | "REFERENCE_NOT_FOUND"
  | "REFERENCE_REJECTED"
  | "REFERENCE_MISMATCH"
  | "INVALID_REFERENCE_KIND"
  | "REVERSAL_AMOUNT_MISMATCH"
  | "ALREADY_REVERSED"
  | "WALLET_NOT_FOUND"
  | "PLAYER_MISMATCH"
  | "BALANCE_LIMIT_EXCEEDED"
  | "INFRASTRUCTURE_PERMANENT";

export class DomainError extends Error {
  constructor(
    public readonly code: FailureCode | "INVALID_MONEY" | "INVALID_STATE",
    message = code,
  ) {
    super(message);
  }
}

export class ConflictError extends Error {
  constructor(public readonly code: string) {
    super(code);
  }
}

export class NotFoundError extends Error {}
