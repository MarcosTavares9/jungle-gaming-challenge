import { DomainError } from "./errors";
import { Money } from "./money";

export type LedgerDirection = "DEBIT" | "CREDIT";
export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {
    Object.freeze(this);
  }

  static create(
    props: Omit<WalletLedgerEntry, "isBalanced">,
  ): WalletLedgerEntry {
    const entry = WalletLedgerEntry.rehydrate(props);
    if (
      !entry.money.isPositive() ||
      entry.balanceAfter.isNegative() ||
      !entry.isBalanced()
    ) {
      throw new DomainError("INVALID_STATE");
    }
    return entry;
  }
  static rehydrate(
    p: Omit<WalletLedgerEntry, "isBalanced">,
  ): WalletLedgerEntry {
    return new WalletLedgerEntry(
      p.id,
      p.walletId,
      p.transactionId,
      p.direction,
      p.money,
      p.balanceBefore,
      p.balanceAfter,
      p.createdAt,
    );
  }
  isBalanced(): boolean {
    return (
      this.direction === "CREDIT"
        ? this.balanceBefore.add(this.money)
        : this.balanceBefore.subtract(this.money)
    ).equals(this.balanceAfter);
  }
}

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}
  static open(p: {
    id: string;
    playerId: string;
    initialBalance: Money;
  }): Wallet {
    if (p.initialBalance.isNegative()) throw new DomainError("INVALID_MONEY");
    const now = new Date();
    return new Wallet(
      p.id,
      p.playerId,
      p.initialBalance.currency,
      p.initialBalance,
      1,
      now,
      now,
    );
  }
  static rehydrate(p: WalletState): Wallet {
    return new Wallet(
      p.id,
      p.playerId,
      p.currency,
      p.balance,
      p.version,
      p.createdAt,
      p.updatedAt,
    );
  }
  get balance(): Money {
    return this._balance;
  }
  get version(): number {
    return this._version;
  }
  get updatedAt(): Date {
    return this._updatedAt;
  }
  debit(
    money: Money,
    transactionId: string,
    reversal = false,
  ): WalletLedgerEntry {
    if (this._balance.isLessThan(money))
      throw new DomainError(
        reversal ? "REVERSAL_INSUFFICIENT_FUNDS" : "INSUFFICIENT_FUNDS",
      );
    return this.move(money, transactionId, "DEBIT");
  }
  credit(money: Money, transactionId: string): WalletLedgerEntry {
    return this.move(money, transactionId, "CREDIT");
  }
  private move(
    money: Money,
    transactionId: string,
    direction: LedgerDirection,
  ): WalletLedgerEntry {
    const before = this._balance;
    const after =
      direction === "CREDIT" ? before.add(money) : before.subtract(money);
    const at = new Date();
    const entry = WalletLedgerEntry.create({
      id: crypto.randomUUID(),
      walletId: this.id,
      transactionId,
      direction,
      money,
      balanceBefore: before,
      balanceAfter: after,
      createdAt: at,
    });
    this._balance = after;
    this._version += 1;
    this._updatedAt = at;
    return entry;
  }
}
