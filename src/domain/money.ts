import Decimal from "decimal.js";
import { DomainError } from "./errors";

const ExactDecimal = Decimal.clone({
  precision: 50,
  rounding: Decimal.ROUND_HALF_EVEN,
});
export interface MoneyProps {
  amount: string;
  currency: string;
}

export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {
    Object.freeze(this);
  }

  static from({ amount, currency }: MoneyProps): Money {
    if (
      typeof amount !== "string" ||
      !/^(0|[1-9]\d{0,15})\.\d{2}$/.test(amount) ||
      !/^[A-Z]{3}$/.test(currency)
    )
      throw new DomainError("INVALID_MONEY");
    return new Money(new ExactDecimal(amount), currency);
  }

  static rehydrate({ amount, currency }: MoneyProps): Money {
    if (
      !/^-?(0|[1-9]\d{0,15})\.\d{2}$/.test(amount) ||
      !/^[A-Z]{3}$/.test(currency)
    ) {
      throw new DomainError("INVALID_MONEY");
    }
    return new Money(new ExactDecimal(amount), currency);
  }

  static zero(currency: string): Money {
    return Money.from({ amount: "0.00", currency });
  }
  add(other: Money): Money {
    this.assertSameCurrency(other);
    const value = this.value.add(other.value);
    if (value.gt("9999999999999999.99"))
      throw new DomainError("BALANCE_LIMIT_EXCEEDED");
    return new Money(value, this.currency);
  }
  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.sub(other.value), this.currency);
  }
  negate(): Money {
    return new Money(this.value.negated(), this.currency);
  }
  isZero(): boolean {
    return this.value.isZero();
  }
  isPositive(): boolean {
    return this.value.gt(0);
  }
  isNegative(): boolean {
    return this.value.lt(0);
  }
  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lt(other.value);
  }
  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.eq(other.value);
  }
  toString(): string {
    return this.value.toFixed(2);
  }
  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }
  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency)
      throw new DomainError("CURRENCY_MISMATCH");
  }
}
