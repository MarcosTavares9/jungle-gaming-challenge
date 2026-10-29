import { describe, expect, test } from "bun:test";
import { Money } from "../../src/domain/money";
import { Wallet, WalletLedgerEntry } from "../../src/domain/wallet";
import {
  WagerTransaction,
  type Kind,
  type TransactionProps,
} from "../../src/domain/transaction";
import {
  InboxMessage,
  OutboxMessage,
  WagerTransactionProcessed,
} from "../../src/domain/events";
import {
  payloadHash,
  wagerSchema,
  moneySchema,
} from "../../src/application/contracts";

const money = (amount: string, currency = "BRL") =>
  Money.from({ amount, currency });
const transaction = (
  kind: Kind = "BET",
  overrides: Partial<TransactionProps> = {},
) =>
  WagerTransaction.create({
    id: crypto.randomUUID(),
    providerId: "provider",
    externalTransactionId: "external",
    idempotencyKey: "key",
    payloadHash: "hash",
    walletId: crypto.randomUUID(),
    playerId: crypto.randomUUID(),
    roundId: "round",
    gameId: "game",
    kind,
    money: money(kind === "LOSS" ? "0.00" : "10.00"),
    referenceExternalTransactionId:
      kind === "REFUND" || kind === "ROLLBACK" ? "reference" : undefined,
    createdAt: new Date(),
    ...overrides,
  });
const open = (amount = "100.00") =>
  Wallet.open({
    id: crypto.randomUUID(),
    playerId: crypto.randomUUID(),
    initialBalance: money(amount),
  });

describe("Money", () => {
  test("preserva centavos e números acima do limite seguro de inteiros JS", () => {
    expect(money("0.10").add(money("0.20")).toString()).toBe("0.30");
    expect(money("9007199254740992.00").add(money("0.01")).toString()).toBe(
      "9007199254740992.01",
    );
    expect(money("0.29").subtract(money("0.01")).toJSON()).toEqual({
      amount: "0.28",
      currency: "BRL",
    });
  });
  test.each([
    "NaN",
    "Infinity",
    "1e2",
    "",
    " ",
    "1",
    "1.0",
    "1.001",
    "1.005",
    "-1.00",
    "+1.00",
    "01.00",
    "1,00",
    "10000000000000000.00",
  ])("rejeita entrada %j sem arredondamento", (amount) => {
    expect(() => money(amount)).toThrow("INVALID_MONEY");
    expect(moneySchema.safeParse({ amount, currency: "BRL" }).success).toBe(
      false,
    );
  });
  test("não aceita valor numérico no contrato", () => {
    expect(
      moneySchema.safeParse({ amount: 1.25, currency: "BRL" }).success,
    ).toBe(false);
  });
  test("operações não alteram os operandos e mantêm a escala", () => {
    const first = money("10.00");
    const second = money("2.00");
    expect(first.add(second)).not.toBe(first);
    expect(first.toString()).toBe("10.00");
    expect(second.toString()).toBe("2.00");
    expect(first.subtract(second).negate().toString()).toBe("-8.00");
    expect(Money.zero("BRL").isZero()).toBe(true);
    expect(first.isPositive()).toBe(true);
    expect(first.negate().isNegative()).toBe(true);
    expect(second.isLessThan(first)).toBe(true);
    expect(first.equals(money("10.00"))).toBe(true);
  });
  test.each(["add", "subtract", "equals", "isLessThan"] as const)(
    "%s recusa moedas diferentes",
    (method) => {
      expect(() => money("1.00")[method](money("1.00", "USD"))).toThrow(
        "CURRENCY_MISMATCH",
      );
    },
  );
  test("reidrata diferença negativa e rejeita moeda malformada", () => {
    expect(
      Money.rehydrate({ amount: "-0.01", currency: "BRL" }).toString(),
    ).toBe("-0.01");
    expect(() => money("1.00", "brl")).toThrow("INVALID_MONEY");
    expect(() => money("9999999999999999.99").add(money("0.01"))).toThrow(
      "BALANCE_LIMIT_EXCEEDED",
    );
  });
});

describe("Wallet e ledger", () => {
  test("crédito e débito produzem lançamentos com saldos e versões corretos", () => {
    const wallet = open();
    expect(wallet.version).toBe(1);
    const debit = wallet.debit(money("80.00"), "bet");
    expect(debit.balanceBefore.toString()).toBe("100.00");
    expect(debit.balanceAfter.toString()).toBe("20.00");
    expect(debit.direction).toBe("DEBIT");
    expect(debit.isBalanced()).toBe(true);
    expect(Object.isFrozen(debit)).toBe(true);
    const credit = wallet.credit(money("5.00"), "win");
    expect(credit.direction).toBe("CREDIT");
    expect(credit.balanceBefore.toString()).toBe("20.00");
    expect(wallet.balance.toString()).toBe("25.00");
    expect(wallet.version).toBe(3);
  });
  test("débito pode esgotar o saldo exatamente", () => {
    const wallet = open();
    wallet.debit(money("100.00"), "bet");
    expect(wallet.balance.isZero()).toBe(true);
  });
  test("rejeição mantém saldo, versão e data anteriores", () => {
    const wallet = open("10.00");
    const at = wallet.updatedAt;
    expect(() => wallet.debit(money("10.01"), "bet")).toThrow(
      "INSUFFICIENT_FUNDS",
    );
    expect(() => wallet.debit(money("10.01"), "rollback", true)).toThrow(
      "REVERSAL_INSUFFICIENT_FUNDS",
    );
    expect(wallet.balance.toString()).toBe("10.00");
    expect(wallet.version).toBe(1);
    expect(wallet.updatedAt).toBe(at);
  });
  test.each(["credit", "debit"] as const)(
    "%s recusa moeda diferente sem alterar estado",
    (operation) => {
      const wallet = open();
      expect(() => wallet[operation](money("1.00", "USD"), "tx")).toThrow(
        "CURRENCY_MISMATCH",
      );
      expect(wallet.balance.toString()).toBe("100.00");
      expect(wallet.version).toBe(1);
    },
  );
  test("recusa zero e saldo de abertura negativo", () => {
    expect(() => open().credit(money("0.00"), "tx")).toThrow("INVALID_STATE");
    expect(() =>
      Wallet.open({
        id: "id",
        playerId: "player",
        initialBalance: money("1.00").negate(),
      }),
    ).toThrow("INVALID_MONEY");
  });
  test("factory do ledger recusa aritmética incorreta", () => {
    const entry = open().debit(money("5.00"), "tx");
    expect(() =>
      WalletLedgerEntry.create({ ...entry, balanceAfter: money("99.00") }),
    ).toThrow("INVALID_STATE");
  });
  test("rehydrate reconstrói uma carteira existente sem incrementar versão", () => {
    const now = new Date();
    const wallet = Wallet.rehydrate({
      id: "id",
      playerId: "player",
      currency: "BRL",
      balance: money("20.00"),
      version: 8,
      createdAt: now,
      updatedAt: now,
    });
    expect(wallet.balance.toString()).toBe("20.00");
    expect(wallet.version).toBe(8);
  });
});

describe("WagerTransaction", () => {
  test.each(["BET", "WIN", "LOSS", "REFUND", "ROLLBACK"] as const)(
    "%s nasce pendente e tem efeito esperado",
    (kind) => {
      const tx = transaction(kind);
      expect(tx.status).toBe("PENDING");
      expect(tx.affectsBalance()).toBe(kind !== "LOSS");
      expect(tx.requiresReference()).toBe(
        kind === "REFUND" || kind === "ROLLBACK",
      );
    },
  );
  test.each(["REFUND", "ROLLBACK"] as const)("%s exige referência", (kind) => {
    expect(() =>
      transaction(kind, { referenceExternalTransactionId: undefined }),
    ).toThrow("REFERENCE_NOT_FOUND");
  });
  test("direção da reversão depende da referência", () => {
    expect(transaction("BET").ledgerDirectionFor()).toBe("DEBIT");
    expect(transaction("WIN").ledgerDirectionFor()).toBe("CREDIT");
    expect(transaction("REFUND").ledgerDirectionFor()).toBe("CREDIT");
    expect(transaction("ROLLBACK").ledgerDirectionFor(transaction("BET"))).toBe(
      "CREDIT",
    );
    expect(transaction("ROLLBACK").ledgerDirectionFor(transaction("WIN"))).toBe(
      "DEBIT",
    );
    expect(
      transaction("ROLLBACK").ledgerDirectionFor(transaction("REFUND")),
    ).toBe("DEBIT");
    expect(() => transaction("ROLLBACK").ledgerDirectionFor()).toThrow(
      "REFERENCE_NOT_FOUND",
    );
  });
  test("referência pendente pode ser resolvida", () => {
    const tx = transaction("REFUND");
    tx.markPendingReference();
    expect(tx.status).toBe("PENDING_REFERENCE");
    const now = new Date();
    tx.markProcessed("reference-id", now);
    expect(tx.referenceTransactionId).toBe("reference-id");
    expect(tx.processedAt).toBe(now);
    expect(tx.isTerminal()).toBe(true);
  });
  test.each(["PROCESSED", "REJECTED", "FAILED"] as const)(
    "estado %s não aceita novas transições",
    (status) => {
      const tx = transaction();
      if (status === "PROCESSED") tx.markProcessed(undefined, new Date());
      else if (status === "REJECTED") tx.reject("INSUFFICIENT_FUNDS");
      else tx.fail("INFRASTRUCTURE_PERMANENT");
      expect(() => tx.markPendingReference()).toThrow("INVALID_STATE");
      expect(() => tx.markProcessed(undefined, new Date())).toThrow(
        "INVALID_STATE",
      );
      expect(() => tx.reject("INSUFFICIENT_FUNDS")).toThrow("INVALID_STATE");
      expect(() => tx.fail("INFRASTRUCTURE_PERMANENT")).toThrow(
        "INVALID_STATE",
      );
    },
  );
  test("compara hash e recusa valor incompatível com kind", () => {
    expect(transaction().matchesPayload("hash")).toBe(true);
    expect(transaction().matchesPayload("other")).toBe(false);
    expect(() => transaction("LOSS", { money: money("1.00") })).toThrow(
      "INVALID_MONEY",
    );
    expect(() => transaction("BET", { money: money("0.00") })).toThrow(
      "INVALID_MONEY",
    );
  });
});

describe("contratos, inbox e outbox", () => {
  test("hash ignora ordem das chaves, preserva valores e funciona em objetos aninhados", () => {
    expect(payloadHash({ b: { z: "1.00", a: 1 }, a: [1, 2] })).toBe(
      payloadHash({ a: [1, 2], b: { a: 1, z: "1.00" } }),
    );
    expect(payloadHash({ amount: "1.00" })).not.toBe(
      payloadHash({ amount: "2.00" }),
    );
    expect(payloadHash([1, 2])).not.toBe(payloadHash([2, 1]));
  });
  test("OPENING não pode entrar pelo contrato externo", () => {
    const tx = transaction().props;
    expect(
      wagerSchema.safeParse({
        providerId: tx.providerId,
        externalTransactionId: tx.externalTransactionId,
        playerId: tx.playerId,
        walletId: tx.walletId,
        roundId: tx.roundId,
        gameId: tx.gameId,
        kind: "OPENING",
        money: tx.money.toJSON(),
      }).success,
    ).toBe(false);
  });
  test("inbox registra a confirmação", () => {
    const inbox = InboxMessage.receive({
      messageId: "msg",
      consumerName: "consumer",
      payloadHash: "hash",
    });
    expect(inbox.isProcessed()).toBe(false);
    const now = new Date();
    inbox.markProcessed(now);
    expect(inbox.isProcessed()).toBe(true);
    expect(inbox.processedAt).toBe(now);
  });
  test("outbox mantém envelope estável e calcula backoff limitado", () => {
    const now = new Date("2026-01-01T00:00:00.000Z");
    const event = WagerTransactionProcessed.from({
      eventId: "event",
      aggregateId: "wallet",
      correlationId: "correlation",
      causationId: "message",
      occurredAt: now,
      data: {
        transactionId: "tx",
        walletId: "wallet",
        providerId: "provider",
        status: "PROCESSED",
      },
    });
    const outbox = OutboxMessage.enqueue(event);
    expect(outbox.payload).toMatchObject({
      eventId: "event",
      eventType: "WagerTransactionProcessed",
      version: 1,
      occurredAt: now.toISOString(),
      causationId: "message",
    });
    expect(outbox.isDue(now)).toBe(true);
    outbox.scheduleRetry(now);
    expect(outbox.attempts).toBe(1);
    expect(outbox.isDue(now)).toBe(false);
    expect(outbox.nextAttemptAt!.getTime() - now.getTime()).toBe(2000);
    for (let i = 0; i < 20; i++) outbox.scheduleRetry(now);
    expect(outbox.nextAttemptAt!.getTime() - now.getTime()).toBe(60_000);
    outbox.markPublished(now);
    expect(outbox.isPending()).toBe(false);
    expect(outbox.isDue(new Date("2027-01-01"))).toBe(false);
  });
});
