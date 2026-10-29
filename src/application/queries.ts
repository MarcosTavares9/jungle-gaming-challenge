import { z } from "zod";
import { NotFoundError } from "../domain/errors";
import { Money } from "../domain/money";
import { type Database, Sql, WalletRecord } from "../infrastructure/database";
import { log, metrics } from "../infrastructure/telemetry";
import type { TransactionRow } from "./processor";

const cursorSchema = z
  .object({ walletId: z.uuid(), version: z.number().int().positive() })
  .strict();
export class Queries {
  constructor(private readonly db: Database) {}
  async wallet(id: string) {
    const row = await this.db.em.fork().findOne(WalletRecord, { id });
    if (!row) throw new NotFoundError("WALLET_NOT_FOUND");
    return {
      id: row.id,
      playerId: row.playerId,
      balance: { amount: row.balance, currency: row.currency },
      version: row.version,
    };
  }
  async transaction(id: string) {
    const [row] = await new Sql(this.db.em.fork()).query<TransactionRow>(
      "SELECT * FROM wager_transactions WHERE id = ?",
      [id],
    );
    if (!row) throw new NotFoundError("TRANSACTION_NOT_FOUND");
    return this.serialize(row);
  }
  async providerTransaction(provider: string, external: string) {
    const [row] = await new Sql(this.db.em.fork()).query<TransactionRow>(
      "SELECT * FROM wager_transactions WHERE provider_id = ? AND external_transaction_id = ?",
      [provider, external],
    );
    if (!row) throw new NotFoundError("TRANSACTION_NOT_FOUND");
    return this.serialize(row);
  }
  async ledger(walletId: string, limit: number, cursor?: string) {
    await this.wallet(walletId);
    let version = 0;
    if (cursor) {
      let parsed: unknown;
      try {
        parsed = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
      } catch {
        throw new z.ZodError([
          { code: "custom", path: ["cursor"], message: "Cursor inválido" },
        ]);
      }
      const data = cursorSchema.parse(parsed);
      if (data.walletId !== walletId)
        throw new z.ZodError([
          {
            code: "custom",
            path: ["cursor"],
            message: "Cursor de outra carteira",
          },
        ]);
      version = data.version;
    }
    const rows = await new Sql(this.db.em.fork()).query<{
      id: string;
      transaction_id: string;
      direction: string;
      amount: string;
      currency: string;
      balance_before: string;
      balance_after: string;
      wallet_version: number;
      created_at: Date;
    }>(
      `SELECT * FROM wallet_ledger WHERE wallet_id = ? AND wallet_version > ? ORDER BY wallet_version,id LIMIT ?`,
      [walletId, version, limit + 1],
    );
    const page = rows.slice(0, limit);
    const last = page.at(-1);
    return {
      items: page.map((r) => ({
        id: r.id,
        transactionId: r.transaction_id,
        direction: r.direction,
        money: { amount: r.amount, currency: r.currency },
        balanceBefore: { amount: r.balance_before, currency: r.currency },
        balanceAfter: { amount: r.balance_after, currency: r.currency },
        walletVersion: r.wallet_version,
        createdAt: r.created_at,
      })),
      nextCursor:
        rows.length > limit && last
          ? Buffer.from(
              JSON.stringify({ walletId, version: last.wallet_version }),
            ).toString("base64url")
          : null,
    };
  }
  async reconcile(walletId: string, correlationId: string) {
    // One SQL statement gives wallet and ledger the same MVCC snapshot.
    const [row] = await new Sql(this.db.em.fork()).query<{
      balance: string;
      currency: string;
      calculated: string;
      checked: string;
    }>(
      `SELECT w.balance::text,w.currency,COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END),0)::numeric(18,2)::text AS calculated,
       COUNT(l.id)::text AS checked FROM wallets w LEFT JOIN wallet_ledger l ON l.wallet_id = w.id WHERE w.id = ? GROUP BY w.id`,
      [walletId],
    );
    if (!row) throw new NotFoundError("WALLET_NOT_FOUND");
    const stored = Money.rehydrate({
      amount: row.balance,
      currency: row.currency,
    });
    const calculated = Money.rehydrate({
      amount: row.calculated,
      currency: row.currency,
    });
    const consistent = stored.equals(calculated);
    if (!consistent) {
      metrics.reconciliation.inc();
      log("error", "reconciliation_mismatch", { walletId, correlationId });
    }
    return {
      walletId,
      storedBalance: stored.toJSON(),
      calculatedBalance: calculated.toJSON(),
      difference: stored.subtract(calculated).toJSON(),
      consistent,
      checkedEntries: Number(row.checked),
    };
  }
  private serialize(row: TransactionRow) {
    return {
      ...row.result,
      transactionId: row.id,
      providerId: row.provider_id,
      externalTransactionId: row.external_transaction_id,
      walletId: row.wallet_id,
      kind: row.kind,
      money: { amount: row.amount, currency: row.currency },
      status: row.status,
      failureCode: row.failure_code,
      referenceTransactionId: row.reference_transaction_id,
      createdAt: row.created_at,
      processedAt: row.processed_at,
    };
  }
}
