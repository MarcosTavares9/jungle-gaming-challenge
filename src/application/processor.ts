import { LockMode } from "@mikro-orm/core";
import { ConflictError, DomainError, type FailureCode } from "../domain/errors";
import { Money } from "../domain/money";
import { Wallet, WalletLedgerEntry } from "../domain/wallet";
import {
  WagerTransaction,
  type Kind,
  type Status,
} from "../domain/transaction";
import {
  InboxMessage,
  OutboxMessage,
  WalletBalanceChanged,
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WagerTransactionFailed,
  type EventContext,
  type IntegrationEvent,
} from "../domain/events";
import { type Database, Sql, WalletRecord } from "../infrastructure/database";
import { config } from "../infrastructure/config";
import { errorCode, log, metrics } from "../infrastructure/telemetry";
import {
  payloadHash,
  type TransactionResult,
  type WagerInput,
} from "./contracts";

export interface TransactionRow {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  idempotency_key: string;
  payload_hash: string;
  wallet_id: string;
  player_id: string;
  round_id: string;
  game_id: string;
  kind: Kind;
  amount: string;
  currency: string;
  reference_external_transaction_id: string | null;
  reference_transaction_id: string | null;
  status: Status;
  failure_code: FailureCode | null;
  result: TransactionResult | null;
  attempts: number;
  next_attempt_at: Date | null;
  created_at: Date;
  processed_at: Date | null;
  correlation_id: string | null;
  causation_id: string | null;
}
export interface ProcessContext extends EventContext {
  inbox?: { messageId: string; hash: string };
}
function domainTransaction(row: TransactionRow): WagerTransaction {
  return WagerTransaction.rehydrate({
    id: row.id,
    providerId: row.provider_id,
    externalTransactionId: row.external_transaction_id,
    idempotencyKey: row.idempotency_key,
    payloadHash: row.payload_hash,
    walletId: row.wallet_id,
    playerId: row.player_id,
    roundId: row.round_id,
    gameId: row.game_id,
    kind: row.kind,
    money: Money.rehydrate({ amount: row.amount, currency: row.currency }),
    referenceExternalTransactionId:
      row.reference_external_transaction_id ?? undefined,
    createdAt: new Date(row.created_at),
    status: row.status,
    referenceTransactionId: row.reference_transaction_id ?? undefined,
    failureCode: row.failure_code ?? undefined,
    processedAt: row.processed_at ? new Date(row.processed_at) : undefined,
  });
}
function domainWallet(row: WalletRecord): Wallet {
  return Wallet.rehydrate({
    ...row,
    balance: Money.rehydrate({ amount: row.balance, currency: row.currency }),
  });
}

export class Processor {
  constructor(private readonly db: Database) {}

  async openWallet(playerId: string, initialBalance: Money) {
    try {
      return await this.db.em.fork().transactional(async (em) => {
        const sql = new Sql(em);
        const wallet = Wallet.open({
          id: crypto.randomUUID(),
          playerId,
          initialBalance,
        });
        const record = em.create(WalletRecord, {
          id: wallet.id,
          playerId,
          currency: wallet.currency,
          balance: wallet.balance.toString(),
          version: 1,
          createdAt: wallet.createdAt,
          updatedAt: wallet.updatedAt,
        });
        await em.persistAndFlush(record);
        if (initialBalance.isPositive()) {
          const id = crypto.randomUUID();
          const result: TransactionResult = {
            transactionId: id,
            status: "PROCESSED",
            balance: initialBalance.toJSON(),
            idempotentReplay: false,
          };
          await sql.query(
            `INSERT INTO wager_transactions
            (id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,
             amount,currency,status,result,processed_at)
            VALUES (?, 'internal', ?, ?, ?, ?, ?, 'opening', 'opening', 'OPENING', ?, ?, 'PROCESSED', ?::jsonb, now())`,
            [
              id,
              id,
              `opening:${wallet.id}`,
              payloadHash({
                walletId: wallet.id,
                initialBalance: initialBalance.toJSON(),
              }),
              wallet.id,
              playerId,
              initialBalance.toString(),
              wallet.currency,
              JSON.stringify(result),
            ],
          );
          const entry = WalletLedgerEntry.create({
            id: crypto.randomUUID(),
            walletId: wallet.id,
            transactionId: id,
            direction: "CREDIT",
            money: initialBalance,
            balanceBefore: Money.zero(wallet.currency),
            balanceAfter: initialBalance,
            createdAt: new Date(),
          });
          await this.insertLedger(sql, entry, 1);
          const ctx = { correlationId: wallet.id };
          await this.enqueue(
            sql,
            WagerTransactionProcessed.from({
              eventId: crypto.randomUUID(),
              aggregateId: wallet.id,
              occurredAt: new Date(),
              ...ctx,
              data: {
                transactionId: id,
                walletId: wallet.id,
                providerId: "internal",
                status: "PROCESSED",
              },
            }),
          );
          await this.balanceEvent(sql, wallet, entry, ctx);
        }
        return {
          id: wallet.id,
          playerId,
          balance: wallet.balance.toJSON(),
          version: wallet.version,
        };
      });
    } catch (error) {
      if (errorCode(error) === "23505")
        throw new ConflictError("WALLET_ALREADY_EXISTS");
      throw error;
    }
  }

  async process(
    input: WagerInput,
    idempotencyKey: string,
    context: ProcessContext,
  ): Promise<TransactionResult> {
    const stop = metrics.latency.startTimer();
    try {
      const result = await this.db.em.fork().transactional(async (em) => {
        const sql = new Sql(em);
        await sql.query(`SET LOCAL lock_timeout = '5s'`);
        const hash = payloadHash(input);
        const existing = await sql.query<TransactionRow>(
          `SELECT * FROM wager_transactions
          WHERE idempotency_key = ? OR (provider_id = ? AND external_transaction_id = ?)`,
          [idempotencyKey, input.providerId, input.externalTransactionId],
        );
        if (existing.length) {
          const row = this.checkReplay(existing, idempotencyKey, hash);
          await this.inbox(sql, row.id, context);
          return { ...row.result!, idempotentReplay: true };
        }
        const id = crypto.randomUUID();
        const inserted = await sql.query<{ id: string }>(
          `INSERT INTO wager_transactions
          (id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,
           amount,currency,reference_external_transaction_id,status,correlation_id,causation_id)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'PENDING',?,?) ON CONFLICT DO NOTHING RETURNING id`,
          [
            id,
            input.providerId,
            input.externalTransactionId,
            idempotencyKey,
            hash,
            input.walletId,
            input.playerId,
            input.roundId,
            input.gameId,
            input.kind,
            input.money.amount,
            input.money.currency,
            input.referenceExternalTransactionId ?? null,
            context.correlationId,
            context.causationId ?? null,
          ],
        );
        // The unique index serializes the operation identity before any wallet lock.
        // Replays never queue behind other replays on the wallet row.
        if (!inserted.length) {
          const rows = await sql.query<TransactionRow>(
            `SELECT * FROM wager_transactions
            WHERE idempotency_key = ? OR (provider_id = ? AND external_transaction_id = ?)`,
            [idempotencyKey, input.providerId, input.externalTransactionId],
          );
          const row = this.checkReplay(rows, idempotencyKey, hash);
          await this.inbox(sql, row.id, context);
          return { ...row.result!, idempotentReplay: true };
        }
        const record = await em.findOne(
          WalletRecord,
          { id: input.walletId },
          { lockMode: LockMode.PESSIMISTIC_WRITE },
        );
        const tx = WagerTransaction.create({
          ...input,
          id,
          idempotencyKey,
          payloadHash: hash,
          money: Money.from(input.money),
          createdAt: new Date(),
        });
        const result = await this.apply(sql, tx, record, context, 0);
        await this.inbox(sql, id, context);
        return result;
      });
      if (result.idempotentReplay) metrics.duplicates.inc();
      else metrics.transactions.inc({ status: result.status });
      log("info", "transaction_result", {
        correlationId: context.correlationId,
        messageId: context.inbox?.messageId,
        transactionId: result.transactionId,
        walletId: input.walletId,
        providerId: input.providerId,
        status: result.status,
        idempotentReplay: result.idempotentReplay,
        failureCode: result.failureCode,
      });
      return result;
    } catch (error) {
      if (["55P03", "40P01", "40001"].includes(errorCode(error)))
        metrics.locks.inc();
      throw error;
    } finally {
      stop();
    }
  }

  async retryReferences(signal?: AbortSignal): Promise<number> {
    const due = await new Sql(this.db.em.fork()).query<{
      id: string;
      wallet_id: string;
    }>(
      `SELECT id,wallet_id FROM wager_transactions WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= now()
       ORDER BY next_attempt_at,id LIMIT 50`,
    );
    let processed = 0;
    for (const candidate of due) {
      if (signal?.aborted) break;
      await this.db.em.fork().transactional(async (em) => {
        const sql = new Sql(em);
        await sql.query(`SET LOCAL lock_timeout = '5s'`);
        const record = await em.findOne(
          WalletRecord,
          { id: candidate.wallet_id },
          { lockMode: LockMode.PESSIMISTIC_WRITE },
        );
        const [row] = await sql.query<TransactionRow>(
          "SELECT * FROM wager_transactions WHERE id = ? FOR UPDATE",
          [candidate.id],
        );
        if (
          !row ||
          row.status !== "PENDING_REFERENCE" ||
          !row.next_attempt_at ||
          new Date(row.next_attempt_at) > new Date()
        )
          return;
        const result = await this.apply(
          sql,
          domainTransaction(row),
          record,
          {
            correlationId: row.correlation_id ?? row.id,
            causationId: row.causation_id ?? undefined,
          },
          row.attempts + 1,
        );
        metrics.retries.inc({ worker: "reference" });
        if (result.status !== "PENDING_REFERENCE")
          metrics.transactions.inc({ status: result.status });
        log("info", "reference_retry_result", {
          correlationId: row.correlation_id ?? row.id,
          messageId: row.causation_id,
          transactionId: row.id,
          walletId: row.wallet_id,
          providerId: row.provider_id,
          status: result.status,
          failureCode: result.failureCode,
          attempts: row.attempts + 1,
        });
        processed += 1;
      });
    }
    return processed;
  }

  async recordPermanentFailure(
    input: WagerInput,
    idempotencyKey: string,
    context: ProcessContext,
    failureCode: "INFRASTRUCTURE_PERMANENT" | "RETRY_EXHAUSTED",
    delivery: {
      body: string;
      messageId: string;
      alreadyDeadLettered?: boolean;
    },
  ): Promise<TransactionResult> {
    const result = await this.db.em.fork().transactional(async (em) => {
      const sql = new Sql(em);
      await sql.query(`SET LOCAL lock_timeout = '5s'`);
      const id = crypto.randomUUID();
      const hash = payloadHash(input);
      const created = await sql.query<{ id: string }>(
        `INSERT INTO wager_transactions
        (id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,
         amount,currency,reference_external_transaction_id,status,correlation_id,causation_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'PENDING',?,?) ON CONFLICT DO NOTHING RETURNING id`,
        [
          id,
          input.providerId,
          input.externalTransactionId,
          idempotencyKey,
          hash,
          input.walletId,
          input.playerId,
          input.roundId,
          input.gameId,
          input.kind,
          input.money.amount,
          input.money.currency,
          input.referenceExternalTransactionId ?? null,
          context.correlationId,
          context.causationId ?? null,
        ],
      );
      let result: TransactionResult;
      if (created.length) {
        const tx = WagerTransaction.create({
          ...input,
          id,
          idempotencyKey,
          payloadHash: hash,
          money: Money.from(input.money),
          createdAt: new Date(),
        });
        tx.fail(failureCode);
        result = {
          transactionId: id,
          status: tx.status,
          failureCode: tx.failureCode,
          idempotentReplay: false,
        };
        await sql.query(
          `UPDATE wager_transactions SET status='FAILED',failure_code=?,processed_at=?,result=?::jsonb WHERE id=?`,
          [failureCode, tx.processedAt, JSON.stringify(result), id],
        );
        await this.enqueue(
          sql,
          WagerTransactionFailed.from({
            eventId: crypto.randomUUID(),
            aggregateId: input.walletId,
            ...context,
            occurredAt: new Date(),
            data: {
              transactionId: id,
              walletId: input.walletId,
              providerId: input.providerId,
              status: "FAILED",
              failureCode,
            },
          }),
        );
      } else {
        const rows = await sql.query<TransactionRow>(
          `SELECT * FROM wager_transactions
          WHERE idempotency_key=? OR (provider_id=? AND external_transaction_id=?) FOR UPDATE`,
          [idempotencyKey, input.providerId, input.externalTransactionId],
        );
        const existing = this.checkReplay(rows, idempotencyKey, hash);
        // A transport failure must never replace a financial result that another instance committed.
        result = { ...existing.result!, idempotentReplay: true };
      }
      await this.inbox(sql, result.transactionId, context);
      if (result.status === "FAILED" && !delivery.alreadyDeadLettered) {
        await sql.query(
          `INSERT INTO outbox (id,aggregate_id,event_type,payload,occurred_at,destination)
          VALUES (?,?,'WagerMessageDeadLetterRequested',?::jsonb,now(),'dlq') ON CONFLICT DO NOTHING`,
          [
            crypto.randomUUID(),
            input.walletId,
            JSON.stringify({
              body: delivery.body,
              deduplicationId: delivery.messageId,
              correlationId: context.correlationId,
              messageId: context.inbox?.messageId ?? null,
              transactionId: result.transactionId,
              walletId: input.walletId,
              providerId: input.providerId,
              failureCode: result.failureCode,
            }),
          ],
        );
      }
      return result;
    });
    if (!result.idempotentReplay)
      metrics.transactions.inc({ status: result.status });
    log("error", "transaction_failure_recorded", {
      correlationId: context.correlationId,
      messageId: context.inbox?.messageId,
      transactionId: result.transactionId,
      walletId: input.walletId,
      providerId: input.providerId,
      status: result.status,
      failureCode: result.failureCode,
      idempotentReplay: result.idempotentReplay,
    });
    return result;
  }

  private checkReplay(
    rows: TransactionRow[],
    key: string,
    hash: string,
  ): TransactionRow {
    const row = rows[0];
    if (
      !row ||
      rows.length !== 1 ||
      row.idempotency_key !== key ||
      row.payload_hash !== hash
    )
      throw new ConflictError("IDEMPOTENCY_CONFLICT");
    if (!row.result) throw new Error("Committed transaction has no result");
    return row;
  }

  private async apply(
    sql: Sql,
    tx: WagerTransaction,
    record: WalletRecord | null,
    ctx: EventContext,
    attempt: number,
  ): Promise<TransactionResult> {
    const wallet = record ? domainWallet(record) : undefined;
    let entry: WalletLedgerEntry | undefined;
    let reference: WagerTransaction | undefined;
    const wasPending = tx.status === "PENDING_REFERENCE";
    try {
      if (!wallet) throw new DomainError("WALLET_NOT_FOUND");
      if (wallet.playerId !== tx.props.playerId)
        throw new DomainError("PLAYER_MISMATCH");
      if (wallet.currency !== tx.props.money.currency)
        throw new DomainError("CURRENCY_MISMATCH");
      if (tx.props.referenceExternalTransactionId) {
        const [row] = await sql.query<TransactionRow>(
          `SELECT * FROM wager_transactions WHERE provider_id = ? AND external_transaction_id = ?`,
          [tx.props.providerId, tx.props.referenceExternalTransactionId],
        );
        if (
          !row ||
          row.status === "PENDING" ||
          row.status === "PENDING_REFERENCE"
        ) {
          if (attempt >= config.REFERENCE_MAX_ATTEMPTS)
            throw new DomainError("REFERENCE_NOT_FOUND");
          tx.markPendingReference();
        } else {
          reference = domainTransaction(row);
          if (row.status !== "PROCESSED")
            throw new DomainError("REFERENCE_REJECTED");
          if (
            row.wallet_id !== wallet.id ||
            row.player_id !== wallet.playerId ||
            row.currency !== wallet.currency ||
            row.round_id !== tx.props.roundId
          ) {
            throw new DomainError("REFERENCE_MISMATCH");
          }
          const allowed =
            tx.props.kind === "ROLLBACK" ? ["BET", "WIN", "REFUND"] : ["BET"];
          if (!allowed.includes(row.kind))
            throw new DomainError("INVALID_REFERENCE_KIND");
          if (tx.requiresReference()) {
            if (!tx.props.money.equals(reference.props.money))
              throw new DomainError("REVERSAL_AMOUNT_MISMATCH");
            const reversed = await sql.query(
              `SELECT id FROM wager_transactions WHERE reference_transaction_id = ? AND kind = ? AND status = 'PROCESSED'`,
              [row.id, tx.props.kind],
            );
            if (reversed.length) throw new DomainError("ALREADY_REVERSED");
          }
        }
      }
      if (tx.status !== "PENDING_REFERENCE" || (wasPending && reference)) {
        if (tx.affectsBalance()) {
          entry =
            tx.ledgerDirectionFor(reference) === "DEBIT"
              ? wallet.debit(
                  tx.props.money,
                  tx.props.id,
                  tx.props.kind === "ROLLBACK",
                )
              : wallet.credit(tx.props.money, tx.props.id);
        }
        tx.markProcessed(reference?.props.id, new Date());
      }
    } catch (error) {
      if (
        !(error instanceof DomainError) ||
        error.code === "INVALID_STATE" ||
        error.code === "INVALID_MONEY"
      )
        throw error;
      tx.reject(error.code);
    }
    if (entry && wallet && record) {
      record.balance = wallet.balance.toString();
      record.version = wallet.version;
      record.updatedAt = wallet.updatedAt;
      await sql.em.flush();
      await this.insertLedger(sql, entry, wallet.version);
    }
    const result: TransactionResult = {
      transactionId: tx.props.id,
      status: tx.status,
      balance: wallet?.balance.toJSON(),
      failureCode: tx.failureCode,
      idempotentReplay: false,
    };
    const nextAttempt =
      tx.status === "PENDING_REFERENCE"
        ? new Date(
            Date.now() +
              Math.min(
                60_000,
                config.REFERENCE_RETRY_MS * 2 ** Math.min(attempt, 10),
              ),
          )
        : null;
    await sql.query(
      `UPDATE wager_transactions SET status = ?, reference_transaction_id = ?, failure_code = ?,
      processed_at = ?, result = ?::jsonb, attempts = ?, next_attempt_at = ? WHERE id = ?`,
      [
        tx.status,
        tx.referenceTransactionId ?? null,
        tx.failureCode ?? null,
        tx.processedAt ?? null,
        JSON.stringify(result),
        attempt,
        nextAttempt,
        tx.props.id,
      ],
    );
    const props = {
      eventId: crypto.randomUUID(),
      aggregateId: tx.props.walletId,
      ...ctx,
      occurredAt: new Date(),
      data: {
        transactionId: tx.props.id,
        walletId: tx.props.walletId,
        providerId: tx.props.providerId,
        status: tx.status,
        failureCode: tx.failureCode,
      },
    };
    if (tx.status === "PROCESSED")
      await this.enqueue(sql, WagerTransactionProcessed.from(props));
    else if (tx.status === "REJECTED")
      await this.enqueue(sql, WagerTransactionRejected.from(props));
    else if (!wasPending)
      await this.enqueue(sql, WagerTransactionPendingReference.from(props));
    if (entry && wallet) await this.balanceEvent(sql, wallet, entry, ctx);
    return result;
  }

  private async inbox(sql: Sql, transactionId: string, ctx: ProcessContext) {
    if (!ctx.inbox) return;
    const message = InboxMessage.receive({
      messageId: ctx.inbox.messageId,
      consumerName: "wager-processor",
      payloadHash: ctx.inbox.hash,
    });
    message.markProcessed(new Date());
    await sql.query(
      `INSERT INTO inbox (consumer_name,message_id,payload_hash,transaction_id,received_at,processed_at)
      VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING`,
      [
        message.consumerName,
        message.messageId,
        message.payloadHash,
        transactionId,
        message.receivedAt,
        message.processedAt,
      ],
    );
    const [existing] = await sql.query<{
      payload_hash: string;
      transaction_id: string;
    }>(
      "SELECT payload_hash,transaction_id FROM inbox WHERE consumer_name = ? AND message_id = ?",
      [message.consumerName, message.messageId],
    );
    if (
      existing?.payload_hash !== message.payloadHash ||
      existing.transaction_id !== transactionId
    )
      throw new ConflictError("INBOX_PAYLOAD_CONFLICT");
  }
  private async insertLedger(sql: Sql, e: WalletLedgerEntry, version: number) {
    await sql.query(
      `INSERT INTO wallet_ledger (id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after,wallet_version,created_at)
      VALUES (?,?,?,?,?,?,?,?,?,?)`,
      [
        e.id,
        e.walletId,
        e.transactionId,
        e.direction,
        e.money.toString(),
        e.money.currency,
        e.balanceBefore.toString(),
        e.balanceAfter.toString(),
        version,
        e.createdAt,
      ],
    );
  }
  private async balanceEvent(
    sql: Sql,
    wallet: Wallet,
    entry: WalletLedgerEntry,
    ctx: EventContext,
  ) {
    await this.enqueue(
      sql,
      WalletBalanceChanged.from({
        eventId: crypto.randomUUID(),
        aggregateId: wallet.id,
        ...ctx,
        occurredAt: new Date(),
        data: {
          walletId: wallet.id,
          transactionId: entry.transactionId,
          direction: entry.direction,
          money: entry.money.toJSON(),
          balanceBefore: entry.balanceBefore.toJSON(),
          balanceAfter: entry.balanceAfter.toJSON(),
          walletVersion: wallet.version,
        },
      }),
    );
  }
  private async enqueue(sql: Sql, event: IntegrationEvent<unknown>) {
    const outbox = OutboxMessage.enqueue(event);
    await sql.query(
      "INSERT INTO outbox (id,aggregate_id,event_type,payload,occurred_at) VALUES (?,?,?,?::jsonb,?)",
      [
        outbox.id,
        outbox.aggregateId,
        outbox.eventType,
        JSON.stringify(outbox.payload),
        outbox.occurredAt,
      ],
    );
  }
}
