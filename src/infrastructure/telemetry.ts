import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  collectDefaultMetrics,
} from "prom-client";
export const registry = new Registry();
collectDefaultMetrics({ register: registry });
export const metrics = {
  transactions: new Counter({
    name: "wager_transactions_total",
    help: "Committed results",
    labelNames: ["status"],
    registers: [registry],
  }),
  duplicates: new Counter({
    name: "wager_duplicates_total",
    help: "Persistent replays",
    registers: [registry],
  }),
  retries: new Counter({
    name: "wager_retries_total",
    help: "Retries",
    labelNames: ["worker"],
    registers: [registry],
  }),
  dlq: new Counter({
    name: "wager_dlq_total",
    help: "Messages moved to DLQ",
    registers: [registry],
  }),
  locks: new Counter({
    name: "wager_lock_conflicts_total",
    help: "Lock timeouts and deadlocks",
    registers: [registry],
  }),
  outboxLag: new Gauge({
    name: "wager_outbox_lag_seconds",
    help: "Age of oldest unpublished event",
    registers: [registry],
  }),
  reconciliation: new Counter({
    name: "wallet_reconciliation_mismatches_total",
    help: "Ledger mismatches",
    registers: [registry],
  }),
  latency: new Histogram({
    name: "wager_processing_seconds",
    help: "Use case latency",
    buckets: [0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10],
    registers: [registry],
  }),
};
export function log(
  level: "info" | "warn" | "error",
  event: string,
  context: Record<string, unknown> = {},
) {
  console.log(
    JSON.stringify({
      level,
      event,
      time: new Date().toISOString(),
      instanceId: process.env.HOSTNAME ?? String(process.pid),
      ...context,
      correlationId: context.correlationId ?? null,
      messageId: context.messageId ?? null,
      transactionId: context.transactionId ?? null,
      walletId: context.walletId ?? null,
      providerId: context.providerId ?? null,
    }),
  );
}
export function errorCode(error: unknown): string {
  if (typeof error !== "object" || !error) return "UNKNOWN";
  const e = error as { code?: string; name?: string; cause?: unknown };
  return e.code ?? (e.cause ? errorCode(e.cause) : (e.name ?? "UNKNOWN"));
}
