import { createHash } from "node:crypto";
import { z } from "zod";

export const moneySchema = z
  .object({
    amount: z.string().regex(/^(0|[1-9]\d{0,15})\.\d{2}$/),
    currency: z.literal("BRL"),
  })
  .strict();
const identifier = z.string().trim().min(1).max(200);
export const keySchema = z.string().min(1).max(400);
export const uuidSchema = z.uuid();
export const walletSchema = z
  .object({ playerId: uuidSchema, initialBalance: moneySchema })
  .strict();
export const wagerSchema = z
  .object({
    providerId: identifier,
    externalTransactionId: identifier,
    playerId: uuidSchema,
    walletId: uuidSchema,
    roundId: identifier,
    gameId: identifier,
    kind: z.enum(["BET", "WIN", "LOSS", "REFUND", "ROLLBACK"]),
    money: moneySchema,
    referenceExternalTransactionId: identifier.optional(),
  })
  .strict()
  .superRefine((p, ctx) => {
    if (
      (p.kind === "REFUND" || p.kind === "ROLLBACK") &&
      !p.referenceExternalTransactionId
    ) {
      ctx.addIssue({
        code: "custom",
        path: ["referenceExternalTransactionId"],
        message: "Referência obrigatória",
      });
    }
    if (p.kind === "BET" && p.referenceExternalTransactionId)
      ctx.addIssue({ code: "custom", message: "BET não aceita referência" });
    if (
      p.kind === "LOSS" ? p.money.amount !== "0.00" : p.money.amount === "0.00"
    ) {
      ctx.addIssue({
        code: "custom",
        path: ["money", "amount"],
        message: "LOSS exige zero; demais operações exigem valor positivo",
      });
    }
  });
export type WagerInput = z.infer<typeof wagerSchema>;
export const messageSchema = z
  .object({
    messageId: identifier,
    type: z.literal("WagerTransactionRequested"),
    occurredAt: z.iso.datetime(),
    data: wagerSchema.safeExtend({ idempotencyKey: keySchema }),
  })
  .strict();
export interface TransactionResult {
  transactionId: string;
  status: string;
  balance?: { amount: string; currency: string };
  failureCode?: string;
  idempotentReplay: boolean;
}

export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.entries(value)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}
export function payloadHash(value: unknown): string {
  return createHash("sha256").update(canonicalJson(value)).digest("hex");
}
