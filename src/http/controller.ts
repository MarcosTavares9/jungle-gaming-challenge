import {
  Body,
  Controller,
  Get,
  Headers,
  Inject,
  Param,
  Post,
  Query,
  Res,
  UseGuards,
} from "@nestjs/common";
import type { Response } from "express";
import { z } from "zod";
import { Processor } from "../application/processor";
import { Queries } from "../application/queries";
import {
  keySchema,
  uuidSchema,
  wagerSchema,
  walletSchema,
  type TransactionResult,
} from "../application/contracts";
import { Money } from "../domain/money";
import { type Database, Sql } from "../infrastructure/database";
import { Queues } from "../infrastructure/queues";
import { registry } from "../infrastructure/telemetry";
import { ProviderIdentityGuard } from "./auth";

export function resultStatus(result: TransactionResult): number {
  if (result.status === "PENDING" || result.status === "PENDING_REFERENCE")
    return 202;
  if (result.status === "REJECTED") return 422;
  if (result.status === "FAILED") return 500;
  return 200;
}
@Controller()
@UseGuards(ProviderIdentityGuard)
export class WagerController {
  constructor(
    @Inject(Processor) private readonly processor: Processor,
    @Inject(Queries) private readonly queries: Queries,
  ) {}
  @Post("wallets")
  createWallet(@Body() body: unknown) {
    const input = walletSchema.parse(body);
    return this.processor.openWallet(
      input.playerId,
      Money.from(input.initialBalance),
    );
  }
  @Get("wallets/:walletId")
  wallet(@Param("walletId") id: string) {
    return this.queries.wallet(uuidSchema.parse(id));
  }
  @Get("wallets/:walletId/ledger")
  ledger(
    @Param("walletId") id: string,
    @Query("limit") limit?: string,
    @Query("cursor") cursor?: string,
  ) {
    return this.queries.ledger(
      uuidSchema.parse(id),
      z.coerce.number().int().min(1).max(100).default(50).parse(limit),
      cursor,
    );
  }
  @Post("wallets/:walletId/reconciliation")
  reconcile(
    @Param("walletId") id: string,
    @Headers("x-correlation-id") correlationId?: string,
  ) {
    return this.queries.reconcile(
      uuidSchema.parse(id),
      correlationId ?? crypto.randomUUID(),
    );
  }
  @Post("wagering/transactions")
  async process(
    @Body() body: unknown,
    @Headers("idempotency-key") key: unknown,
    @Headers("x-correlation-id") correlationId: string | undefined,
    @Res({ passthrough: true }) response: Response,
  ) {
    const result = await this.processor.process(
      wagerSchema.parse(body),
      keySchema.parse(key),
      { correlationId: correlationId ?? crypto.randomUUID() },
    );
    response.status(resultStatus(result));
    return result;
  }
  @Get("wagering/transactions/:id")
  transaction(@Param("id") id: string) {
    return this.queries.transaction(uuidSchema.parse(id));
  }
  @Get("providers/:providerId/wagering/transactions/:externalId")
  providerTransaction(
    @Param("providerId") provider: string,
    @Param("externalId") external: string,
  ) {
    return this.queries.providerTransaction(provider, external);
  }
}
@Controller()
export class OperationsController {
  constructor(
    @Inject("DATABASE") private readonly db: Database,
    @Inject(Queues) private readonly queues: Queues,
  ) {}
  @Get("health/live") live() {
    return { status: "ok" };
  }
  @Get("health/ready") async ready() {
    await Promise.all([
      new Sql(this.db.em.fork()).query("SELECT 1"),
      this.queues.ready(),
    ]);
    return { status: "ok" };
  }
  @Get("metrics") async metrics(@Res() response: Response) {
    response.type(registry.contentType).send(await registry.metrics());
  }
}
