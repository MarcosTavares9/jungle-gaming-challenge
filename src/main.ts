import "reflect-metadata";
import {
  Module,
  type OnApplicationShutdown,
  type BeforeApplicationShutdown,
} from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { Processor } from "./application/processor";
import { Queries } from "./application/queries";
import { config } from "./infrastructure/config";
import { openDatabase } from "./infrastructure/database";
import { Queues } from "./infrastructure/queues";
import { Workers } from "./infrastructure/workers";
import { log } from "./infrastructure/telemetry";
import { WagerController, OperationsController } from "./http/controller";
import { ProviderIdentityGuard } from "./http/auth";
import { ApiErrors } from "./http/errors";
import type { Request, Response, NextFunction } from "express";

const db = await openDatabase();
const queues = new Queues();
const processor = new Processor(db);
const workers = new Workers(db, processor, queues);
@Module({
  controllers: [WagerController, OperationsController],
  providers: [
    ProviderIdentityGuard,
    { provide: "DATABASE", useValue: db },
    { provide: Queues, useValue: queues },
    { provide: Processor, useValue: processor },
    { provide: Queries, useValue: new Queries(db) },
  ],
})
class AppModule implements OnApplicationShutdown, BeforeApplicationShutdown {
  async beforeApplicationShutdown(signal?: string) {
    log("info", "service_stopping", { signal });
    await workers.stop();
  }
  async onApplicationShutdown(signal?: string) {
    queues.close();
    await db.close(true);
    log("info", "service_stopped", { signal });
  }
}
const app = await NestFactory.create(AppModule, { logger: false });
app.use((request: Request, response: Response, next: NextFunction) => {
  const header = request.headers["x-correlation-id"];
  const correlationId =
    typeof header === "string" && header.length > 0 && header.length <= 200
      ? header
      : crypto.randomUUID();
  request.headers["x-correlation-id"] = correlationId;
  response.setHeader("x-correlation-id", correlationId);
  next();
});
app.useGlobalFilters(new ApiErrors());
app.enableShutdownHooks();
await app.listen(config.PORT, "0.0.0.0");
if (config.WORKERS_ENABLED === "true") workers.start();
log("info", "service_started", {
  port: config.PORT,
  workers: config.WORKERS_ENABLED === "true",
});
