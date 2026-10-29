import {
  Catch,
  HttpException,
  type ArgumentsHost,
  type ExceptionFilter,
} from "@nestjs/common";
import type { Request, Response } from "express";
import { ZodError } from "zod";
import { ConflictError, DomainError, NotFoundError } from "../domain/errors";
import { errorCode, log } from "../infrastructure/telemetry";

@Catch()
export class ApiErrors implements ExceptionFilter {
  catch(error: unknown, host: ArgumentsHost) {
    const http = host.switchToHttp();
    const response = http.getResponse<Response>();
    const request = http.getRequest<Request>();
    const correlationId =
      request.headers["x-correlation-id"] ?? crypto.randomUUID();
    response.setHeader("x-correlation-id", String(correlationId));
    if (error instanceof ZodError) {
      response.status(400).json({
        code: "INVALID_PAYLOAD",
        issues: error.issues.map((i) => ({
          path: i.path,
          message: i.message,
        })),
        correlationId,
      });
    } else if (error instanceof ConflictError)
      response.status(409).json({ code: error.code, correlationId });
    else if (error instanceof NotFoundError)
      response.status(404).json({ code: error.message, correlationId });
    else if (error instanceof DomainError)
      response.status(400).json({ code: error.code, correlationId });
    else if (error instanceof HttpException)
      response
        .status(error.getStatus())
        .json({ code: "HTTP_ERROR", correlationId });
    else {
      log("error", "request_failed", { correlationId, code: errorCode(error) });
      response.setHeader("retry-after", "1");
      response
        .status(503)
        .json({ code: "INFRASTRUCTURE_UNAVAILABLE", correlationId });
    }
  }
}
