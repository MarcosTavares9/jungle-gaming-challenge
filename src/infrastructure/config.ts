import { z } from "zod";
const schema = z.object({
  PORT: z.coerce.number().int().min(1).max(65535).default(3000),
  DATABASE_URL: z
    .string()
    .default("postgresql://jungle:jungle@127.0.0.1:15432/jungle"),
  SQS_ENDPOINT: z.url().default("http://localhost:4566"),
  SQS_QUEUE_PREFIX: z
    .string()
    .regex(/^[a-z0-9-]{0,40}$/)
    .default(""),
  AWS_REGION: z.string().default("us-east-1"),
  AWS_ACCESS_KEY_ID: z.string().default("test"),
  AWS_SECRET_ACCESS_KEY: z.string().default("test"),
  WORKERS_ENABLED: z.enum(["true", "false"]).default("true"),
  REFERENCE_MAX_ATTEMPTS: z.coerce.number().int().min(1).default(12),
  REFERENCE_RETRY_MS: z.coerce.number().int().min(10).default(1000),
});
export const config = schema.parse(process.env);
