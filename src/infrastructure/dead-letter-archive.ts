import { createHash } from "node:crypto";
import { type Database, Sql } from "./database";

export interface DeadLetterAudit {
  sourceQueue: string;
  messageId: string;
  body: string;
  outcome: "AUDITED" | "INVALID_PAYLOAD" | "CONFLICT";
  transactionId?: string;
  transactionStatus?: string;
  failureCode?: string;
}

export class DeadLetterArchive {
  constructor(private readonly db: Database) {}
  async save(record: DeadLetterAudit): Promise<void> {
    const hash = createHash("sha256").update(record.body).digest("hex");
    await this.db.em.fork().transactional(async (em) => {
      const sql = new Sql(em);
      await sql.query(
        `INSERT INTO dlq_archive
        (source_queue,message_id,body,payload_hash,outcome,transaction_id,transaction_status,failure_code)
        VALUES (?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING`,
        [
          record.sourceQueue,
          record.messageId,
          record.body,
          hash,
          record.outcome,
          record.transactionId ?? null,
          record.transactionStatus ?? null,
          record.failureCode ?? null,
        ],
      );
      const [existing] = await sql.query<{ payload_hash: string }>(
        "SELECT payload_hash FROM dlq_archive WHERE source_queue=? AND message_id=?",
        [record.sourceQueue, record.messageId],
      );
      if (existing?.payload_hash !== hash)
        throw new Error("DLQ archive payload conflict");
    });
  }
}
