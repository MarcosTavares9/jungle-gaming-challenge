import { Migration } from "@mikro-orm/migrations";

export class Migration202610290003 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      CREATE TABLE dlq_archive (
        source_queue text NOT NULL, message_id varchar(200) NOT NULL, body text NOT NULL,
        payload_hash char(64) NOT NULL,
        outcome text NOT NULL CHECK (outcome IN ('AUDITED','INVALID_PAYLOAD','CONFLICT')),
        transaction_id uuid REFERENCES wager_transactions(id), transaction_status text, failure_code text,
        archived_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (source_queue,message_id)
      );
      CREATE INDEX dlq_archive_transaction ON dlq_archive(transaction_id);
      CREATE FUNCTION prevent_dlq_archive_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'DLQ archive is append-only' USING ERRCODE='23514'; END $$;
      CREATE TRIGGER dlq_archive_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON dlq_archive
        FOR EACH STATEMENT EXECUTE FUNCTION prevent_dlq_archive_mutation();
    `);
  }
  override async down(): Promise<void> {
    this.addSql(
      "DROP TABLE dlq_archive; DROP FUNCTION prevent_dlq_archive_mutation();",
    );
  }
}
