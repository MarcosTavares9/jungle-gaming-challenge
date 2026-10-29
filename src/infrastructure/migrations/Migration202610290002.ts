import { Migration } from "@mikro-orm/migrations";

export class Migration202610290002 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      ALTER TABLE wager_transactions ADD COLUMN correlation_id text, ADD COLUMN causation_id text;
      ALTER TABLE outbox ADD COLUMN destination text NOT NULL DEFAULT 'events'
        CHECK (destination IN ('events','dlq'));
      CREATE UNIQUE INDEX outbox_dlq_delivery ON outbox ((payload->>'deduplicationId')) WHERE destination='dlq';
    `);
  }
  override async down(): Promise<void> {
    // A pending DLQ intent must never be silently converted into a public integration event.
    this.addSql(`DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM outbox WHERE destination='dlq' AND published_at IS NULL) THEN
        RAISE EXCEPTION 'Publish pending DLQ intents before reverting this migration';
      END IF;
    END $$;
    DROP INDEX outbox_dlq_delivery;
    ALTER TABLE outbox DROP COLUMN destination;
    ALTER TABLE wager_transactions DROP COLUMN correlation_id, DROP COLUMN causation_id;`);
  }
}
