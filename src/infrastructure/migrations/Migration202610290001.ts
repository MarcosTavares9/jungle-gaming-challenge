import { Migration } from "@mikro-orm/migrations";

export class Migration202610290001 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      CREATE TABLE wallets (
        id uuid PRIMARY KEY, player_id uuid NOT NULL, currency varchar(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
        balance numeric(18,2) NOT NULL CHECK (balance >= 0), version integer NOT NULL CHECK (version >= 1),
        created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
        UNIQUE (player_id, currency)
      );
      CREATE TABLE wager_transactions (
        id uuid PRIMARY KEY, provider_id varchar(200) NOT NULL, external_transaction_id varchar(200) NOT NULL,
        idempotency_key varchar(400) NOT NULL UNIQUE, payload_hash char(64) NOT NULL,
        wallet_id uuid NOT NULL, player_id uuid NOT NULL, round_id varchar(200) NOT NULL, game_id varchar(200) NOT NULL,
        kind text NOT NULL CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        amount numeric(18,2) NOT NULL CHECK (amount >= 0), currency varchar(3) NOT NULL,
        reference_external_transaction_id varchar(200), reference_transaction_id uuid REFERENCES wager_transactions(id),
        status text NOT NULL CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        failure_code text, result jsonb, attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
        next_attempt_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(), processed_at timestamptz,
        UNIQUE (provider_id, external_transaction_id),
        CHECK ((kind = 'LOSS' AND amount = 0) OR (kind <> 'LOSS' AND amount > 0)),
        CHECK (kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
        CHECK (status NOT IN ('REJECTED','FAILED') OR failure_code IS NOT NULL),
        CHECK (status NOT IN ('PROCESSED','REJECTED','FAILED') OR (result IS NOT NULL AND processed_at IS NOT NULL))
      );
      CREATE UNIQUE INDEX one_reversal_per_kind ON wager_transactions(reference_transaction_id, kind)
        WHERE kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED';
      CREATE INDEX pending_reference_due ON wager_transactions(next_attempt_at, id) WHERE status = 'PENDING_REFERENCE';
      CREATE INDEX transactions_wallet ON wager_transactions(wallet_id, created_at, id);
      CREATE TABLE wallet_ledger (
        id uuid PRIMARY KEY, wallet_id uuid NOT NULL REFERENCES wallets(id),
        transaction_id uuid NOT NULL REFERENCES wager_transactions(id),
        direction text NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
        amount numeric(18,2) NOT NULL CHECK (amount > 0), currency varchar(3) NOT NULL,
        balance_before numeric(18,2) NOT NULL CHECK (balance_before >= 0),
        balance_after numeric(18,2) NOT NULL CHECK (balance_after >= 0),
        wallet_version integer NOT NULL CHECK (wallet_version >= 1), created_at timestamptz NOT NULL DEFAULT now(),
        UNIQUE (wallet_id, transaction_id), UNIQUE (wallet_id, wallet_version),
        CHECK (balance_after = balance_before + CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END)
      );
      CREATE INDEX ledger_cursor ON wallet_ledger(wallet_id, wallet_version, id);
      CREATE TABLE inbox (
        consumer_name text NOT NULL, message_id varchar(200) NOT NULL, payload_hash char(64) NOT NULL,
        transaction_id uuid NOT NULL REFERENCES wager_transactions(id), received_at timestamptz NOT NULL DEFAULT now(),
        processed_at timestamptz NOT NULL, PRIMARY KEY(consumer_name, message_id)
      );
      CREATE TABLE outbox (
        id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, event_type text NOT NULL, payload jsonb NOT NULL,
        occurred_at timestamptz NOT NULL, attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
        next_attempt_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz
      );
      CREATE INDEX outbox_due ON outbox(next_attempt_at, occurred_at, id) WHERE published_at IS NULL;

      CREATE FUNCTION prevent_ledger_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'Ledger is append-only' USING ERRCODE = '23514'; END $$;
      CREATE TRIGGER ledger_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON wallet_ledger
        FOR EACH STATEMENT EXECUTE FUNCTION prevent_ledger_mutation();

      CREATE FUNCTION guard_transaction_state() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
          RAISE EXCEPTION 'Terminal transaction is immutable' USING ERRCODE = '23514';
        END IF;
        IF (to_jsonb(NEW) - ARRAY['status','failure_code','result','attempts','next_attempt_at','processed_at','reference_transaction_id'])
          IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['status','failure_code','result','attempts','next_attempt_at','processed_at','reference_transaction_id']) THEN
          RAISE EXCEPTION 'Transaction payload is immutable' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
      END $$;
      CREATE TRIGGER transaction_state BEFORE UPDATE ON wager_transactions FOR EACH ROW EXECUTE FUNCTION guard_transaction_state();
      CREATE FUNCTION prevent_transaction_delete() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'Transactions are audit records' USING ERRCODE = '23514'; END $$;
      CREATE TRIGGER transaction_no_delete BEFORE DELETE OR TRUNCATE ON wager_transactions
        FOR EACH STATEMENT EXECUTE FUNCTION prevent_transaction_delete();

      CREATE FUNCTION guard_wallet_version() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.currency <> OLD.currency OR NEW.player_id <> OLD.player_id THEN
          RAISE EXCEPTION 'Wallet identity is immutable' USING ERRCODE = '23514';
        END IF;
        IF (NEW.balance <> OLD.balance AND NEW.version <> OLD.version + 1)
          OR (NEW.balance = OLD.balance AND NEW.version <> OLD.version) THEN
          RAISE EXCEPTION 'Invalid wallet version' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
      END $$;
      CREATE TRIGGER wallet_version BEFORE UPDATE ON wallets FOR EACH ROW EXECUTE FUNCTION guard_wallet_version();

      -- Deferred checks see the final state of wallet, transaction and ledger at commit.
      CREATE FUNCTION check_wallet_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE target uuid; w wallets%ROWTYPE; total numeric; last_entry wallet_ledger%ROWTYPE;
      BEGIN
        IF TG_TABLE_NAME = 'wallets' THEN target := NEW.id; ELSE target := NEW.wallet_id; END IF;
        SELECT * INTO w FROM wallets WHERE id = target;
        SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END), 0)
          INTO total FROM wallet_ledger WHERE wallet_id = target;
        IF w.balance <> total THEN RAISE EXCEPTION 'Wallet differs from ledger' USING ERRCODE = '23514'; END IF;
        SELECT * INTO last_entry FROM wallet_ledger WHERE wallet_id = target ORDER BY wallet_version DESC LIMIT 1;
        IF FOUND AND (last_entry.wallet_version <> w.version OR last_entry.balance_after <> w.balance) THEN
          RAISE EXCEPTION 'Ledger version differs from wallet' USING ERRCODE = '23514';
        END IF;
        IF TG_TABLE_NAME = 'wallet_ledger' THEN
          IF NEW.currency <> w.currency OR NEW.balance_before <> COALESCE((
            SELECT balance_after FROM wallet_ledger WHERE wallet_id = target AND wallet_version < NEW.wallet_version
            ORDER BY wallet_version DESC LIMIT 1), 0) THEN
            RAISE EXCEPTION 'Invalid ledger chain' USING ERRCODE = '23514';
          END IF;
        END IF;
        RETURN NULL;
      END $$;
      CREATE CONSTRAINT TRIGGER wallet_ledger_consistency AFTER INSERT OR UPDATE ON wallets
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_wallet_ledger();
      CREATE CONSTRAINT TRIGGER ledger_wallet_consistency AFTER INSERT ON wallet_ledger
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_wallet_ledger();

      CREATE FUNCTION check_transaction_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE target uuid; t wager_transactions%ROWTYPE; n integer;
      BEGIN
        IF TG_TABLE_NAME = 'wager_transactions' THEN target := NEW.id; ELSE target := NEW.transaction_id; END IF;
        SELECT * INTO t FROM wager_transactions WHERE id = target;
        SELECT COUNT(*) INTO n FROM wallet_ledger WHERE transaction_id = target;
        IF (t.status = 'PROCESSED' AND t.kind <> 'LOSS' AND n <> 1)
          OR ((t.status <> 'PROCESSED' OR t.kind = 'LOSS') AND n <> 0) THEN
          RAISE EXCEPTION 'Transaction and ledger disagree' USING ERRCODE = '23514';
        END IF;
        IF EXISTS (SELECT 1 FROM wallet_ledger l WHERE l.transaction_id = target
          AND (l.wallet_id <> t.wallet_id OR l.currency <> t.currency OR l.amount <> t.amount
            OR l.direction <> CASE WHEN t.kind = 'BET' THEN 'DEBIT'
              WHEN t.kind = 'ROLLBACK' AND (SELECT kind FROM wager_transactions WHERE id = t.reference_transaction_id) <> 'BET' THEN 'DEBIT'
              ELSE 'CREDIT' END)) THEN
          RAISE EXCEPTION 'Ledger payload differs from transaction' USING ERRCODE = '23514';
        END IF;
        RETURN NULL;
      END $$;
      CREATE CONSTRAINT TRIGGER transaction_ledger_consistency AFTER INSERT OR UPDATE ON wager_transactions
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_transaction_ledger();
      CREATE CONSTRAINT TRIGGER ledger_transaction_consistency AFTER INSERT ON wallet_ledger
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_transaction_ledger();
    `);
  }
  override async down(): Promise<void> {
    this
      .addSql(`DROP TABLE outbox, inbox, wallet_ledger, wager_transactions, wallets CASCADE;
      DROP FUNCTION check_transaction_ledger(), check_wallet_ledger(), guard_wallet_version(),
        prevent_transaction_delete(), guard_transaction_state(), prevent_ledger_mutation();`);
  }
}
