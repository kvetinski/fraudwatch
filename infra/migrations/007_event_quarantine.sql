-- Generalize the transaction-only quarantine into the durable event ledger.
-- The rename path preserves existing IDs so terminal-order foreign keys and
-- operator history continue to refer to the same records.
DO $migration$
BEGIN
    IF to_regclass('public.event_quarantine') IS NULL THEN
        IF to_regclass('public.scoring_quarantine') IS NULL THEN
            RAISE EXCEPTION 'scoring_quarantine is required before migration 007';
        END IF;

        ALTER TABLE scoring_quarantine RENAME TO event_quarantine;
        ALTER TABLE scoring_quarantine_resubmissions
            RENAME TO event_quarantine_recovery_attempts;
        ALTER TABLE scoring_quarantine_audit RENAME TO event_quarantine_audit;

        ALTER INDEX idx_scoring_quarantine_pending
            RENAME TO idx_event_quarantine_pending;
        ALTER INDEX idx_quarantine_active_resubmission
            RENAME TO idx_event_quarantine_active_recovery;
        ALTER INDEX idx_quarantine_resubmissions_lookup
            RENAME TO idx_event_quarantine_recovery_lookup;
        ALTER INDEX idx_quarantine_audit_lookup
            RENAME TO idx_event_quarantine_audit_lookup;
    ELSIF to_regclass('public.scoring_quarantine') IS NOT NULL THEN
        -- Earlier migrations are intentionally replayable. After the first
        -- rename they recreate empty legacy tables on a later `make migrate`.
        -- Refuse to discard them if anything unexpectedly wrote to them.
        IF EXISTS (SELECT 1 FROM scoring_quarantine)
           OR EXISTS (SELECT 1 FROM scoring_quarantine_resubmissions)
           OR EXISTS (SELECT 1 FROM scoring_quarantine_audit) THEN
            RAISE EXCEPTION 'legacy scoring quarantine tables contain data';
        END IF;
        DROP TABLE scoring_quarantine_resubmissions;
        DROP TABLE scoring_quarantine_audit;
        DROP TABLE scoring_quarantine;
    END IF;
END
$migration$;

ALTER TABLE event_quarantine
    ADD COLUMN IF NOT EXISTS event_type TEXT,
    ADD COLUMN IF NOT EXISTS source_payload_sha256 BYTEA,
    ADD COLUMN IF NOT EXISTS source_payload_was_null BOOLEAN,
    ADD COLUMN IF NOT EXISTS reason_code TEXT,
    ADD COLUMN IF NOT EXISTS payload_purged_at TIMESTAMPTZ;

UPDATE event_quarantine
SET event_type = CASE
        WHEN source_topic = 'fraud-labels.v1' THEN 'label'
        ELSE 'transaction'
    END
WHERE event_type IS NULL;

UPDATE event_quarantine
SET source_payload_sha256 = sha256(COALESCE(kafka_payload, ''::bytea))
WHERE source_payload_sha256 IS NULL;

UPDATE event_quarantine
SET source_payload_was_null = (kafka_payload IS NULL)
WHERE source_payload_was_null IS NULL;

UPDATE event_quarantine
SET reason_code = CASE error_type
        WHEN 'OutOfOrderTransaction' THEN 'transaction.out_of_order'
        WHEN 'TerminalBlocked' THEN 'transaction.terminal_blocked'
        WHEN 'InvalidLabel' THEN 'label.invalid'
        ELSE 'transaction.invalid'
    END
WHERE reason_code IS NULL;

ALTER TABLE event_quarantine
    ALTER COLUMN event_type SET NOT NULL,
    ALTER COLUMN source_payload_sha256 SET NOT NULL,
    ALTER COLUMN source_payload_was_null SET NOT NULL,
    ALTER COLUMN reason_code SET NOT NULL;

DO $constraints$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'event_quarantine_event_type_check'
    ) THEN
        ALTER TABLE event_quarantine
            ADD CONSTRAINT event_quarantine_event_type_check
            CHECK (event_type IN ('transaction', 'label'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'event_quarantine_payload_hash_check'
    ) THEN
        ALTER TABLE event_quarantine
            ADD CONSTRAINT event_quarantine_payload_hash_check
            CHECK (octet_length(source_payload_sha256) = 32);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'event_quarantine_reason_code_check'
    ) THEN
        ALTER TABLE event_quarantine
            ADD CONSTRAINT event_quarantine_reason_code_check
            CHECK (reason_code ~ '^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$');
    END IF;
END
$constraints$;

ALTER TABLE event_quarantine_recovery_attempts
    ADD COLUMN IF NOT EXISTS lease_token TEXT,
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

UPDATE event_quarantine_recovery_attempts
SET lease_token = COALESCE(lease_token, 'legacy-' || id::text),
    lease_expires_at = COALESCE(lease_expires_at, updated_at)
WHERE lease_token IS NULL OR lease_expires_at IS NULL;

ALTER TABLE event_quarantine_recovery_attempts
    ALTER COLUMN lease_token SET NOT NULL,
    ALTER COLUMN lease_expires_at SET NOT NULL;

-- An expired, reconciled attempt may safely retry the exact same correction.
-- The original three-column UNIQUE constraint incorrectly prohibited that.
DO $unique_constraints$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'event_quarantine_recovery_attempts'::regclass
          AND contype = 'u'
    LOOP
        EXECUTE format(
            'ALTER TABLE event_quarantine_recovery_attempts DROP CONSTRAINT %I',
            constraint_name
        );
    END LOOP;
END
$unique_constraints$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_quarantine_recovery_lease
    ON event_quarantine_recovery_attempts (lease_token);

ALTER TABLE event_quarantine_recovery_attempts
    DROP CONSTRAINT IF EXISTS scoring_quarantine_resubmissions_state_check;
ALTER TABLE event_quarantine_recovery_attempts
    DROP CONSTRAINT IF EXISTS event_quarantine_recovery_state_check;
ALTER TABLE event_quarantine_recovery_attempts
    ADD CONSTRAINT event_quarantine_recovery_state_check
    CHECK (state IN ('sending', 'accepted', 'rejected', 'uncertain', 'retryable'));

ALTER TABLE event_quarantine_audit
    DROP CONSTRAINT IF EXISTS scoring_quarantine_audit_action_check;
ALTER TABLE event_quarantine_audit
    DROP CONSTRAINT IF EXISTS event_quarantine_audit_action_check;
ALTER TABLE event_quarantine_audit
    ADD CONSTRAINT event_quarantine_audit_action_check
    CHECK (action IN (
        'quarantined', 'ignored', 'resolved', 'recovery_started',
        'recovery_result', 'recovery_reconciled', 'payload_purged'
    ));

INSERT INTO event_quarantine_audit (
    quarantine_id, action, note, created_at
)
SELECT id, 'quarantined',
       'Existing quarantine record adopted by event ledger', created_at
FROM event_quarantine q
WHERE NOT EXISTS (
    SELECT 1 FROM event_quarantine_audit a
    WHERE a.quarantine_id = q.id AND a.action = 'quarantined'
);
