-- Preserve the exact runtime model identity with every decision. Historical
-- rows predate byte-level model verification, so mark them explicitly rather
-- than deriving a digest from the model reference.
ALTER TABLE fraud_decisions
    ADD COLUMN model_digest TEXT;

ALTER TABLE fraud_decision_versions
    ADD COLUMN model_digest TEXT;

ALTER TABLE terminal_reconciliation_runs
    ADD COLUMN model_digest TEXT;

UPDATE fraud_decisions
SET model_digest = 'legacy:unverified'
WHERE model_digest IS NULL;

UPDATE fraud_decision_versions
SET model_digest = 'legacy:unverified'
WHERE model_digest IS NULL;

UPDATE terminal_reconciliation_runs
SET model_digest = 'legacy:unverified'
WHERE model_digest IS NULL;

ALTER TABLE fraud_decisions
    ALTER COLUMN model_digest SET NOT NULL,
    ADD CONSTRAINT fraud_decisions_model_digest_format
        CHECK (
            model_digest = 'legacy:unverified'
            OR model_digest ~ '^sha256:[0-9a-f]{64}$'
        );

ALTER TABLE fraud_decision_versions
    ALTER COLUMN model_digest SET NOT NULL,
    ADD CONSTRAINT fraud_decision_versions_model_digest_format
        CHECK (
            model_digest = 'legacy:unverified'
            OR model_digest ~ '^sha256:[0-9a-f]{64}$'
        );

ALTER TABLE terminal_reconciliation_runs
    ALTER COLUMN model_digest SET NOT NULL,
    ADD CONSTRAINT terminal_reconciliation_runs_model_digest_format
        CHECK (
            model_digest = 'legacy:unverified'
            OR model_digest ~ '^sha256:[0-9a-f]{64}$'
        );

-- The original unnamed UNIQUE constraint is truncated to this identifier by
-- PostgreSQL's 63-byte identifier limit.
ALTER TABLE model_monitor_snapshots
    DROP CONSTRAINT model_monitor_snapshots_model_reference_window_start_window_key;

ALTER TABLE model_monitor_snapshots
    ADD CONSTRAINT model_monitor_snapshots_model_identity_window_key
        UNIQUE (
            model_reference,
            model_digest,
            feature_schema_version,
            window_start,
            window_end
        );

CREATE INDEX idx_fraud_decisions_model_identity_scored_at
    ON fraud_decisions (
        model_reference,
        model_digest,
        feature_schema_version,
        scored_at
    );
