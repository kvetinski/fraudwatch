-- Migration 013 preserved honest byte identities while retaining historical
-- rows as legacy:unverified. Tighten that compatibility marker so it cannot be
-- used for new data, and close active runs whose preview did not bind bytes.

-- A run previewed before byte identities existed cannot be replayed or
-- promoted honestly: its candidate hash did not bind the model bytes. Close
-- active legacy runs with an audit record so operators can create a fresh
-- preview under a verified bundle without weakening the safety checks.
WITH abandoned AS (
    UPDATE terminal_reconciliation_runs
    SET status = 'abandoned', abandoned_at = now()
    WHERE model_digest = 'legacy:unverified'
      AND status IN ('previewed', 'verified')
    RETURNING id, terminal_id
)
INSERT INTO terminal_reconciliation_audit (
    reconciliation_run_id, terminal_id, action, note
)
SELECT
    id,
    terminal_id,
    'abandoned',
    jsonb_build_object(
        'requested_by', 'migration:014_enforce_verified_model_identity',
        'reason',
        'active run predates verified model-byte identities; re-preview required'
    )
FROM abandoned;

-- Monitoring baselines created before portable export could contain a run ID
-- rather than a digest. Preserve those snapshots honestly without pretending
-- that the original model bytes can now be reconstructed.
UPDATE model_monitor_snapshots
SET model_digest = 'legacy:unverified'
WHERE model_digest !~ '^sha256:[0-9a-f]{64}$';

-- Keep the legacy marker only for rows backfilled by migration 013. New
-- decisions and reconciliation runs must always carry a verified byte
-- identity. Updating an unrelated field on a historical row remains allowed.
CREATE FUNCTION reject_new_unverified_model_digest()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.model_digest = 'legacy:unverified'
       AND (
           TG_OP = 'INSERT'
           OR NEW.model_digest IS DISTINCT FROM OLD.model_digest
       ) THEN
        RAISE EXCEPTION
            'legacy:unverified is reserved for rows predating migration 013'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER fraud_decisions_verified_model_digest
BEFORE INSERT OR UPDATE ON fraud_decisions
FOR EACH ROW EXECUTE FUNCTION reject_new_unverified_model_digest();

CREATE TRIGGER fraud_decision_versions_verified_model_digest
BEFORE INSERT OR UPDATE ON fraud_decision_versions
FOR EACH ROW EXECUTE FUNCTION reject_new_unverified_model_digest();

CREATE TRIGGER terminal_reconciliation_runs_verified_model_digest
BEFORE INSERT OR UPDATE ON terminal_reconciliation_runs
FOR EACH ROW EXECUTE FUNCTION reject_new_unverified_model_digest();

CREATE TRIGGER model_monitor_snapshots_verified_model_digest
BEFORE INSERT OR UPDATE ON model_monitor_snapshots
FOR EACH ROW EXECUTE FUNCTION reject_new_unverified_model_digest();

ALTER TABLE model_monitor_snapshots
    ADD CONSTRAINT model_monitor_snapshots_model_digest_format
        CHECK (
            model_digest = 'legacy:unverified'
            OR model_digest ~ '^sha256:[0-9a-f]{64}$'
        );
