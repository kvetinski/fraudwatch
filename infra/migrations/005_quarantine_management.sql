-- Operator audit and durable submission tracking for scoring quarantines.
-- Apply after 004_scoring_quarantine.sql.
CREATE TABLE IF NOT EXISTS scoring_quarantine_resubmissions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    quarantine_id BIGINT NOT NULL REFERENCES scoring_quarantine(id),
    transaction_id TEXT NOT NULL,
    submitted_payload_sha256 BYTEA NOT NULL,
    state TEXT NOT NULL DEFAULT 'sending'
        CHECK (state IN ('sending', 'accepted', 'rejected', 'uncertain')),
    http_status INTEGER,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (quarantine_id, transaction_id, submitted_payload_sha256)
);

-- Do not send a second replacement while the first attempt may be in flight.
CREATE UNIQUE INDEX IF NOT EXISTS idx_quarantine_active_resubmission
    ON scoring_quarantine_resubmissions(quarantine_id)
    WHERE state IN ('sending', 'accepted', 'uncertain');

CREATE INDEX IF NOT EXISTS idx_quarantine_resubmissions_lookup
    ON scoring_quarantine_resubmissions(quarantine_id, created_at DESC);

CREATE TABLE IF NOT EXISTS scoring_quarantine_audit (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    quarantine_id BIGINT NOT NULL REFERENCES scoring_quarantine(id),
    action TEXT NOT NULL CHECK (action IN ('ignored', 'resolved')),
    note TEXT NOT NULL,
    replacement_transaction_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_quarantine_audit_lookup
    ON scoring_quarantine_audit(quarantine_id, created_at DESC);
