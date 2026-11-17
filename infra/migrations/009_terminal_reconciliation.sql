-- Immutable source events and append-only decision versions make terminal
-- ordering recovery reviewable. fraud_decisions remains the current projection.
CREATE TABLE IF NOT EXISTS valid_event_ledger (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type TEXT NOT NULL CHECK (event_type IN ('transaction', 'label')),
    transaction_id TEXT NOT NULL,
    terminal_id TEXT,
    occurred_at TIMESTAMPTZ,
    fraud_label SMALLINT CHECK (fraud_label IN (0, 1)),
    observed_at TIMESTAMPTZ,
    source_topic TEXT,
    source_partition INTEGER CHECK (source_partition >= 0),
    source_offset BIGINT CHECK (source_offset >= 0),
    kafka_key BYTEA,
    kafka_payload BYTEA,
    source_payload_sha256 BYTEA NOT NULL
        CHECK (octet_length(source_payload_sha256) = 32),
    legacy_imported BOOLEAN NOT NULL DEFAULT false,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_type, transaction_id),
    CHECK (
        (source_topic IS NULL
         AND source_partition IS NULL
         AND source_offset IS NULL)
        OR
        (source_topic IS NOT NULL
         AND source_partition IS NOT NULL
         AND source_offset IS NOT NULL)
    ),
    CHECK (
        (event_type = 'transaction'
         AND terminal_id IS NOT NULL
         AND occurred_at IS NOT NULL
         AND fraud_label IS NULL
         AND observed_at IS NULL)
        OR
        (event_type = 'label'
         AND occurred_at IS NULL
         AND fraud_label IS NOT NULL
         AND observed_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_valid_event_ledger_source
    ON valid_event_ledger (source_topic, source_partition, source_offset)
    WHERE source_topic IS NOT NULL AND legacy_imported IS FALSE;

CREATE INDEX IF NOT EXISTS idx_valid_event_ledger_terminal_time
    ON valid_event_ledger (terminal_id, occurred_at, transaction_id)
    WHERE event_type = 'transaction';

-- Preserve source events for already-scored transactions. Direct-to-Kafka
-- legacy rows may not have recoverable raw bytes; reconciliation fails closed
-- for those terminals until an authoritative payload is restored.
INSERT INTO valid_event_ledger (
    event_type, transaction_id, terminal_id, occurred_at,
    source_topic, source_partition, source_offset, kafka_key, kafka_payload,
    source_payload_sha256, legacy_imported, recorded_at
)
SELECT
    'transaction', st.transaction_id, st.terminal_id, st.occurred_at,
    'transactions.v1', fd.kafka_partition, fd.kafka_offset,
    convert_to(st.terminal_id, 'UTF8'),
    CASE
        WHEN tx.payload_hash = fd.source_payload_hash THEN tx.payload
        ELSE NULL
    END,
    fd.source_payload_hash, true, fd.scored_at
FROM scored_transactions AS st
JOIN fraud_decisions AS fd USING (transaction_id)
LEFT JOIN transactions AS tx USING (transaction_id)
ON CONFLICT (event_type, transaction_id) DO NOTHING;

-- Ordering-quarantined records passed the transaction contract and therefore
-- belong in the valid source ledger even though they were not scored.
WITH quarantine_events AS (
    SELECT q.*,
           convert_from(q.kafka_payload, 'UTF8')::jsonb AS event
    FROM event_quarantine AS q
    WHERE q.event_type = 'transaction'
      AND q.reason_code IN (
          'transaction.out_of_order',
          'transaction.terminal_blocked'
      )
      AND q.kafka_payload IS NOT NULL
)
INSERT INTO valid_event_ledger (
    event_type, transaction_id, terminal_id, occurred_at,
    source_topic, source_partition, source_offset, kafka_key, kafka_payload,
    source_payload_sha256, legacy_imported, recorded_at
)
SELECT
    'transaction', event ->> 'transaction_id', event ->> 'terminal_id',
    (event ->> 'occurred_at')::timestamptz,
    source_topic, source_partition, source_offset, kafka_key, kafka_payload,
    source_payload_sha256, true, created_at
FROM quarantine_events
ON CONFLICT (event_type, transaction_id) DO NOTHING;

-- Existing active labels are fully validated, so adopt a canonical payload.
WITH label_payloads AS (
    SELECT fl.*, st.terminal_id,
           convert_to(
               jsonb_build_object(
                   'transaction_id', fl.transaction_id,
                   'fraud_label', fl.fraud_label,
                   'observed_at', to_char(
                       fl.observed_at AT TIME ZONE 'UTC',
                       'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                   )
               )::text,
               'UTF8'
           ) AS payload
    FROM fraud_labels AS fl
    LEFT JOIN scored_transactions AS st USING (transaction_id)
)
INSERT INTO valid_event_ledger (
    event_type, transaction_id, terminal_id, fraud_label, observed_at,
    kafka_payload, source_payload_sha256, legacy_imported, recorded_at
)
SELECT
    'label', transaction_id, terminal_id, fraud_label, observed_at,
    payload, sha256(payload), true, received_at
FROM label_payloads
ON CONFLICT (event_type, transaction_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS terminal_reconciliation_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    terminal_id TEXT NOT NULL,
    first_quarantine_id BIGINT NOT NULL REFERENCES event_quarantine(id),
    status TEXT NOT NULL DEFAULT 'previewed'
        CHECK (status IN ('previewed', 'verified', 'promoted', 'abandoned')),
    model_reference TEXT NOT NULL,
    feature_schema_version TEXT NOT NULL,
    source_event_count INTEGER NOT NULL CHECK (source_event_count > 0),
    source_event_sha256 BYTEA NOT NULL
        CHECK (octet_length(source_event_sha256) = 32),
    candidate_count INTEGER,
    candidate_sha256 BYTEA,
    parity_verified BOOLEAN NOT NULL DEFAULT false,
    requested_by TEXT NOT NULL,
    note TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    verified_at TIMESTAMPTZ,
    promoted_at TIMESTAMPTZ,
    abandoned_at TIMESTAMPTZ,
    CHECK (
        (candidate_sha256 IS NULL AND candidate_count IS NULL)
        OR
        (candidate_sha256 IS NOT NULL
         AND octet_length(candidate_sha256) = 32
         AND candidate_count IS NOT NULL
         AND candidate_count > 0)
    )
);

-- Keep this migration replayable after adding lifecycle states during local
-- development: an already-created table receives the current constraint.
ALTER TABLE terminal_reconciliation_runs
    ADD COLUMN IF NOT EXISTS abandoned_at TIMESTAMPTZ;
ALTER TABLE terminal_reconciliation_runs
    DROP CONSTRAINT IF EXISTS terminal_reconciliation_runs_status_check;
ALTER TABLE terminal_reconciliation_runs
    ADD CONSTRAINT terminal_reconciliation_runs_status_check
    CHECK (status IN ('previewed', 'verified', 'promoted', 'abandoned'));

CREATE INDEX IF NOT EXISTS idx_terminal_reconciliation_runs_terminal
    ON terminal_reconciliation_runs (terminal_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_terminal_reconciliation_active
    ON terminal_reconciliation_runs (terminal_id)
    WHERE status IN ('previewed', 'verified');

CREATE TABLE IF NOT EXISTS fraud_decision_versions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    decision_version INTEGER NOT NULL CHECK (decision_version > 0),
    event_ledger_id BIGINT NOT NULL REFERENCES valid_event_ledger(id),
    terminal_id TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    fraud_score DOUBLE PRECISION NOT NULL
        CHECK (fraud_score >= 0 AND fraud_score <= 1),
    model_reference TEXT NOT NULL,
    feature_schema_version TEXT NOT NULL,
    feature_vector JSONB NOT NULL,
    source_payload_hash BYTEA NOT NULL
        CHECK (octet_length(source_payload_hash) = 32),
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    origin TEXT NOT NULL CHECK (origin IN ('legacy', 'stream', 'reconciliation')),
    reconciliation_run_id BIGINT REFERENCES terminal_reconciliation_runs(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    promoted_at TIMESTAMPTZ,
    UNIQUE (transaction_id, decision_version),
    CHECK (
        (origin = 'reconciliation' AND reconciliation_run_id IS NOT NULL)
        OR
        (origin <> 'reconciliation' AND reconciliation_run_id IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_fraud_decision_versions_terminal
    ON fraud_decision_versions (terminal_id, occurred_at, transaction_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fraud_decision_versions_run_event
    ON fraud_decision_versions (reconciliation_run_id, transaction_id)
    WHERE reconciliation_run_id IS NOT NULL;

ALTER TABLE fraud_decisions
    ADD COLUMN IF NOT EXISTS decision_version INTEGER NOT NULL DEFAULT 1;

INSERT INTO fraud_decision_versions (
    transaction_id, decision_version, event_ledger_id,
    terminal_id, occurred_at, fraud_score, model_reference,
    feature_schema_version, feature_vector, source_payload_hash,
    kafka_partition, kafka_offset, origin, created_at, promoted_at
)
SELECT
    fd.transaction_id, fd.decision_version, ledger.id,
    st.terminal_id, st.occurred_at, fd.fraud_score, fd.model_reference,
    fd.feature_schema_version, fd.feature_vector, fd.source_payload_hash,
    fd.kafka_partition, fd.kafka_offset, 'legacy', fd.scored_at, fd.scored_at
FROM fraud_decisions AS fd
JOIN scored_transactions AS st USING (transaction_id)
LEFT JOIN valid_event_ledger AS ledger
    ON ledger.event_type = 'transaction'
   AND ledger.transaction_id = fd.transaction_id
ON CONFLICT (transaction_id, decision_version) DO NOTHING;

ALTER TABLE fraud_decision_versions
    ALTER COLUMN event_ledger_id SET NOT NULL;

CREATE TABLE IF NOT EXISTS valid_event_quarantine_links (
    event_ledger_id BIGINT PRIMARY KEY REFERENCES valid_event_ledger(id),
    quarantine_id BIGINT UNIQUE NOT NULL
        REFERENCES event_quarantine(id) ON DELETE CASCADE,
    linked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO valid_event_quarantine_links (event_ledger_id, quarantine_id)
SELECT ledger.id, q.id
FROM event_quarantine AS q
JOIN valid_event_ledger AS ledger
  ON ledger.source_topic = q.source_topic
 AND ledger.source_partition = q.source_partition
 AND ledger.source_offset = q.source_offset
WHERE q.event_type = 'transaction'
  AND q.reason_code IN (
      'transaction.out_of_order',
      'transaction.terminal_blocked'
  )
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS terminal_reconciliation_audit (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reconciliation_run_id BIGINT NOT NULL
        REFERENCES terminal_reconciliation_runs(id),
    terminal_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN (
        'previewed', 'replayed', 'verified', 'promoted', 'unblocked',
        'abandoned'
    )),
    note JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE terminal_reconciliation_audit
    DROP CONSTRAINT IF EXISTS terminal_reconciliation_audit_action_check;
ALTER TABLE terminal_reconciliation_audit
    ADD CONSTRAINT terminal_reconciliation_audit_action_check
    CHECK (action IN (
        'previewed', 'replayed', 'verified', 'promoted', 'unblocked',
        'abandoned'
    ));

CREATE INDEX IF NOT EXISTS idx_terminal_reconciliation_audit_run
    ON terminal_reconciliation_audit (reconciliation_run_id, id);
