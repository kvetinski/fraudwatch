CREATE TABLE IF NOT EXISTS scored_transactions (
    transaction_id TEXT PRIMARY KEY,
    terminal_id TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    fraud_label SMALLINT
        CHECK (fraud_label IN (0, 1)),
    label_observed_at TIMESTAMPTZ,
    CHECK (
        (fraud_label IS NULL) =
        (label_observed_at IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_scored_terminal_time
ON scored_transactions (terminal_id, occurred_at);

CREATE TABLE IF NOT EXISTS fraud_decisions (
    transaction_id TEXT PRIMARY KEY,
    fraud_score DOUBLE PRECISION NOT NULL
        CHECK (fraud_score >= 0 AND fraud_score <= 1),
    model_reference TEXT NOT NULL,
    feature_schema_version TEXT NOT NULL,
    feature_vector JSONB NOT NULL,
    source_payload_hash BYTEA NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    scored_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
