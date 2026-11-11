-- Invalid Kafka events are persisted before their offsets are committed.
-- This is a durable quarantine, not an automatic retry policy.
CREATE TABLE IF NOT EXISTS scoring_quarantine (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_topic TEXT NOT NULL,
    source_partition INTEGER NOT NULL CHECK (source_partition >= 0),
    source_offset BIGINT NOT NULL CHECK (source_offset >= 0),
    kafka_key BYTEA,
    kafka_payload BYTEA,
    error_type TEXT NOT NULL,
    error_message TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'resolved', 'ignored')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    UNIQUE (source_topic, source_partition, source_offset)
);

CREATE INDEX IF NOT EXISTS idx_scoring_quarantine_pending
    ON scoring_quarantine (created_at)
    WHERE status = 'pending';
