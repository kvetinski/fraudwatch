
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    payload_hash BYTEA NOT NULL,
    payload BYTEA NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox_events (
    transaction_id TEXT PRIMARY KEY
        REFERENCES transactions(transaction_id),
    topic TEXT NOT NULL,
    kafka_key TEXT NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ,
    kafka_partition INTEGER,
    kafka_offset BIGINT
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending
ON outbox_events (created_at, transaction_id)
WHERE delivered_at IS NULL;
