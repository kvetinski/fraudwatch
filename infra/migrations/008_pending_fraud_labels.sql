-- Labels can arrive on their Kafka topic before the matching transaction has
-- been scored. Keep those labels out of the active feature set until the
-- transaction worker validates the seven-day delay under shared locks.
CREATE TABLE IF NOT EXISTS pending_fraud_labels (
    transaction_id TEXT PRIMARY KEY,
    fraud_label SMALLINT NOT NULL
        CHECK (fraud_label IN (0, 1)),
    observed_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_topic TEXT,
    source_partition INTEGER,
    source_offset BIGINT,
    kafka_key BYTEA,
    kafka_payload BYTEA,
    source_payload_sha256 BYTEA,
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
        source_payload_sha256 IS NULL
        OR octet_length(source_payload_sha256) = 32
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_fraud_labels_source
    ON pending_fraud_labels (
        source_topic, source_partition, source_offset
    )
    WHERE source_topic IS NOT NULL;

-- Adopt labels staged by the pre-008 application. Their original Kafka
-- coordinates were not retained, so they remain nullable and fail closed if
-- a later transaction proves that such a legacy label violates the delay.
INSERT INTO pending_fraud_labels (
    transaction_id, fraud_label, observed_at, received_at
)
SELECT fl.transaction_id, fl.fraud_label, fl.observed_at, fl.received_at
FROM fraud_labels AS fl
WHERE NOT EXISTS (
    SELECT 1
    FROM scored_transactions AS st
    WHERE st.transaction_id = fl.transaction_id
)
ON CONFLICT (transaction_id) DO NOTHING;

DELETE FROM fraud_labels AS fl
WHERE NOT EXISTS (
    SELECT 1
    FROM scored_transactions AS st
    WHERE st.transaction_id = fl.transaction_id
);
