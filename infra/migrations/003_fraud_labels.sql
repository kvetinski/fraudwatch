CREATE TABLE IF NOT EXISTS fraud_labels (
    transaction_id TEXT PRIMARY KEY,
    fraud_label SMALLINT NOT NULL
        CHECK (fraud_label IN (0, 1)),
    observed_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
