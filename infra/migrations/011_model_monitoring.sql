CREATE TABLE IF NOT EXISTS model_monitor_snapshots (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    model_reference TEXT NOT NULL,
    model_digest TEXT NOT NULL,
    feature_schema_version TEXT NOT NULL,
    dataset_fingerprint TEXT NOT NULL,
    sample_count BIGINT NOT NULL CHECK (sample_count > 0),
    labeled_count BIGINT NOT NULL CHECK (labeled_count >= 0),
    positive_count BIGINT NOT NULL CHECK (positive_count >= 0),
    label_coverage DOUBLE PRECISION NOT NULL CHECK (label_coverage BETWEEN 0 AND 1),
    score_psi DOUBLE PRECISION NOT NULL CHECK (score_psi >= 0),
    feature_psi JSONB NOT NULL,
    fraud_prevalence DOUBLE PRECISION,
    average_precision DOUBLE PRECISION,
    roc_auc DOUBLE PRECISION,
    precision_at_1pct DOUBLE PRECISION,
    recall_at_1pct DOUBLE PRECISION,
    quality_degradation JSONB NOT NULL,
    alert_eligible BOOLEAN NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (window_end > window_start),
    UNIQUE (model_reference, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_model_monitor_snapshots_latest
    ON model_monitor_snapshots (evaluated_at DESC, id DESC);
