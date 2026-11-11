-- Once an out-of-order transaction is discovered, scoring for its terminal
-- is suspended until an explicit, audited history reconciliation exists.
-- This migration intentionally has NO automatic unblock mechanism.
CREATE TABLE IF NOT EXISTS terminal_order_blocks (
    terminal_id TEXT PRIMARY KEY,
    first_quarantine_id BIGINT NOT NULL REFERENCES scoring_quarantine(id),
    last_scored_at TIMESTAMPTZ NOT NULL,
    late_occurred_at TIMESTAMPTZ NOT NULL,
    blocked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT late_before_latest CHECK (late_occurred_at < last_scored_at)
);

CREATE INDEX IF NOT EXISTS idx_terminal_order_blocks_quarantine
    ON terminal_order_blocks (first_quarantine_id);
