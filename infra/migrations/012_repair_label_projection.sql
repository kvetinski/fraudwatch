-- Reconciliation promotions could previously activate fraud_labels without
-- updating the denormalized fields read by online terminal-history features.
-- Repair any affected rows idempotently; future promotions write both tables.
UPDATE scored_transactions AS st
SET fraud_label = fl.fraud_label,
    label_observed_at = fl.observed_at
FROM fraud_labels AS fl
WHERE fl.transaction_id = st.transaction_id
  AND (
      st.fraud_label IS DISTINCT FROM fl.fraud_label
      OR st.label_observed_at IS DISTINCT FROM fl.observed_at
  );
