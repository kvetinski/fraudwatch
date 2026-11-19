-- Keep the active label beside its scored transaction so the per-event feature
-- query remains one indexed table scan. fraud_labels stays the canonical label
-- ledger; both projections are written atomically by the label consumers.
UPDATE scored_transactions AS st
SET fraud_label = fl.fraud_label,
    label_observed_at = fl.observed_at
FROM fraud_labels AS fl
WHERE fl.transaction_id = st.transaction_id
  AND (
      st.fraud_label IS DISTINCT FROM fl.fraud_label
      OR st.label_observed_at IS DISTINCT FROM fl.observed_at
  );
