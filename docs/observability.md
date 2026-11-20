# Observability contract and runbook

FraudWatch separates process telemetry from authoritative system state.
Scoring and label workers report only work performed by their own process. The
independent monitor queries committed Kafka group offsets and read-only
PostgreSQL snapshots, so lag and backlog state remain fresh when a worker
stops, crashes, or rebalances.

## Metric semantics

- `fraudwatch_ingestion_requests_total` is recorded after a final API outcome.
  `accepted` and `duplicate` mean the PostgreSQL transaction and outbox row are
  durable, not that scoring has completed.
- `fraudwatch_ingestion_outbox_published_events_total` advances only after
  Kafka acknowledgment and the outbox delivery update commit together.
- Ingestion readiness depends on PostgreSQL, which is the API's durable
  acceptance boundary. Kafka outages keep the API ready while outbox failure,
  age, and backlog telemetry show degraded downstream publication.
- `fraudwatch_scoring_events_total` and `fraudwatch_labels_events_total`
  advance only after their durable database effect and synchronous Kafka
  offset commit. Their `total` latency includes new, duplicate, and quarantined
  records.
- `fraudwatch_scoring_fraud_score` observes each newly persisted decision once.
  It follows the durable database decision, not redelivery or commit attempts.
- `fraudwatch_kafka_consumer_group_lag` is authoritative log-end minus
  committed-next-offset. Worker assignment and last poll/ack gauges diagnose a
  replica but are not backlog truth.
- `fraudwatch_model_monitor_snapshot_present` distinguishes a genuinely fresh
  zero-age snapshot from a system that has never produced one. Snapshot sample,
  labeled, positive, and coverage gauges describe the same persisted window.
- Ages and durations are seconds; timestamp gauges are Unix time. Transaction,
  customer, and terminal IDs and monetary values are never metric labels.

The monitor connects with PostgreSQL `default_transaction_read_only=on`.
Model evaluation is a separate writer restricted by code to upserting
`model_monitor_snapshots`.

## Model monitoring

Each new training run logs quantile reference bins, validation metrics, dataset
fingerprint, schema version, and model identity. Export recalculates the
portable model SHA-256 digest and writes the baseline beside the model.

The evaluator persists score PSI, PSI for every feature, sample and positive
counts, label coverage, fraud prevalence, AP, ROC-AUC, precision@1%,
recall@1%, and relative quality degradation. Initial thresholds are:

- PSI warning above `0.20`;
- PSI critical above `0.30`;
- quality degradation above `20%`; and
- no drift or quality alert below 1,000 window samples or 20 delayed-label
  positives. The dashboard also shows the labeled sample count and coverage so
  operators can judge the strength of quality estimates above that minimum.

These are starting thresholds informed by the Phase 3.12 full replay. Re-tune
them after representative production windows accumulate.

## Local operation

```bash
make obs-config
make obs-up
make obs-check
make alerts-check
make alerts-test
make alerts-status
```

`obs-config`, `alerts-check`, and `alerts-test` use disposable,
network-disabled validator containers and read-only configuration mounts. They
do not create or attach the FraudWatch application data volumes.

Use Grafana Explore with the `Loki` data source for structured container logs.
Useful labels are `service_name`, `container`, and `environment="local"`.
Application JSON contains timestamp, severity, service, build revision,
message, and contextual outcome/source fields. Raw payloads and monetary
values must never be logged.

## Triage order

1. Check **Services** and `make alerts-status` for availability failures.
2. Check **Streaming** for group/partition lag, assignments, rebalances, queue
   age, last acknowledgment, and commit failures.
3. For old outbox records, verify Kafka connectivity and outbox errors before
   restarting. Accepted rows remain durable in PostgreSQL.
4. For rising consumer lag, distinguish database/model failures from a stopped
   or rebalancing worker. A failed commit leaves the offset behind the durable
   effect; redelivery is expected and idempotent.
5. For quarantine or terminal blocks, use `scripts/quarantine.py` and terminal
   reconciliation. Never delete durable records to make a gauge disappear.
6. Treat drift as actionable only when the evidence gate is one. Review feature
   PSI, score PSI, label coverage, and quality together before retraining.

Prometheus and Loki retain seven days locally and all endpoints bind to
loopback. Alertmanager uses a local no-op receiver by design; adding a real
destination requires environment-specific credentials.

On first startup, the one-shot `loki-init` service creates Loki's storage
directories with ownership for its non-root UID. A successful exited init
container is expected in `make obs-status` and does not indicate a failure.
