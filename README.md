# FraudWatch

FraudWatch is a local-development fraud-detection pipeline built around a Go
ingestion API, Kafka, PostgreSQL, Python scoring and label consumers, MLflow,
Prometheus, and Grafana. It accepts transactions through a durable
transactional outbox, scores them from the stream, stores decisions and
terminal history, and exposes operational metrics and alerts.

The repository currently targets a single-machine Linux environment. It has
useful reliability controls and container definitions, but it is not yet a
production deployment: CI/CD, Kubernetes manifests, authentication, TLS,
secret management, and automated model-drift monitoring are still to be added.

## Architecture

```text
                                      +---------------------------+
                                      | Prometheus -> Grafana     |
                                      | metrics + local alerts    |
                                      +-------------^-------------+
                                                    |
client -> Go ingestion API -> PostgreSQL outbox -> Kafka transactions.v1
                                  |                     |
                                  |                     v
                                  |             Python scoring worker
                                  |                     |
                                  +---------------------v
                                                PostgreSQL
                                      decisions, history, quarantine,
                                           terminal-order blocks

historical replay -----------------------------> Kafka fraud-labels.v1
                                                         |
                                                         v
                                                Python label worker
                                                         |
                                                         v
                                                    PostgreSQL
```

The main processing path is:

1. `POST /v1/transactions` validates a transaction and stores both the source
   record and an outbox event in one PostgreSQL transaction.
2. The Go outbox publisher sends pending events to `transactions.v1` and marks
   them published only after Kafka acknowledges them.
3. The scoring consumer loads one concrete MLflow model version, builds
   point-in-time terminal features, and persists the decision and history in a
   PostgreSQL transaction.
4. The consumer synchronously commits the Kafka offset only after PostgreSQL
   commits. Database state is authoritative; Kafka delivery is at least once.
5. A separate consumer persists delayed labels from `fraud-labels.v1`, also
   committing its Kafka offset only after the database write.

## Reliability behavior

- Identical API retries are accepted as duplicates; reusing a transaction ID
  with a different payload returns `409 Conflict`.
- Outbox publication can be retried. Consumer-side idempotency prevents an
  already stored transaction from creating a duplicate decision.
- Deterministically malformed transaction messages are written to the durable
  `scoring_quarantine` table before their offsets are acknowledged.
- A late event that would invalidate a terminal's feature history is
  quarantined and creates a durable terminal-order block in the same database
  transaction. Later events for that terminal are quarantined; unrelated
  terminals continue processing.
- Terminal-order blocks are deliberately fail-closed. There is no automatic
  unblock until reviewed historical reconciliation exists.
- Infrastructure, model, feature, and database errors remain fatal to the
  scoring process and leave the current Kafka offset uncommitted.
- Metrics are operational signals, not an audit log. PostgreSQL remains the
  source of truth.

## Prerequisites

- Linux (the current monitoring overlay uses Docker host networking)
- Docker Engine with Compose v2
- Python 3.12 with `venv`
- Go 1.26 or a compatible version for the ingestion module
- GNU Make and Git

The setup process clones the simulated dataset from the Fraud Detection
Handbook repository and downloads Python, Go, and container dependencies.

## Quick start

From the repository root:

```bash
make setup
make obs-install
make test
```

`make setup` creates `.venv`, installs the project, downloads the local dataset,
starts Kafka and PostgreSQL, applies migrations, creates both Kafka topics, and
bootstraps an MLflow candidate model. `make obs-install` explicitly installs
the Prometheus Python client used by both workers.

Run the host processes in separate terminals:

```bash
make run-api
make run-scoring
make run-labels
```

Useful endpoints:

| Component | Local endpoint |
|---|---|
| Ingestion health | `http://127.0.0.1:8080/healthz` |
| Scoring metrics | `http://127.0.0.1:9101/metrics` |
| Label metrics | `http://127.0.0.1:9102/metrics` |
| MLflow UI (`make mlflow-ui`) | `http://127.0.0.1:5001` |
| Prometheus | `http://127.0.0.1:9090` |
| Grafana | `http://127.0.0.1:3000` |

Run `make help` to see all supported commands. `make down` stops Kafka and
PostgreSQL without deleting their named volumes.

## Ingestion API

Submit a transaction with all five required fields:

```bash
curl --fail-with-body \
  -H 'Content-Type: application/json' \
  -d '{
    "transaction_id": "demo-0001",
    "customer_id": "customer-42",
    "terminal_id": "terminal-7",
    "amount": 19.95,
    "occurred_at": "2026-11-11T12:00:00Z"
  }' \
  http://127.0.0.1:8080/v1/transactions
```

A valid new transaction returns `202 Accepted`. Amounts must be finite and
non-negative, so zero is valid. IDs must be nonempty, timestamps must be RFC
3339, unknown fields are rejected, and request bodies are limited to 64 KiB.

The API only promises durable acceptance into PostgreSQL and the outbox. A
`202` response does not mean the scoring consumer has already produced a fraud
decision.

## Local application containers

The repository can run the Go API and both Python workers in containers while
reusing the Kafka and PostgreSQL volumes from `infra/compose.yaml`.

```bash
make migrate
make topics
make app-export-model
make app-config
make app-build
make app-up
make app-check
```

Stop any host-run API or workers first so ports `8080`, `9101`, and `9102` are
free. The exported candidate model under `artifacts/candidate-model/` is a
generated, read-only deployment bundle and is intentionally excluded from Git.
Re-export it whenever the MLflow candidate changes.

Inspect or stop only the application containers with:

```bash
make app-status
make app-logs
make app-stop
```

`app-stop` preserves Kafka, PostgreSQL, monitoring services, and all named
volumes. Compose overlays must retain this order because relative paths are
resolved from the first file:

```bash
docker compose -f infra/compose.yaml -f infra/compose.app.yaml config
```

## Observability and alerts

Start the local monitoring overlay after the scoring and label workers are
available:

```bash
make obs-up
make obs-check
```

Grafana provisions a Prometheus data source and the **FraudWatch — Streaming
Operations** dashboard automatically. The current metrics cover:

- scoring and label throughput;
- scoring stage latency histograms;
- committed scoring-consumer lag by partition;
- worker processing and Kafka-commit errors;
- pending quarantine and blocked-terminal counts; and
- committed model-score distributions.

Validate and load the Prometheus rules with:

```bash
make alerts-check
make alerts-test
make alerts-up
make alerts-status
```

The rules cover worker availability, scoring lag and stalls, commit and
processing failures, pending quarantine, blocked terminals, and high scoring
latency. Their thresholds are local-development defaults, and Prometheus rule
evaluation does not send notifications because Alertmanager is not configured.

Prometheus and Grafana bind to loopback using Linux host networking. The
default Grafana login is `admin` / `fraudwatch_local_only`; set
`FRAUDWATCH_GRAFANA_PASSWORD` before the first `make obs-up` for any persistent
local environment. Do not expose these services publicly.

## Quarantine operations

The operator CLI supports inspection, explicit ignore, corrected resubmission,
and audited resolution:

```bash
.venv/bin/python scripts/quarantine.py list
.venv/bin/python scripts/quarantine.py list --status all --limit 50
.venv/bin/python scripts/quarantine.py show 1
.venv/bin/python scripts/quarantine.py show 1 --payload
.venv/bin/python scripts/quarantine.py ignore 1 \
  --reason 'unrecoverable malformed source record'
```

Only resubmit a correction reconstructed from an authoritative source. Send it
through the ingestion API and outbox, never directly to Kafka:

```bash
.venv/bin/python scripts/quarantine.py resubmit 1 \
  --file corrected.json \
  --url http://127.0.0.1:8080/v1/transactions

.venv/bin/python scripts/quarantine.py resolve 1 \
  --note 'replacement verified and scored'
```

Submission intent is recorded before the HTTP request. Network failures are not
retried automatically because the API may already have accepted the event.
Resolution is allowed only after the matching replacement is present in the
required PostgreSQL records. `ignored` or `resolved` quarantine state does not
remove a terminal-order block; these are separate safety controls.

For a real local lifecycle smoke test, keep the API and scoring worker running:

```bash
FRAUDWATCH_INGEST_URL=http://127.0.0.1:8080/v1/transactions \
  .venv/bin/python scripts/smoke_quarantine_lifecycle.py
```

The smoke test intentionally leaves resolved quarantine and scored transaction
records as audit artifacts.

## Historical replay and evaluation

With both workers active, publish a sampled historical stream and wait for its
transactions and labels to be processed:

```bash
.venv/bin/python scripts/replay_dataset.py \
  --days 16 --per-day 1000 --timeout 600
```

Use the replay ID printed by the command:

```bash
.venv/bin/python scripts/verify_replay.py \
  --run-id REPLAY_ID --days 16 --per-day 1000
```

Do not run host and container scoring workers simultaneously when validating a
single-worker replay. Consumers in the same Kafka group will divide partitions,
which changes the intended verification setup.

The current replay is a deterministic daily sample, not a full-dataset streaming
evaluation. Its verifier also pins a concrete model version in source. Treat
full historical evaluation, repeatable performance baselines, and model-version
parameterization as pending work.

## Tests

```bash
make test          # Python and Go, including database integration and Go race checks
make test-python
make test-go
```

Both test targets start the local infrastructure, apply all numbered migrations
to `fraudwatch` and `fraudwatch_test`, and pass the integration database URL.
For a fast Python-only check without database integration:

```bash
.venv/bin/python -m pytest tests -q
```

Database-dependent cases are skipped when `TEST_DATABASE_URL` is not set.

## Repository layout

```text
fraudwatch_ml/                  shared feature, scoring, label, and reliability code
services/ingestion/             Go API and transactional outbox publisher
services/scoring/               Python scoring and delayed-label consumers
infra/compose.yaml              Kafka and PostgreSQL
infra/compose.app.yaml          local application-container overlay
infra/compose.observability.yaml Prometheus and Grafana overlay
infra/migrations/               ordered PostgreSQL schema files
infra/observability/            scrape config, dashboard, rules, and rule tests
scripts/                        setup, training, replay, verification, and operations
tests/                          Python unit and PostgreSQL integration tests
```

Local datasets, MLflow state, exported models, virtual environments, caches,
logs, and secrets are excluded from Git.

## Current limitations

The baseline is intentionally explicit about its remaining gaps:

- The quarantine CLI and lifecycle smoke script still default to the obsolete
  `/transactions` route. Pass `/v1/transactions` explicitly as shown above.
- Go ingress and Python stream validation are not yet defined by one shared
  contract, so their treatment of fields and malformed payloads can drift.
- A poison label record currently terminates the label worker; it has no durable
  label dead-letter path yet.
- Terminal-order recovery, historical reconciliation, and audited unblock are
  manual and incomplete.
- Consumer concurrency, sustained throughput, backpressure, crash recovery, and
  full-dataset replay have not been benchmarked systematically.
- Monitoring does not yet cover Go ingress, label-consumer lag, Kafka/PostgreSQL
  internals, end-to-end latency, true model drift, or notification delivery.
- Python dependencies have ranges but no resolved lock file, and the Prometheus
  client is installed separately instead of being declared in the package
  streaming extras.
- SQL migrations are replayable files but have no migration ledger or checksum
  enforcement.
- There is no CI pipeline, image publication workflow, or local Kubernetes
  deployment yet.
- Local credentials are development defaults; the services have no TLS,
  authentication, authorization, or external secret management.

These constraints make the current system suitable for controlled local
development and evaluation, not public or production traffic.
