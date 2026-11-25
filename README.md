# FraudWatch

FraudWatch is a local-development fraud-detection pipeline built around a Go
ingestion API, Kafka, PostgreSQL, Python scoring and label consumers, MLflow,
Prometheus, Grafana, Alertmanager, Loki, and Alloy. It accepts transactions through a durable
transactional outbox, scores them from the stream, stores decisions and
terminal history, and exposes operational metrics and alerts.

The repository targets a single-machine Linux environment. It includes
Compose development, a persistent Minikube deployment, an ephemeral Kind CI
deployment, and GitHub Actions gates. It is not a production deployment:
authentication, TLS, external secret management, and multi-node data-service
high availability remain outside the local scope.

## Architecture

```text
                                      +---------------------------+
                                      | monitor -> Prometheus     |
                                      | dashboards/alerts/logs    |
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

All containerized services emit structured JSON logs through Alloy to Loki;
Grafana queries both Prometheus metrics and Loki logs.

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
   committing its Kafka offset only after the database write. A label that
   arrives before its transaction is staged outside the active feature set;
   transaction scoring later activates or quarantines it atomically.

## Reliability behavior

- Identical API retries are accepted as duplicates; reusing a transaction ID
  with a different payload returns `409 Conflict`.
- Outbox publication can be retried. Consumer-side idempotency prevents an
  already stored transaction from creating a duplicate decision.
- Deterministically invalid transaction and label records are written to the
  durable `event_quarantine` ledger before their offsets are acknowledged.
  The ledger preserves Kafka coordinates, original bytes, a SHA-256 digest,
  stable reason code, status, and operator audit history. PostgreSQL is the
  dead-letter source of truth; there is no second Kafka dead-letter topic.
- Label and transaction workers acquire the same transaction-ID lock before a
  terminal lock. This closes the cross-topic arrival race, and all terminal
  history windows are calculated in one PostgreSQL statement and snapshot.
- A late event that would invalidate a terminal's feature history is
  quarantined and creates a durable terminal-order block in the same database
  transaction. Later events for that terminal are quarantined; unrelated
  terminals continue processing.
- Terminal-order blocks are deliberately fail-closed. Valid source events and
  every decision version are retained. An operator can unblock only after a
  pinned-model replay verifies source and candidate hashes plus independent
  feature parity, then atomically promotes the new projection.
- Infrastructure failures are classified separately from model/code failures.
  Both fail closed without acknowledging the current record; supervised workers
  can restart and retry transient failures, while model/code failures require
  investigation.
- `SIGINT` and `SIGTERM` stop polling for new work. A record already being
  processed finishes its PostgreSQL-plus-Kafka-commit boundary before the
  consumer closes; a record fetched after shutdown remains unacknowledged.
- Metrics are operational signals, not an audit log. PostgreSQL remains the
  source of truth.

## Event contracts

The versioned JSON Schemas in [`contracts/`](contracts/README.md) define the
authoritative transaction and delayed-label envelopes. A shared fixture corpus
is executed by both Go and Python tests to prevent ingress and stream validation
from drifting independently.

Transactions contain exactly `transaction_id`, `customer_id`, `terminal_id`,
`amount`, and `occurred_at`. Events reject unknown fields, missing or null
amounts, booleans, negative or nonfinite values, whitespace-only IDs, loose or
timezone-free timestamps, payloads over 64 KiB, and Kafka keys different from
`terminal_id`. Zero remains a valid amount, and accepted timestamps normalize
to UTC.

## Prerequisites

- Linux (the current monitoring overlay uses Docker host networking)
- Docker Engine with Compose v2
- Python 3.12 with `venv`
- Go 1.26 or a compatible version for the ingestion module
- GNU Make and Git
- Minikube and kubectl for the persistent local Kubernetes deployment
- Kind for reproducing the ephemeral CI deployment

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
bootstraps an MLflow candidate model. `make obs-install` remains available as a
small standalone metrics-client installation target; `make install` already
includes it through the streaming dependencies.

Python installation and container builds consume hash-verified locks under
`requirements/`. Change dependency ranges in `pyproject.toml`, run `make lock`,
review both lock-file diffs, and then run `make install`. Never hand-edit a lock.

Run the host processes in separate terminals:

```bash
make run-api
make run-scoring
make run-labels
```

Useful endpoints:

| Component | Local endpoint |
|---|---|
| Ingestion liveness | `http://127.0.0.1:8080/livez` |
| Ingestion readiness | `http://127.0.0.1:8080/readyz` |
| Ingestion metrics | `http://127.0.0.1:8080/metrics` |
| Scoring liveness/readiness | `http://127.0.0.1:9101/livez`, `/readyz` |
| Scoring metrics | `http://127.0.0.1:9101/metrics` |
| Label liveness/readiness | `http://127.0.0.1:9102/livez`, `/readyz` |
| Label metrics | `http://127.0.0.1:9102/metrics` |
| Independent monitor | `http://127.0.0.1:9103/metrics` |
| MLflow UI (`make mlflow-ui`) | `http://127.0.0.1:5001` |
| Prometheus | `http://127.0.0.1:9090` |
| Alertmanager | `http://127.0.0.1:9093` |
| Loki | `http://127.0.0.1:3100/ready` |
| Alloy | `http://127.0.0.1:12345` |
| Grafana | `http://127.0.0.1:3000` |

Run `make help` to see all supported commands. `make down` removes only the
Kafka and PostgreSQL containers without deleting their named volumes.

## Stopping and cleaning the local environment

The cleanup targets are deliberately layered so routine cleanup does not erase
datasets, models, or database state:

```bash
make stop-all   # stop every Compose service; keep containers and data
make down-all   # remove every project container/network; keep volumes and files
make clean      # remove Python caches plus build/test output
```

Processes started in a foreground terminal (`make run-api`, `make run-scoring`,
`make run-labels`, and `make mlflow-ui`) are not managed by Compose. Stop those
with Ctrl+C in their terminal.

A full reset requires an explicit confirmation:

```bash
make clean-all CONFIRM=clean-all
```

This stops and removes the complete Compose stack, deletes its named volumes,
and removes `.venv`, the downloaded dataset, exported models, MLflow state,
caches, and FraudWatch-built container images. It intentionally preserves
source files, Git metadata, `.env` files, and the private ignored `review/`
directory. Re-run `make setup` afterward.

## Ingestion API

Submit a transaction with all five required fields:

```bash
curl --include \
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

`make app-build` produces separate ingestion, scoring, label, model-monitor,
and operations images. Base images are digest-pinned; Python dependencies are
installed with `--require-hashes`; scoring/model-monitor images contain the
read-only model bundle and carry its digest as image metadata. `make
app-identity` shows the exact tags. A clean worktree uses the Git revision;
local changes add a deterministic dirty-source digest, and scoring tags also
include the model and complete bundle digests. The Compose services run as
non-root with a read-only root filesystem, all Linux capabilities dropped, no
privilege escalation, a
bounded temporary filesystem, and explicit CPU, memory, and PID limits.

Worker liveness only states that the process and HTTP server are running.
Readiness additionally requires PostgreSQL, a live Kafka partition assignment,
a fresh consume-loop heartbeat, and—on scoring—the loaded model. Readiness goes
false during rebalance, stalled polling, startup, and shutdown. Compose uses
`/readyz`; use `/livez` only for process-restart decisions.

Application image builds use the host network for dependency downloads. This
lets builds follow the Linux host's working DNS and VPN routes when Docker's
default build bridge cannot reach package repositories. This setting applies
only to Dockerfile build steps; running containers remain on the isolated
Compose network.

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

## Local Kubernetes deployment

Export a trusted candidate model first, then deploy the application,
PostgreSQL, three-partition single-node Kafka, and the observability stack into
the dedicated persistent Minikube profile named `fraudwatch`:

```bash
make app-export-model       # omit when artifacts/candidate-model already exists
make k8s-local-up
make k8s-local-status
make k8s-local-smoke
```

`k8s-local-up` creates ignored mode-0600 credentials on first use, starts only
the `fraudwatch` profile, builds and loads exact source/model-tagged images,
waits for the Kubernetes API, applies PostgreSQL and Kafka, runs the checksum
migration and topic Jobs, and then rolls out applications. Every mutating
command refuses to run unless the active context is exactly `fraudwatch`.
Application ConfigMaps and Secrets have content-addressed names, so a changed
reference creates a real pod revision. Normal deploys refuse changed Secret
values because PostgreSQL and Grafana credentials must first be rotated in
their persistent stores; the confirmed preserve-data procedure is documented
in [`docs/deployment.md`](docs/deployment.md#preserve-data-credential-rotation).
The smoke test checks liveness, readiness, and metrics, submits a unique
transaction, proves its outbox delivery and persisted fraud decision, verifies
Prometheus application and Kubernetes discovery, and finds the transaction's
structured ingestion log in Loki.

Forward the API in a foreground terminal when you want to call it manually:

```bash
make k8s-local-port-forward
# API: http://127.0.0.1:8080

make k8s-local-grafana
# Grafana: http://127.0.0.1:3000
```

Grafana's username is `admin`; on first initialization its generated password
is the `GRAFANA_ADMIN_PASSWORD` value in the ignored mode-0600 local environment
file. After a preserve-data rotation, keep that file synchronized with the
password stored by Grafana.
The Kubernetes stack provisions Prometheus, Alertmanager, Grafana, Loki,
Alloy, kube-state-metrics, the shared alert rules, and all five dashboards.

Real credentials live in `infra/k8s/fraudwatch.local.env`, which is ignored.
[`infra/k8s/fraudwatch.env.example`](infra/k8s/fraudwatch.env.example) documents
the required keys. Do not commit the generated file. Stop the local cluster
without losing its volumes using `make k8s-local-stop`. Deleting the profile
and its data requires `make k8s-local-delete CONFIRM=fraudwatch`.

CI creates `kind-fraudwatch-ci`, builds a small synthetic CI-only model bundle,
loads the same role images, applies the `ci` Kustomize overlay, and runs the
same smoke test. The real model bundle remains ignored and is never published
by CI. See [`docs/deployment.md`](docs/deployment.md) for deployment order,
rollout, rollback, and troubleshooting details.

## Observability and alerts

Start the local monitoring overlay after the scoring and label workers are
available:

```bash
make obs-up
make obs-check
```

Grafana provisions Prometheus and Loki data sources plus four dashboards:
**Services**, **Streaming**, **Reliability**, and **Model Quality**. Telemetry
covers ingress and outbox behavior, acknowledged worker outcomes and latency,
independent committed lag for both consumer groups, durable reliability state,
and delayed-label drift/quality metrics.

For a candidate exported before monitoring baselines were introduced, create
its reference bins once, then evaluate a window:

```bash
make model-monitor-baseline
make model-monitor-evaluate \
  MODEL_MONITOR_ARGS='--start 2026-11-17T00:00:00Z --end 2026-11-18T00:00:00Z'
```

New training runs log reference bins, validation metrics, dataset fingerprint,
schema version, and model identity to MLflow. New portable exports include the
baseline automatically. The containerized evaluator idempotently persists the
last complete delayed-label day every hour. Drift and quality alerts stay
gated until a window contains at least 1,000 samples and 20 positive labels;
the model dashboard separately exposes labeled count and label coverage.

Validate and load the Prometheus rules with:

```bash
make alerts-check
make alerts-test
make alerts-up
make alerts-status
```

The configuration and rule checks run in disposable, network-disabled
containers with read-only bind mounts. They do not start the stack or create
its application data volumes.

The rules cover service availability, stale monitoring, lag/stalls, commit and
error ratios, ingress/scoring latency, old durable backlog state, stale model
evaluation, drift, and quality regression. Alertmanager is connected but has
no external receiver credentials; alerts remain visible locally until an
approved notification destination is configured.

Prometheus, Alertmanager, Loki, Alloy, and Grafana bind to loopback using Linux
host networking. Alloy has read-only Docker-socket access to discover and tail
Compose logs; treat that socket access as privileged. The
one-shot `loki-init` service prepares the persistent storage directory for
Loki's non-root runtime user; its successful exited status is expected. The
default Grafana login is `admin` / `fraudwatch_local_only`; set
`FRAUDWATCH_GRAFANA_PASSWORD` before the first `make obs-up` for any persistent
local environment. Do not expose these services publicly. The metric contract
and incident triage order are documented in
[`docs/observability.md`](docs/observability.md).

## Quarantine operations

The operator CLI supports inspection, explicit ignore, leased transaction
resubmission, reconciliation, audited resolution, and payload retention:

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

.venv/bin/python scripts/quarantine.py reconcile 1

.venv/bin/python scripts/quarantine.py resolve 1 \
  --note 'replacement verified and scored'
```

Submission intent and a bounded lease are recorded before the HTTP request.
After a process crash or uncertain network result, wait for the lease to expire
and run `reconcile`; it checks the Go transaction, outbox, scored-transaction,
and decision records. A retry is allowed only when reconciliation proves that
none of those durable effects exist. Resolution requires a delivered outbox
event, matching payload hashes, and complete scoring state.

Pending records always retain their original payload. Purge raw payloads for
resolved or ignored records only after the minimum 30-day retention period:

```bash
.venv/bin/python scripts/quarantine.py purge --older-than-days 30
```

The source SHA-256 digest and every audit entry remain after payload purging.
`ignored` or `resolved` quarantine state does not remove a terminal-order block;
these are separate safety controls.

For a real local lifecycle smoke test, keep the API and scoring worker running:

```bash
FRAUDWATCH_INGEST_URL=http://127.0.0.1:8080/v1/transactions \
  .venv/bin/python scripts/smoke_quarantine_lifecycle.py
```

The smoke test intentionally leaves resolved quarantine and scored transaction
records as audit artifacts.

## Terminal reconciliation

Use the dedicated workflow only for a terminal blocked by an out-of-order
transaction. First freeze the complete transaction and label source under the
concrete model version currently resolved by the scoring configuration:

```bash
.venv/bin/python scripts/terminal_reconciliation.py preview TERMINAL_ID \
  --requested-by "$USER" \
  --note 'reviewed late-event quarantine'
```

Save the printed `run_id` and `source_event_sha256`. Replay creates new,
unpromoted decision versions in event-time and transaction-ID order. It checks
the online feature builder against an independent calculation before marking
the run verified:

```bash
.venv/bin/python scripts/terminal_reconciliation.py replay RUN_ID
.venv/bin/python scripts/terminal_reconciliation.py show RUN_ID
```

Review the run and then explicitly confirm the preview hash. Promotion rechecks
the source and candidate hashes in the same transaction that updates the
current decision projection, activates valid staged labels, resolves the linked
ordering quarantines, and removes the terminal block:

```bash
.venv/bin/python scripts/terminal_reconciliation.py promote RUN_ID \
  --expected-source-sha256 SOURCE_EVENT_SHA256
```

Any source change after preview or verification fails closed. Close the stale
run with an audited reason before creating a fresh preview; this does not
resolve quarantine or unblock the terminal:

```bash
.venv/bin/python scripts/terminal_reconciliation.py abandon RUN_ID \
  --requested-by "$USER" \
  --note 'source changed; replacing snapshot'
```

Original decisions remain in `fraud_decision_versions`; `fraud_decisions` is
only the current projection. Never resolve ordering quarantine manually as a
substitute for this workflow.

## Historical replay and evaluation

The replay runner requires an explicit, reusable run ID and pins the exact
dataset files, model bytes, feature schema, and Kafka topology in
`artifacts/replays/RUN_ID/manifest.json`. Export the candidate model once if the
portable bundle is not already present, then start both consumers.

Run a bounded direct-to-Kafka scoring-capacity sample:

```bash
make app-export-model
make benchmark-replay \
  REPLAY_RUN_ID=sample-kafka-v1 \
  REPLAY_MODE=kafka \
  REPLAY_ARGS='--days 16 --per-day 1000 --timeout 600 \
    --scoring-metrics-url http://127.0.0.1:9101/metrics'
```

For the complete HTTP-to-transactional-outbox-to-Kafka path, also start the Go
API and select HTTP mode. Events for the same terminal remain ordered while
different terminals are submitted concurrently:

```bash
make benchmark-replay \
  REPLAY_RUN_ID=sample-http-v1 \
  REPLAY_MODE=http \
  REPLAY_ARGS='--days 16 --per-day 1000 --http-concurrency 16 --timeout 600 \
    --scoring-metrics-url http://127.0.0.1:9101/metrics'
```

Rerun the identical command after an interruption. Completed daily checkpoints
are skipped; an incomplete day is published again and relies on the normal
idempotency guarantees. Changing the dataset, model, mode, sampling, or
topology under an existing run ID is rejected.

Pass one unique scoring metrics URL for every active member of the scoring
consumer group. The runner hashes the portable model artifact, verifies every
worker reports that same runtime digest, and records the identities at attempt
start, every daily checkpoint, and completion. Interrupted and failed stage
work is persisted immediately and remains part of the final throughput timing.
Replay manifest v2 intentionally rejects older manifests whose timing or
runtime-model evidence is incomplete; start those measurements again with a
new run ID.

The runner reads only one source day at a time and automatically adds seven
label-only timeline days, so every transaction receives its delayed label. A
run completes only when transaction/label counts match, every decision used the
pinned model and feature schema, the HTTP outbox is drained when applicable,
and there are no pending labels, quarantines, or blocked terminals.

For a capacity run, start exactly one scoring worker per Kafka partition in
separate terminals. Keep their metrics endpoints distinct; use the same pattern
for the label workers on ports 9301-9303:

```bash
FRAUDWATCH_MODEL_DIR=artifacts/candidate-model \
  FRAUDWATCH_SCORING_METRICS_PORT=9201 make run-scoring
FRAUDWATCH_MODEL_DIR=artifacts/candidate-model \
  FRAUDWATCH_SCORING_METRICS_PORT=9202 make run-scoring
FRAUDWATCH_MODEL_DIR=artifacts/candidate-model \
  FRAUDWATCH_SCORING_METRICS_PORT=9203 make run-scoring
```

The workers use one native math thread, prepared PostgreSQL statements, and a
bounded per-partition batch by default. Override the measured defaults with
`FRAUDWATCH_NATIVE_MATH_THREADS`,
`FRAUDWATCH_SCORING_BATCH_SIZE_PER_PARTITION`,
`FRAUDWATCH_LABEL_BATCH_SIZE_PER_PARTITION`, or
`FRAUDWATCH_DB_PREPARE_THRESHOLD`. More than three workers cannot increase
parallelism while the topics have three partitions. Do not run host and
container workers simultaneously when measuring a specific topology.

Run the complete April-through-September dataset without sampling:

```bash
make benchmark-replay \
  REPLAY_RUN_ID=phase312-full-v1 \
  REPLAY_MODE=kafka \
  REPLAY_ARGS='--days 183 --per-day 0 --timeout 900 \
    --scoring-metrics-url http://127.0.0.1:9201/metrics \
    --scoring-metrics-url http://127.0.0.1:9202/metrics \
    --scoring-metrics-url http://127.0.0.1:9203/metrics'
```

Keep the scoring workers alive after the replay so their latency histograms can
be collected. Then verify every stored feature vector and score against the
offline implementation, compute September holdout metrics, and compare the
same bounded sample from the one-, two-, and three-replica runs:

```bash
make evaluate-replay \
  EVALUATION_RUN_ID=phase312-full-v1 \
  EVALUATION_ARGS='\
    --scaling-manifest artifacts/replays/phase312-scale-1/manifest.json \
    --scaling-manifest artifacts/replays/phase312-scale-2/manifest.json \
    --scaling-manifest artifacts/replays/phase312-scale-3/manifest.json \
    --metrics-url http://127.0.0.1:9201/metrics \
    --metrics-url http://127.0.0.1:9202/metrics \
    --metrics-url http://127.0.0.1:9203/metrics'
```

The evaluator requires a completed, unsampled 183-day manifest plus exactly
one compatible scaling manifest for each of 1, 2, and 3 scoring replicas. It
reads one day at a time, fails on any feature/score/model/schema mismatch, and
writes machine-readable JSON plus a Markdown summary beside the replay
manifest. The reports include dataset/model identities, topology, exact
counts, workstation and database size, throughput, lag-drain time, processing
p95, calibration, average precision, ROC-AUC, Brier score,
precision/recall at 1%, and fraud scenario recall. After writing both reports,
the command exits nonzero if any required Phase 3.12 gate failed. Generated
replay evidence is ignored by Git and must be regenerated and retained outside
the repository before a release or handoff.

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
contracts/                      versioned JSON Schemas and shared contract fixtures
services/ingestion/             Go API and transactional outbox publisher
services/scoring/               Python scoring and delayed-label consumers
services/monitor/               independent read-only Kafka/PostgreSQL exporter
infra/compose.yaml              Kafka and PostgreSQL
infra/compose.app.yaml          local application-container overlay
infra/compose.observability.yaml local metrics, alerting, dashboards, and logs
infra/migrations/               ordered PostgreSQL schema files
infra/k8s/                      Kustomize bases, local/CI overlays, and lifecycle
infra/observability/            Prometheus/Grafana/Alertmanager/Loki/Alloy config
.github/workflows/              clean-checkout test and Kind E2E gates
scripts/                        setup, training, replay, verification, and operations
tests/                          Python unit and PostgreSQL integration tests
```

Local datasets, MLflow state, exported models, virtual environments, caches,
logs, and secrets are excluded from Git.

## Current limitations

The baseline is intentionally explicit about its remaining gaps:

- Terminal-order reconciliation is intentionally operator-driven and currently
  replays one terminal at a time in memory; bulk recovery scheduling is absent.
- Python dependencies are hash-locked, but automated dependency and base-image
  update pull requests are not configured yet.
- SQL migrations are checksum-verified and serialized by an advisory lock, but
  rollback migrations remain operator-authored rather than automatic.
- CI validates tests and an ephemeral Kind deployment, but immutable GHCR
  publication, provenance attestations, SBOM publication, and automated
  dependency updates are not configured yet.
- The local Kubernetes observability services are single-replica and use
  node-local persistent volumes; they are designed for development rather
  than highly available monitoring.
- Alertmanager has no external notification destination in the checked-in
  local configuration, and distributed tracing is intentionally deferred.
- Local credentials are development defaults; the services have no TLS,
  authentication, authorization, or external secret management.

These constraints make the current system suitable for controlled local
development and evaluation, not public or production traffic.
