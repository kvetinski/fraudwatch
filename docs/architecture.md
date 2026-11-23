# FraudWatch architecture

FraudWatch separates durable acceptance, asynchronous delivery, stream
processing, and operational telemetry. PostgreSQL is authoritative for
accepted transactions, outbox state, fraud decisions, terminal history,
quarantine, reconciliation, and model-monitoring snapshots. Kafka transports
work but is not the dead-letter source of truth.

## Runtime components

| Component | Responsibility | Durable boundary |
|---|---|---|
| Go ingestion | Validate HTTP transactions; write transaction and outbox rows; publish pending outbox records | PostgreSQL transaction, then Kafka acknowledgment plus outbox update |
| Scoring worker | Consume `transactions.v1`, construct point-in-time terminal features, score, quarantine invalid ordering, persist versions | PostgreSQL transaction before synchronous Kafka offset commit |
| Label worker | Consume delayed labels from `fraud-labels.v1`, activate or stage labels safely | PostgreSQL transaction before synchronous Kafka offset commit |
| Independent monitor | Read committed Kafka offsets and read-only PostgreSQL state | Metrics only; never authoritative |
| Model monitor | Evaluate delayed-label windows against the bundled reference distributions | Idempotent PostgreSQL snapshot |

The API returns `202 Accepted` after the transaction and outbox event are
durable. It does not promise that Kafka publication or scoring has completed.
Redelivery is expected because consumer offsets advance only after the database
effect commits. Transaction IDs, source hashes, decision uniqueness, and
version history make repeated delivery safe.

## Ordering and recovery

A terminal's features depend on its prior event history. An out-of-order event
is therefore quarantined and atomically blocks that terminal. Other terminals
continue. Recovery creates an immutable source snapshot, replays it through a
pinned model and schema, verifies independent feature parity and content
hashes, then atomically promotes the corrected decision projection. The old
decision versions remain auditable.

Infrastructure failures are retryable and leave the Kafka offset behind.
Deterministic contract failures are durably quarantined before acknowledgment.
Model/code failures fail closed and require investigation. PostgreSQL failure
removes ingestion readiness; Kafka failure leaves ingestion ready because the
outbox is the durable acceptance boundary, while backlog telemetry shows the
degradation.

## Deployment shape

Compose supports host-oriented development and observability. Kubernetes uses
Kustomize bases plus `local` and `ci` overlays:

1. Namespace and ignored/generated Secret.
2. PostgreSQL and single-node KRaft Kafka StatefulSets with persistent claims.
3. Checksum-verified migration Job.
4. Idempotent three-partition topic Job.
5. Ingestion, scoring, labels, independent monitor, and model-monitor
   Deployments.
6. Prometheus, Alertmanager, Grafana, Loki, Alloy, and kube-state-metrics.

Prometheus discovers every application pod directly, so scaling a consumer
does not hide replicas behind a Service. kube-state-metrics supplies deployment
availability and restart state. Alloy discovers FraudWatch pods and forwards
their container logs to Loki. Grafana provisions both data sources, the shared
application dashboards, and a Kubernetes workload dashboard.

Application pods run as UID/GID 10001 with read-only root filesystems, dropped
capabilities, RuntimeDefault seccomp, bounded temporary volumes, resource
requests/limits, and no service-account token. PostgreSQL and Kafka use their
image-native non-root IDs. Scoring and model-monitor rollouts use `Recreate` so
two model bundles do not process concurrently; parallel scoring replicas are
safe only when they use the same immutable image.

Exact image tags derive from runtime source inputs. Scoring and model-monitor
tags additionally include the serialized model and complete bundle digests.
The model, monitoring baseline, and manifest are embedded read-only rather than
mounted from a workstation path.
