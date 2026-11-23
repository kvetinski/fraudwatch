# Kubernetes deployment and operations

## Prerequisites and identities

Use kubectl, Minikube, Kind, Docker Engine with Compose v2, Python 3.12, and GNU
Make. Docker Engine 24 or newer is recommended. Docker 20.10 on newer Linux
kernels can wedge published localhost ports after large image imports or node
restarts even when the API server is healthy inside the node; upgrade Docker
if Minikube SSH or Kind API TLS accepts TCP but never completes its handshake.

`scripts/image_identity.py` derives the application revision from only the
inputs copied into images. Deployment and documentation edits therefore do not
retag unchanged runtime images. Scoring/model-monitor identities also include
the model tree digest. `make app-identity` prints the exact values.

## Persistent Minikube lifecycle

```bash
make app-export-model
make k8s-local-secrets
make k8s-local-up
make k8s-local-status
make k8s-local-smoke
```

The first command is unnecessary if a trusted immutable bundle already exists.
`k8s-local-secrets` refuses to overwrite an existing file. The deployment
orchestrator validates required keys, rejects example placeholders, creates the
Secret through stdin without logging values, and checks that the read-only URL
enables `default_transaction_read_only`.

All mutations specify and verify context `fraudwatch`. Services stay internal
ClusterIPs; use `make k8s-local-port-forward` rather than a NodePort. Stop and
resume with `make k8s-local-stop` and `make k8s-local-start`. Delete all local
cluster volumes only with explicit confirmation:

```bash
make k8s-local-delete CONFIRM=fraudwatch
```

## Ephemeral Kind lifecycle

```bash
make k8s-ci-up K8S_CI_SECRET_ENV=/path/to/ignored-ci.env
make k8s-ci-smoke
make k8s-ci-delete
```

Kind uses the pinned node image in `infra/k8s/Makefile.inc` and context
`kind-fraudwatch-ci`. The GitHub workflow generates both its random Secret and
a deterministic-data synthetic model fixture in the runner workspace. That
fixture tests serialization, model load, image construction, readiness, and
inference without treating it as a release model.

## Migrations and rollout

Numbered SQL files run under a PostgreSQL advisory lock. Applied versions and
SHA-256 checksums are recorded in `schema_migrations`; changing an applied file
fails deployment. Add a new numbered migration instead. Rollback SQL remains
operator-authored because schema/data reversal cannot be inferred safely.

The deployment order is platform, migration Job, topic Job, applications, then
observability. The final layer installs Prometheus, Alertmanager, Grafana,
Loki, Alloy, and kube-state-metrics with exact digest-pinned images. The
orchestrator deletes and recreates only the two fixed-name Jobs so reruns are
idempotent. It does not automatically replace StatefulSets or PVCs when an
immutable storage field changes; such changes require an explicit backup and
operator-approved migration.

Application updates use exact local tags and `imagePullPolicy: IfNotPresent`.
For a code-only rollback, retain the prior image and rendered manifest, then
reapply that exact image identity. For a scoring rollback, roll back scoring
and model-monitor together because both tags bind source, model, and baseline.
Never use a mutable tag or manually replace the model inside a running pod.

## Verification and troubleshooting

`make k8s-local-smoke` opens temporary loopback port-forwards, checks readiness
and metrics for ingestion/scoring/labels/monitor, submits a unique transaction,
and polls PostgreSQL until the transaction, delivered outbox row, and fraud
decision exist together. It also verifies healthy application targets and
Kubernetes workload metrics in Prometheus, Grafana's database health, and the
matching structured ingestion log in Loki.

To inspect dashboards and logs interactively:

```bash
make k8s-local-grafana
# open http://127.0.0.1:3000
```

Sign in as `admin` with `GRAFANA_ADMIN_PASSWORD` from the ignored
`infra/k8s/fraudwatch.local.env`. Prometheus and Loki are pre-provisioned data
sources. Use the **Kubernetes** dashboard for replica, restart, and crash-loop
state, and Grafana Explore for pod logs.

Useful inspection commands:

```bash
make k8s-local-status
kubectl --context fraudwatch -n fraudwatch get events --sort-by=.lastTimestamp
kubectl --context fraudwatch -n fraudwatch logs deployment/scoring --tail=200
kubectl --context fraudwatch -n fraudwatch logs job/fraudwatch-migrate
```

If a worker is live but unready, check its model/database flags, Kafka
assignment count, and processing-loop heartbeat. If Kafka cannot resolve
`kafka-0.kafka`, ensure the headless Service retains
`publishNotReadyAddresses: true`; the controller address must resolve during
bootstrap. If an old kubectl emits client-skew warnings, install a client within
one supported minor-version window of the pinned cluster.
