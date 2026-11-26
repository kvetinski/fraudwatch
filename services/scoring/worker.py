"""FraudWatch Kafka transaction scoring worker.

Consumes transactions.v1, persists idempotent decisions and terminal-history
records in PostgreSQL transactions, then synchronously commits Kafka offsets.
Reports rolling mean/p50/p95 stage latency for newly scored transactions.

Run from the FraudWatch repository after installing its editable Python package:
    python services/scoring/worker.py
"""

import hashlib
import json
import logging
import math
import os
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter_ns
from typing import Any

# Keep every scoring replica predictable.  Native BLAS/OpenMP fan-out is
# counterproductive for one-row HGB inference and otherwise multiplies by the
# number of Kafka consumers.
NATIVE_MATH_THREADS = int(os.getenv("FRAUDWATCH_NATIVE_MATH_THREADS", "1"))
if NATIVE_MATH_THREADS < 1:
    raise ValueError("FRAUDWATCH_NATIVE_MATH_THREADS must be positive")
for _thread_variable in (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ[_thread_variable] = str(NATIVE_MATH_THREADS)

import mlflow.sklearn
import numpy as np
import pandas as pd
import psycopg
from confluent_kafka import Consumer, KafkaException, TopicPartition
from psycopg.types.json import Jsonb

from fraudwatch_ml.event_ledger import record_transaction_event
from fraudwatch_ml.features import FEATURE_NAMES
from fraudwatch_ml.labels import resolve_pending_label
from fraudwatch_ml.locks import lock_terminal, lock_transaction
from fraudwatch_ml.model_integrity import (
    SHA256_PATTERN,
    verify_portable_model_digest,
)
from fraudwatch_ml.postgres_features import PostgresTerminalHistory
from fraudwatch_ml.profiling import EventTimings, RollingProfiler
from fraudwatch_ml.quarantine import InvalidTransaction, store_quarantined
from fraudwatch_ml.observability import ScoringMetrics, WorkerHealth, start_worker_metrics
from fraudwatch_ml.json_logging import configure_json_logging
from fraudwatch_ml.terminal_order import (
    TerminalQuarantined, block_for_late_event, quarantine_blocked_terminal,
)
from fraudwatch_ml.scoring import (
    FEATURE_SCHEMA_VERSION,
    make_feature_vector,
)
from fraudwatch_ml.transactions import parse_transaction, validate_transaction_key
from fraudwatch_ml.worker_runtime import (
    ShutdownController,
    classify_failure,
    install_shutdown_handlers,
)


ROOT = Path(__file__).resolve().parents[2]
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgres://fraudwatch:fraudwatch_dev@"
    "127.0.0.1:5434/fraudwatch?sslmode=disable",
)
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions.v1")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fraudwatch-scoring-v1")
MODEL_NAME = os.getenv("MODEL_NAME", "fraudwatch-hgb")
STATS_EVERY = int(os.getenv("STATS_EVERY", "1000"))
PROFILE_WINDOW = int(os.getenv("PROFILE_WINDOW", "10000"))
BATCH_SIZE_PER_PARTITION = int(
    os.getenv("FRAUDWATCH_SCORING_BATCH_SIZE_PER_PARTITION", "8")
)
BATCH_WAIT_SECONDS = float(
    os.getenv("FRAUDWATCH_SCORING_BATCH_WAIT_SECONDS", "0.025")
)
DB_PREPARE_THRESHOLD = int(os.getenv("FRAUDWATCH_DB_PREPARE_THRESHOLD", "1"))

logger = logging.getLogger("fraudwatch.scoring")


@dataclass(frozen=True)
class LoadedScorer:
    model: Any
    model_reference: str
    model_digest: str
    array_inference: bool = False

    def __post_init__(self) -> None:
        if SHA256_PATTERN.fullmatch(self.model_digest) is None:
            raise ValueError("Loaded scorer requires a verified SHA-256 model digest")

    def predict_probability(self, values: np.ndarray) -> float:
        """Score one schema-ordered row using the model's supported input type.

        Persisted production models are validated once and then use the
        allocation-light ndarray path.  DataFrame input remains available for
        custom scorers used by reconciliation and tests.
        """
        if values.shape != (len(FEATURE_NAMES),):
            raise ValueError("Unexpected feature schema")
        if self.array_inference:
            model_input = values.reshape(1, -1)
        else:
            model_input = pd.DataFrame([values], columns=list(FEATURE_NAMES))
        return float(self.model.predict_proba(model_input)[0, 1])


def prepare_model_for_array_inference(model: Any) -> Any:
    """Validate named training schema once, then use allocation-light arrays.

    HistGradientBoosting uses feature positions after input validation.  Once
    the persisted feature names are checked here, dropping only that validation
    metadata prevents a warning on every ndarray prediction.  It does not alter
    learned parameters or prediction math.
    """
    if getattr(model, "n_features_in_", None) != len(FEATURE_NAMES):
        raise RuntimeError("Model feature count differs from the scoring schema")
    names = getattr(model, "feature_names_in_", None)
    if names is not None:
        if list(names) != list(FEATURE_NAMES):
            raise RuntimeError("Model feature order differs from the scoring schema")
        delattr(model, "feature_names_in_")
    return model


def load_scorer() -> LoadedScorer:
    """Load one verified portable bundle before Kafka is initialized."""
    bundle = Path(
        os.getenv("FRAUDWATCH_MODEL_DIR", ROOT / "artifacts/candidate-model")
    )
    manifest_path = bundle / "manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError(
            f"Verified portable model bundle is missing at {bundle}; "
            "run make app-export-model"
        )
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("model_name") != MODEL_NAME:
        raise RuntimeError("Exported model name differs from configured MODEL_NAME")
    if manifest.get("feature_names") != list(FEATURE_NAMES):
        raise RuntimeError(
            "Exported model feature order differs from the scoring schema"
        )
    if manifest.get("feature_schema_version") != FEATURE_SCHEMA_VERSION:
        raise RuntimeError(
            "Exported model feature schema version is unsupported"
        )
    model_digest = verify_portable_model_digest(bundle, manifest)
    model = prepare_model_for_array_inference(
        mlflow.sklearn.load_model(str(bundle / "model"))
    )
    model_reference = manifest["model_reference"]
    logger.info(
        "Loaded portable model: %s", model_reference,
        extra={
            "model_reference": model_reference,
            "model_digest": model_digest,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
        },
    )
    return LoadedScorer(
        model=model,
        model_reference=model_reference,
        model_digest=model_digest,
        array_inference=True,
    )


class MeasuredHistory:
    """Time only the history lookup performed by make_feature_vector()."""

    def __init__(self, base: PostgresTerminalHistory, timings: EventTimings):
        self.base = base
        self.timings = timings

    def features(self, terminal_id: str, at: pd.Timestamp):
        t0 = perf_counter_ns()
        try:
            return self.base.features(terminal_id, at)
        finally:
            self.timings.add("history_lookup", perf_counter_ns() - t0)


def process_event(
    conn: psycopg.Connection,
    scorer: LoadedScorer,
    payload: bytes,
    partition: int,
    offset: int,
    timings: EventTimings | None = None,
    *,
    source_topic: str = KAFKA_TOPIC,
    kafka_key: bytes | None = None,
    metrics: ScoringMetrics | None = None,
    batch_transaction: bool = False,
    committed_scores: list[float] | None = None,
) -> bool | TerminalQuarantined:
    """Persist score and historical state atomically; offsets committed by caller.

    Stage timings deliberately separate history lookup from the surrounding
    feature assembly. write_commit includes leaving the PG transaction
    context (COMMIT). Duplicate messages are handled idempotently, but are not
    mixed into the profile of newly scored transactions.
    """
    if timings is None:
        timings = EventTimings()

    t0 = perf_counter_ns()
    event, occurred_at = parse_transaction(payload)
    validate_transaction_key(event, kafka_key)
    payload_hash = hashlib.sha256(payload).digest()
    timings.add("parse", perf_counter_ns() - t0)

    t_precheck = perf_counter_ns()
    transaction = nullcontext() if batch_transaction else conn.transaction()
    with transaction:
        # All workers use this order: transaction identity, then terminal.
        # It closes the label/transaction race without introducing a lock cycle.
        if not batch_transaction:
            lock_transaction(conn, event.transaction_id)
            lock_terminal(conn, event.terminal_id)

        event_ledger_id = record_transaction_event(
            conn,
            transaction_id=event.transaction_id,
            terminal_id=event.terminal_id,
            occurred_at=occurred_at.to_pydatetime(),
            source_topic=source_topic,
            source_partition=partition,
            source_offset=offset,
            kafka_key=kafka_key,
            kafka_payload=payload,
            source_payload_hash=payload_hash,
        )

        existing = conn.execute(
            """
            SELECT source_payload_hash
            FROM fraud_decisions
            WHERE transaction_id = %s
            """,
            (event.transaction_id,),
        ).fetchone()

        if existing is not None:
            if bytes(existing[0]) != payload_hash:
                raise InvalidTransaction(
                    f"Conflicting payload for {event.transaction_id}",
                    reason_code="transaction.conflict",
                )
            return False

        # Duplicate decisions are checked before the block so an earlier
        # committed event is still idempotent on Kafka redelivery.
        block = conn.execute(
            "SELECT 1 FROM terminal_order_blocks WHERE terminal_id = %s",
            (event.terminal_id,),
        ).fetchone()
        if block is not None:
            return quarantine_blocked_terminal(
                conn, terminal_id=event.terminal_id,
                event_ledger_id=event_ledger_id,
                source_topic=source_topic, source_partition=partition,
                source_offset=offset, kafka_key=kafka_key,
                kafka_payload=payload,
            )

        latest = conn.execute(
            """
            SELECT max(occurred_at)
            FROM scored_transactions
            WHERE terminal_id = %s
            """,
            (event.terminal_id,),
        ).fetchone()[0]

        if latest is not None and occurred_at < pd.Timestamp(latest):
            return block_for_late_event(
                conn, terminal_id=event.terminal_id,
                event_ledger_id=event_ledger_id,
                occurred_at=occurred_at.to_pydatetime(),
                last_scored_at=pd.Timestamp(latest).to_pydatetime(),
                source_topic=source_topic, source_partition=partition,
                source_offset=offset, kafka_key=kafka_key,
                kafka_payload=payload,
            )

        timings.add("prechecks", perf_counter_ns() - t_precheck)

        measured_history = MeasuredHistory(PostgresTerminalHistory(conn), timings)
        t_features = perf_counter_ns()
        values = make_feature_vector(event, measured_history)
        if values.shape != (len(FEATURE_NAMES),):
            raise ValueError("Unexpected feature schema")
        feature_vector = {
            name: float(value)
            for name, value in zip(FEATURE_NAMES, values)
        }
        feature_elapsed = perf_counter_ns() - t_features
        history_elapsed = timings.durations_ns.get("history_lookup", 0)
        timings.add("feature_build", max(0, feature_elapsed - history_elapsed))

        t_infer = perf_counter_ns()
        score = scorer.predict_probability(values)
        if not math.isfinite(score) or not 0.0 <= score <= 1.0:
            raise ValueError(f"Invalid model score: {score}")
        timings.add("inference", perf_counter_ns() - t_infer)

        t_write = perf_counter_ns()
        write_result = conn.execute(
            """
            WITH version_insert AS (
                INSERT INTO fraud_decision_versions (
                    transaction_id, decision_version, event_ledger_id,
                    terminal_id, occurred_at, fraud_score, model_reference,
                    model_digest,
                    feature_schema_version, feature_vector,
                    source_payload_hash, kafka_partition, kafka_offset,
                    origin, promoted_at
                ) VALUES (
                    %s, 1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    'stream', now()
                )
                RETURNING transaction_id
            ), decision_insert AS (
                INSERT INTO fraud_decisions (
                    transaction_id, fraud_score, model_reference, model_digest,
                    feature_schema_version, feature_vector,
                    source_payload_hash, kafka_partition, kafka_offset,
                    decision_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                RETURNING transaction_id
            ), scored_insert AS (
                INSERT INTO scored_transactions (
                    transaction_id, terminal_id, occurred_at
                ) VALUES (%s, %s, %s)
                RETURNING transaction_id
            )
            SELECT
                EXISTS (
                    SELECT 1 FROM pending_fraud_labels
                    WHERE transaction_id = %s
                ),
                (SELECT count(*) FROM version_insert),
                (SELECT count(*) FROM decision_insert),
                (SELECT count(*) FROM scored_insert)
            """,
            (
                event.transaction_id,
                event_ledger_id,
                event.terminal_id,
                occurred_at.to_pydatetime(),
                score,
                scorer.model_reference,
                scorer.model_digest,
                FEATURE_SCHEMA_VERSION,
                Jsonb(feature_vector),
                payload_hash,
                partition,
                offset,
                event.transaction_id,
                score,
                scorer.model_reference,
                scorer.model_digest,
                FEATURE_SCHEMA_VERSION,
                Jsonb(feature_vector),
                payload_hash,
                partition,
                offset,
                event.transaction_id,
                event.terminal_id,
                occurred_at.to_pydatetime(),
                event.transaction_id,
            ),
        ).fetchone()
        has_pending_label, version_count, decision_count, scored_count = write_result
        if (version_count, decision_count, scored_count) != (1, 1, 1):
            raise RuntimeError("Incomplete atomic scoring write")
        pending_label = None
        if has_pending_label:
            pending_label = resolve_pending_label(
                conn,
                transaction_id=event.transaction_id,
                terminal_id=event.terminal_id,
                occurred_at=occurred_at.to_pydatetime(),
            )
        if pending_label is not None:
            logger.info(
                "Pending label %s: transaction_id=%s",
                pending_label,
                event.transaction_id,
            )

    # The PG transaction has committed successfully when this point is reached.
    timings.add("write_commit", perf_counter_ns() - t_write)
    if committed_scores is not None:
        committed_scores.append(score)
    elif metrics is not None:
        metrics.score.observe(score)  # Score was committed to PostgreSQL.
    return True


def process_and_commit(conn, scorer, consumer, msg, timings=None, metrics=None):
    """Process or quarantine, then acknowledge Kafka; return event + timing data.

    On any failure during SQL, model inference, out-of-order handling, or Kafka
    commit, raise rather than advancing the Kafka offset.  Retried poison records
    are deduplicated by their (topic, partition, offset) coordinates.
    """
    if timings is None:
        timings = EventTimings()

    t0 = perf_counter_ns()
    quarantined = False
    try:
        payload = msg.value()
        if payload is None:
            raise InvalidTransaction(
                "Kafka tombstone is not a transaction",
                reason_code="transaction.tombstone",
            )
        outcome = process_event(
            conn, scorer, payload, msg.partition(), msg.offset(),
            timings=timings, source_topic=msg.topic(), kafka_key=msg.key(),
            metrics=metrics,
        )
        if isinstance(outcome, TerminalQuarantined):
            logger.warning(
                "Terminal order quarantine: terminal=%s partition=%s "
                "offset=%s new_record=%s reason=%s",
                outcome.terminal_id, msg.partition(), msg.offset(),
                outcome.new_record, outcome.reason,
                extra={
                    "source_topic": msg.topic(),
                    "source_partition": msg.partition(),
                    "source_offset": msg.offset(),
                    "outcome": "quarantined",
                    "reason": outcome.reason,
                    "duration_ms": (perf_counter_ns() - t0) / 1_000_000,
                },
            )
            quarantined = True
            new_decision = False
        else:
            new_decision = outcome
    except InvalidTransaction as exc:
        # Process-event's PostgreSQL transaction has rolled back before reaching
        # this handler. An independent durable quarantine transaction follows.
        inserted = store_quarantined(
            conn,
            source_topic=msg.topic(),
            source_partition=msg.partition(),
            source_offset=msg.offset(),
            kafka_key=msg.key(),
            kafka_payload=msg.value(),
            error=exc,
        )
        logger.warning(
            "Quarantined record topic=%s partition=%s offset=%s "
            "new_record=%s reason=%s",
            msg.topic(), msg.partition(), msg.offset(), inserted, exc,
            extra={
                "source_topic": msg.topic(),
                "source_partition": msg.partition(),
                "source_offset": msg.offset(),
                "outcome": "quarantined",
                "reason": exc.reason_code,
                "duration_ms": (perf_counter_ns() - t0) / 1_000_000,
            },
        )
        quarantined = True
        new_decision = False

    t1 = perf_counter_ns()
    # Neither a successful score nor quarantine is acknowledged until after
    # PostgreSQL confirms a durable commit.
    try:
        committed = consumer.commit(message=msg, asynchronous=False)
        if not committed:
            raise RuntimeError("Kafka offset commit returned no partitions")
        errors = [tp.error for tp in committed if tp.error is not None]
        if errors:
            raise RuntimeError(f"Kafka offset commit failed: {errors}")
    except Exception:
        if metrics is not None:
            metrics.commits.labels(result="failure").inc()
        raise
    t2 = perf_counter_ns()
    if metrics is not None:
        metrics.commits.labels(result="success").inc()

    return new_decision, quarantined, t1 - t0, t2 - t1


@dataclass
class DurableMessageResult:
    """One database-durable result awaiting its partition offset commit."""

    message: Any
    new_decision: bool
    quarantined: bool
    timings: EventTimings
    started_ns: int
    durable_ns: int


def process_without_kafka_commit(
    conn,
    scorer,
    msg,
    *,
    metrics: ScoringMetrics | None = None,
    batch_transaction: bool = False,
    committed_scores: list[float] | None = None,
) -> DurableMessageResult:
    """Persist one message, leaving its Kafka offset for a batch commit."""
    started_ns = perf_counter_ns()
    timings = EventTimings()
    quarantined = False
    try:
        payload = msg.value()
        if payload is None:
            raise InvalidTransaction(
                "Kafka tombstone is not a transaction",
                reason_code="transaction.tombstone",
            )
        outcome = process_event(
            conn,
            scorer,
            payload,
            msg.partition(),
            msg.offset(),
            timings=timings,
            source_topic=msg.topic(),
            kafka_key=msg.key(),
            metrics=metrics,
            batch_transaction=batch_transaction,
            committed_scores=committed_scores,
        )
        if isinstance(outcome, TerminalQuarantined):
            logger.warning(
                "Terminal order quarantine: terminal=%s partition=%s "
                "offset=%s new_record=%s reason=%s",
                outcome.terminal_id,
                msg.partition(),
                msg.offset(),
                outcome.new_record,
                outcome.reason,
                extra={
                    "source_topic": msg.topic(),
                    "source_partition": msg.partition(),
                    "source_offset": msg.offset(),
                    "outcome": "quarantined",
                    "reason": outcome.reason,
                    "duration_ms": (perf_counter_ns() - started_ns) / 1_000_000,
                },
            )
            quarantined = True
            new_decision = False
        else:
            new_decision = bool(outcome)
    except InvalidTransaction as exc:
        inserted = store_quarantined(
            conn,
            source_topic=msg.topic(),
            source_partition=msg.partition(),
            source_offset=msg.offset(),
            kafka_key=msg.key(),
            kafka_payload=msg.value(),
            error=exc,
        )
        logger.warning(
            "Quarantined record topic=%s partition=%s offset=%s "
            "new_record=%s reason=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            inserted,
            exc,
            extra={
                "source_topic": msg.topic(),
                "source_partition": msg.partition(),
                "source_offset": msg.offset(),
                "outcome": "quarantined",
                "reason": exc.reason_code,
                "duration_ms": (perf_counter_ns() - started_ns) / 1_000_000,
            },
        )
        quarantined = True
        new_decision = False
    return DurableMessageResult(
        message=msg,
        new_decision=new_decision,
        quarantined=quarantined,
        timings=timings,
        started_ns=started_ns,
        durable_ns=perf_counter_ns(),
    )


def partition_batches(messages: list[Any]) -> list[list[Any]]:
    """Return deterministic, offset-ordered batches for each topic partition."""
    grouped: dict[tuple[str, int], list[Any]] = {}
    for message in messages:
        grouped.setdefault((message.topic(), message.partition()), []).append(message)
    return [
        sorted(grouped[key], key=lambda message: message.offset())
        for key in sorted(grouped)
    ]


def persist_scoring_partition_batch(
    conn,
    scorer: LoadedScorer,
    messages: list[Any],
    *,
    metrics: ScoringMetrics | None = None,
) -> list[DurableMessageResult]:
    """Persist a valid ordered partition batch at one PostgreSQL boundary.

    Contract-invalid input uses the general record-at-a-time path so poison
    events retain isolated quarantine transactions.  Valid batches acquire
    every transaction lock before every terminal lock, each set in sorted
    order, which prevents cross-partition lock cycles.
    """
    identities = []
    try:
        for message in messages:
            if message.value() is None:
                return [
                    process_without_kafka_commit(conn, scorer, item, metrics=metrics)
                    for item in messages
                ]
            event, _ = parse_transaction(message.value())
            validate_transaction_key(event, message.key())
            identities.append((event.transaction_id, event.terminal_id))
    except InvalidTransaction:
        return [
            process_without_kafka_commit(conn, scorer, item, metrics=metrics)
            for item in messages
        ]

    transaction_ids = [item[0] for item in identities]
    if len(set(transaction_ids)) != len(transaction_ids):
        return [
            process_without_kafka_commit(conn, scorer, item, metrics=metrics)
            for item in messages
        ]
    terminal_ids = sorted({item[1] for item in identities})
    committed_scores: list[float] = []
    with conn.transaction():
        conn.execute(
            """
            SELECT pg_advisory_xact_lock(
                hashtextextended('fraudwatch:transaction:' || locked.id, 0)
            )
            FROM unnest(%s::text[]) AS locked(id)
            ORDER BY locked.id
            """,
            (sorted(transaction_ids),),
        ).fetchall()
        conn.execute(
            """
            SELECT pg_advisory_xact_lock(
                hashtextextended('fraudwatch:terminal:' || locked.id, 0)
            )
            FROM unnest(%s::text[]) AS locked(id)
            ORDER BY locked.id
            """,
            (terminal_ids,),
        ).fetchall()
        results = [
            process_without_kafka_commit(
                conn,
                scorer,
                message,
                metrics=metrics,
                batch_transaction=True,
                committed_scores=committed_scores,
            )
            for message in messages
        ]
    durable_ns = perf_counter_ns()
    for result in results:
        result.durable_ns = durable_ns
    if metrics is not None:
        for score in committed_scores:
            metrics.score.observe(score)
    return results


def commit_partition_batch(consumer, results: list[DurableMessageResult]):
    """Commit only after every delivered offset in this partition is durable."""
    if not results:
        return []
    last = results[-1].message
    offsets = [TopicPartition(last.topic(), last.partition(), last.offset() + 1)]
    committed = consumer.commit(offsets=offsets, asynchronous=False)
    if not committed:
        raise RuntimeError("Kafka batch offset commit returned no partitions")
    errors = [partition.error for partition in committed if partition.error is not None]
    if errors:
        raise RuntimeError(f"Kafka batch offset commit failed: {errors}")
    return committed


STAGES = (
    "parse",
    "prechecks",
    "history_lookup",
    "feature_build",
    "inference",
    "write_commit",
    "process_total",
    "kafka_commit",
    "total",
)


def log_profile(profiler: RollingProfiler, processed: int, new_decisions: int) -> None:
    logger.info(
        "processed=%d new_decisions=%d (rolling-window latency: mean/p50/p95 in ms)",
        processed, new_decisions,
    )
    for stage in STAGES:
        summary = profiler.summary(stage)
        if summary is None:
            continue
        samples, mean, p50, p95 = summary
        logger.info(
            "  %-15s n=%d mean=%.3f p50=%.3f p95=%.3f",
            stage, samples, mean, p50, p95,
        )

def main() -> None:
    configure_json_logging("scoring")
    if STATS_EVERY < 1:
        raise ValueError("STATS_EVERY must be positive")
    if PROFILE_WINDOW < 1:
        raise ValueError("PROFILE_WINDOW must be positive")
    if BATCH_SIZE_PER_PARTITION < 1:
        raise ValueError("FRAUDWATCH_SCORING_BATCH_SIZE_PER_PARTITION must be positive")
    if not 0 < BATCH_WAIT_SECONDS <= 1:
        raise ValueError("FRAUDWATCH_SCORING_BATCH_WAIT_SECONDS must be in (0, 1]")
    if DB_PREPARE_THRESHOLD < 0:
        raise ValueError("FRAUDWATCH_DB_PREPARE_THRESHOLD must be nonnegative")

    metrics = ScoringMetrics()
    health = WorkerHealth(
        ("model", "database", "kafka"),
        heartbeat_timeout=float(
            os.getenv("FRAUDWATCH_WORKER_HEARTBEAT_TIMEOUT_SECONDS", "15")
        ),
    )
    if os.getenv("FRAUDWATCH_METRICS_ENABLED", "1") == "1":
        port = int(os.getenv("FRAUDWATCH_SCORING_METRICS_PORT", "9101"))
        start_worker_metrics(
            port,
            health=health,
            bind=os.getenv("FRAUDWATCH_METRICS_BIND", "127.0.0.1"),
        )
        logger.info(
            "Scoring health and metrics: http://127.0.0.1:%s/readyz", port
        )
    scorer = load_scorer()
    health.set_component("model", True)
    metrics.set_model_identity(
        model_reference=scorer.model_reference,
        model_digest=scorer.model_digest,
        feature_schema_version=FEATURE_SCHEMA_VERSION,
    )
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    def assignment(event, partitions):
        partitions = list(partitions)
        metrics.assignment(event, partitions)
        health.assignment(event, partitions)

    consumer.subscribe(
        [KAFKA_TOPIC],
        on_assign=lambda _consumer, partitions: assignment("assigned", partitions),
        on_revoke=lambda _consumer, partitions: assignment("revoked", partitions),
        on_lost=lambda _consumer, partitions: assignment("lost", partitions),
    )
    shutdown = ShutdownController()
    logger.info(
        "Scoring consumer started: topic=%s group=%s", KAFKA_TOPIC, KAFKA_GROUP_ID,
        extra={
            "source_topic": KAFKA_TOPIC,
            "consumer_group": KAFKA_GROUP_ID,
            "model_reference": scorer.model_reference,
            "model_digest": scorer.model_digest,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
        },
    )

    processed = 0
    new_decisions = 0
    quarantined_count = 0
    profiler = RollingProfiler(window=PROFILE_WINDOW)
    try:
        with install_shutdown_handlers(shutdown):
            with psycopg.connect(
                DATABASE_URL,
                autocommit=True,
                prepare_threshold=DB_PREPARE_THRESHOLD,
            ) as conn:
                health.set_component("database", True)
                while not shutdown.requested:
                    assigned_partitions = max(1, len(consumer.assignment()))
                    messages = consumer.consume(
                        num_messages=min(
                            1000,
                            BATCH_SIZE_PER_PARTITION * assigned_partitions,
                        ),
                        timeout=BATCH_WAIT_SECONDS,
                    )
                    metrics.polled()
                    health.heartbeat()
                    # Do not begin another database transaction after shutdown.
                    # Records fetched by this consume remain unacknowledged.
                    if shutdown.requested:
                        break
                    if not messages:
                        continue
                    for msg in messages:
                        if msg.error():
                            metrics.errors.labels(category="poll").inc()
                            raise KafkaException(msg.error())

                    for partition_messages in partition_batches(messages):
                        results: list[DurableMessageResult] = []
                        commit_attempted = False
                        batch_started_ns = perf_counter_ns()
                        try:
                            results = persist_scoring_partition_batch(
                                conn,
                                scorer,
                                partition_messages,
                                metrics=metrics,
                            )
                            commit_attempted = True
                            commit_partition_batch(consumer, results)
                            committed_ns = perf_counter_ns()
                        except Exception as exc:
                            category = classify_failure(exc)
                            metrics.errors.labels(category=category.value).inc()
                            if commit_attempted:
                                metrics.commits.labels(result="failure").inc()
                            logger.exception(
                                "Scoring failure category=%s; partition batch "
                                "offset not advanced",
                                category.value,
                                extra={
                                    "source_topic": partition_messages[0].topic(),
                                    "source_partition": partition_messages[0].partition(),
                                    "source_offset": partition_messages[0].offset(),
                                    "last_source_offset": partition_messages[-1].offset(),
                                    "outcome": "retry_without_ack",
                                    "reason": category.value,
                                    "duration_ms": (
                                        perf_counter_ns() - batch_started_ns
                                    ) / 1_000_000,
                                },
                            )
                            raise
                        metrics.commits.labels(result="success").inc()
                        metrics.acknowledged(partition_messages)

                        for result in results:
                            processing_ns = result.durable_ns - result.started_ns
                            commit_ns = committed_ns - result.durable_ns
                            metrics.observe_result(
                                new_decision=result.new_decision,
                                quarantined=result.quarantined,
                                timings=result.timings,
                                processing_ns=processing_ns,
                                commit_ns=commit_ns,
                            )
                            processed += 1
                            new_decisions += int(result.new_decision)
                            quarantined_count += int(result.quarantined)
                            if result.new_decision:
                                result.timings.add("process_total", processing_ns)
                                result.timings.add("kafka_commit", commit_ns)
                                result.timings.add(
                                    "total", committed_ns - result.started_ns
                                )
                                profiler.observe(result.timings)

                        if processed // STATS_EVERY != (
                            processed - len(results)
                        ) // STATS_EVERY:
                            logger.info("quarantined_total=%d", quarantined_count)
                            log_profile(profiler, processed, new_decisions)

    except KeyboardInterrupt:
        logger.info("Stopping scoring worker")
    except Exception:
        logger.exception(
            "Scoring worker failed; current Kafka message was not committed"
        )
        raise
    finally:
        health.stop()
        if processed:
            logger.info("quarantined_total=%d", quarantined_count)
            log_profile(profiler, processed, new_decisions)
        if shutdown.requested:
            logger.info(
                "Stopping scoring worker after signal %s",
                shutdown.signal_number,
            )
        consumer.close()


if __name__ == "__main__":
    main()
