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
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter_ns, monotonic
from typing import Any

import mlflow
import mlflow.sklearn
import pandas as pd
import psycopg
from confluent_kafka import Consumer, KafkaException
from mlflow.tracking import MlflowClient
from psycopg.types.json import Jsonb

from fraudwatch_ml.features import FEATURE_NAMES, LABEL_DELAY_DAYS
from fraudwatch_ml.contracts import (
    TRANSACTION_FIELDS,
    EventContractError,
    decode_event_object,
    require_rfc3339,
)
from fraudwatch_ml.postgres_features import PostgresTerminalHistory
from fraudwatch_ml.profiling import EventTimings, RollingProfiler
from fraudwatch_ml.quarantine import InvalidTransaction, store_quarantined
from fraudwatch_ml.observability import (
    ScoringMetrics, start_worker_metrics, refresh_scoring_snapshot,
)
from fraudwatch_ml.terminal_order import (
    TerminalQuarantined, block_for_late_event, quarantine_blocked_terminal,
)
from fraudwatch_ml.scoring import (
    FEATURE_SCHEMA_VERSION,
    Transaction,
    make_feature_frame,
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
MODEL_ALIAS = os.getenv("MODEL_ALIAS", "candidate")
STATS_EVERY = int(os.getenv("STATS_EVERY", "1000"))
PROFILE_WINDOW = int(os.getenv("PROFILE_WINDOW", "10000"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("fraudwatch.scoring")


@dataclass(frozen=True)
class LoadedScorer:
    model: Any
    model_reference: str


def load_scorer() -> LoadedScorer:
    """Resolve the alias once and pin this worker to one concrete version."""
    # Containers can load an exported, portable model without host MLflow paths.
    model_dir = os.getenv("FRAUDWATCH_MODEL_DIR")
    if model_dir:
        bundle = Path(model_dir)
        manifest = json.loads((bundle / "manifest.json").read_text())
        if manifest.get("model_name") != MODEL_NAME:
            raise RuntimeError("Exported model name differs from configured MODEL_NAME")
        if manifest.get("feature_names") != list(FEATURE_NAMES):
            raise RuntimeError("Exported model feature order differs from the scoring schema")
        model = mlflow.sklearn.load_model(str(bundle / "model"))
        if getattr(model, "n_features_in_", len(FEATURE_NAMES)) != len(FEATURE_NAMES):
            raise RuntimeError("Exported model feature count differs from the scoring schema")
        model_reference = manifest["model_reference"]
        logger.info("Loaded portable model: %s", model_reference)
        return LoadedScorer(model=model, model_reference=model_reference)

    tracking_uri = f"sqlite:///{ROOT / 'mlflow.db'}"
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_registry_uri(tracking_uri)

    version = MlflowClient().get_model_version_by_alias(
        MODEL_NAME, MODEL_ALIAS
    )
    model_reference = f"models:/{MODEL_NAME}/{version.version}"
    model = mlflow.sklearn.load_model(model_reference)
    logger.info("Loaded model: %s", model_reference)
    return LoadedScorer(model=model, model_reference=model_reference)


def parse_transaction(payload: bytes) -> tuple[Transaction, pd.Timestamp]:
    """Parse a deterministic transaction schema error into InvalidTransaction.

    InvalidTransaction is the ONLY class automatically sent to quarantine.
    Do not turn database/model errors or timestamp ordering violations into it.
    """
    try:
        data = decode_event_object(
            payload,
            expected_fields=TRANSACTION_FIELDS,
            event_name="transaction",
        )
    except EventContractError as exc:
        raise InvalidTransaction(str(exc)) from exc

    transaction_id = data.get("transaction_id")
    customer_id = data.get("customer_id")
    terminal_id = data.get("terminal_id")
    amount = data.get("amount")
    timestamp = data.get("occurred_at")

    if not isinstance(transaction_id, str) or not transaction_id.strip():
        raise InvalidTransaction("Invalid transaction_id")
    if not isinstance(customer_id, str) or not customer_id.strip():
        raise InvalidTransaction("Invalid customer_id")
    if not isinstance(terminal_id, str) or not terminal_id.strip():
        raise InvalidTransaction("Invalid terminal_id")
    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        raise InvalidTransaction("Invalid amount")
    try:
        finite = math.isfinite(amount)
    except (OverflowError, ValueError, TypeError) as exc:
        raise InvalidTransaction("Amount outside supported range") from exc
    if not finite or amount < 0:
        raise InvalidTransaction("Amount must be non-negative and finite")
    try:
        require_rfc3339(timestamp, field_name="occurred_at")
        occurred_at = pd.Timestamp(timestamp)
        occurred_at = occurred_at.tz_convert("UTC")
    except (EventContractError, ValueError, TypeError, OverflowError) as exc:
        raise InvalidTransaction("Invalid occurred_at timestamp") from exc

    event = Transaction(
        transaction_id=transaction_id,
        terminal_id=terminal_id,
        timestamp=occurred_at,
        amount=float(amount),
    )
    return event, occurred_at


def validate_transaction_key(
    event: Transaction,
    kafka_key: bytes | None,
) -> None:
    """Require the transaction stream key declared by the v1 contract."""
    expected = event.terminal_id.encode("utf-8")
    if kafka_key != expected:
        raise InvalidTransaction("Kafka key must equal terminal_id")


class MeasuredHistory:
    """Time only the history lookup performed by make_feature_frame()."""

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
) -> bool | TerminalQuarantined:
    """Persist score and historical state atomically; offsets committed by caller.

    Stage timings deliberately separate history lookup from the surrounding
    pandas feature assembly. write_commit includes leaving the PG transaction
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
    with conn.transaction():
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (event.terminal_id,),
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
                raise InvalidTransaction(f"Conflicting payload for {event.transaction_id}")
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
                occurred_at=occurred_at.to_pydatetime(),
                last_scored_at=pd.Timestamp(latest).to_pydatetime(),
                source_topic=source_topic, source_partition=partition,
                source_offset=offset, kafka_key=kafka_key,
                kafka_payload=payload,
            )

        staged_label = conn.execute(
            """
            SELECT observed_at
            FROM fraud_labels
            WHERE transaction_id = %s
            """,
            (event.transaction_id,),
        ).fetchone()
        if staged_label is not None:
            earliest = occurred_at + pd.Timedelta(days=LABEL_DELAY_DAYS)
            if pd.Timestamp(staged_label[0]) < earliest:
                raise ValueError(
                    f"Staged label violates the label delay: {event.transaction_id}"
                )

        timings.add("prechecks", perf_counter_ns() - t_precheck)

        measured_history = MeasuredHistory(PostgresTerminalHistory(conn), timings)
        t_features = perf_counter_ns()
        frame = make_feature_frame(event, measured_history)
        if list(frame.columns) != list(FEATURE_NAMES) or len(frame) != 1:
            raise ValueError("Unexpected feature schema")
        # Extract a single row once instead of building a pandas Series per key.
        values = frame.iloc[0].to_numpy(copy=False)
        feature_vector = {
            name: float(value)
            for name, value in zip(FEATURE_NAMES, values)
        }
        feature_elapsed = perf_counter_ns() - t_features
        history_elapsed = timings.durations_ns.get("history_lookup", 0)
        timings.add("feature_build", max(0, feature_elapsed - history_elapsed))

        t_infer = perf_counter_ns()
        score = float(scorer.model.predict_proba(frame)[0, 1])
        if not math.isfinite(score) or not 0.0 <= score <= 1.0:
            raise ValueError(f"Invalid model score: {score}")
        timings.add("inference", perf_counter_ns() - t_infer)

        t_write = perf_counter_ns()
        conn.execute(
            """
            INSERT INTO fraud_decisions (
                transaction_id, fraud_score, model_reference,
                feature_schema_version, feature_vector,
                source_payload_hash, kafka_partition, kafka_offset
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event.transaction_id,
                score,
                scorer.model_reference,
                FEATURE_SCHEMA_VERSION,
                Jsonb(feature_vector),
                payload_hash,
                partition,
                offset,
            ),
        )
        conn.execute(
            """
            INSERT INTO scored_transactions (
                transaction_id, terminal_id, occurred_at
            ) VALUES (%s, %s, %s)
            """,
            (event.transaction_id, event.terminal_id, occurred_at.to_pydatetime()),
        )

    # The PG transaction has committed successfully when this point is reached.
    timings.add("write_commit", perf_counter_ns() - t_write)
    if metrics is not None:
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
            raise InvalidTransaction("Kafka tombstone is not a transaction")
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
            metrics.kafka_commits.labels(result="failure").inc()
        raise
    t2 = perf_counter_ns()
    if metrics is not None:
        metrics.kafka_commits.labels(result="success").inc()

    return new_decision, quarantined, t1 - t0, t2 - t1


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
    if STATS_EVERY < 1:
        raise ValueError("STATS_EVERY must be positive")
    if PROFILE_WINDOW < 1:
        raise ValueError("PROFILE_WINDOW must be positive")

    scorer = load_scorer()
    metrics = ScoringMetrics()
    if os.getenv("FRAUDWATCH_METRICS_ENABLED", "1") == "1":
        port = int(os.getenv("FRAUDWATCH_SCORING_METRICS_PORT", "9101"))
        start_worker_metrics(port, bind=os.getenv("FRAUDWATCH_METRICS_BIND", "127.0.0.1"))
        logger.info("Scoring /metrics: http://127.0.0.1:%s/metrics", port)
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    logger.info("Scoring consumer started: topic=%s group=%s", KAFKA_TOPIC, KAFKA_GROUP_ID)

    processed = 0
    new_decisions = 0
    quarantined_count = 0
    profiler = RollingProfiler(window=PROFILE_WINDOW)
    next_snapshot = 0.0

    try:
        with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
            while True:
                if monotonic() >= next_snapshot:
                    try:
                        refresh_scoring_snapshot(metrics, conn, consumer)
                    except Exception as exc:
                        metrics.snapshot_errors.inc()
                        logger.warning("Monitoring snapshot failed: %s", exc)
                    next_snapshot = monotonic() + 15.0
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    metrics.errors.labels(category="poll").inc()
                    raise KafkaException(msg.error())
                timings = EventTimings()
                try:
                    new_decision, quarantined, processing_ns, commit_ns = process_and_commit(
                        conn, scorer, consumer, msg, timings=timings, metrics=metrics,
                    )
                except Exception:
                    metrics.errors.labels(category="process").inc()
                    raise
                metrics.observe_result(
                    new_decision=bool(new_decision), quarantined=bool(quarantined),
                    timings=timings, processing_ns=processing_ns, commit_ns=commit_ns,
                )

                processed += 1
                new_decisions += int(new_decision)
                quarantined_count += int(quarantined)
                if new_decision:
                    timings.add("process_total", processing_ns)
                    timings.add("kafka_commit", commit_ns)
                    timings.add("total", processing_ns + commit_ns)
                    profiler.observe(timings)

                if processed % STATS_EVERY == 0:
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
        consumer.close()


if __name__ == "__main__":
    main()
