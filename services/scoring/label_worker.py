
import hashlib
import logging
import os
from datetime import timedelta
from time import perf_counter
from dataclasses import dataclass
from typing import Any

import psycopg
from confluent_kafka import Consumer, KafkaException, TopicPartition

from fraudwatch_ml.features import LABEL_DELAY_DAYS
from fraudwatch_ml.labels import parse_label, store_label
from fraudwatch_ml.observability import LabelMetrics, start_worker_metrics
from fraudwatch_ml.json_logging import configure_json_logging
from fraudwatch_ml.quarantine import InvalidLabel, store_quarantined
from fraudwatch_ml.worker_runtime import (
    ShutdownController,
    classify_failure,
    install_shutdown_handlers,
)


logger = logging.getLogger("fraudwatch.labels")


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgres://fraudwatch:fraudwatch_dev@"
    "127.0.0.1:5434/fraudwatch?sslmode=disable",
)

KAFKA_BROKERS = os.getenv(
    "KAFKA_BROKERS", "localhost:9092"
)
KAFKA_TOPIC = os.getenv("KAFKA_LABEL_TOPIC", "fraud-labels.v1")
KAFKA_GROUP_ID = os.getenv("KAFKA_LABEL_GROUP_ID", "fraudwatch-labels-v1")
BATCH_SIZE_PER_PARTITION = int(
    os.getenv("FRAUDWATCH_LABEL_BATCH_SIZE_PER_PARTITION", "64")
)
BATCH_WAIT_SECONDS = float(
    os.getenv("FRAUDWATCH_LABEL_BATCH_WAIT_SECONDS", "0.025")
)
DB_PREPARE_THRESHOLD = int(os.getenv("FRAUDWATCH_DB_PREPARE_THRESHOLD", "1"))


@dataclass
class DurableLabelResult:
    message: Any
    inserted: bool
    quarantined: bool
    started: float
    durable: float


def process_without_kafka_commit(conn, msg) -> DurableLabelResult:
    """Persist one label or quarantine record without advancing Kafka."""
    started = perf_counter()
    quarantined = False
    try:
        payload = msg.value()
        if payload is None:
            raise InvalidLabel(
                "Kafka tombstone is not a fraud label",
                reason_code="label.tombstone",
            )
        inserted = store_label(
            conn,
            payload,
            kafka_key=msg.key(),
            source_topic=msg.topic(),
            source_partition=msg.partition(),
            source_offset=msg.offset(),
        )
    except InvalidLabel as exc:
        inserted = False
        store_quarantined(
            conn,
            source_topic=msg.topic(),
            source_partition=msg.partition(),
            source_offset=msg.offset(),
            kafka_key=msg.key(),
            kafka_payload=msg.value(),
            error=exc,
        )
        logger.warning(
            "Quarantined label topic=%s partition=%s offset=%s reason=%s",
            msg.topic(), msg.partition(), msg.offset(), exc,
            extra={
                "source_topic": msg.topic(),
                "source_partition": msg.partition(),
                "source_offset": msg.offset(),
                "outcome": "quarantined",
                "reason": exc.reason_code,
                "duration_ms": (perf_counter() - started) * 1000,
            },
        )
        quarantined = True
    return DurableLabelResult(
        message=msg,
        inserted=inserted,
        quarantined=quarantined,
        started=started,
        durable=perf_counter(),
    )


def partition_batches(messages):
    grouped = {}
    for message in messages:
        grouped.setdefault((message.topic(), message.partition()), []).append(message)
    return [
        sorted(grouped[key], key=lambda message: message.offset())
        for key in sorted(grouped)
    ]


def try_persist_new_active_labels(conn, messages) -> list[DurableLabelResult] | None:
    """Bulk the common valid, transaction-first label path.

    Returning ``None`` selects the fully general per-record path.  The fast
    path acquires all transaction locks before terminal locks, validates that
    every label is new and immediately activatable, then pipelines the two
    inserts per label inside the caller's transaction.
    """
    started = perf_counter()
    parsed = []
    for message in messages:
        payload = message.value()
        if payload is None:
            return None
        try:
            transaction_id, fraud_label, observed_at = parse_label(payload)
        except InvalidLabel:
            return None
        if message.key() != transaction_id.encode("utf-8"):
            return None
        parsed.append(
            (message, transaction_id, fraud_label, observed_at, bytes(payload))
        )

    transaction_ids = [item[1] for item in parsed]
    if len(set(transaction_ids)) != len(transaction_ids):
        return None

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
    transaction_rows = conn.execute(
        """
        SELECT transaction_id, terminal_id, occurred_at
        FROM scored_transactions
        WHERE transaction_id = ANY(%s)
        """,
        (transaction_ids,),
    ).fetchall()
    transactions = {row[0]: (row[1], row[2]) for row in transaction_rows}
    if len(transactions) != len(transaction_ids):
        return None

    terminal_ids = sorted({value[0] for value in transactions.values()})
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

    existing = conn.execute(
        """
        SELECT transaction_id FROM fraud_labels
        WHERE transaction_id = ANY(%s)
        UNION ALL
        SELECT transaction_id FROM pending_fraud_labels
        WHERE transaction_id = ANY(%s)
        UNION ALL
        SELECT transaction_id FROM valid_event_ledger
        WHERE event_type = 'label' AND transaction_id = ANY(%s)
        LIMIT 1
        """,
        (transaction_ids, transaction_ids, transaction_ids),
    ).fetchone()
    if existing is not None:
        return None

    ledger_rows = []
    label_rows = []
    for message, transaction_id, fraud_label, observed_at, payload in parsed:
        terminal_id, occurred_at = transactions[transaction_id]
        if observed_at < occurred_at + timedelta(days=LABEL_DELAY_DAYS):
            return None
        ledger_rows.append(
            (
                transaction_id,
                terminal_id,
                fraud_label,
                observed_at,
                message.topic(),
                message.partition(),
                message.offset(),
                message.key(),
                payload,
                hashlib.sha256(payload).digest(),
            )
        )
        label_rows.append((transaction_id, fraud_label, observed_at))

    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO valid_event_ledger (
                event_type, transaction_id, terminal_id, fraud_label,
                observed_at, source_topic, source_partition, source_offset,
                kafka_key, kafka_payload, source_payload_sha256
            ) VALUES (
                'label', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            ledger_rows,
        )
        cursor.executemany(
            """
            INSERT INTO fraud_labels (
                transaction_id, fraud_label, observed_at
            ) VALUES (%s, %s, %s)
            """,
            label_rows,
        )
        cursor.executemany(
            """
            UPDATE scored_transactions
            SET fraud_label = %s,
                label_observed_at = %s
            WHERE transaction_id = %s
            """,
            [
                (fraud_label, observed_at, transaction_id)
                for transaction_id, fraud_label, observed_at in label_rows
            ],
        )
    return [
        DurableLabelResult(
            message=item[0],
            inserted=True,
            quarantined=False,
            started=started,
            durable=perf_counter(),
        )
        for item in parsed
    ]


def persist_partition_batch(conn, messages) -> list[DurableLabelResult]:
    """Make one ordered partition batch durable in one PostgreSQL commit.

    ``store_label`` retains its per-record validation and lock ordering.  The
    outer transaction removes one WAL flush per event while keeping the Kafka
    offset behind the durable boundary for the complete contiguous batch.
    """
    with conn.transaction():
        results = try_persist_new_active_labels(conn, messages)
    if results is None:
        # The general path deliberately keeps one transaction per record.
        # Interleaving transaction/terminal locks for several records inside
        # one outer transaction can deadlock with another partition batch.
        results = [
            process_without_kafka_commit(conn, message) for message in messages
        ]
    durable = perf_counter()
    for result in results:
        result.durable = durable
    return results


def process_and_commit(conn, consumer, msg):
    """Persist a label or its quarantine record before committing its offset."""
    started = perf_counter()
    quarantined = False
    try:
        payload = msg.value()
        if payload is None:
            raise InvalidLabel(
                "Kafka tombstone is not a fraud label",
                reason_code="label.tombstone",
            )
        inserted = store_label(
            conn,
            payload,
            kafka_key=msg.key(),
            source_topic=msg.topic(),
            source_partition=msg.partition(),
            source_offset=msg.offset(),
        )
    except InvalidLabel as exc:
        inserted = False
        store_quarantined(
            conn,
            source_topic=msg.topic(),
            source_partition=msg.partition(),
            source_offset=msg.offset(),
            kafka_key=msg.key(),
            kafka_payload=msg.value(),
            error=exc,
        )
        quarantined = True
    durable = perf_counter()

    committed = consumer.commit(message=msg, asynchronous=False)
    if not committed or any(part.error is not None for part in committed):
        raise RuntimeError(f"Label Kafka offset commit failed: {committed}")
    acknowledged = perf_counter()
    return inserted, quarantined, durable - started, acknowledged - durable


def main():
    configure_json_logging("labels")
    if BATCH_SIZE_PER_PARTITION < 1:
        raise ValueError("FRAUDWATCH_LABEL_BATCH_SIZE_PER_PARTITION must be positive")
    if not 0 < BATCH_WAIT_SECONDS <= 1:
        raise ValueError("FRAUDWATCH_LABEL_BATCH_WAIT_SECONDS must be in (0, 1]")
    if DB_PREPARE_THRESHOLD < 0:
        raise ValueError("FRAUDWATCH_DB_PREPARE_THRESHOLD must be nonnegative")
    metrics = LabelMetrics(topic=KAFKA_TOPIC, group_id=KAFKA_GROUP_ID)
    if os.getenv("FRAUDWATCH_METRICS_ENABLED", "1") == "1":
        port = int(os.getenv("FRAUDWATCH_LABEL_METRICS_PORT", "9102"))
        start_worker_metrics(port, bind=os.getenv("FRAUDWATCH_METRICS_BIND", "127.0.0.1"))
        logger.info("Label /metrics: http://127.0.0.1:%s/metrics", port)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })

    consumer.subscribe(
        [KAFKA_TOPIC],
        on_assign=lambda _consumer, partitions: metrics.assignment("assigned", partitions),
        on_revoke=lambda _consumer, partitions: metrics.assignment("revoked", partitions),
        on_lost=lambda _consumer, partitions: metrics.assignment("lost", partitions),
    )
    shutdown = ShutdownController()

    logger.info(
        "Fraud-label consumer started: topic=%s group=%s", KAFKA_TOPIC, KAFKA_GROUP_ID,
        extra={"source_topic": KAFKA_TOPIC, "consumer_group": KAFKA_GROUP_ID},
    )

    try:
        with install_shutdown_handlers(shutdown):
            with psycopg.connect(
                DATABASE_URL,
                autocommit=True,
                prepare_threshold=DB_PREPARE_THRESHOLD,
            ) as conn:

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

                    # If shutdown arrived during poll, leave this fetched record
                    # unacknowledged for the next group member/restart.
                    if shutdown.requested:
                        break

                    if not messages:
                        continue

                    for msg in messages:
                        if msg.error():
                            metrics.errors.labels(category="poll").inc()
                            raise KafkaException(msg.error())

                    for partition_messages in partition_batches(messages):
                        commit_attempted = False
                        batch_started = perf_counter()
                        try:
                            results = persist_partition_batch(
                                conn, partition_messages
                            )
                            last = results[-1].message
                            commit_attempted = True
                            committed = consumer.commit(
                                offsets=[
                                    TopicPartition(
                                        last.topic(),
                                        last.partition(),
                                        last.offset() + 1,
                                    )
                                ],
                                asynchronous=False,
                            )
                            if not committed or any(
                                partition.error is not None
                                for partition in committed
                            ):
                                raise RuntimeError(
                                    f"Label Kafka batch offset commit failed: {committed}"
                                )
                            acknowledged = perf_counter()
                        except Exception as exc:
                            category = classify_failure(exc)
                            metrics.errors.labels(category=category.value).inc()
                            if commit_attempted:
                                metrics.commits.labels(result="failure").inc()
                            logger.exception(
                                "Label worker failure category=%s; partition "
                                "batch offset not advanced",
                                category.value,
                                extra={
                                    "source_topic": partition_messages[0].topic(),
                                    "source_partition": partition_messages[0].partition(),
                                    "source_offset": partition_messages[0].offset(),
                                    "last_source_offset": partition_messages[-1].offset(),
                                    "outcome": "retry_without_ack",
                                    "reason": category.value,
                                    "duration_ms": (
                                        perf_counter() - batch_started
                                    ) * 1000,
                                },
                            )
                            raise
                        metrics.commits.labels(result="success").inc()
                        metrics.acknowledged(partition_messages)
                        for result in results:
                            metrics.observe_result(
                                inserted=result.inserted,
                                quarantined=result.quarantined,
                                db_seconds=result.durable - result.started,
                                commit_seconds=acknowledged - result.durable,
                            )

    except KeyboardInterrupt:
        logger.info("Stopping label worker")
    finally:
        if shutdown.requested:
            logger.info(
                "Stopping label worker after signal %s",
                shutdown.signal_number,
            )
        consumer.close()


if __name__ == "__main__":
    main()
