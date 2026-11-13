
import os
from time import perf_counter

import psycopg
from confluent_kafka import Consumer, KafkaException

from fraudwatch_ml.labels import store_label
from fraudwatch_ml.observability import LabelMetrics, start_worker_metrics
from fraudwatch_ml.quarantine import InvalidLabel, store_quarantined


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgres://fraudwatch:fraudwatch_dev@"
    "127.0.0.1:5434/fraudwatch?sslmode=disable",
)

KAFKA_BROKERS = os.getenv(
    "KAFKA_BROKERS", "localhost:9092"
)


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
        inserted = store_label(conn, payload, kafka_key=msg.key())
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
    metrics = LabelMetrics()
    if os.getenv("FRAUDWATCH_METRICS_ENABLED", "1") == "1":
        port = int(os.getenv("FRAUDWATCH_LABEL_METRICS_PORT", "9102"))
        start_worker_metrics(port, bind=os.getenv("FRAUDWATCH_METRICS_BIND", "127.0.0.1"))
        print(f"Label /metrics: http://127.0.0.1:{port}/metrics", flush=True)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": "fraudwatch-labels-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })

    consumer.subscribe(["fraud-labels.v1"])

    print("Fraud-label consumer started", flush=True)

    try:
        with psycopg.connect(
            DATABASE_URL,
            autocommit=True,
        ) as conn:

            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    metrics.errors.labels(category="poll").inc()
                    raise KafkaException(msg.error())

                try:
                    inserted, quarantined, db_seconds, commit_seconds = (
                        process_and_commit(conn, consumer, msg)
                    )
                except Exception:
                    metrics.errors.labels(category="process").inc()
                    raise
                metrics.observe_result(
                    inserted=inserted,
                    quarantined=quarantined,
                    db_seconds=db_seconds,
                    commit_seconds=commit_seconds,
                )

                print(
                    f"label processed: "
                    f"partition={msg.partition()} "
                    f"offset={msg.offset()} "
                    f"new_label={inserted} "
                    f"quarantined={quarantined}",
                    flush=True,
                )

    except KeyboardInterrupt:
        print("Stopping label worker")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
