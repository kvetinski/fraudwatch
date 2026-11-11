
import os
from time import perf_counter

import psycopg
from confluent_kafka import Consumer, KafkaException

from fraudwatch_ml.labels import store_label
from fraudwatch_ml.observability import LabelMetrics, start_worker_metrics


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgres://fraudwatch:fraudwatch_dev@"
    "127.0.0.1:5434/fraudwatch?sslmode=disable",
)

KAFKA_BROKERS = os.getenv(
    "KAFKA_BROKERS", "localhost:9092"
)


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
                    t0 = perf_counter()
                    inserted = store_label(conn, msg.value())
                    t1 = perf_counter()

                    # Do not acknowledge before PostgreSQL is durable.
                    committed = consumer.commit(message=msg, asynchronous=False)
                    if not committed or any(p.error is not None for p in committed):
                        raise RuntimeError(f"Label Kafka offset commit failed: {committed}")
                    t2 = perf_counter()
                except Exception:
                    metrics.errors.labels(category="process").inc()
                    raise
                metrics.observe_result(
                    inserted=inserted, db_seconds=t1-t0, commit_seconds=t2-t1,
                )

                print(
                    f"label processed: "
                    f"partition={msg.partition()} "
                    f"offset={msg.offset()} "
                    f"new_label={inserted}",
                    flush=True,
                )

    except KeyboardInterrupt:
        print("Stopping label worker")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
