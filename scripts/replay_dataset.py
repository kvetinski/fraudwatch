
import argparse
import json
import os
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from fraudwatch_ml.replay_data import select_day

import pandas as pd
import psycopg
from confluent_kafka import Producer


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data/handbook-raw/data"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgres://fraudwatch:fraudwatch_dev@"
    "127.0.0.1:5434/fraudwatch?sslmode=disable",
)

KAFKA_BROKERS = os.getenv(
    "KAFKA_BROKERS", "localhost:9092"
)

LABEL_DELAY = timedelta(days=7)


def read_day(day: date, limit: int, run_id: str):
    path = DATA_DIR / f"{day.isoformat()}.pkl"
    if not path.is_file():
        raise FileNotFoundError(path)

    df = select_day(pd.read_pickle(path), limit)

    df = df.sort_values(
        ["TX_DATETIME", "TRANSACTION_ID"],
        kind="stable",
    ).head(limit)

    result = []

    for row in df.itertuples(index=False):
        occurred = pd.Timestamp(
            row.TX_DATETIME
        ).to_pydatetime().replace(
            tzinfo=timezone.utc
        )

        tx_id = f"replay-{run_id}-{row.TRANSACTION_ID}"

        event = {
            "transaction_id": tx_id,
            "customer_id": str(row.CUSTOMER_ID),
            "terminal_id": (
                f"replay-{run_id}-{row.TERMINAL_ID}"
            ),
            "amount": float(row.TX_AMOUNT),
            "occurred_at": occurred.isoformat(),
        }

        result.append({
            "transaction": event,
            "fraud_label": int(row.TX_FRAUD),
            "occurred_at": occurred,
        })

    return result


def publish(producer, topic, events, key_field):
    if not events:
        return

    errors = []

    for event in events:
        producer.produce(
            topic,
            key=str(event[key_field]).encode(),
            value=json.dumps(event).encode(),
            on_delivery=lambda err, msg: errors.append(err),
        )

    remaining = producer.flush(30)

    if remaining != 0 or len(errors) != len(events):
        raise RuntimeError(
            f"Kafka delivery incomplete: {topic}"
        )

    failures = [err for err in errors if err is not None]
    if failures:
        raise RuntimeError(
            f"Kafka delivery failed: {failures[0]}"
        )


def wait_for_rows(conn, table, ids, timeout):
    if not ids:
        return

    # Table name comes only from the two fixed
    # internal values used below, not user input.
    if table not in ("fraud_decisions", "fraud_labels"):
        raise ValueError("Unsupported table")

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        count = conn.execute(
            f"""
            SELECT count(*)
            FROM {table}
            WHERE transaction_id = ANY(%s)
            """,
            (ids,),
        ).fetchone()[0]

        if count == len(ids):
            return

        time.sleep(0.25)

    raise TimeoutError(
        f"{table}: expected {len(ids)} records; "
        f"the consumers did not finish before timeout"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=16)
    parser.add_argument("--per-day", type=int, default=50)
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()

    if args.days < 8 or args.per_day < 1:
        parser.error(
            "--days must be >= 8 and --per-day >= 1"
        )

    run_id = uuid.uuid4().hex[:12]
    start = date(2018, 4, 1)

    producer = Producer({
        "bootstrap.servers": KAFKA_BROKERS,
        "acks": "all",
    })

    daily = {}
    transactions_total = 0
    labels_total = 0

    print("Replay ID:", run_id, flush=True)

    with psycopg.connect(
        DATABASE_URL, autocommit=True
    ) as conn:

        for i in range(args.days):
            day = start + timedelta(days=i)

            # First publish the labels that become
            # available after the seven-day delay.
            due_day = day - LABEL_DELAY
            due = daily.get(due_day, [])

            labels = [
                {
                    "transaction_id": item[
                        "transaction"
                    ]["transaction_id"],
                    "fraud_label": item["fraud_label"],
                    "observed_at": (
                        item["occurred_at"] + LABEL_DELAY
                    ).isoformat(),
                }
                for item in due
            ]

            publish(
                producer,
                "fraud-labels.v1",
                labels,
                "transaction_id",
            )

            wait_for_rows(
                conn,
                "fraud_labels",
                [e["transaction_id"] for e in labels],
                args.timeout,
            )

            labels_total += len(labels)

            # Only advance after the label
            # consumer has persisted all due labels.
            items = read_day(
                day, args.per_day, run_id
            )
            daily[day] = items

            transactions = [
                item["transaction"] for item in items
            ]

            publish(
                producer,
                "transactions.v1",
                transactions,
                "terminal_id",
            )

            wait_for_rows(
                conn,
                "fraud_decisions",
                [
                    e["transaction_id"]
                    for e in transactions
                ],
                args.timeout,
            )

            transactions_total += len(transactions)

            print(
                f"{day}: "
                f"scored={len(transactions)} "
                f"labels_applied={len(labels)}",
                flush=True,
            )

    print("\nREPLAY COMPLETE")
    print("Replay ID:", run_id)
    print("Transactions scored:", transactions_total)
    print("Labels ingested:", labels_total)


if __name__ == "__main__":
    main()
