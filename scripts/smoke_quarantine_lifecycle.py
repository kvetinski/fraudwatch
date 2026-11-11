#!/usr/bin/env python3
"""End-to-end FraudWatch quarantine recovery smoke test.

Run from the FraudWatch repository root with the Go ingestion API and scoring
worker running. Creates one deliberately invalid Kafka event, then resubmits
a known-good synthetic correction through the real Go API + outbox.

No existing quarantine record is modified. Keep the resolved test record as an
audit artifact; do not use this procedure to reconstruct real missing data.
"""

import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from urllib.request import urlopen

import psycopg
from confluent_kafka import Producer

ROOT = Path.cwd()
CLI = ROOT / "scripts" / "quarantine.py"
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgres://fraudwatch:fraudwatch_dev@127.0.0.1:5434/fraudwatch?sslmode=disable",
)
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
API_URL = os.getenv("FRAUDWATCH_INGEST_URL", "http://127.0.0.1:8080/transactions")
TIMEOUT_S = int(os.getenv("FRAUDWATCH_SMOKE_TIMEOUT", "120"))


def wait_for(conn, query, params, description):
    deadline = time.monotonic() + TIMEOUT_S
    while time.monotonic() < deadline:
        result = conn.execute(query, params).fetchone()
        if result is not None:
            return result
        time.sleep(0.25)
    raise TimeoutError(
        f"Timed out waiting for {description}. Check make run-scoring, "
        "make run-api, Kafka, and PostgreSQL logs. Do not blindly resubmit."
    )


def run_cli(*args):
    command = [
        sys.executable, str(CLI), "--database-url", DATABASE_URL, *map(str, args)
    ]
    subprocess.run(command, cwd=ROOT, check=True)


def main():
    if not CLI.is_file():
        raise SystemExit("Run this script from the FraudWatch repository root")
    if TIMEOUT_S < 1:
        raise SystemExit("FRAUDWATCH_SMOKE_TIMEOUT must be positive")

    # Avoid creating a quarantined record if the API is known to be down.

    from urllib.parse import urlsplit

    parsed = urlsplit(API_URL)
    health_url = f"{parsed.scheme}://{parsed.netloc}/healthz"

    try:
        with urlopen(health_url, timeout=3) as response:
            if response.status != 200:
                raise RuntimeError(f"API health check returned {response.status}")
    except Exception as exc:
        raise SystemExit(
            f"Go ingestion API is not healthy at {health_url}: {exc}. "
            "Start it with make run-api."
        ) from exc

    run_id = uuid.uuid4().hex[:12]
    txid = f"quarantine-recovery-{run_id}"
    terminal = f"quarantine-terminal-{run_id}"
    correction = {
        "transaction_id": txid,
        "customer_id": "1",
        "terminal_id": terminal,
        "amount": 37.50,
        "occurred_at": "2018-04-01T00:00:00Z",
    }
    invalid = {**correction, "amount": -1.0}
    invalid_payload = json.dumps(invalid, separators=(",", ":")).encode()

    correction_path = Path(tempfile.gettempdir()) / f"fraudwatch-corrected-{run_id}.json"
    correction_path.write_text(json.dumps(correction, indent=2) + "\n", encoding="utf-8")
    print("Transaction ID:", txid, flush=True)
    print("Correction file:", correction_path, flush=True)

    delivery = []
    producer = Producer({"bootstrap.servers": KAFKA_BROKERS, "acks": "all"})
    producer.produce(
        "transactions.v1",
        key=terminal.encode(),
        value=invalid_payload,
        on_delivery=lambda error, msg: delivery.append(
            (error, msg.partition(), msg.offset())
        ),
    )
    if producer.flush(15) != 0 or len(delivery) != 1 or delivery[0][0] is not None:
        raise RuntimeError(f"Kafka publish failed: {delivery}")
    _, partition, offset = delivery[0]
    print(f"Invalid synthetic event published: partition={partition} offset={offset}", flush=True)

    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        qid, error_type, status = wait_for(
            conn,
            """
            SELECT id, error_type, status
            FROM scoring_quarantine
            WHERE source_topic = 'transactions.v1'
              AND source_partition = %s AND source_offset = %s
              AND kafka_key = %s AND kafka_payload = %s
            """,
            (partition, offset, terminal.encode(), invalid_payload),
            "durable quarantine entry",
        )
        assert error_type == "InvalidTransaction", error_type
        assert status == "pending", status
        print(f"Quarantine persisted: id={qid}, status={status}", flush=True)

        # The CLI records the attempt and submits through Go's transactional outbox.
        run_cli("resubmit", qid, "--file", correction_path, "--url", API_URL)

        wait_for(
            conn,
            """
            SELECT d.transaction_id
            FROM fraud_decisions d
            JOIN scored_transactions s USING (transaction_id)
            JOIN outbox_events o USING (transaction_id)
            WHERE d.transaction_id = %s AND o.delivered_at IS NOT NULL
            """,
            (txid,),
            "scored replacement and delivered Go outbox event",
        )
        print("Corrected transaction durably scored", flush=True)

        run_cli(
            "resolve", qid, "--note",
            "Synthetic negative-amount event corrected via Go API and scored",
        )

        record = conn.execute(
            "SELECT status, resolved_at FROM scoring_quarantine WHERE id=%s",
            (qid,),
        ).fetchone()
        decision_count = conn.execute(
            "SELECT count(*) FROM fraud_decisions WHERE transaction_id=%s", (txid,)
        ).fetchone()[0]
        state_count = conn.execute(
            "SELECT count(*) FROM scored_transactions WHERE transaction_id=%s", (txid,)
        ).fetchone()[0]
        audit = conn.execute(
            """
            SELECT action, replacement_transaction_id
            FROM scoring_quarantine_audit
            WHERE quarantine_id=%s ORDER BY id DESC LIMIT 1
            """,
            (qid,),
        ).fetchone()
        assert record[0] == "resolved" and record[1] is not None, record
        assert decision_count == state_count == 1, (decision_count, state_count)
        assert audit == ("resolved", txid), audit

    print("\nQUARANTINE LIFECYCLE: PASSED", flush=True)
    print(f"quarantine_id={qid} status=resolved", flush=True)
    print(f"transaction_id={txid} decisions=1 state_rows=1", flush=True)
    print("audit=resolved", flush=True)


if __name__ == "__main__":
    main()
