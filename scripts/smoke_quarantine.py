"""Send one malformed and one valid event keyed to the same Kafka partition.

Run with the scoring worker running. This script publishes only; it does not
wait for the database, so inspect scoring_quarantine and fraud_decisions.
"""

import json
import uuid

from confluent_kafka import Producer


def main():
    run_id = uuid.uuid4().hex[:12]
    key = f"quarantine-smoke-{run_id}"
    transaction_id = f"quarantine-valid-{run_id}"
    delivered = []
    producer = Producer({"bootstrap.servers": "localhost:9092", "acks": "all"})

    # Shared key ensures both messages go to the same partition and are ordered.
    messages = [
        b"{invalid-json",
        json.dumps({
            "transaction_id": transaction_id,
            "terminal_id": key,
            "amount": 0.0,
            "occurred_at": "2018-04-01T00:00:00Z",
        }).encode(),
    ]

    for payload in messages:
        producer.produce(
            "transactions.v1", key=key.encode(), value=payload,
            on_delivery=lambda error, msg: delivered.append(error),
        )
    remaining = producer.flush(15)
    assert remaining == 0 and len(delivered) == 2 and all(e is None for e in delivered), delivered

    print(f"Invalid payload key: {key}")
    print(f"Valid transaction_id: {transaction_id}")
    print("Published both events in partition order. Verify in PostgreSQL.")


if __name__ == "__main__":
    main()
