"""Cross-language fixture checks for the authoritative event contracts."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from fraudwatch_ml.contracts import MAX_EVENT_BYTES
from fraudwatch_ml.labels import parse_label
from fraudwatch_ml.quarantine import InvalidTransaction
from services.scoring.worker import (
    parse_transaction,
    validate_transaction_key,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts"


def _fixture(name: str) -> list[dict]:
    with (CONTRACTS / "fixtures" / name).open(encoding="utf-8") as stream:
        return json.load(stream)["cases"]


TRANSACTION_CASES = _fixture("transaction.v1.json")
LABEL_CASES = _fixture("fraud-label.v1.json")


@pytest.mark.parametrize(
    "case",
    TRANSACTION_CASES,
    ids=[case["name"] for case in TRANSACTION_CASES],
)
def test_transaction_contract_fixtures(case):
    payload = case["payload"].encode("utf-8")

    if not case["valid_payload"]:
        with pytest.raises(InvalidTransaction):
            parse_transaction(payload)
        return

    event, occurred_at = parse_transaction(payload)
    assert occurred_at == pd.Timestamp(case["normalized_occurred_at"])

    kafka_key = case["kafka_key"].encode("utf-8")
    if case["valid_stream"]:
        validate_transaction_key(event, kafka_key)
    else:
        with pytest.raises(InvalidTransaction, match="Kafka key"):
            validate_transaction_key(event, kafka_key)


@pytest.mark.parametrize(
    "case",
    LABEL_CASES,
    ids=[case["name"] for case in LABEL_CASES],
)
def test_label_contract_fixtures(case):
    payload = case["payload"].encode("utf-8")

    if not case["valid"]:
        with pytest.raises(ValueError):
            parse_label(payload)
        return

    _, _, observed_at = parse_label(payload)
    assert pd.Timestamp(observed_at) == pd.Timestamp(
        case["normalized_observed_at"]
    )


@pytest.mark.parametrize("parser", [parse_transaction, parse_label])
def test_event_payload_limit(parser):
    with pytest.raises(ValueError, match="exceeds 65536 bytes"):
        parser(b" " * (MAX_EVENT_BYTES + 1))


def test_schema_metadata_matches_transport_contract():
    expected = {
        "transaction.v1.schema.json": {
            "required": {
                "transaction_id", "customer_id", "terminal_id",
                "amount", "occurred_at",
            },
            "topic": "transactions.v1",
            "key": "terminal_id",
        },
        "fraud-label.v1.schema.json": {
            "required": {"transaction_id", "fraud_label", "observed_at"},
            "topic": "fraud-labels.v1",
            "key": "transaction_id",
        },
    }

    for filename, contract in expected.items():
        with (CONTRACTS / filename).open(encoding="utf-8") as stream:
            schema = json.load(stream)
        transport = schema["x-fraudwatch-transport"]
        assert schema["additionalProperties"] is False
        assert set(schema["required"]) == contract["required"]
        assert set(schema["properties"]) == contract["required"]
        assert transport["maxBytes"] == MAX_EVENT_BYTES
        assert transport["kafkaTopic"] == contract["topic"]
        assert transport["kafkaKeyField"] == contract["key"]
