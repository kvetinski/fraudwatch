import json
import pytest

from services.scoring.worker import parse_transaction


@pytest.mark.parametrize(
    "amount",
    [0.0, 0.01, 100.0],
)
def test_valid_amounts(amount):
    payload = json.dumps({
        "transaction_id": "test-amount",
        "terminal_id": "123",
        "amount": amount,
        "occurred_at": "2018-04-01T00:00:00Z",
    }).encode()

    transaction, _ = parse_transaction(payload)
    assert transaction.amount == amount


@pytest.mark.parametrize(
    "amount",
    [-1.0, float("nan"), float("inf")],
)
def test_invalid_amounts(amount):
    payload = json.dumps({
        "transaction_id": "test-amount",
        "terminal_id": "123",
        "amount": amount,
        "occurred_at": "2018-04-01T00:00:00Z",
    }).encode()

    with pytest.raises(ValueError):
        parse_transaction(payload)
