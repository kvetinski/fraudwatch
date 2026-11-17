"""Canonical Python validation for transaction stream events."""

from __future__ import annotations

import math

import pandas as pd

from fraudwatch_ml.contracts import (
    TRANSACTION_FIELDS,
    EventContractError,
    decode_event_object,
    require_rfc3339,
)
from fraudwatch_ml.quarantine import InvalidTransaction
from fraudwatch_ml.scoring import Transaction


def parse_transaction(payload: bytes) -> tuple[Transaction, pd.Timestamp]:
    """Parse deterministic schema errors into quarantinable transaction errors."""
    try:
        data = decode_event_object(
            payload,
            expected_fields=TRANSACTION_FIELDS,
            event_name="transaction",
        )
    except EventContractError as exc:
        raise InvalidTransaction(
            str(exc), reason_code="transaction.contract_violation"
        ) from exc

    transaction_id = data.get("transaction_id")
    customer_id = data.get("customer_id")
    terminal_id = data.get("terminal_id")
    amount = data.get("amount")
    timestamp = data.get("occurred_at")

    if not isinstance(transaction_id, str) or not transaction_id.strip():
        raise InvalidTransaction(
            "Invalid transaction_id",
            reason_code="transaction.invalid_transaction_id",
        )
    if not isinstance(customer_id, str) or not customer_id.strip():
        raise InvalidTransaction(
            "Invalid customer_id",
            reason_code="transaction.invalid_customer_id",
        )
    if not isinstance(terminal_id, str) or not terminal_id.strip():
        raise InvalidTransaction(
            "Invalid terminal_id",
            reason_code="transaction.invalid_terminal_id",
        )
    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        raise InvalidTransaction(
            "Invalid amount", reason_code="transaction.invalid_amount"
        )
    try:
        finite = math.isfinite(amount)
    except (OverflowError, ValueError, TypeError) as exc:
        raise InvalidTransaction(
            "Amount outside supported range",
            reason_code="transaction.invalid_amount",
        ) from exc
    if not finite or amount < 0:
        raise InvalidTransaction(
            "Amount must be non-negative and finite",
            reason_code="transaction.invalid_amount",
        )
    try:
        require_rfc3339(timestamp, field_name="occurred_at")
        occurred_at = pd.Timestamp(timestamp)
        occurred_at = occurred_at.tz_convert("UTC")
    except (EventContractError, ValueError, TypeError, OverflowError) as exc:
        raise InvalidTransaction(
            "Invalid occurred_at timestamp",
            reason_code="transaction.invalid_timestamp",
        ) from exc

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
        raise InvalidTransaction(
            "Kafka key must equal terminal_id",
            reason_code="transaction.kafka_key_mismatch",
        )
