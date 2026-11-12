"""Shared event-envelope validation primitives."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Final


MAX_EVENT_BYTES: Final = 64 * 1024
TRANSACTION_FIELDS: Final = frozenset({
    "transaction_id",
    "customer_id",
    "terminal_id",
    "amount",
    "occurred_at",
})
LABEL_FIELDS: Final = frozenset({
    "transaction_id",
    "fraud_label",
    "observed_at",
})

_RFC3339 = re.compile(
    r"^[0-9]{4}-(0[1-9]|1[0-2])-"
    r"(0[1-9]|[12][0-9]|3[01])T"
    r"([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"
    r"(\.[0-9]{1,9})?"
    r"(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$"
)


class EventContractError(ValueError):
    """A deterministic event-envelope contract violation."""


def _reject_json_constant(value: str):
    raise EventContractError(f"Non-JSON numeric constant: {value}")


def decode_event_object(
    payload: bytes,
    *,
    expected_fields: frozenset[str],
    event_name: str,
) -> dict:
    """Decode a size-bounded JSON object with an exact field set."""
    if not isinstance(payload, bytes):
        raise EventContractError(f"{event_name} payload must be bytes")
    if len(payload) > MAX_EVENT_BYTES:
        raise EventContractError(
            f"{event_name} payload exceeds {MAX_EVENT_BYTES} bytes"
        )

    try:
        data = json.loads(payload, parse_constant=_reject_json_constant)
    except EventContractError:
        raise
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise EventContractError(f"Invalid {event_name} JSON") from exc

    if not isinstance(data, dict):
        raise EventContractError(f"{event_name} payload must be a JSON object")
    if set(data) != expected_fields:
        raise EventContractError(f"Invalid {event_name} event schema")
    return data


def require_rfc3339(value: object, *, field_name: str) -> str:
    """Require the strict cross-language RFC3339 profile in the schemas."""
    if not isinstance(value, str) or _RFC3339.fullmatch(value) is None:
        raise EventContractError(f"Invalid {field_name}: strict RFC3339 required")

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EventContractError(f"Invalid {field_name} timestamp") from exc
    return value
