from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from fraudwatch_ml.features import (
    LABEL_DELAY_DAYS,
    TERMINAL_WINDOWS_DAYS,
)


@dataclass
class HistoricalTransaction:
    transaction_id: int
    terminal_id: str
    occurred_at: pd.Timestamp
    fraud_label: Optional[int] = None
    label_observed_at: Optional[pd.Timestamp] = None


class TerminalHistory:
    def __init__(self):
        self.by_id = {}
        self.by_terminal = defaultdict(list)

    def record_transaction(
        self,
        transaction_id: int,
        terminal_id: str,
        occurred_at,
    ):
        occurred_at = pd.Timestamp(occurred_at)
        terminal_id = str(terminal_id)

        existing = self.by_id.get(transaction_id)

        if existing is not None:
            if (
                existing.terminal_id == terminal_id
                and existing.occurred_at == occurred_at
            ):
                return  # Idempotent replay

            raise ValueError(
                f"Conflicting transaction ID: {transaction_id}"
            )

        record = HistoricalTransaction(
            transaction_id=transaction_id,
            terminal_id=terminal_id,
            occurred_at=occurred_at,
        )

        self.by_id[transaction_id] = record
        self.by_terminal[terminal_id].append(record)

    def record_label(
        self,
        transaction_id: int,
        fraud_label: int,
        observed_at,
    ):
        record = self.by_id[transaction_id]
        observed_at = pd.Timestamp(observed_at)

        if fraud_label not in (0, 1):
            raise ValueError("Fraud label must be 0 or 1")

        available_from = (
            record.occurred_at
            + pd.Timedelta(days=LABEL_DELAY_DAYS)
        )

        if observed_at < available_from:
            raise ValueError(
                "Label arrived before the configured delay"
            )

        if record.label_observed_at is not None:
            if (
                record.fraud_label == fraud_label
                and record.label_observed_at == observed_at
            ):
                return  # Idempotent replay

            raise ValueError("Conflicting fraud-label event")

        record.fraud_label = fraud_label
        record.label_observed_at = observed_at

    def features(self, terminal_id: str, at) -> np.ndarray:
        at = pd.Timestamp(at)
        records = self.by_terminal.get(str(terminal_id), [])

        values = []

        for window_days in TERMINAL_WINDOWS_DAYS:
            lower = at - pd.Timedelta(
                days=LABEL_DELAY_DAYS + window_days
            )
            upper = at - pd.Timedelta(
                days=LABEL_DELAY_DAYS
            )

            eligible = [
                record
                for record in records
                if lower <= record.occurred_at <= upper
            ]

            count = len(eligible)

            fraud_count = sum(
                record.fraud_label == 1
                and record.label_observed_at is not None
                and record.label_observed_at <= at
                for record in eligible
            )

            fraud_rate = (
                fraud_count / count if count else 0.0
            )

            values.extend([
                count,
                fraud_count,
                fraud_rate,
            ])

        # Match the offline float32 terminal features.
        return np.asarray(values, dtype=np.float32)
