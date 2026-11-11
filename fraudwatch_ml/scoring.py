"""Offline-parity-checked single-event feature construction and scoring."""

from dataclasses import dataclass

import numpy as np
import pandas as pd

from fraudwatch_ml.features import FEATURE_NAMES, base_features_one
from fraudwatch_ml.online_features import TerminalHistory


FEATURE_SCHEMA_VERSION = "v1"
BASE_FEATURE_COUNT = 6


@dataclass(frozen=True)
class Transaction:
    transaction_id: str
    terminal_id: str
    timestamp: str | pd.Timestamp
    amount: float


def make_feature_vector(transaction: Transaction, history: TerminalHistory) -> np.ndarray:
    """Return exactly 12 float64 values in the training feature order.

    Keep history's float32 feature rounding: offline terminal_risk_features
    explicitly returns float32, then column_stack promotes it to float64.
    """
    timestamp = pd.Timestamp(transaction.timestamp)
    base = base_features_one(timestamp, transaction.amount)
    terminal = np.asarray(
        history.features(transaction.terminal_id, transaction.timestamp),
        dtype=np.float32,
    )

    if terminal.shape != (len(FEATURE_NAMES) - BASE_FEATURE_COUNT,):
        raise ValueError("Invalid terminal history feature count")

    vector = np.empty(len(FEATURE_NAMES), dtype=np.float64)
    vector[:BASE_FEATURE_COUNT] = base
    vector[BASE_FEATURE_COUNT:] = terminal

    if not np.isfinite(vector).all():
        raise ValueError("Non-finite feature value")
    return vector


def make_feature_frame(transaction: Transaction, history: TerminalHistory) -> pd.DataFrame:
    """Named model input, compatible with models trained on DataFrame columns."""
    vector = make_feature_vector(transaction, history)
    return pd.DataFrame([vector], columns=list(FEATURE_NAMES))


class FraudScorer:
    def __init__(self, model, model_reference: str):
        self.model = model
        self.model_reference = model_reference

        if model.n_features_in_ != len(FEATURE_NAMES):
            raise ValueError("Model feature count mismatch")

        if hasattr(model, "feature_names_in_"):
            if list(model.feature_names_in_) != list(FEATURE_NAMES):
                raise ValueError("Model feature order mismatch")

    def score(self, transaction: Transaction, history: TerminalHistory) -> dict:
        features = make_feature_frame(transaction, history)
        fraud_score = float(self.model.predict_proba(features)[0, 1])

        return {
            "transaction_id": transaction.transaction_id,
            "fraud_score": fraud_score,
            "model_reference": self.model_reference,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
        }
