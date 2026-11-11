import numpy as np
import pandas as pd
import pytest

from fraudwatch_ml.features import FEATURE_NAMES, build_features
from fraudwatch_ml.online_features import TerminalHistory
from fraudwatch_ml.scoring import (
    FraudScorer,
    Transaction,
    make_feature_frame,
)


def test_online_offline_parity():
    df = pd.DataFrame({
        "TRANSACTION_ID": [1, 2, 3, 4, 5, 6],
        "TX_DATETIME": pd.to_datetime([
            "2018-04-01",
            "2018-04-06",
            "2018-04-08",
            "2018-04-08",
            "2018-04-10",
            "2018-04-20",
        ]),
        "TERMINAL_ID": ["A", "A", "B", "A", "A", "A"],
        "TX_AMOUNT": [50, 30, 40, 100, 70, 90],
        "TX_FRAUD": [1, 0, 1, 0, 1, 0],
    })

    expected = build_features(df)

    history = TerminalHistory()
    observed = []
    previous = []
    confirmed = set()

    for row in df.itertuples(index=False):
        # Simulate delayed fraud-label delivery.
        for past in previous:
            available_at = (
                past.TX_DATETIME + pd.Timedelta(days=7)
            )

            if (
                past.TRANSACTION_ID not in confirmed
                and available_at <= row.TX_DATETIME
            ):
                history.record_label(
                    past.TRANSACTION_ID,
                    int(past.TX_FRAUD),
                    available_at,
                )
                confirmed.add(past.TRANSACTION_ID)

        tx = Transaction(
            transaction_id=row.TRANSACTION_ID,
            terminal_id=row.TERMINAL_ID,
            timestamp=str(row.TX_DATETIME),
            amount=row.TX_AMOUNT,
        )

        # Score before recording the current transaction.
        features = make_feature_frame(tx, history)
        observed.append(features.to_numpy()[0])

        history.record_transaction(
            tx.transaction_id,
            tx.terminal_id,
            tx.timestamp,
        )
        previous.append(row)

    np.testing.assert_allclose(
        np.asarray(observed),
        expected,
        rtol=0,
        atol=1e-7,
    )


def test_late_label_is_not_used_early():
    history = TerminalHistory()

    history.record_transaction(
        1, "A", "2018-04-01"
    )

    with pytest.raises(ValueError):
        history.record_label(
            1, 1, "2018-04-05"
        )

    history.record_label(
        1, 1, "2018-04-09"
    )

    # Transaction is old enough, but label is
    # not yet available on April 8.
    early = history.features("A", "2018-04-08")

    np.testing.assert_array_equal(
        early,
        [1, 0, 0, 1, 0, 0],
    )

    later = history.features("A", "2018-04-10")

    np.testing.assert_array_equal(
        later,
        [1, 1, 1, 1, 1, 1],
    )


def test_scorer_returns_fraud_score():
    class FakeModel:
        n_features_in_ = 12
        feature_names_in_ = np.array(FEATURE_NAMES)

        def predict_proba(self, X):
            assert list(X.columns) == list(FEATURE_NAMES)
            return np.array([[0.27, 0.73]])

    scorer = FraudScorer(
        FakeModel(),
        model_reference="test-model",
    )

    tx = Transaction(
        transaction_id=42,
        terminal_id="A",
        timestamp="2018-04-20",
        amount=100.0,
    )

    result = scorer.score(tx, TerminalHistory())

    assert result["transaction_id"] == 42
    assert result["fraud_score"] == pytest.approx(0.73)
    assert result["feature_schema_version"] == "v1"
