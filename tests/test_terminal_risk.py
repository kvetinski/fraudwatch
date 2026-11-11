import numpy as np
import pandas as pd

from fraudwatch_ml.features import terminal_risk_features

def test_delayed_terminal_labels():
    df = pd.DataFrame({
        "TRANSACTION_ID": [0, 1, 2, 3],
        "TX_DATETIME": pd.to_datetime([
            "2018-04-01",
            "2018-04-06",
            "2018-04-08",
            "2018-04-10",
        ]),
        "TERMINAL_ID": ["A"] * 4,
        "TX_FRAUD": [1, 1, 0, 0],
    })

    features = terminal_risk_features(df)

    # At April 8:
    # April 1 label is available.
    # April 6 label is not available yet.
    np.testing.assert_array_equal(
        features[2],
        [1, 1, 1, 1, 1, 1],
    )

    # At April 10:
    # April 1 is outside the delayed 7-day window,
    # but remains inside the delayed 30-day window.
    np.testing.assert_array_equal(
        features[3],
        [1, 1, 1, 1, 1, 1],
    )

    # Changing an unavailable label cannot affect
    # the April 8 feature vector.
    df.loc[1, "TX_FRAUD"] = 0
    updated = terminal_risk_features(df)

    np.testing.assert_array_equal(
        features[2],
        updated[2],
    )


def test_future_transactions_do_not_change_past():
    df = pd.DataFrame({
        "TRANSACTION_ID": [0, 1],
        "TX_DATETIME": pd.to_datetime([
            "2018-04-01",
            "2018-04-08",
        ]),
        "TERMINAL_ID": ["A", "A"],
        "TX_FRAUD": [1, 0],
    })

    before = terminal_risk_features(df)

    future = pd.DataFrame({
        "TRANSACTION_ID": [2],
        "TX_DATETIME": pd.to_datetime(["2018-04-30"]),
        "TERMINAL_ID": ["A"],
        "TX_FRAUD": [1],
    })

    extended = pd.concat(
        [df, future],
        ignore_index=True,
    )

    after = terminal_risk_features(extended)

    np.testing.assert_array_equal(
        before,
        after[:len(df)],
    )
