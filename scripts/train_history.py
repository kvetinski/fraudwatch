import numpy as np
import pandas as pd

from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from train_baseline import load_period, make_features, evaluate


def previous_transaction_count(
    df: pd.DataFrame,
    entity_column: str,
    window: str,
) -> np.ndarray:
    # Rolling windows require chronological ordering
    # within each customer or terminal.
    ordered = df.sort_values(
        [entity_column, "TX_DATETIME", "TRANSACTION_ID"],
        kind="stable",
    ).copy()

    rolling_count = (
        ordered
        .groupby(entity_column, sort=False)
        .rolling(
            window,
            on="TX_DATETIME",
            closed="left",
        )["TX_AMOUNT"]
        .count()
    )

    ordered["historical_count"] = (
        rolling_count.fillna(0).to_numpy(dtype=np.int32)
    )

    # Restore the original transaction order.
    return (
        ordered
        .set_index("TRANSACTION_ID")["historical_count"]
        .reindex(df["TRANSACTION_ID"])
        .to_numpy()
    )


def main():
    # Load the entire train + validation timeline.
    # History must continue across the split boundary.
    df = load_period("2018-04-01", "2018-09-01")

    df["customer_tx_count_24h"] = previous_transaction_count(
        df, "CUSTOMER_ID", "24h"
    )

    df["terminal_tx_count_1h"] = previous_transaction_count(
        df, "TERMINAL_ID", "1h"
    )

    base_features = make_features(df)

    historical_features = df[
        ["customer_tx_count_24h", "terminal_tx_count_1h"]
    ].to_numpy(dtype=np.float64)

    X = np.column_stack([base_features, historical_features])
    y = df["TX_FRAUD"].to_numpy()

    train_mask = df["TX_DATETIME"] < "2018-08-01"
    val_mask = ~train_mask

    model = make_pipeline(
        StandardScaler(),
        LogisticRegression(max_iter=500),
    )

    model.fit(X[train_mask], y[train_mask])

    scores = model.predict_proba(X[val_mask])[:, 1]

    print("Features:", X.shape[1])
    print("Customer history:")
    print(df["customer_tx_count_24h"].describe())
    print("Terminal history:")
    print(df["terminal_tx_count_1h"].describe())

    print("\nVALIDATION WITH HISTORICAL FEATURES")
    evaluate(y[val_mask], scores)

    # Evaluation only: never use fraud scenarios as model inputs.
    val_df = df.loc[
        val_mask,
        ["TX_FRAUD", "TX_FRAUD_SCENARIO", "TX_AMOUNT"]
    ].copy()

    def top_one_percent(scores):
        k = max(1, int(np.ceil(len(scores) * 0.01)))
        selected = np.argpartition(scores, -k)[-k:]

        mask = np.zeros(len(scores), dtype=bool)
        mask[selected] = True
        return mask

    val_df["history_alert"] = top_one_percent(scores)
    val_df["amount_alert"] = top_one_percent(
        val_df["TX_AMOUNT"].to_numpy()
    )

    fraud = val_df[val_df["TX_FRAUD"] == 1]

    summary = fraud.groupby("TX_FRAUD_SCENARIO").agg(
        fraud_cases=("TX_FRAUD", "size"),
        history_detected=("history_alert", "sum"),
        amount_detected=("amount_alert", "sum"),
    )

    summary["history_recall_pct"] = (
        100 * summary["history_detected"]
        / summary["fraud_cases"]
    ).round(2)

    summary["amount_recall_pct"] = (
        100 * summary["amount_detected"]
        / summary["fraud_cases"]
    ).round(2)

    print("\nFRAUD DETECTION BY SCENARIO")
    print(summary.to_string())



if __name__ == "__main__":
    main()
