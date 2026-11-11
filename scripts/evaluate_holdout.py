
import numpy as np
import pandas as pd

from sklearn.ensemble import HistGradientBoostingClassifier

from train_baseline import load_period, make_features, evaluate
from train_terminal_risk import (
    terminal_risk_features,
    evaluate_scenarios,
)


def main():
    # Full timeline is needed for historical features.
    df = load_period("2018-04-01", "2018-10-01")

    X = np.column_stack([
        make_features(df),
        terminal_risk_features(df),
    ])
    y = df["TX_FRAUD"].to_numpy()

    train_mask = (
        df["TX_DATETIME"] < pd.Timestamp("2018-08-25")
    ).to_numpy()

    test_mask = (
        df["TX_DATETIME"] >= pd.Timestamp("2018-09-01")
    ).to_numpy()

    model = HistGradientBoostingClassifier(
        max_iter=100,
        max_leaf_nodes=15,
        min_samples_leaf=100,
        learning_rate=0.08,
        early_stopping=False,
        random_state=42,
    )

    print("Training records:", train_mask.sum())
    print("Test records:", test_mask.sum())
    print("Features:", X.shape[1])

    # April–August training; September holdout.
    model.fit(
        X[train_mask],
        y[train_mask],
    )

    scores = model.predict_proba(
        X[test_mask]
    )[:, 1]

    print("\nSEPTEMBER HOLDOUT METRICS")
    evaluate(y[test_mask], scores)

    print("\nSEPTEMBER SCENARIO RECALL")
    test_df = (
        df.loc[test_mask]
        .reset_index(drop=True)
    )
    evaluate_scenarios(test_df, scores)


if __name__ == "__main__":
    main()
