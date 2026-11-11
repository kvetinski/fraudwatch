import numpy as np

from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from train_baseline import load_period, make_features, evaluate


DELAY_DAYS = 7
WINDOWS = (7, 30)


def terminal_risk_features(df):
    # load_period() produces a RangeIndex.
    assert np.array_equal(df.index.to_numpy(), np.arange(len(df)))

    # 3 values per window:
    # historical count, confirmed fraud count, fraud rate.
    features = np.zeros((len(df), 6), dtype=np.float32)

    for _, group in df.groupby("TERMINAL_ID", sort=False):
        group = group.sort_values(
            ["TX_DATETIME", "TRANSACTION_ID"],
            kind="stable",
        )

        times = group["TX_DATETIME"].to_numpy(
            dtype="datetime64[ns]"
        )
        labels = group["TX_FRAUD"].to_numpy(
            dtype=np.int64
        )
        indices = group.index.to_numpy()

        prefix_frauds = np.r_[
            0, np.cumsum(labels, dtype=np.int64)
        ]

        # Labels are available only after seven days.
        # Include observations exactly seven days old.
        right = np.searchsorted(
            times,
            times - np.timedelta64(DELAY_DAYS, "D"),
            side="right",
        )

        for j, window in enumerate(WINDOWS):
            left = np.searchsorted(
                times,
                times - np.timedelta64(
                    DELAY_DAYS + window, "D"
                ),
                side="left",
            )

            counts = right - left
            frauds = (
                prefix_frauds[right]
                - prefix_frauds[left]
            )

            rates = np.divide(
                frauds,
                counts,
                out=np.zeros(len(counts), dtype=float),
                where=counts > 0,
            )

            features[indices, j * 3] = counts
            features[indices, j * 3 + 1] = frauds
            features[indices, j * 3 + 2] = rates

    return features


def evaluate_scenarios(val, scores):
    k = max(1, int(np.ceil(len(scores) * 0.01)))

    # Deterministic ranking, including ties.
    selected = np.lexsort((
        val["TRANSACTION_ID"].to_numpy(),
        -scores,
    ))[:k]

    alerts = np.zeros(len(val), dtype=bool)
    alerts[selected] = True

    fraud = val[val["TX_FRAUD"] == 1].copy()
    fraud["detected"] = alerts[
        val["TX_FRAUD"].to_numpy() == 1
    ]

    result = fraud.groupby("TX_FRAUD_SCENARIO").agg(
        fraud_cases=("TX_FRAUD", "size"),
        detected=("detected", "sum"),
    )

    result["recall_pct"] = (
        100 * result["detected"] / result["fraud_cases"]
    ).round(2)

    print(result.to_string())


def main():
    df = load_period("2018-04-01", "2018-09-01")

    base = make_features(df)
    terminal = terminal_risk_features(df)

    X = np.column_stack([base, terminal])
    y = df["TX_FRAUD"].to_numpy()

    train_mask = (
        df["TX_DATETIME"] < "2018-08-01"
    ).to_numpy()
    val_mask = ~train_mask

    val = df.loc[val_mask].reset_index(drop=True)

    models = {
        "Logistic Regression": make_pipeline(
            StandardScaler(),
            LogisticRegression(max_iter=500),
        ),
        "HistGradientBoosting": HistGradientBoostingClassifier(
            max_iter=100,
            max_leaf_nodes=15,
            min_samples_leaf=100,
            learning_rate=0.08,
            early_stopping=False,
            random_state=42,
        ),
        "Random Forest": RandomForestClassifier(
            n_estimators=100,
            max_depth=12,
            min_samples_leaf=50,
            max_features="sqrt",
            n_jobs=-1,
            random_state=42,
        ),
    }

    print("Records:", len(df))
    print("Features:", X.shape[1])
    print("Terminal history sample:")
    print(terminal[:5])

    for name, model in models.items():
        print(f"\n{name.upper()}")

        model.fit(X[train_mask], y[train_mask])

        scores = model.predict_proba(
            X[val_mask]
        )[:, 1]

        evaluate(y[val_mask], scores)

        print("\nSCENARIO RECALL")
        evaluate_scenarios(val, scores)


    print("\nHGB WITHOUT TERMINAL FEATURES — ABLATION")

    ablation_model = HistGradientBoostingClassifier(
        max_iter=100,
        max_leaf_nodes=15,
        min_samples_leaf=100,
        learning_rate=0.08,
        early_stopping=False,
        random_state=42,
    )

    # Same model, same split, only the six base features.
    ablation_model.fit(
        base[train_mask],
        y[train_mask],
    )

    ablation_scores = ablation_model.predict_proba(
        base[val_mask]
    )[:, 1]

    evaluate(y[val_mask], ablation_scores)

    print("\nSCENARIO RECALL — ABLATION")
    evaluate_scenarios(val, ablation_scores)



if __name__ == "__main__":
    main()
