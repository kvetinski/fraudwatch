from pathlib import Path

import numpy as np
import pandas as pd

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data/handbook-raw/data"

ALERT_BUDGET = 0.01  # Review top 1% of transactions


def load_period(start: str, end: str) -> pd.DataFrame:
    paths = sorted(
        path for path in DATA_DIR.glob("*.pkl")
        if start <= path.stem < end
    )

    if not paths:
        raise ValueError(f"No files for {start} to {end}")

    return pd.concat(
        (pd.read_pickle(path) for path in paths),
        ignore_index=True,
    )


def make_features(df: pd.DataFrame) -> np.ndarray:
    dt = pd.to_datetime(df["TX_DATETIME"])

    seconds = (
        dt.dt.hour * 3600
        + dt.dt.minute * 60
        + dt.dt.second
    )

    hour_angle = 2 * np.pi * seconds / 86400
    weekday_angle = 2 * np.pi * dt.dt.day_of_week / 7

    return np.column_stack([
        np.log1p(df["TX_AMOUNT"].to_numpy()),
        np.sin(hour_angle),
        np.cos(hour_angle),
        np.sin(weekday_angle),
        np.cos(weekday_angle),
        (dt.dt.day_of_week >= 5).astype(int),
    ])


def evaluate(y_true: np.ndarray, scores: np.ndarray):
    ap = average_precision_score(y_true, scores)
    roc_auc = roc_auc_score(y_true, scores)

    n_alerts = max(
        1, int(np.ceil(len(scores) * ALERT_BUDGET))
    )

    selected = np.argpartition(
        scores, -n_alerts
    )[-n_alerts:]

    fraud_found = int(y_true[selected].sum())
    total_fraud = int(y_true.sum())

    precision = fraud_found / n_alerts
    recall = fraud_found / total_fraud

    print(f"Fraud prevalence: {y_true.mean():.4%}")
    print(f"Average precision: {ap:.6f}")
    print(f"ROC-AUC: {roc_auc:.6f}")
    print(f"Alert budget: {n_alerts}")
    print(f"Frauds detected: {fraud_found}")
    print(f"Precision@1%: {precision:.4%}")
    print(f"Recall@1%: {recall:.4%}")


def main():
    train = load_period("2018-04-01", "2018-08-01")
    val = load_period("2018-08-01", "2018-09-01")

    print("Training records:", len(train))
    print("Validation records:", len(val))

    X_train = make_features(train)
    X_val = make_features(val)

    y_train = train["TX_FRAUD"].to_numpy()
    y_val = val["TX_FRAUD"].to_numpy()

    model = make_pipeline(
        StandardScaler(),
        LogisticRegression(max_iter=500),
    )

    model.fit(X_train, y_train)

    scores = model.predict_proba(X_val)[:, 1]

    print("\nVALIDATION METRICS")
    evaluate(y_val, scores)

    print("\nAMOUNT-ONLY BENCHMARK")
    evaluate(
        y_val,
        val["TX_AMOUNT"].to_numpy(),
    )


if __name__ == "__main__":
    main()
