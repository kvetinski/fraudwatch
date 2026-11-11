
import os
from datetime import date, timedelta
from pathlib import Path

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import psycopg

import argparse
from fraudwatch_ml.replay_data import select_day

from fraudwatch_ml.features import (
    FEATURE_NAMES,
    build_features,
)


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data/handbook-raw/data"

RUN_ID = "ec0737541795"
DAYS = 16
PER_DAY = 50

MODEL_URI = "models:/fraudwatch-hgb/1"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgres://fraudwatch:fraudwatch_dev@"
    "127.0.0.1:5434/fraudwatch?sslmode=disable",
)



def load_replay_data(
    run_id: str,
    days: int,
    per_day: int,
) -> pd.DataFrame:
    frames = []
    start = date(2018, 4, 1)

    for i in range(days):
        day = start + timedelta(days=i)
        path = DATA_DIR / f"{day.isoformat()}.pkl"

        df = select_day(
            pd.read_pickle(path),
            per_day,
        ).copy()

        df["replay_id"] = (
            "replay-"
            + run_id
            + "-"
            + df["TRANSACTION_ID"].astype(str)
        )

        df["TERMINAL_ID"] = (
            "replay-"
            + run_id
            + "-"
            + df["TERMINAL_ID"].astype(str)
        )

        frames.append(df)

    return pd.concat(frames, ignore_index=True)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--days", type=int, default=16)
    parser.add_argument("--per-day", type=int, default=1000)
    args = parser.parse_args()

    df = load_replay_data(
        run_id=args.run_id,
        days=args.days,
        per_day=args.per_day,
    )

    print(f"Replay ID: {args.run_id}")
    print(f"Days: {args.days}")
    print(f"Transactions per day: {args.per_day}")
    print(f"Offline transactions loaded: {len(df)}")

    # Continue with existing feature generation,
    # model inference and database comparison.

    X = pd.DataFrame(
        build_features(df),
        columns=FEATURE_NAMES,
    )

    mlflow.set_tracking_uri(
        f"sqlite:///{ROOT / 'mlflow.db'}"
    )
    mlflow.set_registry_uri(
        f"sqlite:///{ROOT / 'mlflow.db'}"
    )

    model = mlflow.sklearn.load_model(MODEL_URI)

    expected_scores = model.predict_proba(X)[:, 1]

    with psycopg.connect(DATABASE_URL) as conn:
        rows = conn.execute(
            """
            SELECT
                transaction_id,
                feature_vector,
                fraud_score,
                model_reference
            FROM fraud_decisions
            WHERE transaction_id LIKE %s
            """,
            (f"replay-{args.run_id}-%",),
        ).fetchall()

    stored = {
        tx_id: (features, score, model_ref)
        for tx_id, features, score, model_ref in rows
    }

    assert len(stored) == len(df), (
        f"Expected {len(df)} decisions, "
        f"found {len(stored)}"
    )

    for i, tx_id in enumerate(df["replay_id"]):
        features, score, model_ref = stored[tx_id]

        assert model_ref == MODEL_URI

        actual_vector = np.asarray(
            [features[name] for name in FEATURE_NAMES],
            dtype=np.float64,
        )

        expected_vector = X.iloc[i].to_numpy(
            dtype=np.float64
        )

        np.testing.assert_allclose(
            actual_vector,
            expected_vector,
            rtol=0,
            atol=1e-6,
            err_msg=f"Feature mismatch: {tx_id}",
        )

        np.testing.assert_allclose(
            score,
            expected_scores[i],
            rtol=1e-8,
            atol=1e-10,
            err_msg=f"Score mismatch: {tx_id}",
        )

    print("Transactions compared:", len(df))
    print("Feature parity: PASSED")
    print("Prediction parity: PASSED")
    print("Model:", MODEL_URI)


if __name__ == "__main__":
    main()
