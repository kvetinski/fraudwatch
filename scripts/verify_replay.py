import argparse
import json
import os
from datetime import date, timedelta
from pathlib import Path

import mlflow.sklearn
import numpy as np
import pandas as pd
import psycopg

from fraudwatch_ml.benchmark_replay import validate_run_id
from fraudwatch_ml.features import (
    FEATURE_NAMES,
    build_features,
)
from fraudwatch_ml.model_integrity import verify_portable_model_digest
from fraudwatch_ml.replay_data import select_day
from fraudwatch_ml.scoring import FEATURE_SCHEMA_VERSION


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data/handbook-raw/data"

MODEL_BUNDLE = ROOT / "artifacts/candidate-model"

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



def load_verified_model(bundle: Path):
    manifest_path = bundle / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SystemExit(
            f"Missing exported model bundle at {bundle}; run make app-export-model"
        ) from exc
    if manifest.get("feature_names") != list(FEATURE_NAMES):
        raise SystemExit("Model bundle feature order differs from the replay schema")
    if manifest.get("feature_schema_version") != FEATURE_SCHEMA_VERSION:
        raise SystemExit("Model bundle feature schema version is unsupported")
    model_digest = verify_portable_model_digest(bundle, manifest)
    model_reference = manifest.get("model_reference")
    if not isinstance(model_reference, str) or not model_reference:
        raise SystemExit("Model bundle has no model reference")
    model = mlflow.sklearn.load_model(str(bundle / "model"))
    return model, model_reference, model_digest


def escaped_like_prefix(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--days", type=int, default=16)
    parser.add_argument("--per-day", type=int, default=1000)
    parser.add_argument("--model-bundle", type=Path, default=MODEL_BUNDLE)
    args = parser.parse_args()
    validate_run_id(args.run_id)

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

    model, model_reference, model_digest = load_verified_model(
        args.model_bundle.resolve()
    )

    expected_scores = model.predict_proba(X)[:, 1]

    with psycopg.connect(DATABASE_URL) as conn:
        rows = conn.execute(
            """
            SELECT
                transaction_id,
                feature_vector,
                fraud_score,
                model_reference,
                model_digest,
                feature_schema_version
            FROM fraud_decisions
            WHERE transaction_id LIKE %s ESCAPE '\\'
            """,
            (escaped_like_prefix(f"replay-{args.run_id}-") + "%",),
        ).fetchall()

    stored = {
        tx_id: (features, score, model_ref, digest, schema)
        for tx_id, features, score, model_ref, digest, schema in rows
    }

    assert len(stored) == len(df), (
        f"Expected {len(df)} decisions, "
        f"found {len(stored)}"
    )

    for i, tx_id in enumerate(df["replay_id"]):
        features, score, model_ref, digest, schema = stored[tx_id]

        assert model_ref == model_reference
        assert digest == model_digest
        assert schema == FEATURE_SCHEMA_VERSION

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
    print("Model:", model_reference)
    print("Model digest:", model_digest)


if __name__ == "__main__":
    main()
