from pathlib import Path

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd

from mlflow.models import infer_signature
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import (
    average_precision_score,
    roc_auc_score,
)

from fraudwatch_ml.data import load_transactions
from fraudwatch_ml.features import (
    FEATURE_NAMES,
    LABEL_DELAY_DAYS,
    TERMINAL_WINDOWS_DAYS,
    build_features,
)
from fraudwatch_ml.model_serialization import (
    APPROVED_SKOPS_TRUSTED_TYPES,
    SKLEARN_SERIALIZATION_FORMAT,
)


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data/handbook-raw/data"

TRAIN_END = pd.Timestamp("2018-07-25")
VAL_START = pd.Timestamp("2018-08-01")
VAL_END = pd.Timestamp("2018-09-01")

ALERT_FRACTION = 0.01

MODEL_PARAMS = {
    "max_iter": 100,
    "max_leaf_nodes": 15,
    "min_samples_leaf": 100,
    "learning_rate": 0.08,
    "early_stopping": False,
    "random_state": 42,
}


def evaluate(y, scores):
    n_alerts = max(
        1, int(np.ceil(len(scores) * ALERT_FRACTION))
    )

    # Preserve our previous evaluation method.
    selected = np.argpartition(
        scores, -n_alerts
    )[-n_alerts:]

    detected = int(y[selected].sum())
    total_frauds = int(y.sum())

    metrics = {
        "average_precision": float(
            average_precision_score(y, scores)
        ),
        "roc_auc": float(
            roc_auc_score(y, scores)
        ),
        "precision_at_1pct": detected / n_alerts,
        "recall_at_1pct": detected / total_frauds,
        "frauds_detected": detected,
        "alert_count": n_alerts,
    }

    return metrics


def main():
    df = load_transactions(
        DATA_DIR,
        "2018-04-01",
        "2018-09-01",
    )

    X = pd.DataFrame(
        build_features(df),
        columns=FEATURE_NAMES,
    )
    y = df["TX_FRAUD"].to_numpy()

    dates = df["TX_DATETIME"]

    train_mask = (dates < TRAIN_END).to_numpy()

    val_mask = (
        (dates >= VAL_START)
        & (dates < VAL_END)
    ).to_numpy()

    X_train = X.loc[train_mask]
    X_val = X.loc[val_mask]

    y_train = y[train_mask]
    y_val = y[val_mask]

    model = HistGradientBoostingClassifier(
        **MODEL_PARAMS
    )

    print("Training records:", len(X_train))
    print("Validation records:", len(X_val))
    print("Features:", len(FEATURE_NAMES))

    model.fit(X_train, y_train)

    scores = model.predict_proba(X_val)[:, 1]
    metrics = evaluate(y_val, scores)

    tracking_uri = (
        f"sqlite:///{ROOT / 'mlflow.db'}"
    )

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("fraudwatch-training")

    with mlflow.start_run(
        run_name="hgb-terminal-risk-v1"
    ) as run:
        mlflow.log_params(MODEL_PARAMS)

        mlflow.log_params({
            "train_end_exclusive": str(TRAIN_END),
            "validation_start": str(VAL_START),
            "validation_end_exclusive": str(VAL_END),
            "label_delay_days": LABEL_DELAY_DAYS,
            "feature_count": len(FEATURE_NAMES),
            "alert_fraction": ALERT_FRACTION,
        })

        mlflow.log_metrics(metrics)

        mlflow.log_dict(
            {
                "feature_names": list(FEATURE_NAMES),
                "terminal_windows_days": list(
                    TERMINAL_WINDOWS_DAYS
                ),
                "label_delay_days": LABEL_DELAY_DAYS,
                "raw_input_columns": [
                    "TRANSACTION_ID",
                    "TX_DATETIME",
                    "TERMINAL_ID",
                    "TX_AMOUNT",
                ],
            },
            "metadata/feature_schema.json",
        )

        example = X_val.iloc[:5]

        signature = infer_signature(
            example,
            model.predict(example),
        )

        model_info = mlflow.sklearn.log_model(
            sk_model=model,
            name="fraud_classifier",
            signature=signature,
            input_example=example,
            serialization_format=SKLEARN_SERIALIZATION_FORMAT,
            skops_trusted_types=list(APPROVED_SKOPS_TRUSTED_TYPES),
        )


        print("\nMLFLOW RUN ID:", run.info.run_id)
        print("MODEL URI:", model_info.model_uri)


    # Verify that the registered artifact can be loaded.
    loaded_model = mlflow.sklearn.load_model(
        model_info.model_uri
    )

    original_scores = model.predict_proba(
        X_val.iloc[:100]
    )[:, 1]

    restored_scores = loaded_model.predict_proba(
        X_val.iloc[:100]
    )[:, 1]

    np.testing.assert_allclose(
        original_scores,
        restored_scores,
        rtol=1e-10,
        atol=1e-12,
    )

    print("Model serialization round-trip: PASSED")

    print("\nVALIDATION METRICS")

    for name, value in metrics.items():
        print(f"{name}: {value:.6f}")


if __name__ == "__main__":
    main()
