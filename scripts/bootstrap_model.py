"""Idempotently bootstrap FraudWatch's local MLflow candidate model.

Uses the existing candidate if present. Otherwise, registers the newest READY
fraud_classifier logged to the fraudwatch-training experiment. Only trains
when no reusable logged model is available.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import mlflow
import mlflow.sklearn
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

ROOT = Path(__file__).resolve().parents[1]
MODEL_NAME = "fraudwatch-hgb"
ALIAS = "candidate"
EXPERIMENT_NAME = "fraudwatch-training"
LOGGED_NAME = "fraud_classifier"


def newest_ready_logged_model(client: MlflowClient):
    experiment = client.get_experiment_by_name(EXPERIMENT_NAME)
    if experiment is None:
        return None

    models = client.search_logged_models(
        experiment_ids=[experiment.experiment_id],
        max_results=50,
        order_by=[{"field_name": "creation_time", "ascending": False}],
    )
    for model in models:
        if model.name == LOGGED_NAME and str(model.status).endswith("READY"):
            return model
    return None


def main() -> None:
    tracking_uri = f"sqlite:///{ROOT / 'mlflow.db'}"
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_registry_uri(tracking_uri)
    client = MlflowClient()

    try:
        existing = client.get_model_version_by_alias(MODEL_NAME, ALIAS)
    except MlflowException:
        existing = None

    if existing is not None:
        print(f"Existing model: models:/{MODEL_NAME}/{existing.version} (@{ALIAS})")
        return

    logged = newest_ready_logged_model(client)
    if logged is None:
        print("No reusable logged model. Training selected HGB model...")
        subprocess.run(
            [sys.executable, str(ROOT / "scripts/train_mlflow.py")],
            check=True,
            cwd=ROOT,
        )
        logged = newest_ready_logged_model(client)

    if logged is None:
        raise RuntimeError("Training finished without a READY fraud_classifier model")

    source_uri = f"models:/{logged.model_id}"
    registered = mlflow.register_model(model_uri=source_uri, name=MODEL_NAME)
    client.set_registered_model_alias(MODEL_NAME, ALIAS, registered.version)

    model_uri = f"models:/{MODEL_NAME}/{registered.version}"
    loaded = mlflow.sklearn.load_model(model_uri)
    if loaded.n_features_in_ != 12:
        raise AssertionError(f"Expected 12 features, got {loaded.n_features_in_}")
    print(f"Registered candidate: {model_uri}; feature schema: 12 (verified)")


if __name__ == "__main__":
    main()
