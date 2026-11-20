#!/usr/bin/env python3
"""Export the currently registered MLflow candidate into a portable local directory.

This avoids embedding a workstation-specific SQLite registry and file:/// artifact
locations in the scoring container. No data or database volumes are modified.
"""
from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from mlflow.exceptions import MlflowException

from fraudwatch_ml.benchmark_replay import sha256_tree
from fraudwatch_ml.features import FEATURE_NAMES
from fraudwatch_ml.model_serialization import (
    APPROVED_SKOPS_TRUSTED_TYPES,
    SKLEARN_SERIALIZATION_FORMAT,
)
from fraudwatch_ml.scoring import FEATURE_SCHEMA_VERSION

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/candidate-model")
    args = parser.parse_args()

    output = args.output.resolve()
    if output.exists():
        raise SystemExit(
            f"Export already exists: {output}. Keep it immutable. "
            "To export a new candidate, move or rename the existing directory first."
        )

    uri = f"sqlite:///{ROOT / 'mlflow.db'}"
    mlflow.set_tracking_uri(uri)
    mlflow.set_registry_uri(uri)
    model_name = "fraudwatch-hgb"
    client = MlflowClient()
    version = client.get_model_version_by_alias(model_name, "candidate")
    reference = f"models:/{model_name}/{version.version}"
    model = mlflow.sklearn.load_model(reference)
    print(f"MLflow model resolved: {reference}")

    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".candidate-export-", dir=output.parent) as tmp:
        staged = Path(tmp) / "bundle"
        staged.mkdir()
        mlflow.sklearn.save_model(
            model,
            path=str(staged / "model"),
            serialization_format=SKLEARN_SERIALIZATION_FORMAT,
            skops_trusted_types=list(APPROVED_SKOPS_TRUSTED_TYPES),
        )

        # Validate the reserialized model on a named-feature input.
        probe = pd.DataFrame(np.zeros((1, len(FEATURE_NAMES))), columns=list(FEATURE_NAMES))
        restored = mlflow.sklearn.load_model(str(staged / "model"))
        np.testing.assert_allclose(
            model.predict_proba(probe), restored.predict_proba(probe),
            rtol=0, atol=1e-12,
        )
        model_digest = f"sha256:{sha256_tree(staged / 'model')}"
        baseline = None
        try:
            downloaded = client.download_artifacts(
                version.run_id,
                "metadata/monitoring_baseline.json",
                str(Path(tmp) / "downloaded"),
            )
            baseline = json.loads(Path(downloaded).read_text(encoding="utf-8"))
            baseline["identity"].update({
                "model_reference": reference,
                "model_digest": model_digest,
                "feature_schema_version": FEATURE_SCHEMA_VERSION,
            })
            (staged / "monitoring-baseline.json").write_text(
                json.dumps(baseline, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        except MlflowException:
            # Older locally trained candidates predate the baseline artifact.
            # They remain exportable, but model monitoring fails visibly until
            # `make model-monitor-baseline` creates the companion baseline.
            baseline = None
        manifest = {
            "model_name": model_name,
            "model_version": str(version.version),
            "model_reference": reference,
            "model_digest": model_digest,
            "feature_names": list(FEATURE_NAMES),
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "dataset_fingerprint": (
                baseline["identity"]["dataset_fingerprint"] if baseline else None
            ),
            "validation_metrics": baseline.get("validation_metrics") if baseline else None,
            "monitoring_baseline_available": baseline is not None,
        }
        (staged / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        staged.rename(output)

    print(f"Portable model exported: {output}")
    print("Model probability round-trip parity: OK")
    print("Do NOT commit the serialized model; load only trusted model artifacts.")


if __name__ == "__main__":
    main()
