#!/usr/bin/env python3
"""Build a small synthetic model bundle for clean-checkout CI only."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

from fraudwatch_ml.features import FEATURE_NAMES
from fraudwatch_ml.model_integrity import sha256_tree
from fraudwatch_ml.model_monitoring import build_monitoring_baseline
from fraudwatch_ml.model_serialization import SKLEARN_SERIALIZATION_FORMAT
from fraudwatch_ml.scoring import FEATURE_SCHEMA_VERSION


MODEL_NAME = "fraudwatch-hgb"
MODEL_REFERENCE = f"models:/{MODEL_NAME}/ci-fixture"
DATASET_FINGERPRINT = "sha256:" + "0" * 64


def build(output: Path) -> None:
    if output.exists():
        raise FileExistsError(f"Refusing to replace existing bundle: {output}")

    rng = np.random.default_rng(42)
    features = rng.normal(size=(2_000, len(FEATURE_NAMES)))
    labels = (
        features[:, 0] + 0.35 * features[:, 1] - 0.20 * features[:, 2] > 0.75
    ).astype(np.int8)
    frame = pd.DataFrame(features, columns=list(FEATURE_NAMES))
    model = LogisticRegression(random_state=42, max_iter=500).fit(frame, labels)

    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".ci-model-", dir=output.parent) as tmp:
        staged = Path(tmp) / "bundle"
        staged.mkdir()
        mlflow.sklearn.save_model(
            model,
            path=str(staged / "model"),
            serialization_format=SKLEARN_SERIALIZATION_FORMAT,
        )
        restored = mlflow.sklearn.load_model(str(staged / "model"))
        np.testing.assert_allclose(
            model.predict_proba(frame.iloc[:10]),
            restored.predict_proba(frame.iloc[:10]),
            rtol=0,
            atol=1e-12,
        )
        digest = f"sha256:{sha256_tree(staged / 'model')}"
        scores = model.predict_proba(frame)[:, 1]
        baseline = build_monitoring_baseline(
            features,
            scores,
            labels,
            model_reference=MODEL_REFERENCE,
            model_digest=digest,
            dataset_fingerprint=DATASET_FINGERPRINT,
            feature_schema_version=FEATURE_SCHEMA_VERSION,
        )
        (staged / "monitoring-baseline.json").write_text(
            json.dumps(baseline, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest = {
            "model_name": MODEL_NAME,
            "model_version": "ci-fixture",
            "model_reference": MODEL_REFERENCE,
            "model_digest": digest,
            "feature_names": list(FEATURE_NAMES),
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "dataset_fingerprint": DATASET_FINGERPRINT,
            "validation_metrics": baseline["validation_metrics"],
            "monitoring_baseline_available": True,
            "ci_fixture": True,
        }
        (staged / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        staged.rename(output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    build(args.output.resolve())
    print(f"CI model fixture created: {args.output}")


if __name__ == "__main__":
    main()
