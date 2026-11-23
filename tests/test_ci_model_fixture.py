import json

import mlflow.sklearn
import pandas as pd

from fraudwatch_ml.features import FEATURE_NAMES
from scripts.build_ci_model_fixture import build


def test_ci_model_fixture_is_portable_and_refuses_overwrite(tmp_path):
    output = tmp_path / "candidate-model"
    build(output)

    manifest = json.loads((output / "manifest.json").read_text())
    model = mlflow.sklearn.load_model(str(output / "model"))
    probabilities = model.predict_proba(
        pd.DataFrame([[0.0] * len(FEATURE_NAMES)], columns=list(FEATURE_NAMES))
    )

    assert manifest["ci_fixture"] is True
    assert manifest["model_name"] == "fraudwatch-hgb"
    assert manifest["monitoring_baseline_available"] is True
    assert probabilities.shape == (1, 2)

    try:
        build(output)
    except FileExistsError:
        pass
    else:
        raise AssertionError("CI fixture builder replaced an existing bundle")
