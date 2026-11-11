from pathlib import Path

import mlflow
import mlflow.sklearn

from fraudwatch_ml.online_features import TerminalHistory
from fraudwatch_ml.scoring import FraudScorer, Transaction


ROOT = Path(__file__).resolve().parents[1]

tracking_uri = f"sqlite:///{ROOT / 'mlflow.db'}"
model_uri = "models:/fraudwatch-hgb@candidate"

mlflow.set_tracking_uri(tracking_uri)
mlflow.set_registry_uri(tracking_uri)

model = mlflow.sklearn.load_model(model_uri)

scorer = FraudScorer(
    model=model,
    model_reference=model_uri,
)

history = TerminalHistory()

transaction = Transaction(
    transaction_id=0,
    terminal_id="3156",
    timestamp="2018-04-01 00:00:31",
    amount=57.16,
)

result = scorer.score(transaction, history)

print("Fraud score:", result["fraud_score"])
print("Model:", result["model_reference"])
print("Feature schema:", result["feature_schema_version"])

assert 0.0 <= result["fraud_score"] <= 1.0

print("Registered model inference: PASSED")
