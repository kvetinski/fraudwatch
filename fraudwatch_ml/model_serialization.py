"""Reviewed serialization policy for trusted FraudWatch sklearn models."""

from typing import Final


SKLEARN_SERIALIZATION_FORMAT: Final = "skops"

# Keep this allowlist explicit. A new estimator family must fail closed until
# its reported internal types have been reviewed and intentionally added here.
APPROVED_SKOPS_TRUSTED_TYPES: Final = (
    "sklearn.ensemble._hist_gradient_boosting.predictor.TreePredictor",
)
