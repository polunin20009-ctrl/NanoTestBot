"""Strictly shadow-only machine-learning support for goal probabilities."""

from .model import (
    ALGORITHM_VERSION,
    ARTIFACT_SCHEMA_VERSION,
    FEATURE_SCHEMA_VERSION,
    ShadowMLConfig,
    prediction_input_fingerprint,
    predict_shadow,
    summarize_training_data,
    train_shadow_model,
)
from .storage import (
    append_prediction_record,
    iter_prediction_records,
    load_model,
    reset_prediction_cache,
    save_model,
)
from .rolling_model import (
    ROLLING_ALGORITHM_VERSION,
    ROLLING_ARTIFACT_ROLE,
    ROLLING_ARTIFACT_TYPE,
    ROLLING_FEATURE_PROFILE,
    predict_shadow_rolling,
    rolling_prediction_input_fingerprint,
    summarize_rolling_training_data,
    train_shadow_rolling_model,
)
from .champion import CandidateComparison, compare_holdout_metrics, save_candidate

__all__ = [
    "ALGORITHM_VERSION",
    "ARTIFACT_SCHEMA_VERSION",
    "FEATURE_SCHEMA_VERSION",
    "ROLLING_ALGORITHM_VERSION",
    "ROLLING_ARTIFACT_ROLE",
    "ROLLING_ARTIFACT_TYPE",
    "ROLLING_FEATURE_PROFILE",
    "ShadowMLConfig",
    "append_prediction_record",
    "iter_prediction_records",
    "load_model",
    "predict_shadow",
    "predict_shadow_rolling",
    "prediction_input_fingerprint",
    "rolling_prediction_input_fingerprint",
    "reset_prediction_cache",
    "save_model",
    "summarize_training_data",
    "summarize_rolling_training_data",
    "train_shadow_model",
    "train_shadow_rolling_model",
]
