"""Leakage-safe core primitives for broad prospective rule research."""

from .features import (
    ALLOWED_FEATURE_NAMES,
    ALLOWED_FEATURES,
    DEFAULT_MAX_PREDICTION_LAG_SECONDS,
    FEATURE_SCHEMA_VERSION,
    extract_feature_vector,
    extract_features,
    evaluate_universe,
)
from .rules import (
    SUPPORTED_OPERATORS,
    Clause,
    RuleManifest,
    evaluate_rule,
    evaluate_snapshot,
    rule_manifest_from_dict,
    universe_spec_from_dict,
)
from .schema import (
    DECISION_PIPELINE,
    DEFAULT_UNIVERSE,
    FAIL,
    PASS,
    SCHEMA_VERSION,
    UNAVAILABLE,
    WIDE_MONITOR,
    UniverseSpec,
    canonical_hash,
    canonical_json,
)

__all__ = [
    "ALLOWED_FEATURE_NAMES",
    "ALLOWED_FEATURES",
    "Clause",
    "DECISION_PIPELINE",
    "DEFAULT_MAX_PREDICTION_LAG_SECONDS",
    "DEFAULT_UNIVERSE",
    "FAIL",
    "FEATURE_SCHEMA_VERSION",
    "PASS",
    "RuleManifest",
    "SCHEMA_VERSION",
    "SUPPORTED_OPERATORS",
    "UNAVAILABLE",
    "UniverseSpec",
    "WIDE_MONITOR",
    "canonical_hash",
    "canonical_json",
    "evaluate_rule",
    "evaluate_snapshot",
    "evaluate_universe",
    "extract_feature_vector",
    "extract_features",
    "rule_manifest_from_dict",
    "universe_spec_from_dict",
]
