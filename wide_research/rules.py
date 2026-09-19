from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .features import ALLOWED_FEATURES, extract_features
from .extended_features import EXTENDED_FEATURES
from .schema import (
    DEFAULT_UNIVERSE,
    FAIL,
    PASS,
    UNAVAILABLE,
    UniverseSpec,
    canonical_hash,
)

# Kept local to this module to make the DSL deliberately tiny and auditable.
SUPPORTED_OPERATORS = (">=", "<=", "==")


def _finite(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return 0.0 if number == 0.0 else number


@dataclass(frozen=True)
class Clause:
    feature: str
    op: str
    value: float

    def __post_init__(self) -> None:
        feature = str(self.feature or "").strip()
        if feature not in ALLOWED_FEATURES and feature not in EXTENDED_FEATURES:
            raise ValueError(f"feature is not in the research allowlist: {feature!r}")
        if self.op not in SUPPORTED_OPERATORS:
            raise ValueError(f"unsupported clause operator: {self.op!r}")
        value = _finite(self.value)
        if value is None:
            raise ValueError("clause value must be a finite number")
        object.__setattr__(self, "feature", feature)
        object.__setattr__(self, "value", value)

    def as_dict(self) -> dict[str, Any]:
        return {"feature": self.feature, "op": self.op, "value": self.value}


@dataclass(frozen=True)
class RuleManifest:
    rule_id: str
    version: str
    clauses: tuple[Clause, ...]
    universe: UniverseSpec = DEFAULT_UNIVERSE
    schema_version: int = 1
    semantics: str = "all_clauses_and_missing_reject_v1"
    feature_schema_version: int | None = None

    def __post_init__(self) -> None:
        rule_id = str(self.rule_id or "").strip()
        version = str(self.version or "").strip()
        if not rule_id or not version:
            raise ValueError("rule_id and version must be non-empty")
        object.__setattr__(self, "rule_id", rule_id)
        object.__setattr__(self, "version", version)
        if type(self.schema_version) is not int or self.schema_version < 1:
            raise ValueError("schema_version must be a positive integer")
        if (
            self.feature_schema_version is not None
            and (
                type(self.feature_schema_version) is not int
                or self.feature_schema_version < 1
            )
        ):
            raise ValueError(
                "feature_schema_version must be a positive integer or null"
            )
        if self.semantics != "all_clauses_and_missing_reject_v1":
            raise ValueError("unsupported rule semantics")
        if not isinstance(self.universe, UniverseSpec):
            raise TypeError("universe must be a UniverseSpec")

        normalized: list[Clause] = []
        for clause in self.clauses:
            if isinstance(clause, Clause):
                normalized.append(clause)
            elif isinstance(clause, Mapping):
                normalized.append(
                    Clause(
                        feature=clause.get("feature"),
                        op=clause.get("op"),
                        value=clause.get("value"),
                    )
                )
            else:
                raise TypeError("every clause must be Clause or a clause mapping")
        normalized.sort(key=lambda item: (item.feature, item.op, item.value))
        canonical_keys = [
            (clause.feature, clause.op, clause.value) for clause in normalized
        ]
        if len(canonical_keys) != len(set(canonical_keys)):
            raise ValueError("duplicate clauses are not allowed")
        object.__setattr__(self, "clauses", tuple(normalized))

    def payload(self) -> dict[str, Any]:
        payload = {
            "schema_version": self.schema_version,
            "rule_id": self.rule_id,
            "version": self.version,
            "semantics": self.semantics,
            "universe": self.universe.as_dict(),
            "clauses": [clause.as_dict() for clause in self.clauses],
        }
        # Legacy manifests did not pin the extractor schema.  Omitting the
        # field for those records preserves their historical checksum; every
        # newly discovered rule supplies it explicitly and is fail-closed.
        if self.feature_schema_version is not None:
            payload["feature_schema_version"] = self.feature_schema_version
        return payload

    @property
    def manifest_hash(self) -> str:
        return canonical_hash(self.payload())

    def as_dict(self) -> dict[str, Any]:
        payload = self.payload()
        payload["manifest_hash"] = self.manifest_hash
        return payload

    manifest = as_dict


def universe_spec_from_dict(payload: Mapping[str, Any]) -> UniverseSpec:
    """Rebuild a universe contract from its canonical manifest payload."""

    if not isinstance(payload, Mapping):
        raise TypeError("universe payload must be a mapping")
    minute_range = payload.get("minute_range")
    minute_range = minute_range if isinstance(minute_range, Mapping) else {}
    technical = payload.get("technical_eligibility")
    technical = technical if isinstance(technical, Mapping) else {}
    stages = payload.get("stages", DEFAULT_UNIVERSE.stages)
    if isinstance(stages, (str, bytes)):
        raise ValueError("universe stages must be a sequence")
    return UniverseSpec(
        universe_id=payload.get("universe_id", DEFAULT_UNIVERSE.universe_id),
        schema_version=payload.get("schema_version", DEFAULT_UNIVERSE.schema_version),
        stages=tuple(stages),
        min_minute=minute_range.get(
            "min_inclusive", DEFAULT_UNIVERSE.min_minute
        ),
        max_minute=minute_range.get(
            "max_inclusive", DEFAULT_UNIVERSE.max_minute
        ),
        require_readiness_passed=technical.get(
            "require_readiness_passed",
            DEFAULT_UNIVERSE.require_readiness_passed,
        ),
        require_publication_context_passed=technical.get(
            "require_publication_context_passed",
            DEFAULT_UNIVERSE.require_publication_context_passed,
        ),
        current_filter_policy=payload.get(
            "current_filter_policy", DEFAULT_UNIVERSE.current_filter_policy
        ),
    )


def rule_manifest_from_dict(
    payload: Mapping[str, Any],
    *,
    verify_hash: bool = True,
) -> RuleManifest:
    """Parse and optionally checksum-verify a canonical rule manifest."""

    if not isinstance(payload, Mapping):
        raise TypeError("rule manifest must be a mapping")
    raw_clauses = payload.get("clauses")
    if not isinstance(raw_clauses, Sequence) or isinstance(
        raw_clauses, (str, bytes)
    ):
        raise ValueError("rule clauses must be a sequence")
    parsed_clauses = [
        Clause(
            feature=clause.get("feature"),
            op=clause.get("op"),
            value=clause.get("value"),
        )
        for clause in raw_clauses
        if isinstance(clause, Mapping)
    ]
    if len(parsed_clauses) != len(raw_clauses):
        raise ValueError("every rule clause must be a mapping")
    universe_payload = payload.get("universe")
    manifest = RuleManifest(
        rule_id=payload.get("rule_id"),
        version=payload.get("version"),
        clauses=tuple(parsed_clauses),
        universe=universe_spec_from_dict(
            universe_payload
            if isinstance(universe_payload, Mapping)
            else DEFAULT_UNIVERSE.as_dict()
        ),
        schema_version=payload.get("schema_version", 1),
        semantics=payload.get(
            "semantics", "all_clauses_and_missing_reject_v1"
        ),
        feature_schema_version=payload.get("feature_schema_version"),
    )
    supplied_hash = str(payload.get("manifest_hash") or "")
    if verify_hash:
        if not supplied_hash:
            raise ValueError("rule manifest checksum is required")
        if supplied_hash != manifest.manifest_hash:
            raise ValueError("rule manifest checksum mismatch")
    return manifest


def _feature_values(feature_vector: Mapping[str, Any]) -> Mapping[str, Any]:
    nested = feature_vector.get("values")
    return nested if isinstance(nested, Mapping) else feature_vector


def _clause_passes(actual: float, clause: Clause) -> bool:
    if clause.op == ">=":
        return actual >= clause.value
    if clause.op == "<=":
        return actual <= clause.value
    return actual == clause.value


def evaluate_rule(
    manifest: RuleManifest,
    feature_vector: Mapping[str, Any],
    *,
    universe_evaluation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate an immutable AND rule with three-valued, missing-safe logic."""
    if not isinstance(manifest, RuleManifest):
        raise TypeError("manifest must be RuleManifest")
    vector_schema = (
        feature_vector.get("schema_version")
        if isinstance(feature_vector, Mapping)
        else None
    )
    feature_schema_compatible = bool(
        manifest.feature_schema_version is None
        or (
            type(vector_schema) is int
            and vector_schema == manifest.feature_schema_version
        )
    )
    values = _feature_values(feature_vector) if isinstance(feature_vector, Mapping) else {}
    evaluations: list[dict[str, Any]] = []
    failed: list[str] = []
    unavailable: list[str] = []
    for clause in manifest.clauses:
        actual = _finite(values.get(clause.feature))
        if actual is None:
            status = UNAVAILABLE
            unavailable.append(clause.feature)
        elif _clause_passes(actual, clause):
            status = PASS
        else:
            status = FAIL
            failed.append(clause.feature)
        evaluations.append(
            {
                **clause.as_dict(),
                "actual": actual,
                "status": status,
            }
        )

    universe_status = (
        str(universe_evaluation.get("status") or "")
        if isinstance(universe_evaluation, Mapping)
        else PASS
    )
    if universe_status == FAIL:
        status = FAIL
        reason = "universe_failed"
    elif universe_status != PASS:
        status = UNAVAILABLE
        reason = "universe_unavailable"
    elif not feature_schema_compatible:
        status = UNAVAILABLE
        reason = "feature_schema_version_mismatch"
    elif failed:
        status = FAIL
        reason = f"clause_failed:{failed[0]}"
    elif unavailable:
        status = UNAVAILABLE
        reason = f"feature_unavailable:{unavailable[0]}"
    else:
        status = PASS
        reason = "pass"

    return {
        "schema_version": 1,
        "rule_id": manifest.rule_id,
        "rule_version": manifest.version,
        "manifest_hash": manifest.manifest_hash,
        "expected_feature_schema_version": manifest.feature_schema_version,
        "actual_feature_schema_version": vector_schema,
        "feature_schema_compatible": feature_schema_compatible,
        "status": status,
        "eligible": status == PASS,
        "reason": reason,
        "failed_features": failed,
        "unavailable_features": unavailable,
        "clauses": evaluations,
    }


def evaluate_snapshot(
    manifest: RuleManifest,
    snapshot: Mapping[str, Any],
    *,
    static_prediction: Mapping[str, Any] | None = None,
    rolling_prediction: Mapping[str, Any] | None = None,
    max_prediction_lag_seconds: float = 300.0,
    include_extended: bool = False,
) -> dict[str, Any]:
    """Convenience path that applies universe, extraction, then the rule."""
    # Local import avoids coupling schema types back to feature extraction.
    from .features import evaluate_universe

    universe = evaluate_universe(snapshot, manifest.universe)
    vector = extract_features(
        snapshot,
        static_prediction=static_prediction,
        rolling_prediction=rolling_prediction,
        max_prediction_lag_seconds=max_prediction_lag_seconds,
        include_extended=include_extended,
    )
    result = evaluate_rule(
        manifest,
        vector,
        universe_evaluation=universe,
    )
    return {**result, "universe": universe, "feature_schema_version": vector["schema_version"]}
