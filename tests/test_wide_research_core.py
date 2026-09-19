from __future__ import annotations

import copy
import json
from dataclasses import FrozenInstanceError

import pytest

from wide_research import (
    ALLOWED_FEATURES,
    DEFAULT_UNIVERSE,
    FAIL,
    FEATURE_SCHEMA_VERSION,
    PASS,
    UNAVAILABLE,
    Clause,
    RuleManifest,
    UniverseSpec,
    evaluate_rule,
    evaluate_snapshot,
    evaluate_universe,
    extract_features,
    rule_manifest_from_dict,
)


OBSERVED_AT = "2026-08-27T12:00:00+00:00"


def _window(minutes: int, value: float = 2.0) -> dict:
    metrics = (
        "xg_total",
        "shots_on_target_total",
        "shots_in_box_total",
        "total_shots_total",
        "corners_total",
        "score_total",
        "pressure_index",
    )
    return {
        "status": "ok",
        "reason": None,
        "requested_window_minutes": minutes,
        "actual_span_minutes": minutes,
        "available_activity_metric_count": 6,
        "baseline_observation_id": f"100:{50 - minutes}:ROLLING_SEED:v1",
        "availability": {name: True for name in metrics},
        "deltas": {name: value for name in metrics},
        "rates_per_minute": {name: value / minutes for name in metrics},
    }


def _snapshot(*, stage: str = "decision_pipeline", minute: int = 50) -> dict:
    raw = {
        "xg_home": 1.2,
        "xg_away": 0.8,
        "shots_on_target_home": 5,
        "shots_on_target_away": 3,
        "corners_home": 4,
        "corners_away": 2,
    }
    return {
        "record_type": "observation",
        "observation_id": "100:50:WINDOW_1:BLOCK:v3",
        "fixture_id": 100,
        "created_at_utc": OBSERVED_AT,
        "stage": stage,
        "minute": minute,
        "match": {"score_home": 1, "score_away": 0},
        "probabilities": {
            "prob_next_15": 64.0,
            "prob_next_25": 72.0,
            "prob_to75": 70.0,
            "prob_to90": 82.0,
            "prob_until_end_decision": 82.0,
            "reputation_base_prob_next_15": 62.0,
            "reputation_adjusted_prob_next_15": 64.0,
            "reputation_base_prob_to90": 79.0,
            "reputation_adjusted_prob_to90": 82.0,
        },
        "features": {
            "pressure_index": 19.0,
            "adjusted_intensity": 0.7,
            "season_context_factor": 1.03,
            "goal_xg_gap": 1.0,
        },
        "raw_metrics": raw,
        "availability": {key: True for key in raw},
        "rolling_dynamics": {
            "schema_version": 1,
            "mode": "shadow_collection",
            "production_applied": False,
            "available_windows": 2,
            "windows": {"5m": _window(5), "10m": _window(10)},
        },
        "gates": {
            "readiness_passed": True,
            "publication_context_passed": True,
        },
        "publication_policy": {
            "publication_context_passed": True,
            "publication_allow": False,
        },
        "channel_signal_filter": {
            "passed": False,
            "prob_to90": 99.9,
        },
        "decision": {
            "final_decision": "BLOCK",
            "future_goal": True,
        },
        "telegram": {"send_ok": False},
        "outcome": {
            "status": "resolved",
            "goal_to90_normal_time": True,
        },
    }


def _prediction(snapshot: dict, *, rolling: bool, probability: float = 84.0) -> dict:
    return {
        "record_type": (
            "shadow_ml_rolling_prediction" if rolling else "shadow_ml_prediction"
        ),
        "observation_id": snapshot["observation_id"],
        "fixture_id": snapshot["fixture_id"],
        "minute": snapshot["minute"],
        "observation_created_at_utc": snapshot["created_at_utc"],
        "created_at_utc": "2026-08-27T12:00:02+00:00",
        "model_created_at_utc": "2026-08-27T11:00:00+00:00",
        "model_data_cutoff_utc": "2026-08-27T10:59:00+00:00",
        "shadow_only": True,
        "production_applied": False,
        "predictions": {
            "next15": {
                "status": "ok",
                "calibrated_probability_pct": probability - 10.0,
                "production_applied": False,
            },
            "to90": {
                "status": "ok",
                "calibrated_probability_pct": probability,
                "production_applied": False,
            },
        },
    }


def test_universe_covers_both_stages_and_ignores_current_filter() -> None:
    blocked = _snapshot()
    wide = _snapshot(stage="wide_monitor")

    assert evaluate_universe(blocked)["status"] == PASS
    assert evaluate_universe(wide)["status"] == PASS
    assert evaluate_universe(blocked)["inputs"]["current_filter_consulted"] is False
    assert DEFAULT_UNIVERSE.as_dict()["current_filter_policy"] == "ignored"


@pytest.mark.parametrize("minute", [46, 60])
def test_universe_minute_boundaries_are_inclusive(minute: int) -> None:
    assert evaluate_universe(_snapshot(minute=minute))["status"] == PASS


@pytest.mark.parametrize("minute", [45, 61])
def test_universe_rejects_minutes_outside_46_to_60(minute: int) -> None:
    assert evaluate_universe(_snapshot(minute=minute))["status"] == FAIL


def test_universe_missing_and_failed_technical_gates_are_not_eligible() -> None:
    missing = _snapshot()
    missing["gates"].pop("readiness_passed")
    failed = _snapshot()
    failed["publication_policy"]["publication_context_passed"] = False

    assert evaluate_universe(missing)["status"] == UNAVAILABLE
    assert evaluate_universe(failed)["status"] == FAIL


def test_manifest_is_immutable_canonical_and_json_serializable() -> None:
    left = RuleManifest(
        "high-pressure",
        "v1",
        (
            Clause("feature.pressure_index", ">=", 15),
            Clause("score.total_goals", "<=", 2.0),
        ),
    )
    right = RuleManifest(
        "high-pressure",
        "v1",
        (
            Clause("score.total_goals", "<=", 2),
            Clause("feature.pressure_index", ">=", 15.0),
        ),
    )

    assert left.manifest_hash == right.manifest_hash
    assert left.as_dict() == right.as_dict()
    json.dumps(left.as_dict(), allow_nan=False)
    with pytest.raises(FrozenInstanceError):
        left.version = "v2"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        left.clauses[0].value = 0.0  # type: ignore[misc]


def test_canonical_manifest_round_trip_and_checksum_rejection() -> None:
    manifest = RuleManifest(
        "round-trip",
        "v1",
        (Clause("bot.prob_to90", ">=", 80.0),),
    )
    assert rule_manifest_from_dict(manifest.as_dict()) == manifest
    changed = manifest.as_dict()
    changed["clauses"][0]["value"] = 81.0
    with pytest.raises(ValueError, match="checksum"):
        rule_manifest_from_dict(changed)
    missing = manifest.as_dict()
    missing.pop("manifest_hash")
    with pytest.raises(ValueError, match="checksum is required"):
        rule_manifest_from_dict(missing)


def test_pinned_feature_schema_round_trips_and_fails_closed() -> None:
    manifest = RuleManifest(
        "schema-pinned",
        "v2",
        (Clause("bot.prob_to90", ">=", 80.0),),
        schema_version=2,
        feature_schema_version=FEATURE_SCHEMA_VERSION,
    )

    assert rule_manifest_from_dict(manifest.as_dict()) == manifest
    assert manifest.as_dict()["feature_schema_version"] == (
        FEATURE_SCHEMA_VERSION
    )
    vector = extract_features(_snapshot())
    vector["schema_version"] = FEATURE_SCHEMA_VERSION + 1
    result = evaluate_rule(manifest, vector)

    assert result["status"] == UNAVAILABLE
    assert result["reason"] == "feature_schema_version_mismatch"
    assert result["feature_schema_compatible"] is False


def test_legacy_unpinned_manifest_keeps_its_original_checksum_contract() -> None:
    legacy = RuleManifest(
        "legacy-unpinned",
        "v1",
        (Clause("bot.prob_to90", ">=", 80.0),),
    )

    assert "feature_schema_version" not in legacy.as_dict()
    assert rule_manifest_from_dict(legacy.as_dict()) == legacy


@pytest.mark.parametrize(
    "feature",
    [
        "outcome.goal_to90_normal_time",
        "decision.final_decision",
        "telegram.send_ok",
        "tg.sent",
        "channel_signal_filter.passed",
        "gates.filter_passed",
    ],
)
def test_dsl_forbids_leaking_features(feature: str) -> None:
    with pytest.raises(ValueError, match="allowlist"):
        Clause(feature, "==", 1)


@pytest.mark.parametrize("op", [">", "<", "!=", "in"])
def test_dsl_has_only_three_supported_operators(op: str) -> None:
    with pytest.raises(ValueError, match="operator"):
        Clause("bot.prob_to90", op, 75)


def test_extractor_uses_only_positive_allowlist_and_ignores_leakage_sections() -> None:
    snapshot = _snapshot()
    before = copy.deepcopy(snapshot)
    first = extract_features(snapshot)
    snapshot["outcome"] = {"goal_to90_normal_time": False, "secret": 99999}
    snapshot["decision"] = {"final_decision": "ALLOW", "secret": -99999}
    snapshot["telegram"] = {"send_ok": True, "secret": 12345}
    snapshot["channel_signal_filter"] = {"passed": True, "prob_to90": 0.0}
    second = extract_features(snapshot)

    assert before["outcome"] != snapshot["outcome"]
    assert first == second
    assert set(first["values"]) == ALLOWED_FEATURES
    assert first["contracts"]["leakage_fields_consulted"] is False
    assert first["values"]["bot.prob_to90"] == 82.0
    assert first["values"]["reputation.delta_to90_pp"] == 3.0
    assert first["values"]["composite.base_quality_v1"] == 1.0
    assert first["values"]["rolling.both_windows_available_v1"] == 1.0
    json.dumps(first, allow_nan=False)


def test_raw_zero_is_available_but_missing_side_rejects_total() -> None:
    snapshot = _snapshot()
    snapshot["raw_metrics"]["corners_home"] = 0
    vector = extract_features(snapshot)
    assert vector["values"]["raw.corners_home"] == 0.0
    assert vector["values"]["raw.corners_total"] == 2.0

    snapshot["availability"]["corners_away"] = False
    unavailable = extract_features(snapshot)
    assert unavailable["values"]["raw.corners_away"] is None
    assert unavailable["values"]["raw.corners_total"] is None


def test_only_strictly_frozen_rolling_metrics_are_extracted() -> None:
    snapshot = _snapshot()
    good = extract_features(snapshot)
    assert good["contracts"]["rolling_5m_frozen"] is True
    assert good["values"]["rolling.5m.delta.xg_total"] == 2.0

    snapshot["rolling_dynamics"]["production_applied"] = True
    bad = extract_features(snapshot)
    assert bad["contracts"]["rolling_5m_frozen"] is False
    assert bad["values"]["rolling.5m.delta.xg_total"] is None
    assert bad["values"]["rolling.both_windows_available_v1"] == 0.0


def test_both_windows_composite_requires_complete_outer_contract() -> None:
    snapshot = _snapshot()
    snapshot["rolling_dynamics"]["available_windows"] = 1

    vector = extract_features(snapshot)

    assert vector["contracts"]["rolling_5m_frozen"] is True
    assert vector["contracts"]["rolling_10m_frozen"] is True
    assert vector["values"]["rolling.both_windows_available_v1"] == 0.0


def test_only_identity_matched_causal_shadow_ml_is_extracted() -> None:
    snapshot = _snapshot()
    static = _prediction(snapshot, rolling=False, probability=83.0)
    rolling = _prediction(snapshot, rolling=True, probability=87.0)
    vector = extract_features(
        snapshot,
        static_prediction=static,
        rolling_prediction=rolling,
    )
    assert vector["values"]["ml.static.prob_to90"] == 83.0
    assert vector["values"]["ml.rolling.prob_to90"] == 87.0
    assert vector["contracts"]["static_ml_causal"] is True
    assert vector["contracts"]["rolling_ml_causal"] is True

    future_trained = copy.deepcopy(rolling)
    future_trained["model_data_cutoff_utc"] = "2026-08-27T12:01:00+00:00"
    unavailable = extract_features(snapshot, rolling_prediction=future_trained)
    assert unavailable["values"]["ml.rolling.prob_to90"] is None
    assert unavailable["contracts"]["rolling_ml_causal"] is False


def test_clause_boundaries_pass_and_missing_is_unavailable() -> None:
    manifest = RuleManifest(
        "candidate-a",
        "v1",
        (
            Clause("bot.prob_to90", ">=", 82),
            Clause("score.total_goals", "<=", 1),
            Clause("minute", "==", 50),
        ),
    )
    vector = extract_features(_snapshot())
    passed = evaluate_rule(manifest, vector)
    assert passed["status"] == PASS
    assert passed["eligible"] is True

    vector["values"]["minute"] = None
    missing = evaluate_rule(manifest, vector)
    assert missing["status"] == UNAVAILABLE
    assert missing["eligible"] is False


def test_same_feature_bounds_round_trip_and_execute_as_closed_range() -> None:
    """Discovery ranges stay inside the existing, auditable AND-only DSL."""

    manifest = RuleManifest(
        "candidate-probability-band",
        "v1",
        (
            Clause("bot.prob_to90", ">=", 80),
            Clause("bot.prob_to90", "<=", 87),
        ),
    )
    restored = rule_manifest_from_dict(manifest.as_dict())
    assert restored.as_dict() == manifest.as_dict()

    for value in (80.0, 83.5, 87.0):
        vector = extract_features(_snapshot())
        vector["values"]["bot.prob_to90"] = value
        assert evaluate_rule(restored, vector)["status"] == PASS

    for value in (79.999, 87.001):
        vector = extract_features(_snapshot())
        vector["values"]["bot.prob_to90"] = value
        assert evaluate_rule(restored, vector)["status"] == FAIL

    missing = extract_features(_snapshot())
    missing["values"]["bot.prob_to90"] = None
    assert evaluate_rule(restored, missing)["status"] == UNAVAILABLE


def test_known_failed_clause_is_deterministic_even_with_another_missing() -> None:
    manifest = RuleManifest(
        "candidate-b",
        "v1",
        (
            Clause("bot.prob_to90", ">=", 90),
            Clause("ml.rolling.prob_to90", ">=", 80),
        ),
    )
    result = evaluate_rule(manifest, extract_features(_snapshot()))
    assert result["status"] == FAIL
    assert result["failed_features"] == ["bot.prob_to90"]
    assert result["unavailable_features"] == ["ml.rolling.prob_to90"]


def test_end_to_end_snapshot_evaluation_propagates_universe() -> None:
    manifest = RuleManifest(
        "candidate-c",
        "v1",
        (Clause("feature.adjusted_intensity", ">=", 0.7),),
    )
    snapshot = _snapshot()
    assert evaluate_snapshot(manifest, snapshot)["status"] == PASS

    snapshot["gates"]["readiness_passed"] = False
    rejected = evaluate_snapshot(manifest, snapshot)
    assert rejected["status"] == FAIL
    assert rejected["reason"] == "universe_failed"


def test_universe_cannot_be_configured_to_consult_current_filter() -> None:
    with pytest.raises(ValueError, match="ignore"):
        UniverseSpec(current_filter_policy="required")
