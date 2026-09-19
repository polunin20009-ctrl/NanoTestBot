from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone

import shadow_ml.model as core
import shadow_ml.rolling_model as rolling


START = datetime(2026, 7, 1, tzinfo=timezone.utc)
METRICS = (
    "xg_total",
    "shots_on_target_total",
    "shots_in_box_total",
    "total_shots_total",
    "corners_total",
    "score_total",
    "pressure_index",
)


def _window(minutes: int, value: float, *, status: str = "ok") -> dict:
    available = status == "ok"
    return {
        "status": status,
        "actual_span_minutes": minutes if available else None,
        "sample_count": 3,
        "crosses_halftime": minutes == 10 if available else None,
        "baseline_observation_id": "must-never-be-a-feature",
        "baseline_stage": "rolling_seed",
        "invalid_reasons": {"secret": "must-never-be-a-feature"},
        "availability": {name: available for name in METRICS},
        "deltas": {
            name: (value if available else 9999.0) for name in METRICS
        },
        "rates_per_minute": {
            name: (value / minutes if available else 9999.0)
            for name in METRICS
        },
    }


def _observation(
    fixture_id: int,
    *,
    pending: bool = False,
    unavailable_10m: bool = False,
) -> dict:
    positive = bool(fixture_id % 2)
    created = START + timedelta(days=fixture_id, minutes=46)
    value = 2.0 if positive else 0.0
    return {
        "record_type": "observation",
        "observation_id": f"{fixture_id}:46:WINDOW_1:v1",
        "observation_key": f"{fixture_id}:46:WINDOW_1:v1",
        "fixture_id": fixture_id,
        "created_at_utc": created.isoformat(),
        "stage": "decision_pipeline",
        "minute": 46,
        "window_name": "WINDOW_1",
        "match": {
            "score_home": 0,
            "score_away": 0,
            "score_state": "0-0",
            "league_type": "League",
            "is_cup": False,
            "is_cup_source": "persisted_league",
        },
        "raw_metrics": {
            "xg_home": value,
            "xg_away": value / 2,
            "shots_on_target_home": 6 if positive else 1,
            "shots_on_target_away": 3 if positive else 0,
            "shots_in_box_home": 8 if positive else 2,
            "shots_in_box_away": 4 if positive else 1,
        },
        "availability": {
            "xg_home": True,
            "xg_away": True,
            "shots_on_target_home": True,
            "shots_on_target_away": True,
            "shots_in_box_home": True,
            "shots_in_box_away": True,
        },
        "data_quality": {
            "stats_health": "healthy",
            "xg_source": "api",
            "xg_confidence": 1.0,
            "tempo_source": "derived",
            "tempo_confidence": 0.8,
            "available_metric_count": 6,
            "total_metric_count": 20,
        },
        "features": {
            "pressure_index": 25.0 if positive else 7.0,
            "save_stress": 0.9 if positive else 0.1,
            "tempo": 1.4 if positive else 0.4,
            "live_intensity": 0.9 if positive else 0.2,
            "adjusted_intensity": 0.9 if positive else 0.2,
            "lambda_2h": 0.08 if positive else 0.01,
            "game_state_factor": 1.0,
            "sample_confidence": 0.8,
        },
        "probabilities": {
            "prob_next_15": 50.0,
            "prob_to90": 50.0,
        },
        "rolling_dynamics": {
            "schema_version": 1,
            "mode": "shadow_collection",
            "production_applied": False,
            "windows": {
                "5m": _window(5, value),
                "10m": _window(
                    10,
                    value * 1.5,
                    status="unavailable" if unavailable_10m else "ok",
                ),
            },
        },
        "decision": {"future_goal": not positive},
        "gates": {"secret_label": not positive},
        "telegram": {"send_ok": positive},
        "outcome": {
            "status": "pending" if pending else "resolved",
            "goal_within_15": positive,
            "goal_within_15_quality": "exact",
            "goal_to90_normal_time": positive,
        },
    }


def _config() -> core.ShadowMLConfig:
    return core.ShadowMLConfig(
        candidate_min_fixtures=12,
        candidate_min_positive_fixtures=2,
        candidate_min_negative_fixtures=2,
        offline_ready_min_fixtures=18,
        offline_ready_min_rows=18,
        offline_ready_min_class_fixtures=2,
        offline_ready_min_span_days=0.0,
        offline_ready_min_calibration_fixtures=2,
        offline_ready_min_holdout_fixtures=2,
        offline_ready_min_positive_fixtures=1,
        offline_ready_min_negative_fixtures=1,
        category_min_fixtures=1,
        max_epochs=80,
        convergence_patience=5,
        platt_min_positive_fixtures=1,
        platt_min_negative_fixtures=1,
        offline_ready_max_next15_unknown_fraction=0.02,
        offline_ready_min_core_availability=0.85,
    )


def test_rolling_extractor_is_allowlisted_and_distinguishes_zero_from_missing() -> None:
    observation = _observation(2, pending=True, unavailable_10m=True)
    numeric, categorical = rolling.rolling_feature_values(observation)

    assert numeric["rolling.5m.delta.total_shots_total"] == 0.0
    assert numeric["rolling.5m.rate_per_minute.total_shots_total"] == 0.0
    assert numeric["rolling.5m.available.total_shots_total"] == 1.0
    assert numeric["rolling.10m.window_available"] == 0.0
    assert numeric["rolling.10m.available.total_shots_total"] == 0.0
    assert "rolling.10m.delta.total_shots_total" not in numeric
    assert categorical["rolling.10m.status"] == "unavailable"
    serialized = json.dumps([numeric, categorical], sort_keys=True)
    for forbidden in (
        "baseline_observation_id",
        "baseline_stage",
        "invalid_reasons",
        "future_goal",
        "secret_label",
        "telegram",
        "sample_count",
    ):
        assert forbidden not in serialized


def test_summary_keeps_unavailable_windows_but_rejects_invalid_blocks() -> None:
    valid = _observation(1, unavailable_10m=True)
    missing = _observation(2)
    missing.pop("rolling_dynamics")
    wrong_schema = _observation(3)
    wrong_schema["rolling_dynamics"]["schema_version"] = 999

    summary = rolling.summarize_rolling_training_data(
        [valid, missing, wrong_schema]
    )

    assert summary["eligible_records"] == 1
    assert summary["unique_fixtures"] == 1
    assert summary["excluded"]["rolling_missing"] == 1
    assert summary["excluded"]["rolling_schema"] == 1
    coverage = summary["data_quality"]["rolling_dynamics"]
    assert coverage["windows"]["5m"]["available_rows"] == 1
    assert coverage["windows"]["10m"]["available_rows"] == 0
    assert coverage["at_least_one_window"]["rows"] == 1


def test_rolling_revision_changes_hash_while_incumbent_ignores_block() -> None:
    rows = [_observation(fixture_id) for fixture_id in range(1, 21)]
    changed = copy.deepcopy(rows)
    changed[0]["rolling_dynamics"]["windows"]["5m"]["deltas"][
        "total_shots_total"
    ] += 1.0

    original_summary = rolling.summarize_rolling_training_data(rows)
    changed_summary = rolling.summarize_rolling_training_data(changed)
    assert original_summary["training_data_hash"] != changed_summary[
        "training_data_hash"
    ]

    raw_changed = copy.deepcopy(rows)
    raw_changed[0]["raw_metrics"]["xg_home"] += 0.25
    assert original_summary["training_data_hash"] != (
        rolling.summarize_rolling_training_data(raw_changed)[
            "training_data_hash"
        ]
    )

    probability_changed = copy.deepcopy(rows)
    probability_changed[0]["probabilities"]["prob_to90"] = 63.0
    assert original_summary["training_data_hash"] != (
        rolling.summarize_rolling_training_data(probability_changed)[
            "training_data_hash"
        ]
    )

    now = "2026-08-01T00:00:00+00:00"
    assert core.train_shadow_model(rows, config=_config(), now=now) == (
        core.train_shadow_model(changed, config=_config(), now=now)
    )


def test_challenger_is_deterministic_separate_and_predicts_both_targets() -> None:
    rows = [_observation(fixture_id) for fixture_id in range(1, 21)]
    first = rolling.train_shadow_rolling_model(
        rows,
        config=_config(),
        now="2026-08-01T00:00:00+00:00",
    )
    second = rolling.train_shadow_rolling_model(
        rows,
        config=_config(),
        now="2026-08-01T00:00:00+00:00",
    )

    assert first == second
    json.dumps(first, allow_nan=False)
    assert first["artifact_type"] == rolling.ROLLING_ARTIFACT_TYPE
    assert first["artifact_role"] == rolling.ROLLING_ARTIFACT_ROLE
    assert first["algorithm_version"] == rolling.ROLLING_ALGORITHM_VERSION
    assert first["feature_profile"] == rolling.ROLLING_FEATURE_PROFILE
    assert first["production_applied"] is False
    assert first["model_id"].startswith("shadow_ml_rolling:")
    for target in core.TARGET_NAMES:
        names = first["targets"][target]["preprocessor"]["feature_names"]
        assert any(name.startswith("num:rolling.5m.") for name in names)
        assert any(name.startswith("num:rolling.10m.") for name in names)

    observation = _observation(101, pending=True)
    prediction = rolling.predict_shadow_rolling(first, observation)
    assert prediction["status"] == "ok"
    assert prediction["production_applied"] is False
    assert prediction["targets"]["next15"]["status"] == "ok"
    assert prediction["targets"]["to90"]["status"] == "ok"

    incumbent = core.train_shadow_model(
        rows,
        config=_config(),
        now="2026-08-01T00:00:00+00:00",
    )
    assert core.predict_shadow(first, observation)["status"] == "invalid_artifact"
    assert rolling.predict_shadow_rolling(incumbent, observation)[
        "status"
    ] == "invalid_artifact"


def test_zero_rolling_coverage_can_never_be_offline_ready() -> None:
    rows = [_observation(fixture_id) for fixture_id in range(1, 21)]
    for record in rows:
        record["rolling_dynamics"]["windows"] = {
            "5m": _window(5, 0.0, status="unavailable"),
            "10m": _window(10, 0.0, status="unavailable"),
        }

    artifact = rolling.train_shadow_rolling_model(
        rows,
        config=_config(),
        now="2026-08-01T00:00:00+00:00",
    )

    assert artifact["status"] == "collecting"
    for target in core.TARGET_NAMES:
        result = artifact["targets"][target]
        assert result["trained"] is True
        assert result["status"] == "collecting"
        assert "offline_rolling_both_windows_fixtures" in result[
            "readiness_reasons"
        ]
        assert "offline_rolling_5m_coverage" in result["readiness_reasons"]
        assert "offline_rolling_10m_coverage" in result["readiness_reasons"]


def test_contract_rejects_missing_rolling_schema_and_ignores_deploy_hashes() -> None:
    first = _observation(1)
    second = copy.deepcopy(first)
    for index, record in enumerate((first, second)):
        record["schema_version"] = 1
        record["config"] = {
            "model_version": "45_plus_v2",
            "factor_versions": {"probability": "v2", "pressure": "v3"},
            "code_hash": f"code-{index}",
            "config_hash": f"config-{index}",
        }
    first["config"]["factor_versions"].update(
        {
            "dynamic_threshold": "old_threshold",
            "rescue_controller": "v1",
        }
    )
    second["config"]["factor_versions"].update(
        {
            "channel_signal_filter": "new_filter",
            "dynamic_threshold": "new_threshold",
            "premium_badge": "new_badge",
            "rescue_controller": "retired",
        }
    )
    assert rolling.rolling_feature_contract(first) == (
        rolling.rolling_feature_contract(second)
    )

    predictive_change = copy.deepcopy(second)
    predictive_change["config"]["factor_versions"]["pressure"] = "v4"
    assert rolling.rolling_feature_contract(predictive_change)["key"] != (
        rolling.rolling_feature_contract(first)["key"]
    )

    invalid = copy.deepcopy(first)
    invalid["rolling_dynamics"]["schema_version"] = 2
    contract = rolling.rolling_feature_contract(invalid)
    assert contract["key"] == ""
    assert contract["error"] == "rolling_schema"
