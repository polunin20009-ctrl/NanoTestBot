from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone

import shadow_ml.model as model


START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _observation(
    fixture_id: int,
    *,
    next15: bool | None = None,
    to90: bool | None = None,
    stage: str = "decision_pipeline",
    status: str = "resolved",
    omit_xg: bool = False,
) -> dict:
    positive = bool(fixture_id % 2)
    if next15 is None and status == "resolved":
        next15 = positive
    if to90 is None and status == "resolved":
        to90 = positive
    raw_metrics = {
        "shots_on_target_home": 7 if positive else 1,
        "shots_on_target_away": 3 if positive else 0,
        "shots_in_box_home": 9 if positive else 2,
        "shots_in_box_away": 4 if positive else 1,
    }
    if not omit_xg:
        raw_metrics["xg_home"] = 2.2 if positive else 0.0
        raw_metrics["xg_away"] = 0.8 if positive else 0.0
    created = START + timedelta(days=fixture_id, minutes=46)
    return {
        "record_type": "observation",
        "observation_id": f"{fixture_id}:46:WINDOW_1:v1",
        "observation_key": f"{fixture_id}:46:WINDOW_1:v1",
        "fixture_id": fixture_id,
        "created_at_utc": created.isoformat(),
        "stage": stage,
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
        "raw_metrics": raw_metrics,
        "availability": {
            key: True for key in raw_metrics
        },
        "data_quality": {
            "stats_health": "healthy",
            "xg_source": "api" if not omit_xg else "missing",
            "xg_confidence": 1.0 if not omit_xg else 0.0,
            "tempo_source": "derived",
            "tempo_confidence": 0.8,
            "available_metric_count": len(raw_metrics),
            "total_metric_count": 20,
        },
        "features": {
            "pressure_index": 24.0 if positive else 8.0,
            "save_stress": 0.9 if positive else 0.2,
            "tempo": 1.4 if positive else 0.5,
            "live_intensity": 0.9 if positive else 0.2,
            "adjusted_intensity": 0.92 if positive else 0.19,
            "lambda_2h": 0.07 if positive else 0.01,
            "game_state_factor": 1.0,
            "season_context_factor": 1.02,
            "sample_confidence": 0.8,
            "pressure_components": {
                "attempt": 0.9 if positive else 0.2,
                "corner": 0.8 if positive else 0.1,
            },
        },
        "probabilities": {
            "reputation_adjusted_prob_next_15": 50.0,
            "reputation_adjusted_prob_to90": 50.0,
            "prob_next_15": 50.0,
            "prob_to90": 50.0,
        },
        # These deliberately contain outcome-looking values. They must never
        # enter the feature extractor.
        "decision": {"final_decision": "ALLOW", "future_goal": not positive},
        "gates": {"secret_label": not positive},
        "telegram": {"send_ok": positive},
        "outcome": {
            "status": status,
            "goal_within_15": next15,
            "goal_within_15_quality": "exact" if next15 is not None else "unknown",
            "goal_to90_normal_time": to90,
            "normal_time_final_score_home": 99 if positive else 0,
            "resolved_at_utc": (created + timedelta(hours=1)).isoformat(),
        },
    }


def _small_config(**overrides) -> model.ShadowMLConfig:
    values = {
        "candidate_min_fixtures": 12,
        "candidate_min_positive_fixtures": 2,
        "candidate_min_negative_fixtures": 2,
        "offline_ready_min_fixtures": 18,
        "offline_ready_min_rows": 18,
        "offline_ready_min_class_fixtures": 2,
        "offline_ready_min_span_days": 0.0,
        "offline_ready_min_calibration_fixtures": 2,
        "offline_ready_min_holdout_fixtures": 2,
        "offline_ready_min_positive_fixtures": 1,
        "offline_ready_min_negative_fixtures": 1,
        "category_min_fixtures": 1,
        "max_epochs": 90,
        "convergence_patience": 5,
        "platt_min_positive_fixtures": 1,
        "platt_min_negative_fixtures": 1,
        "offline_ready_max_next15_unknown_fraction": 0.02,
        "offline_ready_min_core_availability": 0.85,
        "readiness_min_logloss_improvement": 0.001,
        "readiness_min_brier_improvement": 0.0005,
    }
    values.update(overrides)
    return model.ShadowMLConfig(**values)


def _training_rows(count: int = 20) -> list[dict]:
    return [_observation(fixture_id) for fixture_id in range(1, count + 1)]


def _set_feature_contract(
    record: dict,
    *,
    model_version: str,
    probability_version: str,
    additional_factor_versions: dict | None = None,
    schema_version: int = 1,
    code_hash: str = "ignored-code",
    config_hash: str = "ignored-config",
) -> dict:
    factor_versions = {
        "probability": probability_version,
        "pressure": "v3_smooth",
    }
    factor_versions.update(additional_factor_versions or {})
    record["schema_version"] = schema_version
    record["config"] = {
        "model_version": model_version,
        "factor_versions": factor_versions,
        "code_hash": code_hash,
        "config_hash": config_hash,
    }
    return record


def test_training_summary_filters_rows_and_skips_unknown_next15() -> None:
    rows = [
        _observation(1),
        _observation(2, next15=True),
        _observation(3, stage="prefilter"),
        _observation(6, stage="rolling_seed"),
        _observation(4, status="pending"),
        _observation(5),
    ]
    # A stale boolean must still be ignored when label quality says unknown.
    rows[1]["outcome"]["goal_within_15_quality"] = "unknown"
    rows[5]["minute"] = 61

    summary = model.summarize_training_data(rows)

    assert summary["records_seen"] == 6
    assert summary["eligible_records"] == 2
    assert summary["unique_fixtures"] == 2
    assert summary["excluded"]["stage"] == 2
    assert summary["excluded"]["status"] == 1
    assert summary["excluded"]["minute"] == 1
    assert summary["targets"]["next15"]["usable_rows"] == 1
    assert summary["targets"]["next15"]["unknown_labels_skipped"] == 1
    assert summary["targets"]["to90"]["usable_rows"] == 2
    assert summary["data_quality"]["next15"]["accepted_rows"] == 1
    assert summary["data_quality"]["next15"]["unknown_or_invalid_rows"] == 1
    assert (
        summary["data_quality"]["next15"]["unknown_or_invalid_fraction"]
        == 0.5
    )
    assert (
        summary["data_quality"]["core_live_metrics"]["availability_fraction"]
        == 1.0
    )


def test_current_feature_contract_ignores_additive_rolling_collection_block() -> None:
    observation = _observation(1)
    before = model._feature_values(observation)
    observation["rolling_dynamics"] = {
        "schema_version": 1,
        "production_applied": False,
        "windows": {
            "5m": {
                "deltas": {"total_shots_total": 999.0},
                "unexpected": "must-not-be-auto-learned",
            }
        },
    }

    assert model._feature_values(observation) == before


def test_feature_contract_ignores_selection_and_presentation_versions() -> None:
    baseline = _set_feature_contract(
        _observation(1),
        model_version="45_plus_v2",
        probability_version="current_v2",
    )
    old_publication = _set_feature_contract(
        _observation(1),
        model_version="45_plus_v2",
        probability_version="current_v2",
        additional_factor_versions={
            "channel_signal_filter": "old_filter",
            "dynamic_threshold": "old_threshold",
            "premium_badge": "old_badge",
            "rescue_controller": "old_rescue",
        },
    )
    new_publication = _set_feature_contract(
        _observation(1),
        model_version="45_plus_v2",
        probability_version="current_v2",
        additional_factor_versions={
            "channel_signal_filter": "new_filter",
            "dynamic_threshold": "new_threshold",
            "premium_badge": "new_badge",
            "rescue_controller": "retired",
        },
    )

    contract = model._feature_contract(baseline)

    assert model._feature_contract(old_publication) == contract
    assert model._feature_contract(new_publication) == contract
    assert contract["payload"]["contract_schema_version"] == 2
    assert not (
        set(contract["payload"]["factor_versions"])
        & model._NON_PREDICTIVE_FACTOR_VERSION_KEYS
    )


def test_feature_contract_keeps_unknown_and_predictive_factor_versions() -> None:
    baseline = _set_feature_contract(
        _observation(1),
        model_version="45_plus_v2",
        probability_version="current_v2",
        additional_factor_versions={"signal_reputation": "v1"},
    )
    baseline_contract = model._feature_contract(baseline)

    changed_versions = (
        {"probability": "future_v3"},
        {"pressure": "future_v4"},
        {"signal_reputation": "future_v2"},
        {"future_predictive_factor": "v1"},
    )
    for changes in changed_versions:
        changed = copy.deepcopy(baseline)
        changed["config"]["factor_versions"].update(changes)
        assert model._feature_contract(changed)["key"] != (
            baseline_contract["key"]
        )


def test_publication_version_changes_do_not_split_training_cohort() -> None:
    old_rows = [
        _set_feature_contract(
            _observation(fixture_id),
            model_version="45_plus_v2",
            probability_version="current_v2",
            additional_factor_versions={
                "dynamic_threshold": "minute_bucket_v1",
                "rescue_controller": "v1",
            },
        )
        for fixture_id in range(1, 11)
    ]
    new_rows = [
        _set_feature_contract(
            _observation(fixture_id),
            model_version="45_plus_v2",
            probability_version="current_v2",
            additional_factor_versions={
                "channel_signal_filter": "base_p90_75_v1",
                "dynamic_threshold": "minute_bucket_v2",
                "premium_badge": "goals_le2_v1",
                "rescue_controller": "retired",
            },
        )
        for fixture_id in range(101, 111)
    ]

    summary = model.summarize_training_data(old_rows + new_rows)

    assert summary["eligible_records_before_contract_filter"] == 20
    assert summary["eligible_records"] == 20
    assert summary["feature_contract_cohort_count"] == 1
    assert summary["incompatible_rows_excluded"] == 0


def test_next15_quality_policy_and_inferred_weight_are_strict() -> None:
    rows = [_observation(fixture_id) for fixture_id in range(1, 8)]
    qualities = (
        "exact",
        "score_confirmed",
        "inferred",
        "unknown",
        None,
        "unexpected_quality",
        "exact",
    )
    for record, quality in zip(rows, qualities):
        if quality is None:
            record["outcome"].pop("goal_within_15_quality")
        else:
            record["outcome"]["goal_within_15_quality"] = quality
    rows[-1]["outcome"]["goal_within_15"] = "not-a-boolean"

    summary = model.summarize_training_data(rows)
    eligible = model._materialize_eligible(rows)
    samples = model._build_target_samples(eligible, "next15")

    assert summary["targets"]["next15"]["usable_rows"] == 3
    assert summary["data_quality"]["next15"] == {
        "rows_evaluated": 7,
        "accepted_rows": 3,
        "unknown_or_invalid_rows": 4,
        "unknown_or_invalid_fraction": 0.571428571,
        "quality_counts": {
            "exact": 1,
            "score_confirmed": 1,
            "inferred": 1,
            "unknown": 1,
            "missing": 1,
            "unrecognized": 1,
            "invalid_value": 1,
        },
    }
    assert [sample["record_id"] for sample in samples] == [
        "1:46:WINDOW_1:v1",
        "2:46:WINDOW_1:v1",
        "3:46:WINDOW_1:v1",
    ]
    assert [sample["quality_weight"] for sample in samples] == [1.0, 1.0, 0.5]
    assert model._fixture_weights(samples) == [1.0, 1.0, 0.5]


def test_fixture_split_is_chronological_and_has_no_leakage() -> None:
    rows = _training_rows(20)
    # Add another minute for every fixture. A fixture must still occur in only
    # one partition.
    for fixture_id in range(1, 21):
        duplicate_minute = copy.deepcopy(rows[fixture_id - 1])
        duplicate_minute["observation_id"] = f"{fixture_id}:47:WINDOW_1:v1"
        duplicate_minute["observation_key"] = duplicate_minute["observation_id"]
        duplicate_minute["minute"] = 47
        duplicate_minute["created_at_utc"] = (
            START + timedelta(days=fixture_id, minutes=47)
        ).isoformat()
        rows.append(duplicate_minute)

    eligible = model._materialize_eligible(rows)
    split = model._split_fixture_groups(eligible, _small_config())

    assert split["counts"] == {
        "train": 14,
        "calibration": 3,
        "holdout": 3,
    }
    assert split["train"].isdisjoint(split["calibration"])
    assert split["train"].isdisjoint(split["holdout"])
    assert split["calibration"].isdisjoint(split["holdout"])
    assert max(split["train"]) < min(split["calibration"])
    assert max(split["calibration"]) < min(split["holdout"])


def test_model_is_deterministic_json_safe_and_trains_both_targets() -> None:
    rows = _training_rows(20)
    config = _small_config()
    now = "2026-03-01T00:00:00+00:00"

    first = model.train_shadow_model(rows, config=config, now=now)
    second = model.train_shadow_model(rows, config=config, now=now)

    assert first == second
    json.dumps(first, allow_nan=False)
    assert first["schema_version"] == 1
    assert first["algorithm_version"] == "residual_logistic_stdlib_v1"
    assert first["production_applied"] is False
    assert first["training_contract"]["key"] == (
        model.LEGACY_FEATURE_CONTRACT_KEY
    )
    assert first["training_contract"]["payload"]["kind"] == "legacy_or_missing"
    assert len(first["checksum_sha256"]) == 64
    for target in ("next15", "to90"):
        target_model = first["targets"][target]
        assert target_model["trained"] is True
        assert target_model["production_applied"] is False
        assert target_model["status"] == "offline_ready"
        assert target_model["metrics"]["holdout"]["baseline"]["log_loss"] is not None
        assert target_model["metrics"]["holdout"]["candidate"]["log_loss"] is not None
        assert len(target_model["model"]["coefficients"]) == len(
            target_model["preprocessor"]["feature_names"]
        )
        assert (
            target_model["metrics"]["train"]["candidate"][
                "effective_fixture_weight"
            ]
            == target_model["counts"]["train"]["fixtures"]
        )


def test_decision_gates_and_telegram_cannot_change_training_features() -> None:
    rows = _training_rows(20)
    changed = copy.deepcopy(rows)
    for index, record in enumerate(changed):
        record["decision"] = {
            "final_decision": "BLOCK",
            "outcome": 1_000_000 + index,
        }
        record["gates"] = {"goal_to90": bool(index % 3)}
        record["telegram"] = {"send_ok": False, "message_id": index}
    config = _small_config()
    now = "2026-03-01T00:00:00+00:00"

    original_model = model.train_shadow_model(rows, config=config, now=now)
    changed_model = model.train_shadow_model(changed, config=config, now=now)

    assert original_model == changed_model


def test_missing_value_has_separate_indicator_from_real_zero() -> None:
    rows = _training_rows(20)
    # Missing examples are present in train so the schema observes both states.
    for fixture_id in (2, 4, 6):
        rows[fixture_id - 1]["raw_metrics"].pop("xg_home", None)
        rows[fixture_id - 1]["availability"]["xg_home"] = False
    artifact = model.train_shadow_model(
        rows,
        config=_small_config(),
        now="2026-03-01T00:00:00+00:00",
    )
    feature_names = artifact["targets"]["next15"]["preprocessor"]["feature_names"]

    assert "num:raw.xg_home" in feature_names
    assert "missing:raw.xg_home" in feature_names

    real_zero = _observation(100, status="pending")
    real_zero["raw_metrics"]["xg_home"] = 0.0
    real_zero["availability"]["xg_home"] = True
    missing = copy.deepcopy(real_zero)
    missing["raw_metrics"].pop("xg_home")
    missing["availability"]["xg_home"] = False

    zero_prediction = model.predict_shadow(artifact, real_zero)
    missing_prediction = model.predict_shadow(artifact, missing)

    assert zero_prediction["status"] == "ok"
    assert missing_prediction["status"] == "ok"
    assert (
        missing_prediction["targets"]["next15"]["missing_numeric_count"]
        == zero_prediction["targets"]["next15"]["missing_numeric_count"] + 1
    )


def test_collecting_artifact_is_not_trained_and_prediction_is_fail_safe() -> None:
    artifact = model.train_shadow_model(
        _training_rows(20),
        now="2026-03-01T00:00:00+00:00",
    )

    assert artifact["status"] == "collecting"
    assert artifact["production_applied"] is False
    assert artifact["targets"]["next15"]["trained"] is False

    prediction = model.predict_shadow(
        artifact,
        _observation(101, status="pending"),
    )

    assert prediction["status"] == "not_trained"
    assert prediction["production_applied"] is False
    assert prediction["targets"]["next15"]["status"] == "not_trained"


def test_predict_rejects_corrupt_artifact_and_invalid_observation() -> None:
    artifact = model.train_shadow_model(
        _training_rows(20),
        config=_small_config(),
        now="2026-03-01T00:00:00+00:00",
    )
    corrupt = copy.deepcopy(artifact)
    corrupt["targets"]["next15"]["model"]["intercept"] = 999.0

    invalid_artifact = model.predict_shadow(
        corrupt,
        _observation(101, status="pending"),
    )
    invalid_observation = model.predict_shadow(artifact, None)  # type: ignore[arg-type]

    assert invalid_artifact == {
        "status": "invalid_artifact",
        "production_applied": False,
        "targets": {},
        "detail": "schema_or_checksum",
    }
    assert invalid_observation["status"] == "invalid_observation"
    assert invalid_observation["production_applied"] is False


def test_probability_value_one_is_interpreted_as_one_percent() -> None:
    artifact = model.train_shadow_model(
        _training_rows(20),
        config=_small_config(),
        now="2026-03-01T00:00:00+00:00",
    )
    observation = _observation(101, status="pending")
    observation["probabilities"]["reputation_adjusted_prob_next_15"] = 1.0

    prediction = model.predict_shadow(artifact, observation)

    assert prediction["targets"]["next15"]["base_probability_pct"] == 1.0


def test_offline_readiness_requires_rows_and_both_fixture_classes() -> None:
    defaults = model.ShadowMLConfig()
    assert defaults.offline_ready_min_rows == 3000
    assert defaults.offline_ready_min_class_fixtures == 100

    artifact = model.train_shadow_model(
        _training_rows(20),
        config=_small_config(
            offline_ready_min_rows=100,
            offline_ready_min_class_fixtures=20,
        ),
        now="2026-03-01T00:00:00+00:00",
    )

    for target in ("next15", "to90"):
        target_model = artifact["targets"][target]
        assert target_model["trained"] is True
        assert target_model["status"] == "collecting"
        assert "offline_min_rows" in target_model["readiness_reasons"]
        assert (
            "offline_positive_class_fixtures"
            in target_model["readiness_reasons"]
        )
        assert (
            "offline_negative_class_fixtures"
            in target_model["readiness_reasons"]
        )


def test_next15_unknown_fraction_blocks_only_next15_offline_readiness() -> None:
    rows = _training_rows(20)
    rows[0]["outcome"]["goal_within_15_quality"] = "unknown"

    artifact = model.train_shadow_model(
        rows,
        config=_small_config(),
        now="2026-03-01T00:00:00+00:00",
    )

    next15 = artifact["targets"]["next15"]
    to90 = artifact["targets"]["to90"]
    assert next15["trained"] is True
    assert next15["status"] == "collecting"
    assert "offline_next15_unknown_fraction" in next15["readiness_reasons"]
    assert (
        next15["data_quality"]["next15"]["unknown_or_invalid_fraction"]
        == 0.05
    )
    assert "offline_next15_unknown_fraction" not in to90["readiness_reasons"]
    assert to90["status"] == "offline_ready"


def test_low_core_live_metric_availability_blocks_both_targets() -> None:
    rows = _training_rows(20)
    for record in rows:
        record["raw_metrics"].pop("xg_home")
        record["raw_metrics"].pop("xg_away")
        record["availability"]["xg_home"] = False
        record["availability"]["xg_away"] = False

    artifact = model.train_shadow_model(
        rows,
        config=_small_config(),
        now="2026-03-01T00:00:00+00:00",
    )

    quality = artifact["training_summary"]["data_quality"]["core_live_metrics"]
    assert quality["availability_fraction"] == 0.75
    assert quality["per_metric"]["xg"]["availability_fraction"] == 0.0
    for target in ("next15", "to90"):
        target_model = artifact["targets"][target]
        assert target_model["trained"] is True
        assert target_model["status"] == "collecting"
        assert (
            "offline_core_live_metric_availability"
            in target_model["readiness_reasons"]
        )


def test_offline_readiness_requires_real_holdout_improvement() -> None:
    defaults = model.ShadowMLConfig()
    assert defaults.readiness_min_logloss_improvement == 0.001
    assert defaults.readiness_min_brier_improvement == 0.0005

    artifact = model.train_shadow_model(
        _training_rows(20),
        config=_small_config(
            readiness_min_logloss_improvement=1.0,
            readiness_min_brier_improvement=1.0,
        ),
        now="2026-03-01T00:00:00+00:00",
    )

    for target in ("next15", "to90"):
        target_model = artifact["targets"][target]
        assert target_model["trained"] is True
        assert target_model["status"] == "collecting"
        assert "offline_logloss_improvement" in target_model["readiness_reasons"]
        assert "offline_brier_improvement" in target_model["readiness_reasons"]
        assert target_model["holdout_improvement"]["log_loss"] < 1.0
        assert target_model["holdout_improvement"]["brier"] < 1.0


def test_training_data_hash_tracks_labels_quality_and_matches_artifact() -> None:
    rows = _training_rows(20)
    original = model.summarize_training_data(rows)

    label_changed = copy.deepcopy(rows)
    label_changed[0]["outcome"]["goal_within_15"] = False
    changed_label_summary = model.summarize_training_data(label_changed)

    quality_changed = copy.deepcopy(rows)
    quality_changed[0]["outcome"]["goal_within_15_quality"] = "inferred"
    changed_quality_summary = model.summarize_training_data(quality_changed)

    schema_changed = copy.deepcopy(rows)
    schema_changed[0]["outcome"]["outcome_schema_version"] = 2
    changed_schema_summary = model.summarize_training_data(schema_changed)

    assert original["training_data_hash"] != (
        changed_label_summary["training_data_hash"]
    )
    assert original["training_data_hash"] != (
        changed_quality_summary["training_data_hash"]
    )
    assert original["training_data_hash"] != (
        changed_schema_summary["training_data_hash"]
    )

    artifact = model.train_shadow_model(
        rows,
        config=_small_config(),
        now="2026-03-01T00:00:00+00:00",
    )
    assert artifact["training_data_hash"] == original["training_data_hash"]
    assert (
        artifact["training_data_hash"]
        == artifact["training_summary"]["training_data_hash"]
    )


def test_newest_feature_contract_is_selected_and_incompatible_rows_excluded() -> None:
    old_rows = [
        _set_feature_contract(
            _observation(fixture_id),
            model_version="45_plus_v1",
            probability_version="legacy_v1",
        )
        for fixture_id in range(1, 11)
    ]
    new_rows = [
        _set_feature_contract(
            _observation(fixture_id),
            model_version="45_plus_v2",
            probability_version="current_v2",
            # Deployment hashes must not create extra feature cohorts.
            code_hash=f"code-{fixture_id}",
            config_hash=f"config-{fixture_id}",
        )
        for fixture_id in range(101, 121)
    ]
    rows = old_rows + new_rows

    summary = model.summarize_training_data(rows)
    artifact = model.train_shadow_model(
        rows,
        config=_small_config(),
        now="2026-06-01T00:00:00+00:00",
    )

    assert summary["eligible_records_before_contract_filter"] == 30
    assert summary["eligible_records"] == 20
    assert summary["incompatible_rows_excluded"] == 10
    assert summary["feature_contract_cohort_count"] == 2
    assert summary["incompatible_cohort_count"] == 1
    assert summary["excluded"]["feature_contract"] == 10
    assert summary["training_contract"]["payload"] == {
        "contract_schema_version": 2,
        "kind": "versioned",
        "observation_schema_version": 1,
        "model_version": "45_plus_v2",
        "factor_versions": {
            "pressure": "v3_smooth",
            "probability": "current_v2",
        },
    }
    assert artifact["training_contract"] == summary["training_contract"]
    assert artifact["training_summary"]["eligible_records"] == 20
    assert artifact["targets"]["next15"]["counts"]["all"]["rows"] == 20
    assert artifact["targets"]["to90"]["counts"]["all"]["rows"] == 20


def test_predict_rejects_feature_contract_mismatch_but_ignores_hashes() -> None:
    rows = [
        _set_feature_contract(
            _observation(fixture_id),
            model_version="45_plus_v2",
            probability_version="current_v2",
            additional_factor_versions={
                "dynamic_threshold": "old_threshold",
                "rescue_controller": "v1",
            },
        )
        for fixture_id in range(1, 21)
    ]
    artifact = model.train_shadow_model(
        rows,
        config=_small_config(),
        now="2026-03-01T00:00:00+00:00",
    )
    compatible = _set_feature_contract(
        _observation(101, status="pending"),
        model_version="45_plus_v2",
        probability_version="current_v2",
        additional_factor_versions={
            "channel_signal_filter": "new_filter",
            "dynamic_threshold": "new_threshold",
            "premium_badge": "new_badge",
            "rescue_controller": "retired",
        },
        code_hash="a-completely-new-deployment",
        config_hash="a-different-runtime-config",
    )
    incompatible = _set_feature_contract(
        _observation(101, status="pending"),
        model_version="45_plus_v2",
        probability_version="future_v3",
    )

    compatible_prediction = model.predict_shadow(artifact, compatible)
    incompatible_prediction = model.predict_shadow(artifact, incompatible)

    assert compatible_prediction["status"] == "ok"
    assert incompatible_prediction["status"] == "invalid_observation"
    assert incompatible_prediction["reason"] == "feature_contract_mismatch"
    assert incompatible_prediction["production_applied"] is False
    assert incompatible_prediction["expected_feature_contract"] == (
        artifact["training_contract"]["key"]
    )
    assert (
        incompatible_prediction["actual_feature_contract"]
        != incompatible_prediction["expected_feature_contract"]
    )
