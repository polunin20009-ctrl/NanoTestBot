from __future__ import annotations

import copy

import NanoTest as bot


def _pending_observation() -> dict:
    return {
        "record_type": "observation",
        "observation_id": "101:46:WINDOW_1:v2",
        "observation_key": "101:46:WINDOW_1:v2",
        "fixture_id": 101,
        "created_at_utc": "2026-07-25T01:00:00+00:00",
        "schema_version": 1,
        "stage": "decision_pipeline",
        "minute": 46,
        "probabilities": {
            "prob_next_15": 55.0,
            "prob_to90": 80.0,
        },
        "outcome": {"status": "pending"},
    }


def test_prediction_is_separate_and_never_mutates_observation_or_production(
    monkeypatch,
) -> None:
    observation = _pending_observation()
    before = copy.deepcopy(observation)
    captured: list[dict] = []
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_model_cached",
        lambda: {
            "model_id": "shadow:test",
            "algorithm_version": "residual_logistic_stdlib_v1",
            "feature_schema_version": 1,
            "created_at_utc": "2026-07-25T00:00:00+00:00",
            "data_cutoff_utc": "2026-07-24T23:00:00+00:00",
            "status": "collecting",
        },
    )
    monkeypatch.setattr(
        bot,
        "predict_shadow",
        lambda artifact, row: {
            "status": "ok",
            "model_id": "shadow:test",
            "algorithm_version": "residual_logistic_stdlib_v1",
            "production_applied": False,
            "targets": {
                "next15": {
                    "status": "ok",
                    "base_probability_pct": 55.0,
                    "calibrated_probability_pct": 57.0,
                    "delta_pp": 2.0,
                    "production_applied": False,
                },
                "to90": {
                    "status": "ok",
                    "base_probability_pct": 80.0,
                    "calibrated_probability_pct": 79.0,
                    "delta_pp": -1.0,
                    "production_applied": False,
                },
            },
        },
    )

    def capture_append(path, record, *, rotate_max_bytes):
        captured.append(record)
        return True

    monkeypatch.setattr(
        bot,
        "append_shadow_ml_prediction_record",
        capture_append,
    )

    assert bot.predict_and_append_shadow_ml(observation) is True

    assert observation == before
    assert captured[0]["shadow_only"] is True
    assert captured[0]["production_applied"] is False
    assert captured[0]["prediction_key"] == (
        "prediction:101:46:WINDOW_1:v2:shadow:test"
    )
    assert captured[0]["predictions"]["next15"][
        "calibrated_probability_pct"
    ] == 57.0


def test_untrained_shadow_model_writes_no_prediction(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_model_cached",
        lambda: {"model_id": "shadow:collecting"},
    )
    monkeypatch.setattr(
        bot,
        "predict_shadow",
        lambda artifact, row: {
            "status": "not_trained",
            "targets": {
                "next15": {"status": "not_trained"},
                "to90": {"status": "not_trained"},
            },
        },
    )
    monkeypatch.setattr(
        bot,
        "append_shadow_ml_prediction_record",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("prediction journal must not be touched")
        ),
    )

    assert bot.predict_and_append_shadow_ml(_pending_observation()) is False


def test_rolling_challenger_writes_only_its_separate_journal(monkeypatch) -> None:
    observation = _pending_observation()
    before = copy.deepcopy(observation)
    captured: list[tuple[str, dict, int]] = []
    rolling_path = "/tmp/shadow_ml_rolling_predictions.test.jsonl"
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML_ROLLING_CHALLENGER", True)
    monkeypatch.setattr(bot, "SHADOW_ML_ROLLING_PREDICTIONS_FILE", rolling_path)
    monkeypatch.setattr(
        bot,
        "load_shadow_rolling_model_cached",
        lambda: {
            "model_id": "shadow_ml_rolling:test",
            "algorithm_version": "residual_logistic_stdlib_rolling_v2",
            "feature_schema_version": 2,
            "feature_profile": "rolling_5m10m_v1",
            "artifact_role": "rolling_challenger",
            "status": "collecting",
        },
    )
    monkeypatch.setattr(
        bot,
        "predict_shadow_rolling",
        lambda artifact, row: {
            "status": "ok",
            "model_id": "shadow_ml_rolling:test",
            "algorithm_version": "residual_logistic_stdlib_rolling_v2",
            "production_applied": False,
            "targets": {
                "next15": {
                    "status": "ok",
                    "calibrated_probability_pct": 58.0,
                    "production_applied": False,
                },
                "to90": {
                    "status": "ok",
                    "calibrated_probability_pct": 81.0,
                    "production_applied": False,
                },
            },
        },
    )
    monkeypatch.setattr(
        bot,
        "append_shadow_ml_prediction_record",
        lambda path, record, *, rotate_max_bytes: (
            captured.append((path, record, rotate_max_bytes)) or True
        ),
    )

    assert bot.predict_and_append_shadow_rolling_ml(observation) is True
    assert observation == before
    assert captured[0][0] == rolling_path
    record = captured[0][1]
    assert record["record_type"] == "shadow_ml_rolling_prediction"
    assert record["artifact_role"] == "rolling_challenger"
    assert record["feature_profile"] == "rolling_5m10m_v1"
    assert record["prediction_key"].startswith("rolling_prediction:")
    assert record["shadow_only"] is True
    assert record["production_applied"] is False


def test_rolling_retrain_hint_survives_when_static_ml_is_disabled(
    monkeypatch,
) -> None:
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", False)
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML_ROLLING_CHALLENGER", True)
    monkeypatch.setattr(bot, "_shadow_ml_pending_fixture_ids", set())
    monkeypatch.setattr(bot, "_shadow_rolling_pending_fixture_ids", set())
    monkeypatch.setattr(bot, "_shadow_ml_data_revision_pending", False)
    monkeypatch.setattr(bot, "_shadow_rolling_data_revision_pending", False)
    bot._shadow_ml_retrain_event.clear()

    bot.note_shadow_ml_outcome(987, data_revision=True)

    assert bot._shadow_ml_pending_fixture_ids == set()
    assert bot._shadow_rolling_pending_fixture_ids == {987}
    assert bot._shadow_ml_data_revision_pending is False
    assert bot._shadow_rolling_data_revision_pending is True
    assert bot._shadow_ml_retrain_event.is_set()
    bot._shadow_ml_retrain_event.clear()


def test_prefilter_observation_is_not_scored_by_v1_model(monkeypatch) -> None:
    observation = _pending_observation()
    observation["stage"] = "prefilter"
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_model_cached",
        lambda: (_ for _ in ()).throw(
            AssertionError("model must not be loaded for prefilter")
        ),
    )

    assert bot.predict_and_append_shadow_ml(observation) is False


def test_rolling_seed_is_not_scored_by_v1_model(monkeypatch) -> None:
    observation = _pending_observation()
    observation["stage"] = "rolling_seed"
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_model_cached",
        lambda: (_ for _ in ()).throw(
            AssertionError("model must not be loaded for a rolling seed")
        ),
    )

    assert bot.predict_and_append_shadow_ml(observation) is False


def test_forced_training_persists_collecting_artifact(monkeypatch) -> None:
    saved: list[tuple[str, dict]] = []
    artifact = {
        "artifact_type": "shadow_ml_model",
        "model_id": "shadow:empty",
        "status": "collecting",
        "production_applied": False,
        "training_summary": {"unique_fixtures": 0},
    }
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(
        bot,
        "load_joined_observation_history",
        lambda include_pending=False: [],
    )
    monkeypatch.setattr(
        bot,
        "summarize_shadow_ml_training_data",
        lambda records: {
            "eligible_records": 0,
            "unique_fixtures": 0,
            "span_days": 0.0,
        },
    )
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_model_cached",
        lambda force=False: {},
    )
    monkeypatch.setattr(
        bot,
        "train_shadow_model",
        lambda records, config, now: artifact,
    )
    monkeypatch.setattr(
        bot,
        "_shadow_ml_artifact_is_safe",
        lambda value: True,
    )
    monkeypatch.setattr(
        bot,
        "save_shadow_ml_model_file",
        lambda path, value: saved.append((path, value)),
    )
    monkeypatch.setattr(bot, "_set_shadow_ml_model_cache", lambda value: None)

    result = bot._train_shadow_ml_once_local(force=True, trigger="test")

    assert result["status"] == "trained"
    assert result["model_status"] == "collecting"
    assert saved[0][1]["production_applied"] is False


def test_feature_contract_change_resets_collecting_artifact_immediately(
    monkeypatch,
) -> None:
    saved: list[dict] = []
    current = {
        "artifact_type": "shadow_ml_model",
        "created_at_utc": "2026-07-25T00:00:00+00:00",
        "training_contract": {"key": "old-contract"},
        "training_summary": {"unique_fixtures": 500},
        "production_applied": False,
    }
    replacement = {
        "artifact_type": "shadow_ml_model",
        "model_id": "shadow:new-contract",
        "status": "collecting",
        "training_contract": {"key": "new-contract"},
        "training_summary": {"unique_fixtures": 1},
        "production_applied": False,
    }
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(
        bot,
        "load_joined_observation_history",
        lambda include_pending=False: [{"fixture_id": 1}],
    )
    monkeypatch.setattr(
        bot,
        "summarize_shadow_ml_training_data",
        lambda records: {
            "eligible_records": 1,
            "unique_fixtures": 1,
            "span_days": 0.0,
            "training_contract": {"key": "new-contract"},
        },
    )
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_model_cached",
        lambda force=False: current,
    )
    monkeypatch.setattr(
        bot,
        "train_shadow_model",
        lambda records, config, now: replacement,
    )
    monkeypatch.setattr(
        bot,
        "_shadow_ml_artifact_is_safe",
        lambda value: True,
    )
    monkeypatch.setattr(
        bot,
        "save_shadow_ml_model_file",
        lambda path, value: saved.append(value),
    )
    monkeypatch.setattr(bot, "_set_shadow_ml_model_cache", lambda value: None)

    result = bot._train_shadow_ml_once_local(
        force=False,
        trigger="contract-change-test",
    )

    assert result["status"] == "trained"
    assert result["feature_contract_changed"] is True
    assert saved[0]["model_id"] == "shadow:new-contract"


def test_production_applied_artifact_is_rejected() -> None:
    assert bot._shadow_ml_artifact_is_safe(
        {
            "artifact_type": "shadow_ml_model",
            "production_applied": True,
        }
    ) is False


def test_outcome_wakeup_is_debounced_before_full_history_scan(
    monkeypatch,
) -> None:
    calls: list[dict] = []
    monkeypatch.setattr(bot, "_shadow_ml_pending_fixture_ids", set())
    monkeypatch.setattr(bot, "_shadow_ml_known_new_fixtures", 0)
    monkeypatch.setattr(bot, "_shadow_ml_data_revision_pending", False)
    monkeypatch.setattr(
        bot, "_shadow_ml_contract_mismatch_pending", False
    )
    monkeypatch.setattr(
        bot, "_shadow_ml_noop_scan_not_before_monotonic", 0.0
    )
    monkeypatch.setattr(bot, "SHADOW_ML_MIN_NEW_FIXTURES", 25)
    monkeypatch.setattr(bot, "SHADOW_ML_WEEKLY_MIN_NEW_FIXTURES", 5)
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_model_cached",
        lambda: {"model_id": "existing"},
    )
    monkeypatch.setattr(
        bot,
        "_shadow_ml_model_age_seconds",
        lambda artifact: float(bot.SHADOW_ML_RETRAIN_CHECK_SECONDS + 1),
    )

    def train_stub(**kwargs):
        calls.append(kwargs)
        return {
            "status": "skipped",
            "new_fixtures": 24,
            "reason": "no_retrain_threshold",
        }

    monkeypatch.setattr(bot, "train_shadow_ml_once", train_stub)

    bot.note_shadow_ml_outcome(1)
    first = bot.process_shadow_ml_retrain_wakeup(trigger="test")

    assert first["reason"] == "debounced_before_history_scan"
    assert calls == []

    for fixture_id in range(2, 26):
        bot.note_shadow_ml_outcome(fixture_id)
    second = bot.process_shadow_ml_retrain_wakeup(trigger="test")

    assert second["status"] == "skipped"
    assert len(calls) == 1


def test_data_revision_bypasses_fixture_growth_debounce(
    monkeypatch,
) -> None:
    calls: list[dict] = []
    monkeypatch.setattr(bot, "_shadow_ml_pending_fixture_ids", set())
    monkeypatch.setattr(bot, "_shadow_ml_known_new_fixtures", 0)
    monkeypatch.setattr(bot, "_shadow_ml_data_revision_pending", False)
    monkeypatch.setattr(
        bot, "_shadow_ml_contract_mismatch_pending", False
    )
    monkeypatch.setattr(
        bot,
        "_shadow_ml_noop_scan_not_before_monotonic",
        bot.time.monotonic() + 3600.0,
    )
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_model_cached",
        lambda: {"model_id": "existing"},
    )
    monkeypatch.setattr(
        bot,
        "_shadow_ml_model_age_seconds",
        lambda artifact: 0.0,
    )

    def train_stub(**kwargs):
        calls.append(kwargs)
        return {
            "status": "trained",
            "new_fixtures": 0,
        }

    monkeypatch.setattr(bot, "train_shadow_ml_once", train_stub)

    bot.note_shadow_ml_outcome(101, data_revision=True)
    result = bot.process_shadow_ml_retrain_wakeup(
        trigger="revision-test"
    )

    assert result["status"] == "trained"
    assert calls[0]["data_revision_hint"] is True


def test_contract_mismatch_bypasses_active_noop_cooldown(
    monkeypatch,
) -> None:
    calls: list[dict] = []
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(bot, "_shadow_ml_pending_fixture_ids", {101})
    monkeypatch.setattr(bot, "_shadow_ml_known_new_fixtures", 0)
    monkeypatch.setattr(bot, "_shadow_ml_data_revision_pending", False)
    monkeypatch.setattr(bot, "_shadow_ml_contract_mismatch_pending", True)
    monkeypatch.setattr(
        bot,
        "_shadow_ml_noop_scan_not_before_monotonic",
        bot.time.monotonic() + 3600.0,
    )
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_model_cached",
        lambda: {"model_id": "old-contract"},
    )

    def train_stub(**kwargs):
        calls.append(kwargs)
        return {"status": "trained", "new_fixtures": 1}

    monkeypatch.setattr(bot, "train_shadow_ml_once", train_stub)

    result = bot.process_shadow_ml_retrain_wakeup(
        trigger="contract-mismatch"
    )

    assert result["status"] == "trained"
    assert len(calls) == 1
    assert bot._shadow_ml_contract_mismatch_pending is False
    assert bot._shadow_ml_noop_scan_not_before_monotonic == 0.0


def test_noop_scan_cooldown_suppresses_rescan_and_preserves_pending(
    monkeypatch,
) -> None:
    calls: list[dict] = []
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML_ROLLING_CHALLENGER", False)
    monkeypatch.setattr(bot, "_shadow_ml_pending_fixture_ids", set())
    monkeypatch.setattr(bot, "_shadow_ml_known_new_fixtures", 20)
    monkeypatch.setattr(bot, "_shadow_ml_data_revision_pending", False)
    monkeypatch.setattr(
        bot, "_shadow_ml_contract_mismatch_pending", False
    )
    monkeypatch.setattr(
        bot, "_shadow_ml_noop_scan_not_before_monotonic", 0.0
    )
    monkeypatch.setattr(bot, "SHADOW_ML_MIN_NEW_FIXTURES", 25)
    monkeypatch.setattr(bot, "SHADOW_ML_NOOP_SCAN_COOLDOWN_SECONDS", 3600)
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_model_cached",
        lambda: {"model_id": "existing"},
    )
    monkeypatch.setattr(
        bot,
        "_shadow_ml_model_age_seconds",
        lambda artifact: float(bot.SHADOW_ML_RETRAIN_CHECK_SECONDS + 1),
    )

    def train_stub(**kwargs):
        calls.append(kwargs)
        return {
            "status": "skipped",
            "reason": "no_retrain_threshold",
            "new_fixtures": 20,
        }

    monkeypatch.setattr(bot, "train_shadow_ml_once", train_stub)

    for fixture_id in range(1, 6):
        bot.note_shadow_ml_outcome(fixture_id)
    first = bot.process_shadow_ml_retrain_wakeup(trigger="first")
    assert first["reason"] == "no_retrain_threshold"
    assert len(calls) == 1
    assert bot._shadow_ml_noop_scan_not_before_monotonic > bot.time.monotonic()

    for fixture_id in range(6, 11):
        bot.note_shadow_ml_outcome(fixture_id)
    suppressed = bot.process_shadow_ml_retrain_wakeup(trigger="suppressed")
    assert suppressed["reason"] == "noop_scan_cooldown"
    assert suppressed["pending_fixtures"] == 25
    assert bot._shadow_ml_pending_fixture_ids == set(range(6, 11))
    assert len(calls) == 1

    bot._shadow_ml_noop_scan_not_before_monotonic = 0.0
    after_cooldown = bot.process_shadow_ml_retrain_wakeup(
        trigger="after-cooldown"
    )
    assert after_cooldown["reason"] == "no_retrain_threshold"
    assert len(calls) == 2


def test_static_and_rolling_noop_cooldowns_are_independent(
    monkeypatch,
) -> None:
    static_calls: list[dict] = []
    rolling_calls: list[dict] = []
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(bot, "SHADOW_ML_AUTO_RETRAIN", True)
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML_ROLLING_CHALLENGER", True)
    monkeypatch.setattr(bot, "SHADOW_ML_ROLLING_AUTO_RETRAIN", True)
    monkeypatch.setattr(bot, "_shadow_ml_pending_fixture_ids", set())
    monkeypatch.setattr(bot, "_shadow_rolling_pending_fixture_ids", set())
    monkeypatch.setattr(bot, "_shadow_ml_known_new_fixtures", 20)
    monkeypatch.setattr(bot, "_shadow_rolling_known_new_fixtures", 20)
    monkeypatch.setattr(bot, "_shadow_ml_data_revision_pending", False)
    monkeypatch.setattr(bot, "_shadow_rolling_data_revision_pending", False)
    monkeypatch.setattr(bot, "_shadow_ml_contract_mismatch_pending", False)
    monkeypatch.setattr(
        bot, "_shadow_rolling_contract_mismatch_pending", False
    )
    monkeypatch.setattr(
        bot, "_shadow_ml_noop_scan_not_before_monotonic", 0.0
    )
    monkeypatch.setattr(
        bot, "_shadow_rolling_noop_scan_not_before_monotonic", 0.0
    )
    monkeypatch.setattr(bot, "SHADOW_ML_MIN_NEW_FIXTURES", 25)
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_model_cached",
        lambda: {"model_id": "static"},
    )
    monkeypatch.setattr(
        bot,
        "load_shadow_rolling_model_cached",
        lambda: {"model_id": "rolling"},
    )
    monkeypatch.setattr(
        bot,
        "_shadow_ml_model_age_seconds",
        lambda artifact: float(bot.SHADOW_ML_RETRAIN_CHECK_SECONDS + 1),
    )
    no_retrain = {
        "status": "skipped",
        "reason": "no_retrain_threshold",
        "new_fixtures": 20,
    }
    monkeypatch.setattr(
        bot,
        "train_shadow_ml_once",
        lambda **kwargs: static_calls.append(kwargs) or dict(no_retrain),
    )
    monkeypatch.setattr(
        bot,
        "train_shadow_rolling_ml_once",
        lambda **kwargs: rolling_calls.append(kwargs) or dict(no_retrain),
    )

    for fixture_id in range(1, 6):
        bot.note_shadow_ml_outcome(fixture_id)
    bot.process_shadow_ml_retrain_wakeup(trigger="static")

    assert len(static_calls) == 1
    assert bot._shadow_ml_noop_scan_not_before_monotonic > 0.0
    assert bot._shadow_rolling_noop_scan_not_before_monotonic == 0.0

    bot.process_shadow_rolling_retrain_wakeup(trigger="rolling")

    assert len(rolling_calls) == 1
    assert bot._shadow_rolling_noop_scan_not_before_monotonic > 0.0


def test_rolling_scan_error_restores_consumed_hints_without_cooldown(
    monkeypatch,
) -> None:
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML_ROLLING_CHALLENGER", True)
    monkeypatch.setattr(bot, "SHADOW_ML_ROLLING_AUTO_RETRAIN", True)
    monkeypatch.setattr(
        bot, "_shadow_rolling_pending_fixture_ids", set(range(1, 6))
    )
    monkeypatch.setattr(bot, "_shadow_rolling_known_new_fixtures", 20)
    monkeypatch.setattr(bot, "_shadow_rolling_data_revision_pending", True)
    monkeypatch.setattr(
        bot, "_shadow_rolling_contract_mismatch_pending", False
    )
    monkeypatch.setattr(
        bot, "_shadow_rolling_noop_scan_not_before_monotonic", 0.0
    )
    monkeypatch.setattr(
        bot,
        "load_shadow_rolling_model_cached",
        lambda: {"model_id": "rolling"},
    )
    monkeypatch.setattr(
        bot,
        "train_shadow_rolling_ml_once",
        lambda **kwargs: {
            "status": "error",
            "reason": "worker_timeout",
        },
    )

    result = bot.process_shadow_rolling_retrain_wakeup(trigger="timeout")

    assert result["status"] == "error"
    assert bot._shadow_rolling_pending_fixture_ids == set(range(1, 6))
    assert bot._shadow_rolling_data_revision_pending is True
    assert bot._shadow_rolling_noop_scan_not_before_monotonic == 0.0


def test_scan_error_restores_consumed_hints_without_cooldown(
    monkeypatch,
) -> None:
    calls: list[dict] = []
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML_ROLLING_CHALLENGER", False)
    monkeypatch.setattr(bot, "_shadow_ml_pending_fixture_ids", set(range(1, 6)))
    monkeypatch.setattr(bot, "_shadow_ml_known_new_fixtures", 20)
    monkeypatch.setattr(bot, "_shadow_ml_data_revision_pending", True)
    monkeypatch.setattr(bot, "_shadow_ml_contract_mismatch_pending", False)
    monkeypatch.setattr(
        bot, "_shadow_ml_noop_scan_not_before_monotonic", 0.0
    )
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_model_cached",
        lambda: {"model_id": "existing"},
    )
    monkeypatch.setattr(
        bot,
        "_shadow_ml_model_age_seconds",
        lambda artifact: float(bot.SHADOW_ML_RETRAIN_CHECK_SECONDS + 1),
    )

    def train_stub(**kwargs):
        calls.append(kwargs)
        return {"status": "error", "reason": "heavy_worker_busy"}

    monkeypatch.setattr(bot, "train_shadow_ml_once", train_stub)

    first = bot.process_shadow_ml_retrain_wakeup(trigger="busy")
    second = bot.process_shadow_ml_retrain_wakeup(trigger="retry")

    assert first["status"] == second["status"] == "error"
    assert len(calls) == 2
    assert bot._shadow_ml_pending_fixture_ids == set(range(1, 6))
    assert bot._shadow_ml_data_revision_pending is True
    assert bot._shadow_ml_noop_scan_not_before_monotonic == 0.0


def test_busy_training_restores_hints_and_schedules_retry(
    monkeypatch,
) -> None:
    retries: list[bool] = []
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML_ROLLING_CHALLENGER", False)
    monkeypatch.setattr(bot, "_shadow_ml_pending_fixture_ids", set(range(1, 6)))
    monkeypatch.setattr(bot, "_shadow_ml_known_new_fixtures", 20)
    monkeypatch.setattr(bot, "_shadow_ml_data_revision_pending", False)
    monkeypatch.setattr(bot, "_shadow_ml_contract_mismatch_pending", False)
    monkeypatch.setattr(
        bot, "_shadow_ml_noop_scan_not_before_monotonic", 0.0
    )
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_model_cached",
        lambda: {"model_id": "existing"},
    )
    monkeypatch.setattr(
        bot,
        "_shadow_ml_model_age_seconds",
        lambda artifact: float(bot.SHADOW_ML_RETRAIN_CHECK_SECONDS + 1),
    )
    monkeypatch.setattr(
        bot,
        "train_shadow_ml_once",
        lambda **kwargs: {
            "status": "skipped",
            "reason": "training_already_running",
        },
    )
    monkeypatch.setattr(
        bot,
        "_schedule_shadow_ml_retry",
        lambda: retries.append(True),
    )

    result = bot.process_shadow_ml_retrain_wakeup(trigger="busy")

    assert result["reason"] == "training_already_running"
    assert retries == [True]
    assert bot._shadow_ml_pending_fixture_ids == set(range(1, 6))
    assert bot._shadow_ml_noop_scan_not_before_monotonic == 0.0


def test_daemon_wait_uses_earliest_independent_cooldown_deadline(
    monkeypatch,
) -> None:
    now_monotonic = 1000.0
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(bot, "SHADOW_ML_AUTO_RETRAIN", True)
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML_ROLLING_CHALLENGER", True)
    monkeypatch.setattr(bot, "SHADOW_ML_ROLLING_AUTO_RETRAIN", True)
    monkeypatch.setattr(bot, "SHADOW_ML_RETRAIN_CHECK_SECONDS", 300)
    monkeypatch.setattr(
        bot,
        "_shadow_ml_noop_scan_not_before_monotonic",
        now_monotonic + 30.0,
    )
    monkeypatch.setattr(
        bot,
        "_shadow_rolling_noop_scan_not_before_monotonic",
        now_monotonic + 10.0,
    )

    wait_seconds = bot._shadow_ml_daemon_wait_seconds(
        now_monotonic=now_monotonic
    )

    assert wait_seconds == 10.0

    bot._shadow_ml_noop_scan_not_before_monotonic = now_monotonic - 1.0
    bot._shadow_rolling_noop_scan_not_before_monotonic = 0.0
    assert bot._shadow_ml_daemon_wait_seconds(
        now_monotonic=now_monotonic
    ) == 0.05


def test_expired_cooldown_is_cleared_when_growth_is_still_insufficient(
    monkeypatch,
) -> None:
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(bot, "_shadow_ml_pending_fixture_ids", {101})
    monkeypatch.setattr(bot, "_shadow_ml_known_new_fixtures", 0)
    monkeypatch.setattr(bot, "_shadow_ml_data_revision_pending", False)
    monkeypatch.setattr(bot, "_shadow_ml_contract_mismatch_pending", False)
    monkeypatch.setattr(
        bot,
        "_shadow_ml_noop_scan_not_before_monotonic",
        bot.time.monotonic() - 1.0,
    )
    monkeypatch.setattr(bot, "SHADOW_ML_MIN_NEW_FIXTURES", 25)
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_model_cached",
        lambda: {"model_id": "existing"},
    )
    monkeypatch.setattr(
        bot,
        "_shadow_ml_model_age_seconds",
        lambda artifact: float(bot.SHADOW_ML_RETRAIN_CHECK_SECONDS + 1),
    )
    monkeypatch.setattr(
        bot,
        "train_shadow_ml_once",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("history scan must remain debounced")
        ),
    )

    result = bot.process_shadow_ml_retrain_wakeup(trigger="expired")

    assert result["reason"] == "debounced_before_history_scan"
    assert bot._shadow_ml_noop_scan_not_before_monotonic == 0.0


def test_changed_training_hash_retrains_without_new_fixture(
    monkeypatch,
) -> None:
    saved: list[dict] = []
    current = {
        "artifact_type": "shadow_ml_model",
        "created_at_utc": "2026-07-25T00:00:00+00:00",
        "training_contract": {"key": "same-contract"},
        "training_data_hash": "old-hash",
        "training_summary": {"unique_fixtures": 10},
        "production_applied": False,
    }
    replacement = {
        "artifact_type": "shadow_ml_model",
        "model_id": "shadow:corrected-labels",
        "status": "collecting",
        "training_contract": {"key": "same-contract"},
        "training_data_hash": "new-hash",
        "training_summary": {"unique_fixtures": 10},
        "production_applied": False,
    }
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(
        bot,
        "load_joined_observation_history",
        lambda include_pending=False: [{"fixture_id": 1}],
    )
    monkeypatch.setattr(
        bot,
        "summarize_shadow_ml_training_data",
        lambda records: {
            "eligible_records": 10,
            "unique_fixtures": 10,
            "span_days": 10.0,
            "training_contract": {"key": "same-contract"},
            "training_data_hash": "new-hash",
        },
    )
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_model_cached",
        lambda force=False: current,
    )
    monkeypatch.setattr(
        bot,
        "train_shadow_model",
        lambda records, config, now: replacement,
    )
    monkeypatch.setattr(
        bot,
        "_shadow_ml_artifact_is_safe",
        lambda value: True,
    )
    monkeypatch.setattr(
        bot,
        "save_shadow_ml_model_file",
        lambda path, value: saved.append(value),
    )
    monkeypatch.setattr(
        bot,
        "_set_shadow_ml_model_cache",
        lambda value: None,
    )

    result = bot._train_shadow_ml_once_local(
        force=False,
        trigger="label-correction-test",
    )

    assert result["status"] == "trained"
    assert result["data_revision_changed"] is True
    assert saved[0]["model_id"] == "shadow:corrected-labels"


def test_persisted_observation_retries_missing_prediction(
    monkeypatch,
) -> None:
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_RARE_PRECISION", False)
    observation = _pending_observation()
    scored: list[dict] = []
    monkeypatch.setattr(bot, "ENABLE_ROLLING_DYNAMICS_SHADOW", False)
    monkeypatch.setattr(
        bot,
        "append_observation_history",
        lambda record: False,
    )
    monkeypatch.setattr(
        bot,
        "observation_history_record_exists",
        lambda record: True,
    )
    monkeypatch.setattr(
        bot,
        "predict_and_append_shadow_ml",
        lambda record: scored.append(record) or True,
    )

    assert bot.persist_observation_and_score_shadow(observation) is False
    assert scored == [observation]


def test_persist_freezes_rolling_before_shadow_prediction_without_mutation(
    monkeypatch,
) -> None:
    observation = _pending_observation()
    before = copy.deepcopy(observation)
    appended: list[dict] = []
    registered: list[dict] = []
    scored: list[dict] = []
    rolling_scored: list[dict] = []

    def freeze(record):
        frozen = copy.deepcopy(record)
        frozen["rolling_dynamics"] = {
            "schema_version": 1,
            "production_applied": False,
        }
        return frozen

    monkeypatch.setattr(bot, "freeze_observation_rolling_dynamics", freeze)
    monkeypatch.setattr(
        bot,
        "append_observation_history",
        lambda record: appended.append(record) or True,
    )
    monkeypatch.setattr(
        bot,
        "register_observation_rolling_baseline",
        lambda record: registered.append(record) or True,
    )
    monkeypatch.setattr(
        bot,
        "predict_and_append_shadow_ml",
        lambda record: scored.append(record) or True,
    )
    monkeypatch.setattr(
        bot,
        "predict_and_append_shadow_rolling_ml",
        lambda record: rolling_scored.append(record) or True,
    )

    assert bot.persist_observation_and_score_shadow(observation) is True
    assert observation == before
    assert appended == registered == scored == rolling_scored
    assert appended[0] is scored[0] is rolling_scored[0]
    assert appended[0]["rolling_dynamics"]["production_applied"] is False
