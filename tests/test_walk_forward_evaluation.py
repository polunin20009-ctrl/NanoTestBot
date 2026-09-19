from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts import walk_forward_evaluation as cli
from walk_forward import engine
from walk_forward.engine import EvaluationConfig, build_walk_forward_report


UTC = timezone.utc


def _config_block(*, reputation_version: str = "convex_v1_production") -> dict:
    return {
        "model_version": "45_plus_v2",
        "pressure_version": "v3_smooth",
        "factor_versions": {
            "probability": "45_plus_v2",
            "pressure": "v3_smooth",
            "signal_reputation": "v1",
            "signal_reputation_expanded_blend": reputation_version,
            # These publication-only versions must not split the contract.
            "channel_signal_filter": "test_policy",
            "premium_badge": "test_badge",
        },
    }


def _rolling(*, both: bool) -> dict:
    return {
        "schema_version": 1,
        "mode": "shadow_collection",
        "production_applied": False,
        "available_windows": 2 if both else 0,
        "windows": {
            "5m": {
                "status": "ok" if both else "unavailable",
                "requested_window_minutes": 5,
                "actual_span_minutes": 5,
                "available_activity_metric_count": 5 if both else 0,
            },
            "10m": {
                "status": "ok" if both else "unavailable",
                "requested_window_minutes": 10,
                "actual_span_minutes": 10,
                "available_activity_metric_count": 5 if both else 0,
            },
        },
    }


def _observation(
    fixture_id: int,
    minute: int,
    created_at: str,
    *,
    probability: float = 80.0,
    reputation_base: float = 78.0,
    reputation_adjusted: float = 80.0,
    intensity: float = 0.60,
    season: float = 1.03,
    goals: int = 1,
    both_windows: bool = True,
    suffix: str = "A",
    reputation_version: str = "convex_v1_production",
) -> dict:
    observation_id = f"{fixture_id}:{minute}:{suffix}"
    reputation_delta = reputation_adjusted - reputation_base
    filter_passed = bool(
        probability >= 75.0
        and reputation_delta >= 1.5
        and intensity >= 0.55
        and season >= 1.02
    )
    return {
        "record_type": "observation",
        "observation_id": observation_id,
        "observation_key": observation_id,
        "fixture_id": fixture_id,
        "schema_version": 1,
        "stage": "decision_pipeline",
        "created_at_utc": created_at,
        "minute": minute,
        "config": _config_block(reputation_version=reputation_version),
        "match": {
            "home_team_name": f"Home {fixture_id}",
            "away_team_name": f"Away {fixture_id}",
            "score_home": goals,
            "score_away": 0,
        },
        "probabilities": {
            "prob_to90": probability,
            "reputation_base_prob_to90": reputation_base,
            "reputation_adjusted_prob_to90": reputation_adjusted,
        },
        "features": {
            "adjusted_intensity": intensity,
            "season_context_factor": season,
        },
        "publication_policy": {
            "filter_version": "base_rep15_int055_season102_p90_75_v1",
            "publication_context_passed": True,
            "publication_allow": filter_passed,
        },
        "channel_signal_filter": {
            "version": "base_rep15_int055_season102_p90_75_v1",
            "passed": filter_passed,
            "prob_to90": probability,
            "reputation_base_prob_to90": reputation_base,
            "reputation_adjusted_prob_to90": reputation_adjusted,
            "adjusted_intensity": intensity,
            "season_context_factor": season,
            "score_home": goals,
            "score_away": 0,
        },
        "decision": {"active_publication_allow": filter_passed},
        "rolling_dynamics": _rolling(both=both_windows),
        "outcome": {"status": "pending"},
    }


def _outcome(observation: dict, label: bool, resolved_at: str) -> dict:
    return {
        "record_type": "observation_outcome",
        "observation_id": observation["observation_id"],
        "observation_key": observation["observation_id"],
        "fixture_id": observation["fixture_id"],
        "outcome_schema_version": 1,
        "created_at_utc": resolved_at,
        "outcome": {
            "status": "resolved",
            "resolved_at_utc": resolved_at,
            "outcome_scope": "TO_90_NORMAL_TIME",
            "goal_to90_normal_time": label,
        },
    }


def _prediction(
    observation: dict,
    *,
    probability: float,
    model_created_at: str,
    cutoff: str,
    created_at: str,
    source: str = "rolling",
    key_suffix: str = "1",
) -> dict:
    rolling = source == "rolling"
    return {
        "record_type": (
            "shadow_ml_rolling_prediction" if rolling else "shadow_ml_prediction"
        ),
        "prediction_key": f"{source}:{observation['observation_id']}:{key_suffix}",
        "observation_id": observation["observation_id"],
        "fixture_id": observation["fixture_id"],
        "minute": observation["minute"],
        "observation_created_at_utc": observation["created_at_utc"],
        "created_at_utc": created_at,
        "model_created_at_utc": model_created_at,
        "model_data_cutoff_utc": cutoff,
        "model_id": f"{source}-model-{key_suffix}",
        "algorithm_version": f"{source}-algorithm",
        "prediction_status": "ok",
        "shadow_only": True,
        "production_applied": False,
        "predictions": {
            "to90": {
                "status": "ok",
                "calibrated_probability_pct": probability,
                "production_applied": False,
            }
        },
    }


def _write_jsonl(path: Path, records: list[dict], *, invalid_tail: bool = False) -> None:
    content = "".join(json.dumps(record) + "\n" for record in records)
    if invalid_tail:
        content += "{unfinished\n"
    path.write_text(content, encoding="utf-8")


def test_active_and_gzip_replay_uses_first_trigger_and_daily_msk_folds(
    tmp_path: Path,
) -> None:
    path = tmp_path / "observation_history.jsonl"
    archive = tmp_path / "observation_history.20260815T000000000000Z.jsonl.gz"
    ignored = tmp_path / "observation_history.pre_migration.jsonl.gz"

    first = _observation(
        1,
        46,
        "2026-08-14T21:30:00+00:00",  # 2026-08-15 MSK
        both_windows=False,
        suffix="FIRST",
    )
    later = _observation(
        1,
        51,
        "2026-08-14T21:35:00+00:00",
        both_windows=True,
        suffix="LATER",
    )
    three_goals = _observation(
        2,
        50,
        "2026-08-15T10:00:00+00:00",
        goals=3,
        suffix="THREE",
    )
    low = _observation(
        3,
        50,
        "2026-08-16T10:00:00+00:00",
        probability=74.99,
        suffix="LOW",
    )
    clean = _observation(
        4,
        50,
        "2026-08-16T12:00:00+00:00",
        goals=2,
        suffix="CLEAN",
    )
    archive_records = [
        first,
        _outcome(first, True, "2026-08-14T22:30:00+00:00"),
        later,
        _outcome(later, True, "2026-08-14T22:30:00+00:00"),
    ]
    with gzip.open(archive, "wt", encoding="utf-8") as handle:
        for record in archive_records:
            handle.write(json.dumps(record) + "\n")
    with gzip.open(ignored, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(_observation(99, 50, "2026-08-15T10:00:00Z")))
    _write_jsonl(
        path,
        [
            three_goals,
            _outcome(three_goals, False, "2026-08-15T11:00:00+00:00"),
            low,
            _outcome(low, True, "2026-08-16T11:00:00+00:00"),
            clean,
            _outcome(clean, True, "2026-08-16T13:00:00+00:00"),
        ],
        invalid_tail=True,
    )

    report = build_walk_forward_report(
        path,
        config=EvaluationConfig(
            from_date="2026-08-15",
            to_date="2026-08-16",
            ml_mode="disabled",
        ),
    )

    assert report["sources"]["observations"]["invalid_json_lines"] == 1
    assert len(report["sources"]["observations"]["files"]) == 2
    assert report["arms"]["control"]["signals"] == 3
    assert report["arms"]["control"]["hits"] == 2
    assert report["arms"]["control"]["misses"] == 1
    assert report["arms"]["control"]["triggers"][0]["observation_id"] == (
        first["observation_id"]
    )
    assert report["arms"]["both_windows"]["signals"] == 2
    assert all(
        trigger["fixture_id"] != 1
        for trigger in report["arms"]["both_windows"]["triggers"]
    )
    assert report["arms"]["both_windows_goals_le2"]["signals"] == 1
    assert report["arms"]["both_windows_goals_le2"]["hit_rate"] == 1.0
    assert report["arms"]["both_windows_goals_le2"]["wilson_ci95"] is not None
    assert report["arms"]["both_windows_goals_le2_close"]["signals"] == 0
    assert [fold["day_msk"] for fold in report["folds"]] == [
        "2026-08-15",
        "2026-08-16",
    ]


def test_default_never_rescues_derived_arm_on_later_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "observation_history.jsonl"
    first = _observation(
        7,
        46,
        "2026-08-15T10:00:00+00:00",
        both_windows=False,
        suffix="FIRST",
    )
    later = _observation(
        7,
        52,
        "2026-08-15T10:06:00+00:00",
        both_windows=True,
        suffix="LATER",
    )
    _write_jsonl(
        path,
        [
            first,
            _outcome(first, True, "2026-08-15T11:00:00+00:00"),
            later,
            _outcome(later, True, "2026-08-15T11:00:00+00:00"),
        ],
    )

    production_faithful = build_walk_forward_report(
        path,
        config=EvaluationConfig(ml_mode="disabled"),
    )
    exploratory = build_walk_forward_report(
        path,
        config=EvaluationConfig(
            ml_mode="disabled",
            trigger_policy="independent-exploratory",
        ),
    )

    assert production_faithful["arms"]["control"]["signals"] == 1
    assert production_faithful["arms"]["both_windows"]["signals"] == 0
    assert production_faithful["methodology"]["later_data_can_rescue_derived_arm"] is False
    assert exploratory["arms"]["both_windows"]["signals"] == 1
    assert exploratory["arms"]["both_windows"]["triggers"][0][
        "observation_id"
    ] == later["observation_id"]


def test_close_score_arm_freezes_first_control_and_honours_custom_margin(
    tmp_path: Path,
) -> None:
    path = tmp_path / "observation_history.jsonl"
    first = _observation(
        8,
        46,
        "2026-08-15T10:00:00+00:00",
        goals=2,
        suffix="TWO-NIL",
    )
    later = _observation(
        8,
        52,
        "2026-08-15T10:06:00+00:00",
        goals=1,
        suffix="ONE-NIL",
    )
    _write_jsonl(
        path,
        [
            first,
            _outcome(first, True, "2026-08-15T11:00:00+00:00"),
            later,
            _outcome(later, True, "2026-08-15T11:00:00+00:00"),
        ],
    )

    frozen = build_walk_forward_report(
        path, config=EvaluationConfig(ml_mode="disabled")
    )
    exploratory = build_walk_forward_report(
        path,
        config=EvaluationConfig(
            ml_mode="disabled",
            trigger_policy="independent-exploratory",
        ),
    )
    wider = build_walk_forward_report(
        path,
        config=EvaluationConfig(
            ml_mode="disabled", max_score_difference_abs=2
        ),
    )

    arm_id = "both_windows_goals_le2_close"
    assert frozen["config"]["max_score_difference_abs"] == 1
    assert frozen["arms"][arm_id]["signals"] == 0
    assert exploratory["arms"][arm_id]["signals"] == 1
    assert exploratory["arms"][arm_id]["triggers"][0][
        "observation_id"
    ] == later["observation_id"]
    assert wider["arms"][arm_id]["signals"] == 1
    assert wider["arms"][arm_id]["triggers"][0]["observation_id"] == (
        first["observation_id"]
    )
    assert arm_id in engine.render_text_report(wider)


def test_default_requires_recorded_publication_allow_and_full_slice_contract(
    tmp_path: Path,
) -> None:
    path = tmp_path / "observation_history.jsonl"
    missing_publication = _observation(
        21,
        50,
        "2026-08-15T10:00:00+00:00",
        suffix="NO-PUBLICATION",
    )
    missing_publication["publication_policy"].pop("publication_allow")
    missing_publication["decision"].pop("active_publication_allow")
    tampered_slice = _observation(
        22,
        50,
        "2026-08-15T10:01:00+00:00",
        suffix="BAD-SLICE",
    )
    tampered_slice["rolling_dynamics"]["windows"]["10m"][
        "actual_span_minutes"
    ] = 99
    _write_jsonl(
        path,
        [
            missing_publication,
            _outcome(
                missing_publication,
                True,
                "2026-08-15T11:00:00+00:00",
            ),
            tampered_slice,
            _outcome(
                tampered_slice,
                True,
                "2026-08-15T11:01:00+00:00",
            ),
        ],
    )

    report = build_walk_forward_report(
        path,
        config=EvaluationConfig(ml_mode="disabled"),
    )
    historical = build_walk_forward_report(
        path,
        config=EvaluationConfig(
            ml_mode="disabled",
            control_contract_mode="recomputed-historical",
        ),
    )

    assert report["arms"]["control"]["signals"] == 1
    assert report["arms"]["both_windows"]["signals"] == 0
    assert report["eligibility"]["candidate_value_skipped"][
        "control_unavailable:control_inputs_unavailable"
    ] == 1
    assert historical["arms"]["control"]["signals"] == 2
    assert historical["arms"]["both_windows"]["signals"] == 1
    assert historical["config"]["control_contract_mode"] == (
        "recomputed-historical"
    )


def test_latest_compatible_contract_excludes_old_reputation_semantics(
    tmp_path: Path,
) -> None:
    path = tmp_path / "observation_history.jsonl"
    old = _observation(
        1,
        50,
        "2026-08-14T10:00:00+00:00",
        suffix="OLD",
        reputation_version="convex_v1_shadow",
    )
    current = _observation(
        2,
        50,
        "2026-08-15T10:00:00+00:00",
        suffix="CURRENT",
    )
    _write_jsonl(
        path,
        [
            old,
            _outcome(old, True, "2026-08-14T11:00:00+00:00"),
            current,
            _outcome(current, False, "2026-08-15T11:00:00+00:00"),
        ],
    )

    report = build_walk_forward_report(
        path,
        config=EvaluationConfig(ml_mode="disabled"),
    )

    assert report["contract"]["cohort_count"] == 2
    assert report["arms"]["control"]["signals"] == 1
    assert report["arms"]["control"]["triggers"][0]["fixture_id"] == 2
    assert report["eligibility"]["skipped"]["feature_contract"] == 1


def test_outcome_may_precede_observation_in_file_without_affecting_selection(
    tmp_path: Path,
) -> None:
    path = tmp_path / "observation_history.jsonl"
    row = _observation(11, 50, "2026-08-15T10:00:00+00:00")
    outcome = _outcome(row, True, "2026-08-15T11:00:00+00:00")
    # Canonical joining is keyed/ranked, not dependent on physical line order.
    _write_jsonl(path, [outcome, row])

    report = build_walk_forward_report(
        path,
        config=EvaluationConfig(ml_mode="disabled"),
    )

    assert report["arms"]["control"]["signals"] == 1
    assert report["arms"]["control"]["hits"] == 1
    assert report["methodology"]["selection_uses_outcome"] is False


def test_join_diagnostics_distinguish_pending_observations(tmp_path: Path) -> None:
    path = tmp_path / "observation_history.jsonl"
    resolved = _observation(31, 50, "2026-08-15T10:00:00+00:00")
    pending = _observation(32, 50, "2026-08-15T10:01:00+00:00")
    _write_jsonl(
        path,
        [
            resolved,
            _outcome(resolved, True, "2026-08-15T11:00:00+00:00"),
            pending,
        ],
    )

    report = build_walk_forward_report(
        path,
        config=EvaluationConfig(ml_mode="disabled"),
    )

    assert report["join"]["observations"] == 2
    assert report["join"]["joined"] == 1
    assert report["join"]["observations_without_outcome"] == 1


def test_journal_replay_accepts_only_day_frozen_bounded_predictions(
    tmp_path: Path,
) -> None:
    observations = tmp_path / "observation_history.jsonl"
    rolling_predictions = tmp_path / "shadow_ml_rolling_predictions.jsonl"
    row = _observation(1, 50, "2026-08-15T10:00:00+00:00", suffix="TEST")
    _write_jsonl(
        observations,
        [row, _outcome(row, True, "2026-08-15T11:00:00+00:00")],
    )
    invalid_same_day_model = _prediction(
        row,
        probability=99.0,
        model_created_at="2026-08-15T09:00:00+00:00",
        cutoff="2026-08-14T10:00:00+00:00",
        created_at="2026-08-15T10:00:01+00:00",
        key_suffix="invalid",
    )
    missing_observation_time = _prediction(
        row,
        probability=99.0,
        model_created_at="2026-08-14T08:00:00+00:00",
        cutoff="2026-08-14T07:00:00+00:00",
        created_at="2026-08-15T10:00:00.100000+00:00",
        key_suffix="missing-observation-time",
    )
    missing_observation_time.pop("observation_created_at_utc")
    not_shadow = _prediction(
        row,
        probability=99.0,
        model_created_at="2026-08-14T08:00:00+00:00",
        cutoff="2026-08-14T07:00:00+00:00",
        created_at="2026-08-15T10:00:00.200000+00:00",
        key_suffix="not-shadow",
    )
    not_shadow["shadow_only"] = False
    target_applied = _prediction(
        row,
        probability=99.0,
        model_created_at="2026-08-14T08:00:00+00:00",
        cutoff="2026-08-14T07:00:00+00:00",
        created_at="2026-08-15T10:00:00.300000+00:00",
        key_suffix="target-applied",
    )
    target_applied["predictions"]["to90"]["production_applied"] = True
    valid = _prediction(
        row,
        probability=80.0,
        model_created_at="2026-08-14T10:00:00+00:00",
        cutoff="2026-08-14T09:00:00+00:00",
        created_at="2026-08-15T10:00:02+00:00",
        key_suffix="valid",
    )
    _write_jsonl(
        rolling_predictions,
        [
            missing_observation_time,
            not_shadow,
            target_applied,
            invalid_same_day_model,
            valid,
        ],
    )

    report = build_walk_forward_report(
        observations,
        rolling_prediction_path=rolling_predictions,
        config=EvaluationConfig(
            ml_mode="journal-replay",
            ml_source="rolling",
            journal_timing_policy="day-frozen",
        ),
    )

    assert report["methodology"]["ml_mode"] == "journal-replay"
    assert report["prediction_skipped"]["rolling"]["not_day_frozen"] == 1
    assert report["prediction_skipped"]["rolling"][
        "observation_timestamp_missing"
    ] == 1
    assert report["prediction_skipped"]["rolling"]["shadow_only"] == 1
    assert report["prediction_skipped"]["rolling"][
        "target_production_applied"
    ] == 1
    assert report["arms"]["ml_confirm"]["signals"] == 1
    trigger = report["arms"]["ml_confirm"]["triggers"][0]
    assert trigger["rolling_ml_probability_pct"] == 80.0
    assert trigger["rolling_ml_model_id"] == "rolling-model-valid"


def test_fold_local_training_excludes_unresolved_at_cutoff_and_hides_test_outcome(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    observations = tmp_path / "observation_history.jsonl"
    known = _observation(1, 50, "2026-08-14T10:00:00+00:00", suffix="KNOWN")
    late_label = _observation(2, 50, "2026-08-14T11:00:00+00:00", suffix="LATE")
    test = _observation(3, 50, "2026-08-15T22:00:00+00:00", suffix="TEST")
    _write_jsonl(
        observations,
        [
            known,
            _outcome(known, True, "2026-08-14T12:00:00+00:00"),
            late_label,
            # Aug 16 02:00 MSK, after the Aug 16 00:00 fold cutoff.
            _outcome(late_label, False, "2026-08-15T23:00:00+00:00"),
            test,
            _outcome(test, True, "2026-08-15T23:30:00+00:00"),
        ],
    )
    trainer_rows: list[list[str]] = []
    inference_outcomes: list[dict] = []

    def fake_trainer(records, config=None, now=None):
        materialized = list(records)
        trainer_rows.append([row["observation_id"] for row in materialized])
        return {
            "model_id": f"fake-{len(trainer_rows)}",
            "algorithm_version": "fake-v1",
            "created_at_utc": now.isoformat(),
            "data_cutoff_utc": max(row["created_at_utc"] for row in materialized),
            "status": "collecting",
            "targets": {"to90": {"trained": True}},
        }

    def fake_predictor(artifact, observation):
        inference_outcomes.append(dict(observation.get("outcome") or {}))
        return {
            "status": "ok",
            "targets": {
                "to90": {
                    "status": "ok",
                    "calibrated_probability_pct": 85.0,
                }
            },
        }

    monkeypatch.setattr(engine, "train_shadow_model", fake_trainer)
    monkeypatch.setattr(engine, "train_shadow_rolling_model", fake_trainer)
    monkeypatch.setattr(engine, "predict_shadow", fake_predictor)
    monkeypatch.setattr(engine, "predict_shadow_rolling", fake_predictor)

    report = build_walk_forward_report(
        observations,
        config=EvaluationConfig(
            from_date="2026-08-16",
            to_date="2026-08-16",
            ml_mode="fold-local",
            ml_source="rolling",
        ),
    )

    assert trainer_rows == [[known["observation_id"]], [known["observation_id"]]]
    assert inference_outcomes == [{"status": "pending"}, {"status": "pending"}]
    assert report["fold_models"][0]["training_rows"] == 1
    assert report["fold_models"][0]["models"]["rolling"][
        "cutoff_strictly_respected"
    ] is True
    assert report["arms"]["ml_confirm"]["signals"] == 1
    assert report["arms"]["ml_confirm"]["hits"] == 1
    assert "fresh model per fold" in report["methodology"]["ml_leakage_boundary"]


def test_fold_local_prediction_fails_closed_without_artifact_timing() -> None:
    prediction = {
        "targets": {
            "to90": {
                "status": "ok",
                "calibrated_probability_pct": 99.0,
            }
        }
    }
    cutoff = datetime(2026, 8, 15, tzinfo=UTC)
    observation_at = datetime(2026, 8, 15, 1, tzinfo=UTC)

    assert engine._prediction_from_inference(
        prediction,
        {
            "model_id": "missing-cutoff",
            "created_at_utc": cutoff.isoformat(),
            "data_cutoff_utc": None,
        },
        cutoff=cutoff,
        observation_at=observation_at,
    ) is None
    assert engine._prediction_from_inference(
        prediction,
        {
            "model_id": "created-too-late",
            "created_at_utc": observation_at.isoformat(),
            "data_cutoff_utc": "2026-08-14T23:00:00+00:00",
        },
        cutoff=cutoff,
        observation_at=observation_at,
    ) is None


def test_cli_refuses_to_replace_source_or_use_same_output(tmp_path: Path) -> None:
    source = tmp_path / "observation_history.jsonl"
    source.write_text("", encoding="utf-8")
    output = tmp_path / "report.json"

    with pytest.raises(ValueError, match="source journal"):
        cli._assert_safe_outputs((source,), sources=(source,))
    with pytest.raises(ValueError, match="different files"):
        cli._assert_safe_outputs((output, output), sources=(source,))

    archive = tmp_path / "observation_history.20260815T010203000000Z.jsonl.gz"
    with gzip.open(archive, "wt", encoding="utf-8") as handle:
        handle.write("{}\n")
    with pytest.raises(ValueError, match="source journal"):
        cli._assert_safe_outputs((archive,), sources=(source,))
    with pytest.raises(ValueError, match="source journal"):
        cli._assert_safe_outputs(
            (Path(str(source) + ".lock"),),
            sources=(source,),
        )


def test_temporary_sqlite_is_removed_when_report_raises(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "observation_history.jsonl"
    source.write_text("", encoding="utf-8")
    allocated: list[Path] = []
    real_store = engine._ObservationStore

    class TrackingStore(real_store):
        def __init__(self) -> None:
            super().__init__()
            allocated.append(self.path)

    def fail(*args, **kwargs):
        raise RuntimeError("synthetic failure")

    monkeypatch.setattr(engine, "_ObservationStore", TrackingStore)
    monkeypatch.setattr(engine, "_build_walk_forward_report_with_store", fail)

    with pytest.raises(RuntimeError, match="synthetic failure"):
        build_walk_forward_report(
            source,
            config=EvaluationConfig(ml_mode="disabled"),
        )

    assert len(allocated) == 1
    assert not allocated[0].exists()


def test_text_report_contains_uncertainty_and_daily_summary(tmp_path: Path) -> None:
    path = tmp_path / "observation_history.jsonl"
    row = _observation(1, 50, "2026-08-15T10:00:00+00:00")
    _write_jsonl(path, [row, _outcome(row, True, "2026-08-15T11:00:00Z")])
    report = build_walk_forward_report(
        path,
        config=EvaluationConfig(ml_mode="disabled"),
    )

    rendered = engine.render_text_report(report)

    assert "Wilson 95%" in rendered
    assert "2026-08-15" in rendered
    assert "logloss" in rendered
    assert "не является гарантией" in rendered
