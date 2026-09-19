from __future__ import annotations

import copy
import gzip
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pytest

from shadow_candidates.engine import CandidateLayer
from shadow_candidates.rules import (
    ARM_CONTROL,
    ARM_FULL_SLICES,
    ARM_FULL_SLICES_GOALS_LE2,
    ARM_FULL_SLICES_GOALS_LE2_CLOSE,
    ARM_ROLLING_ML_CONFIRM_75,
    DEFAULT_RULESET,
    FAIL,
    PASS,
    UNAVAILABLE,
    evaluate_candidate_arms,
)
from shadow_candidates.storage import AppendOnlyCandidateJournal


OBSERVATION_TIME = "2026-08-22T12:00:00+00:00"
PROSPECTIVE_START = "2026-08-22T12:00:00+00:00"


def _window(minutes: int) -> dict:
    return {
        "status": "ok",
        "reason": None,
        "requested_window_minutes": minutes,
        "actual_span_minutes": minutes,
        "available_activity_metric_count": 5,
    }


def _snapshot(
    fixture_id: int = 100,
    minute: int = 50,
    *,
    created_at: str = OBSERVATION_TIME,
    goals: tuple[int, int] = (1, 1),
) -> dict:
    observation_id = f"{fixture_id}:{minute}:WINDOW_1:ALLOW:v3"
    return {
        "record_type": "observation",
        "observation_id": observation_id,
        "fixture_id": fixture_id,
        "minute": minute,
        "created_at_utc": created_at,
        "schema_version": 1,
        "stage": "decision_pipeline",
        "window_name": "WINDOW_1",
        "match": {
            "home_team_name": "Home",
            "away_team_name": "Away",
            "league_name": "League",
            "score_home": goals[0],
            "score_away": goals[1],
        },
        "publication_policy": {
            "filter_version": DEFAULT_RULESET.control_filter_version,
            "publication_context_passed": True,
            "publication_allow": True,
        },
        "channel_signal_filter": {
            "version": DEFAULT_RULESET.control_filter_version,
            "passed": True,
            "prob_to90": 75.0,
            "reputation_base_prob_to90": 73.5,
            "reputation_adjusted_prob_to90": 75.0,
            "adjusted_intensity": 0.55,
            "season_context_factor": 1.02,
            "score_home": goals[0],
            "score_away": goals[1],
        },
        "probabilities": {
            "prob_to90": 75.0,
            "reputation_base_prob_to90": 73.5,
            "reputation_adjusted_prob_to90": 75.0,
        },
        "rolling_dynamics": {
            "schema_version": 1,
            "mode": "shadow_collection",
            "production_applied": False,
            "available_windows": 2,
            "windows": {"5m": _window(5), "10m": _window(10)},
        },
    }


def _prediction(
    snapshot: dict,
    *,
    rolling: bool = True,
    probability: float = 75.0,
) -> dict:
    record_type = "shadow_ml_rolling_prediction" if rolling else "shadow_ml_prediction"
    return {
        "record_type": record_type,
        "prediction_key": f"prediction:{snapshot['observation_id']}:{record_type}",
        "observation_id": snapshot["observation_id"],
        "fixture_id": snapshot["fixture_id"],
        "minute": snapshot["minute"],
        "created_at_utc": "2026-08-22T12:00:01+00:00",
        "observation_created_at_utc": snapshot["created_at_utc"],
        "model_created_at_utc": "2026-08-22T11:00:00+00:00",
        "model_data_cutoff_utc": "2026-08-22T10:00:00+00:00",
        "model_id": "model-1",
        "shadow_only": True,
        "production_applied": False,
        "predictions": {
            "to90": {
                "status": "ok",
                "readiness_status": "collecting",
                "calibrated_probability_pct": probability,
                "production_applied": False,
            }
        },
    }


def _layer(tmp_path: Path, **kwargs) -> CandidateLayer:
    return CandidateLayer(
        AppendOnlyCandidateJournal(tmp_path / "candidates.jsonl", rotate_max_bytes=0),
        prospective_start_utc=PROSPECTIVE_START,
        now_factory=lambda: datetime(2026, 8, 22, 12, 1, tzinfo=timezone.utc),
        **kwargs,
    )


def test_ruleset_manifest_is_versioned_and_boundary_values_pass() -> None:
    snapshot = _snapshot()
    before = copy.deepcopy(snapshot)
    rolling = _prediction(snapshot)
    static = _prediction(snapshot, rolling=False, probability=80.0)

    result = evaluate_candidate_arms(
        snapshot, static_prediction=static, rolling_prediction=rolling
    )

    assert snapshot == before
    assert DEFAULT_RULESET.version == "candidate_at_first_control_allow_v3"
    assert DEFAULT_RULESET.manifest()["version"] == DEFAULT_RULESET.version
    assert len(DEFAULT_RULESET.manifest()["manifest_hash"]) == 20
    assert {arm["evaluation_status"] for arm in result.values()} == {PASS}
    assert result[ARM_ROLLING_ML_CONFIRM_75]["inputs"]["rolling"][
        "production_applied"
    ] is False


def test_goal_limit_and_ml_threshold_are_cumulative() -> None:
    snapshot = _snapshot(goals=(2, 1))
    result = evaluate_candidate_arms(
        snapshot, rolling_prediction=_prediction(snapshot, probability=99.0)
    )

    assert result[ARM_CONTROL]["evaluation_status"] == PASS
    assert result[ARM_FULL_SLICES]["evaluation_status"] == PASS
    assert result[ARM_FULL_SLICES_GOALS_LE2]["evaluation_status"] == FAIL
    assert result[ARM_FULL_SLICES_GOALS_LE2_CLOSE]["evaluation_status"] == FAIL
    assert result[ARM_ROLLING_ML_CONFIRM_75]["evaluation_status"] == FAIL
    assert result[ARM_ROLLING_ML_CONFIRM_75]["reason"].startswith(
        "prerequisite_failed"
    )


@pytest.mark.parametrize("goals", [(0, 0), (1, 0), (0, 1), (1, 1)])
def test_close_score_candidate_accepts_draw_or_one_goal_margin(goals) -> None:
    result = evaluate_candidate_arms(_snapshot(goals=goals))

    assert result[ARM_FULL_SLICES_GOALS_LE2_CLOSE]["evaluation_status"] == PASS
    assert result[ARM_FULL_SLICES_GOALS_LE2_CLOSE]["inputs"][
        "score_difference_abs"
    ] <= 1


@pytest.mark.parametrize("goals", [(2, 0), (0, 2)])
def test_close_score_candidate_rejects_two_goal_margin(goals) -> None:
    result = evaluate_candidate_arms(_snapshot(goals=goals))

    close = result[ARM_FULL_SLICES_GOALS_LE2_CLOSE]
    assert close["evaluation_status"] == FAIL
    assert close["reason"] == "score_difference_abs"


def test_missing_or_below_threshold_rolling_ml_never_passes() -> None:
    snapshot = _snapshot()
    missing = evaluate_candidate_arms(snapshot)
    below = evaluate_candidate_arms(
        snapshot, rolling_prediction=_prediction(snapshot, probability=74.999999)
    )

    assert missing[ARM_ROLLING_ML_CONFIRM_75]["evaluation_status"] == UNAVAILABLE
    assert below[ARM_ROLLING_ML_CONFIRM_75]["evaluation_status"] == FAIL


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        ({"model_created_at_utc": "2026-08-22T12:00:00+00:00"}, "model_created_at_not_before_observation"),
        ({"model_created_at_utc": None}, "model_created_at_missing_or_invalid"),
        ({"model_data_cutoff_utc": None}, "model_data_cutoff_missing_or_invalid"),
        ({"production_applied": True}, "record_production_applied_not_false"),
        ({"observation_id": "wrong"}, "observation_identity_mismatch"),
        ({"created_at_utc": "2026-08-22T12:06:00+00:00"}, "outside_live_prediction_window"),
    ],
)
def test_rolling_ml_rejects_noncausal_or_identity_invalid_predictions(
    mutation: dict, expected_reason: str
) -> None:
    snapshot = _snapshot()
    prediction = _prediction(snapshot)
    prediction.update(mutation)

    result = evaluate_candidate_arms(snapshot, rolling_prediction=prediction)
    evidence = result[ARM_ROLLING_ML_CONFIRM_75]["inputs"]["rolling"]

    assert result[ARM_ROLLING_ML_CONFIRM_75]["evaluation_status"] == UNAVAILABLE
    assert expected_reason in evidence["unusable_reasons"]


def test_rolling_ml_rejects_target_production_application() -> None:
    snapshot = _snapshot()
    prediction = _prediction(snapshot)
    prediction["predictions"]["to90"]["production_applied"] = True

    result = evaluate_candidate_arms(snapshot, rolling_prediction=prediction)
    evidence = result[ARM_ROLLING_ML_CONFIRM_75]["inputs"]["rolling"]

    assert "target_production_applied_not_false" in evidence["unusable_reasons"]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda rolling: rolling.update(schema_version=2),
        lambda rolling: rolling.update(mode="production"),
        lambda rolling: rolling.update(production_applied=True),
        lambda rolling: rolling.update(available_windows=1),
        lambda rolling: rolling["windows"]["5m"].update(status="unavailable"),
        lambda rolling: rolling["windows"]["5m"].update(actual_span_minutes=8),
        lambda rolling: rolling["windows"]["10m"].update(available_activity_metric_count=0),
    ],
)
def test_full_slices_require_untampered_shadow_contract(mutate) -> None:
    snapshot = _snapshot()
    mutate(snapshot["rolling_dynamics"])

    result = evaluate_candidate_arms(snapshot)

    assert result[ARM_FULL_SLICES]["evaluation_status"] == UNAVAILABLE


@pytest.mark.parametrize("bad_value", [True, math.nan, math.inf, -math.inf])
def test_control_rejects_bool_and_nonfinite_inputs_without_mutation(bad_value) -> None:
    snapshot = _snapshot()
    snapshot["channel_signal_filter"]["prob_to90"] = bad_value
    snapshot["probabilities"]["prob_to90"] = bad_value
    before = copy.deepcopy(snapshot)

    result = evaluate_candidate_arms(snapshot)

    assert snapshot == before
    assert result[ARM_CONTROL]["evaluation_status"] == UNAVAILABLE


def test_control_requires_the_recorded_production_decision_contract() -> None:
    snapshot = _snapshot()
    snapshot["publication_policy"].pop("publication_allow")

    arms = evaluate_candidate_arms(snapshot)

    control = arms[ARM_CONTROL]
    assert control["evaluation_status"] == UNAVAILABLE
    assert "active_publication_allow" in control["unavailable_conditions"]


def test_context_block_is_a_real_fail_not_a_filter_contract_mismatch() -> None:
    snapshot = _snapshot()
    snapshot["publication_policy"]["publication_context_passed"] = False
    snapshot["publication_policy"]["publication_allow"] = False

    arms = evaluate_candidate_arms(snapshot)

    control = arms[ARM_CONTROL]
    assert control["evaluation_status"] == FAIL
    assert control["reason"] == "publication_context_passed"
    assert control["inputs"]["recomputed_filter_passed"] is True
    assert control["inputs"]["recomputed_active_publication_allow"] is False


@pytest.mark.parametrize("collision", ("journal", "lock"))
def test_journal_rejects_index_path_collisions(
    tmp_path: Path,
    collision: str,
) -> None:
    journal_path = tmp_path / "candidate.jsonl"
    index_path = (
        journal_path
        if collision == "journal"
        else Path(str(journal_path) + ".lock")
    )

    with pytest.raises(ValueError, match="different paths"):
        AppendOnlyCandidateJournal(journal_path, index_path=index_path)


def test_direct_immediate_hook_is_not_lost_and_boundary_is_inclusive(tmp_path: Path) -> None:
    snapshot = _snapshot(created_at=PROSPECTIVE_START)
    layer = _layer(tmp_path)

    written = layer.process_snapshot(
        snapshot,
        static_prediction=_prediction(snapshot, rolling=False),
        rolling_prediction=_prediction(snapshot),
    )

    assert written is not None
    assert written["prospective_start_utc"] == PROSPECTIVE_START
    assert written["cohort"]["cohort_claimed"] is True
    assert all(arm["first_allow"] for arm in written["arms"].values())
    assert layer.process_snapshot(
        snapshot, rolling_prediction=_prediction(snapshot)
    ) is not None  # distinct audit inputs, but cohort is frozen
    records = list(layer.journal.iter_records())
    assert records[-1]["cohort"]["cohort_claimed"] is False
    assert all(
        arm["candidate_decision"] == "FROZEN"
        for arm in records[-1]["arms"].values()
    )


def test_prospective_boundary_rejects_earlier_source(tmp_path: Path) -> None:
    layer = _layer(tmp_path)
    earlier = _snapshot(created_at="2026-08-22T11:59:59.999999+00:00")

    assert layer.process_snapshot(earlier) is None
    assert not Path(layer.journal.path).exists()


def test_restart_cannot_mix_a_different_boundary_into_same_ruleset(
    tmp_path: Path,
) -> None:
    layer = _layer(tmp_path)
    snapshot = _snapshot()
    assert layer.process_snapshot(snapshot) is not None
    changed_boundary = CandidateLayer(
        AppendOnlyCandidateJournal(layer.journal.path, rotate_max_bytes=0),
        prospective_start_utc="2026-08-22T11:59:00+00:00",
        now_factory=lambda: datetime(2026, 8, 22, 12, 1, tzinfo=timezone.utc),
    )
    another = _snapshot(fixture_id=101)

    with pytest.raises(ValueError, match="prospective_start_utc mismatch"):
        changed_boundary.process_snapshot(another)


def test_unavailable_at_first_control_is_rejected_forever(tmp_path: Path) -> None:
    layer = _layer(tmp_path)
    first = _snapshot()
    first["rolling_dynamics"] = None
    first_record = layer.process_snapshot(first)
    assert first_record is not None
    assert first_record["arms"][ARM_CONTROL]["candidate_decision"] == "ALLOW"
    assert first_record["arms"][ARM_FULL_SLICES]["candidate_decision"] == (
        "REJECT_UNAVAILABLE"
    )

    later = _snapshot(
        fixture_id=first["fixture_id"],
        minute=51,
        created_at="2026-08-22T12:00:30+00:00",
    )
    prediction = _prediction(later)
    prediction["created_at_utc"] = "2026-08-22T12:00:31+00:00"
    prediction["model_created_at_utc"] = "2026-08-22T11:30:00+00:00"
    later_record = layer.process_snapshot(later, rolling_prediction=prediction)

    assert later_record is not None
    assert all(
        arm["candidate_decision"] == "FROZEN"
        for arm in later_record["arms"].values()
    )
    triggers = layer.journal.trigger_rows_for_observations(
        DEFAULT_RULESET.version,
        [first["observation_id"], later["observation_id"]],
    )
    assert [row["arm_id"] for row in triggers[first["observation_id"]]] == [
        ARM_CONTROL
    ]
    assert triggers[later["observation_id"]] == []


def test_close_score_failure_at_first_control_cannot_be_repaired_later(
    tmp_path: Path,
) -> None:
    layer = _layer(tmp_path)
    first = _snapshot(goals=(2, 0))

    first_record = layer.process_snapshot(first)

    assert first_record is not None
    assert first_record["cohort"]["cohort_claimed"] is True
    assert first_record["arms"][ARM_FULL_SLICES_GOALS_LE2][
        "candidate_decision"
    ] == "ALLOW"
    assert first_record["arms"][ARM_FULL_SLICES_GOALS_LE2_CLOSE][
        "candidate_decision"
    ] == "BLOCK"

    later = _snapshot(
        fixture_id=first["fixture_id"],
        minute=51,
        created_at="2026-08-22T12:00:30+00:00",
        goals=(1, 1),
    )
    later_record = layer.process_snapshot(later)

    assert later_record is not None
    assert later_record["arms"][ARM_FULL_SLICES_GOALS_LE2_CLOSE][
        "evaluation_status"
    ] == PASS
    assert later_record["arms"][ARM_FULL_SLICES_GOALS_LE2_CLOSE][
        "candidate_decision"
    ] == "FROZEN"
    triggers = layer.journal.trigger_rows_for_observations(
        DEFAULT_RULESET.version,
        [first["observation_id"], later["observation_id"]],
    )
    assert ARM_FULL_SLICES_GOALS_LE2_CLOSE not in {
        row["arm_id"] for row in triggers[first["observation_id"]]
    }
    assert triggers[later["observation_id"]] == []


def test_journal_rotates_and_restart_dedupe_uses_jsonl_source(tmp_path: Path) -> None:
    path = tmp_path / "candidate.jsonl"
    journal = AppendOnlyCandidateJournal(path, rotate_max_bytes=200)
    first = {
        "event_id": "one",
        "record_type": "technical",
        "payload": "x" * 250,
    }
    second = {
        "event_id": "two",
        "record_type": "technical",
        "payload": "y" * 250,
    }
    assert journal.append(first)
    assert journal.append(second)
    assert list(tmp_path.glob("candidate.*.jsonl.gz"))

    Path(journal.index_path).unlink()
    restarted = AppendOnlyCandidateJournal(path, rotate_max_bytes=200)
    assert restarted.append(first) is False
    assert {_record["event_id"] for _record in restarted.iter_records()} == {
        "one",
        "two",
    }


def test_batch_outcome_writes_only_triggered_observation_arms(tmp_path: Path) -> None:
    layer = _layer(tmp_path)
    snapshot = _snapshot()
    layer.process_snapshot(snapshot, rolling_prediction=_prediction(snapshot))
    terminal = {
        "record_type": "observation_outcome",
        "observation_id": snapshot["observation_id"],
        "fixture_id": snapshot["fixture_id"],
        "outcome_schema_version": 1,
        "created_at_utc": "2026-08-22T13:00:00+00:00",
        "outcome": {
            "status": "resolved",
            "outcome_scope": "TO_90_NORMAL_TIME",
            "goal_to90_normal_time": True,
            "goal_result_source": "score_and_events",
            "resolved_at_utc": "2026-08-22T13:00:00+00:00",
        },
    }
    unrelated = copy.deepcopy(terminal)
    unrelated["observation_id"] = "999:50:WINDOW_1:ALLOW:v3"

    older_terminal = copy.deepcopy(terminal)
    older_terminal["outcome_schema_version"] = 0
    older_terminal["created_at_utc"] = "2026-08-22T12:59:00+00:00"
    older_terminal["outcome"]["goal_to90_normal_time"] = False
    written = layer.process_outcomes([unrelated, older_terminal, terminal])

    assert len(written) == 5
    assert {record["arm_id"] for record in written} == {
        ARM_CONTROL,
        ARM_FULL_SLICES,
        ARM_FULL_SLICES_GOALS_LE2,
        ARM_FULL_SLICES_GOALS_LE2_CLOSE,
        ARM_ROLLING_ML_CONFIRM_75,
    }
    assert all(record["observation_id"] == snapshot["observation_id"] for record in written)
    assert all(record["outcome"]["result"] == "WIN" for record in written)
    assert layer.process_outcome(terminal) == []


def test_quarantine_revision_supersedes_a_previously_resolved_candidate(tmp_path):
    from scripts.report_shadow_candidates import build_report

    layer = _layer(tmp_path)
    snapshot = _snapshot()
    layer.process_snapshot(snapshot)
    first = {
        "record_type": "observation_outcome",
        "observation_id": snapshot["observation_id"],
        "fixture_id": snapshot["fixture_id"],
        "outcome_schema_version": 1,
        "outcome_revision": 1,
        "created_at_utc": "2026-08-22T13:00:00+00:00",
        "outcome": {
            "status": "resolved",
            "outcome_scope": "TO_90_NORMAL_TIME",
            "goal_to90_normal_time": True,
        },
    }
    assert layer.process_outcome(first)
    corrected = {
        **first,
        "outcome_revision": 2,
        "created_at_utc": "2026-08-22T14:00:00+00:00",
        "outcome": {**first["outcome"], "status": "quarantine"},
    }
    written = layer.process_outcome(corrected)
    assert written
    assert all(row["outcome"]["result"] == "UNAVAILABLE" for row in written)
    assert all(row["outcome"]["goal_to90_normal_time"] is None for row in written)
    assert layer.process_outcome(corrected) == []
    report = build_report(
        str(tmp_path / "candidates.jsonl"),
        str(tmp_path / "missing_observation_journal.jsonl"),
    )
    arms = next(iter(report["rulesets"].values()))["arms"]
    control = next(row for row in arms if row["arm_id"] == ARM_CONTROL)
    assert control["wins"] == control["losses"] == control["pending"] == 0
    assert control["invalid_outcome"] == 1


@pytest.mark.parametrize(
    ("status", "conflict", "expected_result"),
    [
        ("void", False, "VOID"),
        ("resolved", True, "UNAVAILABLE"),
    ],
)
def test_invalid_candidate_outcome_never_retains_stale_boolean_label(
    tmp_path, status, conflict, expected_result
):
    layer = _layer(tmp_path)
    snapshot = _snapshot()
    layer.process_snapshot(snapshot)
    source = {
        "record_type": "observation_outcome",
        "observation_id": snapshot["observation_id"],
        "fixture_id": snapshot["fixture_id"],
        "outcome_schema_version": 1,
        "outcome_revision": 1,
        "created_at_utc": "2026-08-22T13:00:00+00:00",
        "outcome": {
            "status": status,
            "outcome_scope": "TO_90_NORMAL_TIME",
            # Defensive input: a corrected invalid record may retain fields
            # copied from its preceding resolved revision.
            "goal_to90_normal_time": True,
            "outcome_integrity_conflict": conflict,
        },
    }

    written = layer.process_outcome(source)

    assert written
    assert all(row["outcome"]["result"] == expected_result for row in written)
    assert all(
        row["outcome"]["goal_to90_normal_time"] is None for row in written
    )
