from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.report_shadow_candidates import (
    _candidate_outcome_rank,
    _outcome_rank,
    build_report,
    main,
    render_human,
    wilson_interval,
)
from shadow_candidates.engine import CandidateLayer
from shadow_candidates.rules import ARM_CONTROL, ARM_FULL_SLICES
from shadow_candidates.storage import AppendOnlyCandidateJournal


def test_legacy_candidate_and_source_outcomes_use_the_same_zero_revision():
    candidate = {
        "source_outcome_schema_version": 1,
        "source_outcome_created_at_utc": "2026-09-12T12:00:00+00:00",
    }
    older_source = {
        "outcome_schema_version": 1,
        "created_at_utc": "2026-09-12T11:00:00+00:00",
    }
    assert _candidate_outcome_rank(candidate) > _outcome_rank(older_source)


def _snapshot(fixture_id: int, rolling_ok: bool) -> dict:
    observation_id = f"{fixture_id}:50:WINDOW_1:ALLOW:v3"
    rolling = {
        "schema_version": 1,
        "mode": "shadow_collection",
        "production_applied": False,
        "available_windows": 2,
        "windows": {
            "5m": {
                "status": "ok",
                "requested_window_minutes": 5,
                "actual_span_minutes": 5,
                "available_activity_metric_count": 4,
            },
            "10m": {
                "status": "ok",
                "requested_window_minutes": 10,
                "actual_span_minutes": 10,
                "available_activity_metric_count": 4,
            },
        },
    }
    return {
        "record_type": "observation",
        "observation_id": observation_id,
        "fixture_id": fixture_id,
        "minute": 50,
        "created_at_utc": "2026-08-22T12:00:00+00:00",
        "schema_version": 1,
        "stage": "decision_pipeline",
        "match": {"score_home": 1, "score_away": 0},
        "publication_policy": {
            "filter_version": "base_rep15_int055_season102_p90_75_v1",
            "publication_context_passed": True,
            "publication_allow": True,
        },
        "channel_signal_filter": {
            "version": "base_rep15_int055_season102_p90_75_v1",
            "passed": True,
            "prob_to90": 80,
            "reputation_base_prob_to90": 78,
            "reputation_adjusted_prob_to90": 80,
            "adjusted_intensity": 0.6,
            "season_context_factor": 1.03,
            "score_home": 1,
            "score_away": 0,
        },
        "rolling_dynamics": rolling if rolling_ok else None,
    }


def _outcome(snapshot: dict, won: bool, schema: int = 1) -> dict:
    return {
        "record_type": "observation_outcome",
        "observation_id": snapshot["observation_id"],
        "fixture_id": snapshot["fixture_id"],
        "outcome_schema_version": schema,
        "created_at_utc": f"2026-08-22T13:00:0{schema}+00:00",
        "outcome": {
            "status": "resolved",
            "outcome_scope": "TO_90_NORMAL_TIME",
            "goal_to90_normal_time": won,
            "outcome_integrity_conflict": False,
        },
    }


def test_report_joins_active_and_gzip_outcomes_and_uses_first_control(tmp_path: Path) -> None:
    journal_path = tmp_path / "candidate.jsonl"
    observation_path = tmp_path / "observation_history.jsonl"
    layer = CandidateLayer(
        AppendOnlyCandidateJournal(journal_path, rotate_max_bytes=0),
        prospective_start_utc="2026-08-22T12:00:00+00:00",
        now_factory=lambda: datetime(2026, 8, 22, 12, 1, tzinfo=timezone.utc),
    )
    won = _snapshot(100, rolling_ok=True)
    lost = _snapshot(200, rolling_ok=False)
    layer.process_snapshot(won)
    layer.process_snapshot(lost)

    archive = tmp_path / "observation_history.20260822T130000000000Z.jsonl.gz"
    with gzip.open(archive, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(_outcome(won, True)) + "\n")
    observation_path.write_text(json.dumps(_outcome(lost, False)) + "\n", encoding="utf-8")

    report = build_report(str(journal_path), str(observation_path))
    ruleset = next(iter(report["rulesets"].values()))
    arms = {row["arm_id"]: row for row in ruleset["arms"]}

    assert ruleset["first_control_fixture_count"] == 2
    assert arms[ARM_CONTROL]["selected"] == 2
    assert arms[ARM_CONTROL]["wins"] == 1
    assert arms[ARM_CONTROL]["losses"] == 1
    assert arms[ARM_CONTROL]["wilson_95"]["rate_pct"] == 50.0
    assert arms[ARM_FULL_SLICES]["selected"] == 1
    assert arms[ARM_FULL_SLICES]["wins"] == 1
    assert arms[ARM_FULL_SLICES]["rejected_unavailable"] == 1
    assert "Матчей в first-control cohort: 2" in render_human(report)


def test_report_prefers_newer_outcome_schema(tmp_path: Path) -> None:
    journal_path = tmp_path / "candidate.jsonl"
    observation_path = tmp_path / "observation_history.jsonl"
    snapshot = _snapshot(100, rolling_ok=False)
    layer = CandidateLayer(
        AppendOnlyCandidateJournal(journal_path, rotate_max_bytes=0),
        prospective_start_utc="2026-08-22T12:00:00+00:00",
        now_factory=lambda: datetime(2026, 8, 22, 12, 1, tzinfo=timezone.utc),
    )
    layer.process_snapshot(snapshot)
    assert len(layer.process_outcome(_outcome(snapshot, True, schema=1))) == 1
    observation_path.write_text(
        json.dumps(_outcome(snapshot, True, schema=1))
        + "\n"
        + json.dumps(_outcome(snapshot, False, schema=2))
        + "\n",
        encoding="utf-8",
    )

    report = build_report(str(journal_path), str(observation_path))
    ruleset = next(iter(report["rulesets"].values()))
    control = next(row for row in ruleset["arms"] if row["arm_id"] == ARM_CONTROL)

    assert control["wins"] == 0
    assert control["losses"] == 1


def test_report_uses_own_candidate_outcomes_without_observation_history(
    tmp_path: Path,
) -> None:
    journal_path = tmp_path / "candidate.jsonl"
    missing_observation_path = tmp_path / "missing_observation_history.jsonl"
    snapshot = _snapshot(300, rolling_ok=False)
    layer = CandidateLayer(
        AppendOnlyCandidateJournal(journal_path, rotate_max_bytes=0),
        prospective_start_utc="2026-08-22T12:00:00+00:00",
        now_factory=lambda: datetime(2026, 8, 22, 12, 1, tzinfo=timezone.utc),
    )
    layer.process_snapshot(snapshot)
    assert len(layer.process_outcome(_outcome(snapshot, True))) == 1

    report = build_report(str(journal_path), str(missing_observation_path))
    ruleset = next(iter(report["rulesets"].values()))
    control = next(row for row in ruleset["arms"] if row["arm_id"] == ARM_CONTROL)

    assert control["selected"] == 1
    assert control["wins"] == 1
    assert control["pending"] == 0
    assert report["outcome_source_policy"] == (
        "latest_shadow_candidate_or_filtered_observation_outcome"
    )


def test_report_refuses_to_overwrite_active_or_archive_source(tmp_path: Path) -> None:
    journal = tmp_path / "candidate.jsonl"
    observations = tmp_path / "observation_history.jsonl"
    journal.write_text("", encoding="utf-8")
    observations.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="refusing to overwrite source"):
        main(
            [
                "--journal",
                str(journal),
                "--observation-file",
                str(observations),
                "--output",
                str(journal),
            ]
        )

    with pytest.raises(ValueError, match="refusing to overwrite source"):
        main(
            [
                "--journal",
                str(journal),
                "--observation-file",
                str(observations),
                "--output",
                str(journal) + ".index.sqlite3",
            ]
        )


def test_wilson_empty_and_perfect_samples_are_bounded() -> None:
    assert wilson_interval(0, 0)["rate_pct"] is None
    interval = wilson_interval(10, 0)
    assert interval["rate_pct"] == 100.0
    assert 0.0 < interval["low_pct"] < interval["high_pct"] <= 100.0
