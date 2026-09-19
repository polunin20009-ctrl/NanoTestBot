from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone

import pytest

from wide_research.health import evaluate_health
from wide_research.health_snapshot import (
    ObservationJournalSpec,
    ProfileSnapshotSpec,
    build_health_snapshot,
)


NOW = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


def _database(path, profile_id: str, statuses=("shadow", "shadow")) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE store_profile(singleton INTEGER PRIMARY KEY, profile_id TEXT NOT NULL);
        CREATE TABLE phases(phase_id TEXT PRIMARY KEY, status TEXT NOT NULL);
        CREATE TABLE research_runs(
            run_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            started_at_utc TEXT NOT NULL,
            completed_at_utc TEXT
        );
        """
    )
    connection.execute("INSERT INTO store_profile VALUES(1, ?)", (profile_id,))
    connection.executemany(
        "INSERT INTO phases VALUES(?, ?)",
        [(f"phase-{index}", status) for index, status in enumerate(statuses)],
    )
    connection.execute(
        "INSERT INTO research_runs VALUES(?, ?, ?, ?)",
        (
            "run-1",
            "completed",
            "2026-09-11T07:00:00Z",
            "2026-09-11T08:00:00Z",
        ),
    )
    connection.commit()
    connection.close()


def _summary(path, profile_id: str, *, not_admitted=()) -> None:
    path.write_text(
        json.dumps(
            {
                "store_profile": profile_id,
                "cycle_type": "discovery",
                "run_id": "run-2",
                "completed_at_utc": "2026-09-11T09:00:00+00:00",
                "registry": {"not_admitted": list(not_admitted)},
            }
        ),
        encoding="utf-8",
    )


def _spec(tmp_path, profile_id="rare_precision_shadow", **overrides):
    database = tmp_path / f"{profile_id}.sqlite3"
    summary = tmp_path / f"{profile_id}.json"
    statuses = overrides.pop("statuses", ("shadow", "shadow"))
    _database(database, profile_id, statuses=statuses)
    _summary(summary, profile_id)
    values = {
        "profile_id": profile_id,
        "database_path": database,
        "summary_path": summary,
        "capacity": 64,
        "interval_seconds": 604_800,
    }
    values.update(overrides)
    return ProfileSnapshotSpec(**values), database, summary


def test_collects_profile_summary_pool_journal_disk_and_runtime_counters(tmp_path):
    spec, _, summary = _spec(tmp_path)
    _summary(summary, spec.profile_id, not_admitted=("a", "b"))
    journal = tmp_path / "observations.jsonl"
    journal.write_text(
        "\n".join(
            [
                json.dumps({"record_type": "observation_outcome", "created_at_utc": "2026-09-12T10:30:00Z"}),
                json.dumps({"record_type": "observation", "stage": "prefilter", "created_at_utc": "2026-09-12T10:00:00Z"}),
                json.dumps({"record_type": "observation", "stage": "prefilter", "created_at_utc": "2026-09-12T11:59:00+00:00"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    outcomes = {"pending": 2, "overdue": 0}
    retries = {"pending": 1, "failed": 0}
    snapshot = build_health_snapshot(
        profiles=[spec],
        observation_journal=ObservationJournalSpec(journal, stages=("prefilter",)),
        expected_eligible=True,
        eligible_active=1,
        outcome_counters=outcomes,
        retry_counters=retries,
        disk_path=tmp_path,
    )
    assert snapshot["sources"]["ok"] is True
    assert snapshot["observations"]["last_seen_at"] == "2026-09-12T11:59:00Z"
    assert snapshot["discovery"]["profiles"][spec.profile_id]["last_completed_at"] == "2026-09-11T09:00:00Z"
    assert snapshot["pools"]["profiles"][spec.profile_id] == {
        "occupied": 2,
        "capacity": 64,
        "configured_capacity": 64,
        "effective_capacity": 64,
        "reported_configured_capacity": None,
        "purge_transition_reserve": 0,
        "purge_transition_active": False,
        "purge_transition_portfolio_deferred": False,
        "not_admitted": 2,
    }
    assert snapshot["outcomes"] == outcomes and snapshot["outcomes"] is not outcomes
    assert snapshot["retries"] == retries and snapshot["retries"] is not retries
    assert snapshot["disk"]["free_bytes"] > 0
    assert evaluate_health(snapshot, NOW)["overall"] == "warning"  # two rejected candidates
    json.dumps(snapshot)


def test_sqlite_is_opened_read_only_and_evaluated_statuses_define_occupancy(tmp_path):
    spec, database, _ = _spec(
        tmp_path,
        statuses=("candidate", "shadow", "ready", "active", "paused", "retired", "failed"),
    )
    before = (database.stat().st_mtime_ns, database.read_bytes())
    snapshot = build_health_snapshot(
        profiles=[spec], observation_journal=None, expected_eligible=False
    )
    after = (database.stat().st_mtime_ns, database.read_bytes())
    assert snapshot["pools"]["profiles"][spec.profile_id]["occupied"] == 4
    assert before == after
    assert not (tmp_path / f"{database.name}-wal").exists()


def test_running_start_uses_earliest_explicit_or_database_timestamp(tmp_path):
    spec, database, _ = _spec(
        tmp_path, running_since="2026-09-12T09:00:00+00:00"
    )
    connection = sqlite3.connect(database)
    connection.execute(
        "INSERT INTO research_runs VALUES(?, ?, ?, NULL)",
        ("run-live", "running", "2026-09-12T08:30:00Z"),
    )
    connection.commit()
    connection.close()
    snapshot = build_health_snapshot(
        profiles=[spec], observation_journal=None, expected_eligible=False
    )
    profile = snapshot["discovery"]["profiles"][spec.profile_id]
    assert profile["running_since"] == "2026-09-12T08:30:00Z"


def test_bounded_tail_ignores_partial_first_line_and_malformed_records(tmp_path):
    spec, _, _ = _spec(tmp_path)
    journal = tmp_path / "observations.jsonl"
    old = json.dumps(
        {"record_type": "observation", "created_at_utc": "2026-09-12T08:00:00Z", "padding": "x" * 500}
    )
    newest = json.dumps(
        {"record_type": "observation", "created_at_utc": "2026-09-12T11:50:00Z"}
    )
    journal.write_text(old + "\n{bad json}\n" + newest + "\n", encoding="utf-8")
    snapshot = build_health_snapshot(
        profiles=[spec],
        observation_journal=ObservationJournalSpec(journal, max_tail_bytes=180),
        expected_eligible=True,
    )
    assert snapshot["observations"]["last_seen_at"] == "2026-09-12T11:50:00Z"
    source = snapshot["sources"]["observation_journal"]
    assert source["tail_truncated"] is True
    assert source["records_malformed"] == 1


def test_missing_sources_fail_closed_without_creating_a_database(tmp_path):
    missing_db = tmp_path / "missing.sqlite3"
    missing_summary = tmp_path / "missing.json"
    spec = ProfileSnapshotSpec(
        profile_id="primary",
        database_path=missing_db,
        summary_path=missing_summary,
        capacity=10,
        interval_seconds=86_400,
    )
    snapshot = build_health_snapshot(
        profiles=[spec], observation_journal=None, expected_eligible=False
    )
    assert snapshot["sources"]["ok"] is False
    assert len(snapshot["sources"]["errors"]) == 2
    assert snapshot["pools"]["profiles"]["primary"]["capacity"] == 0
    report = evaluate_health(snapshot, NOW)
    assert next(item for item in report["checks"] if item["id"] == "pools.primary")["level"] == "critical"
    assert not missing_db.exists()


def test_store_profile_mismatch_is_rejected_as_source_error(tmp_path):
    spec, _, _ = _spec(tmp_path, profile_id="primary")
    wrong = ProfileSnapshotSpec(
        profile_id="precision_shadow",
        database_path=spec.database_path,
        summary_path=spec.summary_path,
        capacity=40,
        interval_seconds=86_400,
    )
    snapshot = build_health_snapshot(
        profiles=[wrong], observation_journal=None, expected_eligible=False
    )
    assert snapshot["sources"]["ok"] is False
    assert {item["source"] for item in snapshot["sources"]["errors"]} == {
        "profile_database",
        "discovery_summary",
    }


def test_symlinked_journal_is_not_followed(tmp_path):
    spec, _, _ = _spec(tmp_path)
    real = tmp_path / "real.jsonl"
    real.write_text(
        json.dumps({"record_type": "observation", "created_at_utc": "2026-09-12T11:00:00Z"}) + "\n",
        encoding="utf-8",
    )
    link = tmp_path / "link.jsonl"
    link.symlink_to(real)
    snapshot = build_health_snapshot(
        profiles=[spec],
        observation_journal=ObservationJournalSpec(link),
        expected_eligible=True,
    )
    assert snapshot["observations"]["last_seen_at"] is None
    assert any(item["source"] == "observation_journal" for item in snapshot["sources"]["errors"])


def test_duplicate_profiles_and_invalid_counters_are_rejected(tmp_path):
    spec, _, _ = _spec(tmp_path)
    with pytest.raises(ValueError, match="duplicate profile_id"):
        build_health_snapshot(
            profiles=[spec, spec], observation_journal=None, expected_eligible=False
        )
    with pytest.raises(ValueError, match="eligible_active"):
        build_health_snapshot(
            profiles=[spec], observation_journal=None, expected_eligible=False, eligible_active=-1
        )


def test_runtime_counter_inputs_are_not_mutated(tmp_path):
    spec, _, _ = _spec(tmp_path)
    outcomes = {"items": [{"finished_at": "2026-09-12T11:00:00Z"}]}
    snapshot = build_health_snapshot(
        profiles=[spec],
        observation_journal=None,
        expected_eligible=False,
        outcome_counters=outcomes,
    )
    snapshot["outcomes"]["items"][0]["finished_at"] = "changed"
    assert outcomes["items"][0]["finished_at"] == "2026-09-12T11:00:00Z"


def test_purge_transition_uses_registry_effective_pool_limit(tmp_path):
    profile_id = "primary"
    database = tmp_path / "primary.sqlite3"
    summary = tmp_path / "primary.json"
    _database(database, profile_id, statuses=("shadow",) * 20)
    summary.write_text(
        json.dumps(
            {
                "store_profile": profile_id,
                "cycle_type": "discovery",
                "completed_at_utc": "2026-09-11T09:00:00Z",
                "registry": {
                    "not_admitted": [],
                    "purge_transition_portfolio_deferred": False,
                    "pool": {
                        "limit": 10,
                        "effective_limit": 20,
                        "purge_transition_reserve": 10,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    spec = ProfileSnapshotSpec(
        profile_id=profile_id,
        database_path=database,
        summary_path=summary,
        capacity=10,
        interval_seconds=86_400,
    )

    snapshot = build_health_snapshot(
        profiles=[spec], observation_journal=None, expected_eligible=False
    )
    pool = snapshot["pools"]["profiles"][profile_id]
    assert pool == {
        "occupied": 20,
        "capacity": 20,
        "configured_capacity": 10,
        "effective_capacity": 20,
        "reported_configured_capacity": 10,
        "purge_transition_reserve": 10,
        "purge_transition_active": True,
        "purge_transition_portfolio_deferred": False,
        "not_admitted": 0,
    }
    check = next(
        row
        for row in evaluate_health(snapshot, NOW)["checks"]
        if row["id"] == "pools.primary"
    )
    assert check["level"] == "ok"
    assert check["metrics"]["occupancy_fraction"] == 1.0
