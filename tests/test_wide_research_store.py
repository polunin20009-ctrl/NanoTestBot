from __future__ import annotations

import os
import sqlite3
import stat
from concurrent.futures import ThreadPoolExecutor

import pytest

from wide_research.store import (
    ConcurrentUpdateError,
    DatabaseSizeLimitError,
    ImmutableRecordError,
    InvalidLifecycleTransition,
    UnsafeDatabasePathError,
    WideResearchStore,
    WideResearchStoreError,
    safe_database_path,
)


START = "2026-08-27T10:00:00Z"


def make_store(tmp_path, **kwargs) -> WideResearchStore:
    return WideResearchStore(
        tmp_path / "wide_research.sqlite3",
        allowed_root=tmp_path,
        **kwargs,
    )


def test_database_profile_binding_is_permanent(tmp_path) -> None:
    store = make_store(tmp_path)
    first = store.bind_profile("primary")
    assert first["profile_id"] == "primary"
    assert store.bind_profile("primary") == first

    reopened = make_store(tmp_path)
    assert reopened.bind_profile("primary") == first
    with pytest.raises(WideResearchStoreError, match="profile mismatch"):
        reopened.bind_profile("exact_four_shadow")


def register(
    store: WideResearchStore,
    *,
    rule_id: str = "rule-v1",
    phase_id: str = "phase-v1",
    status: str = "shadow",
    start: str = START,
) -> None:
    store.register_rule(rule_id, {"all": [{"feature": "p90", "gte": 0.8}]})
    store.register_phase(
        phase_id,
        rule_id,
        status=status,
        starts_at_utc=start,
        created_at_utc=start,
        policy={"first_trigger": True},
    )


def claim(
    store: WideResearchStore,
    *,
    phase_id: str = "phase-v1",
    rule_id: str = "rule-v1",
    fixture_id: str = "fixture-1",
    observation_id: str = "obs-1",
    when: str = "2026-08-27T10:05:00Z",
    league: str = "League A",
) -> bool:
    return store.claim_first_trigger(
        phase_id=phase_id,
        rule_id=rule_id,
        fixture_id=fixture_id,
        observation_id=observation_id,
        triggered_at_utc=when,
        minute=55,
        league=league,
        input_data={"p90": 0.84, "goals": 1},
    )


def test_sqlite_is_source_of_truth_durable_and_secure(tmp_path):
    store = make_store(tmp_path)
    run = store.start_run(
        "discovery-1",
        config={"train_until": "2026-08-26"},
        started_at_utc=START,
    )
    assert run["status"] == "running"
    register(store)
    assert claim(store)

    mode = stat.S_IMODE(os.stat(store.path).st_mode)
    assert mode == 0o600
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert {
        "research_runs",
        "rules",
        "phases",
        "universe_fixtures",
        "triggers",
        "outcome_events",
        "lifecycle_events",
        "active_pointer",
        "alpha_allocations",
        "phase_looks",
    } <= tables

    reopened = make_store(tmp_path)
    assert reopened.recover()["triggers"] == 1
    assert reopened.list_triggers()[0]["observation_id"] == "obs-1"
    # Generated timestamps do not break idempotent restart registration.
    assert reopened.start_run("discovery-1", config={"train_until": "2026-08-26"})[
        "started_at_utc"
    ] == START.replace("Z", ".000000Z")
    reopened.register_phase(
        "phase-v1",
        "rule-v1",
        policy={"first_trigger": True},
    )


def test_run_alpha_budget_is_atomic_immutable_and_telescopically_bounded(tmp_path):
    store = make_store(tmp_path)
    store.start_run("run-1")
    first = store.allocate_alpha_budget("run-1", global_alpha=0.05)
    store.start_run("run-2")
    second = store.allocate_alpha_budget("run-2", global_alpha=0.05)
    assert first["sequence"] == 1
    assert first["alpha_budget"] == pytest.approx(0.025)
    assert second["sequence"] == 2
    assert second["alpha_budget"] == pytest.approx(0.05 / 6.0)
    assert store.allocate_alpha_budget("run-1", global_alpha=0.05) == first
    with pytest.raises(ImmutableRecordError):
        store.allocate_alpha_budget("run-1", global_alpha=0.04)


def _atomic_import_plan() -> tuple[dict, list[dict]]:
    phases = [
        {
            "rule_id": f"atomic-rule-{index}",
            "manifest": {"rule_id": f"atomic-rule-{index}", "value": index},
            "phase_id": f"atomic-phase-{index}",
            "starts_at_utc": START,
            "policy": {"lifecycle": {"frozen": True}},
        }
        for index in range(2)
    ]
    config = {
        "planned_phase_ids": [value["phase_id"] for value in phases],
        "family_size": len(phases),
    }
    return config, phases


def test_discovery_family_import_rolls_back_everything_on_mid_write_failure(
    tmp_path, monkeypatch
) -> None:
    store = make_store(tmp_path)
    config, phases = _atomic_import_plan()
    original = store._insert_lifecycle_event
    calls = 0

    def fail_on_second_phase(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected import failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(store, "_insert_lifecycle_event", fail_on_second_phase)
    with pytest.raises(RuntimeError, match="injected import failure"):
        store.commit_discovery_import(
            run_id="atomic-run",
            run_config=config,
            global_alpha=0.05,
            phases=phases,
            expected_phase_snapshot={},
            completed_at_utc=START,
        )

    assert store.list_phases() == []
    assert store.list_manifests() == []
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM research_runs").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM alpha_allocations").fetchone()[0] == 0


def test_discovery_family_import_is_idempotent_across_competing_workers(
    tmp_path,
) -> None:
    first = make_store(tmp_path)
    second = make_store(tmp_path)
    config, phases = _atomic_import_plan()

    def commit(store: WideResearchStore) -> dict:
        return store.commit_discovery_import(
            run_id="atomic-run",
            run_config=config,
            global_alpha=0.05,
            phases=phases,
            expected_phase_snapshot={},
            completed_at_utc=START,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(commit, (first, second)))

    assert sorted(result["already_completed"] for result in results) == [False, True]
    assert all(
        result["run"]["started_at_utc"]
        <= result["run"]["completed_at_utc"]
        for result in results
    )
    stored = first.list_phases()
    assert len(stored) == 2
    assert {row["policy"]["statistical_family"]["family_size"] for row in stored} == {2}
    assert first.recover()["alpha_allocations"] == 1


def test_phase_look_freezes_trigger_identities_before_outcomes(tmp_path):
    store = make_store(tmp_path)
    register(store)
    assert claim(store, fixture_id="f1", observation_id="o1", when="2026-08-27T10:01:00Z")
    assert claim(store, fixture_id="f2", observation_id="o2", when="2026-08-27T10:02:00Z")
    frozen = store.freeze_phase_look("phase-v1", 2)
    assert [row["fixture_id"] for row in frozen["trigger_ids"]] == ["f1", "f2"]
    assert claim(store, fixture_id="f0", observation_id="o0", when="2026-08-27T10:00:00Z")
    assert [
        row["fixture_id"] for row in store.triggers_for_phase_look("phase-v1", 2)
    ] == ["f1", "f2"]


def test_phase_transition_and_pointer_swap_are_one_transaction(tmp_path):
    store = make_store(tmp_path)
    register(store, status="ready")
    promoted = store.transition_phase_and_set_pointer(
        "phase-v1",
        "active",
        expected_status="ready",
        pointer_name="prod",
        pointer_phase_id="phase-v1",
        expected_generation=0,
        reason="promote",
    )
    assert promoted["phase"]["status"] == "active"
    assert promoted["pointer"]["phase_id"] == "phase-v1"
    degraded = store.transition_phase_and_set_pointer(
        "phase-v1",
        "degraded",
        expected_status="active",
        pointer_name="prod",
        pointer_phase_id=None,
        expected_generation=1,
        reason="degrade",
    )
    assert degraded["phase"]["status"] == "degraded"
    assert degraded["pointer"]["phase_id"] is None


def test_rules_and_phase_definitions_are_immutable(tmp_path):
    store = make_store(tmp_path)
    first = store.register_rule("r", {"feature": "pressure", "gte": 15})
    same = store.register_rule("r", {"gte": 15, "feature": "pressure"})
    assert first["manifest_hash"] == same["manifest_hash"]
    with pytest.raises(ImmutableRecordError):
        store.register_rule("r", {"feature": "pressure", "gte": 16})

    store.register_phase(
        "p", "r", starts_at_utc=START, created_at_utc=START,
        policy={"minimum_samples": 100}
    )
    with pytest.raises(ImmutableRecordError):
        store.register_phase(
            "p", "r", starts_at_utc=START, policy={"minimum_samples": 50}
        )
    active = store.list_active_phases(at_utc="2026-08-27T11:00:00Z")
    assert active[0]["manifest"] == {"feature": "pressure", "gte": 15}
    assert store.list_manifests(active_only=True)[0]["rule_id"] == "r"


def test_universe_preserves_first_and_updates_only_latest_snapshot(tmp_path):
    store = make_store(tmp_path)
    assert store.record_universe(
        fixture_id="f",
        observation_id="old",
        observed_at_utc="2026-08-27T10:01:00Z",
        minute=51,
        league="A",
        input_data={"value": 1},
    )
    assert not store.record_universe(
        fixture_id="f",
        observation_id="new",
        observed_at_utc="2026-08-27T10:03:00Z",
        minute=53,
        league="B",
        input_data={"value": 2},
    )
    assert not store.record_universe(
        fixture_id="f",
        observation_id="late-old",
        observed_at_utc="2026-08-27T10:02:00Z",
        minute=52,
        input_data={"value": 99},
    )
    row = store.get_universe_fixture("f")
    assert row is not None
    assert row["first_observation_id"] == "old"
    assert row["first_input"] == {"value": 1}
    assert row["latest_observation_id"] == "new"
    assert row["latest_input"] == {"value": 2}


def test_first_trigger_claim_is_atomic_across_workers(tmp_path):
    store = make_store(tmp_path)
    register(store)

    def try_claim(index: int) -> bool:
        return claim(
            store,
            observation_id=f"obs-{index}",
            when=f"2026-08-27T10:05:{index:02d}Z",
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(try_claim, range(16)))
    assert sum(results) == 1
    triggers = store.list_triggers()
    assert len(triggers) == 1
    assert triggers[0]["input"] == {"goals": 1, "p90": 0.84}


def test_late_versioned_outcome_updates_all_rule_versions(tmp_path):
    store = make_store(tmp_path)
    register(store, rule_id="r-old", phase_id="p-old")
    register(store, rule_id="r-new", phase_id="p-new")
    assert claim(store, rule_id="r-old", phase_id="p-old")
    assert claim(store, rule_id="r-new", phase_id="p-new")

    first = store.attach_outcomes(
        {
            "event_id": "outcome-v1",
            "observation_id": "obs-1",
            "version": 1,
            "status": "loss",
            "outcome_at_utc": "2026-08-27T11:00:00Z",
            "payload": {"final_score": "0-0"},
        }
    )
    assert first == {
        "inserted": 1,
        "duplicates": 0,
        "updated_triggers": 2,
        "unmatched": 0,
    }
    store.transition_phase(
        "p-old", "retired", expected_status="shadow", reason="replaced"
    )
    corrected = store.attach_outcomes(
        {
            "event_id": "outcome-v2",
            "observation_id": "obs-1",
            "version": 2,
            "status": "win",
            "outcome_at_utc": "2026-08-27T11:01:00Z",
            "payload": {"final_score": "1-0", "correction": True},
        }
    )
    assert corrected["updated_triggers"] == 2
    assert store.metrics_for_phase("p-old")["win"] == 1
    assert store.metrics_for_phase("p-new")["win"] == 1

    reopened = make_store(tmp_path)
    rows = reopened.list_triggers()
    assert {row["latest_outcome_version"] for row in rows} == {2}
    assert {row["latest_outcome_status"] for row in rows} == {"win"}


def test_outcome_events_are_deduped_and_conflicts_fail_closed(tmp_path):
    store = make_store(tmp_path)
    event = {
        "event_id": "evt",
        "observation_id": "not-triggered-yet",
        "version": 1,
        "status": "win",
        "outcome_at_utc": "2026-08-27T11:00:00Z",
        "payload": {"goals": [89]},
    }
    assert store.attach_outcomes(event)["unmatched"] == 1
    assert store.attach_outcomes(event)["duplicates"] == 1
    with pytest.raises(ImmutableRecordError):
        store.attach_outcomes({**event, "status": "loss"})

    register(store)
    assert claim(store, observation_id="not-triggered-yet")
    trigger = store.list_triggers()[0]
    assert trigger["latest_outcome_status"] == "win"
    assert trigger["latest_outcome_version"] == 1


def test_metrics_include_pending_invalid_dates_and_leagues(tmp_path):
    store = make_store(tmp_path)
    register(store)
    values = [
        ("f1", "o1", "2026-08-27T10:05:00Z", "A"),
        ("f2", "o2", "2026-08-27T10:06:00Z", "A"),
        ("f3", "o3", "2026-08-28T10:07:00Z", "B"),
        ("f4", "o4", "2026-08-28T10:08:00Z", "B"),
    ]
    for fixture, observation, when, league in values:
        assert claim(
            store,
            fixture_id=fixture,
            observation_id=observation,
            when=when,
            league=league,
        )
    store.attach_outcomes(
        [
            {
                "observation_id": "o1",
                "version": 1,
                "status": "win",
                "outcome_at_utc": "2026-08-28T12:00:00Z",
            },
            {
                "observation_id": "o2",
                "version": 1,
                "status": "loss",
                "outcome_at_utc": "2026-08-28T12:00:00Z",
            },
            {
                "observation_id": "o3",
                "version": 1,
                "status": "invalid",
                "outcome_at_utc": "2026-08-28T12:00:00Z",
            },
        ]
    )
    metrics = store.metrics_for_phase("phase-v1")
    assert metrics["total"] == 4
    assert (metrics["win"], metrics["loss"], metrics["pending"], metrics["invalid"]) == (
        1,
        1,
        1,
        1,
    )
    assert metrics["accuracy"] == 0.5
    assert [entry["date"] for entry in metrics["by_date"]] == [
        "2026-08-27",
        "2026-08-28",
    ]
    assert [entry["league"] for entry in metrics["by_league"]] == ["A", "B"]


def test_lifecycle_and_active_pointer_use_compare_and_swap(tmp_path):
    store = make_store(tmp_path)
    register(store)
    with pytest.raises(InvalidLifecycleTransition):
        store.set_active_pointer(
            "telegram", phase_id="phase-v1", expected_generation=0
        )
    store.transition_phase(
        "phase-v1",
        "ready",
        expected_status="shadow",
        reason="prospective thresholds passed",
        metadata={"samples": 120},
    )
    store.transition_phase(
        "phase-v1",
        "active",
        expected_status="ready",
        reason="promoted for pointer test",
    )
    pointer = store.set_active_pointer(
        "telegram",
        phase_id="phase-v1",
        expected_generation=0,
        metadata={"approved": True},
        updated_at_utc="2026-08-27T12:00:00Z",
    )
    assert pointer["generation"] == 1
    assert pointer["rule_id"] == "rule-v1"
    assert make_store(tmp_path).get_active_pointer("telegram")["checksum"] == pointer[
        "checksum"
    ]
    with pytest.raises(ConcurrentUpdateError):
        store.set_active_pointer(
            "telegram", phase_id="phase-v1", expected_generation=0
        )
    cleared = store.set_active_pointer(
        "telegram", phase_id=None, expected_generation=1
    )
    assert cleared["generation"] == 2
    assert cleared["phase_id"] is None


def test_pointer_checksum_corruption_is_detected(tmp_path):
    store = make_store(tmp_path)
    register(store, status="active")
    store.set_active_pointer("prod", phase_id="phase-v1", expected_generation=0)
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE active_pointer SET checksum='tampered' WHERE pointer_name='prod'"
        )
    with pytest.raises(WideResearchStoreError, match="checksum mismatch"):
        store.get_active_pointer("prod")


def test_database_size_limit_fails_closed_without_losing_reads(tmp_path):
    store = make_store(tmp_path)
    register(store)
    size = store.database_size_bytes()
    store.max_db_bytes = size + 100
    with pytest.raises(DatabaseSizeLimitError):
        store.register_rule("too-large", {"blob": "x" * 1000})
    assert store.list_manifests()[0]["rule_id"] == "rule-v1"


def test_safe_realpath_validation_and_timezone_validation(tmp_path):
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    assert safe_database_path(allowed / "db.sqlite", allowed_root=allowed).startswith(
        str(allowed)
    )
    with pytest.raises(UnsafeDatabasePathError):
        safe_database_path(tmp_path / "outside.sqlite", allowed_root=allowed)

    real = allowed / "real.sqlite"
    real.touch()
    link = allowed / "link.sqlite"
    link.symlink_to(real)
    with pytest.raises(UnsafeDatabasePathError):
        safe_database_path(link, allowed_root=allowed)

    store = WideResearchStore(allowed / "valid.sqlite", allowed_root=allowed)
    store.register_rule("r", {"feature": "x"})
    with pytest.raises(ValueError, match="timezone"):
        store.register_phase("p", "r", starts_at_utc="2026-08-27T10:00:00")
