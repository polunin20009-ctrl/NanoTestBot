from __future__ import annotations

import os
import sqlite3
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from wide_research import FEATURE_SCHEMA_VERSION, Clause, RuleManifest
from wide_research.live import WideShadowLayer
from wide_research.retry_spool import RetrySpool, RetrySpoolError
from wide_research.store import (
    ImmutableRecordError,
    LiveRetryCapacityError,
    UnsafeDatabasePathError,
    WideResearchStore,
)
from outcome_revision import outcome_store_version


UTC = timezone.utc


def _snapshot(fixture_id: int, observed: datetime) -> dict:
    return {
        "record_type": "observation",
        "observation_id": f"{fixture_id}:{observed.minute}:reliability",
        "fixture_id": fixture_id,
        "created_at_utc": observed.isoformat(),
        "stage": "decision_pipeline",
        "minute": 50,
        "match": {"score_home": 0, "score_away": 0, "league_id": 7},
        "probabilities": {"prob_to90": 90.0},
        "features": {},
        "raw_metrics": {},
        "availability": {},
        "gates": {"readiness_passed": True, "publication_context_passed": True},
        "publication_policy": {"publication_context_passed": True},
    }


def _store_with_phases(tmp_path, count: int = 1) -> WideResearchStore:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    for index in range(count):
        manifest = RuleManifest(
            f"rule-{index}",
            "v1",
            (Clause("bot.prob_to90", ">=", 80),),
            feature_schema_version=FEATURE_SCHEMA_VERSION,
        )
        store.register_rule_and_phase(
            rule_id=manifest.rule_id,
            manifest=manifest.as_dict(),
            phase_id=f"phase-{index}",
            starts_at_utc="2026-09-12T00:00:00Z",
            created_at_utc="2026-09-12T00:00:00Z",
            status="shadow",
        )
    return store


def test_disappearing_wal_during_validation_is_not_an_error(tmp_path, monkeypatch) -> None:
    store = _store_with_phases(tmp_path)
    real_lstat = os.lstat

    def racing_lstat(path):
        if os.fspath(path).endswith(("-wal", "-shm")):
            raise FileNotFoundError(os.fspath(path))
        return real_lstat(path)

    monkeypatch.setattr(os, "lstat", racing_lstat)
    store._validate_runtime_paths()


def test_runtime_sidecar_symlink_is_still_rejected(tmp_path) -> None:
    store = _store_with_phases(tmp_path)
    sidecar = store.path + "-wal"
    os.symlink(tmp_path / "target", sidecar)
    with pytest.raises(UnsafeDatabasePathError, match="symlink"):
        store._validate_runtime_paths()


def test_transient_failure_does_not_lose_remaining_phases(tmp_path, monkeypatch) -> None:
    store = _store_with_phases(tmp_path, count=2)
    layer = WideShadowLayer(store, refresh_seconds=0)
    original = store.claim_first_trigger
    calls = 0

    def flaky_claim(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise sqlite3.OperationalError("database is locked")
        return original(**kwargs)

    monkeypatch.setattr(store, "claim_first_trigger", flaky_claim)
    result = layer.process_snapshot(
        _snapshot(10, datetime(2026, 9, 12, 12, tzinfo=UTC))
    )
    assert result["status"] == "PASS"
    assert store.metrics_for_phase("phase-0")["total"] == 1
    assert store.metrics_for_phase("phase-1")["total"] == 1
    assert store.pending_live_evaluations() == []


def test_queued_earlier_snapshot_is_replayed_before_later_one(tmp_path, monkeypatch) -> None:
    store = _store_with_phases(tmp_path)
    layer = WideShadowLayer(store, refresh_seconds=0)
    original = store.claim_first_trigger
    blocked = True

    def unavailable_claim(**kwargs):
        if blocked:
            raise sqlite3.OperationalError("database is locked")
        return original(**kwargs)

    monkeypatch.setattr(store, "claim_first_trigger", unavailable_claim)
    start = datetime(2026, 9, 12, 12, tzinfo=UTC)
    first = _snapshot(20, start)
    later = _snapshot(20, start + timedelta(minutes=1))
    assert layer.process_snapshot(first)["status"] == "DEFERRED"
    assert len(store.pending_live_evaluations()) == 1

    blocked = False
    result = layer.process_snapshot(later)
    assert result["status"] == "PASS"
    trigger = store.list_triggers(phase_id="phase-0")[0]
    assert trigger["observation_id"] == first["observation_id"]
    assert store.pending_live_evaluations() == []


def test_live_outcome_revision_changes_sqlite_version(tmp_path) -> None:
    store = _store_with_phases(tmp_path)
    layer = WideShadowLayer(store, refresh_seconds=0)
    snapshot = _snapshot(30, datetime(2026, 9, 12, 12, tzinfo=UTC))
    layer.process_snapshot(snapshot)
    base = {
        "observation_id": snapshot["observation_id"],
        "outcome_schema_version": 2,
        "created_at_utc": "2026-09-12T14:00:00Z",
        "outcome": {
            "status": "resolved",
            "goal_to90_normal_time": False,
            "resolved_at_utc": "2026-09-12T14:00:00Z",
        },
    }
    layer.process_outcomes([base])
    corrected = dict(base)
    corrected["outcome_revision"] = 1
    corrected["outcome"] = dict(base["outcome"], goal_to90_normal_time=True)
    layer.process_outcomes([corrected])
    trigger = store.list_triggers(phase_id="phase-0")[0]
    assert trigger["latest_outcome_version"] == 2_000_000_001
    assert trigger["latest_outcome_status"] == "win"


def test_retry_spool_capacity_is_atomic_across_instances(tmp_path) -> None:
    path = tmp_path / "retry"
    writers = [
        RetrySpool(str(path), max_records=3, max_bytes=100_000)
        for _ in range(12)
    ]
    start = threading.Barrier(len(writers))

    def enqueue(index: int) -> bool:
        start.wait()
        try:
            writers[index].enqueue("snapshot", f"obs-{index}", {"index": index})
        except RetrySpoolError:
            return False
        return True

    with ThreadPoolExecutor(max_workers=len(writers)) as pool:
        accepted = list(pool.map(enqueue, range(len(writers))))

    assert sum(accepted) == 3
    assert writers[0].status()["pending"] == 3


def test_retry_spool_batch_capacity_failure_accepts_nothing(tmp_path) -> None:
    spool = RetrySpool(
        str(tmp_path / "retry-batch"), max_records=1, max_bytes=100_000
    )

    with pytest.raises(RetrySpoolError, match="spool is full"):
        spool.enqueue_many(
            [
                ("outcomes", "batch-1", {"value": 1}),
                ("outcomes", "batch-2", {"value": 2}),
            ]
        )

    assert spool.status()["pending"] == 0


def test_retry_spool_rejects_non_regular_lock_file(tmp_path) -> None:
    path = tmp_path / "retry-unsafe-lock"
    path.mkdir()
    os.mkfifo(path / ".lock")

    with pytest.raises(RetrySpoolError, match="lock must be a regular file"):
        RetrySpool(str(path))


def test_retry_spool_rejects_json_distinct_evidence_and_cleans_crash_temp(
    tmp_path,
) -> None:
    path = tmp_path / "retry"
    spool = RetrySpool(str(path))
    spool.enqueue("snapshot", "same", {"value": True})
    with pytest.raises(RetrySpoolError, match="different evidence"):
        spool.enqueue("snapshot", "same", {"value": 1})

    orphan = path / ".pending-crashed-writer"
    orphan.write_text("partial", encoding="utf-8")
    reopened = RetrySpool(str(path))
    assert not orphan.exists()
    assert reopened.status()["pending"] == 1


def test_live_inbox_is_immutable_and_bounded(tmp_path) -> None:
    store = WideResearchStore(
        tmp_path / "bounded.sqlite3",
        allowed_root=tmp_path,
        max_live_retry_records=1,
        max_live_retry_bytes=1024,
        max_live_retry_record_bytes=512,
    )
    payload = {"snapshot": {"fixture_id": 1}}
    assert store.enqueue_live_evaluation(
        observation_id="obs-1",
        observed_at_utc="2026-09-12T12:00:00Z",
        payload=payload,
    )
    assert not store.enqueue_live_evaluation(
        observation_id="obs-1",
        observed_at_utc="2026-09-12T12:00:00Z",
        payload=payload,
    )
    with pytest.raises(ImmutableRecordError):
        store.enqueue_live_evaluation(
            observation_id="obs-1",
            observed_at_utc="2026-09-12T12:01:00Z",
            payload=payload,
        )
    with pytest.raises(LiveRetryCapacityError, match="inbox is full"):
        store.enqueue_live_evaluation(
            observation_id="obs-2",
            observed_at_utc="2026-09-12T12:01:00Z",
            payload=payload,
        )
    status = store.live_retry_status()
    assert status["pending"] == 1
    assert status["capacity_records"] == 1
    assert 0 < status["bytes"] <= status["capacity_bytes"]


def test_restart_replays_spooled_snapshots_in_observation_time_order(
    tmp_path, monkeypatch
) -> None:
    store = _store_with_phases(tmp_path)
    spool_path = tmp_path / "snapshot-retry"
    layer = WideShadowLayer(
        store, refresh_seconds=0, retry_spool=RetrySpool(str(spool_path))
    )

    def unavailable_enqueue(**_kwargs):
        raise OSError("storage unavailable")

    monkeypatch.setattr(store, "enqueue_live_evaluation", unavailable_enqueue)
    start = datetime(2026, 9, 12, 12, tzinfo=UTC)
    earlier = _snapshot(40, start)
    later = _snapshot(40, start + timedelta(minutes=1))
    # Arrival order can differ from observation time during recovery.
    assert layer.process_snapshot(later)["status"] == "DEFERRED"
    assert layer.process_snapshot(earlier)["status"] == "DEFERRED"
    assert layer.retry_spool.status()["pending_snapshots"] == 2

    reopened_store = WideResearchStore(
        tmp_path / "wide.sqlite3", allowed_root=tmp_path
    )
    reopened = WideShadowLayer(
        reopened_store,
        refresh_seconds=0,
        retry_spool=RetrySpool(str(spool_path)),
    )
    replay = reopened.replay_pending()

    assert replay["status"] == "OK"
    trigger = reopened_store.list_triggers(phase_id="phase-0")[0]
    assert trigger["observation_id"] == earlier["observation_id"]
    assert reopened.pending_retry_status()["status"] == "OK"


def test_restart_uses_phase_state_at_frozen_observation_time(
    tmp_path, monkeypatch
) -> None:
    store = _store_with_phases(tmp_path)
    spool_path = tmp_path / "causal-phase-retry"
    layer = WideShadowLayer(
        store, refresh_seconds=0, retry_spool=RetrySpool(str(spool_path))
    )
    frozen = _snapshot(45, datetime(2026, 9, 12, 12, 15, tzinfo=UTC))

    def unavailable_enqueue(**_kwargs):
        raise OSError("storage unavailable")

    monkeypatch.setattr(store, "enqueue_live_evaluation", unavailable_enqueue)
    assert layer.process_snapshot(frozen)["status"] == "DEFERRED"
    store.transition_phase(
        "phase-0",
        "retired",
        expected_status="shadow",
        reason="generation_replaced",
        changed_at_utc="2026-09-12T12:30:00Z",
    )
    replacement = RuleManifest(
        "replacement-rule",
        "v1",
        (Clause("bot.prob_to90", ">=", 80),),
        feature_schema_version=FEATURE_SCHEMA_VERSION,
    )
    store.register_rule_and_phase(
        rule_id=replacement.rule_id,
        manifest=replacement.as_dict(),
        phase_id="replacement-phase",
        starts_at_utc="2026-09-12T12:30:00Z",
        created_at_utc="2026-09-12T12:30:00Z",
        status="shadow",
    )

    reopened = WideShadowLayer(
        WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path),
        refresh_seconds=0,
        retry_spool=RetrySpool(str(spool_path)),
    )
    assert reopened.replay_pending()["status"] == "OK"
    assert len(reopened.store.list_triggers(phase_id="phase-0")) == 1
    assert reopened.store.list_triggers(phase_id="replacement-phase") == []


def test_outcome_failure_remains_durable_and_replays_after_restart(
    tmp_path, monkeypatch
) -> None:
    store = _store_with_phases(tmp_path)
    spool_path = tmp_path / "outcome-retry"
    layer = WideShadowLayer(
        store, refresh_seconds=0, retry_spool=RetrySpool(str(spool_path))
    )
    snapshot = _snapshot(50, datetime(2026, 9, 12, 12, tzinfo=UTC))
    assert layer.process_snapshot(snapshot)["status"] == "PASS"

    def unavailable_attach(_outcomes):
        raise OSError("storage unavailable")

    monkeypatch.setattr(store, "attach_outcomes", unavailable_attach)
    outcome = {
        "observation_id": snapshot["observation_id"],
        "outcome_schema_version": 2,
        "outcome_revision": 1,
        "created_at_utc": "2026-09-12T14:00:00Z",
        "outcome": {
            "status": "resolved",
            "goal_to90_normal_time": True,
            "resolved_at_utc": "2026-09-12T14:00:00Z",
        },
    }
    result = layer.process_outcomes([outcome])
    assert result["status"] == "DEFERRED"
    assert layer.retry_spool.status()["pending_outcomes"] == 1
    assert store.list_triggers(phase_id="phase-0")[0][
        "latest_outcome_status"
    ] == "pending"

    reopened_store = WideResearchStore(
        tmp_path / "wide.sqlite3", allowed_root=tmp_path
    )
    reopened = WideShadowLayer(
        reopened_store,
        refresh_seconds=0,
        retry_spool=RetrySpool(str(spool_path)),
    )
    replay = reopened.replay_pending()

    assert replay["status"] == "OK"
    trigger = reopened_store.list_triggers(phase_id="phase-0")[0]
    assert trigger["latest_outcome_status"] == "win"
    assert trigger["latest_outcome_version"] == 2_000_000_001
    assert reopened.pending_retry_status()["status"] == "OK"


def test_legacy_schema_and_revision_versions_have_one_canonical_order(
    tmp_path,
) -> None:
    store = _store_with_phases(tmp_path)
    layer = WideShadowLayer(store, refresh_seconds=0)
    snapshot = _snapshot(60, datetime(2026, 9, 12, 12, tzinfo=UTC))
    layer.process_snapshot(snapshot)

    def attach(record: dict, status: str) -> None:
        version = outcome_store_version(record)
        store.attach_outcomes(
            {
                "observation_id": snapshot["observation_id"],
                "version": version,
                "status": status,
                "outcome_at_utc": "2026-09-12T14:00:00Z",
                "payload": {"version": version, "status": status},
            }
        )

    attach({"outcome_schema_version": 2, "outcome_revision": 1}, "win")
    attach({"outcome_schema_version": 3}, "loss")
    attach({"outcome_schema_version": 2, "outcome_revision": 2}, "win")
    trigger = store.list_triggers(phase_id="phase-0")[0]
    assert trigger["latest_outcome_version"] == 3
    assert trigger["latest_outcome_status"] == "loss"

    attach({"outcome_schema_version": 3, "outcome_revision": 1}, "win")
    trigger = store.list_triggers(phase_id="phase-0")[0]
    assert trigger["latest_outcome_version"] == 3_000_000_001
    assert trigger["latest_outcome_status"] == "win"
    with pytest.raises(ValueError, match="include a revision"):
        store.attach_outcomes(
            {
                "observation_id": snapshot["observation_id"],
                "version": 3_000_000_000,
                "status": "loss",
            }
        )


def test_outcome_batches_respect_spool_record_byte_limit(tmp_path) -> None:
    store = _store_with_phases(tmp_path)
    normal_layer = WideShadowLayer(store, refresh_seconds=0)
    start = datetime(2026, 9, 12, 12, tzinfo=UTC)
    snapshots = [_snapshot(70 + index, start + timedelta(minutes=index)) for index in range(3)]
    for snapshot in snapshots:
        assert normal_layer.process_snapshot(snapshot)["status"] == "PASS"

    outcomes = [
        {
            "observation_id": snapshot["observation_id"],
            "outcome_schema_version": 2,
            "created_at_utc": "2026-09-12T14:00:00Z",
            "outcome": {
                "status": "resolved",
                "goal_to90_normal_time": True,
                "resolved_at_utc": "2026-09-12T14:00:00Z",
                "evidence": "x" * 160,
            },
        }
        for snapshot in snapshots
    ]
    normalized = [WideShadowLayer._normalize_live_outcome(row) for row in outcomes]
    one = len(json.dumps(
        {"outcomes": normalized[:1]}, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8"))
    two = len(json.dumps(
        {"outcomes": normalized[:2]}, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8"))
    assert one < two
    spool = RetrySpool(
        str(tmp_path / "small-outcome-retry"),
        max_record_bytes=two - 1,
        max_bytes=10 * two,
        max_records=10,
    )
    layer = WideShadowLayer(store, refresh_seconds=0, retry_spool=spool)

    result = layer.process_outcomes(outcomes)

    assert result["updated_triggers"] == 3
    assert result["outcome_batches"] == 3
    assert layer.pending_retry_status()["status"] == "OK"


def test_outcome_multi_batch_capacity_failure_is_not_partially_accepted(
    tmp_path,
) -> None:
    store = _store_with_phases(tmp_path)
    start = datetime(2026, 9, 12, 12, tzinfo=UTC)
    snapshots = [_snapshot(90 + index, start + timedelta(minutes=index)) for index in range(2)]
    normal = WideShadowLayer(store, refresh_seconds=0)
    for snapshot in snapshots:
        assert normal.process_snapshot(snapshot)["status"] == "PASS"
    outcomes = [
        {
            "observation_id": snapshot["observation_id"],
            "outcome_schema_version": 2,
            "created_at_utc": "2026-09-12T14:00:00Z",
            "outcome": {
                "status": "resolved",
                "goal_to90_normal_time": True,
                "resolved_at_utc": "2026-09-12T14:00:00Z",
                "evidence": "x" * 160,
            },
        }
        for snapshot in snapshots
    ]
    normalized = [WideShadowLayer._normalize_live_outcome(row) for row in outcomes]
    one_size = len(json.dumps(
        {"outcomes": normalized[:1]}, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8"))
    two_size = len(json.dumps(
        {"outcomes": normalized}, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8"))
    spool = RetrySpool(
        str(tmp_path / "one-batch-capacity"),
        max_record_bytes=two_size - 1,
        max_bytes=10 * two_size,
        max_records=1,
    )
    assert one_size < spool.max_record_bytes
    layer = WideShadowLayer(store, refresh_seconds=0, retry_spool=spool)

    with pytest.raises(RetrySpoolError, match="spool is full"):
        layer.process_outcomes(outcomes)

    assert spool.status()["pending"] == 0
    assert {
        row["latest_outcome_status"] for row in store.list_triggers()
    } == {"pending"}
