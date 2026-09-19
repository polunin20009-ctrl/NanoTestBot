from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone

import NanoTest as bot
import pytest

from wide_research.health_runtime import ResearchHealthMonitor
from wide_research.health_snapshot import ProfileSnapshotSpec


NOW = datetime.now(timezone.utc).replace(microsecond=0)


class _ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def _logger(name: str) -> tuple[logging.Logger, _ListHandler]:
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    handler = _ListHandler()
    logger.addHandler(handler)
    return logger, handler


def _journal(path, *, at: datetime = NOW) -> None:
    path.write_text(
        json.dumps(
            {
                "record_type": "observation",
                "stage": "decision_pipeline",
                "created_at_utc": at.isoformat(),
            }
        )
        + "\n",
        encoding="utf-8",
    )


def _database(path, profile_id: str) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE store_profile(
            singleton INTEGER PRIMARY KEY,
            profile_id TEXT NOT NULL
        );
        CREATE TABLE phases(
            phase_id TEXT PRIMARY KEY,
            status TEXT NOT NULL
        );
        CREATE TABLE research_runs(
            run_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            started_at_utc TEXT NOT NULL,
            completed_at_utc TEXT
        );
        """
    )
    connection.execute("INSERT INTO store_profile VALUES(1, ?)", (profile_id,))
    connection.execute("INSERT INTO phases VALUES('phase-1', 'shadow')")
    connection.execute(
        "INSERT INTO research_runs VALUES('run-1', 'completed', ?, ?)",
        ((NOW - timedelta(hours=1)).isoformat(), NOW.isoformat()),
    )
    connection.commit()
    connection.close()


def _summary(path, profile_id: str) -> None:
    path.write_text(
        json.dumps(
            {
                "store_profile": profile_id,
                "cycle_type": "discovery",
                "run_id": "run-1",
                "completed_at_utc": NOW.isoformat(),
                "registry": {"not_admitted": []},
            }
        ),
        encoding="utf-8",
    )


def _monitor(
    tmp_path,
    *,
    profiles=lambda _state, _started: [],
    retry=lambda: {},
    interval_seconds: float = 300,
    retry_interval_seconds: float = 60,
    logger: logging.Logger | None = None,
) -> ResearchHealthMonitor:
    journal = tmp_path / "observations.jsonl"
    if not journal.exists():
        _journal(journal)
    monitor = ResearchHealthMonitor(
        profiles=profiles,
        retry=retry,
        observation_path=journal,
        report_path=tmp_path / "health.json",
        state_path=tmp_path / "alerts.json",
        disk_path=tmp_path,
        interval_seconds=interval_seconds,
        retry_interval_seconds=retry_interval_seconds,
        logger=logger,
    )
    monitor.note_feed(0, at=NOW)
    monitor.note_observation(NOW)
    monitor.note_pending(0, None, at=NOW)
    return monitor


def test_collect_is_read_only_and_rejects_output_source_overlap(tmp_path) -> None:
    database = tmp_path / "primary.sqlite3"
    summary = tmp_path / "primary.json"
    journal = tmp_path / "observations.jsonl"
    _database(database, "primary")
    _summary(summary, "primary")
    _journal(journal)
    spec = ProfileSnapshotSpec(
        profile_id="primary",
        database_path=database,
        summary_path=summary,
        capacity=8,
        interval_seconds=86_400,
    )
    retry_calls: list[None] = []
    before = (database.stat().st_mtime_ns, database.read_bytes())
    monitor = ResearchHealthMonitor(
        profiles=lambda _state, _started: [spec],
        retry=lambda: retry_calls.append(None) or {},
        observation_path=journal,
        report_path=tmp_path / "health.json",
        state_path=tmp_path / "alerts.json",
        disk_path=tmp_path,
    )
    monitor.note_feed(0, at=NOW)
    monitor.note_observation(NOW)
    monitor.note_pending(0, None, at=NOW)
    report = monitor.collect(now=NOW, retry_counters={"pending": 0})

    assert report["snapshot"]["sources"]["ok"] is True
    assert retry_calls == []  # collection and retry replay are separate operations
    assert before == (database.stat().st_mtime_ns, database.read_bytes())
    assert not (tmp_path / "primary.sqlite3-wal").exists()

    overlapping = ResearchHealthMonitor(
        profiles=lambda _state, _started: [spec],
        retry=lambda: {},
        observation_path=journal,
        report_path=database,
        state_path=tmp_path / "other-alerts.json",
        disk_path=tmp_path,
    )
    with pytest.raises(ValueError, match="overlap monitored sources"):
        overlapping.collect(now=NOW)
    assert before == (database.stat().st_mtime_ns, database.read_bytes())


def test_critical_alert_is_not_spammed_and_state_survives_restart(tmp_path) -> None:
    logger, handler = _logger("research-health-dedup-test")
    monitor = _monitor(tmp_path, logger=logger)
    monitor.note_feed(1, at=NOW - timedelta(minutes=31))

    first = monitor.collect(now=NOW)
    second = monitor.collect(now=NOW + timedelta(seconds=1))
    assert [event["check_id"] for event in first["alert_events"]] == [
        "runtime.live_feed"
    ]
    assert second["alert_events"] == []

    restored = _monitor(tmp_path, logger=logger)
    restored.note_feed(1, at=NOW - timedelta(minutes=31))
    third = restored.collect(now=NOW + timedelta(seconds=2))
    assert third["alert_events"] == []
    assert sum("[RESEARCH_HEALTH_ALERT]" in row for row in handler.messages) == 1


def test_retry_runs_periodically_and_start_stop_are_idempotent(tmp_path) -> None:
    calls = 0
    reached_three = threading.Event()

    def retry() -> dict[str, int]:
        nonlocal calls
        calls += 1
        if calls >= 3:
            reached_three.set()
        return {"pending": 0, "failed": 0}

    monitor = _monitor(
        tmp_path,
        retry=retry,
        interval_seconds=0.02,
        retry_interval_seconds=0.01,
    )
    assert monitor.start() is True
    first_thread = monitor.thread
    assert monitor.start() is False
    assert monitor.thread is first_thread
    assert reached_three.wait(1.0)
    assert monitor.stop(timeout=1.0) is True
    assert monitor.is_running is False
    assert monitor.stop(timeout=0.0) is True

    assert monitor.start() is True
    assert monitor.stop(timeout=1.0) is True
    assert calls >= 3


def test_retry_failure_does_not_suppress_scheduled_report(tmp_path) -> None:
    attempted = threading.Event()

    def broken_retry() -> dict[str, int]:
        attempted.set()
        raise RuntimeError("retry storage unavailable")

    logger, _ = _logger("research-health-retry-error-test")
    monitor = _monitor(
        tmp_path,
        retry=broken_retry,
        interval_seconds=0.02,
        retry_interval_seconds=0.02,
        logger=logger,
    )
    assert monitor.start()
    assert attempted.wait(1.0)
    report_path = tmp_path / "health.json"
    deadline = time.monotonic() + 1.0
    while not report_path.exists() and time.monotonic() < deadline:
        time.sleep(0.005)
    assert monitor.stop(timeout=1.0)
    assert report_path.exists()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["snapshot"]["retries"]["failed"] >= 1
    assert report["snapshot"]["retries"]["error_type"] == "RuntimeError"


def test_collects_are_serialized_and_discovery_overlap_is_counted(tmp_path) -> None:
    active = 0
    max_active = 0
    callback_lock = threading.Lock()

    def profiles(_state, _started):
        nonlocal active, max_active
        with callback_lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.03)
        with callback_lock:
            active -= 1
        return []

    monitor = _monitor(tmp_path, profiles=profiles)
    errors: list[BaseException] = []

    def collect() -> None:
        try:
            monitor.collect(now=NOW)
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    threads = [threading.Thread(target=collect) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(1.0)
    assert errors == []
    assert max_active == 1

    monitor.note_discovery("primary", at=NOW)
    monitor.note_discovery("primary", at=NOW + timedelta(seconds=1))
    monitor.note_discovery(
        "primary", {"status": "completed"}, at=NOW + timedelta(seconds=2)
    )
    assert monitor.state["discovery"]["primary"]["active_runs"] == 1
    assert monitor.state["discovery"]["primary"]["running_since"] == (
        NOW.isoformat().replace("+00:00", "Z")
    )
    monitor.note_discovery(
        "primary", {"status": "error"}, at=NOW + timedelta(seconds=3)
    )
    assert monitor.state["discovery"]["primary"]["active_runs"] == 0
    assert monitor.state["discovery"]["primary"]["running_since"] is None
    assert monitor.state["discovery"]["primary"]["failures"] == 1


def test_runtime_validates_callback_and_heartbeat_types(tmp_path) -> None:
    monitor = _monitor(tmp_path, profiles=lambda _state, _started: iter(()))
    with pytest.raises(TypeError, match="return a sequence"):
        monitor.collect(now=NOW)
    with pytest.raises(TypeError, match="non-negative integer"):
        monitor.note_feed(True)
    with pytest.raises(ValueError, match="timezone"):
        monitor.note_observation("2026-09-13T12:00:00")


def test_nanotest_profiles_and_outputs_are_resolved_from_project_root(
    monkeypatch, tmp_path
) -> None:
    fake_module = tmp_path / "NanoTest.py"
    monkeypatch.setattr(bot, "__file__", str(fake_module))
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH", True)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_FOUR_FACTOR", False)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_PRECISION", True)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_RARE_PRECISION", True)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_PRECISION_AUTO_DISCOVERY", False)
    for name, value in (
        ("WIDE_RESEARCH_DB_FILE", "data/primary.sqlite3"),
        ("WIDE_RESEARCH_DISCOVERY_FILE", "stats/primary.json"),
        ("WIDE_RESEARCH_PRECISION_DB_FILE", "data/precision.sqlite3"),
        ("WIDE_RESEARCH_PRECISION_DISCOVERY_FILE", "stats/precision.json"),
        ("WIDE_RESEARCH_RARE_PRECISION_DB_FILE", "data/rare.sqlite3"),
        ("WIDE_RESEARCH_RARE_PRECISION_DISCOVERY_FILE", "stats/rare.json"),
    ):
        monkeypatch.setattr(bot, name, value)
    discovery = {
        "primary": {"failures": 1},
        "precision": {"running_since": NOW.isoformat()},
        "rare_precision": {"failures": 2},
    }
    specs = bot._research_health_profiles(discovery, NOW.isoformat())
    assert [spec.profile_id for spec in specs] == [
        "primary",
        "precision_shadow",
        "rare_precision_shadow",
    ]
    assert specs[1].enabled is False
    assert specs[1].running_since == NOW.isoformat()
    assert specs[2].failures == 2
    assert all(os.path.isabs(os.fspath(spec.database_path)) for spec in specs)
    assert all(
        os.path.commonpath((str(tmp_path), os.fspath(spec.database_path)))
        == str(tmp_path)
        for spec in specs
    )

    captured: dict = {}

    class FakeMonitor:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

        def start(self) -> bool:
            return True

        def stop(self, timeout: float = 5.0) -> bool:
            captured["stop_timeout"] = timeout
            return True

    monkeypatch.setattr(bot, "ResearchHealthMonitor", FakeMonitor)
    monkeypatch.setattr(bot, "_research_health_monitor", None)
    monkeypatch.setattr(bot, "ENABLE_RESEARCH_HEALTH_MONITOR", True)
    monkeypatch.setattr(bot, "RESEARCH_HEALTH_REPORT_FILE", "stats/health.json")
    monkeypatch.setattr(bot, "RESEARCH_HEALTH_ALERT_STATE_FILE", "stats/alerts.json")
    monkeypatch.setattr(bot, "OBSERVATION_HISTORY_FILE", "stats/observations.jsonl")
    assert bot.start_research_health_monitor() is True
    assert captured["report_path"] == str(tmp_path / "stats" / "health.json")
    assert captured["state_path"] == str(tmp_path / "stats" / "alerts.json")
    assert captured["observation_path"] == str(
        tmp_path / "stats" / "observations.jsonl"
    )
    assert bot.stop_research_health_monitor(timeout=0.25) is True
    assert captured["stop_timeout"] == 0.25

    monkeypatch.setattr(bot, "_research_health_monitor", None)
    monkeypatch.setattr(bot, "RESEARCH_HEALTH_REPORT_FILE", "../outside.json")
    with pytest.raises(ValueError, match="leave project"):
        bot.start_research_health_monitor()


def test_nanotest_retry_aggregates_timezone_offsets_and_wakes_controller(
    monkeypatch,
) -> None:
    class Layer:
        def __init__(self, status, updated=0) -> None:
            self.status = status
            self.updated = updated
            self.replays = 0

        def replay_pending(self, *, limit: int):
            assert limit == 32
            self.replays += 1
            return {"status": "OK", "updated_triggers": self.updated}

        def pending_retry_status(self):
            return self.status

    first = Layer(
        {
            "status": "PENDING",
            "pending": 2,
            "oldest_at_utc": "2026-09-13T10:00:00+02:00",
            "inbox": {"max_attempts": 1},
        },
        updated=1,
    )
    second = Layer(
        {
            "status": "PENDING",
            "pending": 3,
            "oldest_at_utc": "2026-09-13T07:30:00Z",
            "inbox": {"max_attempts": 2},
        }
    )
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH", True)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_FOUR_FACTOR", False)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_PRECISION", True)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_RARE_PRECISION", False)
    monkeypatch.setattr(bot, "_get_wide_research_layer", lambda: first)
    monkeypatch.setattr(bot, "_get_wide_research_precision_layer", lambda: second)
    bot._wide_research_wakeup.clear()

    counters = bot._research_retry_tick()
    assert counters == {
        "pending": 5,
        "failed": 3,
        "oldest_pending_at": "2026-09-13T07:30:00Z",
    }
    assert first.replays == second.replays == 1
    assert bot._wide_research_wakeup.is_set()


def test_nanotest_eligible_fixture_count_isolated_per_provider_row(
    monkeypatch,
) -> None:
    rows = [
        {"id": 1, "minute": 50, "active": True},
        {"minute": 50, "active": True},  # no durable fixture identity
        {"id": 2, "minute": 50, "active": True},
        {"id": 3, "minute": "broken", "active": True},
        {"id": 4, "minute": 61, "active": True},
        {"id": 5, "minute": 50, "active": False},
    ]
    seen_no_stats: list[int] = []

    def minute(row):
        if row.get("minute") == "broken":
            raise ValueError("malformed provider minute")
        return int(row["minute"])

    monkeypatch.setattr(bot, "get_fixture_id", lambda row: row.get("id"))
    monkeypatch.setattr(bot, "_fixture_minute_from_raw", minute)
    monkeypatch.setattr(
        bot, "_is_live_fixture_active", lambda row: bool(row.get("active"))
    )
    monkeypatch.setattr(
        bot, "is_no_stats_blocked", lambda key: key == "2"
    )
    monkeypatch.setattr(
        bot,
        "is_no_stats_fixture",
        lambda fixture_id: seen_no_stats.append(fixture_id) or False,
    )

    assert bot._research_health_eligible_fixture_count(rows) == 1
    assert seen_no_stats == [1]
    assert bot._research_health_eligible_fixture_count(None) == 0
    assert bot._research_health_eligible_fixture_count({"id": 1}) == 0


def test_nanotest_eligible_fixture_count_reflects_new_no_stats_blocks(
    monkeypatch,
) -> None:
    rows = [
        {"id": 1, "minute": 50, "active": True},
        {"id": 2, "minute": 50, "active": True},
    ]
    blocked: set[str] = set()
    monkeypatch.setattr(bot, "get_fixture_id", lambda row: row["id"])
    monkeypatch.setattr(bot, "_fixture_minute_from_raw", lambda row: row["minute"])
    monkeypatch.setattr(
        bot, "_is_live_fixture_active", lambda row: bool(row["active"])
    )
    monkeypatch.setattr(
        bot, "is_no_stats_blocked", lambda fixture_id: fixture_id in blocked
    )
    monkeypatch.setattr(bot, "is_no_stats_fixture", lambda _fixture_id: False)

    assert bot._research_health_eligible_fixture_count(rows) == 2
    blocked.update({"1", "2"})
    assert bot._research_health_eligible_fixture_count(rows) == 0


def test_nanotest_pending_heartbeat_normalizes_offsets_and_ignores_bad_clock(
    monkeypatch, tmp_path,
) -> None:
    records = [
        {
            "record_type": "observation",
            "observation_id": "a",
            "fixture_id": 1,
            "schema_version": 1,
            "stage": "decision_pipeline",
            "created_at_utc": "2026-09-13T10:00:00+02:00",
        },
        {
            "record_type": "observation",
            "observation_id": "b",
            "fixture_id": 2,
            "schema_version": 1,
            "stage": "decision_pipeline",
            "created_at_utc": "2026-09-13T07:30:00Z",
        },
        {
            "record_type": "observation",
            "observation_id": "c",
            "fixture_id": 3,
            "schema_version": 1,
            "stage": "decision_pipeline",
            "created_at_utc": "not-a-clock",
        },
    ]
    captured: dict[str, object] = {}

    class Monitor:
        def note_pending(self, count, oldest):
            captured.update(count=count, oldest=oldest)

    monkeypatch.setattr(
        bot, "iter_observation_history_records", lambda *args, **kwargs: iter(records)
    )
    monkeypatch.setattr(
        bot, "OBSERVATION_HISTORY_FILE", str(tmp_path / "observations.jsonl")
    )
    bot._invalidate_observation_reconcile_index()
    monkeypatch.setattr(bot, "_research_health_monitor", Monitor())

    pending_times: dict[int, str] = {}
    pending, _collectable, _terminal = bot._observation_fixture_reconcile_sets(
        pending_oldest_by_fixture=pending_times
    )
    assert pending == {1, 2, 3}
    assert captured == {"count": 3, "oldest": "2026-09-13T07:30:00Z"}
    assert pending_times == {
        1: "2026-09-13T08:00:00Z",
        2: "2026-09-13T07:30:00Z",
    }


def test_nanotest_health_heartbeat_failure_is_non_intrusive(
    monkeypatch,
) -> None:
    class BrokenMonitor:
        def note_feed(self, _count):
            raise RuntimeError("health output unavailable")

    monkeypatch.setattr(bot, "_research_health_monitor", BrokenMonitor())
    bot._research_health_note("note_feed", 1)
