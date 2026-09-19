from __future__ import annotations

import json
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

import NanoTest as bot
from shadow_ml import worker_runtime


def test_cache_set_prunes_expired_entries_and_enforces_cap(monkeypatch) -> None:
    monkeypatch.setattr(bot, "_cache", {})
    monkeypatch.setattr(bot, "CACHE_MAX_ENTRIES", 2)
    clock = iter((100.0, 101.0, 102.0, 103.0))
    monkeypatch.setattr(bot.time, "time", lambda: next(clock))

    bot._cache["expired"] = (1.0, 99.0, {"old": True})
    bot.cache_set("one", 1, ttl=60)
    bot.cache_set("two", 2, ttl=60)
    bot.cache_set("three", 3, ttl=60)

    assert "expired" not in bot._cache
    assert set(bot._cache) == {"two", "three"}
    assert bot.cache_get("three", 60) == 3


def test_telegram_queue_prune_removes_stale_and_caps_fresh_entries() -> None:
    queue = bot.TgEditQueue()
    queue.last_edit_ts = {
        (1, 1): 10.0,
        (1, 2): 95.0,
        (1, 3): 96.0,
        (1, 4): 97.0,
    }
    queue.last_sent_hash = {key: str(key) for key in queue.last_edit_ts}
    queue.chat_block_until = {1: 99.0, 2: 120.0}
    queue._last_429_log_at = {1: 10.0, 2: 95.0}

    queue.prune(now_ts=100.0, max_age_seconds=20, max_entries=2)

    assert set(queue.last_edit_ts) == {(1, 3), (1, 4)}
    assert set(queue.last_sent_hash) == {(1, 3), (1, 4)}
    assert queue.chat_block_until == {2: 120.0}
    assert queue._last_429_log_at == {2: 95.0}


def test_telegram_hash_only_entry_uses_activity_timestamp(monkeypatch) -> None:
    queue = bot.TgEditQueue()
    monkeypatch.setattr(bot.time, "time", lambda: 100.0)
    queue.remember_sent_hash(1, 10, "fresh")

    assert queue.prune(
        now_ts=110.0,
        max_age_seconds=20,
        max_entries=10,
    ) == 0
    assert queue.last_sent_hash[(1, 10)] == "fresh"


def test_process_memory_parser_is_fail_open(tmp_path) -> None:
    status = tmp_path / "status"
    status.write_text(
        "Name:\tpython\nVmHWM:\t23456 kB\nVmRSS:\t12345 kB\n",
        encoding="utf-8",
    )

    assert bot.read_process_memory_kb(str(status)) == {
        "rss_kb": 12345,
        "peak_rss_kb": 23456,
    }
    assert bot.read_process_memory_kb(str(tmp_path / "missing")) == {
        "rss_kb": None,
        "peak_rss_kb": None,
    }


def test_memory_telemetry_interval_does_not_scan_journals(monkeypatch) -> None:
    snapshots = {
        "rss_kb": 1024,
        "peak_rss_kb": 2048,
        "api_cache_entries": 0,
        "tg_message_entries": 0,
        "decision_pending_records": 0,
        "decision_dedupe_keys": 0,
        "decision_outcome_markers": 0,
        "observation_dedupe_keys": 0,
        "rolling_fixtures": 0,
        "rolling_history_records": 0,
        "rolling_computed_payloads": 0,
        "state_top_level_items": 0,
    }
    monkeypatch.setattr(bot, "_memory_telemetry_last_monotonic", 0.0)
    monkeypatch.setattr(bot, "MEMORY_TELEMETRY_INTERVAL_SECONDS", 60)
    monkeypatch.setattr(bot.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(bot, "cache_prune", lambda: 0)
    monkeypatch.setattr(bot.tg_edit_queue, "prune", lambda: 0)
    monkeypatch.setattr(bot, "collect_memory_telemetry", lambda: dict(snapshots))
    monkeypatch.setattr(
        bot,
        "load_joined_observation_history",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("telemetry must not load observation history")
        ),
    )

    assert bot.log_memory_telemetry() is not None
    assert bot.log_memory_telemetry() is None


def test_isolated_ml_parent_never_materializes_history(
    monkeypatch, tmp_path
) -> None:
    worker_payload = {
        "status": "trained",
        "trigger": "test",
        "records": 50,
        "fixtures": 10,
        "new_fixtures": 10,
        "model_id": "isolated-model",
    }
    artifact = {
        "artifact_type": "shadow_ml_model",
        "model_id": "isolated-model",
        "production_applied": False,
    }
    process_calls = []

    @contextmanager
    def snapshot():
        yield "/tmp/point-in-time-observation-history.jsonl"

    class Process:
        returncode = 0

        def __init__(self, command, **kwargs):
            process_calls.append((command, kwargs))

        def communicate(self, timeout=None):
            return json.dumps(worker_payload), ""

        def poll(self):
            return 0

    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(
        bot, "SHADOW_ML_MODEL_FILE", str(tmp_path / "shadow-model.json")
    )
    bot._shadow_ml_stop.clear()
    monkeypatch.setattr(bot, "_shadow_ml_history_snapshot", snapshot)
    monkeypatch.setattr(bot.subprocess, "Popen", Process)
    monkeypatch.setattr(bot, "load_shadow_ml_model_file", lambda path: artifact)
    monkeypatch.setattr(bot, "_shadow_ml_artifact_is_safe", lambda value: True)
    monkeypatch.setattr(
        bot,
        "load_joined_observation_history",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("parent must never materialize ML history")
        ),
    )
    monkeypatch.setattr(
        bot,
        "train_shadow_model",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("parent must never fit ML")
        ),
    )

    result = bot._train_shadow_ml_once_isolated(force=True, trigger="test")

    assert result["status"] == "trained"
    assert result["isolated_process"] is True
    command, kwargs = process_calls[0]
    assert command[0] == bot.sys.executable
    assert "--managed-worker" in command
    assert kwargs["env"]["GOALBOT_LIBRARY_MODE"] == "1"
    assert kwargs["env"]["GOALBOT_SHADOW_ML_WORKER_MEMORY_LIMIT_MB"] == str(
        bot.SHADOW_ML_WORKER_MEMORY_LIMIT_MB
    )


def test_worker_memory_guard_sets_hard_address_space_limit(monkeypatch) -> None:
    import resource

    applied = []
    monkeypatch.setattr(
        resource,
        "getrlimit",
        lambda limit: (resource.RLIM_INFINITY, resource.RLIM_INFINITY),
    )
    monkeypatch.setattr(
        resource,
        "setrlimit",
        lambda limit, values: applied.append((limit, values)),
    )

    result = worker_runtime.apply_worker_memory_limit(
        {worker_runtime.WORKER_MEMORY_LIMIT_ENV: "1536"}
    )

    expected = 1536 * 1024 * 1024
    assert result == 1536
    assert applied == [(resource.RLIMIT_AS, (expected, expected))]


def test_local_isolated_worker_uses_spooled_history_loader(
    monkeypatch, tmp_path
) -> None:
    artifact = {
        "artifact_type": "shadow_ml_model",
        "model_id": "shadow:spooled",
        "status": "collecting",
        "production_applied": False,
        "training_summary": {"unique_fixtures": 1},
    }
    records = [{"fixture_id": 1}]
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_training_history",
        lambda path: (
            records,
            {
                "records_scanned": 10,
                "observation_records": 6,
                "outcome_records": 4,
                "candidate_payloads_seen": 1,
                "joined_training_rows": 1,
            },
        ),
    )
    monkeypatch.setattr(
        bot,
        "load_joined_observation_history",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("isolated worker must use the spooled loader")
        ),
    )
    monkeypatch.setattr(
        bot,
        "summarize_shadow_ml_training_data",
        lambda value: {
            "eligible_records": 1,
            "unique_fixtures": 1,
            "span_days": 0.0,
            "training_data_hash": "hash",
        },
    )
    monkeypatch.setattr(bot, "load_shadow_ml_model_cached", lambda force=False: {})
    monkeypatch.setattr(
        bot,
        "train_shadow_model",
        lambda value, config, now: artifact,
    )
    monkeypatch.setattr(bot, "_shadow_ml_artifact_is_safe", lambda value: True)
    monkeypatch.setattr(bot, "save_shadow_ml_model_file", lambda path, value: None)
    monkeypatch.setattr(bot, "_set_shadow_ml_model_cache", lambda *args, **kwargs: None)

    result = bot._train_shadow_ml_once_local(
        force=True,
        trigger="test",
        history_path=str(tmp_path / "history.jsonl"),
        output_path=str(tmp_path / "candidate.json"),
    )

    assert result["status"] == "trained"
    assert result["history_spool"]["records_scanned"] == 10


def test_local_worker_reports_memory_limit_without_replacing_model(
    monkeypatch, tmp_path
) -> None:
    canonical = tmp_path / "canonical.json"
    canonical.write_text("old-model", encoding="utf-8")
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(
        bot,
        "load_shadow_ml_training_history",
        lambda path: (_ for _ in ()).throw(MemoryError()),
    )

    result = bot._train_shadow_ml_once_local(
        force=True,
        trigger="memory-test",
        history_path=str(tmp_path / "history.jsonl"),
        output_path=str(canonical),
    )

    assert result["status"] == "error"
    assert result["reason"] == "memory_limit_exceeded"
    assert canonical.read_text(encoding="utf-8") == "old-model"


def test_isolated_reputation_parent_never_materializes_history(
    monkeypatch, tmp_path
) -> None:
    model = {
        "schema_version": 1,
        "generated_at_utc": "2026-08-06T00:00:00Z",
        "shadow_only": True,
        "cohorts": {},
    }
    process_calls = []

    @contextmanager
    def snapshot():
        yield "/tmp/point-in-time-decision-history.jsonl"

    class Process:
        returncode = 0

        def __init__(self, command, **kwargs):
            process_calls.append((command, kwargs))

        def communicate(self, timeout=None):
            return json.dumps({
                "status": "rebuilt",
                "rows": 25,
                "generated_at_utc": model["generated_at_utc"],
            }), ""

        def poll(self):
            return 0

    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_SHADOW", True)
    monkeypatch.setattr(
        bot,
        "SIGNAL_REPUTATION_MODEL_FILE",
        str(tmp_path / "reputation.json"),
    )
    monkeypatch.setattr(bot, "_signal_reputation_model_cache", {})
    monkeypatch.setattr(bot, "_signal_reputation_last_refresh_ts", 0.0)
    monkeypatch.setattr(bot, "_signal_reputation_dirty", True)
    bot._signal_reputation_stop.clear()
    monkeypatch.setattr(bot, "_signal_reputation_history_snapshot", snapshot)
    monkeypatch.setattr(bot.subprocess, "Popen", Process)
    monkeypatch.setattr(bot, "load_reputation_model", lambda path: model)
    monkeypatch.setattr(
        bot,
        "load_joined_decision_snapshots",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("parent must never materialize decision history")
        ),
    )
    monkeypatch.setattr(
        bot,
        "build_reputation_model",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("parent must never build reputation")
        ),
    )

    result = bot._refresh_signal_reputation_model_isolated(force=True)

    assert result is model
    command, kwargs = process_calls[0]
    assert command[0] == bot.sys.executable
    assert command[-4] == "--input"
    assert kwargs["env"]["GOALBOT_LIBRARY_MODE"] == "1"


def test_public_reputation_refresh_serves_stale_without_blocking(monkeypatch) -> None:
    stale = {"generated_at_utc": "old"}
    scheduled = []
    monkeypatch.setattr(bot, "SIGNAL_REPUTATION_ISOLATED_REFRESH_ENABLED", True)
    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_SHADOW", True)
    monkeypatch.setattr(bot, "_signal_reputation_model_cache", stale)
    monkeypatch.setattr(bot, "_signal_reputation_last_refresh_ts", 0.0)
    monkeypatch.setattr(bot, "_signal_reputation_dirty", True)
    monkeypatch.setattr(
        bot,
        "_schedule_signal_reputation_refresh",
        lambda force=False: scheduled.append(force) or True,
    )

    assert bot.refresh_signal_reputation_model() is stale
    assert scheduled == [False]


def test_memory_telemetry_failure_is_fail_open(monkeypatch) -> None:
    monkeypatch.setattr(bot, "_memory_telemetry_last_monotonic", 0.0)
    monkeypatch.setattr(bot.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(
        bot,
        "cache_prune",
        lambda: (_ for _ in ()).throw(RuntimeError("test failure")),
    )

    assert bot.log_memory_telemetry(force=True) is None


def test_library_mode_import_does_not_initialize_persistence(tmp_path) -> None:
    persist_dir = tmp_path / "must-not-be-created"
    environment = os.environ.copy()
    environment["GOALBOT_LIBRARY_MODE"] = "1"
    environment["PERSIST_DIR"] = str(persist_dir)

    completed = subprocess.run(
        [sys.executable, "-c", "import NanoTest"],
        cwd=Path(bot.__file__).resolve().parent,
        env=environment,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert not persist_dir.exists()


def test_plain_module_import_never_initializes_persistence(tmp_path) -> None:
    persist_dir = tmp_path / "must-not-be-created"
    state_file = tmp_path / "must-not-be-written.json"
    environment = os.environ.copy()
    environment["GOALBOT_LIBRARY_MODE"] = "0"
    environment["PERSIST_DIR"] = str(persist_dir)
    environment["STATE_FILE"] = str(state_file)

    completed = subprocess.run(
        [sys.executable, "-c", "import NanoTest"],
        cwd=Path(bot.__file__).resolve().parent,
        env=environment,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert not persist_dir.exists()
    assert not state_file.exists()


def test_state_save_uses_writer_specific_atomic_temp_path(
    monkeypatch, tmp_path
) -> None:
    state_file = tmp_path / "state.json"
    legacy_shared_temp = Path(f"{state_file}.tmp")
    legacy_shared_temp.write_text("other-writer-sentinel", encoding="utf-8")

    monkeypatch.setattr(bot, "STATE_FILE", str(state_file))

    bot.save_state_to_disk()

    assert json.loads(state_file.read_text(encoding="utf-8")) == bot.state
    assert legacy_shared_temp.read_text(encoding="utf-8") == (
        "other-writer-sentinel"
    )
    assert list(tmp_path.glob(".state.json.*.tmp")) == []


def test_invalid_ml_candidate_preserves_canonical_model(
    monkeypatch, tmp_path
) -> None:
    canonical = tmp_path / "shadow-model.json"
    canonical.write_text("old-model", encoding="utf-8")

    @contextmanager
    def snapshot():
        yield str(tmp_path / "history.jsonl")

    class Process:
        returncode = 0

        def __init__(self, *args, **kwargs):
            pass

        def communicate(self, timeout=None):
            return json.dumps({
                "status": "trained",
                "model_id": "invalid-candidate",
            }), ""

        def poll(self):
            return 0

    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(bot, "SHADOW_ML_MODEL_FILE", str(canonical))
    monkeypatch.setattr(bot, "_shadow_ml_history_snapshot", snapshot)
    monkeypatch.setattr(bot.subprocess, "Popen", Process)
    monkeypatch.setattr(bot, "load_shadow_ml_model_file", lambda path: {})
    bot._shadow_ml_stop.clear()

    result = bot._train_shadow_ml_once_isolated(force=True, trigger="test")

    assert result["status"] == "error"
    assert result["reason"] == "worker_artifact_mismatch"
    assert canonical.read_text(encoding="utf-8") == "old-model"


def test_reputation_invalidation_during_build_remains_dirty(
    monkeypatch, tmp_path
) -> None:
    generated = "2026-08-06T00:00:00Z"
    model = {
        "schema_version": 1,
        "generated_at_utc": generated,
        "shadow_only": True,
        "cohorts": {},
    }

    @contextmanager
    def snapshot():
        yield str(tmp_path / "decisions.jsonl")

    class Process:
        returncode = 0

        def __init__(self, *args, **kwargs):
            pass

        def communicate(self, timeout=None):
            bot.invalidate_signal_reputation_model()
            return json.dumps({
                "status": "rebuilt",
                "rows": 1,
                "generated_at_utc": generated,
            }), ""

        def poll(self):
            return 0

    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_SHADOW", True)
    monkeypatch.setattr(
        bot,
        "SIGNAL_REPUTATION_MODEL_FILE",
        str(tmp_path / "reputation.json"),
    )
    monkeypatch.setattr(bot, "_signal_reputation_model_cache", {})
    monkeypatch.setattr(bot, "_signal_reputation_last_refresh_ts", 0.0)
    monkeypatch.setattr(bot, "_signal_reputation_dirty", True)
    monkeypatch.setattr(bot, "_signal_reputation_revision", 10)
    monkeypatch.setattr(bot, "_signal_reputation_history_snapshot", snapshot)
    monkeypatch.setattr(bot.subprocess, "Popen", Process)
    monkeypatch.setattr(bot, "load_reputation_model", lambda path: model)
    bot._signal_reputation_stop.clear()

    result = bot._refresh_signal_reputation_model_isolated(force=True)

    assert result is model
    assert bot._signal_reputation_revision == 11
    assert bot._signal_reputation_dirty is True


def test_rolling_worker_command_uses_distinct_variant_and_paths(tmp_path) -> None:
    history = tmp_path / "history.jsonl"
    candidate = tmp_path / "rolling-candidate.json"
    current = tmp_path / "rolling-current.json"

    command = bot._shadow_ml_worker_command(
        history_path=str(history),
        output_path=str(candidate),
        current_model_path=str(current),
        force=True,
        trigger="test",
        data_revision_hint=True,
        variant="rolling",
    )

    assert command[command.index("--variant") + 1] == "rolling"
    assert command[command.index("--output") + 1] == str(candidate.resolve())
    assert command[command.index("--current-model") + 1] == str(
        current.resolve()
    )
    assert "--force" in command
    assert "--data-revision-hint" in command


def test_managed_ml_worker_writes_only_candidate(tmp_path) -> None:
    root = Path(bot.__file__).resolve().parent
    history = tmp_path / "observation_history.jsonl"
    canonical = tmp_path / "canonical.json"
    candidate = tmp_path / "candidate.json"
    history.write_text("", encoding="utf-8")
    environment = os.environ.copy()
    environment["GOALBOT_LIBRARY_MODE"] = "0"

    completed = subprocess.run(
        [
            sys.executable,
            str(root / "scripts" / "train_shadow_ml.py"),
            "--input",
            str(history),
            "--output",
            str(candidate),
            "--current-model",
            str(canonical),
            "--managed-worker",
            "--force",
            "--trigger",
            "test",
        ],
        cwd=root,
        env=environment,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout)["status"] == "trained"
    assert candidate.exists()
    assert not canonical.exists()


def test_managed_rolling_worker_writes_only_rolling_candidate(tmp_path) -> None:
    root = Path(bot.__file__).resolve().parent
    history = tmp_path / "observation_history.jsonl"
    static_canonical = tmp_path / "static-canonical.json"
    rolling_canonical = tmp_path / "rolling-canonical.json"
    rolling_candidate = tmp_path / "rolling-candidate.json"
    history.write_text("", encoding="utf-8")
    static_canonical.write_text('{"sentinel":"static"}\n', encoding="utf-8")
    environment = os.environ.copy()
    environment["GOALBOT_LIBRARY_MODE"] = "0"

    completed = subprocess.run(
        [
            sys.executable,
            str(root / "scripts" / "train_shadow_ml.py"),
            "--input",
            str(history),
            "--output",
            str(rolling_candidate),
            "--current-model",
            str(rolling_canonical),
            "--managed-worker",
            "--variant",
            "rolling",
            "--force",
            "--trigger",
            "test",
        ],
        cwd=root,
        env=environment,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["status"] == "trained"
    artifact = json.loads(rolling_candidate.read_text(encoding="utf-8"))
    assert artifact["artifact_type"] == "shadow_ml_rolling_model"
    assert artifact["artifact_role"] == "rolling_challenger"
    assert not rolling_canonical.exists()
    assert json.loads(static_canonical.read_text(encoding="utf-8")) == {
        "sentinel": "static"
    }


def test_busy_heavy_slot_schedules_shadow_ml_retry(monkeypatch) -> None:
    class BusySlot:
        def acquire(self, **kwargs):
            return False

    retries = []
    monkeypatch.setattr(bot, "ENABLE_SHADOW_ML", True)
    monkeypatch.setattr(bot, "_model_worker_slot", BusySlot())
    monkeypatch.setattr(
        bot,
        "_schedule_shadow_ml_retry",
        lambda delay_seconds=30.0: retries.append(delay_seconds),
    )
    bot._shadow_ml_stop.clear()

    result = bot._train_shadow_ml_once_isolated(trigger="test")

    assert result["status"] == "error"
    assert result["reason"] == "heavy_worker_busy"
    assert retries == [30.0]
