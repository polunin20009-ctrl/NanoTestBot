from __future__ import annotations

import gzip
import json
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import NanoTest as bot


def _configure(monkeypatch, tmp_path: Path, rotate_bytes: int = 0) -> Path:
    path = tmp_path / "observation_history.jsonl"
    monkeypatch.setattr(bot, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(bot, "OBSERVATION_HISTORY_FILE", str(path))
    monkeypatch.setattr(bot, "OBSERVATION_ROTATE_MAX_BYTES", rotate_bytes)
    monkeypatch.setattr(bot, "ENABLE_ROLLING_DYNAMICS_SHADOW", True)
    bot._observation_history_keys = None
    bot._reset_rolling_dynamics_tracker_for_tests()
    with bot.state_lock:
        bot.state["score_timelines"] = {}
    return path


def _observation(fixture_id: int = 101, minute: int = 46) -> dict:
    observation_id = f"{fixture_id}:{minute}:PREFILTER:v1"
    return {
        "record_type": "observation",
        "observation_id": observation_id,
        "observation_key": observation_id,
        "fixture_id": fixture_id,
        "created_at_utc": "2026-07-25T00:00:00+00:00",
        "schema_version": 1,
        "stage": "prefilter",
        "minute": minute,
        "match": {"score_home": 0, "score_away": 0},
        "decision": {"final_decision": "BLOCK", "block_reason": "test"},
        "outcome": {"status": "pending"},
    }


def _goal(minute: int) -> dict:
    return {
        "time": {"elapsed": minute, "extra": 0},
        "type": "Goal",
        "detail": "Normal Goal",
    }


def _rolling_observation(
    fixture_id: int,
    minute: int,
    shots_on_target_total: int,
) -> dict:
    observation_id = f"{fixture_id}:{minute}:TEST:v1"
    return {
        "record_type": "observation",
        "observation_id": observation_id,
        "observation_key": observation_id,
        "fixture_id": fixture_id,
        "created_at_utc": f"2026-07-25T00:{minute:02d}:00+00:00",
        "schema_version": 1,
        "stage": "decision_pipeline",
        "minute": minute,
        "match": {"score_home": 0, "score_away": 0},
        "raw_metrics": {
            "shots_on_target_home": shots_on_target_total,
            "shots_on_target_away": 0,
        },
        "availability": {
            "shots_on_target_home": True,
            "shots_on_target_away": True,
        },
        "data_quality": {"xg_source": "missing"},
        "features": {"pressure_index": float(shots_on_target_total)},
        "outcome": {"status": "pending"},
    }


def test_observation_store_rotates_gzip_and_deduplicates(monkeypatch, tmp_path: Path) -> None:
    path = _configure(monkeypatch, tmp_path, rotate_bytes=250)
    first = _observation(101, 46)
    second = _observation(101, 47)

    assert bot.append_observation_history(first) is True
    assert bot.append_observation_history(second) is True
    bot._observation_history_keys = None
    assert bot.append_observation_history(first) is False

    archives = list(tmp_path.glob("observation_history.*.jsonl.gz"))
    assert archives
    with gzip.open(archives[0], "rt", encoding="utf-8") as handle:
        assert json.loads(handle.readline())["observation_id"] == first["observation_id"]
    assert path.exists()
    assert len(list(bot.iter_observation_history_records(str(path)))) == 2


def test_observation_reader_ignores_generated_dot_export(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure(monkeypatch, tmp_path)
    generated = tmp_path / "observation_history.joined.jsonl"
    generated.write_text(
        json.dumps(_observation(999, 46)) + "\n",
        encoding="utf-8",
    )
    path.write_text(
        json.dumps(_observation(101, 46)) + "\n",
        encoding="utf-8",
    )

    records = list(bot.iter_observation_history_records(str(path)))

    assert [record["fixture_id"] for record in records] == [101]


def test_reconcile_signature_ignores_hardlink_metadata_churn(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure(monkeypatch, tmp_path)
    archive = tmp_path / "observation_history.20260725T010203000000Z.jsonl.gz"
    with gzip.open(archive, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(_observation(101, 46)) + "\n")
    path.write_text(json.dumps(_observation(202, 47)) + "\n", encoding="utf-8")
    signature = bot._observation_history_source_signature(str(path))

    hardlink = tmp_path / "snapshot-hardlink.jsonl.gz"
    os.link(archive, hardlink)
    assert bot._observation_history_source_signature(str(path)) == signature
    hardlink.unlink()
    assert bot._observation_history_source_signature(str(path)) == signature


def test_shadow_ml_snapshot_copies_observation_archives(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure(monkeypatch, tmp_path)
    archive = tmp_path / "observation_history.20260725T010203000000Z.jsonl.gz"
    with gzip.open(archive, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(_observation(101, 46)) + "\n")
    path.write_text(json.dumps(_observation(202, 47)) + "\n", encoding="utf-8")
    source_before = archive.stat()

    with bot._shadow_ml_history_snapshot() as snapshot_path:
        snapshot_archive = Path(snapshot_path).with_name(archive.name)
        assert snapshot_archive.exists()
        assert not os.path.samefile(archive, snapshot_archive)
        source_during = archive.stat()
        assert source_during.st_nlink == source_before.st_nlink
        assert source_during.st_ctime_ns == source_before.st_ctime_ns

    source_after = archive.stat()
    assert source_after.st_nlink == source_before.st_nlink
    assert source_after.st_ctime_ns == source_before.st_ctime_ns


def test_observation_joiner_uses_latest_schema_and_outcome_rank(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure(monkeypatch, tmp_path)
    old_observation = _observation(101, 46)
    old_observation["schema_version"] = 1
    old_observation["created_at_utc"] = "2026-07-25T03:00:00+00:00"
    new_observation = {
        **old_observation,
        "schema_version": 2,
        "created_at_utc": "2026-07-25T01:00:00+00:00",
        "stage": "decision_pipeline",
    }
    current_outcome = {
        "record_type": "observation_outcome",
        "observation_id": old_observation["observation_id"],
        "observation_key": old_observation["observation_id"],
        "fixture_id": 101,
        "outcome_schema_version": 2,
        "created_at_utc": "2026-07-25T02:00:00+00:00",
        "outcome": {"status": "resolved", "goal_to90_normal_time": False},
    }
    later_old_outcome = {
        **current_outcome,
        "outcome_schema_version": 1,
        "created_at_utc": "2026-07-25T04:00:00+00:00",
        "outcome": {"status": "resolved", "goal_to90_normal_time": True},
    }
    path.write_text(
        "".join(
            json.dumps(record) + "\n"
            for record in (
                new_observation,
                old_observation,
                current_outcome,
                later_old_outcome,
            )
        ),
        encoding="utf-8",
    )

    joined = bot.load_joined_observation_history(str(path))

    assert len(joined) == 1
    assert joined[0]["schema_version"] == 2
    assert joined[0]["stage"] == "decision_pipeline"
    assert joined[0]["outcome_schema_version"] == 2
    assert joined[0]["outcome"]["goal_to90_normal_time"] is False
    assert (
        joined[0]["outcome_record_created_at_utc"]
        == "2026-07-25T02:00:00+00:00"
    )


def test_shadow_ml_disk_join_matches_legacy_candidate_semantics(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure(monkeypatch, tmp_path)

    accepted = _rolling_observation(101, 46, 2)
    accepted["outcome"] = {"status": "pending"}
    accepted_outcome_old = {
        "record_type": "observation_outcome",
        "observation_key": accepted["observation_key"],
        "outcome_schema_version": 1,
        "created_at_utc": "2026-07-25T01:00:00+00:00",
        "outcome": {"status": "resolved", "goal_to90_normal_time": True},
    }
    accepted_outcome_new = {
        **accepted_outcome_old,
        "outcome_schema_version": 2,
        "created_at_utc": "2026-07-25T00:30:00+00:00",
        "outcome": {"status": "resolved", "goal_to90_normal_time": False},
    }

    superseded = _rolling_observation(202, 47, 3)
    superseded_new = {
        **superseded,
        "schema_version": 2,
        "created_at_utc": "2026-07-25T00:10:00+00:00",
        "stage": "prefilter",
    }
    superseded_outcome = {
        "record_type": "observation_outcome",
        "observation_key": superseded["observation_key"],
        "outcome_schema_version": 1,
        "created_at_utc": "2026-07-25T02:00:00+00:00",
        "outcome": {"status": "resolved", "goal_to90_normal_time": True},
    }

    pending = _rolling_observation(303, 48, 4)
    archive = tmp_path / "observation_history.20260725T120000000000Z.jsonl.gz"
    with gzip.open(archive, "wt", encoding="utf-8") as handle:
        for record in (
            accepted,
            accepted_outcome_old,
            superseded,
            superseded_outcome,
        ):
            handle.write(json.dumps(record) + "\n")
    path.write_text(
        "".join(
            json.dumps(record) + "\n"
            for record in (
                accepted_outcome_new,
                superseded_new,
                pending,
            )
        ),
        encoding="utf-8",
    )

    legacy = bot.load_joined_observation_history(
        str(path), include_pending=False
    )
    expected = [
        row
        for row in legacy
        if row.get("stage") == "decision_pipeline"
        and bot._safe_int(row.get("fixture_id"), -1) > 0
        and 46 <= bot._safe_int(row.get("minute"), -1) <= 60
    ]
    actual, stats = bot.load_shadow_ml_training_history(str(path))

    assert actual == expected
    assert [row["fixture_id"] for row in actual] == [101]
    assert actual[0]["outcome_schema_version"] == 2
    assert actual[0]["outcome"]["goal_to90_normal_time"] is False
    assert stats == {
        "records_scanned": 7,
        "observation_records": 4,
        "outcome_records": 3,
        "candidate_payloads_seen": 3,
        "joined_training_rows": 1,
    }


def test_shadow_ml_disk_join_removes_temporary_spool(tmp_path: Path) -> None:
    record = _rolling_observation(404, 49, 5)
    record["outcome"] = {"status": "resolved"}

    joined, stats = bot.materialize_training_history(
        [record],
        include_pending=False,
        spool_parent=tmp_path,
        commit_every=1,
    )

    assert [row["fixture_id"] for row in joined] == [404]
    assert stats["joined_training_rows"] == 1
    assert list(tmp_path.iterdir()) == []


def test_shadow_ml_disk_join_equal_rank_and_pending_policy() -> None:
    first = _rolling_observation(505, 50, 1)
    first["outcome"] = {"status": "resolved"}
    second = {
        **first,
        "raw_metrics": {"shots_on_target_home": float("nan")},
    }
    pending = _rolling_observation(606, 51, 2)
    pending["outcome"] = {"status": "pending"}
    void = _rolling_observation(707, 52, 3)
    void["outcome"] = {"status": "void"}

    without_pending, _ = bot.materialize_training_history(
        [first, second, pending, void],
        include_pending=False,
    )
    with_pending, _ = bot.materialize_training_history(
        [first, second, pending, void],
        include_pending=True,
    )

    assert [row["fixture_id"] for row in without_pending] == [505, 707]
    assert [row["fixture_id"] for row in with_pending] == [505, 606, 707]
    assert math.isnan(without_pending[0]["raw_metrics"]["shots_on_target_home"])


def test_raw_metrics_distinguish_real_zero_from_missing(monkeypatch) -> None:
    stats = {
        "shots_on_target": {"home": 0, "away": 3},
        "red_cards": {"home": 0, "away": 1},
    }

    def _metric(fixture, aliases, side):
        for alias in aliases:
            if alias in stats:
                return stats[alias][side]
        return None

    monkeypatch.setattr(bot, "get_any_metric", _metric)
    raw, availability, quality = bot._build_observation_raw_metrics({})

    assert raw["shots_on_target_home"] == 0
    assert availability["shots_on_target_home"] is True
    assert raw["shots_in_box_home"] is None
    assert availability["shots_in_box_home"] is False
    assert quality["available_metric_count"] == 4


def test_prefilter_observation_recovers_identity_from_live_fixture(monkeypatch) -> None:
    monkeypatch.setattr(bot, "get_any_metric", lambda fixture, aliases, side: None)
    observation = bot.build_prefilter_observation(
        fixture_id=500,
        minute=42,
        reason="pre_46_out_of_window",
        raw_fixture={
            "teams": {
                "home": {"id": 10, "name": "Home"},
                "away": {"id": 20, "name": "Away"},
            },
            "league": {"id": 30, "name": "League", "country": "Country"},
            "goals": {"home": 1, "away": 0},
            "fixture": {"status": {"short": "2H"}},
        },
    )

    assert observation["match"]["home_team_id"] == 10
    assert observation["match"]["away_team_name"] == "Away"
    assert observation["match"]["league_id"] == 30
    assert observation["match"]["score_state"] == "1-0"
    assert observation["decision"]["block_reason"] == "pre_46_out_of_window"


def test_observation_outcome_is_joined_for_blocked_prefilter(monkeypatch, tmp_path: Path) -> None:
    path = _configure(monkeypatch, tmp_path)
    assert bot.append_observation_history(_observation(101, 46)) is True

    written = bot.resolve_observation_history_outcomes(
        101,
        [_goal(70)],
        {"status_short": "FT"},
        (1, 0),
        "2026-07-25T02:00:00+00:00",
    )
    joined = bot.load_joined_observation_history(str(path))

    assert written == 1
    assert len(joined) == 1
    assert joined[0]["outcome"]["status"] == "resolved"
    assert joined[0]["outcome"]["normal_time_result"] == "WIN"
    assert joined[0]["outcome"]["first_goal_minute_after_snapshot"] == 70


def test_outcome_retrain_hint_only_uses_structurally_eligible_ml_rows(
    monkeypatch, tmp_path: Path
) -> None:
    _configure(monkeypatch, tmp_path)
    hints: list[tuple[int, dict]] = []
    monkeypatch.setattr(
        bot,
        "note_shadow_ml_outcome",
        lambda fixture_id, **kwargs: hints.append((fixture_id, kwargs)),
    )
    monkeypatch.setattr(bot, "append_shadow_candidate_outcomes", lambda rows: 0)
    monkeypatch.setattr(bot, "append_wide_research_outcomes", lambda rows: 0)

    prefilter = _observation(101, 46)
    bot.resolve_observation_history_outcomes(
        101,
        [_goal(70)],
        {"status_short": "FT"},
        (1, 0),
        "2026-07-25T02:00:00+00:00",
        observations=[prefilter],
    )
    assert hints == []

    decision = _rolling_observation(202, 46, 2)
    decision["rolling_dynamics"] = {
        "schema_version": bot.ROLLING_DYNAMICS_SCHEMA_VERSION,
        "mode": "shadow_collection",
        "production_applied": False,
        "windows": {
            "5m": {"status": "unavailable"},
            "10m": {"status": "unavailable"},
        },
    }
    bot.resolve_observation_history_outcomes(
        202,
        [_goal(70)],
        {"status_short": "FT"},
        (1, 0),
        "2026-07-25T02:00:00+00:00",
        observations=[decision],
    )

    assert hints == [
        (
            202,
            {
                "data_revision": False,
                "static_eligible": True,
                "rolling_eligible": True,
            },
        )
    ]


def test_pending_lookup_streams_without_full_history_materialization(
    monkeypatch, tmp_path: Path
) -> None:
    _configure(monkeypatch, tmp_path)
    pending = _observation(101, 46)
    resolved = _observation(202, 47)
    assert bot.append_observation_history(pending) is True
    assert bot.append_observation_history(resolved) is True
    assert bot.append_observation_history({
        "record_type": "observation_outcome",
        "observation_id": resolved["observation_id"],
        "observation_key": resolved["observation_id"],
        "fixture_id": 202,
        "outcome_schema_version": 1,
        "created_at_utc": "2026-07-25T01:00:00+00:00",
        "outcome": {"status": "resolved"},
    }) is True
    monkeypatch.setattr(
        bot,
        "load_joined_observation_history",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("pending lookup must stream the journal")
        ),
    )

    assert bot.pending_observation_fixture_ids() == [101]
    grouped = bot.pending_observations_by_fixture({101, 202})
    assert list(grouped) == [101]
    assert grouped[101][0]["observation_id"] == pending["observation_id"]


def test_reconcile_index_bootstraps_once_and_tracks_new_appends(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure(monkeypatch, tmp_path)
    first = _observation(101, 46)
    path.write_text(json.dumps(first) + "\n", encoding="utf-8")
    bot._invalidate_observation_reconcile_index()

    original_iter = bot.iter_observation_history_records
    scans = 0

    def counted_iter(*args, **kwargs):
        nonlocal scans
        scans += 1
        yield from original_iter(*args, **kwargs)

    monkeypatch.setattr(bot, "iter_observation_history_records", counted_iter)

    assert bot.pending_observation_fixture_ids() == [101]
    assert scans == 1
    assert bot.append_observation_history(_observation(202, 47)) is True
    assert bot.pending_observation_fixture_ids() == [101, 202]
    assert bot.pending_observations_by_fixture({101, 202}).keys() == {101, 202}
    assert scans == 1

    # A process-style cache close reuses the validated sidecar and still does
    # not parse the source journal again.
    bot._invalidate_observation_reconcile_index()
    assert bot.pending_observation_fixture_ids() == [101, 202]
    assert scans == 1


def test_reconcile_index_detects_external_journal_append(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure(monkeypatch, tmp_path)
    first = _observation(101, 46)
    path.write_text(json.dumps(first) + "\n", encoding="utf-8")
    bot._invalidate_observation_reconcile_index()
    assert bot.pending_observation_fixture_ids() == [101]

    second = _observation(202, 47)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(second) + "\n")

    assert bot.pending_observation_fixture_ids() == [101, 202]


def test_failed_reconcile_build_streams_during_cooldown_without_rebuild_storm(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure(monkeypatch, tmp_path)
    path.write_text(json.dumps(_observation(101, 46)) + "\n", encoding="utf-8")
    bot._invalidate_observation_reconcile_index()
    original_build = bot._build_observation_reconcile_index_locked
    attempts = 0

    def flaky_build(history_path: str) -> dict:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return {
                "history_path": os.path.abspath(history_path),
                "source_signature": bot._observation_history_source_signature(
                    history_path
                ),
                "invalid": True,
                "retry_after_monotonic": time.monotonic() + 300.0,
            }
        return original_build(history_path)

    monkeypatch.setattr(bot, "_build_observation_reconcile_index_locked", flaky_build)
    assert bot.pending_observation_fixture_ids() == [101]
    assert attempts == 1

    # Even a direct writer changing the source must not start another costly
    # SQLite build on every call.  Streaming remains authoritative meanwhile.
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_observation(202, 47)) + "\n")
    assert bot.pending_observation_fixture_ids() == [101, 202]
    assert bot.pending_observation_fixture_ids() == [101, 202]
    assert attempts == 1
    assert bot.append_observation_history(_observation(202, 47)) is False
    assert bot.append_observation_history(_observation(303, 48)) is True
    assert bot.pending_observation_fixture_ids() == [101, 202, 303]
    assert attempts == 1

    assert bot._observation_reconcile_index is not None
    bot._observation_reconcile_index["retry_after_monotonic"] = 0.0
    assert bot.pending_observation_fixture_ids() == [101, 202, 303]
    assert attempts == 2


def test_reconcile_index_joins_outcome_written_before_observation(
    monkeypatch, tmp_path: Path
) -> None:
    _configure(monkeypatch, tmp_path)
    observation = _observation(303, 48)
    outcome = {
        "record_type": "observation_outcome",
        "observation_id": observation["observation_id"],
        "observation_key": observation["observation_id"],
        "fixture_id": 999,
        "outcome_schema_version": 2,
        "outcome_revision": 3,
        "created_at_utc": "2026-07-25T02:00:00+00:00",
        "outcome": {"status": "resolved", "goal_to90": True},
    }

    assert bot.append_observation_history(outcome) is True
    assert bot.append_observation_history(observation) is True
    assert bot.pending_observation_fixture_ids() == []
    grouped = bot.pending_observations_by_fixture(
        {303}, include_terminal=True
    )
    assert list(grouped) == [303]
    assert grouped[303][0]["outcome"]["status"] == "resolved"
    assert grouped[303][0]["outcome_revision"] == 3
    streamed = bot._stream_pending_observations_by_fixture(
        {303}, include_terminal=True
    )
    assert streamed[303][0]["outcome"]["status"] == "resolved"
    assert streamed[303][0]["outcome_revision"] == 3


def test_reconcile_index_chunks_large_fixture_selection(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure(monkeypatch, tmp_path)
    fixtures = range(1, 1_025)
    path.write_text(
        "".join(json.dumps(_observation(fixture_id, 46)) + "\n" for fixture_id in fixtures),
        encoding="utf-8",
    )
    bot._invalidate_observation_reconcile_index()

    grouped = bot.pending_observations_by_fixture(set(fixtures))

    assert len(grouped) == 1_024
    assert grouped[1][0]["fixture_id"] == 1
    assert grouped[1_024][0]["fixture_id"] == 1_024


def test_config_hash_changes_when_decision_threshold_changes(monkeypatch) -> None:
    before = bot._observation_config_snapshot()["config_hash"]
    monkeypatch.setattr(
        bot,
        "DYNAMIC_TO90_THRESHOLD_46_49",
        bot.DYNAMIC_TO90_THRESHOLD_46_49 + 0.5,
    )
    after = bot._observation_config_snapshot()["config_hash"]

    assert before != after


def test_restart_rehydrates_rolling_from_gzip_and_active_history(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure(monkeypatch, tmp_path, rotate_bytes=1)
    at_36 = _rolling_observation(700, 36, 2)
    at_41 = _rolling_observation(700, 41, 4)
    current = _rolling_observation(700, 46, 7)

    assert bot.append_observation_history(at_36) is True
    assert bot.append_observation_history(at_41) is True
    assert list(tmp_path.glob("observation_history.*.jsonl.gz"))
    assert path.exists()

    bot._reset_rolling_dynamics_tracker_for_tests()
    frozen = bot.freeze_observation_rolling_dynamics(current)

    assert frozen["rolling_dynamics"]["windows"]["5m"]["deltas"][
        "shots_on_target_total"
    ] == 3.0
    assert frozen["rolling_dynamics"]["windows"]["10m"]["deltas"][
        "shots_on_target_total"
    ] == 5.0


class _EmptyRollingSeedClient:
    def __init__(self, *, raises: bool = False) -> None:
        self.calls = 0
        self.raises = raises

    def fetch_fixture_statistics(self, fixture_id: int) -> list[dict]:
        self.calls += 1
        if self.raises:
            raise RuntimeError("temporary statistics failure")
        return []

    def _normalize_fixture_basic(self, raw_fixture: dict) -> dict:
        return {
            "fixture_id": {"value": 800},
            "score_home": {"value": 0},
            "score_away": {"value": 0},
        }

    def _normalize_statistics(
        self,
        raw_stats: list[dict],
        raw_fixture: dict,
        fixture_metrics: dict,
    ) -> dict:
        return {}


class _SequenceRollingSeedClient(_EmptyRollingSeedClient):
    def __init__(self, responses: list[object]) -> None:
        super().__init__()
        self.responses = list(responses)

    def fetch_fixture_statistics(self, fixture_id: int) -> list[dict]:
        self.calls += 1
        response = self.responses[min(self.calls - 1, len(self.responses) - 1)]
        if isinstance(response, Exception):
            raise response
        return list(response)

    def _normalize_statistics(
        self,
        raw_stats: list[dict],
        raw_fixture: dict,
        fixture_metrics: dict,
    ) -> dict:
        if not raw_stats:
            return {}
        return {
            "shots_on_target_home": {"value": 1},
            "shots_on_target_away": {"value": 0},
            "total_shots_home": {"value": 3},
            "total_shots_away": {"value": 1},
        }


def test_rolling_seed_uses_four_bounded_slots_and_survives_restart(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure(monkeypatch, tmp_path)
    client = _EmptyRollingSeedClient()
    fixture = {
        "fixture": {"id": 800, "status": {"elapsed": 36, "short": "1H"}},
        "goals": {"home": 0, "away": 0},
    }
    monkeypatch.setattr(
        bot,
        "predict_and_append_shadow_ml",
        lambda record: (_ for _ in ()).throw(
            AssertionError("rolling seeds must never invoke shadow prediction")
        ),
    )

    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 35) is False
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 36) is True
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 38) is False
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 39) is True
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 40) is False
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 41) is True
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 43) is False
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 44) is True
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 45) is False
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 46) is False
    assert client.calls == 8

    bot._observation_history_keys = None
    bot._reset_rolling_dynamics_tracker_for_tests()
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 37) is False
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 40) is False
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 42) is False
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 45) is False
    assert client.calls == 8

    seeds = [
        record
        for record in bot.iter_observation_history_records(str(path))
        if record.get("stage") == "rolling_seed"
    ]
    assert len(seeds) == 4
    assert {seed["rolling_seed"]["slot"] for seed in seeds} == {
        "10m",
        "10m_bridge",
        "5m",
        "5m_bridge",
    }
    assert {seed["rolling_seed"]["statistics_fetch_status"] for seed in seeds} == {
        "empty"
    }
    assert all(
        seed["rolling_seed"]["statistics_fetch_attempt_count"] == 2
        for seed in seeds
    )
    assert all(
        seed["rolling_seed"]["statistics_fetch_attempt_statuses"]
        == ["empty", "empty"]
        for seed in seeds
    )
    assert all(seed["outcome"]["status"] == "void" for seed in seeds)
    assert all(seed["telegram"]["send_attempted"] is False for seed in seeds)
    assert 800 not in bot.pending_observation_fixture_ids()


def test_rolling_seed_statistics_error_is_fail_open_and_retried_once(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure(monkeypatch, tmp_path)
    client = _EmptyRollingSeedClient(raises=True)
    fixture = {
        "fixture": {"id": 800, "status": {"elapsed": 36, "short": "1H"}},
        "goals": {"home": 0, "away": 0},
    }

    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 36) is True
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 37) is False
    assert client.calls == 2
    seed = next(
        record
        for record in bot.iter_observation_history_records(str(path))
        if record.get("stage") == "rolling_seed"
    )
    assert seed["rolling_seed"]["statistics_fetch_status"] == "error"
    assert seed["rolling_seed"]["statistics_fetch_attempt_count"] == 2
    assert seed["rolling_seed"]["statistics_fetch_attempt_statuses"] == [
        "error",
        "error",
    ]


def test_successful_rolling_seed_is_not_retried(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure(monkeypatch, tmp_path)
    client = _SequenceRollingSeedClient([[{"statistics": []}]])
    fixture = {
        "fixture": {"id": 800, "status": {"elapsed": 36, "short": "1H"}},
        "goals": {"home": 0, "away": 0},
    }

    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 36) is True
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 37) is False
    assert client.calls == 1
    seed = next(
        record
        for record in bot.iter_observation_history_records(str(path))
        if record.get("stage") == "rolling_seed"
    )
    assert seed["rolling_seed"]["statistics_fetch_status"] == "ok"
    assert seed["rolling_seed"]["statistics_fetch_attempt_count"] == 1
    assert seed["rolling_seed"]["statistics_fetch_retry_used"] is False


def test_empty_rolling_seed_retry_success_is_a_durable_baseline(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure(monkeypatch, tmp_path)
    client = _SequenceRollingSeedClient([[], [{"statistics": []}]])
    fixture = {
        "fixture": {"id": 800, "status": {"elapsed": 36, "short": "1H"}},
        "goals": {"home": 0, "away": 0},
    }

    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 36) is True
    assert client.calls == 2
    seed = next(
        record
        for record in bot.iter_observation_history_records(str(path))
        if record.get("stage") == "rolling_seed"
    )
    assert seed["rolling_seed"]["statistics_fetch_attempt_statuses"] == [
        "empty",
        "ok",
    ]

    bot._observation_history_keys = None
    bot._reset_rolling_dynamics_tracker_for_tests()
    current = _rolling_observation(800, 46, 4)
    current["created_at_utc"] = "2099-01-01T00:46:00+00:00"
    frozen = bot.freeze_observation_rolling_dynamics(current)

    assert frozen["rolling_dynamics"]["windows"]["10m"]["status"] == "ok"
    assert frozen["rolling_dynamics"]["windows"]["10m"][
        "baseline_observation_id"
    ] == seed["observation_id"]


def test_rolling_seed_append_failure_does_not_repeat_api_in_same_process(
    monkeypatch, tmp_path: Path
) -> None:
    _configure(monkeypatch, tmp_path)
    client = _EmptyRollingSeedClient()
    fixture = {
        "fixture": {"id": 800, "status": {"elapsed": 36, "short": "1H"}},
        "goals": {"home": 0, "away": 0},
    }
    monkeypatch.setattr(bot, "append_observation_history", lambda record: False)
    monkeypatch.setattr(
        bot,
        "observation_history_record_exists",
        lambda record: False,
    )

    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 36) is False
    assert bot.maybe_capture_rolling_dynamics_seed(client, fixture, 800, 37) is False
    assert client.calls == 2


def test_slow_rolling_seed_is_queued_without_blocking_fixture_loop(
    monkeypatch, tmp_path: Path
) -> None:
    _configure(monkeypatch, tmp_path)
    started = threading.Event()
    release = threading.Event()

    class SlowClient(_EmptyRollingSeedClient):
        def fetch_fixture_statistics(self, fixture_id: int) -> list[dict]:
            self.calls += 1
            if self.calls == 1:
                started.set()
                assert release.wait(timeout=2.0)
            return []

    client = SlowClient()
    fixture = {
        "fixture": {"id": 800, "status": {"elapsed": 36, "short": "1H"}},
        "goals": {"home": 0, "away": 0},
    }
    with ThreadPoolExecutor(max_workers=1) as executor:
        before = time.monotonic()
        assert bot.schedule_rolling_dynamics_seed(
            executor, client, fixture, 800, 36
        ) is True
        elapsed = time.monotonic() - before
        assert elapsed < 0.5
        assert started.wait(timeout=1.0)
        assert bot.schedule_rolling_dynamics_seed(
            executor, client, fixture, 800, 36
        ) is False
        release.set()

    assert client.calls == 2
