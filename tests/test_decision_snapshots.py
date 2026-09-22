from __future__ import annotations

import json
import gzip
import os
from pathlib import Path

import pytest

import NanoTest as nanotest
from second_half import storage as second_half_storage


def _configure_store(monkeypatch, tmp_path: Path, *, rotate_bytes: int = 0) -> Path:
    path = tmp_path / "decision_snapshots.jsonl"
    observation_path = tmp_path / "observation_history.jsonl"
    monkeypatch.setattr(nanotest, "ENABLE_DECISION_SNAPSHOTS", True)
    monkeypatch.setattr(nanotest, "DECISION_SNAPSHOTS_FILE", str(path))
    monkeypatch.setattr(
        nanotest, "OBSERVATION_HISTORY_FILE", str(observation_path)
    )
    monkeypatch.setattr(
        nanotest, "SHADOW_ML_MODEL_FILE", str(tmp_path / "shadow_ml_model.json")
    )
    monkeypatch.setattr(
        nanotest,
        "SHADOW_ML_PREDICTIONS_FILE",
        str(tmp_path / "shadow_ml_predictions.jsonl"),
    )
    monkeypatch.setattr(
        nanotest,
        "SIGNAL_REPUTATION_MODEL_FILE",
        str(tmp_path / "signal_reputation.json"),
    )
    monkeypatch.setattr(
        nanotest,
        "SIGNAL_REPUTATION_SHADOW_FILE",
        str(tmp_path / "signal_reputation_shadow.jsonl"),
    )
    monkeypatch.setattr(nanotest, "DECISION_SNAPSHOT_ROTATE_MAX_BYTES", rotate_bytes)
    monkeypatch.setattr(nanotest, "DECISION_SNAPSHOT_DEDUPE_MAX_KEYS", 0)
    monkeypatch.setattr(nanotest, "DECISION_OUTCOME_RECHECK_SECONDS", 30)
    monkeypatch.setattr(nanotest, "STATE_FILE", str(tmp_path / "bot_state.json"))
    nanotest._decision_snapshot_keys_by_file.clear()
    nanotest._decision_snapshot_order_by_file.clear()
    nanotest._decision_snapshot_indexes_by_file.clear()
    nanotest._decision_outcome_last_checked.clear()
    nanotest._second_half_incomplete_retry.clear()
    nanotest.state["second_half_incomplete_retry"] = {}
    nanotest.state["decision_outcome_last_checked"] = {}
    nanotest._observation_history_keys = None
    nanotest._shadow_ml_model_cache = {}
    nanotest._shadow_ml_model_cache_path = ""
    nanotest._shadow_ml_model_cache_mtime_ns = None
    nanotest._shadow_ml_model_last_checked_ts = 0.0
    nanotest._shadow_ml_pending_fixture_ids.clear()
    nanotest._shadow_ml_known_new_fixtures = 0
    nanotest._shadow_ml_data_revision_pending = False
    nanotest._shadow_ml_contract_mismatch_pending = False
    nanotest._signal_reputation_model_cache = {}
    nanotest._signal_reputation_last_refresh_ts = 0.0
    nanotest.state["score_timelines"] = {}
    return path


def _decision(decision_id: str, fixture_id: int = 101, score=(0, 0), minute: int = 46) -> dict:
    return {
        "record_type": "decision",
        "decision_id": decision_id,
        "decision_key": decision_id,
        "fixture_id": fixture_id,
        "created_at_utc": "2026-07-21T00:00:00+00:00",
        "minute": minute,
        "schema_version": 2,
        "match": {"score_home": score[0], "score_away": score[1]},
        "decision": {"final_decision": "BLOCK"},
        "probabilities": {},
        "outcome": {"status": "pending"},
    }


def _simulate_reliability_restart() -> None:
    nanotest.save_state_to_disk()
    nanotest._decision_outcome_last_checked.clear()
    nanotest._second_half_incomplete_retry.clear()
    nanotest.load_state()


def _goal(elapsed: int, extra: int = 0) -> dict:
    return {
        "time": {"elapsed": elapsed, "extra": extra},
        "type": "Goal",
        "detail": "Normal Goal",
    }


def test_score_prefix_excludes_goal_already_visible_in_same_minute() -> None:
    events = [_goal(12), _goal(45, 1), _goal(61)]

    classified = nanotest._classified_goals_after_signal(
        events,
        46,
        {"status_short": "FT"},
        snapshot_score=(2, 0),
    )

    assert [item[0].clock_minute for item in classified["normal_time"]] == [61]


def test_score_prefix_keeps_goal_after_zero_zero_snapshot_in_same_minute() -> None:
    classified = nanotest._classified_goals_after_signal(
        [_goal(46)],
        46,
        {"status_short": "FT"},
        snapshot_score=(0, 0),
    )

    assert [item[0].clock_minute for item in classified["normal_time"]] == [46]


def test_incomplete_event_feed_uses_strict_minute_fallback() -> None:
    classified = nanotest._classified_goals_after_signal(
        [_goal(46)],
        46,
        {"status_short": "FT"},
        snapshot_score=(2, 0),
    )

    assert classified["normal_time"] == []


def test_rotation_is_read_transparently_and_dedupe_survives(monkeypatch, tmp_path: Path) -> None:
    path = _configure_store(monkeypatch, tmp_path, rotate_bytes=250)
    first = _decision("101:46:WINDOW_1:v2")
    second = _decision("101:47:WINDOW_1:v2", minute=47)

    assert nanotest.append_decision_snapshot(first) is True
    assert nanotest.append_decision_snapshot(second) is True
    nanotest._decision_snapshot_keys_by_file.clear()
    nanotest._decision_snapshot_order_by_file.clear()
    assert nanotest.append_decision_snapshot(first) is False

    archives = list(tmp_path.glob("decision_snapshots.*.jsonl.gz"))
    assert archives
    with gzip.open(archives[0], "rt", encoding="utf-8") as handle:
        assert json.loads(handle.readline())["decision_id"] == first["decision_id"]
    assert path.exists()
    assert len(list(nanotest.iter_decision_snapshot_records(str(path)))) == 2


def test_same_minute_block_then_allow_are_both_persisted(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure_store(monkeypatch, tmp_path)
    block_id = nanotest._decision_snapshot_id(
        101, 46, "WINDOW_1", "BLOCK"
    )
    allow_id = nanotest._decision_snapshot_id(
        101, 46, "WINDOW_1", "ALLOW"
    )
    block = _decision(block_id)
    block["schema_version"] = nanotest.DECISION_SNAPSHOT_SCHEMA_VERSION
    allow = _decision(allow_id)
    allow["schema_version"] = nanotest.DECISION_SNAPSHOT_SCHEMA_VERSION
    allow["decision"]["final_decision"] = "ALLOW"

    assert block_id != allow_id
    assert nanotest.append_decision_snapshot(block) is True
    assert nanotest.append_decision_snapshot(allow) is True
    assert nanotest.append_decision_snapshot(block) is False
    assert [
        row["decision_id"]
        for row in nanotest.iter_decision_snapshot_records(str(path))
    ] == [block_id, allow_id]


def test_reader_remains_compatible_with_uncompressed_archive(monkeypatch, tmp_path: Path) -> None:
    path = _configure_store(monkeypatch, tmp_path)
    archive = tmp_path / "decision_snapshots.20260721T000000Z.jsonl"
    archive.write_text(json.dumps(_decision("101:46:WINDOW_1:v2")) + "\n", encoding="utf-8")
    path.write_text(json.dumps(_decision("101:47:WINDOW_1:v2", minute=47)) + "\n", encoding="utf-8")

    records = list(nanotest.iter_decision_snapshot_records(str(path)))

    assert [record["minute"] for record in records] == [46, 47]


def test_reader_ignores_generated_dot_prefixed_export(monkeypatch, tmp_path: Path) -> None:
    path = _configure_store(monkeypatch, tmp_path)
    generated = tmp_path / "decision_snapshots.joined.jsonl"
    generated.write_text(
        json.dumps(_decision("should:not:be:loaded")) + "\n",
        encoding="utf-8",
    )
    path.write_text(
        json.dumps(_decision("101:46:WINDOW_1:v2")) + "\n",
        encoding="utf-8",
    )

    records = list(nanotest.iter_decision_snapshot_records(str(path)))

    assert [record["decision_id"] for record in records] == [
        "101:46:WINDOW_1:v2"
    ]


def test_joiner_uses_newest_outcome_schema(monkeypatch, tmp_path: Path) -> None:
    path = _configure_store(monkeypatch, tmp_path)
    decision = _decision("101:46:WINDOW_1:v2")
    old = {
        "record_type": "outcome", "decision_id": decision["decision_id"],
        "decision_key": decision["decision_id"], "fixture_id": 101,
        "outcome_schema_version": 2, "created_at_utc": "2026-07-21T01:00:00+00:00",
        "outcome": {"status": "resolved", "normal_time_result": "WIN"},
    }
    new = {
        **old,
        "outcome_schema_version": 3,
        "created_at_utc": "2026-07-21T02:00:00+00:00",
        "outcome": {"status": "resolved", "normal_time_result": "LOSS"},
    }
    for record in (decision, old, new):
        assert nanotest.append_decision_snapshot(record) is True

    joined = nanotest.load_joined_decision_snapshots(str(path))

    assert len(joined) == 1
    assert joined[0]["outcome_schema_version"] == 3
    assert joined[0]["outcome"]["normal_time_result"] == "LOSS"


def test_strict_joiner_rejects_mismatched_decision_identifiers(
    monkeypatch, tmp_path: Path
) -> None:
    path = _configure_store(monkeypatch, tmp_path)
    corrupt = _decision("decision-a")
    corrupt["decision_key"] = "decision-b"
    path.write_text(json.dumps(corrupt) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="mismatched"):
        nanotest.load_joined_decision_snapshots(str(path), strict_ids=True)

    assert nanotest.load_joined_decision_snapshots(str(path)) == []


def test_lazy_index_bootstraps_once_and_updates_after_append(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_store(monkeypatch, tmp_path)
    first = _decision("101:46:WINDOW_1:v2")
    second = _decision("101:47:WINDOW_1:v2", minute=47)
    assert nanotest.append_decision_snapshot(first) is True

    real_iterator = nanotest.iter_decision_snapshot_records
    scans = {"count": 0}

    def counted_iterator(path=None):
        scans["count"] += 1
        yield from real_iterator(path)

    monkeypatch.setattr(nanotest, "iter_decision_snapshot_records", counted_iterator)
    assert nanotest.pending_decision_snapshot_fixture_ids() == [101]
    assert scans["count"] == 1

    assert nanotest.append_decision_snapshot(second) is True
    records = nanotest.get_decision_snapshots_for_fixture(101)
    assert [record["minute"] for record in records] == [46, 47]
    assert nanotest.pending_decision_snapshot_fixture_ids() == [101]
    assert scans["count"] == 1


def test_current_outcomes_remove_fixture_from_index_pending_set(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_store(monkeypatch, tmp_path)
    decision = _decision("101:46:WINDOW_1:v2")
    assert nanotest.append_decision_snapshot(decision) is True
    assert nanotest.pending_decision_snapshot_fixture_ids() == [101]

    outcome = {
        "record_type": "outcome",
        "decision_id": decision["decision_id"],
        "decision_key": decision["decision_id"],
        "fixture_id": 101,
        "outcome_schema_version": nanotest.DECISION_OUTCOME_SCHEMA_VERSION,
        "created_at_utc": "2026-07-21T02:00:00+00:00",
        "outcome": {"status": "resolved"},
    }
    assert nanotest.append_decision_snapshot(outcome) is True

    assert nanotest.pending_decision_snapshot_fixture_ids() == []
    index = nanotest._get_decision_snapshot_index()
    assert index["decisions_by_id"] == {}
    assert index["decision_ids_by_fixture"] == {}
    assert index["outcomes_by_id"][decision["decision_id"]] == (
        nanotest.DECISION_OUTCOME_SCHEMA_VERSION,
        "2026-07-21T02:00:00+00:00",
        "resolved",
    )


def test_old_outcome_schema_remains_pending_in_index(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_store(monkeypatch, tmp_path)
    decision = _decision("101:46:WINDOW_1:v2")
    old_outcome = {
        "record_type": "outcome",
        "decision_id": decision["decision_id"],
        "decision_key": decision["decision_id"],
        "fixture_id": 101,
        "outcome_schema_version": nanotest.DECISION_OUTCOME_SCHEMA_VERSION - 1,
        "created_at_utc": "2026-07-21T02:00:00+00:00",
        "outcome": {"status": "resolved"},
    }
    assert nanotest.append_decision_snapshot(decision) is True
    assert nanotest.append_decision_snapshot(old_outcome) is True

    assert nanotest.pending_decision_snapshot_fixture_ids() == [101]


def test_current_quarantine_is_terminal_in_pending_index(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_store(monkeypatch, tmp_path)
    decision = _decision("101:46:WINDOW_1:v2")
    quarantined = {
        "record_type": "outcome",
        "decision_id": decision["decision_id"],
        "decision_key": decision["decision_id"],
        "fixture_id": 101,
        "outcome_schema_version": nanotest.DECISION_OUTCOME_SCHEMA_VERSION,
        "created_at_utc": "2026-07-21T02:00:00+00:00",
        "outcome": {"status": "quarantine"},
    }
    assert nanotest.append_decision_snapshot(decision) is True
    assert nanotest.append_decision_snapshot(quarantined) is True

    assert nanotest.pending_decision_snapshot_fixture_ids() == []
    assert nanotest.get_decision_snapshots_for_fixture(101) == []


def test_rotation_does_not_invalidate_loaded_index(monkeypatch, tmp_path: Path) -> None:
    _configure_store(monkeypatch, tmp_path, rotate_bytes=250)
    assert nanotest.append_decision_snapshot(
        _decision("101:46:WINDOW_1:v2")
    ) is True
    assert len(nanotest.get_decision_snapshots_for_fixture(101)) == 1

    assert nanotest.append_decision_snapshot(
        _decision("101:47:WINDOW_1:v2", minute=47)
    ) is True

    assert len(nanotest.get_decision_snapshots_for_fixture(101)) == 2
    assert list(tmp_path.glob("decision_snapshots.*.jsonl.gz"))


def test_resolver_writes_current_score_aligned_outcome(monkeypatch, tmp_path: Path) -> None:
    path = _configure_store(monkeypatch, tmp_path)
    decision = _decision("101:46:WINDOW_1:v2", score=(2, 0))
    assert nanotest.append_decision_snapshot(decision) is True

    written = nanotest.resolve_decision_snapshot_outcomes(
        101,
        [_goal(12), _goal(45, 1), _goal(61)],
        {"status_short": "FT"},
        (3, 0),
        "2026-07-21T03:00:00+00:00",
    )
    joined = nanotest.load_joined_decision_snapshots(str(path))

    assert written == 1
    assert joined[0]["outcome_schema_version"] == nanotest.DECISION_OUTCOME_SCHEMA_VERSION
    assert joined[0]["outcome"]["first_goal_minute_after_snapshot"] == 61
    assert joined[0]["outcome"]["label_alignment_method"] == "snapshot_score_event_prefix"


def test_resolver_uses_score_delta_when_events_are_missing(monkeypatch, tmp_path: Path) -> None:
    path = _configure_store(monkeypatch, tmp_path)
    decision = _decision("101:61:WINDOW_2:v2", score=(1, 0), minute=61)
    assert nanotest.append_decision_snapshot(decision) is True

    written = nanotest.resolve_decision_snapshot_outcomes(
        101,
        [],
        {"status_short": "FT"},
        (4, 0),
        "2026-07-21T03:00:00+00:00",
    )
    joined = nanotest.load_joined_decision_snapshots(str(path))
    outcome = joined[0]["outcome"]

    assert written == 1
    assert outcome["normal_time_result"] == "WIN"
    assert outcome["goal_to90_normal_time"] is True
    assert outcome["goals_after_snapshot"] == 3
    assert outcome["goal_result_source"] == "score_delta"
    assert outcome["goal_within_15"] is None
    assert outcome["goal_within_15_source"] == "unknown_timing"
    assert outcome["first_goal_minute_after_snapshot"] is None


def test_correction_does_not_downgrade_confirmed_win(monkeypatch, tmp_path: Path) -> None:
    path = _configure_store(monkeypatch, tmp_path)
    decision = _decision("101:46:WINDOW_1:v2", score=(0, 0))
    assert nanotest.append_decision_snapshot(decision) is True

    first = nanotest.resolve_decision_snapshot_outcomes(
        101,
        [],
        {"status_short": "FT"},
        (1, 0),
        "2026-07-21T03:00:00+00:00",
    )
    assert first == 1
    joined = nanotest.load_joined_decision_snapshots(str(path))
    assert joined[0]["outcome"]["normal_time_result"] == "WIN"
    assert joined[0]["outcome"]["goal_to90_normal_time"] is True

    correction = nanotest.resolve_decision_snapshot_outcomes(
        101,
        [],
        {"status_short": "FT"},
        (0, 0),
        "2026-07-21T04:00:00+00:00",
        include_terminal_records=True,
    )
    joined = nanotest.load_joined_decision_snapshots(str(path))
    outcome = joined[0]["outcome"]

    assert correction == 1
    assert outcome["normal_time_result"] == "WIN"
    assert outcome["goal_to90_normal_time"] is True
    assert outcome["goal_result_source"] == "previously_confirmed_win"


def test_reconciler_resolves_blocked_fixture(monkeypatch, tmp_path: Path) -> None:
    _configure_store(monkeypatch, tmp_path)
    assert nanotest.append_decision_snapshot(_decision("101:46:WINDOW_1:v2")) is True

    class Client:
        def fetch_fixture(self, fixture_id: int) -> dict:
            assert fixture_id == 101
            return {
                "fixture": {"id": fixture_id, "status": {"short": "FT"}},
                "goals": {"home": 1, "away": 0},
                "score": {"fulltime": {"home": 1, "away": 0}},
            }

        def fetch_fixture_events(self, fixture_id: int) -> list[dict]:
            return [_goal(70)]

    summary = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), limit=10, now_ts=1_000.0
    )
    joined = nanotest.load_joined_decision_snapshots()

    assert summary == {
        "checked": 1,
        "resolved_fixtures": 1,
        "void_fixtures": 0,
        "records_written": 1,
    }
    assert joined[0]["decision"]["final_decision"] == "BLOCK"
    assert joined[0]["outcome"]["status"] == "resolved"
    assert joined[0]["outcome"]["normal_time_result"] == "WIN"


def test_reconciler_collects_second_half_history_for_observation_only_fixture(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_store(monkeypatch, tmp_path)
    history_path = tmp_path / "second_half_history.jsonl"
    monkeypatch.setattr(nanotest, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(nanotest, "ENABLE_2H_COLLECTION", True)
    monkeypatch.setattr(nanotest, "AUTO_AGGREGATE_2H_STATS", False)
    monkeypatch.setattr(nanotest, "SECOND_HALF_HISTORY_PATH", str(history_path))
    second_half_storage.reset_second_half_history_cache(str(history_path))

    live_fixture = {
        "fixture": {"id": 202, "status": {"short": "2H"}},
        "league": {"id": 218, "name": "Bundesliga", "type": "League", "season": 2026},
        "teams": {
            "home": {"id": 571, "name": "Home"},
            "away": {"id": 601, "name": "Away"},
        },
        "goals": {"home": 0, "away": 0},
    }
    observation = nanotest.build_prefilter_observation(
        fixture_id=202,
        minute=44,
        reason="rolling_dynamics_seed",
        raw_fixture=live_fixture,
    )
    assert nanotest.append_observation_history(observation) is True

    terminal_fixture = {
        **live_fixture,
        "fixture": {
            "id": 202,
            "date": "2026-08-14T10:00:00+00:00",
            "status": {"short": "FT"},
        },
        "goals": {"home": 1, "away": 1},
        "score": {
            "halftime": {"home": 0, "away": 0},
            "fulltime": {"home": 1, "away": 1},
        },
    }

    class Client:
        def fetch_fixture(self, fixture_id: int) -> dict:
            assert fixture_id == 202
            return terminal_fixture

        def fetch_fixture_events(self, fixture_id: int) -> list[dict]:
            assert fixture_id == 202
            return [_goal(55), _goal(70)]

    summary = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), limit=10, now_ts=1_000.0
    )
    records = second_half_storage.load_second_half_history_records(str(history_path))
    joined_observations = nanotest.load_joined_observation_history()

    assert summary["resolved_fixtures"] == 1
    assert len(records) == 1
    assert records[0]["fixture_id"] == 202
    assert records[0]["goals_2h_total"] == 2
    assert records[0]["finished_at"] == "2026-08-14T12:00:00+00:00"
    assert records[0]["finished_at_source"] == "kickoff_plus_2h_estimate"
    assert joined_observations[0]["outcome"]["status"] == "resolved"

    second = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), limit=10, now_ts=2_000.0
    )
    assert second["checked"] == 0
    assert len(second_half_storage.load_second_half_history_records(str(history_path))) == 1


def test_second_half_collection_failure_is_retried_after_observation_resolves(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_store(monkeypatch, tmp_path)
    history_path = tmp_path / "second_half_history.jsonl"
    monkeypatch.setattr(nanotest, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(nanotest, "ENABLE_2H_COLLECTION", True)
    monkeypatch.setattr(nanotest, "SECOND_HALF_HISTORY_PATH", str(history_path))
    monkeypatch.setattr(
        nanotest, "SECOND_HALF_INCOMPLETE_RETRY_BASE_SECONDS", 100
    )
    monkeypatch.setattr(
        nanotest, "SECOND_HALF_INCOMPLETE_RETRY_MAX_SECONDS", 400
    )
    second_half_storage.reset_second_half_history_cache(str(history_path))
    assert nanotest.append_observation_history(
        nanotest.build_prefilter_observation(
            fixture_id=203,
            minute=46,
            reason="candidate",
            raw_fixture={"fixture": {"id": 203}, "goals": {"home": 0, "away": 0}},
        )
    ) is True

    fixture = {
        "fixture": {"id": 203, "status": {"short": "FT"}},
        "goals": {"home": 1, "away": 0},
        "score": {
            "halftime": {"home": 0, "away": 0},
            "fulltime": {"home": 1, "away": 0},
        },
    }

    class Client:
        def fetch_fixture(self, fixture_id: int) -> dict:
            return fixture

        def fetch_fixture_events(self, fixture_id: int) -> list[dict]:
            return [_goal(70)]

    attempts: list[int] = []

    def fail_store(*args, **kwargs) -> bool:
        attempts.append(1)
        return False

    monkeypatch.setattr(nanotest, "store_second_half_history_payload", fail_store)
    first = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=1_000.0
    )
    assert first["resolved_fixtures"] == 1
    assert len(attempts) == 1
    assert nanotest.load_joined_observation_history()[0]["outcome"]["status"] == "resolved"
    assert nanotest._second_half_incomplete_retry[203] == (1, 1_100.0)

    nanotest._decision_outcome_last_checked.clear()
    second = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=1_100.0
    )
    assert second["checked"] == 1
    assert len(attempts) == 2
    assert nanotest._second_half_incomplete_retry[203][0] == 2
    assert second_half_storage.load_second_half_history_records(str(history_path)) == []


def test_incomplete_second_half_data_uses_backoff_without_blocking_outcomes(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_store(monkeypatch, tmp_path)
    history_path = tmp_path / "second_half_history.jsonl"
    monkeypatch.setattr(nanotest, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(nanotest, "ENABLE_2H_COLLECTION", True)
    monkeypatch.setattr(nanotest, "AUTO_AGGREGATE_2H_STATS", False)
    monkeypatch.setattr(nanotest, "SECOND_HALF_HISTORY_PATH", str(history_path))
    monkeypatch.setattr(
        nanotest, "SECOND_HALF_INCOMPLETE_RETRY_BASE_SECONDS", 100
    )
    monkeypatch.setattr(
        nanotest, "SECOND_HALF_INCOMPLETE_RETRY_MAX_SECONDS", 400
    )
    second_half_storage.reset_second_half_history_cache(str(history_path))
    assert nanotest.append_observation_history(
        nanotest.build_prefilter_observation(
            fixture_id=205,
            minute=46,
            reason="candidate",
            raw_fixture={"fixture": {"id": 205}, "goals": {"home": 0, "away": 0}},
        )
    ) is True

    complete_halftime = False
    fetches: list[int] = []

    class Client:
        def fetch_fixture(self, fixture_id: int) -> dict:
            fetches.append(fixture_id)
            score = {"fulltime": {"home": 1, "away": 1}}
            if complete_halftime:
                score["halftime"] = {"home": 0, "away": 0}
            return {
                "fixture": {
                    "id": fixture_id,
                    "date": "2026-08-14T10:00:00+00:00",
                    "status": {"short": "FT"},
                },
                "league": {
                    "id": 218,
                    "name": "Bundesliga",
                    "type": "League",
                    "season": 2026,
                },
                "teams": {
                    "home": {"id": 571, "name": "Home"},
                    "away": {"id": 601, "name": "Away"},
                },
                "goals": {"home": 1, "away": 1},
                "score": score,
            }

        def fetch_fixture_events(self, fixture_id: int) -> list[dict]:
            return []

    first = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=1_000.0
    )
    assert first["resolved_fixtures"] == 1
    assert nanotest._second_half_incomplete_retry[205] == (1, 1_100.0)
    assert second_half_storage.load_second_half_history_records(str(history_path)) == []

    # A new pending outcome is still reconciled while only 2H collection is
    # deferred; this must not trigger another 2H parsing attempt.
    assert nanotest.append_decision_snapshot(
        _decision("205:50:WINDOW_1:v2", fixture_id=205, minute=50)
    ) is True
    during_backoff = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=1_050.0
    )
    assert during_backoff["resolved_fixtures"] == 1
    assert nanotest.load_joined_decision_snapshots()[0]["outcome"]["status"] == "resolved"
    assert nanotest._second_half_incomplete_retry[205] == (1, 1_100.0)

    second = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=1_100.0
    )
    assert second["resolved_fixtures"] == 1
    assert nanotest._second_half_incomplete_retry[205] == (2, 1_300.0)

    before_second_deadline = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=1_250.0
    )
    assert before_second_deadline["checked"] == 0

    complete_halftime = True
    recovered = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=1_300.0
    )
    assert recovered["resolved_fixtures"] == 1
    assert 205 not in nanotest._second_half_incomplete_retry
    records = second_half_storage.load_second_half_history_records(str(history_path))
    assert len(records) == 1
    assert records[0]["fixture_id"] == 205
    assert records[0]["ht_home"] == 0
    assert records[0]["ht_away"] == 0
    assert len(fetches) == 4


def test_second_half_false_store_survives_crash_restart_until_written(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_store(monkeypatch, tmp_path)
    history_path = tmp_path / "second_half_history.jsonl"
    monkeypatch.setattr(nanotest, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(nanotest, "ENABLE_2H_COLLECTION", True)
    monkeypatch.setattr(nanotest, "AUTO_AGGREGATE_2H_STATS", False)
    monkeypatch.setattr(nanotest, "SECOND_HALF_HISTORY_PATH", str(history_path))
    monkeypatch.setattr(nanotest, "SECOND_HALF_INCOMPLETE_RETRY_BASE_SECONDS", 100)
    monkeypatch.setattr(nanotest, "SECOND_HALF_INCOMPLETE_RETRY_MAX_SECONDS", 400)
    second_half_storage.reset_second_half_history_cache(str(history_path))
    assert nanotest.append_observation_history(
        nanotest.build_prefilter_observation(
            fixture_id=208,
            minute=46,
            reason="candidate",
            raw_fixture={"fixture": {"id": 208}, "goals": {"home": 0, "away": 0}},
        )
    ) is True

    fixture = {
        "fixture": {
            "id": 208,
            "date": "2026-08-14T10:00:00+00:00",
            "status": {"short": "FT"},
        },
        "league": {
            "id": 218,
            "name": "Bundesliga",
            "type": "League",
            "season": 2026,
        },
        "teams": {
            "home": {"id": 571, "name": "Home"},
            "away": {"id": 601, "name": "Away"},
        },
        "goals": {"home": 1, "away": 0},
        "score": {
            "halftime": {"home": 0, "away": 0},
            "fulltime": {"home": 1, "away": 0},
        },
    }
    attempts: list[int] = []
    fail_until = {"n": 1}
    real_store = nanotest.store_second_half_history_payload

    def flaky_store(*args, **kwargs):
        attempts.append(1)
        if len(attempts) <= fail_until["n"]:
            return False
        return real_store(*args, **kwargs)

    monkeypatch.setattr(nanotest, "store_second_half_history_payload", flaky_store)

    class Client:
        def fetch_fixture(self, fixture_id: int) -> dict:
            return fixture

        def fetch_fixture_events(self, fixture_id: int) -> list[dict]:
            return [_goal(70)]

    first = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=1_000.0
    )
    assert first["resolved_fixtures"] == 1
    assert len(attempts) == 1
    assert nanotest._second_half_incomplete_retry[208] == (1, 1_100.0)

    _simulate_reliability_restart()
    assert nanotest._second_half_incomplete_retry[208] == (1, 1_100.0)
    assert nanotest._decision_outcome_last_checked.get(208) == 1_000.0

    recovered = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=1_100.0
    )
    assert recovered["checked"] == 1
    assert len(attempts) == 2
    assert 208 not in nanotest._second_half_incomplete_retry
    records = second_half_storage.load_second_half_history_records(str(history_path))
    assert len(records) == 1
    assert records[0]["fixture_id"] == 208


def test_incomplete_second_half_backoff_survives_crash_restart(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_store(monkeypatch, tmp_path)
    history_path = tmp_path / "second_half_history.jsonl"
    monkeypatch.setattr(nanotest, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(nanotest, "ENABLE_2H_COLLECTION", True)
    monkeypatch.setattr(nanotest, "AUTO_AGGREGATE_2H_STATS", False)
    monkeypatch.setattr(nanotest, "SECOND_HALF_HISTORY_PATH", str(history_path))
    monkeypatch.setattr(nanotest, "SECOND_HALF_INCOMPLETE_RETRY_BASE_SECONDS", 100)
    monkeypatch.setattr(nanotest, "SECOND_HALF_INCOMPLETE_RETRY_MAX_SECONDS", 400)
    second_half_storage.reset_second_half_history_cache(str(history_path))
    assert nanotest.append_observation_history(
        nanotest.build_prefilter_observation(
            fixture_id=209,
            minute=46,
            reason="candidate",
            raw_fixture={"fixture": {"id": 209}, "goals": {"home": 0, "away": 0}},
        )
    ) is True

    complete_halftime = False

    class Client:
        def fetch_fixture(self, fixture_id: int) -> dict:
            score = {"fulltime": {"home": 1, "away": 1}}
            if complete_halftime:
                score["halftime"] = {"home": 0, "away": 0}
            return {
                "fixture": {
                    "id": fixture_id,
                    "date": "2026-08-14T10:00:00+00:00",
                    "status": {"short": "FT"},
                },
                "league": {
                    "id": 218,
                    "name": "Bundesliga",
                    "type": "League",
                    "season": 2026,
                },
                "teams": {
                    "home": {"id": 571, "name": "Home"},
                    "away": {"id": 601, "name": "Away"},
                },
                "goals": {"home": 1, "away": 1},
                "score": score,
            }

        def fetch_fixture_events(self, fixture_id: int) -> list[dict]:
            return []

    first = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=1_000.0
    )
    assert first["resolved_fixtures"] == 1
    assert nanotest._second_half_incomplete_retry[209] == (1, 1_100.0)
    assert second_half_storage.load_second_half_history_records(str(history_path)) == []

    _simulate_reliability_restart()
    assert nanotest._second_half_incomplete_retry[209] == (1, 1_100.0)

    during_backoff = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=1_050.0
    )
    assert during_backoff["checked"] == 0
    assert nanotest._second_half_incomplete_retry[209] == (1, 1_100.0)

    complete_halftime = True
    recovered = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=1_100.0
    )
    assert recovered["checked"] == 1
    assert 209 not in nanotest._second_half_incomplete_retry
    records = second_half_storage.load_second_half_history_records(str(history_path))
    assert len(records) == 1
    assert records[0]["fixture_id"] == 209


def test_reconciler_reuses_events_response_and_preserves_unavailable_quality(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_store(monkeypatch, tmp_path)
    history_path = tmp_path / "second_half_history.jsonl"
    monkeypatch.setattr(nanotest, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(nanotest, "ENABLE_2H_COLLECTION", True)
    monkeypatch.setattr(nanotest, "SECOND_HALF_HISTORY_PATH", str(history_path))
    second_half_storage.reset_second_half_history_cache(str(history_path))
    assert nanotest.append_observation_history(
        nanotest.build_prefilter_observation(
            fixture_id=204,
            minute=46,
            reason="candidate",
            raw_fixture={"fixture": {"id": 204}, "goals": {"home": 0, "away": 0}},
        )
    ) is True

    class Client:
        def fetch_fixture(self, fixture_id: int) -> dict:
            return {
                "fixture": {"id": 204, "status": {"short": "FT"}},
                "goals": {"home": 0, "away": 0},
                "score": {
                    "halftime": {"home": 0, "away": 0},
                    "fulltime": {"home": 0, "away": 0},
                },
            }

        def fetch_fixture_events_response(self, fixture_id: int):
            return None

        def fetch_fixture_events(self, fixture_id: int):
            raise AssertionError("must not make a second events request")

    captured_payloads: list[object] = []

    def capture_store(fixture_payload, events_payload, **kwargs) -> bool:
        captured_payloads.append(events_payload)
        return True

    monkeypatch.setattr(nanotest, "store_second_half_history_payload", capture_store)
    monkeypatch.setattr(nanotest, "_aggregate_second_half_history", lambda **kwargs: True)
    summary = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=1_000.0
    )

    assert summary["resolved_fixtures"] == 1
    assert captured_payloads == [None]


def test_reconciler_marks_cancelled_fixture_void(monkeypatch, tmp_path: Path) -> None:
    _configure_store(monkeypatch, tmp_path)
    assert nanotest.append_decision_snapshot(_decision("101:46:WINDOW_1:v2")) is True

    class Client:
        def fetch_fixture(self, fixture_id: int) -> dict:
            return {"fixture": {"id": fixture_id, "status": {"short": "CANC"}}}

    summary = nanotest.reconcile_pending_decision_outcomes(Client(), set(), now_ts=1_000.0)
    joined = nanotest.load_joined_decision_snapshots()

    assert summary["void_fixtures"] == 1
    assert joined[0]["outcome"]["status"] == "void"
    assert joined[0]["outcome"]["void_reason"] == "fixture_status_canc"


def test_reconciler_voids_old_evidence_when_fixture_id_is_rescheduled(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_store(monkeypatch, tmp_path)
    monkeypatch.setattr(nanotest, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(
        nanotest, "DECISION_OUTCOME_RESCHEDULE_MIN_SHIFT_SECONDS", 21_600
    )
    pending_updates: list[tuple[int, str | None]] = []

    class Monitor:
        def note_pending(self, count, oldest):
            pending_updates.append((count, oldest))

    monkeypatch.setattr(nanotest, "_research_health_monitor", Monitor())
    decision = _decision("206:46:WINDOW_1:v2", fixture_id=206)
    decision["created_at_utc"] = "2026-08-06T01:36:13+00:00"
    assert nanotest.append_decision_snapshot(decision) is True

    observation = nanotest.build_prefilter_observation(
        fixture_id=206,
        minute=45,
        reason="candidate",
        raw_fixture={
            "fixture": {"id": 206, "status": {"short": "HT"}},
            "goals": {"home": 0, "away": 0},
        },
    )
    observation["created_at_utc"] = "2026-08-06T01:36:13+00:00"
    assert nanotest.append_observation_history(observation) is True

    class Client:
        def fetch_fixture(self, fixture_id: int) -> dict:
            assert fixture_id == 206
            return {
                "fixture": {
                    "id": fixture_id,
                    "date": "2026-10-04T00:00:00+00:00",
                    "status": {"short": "NS"},
                }
            }

    summary = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=1_800_000_000.0
    )

    assert summary["void_fixtures"] == 1
    assert summary["resolved_fixtures"] == 0
    joined_decisions = nanotest.load_joined_decision_snapshots()
    assert joined_decisions[0]["outcome"]["status"] == "void"
    assert (
        joined_decisions[0]["outcome"]["void_reason"]
        == "fixture_rescheduled_ns"
    )
    joined_observations = nanotest.load_joined_observation_history()
    assert joined_observations[0]["outcome"]["status"] == "void"
    assert (
        joined_observations[0]["outcome"]["void_reason"]
        == "fixture_rescheduled_ns"
    )
    assert pending_updates[0] == (1, "2026-08-06T01:36:13Z")
    assert pending_updates[-1] == (0, None)


@pytest.mark.parametrize("status_short", ["FT", "CANC"])
def test_reconciler_does_not_hide_pending_when_observation_outcome_append_fails(
    monkeypatch, tmp_path: Path, status_short: str
) -> None:
    _configure_store(monkeypatch, tmp_path)
    monkeypatch.setattr(nanotest, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(nanotest, "ENABLE_2H_COLLECTION", False)
    monkeypatch.setattr(nanotest, "ENABLE_OUTCOME_CORRECTION_RECHECK", False)
    pending_updates: list[tuple[int, str | None]] = []

    class Monitor:
        def note_pending(self, count, oldest):
            pending_updates.append((count, oldest))

    monkeypatch.setattr(nanotest, "_research_health_monitor", Monitor())
    assert nanotest.append_decision_snapshot(
        _decision("207:46:WINDOW_1:v2", fixture_id=207)
    ) is True
    observation = nanotest.build_prefilter_observation(
        fixture_id=207,
        minute=46,
        reason="candidate",
        raw_fixture={
            "fixture": {"id": 207, "status": {"short": "2H"}},
            "goals": {"home": 0, "away": 0},
        },
    )
    observation["created_at_utc"] = "2026-09-13T07:30:00+00:00"
    assert nanotest.append_observation_history(observation) is True

    persisted_append = nanotest.append_observation_history

    def fail_observation_outcome(record: dict) -> bool:
        if record.get("record_type") == "observation_outcome":
            return False
        return persisted_append(record)

    monkeypatch.setattr(
        nanotest, "append_observation_history", fail_observation_outcome
    )

    class Client:
        def fetch_fixture(self, fixture_id: int) -> dict:
            return {
                "fixture": {
                    "id": fixture_id,
                    "status": {"short": status_short},
                },
                "goals": {"home": 1, "away": 0},
                "score": {"fulltime": {"home": 1, "away": 0}},
            }

        def fetch_fixture_events(self, fixture_id: int) -> list[dict]:
            return [_goal(70)]

    summary = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), limit=10, now_ts=1_000.0
    )

    assert summary["resolved_fixtures"] + summary["void_fixtures"] == 1
    assert nanotest.load_joined_observation_history()[0]["outcome"]["status"] == "pending"
    assert pending_updates == [(1, "2026-09-13T07:30:00Z")]


def test_reconciler_keeps_late_observation_pending_in_health(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_store(monkeypatch, tmp_path)
    monkeypatch.setattr(nanotest, "ENABLE_DECISION_SNAPSHOTS", False)
    monkeypatch.setattr(nanotest, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(nanotest, "ENABLE_2H_COLLECTION", False)
    monkeypatch.setattr(nanotest, "ENABLE_OUTCOME_CORRECTION_RECHECK", False)
    pending_updates: list[tuple[int, str | None]] = []

    class Monitor:
        def note_pending(self, count, oldest):
            pending_updates.append((count, oldest))

    monkeypatch.setattr(nanotest, "_research_health_monitor", Monitor())
    first = nanotest.build_prefilter_observation(
        fixture_id=208,
        minute=46,
        reason="candidate",
        raw_fixture={
            "fixture": {"id": 208, "status": {"short": "2H"}},
            "goals": {"home": 0, "away": 0},
        },
    )
    first["created_at_utc"] = "2026-09-13T07:30:00+00:00"
    assert nanotest.append_observation_history(first) is True

    late = nanotest.build_prefilter_observation(
        fixture_id=208,
        minute=47,
        reason="late_candidate",
        raw_fixture={
            "fixture": {"id": 208, "status": {"short": "2H"}},
            "goals": {"home": 0, "away": 0},
        },
    )
    late["created_at_utc"] = "2026-09-13T07:31:00+00:00"

    class Client:
        def fetch_fixture(self, fixture_id: int) -> dict:
            return {
                "fixture": {"id": fixture_id, "status": {"short": "FT"}},
                "goals": {"home": 1, "away": 0},
                "score": {"fulltime": {"home": 1, "away": 0}},
            }

        def fetch_fixture_events(self, fixture_id: int) -> list[dict]:
            # This arrives after the reconciler's preload but before its
            # outcome writes, modelling a delayed rolling/background append.
            assert nanotest.append_observation_history(late) is True
            return [_goal(70)]

    summary = nanotest.reconcile_pending_decision_outcomes(
        Client(), set(), limit=10, now_ts=1_000.0
    )

    assert summary["resolved_fixtures"] == 1
    grouped = nanotest.pending_observations_by_fixture({208})
    assert [row["observation_id"] for row in grouped[208]] == [
        late["observation_id"]
    ]
    assert pending_updates[0] == (1, "2026-09-13T07:30:00Z")
    assert pending_updates[-1] == (1, "2026-09-13T07:31:00Z")


def test_reschedule_detector_ignores_small_kickoff_shift(monkeypatch) -> None:
    monkeypatch.setattr(
        nanotest, "DECISION_OUTCOME_RESCHEDULE_MIN_SHIFT_SECONDS", 21_600
    )
    reason = nanotest._rescheduled_pending_fixture_void_reason(
        {
            "fixture": {
                "date": "2026-08-06T06:00:00+00:00",
                "status": {"short": "NS"},
            }
        },
        [
            {
                "created_at_utc": "2026-08-06T01:36:13+00:00",
                "minute": 45,
            }
        ],
    )

    assert reason is None


def test_default_schema_versions_are_migrated() -> None:
    assert nanotest.DECISION_SNAPSHOT_SCHEMA_VERSION >= 3
    assert nanotest.DECISION_OUTCOME_SCHEMA_VERSION >= 4
