from __future__ import annotations

from datetime import datetime, timezone

from outcome_revision import (
    next_outcome_revision,
    outcome_rank,
    outcome_store_version,
    outcomes_semantically_equal,
)

import NanoTest as bot


def test_semantic_comparison_ignores_delivery_metadata() -> None:
    first = {
        "status": "resolved",
        "goal_to90_normal_time": True,
        "resolved_at_utc": "2026-09-12T01:00:00+00:00",
        "outcome_revision": 1,
    }
    repeated = {
        **first,
        "resolved_at_utc": "2026-09-12T02:00:00+00:00",
        "outcome_revision": 99,
    }

    assert outcomes_semantically_equal(first, repeated) is True


def test_revision_is_only_incremented_for_a_real_correction() -> None:
    previous = {
        "outcome_schema_version": 4,
        "outcome_revision": 2,
        "created_at_utc": "2026-09-12T01:00:00+00:00",
        "outcome": {
            "status": "resolved",
            "goal_to90_normal_time": True,
        },
    }

    assert next_outcome_revision(
        previous,
        {
            "status": "resolved",
            "goal_to90_normal_time": True,
            "resolved_at_utc": "2026-09-12T02:00:00+00:00",
        },
        schema_version=4,
    ) is None
    assert next_outcome_revision(
        previous,
        {
            "status": "resolved",
            "goal_to90_normal_time": False,
            "resolved_at_utc": "2026-09-12T02:00:00+00:00",
        },
        schema_version=4,
    ) == 3


def test_revision_wins_even_if_correction_timestamp_is_older() -> None:
    older_revision = {
        "outcome_schema_version": 1,
        "outcome_revision": 1,
        "created_at_utc": "2026-09-12T03:00:00+00:00",
    }
    correction = {
        "outcome_schema_version": 1,
        "outcome_revision": 2,
        "created_at_utc": "2026-09-12T02:00:00+00:00",
    }

    assert outcome_rank(correction) > outcome_rank(older_revision)
    assert outcome_store_version(correction) > outcome_store_version(
        older_revision
    )


def test_legacy_store_version_remains_compatible() -> None:
    legacy = {"outcome_schema_version": 3}
    revised = {"outcome_schema_version": 3, "outcome_revision": 1}

    assert outcome_store_version(legacy) == 3
    assert outcome_store_version(revised) > outcome_store_version(legacy)


def test_observation_journal_keeps_and_selects_same_schema_correction(
    monkeypatch, tmp_path
) -> None:
    history = tmp_path / "observations.jsonl"
    monkeypatch.setattr(bot, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(bot, "OBSERVATION_HISTORY_FILE", str(history))
    monkeypatch.setattr(bot, "OBSERVATION_ROTATE_MAX_BYTES", 0)
    bot._observation_history_keys = None
    observation_id = "77:46:TEST:v1"
    observation = {
        "record_type": "observation",
        "observation_id": observation_id,
        "observation_key": observation_id,
        "fixture_id": 77,
        "schema_version": 1,
        "created_at_utc": "2026-09-12T00:00:00+00:00",
        "outcome": {"status": "pending"},
    }
    first = {
        "record_type": "observation_outcome",
        "observation_id": observation_id,
        "observation_key": observation_id,
        "fixture_id": 77,
        "outcome_schema_version": 1,
        "outcome_revision": 1,
        "created_at_utc": "2026-09-12T02:00:00+00:00",
        "outcome": {
            "status": "resolved",
            "goal_to90_normal_time": True,
        },
    }
    correction = {
        **first,
        "outcome_revision": 2,
        # Revision order, not clock order, is authoritative.
        "created_at_utc": "2026-09-12T01:00:00+00:00",
        "outcome": {
            "status": "resolved",
            "goal_to90_normal_time": False,
        },
    }

    assert bot.append_observation_history(observation) is True
    assert bot.append_observation_history(first) is True
    assert bot.append_observation_history(correction) is True
    assert bot.append_observation_history(correction) is False

    joined = bot.load_joined_observation_history(str(history))
    assert len(joined) == 1
    assert joined[0]["outcome_revision"] == 2
    assert joined[0]["outcome"]["goal_to90_normal_time"] is False


def test_resolver_is_idempotent_and_appends_a_real_label_correction(
    monkeypatch, tmp_path
) -> None:
    history = tmp_path / "observations.jsonl"
    monkeypatch.setattr(bot, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(bot, "OBSERVATION_HISTORY_FILE", str(history))
    monkeypatch.setattr(bot, "OBSERVATION_ROTATE_MAX_BYTES", 0)
    monkeypatch.setattr(bot, "append_shadow_candidate_outcomes", lambda rows: 0)
    monkeypatch.setattr(bot, "append_wide_research_outcomes", lambda rows: 0)
    monkeypatch.setattr(bot, "append_market_benchmark_outcomes", lambda rows: 0)
    bot._observation_history_keys = None
    bot.state["score_timelines"] = {}
    observation_id = "88:46:TEST:v1"
    observation = {
        "record_type": "observation",
        "observation_id": observation_id,
        "observation_key": observation_id,
        "fixture_id": 88,
        "schema_version": 1,
        "created_at_utc": "2026-09-12T00:00:00+00:00",
        "stage": "prefilter",
        "minute": 46,
        "match": {"score_home": 0, "score_away": 0},
        "outcome": {"status": "pending"},
    }
    assert bot.append_observation_history(observation) is True
    goal = {
        "time": {"elapsed": 70, "extra": 0},
        "type": "Goal",
        "detail": "Normal Goal",
    }

    assert bot.resolve_observation_history_outcomes(
        88,
        [goal],
        {"status_short": "FT"},
        (1, 0),
        "2026-09-12T01:00:00+00:00",
        observations=[observation],
    ) == 1
    latest = bot.load_joined_observation_history(str(history))[0]
    assert latest["outcome_revision"] == 1
    assert bot.resolve_observation_history_outcomes(
        88,
        [goal],
        {"status_short": "FT"},
        (1, 0),
        "2026-09-12T02:00:00+00:00",
        observations=[latest],
    ) == 0

    assert bot.resolve_observation_history_outcomes(
        88,
        [],
        {"status_short": "FT"},
        (0, 0),
        "2026-09-12T03:00:00+00:00",
        observations=[latest],
    ) == 1
    corrected = bot.load_joined_observation_history(str(history))[0]
    assert corrected["outcome_revision"] == 2
    assert corrected["outcome"]["goal_to90_normal_time"] is False


def test_reconciler_rechecks_recent_terminal_observations(
    monkeypatch, tmp_path
) -> None:
    history = tmp_path / "observations.jsonl"
    monkeypatch.setattr(bot, "ENABLE_DECISION_SNAPSHOTS", False)
    monkeypatch.setattr(bot, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(bot, "ENABLE_2H_COLLECTION", False)
    monkeypatch.setattr(bot, "ENABLE_OUTCOME_CORRECTION_RECHECK", True)
    monkeypatch.setattr(bot, "OUTCOME_CORRECTION_RECHECK_SECONDS", 300)
    monkeypatch.setattr(bot, "OUTCOME_CORRECTION_LOOKBACK_HOURS", 24)
    monkeypatch.setattr(bot, "OUTCOME_CORRECTION_RECHECK_LIMIT", 5)
    monkeypatch.setattr(bot, "OBSERVATION_HISTORY_FILE", str(history))
    monkeypatch.setattr(bot, "OBSERVATION_ROTATE_MAX_BYTES", 0)
    monkeypatch.setattr(bot, "append_shadow_candidate_outcomes", lambda rows: 0)
    monkeypatch.setattr(bot, "append_wide_research_outcomes", lambda rows: 0)
    monkeypatch.setattr(bot, "append_market_benchmark_outcomes", lambda rows: 0)
    bot._observation_history_keys = None
    bot._decision_outcome_last_checked.clear()
    bot._outcome_correction_last_checked.clear()
    bot._outcome_correction_next_sweep_ts = 0.0
    bot.state["score_timelines"] = {}
    observation_id = "99:46:TEST:v1"
    observation = {
        "record_type": "observation",
        "observation_id": observation_id,
        "observation_key": observation_id,
        "fixture_id": 99,
        "schema_version": 1,
        "created_at_utc": "2026-09-12T00:00:00+00:00",
        "stage": "prefilter",
        "minute": 46,
        "match": {"score_home": 0, "score_away": 0},
        "outcome": {"status": "pending"},
    }
    original = {
        "record_type": "observation_outcome",
        "observation_id": observation_id,
        "observation_key": observation_id,
        "fixture_id": 99,
        "outcome_schema_version": 1,
        "outcome_revision": 1,
        "created_at_utc": "2026-09-12T01:00:00+00:00",
        "outcome": {
            "status": "resolved",
            "outcome_scope": "TO_90_NORMAL_TIME",
            "goal_to90_normal_time": True,
            "goal_to90": True,
            "normal_time_result": "WIN",
        },
    }
    assert bot.append_observation_history(observation) is True
    assert bot.append_observation_history(original) is True

    class Client:
        def fetch_fixture(self, fixture_id: int):
            assert fixture_id == 99
            return {
                "fixture": {"id": 99, "status": {"short": "FT"}},
                "goals": {"home": 0, "away": 0},
                "score": {"fulltime": {"home": 0, "away": 0}},
            }

        def fetch_fixture_events(self, fixture_id: int):
            return []

    now_ts = datetime(
        2026, 9, 12, 2, 0, tzinfo=timezone.utc
    ).timestamp()
    summary = bot.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=now_ts
    )

    assert summary["checked"] == 1
    latest = bot.load_joined_observation_history(str(history))[0]
    assert latest["outcome_revision"] == 2
    assert latest["outcome"]["goal_to90_normal_time"] is False


def test_correction_recheck_defers_when_event_evidence_is_unavailable(
    monkeypatch, tmp_path
) -> None:
    history = tmp_path / "observations.jsonl"
    monkeypatch.setattr(bot, "ENABLE_DECISION_SNAPSHOTS", False)
    monkeypatch.setattr(bot, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(bot, "ENABLE_2H_COLLECTION", False)
    monkeypatch.setattr(bot, "ENABLE_OUTCOME_CORRECTION_RECHECK", True)
    monkeypatch.setattr(bot, "OUTCOME_CORRECTION_RECHECK_SECONDS", 300)
    monkeypatch.setattr(bot, "OUTCOME_CORRECTION_LOOKBACK_HOURS", 24)
    monkeypatch.setattr(bot, "OBSERVATION_HISTORY_FILE", str(history))
    monkeypatch.setattr(bot, "OBSERVATION_ROTATE_MAX_BYTES", 0)
    bot._observation_history_keys = None
    bot._decision_outcome_last_checked.clear()
    bot._outcome_correction_last_checked.clear()
    bot._outcome_correction_next_sweep_ts = 0.0
    observation_id = "199:46:TEST:v1"
    observation = {
        "record_type": "observation", "observation_id": observation_id,
        "fixture_id": 199, "schema_version": 1,
        "created_at_utc": "2026-09-12T00:00:00+00:00",
        "stage": "prefilter", "minute": 46,
        "match": {"score_home": 0, "score_away": 0},
    }
    original = {
        "record_type": "observation_outcome", "observation_id": observation_id,
        "fixture_id": 199, "outcome_schema_version": 1,
        "outcome_revision": 1,
        "created_at_utc": "2026-09-12T01:00:00+00:00",
        "outcome": {"status": "resolved", "goal_to90_normal_time": True},
    }
    assert bot.append_observation_history(observation)
    assert bot.append_observation_history(original)

    class Client:
        def fetch_fixture(self, fixture_id):
            return {
                "fixture": {"id": fixture_id, "status": {"short": "FT"}},
                "score": {"fulltime": {"home": 0, "away": 0}},
            }

        def fetch_fixture_events_response(self, fixture_id):
            return None

    summary = bot.reconcile_pending_decision_outcomes(
        Client(), set(), now_ts=datetime(2026, 9, 12, 2, tzinfo=timezone.utc).timestamp()
    )
    assert summary["checked"] == 1
    latest = bot.load_joined_observation_history(str(history))[0]
    assert latest["outcome_revision"] == 1
    assert latest["outcome"]["goal_to90_normal_time"] is True


def test_events_response_with_provider_error_is_not_empty_evidence(
    monkeypatch,
) -> None:
    client = bot.APISportsMetricsClient.__new__(bot.APISportsMetricsClient)
    monkeypatch.setattr(
        client,
        "_get",
        lambda *args, **kwargs: {
            "errors": {"rateLimit": "Too many requests"},
            "response": [],
        },
    )

    assert client.fetch_fixture_events_response(123) is None
    monkeypatch.setattr(
        client,
        "_get",
        lambda *args, **kwargs: {"errors": [], "response": []},
    )
    assert client.fetch_fixture_events_response(123) == []
