from __future__ import annotations

import copy
import threading
import time
from collections import deque
from pathlib import Path

import pytest

import NanoTest as bot
from market_benchmark import AppendOnlyMarketJournal, iter_market_records
from scripts.report_market_benchmark import build_report


CAPTURED = "2026-09-08T12:00:00+00:00"


def _live_payload(
    *,
    fixture_id: int = 123,
    minute: int = 50,
    score: tuple[int, int] = (1, 1),
    update: str = "2026-09-08T11:59:50+00:00",
) -> dict:
    line = f"{score[0] + score[1] + 0.5:.1f}"
    return {
        "errors": [],
        "response": [
            {
                "fixture": {
                    "id": fixture_id,
                    "status": {"elapsed": minute, "long": "Second Half"},
                },
                "league": {"id": 39, "season": 2026},
                "teams": {
                    "home": {"id": 1, "goals": score[0]},
                    "away": {"id": 2, "goals": score[1]},
                },
                "status": {
                    "stopped": False,
                    "blocked": False,
                    "finished": False,
                },
                "update": update,
                "odds": [
                    {
                        "id": 25,
                        "name": "Match Goals",
                        "values": [
                            {
                                "value": "Over",
                                "odd": "1.50",
                                "handicap": line,
                                "main": True,
                                "suspended": False,
                            },
                            {
                                "value": "Under",
                                "odd": "2.70",
                                "handicap": line,
                                "main": True,
                                "suspended": False,
                            },
                        ],
                    }
                ],
            }
        ],
    }


def _observation() -> dict:
    return {
        "record_type": "observation",
        "observation_id": "123:50:WINDOW_1:ALLOW:v3",
        "observation_key": "123:50:WINDOW_1:ALLOW:v3",
        "fixture_id": 123,
        "created_at_utc": CAPTURED,
        "stage": "decision_pipeline",
        "minute": 50,
        "match": {
            "home_team_name": "Home",
            "away_team_name": "Away",
            "league_name": "League",
            "score_home": 1,
            "score_away": 1,
        },
        "probabilities": {"prob_to90": 81.25},
    }


def _prediction(*, rolling: bool) -> dict:
    observation = _observation()
    return {
        "model_id": "rolling-model" if rolling else "static-model",
        "model_status": "collecting",
        "model_created_at_utc": "2026-09-08T10:00:00+00:00",
        "model_data_cutoff_utc": "2026-09-08T09:00:00+00:00",
        "created_at_utc": "2026-09-08T12:00:00.500000+00:00",
        "prediction_status": "ok",
        "observation_id": observation["observation_id"],
        "fixture_id": observation["fixture_id"],
        "minute": observation["minute"],
        "prediction_input_fingerprint": (
            bot.rolling_prediction_input_fingerprint(observation)
            if rolling
            else bot.prediction_input_fingerprint(observation)
        ),
        "predictions": {
            "to90": {
                "status": "ok",
                "calibrated_probability_pct": 78.0 if rolling else 79.0,
            }
        },
    }


def _evidence() -> dict:
    return {
        "decision_created_at_utc": "2026-09-08T12:00:01+00:00",
        "static_prediction": _prediction(rolling=False),
        "rolling_prediction": _prediction(rolling=True),
    }


@pytest.fixture
def market_runtime(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    journal_path = tmp_path / "market.jsonl"
    monkeypatch.setattr(bot, "ENABLE_MARKET_BENCHMARK", True)
    monkeypatch.setattr(bot, "MARKET_BENCHMARK_JOURNAL_FILE", str(journal_path))
    monkeypatch.setattr(
        bot,
        "MARKET_BENCHMARK_INDEX_FILE",
        str(journal_path) + ".index.sqlite3",
    )
    monkeypatch.setattr(bot, "_market_benchmark_journal", None)
    monkeypatch.setattr(bot, "_market_benchmark_journal_signature", ())
    monkeypatch.setattr(bot, "_market_benchmark_started", True)
    monkeypatch.setattr(bot, "_market_benchmark_queue", deque())
    monkeypatch.setattr(bot, "_market_benchmark_poll_buffer", deque())
    monkeypatch.setattr(bot, "_market_benchmark_quote_cache", {})
    monkeypatch.setattr(bot, "_market_benchmark_dropped_records", 0)
    monkeypatch.setattr(bot, "_market_benchmark_dropped_by_reason", bot.Counter())
    monkeypatch.setattr(bot, "_market_benchmark_dropped_polls", 0)
    monkeypatch.setattr(bot, "_market_benchmark_thread", None)
    monkeypatch.setattr(bot, "_market_benchmark_poll_thread", None)
    monkeypatch.setattr(bot, "_market_prediction_handoff", {})
    monkeypatch.setattr(bot, "_market_prediction_handoff_order", deque())
    bot._market_benchmark_stop.clear()
    bot._market_benchmark_wakeup.clear()
    yield journal_path
    bot.stop_market_benchmark_daemon(timeout=1.0)


def test_poll_persists_snapshot_and_populates_bounded_cache(
    market_runtime: Path,
) -> None:
    class Client:
        @staticmethod
        def fetch_live_odds_payload():
            return _live_payload()

    journal = bot._get_market_benchmark_journal()
    result = bot.collect_market_benchmark_once(
        Client(), journal=journal, captured_at_utc=CAPTURED
    )
    records = list(iter_market_records(market_runtime))

    assert result["accepted_quotes"] == 1
    assert result["written_quotes"] == 1
    assert result["cache_fixtures"] == 1
    snapshot = next(
        record
        for record in records
        if record["record_type"] == "market_odds_snapshot"
    )
    assert snapshot["market"]["market_type"] == "one_more_goal_to90"
    assert snapshot["market"]["target_line"] == 2.5
    assert snapshot["market"]["fair_probability_goal_to90"] == pytest.approx(
        (1 / 1.5) / ((1 / 1.5) + (1 / 2.7))
    )


def test_live_feed_is_uncached_and_unfiltered_for_market_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = bot.APISportsMetricsClient(api_key="test-key")
    calls: list[tuple[str, object, bool]] = []

    def fake_get(path, params=None, cache=False, **kwargs):
        calls.append((path, params, cache))
        return {"response": []}

    monkeypatch.setattr(client, "_get", fake_get)

    assert client.fetch_live_odds_payload() == {"response": []}
    assert calls == [("odds/live", None, False)]


def test_decision_and_outcome_form_a_leakage_safe_four_source_cohort(
    market_runtime: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Client:
        @staticmethod
        def fetch_live_odds_payload():
            return _live_payload()

    journal = bot._get_market_benchmark_journal()
    bot.collect_market_benchmark_once(
        Client(), journal=journal, captured_at_utc="2026-09-08T11:59:55+00:00"
    )
    source = _observation()
    before = copy.deepcopy(source)
    monkeypatch.setattr(
        bot,
        "_build_shadow_candidate_prediction_record",
        lambda observation, *, rolling: _prediction(rolling=rolling),
    )
    monkeypatch.setattr(
        bot, "_utc_now_iso", lambda: "2026-09-08T12:00:01+00:00"
    )

    assert bot.capture_market_benchmark_decision(
        source, prepared_evidence=_evidence()
    )
    assert source == before
    assert bot.append_market_benchmark_outcomes(
        [
            {
                "record_type": "observation_outcome",
                "observation_id": source["observation_id"],
                "fixture_id": 123,
                "outcome_schema_version": 1,
                "created_at_utc": "2026-09-08T14:00:00+00:00",
                "outcome": {
                    "status": "resolved",
                    "outcome_scope": "TO_90_NORMAL_TIME",
                    "goal_to90_normal_time": True,
                    "resolved_at_utc": "2026-09-08T14:00:00+00:00",
                },
            }
        ]
    ) == 1
    while bot._drain_market_benchmark_queue(journal):
        pass

    report = build_report(market_runtime)

    assert report["coverage"]["decision_records"] == 1
    assert report["coverage"]["all_sources_aligned_decisions"] == 1
    cohort = report["metrics"]["all_sources_same_cohort"]["sources"]
    assert cohort["bot"]["average_probability"] == pytest.approx(0.8125)
    assert cohort["static_ml"]["average_probability"] == pytest.approx(0.79)
    assert cohort["rolling_ml"]["average_probability"] == pytest.approx(0.78)
    assert cohort["market"]["rows"] == 1


def test_pre_send_decision_and_delivery_are_durable_separate_records(
    market_runtime: Path,
) -> None:
    observation = _observation()
    evidence = _evidence()

    assert bot.capture_market_benchmark_decision(
        observation,
        prepared_evidence=evidence,
        durable=True,
    )
    assert list(bot._market_benchmark_queue) == []
    first_records = list(iter_market_records(market_runtime))
    assert [record["record_type"] for record in first_records] == [
        "market_benchmark_decision"
    ]

    assert bot.append_market_benchmark_delivery(
        observation,
        {
            "send_attempted": True,
            "send_ok": True,
            "message_id": 77,
            "send_started_at_utc": "2026-09-08T12:00:02+00:00",
            "send_finished_at_utc": "2026-09-08T12:00:03+00:00",
        },
        durable=True,
    )
    records = list(iter_market_records(market_runtime))
    assert [record["record_type"] for record in records] == [
        "market_benchmark_decision",
        "market_benchmark_delivery",
    ]


def test_prediction_from_router_id_is_accepted_only_for_identical_input(
    market_runtime: Path,
) -> None:
    canonical = _observation()
    preview = copy.deepcopy(canonical)
    preview["observation_id"] = "123:50:WIDE_ROUTER:v3"
    preview["observation_key"] = preview["observation_id"]
    evidence = {
        "decision_created_at_utc": "2026-09-08T12:00:01+00:00",
        "static_prediction": {
            **_prediction(rolling=False),
            "observation_id": preview["observation_id"],
            "prediction_input_fingerprint": (
                bot.prediction_input_fingerprint(preview)
            ),
        },
        "rolling_prediction": {
            **_prediction(rolling=True),
            "observation_id": preview["observation_id"],
            "prediction_input_fingerprint": (
                bot.rolling_prediction_input_fingerprint(preview)
            ),
        },
    }

    assert bot.capture_market_benchmark_decision(
        canonical,
        prepared_evidence=evidence,
    )
    decision = bot._market_benchmark_queue[-1]
    assert decision["probabilities"]["static_ml"]["status"] == "available"
    assert decision["probabilities"]["rolling_ml"]["status"] == "available"

    bot._market_benchmark_queue.clear()
    mismatched = copy.deepcopy(evidence)
    mismatched["static_prediction"]["prediction_input_fingerprint"] = "bad"
    assert bot.capture_market_benchmark_decision(
        canonical,
        prepared_evidence=mismatched,
    )
    decision = bot._market_benchmark_queue[-1]
    assert decision["probabilities"]["static_ml"]["status"] == "unavailable"
    assert decision["probabilities"]["static_ml"]["reason"] == (
        "prediction_input_fingerprint_mismatch"
    )


def test_real_router_and_canonical_builders_have_same_predictive_input(
    market_runtime: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bot, "ENABLE_ROLLING_DYNAMICS_SHADOW", False)
    fixture = {
        "fixture_id": 123,
        "score_home": 1,
        "score_away": 1,
        "home_team_id": 1,
        "home_team_name": "Home",
        "away_team_id": 2,
        "away_team_name": "Away",
        "league_id": 39,
        "league_name": "League",
        "league_country": "Country",
    }
    probability = {
        "prob_next_15": 55.0,
        "prob_to90": 81.0,
        "reputation_adjusted_prob_next_15": 55.0,
        "reputation_adjusted_prob_to90": 81.0,
        "pressure_components": {},
        "channel_signal_filter": {
            "passed": True,
            "version": bot.CHANNEL_SIGNAL_FILTER_VERSION,
        },
        "wide_research_router": {"applied": False, "allow": True},
    }
    preview = bot.build_wide_router_observation(
        fixture_id=123,
        minute=50,
        fixture_metrics=fixture,
        probability_result=probability,
    )
    snapshot = bot.build_decision_snapshot(
        fixture_id=123,
        minute=50,
        window_name="WINDOW_1",
        match_identity=bot.build_match_identity_context(fixture),
        score_home=1,
        score_away=1,
        probability_result=probability,
        threshold_result={"threshold": 75.0, "fallback_threshold": 75.0},
        threshold_next15=0.0,
        readiness_result={"passed": True},
        live_gate_result={"required": False, "passed": True},
        anti_garbage_passed=True,
        final_decision="ALLOW",
        block_reason=None,
        factor_context=probability,
        created_at_utc=preview["created_at_utc"],
        decision_created_at_utc=preview["created_at_utc"],
    )
    canonical = bot.build_observation_from_decision(
        snapshot,
        fixture,
        probability,
    )

    assert preview["observation_id"] != canonical["observation_id"]
    assert bot.prediction_input_fingerprint(preview) == (
        bot.prediction_input_fingerprint(canonical)
    )
    assert bot.rolling_prediction_input_fingerprint(preview) == (
        bot.rolling_prediction_input_fingerprint(canonical)
    )


def test_market_prepare_is_fail_open_on_unexpected_error(
    market_runtime: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        bot,
        "build_wide_router_observation",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    evidence = bot.prepare_market_benchmark_evidence(
        fixture_id=123,
        minute=50,
        fixture_metrics={},
        probability_result={},
    )

    assert evidence["quote_status"] == "prepare_error"
    assert evidence["quote_frozen"] is True


def test_future_quote_never_replaces_an_older_causal_quote(
    market_runtime: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = bot._get_market_benchmark_journal()

    class EarlierClient:
        @staticmethod
        def fetch_live_odds_payload():
            return _live_payload(update="2026-09-08T11:59:40+00:00")

    class FutureClient:
        @staticmethod
        def fetch_live_odds_payload():
            return _live_payload(update="2026-09-08T12:00:05+00:00")

    bot.collect_market_benchmark_once(
        EarlierClient(),
        journal=journal,
        captured_at_utc="2026-09-08T11:59:50+00:00",
    )
    bot.collect_market_benchmark_once(
        FutureClient(),
        journal=journal,
        captured_at_utc="2026-09-08T12:00:05+00:00",
    )
    monkeypatch.setattr(
        bot,
        "_build_shadow_candidate_prediction_record",
        lambda observation, *, rolling: _prediction(rolling=rolling),
    )
    monkeypatch.setattr(
        bot, "_utc_now_iso", lambda: "2026-09-08T12:00:01+00:00"
    )

    assert bot.capture_market_benchmark_decision(
        _observation(), prepared_evidence=_evidence()
    )
    decision = bot._market_benchmark_queue[-1]

    assert decision["market"]["status"] == "available"
    assert decision["market"]["captured_at_utc"] == (
        "2026-09-08T11:59:50.000000Z"
    )


def test_prepared_quote_stays_frozen_when_cache_changes_during_send(
    market_runtime: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = bot._get_market_benchmark_journal()

    class Client:
        @staticmethod
        def fetch_live_odds_payload():
            return _live_payload(update="2026-09-08T11:59:40+00:00")

    bot.collect_market_benchmark_once(
        Client(),
        journal=journal,
        captured_at_utc="2026-09-08T11:59:50+00:00",
    )
    frozen_quote = copy.deepcopy(bot._market_benchmark_quote_cache[123][-1])
    evidence = {
        **_evidence(),
        "quote_frozen": True,
        "quote": frozen_quote,
        "quote_status": "available",
    }
    bot._market_benchmark_quote_cache[123] = [
        {
            **frozen_quote,
            "record_key": "future-replacement",
            "captured_at_utc": "2026-09-08T12:00:10+00:00",
            "provider_update_utc": "2026-09-08T12:00:10+00:00",
        }
    ]
    monkeypatch.setattr(
        bot, "_utc_now_iso", lambda: "2026-09-08T12:00:11+00:00"
    )

    assert bot.capture_market_benchmark_decision(
        _observation(), prepared_evidence=evidence
    )

    decision = bot._market_benchmark_queue[-1]
    assert decision["market"]["quote_record_key"] == frozen_quote["record_key"]
    assert decision["decision_created_at_utc"] == (
        "2026-09-08T12:00:01+00:00"
    )


def test_score_mismatch_is_frozen_as_unavailable_not_retried(
    market_runtime: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = bot._get_market_benchmark_journal()

    class Client:
        @staticmethod
        def fetch_live_odds_payload():
            return _live_payload(score=(0, 1))

    bot.collect_market_benchmark_once(
        Client(), journal=journal, captured_at_utc="2026-09-08T11:59:55+00:00"
    )
    monkeypatch.setattr(
        bot,
        "_build_shadow_candidate_prediction_record",
        lambda observation, *, rolling: _prediction(rolling=rolling),
    )
    monkeypatch.setattr(
        bot, "_utc_now_iso", lambda: "2026-09-08T12:00:01+00:00"
    )

    assert bot.capture_market_benchmark_decision(
        _observation(), prepared_evidence=_evidence()
    )

    decision = bot._market_benchmark_queue[-1]
    assert decision["market"]["status"] == "quote_score_mismatch"
    assert decision["market"]["quote_record_key"] is None


def test_market_capture_reuses_exact_prediction_handoff_without_inference(
    market_runtime: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_id = _observation()["observation_id"]
    handoff_records = [
        _prediction(rolling=False),
        _prediction(rolling=True),
    ]
    bot._market_prediction_handoff.clear()
    bot._market_prediction_handoff_order.clear()
    for rolling, record in zip((False, True), handoff_records):
        record["observation_id"] = canonical_id
        bot._remember_market_prediction_handoff(record, rolling=rolling)
    monkeypatch.setattr(
        bot,
        "_build_shadow_candidate_prediction_record",
        lambda *args, **kwargs: pytest.fail("market capture recomputed ML"),
    )

    assert bot.capture_market_benchmark_decision(_observation())
    decision = bot._market_benchmark_queue[-1]

    assert decision["probabilities"]["static_ml"]["probability_pct"] == 79.0
    assert decision["probabilities"]["rolling_ml"]["probability_pct"] == 78.0
    assert canonical_id not in bot._market_prediction_handoff


def test_output_path_collision_fails_closed(
    market_runtime: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        bot, "MARKET_BENCHMARK_JOURNAL_FILE", bot.OBSERVATION_HISTORY_FILE
    )

    with pytest.raises(ValueError, match="overlaps a live source"):
        bot._assert_market_benchmark_runtime_paths()


def test_queue_overflow_drops_shadow_record_without_mutating_source(
    market_runtime: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _observation()
    before = copy.deepcopy(source)
    monkeypatch.setattr(bot, "MARKET_BENCHMARK_QUEUE_MAX", 1)
    bot._market_benchmark_queue.append(
        {
            "record_type": "already_pending",
            "shadow_only": True,
            "production_applied": False,
        }
    )
    monkeypatch.setattr(
        bot,
        "_build_shadow_candidate_prediction_record",
        lambda observation, *, rolling: _prediction(rolling=rolling),
    )

    assert not bot.capture_market_benchmark_decision(
        source, prepared_evidence=_evidence()
    )
    assert source == before
    assert bot._market_benchmark_dropped_records == 1


def _queue_record(record_type: str, key: str) -> dict:
    return {
        "record_type": record_type,
        "record_key": key,
        "fixture_id": 123,
        "captured_at_utc": CAPTURED,
        "shadow_only": True,
        "production_applied": False,
    }


def test_outcome_reserve_and_hard_full_eviction(
    market_runtime: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bot, "MARKET_BENCHMARK_QUEUE_MAX", 4)
    monkeypatch.setattr(bot, "MARKET_BENCHMARK_OUTCOME_QUEUE_RESERVE", 2)

    assert bot._enqueue_market_benchmark_record(
        _queue_record("market_benchmark_decision", "d1")
    )
    assert bot._enqueue_market_benchmark_record(
        _queue_record("market_benchmark_decision", "d2")
    )
    assert not bot._enqueue_market_benchmark_record(
        _queue_record("market_benchmark_decision", "d3")
    )
    assert bot._enqueue_market_benchmark_record(
        _queue_record("market_benchmark_outcome", "o1")
    )
    assert bot._enqueue_market_benchmark_record(
        _queue_record("market_benchmark_outcome", "o2")
    )

    bot._market_benchmark_queue.clear()
    bot._market_benchmark_queue.extend(
        _queue_record("market_benchmark_decision", f"full-{index}")
        for index in range(4)
    )
    assert bot._enqueue_market_benchmark_record(
        _queue_record("market_benchmark_outcome", "priority-outcome")
    )
    queued_keys = {
        record["record_key"] for record in bot._market_benchmark_queue
    }
    assert "priority-outcome" in queued_keys
    assert len(bot._market_benchmark_queue) == 4
    assert bot._market_benchmark_dropped_by_reason[
        "evicted_for_outcome"
    ] == 1


def test_failed_write_requeue_preserves_concurrent_outcome(
    market_runtime: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bot, "MARKET_BENCHMARK_QUEUE_MAX", 4)
    monkeypatch.setattr(bot, "MARKET_BENCHMARK_WRITE_BATCH", 2)
    bot._market_benchmark_queue.extend(
        [
            _queue_record("market_benchmark_decision", "d1"),
            _queue_record("market_benchmark_outcome", "o1"),
        ]
    )

    class FailingJournal:
        @staticmethod
        def append_many(records):
            assert bot._enqueue_market_benchmark_record(
                _queue_record("market_benchmark_outcome", "o2")
            )
            raise OSError("temporary disk failure")

    assert bot._drain_market_benchmark_queue(FailingJournal()) == 0
    queued = list(bot._market_benchmark_queue)
    assert {record["record_key"] for record in queued} == {"d1", "o1", "o2"}
    assert [record["record_key"] for record in queued[:2]] == ["o1", "o2"]


def test_blocked_http_does_not_block_outcome_writer(
    market_runtime: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()

    class Client:
        @staticmethod
        def fetch_live_odds_payload():
            entered.set()
            release.wait(2.0)
            return _live_payload()

    monkeypatch.setattr(bot, "_market_benchmark_started", False)
    monkeypatch.setattr(bot, "MARKET_BENCHMARK_POLL_SECONDS", 60.0)
    bot.start_market_benchmark_daemon(Client())
    assert entered.wait(1.0)
    assert bot._enqueue_market_benchmark_record(
        _queue_record("market_benchmark_outcome", "outcome-while-http-blocked")
    )
    deadline = time.monotonic() + 1.5
    written = False
    while time.monotonic() < deadline:
        written = any(
            record.get("record_key") == "outcome-while-http-blocked"
            for record in iter_market_records(market_runtime)
        )
        if written:
            break
        time.sleep(0.01)
    release.set()
    bot.stop_market_benchmark_daemon(timeout=2.0)

    assert written


def test_daemon_polls_immediately_and_stops_cleanly(
    market_runtime: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    class Client:
        @staticmethod
        def fetch_live_odds_payload():
            nonlocal calls
            calls += 1
            return _live_payload()

    monkeypatch.setattr(bot, "_market_benchmark_started", False)
    monkeypatch.setattr(bot, "MARKET_BENCHMARK_POLL_SECONDS", 0.05)

    bot.start_market_benchmark_daemon(Client())
    deadline = time.monotonic() + 2.0
    while calls == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    bot.stop_market_benchmark_daemon(timeout=2.0)

    assert calls >= 1
    assert bot._market_benchmark_thread is not None
    assert not bot._market_benchmark_thread.is_alive()
    assert bot._market_benchmark_started is False
    assert any(
        record.get("record_type") == "market_benchmark_poll"
        for record in iter_market_records(market_runtime)
    )
