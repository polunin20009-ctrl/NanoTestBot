from __future__ import annotations

import NanoTest as nanotest


def test_score_growth_is_authoritative_when_events_are_missing() -> None:
    assert nanotest.determine_normal_time_goal_result(
        1, 0, 4, 0, goal_minutes=[], normal_time_result="LOSS"
    ) is True


def test_unchanged_score_without_event_evidence_is_loss() -> None:
    assert nanotest.determine_normal_time_goal_result(
        1, 0, 1, 0, goal_minutes=[], normal_time_result="LOSS"
    ) is False


def test_event_evidence_handles_score_correction_edge_case() -> None:
    assert nanotest.determine_normal_time_goal_result(
        1, 0, 1, 0, goal_minutes=[72], normal_time_result=None
    ) is True


def test_training_outcome_uses_score_delta_when_event_feed_is_empty(monkeypatch) -> None:
    saved = []
    record = {
        "signal_id": "fixture-1-signal",
        "fixture_id": 1,
        "signal_minute": 61,
        "signal_score_home": 1,
        "signal_score_away": 0,
    }

    monkeypatch.setattr(
        nanotest,
        "_get_training_signal_records_for_fixture",
        lambda fixture_id: [record],
    )
    monkeypatch.setattr(nanotest, "save_outcome", lambda outcome: saved.append(outcome) or True)
    monkeypatch.setattr(nanotest, "_mark_training_signal_outcome_saved", lambda signal_id: None)
    monkeypatch.setattr(nanotest, "get_score_timeline", lambda fixture_id: [])

    written = nanotest.process_normal_time_outcomes_for_jsonl(
        1,
        events=[],
        fixture_context={"status_short": "FT"},
        normal_time_score=(4, 0),
        resolved_at_utc="2026-07-21T12:00:00+00:00",
    )

    assert written == 1
    assert saved[0]["normal_time_result"] == "WIN"
    assert saved[0]["goal_to90_normal_time"] is True
    assert saved[0]["normal_time_goal_count_after_signal"] == 3
    assert saved[0]["goal_result_source"] == "score_delta"
    assert saved[0]["goal_in_next_15_normal_time"] is None
    assert saved[0]["goal_in_next_15_source"] == "unknown_timing"
    assert saved[0]["first_goal_after_signal_minute"] is None


def test_regular_final_renderer_keeps_goal_minutes_and_short_score_format() -> None:
    message = nanotest.render_final_message(
        {
            "data": {"fixture": {}},
            "signal_home": 1,
            "signal_away": 0,
            "final_home": 4,
            "final_away": 0,
            "goal_minutes": [68, 82],
            "normal_time_result": "LOSS",
        },
        header_text="🚨 Сигнал",
    )

    assert "✅ Финальный счёт: 4-0" in message
    assert "⚽️Голы: 68′ 82′" in message
    assert "Счёт после основного времени" not in message
    assert "Дополнительное время и серия пенальти не учитываются." not in message


def test_extra_time_final_renderer_shows_normal_time_score_disclaimer() -> None:
    message = nanotest.render_final_message(
        {
            "data": {"fixture": {}},
            "signal_home": 1,
            "signal_away": 0,
            "final_home": 4,
            "final_away": 0,
            "goal_minutes": [68, 82],
            "normal_time_result": "WIN",
            "went_to_extra_time": True,
        },
        header_text="🚨 Сигнал",
    )

    assert "✅ Счёт после основного времени: 4 - 0" in message
    assert "⚽️Голы: 68′ 82′" in message
    assert "Дополнительное время и серия пенальти не учитываются." in message
    assert "Финальный счёт:" not in message
    assert "Компенсированное время включено." not in message
