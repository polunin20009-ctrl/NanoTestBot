from __future__ import annotations

import NanoTest as nanotest


def _live_data(minute: int) -> dict:
    return {
        "fixture": {
            "team_home_name": {"value": "Home"},
            "team_away_name": {"value": "Away"},
            "league_name": {"value": "League"},
            "league_country": {"value": "Country"},
            "elapsed": {"value": minute},
            "status": {"value": {"short": "2H", "elapsed": minute}},
            "score_home": {"value": 1},
            "score_away": {"value": 0},
        }
    }


def test_live_message_before_75_shows_both_new_probability_labels() -> None:
    message = nanotest.render_live_message(
        _live_data(74),
        signal_score=(1, 0),
        prob_display=44.0,
        prob_display_90=80.0,
    )

    assert "Гол в ближайшие 15 минут: 44%" in message
    assert "Гол до конца основного времени: 80%" in message
    assert "Вероятность гола до 75 минуты" not in message
    assert "Вероятность гола до 90 минуты" not in message


def test_live_message_from_75_hides_next_15_probability() -> None:
    message = nanotest.render_live_message(
        _live_data(75),
        signal_score=(1, 0),
        prob_display=44.0,
        prob_display_90=31.0,
    )

    assert "Гол в ближайшие 15 минут" not in message
    assert "Гол до конца основного времени: 31%" in message


def test_signal_header_from_75_uses_only_end_of_normal_time() -> None:
    snapshot = nanotest._build_signal_snapshot_data(
        _live_data(75),
        prob_display=44.0,
        signal_score=(1, 0),
        signal_minute=75,
        prob_display_90=31.0,
    )

    message = nanotest.build_signal_header(snapshot)

    assert "Гол в ближайшие 15 минут" not in message
    assert "Гол до конца основного времени: 31%" in message


def test_channel_signal_filter_includes_exact_boundaries() -> None:
    result = nanotest.evaluate_channel_signal_filter(
        75.0,
        {"value": 2},
        {"value": 1},
        reputation_base_prob_to90=73.5,
        reputation_adjusted_prob_to90=75.0,
        adjusted_intensity=0.55,
        season_context_factor=1.02,
    )

    assert result["passed"] is True
    assert result["reason"] == "pass"
    assert result["goals_at_snapshot"] == 3
    assert result["reputation_delta_to90_pp"] == 1.5
    assert result["adjusted_intensity"] == 0.55
    assert result["season_context_factor"] == 1.02


def test_channel_signal_filter_blocks_each_failed_condition() -> None:
    low_probability = nanotest.evaluate_channel_signal_filter(
        74.99, 1, 1,
        reputation_base_prob_to90=73.49,
        reputation_adjusted_prob_to90=74.99,
        adjusted_intensity=0.55,
        season_context_factor=1.02,
    )
    low_reputation_delta = nanotest.evaluate_channel_signal_filter(
        75.0, 1, 1,
        reputation_base_prob_to90=73.51,
        reputation_adjusted_prob_to90=75.0,
        adjusted_intensity=0.55,
        season_context_factor=1.02,
    )
    low_intensity = nanotest.evaluate_channel_signal_filter(
        75.0, 1, 1,
        reputation_base_prob_to90=73.5,
        reputation_adjusted_prob_to90=75.0,
        adjusted_intensity=0.549,
        season_context_factor=1.02,
    )
    low_season = nanotest.evaluate_channel_signal_filter(
        75.0, 1, 1,
        reputation_base_prob_to90=73.5,
        reputation_adjusted_prob_to90=75.0,
        adjusted_intensity=0.55,
        season_context_factor=1.019,
    )
    all_failed = nanotest.evaluate_channel_signal_filter(
        74.99, 1, 1,
        reputation_base_prob_to90=73.5,
        reputation_adjusted_prob_to90=74.99,
        adjusted_intensity=0.549,
        season_context_factor=1.019,
    )

    assert low_probability["passed"] is False
    assert low_probability["reason"] == "prob_to90"
    assert low_reputation_delta["passed"] is False
    assert low_reputation_delta["reason"] == "reputation_delta_to90_pp"
    assert low_intensity["passed"] is False
    assert low_intensity["reason"] == "adjusted_intensity"
    assert low_season["passed"] is False
    assert low_season["reason"] == "season_context_factor"
    assert all_failed["passed"] is False
    assert all_failed["reason"] == (
        "prob_to90+reputation_delta_to90_pp+adjusted_intensity+season_context_factor"
    )


def test_channel_signal_filter_is_fail_closed_for_invalid_input() -> None:
    assert nanotest.evaluate_channel_signal_filter(None, 0, 0)["passed"] is False
    assert nanotest.evaluate_channel_signal_filter(
        90.0, None, 0,
        reputation_base_prob_to90=88.0,
        reputation_adjusted_prob_to90=90.0,
        adjusted_intensity=0.7,
        season_context_factor=1.05,
    )["passed"] is False
    assert nanotest.evaluate_channel_signal_filter(
        90.0, -1, 0,
        reputation_base_prob_to90=88.0,
        reputation_adjusted_prob_to90=90.0,
        adjusted_intensity=0.7,
        season_context_factor=1.05,
    )["passed"] is False
    assert nanotest.evaluate_channel_signal_filter(
        101.0, 0, 0,
        reputation_base_prob_to90=88.0,
        reputation_adjusted_prob_to90=90.0,
        adjusted_intensity=0.7,
        season_context_factor=1.05,
    )["passed"] is False
    assert nanotest.evaluate_channel_signal_filter(
        90.0, 0, 0,
        reputation_base_prob_to90=None,
        reputation_adjusted_prob_to90=90.0,
        adjusted_intensity=0.7,
        season_context_factor=1.05,
    )["passed"] is False
    assert nanotest.evaluate_channel_signal_filter(
        90.0, 0, 0,
        reputation_base_prob_to90=88.0,
        reputation_adjusted_prob_to90=90.0,
        adjusted_intensity=None,
        season_context_factor=1.05,
    )["passed"] is False


def test_saved_legacy_premium_title_is_rendered_as_ordinary_signal() -> None:
    assert nanotest.resolve_signal_header_title(
        saved_header_text="🚨 Premium Сигнал\n\nHome — Away"
    ) == "🚨 Сигнал"


def test_premium_badge_runtime_api_and_new_state_fields_are_removed() -> None:
    fixture_id = 987654320
    assert not hasattr(nanotest, "qualifies_for_premium_badge")
    try:
        nanotest.save_signal_snapshot_state(
            match_id=fixture_id,
            header_text="🚨 Сигнал\n\nHome — Away",
            signal_score_home=1,
            signal_score_away=1,
            signal_minute=50,
            message_id=123,
            chat_id=456,
            prob_to90=75.0,
        )
        with nanotest.state_lock:
            meta = dict(
                nanotest.state["signal_snapshot_meta"][str(fixture_id)]
            )
        assert "premium_badge" not in meta
        assert "premium_badge_rule_version" not in meta
    finally:
        with nanotest.state_lock:
            nanotest.state.get("signal_snapshot_meta", {}).pop(
                str(fixture_id), None
            )
            nanotest.state.get("signal_header_texts", {}).pop(
                str(fixture_id), None
            )
