from __future__ import annotations

import NanoTest as bot


def _strong_probability_result() -> dict:
    return {
        "threshold_source": "dynamic_minute_bucket",
        "xg_confidence": 1.0,
        "xg_total_effective": 1.8,
        "shots_on_target_total": 7.0,
        "shots_in_box_total": 10.0,
        "pressure_index": 20.0,
        "tempo_confidence": 1.0,
    }


def _live_data() -> dict:
    return {
        "fixture": {
            "team_home_name": {"value": "Home"},
            "team_away_name": {"value": "Away"},
            "league_name": {"value": "League"},
            "league_country": {"value": "Country"},
            "elapsed": {"value": 55},
            "status": {"value": {"short": "2H", "elapsed": 55}},
            "score_home": {"value": 0},
            "score_away": {"value": 0},
        }
    }


def test_rescue_controller_allows_only_a_strong_dynamic_near_miss(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_RESCUE_SIGNALS", True)
    result = bot.evaluate_rescue_controller(
        prob_to90=78.0,
        selected_threshold=80.0,
        prob_next_15=40.0,
        threshold_next15=35.0,
        probability_result=_strong_probability_result(),
        live_gate_passed_count=4,
    )

    assert result["candidate"] is True
    assert result["quality_passed"] is True
    assert result["controller_score"] >= bot.RESCUE_MIN_CONTROLLER_SCORE
    assert result["reliability"] >= bot.RESCUE_MIN_RELIABILITY


def test_rescue_controller_does_not_relax_large_shortfall(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_RESCUE_SIGNALS", True)
    result = bot.evaluate_rescue_controller(
        prob_to90=70.0,
        selected_threshold=80.0,
        prob_next_15=40.0,
        threshold_next15=35.0,
        probability_result=_strong_probability_result(),
        live_gate_passed_count=4,
    )

    assert result["candidate"] is False
    assert result["quality_passed"] is False
    assert result["reason"] == "threshold_shortfall"


def test_rescue_confirmation_counts_distinct_match_minutes(monkeypatch) -> None:
    fixture_id = 987654321
    monkeypatch.setattr(bot, "RESCUE_CONFIRMATION_OBSERVATIONS", 2)
    monkeypatch.setattr(bot, "RESCUE_INSTANT_CONTROLLER_SCORE", 99.0)
    evaluation = {"quality_passed": True, "controller_score": 90.0}
    with bot.state_lock:
        bot.state.setdefault("rescue_candidates", {}).pop(str(fixture_id), None)

    first = bot.register_rescue_candidate_observation(fixture_id, 55, evaluation)
    duplicate = bot.register_rescue_candidate_observation(fixture_id, 55, evaluation)
    second = bot.register_rescue_candidate_observation(fixture_id, 56, evaluation)

    assert first["confirmed"] is False
    assert duplicate["qualifying_observations"] == 1
    assert second["confirmed"] is True

    with bot.state_lock:
        bot.state.setdefault("rescue_candidates", {}).pop(str(fixture_id), None)


def test_legacy_rescue_route_uses_ordinary_title() -> None:
    live_message = bot.render_live_message(
        _live_data(),
        signal_score=(0, 0),
        prob_display=40.0,
        prob_display_90=78.0,
        signal_route="rescue",
    )
    snapshot = bot._build_signal_snapshot_data(
        _live_data(),
        prob_display=40.0,
        signal_score=(0, 0),
        signal_minute=55,
        prob_display_90=78.0,
        signal_route="rescue",
    )

    assert live_message.startswith("🚨 Сигнал")
    assert bot.build_signal_header(snapshot).startswith("🚨 Сигнал")


def test_legacy_rescue_title_is_removed_from_final_message() -> None:
    title = bot.resolve_signal_header_title(
        signal_route="rescue",
        saved_header_text="🚨 Rescue Сигнал\n\nHome — Away",
    )
    message = bot.render_final_message(
        {
            "data": _live_data(),
            "signal_home": 0,
            "signal_away": 0,
            "final_home": 1,
            "final_away": 0,
            "goal_minutes": [72],
            "normal_time_result": "WIN",
        },
        header_text=title,
    )

    assert message.startswith("🚨 Сигнал\n")
    assert not message.startswith("🚨 Rescue Сигнал\n")


def test_saved_legacy_rescue_title_is_not_recovered() -> None:
    assert bot.resolve_signal_header_title(
        saved_header_text="🚨 Rescue Сигнал\n\nHome — Away"
    ) == "🚨 Сигнал"
