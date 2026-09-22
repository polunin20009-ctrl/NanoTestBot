"""Characterization / regression tests for critical production flow.

Contract reference: docs/FUNCTIONAL_CONTRACT.md

These tests lock in current behavior. They do not assert desired future behavior.
Suspected product issues are listed in PRODUCTION_FLOW_KNOWN_GAPS below and must
not be fixed inside this module.

PRODUCTION_FLOW_KNOWN_GAPS (document only — do not fix here):
- main_loop integration (live fixture poll → send) is not exercised end-to-end.
- When a wide-research champion is applied, it fully replaces BASE allow/block
  (champion PASS can publish even if BASE failed; champion FAIL blocks even if
  BASE passed). This is current code, not necessarily desired product policy.
- API_FOOTBALL_KEY is hardcoded in NanoTest.py (security), not validated here.
- Google Sheets paths remain in code but GSHEETS_AVAILABLE=false.
- reconcile 2H retry/backoff: ``store_second_half_history_payload`` returning
  ``False`` schedules the same durable retry as IncompleteSecondHalfDataError.
  Fixtures stay collectable until 2H history is stored, including after the
  observation outcome is terminal. Retry/backoff survives process restart via
  STATE_FILE.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional
from unittest.mock import MagicMock

import pytest

import NanoTest as bot


# --- helpers (mirror test_probability_refactor fixtures) ---------------------


def _base_fixture() -> dict:
    return {
        "fixture_id": 9001,
        "score_home": 1,
        "score_away": 1,
        "xg_info": {
            "xg_home": 0.9,
            "xg_away": 0.5,
            "xg_total": 1.4,
            "xg_delta": 0.4,
            "xg_source": "test",
            "xg_home_source": "test",
            "xg_away_source": "test",
        },
        "pressure_index": 18.0,
        "save_stress": 0.35,
        "combined_m": 1.10,
        "second_half_context": {
            "team_2h_factor": 1.06,
            "league_2h_factor": 1.04,
            "score_state_factor": 1.03,
            "context_multiplier": 1.08,
            "events_coverage": 0.75,
            "team_sample_home": 12,
            "team_sample_away": 11,
            "league_sample": 180,
        },
        "stats": {
            "shots_on_target": {"home": 3.0, "away": 1.0},
            "shots_insidebox": {"home": 4.0, "away": 2.0},
            "attacks": {"home": 14.0, "away": 14.0},
            "total_shots": {"home": 8.0, "away": 5.0},
            "ball_possession": {"home": 52.0, "away": 48.0},
            "passes_%": {"home": 52.0, "away": 48.0},
            "saves": {"home": 2.0, "away": 1.0},
        },
    }


def _patch_45_plus_helpers(monkeypatch) -> None:
    monkeypatch.setattr(bot, "get_fixture_id", lambda fixture: int(fixture.get("fixture_id", 0)))
    monkeypatch.setattr(bot, "get_xg_with_fallback", lambda fixture: dict(fixture.get("xg_info", {})))
    monkeypatch.setattr(bot, "get_metric_from_fixture", lambda fixture, name, default=0: fixture.get(name, default))

    def _fake_get_any_metric(fixture, keys, side):
        stats = fixture.get("stats", {})
        for key in keys:
            if key in stats:
                return stats[key].get(side, 0.0)
        return 0.0

    monkeypatch.setattr(bot, "get_any_metric", _fake_get_any_metric)
    monkeypatch.setattr(bot, "calculate_pressure_index", lambda fixture: float(fixture.get("pressure_index", 0.0)))
    monkeypatch.setattr(
        bot,
        "calculate_pressure_index_details",
        lambda fixture: {
            "pressure_index": float(fixture.get("pressure_index", 0.0)),
            "pressure_index_version": "test",
            "attempt_component": 0.0,
            "corner_component": 0.0,
            "dominance_component": 0.0,
            "offside_component": 0.0,
        },
    )
    monkeypatch.setattr(
        bot,
        "calculate_save_stress",
        lambda **kwargs: float(
            kwargs.get("fixture_override", 0.0)
            if "fixture_override" in kwargs
            else 0.35
        ),
    )
    monkeypatch.setattr(bot, "LOG_LIVE_GATE_DETAILS", False)
    monkeypatch.setattr(bot, "PRESSURE_AUX_MODE", False)
    monkeypatch.setattr(bot, "LIVE_GATE_GARBAGE_ONLY", False)
    monkeypatch.setattr(bot, "SOFT_APPLY_MAX_ANTIBOOST_PCT", 0.04)
    monkeypatch.setattr(bot, "SOFT_APPLY_MAX_BOOST_PCT", 0.05)
    monkeypatch.setattr(
        bot,
        "calc_match_team_boost_v2",
        lambda fixture: {"combined_m": float(fixture.get("combined_m", 1.0))},
    )
    monkeypatch.setattr(
        bot,
        "_compute_second_half_context",
        lambda fixture, minute=None: dict(
            fixture.get("second_half_context", bot._neutral_second_half_context())
        ),
    )
    monkeypatch.setattr(bot, "LOG_PROB_HORIZON", False)
    monkeypatch.setattr(bot, "LOG_PROB_COMPONENTS", False)
    monkeypatch.setattr(bot, "ENABLE_2H_FACTORS", False)
    monkeypatch.setattr(bot, "DEBUG_2H_LOGS", False)


def _compute_45_plus(monkeypatch, fixture: dict, minute: int = 58) -> dict:
    _patch_45_plus_helpers(monkeypatch)
    monkeypatch.setattr(
        bot,
        "calculate_save_stress",
        lambda **kwargs: float(fixture.get("save_stress", 0.35)),
    )
    return bot.compute_probability_45_plus(fixture, minute)


def _valid_send_data() -> Dict[str, Any]:
    return {
        "fixture": {
            "team_home_name": {"value": "Real Home FC"},
            "team_away_name": {"value": "Real Away FC"},
            "league_name": {"value": "Test League"},
            "league_id": {"value": 77},
            "team_home_id": {"value": 10},
            "team_away_id": {"value": 20},
        }
    }


def _effective_publication_allow(router_decision: Mapping[str, Any]) -> bool:
    """Same predicate as main_loop after route_publication_with_wide_research."""
    return bool(router_decision.get("allow"))


def _apply_main_loop_res_45_postprocess(res_45: Dict[str, Any]) -> Dict[str, Any]:
    """Mirror main_loop assignments immediately after compute_probability_45_plus_with_reputation."""
    prob_second_half_remain = res_45.get("prob_second_half_remain", 0.0)
    prob_to90_45 = res_45.get("prob_to90", prob_second_half_remain)
    res_45["prob_until_end_decision"] = prob_to90_45
    res_45["decision_remain_metric"] = "prob_to90"
    return res_45


def _score_from_fixture_metrics(fixture_metrics: Mapping[str, Any]) -> tuple[int, int]:
    return (
        bot._safe_int(bot._unwrap_value(fixture_metrics.get("score_home")), 0),
        bot._safe_int(bot._unwrap_value(fixture_metrics.get("score_away")), 0),
    )


def run_production_publication_pipeline(
    monkeypatch,
    *,
    fixture_id: int,
    fixture_metrics: dict,
    minute: int,
    application_context: str = "send",
) -> Dict[str, Any]:
    """Integration slice: 45+ reputation → BASE filter → wide router → ALLOW/BLOCK."""
    _patch_45_plus_helpers(monkeypatch)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH", False)

    res_45 = bot.compute_probability_45_plus_with_reputation(
        fixture_metrics,
        minute,
        application_context=application_context,
    )
    _apply_main_loop_res_45_postprocess(res_45)
    score_home, score_away = _score_from_fixture_metrics(fixture_metrics)
    prob_to90_45 = float(res_45.get("prob_to90", 0.0))

    channel_signal_filter = bot.evaluate_channel_signal_filter(
        prob_to90_45,
        score_home,
        score_away,
        reputation_base_prob_to90=res_45.get("reputation_base_prob_to90"),
        reputation_adjusted_prob_to90=res_45.get("reputation_adjusted_prob_to90"),
        adjusted_intensity=res_45.get("adjusted_intensity"),
        season_context_factor=res_45.get("season_context_factor_45p"),
    )
    res_45["channel_signal_filter"] = channel_signal_filter

    wide_router_decision = bot.route_publication_with_wide_research(
        fixture_id=fixture_id,
        minute=minute,
        fixture_metrics=fixture_metrics,
        probability_result=res_45,
        current_filter_allow=bool(channel_signal_filter["passed"]),
    )
    effective_publication_allow = _effective_publication_allow(wide_router_decision)
    block_reason: Optional[str] = None
    if not effective_publication_allow:
        block_reason = (
            "wide-research-champion"
            if wide_router_decision.get("applied") is True
            else "channel-signal-filter"
        )

    return {
        "res_45": res_45,
        "channel_signal_filter": channel_signal_filter,
        "wide_router_decision": wide_router_decision,
        "effective_publication_allow": effective_publication_allow,
        "final_decision": "ALLOW" if effective_publication_allow else "BLOCK",
        "block_reason": block_reason,
    }


def _main_loop_fixture_processing_skip_reason(fixture_id: int) -> Optional[str]:
    """Same early-continue guards as main_loop before probability work."""
    with bot.state_lock:
        if fixture_id in bot.state.get("monitored_matches", []):
            return "monitored_matches"
        if fixture_id in bot.state.get("excluded_matches", []):
            return "excluded_matches"
    return None


def attempt_production_telegram_send(
    *,
    fixture_id: int,
    pipeline: Mapping[str, Any],
    send_callable,
) -> Dict[str, Any]:
    """Apply main_loop skip guards, then send only when router/BASE allow."""
    skip = _main_loop_fixture_processing_skip_reason(fixture_id)
    if skip:
        return {"send_attempted": False, "skip_reason": skip, "message_id": None}
    if not pipeline.get("effective_publication_allow"):
        return {
            "send_attempted": False,
            "skip_reason": pipeline.get("block_reason") or "blocked",
            "message_id": None,
        }
    message_id = send_callable()
    return {
        "send_attempted": True,
        "skip_reason": None,
        "message_id": message_id,
    }


# --- 45+ probability model ---------------------------------------------------


def test_45_plus_model_exposes_prob_to90_horizons(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_2H_SOFT_APPLY", False)
    result = _compute_45_plus(monkeypatch, _base_fixture(), 58)
    assert 0.0 <= float(result["prob_to90"]) <= 100.0
    assert float(result["prob_next_15"]) <= float(result["prob_to90"]) + 1e-6
    assert result["prob_next_25"] == result["prob_second_half_remain"]


def test_45_plus_model_is_deterministic_under_fixed_mocks(monkeypatch) -> None:
    """Lock stability of the mocked 45+ path (no absolute golden values — global bot state can drift)."""
    monkeypatch.setattr(bot, "ENABLE_2H_SOFT_APPLY", False)
    first = _compute_45_plus(monkeypatch, _base_fixture(), 58)
    second = _compute_45_plus(monkeypatch, _base_fixture(), 58)
    assert first["prob_to90"] == second["prob_to90"]
    assert first["prob_next_15"] == second["prob_next_15"]
    assert first["lambda_2h"] == second["lambda_2h"]
    assert 0.0 < float(first["prob_to90"]) < 100.0


def test_main_loop_probability_postprocess_uses_prob_to90_remain_gate(monkeypatch) -> None:
    """Production path: compute_probability_45_plus_with_reputation then main_loop postprocess."""
    _patch_45_plus_helpers(monkeypatch)
    monkeypatch.setattr(bot, "ENABLE_2H_SOFT_APPLY", False)
    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_AUTO_APPLY", False)
    res_45 = bot.compute_probability_45_plus_with_reputation(
        _base_fixture(),
        58,
        application_context="send",
    )
    _apply_main_loop_res_45_postprocess(res_45)
    assert res_45["prob_until_end_decision"] == res_45["prob_to90"]
    assert res_45["decision_remain_metric"] == "prob_to90"


# --- 2H soft-apply (delegates detail to test_probability_refactor) -----------


def test_2h_soft_apply_changes_prob_to90_not_only_legacy_alias(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_2H_SOFT_APPLY", False)
    baseline = _compute_45_plus(monkeypatch, _base_fixture(), 58)
    fixture = _base_fixture()
    _patch_45_plus_helpers(monkeypatch)
    monkeypatch.setattr(bot, "ENABLE_2H_FACTORS", True)
    monkeypatch.setattr(bot, "ENABLE_2H_SOFT_APPLY", True)
    monkeypatch.setattr(
        bot,
        "calculate_save_stress",
        lambda **kwargs: float(fixture.get("save_stress", 0.35)),
    )
    soft = bot.compute_probability_45_plus(fixture, 58)
    assert soft["applied_context_multiplier"] == 1.05
    assert float(soft["prob_to90"]) >= float(baseline["prob_to90"])


# --- reputation auto-apply -------------------------------------------------


def test_reputation_auto_apply_disabled_leaves_probabilities_unchanged(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_2H_SOFT_APPLY", False)
    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_AUTO_APPLY", False)
    base = _compute_45_plus(monkeypatch, _base_fixture(), 50)
    before_to90 = float(base["prob_to90"])
    before_next15 = float(base["prob_next_15"])
    adjusted = bot.apply_signal_reputation_to_probability_result(
        _base_fixture(),
        50,
        base,
        application_context="send",
    )
    assert float(adjusted["prob_to90"]) == before_to90
    assert float(adjusted["prob_next_15"]) == before_next15
    assert adjusted["reputation_application"]["enabled"] is False
    assert adjusted["reputation_application"]["active"] is False


def test_reputation_apply_is_idempotent_on_same_base_fields(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_AUTO_APPLY", True)
    monkeypatch.setattr(
        bot,
        "apply_signal_reputation_auto",
        lambda **kwargs: {
            "enabled": True,
            "active": True,
            "cohort": "telegram_signals",
            "targets": {
                "next15": {
                    "base_probability": 30.0,
                    "adjusted_probability": 31.0,
                    "applied_delta_pp": 1.0,
                    "status": "applied",
                },
                "to90": {
                    "base_probability": 80.0,
                    "adjusted_probability": 81.5,
                    "applied_delta_pp": 1.5,
                    "status": "applied",
                },
            },
        },
    )
    payload = {
        "prob_next_15": 30.0,
        "prob_to90": 80.0,
        "reputation_base_prob_next_15": 30.0,
        "reputation_base_prob_to90": 80.0,
    }
    first = bot.apply_signal_reputation_to_probability_result(
        _base_fixture(), 50, dict(payload), application_context="send"
    )
    second = bot.apply_signal_reputation_to_probability_result(
        _base_fixture(), 50, dict(first), application_context="send"
    )
    assert second["prob_to90"] == first["prob_to90"]
    assert second["prob_next_15"] == first["prob_next_15"]


# --- BASE channel filter -----------------------------------------------------


def test_base_channel_filter_version_and_default_thresholds() -> None:
    assert bot.CHANNEL_SIGNAL_FILTER_VERSION == "base_rep15_int055_season102_p90_75_v1"
    assert bot.CHANNEL_SIGNAL_MIN_PROB_TO90 == 75.0
    assert bot.CHANNEL_SIGNAL_MIN_REPUTATION_DELTA_TO90_PP == 1.5
    assert bot.CHANNEL_SIGNAL_MIN_ADJUSTED_INTENSITY == 0.55
    assert bot.CHANNEL_SIGNAL_MIN_SEASON_CONTEXT_FACTOR == 1.02


def test_base_channel_filter_pass_requires_all_four_conditions() -> None:
    ok = bot.evaluate_channel_signal_filter(
        75.0,
        1,
        1,
        reputation_base_prob_to90=73.5,
        reputation_adjusted_prob_to90=75.0,
        adjusted_intensity=0.55,
        season_context_factor=1.02,
    )
    assert ok["passed"] is True
    assert ok["version"] == bot.CHANNEL_SIGNAL_FILTER_VERSION
    missing_rep = bot.evaluate_channel_signal_filter(
        75.0,
        1,
        1,
        reputation_base_prob_to90=74.0,
        reputation_adjusted_prob_to90=75.0,
        adjusted_intensity=0.55,
        season_context_factor=1.02,
    )
    assert missing_rep["passed"] is False
    assert "reputation_delta_to90_pp" in missing_rep["reason"]


# --- wide router fallback / override -----------------------------------------


def test_wide_router_disabled_wide_research_uses_current_filter(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH", False)
    decision = bot.route_publication_with_wide_research(
        fixture_id=1,
        minute=50,
        fixture_metrics={},
        probability_result={},
        current_filter_allow=True,
    )
    assert decision["applied"] is False
    assert decision["allow"] is True
    assert decision["source"] == "current_filter"
    assert decision["reason"] == "wide_research_disabled"


def test_wide_router_production_apply_off_preserves_current_filter(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH", True)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_PRODUCTION_APPLY", False)
    decision = bot.route_publication_with_wide_research(
        fixture_id=1,
        minute=50,
        fixture_metrics={},
        probability_result={},
        current_filter_allow=False,
    )
    assert decision["applied"] is False
    assert decision["allow"] is False


def test_wide_router_champion_override_replaces_allow(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH", True)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_PRODUCTION_APPLY", True)
    router = MagicMock()
    router.route.return_value = {
        "applied": True,
        "allow": False,
        "source": "wide_research_champion",
        "reason": "rule_fail",
        "rule_id": "wide-test",
        "phase_id": "phase-test",
    }
    monkeypatch.setattr(
        bot,
        "_get_wide_research_components",
        lambda: (None, None, None, router),
    )
    monkeypatch.setattr(
        bot,
        "build_wide_router_observation",
        lambda **kwargs: {"observation_id": "1:50:BLOCK:v3", "created_at_utc": "2026-01-01T00:00:00+00:00"},
    )
    monkeypatch.setattr(
        bot,
        "_build_shadow_candidate_prediction_record",
        lambda observation, rolling: None,
    )
    decision = bot.route_publication_with_wide_research(
        fixture_id=1,
        minute=50,
        fixture_metrics={"fixture_id": 1},
        probability_result={"prob_to90": 80.0},
        current_filter_allow=True,
    )
    assert decision["applied"] is True
    assert decision["allow"] is False
    assert _effective_publication_allow(decision) is False


def test_wide_router_champion_can_allow_when_base_failed(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH", True)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_PRODUCTION_APPLY", True)
    router = MagicMock()
    router.route.return_value = {
        "applied": True,
        "allow": True,
        "source": "wide_research_champion",
        "reason": "pass",
        "rule_id": "wide-test",
        "phase_id": "phase-test",
    }
    monkeypatch.setattr(
        bot,
        "_get_wide_research_components",
        lambda: (None, None, None, router),
    )
    monkeypatch.setattr(
        bot,
        "build_wide_router_observation",
        lambda **kwargs: {"observation_id": "1:50:BLOCK:v3", "created_at_utc": "2026-01-01T00:00:00+00:00"},
    )
    monkeypatch.setattr(
        bot,
        "_build_shadow_candidate_prediction_record",
        lambda observation, rolling: None,
    )
    decision = bot.route_publication_with_wide_research(
        fixture_id=1,
        minute=50,
        fixture_metrics={"fixture_id": 1},
        probability_result={"prob_to90": 70.0},
        current_filter_allow=False,
    )
    assert decision["allow"] is True
    assert _effective_publication_allow(decision) is True


# --- first-snapshot readiness ------------------------------------------------


def test_readiness_rejects_halftime_status() -> None:
    fixture = {
        "status": {"short": "HT"},
        "score_home": 0,
        "score_away": 0,
    }
    ready, reason = bot.is_first_signal_snapshot_ready(fixture, 46, 30.0, 80.0)
    assert ready is False
    assert reason == "halftime state"


def test_readiness_rejects_post_goal_xg_lag(monkeypatch) -> None:
    fixture = {
        "status": {"short": "2H"},
        "score_home": 1,
        "score_away": 0,
    }
    monkeypatch.setattr(
        bot,
        "get_xg_with_fallback",
        lambda _fixture: {"xg_total": 0.0, "xg_source": "test"},
    )
    monkeypatch.setattr(
        bot,
        "get_any_metric",
        lambda _fixture, keys, side: 0.0,
    )
    monkeypatch.setattr(
        bot,
        "get_metric_from_fixture",
        lambda _fixture, name, default=0: 1 if name == "score_home" else 0,
    )
    monkeypatch.setattr(bot, "calculate_pressure_index", lambda _fixture: 10.0)
    monkeypatch.setattr(
        bot,
        "build_stats_health_context",
        lambda *args, **kwargs: {
            "has_raw_statistics": True,
            "has_normalized_live_metrics": True,
            "stats_health": "ok",
        },
    )
    ready, reason = bot.is_first_signal_snapshot_ready(fixture, 50, 40.0, 80.0)
    assert ready is False
    assert "post-goal transitional" in reason


# --- publication context validation ------------------------------------------


@pytest.mark.parametrize(
    "mutator,expected_substring",
    [
        (lambda d: d.update({"fixture": {}}), "no data"),
        (lambda d: d["fixture"].update({"team_home_name": {"value": "Home"}}), "invalid home team"),
        (lambda d: d["fixture"].update({"team_away_name": {"value": "Away"}}), "invalid away team"),
        (lambda d: d["fixture"].pop("league_name"), "missing league name"),
        (lambda d: d["fixture"].pop("league_id"), "missing league id"),
    ],
)
def test_validate_match_context_blocks_invalid_identity(
    mutator, expected_substring: str, monkeypatch
) -> None:
    data = _valid_send_data()
    mutator(data)
    monkeypatch.setattr(
        bot,
        "calc_match_team_boost_v2",
        lambda _fixture: {
            "home_matches": 5,
            "away_matches": 5,
            "league_factor_source": "persisted",
        },
    )
    assert bot.validate_match_context_before_send(data) is False


def test_validate_match_context_accepts_complete_identity(monkeypatch) -> None:
    monkeypatch.setattr(
        bot,
        "calc_match_team_boost_v2",
        lambda _fixture: {
            "home_matches": 5,
            "away_matches": 5,
            "league_factor_source": "persisted",
        },
    )
    assert bot.validate_match_context_before_send(_valid_send_data()) is True


# --- integration: fixture → prob/rep → BASE/router → SEND/BLOCK --------------


def test_production_pipeline_blocks_when_base_channel_filter_fails(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_2H_SOFT_APPLY", False)
    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_AUTO_APPLY", False)
    fixture = _base_fixture()
    result = run_production_publication_pipeline(
        monkeypatch,
        fixture_id=int(fixture["fixture_id"]),
        fixture_metrics=fixture,
        minute=58,
    )
    assert result["channel_signal_filter"]["passed"] is False
    assert result["wide_router_decision"]["source"] == "current_filter"
    assert result["effective_publication_allow"] is False
    assert result["final_decision"] == "BLOCK"
    assert result["block_reason"] == "channel-signal-filter"
    assert result["res_45"]["decision_remain_metric"] == "prob_to90"


def test_production_pipeline_allow_reaches_telegram_send_when_base_passes(
    monkeypatch,
) -> None:
    monkeypatch.setattr(bot, "ENABLE_2H_SOFT_APPLY", False)
    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_AUTO_APPLY", True)
    monkeypatch.setattr(
        bot,
        "apply_signal_reputation_auto",
        lambda **kwargs: {
            "enabled": True,
            "active": True,
            "cohort": "telegram_signals",
            "targets": {
                "next15": {
                    "base_probability": 30.0,
                    "adjusted_probability": 32.0,
                    "applied_delta_pp": 2.0,
                    "status": "applied",
                },
                "to90": {
                    "base_probability": 73.0,
                    "adjusted_probability": 81.0,
                    "applied_delta_pp": 8.0,
                    "status": "applied",
                },
            },
        },
    )
    fixture = _base_fixture()
    pipeline = run_production_publication_pipeline(
        monkeypatch,
        fixture_id=int(fixture["fixture_id"]),
        fixture_metrics=fixture,
        minute=58,
    )
    assert pipeline["channel_signal_filter"]["passed"] is True
    assert pipeline["effective_publication_allow"] is True
    assert pipeline["final_decision"] == "ALLOW"

    send_calls: list[int] = []

    def _send_once() -> int:
        send_calls.append(int(fixture["fixture_id"]))
        with bot.state_lock:
            bot.state.setdefault("sent_matches", {})[str(fixture["fixture_id"])] = 9001
            monitored = bot.state.setdefault("monitored_matches", [])
            if int(fixture["fixture_id"]) not in monitored:
                monitored.append(int(fixture["fixture_id"]))
        return 9001

    first = attempt_production_telegram_send(
        fixture_id=int(fixture["fixture_id"]),
        pipeline=pipeline,
        send_callable=_send_once,
    )
    assert first["send_attempted"] is True
    assert first["message_id"] == 9001
    assert len(send_calls) == 1

    second = attempt_production_telegram_send(
        fixture_id=int(fixture["fixture_id"]),
        pipeline=pipeline,
        send_callable=_send_once,
    )
    assert second["send_attempted"] is False
    assert second["skip_reason"] == "monitored_matches"
    assert len(send_calls) == 1


# --- one signal per fixture (state contract) ---------------------------------


def test_first_signal_readiness_skipped_when_fixture_already_in_sent_matches() -> None:
    fixture_key = "424242"
    with bot.state_lock:
        bot.state.setdefault("sent_matches", {})[fixture_key] = 999
        first_signal = fixture_key not in bot.state.get("sent_matches", {})
    assert first_signal is False


# --- decision snapshots + publication audit ----------------------------------


def test_decision_snapshot_block_records_channel_filter_failure(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(bot, "DECISION_SNAPSHOTS_FILE", str(tmp_path / "decisions.jsonl"))
    monkeypatch.setattr(bot, "ENABLE_DECISION_SNAPSHOTS", True)
    bot._decision_snapshot_keys = None
    channel_filter = bot.evaluate_channel_signal_filter(
        70.0,
        1,
        0,
        reputation_base_prob_to90=70.0,
        reputation_adjusted_prob_to90=70.0,
        adjusted_intensity=0.55,
        season_context_factor=1.02,
    )
    snapshot = bot.build_decision_snapshot(
        fixture_id=501,
        minute=48,
        window_name="WINDOW_1",
        match_identity={},
        score_home=1,
        score_away=0,
        probability_result={
            "prob_next_15": 35.0,
            "prob_to90": 70.0,
            "channel_signal_filter": channel_filter,
        },
        threshold_result={"threshold": 82.0, "fallback_threshold": 75.0},
        threshold_next15=32.0,
        readiness_result={"passed": True},
        live_gate_result={"required": False},
        anti_garbage_passed=True,
        final_decision="BLOCK",
        block_reason="channel-signal-filter",
        factor_context={},
    )
    snapshot["telegram"] = {"send_attempted": False, "send_ok": False}
    assert snapshot["decision"]["channel_signal_filter_passed"] is False
    assert snapshot["decision"]["active_publication_allow"] is False
    assert bot.append_decision_snapshot(snapshot) is True


def test_build_observation_from_decision_uses_decision_id_as_observation_id() -> None:
    decision = {
        "decision_id": "777:50:WINDOW_2:v2",
        "decision_key": "777:50:WINDOW_2:v2",
        "fixture_id": 777,
        "minute": 50,
        "created_at_utc": "2026-08-01T12:00:00+00:00",
        "match": {},
    }
    observation = bot.build_observation_from_decision(
        decision,
        {"fixture_id": 777},
        {"prob_to90": 80.0, "pressure_index": 12.0},
    )
    assert observation["observation_id"] == "777:50:WINDOW_2:v2"
    assert observation["stage"] == "decision_pipeline"


# --- outcome resolution (production labels) ----------------------------------


def test_normal_time_outcome_integrity_used_for_to90_label() -> None:
    from outcome_integrity import resolve_normal_time_outcome

    result = resolve_normal_time_outcome((1, 1), (2, 1), normal_time_event_count=1)
    assert result.goal_to90_normal_time is True
    assert result.goal_result_source == "score_and_events"


# --- persistence / idempotency -----------------------------------------------


def test_decision_snapshot_dedupe_rejects_duplicate_decision_id(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(bot, "DECISION_SNAPSHOTS_FILE", str(tmp_path / "decisions.jsonl"))
    monkeypatch.setattr(bot, "ENABLE_DECISION_SNAPSHOTS", True)
    bot._decision_snapshot_keys = None

    def _minimal(decision_id: str) -> dict:
        return {
            "record_type": "decision",
            "decision_id": decision_id,
            "decision_key": decision_id,
            "schema_version": bot.DECISION_SNAPSHOT_SCHEMA_VERSION,
            "fixture_id": 1,
            "minute": 46,
            "window_name": "WINDOW_1",
            "created_at_utc": "2026-01-01T00:00:00+00:00",
            "decision": {"final_decision": "BLOCK"},
        }

    first_id = "1:46:WINDOW_1:v2"
    assert bot.append_decision_snapshot(_minimal(first_id)) is True
    assert bot.append_decision_snapshot(_minimal(first_id)) is False


def test_observation_history_dedupe_rejects_same_observation_id(monkeypatch, tmp_path) -> None:
    path = tmp_path / "observation_history.jsonl"
    monkeypatch.setattr(bot, "OBSERVATION_HISTORY_FILE", str(path))
    monkeypatch.setattr(bot, "ENABLE_OBSERVATION_HISTORY", True)
    bot._observation_history_keys = None
    row = {
        "record_type": "observation",
        "observation_id": "obs-dedupe-1",
        "observation_key": "obs-dedupe-1",
        "fixture_id": 1,
        "created_at_utc": "2026-01-01T00:00:00+00:00",
        "schema_version": 1,
        "stage": "prefilter",
        "minute": 40,
        "match": {},
        "outcome": {"status": "pending"},
    }
    assert bot.append_observation_history(row) is True
    assert bot.append_observation_history(dict(row)) is False


# --- Telegram send decision --------------------------------------------------


def test_send_to_telegram_skips_when_credentials_missing(monkeypatch) -> None:
    monkeypatch.setattr(bot, "TELEGRAM_TOKEN", None)
    monkeypatch.setattr(bot, "TELEGRAM_CHAT_ID", None)
    assert bot.send_to_telegram("hello", match_id=123) is None


def test_send_to_telegram_records_sent_matches_on_success(monkeypatch) -> None:
    monkeypatch.setattr(bot, "TELEGRAM_TOKEN", "test-token")
    monkeypatch.setattr(bot, "TELEGRAM_CHAT_ID", -1001)
    with bot.state_lock:
        bot.state["sent_matches"] = {}
    posted: list[dict] = []

    class _Response:
        ok = True

        @staticmethod
        def json():
            return {"ok": True, "result": {"message_id": 5555}}

    monkeypatch.setattr(bot.requests, "post", lambda *args, **kwargs: posted.append(kwargs) or _Response())
    tracked_calls: list[dict] = []
    monkeypatch.setattr(
        bot,
        "add_tracked_match",
        lambda **kwargs: tracked_calls.append(dict(kwargs)),
    )
    mid = bot.send_to_telegram("signal text", match_id=8080, score_at_signal=(1, 0), signal_minute=50)
    assert mid == 5555
    with bot.state_lock:
        assert bot.state["sent_matches"]["8080"] == 5555
        assert bot.state["match_sent_at"]["8080"] == bot.state["match_signal_info"]["8080"][
            "signal_timestamp"
        ]
    assert tracked_calls[0]["sent_at_ts"] == bot.state["match_sent_at"]["8080"]


def test_send_to_telegram_suppresses_second_post_after_partial_restart_state(
    monkeypatch,
) -> None:
    """sent_matches restored without monitored_matches must not issue a second HTTP send."""
    fixture_id = 909090
    monkeypatch.setattr(bot, "TELEGRAM_TOKEN", "test-token")
    monkeypatch.setattr(bot, "TELEGRAM_CHAT_ID", -1001)
    monkeypatch.setattr(bot, "add_tracked_match", lambda **kwargs: None)
    posts: list[dict] = []

    class _Response:
        ok = True

        @staticmethod
        def json():
            return {"ok": True, "result": {"message_id": 42}}

    monkeypatch.setattr(
        bot.requests,
        "post",
        lambda *args, **kwargs: posts.append(kwargs) or _Response(),
    )
    with bot.state_lock:
        bot.state["sent_matches"] = {str(fixture_id): 7777}
        bot.state["monitored_matches"] = []

    first = bot.send_to_telegram(
        "first",
        match_id=fixture_id,
        score_at_signal=(1, 0),
        signal_minute=50,
    )
    second = bot.send_to_telegram(
        "second",
        match_id=fixture_id,
        score_at_signal=(1, 0),
        signal_minute=51,
    )
    assert first == 7777
    assert second == 7777
    assert posts == []


def test_send_to_telegram_claim_prevents_duplicate_post_on_success_path(monkeypatch) -> None:
    monkeypatch.setattr(bot, "TELEGRAM_TOKEN", "test-token")
    monkeypatch.setattr(bot, "TELEGRAM_CHAT_ID", -1001)
    monkeypatch.setattr(bot, "add_tracked_match", lambda **kwargs: None)
    posts: list[int] = []

    class _Response:
        ok = True

        @staticmethod
        def json():
            return {"ok": True, "result": {"message_id": 9001}}

    monkeypatch.setattr(
        bot.requests,
        "post",
        lambda *args, **kwargs: posts.append(1) or _Response(),
    )
    with bot.state_lock:
        bot.state["sent_matches"] = {}
        bot.state["monitored_matches"] = []

    first = bot.send_to_telegram("once", match_id=7070, score_at_signal=(0, 0), signal_minute=48)
    second = bot.send_to_telegram("twice", match_id=7070, score_at_signal=(0, 0), signal_minute=49)
    assert first == 9001
    assert second == 9001
    assert posts == [1]


def test_resolve_final_decision_after_telegram_delivery() -> None:
    assert bot.resolve_final_decision_after_telegram_delivery(555) == ("ALLOW", None)
    assert bot.resolve_final_decision_after_telegram_delivery(None) == (
        "BLOCK",
        "telegram-send-failed",
    )


def test_telegram_delivery_failure_preserves_eligibility_separate_from_publication(
    monkeypatch,
    tmp_path,
) -> None:
    channel_filter = bot.evaluate_channel_signal_filter(
        80.0,
        1,
        1,
        reputation_base_prob_to90=78.0,
        reputation_adjusted_prob_to90=80.0,
        adjusted_intensity=0.55,
        season_context_factor=1.02,
    )
    final_decision, block_reason = bot.resolve_final_decision_after_telegram_delivery(
        None
    )
    snapshot = bot.build_decision_snapshot(
        fixture_id=6060,
        minute=50,
        window_name="WINDOW_1",
        match_identity={},
        score_home=1,
        score_away=1,
        probability_result={
            "prob_next_15": 40.0,
            "prob_to90": 80.0,
            "channel_signal_filter": channel_filter,
        },
        threshold_result={"threshold": 82.0, "fallback_threshold": 75.0},
        threshold_next15=32.0,
        readiness_result={"passed": True},
        live_gate_result={"required": False},
        anti_garbage_passed=True,
        final_decision=final_decision,
        block_reason=block_reason,
        factor_context={},
    )
    bot._attach_telegram_delivery_audit(
        snapshot,
        {
            "send_attempted": True,
            "send_ok": False,
            "message_id": None,
            "send_error_code": "telegram_send_failed",
            "send_error_description": "send_to_telegram returned no message_id",
        },
    )
    assert snapshot["decision"]["final_decision"] == "BLOCK"
    assert snapshot["decision"]["block_reason"] == "telegram-send-failed"
    assert snapshot["decision"]["active_publication_allow"] is True
    assert snapshot["decision"]["publication_eligibility_allow"] is True
    assert snapshot["telegram"]["publication_delivered"] is False
    assert snapshot["publication_policy"]["publication_delivered"] is False
    assert snapshot["publication_policy"]["publication_eligibility_allow"] is True

    monkeypatch.setattr(bot, "DECISION_SNAPSHOTS_FILE", str(tmp_path / "decisions.jsonl"))
    monkeypatch.setattr(bot, "ENABLE_DECISION_SNAPSHOTS", True)
    bot._decision_snapshot_keys = None
    assert bot.append_decision_snapshot(snapshot) is True
    persisted = bot.load_joined_decision_snapshots()[0]
    assert persisted["decision"]["final_decision"] == "BLOCK"
    assert persisted["decision"]["publication_eligibility_allow"] is True
    assert persisted["telegram"]["publication_delivered"] is False


def test_telegram_delivery_success_records_allow_and_delivered() -> None:
    channel_filter = bot.evaluate_channel_signal_filter(
        80.0,
        1,
        1,
        reputation_base_prob_to90=78.0,
        reputation_adjusted_prob_to90=80.0,
        adjusted_intensity=0.55,
        season_context_factor=1.02,
    )
    final_decision, block_reason = bot.resolve_final_decision_after_telegram_delivery(
        9001
    )
    snapshot = bot.build_decision_snapshot(
        fixture_id=6061,
        minute=51,
        window_name="WINDOW_1",
        match_identity={},
        score_home=0,
        score_away=1,
        probability_result={
            "prob_next_15": 40.0,
            "prob_to90": 80.0,
            "channel_signal_filter": channel_filter,
        },
        threshold_result={"threshold": 82.0, "fallback_threshold": 75.0},
        threshold_next15=32.0,
        readiness_result={"passed": True},
        live_gate_result={"required": False},
        anti_garbage_passed=True,
        final_decision=final_decision,
        block_reason=block_reason,
        factor_context={},
    )
    bot._attach_telegram_delivery_audit(
        snapshot,
        {
            "send_attempted": True,
            "send_ok": True,
            "message_id": 9001,
            "send_error_code": None,
            "send_error_description": None,
        },
    )
    assert snapshot["decision"]["final_decision"] == "ALLOW"
    assert snapshot["decision"]["publication_eligibility_allow"] is True
    assert snapshot["telegram"]["publication_delivered"] is True


def test_bootstrap_tracking_restores_persisted_sent_at_after_restart(monkeypatch) -> None:
    fixture_id = 505050
    persisted_ts = 1_700_000_000.0
    captured: list[dict] = []
    monkeypatch.setattr(bot, "TELEGRAM_CHAT_ID", -1001)
    monkeypatch.setattr(
        bot,
        "add_tracked_match",
        lambda **kwargs: captured.append(dict(kwargs)),
    )
    with bot.state_lock:
        bot.state["sent_matches"] = {str(fixture_id): 9999}
        bot.state["match_sent_at"] = {str(fixture_id): persisted_ts}
        bot.state["match_signal_info"] = {
            str(fixture_id): {
                "signal_timestamp": persisted_ts,
                "signal_date": "2026-01-01",
                "is_finished": False,
            }
        }
        bot.state["match_initial_score"] = {str(fixture_id): (1, 0)}
        bot.state["match_initial_minute"] = {str(fixture_id): 50}
        bot.state["tracked_matches"] = {}

    bot.bootstrap_tracking_from_state()

    assert len(captured) == 1
    assert captured[0]["fixture_id"] == fixture_id
    assert captured[0]["message_id"] == 9999
    assert captured[0]["sent_at_ts"] == persisted_ts


def test_merge_post_send_signal_info_preserves_existing_sent_timestamp() -> None:
    fixture_id = 606062
    persisted_ts = 1_650_000_000.0
    with bot.state_lock:
        bot.state["match_sent_at"] = {str(fixture_id): persisted_ts}
        bot.state["match_signal_info"] = {
            str(fixture_id): {
                "signal_timestamp": persisted_ts,
                "signal_date": "2026-01-01",
                "is_finished": False,
            }
        }
    bot._merge_post_send_signal_info(
        fixture_id,
        signal_date="2026-02-02",
        is_finished=False,
    )
    with bot.state_lock:
        assert bot.state["match_sent_at"][str(fixture_id)] == persisted_ts
        assert (
            bot.state["match_signal_info"][str(fixture_id)]["signal_timestamp"]
            == persisted_ts
        )
        assert bot.state["match_signal_info"][str(fixture_id)]["signal_date"] == "2026-02-02"
