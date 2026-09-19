from __future__ import annotations

import logging

import NanoTest as nanotest
from second_half import aggregate, smoothing


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


def _quiet_model_flags(monkeypatch) -> None:
    monkeypatch.setattr(nanotest, "LOG_PROB_HORIZON", False)
    monkeypatch.setattr(nanotest, "LOG_PROB_COMPONENTS", False)
    monkeypatch.setattr(nanotest, "LOG_LIVE_GATE_DETAILS", False)
    monkeypatch.setattr(nanotest, "ENABLE_2H_FACTORS", False)
    monkeypatch.setattr(nanotest, "ENABLE_2H_SOFT_APPLY", False)
    monkeypatch.setattr(nanotest, "DEBUG_2H_LOGS", False)
    monkeypatch.setattr(nanotest, "PRESSURE_AUX_MODE", False)
    monkeypatch.setattr(nanotest, "LIVE_GATE_GARBAGE_ONLY", False)
    monkeypatch.setattr(nanotest, "SOFT_APPLY_MAX_ANTIBOOST_PCT", 0.04)
    monkeypatch.setattr(nanotest, "SOFT_APPLY_MAX_BOOST_PCT", 0.05)


def _patch_model_helpers(monkeypatch) -> None:
    monkeypatch.setattr(nanotest, "get_fixture_id", lambda fixture: int(fixture.get("fixture_id", 0)))
    monkeypatch.setattr(nanotest, "get_xg_with_fallback", lambda fixture: dict(fixture.get("xg_info", {})))
    monkeypatch.setattr(nanotest, "get_metric_from_fixture", lambda fixture, name, default=0: fixture.get(name, default))

    def _fake_get_any_metric(fixture, keys, side):
        stats = fixture.get("stats", {})
        for key in keys:
            if key in stats:
                return stats[key].get(side, 0.0)
        return 0.0

    monkeypatch.setattr(nanotest, "get_any_metric", _fake_get_any_metric)
    monkeypatch.setattr(nanotest, "calculate_pressure_index", lambda fixture: float(fixture.get("pressure_index", 0.0)))
    monkeypatch.setattr(
        nanotest,
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
        nanotest,
        "calculate_save_stress",
        lambda **kwargs: float(kwargs.get("fixture_override", 0.0)) if "fixture_override" in kwargs else 0.35,
    )
    monkeypatch.setattr(
        nanotest,
        "calc_match_team_boost_v2",
        lambda fixture: {"combined_m": float(fixture.get("combined_m", 1.0))},
    )
    monkeypatch.setattr(
        nanotest,
        "_compute_second_half_context",
        lambda fixture, minute=None: dict(fixture.get("second_half_context", nanotest._neutral_second_half_context())),
    )


def _compute(monkeypatch, fixture: dict, minute: int = 58) -> dict:
    _quiet_model_flags(monkeypatch)
    _patch_model_helpers(monkeypatch)
    monkeypatch.setattr(
        nanotest,
        "calculate_save_stress",
        lambda **kwargs: float(fixture.get("save_stress", 0.35)),
    )
    return nanotest.compute_probability_45_plus(fixture, minute)


def test_probability_aliases_remain_compatible(monkeypatch) -> None:
    result = _compute(monkeypatch, _base_fixture())

    assert result["prob_next_25_pct"] == result["prob_second_half_remain_pct"]
    assert result["prob_next_25"] == result["prob_second_half_remain"]
    assert result["season_context_factor_45p"] == result["combined_m_2h"]
    assert result["legacy_combined_m_2h"] == result["combined_m_2h"]


def test_horizon_logging_uses_next_25_labels(monkeypatch, caplog) -> None:
    fixture = _base_fixture()
    _quiet_model_flags(monkeypatch)
    _patch_model_helpers(monkeypatch)
    monkeypatch.setattr(nanotest, "LOG_PROB_HORIZON", True)

    with caplog.at_level(logging.INFO, logger=nanotest.logger.name):
        nanotest.compute_probability_45_plus(fixture, 58)

    messages = "\n".join(record.getMessage() for record in caplog.records if "[PROB_HORIZON]" in record.getMessage())
    assert "horizon_minutes=25" in messages
    assert "metric=prob_next_25" in messages
    assert "legacy_name=prob_second_half_remain" in messages


def test_independent_pressure_index_keeps_full_weight_with_xg_and_shots(monkeypatch) -> None:
    fixture = _base_fixture()

    legacy = _compute(monkeypatch, fixture)
    monkeypatch.setattr(nanotest, "PRESSURE_AUX_MODE", True)
    aux = nanotest.compute_probability_45_plus(fixture, 58)

    assert legacy["pressure_weight_effective"] == 0.2
    assert aux["pressure_weight_effective"] == 0.2
    assert aux["prob_next_25_pct"] == legacy["prob_next_25_pct"]


def test_pressure_aux_mode_keeps_legacy_weight_without_xg_or_shots(monkeypatch) -> None:
    fixture = _base_fixture()
    fixture["xg_info"] = {
        "xg_home": 0.0,
        "xg_away": 0.0,
        "xg_total": 0.0,
        "xg_delta": 0.0,
        "xg_source": "test",
        "xg_home_source": "test",
        "xg_away_source": "test",
    }
    fixture["stats"]["shots_on_target"] = {"home": 0.0, "away": 0.0}
    fixture["stats"]["shots_insidebox"] = {"home": 0.0, "away": 0.0}

    _quiet_model_flags(monkeypatch)
    _patch_model_helpers(monkeypatch)
    monkeypatch.setattr(nanotest, "PRESSURE_AUX_MODE", True)

    result = nanotest.compute_probability_45_plus(fixture, 58)

    assert result["pressure_weight_effective"] == 0.2


def test_second_half_log_only_mode_keeps_probability_unchanged(monkeypatch, caplog) -> None:
    fixture = _base_fixture()
    baseline = _compute(monkeypatch, fixture)

    _quiet_model_flags(monkeypatch)
    _patch_model_helpers(monkeypatch)
    monkeypatch.setattr(nanotest, "ENABLE_2H_FACTORS", True)
    monkeypatch.setattr(nanotest, "DEBUG_2H_LOGS", True)

    with caplog.at_level(logging.INFO, logger=nanotest.logger.name):
        result = nanotest.compute_probability_45_plus(fixture, 58)

    messages = "\n".join(record.getMessage() for record in caplog.records if "[2H_APPLY]" in record.getMessage())
    assert result["prob_next_25_pct"] == baseline["prob_next_25_pct"]
    assert result["prob_second_half_remain_pct"] == baseline["prob_second_half_remain_pct"]
    assert result["raw_context_multiplier"] == 1.08
    assert result["applied_context_multiplier"] == 1.05
    assert "mode=log_only" in messages


def test_second_half_soft_apply_uses_capped_multiplier(monkeypatch) -> None:
    fixture = _base_fixture()
    baseline = _compute(monkeypatch, fixture)

    _quiet_model_flags(monkeypatch)
    _patch_model_helpers(monkeypatch)
    monkeypatch.setattr(nanotest, "ENABLE_2H_SOFT_APPLY", True)

    result = nanotest.compute_probability_45_plus(fixture, 58)

    expected_prob = round(baseline["prob_next_25_pct"] * 1.05, 2)
    assert result["raw_context_multiplier"] == 1.08
    assert result["applied_context_multiplier"] == 1.05
    assert result["prob_next_25_pct"] == expected_prob
    assert result["prob_second_half_remain_pct"] == expected_prob


def test_second_half_soft_apply_uses_asymmetric_antiboost_limit(monkeypatch) -> None:
    fixture = _base_fixture()
    baseline = _compute(monkeypatch, fixture)
    fixture["second_half_context"]["context_multiplier"] = 0.90

    _quiet_model_flags(monkeypatch)
    _patch_model_helpers(monkeypatch)
    monkeypatch.setattr(nanotest, "ENABLE_2H_SOFT_APPLY", True)

    result = nanotest.compute_probability_45_plus(fixture, 58)

    expected_prob = round(baseline["prob_next_25_pct"] * 0.96, 2)
    assert result["raw_context_multiplier"] == 0.90
    assert result["applied_context_multiplier"] == 0.96
    assert result["prob_next_25_pct"] == expected_prob


def test_live_gate_legacy_mode_still_requires_two_passes() -> None:
    result = nanotest.evaluate_window_2_live_gate(
        xg_total=1.15,
        shots_on_target_total=1.0,
        shots_in_box_total=3.0,
        pressure_index=13.0,
        garbage_only=False,
    )

    assert result["mode"] == "legacy"
    assert result["legacy_live_gate_passed_count"] == 1
    assert result["blocked"] is True
    assert result["block_reason"] == "live-gate"


def test_live_gate_garbage_only_blocks_only_on_three_weak_signals() -> None:
    blocked = nanotest.evaluate_window_2_live_gate(
        xg_total=0.75,
        shots_on_target_total=1.0,
        shots_in_box_total=3.0,
        pressure_index=13.0,
        garbage_only=True,
    )
    allowed = nanotest.evaluate_window_2_live_gate(
        xg_total=0.75,
        shots_on_target_total=2.0,
        shots_in_box_total=3.0,
        pressure_index=15.0,
        garbage_only=True,
    )

    assert blocked["mode"] == "garbage_only"
    assert blocked["garbage_weak_count"] == 4
    assert blocked["live_gate_garbage"] is True
    assert blocked["blocked"] is True
    assert allowed["garbage_weak_count"] == 2
    assert allowed["live_gate_garbage"] is False
    assert allowed["blocked"] is False


def test_deprecated_pressure_aux_mode_no_longer_changes_probability(monkeypatch) -> None:
    fixture = _base_fixture()

    legacy = _compute(monkeypatch, fixture)
    monkeypatch.setattr(nanotest, "PRESSURE_AUX_MODE", True)
    aux = nanotest.compute_probability_45_plus(fixture, 58)

    assert aux["pressure_weight_effective"] == 0.2
    assert aux["prob_next_25_pct"] == legacy["prob_next_25_pct"]


def test_tempo_uses_native_attacks_as_a_minute_adjusted_rate(monkeypatch) -> None:
    fixture = _base_fixture()
    _patch_model_helpers(monkeypatch)

    tempo = nanotest.calculate_attacking_tempo(fixture, minute=56)

    assert tempo["tempo_source"] == "attacks"
    assert tempo["tempo_confidence"] == 1.0
    assert tempo["tempo"] == 0.5
    assert round(tempo["tempo_norm"], 4) == round(0.5 / 2.2, 4)


def test_tempo_falls_back_to_shot_and_corner_pace(monkeypatch) -> None:
    fixture = _base_fixture()
    fixture["stats"].pop("attacks")
    fixture["stats"]["corner_kicks"] = {"home": 4.0, "away": 2.0}
    _patch_model_helpers(monkeypatch)

    tempo = nanotest.calculate_attacking_tempo(fixture, minute=52)

    assert tempo["tempo_source"] == "shots_corners_proxy"
    assert tempo["tempo_confidence"] == 0.65
    assert 0.0 < tempo["tempo_norm"] < 1.0


def test_tempo_proxy_is_zero_only_when_activity_data_is_absent(monkeypatch) -> None:
    fixture = _base_fixture()
    fixture["stats"].pop("attacks")
    fixture["stats"]["total_shots"] = {"home": 0.0, "away": 0.0}
    fixture["stats"]["corner_kicks"] = {"home": 0.0, "away": 0.0}
    _patch_model_helpers(monkeypatch)

    tempo = nanotest.calculate_attacking_tempo(fixture, minute=55)

    assert tempo["tempo_source"] == "missing"
    assert tempo["tempo_confidence"] == 0.0
    assert tempo["tempo_norm"] == 0.0


def test_probability_uses_reduced_tempo_weight_for_proxy(monkeypatch) -> None:
    fixture = _base_fixture()
    fixture["stats"].pop("attacks")
    fixture["stats"]["corner_kicks"] = {"home": 4.0, "away": 2.0}

    result = _compute(monkeypatch, fixture, minute=58)

    assert result["tempo_source"] == "shots_corners_proxy"
    assert result["tempo_norm"] > 0.0
    assert result["tempo_weight_effective"] == 0.065


def test_probability_component_log_explains_tempo_source(monkeypatch, caplog) -> None:
    fixture = _base_fixture()
    fixture["stats"].pop("attacks")
    fixture["stats"]["corner_kicks"] = {"home": 4.0, "away": 2.0}
    _quiet_model_flags(monkeypatch)
    _patch_model_helpers(monkeypatch)
    monkeypatch.setattr(nanotest, "LOG_PROB_COMPONENTS", True)

    with caplog.at_level(logging.INFO, logger=nanotest.logger.name):
        nanotest.compute_probability_45_plus(fixture, 58)

    messages = "\n".join(
        record.getMessage()
        for record in caplog.records
        if "[PROB_45+_COMP]" in record.getMessage()
    )
    assert "tempo_source=shots_corners_proxy" in messages
    assert "tempo_confidence=0.65" in messages
    assert "tempo_weight_effective=0.065" in messages


def test_fallback_xg_confidence_reduces_all_xg_influence(monkeypatch) -> None:
    trusted_fixture = _base_fixture()
    trusted_fixture["xg_info"].update(
        {
            "xg_source": "api",
            "xg_home_source": "api",
            "xg_away_source": "api",
            "xg_confidence": 1.0,
            "xg_delta_confidence": 1.0,
            "xg_home_confidence": 1.0,
            "xg_away_confidence": 1.0,
        }
    )
    fallback_fixture = _base_fixture()
    fallback_fixture["xg_info"].update(
        {
            "xg_source": "fallback_estimated",
            "xg_home_source": "fallback_estimated",
            "xg_away_source": "fallback_estimated",
            "xg_confidence": 0.5,
            "xg_delta_confidence": 0.5,
            "xg_home_confidence": 0.5,
            "xg_away_confidence": 0.5,
        }
    )

    trusted = _compute(monkeypatch, trusted_fixture)
    fallback = _compute(monkeypatch, fallback_fixture)

    assert trusted["xg_weight_effective"] == 0.25
    assert fallback["xg_weight_effective"] == 0.125
    assert fallback["live_intensity"] < trusted["live_intensity"]
    assert fallback["goal_xg_gap_factor"] == 0.985
    assert fallback["xg_delta_factor"] == 1.01


def test_box_and_save_components_use_smooth_saturation(monkeypatch) -> None:
    fixture = _base_fixture()
    fixture["stats"]["shots_insidebox"] = {"home": 12.0, "away": 8.0}

    result = _compute(monkeypatch, fixture)

    assert 0.90 < result["shots_in_box_norm"] < 1.0
    assert result["shots_in_box_normalization"] == "hill_half_5.8_exp_2"
    assert result["save_weight_effective"] == 0.08


def test_xg_confidence_log_shows_nominal_and_effective_weight(monkeypatch, caplog) -> None:
    fixture = _base_fixture()
    fixture["xg_info"].update(
        {
            "xg_source": "fallback_estimated",
            "xg_home_source": "fallback_estimated",
            "xg_away_source": "fallback_estimated",
            "xg_confidence": 0.6,
            "xg_delta_confidence": 0.5,
            "xg_home_confidence": 0.6,
            "xg_away_confidence": 0.5,
        }
    )
    _quiet_model_flags(monkeypatch)
    _patch_model_helpers(monkeypatch)
    monkeypatch.setattr(nanotest, "LOG_PROB_COMPONENTS", True)

    with caplog.at_level(logging.INFO, logger=nanotest.logger.name):
        nanotest.compute_probability_45_plus(fixture, 58)

    messages = "\n".join(
        record.getMessage()
        for record in caplog.records
        if "[XG_CONFIDENCE]" in record.getMessage()
    )
    assert "total_confidence=0.600" in messages
    assert "nominal_weight=0.250" in messages
    assert "effective_weight=0.150" in messages


def test_second_half_smoothing_defaults_stay_in_place() -> None:
    weights = smoothing.build_two_tier_weights(20)

    assert aggregate.TEAM_2H_MATCHES == 20
    assert smoothing.WEIGHT_RECENT == 0.6
    assert smoothing.SHRINKAGE_ALPHA == 8.0
    assert round(sum(weights[:10]), 6) == 0.6
    assert round(sum(weights[10:]), 6) == 0.4
    assert smoothing.get_sample_blend_weight(4) == 0.0
    assert smoothing.get_sample_blend_weight(5) == 0.3
    assert smoothing.get_sample_blend_weight(10) == 0.5
    assert smoothing.get_sample_blend_weight(20) == 0.7
