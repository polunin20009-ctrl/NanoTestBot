import NanoTest as bot


def _wrapped(value):
    return {"value": value}


def _fallback_fixture(*, include_box=True):
    fixture = {
        "total_shots_home": _wrapped(9),
        "total_shots_away": _wrapped(6),
        "shots_on_target_home": _wrapped(3),
        "shots_on_target_away": _wrapped(2),
        "corner_kicks_home": _wrapped(4),
        "corner_kicks_away": _wrapped(2),
    }
    if include_box:
        fixture["shots_insidebox_home"] = _wrapped(5)
        fixture["shots_insidebox_away"] = _wrapped(3)
    return fixture


def test_real_api_zero_xg_is_not_replaced_by_fallback(monkeypatch):
    fixture = _fallback_fixture()
    fixture["expected_goals_home"] = _wrapped(0.0)
    fixture["expected_goals_away"] = _wrapped(0.0)

    def fail_estimate(*_args, **_kwargs):
        raise AssertionError("valid API xG=0.00 must not use fallback")

    monkeypatch.setattr(bot, "estimate_xg_from_metrics_combined", fail_estimate)

    result = bot.get_xg_with_fallback(fixture)

    assert result["xg_source"] == "api"
    assert result["xg_total"] == 0.0
    assert result["xg_confidence"] == 1.0


def _normalize_xg_values(home_value, away_value):
    client = bot.APISportsMetricsClient("test-key")
    raw_fixture = {
        "fixture": {"id": 123},
        "teams": {
            "home": {"id": 10, "name": "Home"},
            "away": {"id": 20, "name": "Away"},
        },
    }
    raw_stats = [
        {
            "team": {"id": 10, "name": "Home"},
            "statistics": [{"type": "expected_goals", "value": home_value}],
        },
        {
            "team": {"id": 20, "name": "Away"},
            "statistics": [{"type": "expected_goals", "value": away_value}],
        },
    ]
    return client._normalize_statistics(raw_stats, raw_fixture)


def test_null_api_xg_is_left_missing_and_uses_fallback():
    normalized = _normalize_xg_values(None, None)
    fixture = {**_fallback_fixture(), **normalized}

    result = bot.get_xg_with_fallback(fixture)

    assert "expected_goals_home" not in normalized
    assert "expected_goals_away" not in normalized
    assert "xg_home" not in normalized
    assert "xg_away" not in normalized
    assert result["xg_source"] == "fallback_estimated"
    assert result["xg_total"] > 0.0
    assert result["xg_confidence"] == 0.75


def test_numeric_zero_api_xg_survives_normalization():
    normalized = _normalize_xg_values(0.0, 0)

    result = bot.get_xg_with_fallback(normalized)

    assert normalized["expected_goals_home"]["value"] == 0.0
    assert normalized["expected_goals_away"]["value"] == 0
    assert result["xg_source"] == "api"
    assert result["xg_total"] == 0.0
    assert result["xg_confidence"] == 1.0


def test_null_api_xg_does_not_cause_post_goal_readiness_block():
    fixture = {
        **_fallback_fixture(),
        **_normalize_xg_values(None, None),
        "score_home": _wrapped(0),
        "score_away": _wrapped(2),
        "status_short": "2H",
    }

    ready, reason = bot.is_first_signal_snapshot_ready(
        fixture,
        minute=49,
        prob_next_15=30.0,
        prob_to90=60.0,
    )

    assert ready is True
    assert reason == ""


def test_low_next15_does_not_block_base_p90_snapshot_readiness():
    fixture = {
        **_fallback_fixture(),
        "score_home": _wrapped(0),
        "score_away": _wrapped(0),
        "status_short": "2H",
    }

    ready, reason = bot.is_first_signal_snapshot_ready(
        fixture,
        minute=50,
        prob_next_15=0.0,
        prob_to90=75.0,
    )

    assert ready is True
    assert reason == ""


def test_extreme_probability_with_empty_stats_remains_integrity_blocked():
    ready, reason = bot.is_first_signal_snapshot_ready(
        {
            "score_home": _wrapped(0),
            "score_away": _wrapped(0),
            "status_short": "2H",
        },
        minute=50,
        prob_next_15=85.0,
        prob_to90=75.0,
    )

    assert ready is False
    assert reason == "weak live metrics with extreme probability"


def test_complete_fallback_data_receives_capped_confidence():
    result = bot.get_xg_with_fallback(_fallback_fixture())

    assert result["xg_source"] == "fallback_estimated"
    assert result["xg_home_coverage"] == 1.0
    assert result["xg_away_coverage"] == 1.0
    assert result["xg_home_confidence"] == 0.75
    assert result["xg_away_confidence"] == 0.75
    assert result["xg_confidence"] == 0.75


def test_incomplete_fallback_data_gets_lower_confidence():
    result = bot.get_xg_with_fallback(_fallback_fixture(include_box=False))

    assert result["xg_home_coverage"] == 0.6
    assert result["xg_away_coverage"] == 0.6
    assert result["xg_home_confidence"] == 0.59
    assert result["xg_away_confidence"] == 0.59


def test_missing_fallback_evidence_has_zero_confidence():
    result = bot.get_xg_with_fallback({})

    assert result["xg_total"] == 0.0
    assert result["xg_home_confidence"] == 0.0
    assert result["xg_away_confidence"] == 0.0
    assert result["xg_confidence"] == 0.0


def test_mixed_api_and_fallback_sources_keep_side_specific_confidence():
    fixture = _fallback_fixture()
    fixture["expected_goals_home"] = _wrapped(1.2)

    result = bot.get_xg_with_fallback(fixture)

    assert result["xg_source"] == "fallback_estimated"
    assert result["xg_home_source"] == "api"
    assert result["xg_away_source"] == "fallback_estimated"
    assert result["xg_home_confidence"] == 1.0
    assert result["xg_away_confidence"] == 0.75
    assert result["xg_delta_confidence"] == 0.75
