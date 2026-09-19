"""Independent causal and arithmetic contracts for opt-in research features."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import math

import pytest

from market_benchmark.research import (
    CausalQuoteCache,
    freeze_market_context,
    market_values,
    valid_quote,
)
from wide_research.extended_features import (
    EXTENDED_FEATURE_NAMES,
    MARKET_FEATURE_NAMES,
    NONLINEAR_FEATURE_NAMES,
    has_market_dependency,
)
from wide_research.features import ALLOWED_FEATURE_NAMES, extract_features


AT = datetime(2026, 9, 11, 12, 10, tzinfo=timezone.utc)


def _observation(fixture_id: int = 123) -> dict:
    raw = {
        "xg_home": 1.8, "xg_away": 0.2,
        "shots_in_box_home": 6, "shots_in_box_away": 2,
        "shots_on_target_home": 4, "shots_on_target_away": 1,
        "total_shots_home": 8, "total_shots_away": 2,
        "corners_home": 3, "corners_away": 1,
        "dangerous_attacks_home": 30, "dangerous_attacks_away": 10,
    }
    return {
        "observation_id": f"observation-{fixture_id}",
        "fixture_id": fixture_id,
        "created_at_utc": AT.isoformat(),
        "minute": 55,
        "match": {"score_home": 1, "score_away": 0},
        "probabilities": {"prob_to90": 80.0},
        "features": {
            "pressure_index": 20.0, "adjusted_intensity": 0.6,
            "season_context_factor": 1.05, "game_state_factor": 1.02,
        },
        "raw_metrics": raw,
        "availability": {key: True for key in raw},
        "rolling_dynamics": {
            "schema_version": 1, "mode": "shadow_collection",
            "production_applied": False, "available_windows": 2,
            "windows": {
                f"{minutes}m": {
                    "status": "ok", "requested_window_minutes": minutes,
                    "actual_span_minutes": minutes,
                    "available_activity_metric_count": 1,
                    "availability": {"total_shots_total": True},
                    "deltas": {"total_shots_total": minutes * rate},
                    "rates_per_minute": {"total_shots_total": rate},
                }
                for minutes, rate in ((5, 0.8), (10, 0.4))
            },
        },
    }


def _quote(*, age: float = 0, fixture_id: int = 123,
           over: float = 2.0, under: float = 3.0, key: str | None = None) -> dict:
    timestamp = (AT - timedelta(seconds=age)).isoformat()
    return {
        "record_type": "market_odds_snapshot",
        "record_key": key or f"quote-{fixture_id}-{age}-{over}-{under}",
        "fixture_id": fixture_id,
        "provider": "api_football",
        "shadow_only": True, "production_applied": False,
        "settlement_scope": "normal_time", "bet_id": 25,
        "source_status": {"blocked": False, "stopped": False, "finished": False},
        "score_home": 1, "score_away": 0, "current_goals": 1, "line": 1.5,
        "captured_at_utc": timestamp, "provider_update_utc": timestamp,
        "over_decimal": over, "under_decimal": under,
        "market": {"fair_probability_goal_to90": (1 / over) / (1 / over + 1 / under)},
    }


def _set_path(mapping: dict, path: str, value) -> None:
    keys = path.split(".")
    for key in keys[:-1]:
        mapping = mapping[key]
    mapping[keys[-1]] = value


def _with_market(observation: dict, *quotes: dict) -> dict:
    result = deepcopy(observation)
    result["market_research"] = freeze_market_context(result, quotes)
    return result


def _prediction(observation: dict, probability: float, *, rolling: bool = False) -> dict:
    return {
        "record_type": "shadow_ml_rolling_prediction" if rolling else "shadow_ml_prediction",
        "observation_id": observation["observation_id"],
        "fixture_id": observation["fixture_id"], "minute": observation["minute"],
        "observation_created_at_utc": observation["created_at_utc"],
        "created_at_utc": (AT + timedelta(seconds=1)).isoformat(),
        "model_created_at_utc": (AT - timedelta(days=1)).isoformat(),
        "model_data_cutoff_utc": (AT - timedelta(days=2)).isoformat(),
        "shadow_only": True, "production_applied": False,
        "predictions": {"to90": {
            "status": "ok", "production_applied": False,
            "calibrated_probability_pct": probability,
        }},
    }


@pytest.mark.parametrize("path,value", [
    ("record_type", "market_later_movement"),
    ("record_key", ""),
    ("provider", "another_provider"),
    ("fixture_id", 456),
    ("shadow_only", False),
    ("production_applied", True),
    ("settlement_scope", "extra_time"),
    ("bet_id", 99),
    ("source_status.blocked", True),
    ("source_status.stopped", True),
    ("source_status.finished", True),
    ("source_status.blocked", None),
    ("score_home", 0),
    ("score_away", 1),
    ("current_goals", 2),
    ("line", 2.5),
    ("line", 1.0),
    ("captured_at_utc", (AT + timedelta(seconds=1)).isoformat()),
    ("captured_at_utc", (AT - timedelta(seconds=121)).isoformat()),
    ("provider_update_utc", (AT + timedelta(seconds=1)).isoformat()),
    ("provider_update_utc", (AT - timedelta(seconds=121)).isoformat()),
    ("provider_update_utc", "2026-09-11T12:10:00"),
    ("captured_at_utc", "invalid"),
    ("over_decimal", 1.0),
    ("under_decimal", True),
    ("under_decimal", math.inf),
    ("over_decimal", math.nan),
    ("market.fair_probability_goal_to90", 0.999),
    ("market.fair_probability_goal_to90", None),
])
def test_market_rejects_noncausal_or_nonmatching_quote(path: str, value) -> None:
    observation, quote = _observation(), _quote()
    _set_path(quote, path, value)
    assert valid_quote(quote, observation, AT) is False
    assert market_values(_with_market(observation, quote)) == {}


@pytest.mark.parametrize("age,accepted", [(0, True), (120, True), (120.001, False), (-0.001, False)])
def test_market_quote_freshness_boundary(age: float, accepted: bool) -> None:
    assert valid_quote(_quote(age=age), _observation(), AT) is accepted


def test_market_arithmetic_and_past_only_movements() -> None:
    observation = _with_market(
        _observation(), _quote(over=1.5, under=3.0),
        _quote(age=300, over=2.0, under=3.0),
        _quote(age=600, over=3.0, under=1.5),
    )
    values = market_values(observation)
    assert values == pytest.approx({
        "market.v1.prob_to90": 100 * 2 / 3,
        "market.v1.over_decimal": 1.5,
        "market.v1.overround": 0.0,
        "market.v1.delta_5m_pp": 100 * 2 / 3 - 60,
        "market.v1.delta_10m_pp": 100 / 3,
        "market.v1.odds_ratio_5m": 0.75,
        "market.v1.odds_ratio_10m": 0.5,
    })


@pytest.mark.parametrize("minutes", [5, 10])
def test_movement_baseline_is_at_or_before_boundary_not_nearest_future(minutes: int) -> None:
    observation = _with_market(
        _observation(), _quote(),
        _quote(age=minutes * 60 + 10, over=3.0, under=2.0),
        _quote(age=minutes * 60 - 1, over=1.5, under=3.0),
    )
    context = observation["market_research"]
    assert context["quotes"][f"{minutes}m"]["over_decimal"] == 3.0
    assert market_values(observation)[f"market.v1.delta_{minutes}m_pp"] == pytest.approx(20.0)


@pytest.mark.parametrize("minutes", [5, 10])
def test_movement_baseline_is_not_backfilled_from_after_boundary(minutes: int) -> None:
    observation = _with_market(
        _observation(), _quote(), _quote(age=minutes * 60 - 1),
        _quote(age=minutes * 60 + 121),
    )
    assert f"market.v1.delta_{minutes}m_pp" not in market_values(observation)


@pytest.mark.parametrize("change", ["score", "bet", "provider", "fixture"])
def test_movement_does_not_mix_market_identity_or_score(change: str) -> None:
    baseline = _quote(age=300, over=3.0, under=2.0)
    if change == "score":
        baseline.update(score_home=0, current_goals=0, line=0.5)
    elif change == "bet":
        baseline["bet_id"] = 36
    elif change == "provider":
        baseline["provider"] = "another_provider"
    else:
        baseline["fixture_id"] += 1
    values = market_values(_with_market(_observation(), _quote(), baseline))
    assert values["market.v1.prob_to90"] == pytest.approx(60.0)
    assert "market.v1.delta_5m_pp" not in values


def test_repeated_provider_quote_cannot_refresh_stale_capture() -> None:
    earlier = _quote(age=121, key="same-provider-quote")
    recaptured = deepcopy(earlier)
    recaptured["captured_at_utc"] = AT.isoformat()
    # Even a tampered refresh must not displace the earliest evidence.
    recaptured["provider_update_utc"] = AT.isoformat()
    for records in ((earlier, recaptured), (recaptured, earlier)):
        assert market_values(_with_market(_observation(), *records)) == {}


@pytest.mark.parametrize("field,value", [
    ("observation_id", "different-observation"),
    ("fixture_id", 456),
    ("observation_created_at_utc", (AT - timedelta(seconds=1)).isoformat()),
    ("version", "unreviewed-contract"),
])
def test_market_context_is_bound_to_observation_identity(field: str, value) -> None:
    observation = _with_market(_observation(), _quote())
    observation["market_research"][field] = value
    assert market_values(observation) == {}


def test_future_quotes_and_later_movement_fields_do_not_change_features() -> None:
    baseline = _with_market(_observation(), _quote(), _quote(age=300, over=3.0, under=2.0))
    modified = deepcopy(baseline)
    modified["market_research"]["quotes"]["later"] = _quote(age=-1, over=1.05, under=8.0)
    modified["market_research"]["values"] = {"market.v1.prob_to90": 99.9}
    modified["market_research"]["later_movement"] = {"delta_pp": 99.0}
    modified["market_movement"] = {"goal_after_prediction": True, "closing_odds": 1.01}
    modified["outcome"] = {"goal_to90_normal_time": True}
    modified["telegram"] = {"sent": True}
    assert extract_features(modified, include_extended=True) == extract_features(baseline, include_extended=True)


def test_frozen_evidence_is_independent_of_source_mutation() -> None:
    observation, quote = _observation(), _quote()
    frozen = freeze_market_context(observation, [quote])
    quote["market"]["fair_probability_goal_to90"] = 0.999
    quote["source_status"]["blocked"] = True
    observation["market_research"] = frozen
    assert market_values(observation)["market.v1.prob_to90"] == pytest.approx(60.0)


def test_cache_bounds_recent_fixtures_and_per_fixture_quote_count() -> None:
    cache = CausalQuoteCache(max_fixtures=2, per_fixture=3)
    records = [_quote(age=20 - i, fixture_id=fid) for fid in (1, 2, 3) for i in range(6)]
    cache.update(records, AT.isoformat())
    assert len(cache._quotes) == 2
    assert all(len(rows) == 3 for rows in cache._quotes.values())
    assert all([int((AT - datetime.fromisoformat(q["captured_at_utc"])).total_seconds())
                for q in rows] == [17, 16, 15] for rows in cache._quotes.values())


def test_cache_prunes_ttl_without_new_quotes_and_ignores_future_records() -> None:
    cache = CausalQuoteCache()
    cache.update([_quote(age=720, fixture_id=1), _quote(age=721, fixture_id=2),
                  _quote(age=-1, fixture_id=3)], AT.isoformat())
    assert set(cache._quotes) == {1}
    cache.update([], (AT + timedelta(seconds=1)).isoformat())
    assert cache._quotes == {}


def test_cache_snapshots_are_defensive_copies() -> None:
    cache, quote, observation = CausalQuoteCache(), _quote(), _observation()
    cache.update([quote], AT.isoformat())
    quote["market"]["fair_probability_goal_to90"] = 0.999
    frozen = cache.freeze(observation)
    assert market_values({**observation, "market_research": frozen})["market.v1.prob_to90"] == pytest.approx(60.0)
    frozen["quotes"]["now"]["market"]["fair_probability_goal_to90"] = 0.001
    second = cache.freeze(observation)
    assert market_values({**observation, "market_research": second})["market.v1.prob_to90"] == pytest.approx(60.0)


@pytest.mark.parametrize("kwargs", [{"max_fixtures": 0}, {"max_fixtures": -1},
                                    {"per_fixture": 0}, {"per_fixture": -1}])
def test_cache_requires_strictly_positive_bounds(kwargs: dict) -> None:
    with pytest.raises(ValueError):
        CausalQuoteCache(**kwargs)


def test_cache_keeps_earliest_capture_for_out_of_order_duplicate() -> None:
    cache = CausalQuoteCache()
    earlier = _quote(age=121, key="same-provider-quote")
    later = deepcopy(earlier)
    later["captured_at_utc"] = AT.isoformat()
    later["provider_update_utc"] = AT.isoformat()
    cache.update([later], AT.isoformat())
    cache.update([earlier], AT.isoformat())
    assert market_values({**_observation(), "market_research": cache.freeze(_observation())}) == {}


def test_nonlinear_products_use_documented_scales_and_causal_ml() -> None:
    observation = _with_market(_observation(), _quote())
    values = extract_features(observation, include_extended=True,
                              static_prediction=_prediction(observation, 70.0),
                              rolling_prediction=_prediction(observation, 75.0, rolling=True))["values"]
    assert values["nonlinear.v1.product.intensity.pressure"] == pytest.approx(0.6 * 0.2)
    assert values["nonlinear.v1.product.bot_p90.season"] == pytest.approx(0.8 * 1.05)
    assert values["nonlinear.v1.product.bot_p90.market_p90"] == pytest.approx(0.8 * 0.6)
    assert values["nonlinear.v1.product.market_p90.static_p90"] == pytest.approx(0.6 * 0.7)
    assert values["nonlinear.v1.product.market_p90.rolling_p90"] == pytest.approx(0.6 * 0.75)
    assert values["market.v1.bot_gap_pp"] == pytest.approx(20.0)
    assert values["market.v1.static_gap_pp"] == pytest.approx(10.0)
    assert values["market.v1.rolling_gap_pp"] == pytest.approx(15.0)


def test_nonlinear_ratios_and_side_asymmetry() -> None:
    values = extract_features(_observation(), include_extended=True)["values"]
    assert values["nonlinear.v1.pace5_over_pace10"] == pytest.approx(2.0)
    assert values["nonlinear.v1.box_share"] == pytest.approx(0.8)
    assert values["nonlinear.v1.on_target_share"] == pytest.approx(0.5)
    assert values["nonlinear.v1.xg_per_shot"] == pytest.approx(0.2)
    assert values["nonlinear.v1.home_share.total_shots"] == pytest.approx(0.8)
    assert values["nonlinear.v1.away_share.total_shots"] == pytest.approx(0.2)
    assert values["nonlinear.v1.home_away_balance.total_shots"] == pytest.approx(0.6)
    assert values["nonlinear.v1.trailing_share.total_shots"] == pytest.approx(0.2)


@pytest.mark.parametrize("score,expected", [((1, 0), 0.2), ((0, 1), 0.8), ((0, 0), None)])
def test_trailing_team_feature_tracks_score_not_home_away_label(score: tuple, expected) -> None:
    observation = _observation()
    observation["match"] = dict(zip(("score_home", "score_away"), score))
    value = extract_features(observation, include_extended=True)["values"]["nonlinear.v1.trailing_share.total_shots"]
    assert value == pytest.approx(expected) if expected is not None else value is None


def test_team_swap_reverses_balance_but_preserves_trailing_share() -> None:
    observation = _observation()
    swapped = deepcopy(observation)
    swapped["match"] = {"score_home": 0, "score_away": 1}
    for name in observation["raw_metrics"]:
        other = name[:-4] + "away" if name.endswith("home") else name[:-4] + "home"
        swapped["raw_metrics"][name] = observation["raw_metrics"][other]
    left = extract_features(observation, include_extended=True)["values"]
    right = extract_features(swapped, include_extended=True)["values"]
    for metric in ("xg", "total_shots", "corners"):
        assert right[f"nonlinear.v1.home_away_balance.{metric}"] == pytest.approx(-left[f"nonlinear.v1.home_away_balance.{metric}"])
        assert right[f"nonlinear.v1.trailing_share.{metric}"] == pytest.approx(left[f"nonlinear.v1.trailing_share.{metric}"])


def test_zero_denominators_missing_inputs_and_nonfinite_are_unavailable() -> None:
    observation = _observation()
    observation["raw_metrics"].update(total_shots_home=0, total_shots_away=0)
    observation["availability"]["xg_away"] = False
    observation["features"]["pressure_index"] = math.nan
    observation["rolling_dynamics"]["windows"]["10m"]["rates_per_minute"]["total_shots_total"] = 0
    values = extract_features(observation, include_extended=True)["values"]
    for name in ("pace5_over_pace10", "box_share", "on_target_share", "xg_per_shot",
                 "home_share.total_shots", "home_away_balance.total_shots",
                 "home_share.xg", "product.intensity.pressure"):
        assert values[f"nonlinear.v1.{name}"] is None
    assert all(value is None or math.isfinite(value) for value in values.values())


def test_missing_market_and_ml_are_not_substituted_with_zero() -> None:
    values = extract_features(_observation(), include_extended=True)["values"]
    assert all(values[name] is None for name in MARKET_FEATURE_NAMES)
    assert values["nonlinear.v1.product.bot_p90.static_p90"] is None
    assert values["nonlinear.v1.product.market_p90.pressure"] is None
    assert values["nonlinear.v1.product.bot_p90.pressure"] == pytest.approx(0.16)


def test_nonlinear_ml_inputs_reject_noncausal_model() -> None:
    observation = _with_market(_observation(), _quote())
    prediction = _prediction(observation, 99.0)
    prediction["model_data_cutoff_utc"] = (AT + timedelta(days=1)).isoformat()
    values = extract_features(observation, include_extended=True,
                              static_prediction=prediction)["values"]
    assert values["ml.static.prob_to90"] is None
    assert values["market.v1.static_gap_pp"] is None
    assert values["nonlinear.v1.product.market_p90.static_p90"] is None


def test_extended_extraction_is_opt_in_and_does_not_change_legacy_values() -> None:
    observation = _with_market(_observation(), _quote())
    original = deepcopy(observation)
    legacy = extract_features(observation)
    explicit_legacy = extract_features(observation, include_extended=False)
    extended = extract_features(observation, include_extended=True)
    assert legacy == explicit_legacy
    assert set(legacy["values"]) == set(ALLOWED_FEATURE_NAMES)
    assert not set(EXTENDED_FEATURE_NAMES) & set(legacy["values"])
    assert set(extended["values"]) == set(ALLOWED_FEATURE_NAMES) | set(EXTENDED_FEATURE_NAMES)
    assert {name: extended["values"][name] for name in ALLOWED_FEATURE_NAMES} == legacy["values"]
    assert extended["schema_version"] == legacy["schema_version"]
    assert observation == original


def test_market_dependency_includes_nonlinear_market_products_only() -> None:
    assert all(has_market_dependency(name) for name in MARKET_FEATURE_NAMES)
    assert has_market_dependency("nonlinear.v1.product.bot_p90.market_p90")
    assert has_market_dependency("nonlinear.v1.product.market_p90.pressure")
    assert not has_market_dependency("nonlinear.v1.product.bot_p90.pressure")
    assert not has_market_dependency("nonlinear.v1.home_share.xg")
    assert len(EXTENDED_FEATURE_NAMES) == len(set(EXTENDED_FEATURE_NAMES))
    assert set(NONLINEAR_FEATURE_NAMES).isdisjoint(MARKET_FEATURE_NAMES)
