from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

import wide_research.discovery as discovery
from market_benchmark.research import CausalQuoteCache, freeze_market_context
from test_wide_research_discovery import _observation
from wide_research.discovery import (
    DiscoveryConfig,
    JournalJoinStore,
    discover_extended_scopes,
    discover_rules,
    extract_features,
)
from wide_research.extended_features import has_market_dependency


UTC = timezone.utc
PRODUCT = "nonlinear.v1.product.intensity.pressure"


def _config(**overrides) -> DiscoveryConfig:
    values = {
        "train_fraction": 0.50,
        "validation_fraction": 0.25,
        "max_conjunction_size": 2,
        "beam_width": 48,
        "evaluation_budget": 2500,
        "top_n": 4,
        "min_train_support": 3,
        "min_validation_support": 4,
        "min_holdout_support": 2,
        "selection_mode": "rare_precision",
        "extended_features": True,
        "allow_feature_ranges": True,
        "validation_window_count": 4,
        "quantiles": (0.25, 0.50, 0.75),
        "null_hit_rate": 0.50,
    }
    return DiscoveryConfig(**{**values, **overrides})


@pytest.fixture
def compact_grid(monkeypatch):
    grids = {
        "bot.prob_to90": (">=", ()),
        "feature.adjusted_intensity": (">=", ()),
        "feature.pressure_index": (">=", ()),
        PRODUCT: (">=", ()),
        "market.v1.prob_to90": (">=", ()),
        "market.v1.bot_gap_pp": (">=", ()),
        "market.v1.delta_5m_pp": (">=", ()),
        "nonlinear.v1.product.bot_p90.market_p90": (">=", ()),
    }
    monkeypatch.setattr(discovery, "_threshold_grids", lambda config: grids)
    return grids


def _quote(observation, *, minutes_ago=0, probability=0.80, key=None):
    at = datetime.fromisoformat(observation["created_at_utc"])
    captured = at - timedelta(minutes=minutes_ago, seconds=20)
    over = 1.0 / probability
    under = 1.0 / (1.0 - probability)
    return {
        "record_type": "market_odds_snapshot",
        "record_key": key or f"quote:{observation['fixture_id']}:{minutes_ago}",
        "fixture_id": observation["fixture_id"],
        "provider": "api_football",
        "shadow_only": True,
        "production_applied": False,
        "settlement_scope": "normal_time",
        "bet_id": 25,
        "source_status": {"blocked": False, "stopped": False, "finished": False},
        "score_home": 0,
        "score_away": 0,
        "current_goals": 0,
        "line": 0.5,
        "captured_at_utc": captured.isoformat(),
        "provider_update_utc": (captured - timedelta(seconds=1)).isoformat(),
        "over_decimal": over,
        "under_decimal": under,
        "market": {"fair_probability_goal_to90": probability},
    }


def _rows(count=128, *, market_from=0):
    start = datetime(2026, 8, 1, tzinfo=UTC)
    rows = []
    for index in range(count):
        probability = (60.0, 70.0, 80.0, 90.0)[index % 4]
        row = _observation(
            31000 + index,
            50,
            start + timedelta(hours=index * 3),
            label=probability >= 70.0,
            probability=probability,
            suffix="extended",
        )
        if index >= market_from:
            row["market_research"] = freeze_market_context(
                row, [_quote(row, probability=probability / 100.0)]
            )
        rows.append(row)
    return rows


def _scope_report(rows, config):
    with JournalJoinStore(config=config) as join:
        join.ingest_observation_records(rows)
        return discover_extended_scopes(join, config=config)


def test_extended_thresholds_use_train_values_only(compact_grid):
    rows = _rows()
    config = _config()
    before = discover_rules(rows, config=config)
    train_ids = set(before["manifests"]["splits"]["train"]["fixture_ids"])
    changed = deepcopy(rows)
    for row in changed:
        if row["fixture_id"] in train_ids:
            continue
        row["features"]["adjusted_intensity"] = 20.0
        row["features"]["pressure_index"] = 2500.0
        row["market_research"] = freeze_market_context(
            row, [_quote(row, probability=0.995)]
        )
    after = discover_rules(changed, config=config)
    thresholds = before["manifests"]["threshold_manifest"]
    assert thresholds == after["manifests"]["threshold_manifest"]
    for feature in (PRODUCT, "market.v1.prob_to90"):
        assert thresholds[feature]["effective_thresholds"]
        assert thresholds[feature]["threshold_source_split"] == "train"
        assert thresholds[feature]["train_value_count"] == len(train_ids)


def test_product_discovers_curved_relation_beyond_single_axis_threshold(compact_grid):
    rows = _rows(market_from=128)
    # Neither intensity nor pressure alone orders the positives ahead of the
    # negatives. Their product separates both positive arms of the relation.
    patterns = ((0.20, 90.0, True), (0.90, 20.0, True),
                (0.10, 95.0, False), (0.95, 10.0, False))
    for index, row in enumerate(rows):
        intensity, pressure, label = patterns[index % len(patterns)]
        row["features"]["adjusted_intensity"] = intensity
        row["features"]["pressure_index"] = pressure
        row["probabilities"]["prob_to90"] = 75.0
        row["outcome"]["goal_to90_normal_time"] = label
    report = discover_rules(rows, config=_config(max_conjunction_size=1))
    candidates = {item["candidate_id"]: item for item in report["manifests"]["candidates"]}
    perfect = [result for result in report["results"]["results"]
               if result["validation"]["wins"] == 16
               and result["validation"]["losses"] == 0]
    assert perfect
    for result in perfect:
        assert candidates[result["candidate_id"]]["clauses"][0]["feature"] == PRODUCT
        assert result["train"]["wins"] == 32
        assert result["train"]["losses"] == 0
        assert result["holdout"]["wins"] == 16
        assert result["holdout"]["losses"] == 0


def test_market_only_candidates_require_market_inputs_and_exclude_missing(compact_grid):
    rows = _rows(count=160, market_from=80)
    report = discover_rules(rows, config=_config(market_only=True))
    candidates = report["manifests"]["candidates"]
    assert candidates
    expected = {row["fixture_id"] for row in rows[80:]}
    split_ids = [set(report["manifests"]["splits"][name]["fixture_ids"])
                 for name in ("train", "validation", "holdout")]
    assert set.union(*split_ids) == expected
    assert all(any(has_market_dependency(c["feature"]) for c in candidate["clauses"])
               for candidate in candidates)
    assert report["manifests"]["search"]["rare_precision"]["market_only"] is True


def test_scopes_split_recent_market_cohort_independently(compact_grid):
    rows = _rows(count=160, market_from=80)
    report = _scope_report(rows, _config())
    manifest = report["manifests"]
    scope = manifest["search"]["market_scope"]
    assert scope["status"] == "completed"
    assert scope["eligible_fixtures"] == 80
    assert scope["selection_uses_holdout"] is False
    general_train = set(manifest["splits"]["train"]["fixture_ids"])
    market_train = set(scope["splits"]["train"]["fixture_ids"])
    assert general_train == {row["fixture_id"] for row in rows[:80]}
    assert market_train == {row["fixture_id"] for row in rows[80:120]}
    assert general_train.isdisjoint(market_train)
    candidates = manifest["candidates"]
    assert {candidate["research_scope"] for candidate in candidates} == {
        "general_nonlinear", "causal_market"
    }
    market_candidates = [c for c in candidates if c["research_scope"] == "causal_market"]
    assert all(any(has_market_dependency(c["feature"]) for c in candidate["clauses"])
               for candidate in market_candidates)
    assert report["results"]["market_scope_splits"] == scope["splits"]
    assert all(result["statistical_test"]["family_size"] == len(candidates)
               for result in report["results"]["results"])


def test_market_scope_collects_without_borrowing_no_market_matches(compact_grid):
    rows = _rows(count=128, market_from=126)
    report = _scope_report(rows, _config())
    scope = report["manifests"]["search"]["market_scope"]
    assert scope["status"] == "collecting"
    assert scope["eligible_fixtures"] == 2
    assert report["results"]["market_scope_splits"] is None
    assert all(c["research_scope"] == "general_nonlinear"
               for c in report["manifests"]["candidates"])


def test_extended_scope_selection_is_unchanged_by_holdout_labels(compact_grid):
    rows = _rows(count=160, market_from=80)
    config = _config()
    before = _scope_report(rows, config)
    manifest = before["manifests"]
    # Use the intersection: changing a general holdout row which belongs to
    # market validation would deliberately alter that branch's selection.
    holdout_ids = set(manifest["splits"]["holdout"]["fixture_ids"]) & set(
        manifest["search"]["market_scope"]["splits"]["holdout"]["fixture_ids"]
    )
    assert holdout_ids
    changed = deepcopy(rows)
    for row in changed:
        if row["fixture_id"] in holdout_ids:
            row["outcome"]["goal_to90_normal_time"] = not row["outcome"]["goal_to90_normal_time"]
    after = _scope_report(changed, config)
    assert before["manifests"]["candidates"] == after["manifests"]["candidates"]
    assert before["manifests"]["threshold_manifest"] == after["manifests"]["threshold_manifest"]
    assert manifest["search"]["market_scope"]["threshold_manifest"] == (
        after["manifests"]["search"]["market_scope"]["threshold_manifest"]
    )
    assert [r["holdout"] for r in before["results"]["results"]] != [
        r["holdout"] for r in after["results"]["results"]
    ]


@pytest.mark.parametrize("market_only", [False, True])
def test_each_scope_ignores_all_its_own_holdout_labels(compact_grid, market_only):
    rows = _rows(count=160, market_from=80)
    config = _config(market_only=market_only)
    before = discover_rules(rows, config=config)
    holdout_ids = set(before["manifests"]["splits"]["holdout"]["fixture_ids"])
    changed = deepcopy(rows)
    for row in changed:
        if row["fixture_id"] in holdout_ids:
            row["outcome"]["goal_to90_normal_time"] = not row["outcome"]["goal_to90_normal_time"]
    after = discover_rules(changed, config=config)
    assert before["manifests"]["candidates"]
    assert before["manifests"]["candidates"] == after["manifests"]["candidates"]
    assert before["manifests"]["threshold_manifest"] == after["manifests"]["threshold_manifest"]
    assert [r["holdout"] for r in before["results"]["results"]] != [
        r["holdout"] for r in after["results"]["results"]
    ]


def test_offline_market_join_matches_live_causal_cache_and_ignores_future_quotes():
    observation = _rows(count=1, market_from=1)[0]
    at = datetime.fromisoformat(observation["created_at_utc"])
    quotes = [_quote(observation, minutes_ago=10, probability=0.60),
              _quote(observation, minutes_ago=5, probability=0.70),
              _quote(observation, probability=0.80)]
    future = _quote(observation, probability=0.99, key="future")
    future["captured_at_utc"] = (at + timedelta(seconds=1)).isoformat()
    future["provider_update_utc"] = future["captured_at_utc"]
    quotes.append(future)
    cache = CausalQuoteCache()
    cache.update(quotes, (at + timedelta(seconds=2)).isoformat())
    live = {**observation, "market_research": cache.freeze(observation)}
    with JournalJoinStore(config=_config()) as join:
        join.ingest_observation_records([observation])
        join.ingest_market_quotes(quotes)
        offline = list(join.joined_observations())[0]
    assert offline["market_research"] == live["market_research"]
    values = extract_features(offline, include_extended=True)
    assert values == extract_features(live, include_extended=True)
    assert values["market.v1.prob_to90"] == pytest.approx(80.0)
    assert values["market.v1.delta_5m_pp"] == pytest.approx(10.0)
    assert values["market.v1.delta_10m_pp"] == pytest.approx(20.0)
    assert all(q["record_key"] != "future" for q in offline["market_research"]["quotes"].values())


def test_offline_join_preserves_persisted_market_unavailability():
    observation = _rows(count=1, market_from=1)[0]
    observation["market_research"] = freeze_market_context(observation, [])
    frozen = deepcopy(observation["market_research"])
    with JournalJoinStore(config=_config()) as join:
        join.ingest_observation_records([observation])
        join.ingest_market_quotes([_quote(observation)])
        joined = list(join.joined_observations())[0]
    assert joined["market_research"] == frozen
    assert joined["market_research"]["status"] == "unavailable"
    assert "market.v1.prob_to90" not in extract_features(joined, include_extended=True)
    assert not any(has_market_dependency(name) for name in extract_features(joined, include_extended=True))


@pytest.mark.parametrize("mutation", ["future_capture", "stale", "wrong_score", "wrong_line", "blocked"])
def test_offline_join_rejects_unusable_quote_evidence(mutation):
    observation = _rows(count=1, market_from=1)[0]
    quote = _quote(observation)
    at = datetime.fromisoformat(observation["created_at_utc"])
    if mutation == "future_capture":
        quote["captured_at_utc"] = (at + timedelta(seconds=1)).isoformat()
    elif mutation == "stale":
        quote["captured_at_utc"] = (at - timedelta(seconds=121)).isoformat()
        quote["provider_update_utc"] = quote["captured_at_utc"]
    elif mutation == "wrong_score":
        quote["score_home"] = 1
    elif mutation == "wrong_line":
        quote["line"] = 1.5
    else:
        quote["source_status"]["blocked"] = True
    with JournalJoinStore(config=_config()) as join:
        join.ingest_observation_records([observation])
        join.ingest_market_quotes([quote])
        joined = list(join.joined_observations())[0]
    assert joined["market_research"]["status"] == "unavailable"
    assert "market.v1.prob_to90" not in extract_features(joined, include_extended=True)


def test_legacy_discovery_does_not_backfill_extended_inputs():
    observation = _rows(count=1, market_from=1)[0]
    config = replace(_config(), extended_features=False)
    with JournalJoinStore(config=config) as join:
        join.ingest_observation_records([observation])
        join.ingest_market_quotes([_quote(observation)])
        joined = list(join.joined_observations())[0]
    assert "market_research" not in joined
    assert not any(name.startswith(("market.v1.", "nonlinear.v1."))
                   for name in extract_features(joined))
