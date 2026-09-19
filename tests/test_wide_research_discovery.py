from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts import discover_wide_rules as cli
import wide_research.discovery as discovery_module
from wide_research.discovery import (
    DiscoveryConfig,
    JournalJoinStore,
    assert_safe_outputs,
    discover_rules,
    extract_features,
    write_immutable_atomic,
)
from wide_research.features import ALLOWED_FEATURES


UTC = timezone.utc


def _observation(
    fixture_id: int,
    minute: int,
    created_at: datetime,
    *,
    label: bool,
    probability: float,
    stage: str = "decision_pipeline",
    suffix: str = "A",
) -> dict:
    observation_id = f"{fixture_id}:{minute}:{suffix}"
    resolved_at = created_at + timedelta(hours=2)
    return {
        "record_type": "observation",
        "observation_id": observation_id,
        "observation_key": observation_id,
        "fixture_id": fixture_id,
        "schema_version": 1,
        "stage": stage,
        "minute": minute,
        "created_at_utc": created_at.isoformat(),
        "gates": {
            "readiness_passed": True,
            "publication_context_passed": True,
        },
        "publication_policy": {"publication_context_passed": True},
        "match": {
            "league_id": 100 + fixture_id % 4,
            "score_home": 0,
            "score_away": 0,
        },
        "raw_metrics": {
            "xg_home": 0.8,
            "xg_away": 0.7,
            "shots_on_target_home": 2,
            "shots_on_target_away": 2,
            "shots_in_box_home": 4,
            "shots_in_box_away": 4,
            "total_shots_home": 6,
            "total_shots_away": 6,
            "corners_home": 2,
            "corners_away": 2,
            "red_cards_home": 0,
            "red_cards_away": 0,
        },
        "data_quality": {"xg_confidence": 0.75, "tempo_confidence": 0.65},
        "features": {
            "adjusted_intensity": probability / 100.0,
            "pressure_index": probability / 3.0,
            "season_context_factor": 1.03,
            "game_state_factor": 1.03,
            "goal_xg_gap": 1.0,
            "sample_confidence": 0.6,
        },
        "probabilities": {
            "prob_to90": probability,
            "prob_next_15": probability / 2.0,
            "reputation_base_prob_to90": probability - 2.0,
            "reputation_adjusted_prob_to90": probability,
        },
        "rolling_dynamics": {
            "windows": {
                "5m": {
                    "status": "ok",
                    "rates_per_minute": {
                        "shots_on_target_total": 0.2,
                        "shots_in_box_total": 0.4,
                        "total_shots_total": 0.8,
                        "corners_total": 0.1,
                        "pressure_index": 0.2,
                    },
                },
                "10m": {
                    "status": "ok",
                    "rates_per_minute": {
                        "shots_on_target_total": 0.2,
                        "shots_in_box_total": 0.4,
                        "total_shots_total": 0.8,
                        "corners_total": 0.1,
                        "pressure_index": 0.2,
                    },
                },
            }
        },
        "outcome": {
            "status": "resolved",
            "resolved_at_utc": resolved_at.isoformat(),
            "outcome_scope": "TO_90_NORMAL_TIME",
            "goal_to90_normal_time": label,
        },
    }


def _dataset(count: int = 20) -> list[dict]:
    start = datetime(2026, 8, 1, tzinfo=UTC)
    rows = []
    for index in range(count):
        fixture_id = 1000 + index
        probability = 60.0 + index * 1.5
        label = index % 5 != 0
        created = start + timedelta(hours=index * 3)
        rows.append(
            _observation(
                fixture_id,
                46,
                created,
                label=label,
                probability=probability,
                suffix="early",
            )
        )
        rows.append(
            _observation(
                fixture_id,
                50,
                created + timedelta(minutes=4),
                label=label,
                probability=probability + 3.0,
                suffix="late",
            )
        )
    return rows


def _config() -> DiscoveryConfig:
    return DiscoveryConfig(
        train_fraction=0.50,
        validation_fraction=0.25,
        max_conjunction_size=2,
        beam_width=20,
        evaluation_budget=600,
        top_n=5,
        min_train_support=2,
        min_validation_support=2,
        min_holdout_support=2,
        quantiles=(0.5,),
        null_hit_rate=0.5,
    )


def _clause_matches(features: dict[str, float], clause: dict) -> bool:
    value = features.get(clause["feature"])
    if value is None:
        return False
    if clause["operator"] == ">=":
        return value >= clause["threshold"]
    return value <= clause["threshold"]


def test_fixture_group_split_and_first_trigger_are_strict() -> None:
    rows = _dataset()
    report = discover_rules(rows, config=_config())
    split_ids = {
        name: set(report["manifests"]["splits"][name]["fixture_ids"])
        for name in ("train", "validation", "holdout")
    }
    assert split_ids["train"].isdisjoint(split_ids["validation"])
    assert split_ids["train"].isdisjoint(split_ids["holdout"])
    assert split_ids["validation"].isdisjoint(split_ids["holdout"])
    assert set.union(*split_ids.values()) == {1000 + index for index in range(20)}

    manifests = {
        item["candidate_id"]: item for item in report["manifests"]["candidates"]
    }
    by_fixture: dict[int, list[dict]] = {}
    for record in rows:
        by_fixture.setdefault(record["fixture_id"], []).append(record)
    for result in report["results"]["results"]:
        clauses = manifests[result["candidate_id"]]["clauses"]
        for trigger in result["holdout"]["first_triggers"]:
            observations = sorted(
                by_fixture[trigger["fixture_id"]], key=lambda item: item["created_at_utc"]
            )
            matching = [
                item
                for item in observations
                if all(
                    _clause_matches(extract_features(item), clause) for clause in clauses
                )
            ]
            assert matching
            assert trigger["observation_id"] == matching[0]["observation_id"]


def test_holdout_values_and_labels_never_define_thresholds_or_selection() -> None:
    rows = _dataset()
    first = discover_rules(rows, config=_config())
    holdout_ids = set(first["manifests"]["splits"]["holdout"]["fixture_ids"])
    changed = json.loads(json.dumps(rows))
    for record in changed:
        if record["fixture_id"] in holdout_ids:
            record["probabilities"]["prob_to90"] = 1.0
            record["probabilities"]["final_prob_to90"] = 1.0
            record["features"]["adjusted_intensity"] = 0.01
            record["outcome"]["goal_to90_normal_time"] = not record["outcome"][
                "goal_to90_normal_time"
            ]
    second = discover_rules(changed, config=_config())
    assert first["manifests"]["threshold_manifest"] == second["manifests"][
        "threshold_manifest"
    ]
    assert first["manifests"]["candidates"] == second["manifests"]["candidates"]
    assert first["results"]["results"] != second["results"]["results"]


def test_discovery_is_deterministic_and_top_is_bounded() -> None:
    rows = _dataset()
    first = discover_rules(rows, config=_config())
    second = discover_rules(reversed(rows), config=_config())
    assert first == second
    assert 0 < len(first["manifests"]["candidates"]) <= 5
    assert first["run_id"].startswith("wide-discovery-")
    assert all(
        clause["feature"] in ALLOWED_FEATURES
        for candidate in first["manifests"]["candidates"]
        for clause in candidate["clauses"]
    )


def test_exact_four_factor_search_reaches_depth_and_keeps_full_audit_metrics(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        discovery_module,
        "ATOMIC_THRESHOLD_GRIDS",
        {
            "bot.prob_to90": (">=", (50.0,)),
            "feature.season_context_factor": (">=", (1.0,)),
            "minute": ("<=", (50.0,)),
            "score.total_goals": ("<=", (2.0,)),
        },
    )
    config = DiscoveryConfig(
        train_fraction=0.50,
        validation_fraction=0.25,
        min_conjunction_size=4,
        max_conjunction_size=4,
        beam_width=16,
        evaluation_budget=500,
        top_n=3,
        min_train_support=2,
        min_validation_support=2,
        min_holdout_support=2,
        quantiles=(0.5,),
        null_hit_rate=0.5,
    )
    report = discover_rules(_dataset(40), config=config)
    candidates = report["manifests"]["candidates"]
    search = report["manifests"]["search"]

    assert candidates
    assert report["manifests"]["config"]["min_conjunction_size"] == 4
    assert report["manifests"]["config"]["max_conjunction_size"] == 4
    assert [row["size"] for row in search["levels"]] == [1, 2, 3, 4]
    assert search["budget_exhausted"] is False
    for candidate in candidates:
        assert len(candidate["clauses"]) == 4
        assert len({row["feature"] for row in candidate["clauses"]}) == 4
    for result in report["results"]["results"]:
        for split in ("train", "validation", "holdout"):
            assert "first_triggers" in result[split]
            assert "daily" in result[split]
            assert "weekly" in result[split]
            assert "league_counts" in result[split]
            resolved = result[split]["resolved"]
            assert len(result[split]["first_triggers"]) == resolved
            assert sum(row["resolved"] for row in result[split]["daily"]) == resolved
            assert sum(row["resolved"] for row in result[split]["weekly"]) == resolved
            assert sum(result[split]["league_counts"].values()) == resolved


def test_exact_four_factor_budget_exhaustion_never_returns_short_rules(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        discovery_module,
        "ATOMIC_THRESHOLD_GRIDS",
        {
            "bot.prob_to90": (">=", (50.0,)),
            "feature.season_context_factor": (">=", (1.0,)),
            "minute": ("<=", (50.0,)),
            "score.total_goals": ("<=", (2.0,)),
        },
    )
    config = DiscoveryConfig(
        train_fraction=0.50,
        validation_fraction=0.25,
        min_conjunction_size=4,
        max_conjunction_size=4,
        beam_width=4,
        evaluation_budget=1,
        top_n=3,
        min_train_support=1,
        min_validation_support=1,
        min_holdout_support=1,
        quantiles=(0.5,),
        null_hit_rate=0.5,
    )
    report = discover_rules(_dataset(), config=config)
    assert report["manifests"]["candidates"] == []
    assert report["manifests"]["search"]["budget_exhausted"] is True
    assert max(
        row["size"] for row in report["manifests"]["search"]["levels"]
    ) < 4


def test_eight_factor_shadow_search_reaches_requested_depth(monkeypatch) -> None:
    monkeypatch.setattr(
        discovery_module,
        "ATOMIC_THRESHOLD_GRIDS",
        {
            "bot.prob_to90": (">=", (50.0,)),
            "feature.adjusted_intensity": (">=", (0.1,)),
            "feature.pressure_index": (">=", (1.0,)),
            "feature.season_context_factor": (">=", (1.0,)),
            "minute": ("<=", (60.0,)),
            "feature.game_state_factor": (">=", (1.0,)),
            "feature.sample_confidence": (">=", (0.1,)),
            "score.total_goals": ("<=", (3.0,)),
        },
    )
    config = DiscoveryConfig(
        train_fraction=0.50,
        validation_fraction=0.25,
        min_conjunction_size=8,
        max_conjunction_size=8,
        beam_width=4,
        evaluation_budget=500,
        top_n=3,
        min_train_support=2,
        min_validation_support=2,
        min_holdout_support=2,
        quantiles=(0.5,),
        null_hit_rate=0.5,
    )

    report = discover_rules(_dataset(40), config=config)

    assert report["manifests"]["search"]["requested_depth_reached"] is True
    assert [
        level["size"] for level in report["manifests"]["search"]["levels"]
    ] == list(range(1, 9))
    assert report["manifests"]["candidates"]
    assert all(
        len(candidate["clauses"]) == 8
        for candidate in report["manifests"]["candidates"]
    )


def test_minimum_conjunction_size_cannot_exceed_maximum() -> None:
    with pytest.raises(ValueError, match="cannot exceed"):
        DiscoveryConfig(min_conjunction_size=4, max_conjunction_size=3)


def test_depth_budgets_reach_four_after_a_truncated_pair_level(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        discovery_module,
        "ATOMIC_THRESHOLD_GRIDS",
        {
            "bot.prob_to90": (">=", (50.0,)),
            "feature.adjusted_intensity": (">=", (0.1,)),
            "feature.pressure_index": (">=", (1.0,)),
            "feature.season_context_factor": (">=", (1.0,)),
            "minute": ("<=", (60.0,)),
            "raw.shots_in_box_total": (">=", (1.0,)),
            "raw.shots_on_target_total": (">=", (1.0,)),
            "score.total_goals": ("<=", (3.0,)),
        },
    )
    config = DiscoveryConfig(
        train_fraction=0.50,
        validation_fraction=0.25,
        min_conjunction_size=2,
        max_conjunction_size=4,
        beam_width=4,
        evaluation_budget=32,
        depth_evaluation_budgets=(20, 4, 4, 4),
        top_n=3,
        min_train_support=1,
        min_validation_support=1,
        min_holdout_support=1,
        quantiles=(0.5,),
        null_hit_rate=0.5,
    )
    first = discover_rules(_dataset(40), config=config)
    second = discover_rules(reversed(_dataset(40)), config=config)
    search = first["manifests"]["search"]
    levels = search["levels"]

    assert [row["size"] for row in levels] == [1, 2, 3, 4]
    assert levels[0]["truncated"] is False
    assert levels[1]["truncated"] is True
    assert all(row["evaluations_used"] > 0 for row in levels)
    assert all(
        row["evaluations_used"] <= row["evaluation_budget"]
        for row in levels
    )
    assert search["evaluations_used"] == sum(
        row["evaluations_used"] for row in levels
    )
    assert search["budget_exhausted"] is False
    assert search["requested_depth_reached"] is True
    assert first == second


def test_train_equivalent_thresholds_survive_until_validation(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        discovery_module,
        "ATOMIC_THRESHOLD_GRIDS",
        {"bot.prob_to90": (">=", (75.0, 79.0))},
    )
    start = datetime(2026, 8, 1, tzinfo=UTC)
    probabilities = [85.0] * 6 + [76.0, 80.0, 84.0] + [80.0] * 3
    labels = [True] * 6 + [False, True, True] + [True] * 3
    records = [
        _observation(
            8000 + index,
            50,
            start + timedelta(hours=index),
            label=labels[index],
            probability=probabilities[index],
        )
        for index in range(12)
    ]
    config = DiscoveryConfig(
        train_fraction=0.50,
        validation_fraction=0.25,
        min_conjunction_size=1,
        max_conjunction_size=1,
        beam_width=3,
        evaluation_budget=10,
        top_n=1,
        min_train_support=1,
        min_validation_support=1,
        min_holdout_support=1,
        quantiles=(0.5,),
        null_hit_rate=0.5,
    )

    report = discover_rules(records, config=config)

    assert report["manifests"]["search"][
        "train_signature_max_representatives"
    ] == 3
    assert report["manifests"]["candidates"][0]["clauses"] == [
        {
            "feature": "bot.prob_to90",
            "operator": ">=",
            "threshold": 79.0,
        }
    ]


def test_candidate_identity_includes_search_contract(monkeypatch) -> None:
    clauses = (
        discovery_module.AtomicClause("bot.prob_to90", ">=", 80.0),
    )
    current = discovery_module._candidate_id(clauses)

    monkeypatch.setattr(
        discovery_module,
        "FEATURE_SCHEMA_VERSION",
        discovery_module.FEATURE_SCHEMA_VERSION + 1,
    )

    assert discovery_module._candidate_id(clauses) != current

    monkeypatch.setattr(
        discovery_module,
        "FEATURE_SCHEMA_VERSION",
        discovery_module.FEATURE_SCHEMA_VERSION - 1,
    )
    monkeypatch.setattr(
        discovery_module,
        "DISCOVERY_ENGINE_VERSION",
        "wide_rule_discovery_test_generation",
    )

    assert discovery_module._candidate_id(clauses) != current


def test_candidate_identity_includes_executable_universe() -> None:
    clauses = (
        discovery_module.AtomicClause("bot.prob_to90", ">=", 80.0),
    )

    default_id = discovery_module._candidate_id(
        clauses,
        config=DiscoveryConfig(min_minute=46, max_minute=60),
    )
    later_window_id = discovery_module._candidate_id(
        clauses,
        config=DiscoveryConfig(min_minute=50, max_minute=60),
    )

    assert later_window_id != default_id


def test_precision_mode_builds_atoms_for_every_available_allowed_feature() -> None:
    source = _dataset(8)
    config = DiscoveryConfig(
        train_fraction=0.50,
        validation_fraction=0.25,
        min_conjunction_size=2,
        max_conjunction_size=4,
        beam_width=4,
        evaluation_budget=100,
        depth_evaluation_budgets=(40, 20, 20, 20),
        top_n=3,
        min_train_support=1,
        min_validation_support=1,
        min_holdout_support=1,
        selection_mode="precision_first",
        min_signals_per_week=1.0,
        preferred_signals_per_week=2.0,
        max_signals_per_week=3.0,
        portfolio_max_rules=2,
        quantiles=(0.5,),
        null_hit_rate=0.5,
    )
    rows = {}
    for record in source[:4]:
        row, reason = discovery_module._eligible_row(record, config)
        assert reason == ""
        assert row is not None
        rows.setdefault(row.fixture_id, []).append(row)
    clauses, manifest = discovery_module._build_atomic_clauses(rows, config)
    available = {
        name
        for name, value in rows[next(iter(rows))][0].features.items()
        if value is not None
    }
    generated = {clause.feature for clause in clauses}

    assert generated == available
    assert set(manifest) == set(ALLOWED_FEATURES)
    assert all(
        manifest[name]["effective_thresholds"] == []
        for name in set(ALLOWED_FEATURES) - available
    )
    assert manifest["composite.base_quality_v1"]["effective_thresholds"] == [
        1.0
    ]
    assert manifest["rolling.both_windows_available_v1"][
        "effective_thresholds"
    ] == [1.0]


def test_research_composites_are_not_available_to_standard_profile() -> None:
    config = DiscoveryConfig(
        train_fraction=0.50,
        validation_fraction=0.25,
        min_train_support=1,
        min_validation_support=1,
        min_holdout_support=1,
        quantiles=(0.5,),
    )
    rows = {}
    for record in _dataset(8)[:4]:
        row, reason = discovery_module._eligible_row(record, config)
        assert reason == ""
        assert row is not None
        assert "composite.base_quality_v1" in row.features
        assert "rolling.both_windows_available_v1" in row.features
        rows.setdefault(row.fixture_id, []).append(row)

    clauses, manifest = discovery_module._build_atomic_clauses(rows, config)
    generated = {clause.feature for clause in clauses}

    assert "composite.base_quality_v1" not in generated
    assert "rolling.both_windows_available_v1" not in generated
    assert "composite.base_quality_v1" not in manifest
    assert "rolling.both_windows_available_v1" not in manifest


def test_calendar_frequency_uses_full_exposure_not_trigger_weeks() -> None:
    config = _config()
    start = datetime(2026, 8, 1, tzinfo=UTC)
    records = [
        _observation(
            7000 + index,
            46,
            start + timedelta(days=day),
            label=True,
            probability=80.0,
        )
        for index, day in enumerate((0, 1, 27, 28))
    ]
    parsed = []
    fixtures = {}
    for record in records:
        row, reason = discovery_module._eligible_row(record, config)
        assert reason == ""
        assert row is not None
        parsed.append(row)
        fixtures[row.fixture_id] = [row]
    metrics = discovery_module._metrics(
        parsed[:2],
        len(fixtures),
        evaluation_span_days=discovery_module._evaluation_span_days(fixtures),
    )

    assert metrics["evaluation_span_days"] == 28.0
    assert metrics["calendar_triggers_per_week"] == 0.5
    assert metrics["triggers_per_week"] == 2.0


def test_precision_portfolio_is_frozen_before_holdout() -> None:
    config = DiscoveryConfig(
        train_fraction=0.50,
        validation_fraction=0.25,
        min_conjunction_size=1,
        max_conjunction_size=2,
        beam_width=20,
        evaluation_budget=600,
        top_n=6,
        min_train_support=2,
        min_validation_support=2,
        min_holdout_support=2,
        selection_mode="precision_first",
        min_signals_per_week=1.0,
        preferred_signals_per_week=2.0,
        max_signals_per_week=3.0,
        portfolio_max_rules=3,
        portfolio_beam_width=16,
        quantiles=(0.5,),
        null_hit_rate=0.5,
    )
    rows = _dataset(40)
    first = discover_rules(rows, config=config)
    holdout_ids = set(first["manifests"]["splits"]["holdout"]["fixture_ids"])
    changed = json.loads(json.dumps(rows))
    for record in changed:
        if record["fixture_id"] in holdout_ids:
            record["outcome"]["goal_to90_normal_time"] = not record[
                "outcome"
            ]["goal_to90_normal_time"]
    second = discover_rules(changed, config=config)

    assert first["manifests"]["selected_portfolio"] == second["manifests"][
        "selected_portfolio"
    ]
    assert first["manifests"]["best_available_portfolio"] == second[
        "manifests"
    ]["best_available_portfolio"]
    assert first["results"]["selected_portfolio"]["holdout"] != second[
        "results"
    ]["selected_portfolio"]["holdout"]
    assert first["results"]["selected_portfolio"]["holdout_confirmation"][
        "selection_locked_before_holdout"
    ] is True


def _rare_precision_config(**overrides: object) -> DiscoveryConfig:
    values: dict[str, object] = {
        "train_fraction": 0.50,
        "validation_fraction": 0.25,
        "min_conjunction_size": 1,
        "max_conjunction_size": 2,
        "beam_width": 24,
        "evaluation_budget": 200,
        "top_n": 4,
        "min_train_support": 3,
        "min_validation_support": 4,
        "min_holdout_support": 2,
        "selection_mode": "rare_precision",
        "min_signals_per_week": 10_000.0,
        "preferred_signals_per_week": 10_000.0,
        "quantiles": (0.25, 0.50, 0.75),
        "null_hit_rate": 0.5,
        "allow_feature_ranges": True,
        "validation_window_count": 4,
    }
    values.update(overrides)
    return DiscoveryConfig(**values)


def _rare_dataset(count: int = 80) -> list[dict]:
    start = datetime(2026, 8, 1, tzinfo=UTC)
    probabilities = (60.0, 70.0, 80.0, 90.0)
    rows = []
    for index in range(count):
        probability = probabilities[index % len(probabilities)]
        # Keep several distinct validation trigger signatures while ensuring
        # broad rules have support in every chronological window.
        label = probability >= 70.0
        rows.append(
            _observation(
                9000 + index,
                50,
                start + timedelta(hours=index),
                label=label,
                probability=probability,
                suffix="rare",
            )
        )
    return rows


def _only_probability_grid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        discovery_module,
        "ATOMIC_THRESHOLD_GRIDS",
        {"bot.prob_to90": (">=", ())},
    )
    monkeypatch.setattr(discovery_module, "PRECISION_EXTRA_THRESHOLD_GRIDS", {})
    monkeypatch.setattr(
        discovery_module,
        "PRECISION_COMPOSITE_THRESHOLD_GRIDS",
        {},
    )


def test_rare_precision_requires_ranges_and_exactly_four_windows() -> None:
    with pytest.raises(ValueError, match="allow_feature_ranges"):
        DiscoveryConfig(selection_mode="rare_precision", validation_window_count=4)
    with pytest.raises(ValueError, match="validation_window_count=4"):
        DiscoveryConfig(selection_mode="rare_precision", allow_feature_ranges=True)


def test_rare_precision_builds_non_contradictory_interval_seeds_from_train(
    monkeypatch,
) -> None:
    _only_probability_grid(monkeypatch)
    config = _rare_precision_config()
    rows = {}
    for record in _rare_dataset(40):
        row, reason = discovery_module._eligible_row(record, config)
        assert reason == ""
        assert row is not None
        rows[row.fixture_id] = [row]

    clauses, manifest = discovery_module._build_atomic_clauses(rows, config)
    seeds = discovery_module._interval_seed_candidates(clauses)

    assert {clause.operator for clause in clauses} == {"<=", ">="}
    assert seeds
    for seed in seeds.values():
        assert len(seed) == 2
        assert seed[0].feature == seed[1].feature == "bot.prob_to90"
        lower = next(clause.threshold for clause in seed if clause.operator == ">=")
        upper = next(clause.threshold for clause in seed if clause.operator == "<=")
        assert lower <= upper
        assert discovery_module._clauses_are_compatible(seed)
    assert manifest["bot.prob_to90"]["threshold_source_split"] == "train"
    assert manifest["bot.prob_to90"]["operators"] == ["<=", ">="]


def test_first_trigger_bitsets_equal_canonical_multiminute_matcher() -> None:
    rng = random.Random(97214)
    start = datetime(2026, 8, 1, tzinfo=UTC)
    feature_names = ("bot.prob_to90", "feature.pressure_index", "feature.adjusted_intensity")
    fixtures = {}
    for fixture_id in rng.sample(range(1000, 1040), 40):
        fixture_rows = []
        for position in range(rng.randrange(1, 8)):
            features = {
                name: rng.choice((0.0, 1.0, 2.0, 3.0, 4.0))
                for name in feature_names
                if rng.random() >= 0.2
            }
            fixture_rows.append(
                discovery_module._Row(
                    observation_id=f"{fixture_id}:{position}",
                    fixture_id=fixture_id,
                    created_at=start + timedelta(minutes=fixture_id + position),
                    minute=46 + position,
                    league_key="100",
                    # Changing labels within a match exercises selection of
                    # the actual first matching observation, not its last one.
                    label=rng.randrange(2),
                    features=features,
                )
            )
        fixtures[fixture_id] = fixture_rows
    matcher = discovery_module._FirstTriggerBitsetMatcher(fixtures)
    atoms = [
        discovery_module.AtomicClause(name, operator, threshold, True)
        for name in feature_names
        for operator in ("<=", ">=")
        for threshold in (0.0, 1.0, 2.5, 4.0, 5.0)
    ]
    conjunctions = [(), *[(atom,) for atom in atoms]]
    conjunctions.extend(
        tuple(rng.sample(atoms, rng.randrange(2, 7))) for _ in range(500)
    )
    for clauses in conjunctions:
        expected = discovery_module._first_triggers(fixtures, clauses)
        actual = matcher.first_triggers(clauses)
        assert actual == expected
        assert all(left is right for left, right in zip(actual, expected))
    assert matcher.cached_atom_count == len(atoms)
    assert discovery_module._FirstTriggerBitsetMatcher({}).first_triggers(()) == []


def test_rare_precision_discovers_successful_middle_band(monkeypatch) -> None:
    _only_probability_grid(monkeypatch)
    rows = _rare_dataset()
    for record in rows:
        probability = record["probabilities"]["prob_to90"]
        record["outcome"]["goal_to90_normal_time"] = 70 <= probability <= 80
    report = discover_rules(
        rows,
        config=_rare_precision_config(
            min_conjunction_size=2,
            beam_width=2,
            top_n=2,
        ),
    )
    candidates = report["manifests"]["candidates"]
    assert candidates
    assert report["manifests"]["search"]["rare_precision"][
        "interval_rules_selected"
    ] >= 1
    assert any(
        result["train"]["wins"] == 20
        and result["train"]["losses"] == 0
        and result["validation"]["wins"] == 10
        and result["validation"]["losses"] == 0
        for result in report["results"]["results"]
    )


def test_rare_precision_raw_train_reserve_keeps_small_perfect_candidate() -> None:
    start = datetime(2026, 8, 1, tzinfo=UTC)

    def make_row(index: int, *, validation: bool) -> discovery_module._Row:
        fixture_id = index + (1000 if validation else 0)
        rare = index % (8 if validation else 10) == 0
        return discovery_module._Row(
            observation_id=str(fixture_id),
            fixture_id=fixture_id,
            created_at=start + timedelta(hours=fixture_id),
            minute=50,
            league_key="100",
            label=int(index % (8 if validation else 12) != (7 if validation else 11)),
            features={
                "bot.prob_to90": 90.0 if rare else 60.0,
                "feature.pressure_index": float(index < (28 if validation else 100)),
                "feature.adjusted_intensity": float(index >= (4 if validation else 20)),
                "raw.total_shots_total": float(index % 6 != 0),
                "feature.season_context_factor": float(index % 6 != 1),
            },
        )

    train = {index: [make_row(index, validation=False)] for index in range(120)}
    validation = {
        index + 1000: [make_row(index, validation=True)] for index in range(32)
    }
    rare_atom = discovery_module.AtomicClause("bot.prob_to90", ">=", 85.0)
    atoms = [
        rare_atom,
        *[
            discovery_module.AtomicClause(feature, ">=", 1.0)
            for feature in (
                "feature.pressure_index",
                "feature.adjusted_intensity",
                "raw.total_shots_total",
                "feature.season_context_factor",
            )
        ],
    ]
    # All four broad candidates outrank 12/12 on Wilson: without the reserved
    # raw lane the perfect small candidate cannot even reach validation.
    wilson_ranked = sorted(
        [(atom,) for atom in atoms],
        key=lambda clauses: discovery_module._rank_key(
            discovery_module._ranking_metrics(
                discovery_module._first_triggers(train, clauses), len(train)
            ),
            clauses,
        ),
    )
    assert wilson_ranked[-1] == (rare_atom,)
    selected, diagnostics, metrics = discovery_module._search_candidates(
        train,
        validation,
        _rare_precision_config(
            max_conjunction_size=1,
            beam_width=4,
            min_train_support=12,
        ),
        atoms,
    )
    assert (rare_atom,) in selected
    rare_id = discovery_module._candidate_id(
        (rare_atom,), config=_rare_precision_config()
    )
    assert metrics["train"][rare_id]["wins"] == 12
    assert metrics["train"][rare_id]["losses"] == 0
    assert diagnostics["rare_precision"]["train_raw_precision_reserved_by_depth"] == {1: 1}


def test_rare_precision_shortlist_is_stable_has_raw_reserve_and_no_portfolio(
    monkeypatch,
) -> None:
    _only_probability_grid(monkeypatch)
    report = discover_rules(_rare_dataset(), config=_rare_precision_config())
    search = report["manifests"]["search"]
    rare = search["rare_precision"]
    candidates = report["manifests"]["candidates"]

    assert report["manifests"]["engine_version"] == (
        discovery_module.RARE_PRECISION_DISCOVERY_ENGINE_VERSION
    )
    assert report["manifests"]["threshold_grid_version"] == (
        discovery_module.RARE_PRECISION_ATOMIC_GRID_VERSION
    )
    assert candidates
    assert report["manifests"]["selected_portfolio"] is None
    assert report["manifests"]["best_available_portfolio"] is None
    assert rare["cadence_gate_applied"] is False
    assert rare["or_portfolio_enabled"] is False
    assert rare["selection_uses_holdout"] is False
    assert rare["interval_seed_pairs_available"] > 0
    assert rare["interval_seed_pairs_evaluated"] > 0
    assert rare["raw_precision_reserved_selected"] >= 1
    assert len(rare["validation_windows"]) == 4
    for result in report["results"]["results"]:
        stability = result["validation_stability"]
        assert stability["support_gate_passed"] is True
        assert stability["supported_windows"] == 4
        assert len(stability["windows"]) == 4
        assert result["frequency_gate_applied"] is False
        assert result["selection_lane"] in {
            "raw_precision_reserve",
            "wilson_stability",
        }


def test_rare_precision_validation_support_is_fail_closed_per_window(
    monkeypatch,
) -> None:
    _only_probability_grid(monkeypatch)
    rows = _rare_dataset()
    # Validation is fixtures 40..59.  The final five fixtures form the fourth
    # chronological window; lower-tail rules have aggregate support but no
    # support there and must be excluded before ranking.
    for index in range(55, 60):
        rows[index]["probabilities"]["prob_to90"] = 90.0
        rows[index]["features"]["adjusted_intensity"] = 0.9
    report = discover_rules(rows, config=_rare_precision_config())
    search = report["manifests"]["search"]

    assert search["validation_aggregate_supported"] > search[
        "validation_supported"
    ]
    assert search["rare_precision"]["min_validation_support_per_window"] == 1
    assert all(
        result["validation_stability"]["supported_windows"] == 4
        for result in report["results"]["results"]
    )


def test_rare_precision_selection_and_thresholds_are_holdout_isolated(
    monkeypatch,
) -> None:
    _only_probability_grid(monkeypatch)
    config = _rare_precision_config()
    rows = _rare_dataset()
    first = discover_rules(rows, config=config)
    holdout_ids = set(first["manifests"]["splits"]["holdout"]["fixture_ids"])
    changed = json.loads(json.dumps(rows))
    for record in changed:
        if record["fixture_id"] not in holdout_ids:
            continue
        record["probabilities"]["prob_to90"] = 1.0
        record["features"]["adjusted_intensity"] = 0.01
        record["outcome"]["goal_to90_normal_time"] = not record["outcome"][
            "goal_to90_normal_time"
        ]
    second = discover_rules(changed, config=config)

    assert first["manifests"]["threshold_manifest"] == second["manifests"][
        "threshold_manifest"
    ]
    assert first["manifests"]["search"] == second["manifests"]["search"]
    assert first["manifests"]["candidates"] == second["manifests"]["candidates"]
    selection_fields = (
        "candidate_id",
        "rank_at_discovery",
        "train",
        "validation",
        "selection_lane",
        "validation_stability",
    )
    assert [
        {key: result[key] for key in selection_fields}
        for result in first["results"]["results"]
    ] == [
        {key: result[key] for key in selection_fields}
        for result in second["results"]["results"]
    ]
    assert [result["holdout"] for result in first["results"]["results"]] != [
        result["holdout"] for result in second["results"]["results"]
    ]


def test_universe_accepts_wide_monitor_but_rejects_other_stages() -> None:
    rows = _dataset()
    rows[0]["stage"] = "wide_monitor"
    rows[1]["stage"] = "prefilter"
    report = discover_rules(rows, config=_config())
    assert report["manifests"]["input"]["skipped"]["stage"] == 1
    assert report["manifests"]["input"]["eligible_observations"] == len(rows) - 1


def test_disk_join_requires_exact_causal_prediction() -> None:
    created = datetime(2026, 8, 10, 12, tzinfo=UTC)
    observation = _observation(
        77, 46, created, label=True, probability=80.0, suffix="joined"
    )
    outcome = {
        "record_type": "observation_outcome",
        "observation_id": observation["observation_id"],
        "outcome_schema_version": 1,
        "created_at_utc": (created + timedelta(hours=2)).isoformat(),
        "outcome": observation.pop("outcome"),
    }

    def prediction(key: str, *, observation_time: datetime, production: bool) -> dict:
        return {
            "record_type": "shadow_ml_prediction",
            "prediction_key": key,
            "observation_id": observation["observation_id"],
            "fixture_id": observation["fixture_id"],
            "minute": observation["minute"],
            "observation_created_at_utc": observation_time.isoformat(),
            "created_at_utc": (created + timedelta(seconds=5)).isoformat(),
            "model_created_at_utc": (created - timedelta(hours=1)).isoformat(),
            "model_data_cutoff_utc": (created - timedelta(hours=2)).isoformat(),
            "prediction_status": "ok",
            "shadow_only": True,
            "production_applied": production,
            "predictions": {
                "to90": {
                    "status": "ok",
                    "calibrated_probability_pct": 82.0,
                    "production_applied": False,
                }
            },
        }

    with JournalJoinStore(config=_config()) as store:
        store.ingest_observation_records([observation, outcome])
        store.ingest_predictions(
            [
                prediction("wrong-time", observation_time=created + timedelta(seconds=1), production=False),
                prediction("production", observation_time=created, production=True),
                prediction("valid", observation_time=created, production=False),
            ],
            source="static",
        )
        joined = list(store.joined_observations())
        assert store.diagnostics()["joined"] == 1
    assert joined[0]["outcome"]["goal_to90_normal_time"] is True
    assert (
        joined[0]["_wide_predictions"]["static"]["predictions"]["to90"][
            "calibrated_probability_pct"
        ]
        == 82.0
    )


def test_output_collision_and_immutable_atomic_write(tmp_path: Path) -> None:
    source = tmp_path / "observations.jsonl"
    source.write_text("", encoding="utf-8")
    archive = tmp_path / "observations.20260810T120000Z.jsonl.gz"
    archive.write_bytes(b"")
    with pytest.raises(ValueError):
        assert_safe_outputs((source, None), sources=(source,))
    with pytest.raises(ValueError):
        assert_safe_outputs((archive, None), sources=(source,))
    with pytest.raises(ValueError):
        assert_safe_outputs((tmp_path / "same.json", tmp_path / "same.json"), sources=(source,))

    output = tmp_path / "artifact.json"
    write_immutable_atomic(output, {"value": 1})
    write_immutable_atomic(output, {"value": 1})
    with pytest.raises(FileExistsError):
        write_immutable_atomic(output, {"value": 2})


def test_cli_streams_join_and_writes_two_artifacts(tmp_path: Path) -> None:
    observations = tmp_path / "observations.jsonl"
    rows = _dataset()
    records = []
    for row in rows:
        outcome = row.pop("outcome")
        records.extend(
            [
                row,
                {
                    "record_type": "observation_outcome",
                    "observation_id": row["observation_id"],
                    "outcome_schema_version": 1,
                    "created_at_utc": outcome["resolved_at_utc"],
                    "outcome": outcome,
                },
            ]
        )
    observations.write_text(
        "".join(json.dumps(record) + "\n" for record in records), encoding="utf-8"
    )
    manifest = tmp_path / "manifests.json"
    results = tmp_path / "results.json"
    assert (
        cli.main(
            [
                "--observations",
                str(observations),
                "--manifest-output",
                str(manifest),
                "--results-output",
                str(results),
                "--train-fraction",
                "0.5",
                "--validation-fraction",
                "0.25",
                "--max-conjunction-size",
                "1",
                "--beam-width",
                "10",
                "--evaluation-budget",
                "200",
                "--top-n",
                "3",
                "--min-train-support",
                "2",
                "--min-validation-support",
                "2",
                "--min-holdout-support",
                "2",
                "--null-hit-rate",
                "0.5",
                "--stdout",
                "none",
            ]
        )
        == 0
    )
    assert json.loads(manifest.read_text(encoding="utf-8"))["artifact_type"] == (
        "wide_rule_candidate_manifests"
    )
    assert json.loads(results.read_text(encoding="utf-8"))["artifact_type"] == (
        "wide_rule_candidate_results"
    )
