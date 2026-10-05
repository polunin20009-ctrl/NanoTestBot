from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from wide_research import FEATURE_SCHEMA_VERSION, Clause, RuleManifest
from wide_research.automatic_champion import (
    AutomaticChampionController,
    AutomaticChampionLayer,
    AutomaticChampionPolicy,
)
from wide_research.portfolio import (
    FrozenPortfolioMember,
    FrozenPortfolioSpec,
)
from wide_research.schema import canonical_hash
from wide_research.store import InvalidLifecycleTransition, WideResearchStore


UTC = timezone.utc


def _member(rule_id: str, threshold: float) -> FrozenPortfolioMember:
    return FrozenPortfolioMember(
        source_profile="test",
        source_store_hash=f"store-{rule_id}",
        manifest=RuleManifest(
            rule_id,
            "v1",
            (Clause("bot.prob_to90", ">=", threshold),),
            feature_schema_version=FEATURE_SCHEMA_VERSION,
        ),
    )


def _spec(portfolio_id: str, threshold: float) -> FrozenPortfolioSpec:
    return FrozenPortfolioSpec(
        portfolio_id,
        "v1",
        (_member(f"rule-{portfolio_id}", threshold),),
    )


def _snapshot(
    fixture_id: int,
    probability: float,
    observed: datetime,
    *,
    league_id: int,
) -> dict:
    return {
        "record_type": "observation",
        "observation_id": f"{fixture_id}:50:BLOCK:v3",
        "fixture_id": fixture_id,
        "created_at_utc": observed.isoformat(),
        "stage": "decision_pipeline",
        "minute": 50,
        "match": {
            "score_home": 0,
            "score_away": 0,
            "league_id": league_id,
        },
        "probabilities": {"prob_to90": probability},
        "features": {},
        "raw_metrics": {},
        "availability": {},
        "gates": {
            "readiness_passed": True,
            "publication_context_passed": True,
        },
        "publication_policy": {"publication_context_passed": True},
    }


def _resolve(
    layer: AutomaticChampionLayer,
    snapshot: dict,
    observed: datetime,
    *,
    won: bool,
) -> None:
    layer.process_outcomes(
        [
            {
                "observation_id": snapshot["observation_id"],
                "outcome_schema_version": 1,
                "created_at_utc": (observed + timedelta(hours=2)).isoformat(),
                "outcome": {
                    "status": "resolved",
                    "goal_to90_normal_time": won,
                    "resolved_at_utc": (
                        observed + timedelta(hours=2)
                    ).isoformat(),
                },
            }
        ]
    )


def _store(tmp_path) -> WideResearchStore:
    store = WideResearchStore(
        tmp_path / "automatic.sqlite3", allowed_root=tmp_path
    )
    store.bind_profile("automatic_champion")
    return store


def _permissive_policy() -> AutomaticChampionPolicy:
    return AutomaticChampionPolicy(
        prior_hit_rate=0.5,
        prior_strength=2.0,
        min_resolved=4,
        min_span_days=1.0,
        min_trigger_days=2,
        min_leagues=2,
        max_league_share=1.0,
        min_triggers_per_week=0.0,
        min_posterior_lower=0.5,
        superiority_margin=0.0,
        superiority_probability=0.55,
        champion_min_tenure_days=1.0,
        degradation_window=4,
        degradation_rate=0.5,
    )


def test_short_perfect_run_cannot_replace_baseline(tmp_path) -> None:
    baseline = _spec("baseline", 0.0)
    challenger = _spec("challenger", 80.0)
    specs = (baseline, challenger)
    families = {"baseline": "baseline", "challenger": "challenger"}
    store = _store(tmp_path)
    layer = AutomaticChampionLayer(store, specs, families=families)
    started = datetime.now(UTC) + timedelta(seconds=1)
    controller = AutomaticChampionController(
        store,
        specs,
        families=families,
        baseline_portfolio_id="baseline",
        production_enabled=True,
    )
    controller.reconcile(now_utc=started.isoformat())

    for index in range(9):
        observed = started + timedelta(hours=index + 1)
        snapshot = _snapshot(
            100 + index,
            90.0,
            observed,
            league_id=index + 1,
        )
        layer.process_snapshot(snapshot)
        _resolve(layer, snapshot, observed, won=True)

    result = controller.reconcile(
        now_utc=(started + timedelta(days=15)).isoformat()
    )

    challenger_row = next(
        row
        for row in result["evaluations"]
        if row["portfolio_id"] == "challenger"
    )
    assert challenger_row["metrics"]["wins"] == 9
    assert "insufficient_resolved" in challenger_row["reasons"]
    assert result["active_portfolio_id"] == "baseline"
    assert result["switched"] is False


def test_strong_mature_challenger_swaps_pointer_atomically(tmp_path) -> None:
    baseline = _spec("baseline", 0.0)
    challenger = _spec("challenger", 80.0)
    specs = (baseline, challenger)
    families = {"baseline": "baseline", "challenger": "challenger"}
    store = _store(tmp_path)
    layer = AutomaticChampionLayer(store, specs, families=families)
    started = datetime.now(UTC) + timedelta(seconds=1)
    report_path = tmp_path / "automatic.json"
    controller = AutomaticChampionController(
        store,
        specs,
        families=families,
        baseline_portfolio_id="baseline",
        report_path=str(report_path),
        production_enabled=True,
        policy=_permissive_policy(),
    )
    first = controller.reconcile(now_utc=started.isoformat())
    assert first["active_portfolio_id"] == "baseline"

    outcomes = (
        (90.0, True),
        (50.0, False),
        (90.0, True),
        (50.0, False),
        (90.0, True),
        (90.0, True),
    )
    for index, (probability, won) in enumerate(outcomes):
        observed = started + timedelta(days=index + 1)
        snapshot = _snapshot(
            200 + index,
            probability,
            observed,
            league_id=(index % 2) + 1,
        )
        layer.process_snapshot(snapshot)
        _resolve(layer, snapshot, observed, won=won)

    result = controller.reconcile(
        now_utc=(started + timedelta(days=8)).isoformat()
    )

    assert result["switched"] is True
    assert result["active_portfolio_id"] == "challenger"
    pointer = store.get_active_pointer("automatic_publication_champion")
    assert pointer["rule_id"] == challenger.rule_id
    assert pointer["generation"] == 2
    phases = {row["rule_id"]: row for row in store.list_phases()}
    assert phases[baseline.rule_id]["status"] == "shadow"
    assert phases[challenger.rule_id]["status"] == "active"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    checksum = report.pop("checksum")
    assert checksum == canonical_hash(report)


def test_same_family_never_causes_threshold_churn(tmp_path) -> None:
    baseline = _spec("baseline", 0.0)
    challenger = _spec("challenger", 80.0)
    specs = (baseline, challenger)
    families = {"baseline": "same-template", "challenger": "same-template"}
    store = _store(tmp_path)
    layer = AutomaticChampionLayer(store, specs, families=families)
    started = datetime.now(UTC) + timedelta(seconds=1)
    controller = AutomaticChampionController(
        store,
        specs,
        families=families,
        baseline_portfolio_id="baseline",
        production_enabled=True,
        policy=_permissive_policy(),
    )
    controller.reconcile(now_utc=started.isoformat())

    for index, (probability, won) in enumerate(
        ((90.0, True), (50.0, False)) * 4
    ):
        observed = started + timedelta(days=index + 1)
        snapshot = _snapshot(
            300 + index,
            probability,
            observed,
            league_id=(index % 2) + 1,
        )
        layer.process_snapshot(snapshot)
        _resolve(layer, snapshot, observed, won=won)

    result = controller.reconcile(
        now_utc=(started + timedelta(days=10)).isoformat()
    )
    challenger_row = next(
        row
        for row in result["evaluations"]
        if row["portfolio_id"] == "challenger"
    )
    assert "same_family_as_champion" in challenger_row["reasons"]
    assert result["switched"] is False


def test_policy_change_blocks_switch_but_keeps_last_valid_champion(tmp_path) -> None:
    baseline = _spec("baseline", 0.0)
    challenger = _spec("challenger", 80.0)
    specs = (baseline, challenger)
    families = {"baseline": "baseline", "challenger": "challenger"}
    store = _store(tmp_path)
    AutomaticChampionLayer(store, specs, families=families)
    started = datetime.now(UTC) + timedelta(seconds=1)
    original = AutomaticChampionController(
        store,
        specs,
        families=families,
        baseline_portfolio_id="baseline",
        production_enabled=True,
        policy=_permissive_policy(),
    )
    original.reconcile(now_utc=started.isoformat())

    changed = AutomaticChampionController(
        store,
        specs,
        families=families,
        baseline_portfolio_id="baseline",
        production_enabled=True,
        policy=AutomaticChampionPolicy(
            **{
                **_permissive_policy().manifest(),
                "min_resolved": 5,
            }
        ),
    )
    result = changed.reconcile(
        now_utc=(started + timedelta(days=2)).isoformat()
    )

    assert result["status"] == "blocked"
    assert "policy_hash_mismatch" in result["contract_reasons"]
    assert changed.active_spec().portfolio_id == "baseline"


def test_invalid_atomic_replacement_rolls_back_everything(tmp_path) -> None:
    baseline = _spec("baseline", 0.0)
    challenger = _spec("challenger", 80.0)
    specs = (baseline, challenger)
    families = {"baseline": "baseline", "challenger": "challenger"}
    store = _store(tmp_path)
    AutomaticChampionLayer(store, specs, families=families)
    started = datetime.now(UTC) + timedelta(seconds=1)
    controller = AutomaticChampionController(
        store,
        specs,
        families=families,
        baseline_portfolio_id="baseline",
    )
    controller.reconcile(now_utc=started.isoformat())
    store.transition_phase(
        challenger.phase_id,
        "retired",
        expected_status="shadow",
        reason="test_terminal",
    )

    with pytest.raises(InvalidLifecycleTransition):
        store.swap_active_pointer(
            "automatic_publication_champion",
            from_phase_id=baseline.phase_id,
            to_phase_id=challenger.phase_id,
            expected_generation=1,
        )

    pointer = store.get_active_pointer("automatic_publication_champion")
    assert pointer["rule_id"] == baseline.rule_id
    phases = {row["rule_id"]: row for row in store.list_phases()}
    assert phases[baseline.rule_id]["status"] == "active"
    assert phases[challenger.rule_id]["status"] == "retired"
