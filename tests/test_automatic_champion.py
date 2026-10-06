from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from wide_research import FEATURE_SCHEMA_VERSION, Clause, RuleManifest
from wide_research.automatic_champion import (
    AutomaticChampionController,
    AutomaticChampionLayer,
    AutomaticChampionPolicy,
    AutomaticChampionUnavailable,
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


def test_default_policy_promotes_only_after_full_prospective_gates(
    tmp_path,
) -> None:
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

    for day in range(20):
        for slot, (probability, won) in enumerate(
            (
                (90.0, True),
                (50.0, False),
                (90.0, True),
                (50.0, False),
            )
        ):
            observed = started + timedelta(
                days=day + 1,
                hours=slot,
            )
            snapshot = _snapshot(
                1000 + day * 4 + slot,
                probability,
                observed,
                league_id=(day % 8) + 1,
            )
            layer.process_snapshot(snapshot)
            _resolve(layer, snapshot, observed, won=won)

    result = controller.reconcile(
        now_utc=(started + timedelta(days=22)).isoformat()
    )
    challenger_row = next(
        row
        for row in result["evaluations"]
        if row["portfolio_id"] == "challenger"
    )

    assert challenger_row["metrics"]["resolved"] == 40
    assert challenger_row["eligible"] is True
    assert result["switched"] is True
    assert result["active_portfolio_id"] == "challenger"


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
    with pytest.raises(AutomaticChampionUnavailable):
        changed.active_spec()


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


def test_degraded_champion_returns_to_baseline_atomically(tmp_path) -> None:
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
        policy=_permissive_policy(),
    )
    controller.reconcile(now_utc=started.isoformat())

    for index, (probability, won) in enumerate(
        (
            (90.0, True),
            (50.0, False),
            (90.0, True),
            (50.0, False),
            (90.0, True),
            (90.0, True),
        )
    ):
        observed = started + timedelta(days=index + 1)
        snapshot = _snapshot(
            400 + index,
            probability,
            observed,
            league_id=(index % 2) + 1,
        )
        layer.process_snapshot(snapshot)
        _resolve(layer, snapshot, observed, won=won)

    promoted = controller.reconcile(
        now_utc=(started + timedelta(days=8)).isoformat()
    )
    assert promoted["active_portfolio_id"] == "challenger"

    for index, won in enumerate((False, False, False, True)):
        observed = started + timedelta(days=9 + index)
        snapshot = _snapshot(
            500 + index,
            90.0,
            observed,
            league_id=(index % 2) + 1,
        )
        layer.process_snapshot(snapshot)
        _resolve(layer, snapshot, observed, won=won)
        baseline_only_observed = observed + timedelta(hours=1)
        baseline_only = _snapshot(
            550 + index,
            50.0,
            baseline_only_observed,
            league_id=(index % 2) + 1,
        )
        layer.process_snapshot(baseline_only)
        _resolve(
            layer,
            baseline_only,
            baseline_only_observed,
            won=True,
        )

    recovered = controller.reconcile(
        now_utc=(started + timedelta(days=14)).isoformat()
    )

    assert recovered["switched"] is True
    assert recovered["switch_reason"] == "degraded_champion_fallback"
    assert recovered["degradation_fallback"] is True
    assert recovered["active_portfolio_id"] == "baseline"
    assert recovered["publication_suspended"] is False
    pointer = store.get_active_pointer("automatic_publication_champion")
    assert pointer["rule_id"] == baseline.rule_id
    assert pointer["payload"]["publication_suspended"] is False
    assert controller.active_selection().spec.portfolio_id == "baseline"


def test_degraded_baseline_suspends_until_hysteresis_recovery(tmp_path) -> None:
    baseline = _spec("baseline", 0.0)
    challenger = _spec("challenger", 80.0)
    specs = (baseline, challenger)
    families = {"baseline": "baseline", "challenger": "challenger"}
    store = _store(tmp_path)
    layer = AutomaticChampionLayer(store, specs, families=families)
    started = datetime.now(UTC) + timedelta(seconds=1)
    policy = AutomaticChampionPolicy(
        **{
            **_permissive_policy().manifest(),
            "degradation_recovery_rate": 0.75,
        }
    )
    controller = AutomaticChampionController(
        store,
        specs,
        families=families,
        baseline_portfolio_id="baseline",
        production_enabled=True,
        policy=policy,
    )
    controller.reconcile(now_utc=started.isoformat())

    for index, won in enumerate((False, False, False, True)):
        observed = started + timedelta(days=index + 1)
        snapshot = _snapshot(
            600 + index,
            50.0,
            observed,
            league_id=(index % 2) + 1,
        )
        layer.process_snapshot(snapshot)
        _resolve(layer, snapshot, observed, won=won)

    suspended = controller.reconcile(
        now_utc=(started + timedelta(days=6)).isoformat()
    )
    assert suspended["status"] == "suspended"
    assert suspended["publication_suspended"] is True
    assert suspended["suspension_changed"] is True
    with pytest.raises(AutomaticChampionUnavailable):
        controller.active_spec()

    for index in range(4):
        observed = started + timedelta(days=7 + index)
        snapshot = _snapshot(
            700 + index,
            50.0,
            observed,
            league_id=(index % 2) + 1,
        )
        layer.process_snapshot(snapshot)
        _resolve(layer, snapshot, observed, won=True)

    resumed = controller.reconcile(
        now_utc=(started + timedelta(days=12)).isoformat()
    )
    assert resumed["status"] == "ok"
    assert resumed["publication_suspended"] is False
    assert resumed["suspension_changed"] is True
    assert controller.active_spec().portfolio_id == "baseline"


def test_selection_generation_change_fails_closed(tmp_path) -> None:
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
        production_enabled=True,
        policy=_permissive_policy(),
    )
    controller.reconcile(now_utc=started.isoformat())
    selection = controller.active_selection()
    pointer = store.get_active_pointer("automatic_publication_champion")
    store.set_active_pointer(
        "automatic_publication_champion",
        phase_id=baseline.phase_id,
        expected_generation=pointer["generation"],
        metadata=pointer["payload"],
        updated_at_utc=(started + timedelta(seconds=1)).isoformat(),
    )

    with pytest.raises(
        AutomaticChampionUnavailable,
        match="changed during publication evaluation",
    ):
        controller.confirm_selection(selection)


def test_staged_production_mode_change_reauthorizes_pointer(tmp_path) -> None:
    baseline = _spec("baseline", 0.0)
    challenger = _spec("challenger", 80.0)
    specs = (baseline, challenger)
    families = {"baseline": "baseline", "challenger": "challenger"}
    store = _store(tmp_path)
    AutomaticChampionLayer(store, specs, families=families)
    started = datetime.now(UTC) + timedelta(seconds=1)
    report_only = AutomaticChampionController(
        store,
        specs,
        families=families,
        baseline_portfolio_id="baseline",
        production_enabled=False,
        policy=_permissive_policy(),
    )
    first = report_only.reconcile(now_utc=started.isoformat())

    live = AutomaticChampionController(
        store,
        specs,
        families=families,
        baseline_portfolio_id="baseline",
        production_enabled=True,
        policy=_permissive_policy(),
    )
    second = live.reconcile(
        now_utc=(started + timedelta(seconds=1)).isoformat()
    )

    assert second["status"] == "ok"
    assert second["selector_started_at_utc"] == first["selector_started_at_utc"]
    assert live.active_selection().generation == 2
