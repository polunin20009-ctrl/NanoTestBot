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
    discovery_nomination_family,
    import_ready_discovery_evidence,
    retire_exhausted_discovery_nominations,
    select_ready_discovery_nominations,
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


def _source_store(tmp_path) -> WideResearchStore:
    store = WideResearchStore(
        tmp_path / "precision.sqlite3", allowed_root=tmp_path
    )
    store.bind_profile("precision_shadow")
    return store


def _permissive_policy() -> AutomaticChampionPolicy:
    return AutomaticChampionPolicy(
        prior_hit_rate=0.5,
        prior_strength=2.0,
        min_resolved=4,
        min_span_days=1.0,
        superiority_margin=0.0,
        degradation_window=4,
        degradation_rate=0.5,
    )


def test_ready_discovery_rule_is_frozen_into_a_fresh_duel(tmp_path) -> None:
    source = _source_store(tmp_path)
    manifest = RuleManifest(
        "wide-ready",
        "v1",
        (Clause("bot.prob_to90", ">=", 88.0),),
        feature_schema_version=FEATURE_SCHEMA_VERSION,
    )
    source.register_rule_and_phase(
        rule_id=manifest.rule_id,
        manifest=manifest.as_dict(),
        phase_id="wide-ready:prospective",
        status="ready",
        starts_at_utc=(datetime.now(UTC) - timedelta(days=30)).isoformat(),
        policy={"prospective": True},
    )
    target = _store(tmp_path)

    selected = select_ready_discovery_nominations(
        source,
        target_store=target,
        source_profile="precision_shadow",
        existing_specs=(),
        max_active=5,
    )

    assert len(selected) == 1
    nomination = selected[0]
    assert nomination.members[0].manifest == manifest
    assert nomination.members[0].source_profile == "precision_shadow"
    assert discovery_nomination_family(nomination).startswith("discovery-")
    before = datetime.now(UTC)
    AutomaticChampionLayer(
        target,
        (nomination,),
        families={nomination.portfolio_id: discovery_nomination_family(nomination)},
    )
    admitted = target.list_phases(rule_id=nomination.rule_id)[0]
    assert datetime.fromisoformat(admitted["starts_at_utc"]) >= before
    assert target.list_triggers(phase_id=nomination.phase_id) == []


def test_ready_discovery_evidence_is_reused_without_historical_backfill(
    tmp_path,
) -> None:
    source = _source_store(tmp_path)
    started = datetime.now(UTC) - timedelta(days=12)
    manifest = RuleManifest(
        "wide-certified",
        "v1",
        (Clause("bot.prob_to90", ">=", 88.0),),
        feature_schema_version=FEATURE_SCHEMA_VERSION,
    )
    source.register_rule_and_phase(
        rule_id=manifest.rule_id,
        manifest=manifest.as_dict(),
        phase_id="wide-certified:prospective",
        status="ready",
        starts_at_utc=started.isoformat(),
        policy={"prospective": True},
        created_at_utc=started.isoformat(),
    )
    for index, won in enumerate((True, False)):
        observed = started + timedelta(days=index + 1)
        observation_id = f"certified-{index}"
        source.claim_first_trigger(
            phase_id="wide-certified:prospective",
            rule_id=manifest.rule_id,
            fixture_id=100 + index,
            observation_id=observation_id,
            triggered_at_utc=observed.isoformat(),
            minute=50,
            league=index + 1,
        )
        source.attach_outcomes(
            {
                "observation_id": observation_id,
                "outcome_version": 1,
                "outcome_status": "win" if won else "loss",
                "outcome_at_utc": (
                    observed + timedelta(hours=2)
                ).isoformat(),
                "payload": {"source": "prospective_test"},
            }
        )
    target = _store(tmp_path)
    nomination = select_ready_discovery_nominations(
        source,
        target_store=target,
        source_profile="precision_shadow",
        existing_specs=(),
        max_active=1,
    )[0]
    family = discovery_nomination_family(nomination)

    imported = import_ready_discovery_evidence(
        source, target, nomination, family_id=family
    )
    repeated = import_ready_discovery_evidence(
        source, target, nomination, family_id=family
    )
    AutomaticChampionLayer(
        target,
        (nomination,),
        families={nomination.portfolio_id: family},
    )

    assert imported["claimed"] == 2
    assert imported["outcomes"] == 2
    metrics = target.metrics_for_phase(nomination.phase_id)
    assert metrics["total"] == 2
    assert metrics["pending"] == 0
    assert metrics["win"] == 1
    assert metrics["loss"] == 1
    assert metrics["invalid"] == 0
    phase = target.list_phases(rule_id=nomination.rule_id)[0]
    assert datetime.fromisoformat(phase["starts_at_utc"]) == started
    assert phase["policy"]["automatic_champion"][
        "ready_evidence_imported"
    ] is True
    assert repeated["claimed"] == 0
    assert repeated["inserted_outcomes"] == 0


def test_append_only_candidate_catalog_keeps_active_champion_valid(
    tmp_path,
) -> None:
    baseline = _spec("baseline", 0.0)
    first = _spec("first", 80.0)
    store = _store(tmp_path)
    initial_specs = (baseline, first)
    initial_families = {"baseline": "baseline", "first": "first"}
    AutomaticChampionLayer(store, initial_specs, families=initial_families)
    started = datetime.now(UTC) + timedelta(seconds=1)
    initial = AutomaticChampionController(
        store,
        initial_specs,
        families=initial_families,
        baseline_portfolio_id="baseline",
        production_enabled=True,
    )
    first_result = initial.reconcile(now_utc=started.isoformat())
    active_since = first_result["active_since_utc"]

    added = _spec("added", 90.0)
    extended_specs = (*initial_specs, added)
    extended_families = {**initial_families, "added": "added"}
    AutomaticChampionLayer(store, extended_specs, families=extended_families)
    extended = AutomaticChampionController(
        store,
        extended_specs,
        families=extended_families,
        baseline_portfolio_id="baseline",
        production_enabled=True,
    )

    assert extended.active_selection().spec == baseline
    refreshed = extended.reconcile(
        now_utc=(started + timedelta(minutes=1)).isoformat()
    )
    assert refreshed["status"] == "ok"
    assert refreshed["active_portfolio_id"] == "baseline"
    assert refreshed["active_since_utc"] == active_since
    assert extended.active_selection().generation == 2


def test_failed_dynamic_duel_frees_its_slot_at_fixed_horizon(tmp_path) -> None:
    target = _store(tmp_path)
    nomination = FrozenPortfolioSpec(
        "discovery-ready-precision-shadow-wide-failed",
        "v1",
        (_member("wide-failed", 90.0),),
    )
    AutomaticChampionLayer(
        target,
        (nomination,),
        families={nomination.portfolio_id: "discovery-shape"},
    )
    decision = {
        "status": "ok",
        "active_rule_id": "another-rule",
        "evaluations": [
            {
                "rule_id": nomination.rule_id,
                "portfolio_id": nomination.portfolio_id,
                "eligible": False,
                "metrics": {"resolved": 200},
                "reasons": ["multiple_testing_superiority_gate"],
            }
        ],
    }

    retired = retire_exhausted_discovery_nominations(target, decision)

    assert retired == (nomination.phase_id,)
    assert target.list_phases(rule_id=nomination.rule_id)[0]["status"] == (
        "rejected"
    )


def test_fast_dynamic_duel_keeps_collecting_until_calendar_gate(tmp_path) -> None:
    target = _store(tmp_path)
    nomination = FrozenPortfolioSpec(
        "discovery-ready-precision-shadow-wide-fast",
        "v1",
        (_member("wide-fast", 90.0),),
    )
    AutomaticChampionLayer(
        target,
        (nomination,),
        families={nomination.portfolio_id: "discovery-shape"},
    )
    decision = {
        "status": "ok",
        "active_rule_id": "another-rule",
        "evaluations": [
            {
                "rule_id": nomination.rule_id,
                "portfolio_id": nomination.portfolio_id,
                "eligible": False,
                "metrics": {"resolved": 200},
                "reasons": [
                    "insufficient_span_days",
                    "multiple_testing_superiority_gate",
                ],
            }
        ],
    }

    assert retire_exhausted_discovery_nominations(target, decision) == ()
    assert target.list_phases(rule_id=nomination.rule_id)[0]["status"] == (
        "shadow"
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
            ((90.0, True),) * 10 + ((50.0, False),) * 2
        ):
            observed = started + timedelta(
                days=day + 1,
                hours=slot,
            )
            snapshot = _snapshot(
                1000 + day * 20 + slot,
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

    assert challenger_row["metrics"]["resolved"] == 200
    assert challenger_row["metrics"]["hit_rate"] == 1.0
    assert challenger_row["eligible"] is True
    assert result["switched"] is True
    assert result["active_portfolio_id"] == "challenger"


@pytest.mark.parametrize(
    (
        "challenger_wins",
        "baseline_only_losses",
        "expected_switch",
        "expected_reason",
    ),
    (
        (180, 4, False, "hit_rate_margin_below_gate"),
        (178, 5, True, None),
    ),
)
def test_default_policy_uses_relative_margin_without_absolute_hit_rate(
    tmp_path,
    challenger_wins: int,
    baseline_only_losses: int,
    expected_switch: bool,
    expected_reason: str | None,
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

    for index in range(200):
        observed = started + timedelta(minutes=index)
        snapshot = _snapshot(
            2000 + index,
            90.0,
            observed,
            league_id=1,
        )
        layer.process_snapshot(snapshot)
        _resolve(
            layer,
            snapshot,
            observed,
            won=index < challenger_wins,
        )
    for index in range(baseline_only_losses):
        observed = started + timedelta(minutes=240 + index)
        baseline_only = _snapshot(
            3000 + index,
            50.0,
            observed,
            league_id=1,
        )
        layer.process_snapshot(baseline_only)
        _resolve(layer, baseline_only, observed, won=False)

    too_early = controller.reconcile(
        now_utc=(started + timedelta(days=9)).isoformat()
    )
    early_challenger = next(
        row
        for row in too_early["evaluations"]
        if row["portfolio_id"] == "challenger"
    )
    assert "insufficient_span_days" in early_challenger["reasons"]
    assert too_early["switched"] is False

    result = controller.reconcile(
        now_utc=(started + timedelta(days=12)).isoformat()
    )
    challenger_row = next(
        row
        for row in result["evaluations"]
        if row["portfolio_id"] == "challenger"
    )

    assert challenger_row["metrics"]["resolved"] == 200
    assert challenger_row["metrics"]["hit_rate"] == (
        challenger_wins / 200
    )
    assert challenger_row["metrics"]["trigger_days"] == 1
    assert challenger_row["metrics"]["leagues"] == 1
    assert challenger_row["metrics"]["span_days"] < 1.0
    assert challenger_row["metrics"]["elapsed_days"] == 12.0
    assert result["switched"] is expected_switch
    if expected_reason is None:
        assert challenger_row["metrics"]["hit_rate"] < 0.90
        assert challenger_row["hit_rate_advantage"] >= 0.02
        assert challenger_row["eligible"] is True
        assert (
            challenger_row["required_superiority_probability"] is None
        )
    else:
        assert challenger_row["hit_rate_advantage"] < 0.02
        assert expected_reason in challenger_row["reasons"]


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


@pytest.mark.parametrize(
    "previous_version",
    ("automatic_champion_v2", "automatic_champion_v3"),
)
def test_supported_policy_migrates_without_resetting_active_champion(
    tmp_path,
    previous_version: str,
) -> None:
    baseline = _spec("baseline", 0.0)
    challenger = _spec("challenger", 80.0)
    specs = (baseline, challenger)
    families = {"baseline": "baseline", "challenger": "challenger"}
    store = _store(tmp_path)
    AutomaticChampionLayer(store, specs, families=families)
    started = datetime.now(UTC) + timedelta(seconds=1)
    old = AutomaticChampionController(
        store,
        specs,
        families=families,
        baseline_portfolio_id="baseline",
        production_enabled=True,
        policy=AutomaticChampionPolicy(version=previous_version),
    )
    first = old.reconcile(now_utc=started.isoformat())

    current = AutomaticChampionController(
        store,
        specs,
        families=families,
        baseline_portfolio_id="baseline",
        production_enabled=True,
    )
    migrated = current.reconcile(
        now_utc=(started + timedelta(minutes=1)).isoformat()
    )

    assert migrated["status"] == "ok"
    assert migrated["active_portfolio_id"] == "baseline"
    assert migrated["active_since_utc"] == first["active_since_utc"]
    assert current.active_selection().generation == 2
