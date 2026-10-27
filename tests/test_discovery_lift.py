from __future__ import annotations

from datetime import datetime, timedelta, timezone

from wide_research import FEATURE_SCHEMA_VERSION, Clause, RuleManifest
from wide_research.automatic_champion import (
    discovery_nomination_family,
    import_ready_discovery_evidence,
)
from wide_research.discovery_lift import (
    DiscoveryLiftPolicy,
    plan_discovery_division_lifts,
    run_discovery_division_lift,
)
from wide_research.portfolio import FrozenPortfolioMember, FrozenPortfolioSpec
from wide_research.store import WideResearchStore


UTC = timezone.utc


def _manifest(rule_id: str, threshold: float) -> RuleManifest:
    return RuleManifest(
        rule_id,
        "v1",
        (Clause("bot.prob_to90", ">=", threshold),),
        feature_schema_version=FEATURE_SCHEMA_VERSION,
    )


def _seed_phase(
    store: WideResearchStore,
    *,
    rule_id: str,
    threshold: float,
    status: str,
    wins: int,
    losses: int,
    started: datetime,
    prefix: str = "seed",
) -> str:
    manifest = _manifest(rule_id, threshold)
    phase_id = f"{rule_id}:prospective"
    store.register_rule_and_phase(
        rule_id=rule_id,
        manifest=manifest.as_dict(),
        phase_id=phase_id,
        status="shadow",
        starts_at_utc=started.isoformat(),
        policy={"prospective": True},
        created_at_utc=started.isoformat(),
    )
    for index in range(wins + losses):
        observed = started + timedelta(hours=index + 1)
        oid = f"{prefix}-{rule_id}-{index}"
        assert store.claim_first_trigger(
            phase_id=phase_id,
            rule_id=rule_id,
            fixture_id=10_000 + index,
            observation_id=oid,
            triggered_at_utc=observed.isoformat(),
            minute=50,
            league=(index % 12) + 1,
        )
        store.attach_outcomes(
            {
                "observation_id": oid,
                "outcome_version": 1,
                "outcome_status": "win" if index < wins else "loss",
                "outcome_at_utc": (observed + timedelta(hours=2)).isoformat(),
                "payload": {"source": "discovery_lift_test"},
            }
        )
    if status != "shadow":
        store.transition_phase(
            phase_id,
            status,
            expected_status="shadow",
            reason="test_status",
        )
    return phase_id


def _nomination_spec(
    *,
    source_profile: str,
    rule_id: str,
    threshold: float,
    store_hash: str = "hash",
) -> FrozenPortfolioSpec:
    manifest = _manifest(rule_id, threshold)
    member = FrozenPortfolioMember(
        source_profile=source_profile,
        source_store_hash=store_hash,
        manifest=manifest,
    )
    profile_slug = source_profile.replace("_", "-")
    return FrozenPortfolioSpec(
        portfolio_id=f"discovery-ready-{profile_slug}-{rule_id}",
        version="v1",
        members=(member,),
    )


def test_lift_fills_open_slot_from_mature_d2(tmp_path) -> None:
    rare = WideResearchStore(tmp_path / "rare.sqlite3", allowed_root=tmp_path)
    rare.bind_profile("rare_precision_shadow")
    target = WideResearchStore(tmp_path / "champ.sqlite3", allowed_root=tmp_path)
    target.bind_profile("automatic_champion")
    started = datetime.now(UTC) - timedelta(days=30)
    _seed_phase(
        rare,
        rule_id="wide-d2-strong",
        threshold=88.0,
        status="shadow",
        wins=180,
        losses=20,
        started=started,
        prefix="d2",
    )
    decision = {
        "status": "ok",
        "active_rule_id": "frozen-portfolio-baseline-v1",
        "evaluations": [],
    }
    state_path = tmp_path / "lift.json"
    result = run_discovery_division_lift(
        (("rare_precision_shadow", rare),),
        target_store=target,
        existing_specs=(),
        decision=decision,
        state_path=state_path,
        policy=DiscoveryLiftPolicy(min_interval_hours=0.0),
        max_active=1,
        dry_run=False,
    )
    assert len(result["applied"]) == 1
    assert result["applied"][0]["mode"] == "fill"
    phases = target.list_phases(statuses=("shadow",))
    assert len(phases) == 1
    assert "wide-d2-strong" in phases[0]["rule_id"]
    policy = phases[0]["policy"]["automatic_champion"]["discovery_lift"]
    assert "protection_until_utc" in policy


def test_lift_relegates_hopeless_d1_for_harder_d2(tmp_path) -> None:
    rare = WideResearchStore(tmp_path / "rare.sqlite3", allowed_root=tmp_path)
    rare.bind_profile("rare_precision_shadow")
    target = WideResearchStore(tmp_path / "champ.sqlite3", allowed_root=tmp_path)
    target.bind_profile("automatic_champion")
    started = datetime.now(UTC) - timedelta(days=40)

    weak_spec = _nomination_spec(
        source_profile="rare_precision_shadow",
        rule_id="wide-d1-weak",
        threshold=70.0,
    )
    _seed_phase(
        rare,
        rule_id="wide-d1-weak",
        threshold=70.0,
        status="shadow",
        wins=160,
        losses=40,
        started=started,
        prefix="weak-src",
    )
    import_ready_discovery_evidence(
        rare,
        target,
        weak_spec,
        family_id=discovery_nomination_family(weak_spec),
    )
    # Mark as unprotected mature loser in duel book.
    weak_phase = target.list_phases(rule_id=weak_spec.rule_id)[0]
    target.note_phase_lifecycle_event_once(
        weak_phase["phase_id"],
        expected_status="shadow",
        reason="test_unprotected",
        metadata={},
    )

    _seed_phase(
        rare,
        rule_id="wide-d2-elite",
        threshold=91.0,
        status="shadow",
        wins=184,
        losses=16,
        started=started,
        prefix="elite",
    )

    decision = {
        "status": "ok",
        "active_rule_id": "frozen-portfolio-baseline-v1",
        "evaluations": [
            {
                "rule_id": weak_spec.rule_id,
                "portfolio_id": weak_spec.portfolio_id,
                "eligible": False,
                "hit_rate_advantage": -0.03,
                "metrics": {
                    "resolved": 220,
                    "hit_rate": 0.80,
                },
                "reasons": ["hit_rate_margin_below_gate"],
            }
        ],
    }
    result = run_discovery_division_lift(
        (("rare_precision_shadow", rare),),
        target_store=target,
        existing_specs=(weak_spec,),
        decision=decision,
        state_path=tmp_path / "lift.json",
        policy=DiscoveryLiftPolicy(min_interval_hours=0.0, max_swaps_per_run=1),
        max_active=1,
    )
    assert len(result["applied"]) == 1
    assert result["applied"][0]["mode"] == "hard"
    weak_after = target.list_phases(rule_id=weak_spec.rule_id)[0]
    assert weak_after["status"] == "rejected"
    elite = [
        row
        for row in target.list_phases(statuses=("shadow",))
        if "wide-d2-elite" in str(row.get("rule_id") or "")
    ]
    assert len(elite) == 1


def test_dry_run_does_not_throttle_live_lift(tmp_path) -> None:
    rare = WideResearchStore(tmp_path / "rare.sqlite3", allowed_root=tmp_path)
    rare.bind_profile("rare_precision_shadow")
    target = WideResearchStore(tmp_path / "champ.sqlite3", allowed_root=tmp_path)
    target.bind_profile("automatic_champion")
    started = datetime.now(UTC) - timedelta(days=20)
    _seed_phase(
        rare,
        rule_id="wide-d2-strong",
        threshold=88.0,
        status="shadow",
        wins=180,
        losses=20,
        started=started,
    )
    state_path = tmp_path / "lift.json"
    decision = {"status": "ok", "active_rule_id": "x", "evaluations": []}
    dry = run_discovery_division_lift(
        (("rare_precision_shadow", rare),),
        target_store=target,
        existing_specs=(),
        decision=decision,
        state_path=state_path,
        policy=DiscoveryLiftPolicy(min_interval_hours=24.0),
        max_active=1,
        dry_run=True,
    )
    assert dry["dry_run"] is True
    assert dry["planned"]
    assert dry["applied"] == []
    live = run_discovery_division_lift(
        (("rare_precision_shadow", rare),),
        target_store=target,
        existing_specs=(),
        decision=decision,
        state_path=state_path,
        policy=DiscoveryLiftPolicy(min_interval_hours=24.0),
        max_active=1,
        dry_run=False,
    )
    assert live.get("throttled") is not True
    assert live["applied"]


def test_lift_throttles_within_interval(tmp_path) -> None:
    rare = WideResearchStore(tmp_path / "rare.sqlite3", allowed_root=tmp_path)
    rare.bind_profile("rare_precision_shadow")
    target = WideResearchStore(tmp_path / "champ.sqlite3", allowed_root=tmp_path)
    target.bind_profile("automatic_champion")
    started = datetime.now(UTC) - timedelta(days=20)
    _seed_phase(
        rare,
        rule_id="wide-d2-strong",
        threshold=88.0,
        status="shadow",
        wins=180,
        losses=20,
        started=started,
    )
    state_path = tmp_path / "lift.json"
    first = run_discovery_division_lift(
        (("rare_precision_shadow", rare),),
        target_store=target,
        existing_specs=(),
        decision={"status": "ok", "active_rule_id": "x", "evaluations": []},
        state_path=state_path,
        policy=DiscoveryLiftPolicy(min_interval_hours=24.0),
        max_active=1,
    )
    assert first["applied"]
    second = run_discovery_division_lift(
        (("rare_precision_shadow", rare),),
        target_store=target,
        existing_specs=(),
        decision={"status": "ok", "active_rule_id": "x", "evaluations": []},
        state_path=state_path,
        policy=DiscoveryLiftPolicy(min_interval_hours=24.0),
        max_active=2,
    )
    assert second.get("throttled") is True
    assert second.get("planned") == []


def test_plan_skips_protected_newbie(tmp_path) -> None:
    rare = WideResearchStore(tmp_path / "rare.sqlite3", allowed_root=tmp_path)
    rare.bind_profile("rare_precision_shadow")
    target = WideResearchStore(tmp_path / "champ.sqlite3", allowed_root=tmp_path)
    target.bind_profile("automatic_champion")
    started = datetime.now(UTC) - timedelta(days=20)
    weak_spec = _nomination_spec(
        source_profile="rare_precision_shadow",
        rule_id="wide-d1-new",
        threshold=70.0,
    )
    _seed_phase(
        rare,
        rule_id="wide-d1-new",
        threshold=70.0,
        status="shadow",
        wins=160,
        losses=40,
        started=started,
        prefix="new",
    )
    import_ready_discovery_evidence(
        rare,
        target,
        weak_spec,
        family_id=discovery_nomination_family(weak_spec),
    )
    phase = target.list_phases(rule_id=weak_spec.rule_id)[0]
    policy = dict(phase["policy"])
    automatic = dict(policy.get("automatic_champion") or {})
    automatic["discovery_lift"] = {
        "protection_until_utc": (
            datetime.now(UTC) + timedelta(days=3)
        ).isoformat(),
        "admitted_duel_resolved": 0,
        "newbie_protection_duel_matches": 40,
    }
    policy["automatic_champion"] = automatic
    from wide_research.discovery_lift import _write_phase_policy

    _write_phase_policy(
        target,
        phase_id=phase["phase_id"],
        policy=policy,
        now=datetime.now(UTC),
    )
    _seed_phase(
        rare,
        rule_id="wide-d2-elite",
        threshold=92.0,
        status="shadow",
        wins=190,
        losses=10,
        started=started,
        prefix="elite2",
    )
    swaps = plan_discovery_division_lifts(
        (("rare_precision_shadow", rare),),
        target_store=target,
        existing_specs=(weak_spec,),
        decision={
            "status": "ok",
            "active_rule_id": "baseline",
            "evaluations": [
                {
                    "rule_id": weak_spec.rule_id,
                    "portfolio_id": weak_spec.portfolio_id,
                    "eligible": False,
                    "hit_rate_advantage": -0.05,
                    "metrics": {"resolved": 220, "hit_rate": 0.79},
                    "reasons": ["hit_rate_margin_below_gate"],
                }
            ],
        },
        state={"cooldowns": {}, "last_run_utc": None},
        policy=DiscoveryLiftPolicy(min_interval_hours=0.0),
        max_active=1,
    )
    assert swaps == ()
