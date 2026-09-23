from __future__ import annotations

from datetime import datetime, timedelta, timezone

from wide_research import FEATURE_SCHEMA_VERSION, Clause, RuleManifest
from wide_research.portfolio import (
    BALANCED_TWO_RULE_PORTFOLIO_ID,
    FrozenPortfolioLayer,
    FrozenPortfolioMember,
    FrozenPortfolioSpec,
    balanced_two_rule_spec,
    evaluate_frozen_portfolio,
)
from wide_research.store import WideResearchStore


UTC = timezone.utc


def _member(rule_id: str, threshold: float) -> FrozenPortfolioMember:
    manifest = RuleManifest(
        rule_id,
        "v1",
        (Clause("bot.prob_to90", ">=", threshold),),
        feature_schema_version=FEATURE_SCHEMA_VERSION,
    )
    return FrozenPortfolioMember(
        source_profile="test",
        source_store_hash=f"store-{rule_id}",
        manifest=manifest,
    )


def _snapshot(fixture_id: int, probability: float, observed: datetime) -> dict:
    return {
        "record_type": "observation",
        "observation_id": f"{fixture_id}:50:BLOCK:v3",
        "fixture_id": fixture_id,
        "created_at_utc": observed.isoformat(),
        "stage": "decision_pipeline",
        "minute": 50,
        "match": {"score_home": 0, "score_away": 0, "league_id": 7},
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


def _store(tmp_path) -> WideResearchStore:
    store = WideResearchStore(
        tmp_path / "portfolios.sqlite3", allowed_root=tmp_path
    )
    store.bind_profile("frozen_portfolio_shadow")
    return store


def test_balanced_two_rule_definition_is_source_controlled_and_immutable() -> None:
    spec = balanced_two_rule_spec()

    assert spec.portfolio_id == BALANCED_TWO_RULE_PORTFOLIO_ID
    assert spec.rule_id == "frozen-portfolio-balanced-two-rule-v1"
    assert [member.manifest.rule_id for member in spec.members] == [
        "wide-1f89b379f5d04f3ceb3f",
        "wide-0bed4a7f285d9f108840",
    ]
    assert [member.manifest.manifest_hash for member in spec.members] == [
        "f1f25c7d0bee6e18ebfecf9f6432ab4d08877e96531b5a90805c3bb47bc57cef",
        "52110b23e40659b02e4fd8971aebf54796f7bc529e6fd376ebc7ccb51b136a0b",
    ]
    assert (
        spec.manifest()["portfolio_hash"]
        == "d3439fe217409a364ee4d42f5ea3c241c8d771525c6de50447257ffc54b47257"
    )


def test_pure_portfolio_evaluation_uses_any_member_or_semantics() -> None:
    high = _member("high", 90.0)
    low = _member("low", 80.0)
    spec = FrozenPortfolioSpec("test-or", "v1", (high, low))
    observed = datetime.now(UTC) + timedelta(seconds=1)

    result = evaluate_frozen_portfolio(
        spec,
        _snapshot(10, 85.0, observed),
    )

    assert result["status"] == "PASS"
    assert result["passed_members"] == ["low"]


def test_frozen_portfolios_claim_union_once_and_attach_outcome(tmp_path) -> None:
    low = _member("low", 80.0)
    high = _member("high", 90.0)
    specs = (
        FrozenPortfolioSpec("balanced", "v1", (high,)),
        FrozenPortfolioSpec("broad", "v1", (low, high)),
    )
    store = _store(tmp_path)
    layer = FrozenPortfolioLayer(store, specs)
    observed = datetime.now(UTC) + timedelta(seconds=1)
    snapshot = _snapshot(10, 85.0, observed)

    first = layer.process_snapshot(snapshot)
    second = layer.process_snapshot(snapshot)

    assert [row["portfolio_id"] for row in first["claimed"]] == ["broad"]
    assert first["claimed"][0]["passed_members"] == ["low"]
    assert second["claimed"] == []
    assert store.metrics_for_phase("frozen-portfolio-balanced-v1:prospective")[
        "total"
    ] == 0
    assert store.metrics_for_phase("frozen-portfolio-broad-v1:prospective")[
        "total"
    ] == 1

    result = layer.process_outcomes(
        [
            {
                "observation_id": snapshot["observation_id"],
                "outcome_schema_version": 1,
                "created_at_utc": (observed + timedelta(hours=1)).isoformat(),
                "outcome": {
                    "status": "resolved",
                    "goal_to90_normal_time": True,
                    "resolved_at_utc": (
                        observed + timedelta(hours=1)
                    ).isoformat(),
                },
            }
        ]
    )

    assert result["updated_triggers"] == 1
    assert store.metrics_for_phase("frozen-portfolio-broad-v1:prospective")[
        "win"
    ] == 1


def test_frozen_portfolio_registration_is_idempotent_and_horizon_is_fixed(
    tmp_path,
) -> None:
    member = _member("only", 70.0)
    spec = FrozenPortfolioSpec(
        "limited", "v1", (member,), terminal_horizon=1
    )
    store = _store(tmp_path)
    FrozenPortfolioLayer(store, (spec,))
    original = store.list_phases(rule_id=spec.rule_id)[0]

    restarted = FrozenPortfolioLayer(store, (spec,))
    repeated = store.list_phases(rule_id=spec.rule_id)[0]
    assert repeated["starts_at_utc"] == original["starts_at_utc"]

    observed = datetime.now(UTC) + timedelta(seconds=1)
    first = restarted.process_snapshot(_snapshot(1, 90.0, observed))
    second = restarted.process_snapshot(
        _snapshot(2, 90.0, observed + timedelta(minutes=1))
    )
    assert len(first["claimed"]) == 1
    assert second["claimed"] == []
    assert store.metrics_for_phase(spec.phase_id)["total"] == 1
