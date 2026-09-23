"""Immutable prospective OR-portfolios for already discovered rules.

The portfolio layer deliberately owns a separate evidence store.  A portfolio
is frozen as the exact member manifests that existed when it was registered;
later discovery/lifecycle changes therefore cannot alter its behaviour.  It is
research-only and has no publication router.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence

from .features import FEATURE_SCHEMA_VERSION, evaluate_universe, extract_features
from .live import WideShadowLayer
from .rules import Clause, RuleManifest, evaluate_rule, evaluate_snapshot
from .schema import DEFAULT_UNIVERSE, FAIL, PASS, canonical_hash
from .store import WideResearchStore


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _source_id(snapshot: Mapping[str, Any]) -> str:
    return str(
        snapshot.get("observation_id")
        or snapshot.get("observation_key")
        or snapshot.get("decision_id")
        or ""
    ).strip()


def _league(snapshot: Mapping[str, Any]) -> Optional[str]:
    match = _mapping(snapshot.get("match"))
    value = match.get("league_id") or match.get("league_name")
    normalized = str(value or "").strip()
    return normalized or None


@dataclass(frozen=True)
class FrozenPortfolioMember:
    """One immutable source rule inside a frozen portfolio."""

    source_profile: str
    source_store_hash: str
    manifest: RuleManifest

    def __post_init__(self) -> None:
        profile = str(self.source_profile or "").strip()
        store_hash = str(self.source_store_hash or "").strip()
        if not profile or not store_hash:
            raise ValueError("portfolio member source identity is required")
        if not isinstance(self.manifest, RuleManifest):
            raise TypeError("portfolio member manifest must be RuleManifest")
        if self.manifest.feature_schema_version != FEATURE_SCHEMA_VERSION:
            raise ValueError("portfolio member feature schema is incompatible")
        object.__setattr__(self, "source_profile", profile)
        object.__setattr__(self, "source_store_hash", store_hash)

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_profile": self.source_profile,
            "source_store_hash": self.source_store_hash,
            "source_rule_id": self.manifest.rule_id,
            "manifest": self.manifest.as_dict(),
        }


@dataclass(frozen=True)
class FrozenPortfolioSpec:
    """A versioned OR-union whose evidence begins only after registration."""

    portfolio_id: str
    version: str
    members: tuple[FrozenPortfolioMember, ...]
    terminal_horizon: int = 200
    min_review_hit_rate: float = 0.90

    def __post_init__(self) -> None:
        portfolio_id = str(self.portfolio_id or "").strip()
        version = str(self.version or "").strip()
        if not portfolio_id or not version:
            raise ValueError("portfolio_id and version are required")
        if not self.members:
            raise ValueError("a frozen portfolio requires at least one member")
        identities = [
            (member.source_profile, member.manifest.rule_id)
            for member in self.members
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("duplicate portfolio members are not allowed")
        if type(self.terminal_horizon) is not int or self.terminal_horizon <= 0:
            raise ValueError("terminal_horizon must be a positive integer")
        if not 0.0 < float(self.min_review_hit_rate) <= 1.0:
            raise ValueError("min_review_hit_rate must be in (0, 1]")
        object.__setattr__(self, "portfolio_id", portfolio_id)
        object.__setattr__(self, "version", version)

    @property
    def rule_id(self) -> str:
        return f"frozen-portfolio-{self.portfolio_id}-{self.version}"

    @property
    def phase_id(self) -> str:
        return f"{self.rule_id}:prospective"

    def manifest(self) -> dict[str, Any]:
        payload = {
            "schema_version": 1,
            "portfolio_id": self.portfolio_id,
            "rule_id": self.rule_id,
            "version": self.version,
            "semantics": "earliest_first_trigger_per_fixture_any_member_v1",
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "members": [member.as_dict() for member in self.members],
            "production_enabled": False,
        }
        return {**payload, "portfolio_hash": canonical_hash(payload)}

    def policy(self) -> dict[str, Any]:
        return {
            "portfolio": {
                "portfolio_id": self.portfolio_id,
                "version": self.version,
                "semantics": "any_member_rule_passes_v1",
                "member_rule_ids": [
                    member.manifest.rule_id for member in self.members
                ],
            },
            "terminal_review": {
                "enabled": True,
                "final_look": self.terminal_horizon,
                "min_point_hit_rate": float(self.min_review_hit_rate),
            },
            "production_applied": False,
        }


BALANCED_TWO_RULE_PORTFOLIO_ID = "balanced-two-rule"
BALANCED_TWO_RULE_VERSION = "v1"


def balanced_two_rule_spec(*, terminal_horizon: int = 200) -> FrozenPortfolioSpec:
    """Return the immutable, source-controlled balanced-two-rule definition."""

    asymmetry = FrozenPortfolioMember(
        source_profile="rare_precision_shadow",
        source_store_hash=(
            "38e6666d98fd031af2989f4e7aebc3228e621312c58bde70ea4aed267289a359"
        ),
        manifest=RuleManifest(
            rule_id="wide-1f89b379f5d04f3ceb3f",
            version="wide_prospective_rule_v2",
            clauses=(
                Clause("bot.prob_to90", "<=", 79.0),
                Clause("nonlinear.v1.away_share.corners", ">=", 0.5),
                Clause(
                    "nonlinear.v1.home_away_balance.total_shots",
                    "<=",
                    -0.076923,
                ),
                Clause(
                    "nonlinear.v1.product.box_shots.static_p90",
                    ">=",
                    0.228077,
                ),
                Clause(
                    "nonlinear.v1.trailing_share.shots_on_target",
                    ">=",
                    0.333333,
                ),
            ),
            universe=DEFAULT_UNIVERSE,
            schema_version=2,
            feature_schema_version=FEATURE_SCHEMA_VERSION,
        ),
    )
    market_pressure = FrozenPortfolioMember(
        source_profile="rare_precision_shadow",
        source_store_hash=(
            "82139a90953630cfceaa7f02fec9a0e9613cc273c49e7f373f281a5d0da8c494"
        ),
        manifest=RuleManifest(
            rule_id="wide-0bed4a7f285d9f108840",
            version="wide_prospective_rule_v2",
            clauses=(
                Clause(
                    "nonlinear.v1.product.market_p90.season",
                    ">=",
                    0.832204,
                ),
                Clause(
                    "rolling.10m.delta.pressure_index",
                    ">=",
                    1.82,
                ),
            ),
            universe=DEFAULT_UNIVERSE,
            schema_version=2,
            feature_schema_version=FEATURE_SCHEMA_VERSION,
        ),
    )
    return FrozenPortfolioSpec(
        portfolio_id=BALANCED_TWO_RULE_PORTFOLIO_ID,
        version=BALANCED_TWO_RULE_VERSION,
        members=(asymmetry, market_pressure),
        terminal_horizon=terminal_horizon,
    )


def evaluate_frozen_portfolio(
    spec: FrozenPortfolioSpec,
    snapshot: Mapping[str, Any],
    *,
    static_prediction: Optional[Mapping[str, Any]] = None,
    rolling_prediction: Optional[Mapping[str, Any]] = None,
    max_prediction_lag_seconds: float = 300.0,
) -> dict[str, Any]:
    """Evaluate an immutable OR-portfolio without persistence side effects."""

    member_results: list[dict[str, Any]] = []
    passed_members: list[str] = []
    for member in spec.members:
        result = evaluate_snapshot(
            member.manifest,
            snapshot,
            static_prediction=static_prediction,
            rolling_prediction=rolling_prediction,
            max_prediction_lag_seconds=max_prediction_lag_seconds,
            include_extended=True,
        )
        member_results.append(
            {
                "source_profile": member.source_profile,
                "rule_id": member.manifest.rule_id,
                "manifest_hash": member.manifest.manifest_hash,
                "status": result.get("status"),
                "reason": result.get("reason"),
                "unavailable_features": list(
                    result.get("unavailable_features") or []
                ),
                "failed_features": list(result.get("failed_features") or []),
            }
        )
        if result.get("status") == PASS:
            passed_members.append(member.manifest.rule_id)

    return {
        "portfolio_id": spec.portfolio_id,
        "rule_id": spec.rule_id,
        "portfolio_hash": spec.manifest()["portfolio_hash"],
        "status": PASS if passed_members else FAIL,
        "reason": "pass" if passed_members else "no_member_passed",
        "passed_members": passed_members,
        "members": member_results,
    }


class FrozenPortfolioLayer(WideShadowLayer):
    """Evaluate immutable OR-portfolios with the normal durable retry path."""

    def __init__(
        self,
        store: WideResearchStore,
        specs: Sequence[FrozenPortfolioSpec],
        *,
        max_prediction_lag_seconds: float = 300.0,
    ) -> None:
        normalized = tuple(specs)
        if not normalized:
            raise ValueError("at least one frozen portfolio is required")
        identities = [spec.rule_id for spec in normalized]
        if len(identities) != len(set(identities)):
            raise ValueError("duplicate frozen portfolio identities")
        existing_starts = {
            str(row.get("starts_at_utc") or "")
            for row in store.list_phases()
            if str(row.get("rule_id") or "") in identities
        }
        existing_starts.discard("")
        if len(existing_starts) > 1:
            raise ValueError("frozen portfolios do not share one cohort start")
        cohort_start = (
            next(iter(existing_starts))
            if existing_starts
            else datetime.now(timezone.utc).isoformat()
        )
        for spec in normalized:
            store.register_rule_and_phase(
                rule_id=spec.rule_id,
                manifest=spec.manifest(),
                phase_id=spec.phase_id,
                status="shadow",
                starts_at_utc=cohort_start,
                policy=spec.policy(),
            )
        self.specs = normalized
        super().__init__(
            store,
            max_active_rules=len(normalized),
            refresh_seconds=0.0,
            max_prediction_lag_seconds=max_prediction_lag_seconds,
        )
        self.include_extended = True

    def _process_snapshot_once(
        self,
        snapshot: Mapping[str, Any],
        *,
        static_prediction: Optional[Mapping[str, Any]] = None,
        rolling_prediction: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        if not isinstance(snapshot, Mapping):
            return {"status": "UNAVAILABLE", "reason": "snapshot_not_mapping"}
        universe = evaluate_universe(snapshot)
        if universe.get("status") != PASS:
            return {
                "status": str(universe.get("status") or "UNAVAILABLE"),
                "reason": str(universe.get("reason") or "universe"),
                "universe": universe,
                "evaluated_portfolios": 0,
                "claimed": [],
            }

        observation_id = _source_id(snapshot)
        fixture_id = snapshot.get("fixture_id")
        observed_at = snapshot.get("created_at_utc")
        minute = snapshot.get("minute")
        league = _league(snapshot)
        vector = extract_features(
            snapshot,
            static_prediction=static_prediction,
            rolling_prediction=rolling_prediction,
            max_prediction_lag_seconds=self.max_prediction_lag_seconds,
            include_extended=True,
        )
        self.store.record_universe(
            fixture_id=fixture_id,
            observation_id=observation_id,
            observed_at_utc=observed_at,
            minute=minute,
            league=league,
            input_data={
                "stage": snapshot.get("stage"),
                "universe_id": universe.get("universe_id"),
                "universe_manifest_hash": universe.get(
                    "universe_manifest_hash"
                ),
                "available_feature_count": vector.get("available_count"),
                "contracts": dict(_mapping(vector.get("contracts"))),
            },
        )

        claimed: list[dict[str, Any]] = []
        evaluations: list[dict[str, Any]] = []
        values = _mapping(vector.get("values"))
        for spec in self.specs:
            passed_members: list[str] = []
            member_results: list[dict[str, Any]] = []
            matched_values: dict[str, Any] = {}
            for member in spec.members:
                member_universe = evaluate_universe(
                    snapshot, member.manifest.universe
                )
                result = evaluate_rule(
                    member.manifest,
                    vector,
                    universe_evaluation=member_universe,
                )
                member_results.append(
                    {
                        "source_profile": member.source_profile,
                        "rule_id": member.manifest.rule_id,
                        "manifest_hash": member.manifest.manifest_hash,
                        "status": result.get("status"),
                        "reason": result.get("reason"),
                    }
                )
                if result.get("status") != PASS:
                    continue
                passed_members.append(member.manifest.rule_id)
                for clause in member.manifest.clauses:
                    matched_values[clause.feature] = values.get(clause.feature)

            evaluations.append(
                {
                    "portfolio_id": spec.portfolio_id,
                    "phase_id": spec.phase_id,
                    "status": PASS if passed_members else "FAIL",
                    "passed_members": passed_members,
                    "members": member_results,
                }
            )
            if not passed_members:
                continue
            inserted = self.store.claim_first_trigger(
                phase_id=spec.phase_id,
                rule_id=spec.rule_id,
                fixture_id=fixture_id,
                observation_id=observation_id,
                triggered_at_utc=observed_at,
                minute=minute,
                league=league,
                input_data={
                    "portfolio_hash": spec.manifest()["portfolio_hash"],
                    "passed_members": passed_members,
                    "member_evaluations": member_results,
                    "matched_values": matched_values,
                    "feature_contracts": dict(
                        _mapping(vector.get("contracts"))
                    ),
                    "stage": snapshot.get("stage"),
                    "production_applied": False,
                },
            )
            if inserted:
                claimed.append(
                    {
                        "portfolio_id": spec.portfolio_id,
                        "phase_id": spec.phase_id,
                        "rule_id": spec.rule_id,
                        "fixture_id": fixture_id,
                        "observation_id": observation_id,
                        "minute": minute,
                        "passed_members": passed_members,
                    }
                )
        return {
            "status": PASS,
            "reason": "pass",
            "universe": universe,
            "evaluated_portfolios": len(self.specs),
            "evaluations": evaluations,
            "claimed": claimed,
        }
