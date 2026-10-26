"""Prospective Champion–Challenger selection for publication portfolios.

The selector owns an isolated evidence store. Source-controlled candidates
start a fresh prospective clock here. A READY discovery finalist may reuse the
prospective evidence that earned its certificate; pre-prospective discovery
history is never accepted.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import NormalDist
from typing import Any, Mapping, Optional, Sequence

from .live import WideShadowLayer
from .lifecycle import wilson_interval
from .portfolio import (
    FrozenPortfolioLayer,
    FrozenPortfolioMember,
    FrozenPortfolioSpec,
    frozen_portfolio_spec_from_dict,
)
from .rules import rule_manifest_from_dict
from .schema import canonical_hash
from .store import WideResearchStore


AUTOMATIC_CHAMPION_POLICY_VERSION = "automatic_champion_v4"
AUTOMATIC_CHAMPION_POINTER = "automatic_publication_champion"
AUTOMATIC_CHAMPION_REPORT_SCHEMA_VERSION = 1
DISCOVERY_NOMINATION_PREFIX = "discovery-ready-"
DISCOVERY_NOMINATION_SOURCE_STATUSES = ("ready", "shadow", "paused")
DISCOVERY_NOMINATION_MIN_RESOLVED = 200
DISCOVERY_NOMINATION_MIN_HIT_RATE = 0.86


class AutomaticChampionUnavailable(RuntimeError):
    """Raised when no contract-valid publication champion is available."""


@dataclass(frozen=True)
class ActiveChampionSelection:
    """One checksummed pointer generation used for a publication decision."""

    spec: FrozenPortfolioSpec
    generation: int
    rule_id: str
    phase_id: str
    pointer_updated_at_utc: str


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _parse_utc(value: Any) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("timestamp must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


@dataclass(frozen=True)
class AutomaticChampionPolicy:
    """Source-controlled gates for one automatic selector generation."""

    version: str = AUTOMATIC_CHAMPION_POLICY_VERSION
    prior_hit_rate: float = 0.83
    prior_strength: float = 40.0
    credible_lower_probability: float = 0.10
    min_resolved: int = 200
    min_span_days: float = 10.0
    superiority_margin: float = 0.02
    degradation_window: int = 40
    degradation_rate: float = 0.80
    degradation_recovery_rate: float = 0.80

    def __post_init__(self) -> None:
        probabilities = (
            self.prior_hit_rate,
            self.credible_lower_probability,
            self.superiority_margin,
            self.degradation_rate,
            self.degradation_recovery_rate,
        )
        if any(
            not math.isfinite(float(value)) or not 0.0 <= float(value) <= 1.0
            for value in probabilities
        ):
            raise ValueError("automatic champion rates must be in [0, 1]")
        if not 0.0 < self.credible_lower_probability < 0.5:
            raise ValueError("credible lower probability must be in (0, 0.5)")
        if self.degradation_recovery_rate < self.degradation_rate:
            raise ValueError(
                "degradation recovery rate must be at least degradation rate"
            )
        if (
            not math.isfinite(self.prior_strength)
            or self.prior_strength <= 0.0
            or not math.isfinite(self.min_span_days)
            or self.min_span_days < 0.0
        ):
            raise ValueError("automatic champion scales must be finite")
        counts = (
            self.min_resolved,
            self.degradation_window,
        )
        if any(type(value) is not int or value <= 0 for value in counts):
            raise ValueError("automatic champion counts must be positive integers")

    def manifest(self) -> dict[str, Any]:
        return asdict(self)


def posterior_summary(
    wins: int,
    losses: int,
    *,
    policy: AutomaticChampionPolicy,
) -> dict[str, float]:
    """Return a conservative beta-posterior summary.

    The score remains useful for diagnostics and deterministic ranking, but it
    does not authorize or block a switch.
    """

    successes = int(wins)
    failures = int(losses)
    if successes < 0 or failures < 0:
        raise ValueError("wins and losses must be non-negative")
    alpha = policy.prior_hit_rate * policy.prior_strength + successes
    beta = (1.0 - policy.prior_hit_rate) * policy.prior_strength + failures
    total = alpha + beta
    mean = alpha / total
    variance = alpha * beta / (total * total * (total + 1.0))
    standard_deviation = math.sqrt(max(0.0, variance))
    z_value = NormalDist().inv_cdf(policy.credible_lower_probability)
    lower = min(1.0, max(0.0, mean + z_value * standard_deviation))
    return {
        "alpha": alpha,
        "beta": beta,
        "mean": mean,
        "variance": variance,
        "lower": lower,
    }


def superiority_probability(
    challenger: Mapping[str, Any],
    champion: Mapping[str, Any],
    *,
    margin: float,
) -> float:
    """Approximate ``P(challenger - champion > margin)``."""

    difference = float(challenger["mean"]) - float(champion["mean"])
    variance = float(challenger["variance"]) + float(champion["variance"])
    if variance <= 0.0:
        return 1.0 if difference > float(margin) else 0.0
    z_value = (float(margin) - difference) / math.sqrt(variance)
    return min(1.0, max(0.0, 1.0 - NormalDist().cdf(z_value)))


class AutomaticChampionLayer(FrozenPortfolioLayer):
    """Continuously collect fresh evidence for the fixed candidate catalog."""

    def __init__(
        self,
        store: WideResearchStore,
        specs: Sequence[FrozenPortfolioSpec],
        *,
        families: Mapping[str, str],
        max_prediction_lag_seconds: float = 300.0,
    ) -> None:
        normalized = tuple(specs)
        if not normalized:
            raise ValueError("automatic champion requires candidates")
        identities = [spec.rule_id for spec in normalized]
        if len(identities) != len(set(identities)):
            raise ValueError("duplicate automatic champion candidates")
        normalized_families = {
            spec.rule_id: str(
                families.get(spec.rule_id)
                or families.get(spec.portfolio_id)
                or spec.portfolio_id
            ).strip()
            for spec in normalized
        }
        if any(not value for value in normalized_families.values()):
            raise ValueError("every automatic champion candidate needs a family")

        stored_phases = {
            str(row.get("rule_id") or ""): row
            for row in store.list_phases()
            if str(row.get("rule_id") or "") in identities
        }
        fresh_start = datetime.now(timezone.utc).isoformat()
        for spec in normalized:
            stored_phase = stored_phases.get(spec.rule_id)
            cohort_start = (
                str(stored_phase.get("starts_at_utc") or "")
                if stored_phase is not None
                else fresh_start
            )
            policy = (
                dict(_mapping(stored_phase.get("policy")))
                if stored_phase is not None
                else {
                    "automatic_champion": {
                        "version": AUTOMATIC_CHAMPION_POLICY_VERSION,
                        "family_id": normalized_families[spec.rule_id],
                        "continuous_prospective_evidence": True,
                    },
                    "production_applied": False,
                }
            )
            store.register_rule_and_phase(
                rule_id=spec.rule_id,
                manifest=spec.manifest(),
                phase_id=spec.phase_id,
                status="shadow",
                starts_at_utc=cohort_start,
                policy=policy,
            )
        statuses = {
            str(row.get("rule_id") or ""): str(row.get("status") or "")
            for row in store.list_phases()
        }
        self.specs = tuple(
            spec
            for spec in normalized
            if statuses.get(spec.rule_id)
            in {"candidate", "shadow", "ready", "active"}
        )
        self.families = normalized_families
        WideShadowLayer.__init__(
            self,
            store,
            max_active_rules=max(1, len(self.specs)),
            refresh_seconds=0.0,
            max_prediction_lag_seconds=max_prediction_lag_seconds,
        )
        self.include_extended = True


def is_discovery_nomination(spec: FrozenPortfolioSpec) -> bool:
    return spec.portfolio_id.startswith(DISCOVERY_NOMINATION_PREFIX)


def discovery_nomination_family(spec: FrozenPortfolioSpec) -> str:
    """Group threshold variants of the same discovered rule shape."""

    member = spec.members[0]
    shape = {
        "source_profile": member.source_profile,
        "clauses": [
            {"feature": clause.feature, "op": clause.op}
            for clause in member.manifest.clauses
        ],
    }
    return "discovery-" + canonical_hash(shape)[:16]


def discovery_nomination_source_key(
    spec: FrozenPortfolioSpec,
) -> tuple[str, str]:
    if not is_discovery_nomination(spec) or len(spec.members) != 1:
        raise ValueError("not a singleton discovery nomination")
    member = spec.members[0]
    return member.source_profile, member.manifest.rule_id


def load_discovery_nomination_specs(
    store: WideResearchStore,
) -> tuple[FrozenPortfolioSpec, ...]:
    """Restore every admitted nomination, including closed generations."""

    specs = []
    for row in store.list_manifests():
        manifest = row.get("manifest")
        if not isinstance(manifest, Mapping):
            continue
        if not str(manifest.get("portfolio_id") or "").startswith(
            DISCOVERY_NOMINATION_PREFIX
        ):
            continue
        specs.append(frozen_portfolio_spec_from_dict(manifest))
    specs.sort(key=lambda value: value.rule_id)
    return tuple(specs)


def _discovery_nomination_phase_metrics(
    source_store: WideResearchStore,
    phase: Mapping[str, Any],
) -> tuple[int, int, float]:
    wins = 0
    losses = 0
    resolved = 0
    for trigger in source_store.list_triggers(
        phase_id=str(phase.get("phase_id") or "")
    ):
        status = str(trigger.get("latest_outcome_status") or "pending")
        if status not in {"win", "loss", "invalid"}:
            continue
        resolved += 1
        wins += int(status == "win")
        losses += int(status == "loss")
    scored = wins + losses
    hit_rate = wins / scored if scored else 0.0
    return wins, resolved, hit_rate


def _discovery_nomination_qualifies(
    *,
    status: str,
    resolved: int,
    hit_rate: float,
    min_resolved: int,
    min_hit_rate: float,
) -> bool:
    # READY already passed the certificate path; keep those eligible.
    if status == "ready":
        return True
    return (
        resolved >= int(min_resolved)
        and hit_rate + 1e-12 >= float(min_hit_rate)
    )


def select_ready_discovery_nominations(
    source_store: WideResearchStore,
    *,
    target_store: WideResearchStore,
    source_profile: str,
    existing_specs: Sequence[FrozenPortfolioSpec],
    max_active: int,
    min_resolved: int = DISCOVERY_NOMINATION_MIN_RESOLVED,
    min_hit_rate: float = DISCOVERY_NOMINATION_MIN_HIT_RATE,
    source_statuses: Sequence[str] = DISCOVERY_NOMINATION_SOURCE_STATUSES,
) -> tuple[FrozenPortfolioSpec, ...]:
    """Select the strongest mature discovery rules for open duel slots.

    READY phases remain eligible. Shadow/paused phases also qualify once they
    clear the staged pool bar (default: 200 resolved at ≥86% hit-rate) so a
    strong finalist paused under an older gate is not stranded outside the
    automatic champion catalog.
    """

    return select_ready_discovery_nominations_from_profiles(
        ((str(source_profile), source_store),),
        target_store=target_store,
        existing_specs=existing_specs,
        max_active=max_active,
        min_resolved=min_resolved,
        min_hit_rate=min_hit_rate,
        source_statuses=source_statuses,
    )


def select_ready_discovery_nominations_from_profiles(
    sources: Sequence[tuple[str, WideResearchStore]],
    *,
    target_store: WideResearchStore,
    existing_specs: Sequence[FrozenPortfolioSpec],
    max_active: int,
    min_resolved: int = DISCOVERY_NOMINATION_MIN_RESOLVED,
    min_hit_rate: float = DISCOVERY_NOMINATION_MIN_HIT_RATE,
    source_statuses: Sequence[str] = DISCOVERY_NOMINATION_SOURCE_STATUSES,
) -> tuple[FrozenPortfolioSpec, ...]:
    """Rank mature rules across discovery stores into the remaining duel slots."""

    limit = max(0, int(max_active))
    admitted_keys = {
        discovery_nomination_source_key(spec)
        for spec in existing_specs
        if is_discovery_nomination(spec)
    }
    target_phases = {
        str(row.get("rule_id") or ""): str(row.get("status") or "")
        for row in target_store.list_phases()
    }
    active_dynamic = sum(
        1
        for spec in existing_specs
        if is_discovery_nomination(spec)
        and target_phases.get(spec.rule_id)
        in {"candidate", "shadow", "ready", "active"}
    )
    available = max(0, limit - active_dynamic)
    if available == 0 or not sources:
        return ()

    statuses = tuple(
        dict.fromkeys(
            str(status).lower()
            for status in source_statuses
            if str(status).strip()
        )
    )
    ranked: list[tuple[tuple[Any, ...], str, Mapping[str, Any]]] = []
    for source_profile, source_store in sources:
        profile = str(source_profile)
        for phase in source_store.list_phases(statuses=statuses):
            rule_id = str(phase.get("rule_id") or "")
            key = (profile, rule_id)
            if key in admitted_keys:
                continue
            status = str(phase.get("status") or "")
            wins, resolved, hit_rate = _discovery_nomination_phase_metrics(
                source_store, phase
            )
            if not _discovery_nomination_qualifies(
                status=status,
                resolved=resolved,
                hit_rate=hit_rate,
                min_resolved=min_resolved,
                min_hit_rate=min_hit_rate,
            ):
                continue
            lower, _upper = wilson_interval(wins, resolved)
            ranked.append(
                (
                    (
                        -float(lower),
                        -float(hit_rate),
                        -int(resolved),
                        profile,
                        rule_id,
                    ),
                    profile,
                    phase,
                )
            )
    ranked.sort(key=lambda item: item[0])

    selected = []
    for _score, source_profile, phase in ranked[:available]:
        manifest_payload = phase.get("manifest")
        if not isinstance(manifest_payload, Mapping):
            continue
        manifest = rule_manifest_from_dict(manifest_payload)
        member = FrozenPortfolioMember(
            source_profile=str(source_profile),
            source_store_hash=str(phase.get("manifest_hash") or ""),
            manifest=manifest,
        )
        profile_slug = str(source_profile).replace("_", "-")
        selected.append(
            FrozenPortfolioSpec(
                portfolio_id=(
                    f"{DISCOVERY_NOMINATION_PREFIX}{profile_slug}-"
                    f"{manifest.rule_id}"
                ),
                version="v1",
                members=(member,),
            )
        )
    return tuple(selected)


def import_ready_discovery_evidence(
    source_store: WideResearchStore,
    target_store: WideResearchStore,
    spec: FrozenPortfolioSpec,
    *,
    family_id: str,
) -> dict[str, int]:
    """Copy already prospective mature evidence into the direct duel book.

    READY and pool-mature shadow/paused sources are accepted. Discovery history
    before the source phase remains excluded. The copied rows retain their
    original observation identities and outcome versions, so retries are
    idempotent and corrections continue to reconcile normally.
    """

    source_profile, source_rule_id = discovery_nomination_source_key(spec)
    source_rows = source_store.list_phases(
        statuses=DISCOVERY_NOMINATION_SOURCE_STATUSES,
        rule_id=source_rule_id,
    )
    if len(source_rows) != 1:
        raise ValueError(
            "discovery nomination source is not uniquely mature/READY"
        )
    source_phase = source_rows[0]
    target_store.register_rule_and_phase(
        rule_id=spec.rule_id,
        manifest=spec.manifest(),
        phase_id=spec.phase_id,
        status="shadow",
        starts_at_utc=str(source_phase["starts_at_utc"]),
        policy={
            "automatic_champion": {
                "version": AUTOMATIC_CHAMPION_POLICY_VERSION,
                "family_id": str(family_id),
                "continuous_prospective_evidence": True,
                "ready_evidence_imported": True,
                "source_profile": source_profile,
                "source_phase_id": str(source_phase["phase_id"]),
                "source_status": str(source_phase.get("status") or ""),
            },
            "production_applied": False,
        },
        created_at_utc=str(source_phase["created_at_utc"]),
    )
    claimed = 0
    outcomes = []
    for trigger in source_store.list_triggers(
        phase_id=str(source_phase["phase_id"])
    ):
        inserted = target_store.claim_first_trigger(
            phase_id=spec.phase_id,
            rule_id=spec.rule_id,
            fixture_id=trigger.get("fixture_id"),
            observation_id=trigger.get("observation_id"),
            triggered_at_utc=trigger.get("triggered_at_utc"),
            minute=trigger.get("minute"),
            league=trigger.get("league"),
            input_data={
                "nomination_evidence": True,
                "source_profile": source_profile,
                "source_phase_id": str(source_phase["phase_id"]),
                "source_input": dict(_mapping(trigger.get("input"))),
            },
        )
        claimed += int(inserted)
        status = str(
            trigger.get("latest_outcome_status") or "pending"
        )
        if status == "pending":
            continue
        outcomes.append(
            {
                "observation_id": trigger.get("observation_id"),
                "outcome_version": trigger.get("latest_outcome_version") or 1,
                "outcome_status": status,
                "outcome_label": trigger.get("latest_outcome_label"),
                "outcome_at_utc": trigger.get("latest_outcome_at_utc"),
                "received_at_utc": trigger.get("updated_at_utc"),
                "payload": dict(
                    _mapping(trigger.get("latest_outcome"))
                ),
            }
        )
    attached = target_store.attach_outcomes(outcomes)
    return {
        "claimed": claimed,
        "outcomes": len(outcomes),
        "inserted_outcomes": int(attached.get("inserted") or 0),
        "updated_triggers": int(attached.get("updated_triggers") or 0),
    }


def retire_exhausted_discovery_nominations(
    store: WideResearchStore,
    decision: Mapping[str, Any],
    *,
    terminal_horizon: int = 200,
) -> tuple[str, ...]:
    """Close failed duels at a fixed horizon so later READY rules get a slot."""

    if str(decision.get("status") or "") == "blocked":
        return ()
    active_rule_id = str(decision.get("active_rule_id") or "")
    retired = []
    phases = {
        str(row.get("rule_id") or ""): row for row in store.list_phases()
    }
    for evaluation in decision.get("evaluations") or []:
        if not isinstance(evaluation, Mapping):
            continue
        rule_id = str(evaluation.get("rule_id") or "")
        portfolio_id = str(evaluation.get("portfolio_id") or "")
        if (
            not portfolio_id.startswith(DISCOVERY_NOMINATION_PREFIX)
            or rule_id == active_rule_id
            or evaluation.get("eligible") is True
        ):
            continue
        metrics = _mapping(evaluation.get("metrics"))
        if int(metrics.get("resolved") or 0) < int(terminal_horizon):
            continue
        reasons = {str(value) for value in evaluation.get("reasons") or []}
        if reasons & {
            "insufficient_span_days",
            "champion_comparison_insufficient_resolved",
        }:
            # Fast collection must not recreate the old "hit the sample cap,
            # then freeze while calendar gates catch up" failure mode.
            continue
        phase = phases.get(rule_id)
        if phase is None or str(phase.get("status") or "") != "shadow":
            continue
        phase_id = str(phase.get("phase_id") or "")
        store.transition_phase(
            phase_id,
            "rejected",
            expected_status="shadow",
            reason="automatic_challenger_terminal_review_failed",
            actor="automatic_champion",
            metadata={
                "terminal_horizon": int(terminal_horizon),
                "evaluation": dict(evaluation),
            },
        )
        retired.append(phase_id)
    return tuple(retired)


class AutomaticChampionController:
    """Select a publication candidate using only fresh, comparable evidence."""

    def __init__(
        self,
        store: WideResearchStore,
        specs: Sequence[FrozenPortfolioSpec],
        *,
        families: Mapping[str, str],
        baseline_portfolio_id: str,
        report_path: Optional[str] = None,
        production_enabled: bool = False,
        policy: AutomaticChampionPolicy = AutomaticChampionPolicy(),
        pointer_name: str = AUTOMATIC_CHAMPION_POINTER,
    ) -> None:
        normalized = tuple(specs)
        self.store = store
        self.specs = normalized
        self.spec_by_rule = {spec.rule_id: spec for spec in normalized}
        self.spec_by_portfolio = {spec.portfolio_id: spec for spec in normalized}
        if (
            len(self.spec_by_rule) != len(normalized)
            or len(self.spec_by_portfolio) != len(normalized)
        ):
            raise ValueError("automatic champion candidate identities must be unique")
        if baseline_portfolio_id not in self.spec_by_portfolio:
            raise ValueError("automatic champion baseline is missing")
        self.baseline = self.spec_by_portfolio[baseline_portfolio_id]
        self.families = {
            spec.rule_id: str(
                families.get(spec.rule_id)
                or families.get(spec.portfolio_id)
                or spec.portfolio_id
            ).strip()
            for spec in normalized
        }
        if any(not value for value in self.families.values()):
            raise ValueError("every automatic champion candidate needs a family")
        self.report_path = str(report_path) if report_path else None
        self.production_enabled = bool(production_enabled)
        self.policy = policy
        self.pointer_name = str(pointer_name or AUTOMATIC_CHAMPION_POINTER)
        self.policy_manifest = self.policy.manifest()
        self.policy_hash = canonical_hash(self.policy_manifest)
        self.catalog = {
            spec.rule_id: {
                "portfolio_id": spec.portfolio_id,
                "portfolio_hash": spec.manifest()["portfolio_hash"],
                "family_id": self.families[spec.rule_id],
            }
            for spec in sorted(normalized, key=lambda value: value.rule_id)
        }
        self.catalog_hash = canonical_hash(self.catalog)

    def _phases(self) -> dict[str, Mapping[str, Any]]:
        return {
            str(row["rule_id"]): row
            for row in self.store.list_phases()
            if str(row.get("rule_id") or "") in self.spec_by_rule
        }

    def _pointer_metadata(
        self,
        *,
        selector_started_at_utc: str,
        active_since_utc: str,
        previous_rule_id: Optional[str],
        decision: Optional[Mapping[str, Any]] = None,
        publication_suspended: bool = False,
        suspension_reason: Optional[str] = None,
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "selector_started_at_utc": selector_started_at_utc,
            "active_since_utc": active_since_utc,
            "previous_rule_id": previous_rule_id,
            "policy": self.policy_manifest,
            "policy_hash": self.policy_hash,
            "catalog": self.catalog,
            "catalog_hash": self.catalog_hash,
            "production_enabled": self.production_enabled,
            "publication_suspended": bool(publication_suspended),
            "suspension_reason": (
                str(suspension_reason) if suspension_reason else None
            ),
            "last_switch_decision": dict(decision or {}),
        }

    def _initialize_pointer(self, now: datetime) -> Mapping[str, Any]:
        phases = self._phases()
        phase = phases.get(self.baseline.rule_id)
        if phase is None:
            raise KeyError("automatic champion baseline phase is missing")
        now_text = _utc_iso(now)
        metadata = self._pointer_metadata(
            selector_started_at_utc=now_text,
            active_since_utc=now_text,
            previous_rule_id=None,
            decision={"reason": "baseline_registered"},
        )
        status = str(phase.get("status") or "")
        if status == "active":
            return self.store.set_active_pointer(
                self.pointer_name,
                phase_id=self.baseline.phase_id,
                expected_generation=0,
                metadata=metadata,
                updated_at_utc=now_text,
            )
        return self.store.transition_phase_and_set_pointer(
            self.baseline.phase_id,
            "active",
            expected_status=status,
            pointer_name=self.pointer_name,
            pointer_phase_id=self.baseline.phase_id,
            expected_generation=0,
            reason="automatic_champion_baseline_registered",
            actor="automatic_champion",
            metadata=metadata,
            changed_at_utc=now_text,
        )["pointer"]

    def _validate_pointer(
        self, pointer: Mapping[str, Any]
    ) -> tuple[Mapping[str, Any], list[str]]:
        payload = _mapping(pointer.get("payload"))
        reasons: list[str] = []
        if payload.get("schema_version") != 1:
            reasons.append("pointer_schema_mismatch")
        if payload.get("policy_hash") != self.policy_hash:
            reasons.append("policy_hash_mismatch")
        if _mapping(payload.get("policy")) != self.policy_manifest:
            reasons.append("policy_manifest_mismatch")
        stored_catalog = _mapping(payload.get("catalog"))
        catalog_is_append_only = bool(stored_catalog) and all(
            self.catalog.get(str(rule_id)) == entry
            for rule_id, entry in stored_catalog.items()
        )
        if (
            payload.get("catalog_hash") != self.catalog_hash
            or stored_catalog != self.catalog
        ) and not catalog_is_append_only:
            reasons.append("catalog_manifest_mismatch")
        if payload.get("production_enabled") is not self.production_enabled:
            reasons.append("production_mode_mismatch")
        for field in ("selector_started_at_utc", "active_since_utc"):
            try:
                _parse_utc(payload.get(field))
            except (TypeError, ValueError):
                reasons.append(f"{field}_invalid")
        rule_id = str(pointer.get("rule_id") or "")
        spec = self.spec_by_rule.get(rule_id)
        if spec is None:
            reasons.append("active_candidate_missing")
            return payload, reasons
        if str(pointer.get("phase_id") or "") != spec.phase_id:
            reasons.append("active_phase_identity_mismatch")
        stored = _mapping(_mapping(payload.get("catalog")).get(rule_id))
        if stored != self.catalog.get(rule_id):
            reasons.append("active_catalog_entry_mismatch")
        phase = self._phases().get(rule_id)
        if phase is None:
            reasons.append("active_phase_missing")
        else:
            if str(phase.get("phase_id") or "") != spec.phase_id:
                reasons.append("registered_phase_identity_mismatch")
            if str(phase.get("status") or "") != "active":
                reasons.append("active_phase_not_active")
        return payload, reasons

    def _refresh_catalog(
        self,
        pointer: Mapping[str, Any],
        payload: Mapping[str, Any],
        *,
        now: datetime,
    ) -> Mapping[str, Any]:
        """Authorize append-only challengers without resetting the champion."""

        metadata = self._pointer_metadata(
            selector_started_at_utc=_utc_iso(
                _parse_utc(payload.get("selector_started_at_utc"))
            ),
            active_since_utc=_utc_iso(
                _parse_utc(payload.get("active_since_utc"))
            ),
            previous_rule_id=(
                str(payload.get("previous_rule_id"))
                if payload.get("previous_rule_id")
                else None
            ),
            decision={
                "reason": "candidate_catalog_extended",
                "previous_catalog_hash": payload.get("catalog_hash"),
                "catalog_hash": self.catalog_hash,
            },
            publication_suspended=bool(payload.get("publication_suspended")),
            suspension_reason=(
                str(payload.get("suspension_reason"))
                if payload.get("suspension_reason")
                else None
            ),
        )
        return self.store.set_active_pointer(
            self.pointer_name,
            phase_id=str(pointer["phase_id"]),
            expected_generation=int(pointer["generation"]),
            metadata=metadata,
            updated_at_utc=_utc_iso(now),
        )

    def _refresh_policy(
        self,
        pointer: Mapping[str, Any],
        payload: Mapping[str, Any],
        *,
        now: datetime,
    ) -> Mapping[str, Any]:
        """Apply an explicitly supported selector-policy migration."""

        previous_policy = _mapping(payload.get("policy"))
        if previous_policy.get("version") not in {
            "automatic_champion_v2",
            "automatic_champion_v3",
        }:
            raise AutomaticChampionUnavailable(
                "automatic champion policy migration is not authorized"
            )
        metadata = self._pointer_metadata(
            selector_started_at_utc=_utc_iso(
                _parse_utc(payload.get("selector_started_at_utc"))
            ),
            active_since_utc=_utc_iso(
                _parse_utc(payload.get("active_since_utc"))
            ),
            previous_rule_id=(
                str(payload.get("previous_rule_id"))
                if payload.get("previous_rule_id")
                else None
            ),
            decision={
                "reason": "selector_policy_v4_activated",
                "previous_policy_hash": payload.get("policy_hash"),
                "policy_hash": self.policy_hash,
            },
            publication_suspended=bool(payload.get("publication_suspended")),
            suspension_reason=(
                str(payload.get("suspension_reason"))
                if payload.get("suspension_reason")
                else None
            ),
        )
        return self.store.set_active_pointer(
            self.pointer_name,
            phase_id=str(pointer["phase_id"]),
            expected_generation=int(pointer["generation"]),
            metadata=metadata,
            updated_at_utc=_utc_iso(now),
        )

    def active_selection(self) -> ActiveChampionSelection:
        """Return one validated pointer generation or fail publication closed."""

        pointer = self.store.get_active_pointer(self.pointer_name)
        if pointer is None:
            raise AutomaticChampionUnavailable(
                "automatic champion pointer is missing"
            )
        payload, reasons = self._validate_pointer(pointer)
        if reasons:
            raise AutomaticChampionUnavailable(
                "automatic champion pointer is invalid: " + ",".join(reasons)
            )
        if bool(payload.get("publication_suspended")):
            reason = str(
                payload.get("suspension_reason") or "champion_degraded"
            )
            raise AutomaticChampionUnavailable(
                "automatic champion publication is suspended: " + reason
            )
        rule_id = str(pointer.get("rule_id") or "")
        spec = self.spec_by_rule[rule_id]
        return ActiveChampionSelection(
            spec=spec,
            generation=int(pointer["generation"]),
            rule_id=rule_id,
            phase_id=str(pointer["phase_id"]),
            pointer_updated_at_utc=str(pointer["updated_at_utc"]),
        )

    def confirm_selection(
        self, selection: ActiveChampionSelection
    ) -> ActiveChampionSelection:
        """Linearize evaluation against a still-current pointer generation."""

        current = self.active_selection()
        if (
            current.generation != selection.generation
            or current.rule_id != selection.rule_id
            or current.phase_id != selection.phase_id
        ):
            raise AutomaticChampionUnavailable(
                "automatic champion changed during publication evaluation"
            )
        return current

    def active_spec(self) -> FrozenPortfolioSpec:
        """Return the validated active candidate or fail publication closed."""

        return self.active_selection().spec

    def _refresh_production_mode(
        self,
        pointer: Mapping[str, Any],
        payload: Mapping[str, Any],
        *,
        now: datetime,
    ) -> Mapping[str, Any]:
        """Authorize a staged apply-mode change without resetting evidence."""

        now_text = _utc_iso(now)
        metadata = self._pointer_metadata(
            selector_started_at_utc=_utc_iso(
                _parse_utc(payload.get("selector_started_at_utc"))
            ),
            active_since_utc=_utc_iso(
                _parse_utc(payload.get("active_since_utc"))
            ),
            previous_rule_id=(
                str(payload.get("previous_rule_id"))
                if payload.get("previous_rule_id")
                else None
            ),
            decision={
                "reason": "production_mode_changed",
                "production_enabled": self.production_enabled,
            },
            publication_suspended=bool(
                payload.get("publication_suspended")
            ),
            suspension_reason=(
                str(payload.get("suspension_reason"))
                if payload.get("suspension_reason")
                else None
            ),
        )
        return self.store.set_active_pointer(
            self.pointer_name,
            phase_id=str(pointer["phase_id"]),
            expected_generation=int(pointer["generation"]),
            metadata=metadata,
            updated_at_utc=now_text,
        )

    def _metrics(
        self,
        phase_id: str,
        *,
        starts_at: datetime,
    ) -> dict[str, Any]:
        rows = []
        pending = 0
        for row in self.store.list_triggers(phase_id=phase_id):
            triggered = _parse_utc(row.get("triggered_at_utc"))
            if triggered < starts_at:
                continue
            status = str(row.get("latest_outcome_status") or "pending")
            if status == "pending":
                pending += 1
                continue
            if status not in {"win", "loss", "invalid"}:
                pending += 1
                continue
            rows.append((triggered, row, status))
        rows.sort(
            key=lambda item: (
                item[0],
                str(item[1].get("fixture_id") or ""),
            )
        )
        wins = sum(status == "win" for _triggered, _row, status in rows)
        losses = len(rows) - wins
        times = [triggered for triggered, _row, _status in rows]
        span_days = (
            max(0.0, (times[-1] - times[0]).total_seconds() / 86400.0)
            if len(times) >= 2
            else 0.0
        )
        trigger_days = {value.date().isoformat() for value in times}
        leagues = Counter(
            str(row.get("league") or "<unknown>")
            for _triggered, row, _status in rows
        )
        posterior = posterior_summary(wins, losses, policy=self.policy)
        recent = rows[-self.policy.degradation_window :]
        recent_wins = sum(
            status == "win" for _triggered, _row, status in recent
        )
        recent_rate = recent_wins / len(recent) if recent else 1.0
        return {
            "wins": wins,
            "losses": losses,
            "resolved": len(rows),
            "pending": pending,
            "hit_rate": wins / len(rows) if rows else None,
            "span_days": span_days,
            "trigger_days": len(trigger_days),
            "leagues": len(leagues),
            "max_league_share": (
                max(leagues.values(), default=0) / len(rows)
                if rows
                else 1.0
            ),
            "triggers_per_week": (
                len(rows) / max(1.0, span_days / 7.0)
                if rows
                else 0.0
            ),
            "posterior": posterior,
            "recent_resolved": len(recent),
            "recent_hit_rate": recent_rate,
            "degraded": (
                len(recent) >= self.policy.degradation_window
                and recent_rate < self.policy.degradation_rate
            ),
            "starts_at_utc": _utc_iso(starts_at),
        }

    def _eligibility_reasons(self, metrics: Mapping[str, Any]) -> list[str]:
        reasons: list[str] = []
        if int(metrics["resolved"]) < self.policy.min_resolved:
            reasons.append("insufficient_resolved")
        if float(metrics["elapsed_days"]) < self.policy.min_span_days:
            reasons.append("insufficient_span_days")
        return reasons

    def reconcile(self, *, now_utc: Optional[str] = None) -> dict[str, Any]:
        now = _parse_utc(now_utc) if now_utc else datetime.now(timezone.utc)
        pointer = self.store.get_active_pointer(self.pointer_name)
        if pointer is None:
            pointer = self._initialize_pointer(now)
        payload, contract_reasons = self._validate_pointer(pointer)
        if contract_reasons == ["production_mode_mismatch"]:
            pointer = self._refresh_production_mode(
                pointer, payload, now=now
            )
            payload, contract_reasons = self._validate_pointer(pointer)
        if set(contract_reasons) == {
            "policy_hash_mismatch",
            "policy_manifest_mismatch",
        } and _mapping(payload.get("policy")).get("version") in {
            "automatic_champion_v2",
            "automatic_champion_v3",
        }:
            pointer = self._refresh_policy(pointer, payload, now=now)
            payload, contract_reasons = self._validate_pointer(pointer)
        if (
            not contract_reasons
            and (
                payload.get("catalog_hash") != self.catalog_hash
                or _mapping(payload.get("catalog")) != self.catalog
            )
        ):
            pointer = self._refresh_catalog(pointer, payload, now=now)
            payload, contract_reasons = self._validate_pointer(pointer)
        selector_started = _parse_utc(payload.get("selector_started_at_utc"))
        active_since = _parse_utc(payload.get("active_since_utc"))
        active_rule_id = str(pointer.get("rule_id") or "")
        active_phase_id = str(pointer.get("phase_id") or "")
        phases = self._phases()
        if active_rule_id not in phases:
            contract_reasons.append("active_phase_missing")

        active_family = self.families.get(active_rule_id)
        distinct_challenger_families = {
            family
            for rule_id, family in self.families.items()
            if rule_id != active_rule_id and family != active_family
        }
        family_count = max(1, len(distinct_challenger_families))

        evaluations: list[dict[str, Any]] = []
        for spec in self.specs:
            phase = phases.get(spec.rule_id)
            if phase is None:
                evaluations.append(
                    {
                        "rule_id": spec.rule_id,
                        "portfolio_id": spec.portfolio_id,
                        "eligible": False,
                        "reasons": ["phase_missing"],
                    }
                )
                continue
            comparison_start = max(
                selector_started,
                active_since,
                _parse_utc(phase.get("starts_at_utc")),
                _parse_utc(phases[active_rule_id].get("starts_at_utc"))
                if active_rule_id in phases
                else selector_started,
            )
            metrics = self._metrics(
                str(phase["phase_id"]),
                starts_at=comparison_start,
            )
            metrics["elapsed_days"] = max(
                0.0, (now - comparison_start).total_seconds() / 86400.0
            )
            row = {
                "rule_id": spec.rule_id,
                "portfolio_id": spec.portfolio_id,
                "family_id": self.families[spec.rule_id],
                "metrics": metrics,
                "eligible": False,
                "reasons": [],
            }
            if spec.rule_id == active_rule_id:
                row["reasons"] = ["active_champion"]
                evaluations.append(row)
                continue
            reasons = self._eligibility_reasons(metrics)
            if str(phase.get("status") or "") not in {
                "candidate",
                "shadow",
                "ready",
            }:
                reasons.append("candidate_not_competing")
            if self.families[spec.rule_id] == active_family:
                reasons.append("same_family_as_champion")
            active_metrics = self._metrics(
                active_phase_id,
                starts_at=comparison_start,
            )
            probability = superiority_probability(
                _mapping(metrics["posterior"]),
                _mapping(active_metrics["posterior"]),
                margin=self.policy.superiority_margin,
            )
            point_advantage = (
                float(metrics.get("hit_rate") or 0.0)
                - float(active_metrics.get("hit_rate") or 0.0)
            )
            if int(active_metrics["resolved"]) < self.policy.min_resolved:
                reasons.append("champion_comparison_insufficient_resolved")
            if point_advantage < self.policy.superiority_margin:
                reasons.append("hit_rate_margin_below_gate")
            row.update(
                {
                    "champion_metrics": active_metrics,
                    "hit_rate_advantage": point_advantage,
                    "posterior_mean_advantage": (
                        float(_mapping(metrics["posterior"])["mean"])
                        - float(
                            _mapping(active_metrics["posterior"])["mean"]
                        )
                    ),
                    "superiority_probability": probability,
                    "required_superiority_probability": None,
                    "eligible": not reasons and not contract_reasons,
                    "reasons": reasons + list(contract_reasons),
                }
            )
            evaluations.append(row)

        # One representative per pre-registered family may compete.  This
        # prevents adjacent thresholds and near-identical OR-portfolios from
        # producing churn while the multiplicity gate still counts families.
        representatives: dict[str, dict[str, Any]] = {}
        for row in evaluations:
            if row.get("rule_id") == active_rule_id or "metrics" not in row:
                continue
            family = str(row.get("family_id") or "")
            current = representatives.get(family)
            score = (
                float(_mapping(_mapping(row["metrics"]).get("posterior")).get("lower") or 0.0),
                int(_mapping(row["metrics"]).get("resolved") or 0),
                str(row.get("rule_id") or ""),
            )
            current_score = (
                (
                    float(
                        _mapping(
                            _mapping(current["metrics"]).get("posterior")
                        ).get("lower")
                        or 0.0
                    ),
                    int(_mapping(current["metrics"]).get("resolved") or 0),
                    str(current.get("rule_id") or ""),
                )
                if current is not None
                else None
            )
            if current_score is None or score > current_score:
                representatives[family] = row

        eligible = [
            row for row in representatives.values() if row.get("eligible") is True
        ]
        eligible.sort(
            key=lambda row: (
                -float(
                    _mapping(_mapping(row["metrics"])["posterior"])["lower"]
                ),
                -float(row.get("superiority_probability") or 0.0),
                -int(_mapping(row["metrics"]).get("resolved") or 0),
                str(row.get("rule_id") or ""),
            )
        )
        recommended = eligible[0] if eligible else None
        active_evaluation = next(
            (
                row
                for row in evaluations
                if row.get("rule_id") == active_rule_id
            ),
            {},
        )
        active_metrics_before = _mapping(active_evaluation.get("metrics"))
        degradation_detected = bool(
            active_metrics_before.get("degraded")
        )
        # Degraded champions stay on the throne until a prospective challenger
        # wins fairly. Auto-falling back to a "stable" baseline reset every
        # challenger's comparison window and erased hard-earned duel progress.
        degradation_fallback = False

        was_suspended = bool(payload.get("publication_suspended"))
        active_recovered = bool(
            int(active_metrics_before.get("recent_resolved") or 0)
            >= self.policy.degradation_window
            and float(active_metrics_before.get("recent_hit_rate") or 0.0)
            >= self.policy.degradation_recovery_rate
        )
        switched = False
        switch_reason: Optional[str] = None
        previous_rule_id: Optional[str] = None
        if recommended is not None and self.production_enabled:
            previous_rule_id = active_rule_id
            now_text = _utc_iso(now)
            switch_reason = "prospective_challenger_superior"
            decision = {
                "reason": switch_reason,
                "from_rule_id": active_rule_id,
                "to_rule_id": recommended["rule_id"],
                "comparison_started_at_utc": _mapping(
                    recommended["metrics"]
                ).get("starts_at_utc"),
                "hit_rate_advantage": recommended.get(
                    "hit_rate_advantage"
                ),
                "posterior_mean_advantage": recommended.get(
                    "posterior_mean_advantage"
                ),
                "superiority_probability": recommended.get(
                    "superiority_probability"
                ),
                "required_superiority_probability": None,
            }
            metadata = self._pointer_metadata(
                selector_started_at_utc=_utc_iso(selector_started),
                active_since_utc=now_text,
                previous_rule_id=active_rule_id,
                decision=decision,
                publication_suspended=False,
                suspension_reason=None,
            )
            transaction = self.store.swap_active_pointer(
                self.pointer_name,
                from_phase_id=active_phase_id,
                to_phase_id=str(phases[str(recommended["rule_id"])]["phase_id"]),
                expected_generation=int(pointer["generation"]),
                metadata=metadata,
                changed_at_utc=now_text,
            )
            pointer = transaction["pointer"]
            active_rule_id = str(pointer["rule_id"])
            switched = True

        suspension_changed = False
        if (
            not switched
            and self.production_enabled
            and not contract_reasons
            and active_rule_id == self.baseline.rule_id
        ):
            target_suspended = was_suspended
            suspension_reason = (
                str(payload.get("suspension_reason"))
                if payload.get("suspension_reason")
                else None
            )
            suspension_decision: Optional[dict[str, Any]] = None
            if degradation_detected and not was_suspended:
                target_suspended = True
                suspension_reason = "baseline_degraded"
                suspension_decision = {
                    "reason": "baseline_degraded_publication_suspended",
                    "rule_id": active_rule_id,
                    "recent_resolved": active_metrics_before.get(
                        "recent_resolved"
                    ),
                    "recent_hit_rate": active_metrics_before.get(
                        "recent_hit_rate"
                    ),
                }
            elif was_suspended and active_recovered:
                target_suspended = False
                suspension_reason = None
                suspension_decision = {
                    "reason": "baseline_recovered_publication_resumed",
                    "rule_id": active_rule_id,
                    "recent_resolved": active_metrics_before.get(
                        "recent_resolved"
                    ),
                    "recent_hit_rate": active_metrics_before.get(
                        "recent_hit_rate"
                    ),
                }
            if suspension_decision is not None:
                metadata = self._pointer_metadata(
                    selector_started_at_utc=_utc_iso(selector_started),
                    active_since_utc=_utc_iso(active_since),
                    previous_rule_id=(
                        str(payload.get("previous_rule_id"))
                        if payload.get("previous_rule_id")
                        else None
                    ),
                    decision=suspension_decision,
                    publication_suspended=target_suspended,
                    suspension_reason=suspension_reason,
                )
                pointer = self.store.set_active_pointer(
                    self.pointer_name,
                    phase_id=active_phase_id,
                    expected_generation=int(pointer["generation"]),
                    metadata=metadata,
                    updated_at_utc=_utc_iso(now),
                )
                payload = _mapping(pointer.get("payload"))
                suspension_changed = True

        current_active_rule_id = str(pointer.get("rule_id") or "")
        current_evaluation = next(
            (
                row
                for row in evaluations
                if row.get("rule_id") == current_active_rule_id
            ),
            {},
        )
        current_metrics = _mapping(current_evaluation.get("metrics"))
        publication_suspended = bool(
            _mapping(pointer.get("payload")).get("publication_suspended")
        )
        reported_active_since = _parse_utc(
            _mapping(pointer.get("payload")).get("active_since_utc")
        )
        reported_tenure_days = max(
            0.0, (now - reported_active_since).total_seconds() / 86400.0
        )

        result = {
            "schema_version": AUTOMATIC_CHAMPION_REPORT_SCHEMA_VERSION,
            "generated_at_utc": _utc_iso(now),
            "status": (
                "blocked"
                if contract_reasons
                else "suspended"
                if publication_suspended
                else "degraded"
                if bool(current_metrics.get("degraded"))
                else "ok"
            ),
            "production_enabled": self.production_enabled,
            "policy_hash": self.policy_hash,
            "catalog_hash": self.catalog_hash,
            "selector_started_at_utc": _utc_iso(selector_started),
            "active_rule_id": current_active_rule_id,
            "active_portfolio_id": self.spec_by_rule.get(
                current_active_rule_id, self.baseline
            ).portfolio_id,
            "active_since_utc": (
                str(_mapping(pointer.get("payload")).get("active_since_utc"))
            ),
            "active_tenure_days": reported_tenure_days,
            "active_degraded": bool(current_metrics.get("degraded")),
            "degradation_detected_rule_id": (
                str(active_evaluation.get("rule_id"))
                if degradation_detected
                else None
            ),
            "degradation_fallback": bool(
                switched and degradation_fallback
            ),
            "publication_suspended": publication_suspended,
            "suspension_reason": _mapping(pointer.get("payload")).get(
                "suspension_reason"
            ),
            "suspension_changed": suspension_changed,
            "contract_reasons": contract_reasons,
            "candidate_family_count": family_count,
            "recommended_rule_id": (
                str(recommended["rule_id"]) if recommended else None
            ),
            "recommended_portfolio_id": (
                str(recommended["portfolio_id"]) if recommended else None
            ),
            "switched": switched,
            "switch_reason": switch_reason,
            "previous_rule_id": previous_rule_id,
            "evaluations": evaluations,
        }
        result["checksum"] = canonical_hash(result)
        if self.report_path:
            write_champion_report_atomic(self.report_path, result)
        return result


def write_champion_report_atomic(
    path: os.PathLike[str] | str,
    payload: Mapping[str, Any],
) -> None:
    """Write the latest selector decision durably without partial JSON."""

    body = dict(payload)
    supplied = str(body.pop("checksum", "") or "")
    if not supplied or supplied != canonical_hash(body):
        raise ValueError("automatic champion report checksum mismatch")
    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=str(destination.parent),
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(
                dict(payload),
                handle,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, destination)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


__all__ = [
    "ActiveChampionSelection",
    "AUTOMATIC_CHAMPION_POINTER",
    "AUTOMATIC_CHAMPION_POLICY_VERSION",
    "AutomaticChampionController",
    "AutomaticChampionLayer",
    "AutomaticChampionPolicy",
    "AutomaticChampionUnavailable",
    "posterior_summary",
    "superiority_probability",
    "write_champion_report_atomic",
]
