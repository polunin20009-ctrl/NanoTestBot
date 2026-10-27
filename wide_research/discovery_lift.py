"""Promotion / relegation lift between research (D2) and champion duels (D1).

D2 is rare/precision/4f research. D1 is the automatic-champion discovery
challenger table. This module opens slots for stronger mature D2 rules by
relegating hopeless D1 challengers, without touching the live champion pointer
or resetting other duel windows.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .automatic_champion import (
    DISCOVERY_NOMINATION_MIN_HIT_RATE,
    DISCOVERY_NOMINATION_MIN_RESOLVED,
    DISCOVERY_NOMINATION_PREFIX,
    DISCOVERY_NOMINATION_SOURCE_STATUSES,
    _discovery_nomination_phase_metrics,
    _discovery_nomination_qualifies,
    discovery_nomination_family,
    discovery_nomination_source_key,
    import_ready_discovery_evidence,
    is_discovery_nomination,
)
from .lifecycle import wilson_interval
from .portfolio import (
    FrozenPortfolioMember,
    FrozenPortfolioSpec,
)
from .rules import rule_manifest_from_dict
from .store import WideResearchStore


DISCOVERY_LIFT_STATE_VERSION = 1
DISCOVERY_LIFT_RELEGATE_REASON = "discovery_lift_relegated"
DISCOVERY_LIFT_ADMIT_REASON = "discovery_lift_admitted"


@dataclass(frozen=True)
class DiscoveryLiftPolicy:
    """Tunable promotion / relegation gates."""

    max_swaps_per_run: int = 2
    min_interval_hours: float = 24.0
    d2_min_resolved: int = DISCOVERY_NOMINATION_MIN_RESOLVED
    d2_min_hit_rate: float = DISCOVERY_NOMINATION_MIN_HIT_RATE
    d2_reentry_min_hit_rate: float = 0.88
    d2_reentry_cooldown_days: int = 14
    d1_min_duel_resolved: int = 200
    d1_max_advantage_pp: float = 0.01
    hard_entry_hit_rate: float = 0.90
    ordinary_edge_pp: float = 0.02
    newbie_protection_days: int = 7
    newbie_protection_duel_matches: int = 40


@dataclass(frozen=True)
class DiscoveryLiftCandidate:
    source_profile: str
    source_rule_id: str
    source_phase_id: str
    status: str
    wins: int
    resolved: int
    hit_rate: float
    wilson_lower: float
    family_id: str
    reentry: bool
    manifest: Mapping[str, Any]
    manifest_hash: str

    @property
    def source_key(self) -> str:
        return f"{self.source_profile}:{self.source_rule_id}"


@dataclass(frozen=True)
class DiscoveryLiftIncumbent:
    rule_id: str
    phase_id: str
    portfolio_id: str
    family_id: str
    source_profile: str
    source_rule_id: str
    duel_resolved: int
    duel_hit_rate: float
    duel_wins: int
    advantage: float
    wilson_lower: float
    protected: bool
    eligible: bool


@dataclass(frozen=True)
class DiscoveryLiftSwap:
    candidate: DiscoveryLiftCandidate
    incumbent: Optional[DiscoveryLiftIncumbent]
    mode: str  # "fill" | "hard" | "ordinary"

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "candidate": {
                "source_key": self.candidate.source_key,
                "source_profile": self.candidate.source_profile,
                "source_rule_id": self.candidate.source_rule_id,
                "hit_rate": self.candidate.hit_rate,
                "resolved": self.candidate.resolved,
                "wilson_lower": self.candidate.wilson_lower,
                "reentry": self.candidate.reentry,
                "family_id": self.candidate.family_id,
            },
            "incumbent": (
                None
                if self.incumbent is None
                else {
                    "rule_id": self.incumbent.rule_id,
                    "portfolio_id": self.incumbent.portfolio_id,
                    "source_key": (
                        f"{self.incumbent.source_profile}:"
                        f"{self.incumbent.source_rule_id}"
                    ),
                    "duel_hit_rate": self.incumbent.duel_hit_rate,
                    "duel_resolved": self.incumbent.duel_resolved,
                    "advantage": self.incumbent.advantage,
                    "wilson_lower": self.incumbent.wilson_lower,
                    "family_id": self.incumbent.family_id,
                }
            ),
        }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def default_discovery_lift_state_path() -> Path:
    return Path("stats") / "discovery_division_lift.json"


def load_discovery_lift_state(path: Path | str) -> dict[str, Any]:
    target = Path(path)
    if not target.is_file():
        return {
            "version": DISCOVERY_LIFT_STATE_VERSION,
            "last_run_utc": None,
            "cooldowns": {},
            "history": [],
        }
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "version": DISCOVERY_LIFT_STATE_VERSION,
            "last_run_utc": None,
            "cooldowns": {},
            "history": [],
        }
    if not isinstance(payload, dict):
        return {
            "version": DISCOVERY_LIFT_STATE_VERSION,
            "last_run_utc": None,
            "cooldowns": {},
            "history": [],
        }
    payload.setdefault("version", DISCOVERY_LIFT_STATE_VERSION)
    payload.setdefault("last_run_utc", None)
    payload.setdefault("cooldowns", {})
    payload.setdefault("history", [])
    return payload


def save_discovery_lift_state(path: Path | str, payload: Mapping[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(dict(payload), indent=2, sort_keys=True) + "\n"
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(target.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, target)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.unlink(tmp_name)
            except OSError:
                pass


def _cooldown_active(
    state: Mapping[str, Any],
    *,
    source_key: str,
    now: datetime,
) -> bool:
    raw = _mapping(state.get("cooldowns")).get(source_key)
    until = _parse_utc(raw)
    return until is not None and until > now


def _family_for_manifest(
    *,
    source_profile: str,
    manifest_payload: Mapping[str, Any],
    manifest_hash: str,
) -> str:
    manifest = rule_manifest_from_dict(manifest_payload)
    member = FrozenPortfolioMember(
        source_profile=str(source_profile),
        source_store_hash=str(manifest_hash),
        manifest=manifest,
    )
    spec = FrozenPortfolioSpec(
        portfolio_id=f"{DISCOVERY_NOMINATION_PREFIX}probe-{manifest.rule_id}",
        version="v1",
        members=(member,),
    )
    return discovery_nomination_family(spec)


def _build_nomination_spec(candidate: DiscoveryLiftCandidate) -> FrozenPortfolioSpec:
    manifest = rule_manifest_from_dict(candidate.manifest)
    member = FrozenPortfolioMember(
        source_profile=candidate.source_profile,
        source_store_hash=candidate.manifest_hash,
        manifest=manifest,
    )
    profile_slug = candidate.source_profile.replace("_", "-")
    return FrozenPortfolioSpec(
        portfolio_id=(
            f"{DISCOVERY_NOMINATION_PREFIX}{profile_slug}-"
            f"{manifest.rule_id}"
        ),
        version="v1",
        members=(member,),
    )


def collect_d2_lift_candidates(
    sources: Sequence[tuple[str, WideResearchStore]],
    *,
    admitted_source_keys: set[tuple[str, str]],
    state: Mapping[str, Any],
    policy: DiscoveryLiftPolicy,
    now: datetime,
) -> list[DiscoveryLiftCandidate]:
    """Rank mature D2 rules that are allowed to request a D1 slot."""

    ranked: list[DiscoveryLiftCandidate] = []
    for source_profile, source_store in sources:
        profile = str(source_profile)
        for phase in source_store.list_phases(
            statuses=DISCOVERY_NOMINATION_SOURCE_STATUSES
        ):
            rule_id = str(phase.get("rule_id") or "")
            key = (profile, rule_id)
            if not rule_id or key in admitted_source_keys:
                continue
            status = str(phase.get("status") or "")
            wins, resolved, hit_rate = _discovery_nomination_phase_metrics(
                source_store, phase
            )
            source_key = f"{profile}:{rule_id}"
            reentry = source_key in _mapping(state.get("cooldowns"))
            if reentry and _cooldown_active(
                state, source_key=source_key, now=now
            ):
                continue
            min_hit = (
                policy.d2_reentry_min_hit_rate
                if reentry
                else policy.d2_min_hit_rate
            )
            if not _discovery_nomination_qualifies(
                status=status,
                resolved=resolved,
                hit_rate=hit_rate,
                min_resolved=policy.d2_min_resolved,
                min_hit_rate=min_hit,
            ):
                continue
            # Re-entry always requires the mature sample floor even for READY.
            if reentry and (
                resolved < policy.d2_min_resolved
                or hit_rate + 1e-12 < policy.d2_reentry_min_hit_rate
            ):
                continue
            manifest_payload = phase.get("manifest")
            if not isinstance(manifest_payload, Mapping):
                continue
            family_id = _family_for_manifest(
                source_profile=profile,
                manifest_payload=manifest_payload,
                manifest_hash=str(phase.get("manifest_hash") or ""),
            )
            lower, _upper = wilson_interval(wins, resolved)
            ranked.append(
                DiscoveryLiftCandidate(
                    source_profile=profile,
                    source_rule_id=rule_id,
                    source_phase_id=str(phase.get("phase_id") or ""),
                    status=status,
                    wins=int(wins),
                    resolved=int(resolved),
                    hit_rate=float(hit_rate),
                    wilson_lower=float(lower),
                    family_id=family_id,
                    reentry=bool(reentry),
                    manifest=dict(manifest_payload),
                    manifest_hash=str(phase.get("manifest_hash") or ""),
                )
            )
    ranked.sort(
        key=lambda row: (
            -row.wilson_lower,
            -row.hit_rate,
            -row.resolved,
            row.source_profile,
            row.source_rule_id,
        )
    )
    return ranked


def _incumbent_protected(
    phase: Mapping[str, Any],
    *,
    duel_resolved: int,
    now: datetime,
    policy: DiscoveryLiftPolicy,
) -> bool:
    lift = _mapping(
        _mapping(phase.get("policy")).get("automatic_champion")
    ).get("discovery_lift")
    lift_map = _mapping(lift)
    until = _parse_utc(lift_map.get("protection_until_utc"))
    if until is not None and until > now:
        return True
    admitted_duel = lift_map.get("admitted_duel_resolved")
    if admitted_duel is None:
        return False
    try:
        floor = int(admitted_duel) + int(policy.newbie_protection_duel_matches)
    except (TypeError, ValueError):
        return False
    return int(duel_resolved) < floor


def collect_d1_lift_incumbents(
    *,
    target_store: WideResearchStore,
    existing_specs: Sequence[FrozenPortfolioSpec],
    decision: Mapping[str, Any],
    policy: DiscoveryLiftPolicy,
    now: datetime,
) -> list[DiscoveryLiftIncumbent]:
    """Rank hopable D1 discovery challengers (worst first)."""

    phases = {
        str(row.get("rule_id") or ""): row
        for row in target_store.list_phases()
    }
    evaluations = {
        str(row.get("rule_id") or ""): row
        for row in (decision.get("evaluations") or [])
        if isinstance(row, Mapping)
    }
    incumbents: list[DiscoveryLiftIncumbent] = []
    for spec in existing_specs:
        if not is_discovery_nomination(spec):
            continue
        phase = phases.get(spec.rule_id)
        if phase is None or str(phase.get("status") or "") not in {
            "candidate",
            "shadow",
            "ready",
        }:
            continue
        if spec.rule_id == str(decision.get("active_rule_id") or ""):
            continue
        evaluation = evaluations.get(spec.rule_id) or {}
        metrics = _mapping(evaluation.get("metrics"))
        duel_resolved = int(metrics.get("resolved") or 0)
        if duel_resolved < int(policy.d1_min_duel_resolved):
            continue
        if evaluation.get("eligible") is True:
            continue
        advantage = evaluation.get("hit_rate_advantage")
        try:
            adv = float(advantage)
        except (TypeError, ValueError):
            continue
        if adv + 1e-12 >= float(policy.d1_max_advantage_pp):
            continue
        duel_hit_rate = metrics.get("hit_rate")
        try:
            hit_rate = float(duel_hit_rate)
        except (TypeError, ValueError):
            continue
        wins = int(round(hit_rate * duel_resolved))
        lower, _upper = wilson_interval(wins, duel_resolved)
        source_profile, source_rule_id = discovery_nomination_source_key(spec)
        protected = _incumbent_protected(
            phase,
            duel_resolved=duel_resolved,
            now=now,
            policy=policy,
        )
        if protected:
            continue
        incumbents.append(
            DiscoveryLiftIncumbent(
                rule_id=spec.rule_id,
                phase_id=str(phase.get("phase_id") or ""),
                portfolio_id=spec.portfolio_id,
                family_id=discovery_nomination_family(spec),
                source_profile=source_profile,
                source_rule_id=source_rule_id,
                duel_resolved=duel_resolved,
                duel_hit_rate=hit_rate,
                duel_wins=wins,
                advantage=adv,
                wilson_lower=float(lower),
                protected=False,
                eligible=False,
            )
        )
    incumbents.sort(
        key=lambda row: (
            row.advantage,
            row.wilson_lower,
            -row.duel_resolved,
            row.rule_id,
        )
    )
    return incumbents


def _candidate_beats_incumbent(
    candidate: DiscoveryLiftCandidate,
    incumbent: DiscoveryLiftIncumbent,
    *,
    policy: DiscoveryLiftPolicy,
) -> str | None:
    # A) hard entry
    if (
        candidate.hit_rate + 1e-12 >= float(policy.hard_entry_hit_rate)
        and candidate.resolved >= int(policy.d2_min_resolved)
        and incumbent.advantage <= 1e-12
    ):
        return "hard"
    # B) ordinary entry
    if (
        candidate.wilson_lower > incumbent.wilson_lower + 1e-12
        and candidate.hit_rate + 1e-12
        >= incumbent.duel_hit_rate + float(policy.ordinary_edge_pp)
        and candidate.resolved >= int(policy.d2_min_resolved)
        and incumbent.duel_resolved >= int(policy.d1_min_duel_resolved)
    ):
        return "ordinary"
    return None


def plan_discovery_division_lifts(
    sources: Sequence[tuple[str, WideResearchStore]],
    *,
    target_store: WideResearchStore,
    existing_specs: Sequence[FrozenPortfolioSpec],
    decision: Mapping[str, Any],
    state: Mapping[str, Any],
    policy: DiscoveryLiftPolicy = DiscoveryLiftPolicy(),
    now: Optional[datetime] = None,
    max_active: int = 20,
) -> tuple[DiscoveryLiftSwap, ...]:
    """Propose fill/relegation swaps without mutating stores."""

    current = now or _utc_now()
    last_run = _parse_utc(state.get("last_run_utc"))
    if last_run is not None:
        elapsed_h = (current - last_run).total_seconds() / 3600.0
        if elapsed_h + 1e-9 < float(policy.min_interval_hours):
            return ()

    phases = {
        str(row.get("rule_id") or ""): str(row.get("status") or "")
        for row in target_store.list_phases()
    }
    active_dynamic = [
        spec
        for spec in existing_specs
        if is_discovery_nomination(spec)
        and phases.get(spec.rule_id) in {"candidate", "shadow", "ready", "active"}
    ]
    admitted_keys = {
        discovery_nomination_source_key(spec)
        for spec in existing_specs
        if is_discovery_nomination(spec)
    }
    occupied_families = {
        discovery_nomination_family(spec) for spec in active_dynamic
    }
    open_slots = max(0, int(max_active) - len(active_dynamic))
    candidates = collect_d2_lift_candidates(
        sources,
        admitted_source_keys=admitted_keys,
        state=state,
        policy=policy,
        now=current,
    )
    incumbents = collect_d1_lift_incumbents(
        target_store=target_store,
        existing_specs=existing_specs,
        decision=decision,
        policy=policy,
        now=current,
    )

    swaps: list[DiscoveryLiftSwap] = []
    used_candidate_keys: set[str] = set()
    used_incumbent_ids: set[str] = set()
    used_families = set(occupied_families)

    # 1) Fill empty slots first.
    for candidate in candidates:
        if len(swaps) >= int(policy.max_swaps_per_run):
            break
        if open_slots <= 0:
            break
        if candidate.source_key in used_candidate_keys:
            continue
        if candidate.family_id in used_families:
            continue
        swaps.append(
            DiscoveryLiftSwap(
                candidate=candidate,
                incumbent=None,
                mode="fill",
            )
        )
        used_candidate_keys.add(candidate.source_key)
        used_families.add(candidate.family_id)
        open_slots -= 1

    # 2) Relegate hopeless incumbents for stronger outsiders.
    for candidate in candidates:
        if len(swaps) >= int(policy.max_swaps_per_run):
            break
        if candidate.source_key in used_candidate_keys:
            continue
        for incumbent in incumbents:
            if incumbent.rule_id in used_incumbent_ids:
                continue
            # Same-family replacement is allowed (stronger sibling). Otherwise
            # the candidate may not take a family seat already held by another
            # active challenger.
            if (
                candidate.family_id in used_families
                and candidate.family_id != incumbent.family_id
            ):
                continue
            mode = _candidate_beats_incumbent(
                candidate, incumbent, policy=policy
            )
            if mode is None:
                continue
            swaps.append(
                DiscoveryLiftSwap(
                    candidate=candidate,
                    incumbent=incumbent,
                    mode=mode,
                )
            )
            used_candidate_keys.add(candidate.source_key)
            used_incumbent_ids.add(incumbent.rule_id)
            used_families.discard(incumbent.family_id)
            used_families.add(candidate.family_id)
            break
    return tuple(swaps)


def apply_discovery_division_lifts(
    swaps: Sequence[DiscoveryLiftSwap],
    *,
    sources: Sequence[tuple[str, WideResearchStore]],
    target_store: WideResearchStore,
    decision: Mapping[str, Any],
    state: Mapping[str, Any],
    state_path: Path | str,
    policy: DiscoveryLiftPolicy = DiscoveryLiftPolicy(),
    now: Optional[datetime] = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Apply planned swaps and persist lift state."""

    current = now or _utc_now()
    source_map = {str(profile): store for profile, store in sources}
    applied: list[dict[str, Any]] = []
    errors: list[str] = []
    # Dry-run must not advance last_run_utc — that would throttle real lifts.
    next_state = {
        "version": DISCOVERY_LIFT_STATE_VERSION,
        "last_run_utc": (
            state.get("last_run_utc") if dry_run else _utc_iso(current)
        ),
        "cooldowns": dict(_mapping(state.get("cooldowns"))),
        "history": list(state.get("history") or [])[-50:],
    }

    if dry_run:
        next_state["history"].append(
            {
                "at_utc": _utc_iso(current),
                "dry_run": True,
                "swaps": [swap.as_dict() for swap in swaps],
            }
        )
        next_state["history"] = next_state["history"][-50:]
        save_discovery_lift_state(state_path, next_state)
        return {
            "dry_run": True,
            "planned": [swap.as_dict() for swap in swaps],
            "applied": [],
            "errors": [],
        }

    evaluations = {
        str(row.get("rule_id") or ""): row
        for row in (decision.get("evaluations") or [])
        if isinstance(row, Mapping)
    }

    for swap in swaps:
        try:
            if swap.incumbent is not None:
                incumbent_phase = next(
                    (
                        row
                        for row in target_store.list_phases()
                        if str(row.get("phase_id") or "")
                        == swap.incumbent.phase_id
                    ),
                    None,
                )
                if incumbent_phase is None:
                    raise RuntimeError(
                        f"missing incumbent phase {swap.incumbent.phase_id}"
                    )
                target_store.transition_phase(
                    swap.incumbent.phase_id,
                    "rejected",
                    expected_status=str(
                        incumbent_phase.get("status") or "shadow"
                    ),
                    reason=DISCOVERY_LIFT_RELEGATE_REASON,
                    actor="discovery_lift",
                    metadata={
                        "swap": swap.as_dict(),
                        "source_profile": swap.incumbent.source_profile,
                        "source_rule_id": swap.incumbent.source_rule_id,
                        "cooldown_until_utc": _utc_iso(
                            current
                            + timedelta(
                                days=int(policy.d2_reentry_cooldown_days)
                            )
                        ),
                    },
                    changed_at_utc=_utc_iso(current),
                )
                next_state["cooldowns"][
                    f"{swap.incumbent.source_profile}:"
                    f"{swap.incumbent.source_rule_id}"
                ] = _utc_iso(
                    current
                    + timedelta(days=int(policy.d2_reentry_cooldown_days))
                )

            source_store = source_map.get(swap.candidate.source_profile)
            if source_store is None:
                raise RuntimeError(
                    f"missing source store {swap.candidate.source_profile}"
                )
            spec = _build_nomination_spec(swap.candidate)
            family_id = swap.candidate.family_id
            imported = import_ready_discovery_evidence(
                source_store,
                target_store,
                spec,
                family_id=family_id,
            )
            # Annotate protection on the new phase.
            phase_rows = target_store.list_phases(rule_id=spec.rule_id)
            if len(phase_rows) == 1:
                phase = phase_rows[0]
                duel_at_admit = int(
                    _mapping(
                        _mapping(evaluations.get(spec.rule_id)).get("metrics")
                    ).get("resolved")
                    or 0
                )
                # New nomination starts with imported evidence; protection
                # should count from current duel book size after import.
                metrics = target_store.metrics_for_phase(
                    str(phase.get("phase_id") or "")
                )
                duel_at_admit = int(metrics.get("win") or 0) + int(
                    metrics.get("loss") or 0
                )
                _patch_phase_discovery_lift_policy(
                    target_store,
                    phase_id=str(phase.get("phase_id") or ""),
                    expected_status=str(phase.get("status") or "shadow"),
                    now=current,
                    policy=policy,
                    admitted_duel_resolved=duel_at_admit,
                    swap=swap,
                )
            applied.append(
                {
                    **swap.as_dict(),
                    "imported": imported,
                    "portfolio_id": spec.portfolio_id,
                }
            )
            # Successful promotion clears cooldown for this source key.
            next_state["cooldowns"].pop(swap.candidate.source_key, None)
        except Exception as exc:  # pragma: no cover - surfaced to caller logs
            errors.append(f"{swap.candidate.source_key}: {exc}")

    next_state["history"].append(
        {
            "at_utc": _utc_iso(current),
            "dry_run": False,
            "applied": applied,
            "errors": errors,
        }
    )
    next_state["history"] = next_state["history"][-50:]
    save_discovery_lift_state(state_path, next_state)
    return {
        "dry_run": False,
        "planned": [swap.as_dict() for swap in swaps],
        "applied": applied,
        "errors": errors,
    }


def _patch_phase_discovery_lift_policy(
    store: WideResearchStore,
    *,
    phase_id: str,
    expected_status: str,
    now: datetime,
    policy: DiscoveryLiftPolicy,
    admitted_duel_resolved: int,
    swap: DiscoveryLiftSwap,
) -> None:
    """Write discovery_lift protection into phase policy + lifecycle note."""

    protection_until = now + timedelta(days=int(policy.newbie_protection_days))
    lift_payload = {
        "admitted_at_utc": _utc_iso(now),
        "protection_until_utc": _utc_iso(protection_until),
        "admitted_duel_resolved": int(admitted_duel_resolved),
        "newbie_protection_duel_matches": int(
            policy.newbie_protection_duel_matches
        ),
        "mode": swap.mode,
    }
    # Direct policy patch through store private write would be brittle across
    # versions; use note event always, and attempt an in-place policy update
    # via the same pattern as freeze helpers when status matches.
    row = None
    for item in store.list_phases():
        if str(item.get("phase_id") or "") == phase_id:
            row = item
            break
    if row is None:
        return
    current_policy = dict(_mapping(row.get("policy")))
    automatic = dict(_mapping(current_policy.get("automatic_champion")))
    automatic["discovery_lift"] = lift_payload
    current_policy["automatic_champion"] = automatic
    # store has no generic merge API; encode via lifecycle note and keep the
    # protection readable from event metadata for collect_d1 checks by also
    # stashing on the in-memory row after a same-status note.
    store.note_phase_lifecycle_event_once(
        phase_id,
        expected_status=expected_status,
        reason=DISCOVERY_LIFT_ADMIT_REASON,
        metadata=lift_payload,
        changed_at_utc=_utc_iso(now),
    )
    # Use transition no-op path is not available; write policy with a tiny
    # dedicated update implemented locally through SQLite-backed store helper.
    _write_phase_policy(store, phase_id=phase_id, policy=current_policy, now=now)


def _write_phase_policy(
    store: WideResearchStore,
    *,
    phase_id: str,
    policy: Mapping[str, Any],
    now: datetime,
) -> None:
    """Update one phase policy blob atomically."""

    from . import store as store_module

    policy_json = store_module._canonical_json(dict(policy))
    policy_hash = store_module._json_hash(policy_json)
    changed = _utc_iso(now)
    with store._write(projected_bytes=len(policy_json) + 1024) as connection:
        row = connection.execute(
            "SELECT status FROM phases WHERE phase_id=?",
            (phase_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown phase {phase_id}")
        connection.execute(
            "UPDATE phases SET policy_json=?, policy_hash=?, "
            "updated_at_utc=? WHERE phase_id=?",
            (policy_json, policy_hash, changed, phase_id),
        )


def run_discovery_division_lift(
    sources: Sequence[tuple[str, WideResearchStore]],
    *,
    target_store: WideResearchStore,
    existing_specs: Sequence[FrozenPortfolioSpec],
    decision: Mapping[str, Any],
    state_path: Path | str,
    policy: DiscoveryLiftPolicy = DiscoveryLiftPolicy(),
    max_active: int = 20,
    now: Optional[datetime] = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Plan and apply one lift pass."""

    current = now or _utc_now()
    state = load_discovery_lift_state(state_path)
    swaps = plan_discovery_division_lifts(
        sources,
        target_store=target_store,
        existing_specs=existing_specs,
        decision=decision,
        state=state,
        policy=policy,
        now=current,
        max_active=max_active,
    )
    if not swaps:
        # Still stamp last_run only when interval allows? Keep unchanged if
        # throttled with empty plan due to interval; plan already returns ().
        last_run = _parse_utc(state.get("last_run_utc"))
        if last_run is not None:
            elapsed_h = (current - last_run).total_seconds() / 3600.0
            if elapsed_h + 1e-9 < float(policy.min_interval_hours):
                return {
                    "throttled": True,
                    "planned": [],
                    "applied": [],
                    "errors": [],
                    "dry_run": dry_run,
                }
        # No swaps available; record the attempt so we do not spin every loop.
        state = {
            **state,
            "last_run_utc": _utc_iso(current),
        }
        if not dry_run:
            save_discovery_lift_state(state_path, state)
        return {
            "throttled": False,
            "planned": [],
            "applied": [],
            "errors": [],
            "dry_run": dry_run,
        }
    return apply_discovery_division_lifts(
        swaps,
        sources=sources,
        target_store=target_store,
        decision=decision,
        state=state,
        state_path=state_path,
        policy=policy,
        now=current,
        dry_run=dry_run,
    )


__all__ = [
    "DiscoveryLiftCandidate",
    "DiscoveryLiftIncumbent",
    "DiscoveryLiftPolicy",
    "DiscoveryLiftSwap",
    "apply_discovery_division_lifts",
    "collect_d1_lift_incumbents",
    "collect_d2_lift_candidates",
    "default_discovery_lift_state_path",
    "load_discovery_lift_state",
    "plan_discovery_division_lifts",
    "run_discovery_division_lift",
    "save_discovery_lift_state",
]
