"""Registry and lifecycle controller for the wide rule factory."""

from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .lifecycle import (
    GateDecision,
    LifecyclePolicy,
    SHADOW,
    binomial_upper_tail,
    build_active_manifest,
    evaluate_degradation,
    evaluate_readiness,
    validate_active_manifest,
    wilson_interval,
    write_active_manifest_atomic,
)
from .rules import Clause, RuleManifest
from .features import FEATURE_SCHEMA_VERSION
from .extended_features import EXTENDED_FEATURES
from .discovery import (
    PURGED_SPLIT_POLICY,
    TEMPORAL_PURGE_VERSION,
    TEMPORAL_PURGED_ENGINE_SUFFIX,
)
from .schema import UniverseSpec, canonical_hash
from .store import WideResearchStore


EVALUATED_STATUSES = ("candidate", "shadow", "ready", "active")
IMPORT_IDENTITY_STATUSES = (*EVALUATED_STATUSES, "paused")
TERMINAL_REVIEW_POLICY_VERSION = "wide_terminal_review_v1"
MAX_EVALUATED_PHASES = 64


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


def _phase_feature_schema_compatible(phase: Mapping[str, Any]) -> bool:
    """Return whether a phase is pinned to the extractor running now."""

    manifest = _mapping(phase.get("manifest"))
    value = manifest.get("feature_schema_version")
    return type(value) is int and value == FEATURE_SCHEMA_VERSION


def _phase_lifecycle_policy(
    phase: Mapping[str, Any],
) -> Optional[LifecyclePolicy]:
    """Rebuild the immutable policy registered with a prospective phase."""

    payload = _mapping(_mapping(phase.get("policy")).get("lifecycle"))
    if not payload:
        return None
    expected = set(LifecyclePolicy.__dataclass_fields__)
    if set(payload) != expected:
        return None
    values = dict(payload)
    looks = values.get("allowed_looks")
    if not isinstance(looks, Sequence) or isinstance(looks, (str, bytes)):
        return None
    values["allowed_looks"] = tuple(looks)
    try:
        policy = LifecyclePolicy(**values)
    except (TypeError, ValueError):
        return None
    normalized_manifest = policy.manifest()
    normalized_manifest["allowed_looks"] = list(policy.allowed_looks)
    return policy if normalized_manifest == dict(payload) else None


def _phase_runtime_compatible(phase: Mapping[str, Any]) -> bool:
    policy = _mapping(phase.get("policy"))
    terminal_review = policy.get("terminal_review")
    return (
        _phase_feature_schema_compatible(phase)
        and _phase_lifecycle_policy(phase) is not None
        and (
            not terminal_review
            or _phase_terminal_review_policy(phase) is not None
        )
    )


def _phase_terminal_review_policy(
    phase: Mapping[str, Any],
) -> Optional[dict[str, Any]]:
    """Parse the optional, immutable hard-shadow terminal review contract."""

    payload = _mapping(_mapping(phase.get("policy")).get("terminal_review"))
    if not payload:
        return None
    if set(payload) != {
        "version",
        "enabled",
        "final_look",
        "min_point_hit_rate",
    }:
        return None
    if payload.get("version") != TERMINAL_REVIEW_POLICY_VERSION:
        return None
    enabled = payload.get("enabled")
    final_look = payload.get("final_look")
    min_rate = payload.get("min_point_hit_rate")
    if type(enabled) is not bool or type(final_look) is not int or final_look < 1:
        return None
    try:
        normalized_rate = float(min_rate)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(normalized_rate) or not 0.0 <= normalized_rate <= 1.0:
        return None
    lifecycle = _phase_lifecycle_policy(phase)
    if lifecycle is None or final_look != max(lifecycle.allowed_looks):
        return None
    return {
        "version": TERMINAL_REVIEW_POLICY_VERSION,
        "enabled": enabled,
        "final_look": final_look,
        "min_point_hit_rate": normalized_rate,
    }


def _terminal_phase(phase: Mapping[str, Any]) -> bool:
    # Consult the frozen policy rather than the current controller flag.
    return _mapping(_mapping(phase.get("policy")).get("terminal_review")).get(
        "enabled"
    ) is True


def _execution_fingerprint(manifest: Mapping[str, Any]) -> str:
    return canonical_hash({
        name: manifest.get(name)
        for name in ("clauses", "universe", "semantics", "feature_schema_version")
    })


def _verify_hash(
    payload: Mapping[str, Any],
    field: str,
    *,
    excluded_fields: Sequence[str] = (),
) -> None:
    supplied = str(payload.get(field) or "")
    body = dict(payload)
    body.pop(field, None)
    for excluded in excluded_fields:
        body.pop(excluded, None)
    if not supplied or canonical_hash(body) != supplied:
        raise ValueError(f"{field} checksum mismatch")


def _validate_temporal_partition_contract(manifests: Mapping[str, Any]) -> None:
    """Reject a purged-engine label without a complete, auditable purge.

    The discovery process is local, but it writes the prospective registry
    before the parent process sees its compact worker summary.  Validation
    therefore belongs at the controller boundary as well as in the parent.
    """

    engine = str(manifests.get("engine_version") or "")
    split_policy = str(manifests.get("split_policy") or "")
    config = _mapping(manifests.get("config"))
    splits = _mapping(manifests.get("splits"))
    engine_purged = engine.endswith(TEMPORAL_PURGED_ENGINE_SUFFIX)
    config_purged = config.get("temporal_purge") is True
    policy_purged = split_policy == PURGED_SPLIT_POLICY
    audit_present = "temporal_purge" in splits
    if not any((engine_purged, config_purged, policy_purged, audit_present)):
        return
    if not all((engine_purged, config_purged, policy_purged, audit_present)):
        raise ValueError("incomplete temporal purge discovery contract")
    if engine == TEMPORAL_PURGED_ENGINE_SUFFIX:
        raise ValueError("temporal purge engine base is missing")

    try:
        configured_embargo = float(config.get("temporal_embargo_seconds"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("invalid temporal purge embargo") from exc
    if not math.isfinite(configured_embargo) or configured_embargo < 0.0:
        raise ValueError("invalid temporal purge embargo")

    split_times: dict[str, tuple[datetime, datetime]] = {}
    split_ids: dict[str, tuple[int, ...]] = {}
    for name in ("train", "validation", "holdout"):
        details = _mapping(splits.get(name))
        ids = details.get("fixture_ids")
        if not isinstance(ids, Sequence) or isinstance(ids, (str, bytes)):
            raise ValueError(f"temporal purge {name} fixture IDs are missing")
        normalized_ids = tuple(ids)
        if (
            any(type(value) is not int or value <= 0 for value in normalized_ids)
            or len(normalized_ids) != len(set(normalized_ids))
        ):
            raise ValueError(f"temporal purge {name} fixture IDs are invalid")
        try:
            fixture_count = int(details.get("fixture_count"))
            observation_count = int(details.get("observation_count"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"temporal purge {name} counters are invalid") from exc
        if (
            fixture_count != len(normalized_ids)
            or fixture_count <= 0
            or observation_count < fixture_count
            or details.get("fixture_ids_sha256")
            != canonical_hash(normalized_ids)
        ):
            raise ValueError(f"temporal purge {name} audit is inconsistent")
        first = _parse_utc(details.get("first_observation_utc"))
        last = _parse_utc(details.get("last_observation_utc"))
        if first > last:
            raise ValueError(f"temporal purge {name} timestamps are invalid")
        split_times[name] = (first, last)
        split_ids[name] = normalized_ids

    if any(
        set(split_ids[left]).intersection(split_ids[right])
        for left, right in (
            ("train", "validation"),
            ("train", "holdout"),
            ("validation", "holdout"),
        )
    ):
        raise ValueError("temporal purge fixture groups overlap")

    audit = _mapping(splits.get("temporal_purge"))
    try:
        audit_embargo = float(audit.get("embargo_seconds"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("invalid temporal purge audit embargo") from exc
    boundaries = _mapping(audit.get("boundaries"))
    if (
        audit.get("version") != TEMPORAL_PURGE_VERSION
        or audit.get("no_fixture_reallocation") is not True
        or not math.isfinite(audit_embargo)
        or audit_embargo != configured_embargo
        or set(boundaries)
        != {"train_to_validation", "validation_to_holdout"}
    ):
        raise ValueError("invalid temporal purge audit")

    for earlier, later in (("train", "validation"), ("validation", "holdout")):
        boundary = _mapping(boundaries.get(f"{earlier}_to_{later}"))
        next_observation = _parse_utc(
            boundary.get("next_split_first_observation_utc")
        )
        cutoff = _parse_utc(boundary.get("information_cutoff_utc"))
        try:
            nominal = int(boundary.get("nominal_fixture_count"))
            retained = int(boundary.get("retained_fixture_count"))
            purged = int(boundary.get("purged_fixture_count"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("invalid temporal purge boundary counters") from exc
        reasons = boundary.get("purge_reasons")
        digest = str(boundary.get("purged_fixture_ids_sha256") or "")
        valid_digest = len(digest) == 64 and all(
            character in "0123456789abcdef" for character in digest.lower()
        )
        valid_reasons = (
            isinstance(reasons, Mapping)
            and all(
                isinstance(key, str)
                and type(value) is int
                and value >= 0
                for key, value in reasons.items()
            )
        )
        if (
            abs((next_observation - cutoff).total_seconds() - audit_embargo)
            > 1e-6
            or next_observation > split_times[later][0]
            or split_times[earlier][1] >= cutoff
            or retained != len(split_ids[earlier])
            or nominal != retained + purged
            or nominal < 0
            or purged < 0
            or not valid_digest
            or not valid_reasons
            or sum(reasons.values()) != purged
        ):
            raise ValueError("invalid temporal purge boundary audit")


def validate_discovery_report(report: Mapping[str, Any]) -> None:
    if not isinstance(report, Mapping):
        raise TypeError("discovery report must be a mapping")
    manifests = report.get("manifests")
    results = report.get("results")
    if not isinstance(manifests, Mapping) or not isinstance(results, Mapping):
        raise ValueError("discovery report requires manifests and results")
    if manifests.get("artifact_type") != "wide_rule_candidate_manifests":
        raise ValueError("unexpected manifests artifact type")
    if results.get("artifact_type") != "wide_rule_candidate_results":
        raise ValueError("unexpected results artifact type")
    # CLI source diagnostics are intentionally appended after the immutable
    # statistical artifact is hashed; filenames must not alter rule identity.
    _verify_hash(
        manifests,
        "artifact_sha256",
        excluded_fields=("sources",),
    )
    _verify_hash(results, "artifact_sha256")
    # Older result artifacts did not repeat every engine/schema field.  When
    # present they still have to agree, while the data partition identity is
    # mandatory on both halves of every supported artifact.
    for field in (
        "engine_version",
        "feature_schema_version",
        "threshold_grid_version",
    ):
        manifest_value = manifests.get(field)
        result_value = results.get(field)
        if (
            manifest_value is not None
            and result_value is not None
            and manifest_value != result_value
        ):
            raise ValueError(f"discovery {field} mismatch")
    for field in ("dataset_as_of_utc", "split_policy", "splits"):
        if manifests.get(field) != results.get(field):
            raise ValueError(f"discovery {field} mismatch")
    manifest_feature_schema = manifests.get("feature_schema_version")
    if manifest_feature_schema is not None:
        if (
            type(manifest_feature_schema) is not int
            or manifest_feature_schema != FEATURE_SCHEMA_VERSION
        ):
            raise ValueError("unsupported discovery feature schema version")
    _validate_temporal_partition_contract(manifests)
    candidates = manifests.get("candidates")
    if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
        raise ValueError("candidate manifests must be a sequence")
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            raise ValueError("candidate manifest must be a mapping")
        _verify_hash(candidate, "manifest_sha256")
        for field in (
            "engine_version",
            "feature_schema_version",
            "threshold_grid_version",
        ):
            parent_value = manifests.get(field)
            candidate_value = candidate.get(field)
            if (
                parent_value is not None
                and candidate_value != parent_value
            ):
                raise ValueError(
                    f"candidate {field} does not match discovery artifact"
                )

    candidate_ids = {
        str(candidate.get("candidate_id") or "")
        for candidate in candidates
        if isinstance(candidate, Mapping)
    }
    for portfolio_name in (
        "selected_portfolio",
        "best_available_portfolio",
    ):
        portfolio = manifests.get(portfolio_name)
        if portfolio is None:
            continue
        if not isinstance(portfolio, Mapping):
            raise ValueError(f"{portfolio_name} must be a mapping or null")
        members = portfolio.get("member_candidate_ids", [])
        if not isinstance(members, Sequence) or isinstance(
            members, (str, bytes)
        ):
            raise ValueError(f"{portfolio_name} members must be a sequence")
        if not {str(value) for value in members}.issubset(candidate_ids):
            raise ValueError(
                f"{portfolio_name} contains an unknown candidate"
            )


def _search_generation(manifests: Mapping[str, Any]) -> dict[str, Any]:
    """Canonical, outcome-independent identity of one search contract."""

    feature_schema = manifests.get("feature_schema_version")
    if feature_schema is not None:
        feature_schema = int(feature_schema)
    candidate_contracts = []
    for candidate in manifests.get("candidates", []):
        if not isinstance(candidate, Mapping):
            continue
        universe = _mapping(candidate.get("universe"))
        stages = universe.get("stages", ())
        if isinstance(stages, (str, bytes)):
            stages = ()
        candidate_contracts.append(
            {
                "universe": {
                    "stages": sorted(str(value) for value in stages),
                    "minute_min": universe.get("minute_min"),
                    "minute_max": universe.get("minute_max"),
                    "current_filter_policy": universe.get(
                        "current_filter_policy", "ignored"
                    ),
                },
                "trigger_policy": candidate.get("trigger_policy"),
            }
        )
    unique_contracts = {
        canonical_hash(value): value for value in candidate_contracts
    }
    payload = {
        "engine_version": str(manifests.get("engine_version") or "legacy"),
        "feature_schema_version": feature_schema,
        "threshold_grid_version": str(
            manifests.get("threshold_grid_version") or "legacy"
        ),
        "execution_contracts": [
            unique_contracts[key] for key in sorted(unique_contracts)
        ],
    }
    return {**payload, "generation_id": canonical_hash(payload)[:20]}


def _purge_only_generation_transition(
    previous: Mapping[str, Any], current: Mapping[str, Any],
) -> bool:
    """Historical split hygiene does not invalidate frozen live execution."""
    previous_engine = str(previous.get("engine_version") or "")
    return bool(
        previous_engine
        and not previous_engine.endswith(TEMPORAL_PURGED_ENGINE_SUFFIX)
        and current.get("engine_version")
        == previous_engine + TEMPORAL_PURGED_ENGINE_SUFFIX
        and all(
            previous.get(field) == current.get(field)
            for field in (
                "feature_schema_version", "threshold_grid_version", "execution_contracts"
            )
        )
    )


def _canonical_rule(candidate: Mapping[str, Any]) -> RuleManifest:
    universe = _mapping(candidate.get("universe"))
    stages = universe.get("stages", ("decision_pipeline", "wide_monitor"))
    if isinstance(stages, (str, bytes)):
        raise ValueError("candidate stages must be a sequence")
    clauses = candidate.get("clauses")
    if not isinstance(clauses, Sequence) or isinstance(clauses, (str, bytes)):
        raise ValueError("candidate clauses must be a sequence")
    feature_schema = candidate.get("feature_schema_version")
    if feature_schema is not None:
        if (
            type(feature_schema) is not int
            or feature_schema < 1
            or feature_schema > FEATURE_SCHEMA_VERSION
        ):
            raise ValueError("unsupported candidate feature schema version")
    parsed_clauses = tuple(
        Clause(
            feature=clause.get("feature"),
            op=clause.get("operator"),
            value=clause.get("threshold"),
        )
        for clause in clauses
        if isinstance(clause, Mapping)
    )
    schema_pinned_features = {
        "composite.base_quality_v1",
        "rolling.both_windows_available_v1",
    }
    if any(
        clause.feature in schema_pinned_features for clause in parsed_clauses
    ) and feature_schema != FEATURE_SCHEMA_VERSION:
        raise ValueError(
            "composite candidate requires the current feature schema"
        )
    return RuleManifest(
        rule_id=str(candidate.get("candidate_id") or ""),
        version=(
            "wide_prospective_rule_v2"
            if feature_schema is not None
            else "wide_prospective_rule_v1"
        ),
        clauses=parsed_clauses,
        universe=UniverseSpec(
            stages=tuple(stages),
            min_minute=int(universe.get("minute_min", 46)),
            max_minute=int(universe.get("minute_max", 60)),
            current_filter_policy=str(
                universe.get("current_filter_policy", "ignored")
            ),
        ),
        schema_version=2 if feature_schema is not None else 1,
        feature_schema_version=feature_schema,
    )


class WideResearchController:
    """Import frozen discoveries and advance only prospective evidence."""

    def __init__(
        self,
        store: WideResearchStore,
        *,
        active_manifest_path: str,
        prospective_start_utc: str,
        production_enabled: bool = False,
        max_shadow_rules: int = 10,
        policy: LifecyclePolicy = LifecyclePolicy(),
        pointer_name: str = "production",
        terminal_review_enabled: bool = False,
        terminal_review_min_hit_rate: float = 0.90,
    ) -> None:
        self.store = store
        self.active_manifest_path = str(active_manifest_path)
        self.prospective_start_utc = _utc_iso(_parse_utc(prospective_start_utc))
        self.production_enabled = bool(production_enabled)
        self.max_shadow_rules = max(1, min(64, int(max_shadow_rules)))
        self.policy = policy
        self.pointer_name = str(pointer_name or "production")
        self.terminal_review_enabled = bool(terminal_review_enabled)
        if self.terminal_review_enabled and self.production_enabled:
            raise ValueError("terminal review is hard-shadow and cannot enable production")
        self.terminal_review_min_hit_rate = float(
            terminal_review_min_hit_rate
        )
        if (
            not math.isfinite(self.terminal_review_min_hit_rate)
            or not 0.0 <= self.terminal_review_min_hit_rate <= 1.0
        ):
            raise ValueError(
                "terminal_review_min_hit_rate must be finite and in [0, 1]"
            )

    def _terminal_review_manifest(self) -> dict[str, Any]:
        return {
            "version": TERMINAL_REVIEW_POLICY_VERSION,
            "enabled": self.terminal_review_enabled,
            "final_look": max(self.policy.allowed_looks),
            "min_point_hit_rate": self.terminal_review_min_hit_rate,
        }

    def _statistical_family_descriptor(
        self, phase: Mapping[str, Any]
    ) -> dict[str, Any]:
        policy = _mapping(phase.get("policy"))
        lifecycle = _phase_lifecycle_policy(phase)
        family = _mapping(policy.get("statistical_family"))
        run_id = str(
            family.get("run_id") or phase.get("run_id") or ""
        ).strip()
        try:
            family_size = int(family.get("family_size"))
            run_sequence = int(family.get("run_sequence"))
            alpha_budget = float(family.get("alpha_budget"))
        except (TypeError, ValueError):
            family_size = 0
            run_sequence = 0
            alpha_budget = 0.0
        registered = bool(
            run_id
            and family_size >= 1
            and run_sequence >= 1
            and math.isfinite(alpha_budget)
            and lifecycle is not None
            and 0.0 < alpha_budget <= lifecycle.family_alpha
            and family.get("inter_run_correction")
            in {
                "geometric_alpha_spending_v1",
                "telescoping_alpha_spending_v1",
            }
            and family.get("intra_run_correction") == "holm_step_down_v1"
            and family.get("milestone_correction")
            == "bonferroni_fixed_looks_v1"
        )
        return {
            "key": f"run:{run_id}" if registered else f"legacy:{run_id or phase.get('phase_id')}",
            "run_id": run_id or None,
            "family_size": family_size if registered else 1,
            "run_sequence": run_sequence if registered else None,
            "alpha_budget": (
                alpha_budget
                if registered
                else (
                    lifecycle.family_alpha / 2.0
                    if lifecycle is not None
                    else 0.0
                )
            ),
            "registered": registered,
            "lifecycle_policy": lifecycle,
            "lifecycle_manifest": (
                lifecycle.manifest() if lifecycle is not None else None
            ),
        }

    def import_report(
        self,
        report: Mapping[str, Any],
        *,
        imported_at_utc: Optional[str] = None,
    ) -> dict[str, Any]:
        validate_discovery_report(report)
        now = (
            _parse_utc(imported_at_utc)
            if imported_at_utc is not None
            else datetime.now(timezone.utc)
        )
        configured = _parse_utc(self.prospective_start_utc)
        starts = max(now, configured)
        manifests = _mapping(report.get("manifests"))
        if (_mapping(manifests.get("config")).get("error_refinement")
            or any(candidate.get("refinement_lineage") for candidate in manifests.get("candidates", []))
            or any(c.get("feature") in EXTENDED_FEATURES
               for candidate in manifests.get("candidates", [])
               for c in candidate.get("clauses", []))):
            if (self.store.profile_id != "rare_precision_shadow" or self.production_enabled
                or not self.terminal_review_enabled):
                raise ValueError("extended rules require isolated terminal shadow testing")
        results_artifact = _mapping(report.get("results"))
        search_generation = _search_generation(manifests)
        run_id = str(report.get("run_id") or "").strip()
        if not run_id:
            raise ValueError("discovery run_id is required")
        run_config = {
            "manifest_artifact_sha256": manifests.get("artifact_sha256"),
            "results_artifact_sha256": results_artifact.get("artifact_sha256"),
            "dataset_as_of_utc": manifests.get("dataset_as_of_utc"),
            "prospective_evidence_only": True,
            "lifecycle_policy": self.policy.manifest(),
            "selected_portfolio": manifests.get("selected_portfolio"),
            "best_available_portfolio": manifests.get(
                "best_available_portfolio"
            ),
            "search_generation": search_generation,
            "admission_policy": "generation_and_validation_rank_v1",
            "terminal_review": self._terminal_review_manifest(),
        }
        results_by_id = {
            str(row.get("candidate_id") or ""): row
            for row in results_artifact.get("results", [])
            if isinstance(row, Mapping)
        }
        selected_portfolio = _mapping(manifests.get("selected_portfolio"))
        selected_portfolio_members = {
            str(value)
            for value in selected_portfolio.get(
                "member_candidate_ids", []
            )
        }
        best_available_portfolio = _mapping(
            manifests.get("best_available_portfolio")
        )
        best_available_portfolio_members = {
            str(value)
            for value in best_available_portfolio.get(
                "member_candidate_ids", []
            )
        }
        portfolio_members_to_protect = (
            selected_portfolio_members
            if selected_portfolio_members
            else best_available_portfolio_members
        )
        imported: list[dict[str, Any]] = []
        reused: list[dict[str, Any]] = []
        legacy_replacements: list[dict[str, Any]] = []
        candidates = sorted(
            (
                candidate
                for candidate in manifests.get("candidates", [])
                if isinstance(candidate, Mapping)
            ),
            key=lambda candidate: (
                int(candidate.get("rank_at_discovery") or 10**9),
                str(candidate.get("candidate_id") or ""),
            ),
        )[: self.max_shadow_rules]
        considered_candidate_ids = {
            str(candidate.get("candidate_id") or "")
            for candidate in candidates
        }
        if not portfolio_members_to_protect.issubset(
            considered_candidate_ids
        ):
            raise RuntimeError(
                "the validation-selected portfolio is larger than the "
                "configured prospective shadow pool"
            )
        # Paused terminal reviews remain immutable identities and must not be
        # silently restarted when the same rule is rediscovered.  They do not
        # consume an executable shadow slot.
        identity_snapshot = self.store.list_phases(
            statuses=IMPORT_IDENTITY_STATUSES
        )
        pool_snapshot = [
            row
            for row in identity_snapshot
            if str(row.get("status")) in EVALUATED_STATUSES
        ]
        phases_by_rule: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for phase in identity_snapshot:
            phases_by_rule[str(phase.get("rule_id") or "")].append(phase)
        terminal_executions = {
            _execution_fingerprint(_mapping(row.get("manifest"))): row
            for row in identity_snapshot
            if _terminal_phase(row)
            and _phase_runtime_compatible(row)
            and self._statistical_family_descriptor(row)["registered"]
        }
        purge_preserved_executions = {
            _execution_fingerprint(_mapping(row.get("manifest"))): row
            for row in identity_snapshot
            if _phase_runtime_compatible(row)
            and self._statistical_family_descriptor(row)["registered"]
            and _purge_only_generation_transition(
                _mapping(_mapping(_mapping(row.get("policy")).get("discovery")).get("search_generation")),
                search_generation,
            )
        }
        prepared: list[tuple[Mapping[str, Any], RuleManifest]] = []
        for candidate in candidates:
            rule = _canonical_rule(candidate)
            if len(rule.clauses) != len(candidate.get("clauses", [])):
                raise ValueError("candidate contains an invalid clause")
            existing = phases_by_rule.get(rule.rule_id, [])
            registered_existing = [
                row
                for row in existing
                if self._statistical_family_descriptor(row)["registered"]
            ]
            if registered_existing:
                if any(
                    _mapping(row.get("manifest")) != rule.as_dict()
                    for row in registered_existing
                ):
                    raise ValueError(
                        "candidate rule_id collides with another executable "
                        "rule manifest"
                    )
                if self.terminal_review_enabled and any(
                    _phase_terminal_review_policy(row) != self._terminal_review_manifest()
                    for row in registered_existing
                ):
                    raise ValueError("existing rule has a different frozen terminal review policy")
                reused.append(
                    {
                        "rule_id": rule.rule_id,
                        "phase_id": registered_existing[0]["phase_id"],
                    }
                )
                continue
            execution_alias = terminal_executions.get(
                _execution_fingerprint(rule.as_dict())
            )
            if self.terminal_review_enabled and execution_alias is not None:
                if _phase_terminal_review_policy(execution_alias) != self._terminal_review_manifest():
                    raise ValueError("existing execution has a different frozen terminal review policy")
                reused.append({
                    "rule_id": execution_alias["rule_id"],
                    "candidate_id": rule.rule_id,
                    "phase_id": execution_alias["phase_id"],
                    "reason": "same_frozen_execution",
                })
                continue
            purge_alias = purge_preserved_executions.get(
                _execution_fingerprint(rule.as_dict())
            )
            if purge_alias is not None:
                if (self.terminal_review_enabled or _terminal_phase(purge_alias)) and (
                    _phase_terminal_review_policy(purge_alias) != self._terminal_review_manifest()
                ):
                    raise ValueError("existing execution has a different frozen terminal review policy")
                reused.append({
                    "rule_id": purge_alias["rule_id"],
                    "candidate_id": rule.rule_id,
                    "phase_id": purge_alias["phase_id"],
                    "reason": "same_execution_purge_policy_transition",
                })
                continue
            for legacy in existing:
                if str(legacy.get("status")) in {"candidate", "shadow", "ready"}:
                    legacy_replacements.append(dict(legacy))
            prepared.append((candidate, rule))

        protected_phases = [
            row
            for row in pool_snapshot
            if str(row.get("status")) in {"ready", "active"}
            if _phase_runtime_compatible(row)
        ]
        shadow_phases = [
            row
            for row in pool_snapshot
            if str(row.get("status")) in {"candidate", "shadow"}
        ]
        legacy_replacement_ids = {
            str(row.get("phase_id") or "") for row in legacy_replacements
        }
        same_generation: list[dict[str, Any]] = []
        incompatible_generation: list[dict[str, Any]] = []
        purge_preserved_phase_ids: list[str] = []
        for phase in shadow_phases:
            if str(phase.get("phase_id") or "") in legacy_replacement_ids:
                continue
            discovery = _mapping(_mapping(phase.get("policy")).get("discovery"))
            phase_generation = _mapping(discovery.get("search_generation"))
            purge_compatible = (
                _phase_runtime_compatible(phase)
                and _purge_only_generation_transition(phase_generation, search_generation)
            )
            if purge_compatible:
                purge_preserved_phase_ids.append(str(phase["phase_id"]))
            if (
                phase_generation.get("generation_id")
                == search_generation["generation_id"]
                or not candidates
                or (_terminal_phase(phase) and _phase_runtime_compatible(phase))
                or purge_compatible
            ):
                # An empty shortlist contains no execution contracts from
                # which to infer a replacement generation.  Keep ongoing
                # prospective tests; the runtime compatibility check handles
                # actual schema invalidation independently.
                same_generation.append(dict(phase))
            else:
                incompatible_generation.append(dict(phase))

        # A purge-only engine revision changes candidate IDs while leaving the
        # executable rule contract untouched.  Preserve those already-running
        # prospective clocks, but also give the newly leakage-safe generation
        # one bounded cohort of capacity.  The reserve shrinks naturally as
        # legacy phases become terminal; at most one normal pool is added.
        purge_transition_phases = [
            row
            for row in pool_snapshot
            if _phase_runtime_compatible(row)
            and _purge_only_generation_transition(
                _mapping(
                    _mapping(
                        _mapping(row.get("policy")).get("discovery")
                    ).get("search_generation")
                ),
                search_generation,
            )
        ]
        migration_reserve = min(
            self.max_shadow_rules,
            len(purge_transition_phases),
            max(0, MAX_EVALUATED_PHASES - self.max_shadow_rules),
        )
        effective_pool_limit = self.max_shadow_rules + migration_reserve
        slots = effective_pool_limit - len(protected_phases)
        if slots < len(same_generation):
            raise RuntimeError(
                "existing same-generation shadow phases exceed the configured "
                "pool plus purge-transition reserve; refusing "
                "outcome-adaptive eviction"
            )
        available_new_slots = max(0, slots - len(same_generation))
        required = [
            row
            for row in prepared
            if row[1].rule_id in portfolio_members_to_protect
        ]
        purge_portfolio_deferred = bool(
            len(required) > available_new_slots and purge_preserved_phase_ids
        )
        if len(required) > available_new_slots and not purge_portfolio_deferred:
            raise RuntimeError(
                "the validation-selected portfolio does not fit atomically "
                "inside the prospective shadow pool"
            )
        required_ids = {rule.rule_id for _candidate, rule in required}
        optional = [
            row for row in prepared if row[1].rule_id not in required_ids
        ]
        # A validation-selected portfolio is one atomic hypothesis.  If it
        # cannot fit during the bounded migration overlap, importing lower
        # ranked optional rules while skipping the portfolio would invert the
        # frozen validation ranking.  Defer the whole new cohort and retry on
        # a later discovery after capacity becomes available.
        admitted = (
            []
            if purge_portfolio_deferred
            else required + optional[: available_new_slots - len(required)]
        )
        admitted_ids = {rule.rule_id for _candidate, rule in admitted}
        not_admitted = [
            {
                "rule_id": rule.rule_id,
                "rank": candidate.get("rank_at_discovery"),
                "reason": (
                    "purge_transition_portfolio_capacity"
                    if purge_portfolio_deferred
                    else "same_generation_pool_capacity"
                ),
            }
            for candidate, rule in prepared
            if rule.rule_id not in admitted_ids
        ]

        phase_specs: list[dict[str, Any]] = []
        for candidate, rule in admitted:
            phase_key = canonical_hash(
                {"run_id": run_id, "rule_id": rule.rule_id}
            )[:16]
            phase_id = f"{rule.rule_id}:run:{phase_key}"
            result = _mapping(results_by_id.get(rule.rule_id))
            phase_specs.append(
                {
                    "rule_id": rule.rule_id,
                    "manifest": rule.as_dict(),
                    "phase_id": phase_id,
                    "starts_at_utc": _utc_iso(starts),
                    "policy": {
                        "prospective_evidence_only": True,
                        "historical_metrics_are_evidence": False,
                        "lifecycle": self.policy.manifest(),
                        "terminal_review": self._terminal_review_manifest(),
                        "discovery": {
                            "run_id": run_id,
                            "rank": candidate.get("rank_at_discovery"),
                            "candidate_manifest_sha256": candidate.get(
                                "manifest_sha256"
                            ),
                            "historical_holdout": result.get("holdout"),
                            **({"research_scope": candidate["research_scope"]}
                               if candidate.get("research_scope") else {}),
                            **({"refinement_lineage": candidate["refinement_lineage"]}
                               if candidate.get("refinement_lineage") else {}),
                            "historical_statistical_test": result.get(
                                "statistical_test"
                            ),
                            "search_generation": search_generation,
                            "selected_portfolio_id": selected_portfolio.get(
                                "portfolio_id"
                            ),
                            "selected_portfolio_member": (
                                rule.rule_id in selected_portfolio_members
                            ),
                            "best_available_portfolio_id": (
                                best_available_portfolio.get("portfolio_id")
                            ),
                            "best_available_portfolio_member": (
                                rule.rule_id
                                in best_available_portfolio_members
                            ),
                        },
                    },
                }
            )

        incompatible_ids = {
            str(row.get("phase_id") or "")
            for row in incompatible_generation
        }
        retirement_rows = {
            str(row.get("phase_id") or ""): row
            for row in (*incompatible_generation, *legacy_replacements)
            if str(row.get("phase_id") or "")
        }
        retirement_specs: list[dict[str, Any]] = []
        for phase_id, row in retirement_rows.items():
            status = str(row.get("status") or "")
            if status not in {"candidate", "shadow", "ready"}:
                continue
            retirement_specs.append(
                {
                    "phase_id": phase_id,
                    "expected_status": status,
                    "reason": (
                        "search_generation_replaced"
                        if phase_id in incompatible_ids
                        else "legacy_family_without_alpha_allocation"
                    ),
                    "metadata": {
                        "replacement_run_id": run_id,
                        "replacement_search_generation": search_generation,
                        "outcome_independent": True,
                    },
                }
            )

        alpha_allocation: Optional[Mapping[str, Any]] = None
        retired: list[str] = []
        if phase_specs or retirement_specs:
            run_config.update(
                {
                    "admitted_candidate_ids": [
                        value["rule_id"] for value in phase_specs
                    ],
                    "planned_phase_ids": [
                        value["phase_id"] for value in phase_specs
                    ],
                    "family_size": len(phase_specs),
                    "planned_retirements": [
                        value["phase_id"] for value in retirement_specs
                    ],
                    "atomic_import": "sqlite_single_transaction_v1",
                }
            )
            committed = self.store.commit_discovery_import(
                run_id=run_id,
                run_config=run_config,
                global_alpha=self.policy.family_alpha,
                phases=phase_specs,
                retirements=retirement_specs,
                expected_phase_snapshot={
                    str(row["phase_id"]): str(row["status"])
                    for row in pool_snapshot
                },
                completed_at_utc=_utc_iso(now),
            )
            alpha_allocation = _mapping(committed.get("alpha_allocation"))
            retired = [str(value) for value in committed.get("retired", [])]
            committed_phases = {
                str(row.get("phase_id")): row
                for row in committed.get("phases", [])
                if isinstance(row, Mapping)
            }
            if committed.get("already_completed"):
                reused.extend(
                    {
                        "rule_id": str(value["rule_id"]),
                        "phase_id": str(value["phase_id"]),
                    }
                    for value in phase_specs
                )
            else:
                imported.extend(
                    {
                        "rule_id": str(value["rule_id"]),
                        "phase_id": str(value["phase_id"]),
                        "starts_at_utc": str(
                            committed_phases.get(
                                str(value["phase_id"]), value
                            ).get("starts_at_utc")
                        ),
                    }
                    for value in phase_specs
                )
        return {
            "run_id": run_id,
            "imported": imported,
            "reused": reused,
            "retired": retired,
            "not_admitted": not_admitted,
            "empty_report_preserved_pool": not candidates,
            "purge_transition_preserved_phase_ids": purge_preserved_phase_ids,
            "purge_transition_portfolio_deferred": purge_portfolio_deferred,
            "search_generation": search_generation,
            "pool": {
                "limit": self.max_shadow_rules,
                "effective_limit": effective_pool_limit,
                "purge_transition_reserve": migration_reserve,
                "protected_ready_or_active": len(protected_phases),
                "same_generation_existing": len(same_generation),
                "new_admitted": len(admitted),
                "incompatible_retired": len(retired),
                "outcome_adaptive_eviction": False,
            },
            "prospective_start_utc": _utc_iso(starts),
            "alpha_allocation": {
                "sequence": (
                    int(alpha_allocation["sequence"])
                    if alpha_allocation
                    else None
                ),
                "alpha_budget": (
                    float(alpha_allocation["alpha_budget"])
                    if alpha_allocation
                    else None
                ),
                "family_size": len(admitted),
            },
        }

    @staticmethod
    def _discovery_rank_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
        policy = _mapping(row.get("policy"))
        discovery = _mapping(policy.get("discovery"))
        return (
            int(discovery.get("rank") or 10**9),
            str(row.get("created_at_utc") or ""),
            str(row.get("phase_id") or ""),
        )

    def _prospective_strength_key(self, row: Mapping[str, Any]) -> tuple[Any, ...]:
        metrics = self.store.metrics_for_phase(str(row["phase_id"]))
        resolved = int(metrics.get("resolved") or 0)
        wins = int(metrics.get("win") or 0)
        lower, _upper = wilson_interval(wins, resolved)
        return (
            -resolved,
            -lower,
            self._discovery_rank_key(row),
        )

    @staticmethod
    def _prospective_metrics(
        triggers: Sequence[Mapping[str, Any]],
        *,
        limit: Optional[int] = None,
        invalid_as_loss: bool = False,
    ) -> dict[str, Any]:
        counted_statuses = {"win", "loss"}
        if invalid_as_loss:
            counted_statuses.add("invalid")
        resolved = [
            row
            for row in triggers
            if str(row.get("latest_outcome_status") or "") in counted_statuses
        ]
        resolved.sort(
            key=lambda row: (
                str(row.get("triggered_at_utc") or ""),
                str(row.get("fixture_id") or ""),
            )
        )
        if limit is not None:
            resolved = resolved[: int(limit)]
        wins = sum(
            str(row.get("latest_outcome_status") or "") == "win"
            for row in resolved
        )
        losses = len(resolved) - wins
        parsed_times = [
            _parse_utc(row.get("triggered_at_utc")) for row in resolved
        ]
        span_days = (
            max(0.0, (parsed_times[-1] - parsed_times[0]).total_seconds() / 86400.0)
            if len(parsed_times) >= 2
            else 0.0
        )
        days = {value.date().isoformat() for value in parsed_times}
        leagues = Counter(str(row.get("league") or "<unknown>") for row in resolved)
        weekly_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row, value in zip(resolved, parsed_times):
            iso = value.isocalendar()
            weekly_groups[f"{iso.year}-W{iso.week:02d}"].append(row)
        weekly = []
        for period in sorted(weekly_groups):
            rows = weekly_groups[period]
            period_wins = sum(
                str(row.get("latest_outcome_status") or "") == "win"
                for row in rows
            )
            weekly.append(
                {
                    "period": period,
                    "wins": period_wins,
                    "losses": len(rows) - period_wins,
                }
            )
        return {
            "wins": wins,
            "losses": losses,
            "invalid_counted_as_losses": sum(
                str(row.get("latest_outcome_status") or "") == "invalid"
                for row in resolved
            ),
            "span_days": span_days,
            "trigger_days": len(days),
            "leagues": len(leagues),
            "max_league_share": (
                max(leagues.values(), default=0) / len(resolved)
                if resolved
                else 1.0
            ),
            "triggers_per_week": (
                len(resolved) / max(1.0, span_days / 7.0)
                if resolved
                else 0.0
            ),
            "weekly": weekly,
        }

    def _readiness_evidence(
        self,
        phase: Mapping[str, Any],
        policy: LifecyclePolicy,
    ) -> tuple[dict[str, Any], Optional[int]]:
        phase_id = str(phase["phase_id"])
        triggers = self.store.list_triggers(phase_id=phase_id)
        latest_closed: Optional[tuple[int, list[dict[str, Any]]]] = None
        for look in policy.allowed_looks:
            if len(triggers) < int(look):
                break
            frozen = self.store.freeze_phase_look(phase_id, int(look))
            if frozen is None:
                break
            rows = self.store.triggers_for_phase_look(phase_id, int(look))
            if any(
                str(row.get("latest_outcome_status") or "pending") == "pending"
                for row in rows
            ):
                # Freeze every reached identity cohort even while earlier
                # outcomes are pending.  None can become a closed look yet.
                continue
            latest_closed = (int(look), rows)
        if latest_closed is None:
            return self._prospective_metrics(
                triggers,
                invalid_as_loss=True,
            ), None
        milestone, rows = latest_closed
        return self._prospective_metrics(
            rows,
            invalid_as_loss=True,
        ), milestone

    def _publish_fallback_before_pointer_clear(
        self,
        pointer: Mapping[str, Any],
        *,
        now: datetime,
        previous_rule_id: Optional[str],
    ) -> dict[str, Any]:
        """Publish the safe fallback before a destructive pointer CAS.

        If the process dies between the file replacement and SQLite update,
        Telegram merely falls back early.  The inverse ordering could leave a
        degraded rule live on disk until the next successful reconcile.
        """

        payload = build_active_manifest(
            generation=int(pointer.get("generation") or 0) + 1,
            rule=None,
            effective_from_utc=_utc_iso(now),
            previous_rule_id=previous_rule_id,
            production_enabled=self.production_enabled,
            fallback="current_filter",
        )
        write_active_manifest_atomic(Path(self.active_manifest_path), payload)
        return payload

    def reconcile(self, *, now_utc: Optional[str] = None) -> dict[str, Any]:
        now = _parse_utc(now_utc) if now_utc else datetime.now(timezone.utc)
        pointer = self.store.get_active_pointer(self.pointer_name)
        previous_rule_id = str(pointer.get("rule_id") or "") if pointer else None

        # Repair only toward the safe side.  A pointer may publish only an
        # ACTIVE phase; an orphan ACTIVE phase is returned to shadow and must
        # pass the frozen readiness certificate again before promotion.
        phase_rows = self.store.list_phases()
        by_phase = {str(row["phase_id"]): row for row in phase_rows}
        pointed_phase_id = str(pointer.get("phase_id") or "") if pointer else ""
        pointed_phase = by_phase.get(pointed_phase_id) if pointed_phase_id else None
        if (
            pointer
            and pointed_phase is not None
            and str(pointed_phase.get("status")) == "active"
            and (
                not _phase_runtime_compatible(pointed_phase)
                or _terminal_phase(pointed_phase)
            )
        ):
            self._publish_fallback_before_pointer_clear(
                pointer,
                now=now,
                previous_rule_id=previous_rule_id,
            )
            transaction = self.store.transition_phase_and_set_pointer(
                pointed_phase_id,
                "degraded",
                expected_status="active",
                pointer_name=self.pointer_name,
                pointer_phase_id=None,
                expected_generation=int(pointer["generation"]),
                reason="phase_runtime_contract_incompatible",
                metadata={
                    "reason": "phase_runtime_contract_incompatible",
                    "previous_rule_id": previous_rule_id,
                    "expected_feature_schema_version": FEATURE_SCHEMA_VERSION,
                },
                changed_at_utc=_utc_iso(now),
            )
            pointer = transaction["pointer"]
            pointed_phase_id = ""
            pointed_phase = None
        if pointer and pointed_phase_id and (
            pointed_phase is None or str(pointed_phase.get("status")) != "active"
        ):
            self._publish_fallback_before_pointer_clear(
                pointer,
                now=now,
                previous_rule_id=previous_rule_id,
            )
            pointer = self.store.set_active_pointer(
                self.pointer_name,
                phase_id=None,
                expected_generation=int(pointer["generation"]),
                metadata={
                    "reason": "stale_or_non_active_pointer",
                    "previous_rule_id": previous_rule_id,
                },
                updated_at_utc=_utc_iso(now),
            )
            pointed_phase_id = ""
        phase_rows = self.store.list_phases()
        for phase in phase_rows:
            phase_id = str(phase["phase_id"])
            status = str(phase.get("status"))
            if not _phase_runtime_compatible(phase):
                if status in {"candidate", "shadow", "ready"}:
                    self.store.transition_phase(
                        phase_id,
                        "retired",
                        expected_status=status,
                        reason="phase_runtime_contract_incompatible",
                        metadata={
                            "expected_feature_schema_version": (
                                FEATURE_SCHEMA_VERSION
                            )
                        },
                        changed_at_utc=_utc_iso(now),
                    )
                elif status == "active" and phase_id != pointed_phase_id:
                    self.store.transition_phase(
                        phase_id,
                        "degraded",
                        expected_status="active",
                        reason="phase_runtime_contract_incompatible",
                        metadata={
                            "expected_feature_schema_version": (
                                FEATURE_SCHEMA_VERSION
                            )
                        },
                        changed_at_utc=_utc_iso(now),
                    )
                continue
            if str(phase.get("status")) != "active" or phase_id == pointed_phase_id:
                continue
            self.store.transition_phase(
                phase_id,
                "shadow",
                expected_status="active",
                reason="orphan_active_recovery",
                metadata={"prospective_revalidation_required": True},
                changed_at_utc=_utc_iso(now),
            )

        phase_rows = self.store.list_phases()
        review_phases = []
        for row in phase_rows:
            status = str(row.get("status"))
            terminal_review = _phase_terminal_review_policy(row)
            if status not in {"candidate", "shadow", "ready"} and not (
                status == "paused"
                and terminal_review is not None
                and terminal_review["enabled"]
            ):
                continue
            if _phase_runtime_compatible(row):
                review_phases.append(row)
        review_phases.sort(key=self._discovery_rank_key)
        compatible_phase_rows = [
            row for row in phase_rows if _phase_runtime_compatible(row)
        ]
        descriptors = {
            str(row["phase_id"]): self._statistical_family_descriptor(row)
            for row in compatible_phase_rows
        }
        families: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for phase in compatible_phase_rows:
            descriptor = descriptors[str(phase["phase_id"])]
            families[str(descriptor["key"])].append(phase)

        evidence: dict[str, tuple[dict[str, Any], Optional[int], float]] = {}
        holm: dict[str, dict[str, Any]] = {}
        for family_key, members in families.items():
            family_descriptors = [
                descriptors[str(member["phase_id"])] for member in members
            ]
            registered = all(
                bool(descriptor["registered"])
                for descriptor in family_descriptors
            )
            family_size = max(
                len(members),
                *(int(descriptor["family_size"]) for descriptor in family_descriptors),
            )
            alpha_budget = min(
                float(descriptor["alpha_budget"])
                for descriptor in family_descriptors
            )
            consistent = len(
                {
                    (
                        descriptor.get("run_id"),
                        descriptor.get("family_size"),
                        descriptor.get("run_sequence"),
                        descriptor.get("alpha_budget"),
                        descriptor.get("registered"),
                        canonical_hash(
                            descriptor.get("lifecycle_manifest") or {}
                        ),
                    )
                    for descriptor in family_descriptors
                }
            ) == 1
            ordered_hypotheses: list[tuple[str, float]] = []
            for phase in members:
                phase_id = str(phase["phase_id"])
                descriptor = descriptors[phase_id]
                phase_policy = descriptor.get("lifecycle_policy")
                assert isinstance(phase_policy, LifecyclePolicy)
                metrics, milestone = self._readiness_evidence(
                    phase, phase_policy
                )
                wins = int(metrics.get("wins") or 0)
                losses = int(metrics.get("losses") or 0)
                total = wins + losses
                p_value = (
                    binomial_upper_tail(wins, total, phase_policy.null_hit_rate)
                    if total
                    else 1.0
                )
                evidence[phase_id] = (metrics, milestone, p_value)
                ordered_hypotheses.append((phase_id, p_value))
            ordered_hypotheses.sort(key=lambda item: (item[1], item[0]))
            look_counts = {
                len(descriptor["lifecycle_policy"].allowed_looks)
                for descriptor in family_descriptors
                if isinstance(
                    descriptor.get("lifecycle_policy"), LifecyclePolicy
                )
            }
            look_alpha = alpha_budget / max(1, max(look_counts, default=1))
            step_down_open = True
            for index, (phase_id, p_value) in enumerate(ordered_hypotheses):
                rank = index + 1
                threshold = look_alpha / (family_size - rank + 1)
                local_pass = p_value <= threshold
                passed = step_down_open and local_pass
                if not local_pass:
                    step_down_open = False
                holm[phase_id] = {
                    "family_key": family_key,
                    "family_size": family_size,
                    "alpha_budget": alpha_budget,
                    "rank": rank,
                    "threshold": threshold,
                    "passed": passed,
                    "registered": registered,
                    "consistent": consistent,
                    "lifecycle_policy": descriptors[phase_id][
                        "lifecycle_policy"
                    ],
                }

        readiness: list[dict[str, Any]] = []
        for phase in review_phases:
            phase_id = str(phase["phase_id"])
            metrics, milestone, _p_value = evidence[phase_id]
            holm_result = holm[phase_id]
            phase_policy = holm_result["lifecycle_policy"]
            decision = evaluate_readiness(
                metrics,
                policy=phase_policy,
                family_size=int(holm_result["family_size"]),
                family_rank=int(holm_result["rank"]),
                alpha_budget=float(holm_result["alpha_budget"]),
                milestone_closed=milestone is not None,
            )
            reasons = list(decision.reasons)
            if not holm_result["passed"] and "multiple_testing_gate" not in reasons:
                reasons.append("multiple_testing_gate")
            if not holm_result["registered"]:
                reasons.append("unregistered_statistical_family")
            if not holm_result["consistent"]:
                reasons.append("statistical_family_mismatch")
            decision_metrics = dict(decision.metrics)
            decision_metrics.update(
                {
                    "statistical_family": holm_result["family_key"],
                    "family_size": int(holm_result["family_size"]),
                    "holm_rank": int(holm_result["rank"]),
                    "holm_threshold": float(holm_result["threshold"]),
                    "holm_step_down_passed": bool(holm_result["passed"]),
                }
            )
            decision = GateDecision(
                eligible=not reasons,
                target_state=decision.target_state if not reasons else SHADOW,
                reasons=tuple(reasons),
                metrics=decision_metrics,
            )
            readiness.append(
                {
                    "phase_id": phase_id,
                    "milestone": milestone,
                    **decision.as_dict(),
                }
            )
            terminal_review = _phase_terminal_review_policy(phase)
            terminal_rows = None
            if terminal_review and terminal_review["enabled"]:
                try:
                    terminal_rows = self.store.triggers_for_phase_look(
                        phase_id, int(terminal_review["final_look"])
                    )
                except KeyError:
                    pass
            if terminal_rows is not None:
                terminal_metrics = self._prospective_metrics(
                    terminal_rows, invalid_as_loss=True
                )
                total = terminal_metrics["wins"] + terminal_metrics["losses"]
                pending = len(terminal_rows) - total
                terminal_closed = pending == 0
                hit_rate = terminal_metrics["wins"] / total if terminal_closed else None
                terminal_disposition = {
                    "closed": terminal_closed,
                    "final_look": int(terminal_review["final_look"]),
                    "resolved": total,
                    "pending": pending,
                    "hit_rate": hit_rate,
                    "min_point_hit_rate": float(
                        terminal_review["min_point_hit_rate"]
                    ),
                    "reviewable": (
                        hit_rate >= float(terminal_review["min_point_hit_rate"])
                        if terminal_closed else None
                    ),
                    "strict_readiness_passed": bool(terminal_closed and decision.eligible),
                    "production_applied": False,
                }
                readiness[-1]["terminal_review"] = terminal_disposition
                if str(phase["status"]) != "paused":
                    self.store.transition_phase(
                        phase_id,
                        "paused",
                        expected_status=str(phase["status"]),
                        reason="prospective_terminal_cohort_frozen",
                        metadata=terminal_disposition,
                        changed_at_utc=_utc_iso(now),
                    )
            elif decision.eligible:
                if str(phase["status"]) != "ready":
                    self.store.transition_phase(
                        phase_id,
                        "ready",
                        expected_status=str(phase["status"]),
                        reason="prospective_readiness_passed",
                        metadata={
                            "milestone": milestone,
                            "decision": decision.as_dict(),
                        },
                        changed_at_utc=_utc_iso(now),
                    )
            elif str(phase["status"]) == "ready":
                self.store.transition_phase(
                    phase_id,
                    "shadow",
                    expected_status="ready",
                    reason="prospective_readiness_revoked",
                    metadata={
                        "milestone": milestone,
                        "decision": decision.as_dict(),
                    },
                    changed_at_utc=_utc_iso(now),
                )

        degradation: Optional[dict[str, Any]] = None
        promoted: Optional[str] = None
        if pointer and pointer.get("phase_id"):
            phase_rows = self.store.list_phases()
            current = next(
                (
                    row
                    for row in phase_rows
                    if str(row.get("phase_id")) == str(pointer.get("phase_id"))
                ),
                None,
            )
            if current and str(current.get("status")) == "active":
                active_policy = _phase_lifecycle_policy(current)
                if active_policy is None:
                    raise RuntimeError(
                        "active phase lost its frozen lifecycle policy"
                    )
                triggers = self.store.list_triggers(
                    phase_id=str(current["phase_id"])
                )
                recent = [
                    row
                    for row in triggers
                    if str(row.get("latest_outcome_status") or "")
                    in {"win", "loss"}
                ]
                recent.sort(key=lambda row: str(row.get("triggered_at_utc") or ""))
                fast = self._prospective_metrics(
                    recent[-active_policy.degradation_fast_n :]
                )
                slow = self._prospective_metrics(
                    recent[-active_policy.degradation_slow_n :]
                )
                tenure = max(
                    0.0,
                    (
                        now - _parse_utc(current.get("updated_at_utc"))
                    ).total_seconds()
                    / 86400.0,
                )
                degradation_decision = evaluate_degradation(
                    recent_fast=fast,
                    recent_slow=slow,
                    tenure_days=tenure,
                    policy=active_policy,
                )
                degradation = degradation_decision.as_dict()
                if degradation_decision.eligible:
                    self._publish_fallback_before_pointer_clear(
                        pointer,
                        now=now,
                        previous_rule_id=previous_rule_id,
                    )
                    transaction = self.store.transition_phase_and_set_pointer(
                        str(current["phase_id"]),
                        "degraded",
                        expected_status="active",
                        pointer_name=self.pointer_name,
                        pointer_phase_id=None,
                        expected_generation=int(pointer["generation"]),
                        reason="prospective_degradation_gate",
                        metadata={
                            "reason": "champion_degraded",
                            "previous_rule_id": previous_rule_id,
                            "decision": degradation,
                        },
                        changed_at_utc=_utc_iso(now),
                    )
                    pointer = transaction["pointer"]

        if self.production_enabled and (
            not pointer or not pointer.get("phase_id")
        ):
            ready = self.store.list_phases(statuses=("ready",))
            ready = [
                row for row in ready
                if _phase_runtime_compatible(row) and not _terminal_phase(row)
            ]
            ready.sort(key=self._prospective_strength_key)
            if ready:
                selected = ready[0]
                expected_generation = int(pointer["generation"]) if pointer else 0
                transaction = self.store.transition_phase_and_set_pointer(
                    str(selected["phase_id"]),
                    "active",
                    expected_status="ready",
                    pointer_name=self.pointer_name,
                    pointer_phase_id=str(selected["phase_id"]),
                    expected_generation=expected_generation,
                    reason="automatic_champion_promotion",
                    metadata={
                        "reason": "automatic_champion_promotion",
                        "prospective_only": True,
                        "previous_rule_id": previous_rule_id,
                    },
                    changed_at_utc=_utc_iso(now),
                )
                pointer = transaction["pointer"]
                promoted = str(selected["phase_id"])

        published = self._publish_active_manifest(pointer)
        return {
            "readiness": readiness,
            "degradation": degradation,
            "promoted_phase_id": promoted,
            "active_pointer": pointer,
            "active_manifest": published,
        }

    def _publish_active_manifest(
        self, pointer: Optional[Mapping[str, Any]]
    ) -> dict[str, Any]:
        rule_payload: Optional[dict[str, Any]] = None
        previous_rule_id: Optional[str] = None
        generation = 1
        effective = self.prospective_start_utc
        if pointer is not None:
            generation = max(1, int(pointer.get("generation") or 1))
            effective = str(pointer.get("updated_at_utc") or effective)
            metadata = _mapping(pointer.get("payload"))
            previous_rule_id = str(metadata.get("previous_rule_id") or "") or None
            phase_id = pointer.get("phase_id")
            if phase_id:
                rows = self.store.list_phases()
                phase = next(
                    (
                        row
                        for row in rows
                        if str(row.get("phase_id")) == str(phase_id)
                    ),
                    None,
                )
                if phase is not None and str(phase.get("status")) == "active":
                    if not _phase_runtime_compatible(phase) or _terminal_phase(phase):
                        phase = None
                if phase is not None and str(phase.get("status")) == "active":
                    rule_payload = {
                        "phase_id": phase["phase_id"],
                        "rule_id": phase["rule_id"],
                        "manifest": phase["manifest"],
                    }
        payload = build_active_manifest(
            generation=generation,
            rule=rule_payload,
            effective_from_utc=effective,
            previous_rule_id=previous_rule_id,
            production_enabled=self.production_enabled,
            fallback="current_filter",
        )
        destination = Path(self.active_manifest_path)
        if destination.exists():
            try:
                current = json.loads(destination.read_text(encoding="utf-8"))
            except (OSError, ValueError, TypeError):
                current = None
            if current == payload and validate_active_manifest(current):
                return payload
        write_active_manifest_atomic(destination, payload)
        return payload


__all__ = ["WideResearchController", "validate_discovery_report"]
