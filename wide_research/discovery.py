"""Deterministic, leakage-resistant discovery of wide-universe signal rules.

The live bot is intentionally not imported here.  The public ``discover_rules``
function accepts already joined observation mappings, while ``JournalJoinStore``
provides the disk-backed streaming adapter used by the command-line tool.

Discovery and evaluation are separated in time and by fixture:

* thresholds (including quantiles) are built from the discovery split only;
* the validation split chooses at most ten frozen candidates;
* the holdout split is opened only after those candidates are frozen;
* every rule gets at most one trigger per fixture (the earliest match).

This module is stdlib-only so that an offline research run cannot silently
change the production dependency surface.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
import re
import sqlite3
import tempfile
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional, Sequence

from .features import (
    ALLOWED_FEATURE_NAMES,
    FEATURE_SCHEMA_VERSION,
    evaluate_universe,
    extract_features as extract_feature_vector,
)
from .rules import Clause as ManifestClause
from .rules import RuleManifest
from .schema import PASS, UniverseSpec
from .extended_features import EXTENDED_FEATURE_NAMES, EXTENDED_FEATURES, PRODUCTS, PRIMITIVES, has_market_dependency
from market_benchmark.research import freeze_market_context, HISTORY_SECONDS
from outcome_revision import outcome_rank


UTC = timezone.utc
DISCOVERY_SCHEMA_VERSION = 2
DISCOVERY_ENGINE_VERSION = "wide_rule_discovery_v3"
RULE_MANIFEST_VERSION = "wide_rule_manifest_v2"
ATOMIC_GRID_VERSION = "wide_atomic_grid_v2"
RARE_PRECISION_DISCOVERY_ENGINE_VERSION = "wide_rule_discovery_rare_v1"
RARE_PRECISION_ATOMIC_GRID_VERSION = "wide_atomic_range_grid_v1"
EXTENDED_DISCOVERY_ENGINE_VERSION = "wide_rule_discovery_extended_v1"
REFINEMENT_DISCOVERY_ENGINE_VERSION = "wide_rule_discovery_refinement_v1"
EXTENDED_ATOMIC_GRID_VERSION = "wide_extended_grid_v1"
TEMPORAL_PURGE_VERSION = "chronological_fixture_purge_v1"
TEMPORAL_PURGED_ENGINE_SUFFIX = "_purged_v1"
PURGED_SPLIT_POLICY = "chronological_fixture_group_purged_v1"
TRAIN_SIGNATURE_MAX_REPRESENTATIVES = 3
_ARCHIVE_STAMP = re.compile(r"^\d{8}T\d{6}(?:\d{6})?Z$")
_WILSON_Z_95 = 1.959963984540054
RARE_PRECISION_VALIDATION_WINDOWS = 4
RARE_PRECISION_WILSON_WEIGHT = 0.5


def _discovery_engine_version(config: "DiscoveryConfig") -> str:
    if config.error_refinement:
        version = REFINEMENT_DISCOVERY_ENGINE_VERSION
    elif config.extended_features:
        version = EXTENDED_DISCOVERY_ENGINE_VERSION
    elif config.selection_mode == "rare_precision":
        version = RARE_PRECISION_DISCOVERY_ENGINE_VERSION
    else:
        version = DISCOVERY_ENGINE_VERSION
    if config.temporal_purge:
        return version + TEMPORAL_PURGED_ENGINE_SUFFIX
    return version


def _atomic_grid_version(config: "DiscoveryConfig") -> str:
    if config.extended_features:
        return EXTENDED_ATOMIC_GRID_VERSION
    if config.selection_mode == "rare_precision":
        return RARE_PRECISION_ATOMIC_GRID_VERSION
    return ATOMIC_GRID_VERSION


# Direction and pre-registered thresholds.  Data-derived additions are allowed
# only as quantiles of the discovery split and are recorded as such.
ATOMIC_THRESHOLD_GRIDS: Mapping[str, tuple[str, tuple[float, ...]]] = {
    "minute": ("<=", (48.0, 50.0, 52.0, 55.0, 57.0, 60.0)),
    "score.total_goals": ("<=", (0.0, 1.0, 2.0, 3.0)),
    "bot.prob_to90": (">=", (65.0, 70.0, 75.0, 79.0, 80.0, 83.0, 85.0, 88.0, 90.0)),
    "bot.prob_next_15": (">=", (25.0, 30.0, 35.0, 40.0, 45.0, 50.0)),
    "reputation.delta_to90_pp": (">=", (0.0, 0.5, 1.0, 1.5, 2.0)),
    "feature.adjusted_intensity": (">=", (0.40, 0.50, 0.55, 0.60, 0.70, 0.80, 1.0)),
    "feature.pressure_index": (">=", (10.0, 15.0, 20.0, 25.0, 30.0)),
    "feature.season_context_factor": (">=", (0.98, 1.0, 1.02, 1.04, 1.06)),
    "feature.game_state_factor": (">=", (0.98, 1.0, 1.02, 1.03, 1.05)),
    "feature.goal_xg_gap": ("<=", (0.0, 0.5, 1.0, 1.3, 1.5, 2.0)),
    "feature.sample_confidence": (">=", (0.25, 0.40, 0.50, 0.60, 0.75)),
    "raw.xg_total": (">=", (0.5, 1.0, 1.5, 2.0, 2.5, 3.0)),
    "raw.shots_on_target_total": (">=", (2.0, 3.0, 4.0, 5.0, 6.0, 8.0)),
    "raw.shots_in_box_total": (">=", (4.0, 6.0, 8.0, 10.0, 12.0, 15.0)),
    "raw.total_shots_total": (">=", (6.0, 8.0, 10.0, 12.0, 15.0, 20.0)),
    "raw.corners_total": ("<=", (4.0, 6.0, 8.0, 10.0, 12.0)),
    "raw.red_cards_total": ("<=", (0.0, 1.0)),
    "rolling.5m.rate_per_minute.shots_on_target_total": (">=", (0.0, 0.1, 0.2, 0.3, 0.5, 0.75)),
    "rolling.5m.rate_per_minute.shots_in_box_total": (">=", (0.0, 0.2, 0.4, 0.6, 0.8, 1.0)),
    "rolling.5m.rate_per_minute.total_shots_total": (">=", (0.0, 0.5, 0.75, 1.0, 1.5, 2.0)),
    "rolling.5m.rate_per_minute.corners_total": (">=", (0.0, 0.1, 0.2, 0.3, 0.5)),
    "rolling.5m.rate_per_minute.pressure_index": (">=", (0.0, 0.1, 0.2, 0.3, 0.5, 0.75)),
    "rolling.10m.rate_per_minute.shots_on_target_total": (">=", (0.0, 0.1, 0.2, 0.3, 0.5, 0.75)),
    "rolling.10m.rate_per_minute.shots_in_box_total": (">=", (0.0, 0.2, 0.4, 0.6, 0.8, 1.0)),
    "rolling.10m.rate_per_minute.total_shots_total": (">=", (0.0, 0.5, 0.75, 1.0, 1.5, 2.0)),
    "rolling.10m.rate_per_minute.corners_total": (">=", (0.0, 0.1, 0.2, 0.3, 0.5)),
    "rolling.10m.rate_per_minute.pressure_index": (">=", (0.0, 0.1, 0.2, 0.3, 0.5, 0.75)),
    "ml.static.prob_to90": (">=", (60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0)),
    "ml.rolling.prob_to90": (">=", (60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0)),
}

# The production-capable search keeps its original, deliberately small,
# pre-registered grid.  The isolated precision profile additionally examines
# every reviewed causal numeric feature.  Extra features use train-only
# quantiles, avoiding arbitrary hand-tuned cut-offs and any holdout leakage.
_PRECISION_LOWER_IS_FAVOURABLE = frozenset(
    {
        "score.home_goals",
        "score.away_goals",
        "score.goal_difference_abs",
        "feature.goal_xg_gap",
        "raw.corners_total",
        "raw.red_cards_home",
        "raw.red_cards_away",
        "raw.red_cards_total",
        "raw.yellow_cards_home",
        "raw.yellow_cards_away",
        "raw.yellow_cards_total",
    }
)
PRECISION_EXTRA_THRESHOLD_GRIDS: Mapping[str, tuple[str, tuple[float, ...]]] = {
    feature: (
        "<=" if feature in _PRECISION_LOWER_IS_FAVOURABLE else ">=",
        (),
    )
    for feature in ALLOWED_FEATURE_NAMES
    if feature not in ATOMIC_THRESHOLD_GRIDS
}

# These causal shortcuts are deliberately confined to the hard-shadow
# precision laboratory.  In particular, the production-capable primary
# profile must not discover or promote them before prospective validation.
PRECISION_COMPOSITE_THRESHOLD_GRIDS: Mapping[
    str, tuple[str, tuple[float, ...]]
] = {
    "score.goal_difference_abs": ("<=", (0.0, 1.0, 2.0)),
    "composite.base_quality_v1": (">=", (1.0,)),
    "rolling.both_windows_available_v1": (">=", (1.0,)),
}
PRECISION_FIXED_ONLY_FEATURES = frozenset(
    {
        "composite.base_quality_v1",
        "rolling.both_windows_available_v1",
    }
)


def _threshold_grids(
    config: "DiscoveryConfig",
) -> Mapping[str, tuple[str, tuple[float, ...]]]:
    if config.selection_mode not in {"precision_first", "rare_precision"}:
        return ATOMIC_THRESHOLD_GRIDS
    return {
        **({name: (">=", ()) for name in EXTENDED_FEATURE_NAMES} if config.extended_features else {}),
        **PRECISION_EXTRA_THRESHOLD_GRIDS,
        **ATOMIC_THRESHOLD_GRIDS,
        **PRECISION_COMPOSITE_THRESHOLD_GRIDS,
    }


@dataclass(frozen=True)
class DiscoveryConfig:
    """Pre-registered search limits and chronological split policy."""

    min_minute: int = 46
    max_minute: int = 60
    train_fraction: float = 0.60
    validation_fraction: float = 0.20
    min_conjunction_size: int = 1
    max_conjunction_size: int = 3
    beam_width: int = 64
    evaluation_budget: int = 12_000
    depth_evaluation_budgets: tuple[int, ...] = ()
    top_n: int = 10
    min_train_support: int = 40
    min_validation_support: int = 15
    min_holdout_support: int = 15
    target_hit_rate: float = 0.95
    null_hit_rate: float = 0.90
    family_alpha: float = 0.05
    selection_mode: str = "standard"
    extended_features: bool = False
    market_only: bool = False
    error_refinement: bool = False
    min_signals_per_week: float = 0.0
    preferred_signals_per_week: float = 0.0
    # Soft reporting-band ceiling only; minimum cadence is the actual gate.
    max_signals_per_week: float = 0.0
    portfolio_max_rules: int = 1
    portfolio_beam_width: int = 32
    quantiles: tuple[float, ...] = (0.20, 0.35, 0.50, 0.65, 0.80)
    max_prediction_lag_seconds: float = 300.0
    allow_feature_ranges: bool = False
    validation_window_count: int = 1
    # Opt-in so historical artifacts and prospective candidate identities keep
    # their original execution contract.  When enabled, labels from an earlier
    # split must have been available before the next split began.
    temporal_purge: bool = False
    temporal_embargo_seconds: float = 300.0

    def __post_init__(self) -> None:
        if not 0 < int(self.min_minute) <= int(self.max_minute):
            raise ValueError("invalid minute window")
        if not 0.0 < float(self.train_fraction) < 1.0:
            raise ValueError("train_fraction must be in (0, 1)")
        if not 0.0 < float(self.validation_fraction) < 1.0:
            raise ValueError("validation_fraction must be in (0, 1)")
        if self.train_fraction + self.validation_fraction >= 1.0:
            raise ValueError("train and validation fractions must leave a holdout")
        if not 1 <= int(self.min_conjunction_size) <= 8:
            raise ValueError("min_conjunction_size must be in [1, 8]")
        if not 1 <= int(self.max_conjunction_size) <= 8:
            raise ValueError("max_conjunction_size must be in [1, 8]")
        if int(self.min_conjunction_size) > int(self.max_conjunction_size):
            raise ValueError(
                "min_conjunction_size cannot exceed max_conjunction_size"
            )
        for name in (
            "beam_width",
            "evaluation_budget",
            "top_n",
            "min_train_support",
            "min_validation_support",
            "min_holdout_support",
        ):
            if int(getattr(self, name)) <= 0:
                raise ValueError(f"{name} must be positive")
        if int(self.top_n) > 20:
            raise ValueError("top_n cannot exceed 20")
        depth_budgets = tuple(int(value) for value in self.depth_evaluation_budgets)
        if depth_budgets:
            if len(depth_budgets) != int(self.max_conjunction_size):
                raise ValueError(
                    "depth_evaluation_budgets must contain one positive budget "
                    "for every conjunction depth"
                )
            if any(value <= 0 for value in depth_budgets):
                raise ValueError("depth evaluation budgets must be positive")
            if sum(depth_budgets) > int(self.evaluation_budget):
                raise ValueError(
                    "depth evaluation budgets cannot exceed evaluation_budget"
                )
        if self.selection_mode not in {
            "standard",
            "precision_first",
            "rare_precision",
        }:
            raise ValueError(
                "selection_mode must be 'standard', 'precision_first', or "
                "'rare_precision'"
            )
        if int(self.validation_window_count) <= 0:
            raise ValueError("validation_window_count must be positive")
        if self.extended_features and self.selection_mode != "rare_precision":
            raise ValueError("extended features require isolated rare precision")
        if self.market_only and not self.extended_features:
            raise ValueError("market scope requires extended features")
        if self.error_refinement and not self.extended_features:
            raise ValueError("error refinement requires isolated extended features")
        if self.selection_mode == "rare_precision":
            if not bool(self.allow_feature_ranges):
                raise ValueError(
                    "rare_precision requires allow_feature_ranges=True"
                )
            if int(self.validation_window_count) != (
                RARE_PRECISION_VALIDATION_WINDOWS
            ):
                raise ValueError(
                    "rare_precision requires validation_window_count=4"
                )
        for name in (
            "min_signals_per_week",
            "preferred_signals_per_week",
            "max_signals_per_week",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0.0:
                raise ValueError(f"{name} must be finite and non-negative")
        if (
            float(self.preferred_signals_per_week) > 0.0
            and float(self.preferred_signals_per_week)
            < float(self.min_signals_per_week)
        ):
            raise ValueError(
                "preferred_signals_per_week cannot be below "
                "min_signals_per_week"
            )
        if (
            float(self.max_signals_per_week) > 0.0
            and float(self.max_signals_per_week)
            < max(
                float(self.min_signals_per_week),
                float(self.preferred_signals_per_week),
            )
        ):
            raise ValueError(
                "max_signals_per_week cannot be below the minimum or "
                "preferred cadence"
            )
        if not 1 <= int(self.portfolio_max_rules) <= int(self.top_n):
            raise ValueError("portfolio_max_rules must be in [1, top_n]")
        if int(self.portfolio_beam_width) <= 0:
            raise ValueError("portfolio_beam_width must be positive")
        for name in ("target_hit_rate", "null_hit_rate", "family_alpha"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be in [0, 1]")
        if self.target_hit_rate < self.null_hit_rate:
            raise ValueError("target_hit_rate cannot be below null_hit_rate")
        quantiles = tuple(float(value) for value in self.quantiles)
        if not quantiles or any(not 0.0 < value < 1.0 for value in quantiles):
            raise ValueError("quantiles must be non-empty values in (0, 1)")
        if quantiles != tuple(sorted(set(quantiles))):
            raise ValueError("quantiles must be sorted and unique")
        if not math.isfinite(float(self.max_prediction_lag_seconds)) or float(
            self.max_prediction_lag_seconds
        ) < 0.0:
            raise ValueError("max_prediction_lag_seconds must be non-negative")
        if not math.isfinite(float(self.temporal_embargo_seconds)) or float(
            self.temporal_embargo_seconds
        ) < 0.0:
            raise ValueError("temporal_embargo_seconds must be non-negative")


@dataclass(frozen=True, order=True)
class AtomicClause:
    feature: str
    operator: str
    threshold: float
    _allow_alternate_operator: bool = field(
        default=False,
        compare=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        grid = ATOMIC_THRESHOLD_GRIDS.get(
            self.feature,
            PRECISION_EXTRA_THRESHOLD_GRIDS.get(self.feature),
        )
        if grid is None and self._allow_alternate_operator and self.feature in EXTENDED_FEATURES:
            grid = (">=", ())
        if grid is None:
            raise ValueError(f"unsupported feature: {self.feature}")
        expected = grid[0]
        allowed = (
            {">=", "<="}
            if self._allow_alternate_operator
            else {expected}
        )
        if self.operator not in allowed:
            raise ValueError(f"{self.feature} requires operator {expected}")
        if not math.isfinite(float(self.threshold)):
            raise ValueError("threshold must be finite")

    def as_dict(self) -> dict[str, Any]:
        return {
            "feature": self.feature,
            "operator": self.operator,
            "threshold": float(self.threshold),
        }


@dataclass(frozen=True)
class _Row:
    observation_id: str
    fixture_id: int
    created_at: datetime
    minute: int
    league_key: str
    label: int
    features: Mapping[str, float]
    # Time at which this row's target became knowable.  Direct unit fixtures
    # may omit it, but temporal purging treats a missing value as unavailable.
    label_available_at: Optional[datetime] = None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _finite(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _safe_int(value: Any) -> Optional[int]:
    number = _finite(value)
    if number is None or not number.is_integer():
        return None
    return int(number)


def _parse_utc(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _sum_values(mapping: Mapping[str, Any], *names: str) -> Optional[float]:
    values = [_finite(mapping.get(name)) for name in names]
    if any(value is None for value in values):
        return None
    return float(sum(value for value in values if value is not None))


def _first_finite(*values: Any) -> Optional[float]:
    for value in values:
        number = _finite(value)
        if number is not None:
            return number
    return None


def _rolling_activity(record: Mapping[str, Any], window: str) -> tuple[Optional[float], Optional[float]]:
    block = _mapping(_mapping(_mapping(record.get("rolling_dynamics")).get("windows")).get(window))
    if str(block.get("status") or "").lower() != "ok":
        return None, None
    rates = _mapping(block.get("rates_per_minute"))
    activity_values = [
        _finite(rates.get(name))
        for name in (
            "xg_total",
            "shots_on_target_total",
            "shots_in_box_total",
            "total_shots_total",
            "corners_total",
        )
    ]
    present = [value for value in activity_values if value is not None]
    activity = sum(present) if present else None
    return activity, _finite(rates.get("pressure_index"))


def extract_features(record: Mapping[str, Any], *, include_extended: bool = False) -> dict[str, float]:
    """Use exactly the same allow-listed feature contract as live scoring.

    The joiner places full, causally validated prediction records in the
    private ``_wide_predictions`` block.  The shared extractor validates them
    again and keeps missing values missing, so discovery cannot accidentally
    use a looser feature definition than prospective shadow evaluation.
    """

    predictions = _mapping(record.get("_wide_predictions"))
    vector = extract_feature_vector(
        record,
        static_prediction=_mapping(predictions.get("static")) or None,
        rolling_prediction=_mapping(predictions.get("rolling")) or None,
        include_extended=include_extended,
    )
    values = _mapping(vector.get("values"))
    return {
        name: float(value)
        for name, value in values.items()
        if value is not None and _finite(value) is not None
    }


def _resolved_label(record: Mapping[str, Any]) -> Optional[int]:
    outcome = _mapping(record.get("outcome"))
    if str(outcome.get("status") or "").strip().lower() != "resolved":
        return None
    value = outcome.get("goal_to90_normal_time")
    if value is True or value == 1:
        return 1
    if value is False or value == 0:
        return 0
    return None


def _eligible_row(record: Mapping[str, Any], config: DiscoveryConfig) -> tuple[Optional[_Row], str]:
    if str(record.get("record_type") or "observation") != "observation":
        return None, "record_type"
    universe = evaluate_universe(
        record,
        UniverseSpec(min_minute=config.min_minute, max_minute=config.max_minute),
    )
    if universe.get("status") != PASS:
        return None, str(universe.get("reason") or "universe")
    fixture_id = _safe_int(record.get("fixture_id"))
    minute = _safe_int(record.get("minute"))
    created_at = _parse_utc(record.get("created_at_utc"))
    observation_id = str(
        record.get("observation_id") or record.get("observation_key") or ""
    ).strip()
    if fixture_id is None or fixture_id <= 0:
        return None, "fixture_id"
    if minute is None or not config.min_minute <= minute <= config.max_minute:
        return None, "minute"
    if created_at is None:
        return None, "timestamp"
    if not observation_id:
        return None, "observation_id"
    label = _resolved_label(record)
    if label is None:
        return None, "unresolved_outcome"
    outcome = _mapping(record.get("outcome"))
    declared_resolved_at = _parse_utc(outcome.get("resolved_at_utc"))
    has_authoritative_label_time = (
        "_outcome_record_created_at_utc" in record
        or "outcome_record_created_at_utc" in record
    )
    authoritative_label_time = record.get(
        "_outcome_record_created_at_utc",
        record.get("outcome_record_created_at_utc"),
    )
    known_at = _parse_utc(authoritative_label_time)
    # A purged run must not substitute the provider's nominal resolution time
    # for an absent journal timestamp: that would make a late/backfilled label
    # appear available before a split boundary.  Legacy discovery keeps its
    # historical fallback for direct callers that do not carry journal
    # metadata.
    label_available_at = (
        known_at
        if has_authoritative_label_time or config.temporal_purge
        else declared_resolved_at
    )
    outcome_timing = known_at or declared_resolved_at
    if outcome_timing is None or outcome_timing <= created_at:
        return None, "outcome_timing"
    match = _mapping(record.get("match"))
    league_key = str(
        match.get("league_id")
        or match.get("league_name")
        or "unknown"
    )
    features = extract_features(record, include_extended=config.extended_features)
    if config.market_only and "market.v1.prob_to90" not in features:
        return None, "causal_market_unavailable"
    return (
        _Row(
            observation_id=observation_id,
            fixture_id=fixture_id,
            created_at=created_at,
            minute=minute,
            league_key=league_key,
            label=label,
            features=features,
            label_available_at=label_available_at,
        ),
        "",
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _quantile(values: Sequence[float], probability: float) -> float:
    """Inclusive linear quantile with no dependency on statistics internals."""

    ordered = sorted(float(value) for value in values)
    if not ordered:
        raise ValueError("quantile requires at least one value")
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * float(probability)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _normalise_threshold(value: float) -> float:
    return float(round(float(value), 6))


def _clause_key(clause: AtomicClause) -> tuple[str, str, float]:
    return clause.feature, clause.operator, float(clause.threshold)


def _rule_key(clauses: Sequence[AtomicClause]) -> tuple[tuple[str, str, float], ...]:
    return tuple(sorted((_clause_key(clause) for clause in clauses)))


def _candidate_id(
    clauses: Sequence[AtomicClause],
    *,
    config: DiscoveryConfig = DiscoveryConfig(),
) -> str:
    payload = {
        "discovery_engine_version": _discovery_engine_version(config),
        "rule_manifest_version": RULE_MANIFEST_VERSION,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "threshold_grid_version": _atomic_grid_version(config),
        "universe": {
            "stages": ["decision_pipeline", "wide_monitor"],
            "minute_min": int(config.min_minute),
            "minute_max": int(config.max_minute),
            "current_filter_policy": "ignored",
        },
        "clauses": [
            {"feature": feature, "operator": operator, "threshold": threshold}
            for feature, operator, threshold in _rule_key(clauses)
        ],
        "trigger_policy": "first_matching_observation_per_fixture",
    }
    return "wide-" + _digest(payload)[:20]


def _matches(row: _Row, clauses: Sequence[AtomicClause]) -> bool:
    for clause in clauses:
        value = row.features.get(clause.feature)
        if value is None:
            return False
        if clause.operator == ">=" and value < clause.threshold:
            return False
        if clause.operator == "<=" and value > clause.threshold:
            return False
    return True


def _first_triggers(
    fixtures: Mapping[int, Sequence[_Row]], clauses: Sequence[AtomicClause]
) -> list[_Row]:
    selected: list[_Row] = []
    for fixture_id in sorted(fixtures):
        for row in fixtures[fixture_id]:
            if _matches(row, clauses):
                selected.append(row)
                break
    selected.sort(key=lambda row: (row.created_at, row.fixture_id, row.observation_id))
    return selected


def _wilson_interval(wins: int, total: int) -> tuple[Optional[float], Optional[float]]:
    if total <= 0:
        return None, None
    proportion = wins / total
    z2 = _WILSON_Z_95**2
    denominator = 1.0 + z2 / total
    centre = (proportion + z2 / (2.0 * total)) / denominator
    margin = (
        _WILSON_Z_95
        * math.sqrt(
            proportion * (1.0 - proportion) / total
            + z2 / (4.0 * total**2)
        )
        / denominator
    )
    return max(0.0, centre - margin), min(1.0, centre + margin)


def _period_rows(rows: Sequence[_Row], period: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[_Row]] = defaultdict(list)
    for row in rows:
        if period == "day":
            key = row.created_at.date().isoformat()
        else:
            iso = row.created_at.isocalendar()
            key = f"{iso.year}-W{iso.week:02d}"
        grouped[key].append(row)
    result = []
    for key in sorted(grouped):
        values = grouped[key]
        wins = sum(row.label for row in values)
        result.append(
            {
                "period": key,
                "wins": wins,
                "losses": len(values) - wins,
                "resolved": len(values),
                "hit_rate": round(wins / len(values), 9),
            }
        )
    return result


def _evaluation_span_days(fixtures: Mapping[int, Sequence[_Row]]) -> float:
    timestamps = [
        row.created_at for fixture_rows in fixtures.values() for row in fixture_rows
    ]
    if not timestamps:
        return 0.0
    elapsed_days = (
        max(timestamps) - min(timestamps)
    ).total_seconds() / 86400.0
    # A same-day split still represents one calendar day of exposure.  For
    # longer spans the elapsed wall-clock interval naturally includes weeks
    # in which a rule produced no triggers.
    return max(1.0, elapsed_days)


def _calendar_triggers_per_week(total: int, evaluation_span_days: float) -> float:
    if total <= 0 or evaluation_span_days <= 0.0:
        return 0.0
    return float(total) / (float(evaluation_span_days) / 7.0)


def _metrics(
    rows: Sequence[_Row],
    universe_fixture_count: int,
    *,
    evaluation_span_days: Optional[float] = None,
) -> dict[str, Any]:
    total = len(rows)
    wins = sum(row.label for row in rows)
    lower, upper = _wilson_interval(wins, total)
    league_counts = Counter(row.league_key for row in rows)
    max_league = max(league_counts.values(), default=0)
    first_at = min((row.created_at for row in rows), default=None)
    last_at = max((row.created_at for row in rows), default=None)
    daily = _period_rows(rows, "day")
    weekly = _period_rows(rows, "week")
    span_days = (
        (last_at - first_at).total_seconds() / 86400.0
        if first_at is not None and last_at is not None
        else 0.0
    )
    exposure_days = (
        max(0.0, float(evaluation_span_days))
        if evaluation_span_days is not None
        else max(1.0, span_days) if rows else 0.0
    )
    calendar_rate = _calendar_triggers_per_week(total, exposure_days)
    return {
        "wins": wins,
        "losses": total - wins,
        "resolved": total,
        "hit_rate": round(wins / total, 9) if total else None,
        "hit_rate_pct": round(100.0 * wins / total, 6) if total else None,
        "wilson_95": (
            [round(lower, 9), round(upper, 9)] if lower is not None else None
        ),
        "wilson_lower": round(lower, 9) if lower is not None else None,
        "wilson_upper": round(upper, 9) if upper is not None else None,
        "fixture_coverage": (
            round(total / universe_fixture_count, 9)
            if universe_fixture_count
            else None
        ),
        "first_trigger_utc": first_at.isoformat() if first_at else None,
        "last_trigger_utc": last_at.isoformat() if last_at else None,
        "span_days": round(span_days, 9),
        "evaluation_span_days": round(exposure_days, 9),
        "trigger_days": len(daily),
        "leagues": len(league_counts),
        "max_league_share": round(max_league / total, 9) if total else None,
        "league_hhi": (
            round(sum((count / total) ** 2 for count in league_counts.values()), 9)
            if total
            else None
        ),
        "league_counts": dict(sorted(league_counts.items())),
        "daily": daily,
        "weekly": weekly,
        "triggers_per_week": (
            round(total / max(1, len(weekly)), 9) if total else 0.0
        ),
        "calendar_triggers_per_week": round(calendar_rate, 9),
        "first_triggers": [
            {
                "fixture_id": row.fixture_id,
                "observation_id": row.observation_id,
                "created_at_utc": row.created_at.isoformat(),
                "minute": row.minute,
                "league_key": row.league_key,
                "label": row.label,
            }
            for row in rows
        ],
    }


def _ranking_metrics(
    rows: Sequence[_Row],
    universe_fixture_count: int,
    *,
    evaluation_span_days: Optional[float] = None,
) -> dict[str, Any]:
    """Return only the fields needed while exploring thousands of rules.

    Full metrics intentionally retain trigger identities and period/league
    breakdowns for auditable finalists.  Caching those large structures for
    every beam-search proposal makes deeper searches needlessly memory-heavy.
    """

    total = len(rows)
    wins = sum(row.label for row in rows)
    lower, upper = _wilson_interval(wins, total)
    exposure_days = max(0.0, float(evaluation_span_days or 0.0))
    return {
        "wins": wins,
        "losses": total - wins,
        "resolved": total,
        "hit_rate": round(wins / total, 9) if total else None,
        "hit_rate_pct": round(100.0 * wins / total, 6) if total else None,
        "wilson_lower": round(lower, 9) if lower is not None else None,
        "wilson_upper": round(upper, 9) if upper is not None else None,
        "fixture_coverage": (
            round(total / universe_fixture_count, 9)
            if universe_fixture_count
            else None
        ),
        "evaluation_span_days": round(exposure_days, 9),
        "calendar_triggers_per_week": round(
            _calendar_triggers_per_week(total, exposure_days), 9
        ),
    }


def _rank_key(metrics: Mapping[str, Any], clauses: Sequence[AtomicClause]) -> tuple[Any, ...]:
    lower = metrics.get("wilson_lower")
    hit_rate = metrics.get("hit_rate")
    return (
        -(float(lower) if lower is not None else -1.0),
        -(float(hit_rate) if hit_rate is not None else -1.0),
        -int(metrics.get("resolved") or 0),
        len(clauses),
        _rule_key(clauses),
    )


def _binomial_upper_tail(wins: int, total: int, null_rate: float) -> float:
    if total <= 0 or wins <= 0:
        return 1.0
    if null_rate <= 0.0:
        return 0.0
    if null_rate >= 1.0:
        return 1.0
    logs = []
    for value in range(wins, total + 1):
        logs.append(
            math.lgamma(total + 1)
            - math.lgamma(value + 1)
            - math.lgamma(total - value + 1)
            + value * math.log(null_rate)
            + (total - value) * math.log1p(-null_rate)
        )
    largest = max(logs)
    return min(1.0, math.exp(largest) * math.fsum(math.exp(item - largest) for item in logs))


def _holm(p_values: Mapping[str, float], alpha: float) -> dict[str, dict[str, Any]]:
    ordered = sorted(p_values.items(), key=lambda item: (item[1], item[0]))
    count = len(ordered)
    previous = 0.0
    rejected_so_far = True
    result: dict[str, dict[str, Any]] = {}
    for index, (candidate_id, p_value) in enumerate(ordered):
        multiplier = count - index
        adjusted = max(previous, min(1.0, p_value * multiplier))
        previous = adjusted
        threshold = alpha / multiplier
        rejected = rejected_so_far and p_value <= threshold
        if not rejected:
            rejected_so_far = False
        result[candidate_id] = {
            "one_sided_binomial_p": round(p_value, 12),
            "holm_rank": index + 1,
            "holm_threshold": round(threshold, 12),
            "holm_adjusted_p": round(adjusted, 12),
            "reject_null_at_alpha": rejected,
        }
    return result


def _split_fixtures(
    fixtures: Mapping[int, Sequence[_Row]], config: DiscoveryConfig
) -> tuple[dict[str, dict[int, Sequence[_Row]]], dict[str, Any]]:
    ordered = sorted(
        fixtures,
        key=lambda fixture_id: (
            fixtures[fixture_id][0].created_at,
            fixture_id,
        ),
    )
    count = len(ordered)
    if count < 3:
        raise ValueError("at least three resolved fixtures are required")
    train_end = max(1, min(count - 2, int(math.floor(count * config.train_fraction))))
    validation_count = max(1, int(math.floor(count * config.validation_fraction)))
    validation_end = min(count - 1, train_end + validation_count)
    if validation_end <= train_end:
        validation_end = train_end + 1
    nominal_ids = {
        "train": ordered[:train_end],
        "validation": ordered[train_end:validation_end],
        "holdout": ordered[validation_end:],
    }
    ids = {name: list(fixture_ids) for name, fixture_ids in nominal_ids.items()}
    purge_metadata: Optional[dict[str, Any]] = None
    if config.temporal_purge:
        embargo = timedelta(seconds=float(config.temporal_embargo_seconds))
        purge_metadata = {
            "version": TEMPORAL_PURGE_VERSION,
            "embargo_seconds": float(config.temporal_embargo_seconds),
            "no_fixture_reallocation": True,
            "boundaries": {},
        }
        for earlier, later in (("train", "validation"), ("validation", "holdout")):
            later_ids = nominal_ids[later]
            # Boundaries are frozen from the nominal chronological split.  A
            # purge never moves a later fixture backwards to make the sample
            # look larger after seeing labels.
            boundary = min(
                row.created_at
                for fixture_id in later_ids
                for row in fixtures[fixture_id]
            )
            cutoff = boundary - embargo
            kept: list[int] = []
            removed: list[int] = []
            reasons: Counter[str] = Counter()
            for fixture_id in nominal_ids[earlier]:
                rows = fixtures[fixture_id]
                if any(row.created_at >= cutoff for row in rows):
                    reason = "fixture_observation_crosses_cutoff"
                elif any(row.label_available_at is None for row in rows):
                    reason = "label_availability_missing"
                elif any(
                    row.label_available_at is not None
                    and row.label_available_at >= cutoff
                    for row in rows
                ):
                    reason = "label_not_available_before_cutoff"
                else:
                    kept.append(fixture_id)
                    continue
                removed.append(fixture_id)
                reasons[reason] += 1
            ids[earlier] = kept
            purge_metadata["boundaries"][f"{earlier}_to_{later}"] = {
                "next_split_first_observation_utc": boundary.isoformat(),
                "information_cutoff_utc": cutoff.isoformat(),
                "nominal_fixture_count": len(nominal_ids[earlier]),
                "retained_fixture_count": len(kept),
                "purged_fixture_count": len(removed),
                "purged_fixture_ids_sha256": _digest(removed),
                "purge_reasons": dict(sorted(reasons.items())),
            }
        empty = [name for name in ("train", "validation", "holdout") if not ids[name]]
        if empty:
            raise ValueError(
                "temporal purge left empty split(s): " + ", ".join(empty)
            )
    splits = {
        name: {fixture_id: fixtures[fixture_id] for fixture_id in fixture_ids}
        for name, fixture_ids in ids.items()
    }
    metadata: dict[str, Any] = {}
    for name, fixture_ids in ids.items():
        rows = [row for fixture_id in fixture_ids for row in fixtures[fixture_id]]
        metadata[name] = {
            "fixture_count": len(fixture_ids),
            "observation_count": len(rows),
            "fixture_ids": fixture_ids,
            "fixture_ids_sha256": _digest(fixture_ids),
            "first_observation_utc": min(row.created_at for row in rows).isoformat(),
            "last_observation_utc": max(row.created_at for row in rows).isoformat(),
        }
    if purge_metadata is not None:
        metadata["temporal_purge"] = purge_metadata
    return splits, metadata


def _build_atomic_clauses(
    train: Mapping[int, Sequence[_Row]], config: DiscoveryConfig
) -> tuple[list[AtomicClause], dict[str, Any]]:
    rows = [row for values in train.values() for row in values]
    clauses: list[AtomicClause] = []
    manifest: dict[str, Any] = {}
    grids = _threshold_grids(config)
    for feature in sorted(grids):
        operator, fixed_values = grids[feature]
        train_values = [row.features[feature] for row in rows if feature in row.features]
        quantile_values = (
            [
                _normalise_threshold(_quantile(train_values, q))
                for q in config.quantiles
            ]
            if train_values and feature not in PRECISION_FIXED_ONLY_FEATURES
            else []
        )
        fixed = [_normalise_threshold(value) for value in fixed_values]
        # In precision mode an unavailable feature must consume neither the
        # depth-1 quota nor deeper proposal slots.  The legacy profile keeps
        # its historical fixed-grid behaviour for compatibility.
        thresholds = (
            sorted(set(fixed + quantile_values))
            if train_values
            or config.selection_mode not in {"precision_first", "rare_precision"}
            else []
        )
        operators = [operator]
        if (
            config.selection_mode == "rare_precision"
            and config.allow_feature_ranges
            and feature not in PRECISION_FIXED_ONLY_FEATURES
        ):
            operators = ["<=", ">="]
        for effective_operator in operators:
            for threshold in thresholds:
                clauses.append(
                    AtomicClause(
                        feature,
                        effective_operator,
                        threshold,
                        _allow_alternate_operator=(
                            config.selection_mode == "rare_precision"
                        ),
                    )
                )
        manifest[feature] = {
            "operator": operator,
            "fixed_thresholds": fixed,
            "train_quantiles": [
                {"quantile": q, "threshold": value}
                for q, value in zip(config.quantiles, quantile_values)
            ],
            "train_value_count": len(train_values),
            "effective_thresholds": thresholds,
        }
        if config.selection_mode == "rare_precision":
            manifest[feature].update(
                {
                    "operators": operators,
                    "threshold_source_split": "train",
                    "effective_clauses": [
                        {
                            "operator": effective_operator,
                            "thresholds": thresholds,
                        }
                        for effective_operator in operators
                    ],
                }
            )
    clauses.sort(key=_clause_key)
    return clauses, manifest


def _clauses_are_compatible(clauses: Sequence[AtomicClause]) -> bool:
    """Reject redundant or impossible same-feature constraints.

    A rare-precision range may contain one lower and one upper bound for a
    feature.  More than one clause in the same direction is redundant, and a
    lower bound above its upper bound is contradictory.  Existing search
    modes never call this helper and therefore retain their exact historical
    proposal language.
    """

    by_feature: dict[str, dict[str, float]] = defaultdict(dict)
    for clause in clauses:
        feature_clauses = by_feature[clause.feature]
        if clause.operator in feature_clauses:
            return False
        feature_clauses[clause.operator] = float(clause.threshold)
    for feature_clauses in by_feature.values():
        lower = feature_clauses.get(">=")
        upper = feature_clauses.get("<=")
        if lower is not None and upper is not None and lower > upper:
            return False
    return True


def _is_interval_rule(clauses: Sequence[AtomicClause]) -> bool:
    operators_by_feature: dict[str, set[str]] = defaultdict(set)
    for clause in clauses:
        operators_by_feature[clause.feature].add(clause.operator)
    return any(operators == {">=", "<="} for operators in operators_by_feature.values())


def _interval_seed_candidates(
    atomic_clauses: Sequence[AtomicClause],
) -> dict[tuple[tuple[str, str, float], ...], tuple[AtomicClause, ...]]:
    """Build deterministic, non-contradictory two-clause interval seeds."""

    by_feature: dict[str, dict[str, list[AtomicClause]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for clause in atomic_clauses:
        by_feature[clause.feature][clause.operator].append(clause)
    seeds: dict[
        tuple[tuple[str, str, float], ...], tuple[AtomicClause, ...]
    ] = {}
    for feature in sorted(by_feature):
        lowers = sorted(by_feature[feature].get(">=", ()), key=_clause_key)
        uppers = sorted(by_feature[feature].get("<=", ()), key=_clause_key)
        for lower in lowers:
            for upper in uppers:
                combined = tuple(sorted((lower, upper), key=_clause_key))
                if not _clauses_are_compatible(combined):
                    continue
                seeds.setdefault(_rule_key(combined), combined)
    return seeds


def _chronological_validation_windows(
    fixtures: Mapping[int, Sequence[_Row]], window_count: int
) -> tuple[list[dict[int, Sequence[_Row]]], list[dict[str, Any]]]:
    """Split validation fixtures into contiguous, near-equal time windows."""

    ordered = sorted(
        fixtures,
        key=lambda fixture_id: (
            fixtures[fixture_id][0].created_at,
            fixture_id,
        ),
    )
    base, remainder = divmod(len(ordered), int(window_count))
    windows: list[dict[int, Sequence[_Row]]] = []
    metadata: list[dict[str, Any]] = []
    offset = 0
    for index in range(int(window_count)):
        size = base + (1 if index < remainder else 0)
        fixture_ids = ordered[offset : offset + size]
        offset += size
        window = {
            fixture_id: fixtures[fixture_id] for fixture_id in fixture_ids
        }
        windows.append(window)
        rows = [row for fixture_id in fixture_ids for row in fixtures[fixture_id]]
        metadata.append(
            {
                "index": index + 1,
                "fixture_count": len(fixture_ids),
                "observation_count": len(rows),
                "fixture_ids_sha256": _digest(fixture_ids),
                "first_observation_utc": (
                    min(row.created_at for row in rows).isoformat()
                    if rows
                    else None
                ),
                "last_observation_utc": (
                    max(row.created_at for row in rows).isoformat()
                    if rows
                    else None
                ),
            }
        )
    return windows, metadata


def _rare_precision_combined_score(metrics: Mapping[str, Any]) -> Optional[float]:
    """Blend uncertainty-aware and raw precision with fixed equal weights."""

    lower = metrics.get("wilson_lower")
    hit_rate = metrics.get("hit_rate")
    if lower is None or hit_rate is None:
        return None
    score = (
        RARE_PRECISION_WILSON_WEIGHT * float(lower)
        + (1.0 - RARE_PRECISION_WILSON_WEIGHT) * float(hit_rate)
    )
    return round(score, 9)


def _rare_validation_stability(
    aggregate_metrics: Mapping[str, Any],
    window_metrics: Sequence[Mapping[str, Any]],
    *,
    min_total_support: int,
    min_window_support: int,
) -> dict[str, Any]:
    hit_rates = [
        float(metrics["hit_rate"])
        for metrics in window_metrics
        if metrics.get("hit_rate") is not None
    ]
    wilson_lowers = [
        float(metrics["wilson_lower"])
        for metrics in window_metrics
        if metrics.get("wilson_lower") is not None
    ]
    combined_scores = [
        score
        for metrics in window_metrics
        if (score := _rare_precision_combined_score(metrics)) is not None
    ]
    supported_windows = sum(
        int(metrics.get("resolved") or 0) >= int(min_window_support)
        for metrics in window_metrics
    )
    required_windows = len(window_metrics)
    aggregate_support_passed = (
        int(aggregate_metrics.get("resolved") or 0) >= int(min_total_support)
    )
    support_gate_passed = bool(
        required_windows > 0
        and supported_windows == required_windows
        and aggregate_support_passed
    )
    return {
        "policy": "four_chronological_windows_fail_closed_v1",
        "selection_uses_holdout": False,
        "wilson_weight": RARE_PRECISION_WILSON_WEIGHT,
        "raw_precision_weight": 1.0 - RARE_PRECISION_WILSON_WEIGHT,
        "required_windows": required_windows,
        "supported_windows": supported_windows,
        "min_total_support": int(min_total_support),
        "min_window_support": int(min_window_support),
        "aggregate_support_gate_passed": aggregate_support_passed,
        "support_gate_passed": support_gate_passed,
        "aggregate_combined_score": _rare_precision_combined_score(
            aggregate_metrics
        ),
        "minimum_window_combined_score": (
            round(min(combined_scores), 9)
            if len(combined_scores) == required_windows and combined_scores
            else None
        ),
        "mean_window_combined_score": (
            round(sum(combined_scores) / len(combined_scores), 9)
            if len(combined_scores) == required_windows and combined_scores
            else None
        ),
        "minimum_window_hit_rate": (
            round(min(hit_rates), 9)
            if len(hit_rates) == required_windows and hit_rates
            else None
        ),
        "maximum_window_hit_rate": (
            round(max(hit_rates), 9)
            if len(hit_rates) == required_windows and hit_rates
            else None
        ),
        "minimum_window_wilson_lower": (
            round(min(wilson_lowers), 9)
            if len(wilson_lowers) == required_windows and wilson_lowers
            else None
        ),
        "windows": [
            {"index": index, **dict(metrics)}
            for index, metrics in enumerate(window_metrics, 1)
        ],
    }


class _FirstTriggerBitsetMatcher:
    """Exact first-trigger evaluator optimized for repeated train searches.

    Each atomic clause is cached as a Python integer bitset over observations
    flattened fixture-by-fixture.  Conjunctions are bitwise ANDs.  Once the
    first matching observation of a fixture is found, all bits through that
    fixture's end are cleared, preserving ``_first_triggers`` semantics while
    avoiding a Python loop over every observation for every proposal.
    """

    def __init__(self, fixtures: Mapping[int, Sequence[_Row]]) -> None:
        rows: list[_Row] = []
        fixture_end_by_position: list[int] = []
        for fixture_id in sorted(fixtures):
            fixture_rows = fixtures[fixture_id]
            end = len(rows) + len(fixture_rows)
            rows.extend(fixture_rows)
            fixture_end_by_position.extend([end] * len(fixture_rows))
        self.rows = tuple(rows)
        self.fixture_end_by_position = tuple(fixture_end_by_position)
        self._atom_bits: dict[tuple[str, str, float], int] = {}

    @property
    def cached_atom_count(self) -> int:
        return len(self._atom_bits)

    def _bits_for_clause(self, clause: AtomicClause) -> int:
        key = _clause_key(clause)
        cached = self._atom_bits.get(key)
        if cached is not None:
            return cached
        packed = bytearray((len(self.rows) + 7) // 8)
        for index, row in enumerate(self.rows):
            value = row.features.get(clause.feature)
            if value is None:
                continue
            # Use precisely the canonical matcher's comparisons (including
            # their behaviour for a non-finite value in an internal _Row).
            matched = (
                not value < clause.threshold
                if clause.operator == ">="
                else not value > clause.threshold
            )
            if matched:
                packed[index >> 3] |= 1 << (index & 7)
        bits = int.from_bytes(packed, byteorder="little")
        self._atom_bits[key] = bits
        return bits

    def first_triggers(self, clauses: Sequence[AtomicClause]) -> list[_Row]:
        if not self.rows:
            return []
        bits = (1 << len(self.rows)) - 1
        for clause in clauses:
            bits &= self._bits_for_clause(clause)
            if not bits:
                return []
        selected: list[_Row] = []
        while bits:
            first_bit = bits & -bits
            position = first_bit.bit_length() - 1
            selected.append(self.rows[position])
            fixture_end = self.fixture_end_by_position[position]
            bits &= -(1 << fixture_end)
        selected.sort(
            key=lambda row: (row.created_at, row.fixture_id, row.observation_id)
        )
        return selected


def _search_rare_precision_candidates(
    train: Mapping[int, Sequence[_Row]],
    validation: Mapping[int, Sequence[_Row]],
    config: DiscoveryConfig,
    atomic_clauses: Sequence[AtomicClause],
) -> tuple[
    list[tuple[AtomicClause, ...]],
    dict[str, Any],
    dict[str, dict[str, Any]],
]:
    """Search a hard-shadow shortlist for rare, stable precision rules.

    Selection is based only on train and four contiguous validation windows.
    Cadence is diagnostic only, no OR portfolio is built, and the holdout is
    not accepted by this function at all.
    """

    budget_used = 0
    budget_used_by_depth: Counter[int] = Counter()
    depth_budgets = {
        depth: int(value)
        for depth, value in enumerate(config.depth_evaluation_budgets, 1)
    }
    global_truncated_depths: set[int] = set()
    quota_truncated_depths: set[int] = set()
    evaluated: dict[
        tuple[tuple[str, str, float], ...],
        tuple[tuple[AtomicClause, ...], dict[str, Any]],
    ] = {}
    levels: list[dict[str, Any]] = []
    train_span_days = _evaluation_span_days(train)
    validation_span_days = _evaluation_span_days(validation)
    validation_windows, validation_window_metadata = (
        _chronological_validation_windows(
            validation,
            config.validation_window_count,
        )
    )
    min_window_support = max(
        1,
        int(
            math.ceil(
                float(config.min_validation_support)
                / float(config.validation_window_count)
            )
        ),
    )
    interval_seeds = _interval_seed_candidates(atomic_clauses)
    train_matcher = _FirstTriggerBitsetMatcher(train)

    def evaluate(clauses: tuple[AtomicClause, ...]) -> Optional[dict[str, Any]]:
        nonlocal budget_used
        key = _rule_key(clauses)
        cached = evaluated.get(key)
        if cached is not None:
            return cached[1]
        depth = len(clauses)
        if budget_used >= config.evaluation_budget:
            global_truncated_depths.add(depth)
            return None
        depth_budget = depth_budgets.get(depth)
        if depth_budget is not None and budget_used_by_depth[depth] >= depth_budget:
            quota_truncated_depths.add(depth)
            return None
        budget_used += 1
        budget_used_by_depth[depth] += 1
        metrics = _ranking_metrics(
            train_matcher.first_triggers(clauses),
            len(train),
            evaluation_span_days=train_span_days,
        )
        evaluated[key] = (clauses, metrics)
        return metrics

    def level_diagnostics(
        *, depth: int, proposals_available: int, retained: int
    ) -> dict[str, Any]:
        used = int(budget_used_by_depth[depth])
        return {
            "size": depth,
            "proposals_available": int(proposals_available),
            "evaluation_budget": depth_budgets.get(depth),
            "evaluations_used": used,
            "truncated": bool(used < proposals_available),
            "quota_truncated": depth in quota_truncated_depths,
            "global_budget_truncated": depth in global_truncated_depths,
            "retained": int(retained),
        }

    train_raw_reserve_slots = min(
        int(config.beam_width),
        max(1, int(math.ceil(float(config.beam_width) * 0.25))),
    )
    train_raw_reserved_by_depth: dict[int, int] = {}

    def diverse_beam(
        ranked: Sequence[tuple[AtomicClause, ...]],
    ) -> list[tuple[AtomicClause, ...]]:
        selected: list[tuple[AtomicClause, ...]] = []
        signature_counts: Counter[tuple[tuple[int, str], ...]] = Counter()
        selected_keys: set[tuple[tuple[str, str, float], ...]] = set()

        def raw_train_key(clauses: tuple[AtomicClause, ...]) -> tuple[Any, ...]:
            metrics = evaluated[_rule_key(clauses)][1]
            hit_rate = metrics.get("hit_rate")
            lower = metrics.get("wilson_lower")
            return (
                -(float(hit_rate) if hit_rate is not None else -1.0),
                -(float(lower) if lower is not None else -1.0),
                -int(metrics.get("resolved") or 0),
                len(clauses),
                _rule_key(clauses),
            )

        def admit(clauses: tuple[AtomicClause, ...]) -> bool:
            key = _rule_key(clauses)
            if key in selected_keys:
                return False
            signature = tuple(
                (row.fixture_id, row.observation_id)
                for row in train_matcher.first_triggers(clauses)
            )
            if signature_counts[signature] >= TRAIN_SIGNATURE_MAX_REPRESENTATIVES:
                return False
            signature_counts[signature] += 1
            selected_keys.add(key)
            selected.append(clauses)
            return True

        raw_admitted = 0
        for clauses in sorted(ranked, key=raw_train_key):
            if admit(clauses):
                raw_admitted += 1
            if raw_admitted >= train_raw_reserve_slots:
                break
        if config.extended_features and not config.market_only:
            nonlinear_admitted = 0
            for clauses in ranked:
                if any(c.feature.startswith("nonlinear.v1.") for c in clauses) and admit(clauses):
                    nonlinear_admitted += 1
                if nonlinear_admitted >= max(1, config.beam_width // 4) or len(selected) >= config.beam_width:
                    break
        for clauses in ranked:
            if len(selected) >= int(config.beam_width):
                break
            admit(clauses)
        depth = len(selected[0]) if selected else 0
        if depth:
            train_raw_reserved_by_depth[depth] = raw_admitted
        return selected

    current: list[tuple[AtomicClause, ...]] = []
    for clause in atomic_clauses:
        metrics = evaluate((clause,))
        if metrics is None:
            break
        if (int(metrics["resolved"]) >= config.min_train_support
            and (not config.market_only or has_market_dependency(clause.feature))):
            current.append((clause,))
    current.sort(
        key=lambda clauses: _rank_key(
            evaluated[_rule_key(clauses)][1], clauses
        )
    )
    current = diverse_beam(current)
    levels.append(
        level_diagnostics(
            depth=1,
            proposals_available=len(atomic_clauses),
            retained=len(current),
        )
    )
    pool = list(current) if config.min_conjunction_size <= 1 else []

    for size in range(2, config.max_conjunction_size + 1):
        proposals: dict[
            tuple[tuple[str, str, float], ...], tuple[AtomicClause, ...]
        ] = {}
        for existing in current:
            for atomic in atomic_clauses:
                combined = tuple(sorted((*existing, atomic), key=_clause_key))
                if len({_clause_key(clause) for clause in combined}) != len(combined):
                    continue
                if not _clauses_are_compatible(combined):
                    continue
                proposals.setdefault(_rule_key(combined), combined)
        if size == 2:
            # Range seeds do not depend on their individual bounds surviving
            # the depth-1 beam.  This keeps narrow train-derived intervals in
            # the search language without inflating every deeper beam level.
            for key, combined in interval_seeds.items():
                if config.market_only and not any(has_market_dependency(c.feature) for c in combined):
                    continue
                proposals.setdefault(key, combined)

        retained: list[tuple[AtomicClause, ...]] = []
        proposal_keys = list(proposals)
        proposal_keys.sort(
            key=lambda key: (
                0 if size == 2 and key in interval_seeds else 1,
                _digest(key),
                key,
            )
        )
        for key in proposal_keys:
            metrics = evaluate(proposals[key])
            if metrics is None:
                break
            if int(metrics["resolved"]) >= config.min_train_support:
                retained.append(proposals[key])
        retained.sort(
            key=lambda clauses: _rank_key(
                evaluated[_rule_key(clauses)][1], clauses
            )
        )
        current = diverse_beam(retained)
        if size >= config.min_conjunction_size:
            pool.extend(current)
        levels.append(
            level_diagnostics(
                depth=size,
                proposals_available=len(proposals),
                retained=len(current),
            )
        )
        if not current:
            break

    validation_rows: dict[str, dict[str, Any]] = {}
    validation_stability: dict[str, dict[str, Any]] = {}
    aggregate_supported = 0
    supported: list[tuple[AtomicClause, ...]] = []
    for clauses in pool:
        candidate_id = _candidate_id(clauses, config=config)
        aggregate = _ranking_metrics(
            _first_triggers(validation, clauses),
            len(validation),
            evaluation_span_days=validation_span_days,
        )
        validation_rows[candidate_id] = aggregate
        if int(aggregate["resolved"]) >= config.min_validation_support:
            aggregate_supported += 1
        window_metrics = [
            _ranking_metrics(
                _first_triggers(window, clauses),
                len(window),
                evaluation_span_days=_evaluation_span_days(window),
            )
            for window in validation_windows
        ]
        stability = _rare_validation_stability(
            aggregate,
            window_metrics,
            min_total_support=config.min_validation_support,
            min_window_support=min_window_support,
        )
        validation_stability[candidate_id] = stability
        if stability["support_gate_passed"]:
            supported.append(clauses)

    def selection_key(clauses: tuple[AtomicClause, ...]) -> tuple[Any, ...]:
        candidate_id = _candidate_id(clauses, config=config)
        aggregate = validation_rows[candidate_id]
        stability = validation_stability[candidate_id]

        def descending(value: Any) -> float:
            return -(float(value) if value is not None else -1.0)

        return (
            descending(stability.get("minimum_window_combined_score")),
            descending(stability.get("minimum_window_hit_rate")),
            descending(stability.get("mean_window_combined_score")),
            descending(stability.get("aggregate_combined_score")),
            descending(aggregate.get("hit_rate")),
            descending(aggregate.get("wilson_lower")),
            -int(aggregate.get("resolved") or 0),
            _rank_key(evaluated[_rule_key(clauses)][1], clauses),
            candidate_id,
        )

    supported.sort(key=selection_key)
    selectable: list[tuple[AtomicClause, ...]] = []
    seen_trigger_signatures: set[tuple[tuple[int, str], ...]] = set()
    for clauses in supported:
        triggers = _first_triggers(validation, clauses)
        signature = tuple((row.fixture_id, row.observation_id) for row in triggers)
        if signature in seen_trigger_signatures:
            continue
        seen_trigger_signatures.add(signature)
        selectable.append(clauses)
    selectable.sort(key=selection_key)

    def raw_precision_key(
        clauses: tuple[AtomicClause, ...],
    ) -> tuple[Any, ...]:
        candidate_id = _candidate_id(clauses, config=config)
        aggregate = validation_rows[candidate_id]
        stability = validation_stability[candidate_id]

        def descending(value: Any) -> float:
            return -(float(value) if value is not None else -1.0)

        # The reserve still ranks on the weakest chronological window first;
        # it merely gives raw hit rate a guaranteed lane alongside Wilson.
        return (
            descending(stability.get("minimum_window_hit_rate")),
            descending(aggregate.get("hit_rate")),
            descending(stability.get("minimum_window_wilson_lower")),
            descending(stability.get("minimum_window_combined_score")),
            -int(aggregate.get("resolved") or 0),
            len(clauses),
            _rule_key(clauses),
            candidate_id,
        )

    raw_reserve_slots = min(
        int(config.top_n),
        max(1, int(math.ceil(float(config.top_n) * 0.25))),
    )
    raw_ranked = sorted(selectable, key=raw_precision_key)
    selected: list[tuple[AtomicClause, ...]] = []
    selection_lanes: dict[str, str] = {}
    for clauses in raw_ranked[:raw_reserve_slots]:
        candidate_id = _candidate_id(clauses, config=config)
        selected.append(clauses)
        selection_lanes[candidate_id] = "raw_precision_reserve"
    if config.extended_features and not config.market_only:
        added = 0
        for clauses in selectable:
            candidate_id = _candidate_id(clauses, config=config)
            if candidate_id in selection_lanes or not any(c.feature.startswith("nonlinear.v1.") for c in clauses):
                continue
            if len(selected) >= config.top_n or added >= max(1, config.top_n // 4):
                break
            selected.append(clauses)
            selection_lanes[candidate_id] = "nonlinear_reserve"
            added += 1
    for clauses in selectable:
        if len(selected) >= int(config.top_n):
            break
        candidate_id = _candidate_id(clauses, config=config)
        if candidate_id in selection_lanes:
            continue
        selected.append(clauses)
        selection_lanes[candidate_id] = "wilson_stability"

    offspring_lineage = {}
    refinement_diagnostics = None
    if config.error_refinement:
        from .refinement import refine_candidates
        offspring, refinement_diagnostics = refine_candidates(
            train, validation, config, atomic_clauses, pool, excluded_rules=selected
        )
        for child in offspring:
            clauses = child["rule"]
            cid = _candidate_id(clauses, config=config)
            selected.append(clauses)
            selection_lanes[cid] = "error_refinement"
            validation_stability[cid] = child["stability"]
            offspring_lineage[cid] = child["lineage"]

    interval_seed_evaluated = sum(key in evaluated for key in interval_seeds)
    interval_seed_supported = sum(
        key in evaluated
        and int(evaluated[key][1].get("resolved") or 0)
        >= int(config.min_train_support)
        for key in interval_seeds
    )
    diagnostics = {
        "evaluation_budget": config.evaluation_budget,
        "evaluations_used": budget_used,
        "budget_exhausted": bool(global_truncated_depths),
        "global_budget_truncated_depths": sorted(global_truncated_depths),
        "depth_budget_truncated_depths": sorted(quota_truncated_depths),
        "depth_evaluation_budgets": (
            list(config.depth_evaluation_budgets)
            if config.depth_evaluation_budgets
            else None
        ),
        "requested_depth_reached": bool(
            levels and levels[-1]["size"] == config.max_conjunction_size
        ),
        "atomic_clause_count": len(atomic_clauses),
        "train_signature_max_representatives": (
            TRAIN_SIGNATURE_MAX_REPRESENTATIVES
        ),
        "levels": levels,
        "eligible_before_validation": len(pool),
        "validation_aggregate_supported": aggregate_supported,
        "validation_supported": len(supported),
        "validation_unique": len(selectable),
        "selected": len(selected),
        "rare_precision": {
            "extended_features": config.extended_features,
            "market_only": config.market_only,
            "nonlinear_selected": sum(any(c.feature.startswith("nonlinear.v1.") for c in rule) for rule in selected),
            "market_selected": sum(any(has_market_dependency(c.feature) for c in rule) for rule in selected),
            "selection_policy": "wilson_raw_precision_temporal_stability_v1",
            "selection_uses_holdout": False,
            "cadence_gate_applied": False,
            "or_portfolio_enabled": False,
            "threshold_data_split": "train",
            "validation_window_count": int(config.validation_window_count),
            "required_supported_windows": int(config.validation_window_count),
            "min_total_validation_support": int(config.min_validation_support),
            "min_validation_support_per_window": min_window_support,
            "validation_windows": validation_window_metadata,
            "interval_seed_pairs_available": len(interval_seeds),
            "interval_seed_pairs_evaluated": interval_seed_evaluated,
            "interval_seed_pairs_train_supported": interval_seed_supported,
            "interval_rules_in_candidate_pool": sum(
                _is_interval_rule(clauses) for clauses in pool
            ),
            "interval_rules_selected": sum(
                _is_interval_rule(clauses) for clauses in selected
            ),
            "train_raw_precision_reserved_slots_per_depth": (
                train_raw_reserve_slots
            ),
            "train_raw_precision_reserved_by_depth": dict(
                sorted(train_raw_reserved_by_depth.items())
            ),
            "train_bitset_observation_count": len(train_matcher.rows),
            "train_bitset_cached_atom_count": train_matcher.cached_atom_count,
            "raw_precision_reserved_slots": raw_reserve_slots,
            "raw_precision_reserved_selected": sum(
                lane == "raw_precision_reserve"
                for lane in selection_lanes.values()
            ),
            "selection_lanes": dict(sorted(selection_lanes.items())),
        },
    }
    train_metrics = {
        _candidate_id(clauses, config=config): _metrics(
            _first_triggers(train, clauses),
            len(train),
            evaluation_span_days=train_span_days,
        )
        for clauses in selected
    }
    if refinement_diagnostics is not None:
        diagnostics["error_refinement"] = refinement_diagnostics
    validation_metrics = {
        _candidate_id(clauses, config=config): _metrics(
            _first_triggers(validation, clauses),
            len(validation),
            evaluation_span_days=validation_span_days,
        )
        for clauses in selected
    }
    return selected, diagnostics, {
        "train": train_metrics,
        "validation": validation_metrics,
        "validation_stability": {
            _candidate_id(clauses, config=config): validation_stability[
                _candidate_id(clauses, config=config)
            ]
            for clauses in selected
        },
        "selection_lanes": selection_lanes,
        "offspring_lineage": offspring_lineage,
    }


def _search_candidates(
    train: Mapping[int, Sequence[_Row]],
    validation: Mapping[int, Sequence[_Row]],
    config: DiscoveryConfig,
    atomic_clauses: Sequence[AtomicClause],
) -> tuple[list[tuple[AtomicClause, ...]], dict[str, Any], dict[str, dict[str, Any]]]:
    if config.selection_mode == "rare_precision":
        return _search_rare_precision_candidates(
            train,
            validation,
            config,
            atomic_clauses,
        )

    budget_used = 0
    budget_used_by_depth: Counter[int] = Counter()
    depth_budgets = {
        depth: int(value)
        for depth, value in enumerate(config.depth_evaluation_budgets, 1)
    }
    global_truncated_depths: set[int] = set()
    quota_truncated_depths: set[int] = set()
    evaluated: dict[tuple[tuple[str, str, float], ...], tuple[tuple[AtomicClause, ...], dict[str, Any]]] = {}
    levels: list[dict[str, Any]] = []
    train_span_days = _evaluation_span_days(train)
    validation_span_days = _evaluation_span_days(validation)

    def evaluate(clauses: tuple[AtomicClause, ...]) -> Optional[dict[str, Any]]:
        nonlocal budget_used
        key = _rule_key(clauses)
        cached = evaluated.get(key)
        if cached is not None:
            return cached[1]
        depth = len(clauses)
        if budget_used >= config.evaluation_budget:
            global_truncated_depths.add(depth)
            return None
        depth_budget = depth_budgets.get(depth)
        if (
            depth_budget is not None
            and budget_used_by_depth[depth] >= depth_budget
        ):
            quota_truncated_depths.add(depth)
            return None
        budget_used += 1
        budget_used_by_depth[depth] += 1
        triggers = _first_triggers(train, clauses)
        metrics = _ranking_metrics(
            triggers,
            len(train),
            evaluation_span_days=train_span_days,
        )
        evaluated[key] = (clauses, metrics)
        return metrics

    def level_diagnostics(
        *, depth: int, proposals_available: int, retained: int
    ) -> dict[str, Any]:
        used = int(budget_used_by_depth[depth])
        return {
            "size": depth,
            "proposals_available": int(proposals_available),
            "evaluation_budget": depth_budgets.get(depth),
            "evaluations_used": used,
            "truncated": bool(used < proposals_available),
            "quota_truncated": depth in quota_truncated_depths,
            "global_budget_truncated": depth in global_truncated_depths,
            "retained": int(retained),
        }

    def diverse_beam(
        ranked: Sequence[tuple[AtomicClause, ...]],
    ) -> list[tuple[AtomicClause, ...]]:
        """Keep deterministic train-trigger diversity inside the beam.

        Composite shortcuts and their expanded primitive equivalents often
        select exactly the same historical observations.  Retaining all such
        aliases would crowd genuinely different refinements out of deeper
        levels.  This affects exploration only; validation and holdout remain
        unopened here.
        """

        selected: list[tuple[AtomicClause, ...]] = []
        signature_counts: Counter[tuple[tuple[int, str], ...]] = Counter()
        for clauses in ranked:
            triggers = _first_triggers(train, clauses)
            signature = tuple(
                (row.fixture_id, row.observation_id) for row in triggers
            )
            # Equal train triggers do not prove future equivalence.  Keep a
            # small number of structurally distinct representatives so, for
            # example, a stricter threshold can still win on validation.
            if (
                signature_counts[signature]
                >= TRAIN_SIGNATURE_MAX_REPRESENTATIVES
            ):
                continue
            signature_counts[signature] += 1
            selected.append(clauses)
            if len(selected) >= int(config.beam_width):
                break
        return selected

    current: list[tuple[AtomicClause, ...]] = []
    for clause in atomic_clauses:
        metrics = evaluate((clause,))
        if metrics is None:
            break
        if int(metrics["resolved"]) >= config.min_train_support:
            current.append((clause,))
    current.sort(key=lambda clauses: _rank_key(evaluated[_rule_key(clauses)][1], clauses))
    current = diverse_beam(current)
    levels.append(
        level_diagnostics(
            depth=1,
            proposals_available=len(atomic_clauses),
            retained=len(current),
        )
    )
    pool = list(current) if config.min_conjunction_size <= 1 else []

    for size in range(2, config.max_conjunction_size + 1):
        proposals: dict[tuple[tuple[str, str, float], ...], tuple[AtomicClause, ...]] = {}
        for existing in current:
            used_features = {clause.feature for clause in existing}
            for atomic in atomic_clauses:
                if atomic.feature in used_features:
                    continue
                combined = tuple(sorted((*existing, atomic), key=_clause_key))
                key = _rule_key(combined)
                proposals.setdefault(key, combined)
        retained: list[tuple[AtomicClause, ...]] = []
        proposal_keys = list(proposals)
        if depth_budgets:
            # A stable hash avoids the old feature-name bias when a planned
            # depth quota samples only part of a very large proposal set.
            proposal_keys.sort(key=lambda key: (_digest(key), key))
        else:
            proposal_keys.sort()
        for key in proposal_keys:
            metrics = evaluate(proposals[key])
            if metrics is None:
                break
            if int(metrics["resolved"]) >= config.min_train_support:
                retained.append(proposals[key])
        retained.sort(key=lambda clauses: _rank_key(evaluated[_rule_key(clauses)][1], clauses))
        current = diverse_beam(retained)
        if size >= config.min_conjunction_size:
            pool.extend(current)
        levels.append(
            level_diagnostics(
                depth=size,
                proposals_available=len(proposals),
                retained=len(current),
            )
        )
        if not current:
            break

    # Validation is used for model/rule selection.  Holdout is intentionally
    # absent from this function's arguments.
    validation_rows: dict[str, dict[str, Any]] = {}
    supported: list[tuple[AtomicClause, ...]] = []
    for clauses in pool:
        triggers = _first_triggers(validation, clauses)
        metrics = _ranking_metrics(
            triggers,
            len(validation),
            evaluation_span_days=validation_span_days,
        )
        candidate_id = _candidate_id(clauses, config=config)
        validation_rows[candidate_id] = metrics
        if int(metrics["resolved"]) < config.min_validation_support:
            continue
        supported.append(clauses)

    def selection_key(clauses: tuple[AtomicClause, ...]) -> tuple[Any, ...]:
        metrics = validation_rows[_candidate_id(clauses, config=config)]
        cadence = float(metrics.get("calendar_triggers_per_week") or 0.0)
        cadence_gate = (
            config.selection_mode != "precision_first"
            or cadence >= float(config.min_signals_per_week)
        )
        return (
            0 if cadence_gate else 1,
            _rank_key(metrics, clauses),
            _rank_key(evaluated[_rule_key(clauses)][1], clauses),
            _candidate_id(clauses, config=config),
        )

    supported.sort(key=selection_key)
    selectable: list[tuple[AtomicClause, ...]] = []
    seen_trigger_signatures: set[tuple[tuple[int, str], ...]] = set()
    for clauses in supported:
        triggers = _first_triggers(validation, clauses)
        signature = tuple((row.fixture_id, row.observation_id) for row in triggers)
        if signature in seen_trigger_signatures:
            continue
        seen_trigger_signatures.add(signature)
        selectable.append(clauses)

    selectable.sort(key=selection_key)
    selected = selectable[: config.top_n]
    diagnostics = {
        "evaluation_budget": config.evaluation_budget,
        "evaluations_used": budget_used,
        "budget_exhausted": bool(global_truncated_depths),
        "global_budget_truncated_depths": sorted(global_truncated_depths),
        "depth_budget_truncated_depths": sorted(quota_truncated_depths),
        "depth_evaluation_budgets": (
            list(config.depth_evaluation_budgets)
            if config.depth_evaluation_budgets
            else None
        ),
        "requested_depth_reached": bool(
            levels and levels[-1]["size"] == config.max_conjunction_size
        ),
        "atomic_clause_count": len(atomic_clauses),
        "train_signature_max_representatives": (
            TRAIN_SIGNATURE_MAX_REPRESENTATIVES
        ),
        "levels": levels,
        "eligible_before_validation": len(pool),
        "validation_supported": len(supported),
        "validation_unique": len(selectable),
        "selected": len(selected),
    }
    # Only finalists receive the complete auditable trigger/period breakdown.
    # Recomputing at most ``top_n`` rules is much cheaper than retaining full
    # metrics for every explored proposal.
    train_metrics = {
        _candidate_id(clauses, config=config): _metrics(
            _first_triggers(train, clauses),
            len(train),
            evaluation_span_days=train_span_days,
        )
        for clauses in selected
    }
    validation_metrics = {
        _candidate_id(clauses, config=config): _metrics(
            _first_triggers(validation, clauses),
            len(validation),
            evaluation_span_days=validation_span_days,
        )
        for clauses in selected
    }
    return selected, diagnostics, {
        "train": train_metrics,
        "validation": validation_metrics,
    }


def _merge_first_triggers(groups: Sequence[Sequence[_Row]]) -> list[_Row]:
    """Union rule triggers by fixture, keeping the earliest causal trigger."""

    by_fixture: dict[int, _Row] = {}
    for rows in groups:
        for row in rows:
            previous = by_fixture.get(row.fixture_id)
            if previous is None or (
                row.created_at,
                row.minute,
                row.observation_id,
            ) < (
                previous.created_at,
                previous.minute,
                previous.observation_id,
            ):
                by_fixture[row.fixture_id] = row
    return sorted(
        by_fixture.values(),
        key=lambda row: (row.created_at, row.fixture_id, row.observation_id),
    )


def _portfolio_rank_key(
    metrics: Mapping[str, Any],
    member_ids: Sequence[str],
    config: DiscoveryConfig,
) -> tuple[Any, ...]:
    cadence = float(metrics.get("calendar_triggers_per_week") or 0.0)
    lower = metrics.get("wilson_lower")
    hit_rate = metrics.get("hit_rate")
    minimum = float(config.min_signals_per_week)
    preferred = float(config.preferred_signals_per_week or minimum)
    maximum = float(config.max_signals_per_week)
    frequency_gate = cadence >= minimum
    in_preferred_band = frequency_gate and (
        maximum <= 0.0 or cadence <= maximum
    )
    return (
        0 if frequency_gate else 1,
        -(float(lower) if lower is not None else -1.0),
        -(float(hit_rate) if hit_rate is not None else -1.0),
        0 if in_preferred_band else 1,
        abs(cadence - preferred),
        len(member_ids),
        tuple(sorted(member_ids)),
    )


def _select_precision_portfolio(
    candidates: Sequence[tuple[AtomicClause, ...]],
    validation: Mapping[int, Sequence[_Row]],
    config: DiscoveryConfig,
) -> dict[str, Any]:
    """Freeze a validation-only OR portfolio before holdout is opened."""

    if not candidates:
        return {
            "selected": None,
            "best_available": None,
            "evaluated_subsets": 0,
        }
    exposure_days = _evaluation_span_days(validation)
    candidate_by_id = {
        _candidate_id(clauses, config=config): clauses for clauses in candidates
    }
    trigger_rows = {
        candidate_id: _first_triggers(validation, clauses)
        for candidate_id, clauses in candidate_by_id.items()
    }
    candidate_ids = tuple(sorted(candidate_by_id))
    evaluated: dict[tuple[str, ...], tuple[list[_Row], dict[str, Any]]] = {}

    def evaluate(member_ids: tuple[str, ...]) -> tuple[list[_Row], dict[str, Any]]:
        cached = evaluated.get(member_ids)
        if cached is not None:
            return cached
        rows = _merge_first_triggers(
            [trigger_rows[candidate_id] for candidate_id in member_ids]
        )
        metrics = _ranking_metrics(
            rows,
            len(validation),
            evaluation_span_days=exposure_days,
        )
        evaluated[member_ids] = (rows, metrics)
        return rows, metrics

    current = [(candidate_id,) for candidate_id in candidate_ids]
    all_subsets: list[tuple[str, ...]] = []
    max_rules = min(int(config.portfolio_max_rules), len(candidate_ids))
    for size in range(1, max_rules + 1):
        if size > 1:
            proposals = {
                tuple(sorted((*members, candidate_id)))
                for members in current
                for candidate_id in candidate_ids
                if candidate_id not in members
            }
            current = sorted(
                proposals,
                key=lambda members: _portfolio_rank_key(
                    evaluate(members)[1], members, config
                ),
            )[: int(config.portfolio_beam_width)]
        else:
            current = sorted(
                current,
                key=lambda members: _portfolio_rank_key(
                    evaluate(members)[1], members, config
                ),
            )[: int(config.portfolio_beam_width)]
        if not current:
            break
        all_subsets.extend(current)

    ranked = sorted(
        set(all_subsets),
        key=lambda members: _portfolio_rank_key(
            evaluate(members)[1], members, config
        ),
    )
    minimum = float(config.min_signals_per_week)
    feasible = [
        members
        for members in ranked
        if float(
            evaluate(members)[1].get("calendar_triggers_per_week") or 0.0
        )
        >= minimum
    ]

    def payload(member_ids: tuple[str, ...]) -> dict[str, Any]:
        metrics = evaluate(member_ids)[1]
        cadence = float(metrics.get("calendar_triggers_per_week") or 0.0)
        maximum = float(config.max_signals_per_week)
        preferred = float(config.preferred_signals_per_week or minimum)
        identity = {
            "member_candidate_ids": list(member_ids),
            "trigger_policy": "earliest_first_trigger_per_fixture_any_rule_v1",
        }
        return {
            "portfolio_id": "wide-portfolio-" + _digest(identity)[:20],
            **identity,
            "selection_split": "validation",
            "frequency_gate_passed": cadence >= minimum,
            "preferred_frequency_band_passed": bool(
                cadence >= minimum and (maximum <= 0.0 or cadence <= maximum)
            ),
            "distance_from_preferred_signals_per_week": round(
                abs(cadence - preferred), 9
            ),
            "validation": metrics,
        }

    return {
        "selected": payload(feasible[0]) if feasible else None,
        "best_available": payload(ranked[0]) if ranked and not feasible else None,
        "evaluated_subsets": len(evaluated),
        "candidate_count": len(candidate_ids),
        "portfolio_beam_width": int(config.portfolio_beam_width),
        "portfolio_max_rules": max_rules,
    }


def _portfolio_metrics_for_split(
    member_ids: Sequence[str],
    clauses_by_id: Mapping[str, tuple[AtomicClause, ...]],
    fixtures: Mapping[int, Sequence[_Row]],
) -> dict[str, Any]:
    rows = _merge_first_triggers(
        [
            _first_triggers(fixtures, clauses_by_id[candidate_id])
            for candidate_id in member_ids
        ]
    )
    return _metrics(
        rows,
        len(fixtures),
        evaluation_span_days=_evaluation_span_days(fixtures),
    )


def discover_rules(
    joined_observations: Iterable[Mapping[str, Any]],
    *,
    config: DiscoveryConfig = DiscoveryConfig(),
) -> dict[str, Any]:
    """Discover and honestly evaluate rules over the entire eligible universe."""

    fixture_rows: dict[int, list[_Row]] = defaultdict(list)
    skipped: Counter[str] = Counter()
    seen_observations: set[str] = set()
    for record in joined_observations:
        row, reason = _eligible_row(record, config)
        if row is None:
            skipped[reason] += 1
            continue
        if row.observation_id in seen_observations:
            skipped["duplicate_observation_id"] += 1
            continue
        seen_observations.add(row.observation_id)
        fixture_rows[row.fixture_id].append(row)
    for rows in fixture_rows.values():
        rows.sort(key=lambda row: (row.created_at, row.minute, row.observation_id))

    splits, split_metadata = _split_fixtures(fixture_rows, config)
    atomic_clauses, grid_manifest = _build_atomic_clauses(splits["train"], config)
    selected, search_diagnostics, selection_metrics = _search_candidates(
        splits["train"], splits["validation"], config, atomic_clauses
    )
    portfolio_search = (
        _select_precision_portfolio(
            selected,
            splits["validation"],
            config,
        )
        if config.selection_mode == "precision_first"
        else None
    )
    if portfolio_search is not None:
        search_diagnostics["portfolio"] = {
            key: value
            for key, value in portfolio_search.items()
            if key not in {"selected", "best_available"}
        }

    def portfolio_manifest(value: Any) -> Optional[dict[str, Any]]:
        if not isinstance(value, Mapping):
            return None
        return {
            key: value.get(key)
            for key in (
                "portfolio_id",
                "member_candidate_ids",
                "trigger_policy",
                "selection_split",
                "frequency_gate_passed",
                "preferred_frequency_band_passed",
                "distance_from_preferred_signals_per_week",
            )
        }

    selected_portfolio_manifest = portfolio_manifest(
        (portfolio_search or {}).get("selected")
    )
    best_available_portfolio_manifest = portfolio_manifest(
        (portfolio_search or {}).get("best_available")
    )

    # The holdout is first evaluated here, after candidate IDs and ranking have
    # been frozen by discovery+validation.
    holdout_span_days = _evaluation_span_days(splits["holdout"])
    holdout_metrics: dict[str, dict[str, Any]] = {}
    for clauses in selected:
        candidate_id = _candidate_id(clauses, config=config)
        triggers = _first_triggers(splits["holdout"], clauses)
        holdout_metrics[candidate_id] = _metrics(
            triggers,
            len(splits["holdout"]),
            evaluation_span_days=holdout_span_days,
        )

    p_values = {
        candidate_id: _binomial_upper_tail(
            int(metrics["wins"]), int(metrics["resolved"]), config.null_hit_rate
        )
        for candidate_id, metrics in holdout_metrics.items()
    }
    inference = _holm(p_values, config.family_alpha)

    manifests = []
    results = []
    train_fixture_ids = split_metadata["train"]["fixture_ids"]
    validation_fixture_ids = split_metadata["validation"]["fixture_ids"]
    for rank, clauses in enumerate(selected, 1):
        candidate_id = _candidate_id(clauses, config=config)
        manifest_body = {
            "schema_version": DISCOVERY_SCHEMA_VERSION,
            "manifest_version": RULE_MANIFEST_VERSION,
            "candidate_id": candidate_id,
            "rank_at_discovery": rank,
            "engine_version": _discovery_engine_version(config),
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "threshold_grid_version": _atomic_grid_version(config),
            "universe": {
                "stages": ["decision_pipeline", "wide_monitor"],
                "minute_min": config.min_minute,
                "minute_max": config.max_minute,
                "current_filter_policy": "ignored",
            },
            "trigger_policy": "first_matching_observation_per_fixture",
            "clauses": [clause.as_dict() for clause in sorted(clauses, key=_clause_key)],
            "selection_data": {
                "train_fixture_ids_sha256": _digest(train_fixture_ids),
                "validation_fixture_ids_sha256": _digest(validation_fixture_ids),
                "train_fixture_count": len(train_fixture_ids),
                "validation_fixture_count": len(validation_fixture_ids),
                "train_span": [
                    split_metadata["train"]["first_observation_utc"],
                    split_metadata["train"]["last_observation_utc"],
                ],
                "validation_span": [
                    split_metadata["validation"]["first_observation_utc"],
                    split_metadata["validation"]["last_observation_utc"],
                ],
            },
            "production_applied": False,
            "state": "RESEARCH",
        }
        lineage = selection_metrics.get("offspring_lineage", {}).get(candidate_id)
        if lineage is not None:
            manifest_body["refinement_lineage"] = lineage
        manifest = {**manifest_body, "manifest_sha256": _digest(manifest_body)}
        manifests.append(manifest)
        holdout = holdout_metrics[candidate_id]
        result_row = {
            "candidate_id": candidate_id,
            "rank_at_discovery": rank,
            "train": selection_metrics["train"][candidate_id],
            "validation": selection_metrics["validation"][candidate_id],
            "holdout": holdout,
            "holdout_support_gate_passed": (
                int(holdout["resolved"]) >= config.min_holdout_support
            ),
            "frequency_gates": {
                split_name: bool(
                    config.selection_mode != "precision_first"
                    or float(
                        split_metrics.get("calendar_triggers_per_week")
                        or 0.0
                    )
                    >= float(config.min_signals_per_week)
                )
                for split_name, split_metrics in (
                    ("train", selection_metrics["train"][candidate_id]),
                    (
                        "validation",
                        selection_metrics["validation"][candidate_id],
                    ),
                    ("holdout", holdout),
                )
            },
            "statistical_test": {
                "alternative": f"hit_rate > {config.null_hit_rate}",
                "family_alpha": config.family_alpha,
                "family_size": len(selected),
                **inference.get(candidate_id, {}),
            },
        }
        if config.selection_mode == "rare_precision":
            result_row.update(
                {
                    "selection_lane": selection_metrics["selection_lanes"][
                        candidate_id
                    ],
                    "validation_stability": selection_metrics[
                        "validation_stability"
                    ][candidate_id],
                    "frequency_gate_applied": False,
                }
            )
        if lineage is not None:
            result_row["refinement_lineage"] = lineage
        results.append(result_row)

    as_of = max(
        (row.created_at for rows in fixture_rows.values() for row in rows),
        default=None,
    )
    config_payload = asdict(config)
    config_payload["quantiles"] = list(config.quantiles)
    config_payload["depth_evaluation_budgets"] = list(
        config.depth_evaluation_budgets
    )
    common = {
        "schema_version": DISCOVERY_SCHEMA_VERSION,
        "engine_version": _discovery_engine_version(config),
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "threshold_grid_version": _atomic_grid_version(config),
        "dataset_as_of_utc": as_of.isoformat() if as_of else None,
        "config": config_payload,
        "split_policy": (
            PURGED_SPLIT_POLICY
            if config.temporal_purge
            else "chronological_fixture_group_v1"
        ),
        "splits": split_metadata,
        "input": {
            "eligible_observations": len(seen_observations),
            "eligible_fixtures": len(fixture_rows),
            "skipped": dict(sorted(skipped.items())),
        },
        "threshold_manifest": grid_manifest,
        "search": search_diagnostics,
    }
    manifest_artifact_body = {
        **common,
        "artifact_type": "wide_rule_candidate_manifests",
        "candidates": manifests,
        "selected_portfolio": selected_portfolio_manifest,
        "best_available_portfolio": best_available_portfolio_manifest,
    }
    manifest_artifact = {
        **manifest_artifact_body,
        "artifact_sha256": _digest(manifest_artifact_body),
    }
    clauses_by_id = {
        _candidate_id(clauses, config=config): clauses for clauses in selected
    }

    def portfolio_results(value: Any) -> Optional[dict[str, Any]]:
        manifest = portfolio_manifest(value)
        if manifest is None:
            return None
        member_ids = manifest.get("member_candidate_ids") or []
        holdout = _portfolio_metrics_for_split(
            member_ids, clauses_by_id, splits["holdout"]
        )
        holdout_resolved = int(holdout["resolved"])
        holdout_wins = int(holdout["wins"])
        holdout_support_passed = (
            holdout_resolved >= int(config.min_holdout_support)
        )
        holdout_p_value = _binomial_upper_tail(
            holdout_wins,
            holdout_resolved,
            config.null_hit_rate,
        )
        holdout_hit_rate = holdout.get("hit_rate")
        return {
            **manifest,
            "train": _portfolio_metrics_for_split(
                member_ids, clauses_by_id, splits["train"]
            ),
            "validation": _portfolio_metrics_for_split(
                member_ids, clauses_by_id, splits["validation"]
            ),
            "holdout": holdout,
            "holdout_support_gate_passed": holdout_support_passed,
            "holdout_target_rate_gate_passed": bool(
                holdout_hit_rate is not None
                and float(holdout_hit_rate) >= float(config.target_hit_rate)
            ),
            "holdout_confirmation": {
                "selection_locked_before_holdout": True,
                "alternative": f"hit_rate > {config.null_hit_rate}",
                "alpha": config.family_alpha,
                "one_sided_binomial_p": round(holdout_p_value, 12),
                "confirmed_above_null_rate": bool(
                    holdout_support_passed
                    and holdout_p_value <= float(config.family_alpha)
                ),
            },
        }

    results_artifact_body = {
        "schema_version": DISCOVERY_SCHEMA_VERSION,
        "engine_version": _discovery_engine_version(config),
        "artifact_type": "wide_rule_candidate_results",
        "dataset_as_of_utc": common["dataset_as_of_utc"],
        "split_policy": common["split_policy"],
        "splits": split_metadata,
        "null_hit_rate": config.null_hit_rate,
        "family_alpha": config.family_alpha,
        "results": results,
        "selected_portfolio": portfolio_results(
            (portfolio_search or {}).get("selected")
        ),
        "best_available_portfolio": portfolio_results(
            (portfolio_search or {}).get("best_available")
        ),
    }
    results_artifact = {
        **results_artifact_body,
        "artifact_sha256": _digest(results_artifact_body),
    }
    run_id = "wide-discovery-" + _digest(
        {
            "manifest": manifest_artifact["artifact_sha256"],
            "results": results_artifact["artifact_sha256"],
        }
    )[:20]
    return {
        "run_id": run_id,
        "manifests": manifest_artifact,
        "results": results_artifact,
    }


def discover_extended_scopes(join: "JournalJoinStore", *, config: DiscoveryConfig) -> dict[str, Any]:
    """Two pre-registered searches, one admission family, never holdout-ranked.

The market-only branch splits its own causal-quote cohort. This is essential
when market collection began after the general train period. All its rules
require a market input, so their live and historical universe stays identical.
"""
    if not config.extended_features or config.market_only:
        raise ValueError("extended scopes require the general extended config")
    general = discover_rules(join.joined_observations(), config=config)
    market_config = replace(config, market_only=True)
    available_fixtures = set()
    for observation in join.joined_observations():
        row, _ = _eligible_row(observation, market_config)
        if row is not None:
            available_fixtures.add(row.fixture_id)
    minimum = max(3, math.ceil(config.min_train_support / config.train_fraction),
                  math.ceil(config.min_validation_support / config.validation_fraction),
                  math.ceil(config.min_holdout_support / (1 - config.train_fraction - config.validation_fraction)))
    market = (discover_rules(join.joined_observations(), config=market_config)
              if len(available_fixtures) >= minimum else None)
    body = dict(general["manifests"])
    body.pop("artifact_sha256", None)
    result_body = dict(general["results"])
    result_body.pop("artifact_sha256", None)
    scope = {
        "status": "completed" if market else "collecting",
        "eligible_fixtures": len(available_fixtures),
        "minimum_fixtures_to_attempt": minimum,
        "market_dependency_required": True,
        "selection_uses_holdout": False,
        "candidate_count": len(market["manifests"]["candidates"]) if market else 0,
        "reason": ("completed" if market and market["manifests"]["candidates"] else
                   "no_supported_candidates" if market else "insufficient_causal_market_fixtures"),
    }
    if market:
        scope.update({key: market["manifests"][key] for key in ("input", "splits", "search", "threshold_manifest", "config")})
    body["search"] = {**body["search"], "market_scope": scope,
                      "total_scope_evaluations": body["search"]["evaluations_used"] + (market["manifests"]["search"]["evaluations_used"] if market else 0)
                          + body["search"].get("error_refinement", {}).get("proposals_evaluated", 0)
                          + (market["manifests"]["search"].get("error_refinement", {}).get("proposals_evaluated", 0) if market else 0)}
    body["expression_library"] = {"version": "nonlinear_v1", "primitives": PRIMITIVES, "products": PRODUCTS,
                                  "feature_names": list(EXTENDED_FEATURE_NAMES)}
    candidates = []
    results = []
    seen = set()
    for scope_name, report in (("general_nonlinear", general), ("causal_market", market)):
        if report is None:
            continue
        by_id = {r["candidate_id"]: r for r in report["results"]["results"]}
        for candidate in report["manifests"]["candidates"]:
            cid = candidate["candidate_id"]
            if cid in seen:
                continue
            seen.add(cid)
            candidate = {**candidate, "research_scope": scope_name, "rank_at_discovery": len(candidates) + 1}
            candidate.pop("manifest_sha256", None)
            candidate["manifest_sha256"] = _digest(candidate)
            candidates.append(candidate)
            results.append({**by_id[cid], "research_scope": scope_name,
                            "rank_at_discovery": len(candidates)})
    # Both selected families share correction; no reporting of per-branch
    # significance as if the other branch had not been searched.
    adjusted = _holm({r["candidate_id"]: r["statistical_test"]["one_sided_binomial_p"] for r in results}, config.family_alpha)
    for row in results:
        row["statistical_test"] = {**row["statistical_test"], "family_size": len(results), **adjusted[row["candidate_id"]]}
    body["candidates"] = candidates
    result_body["results"] = results
    result_body["market_scope_splits"] = market["manifests"]["splits"] if market else None
    body["artifact_sha256"] = _digest(body)
    result_body["artifact_sha256"] = _digest(result_body)
    return {"run_id": "wide-discovery-" + _digest({"manifest": body["artifact_sha256"], "results": result_body["artifact_sha256"]})[:20],
            "manifests": body, "results": result_body}


def discover_journal_paths(active_path: os.PathLike[str] | str) -> list[Path]:
    """Return strict timestamp archives oldest-first and active file last."""

    active = Path(active_path).expanduser().resolve()
    filename = active.name
    stem = filename[:-6] if filename.endswith(".jsonl") else active.stem
    prefix = stem + "."
    archives: list[tuple[str, Path]] = []
    try:
        candidates = list(active.parent.iterdir())
    except OSError:
        candidates = []
    for candidate in candidates:
        name = candidate.name
        if name == filename or not name.startswith(prefix):
            continue
        if name.endswith(".jsonl.gz"):
            stamp = name[len(prefix) : -len(".jsonl.gz")]
        elif name.endswith(".jsonl"):
            stamp = name[len(prefix) : -len(".jsonl")]
        else:
            continue
        if _ARCHIVE_STAMP.fullmatch(stamp):
            archives.append((stamp, candidate.resolve()))
    paths = [path for _, path in sorted(archives)]
    if active.exists():
        paths.append(active)
    return paths


def stream_jsonl(
    active_path: os.PathLike[str] | str,
    diagnostics: Optional[dict[str, Any]] = None,
) -> Iterator[dict[str, Any]]:
    """Stream active+gzip archives without retaining raw records in RAM."""

    stats = diagnostics if diagnostics is not None else {}
    stats.update({"files": [], "lines": 0, "records": 0, "invalid_lines": 0})
    for source in discover_journal_paths(active_path):
        opener: Callable[..., Any] = gzip.open if source.suffix == ".gz" else open
        file_lines = 0
        try:
            with opener(source, "rt", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    file_lines += 1
                    stats["lines"] += 1
                    try:
                        record = json.loads(line)
                    except (TypeError, ValueError):
                        stats["invalid_lines"] += 1
                        continue
                    if not isinstance(record, dict):
                        stats["invalid_lines"] += 1
                        continue
                    stats["records"] += 1
                    yield record
        except OSError:
            stats.setdefault("read_errors", 0)
            stats["read_errors"] += 1
        stats["files"].append({"path": str(source), "lines": file_lines})


class JournalJoinStore:
    """Ephemeral SQLite observation/outcome/prediction join for the CLI."""

    def __init__(self, *, config: DiscoveryConfig = DiscoveryConfig()) -> None:
        descriptor, name = tempfile.mkstemp(prefix="wide-discovery-", suffix=".sqlite3")
        os.close(descriptor)
        self.path = Path(name)
        self.config = config
        self.connection = sqlite3.connect(str(self.path))
        self.connection.execute("PRAGMA journal_mode=OFF")
        self.connection.execute("PRAGMA synchronous=OFF")
        self.connection.execute("PRAGMA temp_store=FILE")
        self.connection.execute("PRAGMA cache_size=-32768")
        self.connection.executescript(
            """
            CREATE TABLE observations (
                observation_id TEXT PRIMARY KEY,
                fixture_id INTEGER NOT NULL,
                minute INTEGER NOT NULL,
                created_epoch REAL NOT NULL,
                schema_version INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX observations_order_idx
                ON observations(created_epoch, fixture_id, observation_id);
            CREATE TABLE outcomes (
                observation_id TEXT PRIMARY KEY,
                resolved_epoch REAL,
                schema_version INTEGER NOT NULL,
                outcome_revision INTEGER NOT NULL,
                record_created_at_utc TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE TABLE predictions (
                source TEXT NOT NULL,
                prediction_key TEXT NOT NULL,
                observation_id TEXT NOT NULL,
                created_epoch REAL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY(source, prediction_key)
            );
            CREATE INDEX predictions_observation_idx
                ON predictions(source, observation_id, created_epoch);
            CREATE TABLE market_quotes (
                record_key TEXT PRIMARY KEY, fixture_id INTEGER NOT NULL,
                captured_epoch REAL NOT NULL, payload_json TEXT NOT NULL
            );
            CREATE INDEX market_quotes_time_idx ON market_quotes(fixture_id, captured_epoch);
            """
        )
        self.counts: Counter[str] = Counter()
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.connection.close()
        finally:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass

    def __enter__(self) -> "JournalJoinStore":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    def ingest_observation_records(self, records: Iterable[Mapping[str, Any]]) -> None:
        with self.connection:
            for record in records:
                observation_id = str(
                    record.get("observation_id") or record.get("observation_key") or ""
                ).strip()
                if not observation_id:
                    self.counts["missing_observation_id"] += 1
                    continue
                record_type = str(record.get("record_type") or "")
                if record_type == "observation":
                    stage = str(record.get("stage") or "")
                    minute = _safe_int(record.get("minute"))
                    fixture_id = _safe_int(record.get("fixture_id"))
                    created_at = _parse_utc(record.get("created_at_utc"))
                    if stage not in {"decision_pipeline", "wide_monitor"}:
                        self.counts["non_universe_stage"] += 1
                        continue
                    if minute is None or not self.config.min_minute <= minute <= self.config.max_minute:
                        self.counts["non_universe_minute"] += 1
                        continue
                    if fixture_id is None or fixture_id <= 0 or created_at is None:
                        self.counts["invalid_observation_identity"] += 1
                        continue
                    try:
                        payload = _canonical_json(record)
                    except (TypeError, ValueError, OverflowError):
                        self.counts["invalid_json_payload"] += 1
                        continue
                    self.connection.execute(
                        """
                        INSERT INTO observations VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(observation_id) DO UPDATE SET
                            fixture_id=excluded.fixture_id,
                            minute=excluded.minute,
                            created_epoch=excluded.created_epoch,
                            schema_version=excluded.schema_version,
                            payload_json=excluded.payload_json
                        WHERE excluded.schema_version > observations.schema_version
                           OR (excluded.schema_version = observations.schema_version
                               AND excluded.created_epoch >= observations.created_epoch)
                        """,
                        (
                            observation_id,
                            fixture_id,
                            minute,
                            created_at.timestamp(),
                            _safe_int(record.get("schema_version")) or 0,
                            payload,
                        ),
                    )
                    self.counts["observation_records"] += 1
                elif record_type == "observation_outcome":
                    outcome = _mapping(record.get("outcome"))
                    declared_resolved_at = _parse_utc(outcome.get("resolved_at_utc"))
                    journal_created_at = _parse_utc(record.get("created_at_utc"))
                    # A label cannot be treated as known before the journal
                    # record carrying it existed, even if the provider's
                    # declared match-resolution timestamp is earlier.
                    available_times = [
                        value
                        for value in (declared_resolved_at, journal_created_at)
                        if value is not None
                    ]
                    # Without the journal timestamp we cannot prove when this
                    # particular revision became available.  Keep the label
                    # for holdout evaluation, but make temporal purging treat
                    # it as unavailable for an earlier split.
                    resolved_at = (
                        max(available_times)
                        if journal_created_at is not None
                        else None
                    )
                    schema_version, revision, _raw_created_at = outcome_rank(record)
                    ranked_created_at = (
                        journal_created_at.isoformat()
                        if journal_created_at is not None
                        else ""
                    )
                    try:
                        payload = _canonical_json(outcome)
                    except (TypeError, ValueError, OverflowError):
                        self.counts["invalid_json_payload"] += 1
                        continue
                    self.connection.execute(
                        """
                        INSERT INTO outcomes VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(observation_id) DO UPDATE SET
                            resolved_epoch=excluded.resolved_epoch,
                            schema_version=excluded.schema_version,
                            outcome_revision=excluded.outcome_revision,
                            record_created_at_utc=excluded.record_created_at_utc,
                            payload_json=excluded.payload_json
                        WHERE excluded.schema_version > outcomes.schema_version
                           OR (excluded.schema_version = outcomes.schema_version
                               AND excluded.outcome_revision > outcomes.outcome_revision)
                           OR (excluded.schema_version = outcomes.schema_version
                               AND excluded.outcome_revision = outcomes.outcome_revision
                               AND excluded.record_created_at_utc >= outcomes.record_created_at_utc)
                        """,
                        (
                            observation_id,
                            resolved_at.timestamp() if resolved_at else None,
                            schema_version,
                            revision,
                            ranked_created_at,
                            payload,
                        ),
                    )
                    self.counts["outcome_records"] += 1

    def ingest_predictions(
        self, records: Iterable[Mapping[str, Any]], *, source: str
    ) -> None:
        if source not in {"static", "rolling"}:
            raise ValueError("prediction source must be static or rolling")
        expected_type = (
            "shadow_ml_prediction" if source == "static" else "shadow_ml_rolling_prediction"
        )
        with self.connection:
            for record in records:
                if str(record.get("record_type") or "") != expected_type:
                    self.counts[f"{source}_wrong_record_type"] += 1
                    continue
                observation_id = str(record.get("observation_id") or "").strip()
                prediction_key = str(record.get("prediction_key") or "").strip()
                if not observation_id or not prediction_key:
                    self.counts[f"{source}_missing_identity"] += 1
                    continue
                created_at = _parse_utc(record.get("created_at_utc"))
                try:
                    payload = _canonical_json(record)
                except (TypeError, ValueError, OverflowError):
                    self.counts["invalid_json_payload"] += 1
                    continue
                self.connection.execute(
                    "INSERT OR IGNORE INTO predictions VALUES (?, ?, ?, ?, ?)",
                    (
                        source,
                        prediction_key,
                        observation_id,
                        created_at.timestamp() if created_at else None,
                        payload,
                    ),
                )
                self.counts[f"{source}_prediction_records"] += 1

    def ingest_market_quotes(self, records: Iterable[Mapping[str, Any]]) -> None:
        from market_benchmark.research import _time
        with self.connection:
            for quote in records:
                if quote.get("record_type") != "market_odds_snapshot":
                    continue
                captured = _time(quote.get("captured_at_utc"))
                fid = _safe_int(quote.get("fixture_id"))
                key = str(quote.get("record_key") or "")
                if captured is None or fid is None or fid <= 0 or not key:
                    self.counts["market_invalid_identity"] += 1
                    continue
                try:
                    payload = _canonical_json(quote)
                except (TypeError, ValueError, OverflowError):
                    self.counts["market_invalid_payload"] += 1
                    continue
                self.connection.execute(
                    """INSERT INTO market_quotes VALUES(?,?,?,?)
                    ON CONFLICT(record_key) DO UPDATE SET
                        captured_epoch=excluded.captured_epoch,payload_json=excluded.payload_json
                    WHERE excluded.captured_epoch < market_quotes.captured_epoch""",
                    (key, fid, captured.timestamp(), payload),
                )
                self.counts["market_quote_records"] += 1

    def _causal_prediction(
        self,
        payloads: Sequence[str],
        observation: Mapping[str, Any],
        outcome_resolved_at: Optional[datetime],
        *,
        source: str,
    ) -> Optional[dict[str, Any]]:
        observation_at = _parse_utc(observation.get("created_at_utc"))
        fixture_id = _safe_int(observation.get("fixture_id"))
        minute = _safe_int(observation.get("minute"))
        observation_id = str(observation.get("observation_id") or "").strip()
        if observation_at is None:
            return None
        for payload in payloads:
            try:
                prediction = json.loads(payload)
            except (TypeError, ValueError):
                continue
            target = _mapping(_mapping(prediction.get("predictions")).get("to90"))
            prediction_at = _parse_utc(prediction.get("created_at_utc"))
            model_at = _parse_utc(prediction.get("model_created_at_utc"))
            cutoff_at = _parse_utc(prediction.get("model_data_cutoff_utc"))
            recorded_observation_at = _parse_utc(
                prediction.get("observation_created_at_utc")
            )
            if (
                prediction.get("shadow_only") is not True
                or prediction.get("production_applied") is not False
                or target.get("production_applied") is not False
                or str(prediction.get("prediction_status") or "") not in {"ok", "partial"}
                or str(target.get("status") or "") != "ok"
                or str(prediction.get("observation_id") or "") != observation_id
                or _safe_int(prediction.get("fixture_id")) != fixture_id
                or _safe_int(prediction.get("minute")) != minute
                or recorded_observation_at != observation_at
                or prediction_at is None
                or model_at is None
                or cutoff_at is None
                or cutoff_at > model_at
                or model_at >= observation_at
                or cutoff_at >= observation_at
            ):
                continue
            lag = (prediction_at - observation_at).total_seconds()
            if lag < 0.0 or lag > self.config.max_prediction_lag_seconds:
                continue
            if outcome_resolved_at is not None and prediction_at >= outcome_resolved_at:
                continue
            probability = _finite(target.get("calibrated_probability_pct"))
            if probability is None or not 0.0 <= probability <= 100.0:
                continue
            # Preserve the complete record.  The shared live feature extractor
            # repeats all identity/timestamp/model-cutoff checks before it
            # exposes either probability to the search surface.
            return dict(prediction)
        return None

    def joined_observations(self) -> Iterator[dict[str, Any]]:
        cursor = self.connection.execute(
            """
            SELECT o.payload_json, x.payload_json, x.resolved_epoch, o.observation_id,
                   x.schema_version, x.outcome_revision
            FROM observations AS o
            LEFT JOIN outcomes AS x USING(observation_id)
            ORDER BY o.created_epoch, o.fixture_id, o.observation_id
            """
        )
        for (observation_json, outcome_json, resolved_epoch, observation_id,
             schema_version, revision) in cursor:
            try:
                observation = json.loads(observation_json)
            except (TypeError, ValueError):
                continue
            if not isinstance(observation, dict):
                continue
            # ``resolved_epoch`` is deliberately the conservative label
            # availability time (the later of the provider timestamp and the
            # journal write).  It is correct for temporal split purging, but it
            # must not be used as the causal prediction cutoff: a delayed
            # journal write would otherwise admit a prediction created after
            # the match had already resolved.
            label_available_at = (
                datetime.fromtimestamp(resolved_epoch, UTC)
                if resolved_epoch is not None
                else None
            )
            causal_outcome_cutoff_at = label_available_at
            if outcome_json:
                observation["_outcome_record_created_at_utc"] = (
                    label_available_at.isoformat()
                    if label_available_at is not None
                    else None
                )
                try:
                    outcome = json.loads(outcome_json)
                except (TypeError, ValueError):
                    outcome = {}
                if isinstance(outcome, dict):
                    observation["outcome"] = outcome
                    observation["outcome_schema_version"] = schema_version
                    if revision:
                        observation["outcome_revision"] = revision
                    declared_resolved_at = _parse_utc(
                        outcome.get("resolved_at_utc")
                    )
                    causal_times = [
                        value
                        for value in (declared_resolved_at, label_available_at)
                        if value is not None
                    ]
                    causal_outcome_cutoff_at = (
                        min(causal_times) if causal_times else None
                    )
            predictions: dict[str, Any] = {}
            for source in ("static", "rolling"):
                payloads = [
                    row[0]
                    for row in self.connection.execute(
                        """
                        SELECT payload_json FROM predictions
                        WHERE source=? AND observation_id=?
                        ORDER BY created_epoch, prediction_key
                        """,
                        (source, observation_id),
                    )
                ]
                selected = self._causal_prediction(
                    payloads,
                    observation,
                    causal_outcome_cutoff_at,
                    source=source,
                )
                if selected is not None:
                    predictions[source] = selected
            observation["_wide_predictions"] = predictions
            # New snapshots contain frozen availability, including explicit
            # unavailability after a restart. Never backfill over that evidence.
            if self.config.extended_features and "market_research" not in observation:
                observed = _parse_utc(observation.get("created_at_utc"))
                quotes = [json.loads(row[0]) for row in self.connection.execute(
                    """SELECT payload_json FROM market_quotes WHERE fixture_id=?
                    AND captured_epoch>=? AND captured_epoch<=?
                    ORDER BY captured_epoch,record_key""",
                    (observation.get("fixture_id"), observed.timestamp() - HISTORY_SECONDS, observed.timestamp()),
                )]
                observation["market_research"] = freeze_market_context(observation, quotes)
            yield observation

    def diagnostics(self) -> dict[str, Any]:
        observations = int(
            self.connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        )
        outcomes = int(
            self.connection.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0]
        )
        joined = int(
            self.connection.execute(
                "SELECT COUNT(*) FROM observations INNER JOIN outcomes USING(observation_id)"
            ).fetchone()[0]
        )
        return {
            "store": "ephemeral_sqlite",
            "observations": observations,
            "outcomes": outcomes,
            "joined": joined,
            "observations_without_outcome": observations - joined,
            "counts": dict(sorted(self.counts.items())),
        }


def normalised_path(path: os.PathLike[str] | str) -> str:
    return os.path.normcase(os.path.realpath(os.path.abspath(os.fspath(path))))


def assert_safe_outputs(
    outputs: Sequence[Optional[os.PathLike[str] | str]],
    *,
    sources: Sequence[Optional[os.PathLike[str] | str]],
) -> None:
    protected: set[str] = set()
    for source in sources:
        if source is None:
            continue
        protected.add(normalised_path(source))
        protected.add(normalised_path(str(source) + ".lock"))
        protected.add(normalised_path(str(source) + ".index.sqlite3"))
        protected.update(normalised_path(path) for path in discover_journal_paths(source))
    selected = [normalised_path(path) for path in outputs if path is not None]
    if len(selected) != len(set(selected)):
        raise ValueError("manifest and results outputs must be different files")
    if any(path in protected for path in selected):
        raise ValueError("output cannot replace a source journal, archive, lock, or index")


def write_immutable_atomic(path: os.PathLike[str] | str, payload: Mapping[str, Any]) -> None:
    """Atomically create a JSON artifact; existing different bytes are refused."""

    target = Path(path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        indent=2,
        sort_keys=True,
    ) + "\n"
    if target.exists():
        if target.read_text(encoding="utf-8") == content:
            return
        raise FileExistsError(f"immutable artifact already exists: {target}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent)
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        # O_EXCL-style final creation prevents concurrent replacement.  A hard
        # link is atomic on the same filesystem and fails if target appeared.
        try:
            os.link(temporary, target)
        except FileExistsError:
            if target.read_text(encoding="utf-8") != content:
                raise
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


__all__ = [
    "ATOMIC_GRID_VERSION",
    "ATOMIC_THRESHOLD_GRIDS",
    "DISCOVERY_ENGINE_VERSION",
    "RARE_PRECISION_ATOMIC_GRID_VERSION",
    "RARE_PRECISION_DISCOVERY_ENGINE_VERSION",
    "DiscoveryConfig",
    "JournalJoinStore",
    "assert_safe_outputs",
    "discover_journal_paths",
    "discover_rules",
    "extract_features",
    "stream_jsonl",
    "write_immutable_atomic",
]
