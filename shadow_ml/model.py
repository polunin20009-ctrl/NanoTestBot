"""Dependency-light shadow probability models.

This module deliberately has no runtime integration and no third-party
dependencies.  It consumes already joined observation rows, trains two
independent residual logistic models, and returns JSON-serializable artifacts.
The production probability is used as a fixed logit offset, so an uninformative
model naturally falls back toward the existing bot rather than replacing it.
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


ARTIFACT_SCHEMA_VERSION = 1
ALGORITHM_VERSION = "residual_logistic_stdlib_v1"
FEATURE_SCHEMA_VERSION = 1
TARGET_NAMES = ("next15", "to90")
_MISSING_CATEGORY = "__MISSING__"
_OTHER_CATEGORY = "__OTHER__"
_NEXT15_ACCEPTED_QUALITIES = {"exact", "score_confirmed", "inferred"}
FEATURE_CONTRACT_SCHEMA_VERSION = 2
LEGACY_FEATURE_CONTRACT_KEY = "feature_contract:legacy_or_missing:v2"

# These versions describe selection or presentation after the probability has
# already been calculated.  They remain in the immutable observation config
# for auditability, but changing them cannot alter any value consumed by
# ``_feature_values`` or ``_target_base_probability``.  Unknown future factor
# versions are deliberately retained in the contract (fail-safe) unless they
# are explicitly reviewed and added here.
_NON_PREDICTIVE_FACTOR_VERSION_KEYS = frozenset(
    {
        "channel_signal_filter",
        "dynamic_threshold",
        "premium_badge",
        "rescue_controller",
    }
)


@dataclass(frozen=True)
class ShadowMLConfig:
    """Training, calibration, and offline-readiness settings."""

    train_fraction: float = 0.70
    calibration_fraction: float = 0.15
    holdout_fraction: float = 0.15

    candidate_min_fixtures: int = 150
    candidate_min_positive_fixtures: int = 10
    candidate_min_negative_fixtures: int = 10

    offline_ready_min_fixtures: int = 500
    offline_ready_min_rows: int = 3000
    offline_ready_min_class_fixtures: int = 100
    offline_ready_min_span_days: float = 56.0
    offline_ready_min_calibration_fixtures: int = 75
    offline_ready_min_holdout_fixtures: int = 75
    offline_ready_min_positive_fixtures: int = 20
    offline_ready_min_negative_fixtures: int = 20

    l2: float = 0.05
    learning_rate: float = 0.03
    max_epochs: int = 220
    convergence_tolerance: float = 1e-7
    convergence_patience: int = 12

    category_min_fixtures: int = 5
    clip_quantile: float = 0.01
    probability_epsilon: float = 1e-4

    platt_l2: float = 0.02
    platt_max_iterations: int = 40
    platt_min_positive_fixtures: int = 5
    platt_min_negative_fixtures: int = 5

    offline_ready_max_next15_unknown_fraction: float = 0.02
    offline_ready_min_core_availability: float = 0.85
    readiness_min_logloss_improvement: float = 0.001
    readiness_min_brier_improvement: float = 0.0005

    def __post_init__(self) -> None:
        fractions = (
            float(self.train_fraction),
            float(self.calibration_fraction),
            float(self.holdout_fraction),
        )
        if any(value < 0.0 for value in fractions):
            raise ValueError("split fractions must be non-negative")
        if not math.isclose(sum(fractions), 1.0, rel_tol=0.0, abs_tol=1e-9):
            raise ValueError("split fractions must sum to 1")
        if self.train_fraction <= 0.0:
            raise ValueError("train_fraction must be positive")
        if not 0.0 <= self.clip_quantile < 0.5:
            raise ValueError("clip_quantile must be in [0, 0.5)")
        if not 0.0 < self.probability_epsilon < 0.5:
            raise ValueError("probability_epsilon must be in (0, 0.5)")
        integer_fields = (
            self.candidate_min_fixtures,
            self.candidate_min_positive_fixtures,
            self.candidate_min_negative_fixtures,
            self.offline_ready_min_fixtures,
            self.offline_ready_min_rows,
            self.offline_ready_min_class_fixtures,
            self.offline_ready_min_calibration_fixtures,
            self.offline_ready_min_holdout_fixtures,
            self.offline_ready_min_positive_fixtures,
            self.offline_ready_min_negative_fixtures,
            self.max_epochs,
            self.convergence_patience,
            self.category_min_fixtures,
            self.platt_max_iterations,
            self.platt_min_positive_fixtures,
            self.platt_min_negative_fixtures,
        )
        if any(int(value) < 0 for value in integer_fields):
            raise ValueError("count settings must be non-negative")
        if self.max_epochs <= 0:
            raise ValueError("max_epochs must be positive")
        if self.learning_rate <= 0.0:
            raise ValueError("learning_rate must be positive")
        if self.l2 < 0.0 or self.platt_l2 < 0.0:
            raise ValueError("regularization must be non-negative")
        if not 0.0 <= self.offline_ready_max_next15_unknown_fraction <= 1.0:
            raise ValueError(
                "offline_ready_max_next15_unknown_fraction must be in [0, 1]"
            )
        if not 0.0 <= self.offline_ready_min_core_availability <= 1.0:
            raise ValueError(
                "offline_ready_min_core_availability must be in [0, 1]"
            )
        if (
            self.readiness_min_logloss_improvement < 0.0
            or self.readiness_min_brier_improvement < 0.0
        ):
            raise ValueError("readiness improvements must be non-negative")


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _finite_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return float(value)
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _safe_int(value: Any) -> Optional[int]:
    number = _finite_float(value)
    if number is None:
        return None
    try:
        return int(number)
    except (OverflowError, ValueError):
        return None


def _parse_timestamp(value: Any) -> Optional[datetime]:
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
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalise_now(now: Any) -> datetime:
    parsed = _parse_timestamp(now)
    return parsed or datetime.now(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _label(value: Any) -> Optional[int]:
    if value is True or value == 1:
        return 1
    if value is False or value == 0:
        return 0
    return None


def _normalise_probability(value: Any) -> Optional[float]:
    number = _finite_float(value)
    if number is None:
        return None
    # The observation schema stores model probabilities in percentage points.
    # Treat 1.0 as 1%, not 100%; guessing by magnitude would catastrophically
    # misread legitimate low probabilities.
    if number < 0.0 or number > 100.0:
        return None
    return number / 100.0


def _record_identity(record: Mapping[str, Any], fallback: int) -> str:
    value = (
        record.get("observation_key")
        or record.get("observation_id")
        or record.get("decision_key")
        or record.get("decision_id")
    )
    text = str(value or "").strip()
    return text or f"anonymous:{fallback}"


def _normalise_contract_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _normalise_contract_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_normalise_contract_value(item) for item in value]
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    return str(value)


def _feature_contract(record: Mapping[str, Any]) -> Dict[str, Any]:
    """Return the deterministic feature-compatibility contract for a row.

    Runtime code/config hashes are deliberately excluded: the contract tracks
    the semantic observation/model/factor versions that determine model-input
    meaning, not each deployment build or downstream publication policy.
    """

    schema_version = _safe_int(record.get("schema_version"))
    config = _as_mapping(record.get("config"))
    model_version = str(config.get("model_version") or "").strip()
    factor_versions = config.get("factor_versions")
    predictive_factor_versions = (
        {
            str(key): value
            for key, value in factor_versions.items()
            if str(key) not in _NON_PREDICTIVE_FACTOR_VERSION_KEYS
        }
        if isinstance(factor_versions, Mapping)
        else {}
    )
    if (
        schema_version is None
        or schema_version <= 0
        or not model_version
        or not isinstance(factor_versions, Mapping)
        or not predictive_factor_versions
    ):
        return {
            "key": LEGACY_FEATURE_CONTRACT_KEY,
            "payload": {
                "contract_schema_version": FEATURE_CONTRACT_SCHEMA_VERSION,
                "kind": "legacy_or_missing",
                "observation_schema_version": None,
                "model_version": None,
                "factor_versions": {},
            },
        }

    payload = {
        "contract_schema_version": FEATURE_CONTRACT_SCHEMA_VERSION,
        "kind": "versioned",
        "observation_schema_version": int(schema_version),
        "model_version": model_version,
        "factor_versions": _normalise_contract_value(
            predictive_factor_versions
        ),
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "key": f"feature_contract:{hashlib.sha256(encoded).hexdigest()}",
        "payload": payload,
    }


def _eligible_record(
    record: Mapping[str, Any],
) -> Tuple[bool, Optional[int], Optional[datetime], str]:
    if str(record.get("stage") or "") != "decision_pipeline":
        return False, None, None, "stage"
    outcome = _as_mapping(record.get("outcome"))
    if str(outcome.get("status") or "") != "resolved":
        return False, None, None, "status"
    fixture_id = _safe_int(record.get("fixture_id"))
    if fixture_id is None or fixture_id <= 0:
        return False, None, None, "fixture_id"
    minute = _safe_int(record.get("minute"))
    if minute is None or minute < 46 or minute > 60:
        return False, None, None, "minute"
    timestamp = _parse_timestamp(record.get("created_at_utc"))
    if timestamp is None:
        return False, None, None, "timestamp"
    return True, fixture_id, timestamp, ""


def _target_label(record: Mapping[str, Any], target: str) -> Optional[int]:
    outcome = _as_mapping(record.get("outcome"))
    if target == "next15":
        quality = str(outcome.get("goal_within_15_quality") or "").strip().lower()
        if quality not in _NEXT15_ACCEPTED_QUALITIES:
            return None
        return _label(outcome.get("goal_within_15"))
    return _label(
        outcome.get("goal_to90_normal_time")
        if outcome.get("goal_to90_normal_time") is not None
        else outcome.get("goal_to90")
    )


def _target_quality_weight(record: Mapping[str, Any], target: str) -> float:
    if target != "next15":
        return 1.0
    quality = str(
        _as_mapping(record.get("outcome")).get("goal_within_15_quality") or ""
    ).strip().lower()
    return 0.5 if quality == "inferred" else 1.0


def _next15_quality_diagnostics(
    eligible: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    quality_counts: Dict[str, int] = {
        "exact": 0,
        "score_confirmed": 0,
        "inferred": 0,
        "unknown": 0,
        "missing": 0,
        "unrecognized": 0,
        "invalid_value": 0,
    }
    accepted = 0
    excluded = 0
    for item in eligible:
        outcome = _as_mapping(_as_mapping(item.get("record")).get("outcome"))
        raw_quality = str(
            outcome.get("goal_within_15_quality") or ""
        ).strip().lower()
        if not raw_quality:
            quality_counts["missing"] += 1
            excluded += 1
            continue
        if raw_quality == "unknown":
            quality_counts["unknown"] += 1
            excluded += 1
            continue
        if raw_quality not in _NEXT15_ACCEPTED_QUALITIES:
            quality_counts["unrecognized"] += 1
            excluded += 1
            continue
        if _label(outcome.get("goal_within_15")) is None:
            quality_counts["invalid_value"] += 1
            excluded += 1
            continue
        quality_counts[raw_quality] += 1
        accepted += 1
    total = len(eligible)
    return {
        "rows_evaluated": total,
        "accepted_rows": accepted,
        "unknown_or_invalid_rows": excluded,
        "unknown_or_invalid_fraction": (
            round(excluded / total, 9) if total else None
        ),
        "quality_counts": quality_counts,
    }


def _core_live_metric_diagnostics(
    eligible: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    metric_names = ("xg", "shots_on_target", "shots_in_box", "pressure_index")
    available_by_metric = {name: 0 for name in metric_names}
    for item in eligible:
        record = _as_mapping(item.get("record"))
        raw_metrics = _as_mapping(record.get("raw_metrics"))
        availability = _as_mapping(record.get("availability"))
        for metric in ("xg", "shots_on_target", "shots_in_box"):
            available = all(
                availability.get(f"{metric}_{side}") is True
                and _finite_float(raw_metrics.get(f"{metric}_{side}")) is not None
                for side in ("home", "away")
            )
            available_by_metric[metric] += int(available)
        pressure_available = (
            _finite_float(
                _as_mapping(record.get("features")).get("pressure_index")
            )
            is not None
        )
        available_by_metric["pressure_index"] += int(pressure_available)

    rows = len(eligible)
    total_slots = rows * len(metric_names)
    available_slots = sum(available_by_metric.values())
    return {
        "rows_evaluated": rows,
        "metric_groups": list(metric_names),
        "available_slots": available_slots,
        "total_slots": total_slots,
        "availability_fraction": (
            round(available_slots / total_slots, 9) if total_slots else None
        ),
        "per_metric": {
            name: {
                "available_rows": available_by_metric[name],
                "availability_fraction": (
                    round(available_by_metric[name] / rows, 9)
                    if rows
                    else None
                ),
            }
            for name in metric_names
        },
    }


def _target_base_probability(
    record: Mapping[str, Any],
    target: str,
) -> Optional[float]:
    probabilities = _as_mapping(record.get("probabilities"))
    if target == "next15":
        keys = (
            "pre_ml_prob_next_15",
            "reputation_adjusted_prob_next_15",
            "final_prob_next_15",
            "prob_next_15",
        )
    else:
        keys = (
            "pre_ml_prob_to90",
            "reputation_adjusted_prob_to90",
            "final_prob_to90",
            "prob_to90",
            "prob_until_end_decision",
        )
    for key in keys:
        probability = _normalise_probability(probabilities.get(key))
        if probability is not None:
            return probability
    return None


def _class_fixture_counts(samples: Sequence[Mapping[str, Any]]) -> Dict[str, int]:
    positives = {int(sample["fixture_id"]) for sample in samples if sample["label"] == 1}
    negatives = {int(sample["fixture_id"]) for sample in samples if sample["label"] == 0}
    fixtures = {int(sample["fixture_id"]) for sample in samples}
    return {
        "fixtures": len(fixtures),
        "positive_fixtures": len(positives),
        "negative_fixtures": len(negatives),
        "rows": len(samples),
    }


def summarize_training_data(records: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    """Summarize joined observation rows without training a model."""

    materialized = list(records)
    all_eligible: List[Dict[str, Any]] = []
    excluded = {
        "stage": 0,
        "status": 0,
        "fixture_id": 0,
        "minute": 0,
        "timestamp": 0,
        "not_mapping": 0,
        "feature_contract": 0,
    }
    seen_record_ids: set[str] = set()
    duplicate_records = 0

    for index, raw in enumerate(materialized):
        if not isinstance(raw, Mapping):
            excluded["not_mapping"] += 1
            continue
        record_id = _record_identity(raw, index)
        if record_id in seen_record_ids:
            duplicate_records += 1
            continue
        seen_record_ids.add(record_id)
        valid, fixture_id, timestamp, reason = _eligible_record(raw)
        if not valid:
            excluded[reason] += 1
            continue
        all_eligible.append(
            {
                "record": raw,
                "record_id": record_id,
                "fixture_id": int(fixture_id),  # type: ignore[arg-type]
                "timestamp": timestamp,
                "contract": _feature_contract(raw),
            }
        )

    training_contract = _select_training_contract(all_eligible)
    selected_key = (
        str(training_contract.get("key") or "")
        if isinstance(training_contract, Mapping)
        else ""
    )
    eligible = [
        item
        for item in all_eligible
        if str(_as_mapping(item.get("contract")).get("key") or "") == selected_key
    ] if selected_key else []
    incompatible = len(all_eligible) - len(eligible)
    excluded["feature_contract"] = incompatible

    cohort_accumulator: Dict[str, Dict[str, Any]] = {}
    for item in all_eligible:
        contract = _as_mapping(item.get("contract"))
        key = str(contract.get("key") or "")
        cohort = cohort_accumulator.setdefault(
            key,
            {
                "key": key,
                "payload": dict(_as_mapping(contract.get("payload"))),
                "rows": 0,
                "fixture_ids": set(),
                "newest_observation_utc": None,
            },
        )
        cohort["rows"] += 1
        cohort["fixture_ids"].add(int(item["fixture_id"]))
        timestamp_iso = _iso_utc(item["timestamp"])
        if (
            cohort["newest_observation_utc"] is None
            or timestamp_iso > cohort["newest_observation_utc"]
        ):
            cohort["newest_observation_utc"] = timestamp_iso
    cohorts: List[Dict[str, Any]] = []
    for key in sorted(cohort_accumulator):
        cohort = cohort_accumulator[key]
        cohorts.append(
            {
                "key": cohort["key"],
                "payload": cohort["payload"],
                "rows": int(cohort["rows"]),
                "fixtures": len(cohort["fixture_ids"]),
                "newest_observation_utc": cohort["newest_observation_utc"],
                "selected": key == selected_key,
            }
        )

    fixtures = {int(item["fixture_id"]) for item in eligible}
    timestamps = [item["timestamp"] for item in eligible]
    span_days = (
        (max(timestamps) - min(timestamps)).total_seconds() / 86400.0
        if timestamps
        else 0.0
    )
    target_summaries: Dict[str, Any] = {}
    for target in TARGET_NAMES:
        rows = 0
        unknown = 0
        missing_base = 0
        positive_fixtures: set[int] = set()
        negative_fixtures: set[int] = set()
        usable_fixtures: set[int] = set()
        for item in eligible:
            record = item["record"]
            fixture_id = int(item["fixture_id"])
            target_label = _target_label(record, target)
            if target_label is None:
                unknown += 1
                continue
            if _target_base_probability(record, target) is None:
                missing_base += 1
                continue
            rows += 1
            usable_fixtures.add(fixture_id)
            if target_label:
                positive_fixtures.add(fixture_id)
            else:
                negative_fixtures.add(fixture_id)
        target_summaries[target] = {
            "usable_rows": rows,
            "usable_fixtures": len(usable_fixtures),
            "positive_fixtures": len(positive_fixtures),
            "negative_fixtures": len(negative_fixtures),
            "unknown_labels_skipped": unknown,
            "missing_base_probability_skipped": missing_base,
        }

    data_quality = {
        "next15": _next15_quality_diagnostics(eligible),
        "core_live_metrics": _core_live_metric_diagnostics(eligible),
    }

    return {
        "records_seen": len(materialized),
        "eligible_records_before_contract_filter": len(all_eligible),
        "eligible_records": len(eligible),
        "unique_fixtures": len(fixtures),
        "duplicate_records_skipped": duplicate_records,
        "first_observation_utc": _iso_utc(min(timestamps)) if timestamps else None,
        "last_observation_utc": _iso_utc(max(timestamps)) if timestamps else None,
        "span_days": round(span_days, 6),
        "training_contract": (
            {
                "key": selected_key,
                "payload": dict(
                    _as_mapping(_as_mapping(training_contract).get("payload"))
                ),
            }
            if selected_key
            else None
        ),
        "feature_contract_cohort_count": len(cohorts),
        "incompatible_rows_excluded": incompatible,
        "incompatible_cohort_count": sum(
            1 for cohort in cohorts if not cohort["selected"]
        ),
        "feature_contract_cohorts": cohorts,
        "excluded": excluded,
        "targets": target_summaries,
        "data_quality": data_quality,
        "training_data_hash": _training_data_hash(eligible),
    }


def _all_eligible_items(
    records: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    eligible: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue
        record_id = _record_identity(record, index)
        if record_id in seen:
            continue
        seen.add(record_id)
        valid, fixture_id, timestamp, _ = _eligible_record(record)
        if not valid:
            continue
        eligible.append(
            {
                "record": record,
                "record_id": record_id,
                "fixture_id": int(fixture_id),  # type: ignore[arg-type]
                "timestamp": timestamp,
                "contract": _feature_contract(record),
            }
        )
    return eligible


def _select_training_contract(
    eligible: Sequence[Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not eligible:
        return None
    newest = max(
        eligible,
        key=lambda item: (
            item["timestamp"],
            str(item.get("record_id") or ""),
        ),
    )
    contract = _as_mapping(newest.get("contract"))
    key = str(contract.get("key") or "")
    if not key:
        return None
    return {
        "key": key,
        "payload": dict(_as_mapping(contract.get("payload"))),
    }


def _materialize_eligible(
    records: Sequence[Mapping[str, Any]],
    contract_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    eligible = _all_eligible_items(records)
    selected_key = str(contract_key or "")
    if not selected_key:
        selected = _select_training_contract(eligible)
        selected_key = str((selected or {}).get("key") or "")
    if not selected_key:
        return []
    return [
        item
        for item in eligible
        if str(_as_mapping(item.get("contract")).get("key") or "")
        == selected_key
    ]


def _split_fixture_groups(
    eligible: Sequence[Mapping[str, Any]],
    config: ShadowMLConfig,
) -> Dict[str, Any]:
    first_seen: Dict[int, datetime] = {}
    for item in eligible:
        fixture_id = int(item["fixture_id"])
        timestamp = item["timestamp"]
        current = first_seen.get(fixture_id)
        if current is None or timestamp < current:
            first_seen[fixture_id] = timestamp
    ordered = sorted(first_seen, key=lambda fixture_id: (first_seen[fixture_id], fixture_id))
    count = len(ordered)
    if count == 0:
        train_count = calibration_count = 0
    elif count == 1:
        train_count, calibration_count = 1, 0
    elif count == 2:
        train_count, calibration_count = 1, 0
    else:
        train_count = max(1, int(math.floor(count * config.train_fraction)))
        calibration_count = max(
            1, int(math.floor(count * config.calibration_fraction))
        )
        if train_count + calibration_count >= count:
            train_count = max(1, count - 2)
            calibration_count = 1
    holdout_count = count - train_count - calibration_count
    train = ordered[:train_count]
    calibration = ordered[train_count:train_count + calibration_count]
    holdout = ordered[train_count + calibration_count:]
    return {
        "train": set(train),
        "calibration": set(calibration),
        "holdout": set(holdout),
        "ordered": ordered,
        "first_seen": first_seen,
        "counts": {
            "train": len(train),
            "calibration": len(calibration),
            "holdout": len(holdout),
        },
    }


_FEATURE_NUMERIC_KEYS = (
    "pressure_index",
    "save_stress",
    "tempo",
    "live_intensity",
    "adjusted_intensity",
    "lambda_2h",
    "game_state_factor",
    "goal_xg_gap_factor",
    "xg_delta_factor",
    "season_context_factor",
    "season_context_factor_45p",
    "team_2h_factor",
    "league_2h_factor",
    "score_state_factor",
    "sample_confidence",
)
_QUALITY_NUMERIC_KEYS = (
    "xg_confidence",
    "tempo_confidence",
    "available_metric_count",
    "total_metric_count",
)


def _feature_values(
    record: Mapping[str, Any],
) -> Tuple[Dict[str, float], Dict[str, str]]:
    """Extract only pre-outcome fields from an observation.

    `decision`, `gates`, `telegram`, and `outcome` are intentionally never read
    here.  This explicit allow-list is the main defence against label leakage.
    """

    numeric: Dict[str, float] = {}
    categorical: Dict[str, str] = {}

    minute = _finite_float(record.get("minute"))
    if minute is not None:
        numeric["context.minute"] = minute
        numeric["context.minutes_to90"] = max(0.0, 90.0 - minute)

    match = _as_mapping(record.get("match"))
    score_home = _finite_float(match.get("score_home"))
    score_away = _finite_float(match.get("score_away"))
    if score_home is not None:
        numeric["match.score_home"] = score_home
    if score_away is not None:
        numeric["match.score_away"] = score_away
    if score_home is not None and score_away is not None:
        numeric["match.score_total"] = score_home + score_away
        numeric["match.score_diff"] = score_home - score_away
        numeric["match.score_abs_diff"] = abs(score_home - score_away)
        numeric["match.is_draw"] = float(score_home == score_away)

    is_cup = match.get("is_cup")
    if isinstance(is_cup, bool):
        numeric["match.is_cup"] = float(is_cup)

    categorical["context.window_name"] = str(
        record.get("window_name") or _MISSING_CATEGORY
    )
    categorical["match.league_type"] = str(
        match.get("league_type") or _MISSING_CATEGORY
    )
    categorical["match.is_cup_source"] = str(
        match.get("is_cup_source") or _MISSING_CATEGORY
    )
    categorical["match.score_state"] = str(
        match.get("score_state") or _MISSING_CATEGORY
    )

    raw_metrics = _as_mapping(record.get("raw_metrics"))
    for key, value in raw_metrics.items():
        number = _finite_float(value)
        if number is not None:
            numeric[f"raw.{key}"] = number

    availability = _as_mapping(record.get("availability"))
    for key, value in availability.items():
        if isinstance(value, bool):
            numeric[f"available.{key}"] = float(value)

    quality = _as_mapping(record.get("data_quality"))
    for key in _QUALITY_NUMERIC_KEYS:
        number = _finite_float(quality.get(key))
        if number is not None:
            numeric[f"quality.{key}"] = number
    for key in ("stats_health", "xg_source", "tempo_source"):
        categorical[f"quality.{key}"] = str(
            quality.get(key) or _MISSING_CATEGORY
        )

    features = _as_mapping(record.get("features"))
    for key in _FEATURE_NUMERIC_KEYS:
        number = _finite_float(features.get(key))
        if number is not None:
            numeric[f"feature.{key}"] = number
    pressure_components = _as_mapping(features.get("pressure_components"))
    for key, value in pressure_components.items():
        number = _finite_float(value)
        if number is not None:
            numeric[f"pressure.{key}"] = number

    return numeric, categorical


def prediction_input_fingerprint(
    record: Mapping[str, Any],
    *,
    feature_extractor: Optional[
        Callable[[Mapping[str, Any]], Tuple[Dict[str, float], Dict[str, str]]]
    ] = None,
    contract_extractor: Optional[
        Callable[[Mapping[str, Any]], Dict[str, Any]]
    ] = None,
    profile: str = "static",
) -> str:
    """Hash exactly the observation values consumed by shadow inference.

    Identity, outcome, decision, Telegram, and other downstream fields are
    intentionally absent.  This lets an audit prove that a prediction made on
    a router preview used the same predictive input as the final canonical
    decision observation, even when their observation IDs differ.
    """

    if not isinstance(record, Mapping):
        raise TypeError("prediction input must be a mapping")
    extractor = feature_extractor or _feature_values
    contract = contract_extractor or _feature_contract
    numeric, categorical = extractor(record)
    payload = {
        "fingerprint_schema_version": 1,
        "profile": str(profile),
        "feature_contract_key": str(contract(record).get("key") or ""),
        "base_probabilities": {
            target: _target_base_probability(record, target)
            for target in TARGET_NAMES
        },
        "numeric": numeric,
        "categorical": categorical,
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _build_target_samples(
    eligible: Sequence[Mapping[str, Any]],
    target: str,
    *,
    feature_extractor: Optional[
        Callable[[Mapping[str, Any]], Tuple[Dict[str, float], Dict[str, str]]]
    ] = None,
) -> List[Dict[str, Any]]:
    extractor = feature_extractor or _feature_values
    samples: List[Dict[str, Any]] = []
    for item in eligible:
        record = item["record"]
        target_label = _target_label(record, target)
        base_probability = _target_base_probability(record, target)
        if target_label is None or base_probability is None:
            continue
        numeric, categorical = extractor(record)
        samples.append(
            {
                "record_id": item["record_id"],
                "fixture_id": int(item["fixture_id"]),
                "timestamp": item["timestamp"],
                "label": int(target_label),
                "base_probability": float(base_probability),
                "quality_weight": _target_quality_weight(record, target),
                "numeric": numeric,
                "categorical": categorical,
            }
        )
    return samples


def _quantile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = max(0.0, min(1.0, q)) * (len(ordered) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _fit_preprocessor(
    samples: Sequence[Mapping[str, Any]],
    config: ShadowMLConfig,
) -> Dict[str, Any]:
    numeric_names = sorted(
        {
            str(name)
            for sample in samples
            for name in _as_mapping(sample.get("numeric"))
        }
    )
    numeric_spec: Dict[str, Dict[str, float]] = {}
    for name in numeric_names:
        values = [
            float(value)
            for sample in samples
            for value in [_as_mapping(sample.get("numeric")).get(name)]
            if _finite_float(value) is not None
        ]
        values = [float(value) for value in values]
        median = statistics.median(values) if values else 0.0
        low = _quantile(values, config.clip_quantile) if values else median
        high = _quantile(values, 1.0 - config.clip_quantile) if values else median
        q25 = _quantile(values, 0.25) if values else median
        q75 = _quantile(values, 0.75) if values else median
        scale = q75 - q25
        if scale <= 1e-9 and len(values) > 1:
            scale = statistics.pstdev(values)
        if not math.isfinite(scale) or scale <= 1e-9:
            scale = 1.0
        numeric_spec[name] = {
            "median": float(median),
            "scale": float(scale),
            "clip_low": float(min(low, high)),
            "clip_high": float(max(low, high)),
        }

    categorical_names = sorted(
        {
            str(name)
            for sample in samples
            for name in _as_mapping(sample.get("categorical"))
        }
    )
    categorical_spec: Dict[str, Dict[str, Any]] = {}
    for name in categorical_names:
        fixtures_by_value: Dict[str, set[int]] = {}
        for sample in samples:
            value = str(
                _as_mapping(sample.get("categorical")).get(
                    name, _MISSING_CATEGORY
                )
                or _MISSING_CATEGORY
            )
            fixtures_by_value.setdefault(value, set()).add(int(sample["fixture_id"]))
        retained = sorted(
            value
            for value, fixtures in fixtures_by_value.items()
            if value not in {_MISSING_CATEGORY, _OTHER_CATEGORY}
            and len(fixtures) >= config.category_min_fixtures
        )
        categorical_spec[name] = {
            "values": [_MISSING_CATEGORY, _OTHER_CATEGORY, *retained]
        }

    feature_names: List[str] = []
    for name in numeric_names:
        feature_names.extend((f"num:{name}", f"missing:{name}"))
    for name in categorical_names:
        for value in categorical_spec[name]["values"]:
            feature_names.append(f"cat:{name}={value}")

    feature_hash = hashlib.sha256(
        json.dumps(
            feature_names,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "schema_version": FEATURE_SCHEMA_VERSION,
        "numeric": numeric_spec,
        "categorical": categorical_spec,
        "feature_names": feature_names,
        "feature_hash": feature_hash,
    }


def _transform_features(
    numeric: Mapping[str, Any],
    categorical: Mapping[str, Any],
    preprocessor: Mapping[str, Any],
) -> List[float]:
    vector: List[float] = []
    numeric_spec = _as_mapping(preprocessor.get("numeric"))
    for name in sorted(numeric_spec):
        spec = _as_mapping(numeric_spec[name])
        value = _finite_float(numeric.get(name))
        missing = value is None
        if missing:
            value = float(spec.get("median") or 0.0)
        low = float(spec.get("clip_low") or 0.0)
        high = float(spec.get("clip_high") or 0.0)
        value = max(low, min(high, float(value)))
        median = float(spec.get("median") or 0.0)
        scale = float(spec.get("scale") or 1.0)
        if not math.isfinite(scale) or abs(scale) <= 1e-12:
            scale = 1.0
        vector.append((value - median) / scale)
        vector.append(float(missing))

    categorical_spec = _as_mapping(preprocessor.get("categorical"))
    for name in sorted(categorical_spec):
        values = list(_as_mapping(categorical_spec[name]).get("values") or [])
        raw_value = str(categorical.get(name, _MISSING_CATEGORY) or _MISSING_CATEGORY)
        selected = raw_value if raw_value in values else _OTHER_CATEGORY
        vector.extend(float(value == selected) for value in values)
    return vector


def _fixture_weights(samples: Sequence[Mapping[str, Any]]) -> List[float]:
    row_counts: Dict[int, int] = {}
    for sample in samples:
        fixture_id = int(sample["fixture_id"])
        row_counts[fixture_id] = row_counts.get(fixture_id, 0) + 1
    weights: List[float] = []
    for sample in samples:
        fixture_id = int(sample["fixture_id"])
        quality = max(0.0, float(sample.get("quality_weight") or 0.0))
        denominator = row_counts.get(fixture_id, 0)
        weights.append(quality / denominator if denominator > 0.0 else 0.0)
    return weights


def _clip_probability(probability: float, epsilon: float) -> float:
    return max(epsilon, min(1.0 - epsilon, float(probability)))


def _logit(probability: float, epsilon: float) -> float:
    value = _clip_probability(probability, epsilon)
    return math.log(value / (1.0 - value))


def _sigmoid(value: float) -> float:
    if value >= 0.0:
        exp_value = math.exp(-min(value, 50.0))
        return 1.0 / (1.0 + exp_value)
    exp_value = math.exp(max(value, -50.0))
    return exp_value / (1.0 + exp_value)


def _linear_probability(
    coefficients: Sequence[float],
    intercept: float,
    vector: Sequence[float],
    base_probability: float,
    epsilon: float,
) -> float:
    score = _logit(base_probability, epsilon) + float(intercept)
    score += sum(float(weight) * float(value) for weight, value in zip(coefficients, vector))
    return _sigmoid(score)


def _fit_residual_logistic(
    samples: Sequence[Mapping[str, Any]],
    vectors: Sequence[Sequence[float]],
    config: ShadowMLConfig,
) -> Dict[str, Any]:
    dimension = len(vectors[0]) if vectors else 0
    coefficients = [0.0] * dimension
    intercept = 0.0
    first_moment = [0.0] * (dimension + 1)
    second_moment = [0.0] * (dimension + 1)
    weights = _fixture_weights(samples)
    total_weight = sum(weights) or 1.0
    beta1, beta2 = 0.9, 0.999
    adam_epsilon = 1e-8
    previous_loss: Optional[float] = None
    stable_epochs = 0
    epochs_completed = 0

    for epoch in range(1, config.max_epochs + 1):
        gradient = [0.0] * dimension
        intercept_gradient = 0.0
        data_loss = 0.0
        for sample, vector, sample_weight in zip(samples, vectors, weights):
            probability = _linear_probability(
                coefficients,
                intercept,
                vector,
                float(sample["base_probability"]),
                config.probability_epsilon,
            )
            label = int(sample["label"])
            error = probability - label
            intercept_gradient += sample_weight * error
            for index, value in enumerate(vector):
                gradient[index] += sample_weight * error * float(value)
            clipped = _clip_probability(probability, config.probability_epsilon)
            data_loss += sample_weight * (
                -(label * math.log(clipped) + (1 - label) * math.log1p(-clipped))
            )

        intercept_gradient /= total_weight
        for index in range(dimension):
            gradient[index] = (
                gradient[index] / total_weight
                + config.l2 * coefficients[index]
            )
        loss = (
            data_loss / total_weight
            + 0.5 * config.l2 * sum(value * value for value in coefficients)
        )

        gradients = [intercept_gradient, *gradient]
        parameters = [intercept, *coefficients]
        correction1 = 1.0 - beta1**epoch
        correction2 = 1.0 - beta2**epoch
        for index, grad in enumerate(gradients):
            first_moment[index] = beta1 * first_moment[index] + (1.0 - beta1) * grad
            second_moment[index] = (
                beta2 * second_moment[index] + (1.0 - beta2) * grad * grad
            )
            adjusted_first = first_moment[index] / correction1
            adjusted_second = second_moment[index] / correction2
            parameters[index] -= (
                config.learning_rate
                * adjusted_first
                / (math.sqrt(adjusted_second) + adam_epsilon)
            )
        intercept = max(-12.0, min(12.0, parameters[0]))
        coefficients = [
            max(-12.0, min(12.0, value)) for value in parameters[1:]
        ]
        epochs_completed = epoch

        if previous_loss is not None and abs(previous_loss - loss) <= (
            config.convergence_tolerance * max(1.0, abs(previous_loss))
        ):
            stable_epochs += 1
            if stable_epochs >= config.convergence_patience:
                break
        else:
            stable_epochs = 0
        previous_loss = loss

    return {
        "intercept": float(intercept),
        "coefficients": [float(value) for value in coefficients],
        "epochs_completed": epochs_completed,
        "final_objective": float(previous_loss) if previous_loss is not None else None,
        "l2": float(config.l2),
        "optimizer": "deterministic_full_batch_adam",
    }


def _predict_raw_samples(
    samples: Sequence[Mapping[str, Any]],
    preprocessor: Mapping[str, Any],
    model: Mapping[str, Any],
    config: ShadowMLConfig,
) -> List[float]:
    coefficients = list(model.get("coefficients") or [])
    intercept = float(model.get("intercept") or 0.0)
    probabilities: List[float] = []
    for sample in samples:
        vector = _transform_features(
            _as_mapping(sample.get("numeric")),
            _as_mapping(sample.get("categorical")),
            preprocessor,
        )
        probabilities.append(
            _linear_probability(
                coefficients,
                intercept,
                vector,
                float(sample["base_probability"]),
                config.probability_epsilon,
            )
        )
    return probabilities


def _fit_platt(
    samples: Sequence[Mapping[str, Any]],
    raw_probabilities: Sequence[float],
    config: ShadowMLConfig,
) -> Dict[str, Any]:
    counts = _class_fixture_counts(samples)
    if (
        counts["positive_fixtures"] < config.platt_min_positive_fixtures
        or counts["negative_fixtures"] < config.platt_min_negative_fixtures
    ):
        return {
            "status": "identity",
            "reason": "insufficient_classes",
            "slope": 1.0,
            "intercept": 0.0,
        }
    weights = _fixture_weights(samples)
    slope, intercept = 1.0, 0.0
    for _ in range(config.platt_max_iterations):
        gradient_slope = config.platt_l2 * (slope - 1.0)
        gradient_intercept = 0.0
        h_ss = config.platt_l2
        h_si = 0.0
        h_ii = 1e-9
        for sample, raw_probability, weight in zip(
            samples, raw_probabilities, weights
        ):
            logit_value = max(
                -12.0,
                min(
                    12.0,
                    _logit(raw_probability, config.probability_epsilon),
                ),
            )
            probability = _sigmoid(slope * logit_value + intercept)
            error = probability - int(sample["label"])
            variance = max(1e-9, probability * (1.0 - probability))
            gradient_slope += weight * error * logit_value
            gradient_intercept += weight * error
            h_ss += weight * variance * logit_value * logit_value
            h_si += weight * variance * logit_value
            h_ii += weight * variance
        determinant = h_ss * h_ii - h_si * h_si
        if not math.isfinite(determinant) or determinant <= 1e-12:
            return {
                "status": "identity",
                "reason": "singular",
                "slope": 1.0,
                "intercept": 0.0,
            }
        delta_slope = (
            gradient_slope * h_ii - gradient_intercept * h_si
        ) / determinant
        delta_intercept = (
            gradient_intercept * h_ss - gradient_slope * h_si
        ) / determinant
        slope -= max(-1.0, min(1.0, delta_slope))
        intercept -= max(-1.0, min(1.0, delta_intercept))
        slope = max(0.02, min(8.0, slope))
        intercept = max(-8.0, min(8.0, intercept))
        if max(abs(delta_slope), abs(delta_intercept)) < 1e-7:
            break
    if not math.isfinite(slope) or not math.isfinite(intercept) or slope <= 0.0:
        return {
            "status": "identity",
            "reason": "invalid_fit",
            "slope": 1.0,
            "intercept": 0.0,
        }
    return {
        "status": "fitted",
        "reason": None,
        "slope": float(slope),
        "intercept": float(intercept),
    }


def _apply_platt(probability: float, calibration: Mapping[str, Any], epsilon: float) -> float:
    slope = _finite_float(calibration.get("slope"))
    intercept = _finite_float(calibration.get("intercept"))
    if slope is None or intercept is None or slope <= 0.0:
        return _clip_probability(probability, epsilon)
    return _sigmoid(slope * _logit(probability, epsilon) + intercept)


def _metrics(
    samples: Sequence[Mapping[str, Any]],
    probabilities: Sequence[float],
    epsilon: float,
) -> Dict[str, Any]:
    if not samples or len(samples) != len(probabilities):
        return {
            "rows": len(samples),
            "fixtures": 0,
            "positive_fixtures": 0,
            "negative_fixtures": 0,
            "log_loss": None,
            "brier": None,
            "ece": None,
        }
    weights = _fixture_weights(samples)
    total_weight = sum(weights) or 1.0
    log_loss = 0.0
    brier = 0.0
    bins: List[List[float]] = [[0.0, 0.0, 0.0] for _ in range(10)]
    for sample, raw_probability, weight in zip(samples, probabilities, weights):
        probability = _clip_probability(raw_probability, epsilon)
        label = int(sample["label"])
        log_loss += weight * (
            -(label * math.log(probability) + (1 - label) * math.log1p(-probability))
        )
        brier += weight * (probability - label) ** 2
        bin_index = min(9, int(probability * 10.0))
        bins[bin_index][0] += weight
        bins[bin_index][1] += weight * probability
        bins[bin_index][2] += weight * label
    ece = 0.0
    for bin_weight, probability_sum, label_sum in bins:
        if bin_weight <= 0.0:
            continue
        ece += (
            bin_weight
            / total_weight
            * abs(probability_sum / bin_weight - label_sum / bin_weight)
        )
    counts = _class_fixture_counts(samples)
    return {
        **counts,
        "effective_fixture_weight": round(total_weight, 9),
        "log_loss": round(log_loss / total_weight, 9),
        "brier": round(brier / total_weight, 9),
        "ece": round(ece, 9),
    }


def _split_metadata(
    split: Mapping[str, Any],
    eligible: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for name in ("train", "calibration", "holdout"):
        fixture_ids = set(split[name])
        timestamps = [
            item["timestamp"]
            for item in eligible
            if int(item["fixture_id"]) in fixture_ids
        ]
        fixture_digest = hashlib.sha256(
            ",".join(str(value) for value in sorted(fixture_ids)).encode("utf-8")
        ).hexdigest()
        result[name] = {
            "fixtures": len(fixture_ids),
            "first_observation_utc": _iso_utc(min(timestamps)) if timestamps else None,
            "last_observation_utc": _iso_utc(max(timestamps)) if timestamps else None,
            "fixture_id_hash": fixture_digest,
        }
    return result


def _training_data_hash(eligible: Sequence[Mapping[str, Any]]) -> str:
    rows: List[List[Any]] = []
    for item in eligible:
        record = item["record"]
        outcome = _as_mapping(record.get("outcome"))
        next15_quality = str(
            outcome.get("goal_within_15_quality") or ""
        ).strip().lower()
        to90_value = (
            outcome.get("goal_to90_normal_time")
            if outcome.get("goal_to90_normal_time") is not None
            else outcome.get("goal_to90")
        )
        rows.append(
            [
                item["record_id"],
                int(item["fixture_id"]),
                _iso_utc(item["timestamp"]),
                _label(outcome.get("goal_within_15")),
                next15_quality,
                _label(to90_value),
                outcome.get("outcome_schema_version")
                if outcome.get("outcome_schema_version") is not None
                else record.get("outcome_schema_version"),
            ]
        )
    rows.sort(key=lambda value: (value[2], value[0]))
    return hashlib.sha256(
        json.dumps(rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _train_target(
    target: str,
    samples: Sequence[Mapping[str, Any]],
    split: Mapping[str, Any],
    config: ShadowMLConfig,
    training_summary: Mapping[str, Any],
) -> Dict[str, Any]:
    partitions = {
        name: [
            sample
            for sample in samples
            if int(sample["fixture_id"]) in split[name]
        ]
        for name in ("train", "calibration", "holdout")
    }
    all_counts = _class_fixture_counts(samples)
    train_counts = _class_fixture_counts(partitions["train"])
    sample_timestamps = [
        sample["timestamp"]
        for sample in samples
        if isinstance(sample.get("timestamp"), datetime)
    ]
    target_span_days = (
        (max(sample_timestamps) - min(sample_timestamps)).total_seconds()
        / 86400.0
        if sample_timestamps
        else 0.0
    )
    candidate_reasons: List[str] = []
    if all_counts["fixtures"] < config.candidate_min_fixtures:
        candidate_reasons.append("candidate_min_fixtures")
    if train_counts["positive_fixtures"] < config.candidate_min_positive_fixtures:
        candidate_reasons.append("candidate_positive_class")
    if train_counts["negative_fixtures"] < config.candidate_min_negative_fixtures:
        candidate_reasons.append("candidate_negative_class")

    base_metrics = {
        name: _metrics(
            partition,
            [float(sample["base_probability"]) for sample in partition],
            config.probability_epsilon,
        )
        for name, partition in partitions.items()
    }
    summary_quality = _as_mapping(training_summary.get("data_quality"))
    next15_quality = dict(_as_mapping(summary_quality.get("next15")))
    core_live_metrics = dict(
        _as_mapping(summary_quality.get("core_live_metrics"))
    )
    result: Dict[str, Any] = {
        "target": target,
        "label_field": (
            "outcome.goal_within_15"
            if target == "next15"
            else "outcome.goal_to90_normal_time"
        ),
        "production_applied": False,
        "trained": False,
        "status": "collecting",
        "readiness_reasons": list(candidate_reasons),
        "counts": {
            "all": all_counts,
            "span_days": round(target_span_days, 6),
            **{
                name: _class_fixture_counts(partition)
                for name, partition in partitions.items()
            },
        },
        "metrics": {
            name: {"baseline": base_metrics[name], "candidate": None}
            for name in partitions
        },
        "data_quality": {
            "next15": next15_quality,
            "core_live_metrics": core_live_metrics,
        },
        "preprocessor": None,
        "model": None,
        "calibration": {
            "status": "identity",
            "reason": "model_not_trained",
            "slope": 1.0,
            "intercept": 0.0,
        },
    }
    if candidate_reasons:
        return result

    preprocessor = _fit_preprocessor(partitions["train"], config)
    train_vectors = [
        _transform_features(
            _as_mapping(sample.get("numeric")),
            _as_mapping(sample.get("categorical")),
            preprocessor,
        )
        for sample in partitions["train"]
    ]
    model = _fit_residual_logistic(
        partitions["train"], train_vectors, config
    )
    calibration_raw = _predict_raw_samples(
        partitions["calibration"], preprocessor, model, config
    )
    calibration = _fit_platt(
        partitions["calibration"], calibration_raw, config
    )
    candidate_metrics: Dict[str, Any] = {}
    for name, partition in partitions.items():
        raw = _predict_raw_samples(partition, preprocessor, model, config)
        calibrated = [
            _apply_platt(probability, calibration, config.probability_epsilon)
            for probability in raw
        ]
        candidate_metrics[name] = _metrics(
            partition, calibrated, config.probability_epsilon
        )
        result["metrics"][name]["candidate"] = candidate_metrics[name]

    readiness_reasons: List[str] = []
    if all_counts["fixtures"] < config.offline_ready_min_fixtures:
        readiness_reasons.append("offline_min_fixtures")
    if all_counts["rows"] < config.offline_ready_min_rows:
        readiness_reasons.append("offline_min_rows")
    if (
        all_counts["positive_fixtures"]
        < config.offline_ready_min_class_fixtures
    ):
        readiness_reasons.append("offline_positive_class_fixtures")
    if (
        all_counts["negative_fixtures"]
        < config.offline_ready_min_class_fixtures
    ):
        readiness_reasons.append("offline_negative_class_fixtures")
    if target_span_days < config.offline_ready_min_span_days:
        readiness_reasons.append("offline_min_span_days")
    calibration_counts = _class_fixture_counts(partitions["calibration"])
    holdout_counts = _class_fixture_counts(partitions["holdout"])
    if calibration_counts["fixtures"] < config.offline_ready_min_calibration_fixtures:
        readiness_reasons.append("offline_calibration_fixtures")
    if holdout_counts["fixtures"] < config.offline_ready_min_holdout_fixtures:
        readiness_reasons.append("offline_holdout_fixtures")
    for name, counts in (
        ("calibration", calibration_counts),
        ("holdout", holdout_counts),
    ):
        if counts["positive_fixtures"] < config.offline_ready_min_positive_fixtures:
            readiness_reasons.append(f"offline_{name}_positive_class")
        if counts["negative_fixtures"] < config.offline_ready_min_negative_fixtures:
            readiness_reasons.append(f"offline_{name}_negative_class")

    if target == "next15":
        unknown_fraction = _finite_float(
            next15_quality.get("unknown_or_invalid_fraction")
        )
        if (
            unknown_fraction is None
            or unknown_fraction
            > config.offline_ready_max_next15_unknown_fraction
        ):
            readiness_reasons.append("offline_next15_unknown_fraction")

    core_availability = _finite_float(
        core_live_metrics.get("availability_fraction")
    )
    if (
        core_availability is None
        or core_availability < config.offline_ready_min_core_availability
    ):
        readiness_reasons.append("offline_core_live_metric_availability")

    holdout_baseline = base_metrics["holdout"]
    holdout_candidate = candidate_metrics["holdout"]
    baseline_logloss = _finite_float(holdout_baseline.get("log_loss"))
    candidate_logloss = _finite_float(holdout_candidate.get("log_loss"))
    baseline_brier = _finite_float(holdout_baseline.get("brier"))
    candidate_brier = _finite_float(holdout_candidate.get("brier"))
    logloss_improvement = (
        baseline_logloss - candidate_logloss
        if baseline_logloss is not None and candidate_logloss is not None
        else None
    )
    brier_improvement = (
        baseline_brier - candidate_brier
        if baseline_brier is not None and candidate_brier is not None
        else None
    )
    if baseline_logloss is None or candidate_logloss is None:
        readiness_reasons.append("offline_holdout_metrics")
    elif (
        logloss_improvement is None
        or logloss_improvement < config.readiness_min_logloss_improvement
    ):
        readiness_reasons.append("offline_logloss_improvement")
    if baseline_brier is None or candidate_brier is None:
        if "offline_holdout_metrics" not in readiness_reasons:
            readiness_reasons.append("offline_holdout_metrics")
    elif (
        brier_improvement is None
        or brier_improvement < config.readiness_min_brier_improvement
    ):
        readiness_reasons.append("offline_brier_improvement")

    result.update(
        {
            "trained": True,
            "status": "offline_ready" if not readiness_reasons else "collecting",
            "readiness_reasons": readiness_reasons,
            "preprocessor": preprocessor,
            "model": model,
            "calibration": calibration,
            "holdout_improvement": {
                "log_loss": (
                    round(logloss_improvement, 9)
                    if logloss_improvement is not None
                    else None
                ),
                "brier": (
                    round(brier_improvement, 9)
                    if brier_improvement is not None
                    else None
                ),
            },
        }
    )
    return result


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _with_checksum(artifact: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(artifact)
    payload.pop("checksum_sha256", None)
    payload["checksum_sha256"] = hashlib.sha256(_canonical_bytes(payload)).hexdigest()
    return payload


def _checksum_valid(artifact: Mapping[str, Any]) -> bool:
    checksum = str(artifact.get("checksum_sha256") or "")
    if not checksum:
        return False
    payload = dict(artifact)
    payload.pop("checksum_sha256", None)
    try:
        expected = hashlib.sha256(_canonical_bytes(payload)).hexdigest()
    except (TypeError, ValueError, OverflowError):
        return False
    return checksum == expected


def train_shadow_model(
    records: Iterable[Mapping[str, Any]],
    config: Optional[ShadowMLConfig] = None,
    now: Any = None,
) -> Dict[str, Any]:
    """Train a deterministic two-target shadow model artifact."""

    selected_config = config or ShadowMLConfig()
    if not isinstance(selected_config, ShadowMLConfig):
        raise TypeError("config must be ShadowMLConfig or None")
    materialized = list(records)
    summary = summarize_training_data(materialized)
    training_contract = _as_mapping(summary.get("training_contract"))
    eligible = _materialize_eligible(
        materialized,
        contract_key=str(training_contract.get("key") or "") or None,
    )
    split = _split_fixture_groups(eligible, selected_config)
    created_at = _normalise_now(now)
    targets = {
        target: _train_target(
            target,
            _build_target_samples(eligible, target),
            split,
            selected_config,
            summary,
        )
        for target in TARGET_NAMES
    }
    artifact: Dict[str, Any] = {
        "artifact_type": "shadow_ml_model",
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "created_at_utc": _iso_utc(created_at),
        "data_cutoff_utc": summary.get("last_observation_utc"),
        "training_data_hash": summary.get("training_data_hash"),
        "training_contract": (
            {
                "key": str(training_contract.get("key") or ""),
                "payload": dict(_as_mapping(training_contract.get("payload"))),
            }
            if training_contract
            else None
        ),
        "config": asdict(selected_config),
        "training_summary": summary,
        "splits": _split_metadata(split, eligible),
        "targets": targets,
        "status": (
            "offline_ready"
            if all(targets[name]["status"] == "offline_ready" for name in TARGET_NAMES)
            else "collecting"
        ),
        "shadow_only": True,
        "production_applied": False,
    }
    model_seed = hashlib.sha256(_canonical_bytes(artifact)).hexdigest()[:20]
    artifact["model_id"] = f"shadow_ml:{ALGORITHM_VERSION}:{model_seed}"
    return _with_checksum(artifact)


def _invalid_prediction(status: str, detail: Optional[str] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "status": status,
        "production_applied": False,
        "targets": {},
    }
    if detail:
        payload["detail"] = detail
    return payload


def _predict_shadow_artifact(
    artifact: Mapping[str, Any],
    observation: Mapping[str, Any],
    *,
    artifact_type: str,
    algorithm_version: str,
    feature_extractor: Callable[
        [Mapping[str, Any]], Tuple[Dict[str, float], Dict[str, str]]
    ],
    contract_extractor: Callable[[Mapping[str, Any]], Dict[str, Any]],
) -> Dict[str, Any]:
    """Shared fail-safe inference for strictly separated shadow families."""

    if not isinstance(artifact, Mapping):
        return _invalid_prediction("invalid_artifact", "not_mapping")
    if (
        artifact.get("artifact_type") != artifact_type
        or _safe_int(artifact.get("schema_version")) != ARTIFACT_SCHEMA_VERSION
        or artifact.get("algorithm_version") != algorithm_version
        or not _checksum_valid(artifact)
    ):
        return _invalid_prediction("invalid_artifact", "schema_or_checksum")
    if not isinstance(observation, Mapping):
        return _invalid_prediction("invalid_observation", "not_mapping")

    training_contract = _as_mapping(artifact.get("training_contract"))
    training_contract_key = str(training_contract.get("key") or "")
    any_trained_target = any(
        bool(_as_mapping(target).get("trained"))
        for target in _as_mapping(artifact.get("targets")).values()
    )
    if not training_contract_key and any_trained_target:
        return _invalid_prediction("invalid_artifact", "missing_training_contract")
    if training_contract_key:
        observation_contract = contract_extractor(observation)
        if str(observation_contract.get("key") or "") != training_contract_key:
            mismatch = _invalid_prediction("invalid_observation")
            mismatch["reason"] = "feature_contract_mismatch"
            mismatch["expected_feature_contract"] = training_contract_key
            mismatch["actual_feature_contract"] = observation_contract.get("key")
            return mismatch

    config_payload = _as_mapping(artifact.get("config"))
    try:
        probability_epsilon = float(
            config_payload.get("probability_epsilon")
            or ShadowMLConfig().probability_epsilon
        )
    except (TypeError, ValueError):
        probability_epsilon = ShadowMLConfig().probability_epsilon

    numeric, categorical = feature_extractor(observation)
    target_predictions: Dict[str, Any] = {}
    successful = 0
    trained_targets = 0
    for target in TARGET_NAMES:
        target_artifact = _as_mapping(_as_mapping(artifact.get("targets")).get(target))
        base_probability = _target_base_probability(observation, target)
        if not bool(target_artifact.get("trained")):
            target_predictions[target] = {
                "status": "not_trained",
                "readiness_status": str(target_artifact.get("status") or "collecting"),
                "base_probability_pct": (
                    round(base_probability * 100.0, 6)
                    if base_probability is not None
                    else None
                ),
                "production_applied": False,
            }
            continue
        trained_targets += 1
        if base_probability is None:
            target_predictions[target] = {
                "status": "unavailable",
                "reason": "missing_base_probability",
                "readiness_status": str(target_artifact.get("status") or "collecting"),
                "production_applied": False,
            }
            continue
        preprocessor = _as_mapping(target_artifact.get("preprocessor"))
        model = _as_mapping(target_artifact.get("model"))
        feature_names = list(preprocessor.get("feature_names") or [])
        coefficients = list(model.get("coefficients") or [])
        if not feature_names or len(coefficients) != len(feature_names):
            target_predictions[target] = {
                "status": "invalid_model",
                "reason": "feature_dimension",
                "readiness_status": str(target_artifact.get("status") or "collecting"),
                "production_applied": False,
            }
            continue
        if any(_finite_float(value) is None for value in coefficients):
            target_predictions[target] = {
                "status": "invalid_model",
                "reason": "non_finite_coefficients",
                "readiness_status": str(target_artifact.get("status") or "collecting"),
                "production_applied": False,
            }
            continue
        vector = _transform_features(numeric, categorical, preprocessor)
        if len(vector) != len(coefficients):
            target_predictions[target] = {
                "status": "invalid_model",
                "reason": "transformed_dimension",
                "readiness_status": str(target_artifact.get("status") or "collecting"),
                "production_applied": False,
            }
            continue
        raw_probability = _linear_probability(
            [float(value) for value in coefficients],
            float(model.get("intercept") or 0.0),
            vector,
            base_probability,
            probability_epsilon,
        )
        calibrated_probability = _apply_platt(
            raw_probability,
            _as_mapping(target_artifact.get("calibration")),
            probability_epsilon,
        )
        numeric_spec = _as_mapping(preprocessor.get("numeric"))
        missing_count = sum(name not in numeric for name in numeric_spec)
        target_predictions[target] = {
            "status": "ok",
            "readiness_status": str(target_artifact.get("status") or "collecting"),
            "base_probability_pct": round(base_probability * 100.0, 6),
            "raw_probability_pct": round(raw_probability * 100.0, 6),
            "calibrated_probability_pct": round(
                calibrated_probability * 100.0, 6
            ),
            "delta_pp": round(
                (calibrated_probability - base_probability) * 100.0, 6
            ),
            "missing_numeric_count": missing_count,
            "numeric_feature_count": len(numeric_spec),
            "missing_numeric_ratio": round(
                missing_count / max(1, len(numeric_spec)), 6
            ),
            "production_applied": False,
        }
        successful += 1

    if successful == len(TARGET_NAMES):
        status = "ok"
    elif successful:
        status = "partial"
    elif trained_targets:
        status = "unavailable"
    else:
        status = "not_trained"
    return {
        "status": status,
        "model_id": artifact.get("model_id"),
        "algorithm_version": artifact.get("algorithm_version"),
        "production_applied": False,
        "targets": target_predictions,
    }


def predict_shadow(
    artifact: Mapping[str, Any],
    observation: Mapping[str, Any],
) -> Dict[str, Any]:
    """Run fail-safe online inference without changing production state."""

    return _predict_shadow_artifact(
        artifact,
        observation,
        artifact_type="shadow_ml_model",
        algorithm_version=ALGORITHM_VERSION,
        feature_extractor=_feature_values,
        contract_extractor=_feature_contract,
    )
