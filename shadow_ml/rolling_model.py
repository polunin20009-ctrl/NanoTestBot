"""Strictly shadow-only challenger that adds frozen 5/10-minute dynamics.

The incumbent model deliberately ignores ``rolling_dynamics``.  This module
keeps that contract intact and builds a separate artifact family, feature
contract, and prediction API so the two models can be compared prospectively
without either one affecting production probabilities.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from . import model as core
from .rolling import ROLLING_DYNAMICS_SCHEMA_VERSION


ROLLING_ARTIFACT_TYPE = "shadow_ml_rolling_model"
ROLLING_ARTIFACT_ROLE = "rolling_challenger"
ROLLING_ALGORITHM_VERSION = "residual_logistic_stdlib_rolling_v2"
ROLLING_FEATURE_SCHEMA_VERSION = 2
ROLLING_FEATURE_PROFILE = "rolling_5m10m_v1"
ROLLING_FEATURE_CONTRACT_SCHEMA_VERSION = 1
ROLLING_WINDOWS = (5, 10)
ROLLING_OFFLINE_READY_MIN_WINDOW_ROW_FRACTION = 0.60

_ROLLING_METRICS = (
    "xg_total",
    "shots_on_target_total",
    "shots_in_box_total",
    "total_shots_total",
    "corners_total",
    "score_total",
    "pressure_index",
)


def _rolling_block_error(record: Mapping[str, Any]) -> Optional[str]:
    block = record.get("rolling_dynamics")
    if not isinstance(block, Mapping):
        return "rolling_missing"
    if core._safe_int(block.get("schema_version")) != ROLLING_DYNAMICS_SCHEMA_VERSION:
        return "rolling_schema"
    if block.get("production_applied") is not False:
        return "rolling_production_flag"
    if str(block.get("mode") or "") != "shadow_collection":
        return "rolling_mode"
    if not isinstance(block.get("windows"), Mapping):
        return "rolling_windows"
    return None


def rolling_feature_contract(record: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a contract that binds base semantics and rolling semantics."""

    block_error = _rolling_block_error(record)
    base_contract = core._feature_contract(record)
    payload = {
        "contract_schema_version": ROLLING_FEATURE_CONTRACT_SCHEMA_VERSION,
        "kind": "rolling_challenger",
        "base_feature_contract": {
            "key": str(base_contract.get("key") or ""),
            "payload": dict(core._as_mapping(base_contract.get("payload"))),
        },
        "feature_profile": ROLLING_FEATURE_PROFILE,
        "feature_schema_version": ROLLING_FEATURE_SCHEMA_VERSION,
        "rolling_dynamics_schema_version": ROLLING_DYNAMICS_SCHEMA_VERSION,
        "windows_minutes": list(ROLLING_WINDOWS),
    }
    if block_error:
        return {"key": "", "payload": payload, "error": block_error}
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "key": f"rolling_feature_contract:{hashlib.sha256(encoded).hexdigest()}",
        "payload": payload,
    }


def rolling_feature_values(
    record: Mapping[str, Any],
) -> Tuple[Dict[str, float], Dict[str, str]]:
    """Extract the incumbent allow-list plus a strict rolling allow-list.

    Baseline identifiers, provider errors, decisions, gates, Telegram fields,
    and outcomes are never read.  A real zero remains distinct from missing:
    every metric has an explicit availability flag while an unavailable value
    is omitted and handled by the existing train-only missing indicator.
    """

    numeric, categorical = core._feature_values(record)
    block = core._as_mapping(record.get("rolling_dynamics"))
    windows = core._as_mapping(block.get("windows"))

    for minutes in ROLLING_WINDOWS:
        prefix = f"rolling.{minutes}m"
        raw_window = windows.get(f"{minutes}m")
        window = core._as_mapping(raw_window)
        raw_status = str(window.get("status") or "").strip().lower()
        status = raw_status if raw_status in {"ok", "unavailable"} else "missing"
        is_ok = status == "ok"
        categorical[f"{prefix}.status"] = status
        numeric[f"{prefix}.window_available"] = float(is_ok)

        span = core._finite_float(window.get("actual_span_minutes"))
        if is_ok and span is not None and span > 0.0:
            numeric[f"{prefix}.actual_span_minutes"] = span
        crosses_halftime = window.get("crosses_halftime")
        if is_ok and isinstance(crosses_halftime, bool):
            numeric[f"{prefix}.crosses_halftime"] = float(crosses_halftime)

        availability = core._as_mapping(window.get("availability"))
        deltas = core._as_mapping(window.get("deltas"))
        rates = core._as_mapping(window.get("rates_per_minute"))
        for metric in _ROLLING_METRICS:
            available = is_ok and availability.get(metric) is True
            numeric[f"{prefix}.available.{metric}"] = float(available)
            if not available:
                continue
            delta = core._finite_float(deltas.get(metric))
            if delta is not None:
                numeric[f"{prefix}.delta.{metric}"] = delta
            rate = core._finite_float(rates.get(metric))
            if rate is not None:
                numeric[f"{prefix}.rate_per_minute.{metric}"] = rate

    return numeric, categorical


def rolling_prediction_input_fingerprint(record: Mapping[str, Any]) -> str:
    """Hash the exact base + 5/10-minute input consumed by the challenger."""

    return core.prediction_input_fingerprint(
        record,
        feature_extractor=rolling_feature_values,
        contract_extractor=rolling_feature_contract,
        profile=ROLLING_FEATURE_PROFILE,
    )


def _all_rolling_eligible_items(
    records: Sequence[Mapping[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, int], int]:
    excluded = {
        "stage": 0,
        "status": 0,
        "fixture_id": 0,
        "minute": 0,
        "timestamp": 0,
        "not_mapping": 0,
        "feature_contract": 0,
        "rolling_missing": 0,
        "rolling_schema": 0,
        "rolling_production_flag": 0,
        "rolling_mode": 0,
        "rolling_windows": 0,
    }
    eligible: List[Dict[str, Any]] = []
    seen: set[str] = set()
    duplicates = 0
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            excluded["not_mapping"] += 1
            continue
        record_id = core._record_identity(record, index)
        if record_id in seen:
            duplicates += 1
            continue
        seen.add(record_id)
        valid, fixture_id, timestamp, reason = core._eligible_record(record)
        if not valid:
            excluded[reason] += 1
            continue
        rolling_error = _rolling_block_error(record)
        if rolling_error:
            excluded[rolling_error] += 1
            continue
        eligible.append(
            {
                "record": record,
                "record_id": record_id,
                "fixture_id": int(fixture_id),  # type: ignore[arg-type]
                "timestamp": timestamp,
                "contract": rolling_feature_contract(record),
            }
        )
    return eligible, excluded, duplicates


def _select_contract(
    eligible: Sequence[Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not eligible:
        return None
    newest = max(
        eligible,
        key=lambda item: (item["timestamp"], str(item.get("record_id") or "")),
    )
    contract = core._as_mapping(newest.get("contract"))
    key = str(contract.get("key") or "")
    if not key:
        return None
    return {
        "key": key,
        "payload": dict(core._as_mapping(contract.get("payload"))),
    }


def _materialize_rolling_eligible(
    records: Sequence[Mapping[str, Any]],
    contract_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    eligible, _, _ = _all_rolling_eligible_items(records)
    selected_key = str(contract_key or "")
    if not selected_key:
        selected_key = str((_select_contract(eligible) or {}).get("key") or "")
    return [
        item
        for item in eligible
        if str(core._as_mapping(item.get("contract")).get("key") or "")
        == selected_key
    ]


def _coverage(eligible: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    fixture_ids = {int(item["fixture_id"]) for item in eligible}
    window_rows: Dict[str, int] = {f"{minutes}m": 0 for minutes in ROLLING_WINDOWS}
    window_fixtures: Dict[str, set[int]] = {
        f"{minutes}m": set() for minutes in ROLLING_WINDOWS
    }
    statuses: Dict[str, Dict[str, int]] = {
        f"{minutes}m": {} for minutes in ROLLING_WINDOWS
    }
    at_least_one_rows = 0
    both_rows = 0
    at_least_one_fixtures: set[int] = set()
    both_fixtures: set[int] = set()
    for item in eligible:
        record = core._as_mapping(item.get("record"))
        windows = core._as_mapping(
            core._as_mapping(record.get("rolling_dynamics")).get("windows")
        )
        fixture_id = int(item["fixture_id"])
        ok_flags = []
        for minutes in ROLLING_WINDOWS:
            name = f"{minutes}m"
            window = core._as_mapping(windows.get(name))
            status = str(window.get("status") or "missing").lower()
            statuses[name][status] = statuses[name].get(status, 0) + 1
            is_ok = status == "ok"
            ok_flags.append(is_ok)
            if is_ok:
                window_rows[name] += 1
                window_fixtures[name].add(fixture_id)
        if any(ok_flags):
            at_least_one_rows += 1
            at_least_one_fixtures.add(fixture_id)
        if all(ok_flags):
            both_rows += 1
            both_fixtures.add(fixture_id)
    total_rows = len(eligible)
    return {
        "schema_version": ROLLING_DYNAMICS_SCHEMA_VERSION,
        "rows": total_rows,
        "fixtures": len(fixture_ids),
        "windows": {
            name: {
                "available_rows": window_rows[name],
                "available_fixtures": len(window_fixtures[name]),
                "available_fraction": (
                    round(window_rows[name] / total_rows, 9) if total_rows else None
                ),
                "status_counts": dict(sorted(statuses[name].items())),
            }
            for name in window_rows
        },
        "at_least_one_window": {
            "rows": at_least_one_rows,
            "fixtures": len(at_least_one_fixtures),
        },
        "both_windows": {
            "rows": both_rows,
            "fixtures": len(both_fixtures),
        },
    }


def _rolling_training_data_hash(eligible: Sequence[Mapping[str, Any]]) -> str:
    rows: List[Any] = []
    for item in eligible:
        numeric, categorical = rolling_feature_values(item["record"])
        record = item["record"]
        feature_payload = {
            "numeric": {key: numeric[key] for key in sorted(numeric)},
            "categorical": {
                key: categorical[key] for key in sorted(categorical)
            },
            "base_probabilities": {
                target: core._target_base_probability(record, target)
                for target in core.TARGET_NAMES
            },
        }
        feature_digest = hashlib.sha256(
            json.dumps(
                feature_payload,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        rows.append(
            [
                str(item["record_id"]),
                feature_digest,
            ]
        )
    rows.sort(key=lambda value: value[0])
    payload = {
        "base_training_data_hash": core._training_data_hash(eligible),
        "feature_profile": ROLLING_FEATURE_PROFILE,
        "rows": rows,
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


def summarize_rolling_training_data(
    records: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Summarize only prospectively frozen rolling-compatible rows."""

    materialized = list(records)
    all_eligible, excluded, duplicates = _all_rolling_eligible_items(materialized)
    training_contract = _select_contract(all_eligible)
    selected_key = str((training_contract or {}).get("key") or "")
    eligible = [
        item
        for item in all_eligible
        if str(core._as_mapping(item.get("contract")).get("key") or "")
        == selected_key
    ] if selected_key else []
    incompatible = len(all_eligible) - len(eligible)
    excluded["feature_contract"] = incompatible

    selected_records = [item["record"] for item in eligible]
    base_summary = core.summarize_training_data(selected_records)
    cohorts: Dict[str, Dict[str, Any]] = {}
    for item in all_eligible:
        contract = core._as_mapping(item.get("contract"))
        key = str(contract.get("key") or "")
        cohort = cohorts.setdefault(
            key,
            {
                "key": key,
                "payload": dict(core._as_mapping(contract.get("payload"))),
                "rows": 0,
                "fixture_ids": set(),
                "newest_observation_utc": None,
            },
        )
        cohort["rows"] += 1
        cohort["fixture_ids"].add(int(item["fixture_id"]))
        timestamp = core._iso_utc(item["timestamp"])
        if not cohort["newest_observation_utc"] or timestamp > cohort["newest_observation_utc"]:
            cohort["newest_observation_utc"] = timestamp

    summary = dict(base_summary)
    summary.update(
        {
            "records_seen": len(materialized),
            "eligible_records_before_contract_filter": len(all_eligible),
            "eligible_records": len(eligible),
            "duplicate_records_skipped": duplicates,
            "training_contract": training_contract,
            "feature_contract_cohort_count": len(cohorts),
            "incompatible_rows_excluded": incompatible,
            "incompatible_cohort_count": sum(key != selected_key for key in cohorts),
            "feature_contract_cohorts": [
                {
                    "key": cohort["key"],
                    "payload": cohort["payload"],
                    "rows": cohort["rows"],
                    "fixtures": len(cohort["fixture_ids"]),
                    "newest_observation_utc": cohort["newest_observation_utc"],
                    "selected": key == selected_key,
                }
                for key, cohort in sorted(cohorts.items())
            ],
            "excluded": excluded,
            "training_data_hash": _rolling_training_data_hash(eligible),
            "feature_profile": ROLLING_FEATURE_PROFILE,
        }
    )
    data_quality = dict(core._as_mapping(summary.get("data_quality")))
    data_quality["rolling_dynamics"] = _coverage(eligible)
    summary["data_quality"] = data_quality
    return summary


def _apply_rolling_readiness(
    target_artifact: Dict[str, Any],
    training_summary: Mapping[str, Any],
    config: core.ShadowMLConfig,
) -> Dict[str, Any]:
    """Prevent readiness unless the model saw enough real rolling windows."""

    rolling_quality = dict(
        core._as_mapping(
            core._as_mapping(training_summary.get("data_quality")).get(
                "rolling_dynamics"
            )
        )
    )
    data_quality = dict(core._as_mapping(target_artifact.get("data_quality")))
    data_quality["rolling_dynamics"] = rolling_quality
    target_artifact["data_quality"] = data_quality
    if not bool(target_artifact.get("trained")):
        return target_artifact

    reasons = list(target_artifact.get("readiness_reasons") or [])
    both_fixtures = core._safe_int(
        core._as_mapping(rolling_quality.get("both_windows")).get("fixtures")
    )
    if (
        both_fixtures is None
        or both_fixtures < config.offline_ready_min_fixtures
    ):
        reasons.append("offline_rolling_both_windows_fixtures")

    windows = core._as_mapping(rolling_quality.get("windows"))
    for minutes in ROLLING_WINDOWS:
        fraction = core._finite_float(
            core._as_mapping(windows.get(f"{minutes}m")).get(
                "available_fraction"
            )
        )
        if (
            fraction is None
            or fraction < ROLLING_OFFLINE_READY_MIN_WINDOW_ROW_FRACTION
        ):
            reasons.append(f"offline_rolling_{minutes}m_coverage")

    target_artifact["readiness_reasons"] = list(dict.fromkeys(reasons))
    target_artifact["status"] = (
        "offline_ready"
        if not target_artifact["readiness_reasons"]
        else "collecting"
    )
    return target_artifact


def train_shadow_rolling_model(
    records: Iterable[Mapping[str, Any]],
    config: Optional[core.ShadowMLConfig] = None,
    now: Any = None,
) -> Dict[str, Any]:
    """Train the deterministic rolling challenger as a separate artifact."""

    selected_config = config or core.ShadowMLConfig()
    if not isinstance(selected_config, core.ShadowMLConfig):
        raise TypeError("config must be ShadowMLConfig or None")
    materialized = list(records)
    summary = summarize_rolling_training_data(materialized)
    training_contract = core._as_mapping(summary.get("training_contract"))
    eligible = _materialize_rolling_eligible(
        materialized,
        contract_key=str(training_contract.get("key") or "") or None,
    )
    split = core._split_fixture_groups(eligible, selected_config)
    created_at = core._normalise_now(now)
    targets = {}
    for target in core.TARGET_NAMES:
        target_artifact = core._train_target(
            target,
            core._build_target_samples(
                eligible,
                target,
                feature_extractor=rolling_feature_values,
            ),
            split,
            selected_config,
            summary,
        )
        targets[target] = _apply_rolling_readiness(
            target_artifact,
            summary,
            selected_config,
        )
    artifact: Dict[str, Any] = {
        "artifact_type": ROLLING_ARTIFACT_TYPE,
        "artifact_role": ROLLING_ARTIFACT_ROLE,
        "schema_version": core.ARTIFACT_SCHEMA_VERSION,
        "algorithm_version": ROLLING_ALGORITHM_VERSION,
        "feature_schema_version": ROLLING_FEATURE_SCHEMA_VERSION,
        "feature_profile": ROLLING_FEATURE_PROFILE,
        "rolling_readiness_policy": {
            "min_both_windows_fixtures": (
                selected_config.offline_ready_min_fixtures
            ),
            "min_each_window_row_fraction": (
                ROLLING_OFFLINE_READY_MIN_WINDOW_ROW_FRACTION
            ),
        },
        "created_at_utc": core._iso_utc(created_at),
        "data_cutoff_utc": summary.get("last_observation_utc"),
        "training_data_hash": summary.get("training_data_hash"),
        "training_contract": (
            {
                "key": str(training_contract.get("key") or ""),
                "payload": dict(core._as_mapping(training_contract.get("payload"))),
            }
            if training_contract
            else None
        ),
        "config": asdict(selected_config),
        "training_summary": summary,
        "splits": core._split_metadata(split, eligible),
        "targets": targets,
        "status": (
            "offline_ready"
            if all(
                targets[name]["status"] == "offline_ready"
                for name in core.TARGET_NAMES
            )
            else "collecting"
        ),
        "shadow_only": True,
        "production_applied": False,
    }
    seed = hashlib.sha256(core._canonical_bytes(artifact)).hexdigest()[:20]
    artifact["model_id"] = f"shadow_ml_rolling:{ROLLING_ALGORITHM_VERSION}:{seed}"
    return core._with_checksum(artifact)


def predict_shadow_rolling(
    artifact: Mapping[str, Any],
    observation: Mapping[str, Any],
) -> Dict[str, Any]:
    """Predict with the challenger while remaining strictly fail-safe."""

    prediction = core._predict_shadow_artifact(
        artifact,
        observation,
        artifact_type=ROLLING_ARTIFACT_TYPE,
        algorithm_version=ROLLING_ALGORITHM_VERSION,
        feature_extractor=rolling_feature_values,
        contract_extractor=rolling_feature_contract,
    )
    prediction["artifact_role"] = ROLLING_ARTIFACT_ROLE
    prediction["feature_profile"] = ROLLING_FEATURE_PROFILE
    return prediction
