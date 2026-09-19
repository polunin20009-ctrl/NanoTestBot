from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Mapping, Optional, Sequence


ARM_CONTROL = "control_current_filter"
ARM_FULL_SLICES = "full_slices_5m_10m"
ARM_FULL_SLICES_GOALS_LE2 = "full_slices_goals_le2"
ARM_FULL_SLICES_GOALS_LE2_CLOSE = "full_slices_goals_le2_close"
ARM_ROLLING_ML_CONFIRM_75 = "rolling_ml_confirm_75"

PASS = "pass"
FAIL = "fail"
UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class ArmDefinition:
    arm_id: str
    version: str
    prerequisite_arm: Optional[str]
    conditions: tuple[str, ...]
    description: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "arm_id": self.arm_id,
            "version": self.version,
            "prerequisite_arm": self.prerequisite_arm,
            "conditions": list(self.conditions),
            "description": self.description,
        }


@dataclass(frozen=True)
class RuleSet:
    schema_version: int
    version: str
    semantics: str
    control_filter_version: str
    min_prob_to90: float
    min_reputation_delta_to90_pp: float
    min_adjusted_intensity: float
    min_season_context_factor: float
    max_goals_at_snapshot: int
    max_score_difference_abs: int
    rolling_ml_min_probability_pct: float
    arms: tuple[ArmDefinition, ...]

    def manifest(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "schema_version": self.schema_version,
            "version": self.version,
            "semantics": self.semantics,
            "control_filter_version": self.control_filter_version,
            "thresholds": {
                "min_prob_to90": self.min_prob_to90,
                "min_reputation_delta_to90_pp": (
                    self.min_reputation_delta_to90_pp
                ),
                "min_adjusted_intensity": self.min_adjusted_intensity,
                "min_season_context_factor": self.min_season_context_factor,
                "max_goals_at_snapshot": self.max_goals_at_snapshot,
                "max_score_difference_abs": self.max_score_difference_abs,
                "rolling_ml_min_probability_pct": (
                    self.rolling_ml_min_probability_pct
                ),
            },
            "arms": [arm.as_dict() for arm in self.arms],
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        payload["manifest_hash"] = hashlib.sha256(encoded).hexdigest()[:20]
        return payload


DEFAULT_RULESET = RuleSet(
    schema_version=1,
    version="candidate_at_first_control_allow_v3",
    semantics="candidate_at_first_control_allow",
    control_filter_version="base_rep15_int055_season102_p90_75_v1",
    min_prob_to90=75.0,
    min_reputation_delta_to90_pp=1.5,
    min_adjusted_intensity=0.55,
    min_season_context_factor=1.02,
    max_goals_at_snapshot=2,
    max_score_difference_abs=1,
    rolling_ml_min_probability_pct=75.0,
    arms=(
        ArmDefinition(
            ARM_CONTROL,
            "control_current_filter_v1",
            None,
            (
                "publication_context_passed",
                "prob_to90>=75",
                "reputation_delta_to90_pp>=1.5",
                "adjusted_intensity>=0.55",
                "season_context_factor>=1.02",
            ),
            "Current production publication filter, evaluated in shadow.",
        ),
        ArmDefinition(
            ARM_FULL_SLICES,
            "full_slices_5m_10m_v1",
            ARM_CONTROL,
            ("rolling_window_5m.status=ok", "rolling_window_10m.status=ok"),
            "Control plus both real rolling windows with status=ok.",
        ),
        ArmDefinition(
            ARM_FULL_SLICES_GOALS_LE2,
            "full_slices_goals_le2_v1",
            ARM_FULL_SLICES,
            ("goals_at_snapshot<=2",),
            "Full slices plus no more than two goals at the snapshot.",
        ),
        ArmDefinition(
            ARM_FULL_SLICES_GOALS_LE2_CLOSE,
            "full_slices_goals_le2_close_v1",
            ARM_FULL_SLICES_GOALS_LE2,
            ("abs(score_home-score_away)<=1",),
            (
                "Full slices, no more than two goals, and no team leading "
                "by more than one goal at the frozen first-control snapshot."
            ),
        ),
        ArmDefinition(
            ARM_ROLLING_ML_CONFIRM_75,
            "rolling_ml_confirm_75_v1",
            ARM_FULL_SLICES_GOALS_LE2,
            (
                "rolling_ml.to90.status=ok",
                "rolling_ml.to90.calibrated_probability_pct>=75",
            ),
            "Previous arm plus a causal rolling-ML confirmation.",
        ),
    ),
)


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


def _first_value(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _iso_timestamp(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        return parsed if parsed.utcoffset() is not None else None
    except ValueError:
        return None


def _result(
    *,
    arm: ArmDefinition,
    status: str,
    reason: str,
    failed: Sequence[str] = (),
    unavailable: Sequence[str] = (),
    inputs: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "arm_id": arm.arm_id,
        "arm_version": arm.version,
        "prerequisite_arm": arm.prerequisite_arm,
        "evaluation_status": status,
        "eligible": status == PASS,
        "reason": reason,
        "failed_conditions": list(failed),
        "unavailable_conditions": list(unavailable),
        "inputs": dict(inputs or {}),
    }


def _cascade(
    parent: Mapping[str, Any],
    arm: ArmDefinition,
    *,
    own_status: str,
    own_reason: str,
    failed: Sequence[str] = (),
    unavailable: Sequence[str] = (),
    inputs: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    parent_status = parent.get("evaluation_status")
    if parent_status == FAIL:
        return _result(
            arm=arm,
            status=FAIL,
            reason=f"prerequisite_failed:{arm.prerequisite_arm}",
            failed=(f"prerequisite:{arm.prerequisite_arm}",),
            inputs=inputs,
        )
    if parent_status == UNAVAILABLE:
        return _result(
            arm=arm,
            status=UNAVAILABLE,
            reason=f"prerequisite_unavailable:{arm.prerequisite_arm}",
            unavailable=(f"prerequisite:{arm.prerequisite_arm}",),
            inputs=inputs,
        )
    return _result(
        arm=arm,
        status=own_status,
        reason=own_reason,
        failed=failed,
        unavailable=unavailable,
        inputs=inputs,
    )


def _control_inputs(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    channel_filter = _mapping(snapshot.get("channel_signal_filter"))
    probabilities = _mapping(snapshot.get("probabilities"))
    factors = _mapping(snapshot.get("factors"))
    features = _mapping(snapshot.get("features"))
    match = _mapping(snapshot.get("match"))
    policy = _mapping(snapshot.get("publication_policy"))
    gates = _mapping(snapshot.get("gates"))
    decision = _mapping(snapshot.get("decision"))
    config = _mapping(snapshot.get("config"))
    runtime = _mapping(snapshot.get("runtime"))
    factor_versions = _mapping(
        _first_value(config.get("factor_versions"), runtime.get("factor_versions"))
    )

    return {
        "filter_version": _first_value(
            channel_filter.get("version"),
            policy.get("filter_version"),
            factor_versions.get("channel_signal_filter"),
        ),
        "publication_context_passed": _first_value(
            policy.get("publication_context_passed"),
            gates.get("publication_context_passed"),
        ),
        "active_publication_allow": _first_value(
            policy.get("publication_allow"),
            decision.get("active_publication_allow"),
            gates.get("active_publication_allow"),
        ),
        "embedded_filter_passed": channel_filter.get("passed"),
        "prob_to90": _first_value(
            channel_filter.get("prob_to90"), probabilities.get("prob_to90")
        ),
        "reputation_base_prob_to90": _first_value(
            channel_filter.get("reputation_base_prob_to90"),
            probabilities.get("reputation_base_prob_to90"),
        ),
        "reputation_adjusted_prob_to90": _first_value(
            channel_filter.get("reputation_adjusted_prob_to90"),
            probabilities.get("reputation_adjusted_prob_to90"),
            probabilities.get("prob_to90"),
        ),
        "adjusted_intensity": _first_value(
            channel_filter.get("adjusted_intensity"),
            factors.get("adjusted_intensity"),
            features.get("adjusted_intensity"),
        ),
        "season_context_factor": _first_value(
            channel_filter.get("season_context_factor"),
            factors.get("season_context_factor_45p"),
            features.get("season_context_factor"),
        ),
        "score_home": _first_value(
            channel_filter.get("score_home"), match.get("score_home")
        ),
        "score_away": _first_value(
            channel_filter.get("score_away"), match.get("score_away")
        ),
    }


def _evaluate_control(
    snapshot: Mapping[str, Any], ruleset: RuleSet, arm: ArmDefinition
) -> Dict[str, Any]:
    raw = _control_inputs(snapshot)
    missing: list[str] = []
    if raw["filter_version"] is None:
        missing.append("channel_signal_filter.version")
    if not isinstance(raw["publication_context_passed"], bool):
        missing.append("publication_context_passed")
    if not isinstance(raw["embedded_filter_passed"], bool):
        missing.append("embedded_filter_passed")
    if not isinstance(raw["active_publication_allow"], bool):
        missing.append("active_publication_allow")

    numeric_names = (
        "prob_to90",
        "reputation_base_prob_to90",
        "reputation_adjusted_prob_to90",
        "adjusted_intensity",
        "season_context_factor",
        "score_home",
        "score_away",
    )
    numeric = {name: _finite(raw[name]) for name in numeric_names}
    missing.extend(name for name, value in numeric.items() if value is None)
    evidence = {**raw, **numeric}
    evidence["expected_filter_version"] = ruleset.control_filter_version

    if missing:
        return _result(
            arm=arm,
            status=UNAVAILABLE,
            reason="control_inputs_unavailable",
            unavailable=missing,
            inputs=evidence,
        )
    if raw["filter_version"] != ruleset.control_filter_version:
        return _result(
            arm=arm,
            status=UNAVAILABLE,
            reason="incompatible_control_filter_version",
            unavailable=("channel_signal_filter.version",),
            inputs=evidence,
        )

    home = numeric["score_home"]
    away = numeric["score_away"]
    assert home is not None and away is not None
    invalid = []
    if home < 0 or not home.is_integer():
        invalid.append("score_home")
    if away < 0 or not away.is_integer():
        invalid.append("score_away")
    for name in ("prob_to90", "reputation_base_prob_to90", "reputation_adjusted_prob_to90"):
        value = numeric[name]
        if value is None or value < 0 or value > 100:
            invalid.append(name)
    for name in ("adjusted_intensity", "season_context_factor"):
        value = numeric[name]
        if value is None or value < 0:
            invalid.append(name)
    if invalid:
        return _result(
            arm=arm,
            status=UNAVAILABLE,
            reason="invalid_control_inputs",
            unavailable=invalid,
            inputs=evidence,
        )

    base = numeric["reputation_base_prob_to90"]
    adjusted = numeric["reputation_adjusted_prob_to90"]
    probability = numeric["prob_to90"]
    intensity = numeric["adjusted_intensity"]
    season = numeric["season_context_factor"]
    assert None not in (base, adjusted, probability, intensity, season)
    reputation_delta = round(float(adjusted) - float(base), 6)
    goals = int(home) + int(away)
    evidence.update(
        {
            "reputation_delta_to90_pp": reputation_delta,
            "goals_at_snapshot": goals,
            "required": {
                "min_prob_to90": ruleset.min_prob_to90,
                "min_reputation_delta_to90_pp": ruleset.min_reputation_delta_to90_pp,
                "min_adjusted_intensity": ruleset.min_adjusted_intensity,
                "min_season_context_factor": ruleset.min_season_context_factor,
            },
        }
    )
    filter_failed = []
    if float(probability) < ruleset.min_prob_to90:
        filter_failed.append("prob_to90")
    if reputation_delta < ruleset.min_reputation_delta_to90_pp:
        filter_failed.append("reputation_delta_to90_pp")
    if float(intensity) < ruleset.min_adjusted_intensity:
        filter_failed.append("adjusted_intensity")
    if float(season) < ruleset.min_season_context_factor:
        filter_failed.append("season_context_factor")
    recomputed_filter_passed = not filter_failed
    recomputed_active_allow = bool(
        raw["publication_context_passed"] is True
        and recomputed_filter_passed
    )
    failed = list(filter_failed)
    if raw["publication_context_passed"] is not True:
        failed.insert(0, "publication_context_passed")
    evidence["recomputed_filter_passed"] = recomputed_filter_passed
    evidence["recomputed_active_publication_allow"] = (
        recomputed_active_allow
    )

    embedded = raw["embedded_filter_passed"]
    active = raw["active_publication_allow"]
    if isinstance(embedded, bool) and embedded != recomputed_filter_passed:
        return _result(
            arm=arm,
            status=UNAVAILABLE,
            reason="embedded_filter_contract_mismatch",
            unavailable=("embedded_filter_passed",),
            inputs=evidence,
        )
    if isinstance(active, bool) and active != recomputed_active_allow:
        return _result(
            arm=arm,
            status=UNAVAILABLE,
            reason="active_publication_contract_mismatch",
            unavailable=("active_publication_allow",),
            inputs=evidence,
        )
    return _result(
        arm=arm,
        status=PASS if recomputed_active_allow else FAIL,
        reason="pass" if recomputed_active_allow else "+".join(failed),
        failed=failed,
        inputs=evidence,
    )


def _prediction_evidence(
    prediction: Optional[Mapping[str, Any]],
    snapshot: Mapping[str, Any],
    *,
    expected_record_type: str,
    max_live_lag_seconds: float,
) -> Dict[str, Any]:
    if not isinstance(prediction, Mapping):
        return {"available": False, "usable": False, "reason": "missing"}
    target = _mapping(_mapping(prediction.get("predictions")).get("to90"))
    probability = _finite(target.get("calibrated_probability_pct"))
    source_created_at_utc = snapshot.get("created_at_utc")
    source_time = _iso_timestamp(source_created_at_utc)
    prediction_time = _iso_timestamp(prediction.get("created_at_utc"))
    recorded_observation_time = _iso_timestamp(
        prediction.get("observation_created_at_utc")
    )
    model_created_time = _iso_timestamp(prediction.get("model_created_at_utc"))
    model_cutoff_time = _iso_timestamp(prediction.get("model_data_cutoff_utc"))
    lag_seconds: Optional[float] = None
    if source_time is not None and prediction_time is not None:
        try:
            lag_seconds = (prediction_time - source_time).total_seconds()
        except TypeError:
            lag_seconds = None
    record_type = str(prediction.get("record_type") or "")
    type_ok = record_type == expected_record_type
    time_ok = (
        lag_seconds is not None
        and 0.0 <= lag_seconds <= float(max_live_lag_seconds)
    )
    source_id = str(
        snapshot.get("observation_id") or snapshot.get("decision_id") or ""
    )
    prediction_source_id = str(
        prediction.get("observation_id") or prediction.get("decision_id") or ""
    )
    source_fixture = snapshot.get("fixture_id")
    prediction_fixture = prediction.get("fixture_id")
    source_minute = _finite(snapshot.get("minute"))
    prediction_minute = _finite(prediction.get("minute"))
    identity_ok = bool(
        source_id
        and source_id == prediction_source_id
        and source_fixture is not None
        and not isinstance(source_fixture, bool)
        and prediction_fixture is not None
        and not isinstance(prediction_fixture, bool)
        and str(source_fixture) == str(prediction_fixture)
        and source_minute is not None
        and prediction_minute is not None
        and source_minute.is_integer()
        and prediction_minute.is_integer()
        and int(source_minute) == int(prediction_minute)
        and source_time is not None
        and recorded_observation_time is not None
        and source_time == recorded_observation_time
    )
    model_timing_ok = bool(
        source_time is not None
        and model_created_time is not None
        and model_cutoff_time is not None
        and model_created_time < source_time
        and model_cutoff_time < source_time
        and model_cutoff_time <= model_created_time
    )
    shadow_contract_ok = bool(
        prediction.get("shadow_only") is True
        and prediction.get("production_applied") is False
        and target.get("production_applied") is False
    )
    probability_ok = probability is not None and 0.0 <= probability <= 100.0
    status = str(target.get("status") or "").lower()
    unusable_reasons: list[str] = []
    if not type_ok:
        unusable_reasons.append("wrong_record_type")
    if source_time is None:
        unusable_reasons.append("source_created_at_invalid")
    if prediction_time is None:
        unusable_reasons.append("prediction_created_at_invalid")
    elif not time_ok:
        unusable_reasons.append("outside_live_prediction_window")
    if recorded_observation_time is None:
        unusable_reasons.append("observation_created_at_missing_or_invalid")
    if not identity_ok:
        unusable_reasons.append("observation_identity_mismatch")
    if model_created_time is None:
        unusable_reasons.append("model_created_at_missing_or_invalid")
    if model_cutoff_time is None:
        unusable_reasons.append("model_data_cutoff_missing_or_invalid")
    if (
        source_time is not None
        and model_created_time is not None
        and model_created_time >= source_time
    ):
        unusable_reasons.append("model_created_at_not_before_observation")
    if (
        source_time is not None
        and model_cutoff_time is not None
        and model_cutoff_time >= source_time
    ):
        unusable_reasons.append("model_cutoff_not_before_observation")
    if (
        model_created_time is not None
        and model_cutoff_time is not None
        and model_cutoff_time > model_created_time
    ):
        unusable_reasons.append("model_cutoff_after_model_created")
    if prediction.get("shadow_only") is not True:
        unusable_reasons.append("prediction_shadow_only_not_true")
    if prediction.get("production_applied") is not False:
        unusable_reasons.append("record_production_applied_not_false")
    if target.get("production_applied") is not False:
        unusable_reasons.append("target_production_applied_not_false")
    if status != "ok":
        unusable_reasons.append("target_status_not_ok")
    if not probability_ok:
        unusable_reasons.append("calibrated_probability_unavailable")
    usable = not unusable_reasons and model_timing_ok
    reason = "ok" if usable else unusable_reasons[0]
    return {
        "available": True,
        "usable": usable,
        "reason": reason,
        "unusable_reasons": unusable_reasons,
        "record_type": record_type or None,
        "prediction_key": prediction.get("prediction_key"),
        "model_id": prediction.get("model_id"),
        "created_at_utc": prediction.get("created_at_utc"),
        "lag_seconds": round(lag_seconds, 6) if lag_seconds is not None else None,
        "observation_created_at_utc": prediction.get("observation_created_at_utc"),
        "model_created_at_utc": prediction.get("model_created_at_utc"),
        "model_data_cutoff_utc": prediction.get("model_data_cutoff_utc"),
        "identity_ok": identity_ok,
        "model_timing_ok": model_timing_ok,
        "shadow_contract_ok": shadow_contract_ok,
        "to90_status": status or None,
        "to90_readiness_status": target.get("readiness_status"),
        "to90_calibrated_probability_pct": probability,
        "production_applied": bool(target.get("production_applied", False)),
    }


def evaluate_candidate_arms(
    snapshot: Mapping[str, Any],
    *,
    static_prediction: Optional[Mapping[str, Any]] = None,
    rolling_prediction: Optional[Mapping[str, Any]] = None,
    ruleset: RuleSet = DEFAULT_RULESET,
    max_live_prediction_lag_seconds: float = 300.0,
) -> Dict[str, Any]:
    """Evaluate all raw arms at one immutable source snapshot.

    Cohort claiming (the first control ALLOW per fixture) is intentionally done
    by the journal engine, not by this pure function.
    """
    definitions = {arm.arm_id: arm for arm in ruleset.arms}
    control = _evaluate_control(snapshot, ruleset, definitions[ARM_CONTROL])

    rolling = _mapping(snapshot.get("rolling_dynamics"))
    windows = _mapping(rolling.get("windows"))
    window_5m = _mapping(windows.get("5m"))
    window_10m = _mapping(windows.get("10m"))
    slice_inputs = {
        "rolling_schema_version": rolling.get("schema_version"),
        "available_windows": rolling.get("available_windows"),
        "window_5m_status": window_5m.get("status"),
        "window_5m_reason": window_5m.get("reason"),
        "window_5m_actual_span_minutes": window_5m.get("actual_span_minutes"),
        "window_10m_status": window_10m.get("status"),
        "window_10m_reason": window_10m.get("reason"),
        "window_10m_actual_span_minutes": window_10m.get("actual_span_minutes"),
        "mode": rolling.get("mode"),
        "production_applied": rolling.get("production_applied"),
    }
    rolling_contract_failures: list[str] = []
    schema_version = rolling.get("schema_version")
    if type(schema_version) is not int or schema_version != 1:
        rolling_contract_failures.append("rolling_schema_version")
    if rolling.get("mode") != "shadow_collection":
        rolling_contract_failures.append("rolling_mode")
    if rolling.get("production_applied") is not False:
        rolling_contract_failures.append("rolling_production_applied")
    if type(rolling.get("available_windows")) is not int or rolling.get(
        "available_windows"
    ) != 2:
        rolling_contract_failures.append("available_windows")
    for name, requested, window in (
        ("5m", 5, window_5m),
        ("10m", 10, window_10m),
    ):
        if str(window.get("status") or "").lower() != "ok":
            rolling_contract_failures.append(f"rolling_window_{name}.status")
        if (
            type(window.get("requested_window_minutes")) is not int
            or window.get("requested_window_minutes") != requested
        ):
            rolling_contract_failures.append(
                f"rolling_window_{name}.requested_window_minutes"
            )
        span = _finite(window.get("actual_span_minutes"))
        if (
            span is None
            or not span.is_integer()
            or not requested <= int(span) <= requested + 2
        ):
            rolling_contract_failures.append(
                f"rolling_window_{name}.actual_span_minutes"
            )
        activity_count = _finite(window.get("available_activity_metric_count"))
        if (
            activity_count is None
            or not activity_count.is_integer()
            or activity_count < 1
        ):
            rolling_contract_failures.append(
                f"rolling_window_{name}.available_activity_metric_count"
            )
    slices = _cascade(
        control,
        definitions[ARM_FULL_SLICES],
        own_status=PASS if not rolling_contract_failures else UNAVAILABLE,
        own_reason=(
            "pass" if not rolling_contract_failures else "rolling_windows_unavailable"
        ),
        unavailable=tuple(rolling_contract_failures),
        inputs=slice_inputs,
    )

    control_values = _mapping(control.get("inputs"))
    goals = _finite(control_values.get("goals_at_snapshot"))
    goal_inputs = {
        "goals_at_snapshot": goals,
        "max_goals_at_snapshot": ruleset.max_goals_at_snapshot,
    }
    if goals is None or goals < 0 or not goals.is_integer():
        goals_status = UNAVAILABLE
        goals_reason = "goals_at_snapshot_unavailable"
        goals_failed: Sequence[str] = ()
        goals_unavailable: Sequence[str] = ("goals_at_snapshot",)
    elif int(goals) > ruleset.max_goals_at_snapshot:
        goals_status = FAIL
        goals_reason = "goals_at_snapshot"
        goals_failed = ("goals_at_snapshot",)
        goals_unavailable = ()
    else:
        goals_status = PASS
        goals_reason = "pass"
        goals_failed = ()
        goals_unavailable = ()
    goals_arm = _cascade(
        slices,
        definitions[ARM_FULL_SLICES_GOALS_LE2],
        own_status=goals_status,
        own_reason=goals_reason,
        failed=goals_failed,
        unavailable=goals_unavailable,
        inputs=goal_inputs,
    )

    score_home = _finite(control_values.get("score_home"))
    score_away = _finite(control_values.get("score_away"))
    score_difference_abs = (
        abs(float(score_home) - float(score_away))
        if score_home is not None and score_away is not None
        else None
    )
    close_inputs = {
        "score_home": score_home,
        "score_away": score_away,
        "score_difference_abs": score_difference_abs,
        "max_score_difference_abs": ruleset.max_score_difference_abs,
    }
    if (
        score_home is None
        or score_away is None
        or score_home < 0
        or score_away < 0
        or not score_home.is_integer()
        or not score_away.is_integer()
    ):
        close_status = UNAVAILABLE
        close_reason = "score_difference_unavailable"
        close_failed: Sequence[str] = ()
        close_unavailable: Sequence[str] = ("score_difference_abs",)
    elif score_difference_abs is None:
        close_status = UNAVAILABLE
        close_reason = "score_difference_unavailable"
        close_failed = ()
        close_unavailable = ("score_difference_abs",)
    elif score_difference_abs > ruleset.max_score_difference_abs:
        close_status = FAIL
        close_reason = "score_difference_abs"
        close_failed = ("score_difference_abs",)
        close_unavailable = ()
    else:
        close_status = PASS
        close_reason = "pass"
        close_failed = ()
        close_unavailable = ()
    close_arm = _cascade(
        goals_arm,
        definitions[ARM_FULL_SLICES_GOALS_LE2_CLOSE],
        own_status=close_status,
        own_reason=close_reason,
        failed=close_failed,
        unavailable=close_unavailable,
        inputs=close_inputs,
    )

    static_evidence = _prediction_evidence(
        static_prediction,
        snapshot,
        expected_record_type="shadow_ml_prediction",
        max_live_lag_seconds=max_live_prediction_lag_seconds,
    )
    rolling_evidence = _prediction_evidence(
        rolling_prediction,
        snapshot,
        expected_record_type="shadow_ml_rolling_prediction",
        max_live_lag_seconds=max_live_prediction_lag_seconds,
    )
    rolling_probability = _finite(
        rolling_evidence.get("to90_calibrated_probability_pct")
    )
    ml_inputs = {
        "threshold_pct": ruleset.rolling_ml_min_probability_pct,
        "static": static_evidence,
        "rolling": rolling_evidence,
    }
    if not rolling_evidence.get("usable"):
        ml_status = UNAVAILABLE
        ml_reason = f"rolling_ml_unavailable:{rolling_evidence.get('reason')}"
        ml_failed: Sequence[str] = ()
        ml_unavailable: Sequence[str] = ("rolling_ml.to90",)
    elif (
        rolling_probability is not None
        and rolling_probability >= ruleset.rolling_ml_min_probability_pct
    ):
        ml_status = PASS
        ml_reason = "pass"
        ml_failed = ()
        ml_unavailable = ()
    else:
        ml_status = FAIL
        ml_reason = "rolling_ml_probability_below_threshold"
        ml_failed = ("rolling_ml.to90.calibrated_probability_pct",)
        ml_unavailable = ()
    ml_arm = _cascade(
        goals_arm,
        definitions[ARM_ROLLING_ML_CONFIRM_75],
        own_status=ml_status,
        own_reason=ml_reason,
        failed=ml_failed,
        unavailable=ml_unavailable,
        inputs=ml_inputs,
    )
    return {
        ARM_CONTROL: control,
        ARM_FULL_SLICES: slices,
        ARM_FULL_SLICES_GOALS_LE2: goals_arm,
        ARM_FULL_SLICES_GOALS_LE2_CLOSE: close_arm,
        ARM_ROLLING_ML_CONFIRM_75: ml_arm,
    }
