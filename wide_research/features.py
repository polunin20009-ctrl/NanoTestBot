from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from .schema import DEFAULT_UNIVERSE, FAIL, PASS, UNAVAILABLE, UniverseSpec
from .extended_features import EXTENDED_FEATURES, EXTENDED_FEATURE_NAMES, extend_values


FEATURE_SCHEMA_VERSION = 2
DEFAULT_MAX_PREDICTION_LAG_SECONDS = 300.0

_BOT_PROBABILITY_FIELDS = {
    "prob_next_15": "bot.prob_next_15",
    "prob_next_25": "bot.prob_next_25",
    "prob_to75": "bot.prob_to75",
    "prob_to90": "bot.prob_to90",
    "prob_until_end_decision": "bot.prob_until_end",
    "baseline_prob_next_15": "bot.baseline_prob_next_15",
    "baseline_prob_next_25": "bot.baseline_prob_next_25",
    "baseline_prob_to75": "bot.baseline_prob_to75",
    "baseline_prob_to90": "bot.baseline_prob_to90",
}

_REPUTATION_TARGETS = {
    "next_15": (
        "reputation_base_prob_next_15",
        "reputation_adjusted_prob_next_15",
    ),
    "to90": (
        "reputation_base_prob_to90",
        "reputation_adjusted_prob_to90",
    ),
}

_FEATURE_FIELDS = (
    "pressure_index",
    "save_stress",
    "tempo",
    "live_intensity",
    "adjusted_intensity",
    "lambda_2h",
    "game_state_factor",
    "goal_xg_gap",
    "goal_xg_gap_factor",
    "xg_delta_factor",
    "urgency_factor",
    "season_context_factor",
    "team_2h_factor",
    "league_2h_factor",
    "score_state_factor",
    "context_multiplier",
    "applied_context_multiplier",
    "sample_confidence",
    "xg_confidence",
    "xg_delta_confidence",
    "xg_weight_effective",
)

_RAW_METRICS = (
    "xg",
    "shots_on_target",
    "shots_in_box",
    "total_shots",
    "saves",
    "corners",
    "possession",
    "yellow_cards",
    "red_cards",
    "attacks",
    "dangerous_attacks",
)

_ROLLING_METRICS = (
    "xg_total",
    "shots_on_target_total",
    "shots_in_box_total",
    "total_shots_total",
    "corners_total",
    "score_total",
    "pressure_index",
)

_ML_TARGETS = {
    "next15": "prob_next_15",
    "to90": "prob_to90",
}

_COMPOSITE_FEATURES = (
    "composite.base_quality_v1",
    "rolling.both_windows_available_v1",
)


def _allowed_feature_names() -> tuple[str, ...]:
    names = ["minute", "score.home_goals", "score.away_goals"]
    names.extend(
        ("score.total_goals", "score.goal_difference", "score.goal_difference_abs")
    )
    names.extend(_BOT_PROBABILITY_FIELDS.values())
    for target in _REPUTATION_TARGETS:
        names.extend(
            (
                f"reputation.base_prob_{target}",
                f"reputation.adjusted_prob_{target}",
                f"reputation.delta_{target}_pp",
            )
        )
    names.extend(f"feature.{name}" for name in _FEATURE_FIELDS)
    for metric in _RAW_METRICS:
        names.extend(
            (
                f"raw.{metric}_home",
                f"raw.{metric}_away",
                f"raw.{metric}_total",
            )
        )
    for window in (5, 10):
        for kind in ("delta", "rate_per_minute"):
            names.extend(
                f"rolling.{window}m.{kind}.{metric}"
                for metric in _ROLLING_METRICS
            )
    for model in ("static", "rolling"):
        names.extend(
            f"ml.{model}.{output_name}" for output_name in _ML_TARGETS.values()
        )
    names.extend(_COMPOSITE_FEATURES)
    return tuple(sorted(names))


ALLOWED_FEATURE_NAMES = _allowed_feature_names()
ALLOWED_FEATURES = frozenset(ALLOWED_FEATURE_NAMES)

# A second, explicit denylist makes the intended safety boundary reviewable.
# Extraction is still positive-allowlist based; no arbitrary path is flattened.
FORBIDDEN_LEAKAGE_TOKENS = frozenset(
    {
        "outcome",
        "result",
        "label",
        "decision",
        "final_decision",
        "telegram",
        "tg",
        "filter",
        "passed",
        "send",
        "win",
        "loss",
    }
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
    if not math.isfinite(number):
        return None
    return 0.0 if number == 0.0 else number


def _probability(value: Any) -> Optional[float]:
    number = _finite(value)
    return number if number is not None and 0.0 <= number <= 100.0 else None


def _nonnegative_integer(value: Any) -> Optional[float]:
    number = _finite(value)
    if number is None or number < 0.0 or not number.is_integer():
        return None
    return number


def _aware_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _first_bool(*values: Any) -> Optional[bool]:
    for value in values:
        if type(value) is bool:
            return value
    return None


def evaluate_universe(
    snapshot: Mapping[str, Any],
    spec: UniverseSpec = DEFAULT_UNIVERSE,
) -> dict[str, Any]:
    """Evaluate technical eligibility without consulting production selection."""
    if not isinstance(snapshot, Mapping):
        return {
            "status": UNAVAILABLE,
            "eligible": False,
            "reason": "snapshot_not_mapping",
            "universe_id": spec.universe_id,
            "universe_manifest_hash": spec.manifest_hash,
            "failed_checks": [],
            "unavailable_checks": ["snapshot"],
        }

    stage_value = snapshot.get("stage")
    stage = str(stage_value).strip() if stage_value is not None else ""
    minute = _nonnegative_integer(snapshot.get("minute"))
    source_id = str(
        snapshot.get("observation_id") or snapshot.get("decision_id") or ""
    ).strip()
    fixture_id = _nonnegative_integer(snapshot.get("fixture_id"))
    source_time = _aware_datetime(snapshot.get("created_at_utc"))
    gates = _mapping(snapshot.get("gates"))
    policy = _mapping(snapshot.get("publication_policy"))
    readiness = _first_bool(gates.get("readiness_passed"))
    publication_context = _first_bool(
        policy.get("publication_context_passed"),
        gates.get("publication_context_passed"),
    )

    unavailable: list[str] = []
    failed: list[str] = []
    if not stage:
        unavailable.append("stage")
    elif stage not in spec.stages:
        failed.append("stage")
    if minute is None:
        unavailable.append("minute")
    elif not spec.min_minute <= int(minute) <= spec.max_minute:
        failed.append("minute_range")
    if not source_id:
        unavailable.append("source_id")
    if fixture_id is None or fixture_id <= 0:
        unavailable.append("fixture_id")
    if source_time is None:
        unavailable.append("created_at_utc")
    if spec.require_readiness_passed:
        if readiness is None:
            unavailable.append("readiness_passed")
        elif readiness is not True:
            failed.append("readiness_passed")
    if spec.require_publication_context_passed:
        if publication_context is None:
            unavailable.append("publication_context_passed")
        elif publication_context is not True:
            failed.append("publication_context_passed")

    # For conjunctions, a known failed hard gate is conclusive even when some
    # other metadata is unavailable.
    if failed:
        status = FAIL
        reason = failed[0]
    elif unavailable:
        status = UNAVAILABLE
        reason = unavailable[0]
    else:
        status = PASS
        reason = "pass"
    return {
        "status": status,
        "eligible": status == PASS,
        "reason": reason,
        "universe_id": spec.universe_id,
        "universe_manifest_hash": spec.manifest_hash,
        "failed_checks": failed,
        "unavailable_checks": unavailable,
        "inputs": {
            "stage": stage or None,
            "minute": int(minute) if minute is not None else None,
            "readiness_passed": readiness,
            "publication_context_passed": publication_context,
            "current_filter_consulted": False,
        },
    }


def _extract_score(snapshot: Mapping[str, Any], values: dict[str, Optional[float]]) -> None:
    match = _mapping(snapshot.get("match"))
    home = _nonnegative_integer(match.get("score_home"))
    away = _nonnegative_integer(match.get("score_away"))
    values["score.home_goals"] = home
    values["score.away_goals"] = away
    if home is not None and away is not None:
        values["score.total_goals"] = home + away
        values["score.goal_difference"] = home - away
        values["score.goal_difference_abs"] = abs(home - away)


def _extract_bot_probabilities(
    snapshot: Mapping[str, Any], values: dict[str, Optional[float]]
) -> None:
    probabilities = _mapping(snapshot.get("probabilities"))
    for source_name, feature_name in _BOT_PROBABILITY_FIELDS.items():
        values[feature_name] = _probability(probabilities.get(source_name))
    for target, (base_name, adjusted_name) in _REPUTATION_TARGETS.items():
        base = _probability(probabilities.get(base_name))
        adjusted = _probability(probabilities.get(adjusted_name))
        values[f"reputation.base_prob_{target}"] = base
        values[f"reputation.adjusted_prob_{target}"] = adjusted
        values[f"reputation.delta_{target}_pp"] = (
            adjusted - base if base is not None and adjusted is not None else None
        )


def _extract_engineered_features(
    snapshot: Mapping[str, Any], values: dict[str, Optional[float]]
) -> None:
    features = _mapping(snapshot.get("features"))
    for name in _FEATURE_FIELDS:
        values[f"feature.{name}"] = _finite(features.get(name))


def _extract_raw_metrics(
    snapshot: Mapping[str, Any], values: dict[str, Optional[float]]
) -> None:
    raw = _mapping(snapshot.get("raw_metrics"))
    availability = _mapping(snapshot.get("availability"))
    for metric in _RAW_METRICS:
        side_values: list[Optional[float]] = []
        for side in ("home", "away"):
            raw_name = f"{metric}_{side}"
            number = (
                _finite(raw.get(raw_name))
                if availability.get(raw_name) is True
                else None
            )
            if number is not None and number < 0.0:
                number = None
            values[f"raw.{raw_name}"] = number
            side_values.append(number)
        values[f"raw.{metric}_total"] = (
            side_values[0] + side_values[1]
            if all(number is not None for number in side_values)
            else None
        )


def _valid_rolling_window(
    rolling: Mapping[str, Any], window_minutes: int
) -> Optional[Mapping[str, Any]]:
    if (
        type(rolling.get("schema_version")) is not int
        or rolling.get("schema_version") != 1
        or rolling.get("mode") != "shadow_collection"
        or rolling.get("production_applied") is not False
    ):
        return None
    window = _mapping(_mapping(rolling.get("windows")).get(f"{window_minutes}m"))
    span = _finite(window.get("actual_span_minutes"))
    activity_count = _nonnegative_integer(
        window.get("available_activity_metric_count")
    )
    if (
        str(window.get("status") or "").lower() != "ok"
        or type(window.get("requested_window_minutes")) is not int
        or window.get("requested_window_minutes") != window_minutes
        or span is None
        or not span.is_integer()
        or not window_minutes <= int(span) <= window_minutes + 2
        or activity_count is None
        or activity_count < 1
    ):
        return None
    return window


def _extract_rolling(
    snapshot: Mapping[str, Any], values: dict[str, Optional[float]]
) -> dict[str, bool]:
    rolling = _mapping(snapshot.get("rolling_dynamics"))
    contract: dict[str, bool] = {}
    for minutes in (5, 10):
        window = _valid_rolling_window(rolling, minutes)
        contract[f"{minutes}m"] = window is not None
        availability = _mapping(window.get("availability")) if window else {}
        deltas = _mapping(window.get("deltas")) if window else {}
        rates = _mapping(window.get("rates_per_minute")) if window else {}
        for metric in _ROLLING_METRICS:
            available = availability.get(metric) is True
            delta = _finite(deltas.get(metric)) if available else None
            rate = _finite(rates.get(metric)) if available else None
            if metric != "pressure_index":
                if delta is not None and delta < 0.0:
                    delta = None
                if rate is not None and rate < 0.0:
                    rate = None
            values[f"rolling.{minutes}m.delta.{metric}"] = (
                delta if window is not None else None
            )
            values[f"rolling.{minutes}m.rate_per_minute.{metric}"] = (
                rate if window is not None else None
            )
    contract["both"] = bool(
        type(rolling.get("available_windows")) is int
        and rolling.get("available_windows") == 2
        and contract.get("5m") is True
        and contract.get("10m") is True
    )
    return contract


def _extract_composites(
    values: dict[str, Optional[float]],
    rolling_contract: Mapping[str, bool],
) -> None:
    """Build causal research-only shortcuts from already reviewed inputs.

    These values never consult Telegram, production decisions, outcomes, or a
    persisted filter result.  They merely make a stable conjunction available
    as one search atom, allowing deeper research to spend its clause budget on
    genuinely new refinements instead of repeatedly reconstructing BASE.
    """

    base_inputs = (
        values.get("bot.prob_to90"),
        values.get("reputation.delta_to90_pp"),
        values.get("feature.adjusted_intensity"),
        values.get("feature.season_context_factor"),
    )
    if all(value is not None for value in base_inputs):
        probability, reputation_delta, intensity, season = base_inputs
        values["composite.base_quality_v1"] = float(
            float(probability) >= 75.0
            and float(reputation_delta) >= 1.5
            and float(intensity) >= 0.55
            and float(season) >= 1.02
        )

    values["rolling.both_windows_available_v1"] = float(
        rolling_contract.get("both") is True
    )


def _prediction_is_causal(
    prediction: Any,
    snapshot: Mapping[str, Any],
    *,
    expected_record_type: str,
    max_prediction_lag_seconds: float,
) -> bool:
    if not isinstance(prediction, Mapping):
        return False
    if prediction.get("record_type") != expected_record_type:
        return False
    source_id = str(
        snapshot.get("observation_id") or snapshot.get("decision_id") or ""
    ).strip()
    prediction_source_id = str(
        prediction.get("observation_id") or prediction.get("decision_id") or ""
    ).strip()
    if not source_id or source_id != prediction_source_id:
        return False
    source_fixture = _nonnegative_integer(snapshot.get("fixture_id"))
    prediction_fixture = _nonnegative_integer(prediction.get("fixture_id"))
    source_minute = _nonnegative_integer(snapshot.get("minute"))
    prediction_minute = _nonnegative_integer(prediction.get("minute"))
    if (
        source_fixture is None
        or prediction_fixture != source_fixture
        or source_minute is None
        or prediction_minute != source_minute
    ):
        return False
    source_time = _aware_datetime(snapshot.get("created_at_utc"))
    recorded_source_time = _aware_datetime(
        prediction.get("observation_created_at_utc")
    )
    prediction_time = _aware_datetime(prediction.get("created_at_utc"))
    model_created = _aware_datetime(prediction.get("model_created_at_utc"))
    model_cutoff = _aware_datetime(prediction.get("model_data_cutoff_utc"))
    if (
        source_time is None
        or recorded_source_time != source_time
        or prediction_time is None
        or model_created is None
        or model_cutoff is None
    ):
        return False
    lag = (prediction_time - source_time).total_seconds()
    if not 0.0 <= lag <= max_prediction_lag_seconds:
        return False
    if not model_cutoff <= model_created < source_time:
        return False
    if model_cutoff >= source_time:
        return False
    return bool(
        prediction.get("shadow_only") is True
        and prediction.get("production_applied") is False
    )


def _extract_prediction(
    prediction: Any,
    snapshot: Mapping[str, Any],
    values: dict[str, Optional[float]],
    *,
    model_name: str,
    expected_record_type: str,
    max_prediction_lag_seconds: float,
) -> bool:
    causal = _prediction_is_causal(
        prediction,
        snapshot,
        expected_record_type=expected_record_type,
        max_prediction_lag_seconds=max_prediction_lag_seconds,
    )
    predictions = _mapping(prediction.get("predictions")) if causal else {}
    any_usable = False
    for target_name, output_name in _ML_TARGETS.items():
        target = _mapping(predictions.get(target_name))
        value = None
        if (
            causal
            and str(target.get("status") or "").lower() == "ok"
            and target.get("production_applied") is False
        ):
            value = _probability(target.get("calibrated_probability_pct"))
        values[f"ml.{model_name}.{output_name}"] = value
        any_usable = any_usable or value is not None
    return causal and any_usable


def extract_features(
    snapshot: Mapping[str, Any],
    *,
    static_prediction: Optional[Mapping[str, Any]] = None,
    rolling_prediction: Optional[Mapping[str, Any]] = None,
    max_prediction_lag_seconds: float = DEFAULT_MAX_PREDICTION_LAG_SECONDS,
    include_extended: bool = False,
) -> dict[str, Any]:
    """Extract only the reviewed, pre-outcome numeric feature allowlist."""
    if not isinstance(snapshot, Mapping):
        snapshot = {}
    try:
        max_lag = float(max_prediction_lag_seconds)
    except (TypeError, ValueError):
        max_lag = DEFAULT_MAX_PREDICTION_LAG_SECONDS
    if not math.isfinite(max_lag) or max_lag < 0.0:
        max_lag = DEFAULT_MAX_PREDICTION_LAG_SECONDS

    values: dict[str, Optional[float]] = {
        name: None for name in ALLOWED_FEATURE_NAMES
    }
    values["minute"] = _nonnegative_integer(snapshot.get("minute"))
    _extract_score(snapshot, values)
    _extract_bot_probabilities(snapshot, values)
    _extract_engineered_features(snapshot, values)
    _extract_raw_metrics(snapshot, values)
    rolling_contract = _extract_rolling(snapshot, values)
    _extract_composites(values, rolling_contract)
    static_causal = _extract_prediction(
        static_prediction,
        snapshot,
        values,
        model_name="static",
        expected_record_type="shadow_ml_prediction",
        max_prediction_lag_seconds=max_lag,
    )
    rolling_causal = _extract_prediction(
        rolling_prediction,
        snapshot,
        values,
        model_name="rolling",
        expected_record_type="shadow_ml_rolling_prediction",
        max_prediction_lag_seconds=max_lag,
    )
    if include_extended:
        extend_values(snapshot, values)
    missing = [name for name in values if values[name] is None]
    return {
        "schema_version": FEATURE_SCHEMA_VERSION,
        "values": values,
        "available_count": len(values) - len(missing),
        "missing_features": missing,
        "contracts": {
            "rolling_5m_frozen": rolling_contract["5m"],
            "rolling_10m_frozen": rolling_contract["10m"],
            "static_ml_causal": static_causal,
            "rolling_ml_causal": rolling_causal,
            "leakage_fields_consulted": False,
        },
    }


extract_feature_vector = extract_features
