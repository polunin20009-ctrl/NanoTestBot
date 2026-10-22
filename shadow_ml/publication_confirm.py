"""Optional rolling-ML confirm gate for publication routing.

This is deliberately not a probability replacement.  When enabled, a champion
pass may still be blocked if a causal rolling to90 score is available and below
the configured floor.  Missing / non-ready ML fails open so the champion path
keeps working while the confirm is rolled out.
"""

from __future__ import annotations

import math
from typing import Any, Mapping, Optional


DEFAULT_MIN_TO90_PCT = 75.0


def _finite_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def extract_rolling_to90_pct(
    rolling_prediction: Optional[Mapping[str, Any]],
) -> Optional[float]:
    """Return calibrated rolling to90 in percentage points, or ``None``."""

    if not isinstance(rolling_prediction, Mapping):
        return None
    predictions = rolling_prediction.get("predictions")
    if not isinstance(predictions, Mapping):
        predictions = rolling_prediction.get("targets")
    if not isinstance(predictions, Mapping):
        return None
    target = predictions.get("to90")
    if not isinstance(target, Mapping):
        return None
    if str(target.get("status") or "") != "ok":
        return None
    value = _finite_float(target.get("calibrated_probability_pct"))
    if value is None:
        value = _finite_float(target.get("probability_pct"))
    if value is None or not 0.0 <= value <= 100.0:
        return None
    return value


def apply_rolling_to90_publication_confirm(
    *,
    champion_allow: bool,
    rolling_prediction: Optional[Mapping[str, Any]],
    enabled: bool,
    min_to90_pct: float = DEFAULT_MIN_TO90_PCT,
    require_offline_ready: bool = True,
) -> dict[str, Any]:
    """Possibly tighten a champion allow with a rolling to90 floor.

    Fail-open: when the confirm is enabled but rolling ML is missing, not
    ``ok``, or not ``offline_ready``, the champion decision is unchanged.
    """

    result: dict[str, Any] = {
        "enabled": bool(enabled),
        "applied": False,
        "allow": bool(champion_allow),
        "reason": "confirm_disabled",
        "min_to90_pct": float(min_to90_pct),
        "rolling_to90_pct": None,
        "model_status": None,
        "model_id": None,
        "require_offline_ready": bool(require_offline_ready),
    }
    if not champion_allow:
        result["reason"] = "champion_blocked"
        return result
    if not enabled:
        return result

    model_status = None
    model_id = None
    if isinstance(rolling_prediction, Mapping):
        model_status = rolling_prediction.get("model_status")
        model_id = rolling_prediction.get("model_id")
    result["model_status"] = model_status
    result["model_id"] = model_id

    to90_pct = extract_rolling_to90_pct(rolling_prediction)
    result["rolling_to90_pct"] = to90_pct
    if to90_pct is None:
        result["applied"] = True
        result["reason"] = "fail_open_unavailable"
        return result
    if require_offline_ready and str(model_status or "") != "offline_ready":
        result["applied"] = True
        result["reason"] = "fail_open_not_offline_ready"
        return result

    result["applied"] = True
    if to90_pct + 1e-12 >= float(min_to90_pct):
        result["allow"] = True
        result["reason"] = "confirm_pass"
        return result
    result["allow"] = False
    result["reason"] = "confirm_below_threshold"
    return result
