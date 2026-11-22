"""Calibrate a publication ``bot.prob_to90`` floor on top of a combination.

The combination remains the match-pattern gate. This layer only chooses an
extra probability floor that maximises a statistically trustworthy hit rate
(Wilson lower confidence bound), not bankroll or raw signal count.

A raw ``max(hit_rate)`` search would collapse onto tiny high-threshold pockets.
``LCB`` keeps the same objective the operator asked for — проходимость — while
refusing to trust sparse samples.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional, Sequence

from .lifecycle import wilson_interval
from .schema import canonical_hash


PROB_THRESHOLD_CALIBRATION_VERSION = "prob_threshold_lcb_v1"
# One-sided ~95% normal quantile for a lower confidence bound.
DEFAULT_LCB_Z = 1.6448536269514722


def _finite(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


@dataclass(frozen=True)
class ProbThresholdCalibrationPolicy:
    """Source-controlled knobs for the publication probability sieve."""

    version: str = PROB_THRESHOLD_CALIBRATION_VERSION
    z: float = DEFAULT_LCB_Z
    min_resolved: int = 40
    min_threshold: float = 75.0
    max_threshold: float = 90.0
    grid_step: float = 0.5
    # Keep the previous floor unless another eligible threshold improves LCB
    # by at least this absolute amount (hit-rate fraction).
    hysteresis_lcb_eps: float = 0.005
    require_improvement_vs_baseline: bool = True

    def __post_init__(self) -> None:
        if self.version != PROB_THRESHOLD_CALIBRATION_VERSION:
            raise ValueError("unsupported prob-threshold calibration version")
        if not math.isfinite(self.z) or self.z <= 0.0:
            raise ValueError("z must be a positive finite number")
        if type(self.min_resolved) is not int or self.min_resolved <= 0:
            raise ValueError("min_resolved must be a positive integer")
        for name in ("min_threshold", "max_threshold", "grid_step"):
            value = float(getattr(self, name))
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
        if self.grid_step <= 0.0:
            raise ValueError("grid_step must be positive")
        if self.min_threshold > self.max_threshold:
            raise ValueError("min_threshold cannot exceed max_threshold")
        if not math.isfinite(self.hysteresis_lcb_eps) or self.hysteresis_lcb_eps < 0.0:
            raise ValueError("hysteresis_lcb_eps must be finite and >= 0")

    def manifest(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ThresholdScore:
    """One candidate probability floor and its hit-rate evidence."""

    threshold: Optional[float]
    resolved: int
    wins: int
    losses: int
    hit_rate: Optional[float]
    lcb: Optional[float]
    eligible: bool
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "threshold": self.threshold,
            "resolved": self.resolved,
            "wins": self.wins,
            "losses": self.losses,
            "hit_rate": self.hit_rate,
            "lcb": self.lcb,
            "eligible": self.eligible,
            "reason": self.reason,
        }


def hit_rate_lcb(wins: int, total: int, *, z: float = DEFAULT_LCB_Z) -> float:
    """Return the Wilson lower bound for a hit rate."""

    lower, _upper = wilson_interval(int(wins), int(total), z=float(z))
    return float(lower)


def calibration_inputs_from_feature_values(
    values: Mapping[str, Any],
) -> dict[str, Any]:
    """Persist the bot probability used later for threshold calibration."""

    payload: dict[str, Any] = {}
    prob = _finite(values.get("bot.prob_to90"))
    if prob is not None:
        payload["bot.prob_to90"] = float(prob)
    baseline = _finite(values.get("bot.baseline_prob_to90"))
    if baseline is not None:
        payload["bot.baseline_prob_to90"] = float(baseline)
    return payload


def extract_bot_prob_to90(payload: Mapping[str, Any]) -> Optional[float]:
    """Read ``bot.prob_to90`` from trigger input / nested source payloads."""

    direct = _finite(payload.get("bot.prob_to90"))
    if direct is not None:
        return direct
    calibration = _mapping(payload.get("calibration_inputs"))
    nested = _finite(calibration.get("bot.prob_to90"))
    if nested is not None:
        return nested
    matched = _mapping(payload.get("matched_values"))
    matched_prob = _finite(matched.get("bot.prob_to90"))
    if matched_prob is not None:
        return matched_prob
    source_input = _mapping(payload.get("source_input"))
    source_matched = _mapping(source_input.get("matched_values"))
    source_prob = _finite(source_matched.get("bot.prob_to90"))
    if source_prob is not None:
        return source_prob
    source_calibration = _mapping(source_input.get("calibration_inputs"))
    return _finite(source_calibration.get("bot.prob_to90"))


def iter_threshold_grid(policy: ProbThresholdCalibrationPolicy) -> tuple[float, ...]:
    """Return inclusive ``[min, max]`` thresholds on ``grid_step``."""

    values: list[float] = []
    current = float(policy.min_threshold)
    # Guard against float drift near the top of the grid.
    while current <= float(policy.max_threshold) + 1e-9:
        values.append(round(current, 10))
        current += float(policy.grid_step)
    if not values or values[-1] < float(policy.max_threshold) - 1e-9:
        values.append(float(policy.max_threshold))
    # De-dupe while preserving order.
    deduped: list[float] = []
    seen: set[float] = set()
    for value in values:
        key = round(value, 10)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(float(value))
    return tuple(deduped)


def _score_threshold(
    *,
    threshold: Optional[float],
    samples: Sequence[tuple[float, bool]],
    policy: ProbThresholdCalibrationPolicy,
) -> ThresholdScore:
    if threshold is None:
        selected = list(samples)
        label = "baseline"
    else:
        selected = [
            (prob, won) for prob, won in samples if prob + 1e-12 >= float(threshold)
        ]
        label = f"threshold_{threshold:g}"
    resolved = len(selected)
    wins = sum(1 for _prob, won in selected if won)
    losses = resolved - wins
    if resolved <= 0:
        return ThresholdScore(
            threshold=threshold,
            resolved=0,
            wins=0,
            losses=0,
            hit_rate=None,
            lcb=None,
            eligible=False,
            reason=f"{label}_empty",
        )
    hit_rate = wins / resolved
    lcb = hit_rate_lcb(wins, resolved, z=policy.z)
    if resolved < int(policy.min_resolved):
        return ThresholdScore(
            threshold=threshold,
            resolved=resolved,
            wins=wins,
            losses=losses,
            hit_rate=hit_rate,
            lcb=lcb,
            eligible=False,
            reason=f"{label}_insufficient_resolved",
        )
    return ThresholdScore(
        threshold=threshold,
        resolved=resolved,
        wins=wins,
        losses=losses,
        hit_rate=hit_rate,
        lcb=lcb,
        eligible=True,
        reason=f"{label}_ok",
    )


def _rank_key(score: ThresholdScore) -> tuple[float, float, int, float]:
    # Higher LCB wins. Ties prefer higher point hit-rate, then more evidence,
    # then the lower (less aggressive) probability floor.
    return (
        float(score.lcb or 0.0),
        float(score.hit_rate or 0.0),
        int(score.resolved),
        -float(score.threshold if score.threshold is not None else -1e9),
    )


def calibrate_publication_min_prob(
    samples: Sequence[Mapping[str, Any]] | Sequence[tuple[Any, Any]],
    *,
    policy: ProbThresholdCalibrationPolicy = ProbThresholdCalibrationPolicy(),
    previous_threshold: Optional[float] = None,
    calibrated_at_utc: Optional[str] = None,
    rule_id: Optional[str] = None,
    phase_id: Optional[str] = None,
) -> dict[str, Any]:
    """Choose the probability floor with the best eligible Wilson LCB.

    ``samples`` entries are either ``(prob_to90, won_bool)`` tuples or mappings
    with ``prob_to90`` / ``bot.prob_to90`` and ``won`` / ``outcome_status``.
    Rows without a finite probability are ignored.
    """

    normalized: list[tuple[float, bool]] = []
    ignored = 0
    for row in samples:
        if isinstance(row, Mapping):
            prob = extract_bot_prob_to90(row)
            if prob is None:
                prob = _finite(row.get("prob_to90"))
            status = str(row.get("outcome_status") or row.get("status") or "").lower()
            if "won" in row:
                won = bool(row.get("won"))
            elif status == "win":
                won = True
            elif status == "loss":
                won = False
            else:
                ignored += 1
                continue
        else:
            if not isinstance(row, (tuple, list)) or len(row) != 2:
                ignored += 1
                continue
            prob = _finite(row[0])
            won = bool(row[1])
        if prob is None:
            ignored += 1
            continue
        normalized.append((float(prob), bool(won)))

    baseline = _score_threshold(
        threshold=None, samples=normalized, policy=policy
    )
    grid_scores = [
        _score_threshold(threshold=threshold, samples=normalized, policy=policy)
        for threshold in iter_threshold_grid(policy)
    ]
    eligible = [score for score in grid_scores if score.eligible]
    selected: Optional[ThresholdScore] = None
    selection_reason = "no_eligible_threshold"

    if eligible:
        best = max(eligible, key=_rank_key)
        if (
            policy.require_improvement_vs_baseline
            and baseline.eligible
            and baseline.lcb is not None
            and best.lcb is not None
            and float(best.lcb) <= float(baseline.lcb) + 1e-15
        ):
            # A flat or worse LCB means the floor only burns volume.
            selection_reason = "no_threshold_beats_baseline_lcb"
        else:
            selected = best
            selection_reason = "max_lcb"

        previous = _finite(previous_threshold)
        if (
            selected is not None
            and previous is not None
            and float(policy.min_threshold) - 1e-12
            <= previous
            <= float(policy.max_threshold) + 1e-12
        ):
            previous_score = next(
                (
                    score
                    for score in grid_scores
                    if score.threshold is not None
                    and abs(float(score.threshold) - previous) <= 1e-9
                ),
                _score_threshold(
                    threshold=previous, samples=normalized, policy=policy
                ),
            )
            if (
                previous_score.eligible
                and previous_score.lcb is not None
                and selected.lcb is not None
                and float(selected.lcb)
                < float(previous_score.lcb) + float(policy.hysteresis_lcb_eps)
            ):
                # Prefer stability when the challenger floor is not clearly better.
                if (
                    not policy.require_improvement_vs_baseline
                    or not baseline.eligible
                    or baseline.lcb is None
                    or float(previous_score.lcb) > float(baseline.lcb) + 1e-15
                ):
                    selected = previous_score
                    selection_reason = "hysteresis_keep_previous"

    body = {
        "schema_version": 1,
        "version": policy.version,
        "policy": policy.manifest(),
        "rule_id": rule_id,
        "phase_id": phase_id,
        "calibrated_at_utc": calibrated_at_utc,
        "sample_count_with_prob": len(normalized),
        "sample_count_ignored": ignored,
        "baseline": baseline.as_dict(),
        "selected_threshold": (
            None if selected is None else selected.threshold
        ),
        "selected": None if selected is None else selected.as_dict(),
        "selection_reason": selection_reason,
        "grid": [score.as_dict() for score in grid_scores],
        "active": selected is not None and selected.threshold is not None,
    }
    body["checksum"] = canonical_hash(
        {key: value for key, value in body.items() if key != "checksum"}
    )
    return body


def apply_publication_prob_gate(
    *,
    combination_allow: bool,
    bot_prob_to90: Any,
    calibration: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    """AND the combination decision with the calibrated probability floor."""

    payload = _mapping(calibration)
    threshold = _finite(payload.get("selected_threshold"))
    active = bool(payload.get("active")) and threshold is not None
    result = {
        "combination_allow": bool(combination_allow),
        "calibration_active": active,
        "selected_threshold": threshold,
        "bot_prob_to90": _finite(bot_prob_to90),
        "allow": bool(combination_allow),
        "reason": "combination_only",
    }
    if not combination_allow:
        result["reason"] = "combination_blocked"
        return result
    if not active:
        result["reason"] = "calibration_inactive"
        return result
    prob = _finite(bot_prob_to90)
    result["bot_prob_to90"] = prob
    if prob is None:
        result["allow"] = False
        result["reason"] = "bot_prob_unavailable_for_calibrated_gate"
        return result
    if prob + 1e-12 < float(threshold):
        result["allow"] = False
        result["reason"] = "prob_below_calibrated_threshold"
        return result
    result["allow"] = True
    result["reason"] = "combination_and_calibrated_prob"
    return result


__all__ = [
    "DEFAULT_LCB_Z",
    "PROB_THRESHOLD_CALIBRATION_VERSION",
    "ProbThresholdCalibrationPolicy",
    "ThresholdScore",
    "apply_publication_prob_gate",
    "calibrate_publication_min_prob",
    "calibration_inputs_from_feature_values",
    "extract_bot_prob_to90",
    "hit_rate_lcb",
    "iter_threshold_grid",
]
