"""Calibrate publication probability floors on top of a combination.

The combination remains the match-pattern gate. This layer chooses extra
probability floors that maximise a statistically trustworthy hit rate
(Wilson lower confidence bound).

Two tracks run in parallel:

- ``bot`` — publication track (live AND gate)
- ``rolling_ml`` — shadow track (logged only, for head-to-head comparison)

A raw ``max(hit_rate)`` search would collapse onto tiny high-threshold pockets.
``LCB`` keeps the operator objective — проходимость — while refusing sparse
samples.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional, Sequence

from .lifecycle import wilson_interval
from .schema import canonical_hash


PROB_THRESHOLD_CALIBRATION_VERSION = "prob_threshold_lcb_v1"
PROB_THRESHOLD_DUAL_VERSION = "prob_threshold_dual_lcb_v1"
# One-sided ~95% normal quantile for a lower confidence bound.
DEFAULT_LCB_Z = 1.6448536269514722

PUBLICATION_TRACK = "bot"
SHADOW_TRACKS = ("rolling_ml",)
TRACK_FEATURES = {
    "bot": "bot.prob_to90",
    "rolling_ml": "ml.rolling.prob_to90",
}


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
    """Persist bot and ML probabilities used later for dual calibration."""

    payload: dict[str, Any] = {}
    for feature in (
        "bot.prob_to90",
        "bot.baseline_prob_to90",
        "ml.rolling.prob_to90",
        "ml.static.prob_to90",
    ):
        prob = _finite(values.get(feature))
        if prob is not None:
            payload[feature] = float(prob)
    return payload


def extract_prob_feature(
    payload: Mapping[str, Any],
    feature: str,
) -> Optional[float]:
    """Read one probability feature from trigger input / nested payloads."""

    feature_name = str(feature or "").strip()
    if not feature_name:
        return None
    direct = _finite(payload.get(feature_name))
    if direct is not None:
        return direct
    if feature_name == "bot.prob_to90":
        alias = _finite(payload.get("prob_to90"))
        if alias is not None:
            return alias
    calibration = _mapping(payload.get("calibration_inputs"))
    nested = _finite(calibration.get(feature_name))
    if nested is not None:
        return nested
    matched = _mapping(payload.get("matched_values"))
    matched_prob = _finite(matched.get(feature_name))
    if matched_prob is not None:
        return matched_prob
    source_input = _mapping(payload.get("source_input"))
    source_matched = _mapping(source_input.get("matched_values"))
    source_prob = _finite(source_matched.get(feature_name))
    if source_prob is not None:
        return source_prob
    source_calibration = _mapping(source_input.get("calibration_inputs"))
    return _finite(source_calibration.get(feature_name))


def extract_bot_prob_to90(payload: Mapping[str, Any]) -> Optional[float]:
    """Read ``bot.prob_to90`` from trigger input / nested source payloads."""

    return extract_prob_feature(payload, "bot.prob_to90")


def extract_rolling_ml_prob_to90(payload: Mapping[str, Any]) -> Optional[float]:
    """Read ``ml.rolling.prob_to90`` from trigger input / nested payloads."""

    return extract_prob_feature(payload, "ml.rolling.prob_to90")


def rolling_ml_prob_from_prediction(
    prediction: Optional[Mapping[str, Any]],
) -> Optional[float]:
    """Read calibrated rolling-ML to90 from a shadow prediction record."""

    payload = _mapping(prediction)
    predictions = _mapping(payload.get("predictions"))
    target = _mapping(predictions.get("to90"))
    if str(target.get("status") or "").lower() != "ok":
        return None
    if target.get("production_applied") is True:
        return None
    return _finite(target.get("calibrated_probability_pct"))


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


def _normalize_outcome_samples(
    samples: Sequence[Mapping[str, Any]] | Sequence[tuple[Any, Any]],
    *,
    prob_feature: str,
) -> tuple[list[tuple[float, bool]], int]:
    normalized: list[tuple[float, bool]] = []
    ignored = 0
    for row in samples:
        if isinstance(row, Mapping):
            prob = extract_prob_feature(row, prob_feature)
            if prob is None and prob_feature == "bot.prob_to90":
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
    return normalized, ignored


def calibrate_publication_min_prob(
    samples: Sequence[Mapping[str, Any]] | Sequence[tuple[Any, Any]],
    *,
    policy: ProbThresholdCalibrationPolicy = ProbThresholdCalibrationPolicy(),
    previous_threshold: Optional[float] = None,
    calibrated_at_utc: Optional[str] = None,
    rule_id: Optional[str] = None,
    phase_id: Optional[str] = None,
    prob_feature: str = "bot.prob_to90",
    track_id: Optional[str] = None,
) -> dict[str, Any]:
    """Choose the probability floor with the best eligible Wilson LCB.

    ``samples`` entries are either ``(prob_to90, won_bool)`` tuples or mappings
    carrying the requested ``prob_feature`` plus ``won`` / ``outcome_status``.
    Rows without a finite probability are ignored.
    """

    feature_name = str(prob_feature or "bot.prob_to90").strip() or "bot.prob_to90"
    normalized, ignored = _normalize_outcome_samples(
        samples, prob_feature=feature_name
    )

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
        "track_id": track_id,
        "prob_feature": feature_name,
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


def publication_track_calibration(
    calibration: Optional[Mapping[str, Any]],
) -> Optional[dict[str, Any]]:
    """Return the live publication track payload (bot), with v1 compat."""

    payload = _mapping(calibration)
    if not payload:
        return None
    tracks = _mapping(payload.get("tracks"))
    if tracks:
        track = _mapping(tracks.get(PUBLICATION_TRACK))
        return dict(track) if track else None
    return dict(payload)


def shadow_track_calibration(
    calibration: Optional[Mapping[str, Any]],
    track_id: str = "rolling_ml",
) -> Optional[dict[str, Any]]:
    """Return one shadow-track calibration payload."""

    payload = _mapping(calibration)
    tracks = _mapping(payload.get("tracks"))
    track = _mapping(tracks.get(str(track_id)))
    return dict(track) if track else None


def _track_allows(
    *,
    calibration: Optional[Mapping[str, Any]],
    prob: Optional[float],
) -> tuple[bool, str]:
    payload = _mapping(calibration)
    threshold = _finite(payload.get("selected_threshold"))
    active = bool(payload.get("active")) and threshold is not None
    if not active:
        return True, "calibration_inactive"
    if prob is None:
        return False, "prob_unavailable"
    if prob + 1e-12 < float(threshold):
        return False, "prob_below_threshold"
    return True, "prob_pass"


def _outcome_bucket(wins: int, losses: int, *, z: float) -> dict[str, Any]:
    resolved = int(wins) + int(losses)
    hit_rate = (wins / resolved) if resolved else None
    lcb = hit_rate_lcb(wins, resolved, z=z) if resolved else None
    return {
        "resolved": resolved,
        "wins": int(wins),
        "losses": int(losses),
        "hit_rate": hit_rate,
        "lcb": lcb,
    }


def summarize_dual_shadow_comparison(
    samples: Sequence[Mapping[str, Any]],
    *,
    bot_calibration: Mapping[str, Any],
    rolling_ml_calibration: Mapping[str, Any],
    policy: ProbThresholdCalibrationPolicy = ProbThresholdCalibrationPolicy(),
) -> dict[str, Any]:
    """Compare bot vs rolling-ML gates on the same combination outcomes."""

    both_allow = 0
    bot_only = 0
    ml_only = 0
    neither = 0
    missing_either_prob = 0

    bot_wins = bot_losses = 0
    ml_wins = ml_losses = 0
    bot_only_wins = bot_only_losses = 0
    ml_only_wins = ml_only_losses = 0

    for row in samples:
        if not isinstance(row, Mapping):
            continue
        status = str(row.get("outcome_status") or row.get("status") or "").lower()
        if "won" in row:
            won = bool(row.get("won"))
        elif status == "win":
            won = True
        elif status == "loss":
            won = False
        else:
            continue
        bot_prob = extract_bot_prob_to90(row)
        ml_prob = extract_rolling_ml_prob_to90(row)
        if bot_prob is None or ml_prob is None:
            missing_either_prob += 1
            continue
        bot_allow, _bot_reason = _track_allows(
            calibration=bot_calibration, prob=bot_prob
        )
        ml_allow, _ml_reason = _track_allows(
            calibration=rolling_ml_calibration, prob=ml_prob
        )
        if bot_allow and ml_allow:
            both_allow += 1
        elif bot_allow:
            bot_only += 1
            if won:
                bot_only_wins += 1
            else:
                bot_only_losses += 1
        elif ml_allow:
            ml_only += 1
            if won:
                ml_only_wins += 1
            else:
                ml_only_losses += 1
        else:
            neither += 1
        if bot_allow:
            if won:
                bot_wins += 1
            else:
                bot_losses += 1
        if ml_allow:
            if won:
                ml_wins += 1
            else:
                ml_losses += 1

    return {
        "comparable_resolved": both_allow + bot_only + ml_only + neither,
        "missing_either_prob": missing_either_prob,
        "overlap": {
            "both_allow": both_allow,
            "bot_only": bot_only,
            "rolling_ml_only": ml_only,
            "neither": neither,
        },
        "bot_gate": _outcome_bucket(bot_wins, bot_losses, z=policy.z),
        "rolling_ml_gate": _outcome_bucket(ml_wins, ml_losses, z=policy.z),
        "disagreement": {
            "bot_only": _outcome_bucket(
                bot_only_wins, bot_only_losses, z=policy.z
            ),
            "rolling_ml_only": _outcome_bucket(
                ml_only_wins, ml_only_losses, z=policy.z
            ),
        },
    }


def _previous_track_threshold(
    previous: Optional[Mapping[str, Any]],
    track_id: str,
) -> Optional[float]:
    payload = _mapping(previous)
    tracks = _mapping(payload.get("tracks"))
    if tracks:
        track = _mapping(tracks.get(track_id))
        if not bool(track.get("active")):
            return None
        return _finite(track.get("selected_threshold"))
    # Legacy single-track payloads are the bot publication floor only.
    if track_id != PUBLICATION_TRACK:
        return None
    if not bool(payload.get("active")):
        return None
    return _finite(payload.get("selected_threshold"))


def calibrate_dual_publication_floors(
    samples: Sequence[Mapping[str, Any]],
    *,
    policy: ProbThresholdCalibrationPolicy = ProbThresholdCalibrationPolicy(),
    previous: Optional[Mapping[str, Any]] = None,
    calibrated_at_utc: Optional[str] = None,
    rule_id: Optional[str] = None,
    phase_id: Optional[str] = None,
    publication_track: str = PUBLICATION_TRACK,
) -> dict[str, Any]:
    """Calibrate bot (live) and rolling-ML (shadow) floors side by side."""

    if publication_track != PUBLICATION_TRACK:
        raise ValueError("only the bot publication track is supported")

    tracks: dict[str, Any] = {}
    for track_id, feature in TRACK_FEATURES.items():
        tracks[track_id] = calibrate_publication_min_prob(
            samples,
            policy=policy,
            previous_threshold=_previous_track_threshold(previous, track_id),
            calibrated_at_utc=calibrated_at_utc,
            rule_id=rule_id,
            phase_id=phase_id,
            prob_feature=feature,
            track_id=track_id,
        )

    comparison = summarize_dual_shadow_comparison(
        samples,
        bot_calibration=tracks["bot"],
        rolling_ml_calibration=tracks["rolling_ml"],
        policy=policy,
    )
    publication = tracks[PUBLICATION_TRACK]
    body = {
        "schema_version": 2,
        "version": PROB_THRESHOLD_DUAL_VERSION,
        "policy": policy.manifest(),
        "rule_id": rule_id,
        "phase_id": phase_id,
        "calibrated_at_utc": calibrated_at_utc,
        "publication_track": PUBLICATION_TRACK,
        "shadow_tracks": list(SHADOW_TRACKS),
        "tracks": tracks,
        "shadow_comparison": comparison,
        # Backward-compatible aliases for the live publication gate.
        "active": bool(publication.get("active")),
        "selected_threshold": publication.get("selected_threshold"),
        "selected": publication.get("selected"),
        "selection_reason": publication.get("selection_reason"),
        "baseline": publication.get("baseline"),
        "sample_count_with_prob": publication.get("sample_count_with_prob"),
        "sample_count_ignored": publication.get("sample_count_ignored"),
        "prob_feature": TRACK_FEATURES[PUBLICATION_TRACK],
    }
    body["checksum"] = canonical_hash(
        {key: value for key, value in body.items() if key != "checksum"}
    )
    return body


def slim_dual_calibration_for_cache(
    calibration: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    """Drop heavy per-threshold grids while keeping hysteresis state.

    Per-rule report caches keep selected floors / baselines / shadow
    comparison so a newly promoted champion can warm-start without
    carrying every grid row for every candidate.
    """

    payload = dict(_mapping(calibration))
    if not payload:
        return {}
    tracks_out: dict[str, Any] = {}
    for track_id, track in _mapping(payload.get("tracks")).items():
        slim_track = {
            key: value
            for key, value in dict(track).items()
            if key != "grid"
        }
        tracks_out[str(track_id)] = slim_track
    if tracks_out:
        payload["tracks"] = tracks_out
    # Top-level legacy payloads may also embed a grid.
    payload.pop("grid", None)
    return payload


def calibration_summary_row(
    calibration: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    """Compact operator-facing row for one rule's dual LCB state."""

    payload = _mapping(calibration)
    publication = publication_track_calibration(payload) or {}
    tracks_in = _mapping(payload.get("tracks"))
    tracks_out: dict[str, Any] = {}
    for track_id, track in tracks_in.items():
        track_payload = _mapping(track)
        tracks_out[str(track_id)] = {
            "active": bool(track_payload.get("active")),
            "selected_threshold": track_payload.get("selected_threshold"),
            "selection_reason": track_payload.get("selection_reason"),
            "sample_count_with_prob": track_payload.get(
                "sample_count_with_prob"
            ),
            "baseline": track_payload.get("baseline"),
        }
    comparison = _mapping(payload.get("shadow_comparison"))
    return {
        "rule_id": payload.get("rule_id"),
        "phase_id": payload.get("phase_id"),
        "active": bool(payload.get("active")),
        "selected_threshold": payload.get("selected_threshold"),
        "selection_reason": payload.get("selection_reason"),
        "sample_count_with_prob": payload.get("sample_count_with_prob"),
        "baseline": publication.get("baseline") or payload.get("baseline"),
        "tracks": tracks_out,
        "shadow_comparison": {
            "comparable_resolved": comparison.get("comparable_resolved"),
            "overlap": comparison.get("overlap"),
            "bot_gate": comparison.get("bot_gate"),
            "rolling_ml_gate": comparison.get("rolling_ml_gate"),
        },
    }


def apply_publication_prob_gate(
    *,
    combination_allow: bool,
    bot_prob_to90: Any,
    calibration: Optional[Mapping[str, Any]],
    prob_key: str = "bot_prob_to90",
    unavailable_reason: str = "bot_prob_unavailable_for_calibrated_gate",
) -> dict[str, Any]:
    """AND the combination decision with one calibrated probability floor."""

    track = publication_track_calibration(calibration) or _mapping(calibration)
    threshold = _finite(track.get("selected_threshold"))
    active = bool(track.get("active")) and threshold is not None
    result = {
        "combination_allow": bool(combination_allow),
        "calibration_active": active,
        "selected_threshold": threshold,
        prob_key: _finite(bot_prob_to90),
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
    result[prob_key] = prob
    if prob is None:
        result["allow"] = False
        result["reason"] = unavailable_reason
        return result
    if prob + 1e-12 < float(threshold):
        result["allow"] = False
        result["reason"] = "prob_below_calibrated_threshold"
        return result
    result["allow"] = True
    result["reason"] = "combination_and_calibrated_prob"
    return result


def apply_dual_prob_threshold_gates(
    *,
    combination_allow: bool,
    bot_prob_to90: Any,
    rolling_ml_prob_to90: Any,
    calibration: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    """Apply the live bot gate and record the rolling-ML shadow decision."""

    publication = apply_publication_prob_gate(
        combination_allow=combination_allow,
        bot_prob_to90=bot_prob_to90,
        calibration=calibration,
        prob_key="bot_prob_to90",
        unavailable_reason="bot_prob_unavailable_for_calibrated_gate",
    )
    shadow_ml = apply_publication_prob_gate(
        combination_allow=combination_allow,
        bot_prob_to90=rolling_ml_prob_to90,
        calibration=shadow_track_calibration(calibration, "rolling_ml"),
        prob_key="rolling_ml_prob_to90",
        unavailable_reason="rolling_ml_prob_unavailable_for_calibrated_gate",
    )
    # Re-label shadow reasons so logs stay unambiguous.
    shadow_reason = str(shadow_ml.get("reason") or "")
    if shadow_reason == "combination_and_calibrated_prob":
        shadow_reason = "shadow_combination_and_calibrated_prob"
    elif shadow_reason == "prob_below_calibrated_threshold":
        shadow_reason = "shadow_prob_below_calibrated_threshold"
    elif shadow_reason == "calibration_inactive":
        shadow_reason = "shadow_calibration_inactive"
    shadow_ml = {**shadow_ml, "reason": shadow_reason, "shadow_only": True}

    disagreement = None
    if combination_allow:
        pub_allow = bool(publication.get("allow"))
        sh_allow = bool(shadow_ml.get("allow"))
        if pub_allow and not sh_allow:
            disagreement = "publication_only"
        elif sh_allow and not pub_allow:
            disagreement = "shadow_rolling_ml_only"
        elif pub_allow and sh_allow:
            disagreement = "both_allow"
        else:
            disagreement = "neither"

    return {
        "publication_track": PUBLICATION_TRACK,
        "combination_allow": bool(combination_allow),
        "calibration_active": bool(publication.get("calibration_active")),
        "selected_threshold": publication.get("selected_threshold"),
        "bot_prob_to90": publication.get("bot_prob_to90"),
        "rolling_ml_prob_to90": shadow_ml.get("rolling_ml_prob_to90"),
        "allow": bool(publication.get("allow")),
        "reason": publication.get("reason"),
        "publication": publication,
        "shadow": {"rolling_ml": shadow_ml},
        "disagreement": disagreement,
        "shadow_comparison": _mapping(_mapping(calibration).get("shadow_comparison")),
    }


__all__ = [
    "DEFAULT_LCB_Z",
    "PROB_THRESHOLD_CALIBRATION_VERSION",
    "PROB_THRESHOLD_DUAL_VERSION",
    "PUBLICATION_TRACK",
    "SHADOW_TRACKS",
    "TRACK_FEATURES",
    "ProbThresholdCalibrationPolicy",
    "ThresholdScore",
    "apply_dual_prob_threshold_gates",
    "apply_publication_prob_gate",
    "calibrate_dual_publication_floors",
    "calibrate_publication_min_prob",
    "calibration_inputs_from_feature_values",
    "calibration_summary_row",
    "extract_bot_prob_to90",
    "extract_prob_feature",
    "extract_rolling_ml_prob_to90",
    "hit_rate_lcb",
    "iter_threshold_grid",
    "publication_track_calibration",
    "rolling_ml_prob_from_prediction",
    "shadow_track_calibration",
    "slim_dual_calibration_for_cache",
    "summarize_dual_shadow_comparison",
]
