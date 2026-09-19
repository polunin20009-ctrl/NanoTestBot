from __future__ import annotations

import os
from typing import Iterable, Sequence


# Preserve the current rollout defaults: recent form gets 60% of the weight,
# shrinkage remains anchored to an 8-match prior, and downstream callers can
# still override both values via env without changing the baseline math.
WEIGHT_RECENT = float(os.environ.get("WEIGHT_RECENT", "0.6"))
SHRINKAGE_ALPHA = float(os.environ.get("SHRINKAGE_ALPHA", "8.0"))


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def compute_weighted_average(values: Sequence[float], weights: Sequence[float]) -> float:
    if not values or not weights or len(values) != len(weights):
        return 0.0
    total_weight = float(sum(weights))
    if total_weight <= 0.0:
        return 0.0
    return float(sum(float(weight) * float(value) for value, weight in zip(values, weights)) / total_weight)


def effective_sample_size(weights: Sequence[float]) -> float:
    if not weights:
        return 0.0
    total_weight = float(sum(weights))
    squared_sum = float(sum(float(weight) ** 2 for weight in weights))
    if total_weight <= 0.0 or squared_sum <= 0.0:
        return 0.0
    return float((total_weight ** 2) / squared_sum)


def smooth_metric(
    raw_metric: float,
    weights: Sequence[float],
    league_mean: float,
    alpha: float = SHRINKAGE_ALPHA,
) -> float:
    n_eff = effective_sample_size(weights)
    denominator = n_eff + float(alpha)
    if denominator <= 0.0:
        return float(league_mean)
    return float((n_eff * float(raw_metric) + float(alpha) * float(league_mean)) / denominator)


def get_sample_blend_weight(sample_matches: int) -> float:
    # Keep the existing staged rollout for team-vs-league blending.
    matches = int(sample_matches or 0)
    if matches < 5:
        return 0.0
    if matches < 10:
        return 0.3
    if matches < 20:
        return 0.5
    return 0.7


def blend_with_league(team_metric: float, league_mean: float, sample_matches: int) -> float:
    team_weight = get_sample_blend_weight(sample_matches)
    league_weight = 1.0 - team_weight
    return float(team_weight * float(team_metric) + league_weight * float(league_mean))


def build_two_tier_weights(sample_count: int, recent_share: float = WEIGHT_RECENT) -> list[float]:
    count = max(0, int(sample_count or 0))
    if count <= 0:
        return []
    if count <= 10:
        return [1.0] * count

    # For the 20-match team window, the newest 10 matches carry 60% total
    # weight and the older tier carries the remaining 40%.
    recent_count = min(10, count)
    older_count = max(0, min(10, count - recent_count))
    weights: list[float] = []
    recent_weight = float(recent_share) / recent_count if recent_count > 0 else 0.0
    older_weight = float(1.0 - recent_share) / older_count if older_count > 0 else 0.0

    for index in range(count):
        if index < recent_count:
            weights.append(recent_weight)
        else:
            weights.append(older_weight)
    return weights
