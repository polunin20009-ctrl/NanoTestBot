"""Statistical lifecycle gates for the wide rule factory.

The functions in this module are deliberately pure.  Live storage and the
Telegram router may use the same decisions without either component being
able to weaken the pre-registered policy.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import NormalDist
from typing import Any, Iterable, Mapping, Optional, Sequence


LIFECYCLE_POLICY_VERSION = "wide_lifecycle_v1"
ACTIVE_MANIFEST_SCHEMA_VERSION = 1
READY = "READY"
SHADOW = "SHADOW"
CHAMPION = "CHAMPION"
DEGRADED = "DEGRADED"
PAUSED = "PAUSED"


def _finite(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def wilson_interval(wins: int, total: int, z: float = 1.959963984540054) -> tuple[float, float]:
    """Return the two-sided Wilson interval as fractions in ``[0, 1]``."""

    n = int(total)
    w = int(wins)
    if n <= 0 or w < 0 or w > n:
        return 0.0, 1.0
    p = w / n
    z2 = z * z
    denominator = 1.0 + z2 / n
    centre = (p + z2 / (2.0 * n)) / denominator
    margin = z * math.sqrt((p * (1.0 - p) + z2 / (4.0 * n)) / n) / denominator
    return max(0.0, centre - margin), min(1.0, centre + margin)


def binomial_upper_tail(wins: int, total: int, null_rate: float) -> float:
    """Exact ``P[X >= wins]`` for ``X ~ Binomial(total, null_rate)``.

    The intended lifecycle sample sizes are a few hundred, for which the
    stdlib implementation is both deterministic and sufficiently fast.
    """

    n = int(total)
    k = int(wins)
    p = float(null_rate)
    if n < 0 or k < 0 or k > n or not 0.0 <= p <= 1.0:
        raise ValueError("invalid binomial arguments")
    if k == 0:
        return 1.0
    if p == 0.0:
        return 0.0
    if p == 1.0:
        return 1.0
    # Summing the probability masses directly can overflow at larger sample
    # sizes even though the final probability is in [0, 1].  Work in log
    # space and use log-sum-exp so every pre-registered milestone is safe.
    log_terms = [
        math.lgamma(n + 1)
        - math.lgamma(successes + 1)
        - math.lgamma(n - successes + 1)
        + successes * math.log(p)
        + (n - successes) * math.log1p(-p)
        for successes in range(k, n + 1)
    ]
    largest = max(log_terms)
    tail = math.exp(largest) * math.fsum(
        math.exp(item - largest) for item in log_terms
    )
    return min(1.0, max(0.0, tail))


def holm_adjusted_pass(
    p_value: float,
    *,
    family_size: int,
    rank: int,
    alpha: float,
) -> bool:
    """Conservative Holm step-down threshold for a pre-ranked hypothesis."""

    m = max(1, int(family_size))
    position = min(m, max(1, int(rank)))
    return float(p_value) <= float(alpha) / (m - position + 1)


@dataclass(frozen=True)
class LifecyclePolicy:
    """Immutable gates for automatic READY/promotion/degradation decisions."""

    version: str = LIFECYCLE_POLICY_VERSION
    target_hit_rate: float = 0.95
    null_hit_rate: float = 0.90
    min_resolved: int = 200
    min_span_days: float = 28.0
    min_trigger_days: int = 14
    min_leagues: int = 8
    max_league_share: float = 0.35
    min_wilson_lower: float = 0.90
    min_weekly_windows: int = 4
    min_weekly_hit_rate: float = 0.90
    min_triggers_per_week: float = 2.0
    family_alpha: float = 0.05
    allowed_looks: tuple[int, ...] = (50, 100, 150, 200, 300, 500, 750, 1000)
    champion_min_tenure_days: float = 14.0
    degradation_fast_n: int = 50
    degradation_fast_rate: float = 0.80
    degradation_slow_n: int = 150
    degradation_null_rate: float = 0.90
    degradation_confidence: float = 0.95

    def __post_init__(self) -> None:
        rates = (
            self.target_hit_rate,
            self.null_hit_rate,
            self.max_league_share,
            self.min_wilson_lower,
            self.min_weekly_hit_rate,
            self.family_alpha,
            self.degradation_fast_rate,
            self.degradation_null_rate,
            self.degradation_confidence,
        )
        if any(not 0.0 <= float(value) <= 1.0 for value in rates):
            raise ValueError("lifecycle rates must be in [0, 1]")
        if self.target_hit_rate < self.null_hit_rate:
            raise ValueError("target_hit_rate cannot be below null_hit_rate")
        counts = (
            self.min_resolved,
            self.min_trigger_days,
            self.min_leagues,
            self.min_weekly_windows,
            self.degradation_fast_n,
            self.degradation_slow_n,
        )
        if any(int(value) <= 0 for value in counts):
            raise ValueError("lifecycle counts must be positive")
        looks = tuple(int(item) for item in self.allowed_looks)
        if not looks or looks != tuple(sorted(set(looks))) or looks[0] <= 0:
            raise ValueError("allowed_looks must be sorted unique positive integers")

    def manifest(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GateDecision:
    eligible: bool
    target_state: str
    reasons: tuple[str, ...]
    metrics: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "eligible": self.eligible,
            "target_state": self.target_state,
            "reasons": list(self.reasons),
            "metrics": dict(self.metrics),
        }


def _weekly_rows(metrics: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    value = metrics.get("weekly")
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [item for item in value if isinstance(item, Mapping)]
    return []


def _milestone_look(total: int, allowed: Sequence[int]) -> Optional[tuple[int, int]]:
    """Return one-based look rank only when ``total`` is at a closed milestone."""

    values = tuple(int(item) for item in allowed)
    if int(total) not in values:
        return None
    return values.index(int(total)) + 1, len(values)


def evaluate_readiness(
    metrics: Mapping[str, Any],
    *,
    policy: LifecyclePolicy = LifecyclePolicy(),
    family_size: int = 1,
    family_rank: int = 1,
    alpha_budget: Optional[float] = None,
    require_closed_milestone: bool = True,
    milestone_closed: Optional[bool] = None,
) -> GateDecision:
    """Evaluate a fixed prospective phase for READY eligibility.

    Historical metrics must never be passed here.  The caller is responsible
    for selecting an immutable prospective ``phase_id``.
    """

    wins = int(metrics.get("wins") or 0)
    losses = int(metrics.get("losses") or 0)
    total = wins + losses
    rate = wins / total if total else 0.0
    lower, upper = wilson_interval(wins, total)
    span_days = float(_finite(metrics.get("span_days")) or 0.0)
    trigger_days = int(metrics.get("trigger_days") or 0)
    leagues = int(metrics.get("leagues") or 0)
    max_share = float(_finite(metrics.get("max_league_share")) or 1.0)
    triggers_per_week = float(_finite(metrics.get("triggers_per_week")) or 0.0)
    weekly = _weekly_rows(metrics)
    stable_weeks = sum(
        1
        for row in weekly
        if int(row.get("wins") or 0) + int(row.get("losses") or 0) > 0
        and int(row.get("wins") or 0)
        / (int(row.get("wins") or 0) + int(row.get("losses") or 0))
        >= policy.min_weekly_hit_rate
    )
    p_value = binomial_upper_tail(wins, total, policy.null_hit_rate) if total else 1.0
    reasons: list[str] = []
    milestone = _milestone_look(total, policy.allowed_looks)
    if milestone_closed is False:
        milestone = None
    if require_closed_milestone and milestone is None:
        reasons.append("not_closed_milestone")
    if total < policy.min_resolved:
        reasons.append("insufficient_resolved")
    if rate < policy.target_hit_rate:
        reasons.append("hit_rate_below_target")
    if lower < policy.min_wilson_lower:
        reasons.append("wilson_lower_below_gate")
    if span_days < policy.min_span_days:
        reasons.append("insufficient_span_days")
    if trigger_days < policy.min_trigger_days:
        reasons.append("insufficient_trigger_days")
    if leagues < policy.min_leagues:
        reasons.append("insufficient_leagues")
    if max_share > policy.max_league_share:
        reasons.append("league_concentration")
    if triggers_per_week < policy.min_triggers_per_week:
        reasons.append("insufficient_frequency")
    if len(weekly) < policy.min_weekly_windows or stable_weeks < policy.min_weekly_windows:
        reasons.append("weekly_instability")
    effective_alpha = (
        policy.family_alpha
        if alpha_budget is None
        else float(alpha_budget)
    )
    if not math.isfinite(effective_alpha) or not 0.0 < effective_alpha <= 1.0:
        raise ValueError("alpha_budget must be finite and in (0, 1]")
    look_rank, look_count = milestone or (len(policy.allowed_looks), len(policy.allowed_looks))
    look_alpha = effective_alpha / max(1, look_count)
    if not holm_adjusted_pass(
        p_value,
        family_size=max(1, int(family_size)),
        rank=max(1, int(family_rank)),
        alpha=look_alpha,
    ):
        reasons.append("multiple_testing_gate")
    summary = {
        "wins": wins,
        "losses": losses,
        "resolved": total,
        "hit_rate": rate,
        "wilson_lower": lower,
        "wilson_upper": upper,
        "span_days": span_days,
        "trigger_days": trigger_days,
        "leagues": leagues,
        "max_league_share": max_share,
        "triggers_per_week": triggers_per_week,
        "weekly_windows": len(weekly),
        "stable_weekly_windows": stable_weeks,
        "binomial_p_value_vs_null": p_value,
        "closed_milestone": total if milestone else None,
        "alpha_budget": effective_alpha,
        "look_alpha": look_alpha,
        "policy_version": policy.version,
    }
    return GateDecision(not reasons, READY if not reasons else SHADOW, tuple(reasons), summary)


def evaluate_degradation(
    *,
    recent_fast: Mapping[str, Any],
    recent_slow: Mapping[str, Any],
    tenure_days: float,
    policy: LifecyclePolicy = LifecyclePolicy(),
) -> GateDecision:
    """Use hysteresis: a short severe failure or strong slow evidence is needed."""

    fast_wins = int(recent_fast.get("wins") or 0)
    fast_losses = int(recent_fast.get("losses") or 0)
    fast_n = fast_wins + fast_losses
    fast_rate = fast_wins / fast_n if fast_n else 1.0
    slow_wins = int(recent_slow.get("wins") or 0)
    slow_losses = int(recent_slow.get("losses") or 0)
    slow_n = slow_wins + slow_losses
    confidence = float(policy.degradation_confidence)
    z_value = NormalDist().inv_cdf((1.0 + confidence) / 2.0)
    _slow_lower, slow_upper = wilson_interval(
        slow_wins,
        slow_n,
        z=z_value,
    )
    reasons: list[str] = []
    if float(tenure_days) < policy.champion_min_tenure_days:
        reasons.append("minimum_tenure")
    severe_fast = (
        fast_n >= policy.degradation_fast_n
        and fast_rate < policy.degradation_fast_rate
    )
    significant_slow = (
        slow_n >= policy.degradation_slow_n
        and slow_upper < policy.degradation_null_rate
    )
    degraded = (severe_fast or significant_slow) and not reasons
    if not severe_fast:
        reasons.append("fast_window_not_degraded")
    if not significant_slow:
        reasons.append("slow_window_not_degraded")
    return GateDecision(
        degraded,
        DEGRADED if degraded else CHAMPION,
        tuple(reasons),
        {
            "fast_resolved": fast_n,
            "fast_hit_rate": fast_rate,
            "slow_resolved": slow_n,
            "slow_wilson_upper": slow_upper,
            "tenure_days": float(tenure_days),
            "policy_version": policy.version,
        },
    )


def canonical_checksum(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("checksum", None)
    encoded = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_active_manifest(
    *,
    generation: int,
    rule: Optional[Mapping[str, Any]],
    effective_from_utc: str,
    previous_rule_id: Optional[str],
    production_enabled: bool,
    fallback: str = "current_filter",
) -> dict[str, Any]:
    if type(generation) is not int or generation < 1:
        raise ValueError("generation must be an integer >= 1")
    payload: dict[str, Any] = {
        "schema_version": ACTIVE_MANIFEST_SCHEMA_VERSION,
        "generation": int(generation),
        "effective_from_utc": str(effective_from_utc),
        "production_enabled": bool(production_enabled),
        "fallback": str(fallback),
        "previous_rule_id": previous_rule_id,
        "rule": dict(rule) if isinstance(rule, Mapping) else None,
    }
    payload["checksum"] = canonical_checksum(payload)
    return payload


def validate_active_manifest(payload: Any) -> bool:
    if not isinstance(payload, Mapping):
        return False
    schema_version = payload.get("schema_version")
    generation = payload.get("generation")
    if type(schema_version) is not int or type(generation) is not int:
        return False
    if schema_version != ACTIVE_MANIFEST_SCHEMA_VERSION:
        return False
    if generation < 1:
        return False
    if str(payload.get("fallback") or "") != "current_filter":
        return False
    checksum = str(payload.get("checksum") or "")
    if not checksum:
        return False
    try:
        return checksum == canonical_checksum(payload)
    except (TypeError, ValueError, OverflowError):
        return False


def write_active_manifest_atomic(path: os.PathLike[str] | str, payload: Mapping[str, Any]) -> None:
    """Atomically publish a checksummed last-known-good production pointer."""

    if not validate_active_manifest(payload):
        raise ValueError("invalid active manifest")
    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent)
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, destination)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


class ActiveManifestCache:
    """Fail closed while retaining a previously validated manifest in memory."""

    def __init__(self, path: os.PathLike[str] | str) -> None:
        self.path = Path(path).expanduser().resolve()
        self._signature: Optional[tuple[int, int]] = None
        self._last_good: Optional[dict[str, Any]] = None

    def load(self, *, force: bool = False) -> Optional[dict[str, Any]]:
        try:
            stat = self.path.stat()
            signature = (int(stat.st_mtime_ns), int(stat.st_size))
        except OSError:
            return self._last_good
        if not force and signature == self._signature:
            return self._last_good
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, ValueError, TypeError):
            return self._last_good
        if not validate_active_manifest(payload):
            return self._last_good
        current_raw = (self._last_good or {}).get("generation")
        current_generation = -1 if current_raw is None else int(current_raw)
        incoming_generation = int(payload.get("generation"))
        if incoming_generation < current_generation:
            return self._last_good
        if (
            incoming_generation == current_generation
            and self._last_good is not None
            and payload.get("checksum") != self._last_good.get("checksum")
        ):
            return self._last_good
        self._signature = signature
        self._last_good = dict(payload)
        return dict(self._last_good)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
