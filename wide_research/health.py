"""Operational health checks for the prospective research pipeline.

This module deliberately does not open any project database or journal.  The
runtime supplies a small, already aggregated snapshot and receives a JSON-safe
report.  Keeping the evaluator pure makes it possible to test alert thresholds
without changing research state.

``AlertTracker`` adds edge-triggered notifications, recovery hysteresis and a
cooldown.  Its state can be stored in an ordinary JSON file with an atomic
replace; it never writes to the research SQLite databases.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


HEALTH_SCHEMA_VERSION = "wide_research_health_v1"
ALERT_STATE_SCHEMA_VERSION = 1

_LEVEL_RANK = {"skipped": 0, "ok": 0, "warning": 1, "critical": 2}


@dataclass(frozen=True)
class HealthPolicy:
    """Thresholds for operational failures, not statistical quality gates."""

    observation_warning_seconds: int = 300
    observation_critical_seconds: int = 900
    outcome_grace_seconds: int = 10_800
    outcome_critical_age_seconds: int = 21_600
    outcome_critical_count: int = 10
    discovery_default_interval_seconds: int = 86_400
    discovery_grace_seconds: int = 3_600
    discovery_max_runtime_seconds: int = 10_800
    # Kept in the serialized policy for backwards-compatible telemetry.  A
    # bounded portfolio is expected to fill, so occupancy fractions alone do
    # not determine severity; actual admission failures do.
    pool_warning_fraction: float = 0.70
    pool_critical_fraction: float = 0.95
    pool_critical_not_admitted: int = 5
    retry_warning_age_seconds: int = 300
    retry_critical_age_seconds: int = 1_800
    retry_critical_failures: int = 3
    disk_warning_fraction: float = 0.10
    disk_critical_fraction: float = 0.05
    disk_warning_bytes: int = 1_073_741_824
    disk_critical_bytes: int = 536_870_912

    def __post_init__(self) -> None:
        positive = (
            self.observation_warning_seconds,
            self.observation_critical_seconds,
            self.outcome_grace_seconds,
            self.outcome_critical_age_seconds,
            self.outcome_critical_count,
            self.discovery_default_interval_seconds,
            self.discovery_grace_seconds,
            self.discovery_max_runtime_seconds,
            self.pool_critical_not_admitted,
            self.retry_warning_age_seconds,
            self.retry_critical_age_seconds,
            self.retry_critical_failures,
            self.disk_warning_bytes,
            self.disk_critical_bytes,
        )
        if any(isinstance(item, bool) or int(item) <= 0 for item in positive):
            raise ValueError("health timing and count thresholds must be positive")
        fractions = (
            self.pool_warning_fraction,
            self.pool_critical_fraction,
            self.disk_warning_fraction,
            self.disk_critical_fraction,
        )
        if any(not 0.0 < float(item) <= 1.0 for item in fractions):
            raise ValueError("health fractions must be in (0, 1]")
        if self.observation_warning_seconds >= self.observation_critical_seconds:
            raise ValueError("observation warning must precede critical threshold")
        if self.pool_warning_fraction >= self.pool_critical_fraction:
            raise ValueError("pool warning must precede critical threshold")
        if self.disk_critical_fraction >= self.disk_warning_fraction:
            raise ValueError("disk critical fraction must be below warning")
        if self.disk_critical_bytes >= self.disk_warning_bytes:
            raise ValueError("disk critical bytes must be below warning")


def _utc(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    else:
        raise ValueError(f"{field} must be an aware timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _optional_utc(value: Any, *, field: str) -> Optional[datetime]:
    if value in (None, ""):
        return None
    return _utc(value, field=field)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _integer(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return max(0, number)


def _number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _age_seconds(now: datetime, timestamp: Optional[datetime]) -> Optional[float]:
    if timestamp is None:
        return None
    return max(0.0, (now - timestamp).total_seconds())


def _check(
    check_id: str,
    level: str,
    message: str,
    *,
    metrics: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    if level not in _LEVEL_RANK:
        raise ValueError(f"unsupported health level: {level}")
    return {
        "id": check_id,
        "level": level,
        "message": str(message),
        "metrics": dict(metrics or {}),
    }


def _observation_check(
    payload: Mapping[str, Any], now: datetime, policy: HealthPolicy
) -> dict[str, Any]:
    active = _integer(payload.get("eligible_active"))
    expected = bool(payload.get("expected_eligible", False)) or active > 0
    last_seen = _optional_utc(payload.get("last_seen_at"), field="observations.last_seen_at")
    age = _age_seconds(now, last_seen)
    metrics = {
        "expected_eligible": expected,
        "eligible_active": active,
        "last_seen_at": _iso(last_seen) if last_seen else None,
        "age_seconds": age,
    }
    if not expected:
        return _check(
            "observations.flow",
            "skipped",
            "No eligible fixtures are currently expected; stale flow is not an incident.",
            metrics=metrics,
        )
    if age is None or age >= policy.observation_critical_seconds:
        return _check(
            "observations.flow", "critical", "Eligible fixtures exist but observations have stopped.", metrics=metrics
        )
    if age >= policy.observation_warning_seconds:
        return _check(
            "observations.flow", "warning", "Observation flow is late while eligible fixtures exist.", metrics=metrics
        )
    return _check("observations.flow", "ok", "Observation flow is current.", metrics=metrics)


def _outcome_check(
    payload: Mapping[str, Any], now: datetime, policy: HealthPolicy
) -> dict[str, Any]:
    grace = _integer(payload.get("grace_seconds"), policy.outcome_grace_seconds)
    if grace <= 0:
        grace = policy.outcome_grace_seconds
    items = payload.get("items")
    pending = _integer(payload.get("pending"))
    overdue = _integer(payload.get("overdue"))
    oldest_finished: Optional[datetime] = _optional_utc(
        payload.get("oldest_pending_at"), field="outcomes.oldest_pending_at"
    )
    if isinstance(items, Sequence) and not isinstance(items, (str, bytes, bytearray)):
        pending = 0
        overdue = 0
        oldest_finished = None
        for index, raw in enumerate(items):
            if not isinstance(raw, Mapping) or raw.get("resolved_at") not in (None, ""):
                continue
            finished = _optional_utc(raw.get("finished_at"), field=f"outcomes.items[{index}].finished_at")
            if finished is None:
                continue
            pending += 1
            oldest_finished = finished if oldest_finished is None else min(oldest_finished, finished)
            if (now - finished).total_seconds() >= grace:
                overdue += 1
    oldest_age = _age_seconds(now, oldest_finished)
    metrics = {
        "pending": pending,
        "overdue": overdue,
        "grace_seconds": grace,
        "oldest_pending_at": _iso(oldest_finished) if oldest_finished else None,
        "oldest_age_seconds": oldest_age,
    }
    if overdue <= 0:
        return _check(
            "outcomes.reconciliation", "ok", "No finished fixture is past the outcome grace period.", metrics=metrics
        )
    critical_age = grace + policy.outcome_critical_age_seconds
    if overdue >= policy.outcome_critical_count or (oldest_age is not None and oldest_age >= critical_age):
        return _check(
            "outcomes.reconciliation", "critical", "Finished fixture outcomes are seriously overdue.", metrics=metrics
        )
    return _check(
        "outcomes.reconciliation", "warning", "Finished fixture outcomes are overdue.", metrics=metrics
    )


def _discovery_checks(
    payload: Mapping[str, Any], now: datetime, policy: HealthPolicy
) -> list[dict[str, Any]]:
    profiles = payload.get("profiles", {})
    if not isinstance(profiles, Mapping) or not profiles:
        return [_check("discovery.profiles", "skipped", "No discovery profiles were supplied.")]
    checks: list[dict[str, Any]] = []
    for raw_name, raw in sorted(profiles.items(), key=lambda item: str(item[0])):
        name = str(raw_name)
        check_id = f"discovery.{name}"
        if not isinstance(raw, Mapping):
            checks.append(_check(check_id, "critical", "Discovery profile snapshot is invalid."))
            continue
        enabled = bool(raw.get("enabled", True))
        failures = _integer(raw.get("failures"))
        interval = _integer(raw.get("interval_seconds"), policy.discovery_default_interval_seconds)
        if interval <= 0:
            interval = policy.discovery_default_interval_seconds
        last_completed = _optional_utc(raw.get("last_completed_at"), field=f"{check_id}.last_completed_at")
        running_since = _optional_utc(raw.get("running_since"), field=f"{check_id}.running_since")
        monitoring_since = _optional_utc(raw.get("monitoring_since"), field=f"{check_id}.monitoring_since")
        completed_age = _age_seconds(now, last_completed)
        runtime = _age_seconds(now, running_since)
        monitored_age = _age_seconds(now, monitoring_since)
        metrics = {
            "enabled": enabled,
            "failures": failures,
            "interval_seconds": interval,
            "last_completed_at": _iso(last_completed) if last_completed else None,
            "completed_age_seconds": completed_age,
            "running_since": _iso(running_since) if running_since else None,
            "runtime_seconds": runtime,
        }
        if not enabled:
            checks.append(_check(check_id, "skipped", "Discovery profile is disabled.", metrics=metrics))
            continue
        if failures >= 3:
            checks.append(_check(check_id, "critical", "Discovery profile has repeated failures.", metrics=metrics))
            continue
        if runtime is not None:
            if runtime >= policy.discovery_max_runtime_seconds:
                checks.append(_check(check_id, "critical", "Discovery cycle appears stuck.", metrics=metrics))
            elif failures:
                checks.append(_check(check_id, "warning", "Discovery cycle is running after a failure.", metrics=metrics))
            else:
                checks.append(_check(check_id, "ok", "Discovery cycle is currently running.", metrics=metrics))
            continue
        due_after = interval + policy.discovery_grace_seconds
        effective_age = completed_age if completed_age is not None else monitored_age
        if effective_age is None or effective_age >= due_after:
            level = "critical" if effective_age is None or effective_age >= due_after + interval else "warning"
            checks.append(_check(check_id, level, "Discovery profile is overdue.", metrics=metrics))
        elif failures:
            checks.append(_check(check_id, "warning", "Latest discovery attempt failed.", metrics=metrics))
        else:
            checks.append(_check(check_id, "ok", "Discovery profile is on schedule.", metrics=metrics))
    return checks


def _pool_checks(payload: Mapping[str, Any], policy: HealthPolicy) -> list[dict[str, Any]]:
    profiles = payload.get("profiles", {})
    if not isinstance(profiles, Mapping) or not profiles:
        return [_check("pools.profiles", "skipped", "No candidate pools were supplied.")]
    checks: list[dict[str, Any]] = []
    for raw_name, raw in sorted(profiles.items(), key=lambda item: str(item[0])):
        name = str(raw_name)
        check_id = f"pools.{name}"
        if not isinstance(raw, Mapping):
            checks.append(_check(check_id, "critical", "Candidate-pool snapshot is invalid."))
            continue
        occupied = _integer(raw.get("occupied"))
        capacity = _integer(raw.get("effective_capacity", raw.get("capacity")))
        configured_capacity = _integer(
            raw.get("configured_capacity", raw.get("capacity"))
        )
        transition_reserve = _integer(raw.get("purge_transition_reserve"))
        transition_active = bool(raw.get("purge_transition_active", False))
        transition_portfolio_deferred = bool(
            raw.get("purge_transition_portfolio_deferred", False)
        )
        not_admitted = _integer(raw.get("not_admitted"))
        fraction = occupied / capacity if capacity > 0 else None
        capacity_state = (
            "invalid"
            if capacity <= 0
            else "over_capacity"
            if occupied > capacity
            else "full"
            if occupied == capacity
            else "available"
        )
        metrics = {
            "occupied": occupied,
            "capacity": capacity,
            "configured_capacity": configured_capacity,
            "effective_capacity": capacity,
            "purge_transition_reserve": transition_reserve,
            "purge_transition_active": transition_active,
            "purge_transition_portfolio_deferred": (
                transition_portfolio_deferred
            ),
            "occupancy_fraction": fraction,
            "capacity_state": capacity_state,
            "not_admitted": not_admitted,
        }
        if (
            capacity <= 0
            or configured_capacity <= 0
            or capacity < configured_capacity
            or occupied > capacity
            or (
                transition_active
                and (
                    transition_reserve <= 0
                    or capacity
                    != configured_capacity + transition_reserve
                )
            )
        ):
            checks.append(_check(check_id, "critical", "Candidate-pool counters are invalid.", metrics=metrics))
        elif transition_portfolio_deferred:
            checks.append(
                _check(
                    check_id,
                    "critical",
                    "A validation-selected portfolio was deferred during the purge transition.",
                    metrics=metrics,
                )
            )
        elif not_admitted >= policy.pool_critical_not_admitted:
            checks.append(
                _check(
                    check_id,
                    "critical",
                    "Candidate-pool capacity rejected many discovered candidates.",
                    metrics=metrics,
                )
            )
        elif not_admitted > 0:
            checks.append(
                _check(
                    check_id,
                    "warning",
                    "Candidate-pool capacity rejected a discovered candidate.",
                    metrics=metrics,
                )
            )
        elif transition_active:
            checks.append(
                _check(
                    check_id,
                    "ok",
                    "Candidate pool is using its bounded purge-transition reserve as planned.",
                    metrics=metrics,
                )
            )
        elif capacity_state == "full":
            checks.append(
                _check(
                    check_id,
                    "ok",
                    "Candidate pool is at its bounded capacity with no rejected candidates.",
                    metrics=metrics,
                )
            )
        else:
            checks.append(_check(check_id, "ok", "Candidate pool has available capacity.", metrics=metrics))
    return checks


def _retry_check(payload: Mapping[str, Any], now: datetime, policy: HealthPolicy) -> dict[str, Any]:
    pending = _integer(payload.get("pending"))
    failed = _integer(payload.get("failed"))
    oldest = _optional_utc(payload.get("oldest_pending_at"), field="retries.oldest_pending_at")
    age = _age_seconds(now, oldest)
    metrics = {
        "pending": pending,
        "failed": failed,
        "oldest_pending_at": _iso(oldest) if oldest else None,
        "oldest_age_seconds": age,
    }
    if failed >= policy.retry_critical_failures or (age is not None and age >= policy.retry_critical_age_seconds):
        return _check("retries.durable", "critical", "Durable research writes are repeatedly failing or stuck.", metrics=metrics)
    if failed > 0 or (pending > 0 and (age is None or age >= policy.retry_warning_age_seconds)):
        return _check("retries.durable", "warning", "Durable research writes require retry attention.", metrics=metrics)
    return _check("retries.durable", "ok", "Durable research retry queue is healthy.", metrics=metrics)


def _disk_check(payload: Any, policy: HealthPolicy) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or not payload:
        return _check("disk.space", "skipped", "Disk statistics were not supplied.")
    free = _number(payload.get("free_bytes"))
    total = _number(payload.get("total_bytes"))
    if free is None or total is None or free < 0 or total <= 0 or free > total:
        return _check("disk.space", "critical", "Disk statistics are invalid.")
    fraction = free / total
    metrics = {"free_bytes": int(free), "total_bytes": int(total), "free_fraction": fraction}
    if free <= policy.disk_critical_bytes or fraction <= policy.disk_critical_fraction:
        return _check("disk.space", "critical", "Research storage is critically low.", metrics=metrics)
    if free <= policy.disk_warning_bytes or fraction <= policy.disk_warning_fraction:
        return _check("disk.space", "warning", "Research storage is running low.", metrics=metrics)
    return _check("disk.space", "ok", "Research storage has sufficient free space.", metrics=metrics)


def evaluate_health(
    snapshot: Mapping[str, Any],
    now: datetime | str,
    policy: HealthPolicy = HealthPolicy(),
) -> dict[str, Any]:
    """Evaluate one read-only operational snapshot.

    No health status represents prediction quality.  In particular, ``ok``
    means that data collection is functioning, not that a rule reaches 93%.
    """

    if not isinstance(snapshot, Mapping):
        raise TypeError("snapshot must be a mapping")
    current = _utc(now, field="now")
    observations = snapshot.get("observations", {})
    outcomes = snapshot.get("outcomes", {})
    discovery = snapshot.get("discovery", {})
    pools = snapshot.get("pools", {})
    retries = snapshot.get("retries", {})
    sections = (observations, outcomes, discovery, pools, retries)
    if any(not isinstance(section, Mapping) for section in sections):
        raise TypeError("health snapshot sections must be mappings")

    checks = [
        _observation_check(observations, current, policy),
        _outcome_check(outcomes, current, policy),
        *_discovery_checks(discovery, current, policy),
        *_pool_checks(pools, policy),
        _retry_check(retries, current, policy),
        _disk_check(snapshot.get("disk"), policy),
    ]
    worst = max((_LEVEL_RANK[item["level"]] for item in checks), default=0)
    overall = "critical" if worst >= 2 else "warning" if worst == 1 else "ok"
    return {
        "schema_version": HEALTH_SCHEMA_VERSION,
        "evaluated_at": _iso(current),
        "overall": overall,
        "checks": checks,
        "counts": {
            level: sum(1 for item in checks if item["level"] == level)
            for level in ("critical", "warning", "ok", "skipped")
        },
        "policy": asdict(policy),
    }


@dataclass(frozen=True)
class AlertPolicy:
    warning_after: int = 2
    critical_after: int = 1
    recover_after: int = 2
    reminder_seconds: int = 21_600

    def __post_init__(self) -> None:
        values = (self.warning_after, self.critical_after, self.recover_after, self.reminder_seconds)
        if any(isinstance(item, bool) or int(item) <= 0 for item in values):
            raise ValueError("alert thresholds must be positive integers")


class AlertTracker:
    """Turn repeated health reports into deduplicated alert/recovery events."""

    def __init__(
        self,
        *,
        policy: AlertPolicy = AlertPolicy(),
        state: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.policy = policy
        self._checks: dict[str, dict[str, Any]] = {}
        if state is not None:
            raw_checks = state.get("checks", {}) if isinstance(state, Mapping) else {}
            if isinstance(raw_checks, Mapping):
                for raw_id, raw in raw_checks.items():
                    if not isinstance(raw, Mapping):
                        continue
                    try:
                        last_emitted = _optional_utc(
                            raw.get("last_emitted_at"),
                            field=f"alerts.{raw_id}.last_emitted_at",
                        )
                    except ValueError:
                        last_emitted = None
                    self._checks[str(raw_id)] = {
                        "active": bool(raw.get("active", False)),
                        "unhealthy_count": _integer(raw.get("unhealthy_count")),
                        "healthy_count": _integer(raw.get("healthy_count")),
                        "level": str(raw.get("level", "ok")),
                        "message": str(raw.get("message", "")),
                        "last_emitted_at": _iso(last_emitted) if last_emitted else None,
                    }

    def update(self, report: Mapping[str, Any], now: datetime | str) -> list[dict[str, Any]]:
        current = _utc(now, field="now")
        checks = report.get("checks", []) if isinstance(report, Mapping) else []
        if not isinstance(checks, Sequence) or isinstance(checks, (str, bytes, bytearray)):
            raise TypeError("health report checks must be a sequence")
        events: list[dict[str, Any]] = []
        for raw in checks:
            if not isinstance(raw, Mapping) or not raw.get("id"):
                continue
            check_id = str(raw["id"])
            level = str(raw.get("level", "critical"))
            if level not in _LEVEL_RANK:
                level = "critical"
            message = str(raw.get("message", ""))
            metrics = dict(raw.get("metrics", {})) if isinstance(raw.get("metrics"), Mapping) else {}
            state = self._checks.setdefault(
                check_id,
                {
                    "active": False,
                    "unhealthy_count": 0,
                    "healthy_count": 0,
                    "level": "ok",
                    "message": "",
                    "last_emitted_at": None,
                },
            )
            unhealthy = _LEVEL_RANK[level] > 0
            previous_level = str(state.get("level", "ok"))
            if unhealthy:
                state["healthy_count"] = 0
                state["unhealthy_count"] = _integer(state.get("unhealthy_count")) + 1
                required = self.policy.critical_after if level == "critical" else self.policy.warning_after
                should_emit = False
                kind = "firing"
                if not state["active"] and state["unhealthy_count"] >= required:
                    state["active"] = True
                    should_emit = True
                elif state["active"] and _LEVEL_RANK[level] > _LEVEL_RANK.get(previous_level, 0):
                    should_emit = True
                    kind = "escalated"
                elif state["active"]:
                    last = _optional_utc(state.get("last_emitted_at"), field=f"alerts.{check_id}.last_emitted_at")
                    if last is None or (current - last).total_seconds() >= self.policy.reminder_seconds:
                        should_emit = True
                        kind = "reminder"
                if should_emit:
                    state["last_emitted_at"] = _iso(current)
                    events.append(
                        {
                            "kind": kind,
                            "check_id": check_id,
                            "level": level,
                            "message": message,
                            "metrics": metrics,
                            "at": _iso(current),
                        }
                    )
            else:
                state["unhealthy_count"] = 0
                if state["active"]:
                    state["healthy_count"] = _integer(state.get("healthy_count")) + 1
                    if state["healthy_count"] >= self.policy.recover_after:
                        events.append(
                            {
                                "kind": "resolved",
                                "check_id": check_id,
                                "level": "ok",
                                "previous_level": previous_level,
                                "message": "Operational health recovered.",
                                "metrics": metrics,
                                "at": _iso(current),
                            }
                        )
                        state["active"] = False
                        state["healthy_count"] = 0
                        state["last_emitted_at"] = _iso(current)
                else:
                    state["healthy_count"] = 0
            state["level"] = level
            state["message"] = message
        return events

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": ALERT_STATE_SCHEMA_VERSION,
            "policy": asdict(self.policy),
            "checks": {key: dict(value) for key, value in sorted(self._checks.items())},
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "AlertTracker":
        raw_policy = payload.get("policy", {}) if isinstance(payload, Mapping) else {}
        try:
            policy = (
                AlertPolicy(
                    **{
                        key: raw_policy[key]
                        for key in (
                            "warning_after",
                            "critical_after",
                            "recover_after",
                            "reminder_seconds",
                        )
                        if key in raw_policy
                    }
                )
                if isinstance(raw_policy, Mapping) and raw_policy
                else AlertPolicy()
            )
        except (TypeError, ValueError):
            policy = AlertPolicy()
        return cls(policy=policy, state=payload)

    @classmethod
    def load(cls, path: os.PathLike[str] | str, *, policy: Optional[AlertPolicy] = None) -> "AlertTracker":
        source = Path(path)
        if not source.exists():
            return cls(policy=policy or AlertPolicy())
        try:
            payload = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return cls(policy=policy or AlertPolicy())
        if policy is not None:
            return cls(policy=policy, state=payload if isinstance(payload, Mapping) else None)
        return cls.from_dict(payload) if isinstance(payload, Mapping) else cls()

    def save_atomic(self, path: os.PathLike[str] | str) -> None:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent)
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(self.to_dict(), handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_name, destination)
            try:
                directory_fd = os.open(destination.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            except OSError:
                pass
        finally:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass


__all__ = [
    "ALERT_STATE_SCHEMA_VERSION",
    "HEALTH_SCHEMA_VERSION",
    "AlertPolicy",
    "AlertTracker",
    "HealthPolicy",
    "evaluate_health",
]
