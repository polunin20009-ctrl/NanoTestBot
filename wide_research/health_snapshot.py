"""Read-only aggregation for :mod:`wide_research.health`.

All source paths and runtime counters are explicit arguments.  The collector
does not import the bot, discover files by convention, or instantiate the
writable research store.  SQLite databases are opened with ``mode=ro`` and
``PRAGMA query_only``; journals and summaries are opened without following
symlinks.
"""

from __future__ import annotations

import copy
import json
import os
import sqlite3
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence
from urllib.parse import quote


SNAPSHOT_SCHEMA_VERSION = "wide_research_health_snapshot_v1"
_EVALUATED_PHASE_STATUSES = ("candidate", "shadow", "ready", "active")


@dataclass(frozen=True)
class ProfileSnapshotSpec:
    """Explicit read sources and runtime state for one discovery profile."""

    profile_id: str
    database_path: os.PathLike[str] | str
    summary_path: os.PathLike[str] | str | None
    capacity: int
    interval_seconds: int
    enabled: bool = True
    running_since: str | datetime | None = None
    monitoring_since: str | datetime | None = None
    failures: int = 0
    not_admitted: int | None = None

    def __post_init__(self) -> None:
        if not str(self.profile_id).strip():
            raise ValueError("profile_id must be non-empty")
        if isinstance(self.capacity, bool) or int(self.capacity) <= 0:
            raise ValueError("capacity must be a positive integer")
        if isinstance(self.interval_seconds, bool) or int(self.interval_seconds) <= 0:
            raise ValueError("interval_seconds must be a positive integer")
        if isinstance(self.failures, bool) or int(self.failures) < 0:
            raise ValueError("failures must be a non-negative integer")
        if self.not_admitted is not None and (
            isinstance(self.not_admitted, bool) or int(self.not_admitted) < 0
        ):
            raise ValueError("not_admitted must be a non-negative integer")


@dataclass(frozen=True)
class ObservationJournalSpec:
    """Bounded-tail policy for the active observation JSONL journal."""

    path: os.PathLike[str] | str
    record_types: tuple[str, ...] = ("observation",)
    stages: tuple[str, ...] = ()
    timestamp_fields: tuple[str, ...] = (
        "created_at_utc",
        "observed_at_utc",
        "captured_at_utc",
    )
    max_tail_bytes: int = 2 * 1024 * 1024

    def __post_init__(self) -> None:
        if isinstance(self.max_tail_bytes, bool) or int(self.max_tail_bytes) <= 0:
            raise ValueError("max_tail_bytes must be positive")
        if not self.timestamp_fields:
            raise ValueError("at least one timestamp field is required")


def _iso_utc(value: Any, field: str) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field} is not an ISO-8601 timestamp") from exc
    else:
        raise ValueError(f"{field} is not a timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _later_timestamp(*values: Optional[str]) -> Optional[str]:
    present = [value for value in values if value]
    if not present:
        return None
    return max(present, key=lambda value: datetime.fromisoformat(value.replace("Z", "+00:00")))


def _earlier_timestamp(*values: Optional[str]) -> Optional[str]:
    present = [value for value in values if value]
    if not present:
        return None
    return min(present, key=lambda value: datetime.fromisoformat(value.replace("Z", "+00:00")))


def _non_negative_int(value: Any, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return max(0, int(value))
    except (TypeError, ValueError, OverflowError):
        return default


def _summary_integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if parsed < minimum:
        raise ValueError(f"{field} must be >= {minimum}")
    return parsed


def _open_regular_readonly(path: os.PathLike[str] | str) -> tuple[int, os.stat_result, str]:
    source = os.path.abspath(os.fspath(path))
    flags = os.O_RDONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(source, flags)
    try:
        information = os.fstat(descriptor)
        if not stat.S_ISREG(information.st_mode):
            raise ValueError("source must be a regular file")
        return descriptor, information, source
    except Exception:
        os.close(descriptor)
        raise


def _read_json_object(
    path: os.PathLike[str] | str, *, max_bytes: int = 16 * 1024 * 1024
) -> dict[str, Any]:
    descriptor, information, _ = _open_regular_readonly(path)
    try:
        if information.st_size > max_bytes:
            raise ValueError("JSON summary exceeds the read limit")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            block = os.read(descriptor, min(remaining, 64 * 1024))
            if not block:
                break
            chunks.append(block)
            remaining -= len(block)
        raw = b"".join(chunks)
    finally:
        os.close(descriptor)
    if len(raw) > max_bytes:
        raise ValueError("JSON summary exceeds the read limit")
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("JSON summary must contain an object")
    return payload


def _tail_observation_timestamp(spec: ObservationJournalSpec) -> tuple[Optional[str], dict[str, Any]]:
    descriptor, information, source = _open_regular_readonly(spec.path)
    try:
        start = max(0, information.st_size - int(spec.max_tail_bytes))
        os.lseek(descriptor, start, os.SEEK_SET)
        raw = os.read(descriptor, int(spec.max_tail_bytes))
    finally:
        os.close(descriptor)
    if start > 0:
        separator = raw.find(b"\n")
        raw = raw[separator + 1 :] if separator >= 0 else b""

    allowed_types = {str(value) for value in spec.record_types}
    allowed_stages = {str(value).strip().lower() for value in spec.stages}
    latest: Optional[str] = None
    scanned = 0
    malformed = 0
    matched = 0
    for line in raw.splitlines():
        if not line.strip():
            continue
        scanned += 1
        try:
            record = json.loads(line.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError):
            malformed += 1
            continue
        if not isinstance(record, Mapping):
            continue
        if allowed_types and str(record.get("record_type") or "") not in allowed_types:
            continue
        if allowed_stages and str(record.get("stage") or "").strip().lower() not in allowed_stages:
            continue
        timestamp: Optional[str] = None
        for field in spec.timestamp_fields:
            if record.get(field) in (None, ""):
                continue
            try:
                timestamp = _iso_utc(record.get(field), f"observation.{field}")
            except ValueError:
                continue
            if timestamp:
                break
        if timestamp is None:
            continue
        matched += 1
        latest = _later_timestamp(latest, timestamp)
    return latest, {
        "path": source,
        "bytes_read": len(raw),
        "tail_truncated": start > 0,
        "records_scanned": scanned,
        "records_malformed": malformed,
        "records_matched": matched,
    }


def _sqlite_uri(path: str) -> str:
    return "file:" + quote(path, safe="/") + "?mode=ro"


def _read_profile_database(spec: ProfileSnapshotSpec) -> dict[str, Any]:
    descriptor, _, source = _open_regular_readonly(spec.database_path)
    os.close(descriptor)
    connection = sqlite3.connect(_sqlite_uri(source), uri=True, timeout=0.25)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=250")
        bound = connection.execute(
            "SELECT profile_id FROM store_profile WHERE singleton=1"
        ).fetchone()
        if bound is None or str(bound[0]) != str(spec.profile_id):
            raise ValueError("SQLite store_profile does not match profile spec")
        placeholders = ",".join("?" for _ in _EVALUATED_PHASE_STATUSES)
        occupied = int(
            connection.execute(
                f"SELECT COUNT(*) FROM phases WHERE status IN ({placeholders})",
                _EVALUATED_PHASE_STATUSES,
            ).fetchone()[0]
        )
        completed = connection.execute(
            "SELECT MAX(completed_at_utc) FROM research_runs WHERE status='completed'"
        ).fetchone()[0]
        running = connection.execute(
            "SELECT MIN(started_at_utc) FROM research_runs WHERE status='running'"
        ).fetchone()[0]
        last_completed = _iso_utc(completed, "research_runs.completed_at_utc")
        running_since = _iso_utc(running, "research_runs.started_at_utc")
        return {
            "path": source,
            "occupied": occupied,
            "last_completed_at": last_completed,
            "running_since": running_since,
        }
    finally:
        connection.close()


def _summary_values(spec: ProfileSnapshotSpec) -> dict[str, Any]:
    if spec.summary_path is None:
        return {
            "present": False,
            "last_completed_at": None,
            "not_admitted": 0,
            "pool_limit": None,
            "pool_effective_limit": None,
            "purge_transition_reserve": 0,
            "purge_transition_portfolio_deferred": False,
        }
    payload = _read_json_object(spec.summary_path)
    reported_profile = payload.get("store_profile")
    if reported_profile not in (None, "") and str(reported_profile) != str(spec.profile_id):
        raise ValueError("summary store_profile does not match profile spec")
    completed = None
    if str(payload.get("cycle_type") or "") == "discovery":
        completed = _iso_utc(payload.get("completed_at_utc"), "summary.completed_at_utc")
    registry = payload.get("registry")
    not_admitted = 0
    pool_limit: int | None = None
    effective_limit: int | None = None
    transition_reserve = 0
    transition_portfolio_deferred = False
    if isinstance(registry, Mapping):
        raw = registry.get("not_admitted", 0)
        if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
            not_admitted = len(raw)
        else:
            not_admitted = _non_negative_int(raw)
        transition_portfolio_deferred = bool(
            registry.get("purge_transition_portfolio_deferred", False)
        )
        pool = registry.get("pool")
        if pool is not None and not isinstance(pool, Mapping):
            raise ValueError("summary.registry.pool must be an object")
        if isinstance(pool, Mapping):
            if pool.get("limit") is not None:
                pool_limit = _summary_integer(
                    pool.get("limit"), "summary.registry.pool.limit", minimum=1
                )
            if pool.get("effective_limit") is not None:
                effective_limit = _summary_integer(
                    pool.get("effective_limit"),
                    "summary.registry.pool.effective_limit",
                    minimum=1,
                )
            if pool.get("purge_transition_reserve") is not None:
                transition_reserve = _summary_integer(
                    pool.get("purge_transition_reserve"),
                    "summary.registry.pool.purge_transition_reserve",
                )
            if effective_limit is None and pool_limit is not None:
                effective_limit = pool_limit + transition_reserve
            if (
                pool_limit is not None
                and effective_limit is not None
                and effective_limit < pool_limit
            ):
                raise ValueError(
                    "summary registry effective pool limit is below its limit"
                )
            if (
                transition_reserve
                and pool_limit is not None
                and effective_limit is not None
                and effective_limit != pool_limit + transition_reserve
            ):
                raise ValueError(
                    "summary registry transition reserve does not match effective limit"
                )
    return {
        "present": True,
        "path": os.path.abspath(os.fspath(spec.summary_path)),
        "run_id": str(payload.get("run_id") or "") or None,
        "cycle_type": str(payload.get("cycle_type") or "") or None,
        "last_completed_at": completed,
        "not_admitted": not_admitted,
        "pool_limit": pool_limit,
        "pool_effective_limit": effective_limit,
        "purge_transition_reserve": transition_reserve,
        "purge_transition_portfolio_deferred": transition_portfolio_deferred,
    }


def _source_error(source: str, path: Any, exc: BaseException, *, profile_id: str | None = None) -> dict[str, Any]:
    result = {
        "source": source,
        "path": os.path.abspath(os.fspath(path)) if path is not None else None,
        "error_type": type(exc).__name__,
        "message": str(exc),
    }
    if profile_id is not None:
        result["profile_id"] = profile_id
    return result


def build_health_snapshot(
    *,
    profiles: Sequence[ProfileSnapshotSpec],
    observation_journal: ObservationJournalSpec | None,
    expected_eligible: bool,
    eligible_active: int = 0,
    outcome_counters: Mapping[str, Any] | None = None,
    retry_counters: Mapping[str, Any] | None = None,
    disk_path: os.PathLike[str] | str | None = None,
) -> dict[str, Any]:
    """Collect a JSON-safe runtime snapshot without modifying any source.

    ``outcome_counters`` and ``retry_counters`` are copied from runtime-owned
    aggregates.  The collector never attempts to infer full-time state from
    historical journals.
    """

    if isinstance(eligible_active, bool) or int(eligible_active) < 0:
        raise ValueError("eligible_active must be a non-negative integer")
    if outcome_counters is not None and not isinstance(outcome_counters, Mapping):
        raise TypeError("outcome_counters must be a mapping")
    if retry_counters is not None and not isinstance(retry_counters, Mapping):
        raise TypeError("retry_counters must be a mapping")

    discovery_profiles: dict[str, Any] = {}
    pool_profiles: dict[str, Any] = {}
    source_profiles: dict[str, Any] = {}
    errors: list[dict[str, Any]] = []
    seen: set[str] = set()
    for spec in profiles:
        if not isinstance(spec, ProfileSnapshotSpec):
            raise TypeError("profiles must contain ProfileSnapshotSpec values")
        profile_id = str(spec.profile_id)
        if profile_id in seen:
            raise ValueError(f"duplicate profile_id: {profile_id}")
        seen.add(profile_id)
        database: dict[str, Any] = {}
        summary: dict[str, Any] = {}
        profile_error_count = 0
        try:
            database = _read_profile_database(spec)
        except (OSError, sqlite3.Error, UnicodeError, ValueError) as exc:
            errors.append(_source_error("profile_database", spec.database_path, exc, profile_id=profile_id))
            profile_error_count += 1
        try:
            summary = _summary_values(spec)
        except (OSError, json.JSONDecodeError, UnicodeError, ValueError) as exc:
            errors.append(_source_error("discovery_summary", spec.summary_path, exc, profile_id=profile_id))
            profile_error_count += 1

        explicit_running = _iso_utc(spec.running_since, f"profiles.{profile_id}.running_since")
        monitoring_since = _iso_utc(spec.monitoring_since, f"profiles.{profile_id}.monitoring_since")
        last_completed = _later_timestamp(
            database.get("last_completed_at"), summary.get("last_completed_at")
        )
        running_since = _earlier_timestamp(explicit_running, database.get("running_since"))
        not_admitted = (
            int(spec.not_admitted)
            if spec.not_admitted is not None
            else _non_negative_int(summary.get("not_admitted"))
        )
        discovery_profiles[profile_id] = {
            "enabled": bool(spec.enabled),
            "last_completed_at": last_completed,
            "interval_seconds": int(spec.interval_seconds),
            "running_since": running_since,
            "monitoring_since": monitoring_since,
            "failures": int(spec.failures) + profile_error_count,
        }
        configured_capacity = int(spec.capacity)
        reported_effective_capacity = summary.get("pool_effective_limit")
        effective_capacity = (
            max(configured_capacity, int(reported_effective_capacity))
            if reported_effective_capacity is not None
            else configured_capacity
        )
        reported_transition_reserve = _non_negative_int(
            summary.get("purge_transition_reserve")
        )
        pool_profiles[profile_id] = {
            "occupied": database.get("occupied"),
            "capacity": effective_capacity if "occupied" in database else 0,
            "configured_capacity": configured_capacity,
            "effective_capacity": (
                effective_capacity if "occupied" in database else 0
            ),
            "reported_configured_capacity": summary.get("pool_limit"),
            "purge_transition_reserve": reported_transition_reserve,
            "purge_transition_active": bool(
                reported_transition_reserve
                and effective_capacity > configured_capacity
            ),
            "purge_transition_portfolio_deferred": bool(
                summary.get("purge_transition_portfolio_deferred", False)
            ),
            "not_admitted": not_admitted,
        }
        source_profiles[profile_id] = {"database": database, "summary": summary}

    observation_timestamp: Optional[str] = None
    observation_source: dict[str, Any] = {"present": observation_journal is not None}
    if observation_journal is not None:
        if not isinstance(observation_journal, ObservationJournalSpec):
            raise TypeError("observation_journal must be an ObservationJournalSpec")
        try:
            observation_timestamp, details = _tail_observation_timestamp(observation_journal)
            observation_source.update(details)
        except (OSError, UnicodeError, ValueError) as exc:
            errors.append(_source_error("observation_journal", observation_journal.path, exc))

    disk: dict[str, Any] = {}
    disk_source: dict[str, Any] = {"present": disk_path is not None}
    if disk_path is not None:
        try:
            target = os.path.abspath(os.fspath(disk_path))
            usage = os.statvfs(target)
            disk = {
                "free_bytes": int(usage.f_bavail * usage.f_frsize),
                "total_bytes": int(usage.f_blocks * usage.f_frsize),
            }
            disk_source["path"] = target
        except (OSError, ValueError) as exc:
            errors.append(_source_error("disk", disk_path, exc))
            # Invalid values make an explicitly requested disk check fail
            # closed in evaluate_health instead of silently becoming skipped.
            disk = {"free_bytes": -1, "total_bytes": 0}

    return {
        "snapshot_schema_version": SNAPSHOT_SCHEMA_VERSION,
        "observations": {
            "expected_eligible": bool(expected_eligible),
            "eligible_active": int(eligible_active),
            "last_seen_at": observation_timestamp,
        },
        "outcomes": copy.deepcopy(dict(outcome_counters or {})),
        "discovery": {"profiles": discovery_profiles},
        "pools": {"profiles": pool_profiles},
        "retries": copy.deepcopy(dict(retry_counters or {})),
        "disk": disk,
        "sources": {
            "ok": not errors,
            "errors": errors,
            "profiles": source_profiles,
            "observation_journal": observation_source,
            "disk": disk_source,
        },
    }


__all__ = [
    "SNAPSHOT_SCHEMA_VERSION",
    "ObservationJournalSpec",
    "ProfileSnapshotSpec",
    "build_health_snapshot",
]
