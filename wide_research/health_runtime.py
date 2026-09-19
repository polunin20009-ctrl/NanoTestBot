"""Runtime counters and an independent operational reporting thread.

The monitor only writes its report and alert-state files.  Profile databases,
discovery summaries and the observation journal are passed to the read-only
snapshot collector.  The retry callback is deliberately separate: callers may
use it to replay their own durable queues without making report collection
itself writable.
"""
from __future__ import annotations

import copy
import json
import logging
import math
import os
import tempfile
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypeAlias

from .health import AlertTracker, evaluate_health
from .health_snapshot import (
    ObservationJournalSpec,
    ProfileSnapshotSpec,
    build_health_snapshot,
)


PathValue: TypeAlias = str | os.PathLike[str]
ProfileProvider: TypeAlias = Callable[
    [Mapping[str, Mapping[str, Any]], str], Sequence[ProfileSnapshotSpec]
]
RetryProvider: TypeAlias = Callable[[], Mapping[str, Any]]


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
        raise ValueError(f"{field} must be a timezone-aware timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _now() -> str:
    return _iso(datetime.now(timezone.utc))


def _timestamp(value: Any | None, *, field: str) -> str:
    return _iso(_utc(_now() if value is None else value, field=field))


def _age(now: datetime, value: Any) -> float | None:
    if value in (None, ""):
        return None
    return max(0.0, (now - _utc(value, field="runtime timestamp")).total_seconds())


def _non_negative_integer(value: Any, *, field: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be a non-negative integer")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise TypeError(f"{field} must be a non-negative integer") from exc
    if result < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return result


def _positive_seconds(value: Any, *, field: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be a positive finite number")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise TypeError(f"{field} must be a positive finite number") from exc
    if not math.isfinite(result) or result <= 0.0:
        raise ValueError(f"{field} must be a positive finite number")
    return result


def _non_negative_seconds(value: Any, *, field: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be a non-negative finite number")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise TypeError(f"{field} must be a non-negative finite number") from exc
    if not math.isfinite(result) or result < 0.0:
        raise ValueError(f"{field} must be a non-negative finite number")
    return result


def _counter(value: Any) -> int:
    """Best-effort counter coercion for data returned by a failed callback."""

    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError, OverflowError):
        return 0


def _absolute_path(value: PathValue, *, field: str) -> str:
    try:
        path = os.fspath(value)
    except TypeError as exc:
        raise TypeError(f"{field} must be a filesystem path") from exc
    if not path:
        raise ValueError(f"{field} must be a non-empty filesystem path")
    return os.path.abspath(path)


def _canonical_path(value: PathValue) -> str:
    return os.path.normcase(os.path.realpath(os.path.abspath(os.fspath(value))))


def write_report(path: PathValue, payload: Mapping[str, Any]) -> None:
    """Atomically replace one JSON report and durably publish its directory."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent)
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, destination)
        try:
            directory_fd = os.open(destination.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            # Some filesystems do not support fsync on directories.  The file
            # itself has still been flushed and atomically replaced.
            pass
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


class ResearchHealthMonitor:
    """Own runtime heartbeats, retry scheduling and health-report publication."""

    def __init__(
        self,
        *,
        profiles: ProfileProvider,
        retry: RetryProvider,
        observation_path: PathValue,
        report_path: PathValue,
        state_path: PathValue,
        disk_path: PathValue,
        interval_seconds: float = 300,
        retry_interval_seconds: float = 60,
        logger: logging.Logger | None = None,
    ) -> None:
        if not callable(profiles):
            raise TypeError("profiles must be callable")
        if not callable(retry):
            raise TypeError("retry must be callable")
        self.profiles = profiles
        self.retry = retry
        # Freeze path interpretation at construction.  A later chdir must not
        # redirect reports or make the monitor inspect a different journal.
        self.observation_path = _absolute_path(
            observation_path, field="observation_path"
        )
        self.report_path = _absolute_path(report_path, field="report_path")
        self.state_path = _absolute_path(state_path, field="state_path")
        self.disk_path = _absolute_path(disk_path, field="disk_path")
        self.interval = _positive_seconds(
            interval_seconds, field="interval_seconds"
        )
        self.retry_interval = _positive_seconds(
            retry_interval_seconds, field="retry_interval_seconds"
        )
        outputs = {
            _canonical_path(self.report_path),
            _canonical_path(self.state_path),
        }
        if len(outputs) != 2:
            raise ValueError("report_path and state_path must be distinct")
        if _canonical_path(self.observation_path) in outputs:
            raise ValueError("health outputs must not overlap the observation journal")

        self.logger = logger or logging.getLogger(__name__)
        self.started_at = _now()
        self.lock = threading.RLock()
        self._collect_lock = threading.Lock()
        self._lifecycle_lock = threading.RLock()
        self.state: dict[str, Any] = {
            "feed_at": None,
            "eligible_active": 0,
            "observation_at": None,
            "reconcile_at": None,
            "pending": 0,
            "oldest_pending_at": None,
            "discovery": {},
        }
        self.alerts = AlertTracker.load(self.state_path)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    @property
    def is_running(self) -> bool:
        with self._lifecycle_lock:
            return bool(self.thread and self.thread.is_alive())

    def note_feed(
        self, eligible_active: int, *, at: str | datetime | None = None
    ) -> None:
        count = _non_negative_integer(eligible_active, field="eligible_active")
        timestamp = _timestamp(at, field="feed_at")
        with self.lock:
            self.state.update(feed_at=timestamp, eligible_active=count)

    def note_observation(self, at: str | datetime) -> None:
        timestamp = _timestamp(at, field="observation_at")
        with self.lock:
            self.state["observation_at"] = timestamp

    def note_pending(
        self,
        count: int,
        oldest_at: str | datetime | None,
        *,
        at: str | datetime | None = None,
    ) -> None:
        pending = _non_negative_integer(count, field="pending")
        reconciled_at = _timestamp(at, field="reconcile_at")
        oldest = (
            _timestamp(oldest_at, field="oldest_pending_at")
            if oldest_at is not None and pending
            else None
        )
        with self.lock:
            self.state.update(
                reconcile_at=reconciled_at,
                pending=pending,
                oldest_pending_at=oldest,
            )

    def note_discovery(
        self,
        profile: str,
        result: Mapping[str, Any] | None = None,
        *,
        at: str | datetime | None = None,
    ) -> None:
        name = str(profile).strip()
        if not name:
            raise ValueError("profile must be non-empty")
        if result is not None and not isinstance(result, Mapping):
            raise TypeError("discovery result must be a mapping")
        timestamp = _timestamp(at, field=f"discovery.{name}.at")
        with self.lock:
            row = self.state["discovery"].setdefault(
                name,
                {
                    "failures": 0,
                    "active_runs": 0,
                    "running_since": None,
                },
            )
            active_runs = _non_negative_integer(
                row.get("active_runs", 0), field="active_runs"
            )
            if result is None:
                row["active_runs"] = active_runs + 1
                existing = row.get("running_since")
                if existing is None or _utc(
                    timestamp, field="discovery running timestamp"
                ) < _utc(existing, field="discovery running_since"):
                    row["running_since"] = timestamp
                return

            row["active_runs"] = max(0, active_runs - 1)
            if row["active_runs"] == 0:
                row["running_since"] = None
            row["failures"] = (
                0
                if result.get("status") == "completed"
                else _non_negative_integer(
                    row.get("failures", 0), field="discovery failures"
                )
                + 1
            )

    def _profile_specs(
        self, discovery_state: Mapping[str, Mapping[str, Any]]
    ) -> tuple[ProfileSnapshotSpec, ...]:
        supplied = self.profiles(discovery_state, self.started_at)
        if isinstance(supplied, (str, bytes, bytearray)) or not isinstance(
            supplied, Sequence
        ):
            raise TypeError("profiles callback must return a sequence")
        specifications = tuple(supplied)
        if any(not isinstance(spec, ProfileSnapshotSpec) for spec in specifications):
            raise TypeError("profiles callback must return ProfileSnapshotSpec values")

        outputs = {
            _canonical_path(self.report_path),
            _canonical_path(self.state_path),
        }
        sources = {_canonical_path(self.observation_path)}
        for spec in specifications:
            sources.add(_canonical_path(spec.database_path))
            if spec.summary_path is not None:
                sources.add(_canonical_path(spec.summary_path))
        if outputs & sources:
            raise ValueError("health outputs must not overlap monitored sources")
        return specifications

    def collect(
        self,
        *,
        now: datetime | None = None,
        retry_counters: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Collect and publish one report without invoking the retry callback."""

        if retry_counters is not None and not isinstance(retry_counters, Mapping):
            raise TypeError("retry_counters must be a mapping")
        current = _utc(
            datetime.now(timezone.utc) if now is None else now, field="now"
        )
        with self._collect_lock:
            with self.lock:
                state = copy.deepcopy(self.state)
            specifications = self._profile_specs(state["discovery"])
            snapshot = build_health_snapshot(
                profiles=specifications,
                observation_journal=ObservationJournalSpec(
                    self.observation_path,
                    stages=("decision_pipeline", "wide_monitor"),
                ),
                expected_eligible=state["eligible_active"] > 0,
                eligible_active=state["eligible_active"],
                retry_counters=dict(retry_counters or {}),
                disk_path=self.disk_path,
            )
            cached = state["observation_at"]
            cached_age = _age(current, cached)
            journal_age = _age(
                current, snapshot["observations"]["last_seen_at"]
            )
            if cached_age is not None and (
                journal_age is None or cached_age < journal_age
            ):
                snapshot["observations"]["last_seen_at"] = cached

            report = evaluate_health(snapshot, current)
            feed_age = _age(current, state["feed_at"] or self.started_at)
            reconcile_age = _age(
                current, state["reconcile_at"] or self.started_at
            )
            pending_age = (
                _age(current, state["oldest_pending_at"])
                if state["pending"]
                else 0.0
            )
            extra = [
                {
                    "id": "runtime.live_feed",
                    "level": (
                        "critical"
                        if feed_age is None or feed_age >= 1800
                        else "warning" if feed_age >= 600 else "ok"
                    ),
                    "message": "Live fixture feed heartbeat.",
                    "metrics": {"age_seconds": feed_age},
                },
                {
                    "id": "runtime.reconciliation",
                    "level": (
                        "critical"
                        if reconcile_age is None or reconcile_age >= 3600
                        else "warning" if reconcile_age >= 1200 else "ok"
                    ),
                    "message": "Observation outcome reconciliation heartbeat.",
                    "metrics": {"age_seconds": reconcile_age},
                },
                {
                    "id": "outcomes.unresolved_evidence",
                    "level": (
                        "critical"
                        if state["pending"]
                        and (pending_age is None or pending_age >= 86_400)
                        else "warning"
                        if state["pending"]
                        and pending_age is not None
                        and pending_age >= 43_200
                        else "ok"
                    ),
                    "message": (
                        "Age of observations still awaiting a terminal outcome "
                        "(not an inferred full-time status)."
                    ),
                    "metrics": {
                        "pending_fixtures": state["pending"],
                        "oldest_observation_at": state["oldest_pending_at"],
                        "age_seconds": pending_age,
                    },
                },
                {
                    "id": "sources.reads",
                    "level": (
                        "warning" if snapshot["sources"]["errors"] else "ok"
                    ),
                    "message": "Research health source readability.",
                    "metrics": {"errors": snapshot["sources"]["errors"]},
                },
            ]
            report["checks"].extend(extra)
            report["counts"] = {
                level: sum(row["level"] == level for row in report["checks"])
                for level in ("critical", "warning", "ok", "skipped")
            }
            report["overall"] = (
                "critical"
                if report["counts"]["critical"]
                else "warning" if report["counts"]["warning"] else "ok"
            )
            report["snapshot"] = snapshot
            report["runtime"] = state
            report["alert_events"] = self.alerts.update(report, current)

            write_error: BaseException | None = None
            try:
                write_report(self.report_path, report)
            except (OSError, TypeError, ValueError) as exc:
                write_error = exc
            try:
                self.alerts.save_atomic(self.state_path)
            except (OSError, TypeError, ValueError) as exc:
                if write_error is None:
                    write_error = exc

            # Emit edge-triggered events even if one operational output is
            # temporarily unwritable.  The in-memory tracker still suppresses
            # duplicate events for the lifetime of this monitor.
            for event in report["alert_events"]:
                self.logger.log(
                    logging.INFO
                    if event["kind"] == "resolved"
                    else logging.ERROR
                    if event["level"] == "critical"
                    else logging.WARNING,
                    "[RESEARCH_HEALTH_ALERT] %s",
                    json.dumps(event, ensure_ascii=False),
                )
            if write_error is not None:
                raise RuntimeError("failed to persist research health output") from write_error
            self.logger.info(
                "[RESEARCH_HEALTH] overall=%s checks=%s report=%s",
                report["overall"],
                report["counts"],
                self.report_path,
            )
            return report

    def _run(self) -> None:
        next_retry = next_report = time.monotonic()
        retry_counters: dict[str, Any] = {}
        consecutive_retry_errors = 0
        while not self.stop_event.is_set():
            current = time.monotonic()
            if current >= next_retry:
                try:
                    supplied = self.retry()
                    if not isinstance(supplied, Mapping):
                        raise TypeError("retry callback must return a mapping")
                    retry_counters = copy.deepcopy(dict(supplied))
                    consecutive_retry_errors = 0
                except Exception as exc:
                    consecutive_retry_errors += 1
                    retry_counters = dict(retry_counters)
                    retry_counters["failed"] = max(
                        _counter(retry_counters.get("failed", 0)),
                        consecutive_retry_errors,
                    )
                    retry_counters["error_type"] = type(exc).__name__
                    self.logger.exception(
                        "[RESEARCH_HEALTH_ERROR] retry_tick_failed"
                    )
                next_retry = time.monotonic() + self.retry_interval

            if self.stop_event.is_set():
                break
            current = time.monotonic()
            if current >= next_report:
                try:
                    self.collect(retry_counters=retry_counters)
                except Exception:
                    self.logger.exception(
                        "[RESEARCH_HEALTH_ERROR] periodic_check_failed"
                    )
                next_report = time.monotonic() + self.interval

            wait_seconds = max(
                0.0,
                min(next_retry, next_report) - time.monotonic(),
            )
            self.stop_event.wait(wait_seconds)

    def start(self) -> bool:
        """Start the daemon once; return whether a new thread was created."""

        with self._lifecycle_lock:
            if self.thread and self.thread.is_alive():
                return False
            self.stop_event.clear()
            thread = threading.Thread(
                target=self._run,
                name="research-health",
                daemon=True,
            )
            self.thread = thread
            try:
                thread.start()
            except Exception:
                self.thread = None
                self.stop_event.set()
                raise
            return True

    def stop(self, timeout: float = 5.0) -> bool:
        """Request shutdown and return ``True`` only after the thread exits."""

        bounded_timeout = _non_negative_seconds(timeout, field="timeout")
        with self._lifecycle_lock:
            self.stop_event.set()
            thread = self.thread
            if thread is None:
                return True
            if thread is threading.current_thread():
                return False
            thread.join(bounded_timeout)
            if thread.is_alive():
                return False
            if self.thread is thread:
                self.thread = None
            return True


__all__ = ["ResearchHealthMonitor", "write_report"]
