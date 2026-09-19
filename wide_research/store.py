"""Durable source-of-truth storage for wide prospective research.

The store deliberately keeps all candidate state in SQLite.  In particular,
there is no JSONL replay or in-memory set whose cost grows with the number of
observations.  Every public write uses ``BEGIN IMMEDIATE`` and is therefore
safe when several bot workers race to claim the same first trigger.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import math
import os
import sqlite3
import stat
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional, Sequence, Union


PathLike = Union[str, os.PathLike[str]]

SCHEMA_VERSION = 2
DEFAULT_MAX_DB_BYTES = 4 * 1024 * 1024 * 1024
DEFAULT_MAX_LIVE_RETRY_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_LIVE_RETRY_RECORDS = 20_000
DEFAULT_MAX_LIVE_RETRY_RECORD_BYTES = 2 * 1024 * 1024
OUTCOME_VERSION_MULTIPLIER = 1_000_000_000
SQLITE_MAX_INTEGER = (1 << 63) - 1

PHASE_STATUSES = frozenset(
    {
        "candidate",
        "shadow",
        "ready",
        "active",
        "degraded",
        "paused",
        "retired",
        "rejected",
        "failed",
    }
)
EVALUATED_PHASE_STATUSES = frozenset(
    {"candidate", "shadow", "ready", "active"}
)
TERMINAL_PHASE_STATUSES = frozenset({"retired", "rejected"})
OUTCOME_STATUSES = frozenset({"win", "loss", "pending", "invalid"})


class WideResearchStoreError(RuntimeError):
    """Base exception for wide-research persistence failures."""


class UnsafeDatabasePathError(WideResearchStoreError, ValueError):
    """Raised when the requested database path is unsafe or ambiguous."""


class DatabaseSizeLimitError(WideResearchStoreError):
    """Raised before a write when the configured storage budget is exhausted."""


class LiveRetryCapacityError(WideResearchStoreError):
    """Raised when the bounded live-evaluation inbox cannot accept a record."""


class ImmutableRecordError(WideResearchStoreError, ValueError):
    """Raised when a caller tries to mutate an immutable identity/version."""


class ConcurrentUpdateError(WideResearchStoreError):
    """Raised when a generation/status compare-and-swap no longer matches."""


class InvalidLifecycleTransition(WideResearchStoreError, ValueError):
    """Raised for an invalid or terminal phase transition."""


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"value is not canonical JSON: {exc}") from exc


def _json_hash(canonical_json: str) -> str:
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _required_text(value: Any, name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{name} must be a non-empty string")
    if "\x00" in normalized:
        raise ValueError(f"{name} must not contain NUL")
    return normalized


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _utc_timestamp(value: Any, name: str, *, optional: bool = False) -> Optional[str]:
    if value is None or value == "":
        if optional:
            return None
        raise ValueError(f"{name} is required")
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _minute(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("minute must be numeric") from exc
    if not math.isfinite(parsed) or parsed < 0.0 or parsed > 200.0:
        raise ValueError("minute must be finite and between 0 and 200")
    return parsed


def _outcome_version(value: Any) -> int:
    """Validate legacy schema versions and packed schema/revision versions.

    Values below ``OUTCOME_VERSION_MULTIPLIER`` are legacy schema versions.
    Larger values are the packed ``schema * multiplier + revision`` form.  A
    multiple with revision zero is ambiguous with a very large legacy schema
    and would give two raw versions the same ordering rank, so it is rejected.
    """

    if isinstance(value, bool):
        raise ValueError("outcome version must be an integer")
    try:
        version = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("outcome version must be an integer") from exc
    if isinstance(value, float) and (
        not math.isfinite(value) or not value.is_integer()
    ):
        raise ValueError("outcome version must be an integer")
    if version < 1 or version > SQLITE_MAX_INTEGER:
        raise ValueError("outcome version is outside SQLite integer range")
    if (
        version >= OUTCOME_VERSION_MULTIPLIER
        and version % OUTCOME_VERSION_MULTIPLIER == 0
    ):
        raise ValueError("packed outcome version must include a revision")
    return version


def _outcome_version_rank(version: int) -> int:
    """Return the comparable schema-first rank for either storage format."""

    return (
        version * OUTCOME_VERSION_MULTIPLIER
        if version < OUTCOME_VERSION_MULTIPLIER
        else version
    )


def safe_database_path(
    path: PathLike,
    *,
    allowed_root: Optional[PathLike] = None,
) -> str:
    """Resolve and validate a file-backed SQLite path.

    An explicit symlink at the database path is rejected.  When
    ``allowed_root`` is supplied, parent-directory symlinks are resolved first
    and the final target must remain below that root.
    """

    raw = os.fspath(path)
    if not raw or raw == ":memory:" or "\x00" in raw:
        raise UnsafeDatabasePathError("a real file path is required")
    absolute = os.path.abspath(os.path.expanduser(raw))
    if os.path.lexists(absolute) and os.path.islink(absolute):
        raise UnsafeDatabasePathError("database path must not be a symlink")
    resolved = os.path.realpath(absolute)
    if resolved in {os.path.sep, os.path.expanduser("~")}:
        raise UnsafeDatabasePathError("database target is too broad")
    if allowed_root is not None:
        root = os.path.realpath(
            os.path.abspath(os.path.expanduser(os.fspath(allowed_root)))
        )
        try:
            inside = os.path.commonpath((root, resolved)) == root
        except ValueError as exc:
            raise UnsafeDatabasePathError("database and root are incompatible") from exc
        if not inside or resolved == root:
            raise UnsafeDatabasePathError("database must be a file below allowed_root")
    if os.path.exists(resolved):
        mode = os.stat(resolved, follow_symlinks=False).st_mode
        if not stat.S_ISREG(mode):
            raise UnsafeDatabasePathError("database target must be a regular file")
    parent = os.path.dirname(resolved)
    if not parent or os.path.exists(parent) and not os.path.isdir(parent):
        raise UnsafeDatabasePathError("database parent must be a directory")
    return resolved


class WideResearchStore:
    """SQLite source of truth for discovery, prospective phases and promotion."""

    def __init__(
        self,
        path: PathLike,
        *,
        allowed_root: Optional[PathLike] = None,
        max_db_bytes: int = DEFAULT_MAX_DB_BYTES,
        busy_timeout_ms: int = 30_000,
        max_live_retry_bytes: int = DEFAULT_MAX_LIVE_RETRY_BYTES,
        max_live_retry_records: int = DEFAULT_MAX_LIVE_RETRY_RECORDS,
        max_live_retry_record_bytes: int = DEFAULT_MAX_LIVE_RETRY_RECORD_BYTES,
    ) -> None:
        self.path = safe_database_path(path, allowed_root=allowed_root)
        self.max_db_bytes = int(max_db_bytes)
        self.busy_timeout_ms = max(1, int(busy_timeout_ms))
        self.max_live_retry_bytes = int(max_live_retry_bytes)
        self.max_live_retry_records = int(max_live_retry_records)
        self.max_live_retry_record_bytes = int(max_live_retry_record_bytes)
        if self.max_db_bytes <= 0:
            raise ValueError("max_db_bytes must be positive")
        if (
            self.max_live_retry_bytes <= 0
            or self.max_live_retry_records <= 0
            or self.max_live_retry_record_bytes <= 0
        ):
            raise ValueError("live retry capacity limits must be positive")
        parent = os.path.dirname(self.path)
        Path(parent).mkdir(mode=0o700, parents=True, exist_ok=True)
        self._initialize()
        self._secure_files()

    def _connect(self) -> sqlite3.Connection:
        self._validate_runtime_paths()
        old_umask = os.umask(0o077)
        try:
            connection = sqlite3.connect(
                self.path,
                timeout=self.busy_timeout_ms / 1000.0,
                isolation_level=None,
            )
        finally:
            os.umask(old_umask)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA wal_autocheckpoint=1000")
        self._secure_files()
        return connection

    def _validate_runtime_paths(self) -> None:
        if os.path.realpath(self.path) != self.path:
            raise UnsafeDatabasePathError("database path changed after validation")
        for candidate in (self.path, self.path + "-wal", self.path + "-shm"):
            # A WAL or SHM file may legitimately disappear between two
            # syscalls when SQLite checkpoints it.  One non-following stat
            # avoids treating that normal lifecycle as a storage failure.
            try:
                mode = os.lstat(candidate).st_mode
            except FileNotFoundError:
                continue
            if stat.S_ISLNK(mode):
                raise UnsafeDatabasePathError(
                    f"SQLite file must not be a symlink: {candidate}"
                )
            if not stat.S_ISREG(mode):
                raise UnsafeDatabasePathError(
                    f"SQLite file must be regular: {candidate}"
                )

    def _secure_files(self) -> None:
        for candidate in (self.path, self.path + "-wal", self.path + "-shm"):
            try:
                mode = os.stat(candidate, follow_symlinks=False).st_mode
                if not stat.S_ISREG(mode):
                    raise UnsafeDatabasePathError(
                        f"SQLite companion is not a regular file: {candidate}"
                    )
                os.chmod(candidate, 0o600, follow_symlinks=False)
            except FileNotFoundError:
                continue

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            for statement in _SCHEMA_STATEMENTS:
                connection.execute(statement)
            current = connection.execute("PRAGMA user_version").fetchone()[0]
            if current not in (0, 1, SCHEMA_VERSION):
                raise WideResearchStoreError(
                    f"unsupported schema version {current}; expected <= {SCHEMA_VERSION}"
                )
            connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def database_size_bytes(self) -> int:
        total = 0
        for candidate in (self.path, self.path + "-wal", self.path + "-shm"):
            try:
                total += os.path.getsize(candidate)
            except FileNotFoundError:
                continue
        return total

    @property
    def profile_id(self) -> Optional[str]:
        connection = self._connect()
        try:
            row = connection.execute("SELECT profile_id FROM store_profile WHERE singleton=1").fetchone()
            return str(row[0]) if row is not None else None
        finally:
            connection.close()

    def bind_profile(self, profile_id: str) -> Dict[str, str]:
        """Permanently bind this database to one isolated runtime profile."""

        profile = _required_text(profile_id, "profile_id")
        now = _utc_now()
        with self._write(projected_bytes=len(profile) + 256) as connection:
            row = connection.execute(
                "SELECT profile_id, created_at_utc FROM store_profile WHERE singleton=1"
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO store_profile(singleton, profile_id, created_at_utc) "
                    "VALUES(1, ?, ?)",
                    (profile, now),
                )
                return {"profile_id": profile, "created_at_utc": now}
            bound = str(row["profile_id"])
            if bound != profile:
                raise WideResearchStoreError(
                    "wide-research database profile mismatch: "
                    f"bound={bound} requested={profile}"
                )
            return {
                "profile_id": bound,
                "created_at_utc": str(row["created_at_utc"]),
            }

    def _ensure_capacity(self, projected_bytes: int = 0) -> None:
        current = self.database_size_bytes()
        if current + max(0, int(projected_bytes)) > self.max_db_bytes:
            raise DatabaseSizeLimitError(
                "wide-research database size limit reached: "
                f"current={current} projected={projected_bytes} "
                f"limit={self.max_db_bytes}"
            )

    @contextlib.contextmanager
    def _write(self, *, projected_bytes: int = 0) -> Iterator[sqlite3.Connection]:
        self._ensure_capacity(projected_bytes)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._ensure_capacity(projected_bytes)
            yield connection
            self._ensure_capacity(0)
            connection.commit()
            self._secure_files()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextlib.contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def _decode_row(row: sqlite3.Row) -> Dict[str, Any]:
        result = dict(row)
        for key in tuple(result):
            if key.endswith("_json") and result[key] is not None:
                decoded_key = key[:-5]
                result[decoded_key] = json.loads(str(result[key]))
        return result

    def start_run(
        self,
        run_id: str,
        *,
        config: Optional[Mapping[str, Any]] = None,
        started_at_utc: Any = None,
    ) -> Dict[str, Any]:
        run = _required_text(run_id, "run_id")
        explicit_started = started_at_utc is not None
        started = _utc_timestamp(started_at_utc or _utc_now(), "started_at_utc")
        config_json = _canonical_json(dict(config or {}))
        with self._write(projected_bytes=len(config_json) + 512) as connection:
            existing = connection.execute(
                "SELECT * FROM research_runs WHERE run_id=?", (run,)
            ).fetchone()
            if existing is not None:
                if existing["config_json"] != config_json or (
                    explicit_started and existing["started_at_utc"] != started
                ):
                    raise ImmutableRecordError(f"research run {run} already differs")
                return self._decode_row(existing)
            connection.execute(
                """
                INSERT INTO research_runs(
                    run_id, status, started_at_utc, completed_at_utc,
                    config_json, created_at_utc
                ) VALUES (?, 'running', ?, NULL, ?, ?)
                """,
                (run, started, config_json, _utc_now()),
            )
            row = connection.execute(
                "SELECT * FROM research_runs WHERE run_id=?", (run,)
            ).fetchone()
            assert row is not None
            return self._decode_row(row)

    def finish_run(
        self,
        run_id: str,
        *,
        status: str = "completed",
        completed_at_utc: Any = None,
    ) -> Dict[str, Any]:
        run = _required_text(run_id, "run_id")
        normalized_status = _required_text(status, "status")
        if normalized_status not in {"completed", "failed", "cancelled"}:
            raise ValueError("run status must be completed, failed, or cancelled")
        completed = _utc_timestamp(
            completed_at_utc or _utc_now(), "completed_at_utc"
        )
        with self._write(projected_bytes=256) as connection:
            row = connection.execute(
                "SELECT * FROM research_runs WHERE run_id=?", (run,)
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown research run {run}")
            if row["status"] != "running":
                if row["status"] == normalized_status:
                    return self._decode_row(row)
                raise ConcurrentUpdateError(f"research run {run} is already finished")
            connection.execute(
                """
                UPDATE research_runs
                SET status=?, completed_at_utc=?
                WHERE run_id=? AND status='running'
                """,
                (normalized_status, completed, run),
            )
            updated = connection.execute(
                "SELECT * FROM research_runs WHERE run_id=?", (run,)
            ).fetchone()
            assert updated is not None
            return self._decode_row(updated)

    def allocate_alpha_budget(
        self,
        run_id: str,
        *,
        global_alpha: float,
        created_at_utc: Any = None,
    ) -> Dict[str, Any]:
        """Allocate an immutable telescoping alpha budget to one discovery run.

        The first run receives ``global_alpha / 2``, the second ``/ 4``, and
        in general run ``n`` receives ``global_alpha / (n * (n + 1))``.
        The infinite sum is bounded by ``global_alpha`` without destroying
        the power of later searches exponentially. Allocation and sequence
        selection share one SQLite write transaction, so two concurrent
        discovery workers cannot receive the same sequence.
        """

        run = _required_text(run_id, "run_id")
        alpha = float(global_alpha)
        if not math.isfinite(alpha) or not 0.0 < alpha <= 1.0:
            raise ValueError("global_alpha must be finite and in (0, 1]")
        created = _utc_timestamp(
            created_at_utc or _utc_now(), "created_at_utc"
        )
        with self._write(projected_bytes=512) as connection:
            if connection.execute(
                "SELECT 1 FROM research_runs WHERE run_id=?", (run,)
            ).fetchone() is None:
                raise KeyError(f"unknown research run {run}")
            existing = connection.execute(
                "SELECT * FROM alpha_allocations WHERE run_id=?", (run,)
            ).fetchone()
            if existing is not None:
                if not math.isclose(
                    float(existing["global_alpha"]),
                    alpha,
                    rel_tol=0.0,
                    abs_tol=1e-15,
                ):
                    raise ImmutableRecordError(
                        f"research run {run} already has another alpha budget"
                    )
                return self._decode_row(existing)
            sequence = int(
                connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 FROM alpha_allocations"
                ).fetchone()[0]
            )
            budget = alpha / (sequence * (sequence + 1))
            if budget <= 0.0:
                raise WideResearchStoreError("alpha budget underflow")
            connection.execute(
                """
                INSERT INTO alpha_allocations(
                    run_id, sequence, global_alpha, alpha_budget,
                    created_at_utc
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (run, sequence, alpha, budget, created),
            )
            row = connection.execute(
                "SELECT * FROM alpha_allocations WHERE run_id=?", (run,)
            ).fetchone()
            assert row is not None
            return self._decode_row(row)

    def commit_discovery_import(
        self,
        *,
        run_id: str,
        run_config: Mapping[str, Any],
        global_alpha: float,
        phases: Sequence[Mapping[str, Any]],
        retirements: Sequence[Mapping[str, Any]] = (),
        expected_phase_snapshot: Mapping[str, str],
        completed_at_utc: Any = None,
    ) -> Dict[str, Any]:
        """Atomically register one immutable prospective hypothesis family.

        Discovery admission is calculated outside SQLite, but the exact
        evaluated-phase snapshot used for that calculation is compared under
        ``BEGIN IMMEDIATE``.  Run creation, alpha allocation, every rule and
        phase, generation retirement, and run completion then commit together.
        A crash or a competing importer therefore cannot expose half a Holm
        family or an over-capacity shadow pool.
        """

        run = _required_text(run_id, "run_id")
        if not isinstance(run_config, Mapping):
            raise ValueError("run_config must be a mapping")
        config_json = _canonical_json(dict(run_config))
        alpha = float(global_alpha)
        if not math.isfinite(alpha) or not 0.0 < alpha <= 1.0:
            raise ValueError("global_alpha must be finite and in (0, 1]")
        completed = _utc_timestamp(
            completed_at_utc or _utc_now(), "completed_at_utc"
        )

        snapshot: dict[str, str] = {}
        for phase_id, status in expected_phase_snapshot.items():
            phase = _required_text(phase_id, "phase_id")
            normalized = _required_text(status, "status").lower()
            if normalized not in EVALUATED_PHASE_STATUSES:
                raise ValueError("expected snapshot contains non-evaluated status")
            snapshot[phase] = normalized

        normalized_phases: list[dict[str, Any]] = []
        seen_phase_ids: set[str] = set()
        seen_rule_ids: set[str] = set()
        for value in phases:
            if not isinstance(value, Mapping):
                raise ValueError("every phase import must be a mapping")
            phase_id = _required_text(value.get("phase_id"), "phase_id")
            rule_id = _required_text(value.get("rule_id"), "rule_id")
            if phase_id in seen_phase_ids or rule_id in seen_rule_ids:
                raise ValueError("discovery family phase and rule IDs must be unique")
            seen_phase_ids.add(phase_id)
            seen_rule_ids.add(rule_id)
            manifest = value.get("manifest")
            policy = value.get("policy")
            if not isinstance(manifest, Mapping) or not isinstance(policy, Mapping):
                raise ValueError("phase import requires manifest and policy mappings")
            if "statistical_family" in policy:
                raise ValueError(
                    "statistical_family is assigned atomically by the store"
                )
            normalized_phases.append(
                {
                    "phase_id": phase_id,
                    "rule_id": rule_id,
                    "manifest_json": _canonical_json(dict(manifest)),
                    "starts_at_utc": _utc_timestamp(
                        value.get("starts_at_utc"), "starts_at_utc"
                    ),
                    "policy": dict(policy),
                }
            )

        normalized_retirements: list[dict[str, Any]] = []
        retired_ids: set[str] = set()
        for value in retirements:
            if not isinstance(value, Mapping):
                raise ValueError("every retirement must be a mapping")
            phase_id = _required_text(value.get("phase_id"), "phase_id")
            expected = _required_text(
                value.get("expected_status"), "expected_status"
            ).lower()
            if expected not in EVALUATED_PHASE_STATUSES:
                raise ValueError("retirement requires an evaluated phase status")
            if phase_id in retired_ids or phase_id in seen_phase_ids:
                raise ValueError("duplicate discovery retirement phase")
            retired_ids.add(phase_id)
            reason = _required_text(value.get("reason"), "reason")
            metadata = value.get("metadata", {})
            if not isinstance(metadata, Mapping):
                raise ValueError("retirement metadata must be a mapping")
            normalized_retirements.append(
                {
                    "phase_id": phase_id,
                    "expected_status": expected,
                    "reason": reason,
                    "metadata": dict(metadata),
                }
            )

        family_size = len(normalized_phases)
        projected = len(config_json) + sum(
            len(value["manifest_json"])
            + len(_canonical_json(value["policy"]))
            + 2048
            for value in normalized_phases
        ) + sum(
            len(_canonical_json(value["metadata"])) + 1024
            for value in normalized_retirements
        )
        with self._write(projected_bytes=projected) as connection:
            existing_run = connection.execute(
                "SELECT * FROM research_runs WHERE run_id=?", (run,)
            ).fetchone()
            if existing_run is not None:
                if str(existing_run["config_json"]) != config_json:
                    raise ImmutableRecordError(
                        f"research run {run} already differs"
                    )
                if str(existing_run["status"]) == "completed":
                    allocation = connection.execute(
                        "SELECT * FROM alpha_allocations WHERE run_id=?", (run,)
                    ).fetchone()
                    rows = connection.execute(
                        "SELECT * FROM phases WHERE run_id=? ORDER BY phase_id",
                        (run,),
                    ).fetchall()
                    planned_ids = {
                        str(value)
                        for value in run_config.get("planned_phase_ids", [])
                    }
                    actual_ids = {str(row["phase_id"]) for row in rows}
                    if actual_ids != planned_ids or bool(planned_ids) != bool(
                        allocation
                    ):
                        raise ImmutableRecordError(
                            f"completed research run {run} has an incomplete family"
                        )
                    return {
                        "run": self._decode_row(existing_run),
                        "alpha_allocation": (
                            self._decode_row(allocation)
                            if allocation is not None
                            else None
                        ),
                        "phases": [self._decode_row(row) for row in rows],
                        "retired": [],
                        "already_completed": True,
                    }
                if str(existing_run["status"]) != "running":
                    raise ConcurrentUpdateError(
                        f"research run {run} is already finished"
                    )

            current_rows = connection.execute(
                "SELECT phase_id, status FROM phases WHERE status IN "
                "('candidate','shadow','ready','active')"
            ).fetchall()
            current_snapshot = {
                str(row["phase_id"]): str(row["status"])
                for row in current_rows
            }
            if current_snapshot != snapshot:
                raise ConcurrentUpdateError(
                    "evaluated phase pool changed during discovery admission"
                )

            # This run is created and completed by one atomic import.  Use the
            # caller's normalized commit timestamp for every row so a fast
            # transaction cannot record started_at a few milliseconds after
            # completed_at.
            created = completed
            if existing_run is None:
                connection.execute(
                    """
                    INSERT INTO research_runs(
                        run_id, status, started_at_utc, completed_at_utc,
                        config_json, created_at_utc
                    ) VALUES (?, 'running', ?, NULL, ?, ?)
                    """,
                    (run, created, config_json, created),
                )

            allocation = connection.execute(
                "SELECT * FROM alpha_allocations WHERE run_id=?", (run,)
            ).fetchone()
            if family_size:
                if allocation is None:
                    sequence = int(
                        connection.execute(
                            "SELECT COALESCE(MAX(sequence), 0) + 1 "
                            "FROM alpha_allocations"
                        ).fetchone()[0]
                    )
                    budget = alpha / (sequence * (sequence + 1))
                    if budget <= 0.0:
                        raise WideResearchStoreError("alpha budget underflow")
                    connection.execute(
                        """
                        INSERT INTO alpha_allocations(
                            run_id, sequence, global_alpha, alpha_budget,
                            created_at_utc
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (run, sequence, alpha, budget, created),
                    )
                    allocation = connection.execute(
                        "SELECT * FROM alpha_allocations WHERE run_id=?", (run,)
                    ).fetchone()
                elif not math.isclose(
                    float(allocation["global_alpha"]),
                    alpha,
                    rel_tol=0.0,
                    abs_tol=1e-15,
                ):
                    raise ImmutableRecordError(
                        f"research run {run} already has another alpha budget"
                    )
                assert allocation is not None

            registered_rows: list[sqlite3.Row] = []
            for value in normalized_phases:
                manifest_json = str(value["manifest_json"])
                manifest_hash = _json_hash(manifest_json)
                rule_id = str(value["rule_id"])
                existing_rule = connection.execute(
                    "SELECT * FROM rules WHERE rule_id=?", (rule_id,)
                ).fetchone()
                if existing_rule is None:
                    connection.execute(
                        """
                        INSERT INTO rules(
                            rule_id, manifest_hash, manifest_json, created_at_utc
                        ) VALUES (?, ?, ?, ?)
                        """,
                        (rule_id, manifest_hash, manifest_json, created),
                    )
                elif (
                    str(existing_rule["manifest_hash"]) != manifest_hash
                    or str(existing_rule["manifest_json"]) != manifest_json
                ):
                    raise ImmutableRecordError(
                        f"rule {rule_id} already exists with another manifest"
                    )

                assert allocation is not None
                policy = dict(value["policy"])
                policy["statistical_family"] = {
                    "run_id": run,
                    "family_size": family_size,
                    "run_sequence": int(allocation["sequence"]),
                    "alpha_budget": float(allocation["alpha_budget"]),
                    "inter_run_correction": "telescoping_alpha_spending_v1",
                    "intra_run_correction": "holm_step_down_v1",
                    "milestone_correction": "bonferroni_fixed_looks_v1",
                }
                policy_json = _canonical_json(policy)
                policy_hash = _json_hash(policy_json)
                phase_id = str(value["phase_id"])
                existing_phase = connection.execute(
                    "SELECT * FROM phases WHERE phase_id=?", (phase_id,)
                ).fetchone()
                if existing_phase is not None:
                    immutable = (
                        str(existing_phase["rule_id"]),
                        existing_phase["run_id"],
                        str(existing_phase["starts_at_utc"]),
                        str(existing_phase["policy_hash"]),
                        str(existing_phase["policy_json"]),
                    )
                    proposed = (
                        rule_id,
                        run,
                        str(value["starts_at_utc"]),
                        policy_hash,
                        policy_json,
                    )
                    if immutable != proposed:
                        raise ImmutableRecordError(
                            f"phase {phase_id} already differs"
                        )
                else:
                    connection.execute(
                        """
                        INSERT INTO phases(
                            phase_id, rule_id, run_id, status, starts_at_utc,
                            stops_at_utc, policy_hash, policy_json,
                            created_at_utc, updated_at_utc
                        ) VALUES (?, ?, ?, 'shadow', ?, NULL, ?, ?, ?, ?)
                        """,
                        (
                            phase_id,
                            rule_id,
                            run,
                            value["starts_at_utc"],
                            policy_hash,
                            policy_json,
                            created,
                            created,
                        ),
                    )
                    self._insert_lifecycle_event(
                        connection,
                        phase_id=phase_id,
                        from_status=None,
                        to_status="shadow",
                        reason="phase_registered",
                        actor="store",
                        metadata={},
                        created_at_utc=created,
                    )
                phase_row = connection.execute(
                    "SELECT * FROM phases WHERE phase_id=?", (phase_id,)
                ).fetchone()
                assert phase_row is not None
                registered_rows.append(phase_row)

            # Validate every compare-and-swap before applying any retirement.
            retirement_rows: dict[str, sqlite3.Row] = {}
            for value in normalized_retirements:
                phase_id = str(value["phase_id"])
                row = connection.execute(
                    "SELECT * FROM phases WHERE phase_id=?", (phase_id,)
                ).fetchone()
                if row is None:
                    raise KeyError(f"unknown phase {phase_id}")
                if str(row["status"]) != str(value["expected_status"]):
                    raise ConcurrentUpdateError(
                        f"phase {phase_id} status changed during import"
                    )
                retirement_rows[phase_id] = row
            for value in normalized_retirements:
                phase_id = str(value["phase_id"])
                row = retirement_rows[phase_id]
                connection.execute(
                    "UPDATE phases SET status='retired', updated_at_utc=? "
                    "WHERE phase_id=?",
                    (completed, phase_id),
                )
                self._insert_lifecycle_event(
                    connection,
                    phase_id=phase_id,
                    from_status=str(row["status"]),
                    to_status="retired",
                    reason=str(value["reason"]),
                    actor="automation",
                    metadata=value["metadata"],
                    created_at_utc=completed,
                )

            connection.execute(
                """
                UPDATE research_runs
                SET status='completed', completed_at_utc=?
                WHERE run_id=? AND status='running'
                """,
                (completed, run),
            )
            completed_run = connection.execute(
                "SELECT * FROM research_runs WHERE run_id=?", (run,)
            ).fetchone()
            assert completed_run is not None
            return {
                "run": self._decode_row(completed_run),
                "alpha_allocation": (
                    self._decode_row(allocation)
                    if allocation is not None
                    else None
                ),
                "phases": [self._decode_row(row) for row in registered_rows],
                "retired": [
                    str(value["phase_id"]) for value in normalized_retirements
                ],
                "already_completed": False,
            }

    def register_rule(
        self,
        rule_id: str,
        manifest: Mapping[str, Any],
        *,
        created_at_utc: Any = None,
    ) -> Dict[str, Any]:
        rule = _required_text(rule_id, "rule_id")
        if not isinstance(manifest, Mapping):
            raise ValueError("manifest must be a mapping")
        manifest_json = _canonical_json(dict(manifest))
        manifest_hash = _json_hash(manifest_json)
        created = _utc_timestamp(created_at_utc or _utc_now(), "created_at_utc")
        with self._write(projected_bytes=len(manifest_json) + 512) as connection:
            existing = connection.execute(
                "SELECT * FROM rules WHERE rule_id=?", (rule,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["manifest_hash"] != manifest_hash
                    or existing["manifest_json"] != manifest_json
                ):
                    raise ImmutableRecordError(
                        f"rule {rule} already exists with another manifest"
                    )
                return self._decode_row(existing)
            connection.execute(
                """
                INSERT INTO rules(rule_id, manifest_hash, manifest_json, created_at_utc)
                VALUES (?, ?, ?, ?)
                """,
                (rule, manifest_hash, manifest_json, created),
            )
            row = connection.execute(
                "SELECT * FROM rules WHERE rule_id=?", (rule,)
            ).fetchone()
            assert row is not None
            return self._decode_row(row)

    def register_phase(
        self,
        phase_id: str,
        rule_id: str,
        *,
        run_id: Optional[str] = None,
        status: str = "shadow",
        starts_at_utc: Any = None,
        stops_at_utc: Any = None,
        policy: Optional[Mapping[str, Any]] = None,
        created_at_utc: Any = None,
    ) -> Dict[str, Any]:
        phase = _required_text(phase_id, "phase_id")
        rule = _required_text(rule_id, "rule_id")
        run = _required_text(run_id, "run_id") if run_id is not None else None
        normalized_status = _required_text(status, "status").lower()
        if normalized_status not in PHASE_STATUSES:
            raise ValueError(f"unsupported phase status {normalized_status}")
        explicit_starts = starts_at_utc is not None
        starts = _utc_timestamp(starts_at_utc or _utc_now(), "starts_at_utc")
        stops = _utc_timestamp(stops_at_utc, "stops_at_utc", optional=True)
        if stops is not None and stops <= starts:
            raise ValueError("stops_at_utc must be after starts_at_utc")
        policy_json = _canonical_json(dict(policy or {}))
        policy_hash = _json_hash(policy_json)
        created = _utc_timestamp(created_at_utc or _utc_now(), "created_at_utc")
        projected = len(policy_json) + 1024
        with self._write(projected_bytes=projected) as connection:
            if connection.execute(
                "SELECT 1 FROM rules WHERE rule_id=?", (rule,)
            ).fetchone() is None:
                raise KeyError(f"unknown rule {rule}")
            if run is not None and connection.execute(
                "SELECT 1 FROM research_runs WHERE run_id=?", (run,)
            ).fetchone() is None:
                raise KeyError(f"unknown research run {run}")
            existing = connection.execute(
                "SELECT * FROM phases WHERE phase_id=?", (phase,)
            ).fetchone()
            if existing is not None:
                comparable_starts = starts if explicit_starts else existing["starts_at_utc"]
                immutable = (
                    existing["rule_id"],
                    existing["run_id"],
                    existing["starts_at_utc"],
                    existing["stops_at_utc"],
                    existing["policy_hash"],
                    existing["policy_json"],
                )
                proposed = (
                    rule,
                    run,
                    comparable_starts,
                    stops,
                    policy_hash,
                    policy_json,
                )
                if immutable != proposed:
                    raise ImmutableRecordError(
                        f"phase {phase} already exists with another definition"
                    )
                return self._decode_row(existing)
            connection.execute(
                """
                INSERT INTO phases(
                    phase_id, rule_id, run_id, status, starts_at_utc,
                    stops_at_utc, policy_hash, policy_json, created_at_utc,
                    updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    phase,
                    rule,
                    run,
                    normalized_status,
                    starts,
                    stops,
                    policy_hash,
                    policy_json,
                    created,
                    created,
                ),
            )
            self._insert_lifecycle_event(
                connection,
                phase_id=phase,
                from_status=None,
                to_status=normalized_status,
                reason="phase_registered",
                actor="store",
                metadata={},
                created_at_utc=created,
            )
            row = connection.execute(
                "SELECT * FROM phases WHERE phase_id=?", (phase,)
            ).fetchone()
            assert row is not None
            return self._decode_row(row)

    def register_rule_and_phase(
        self,
        *,
        rule_id: str,
        manifest: Mapping[str, Any],
        phase_id: str,
        run_id: Optional[str] = None,
        status: str = "shadow",
        starts_at_utc: Any = None,
        stops_at_utc: Any = None,
        policy: Optional[Mapping[str, Any]] = None,
        created_at_utc: Any = None,
    ) -> Dict[str, Any]:
        """Convenience registration; each half remains immutable/idempotent."""

        rule = self.register_rule(rule_id, manifest)
        phase = self.register_phase(
            phase_id,
            rule_id,
            run_id=run_id,
            status=status,
            starts_at_utc=starts_at_utc,
            stops_at_utc=stops_at_utc,
            policy=policy,
            created_at_utc=created_at_utc,
        )
        return {"rule": rule, "phase": phase}

    def list_manifests(self, *, active_only: bool = False) -> list[Dict[str, Any]]:
        if active_only:
            phases = self.list_active_phases()
            seen: set[str] = set()
            result: list[Dict[str, Any]] = []
            for phase in phases:
                rule_id = str(phase["rule_id"])
                if rule_id in seen:
                    continue
                seen.add(rule_id)
                result.append(
                    {
                        "rule_id": rule_id,
                        "manifest_hash": phase["manifest_hash"],
                        "manifest": phase["manifest"],
                        "created_at_utc": phase["rule_created_at_utc"],
                    }
                )
            return result
        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM rules ORDER BY created_at_utc, rule_id"
            ).fetchall()
            return [self._decode_row(row) for row in rows]

    def list_phases(
        self,
        *,
        statuses: Optional[Sequence[str]] = None,
        rule_id: Optional[str] = None,
    ) -> list[Dict[str, Any]]:
        """List phase definitions, including future and terminal versions."""

        clauses: list[str] = []
        params: list[Any] = []
        if statuses is not None:
            normalized = sorted(
                {
                    _required_text(status, "status").lower()
                    for status in statuses
                }
            )
            if any(status not in PHASE_STATUSES for status in normalized):
                raise ValueError("unsupported phase status")
            if not normalized:
                return []
            clauses.append(
                "p.status IN (" + ",".join("?" for _ in normalized) + ")"
            )
            params.extend(normalized)
        if rule_id is not None:
            clauses.append("p.rule_id=?")
            params.append(_required_text(rule_id, "rule_id"))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._read() as connection:
            rows = connection.execute(
                """
                SELECT p.*, r.manifest_hash, r.manifest_json,
                       r.created_at_utc AS rule_created_at_utc
                FROM phases AS p
                JOIN rules AS r ON r.rule_id=p.rule_id
                """
                + where
                + " ORDER BY p.created_at_utc, p.phase_id",
                params,
            ).fetchall()
            result: list[Dict[str, Any]] = []
            for row in rows:
                decoded = self._decode_row(row)
                decoded["manifest"] = json.loads(str(row["manifest_json"]))
                result.append(decoded)
            return result

    def list_active_phases(self, *, at_utc: Any = None) -> list[Dict[str, Any]]:
        at = _utc_timestamp(at_utc or _utc_now(), "at_utc")
        placeholders = ",".join("?" for _ in EVALUATED_PHASE_STATUSES)
        params: tuple[Any, ...] = (
            at,
            at,
            *sorted(EVALUATED_PHASE_STATUSES),
            at,
            at,
        )
        with self._read() as connection:
            rows = connection.execute(
                f"""
                SELECT
                    p.*,
                    historical.to_status AS historical_status,
                    r.manifest_hash,
                    r.manifest_json,
                    r.created_at_utc AS rule_created_at_utc
                FROM phases AS p
                JOIN rules AS r ON r.rule_id=p.rule_id
                JOIN lifecycle_events AS historical
                  ON historical.rowid = (
                      SELECT event.rowid
                      FROM lifecycle_events AS event
                      WHERE event.phase_id=p.phase_id
                        AND event.created_at_utc <= ?
                      ORDER BY event.created_at_utc DESC, event.rowid DESC
                      LIMIT 1
                  )
                WHERE p.created_at_utc <= ?
                  AND historical.to_status IN ({placeholders})
                  AND p.starts_at_utc <= ?
                  AND (p.stops_at_utc IS NULL OR p.stops_at_utc > ?)
                ORDER BY p.created_at_utc, p.phase_id
                """,
                params,
            ).fetchall()
            result: list[Dict[str, Any]] = []
            for row in rows:
                decoded = self._decode_row(row)
                decoded["current_status"] = decoded["status"]
                decoded["status"] = decoded.pop("historical_status")
                decoded["manifest"] = json.loads(str(row["manifest_json"]))
                result.append(decoded)
            return result

    def record_universe(
        self,
        *,
        fixture_id: Any,
        observation_id: Any,
        observed_at_utc: Any,
        minute: Any = None,
        league: Any = None,
        input_data: Optional[Mapping[str, Any]] = None,
    ) -> bool:
        fixture = _required_text(fixture_id, "fixture_id")
        observation = _required_text(observation_id, "observation_id")
        observed = _utc_timestamp(observed_at_utc, "observed_at_utc")
        parsed_minute = _minute(minute)
        normalized_league = str(league or "").strip() or None
        input_json = _canonical_json(dict(input_data or {}))
        projected = len(input_json) + 1024
        with self._write(projected_bytes=projected) as connection:
            existing = connection.execute(
                "SELECT latest_observed_at_utc FROM universe_fixtures WHERE fixture_id=?",
                (fixture,),
            ).fetchone()
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO universe_fixtures(
                        fixture_id, first_observation_id, first_seen_at_utc,
                        first_minute, first_league, first_input_json,
                        latest_observation_id, latest_observed_at_utc,
                        latest_minute, latest_league, latest_input_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        fixture,
                        observation,
                        observed,
                        parsed_minute,
                        normalized_league,
                        input_json,
                        observation,
                        observed,
                        parsed_minute,
                        normalized_league,
                        input_json,
                    ),
                )
                return True
            if observed >= str(existing["latest_observed_at_utc"]):
                connection.execute(
                    """
                    UPDATE universe_fixtures
                    SET latest_observation_id=?, latest_observed_at_utc=?,
                        latest_minute=?, latest_league=?, latest_input_json=?
                    WHERE fixture_id=?
                    """,
                    (
                        observation,
                        observed,
                        parsed_minute,
                        normalized_league,
                        input_json,
                        fixture,
                    ),
                )
            return False

    def get_universe_fixture(self, fixture_id: Any) -> Optional[Dict[str, Any]]:
        fixture = _required_text(fixture_id, "fixture_id")
        with self._read() as connection:
            row = connection.execute(
                "SELECT * FROM universe_fixtures WHERE fixture_id=?", (fixture,)
            ).fetchone()
            return self._decode_row(row) if row is not None else None

    def enqueue_live_evaluation(
        self,
        *,
        observation_id: Any,
        observed_at_utc: Any,
        payload: Mapping[str, Any],
    ) -> bool:
        """Durably queue one frozen live evaluation for idempotent replay.

        The observation identity is immutable.  Re-enqueuing the exact same
        payload is harmless; reusing the identity for different evidence is
        rejected rather than silently changing a prospective decision.
        """

        observation = _required_text(observation_id, "observation_id")
        observed = _utc_timestamp(observed_at_utc, "observed_at_utc")
        payload_json = _canonical_json(dict(payload))
        payload_bytes = len(payload_json.encode("utf-8"))
        if payload_bytes > self.max_live_retry_record_bytes:
            raise LiveRetryCapacityError(
                "live evaluation exceeds retry record size limit"
            )
        payload_hash = _json_hash(payload_json)
        with self._write(projected_bytes=payload_bytes + 1024) as connection:
            existing = connection.execute(
                "SELECT observed_at_utc, payload_hash, payload_json "
                "FROM live_evaluation_inbox "
                "WHERE observation_id=?",
                (observation,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["observed_at_utc"]) != observed
                    or str(existing["payload_hash"]) != payload_hash
                    or str(existing["payload_json"]) != payload_json
                ):
                    raise ImmutableRecordError(
                        f"live evaluation {observation} already differs"
                    )
                return False
            capacity = connection.execute(
                """
                SELECT COUNT(*) AS pending,
                       COALESCE(SUM(LENGTH(CAST(payload_json AS BLOB))), 0) AS bytes
                FROM live_evaluation_inbox
                """
            ).fetchone()
            assert capacity is not None
            if (
                int(capacity["pending"]) >= self.max_live_retry_records
                or int(capacity["bytes"]) + payload_bytes
                > self.max_live_retry_bytes
            ):
                raise LiveRetryCapacityError(
                    "live evaluation retry inbox is full; event was not accepted"
                )
            connection.execute(
                """
                INSERT INTO live_evaluation_inbox(
                    observation_id, observed_at_utc, payload_hash,
                    payload_json, attempts, last_error, created_at_utc
                ) VALUES (?, ?, ?, ?, 0, NULL, ?)
                """,
                (observation, observed, payload_hash, payload_json, _utc_now()),
            )
            return True

    def pending_live_evaluations(self, *, limit: int = 32) -> list[Dict[str, Any]]:
        bounded = max(1, min(256, int(limit)))
        with self._read() as connection:
            rows = connection.execute(
                """
                SELECT * FROM live_evaluation_inbox
                ORDER BY observed_at_utc, created_at_utc, observation_id
                LIMIT ?
                """,
                (bounded,),
            ).fetchall()
            return [self._decode_row(row) for row in rows]

    def live_retry_status(self) -> Dict[str, Any]:
        """Read aggregate inbox health without loading frozen payloads."""
        with self._read() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS pending, MIN(created_at_utc) AS oldest_at_utc, "
                "COALESCE(MAX(attempts), 0) AS max_attempts, "
                "COALESCE(SUM(LENGTH(CAST(payload_json AS BLOB))), 0) AS bytes "
                "FROM live_evaluation_inbox"
            ).fetchone()
            result = dict(row)
            result.update(
                capacity_records=self.max_live_retry_records,
                capacity_bytes=self.max_live_retry_bytes,
                capacity_record_bytes=self.max_live_retry_record_bytes,
            )
            return result

    def acknowledge_live_evaluation(self, observation_id: Any) -> bool:
        observation = _required_text(observation_id, "observation_id")
        with self._write(projected_bytes=0) as connection:
            cursor = connection.execute(
                "DELETE FROM live_evaluation_inbox WHERE observation_id=?",
                (observation,),
            )
            return cursor.rowcount == 1

    def note_live_evaluation_failure(
        self, observation_id: Any, error: Any
    ) -> None:
        observation = _required_text(observation_id, "observation_id")
        message = str(error or "unknown error")[:1000]
        with self._write(projected_bytes=len(message) + 64) as connection:
            connection.execute(
                """
                UPDATE live_evaluation_inbox
                SET attempts=attempts+1, last_error=?
                WHERE observation_id=?
                """,
                (message, observation),
            )

    def claim_first_trigger(
        self,
        *,
        phase_id: str,
        rule_id: str,
        fixture_id: Any,
        observation_id: Any,
        triggered_at_utc: Any,
        minute: Any = None,
        league: Any = None,
        input_data: Optional[Mapping[str, Any]] = None,
    ) -> bool:
        """Atomically claim the first trigger for one phase/rule/fixture."""

        phase = _required_text(phase_id, "phase_id")
        rule = _required_text(rule_id, "rule_id")
        fixture = _required_text(fixture_id, "fixture_id")
        observation = _required_text(observation_id, "observation_id")
        triggered = _utc_timestamp(triggered_at_utc, "triggered_at_utc")
        parsed_minute = _minute(minute)
        normalized_league = str(league or "").strip() or None
        input_json = _canonical_json(dict(input_data or {}))
        projected = len(input_json) * 2 + 2048
        with self._write(projected_bytes=projected) as connection:
            definition = connection.execute(
                "SELECT * FROM phases WHERE phase_id=?", (phase,)
            ).fetchone()
            if definition is None:
                raise KeyError(f"unknown phase {phase}")
            if definition["rule_id"] != rule:
                raise ImmutableRecordError(
                    f"phase {phase} belongs to rule {definition['rule_id']}, not {rule}"
                )
            historical = connection.execute(
                """
                SELECT to_status
                FROM lifecycle_events
                WHERE phase_id=? AND created_at_utc <= ?
                ORDER BY created_at_utc DESC, rowid DESC
                LIMIT 1
                """,
                (phase, triggered),
            ).fetchone()
            if (
                str(definition["created_at_utc"]) > triggered
                or historical is None
                or str(historical["to_status"]) not in EVALUATED_PHASE_STATUSES
            ):
                return False
            terminal = json.loads(definition["policy_json"]).get("terminal_review") or {}
            if terminal.get("enabled") is True:
                horizon = terminal.get("final_look")
                if type(horizon) is not int or horizon <= 0:
                    raise ImmutableRecordError("invalid frozen terminal horizon")
                # Cap admission under the same write transaction as the
                # first-trigger claim, even before the lifecycle cache refresh.
                count = connection.execute(
                    "SELECT COUNT(*) FROM triggers WHERE phase_id=?", (phase,)
                ).fetchone()[0]
                if count >= horizon:
                    return False
            if triggered < definition["starts_at_utc"] or (
                definition["stops_at_utc"] is not None
                and triggered >= definition["stops_at_utc"]
            ):
                return False
            connection.execute(
                """
                INSERT OR IGNORE INTO universe_fixtures(
                    fixture_id, first_observation_id, first_seen_at_utc,
                    first_minute, first_league, first_input_json,
                    latest_observation_id, latest_observed_at_utc,
                    latest_minute, latest_league, latest_input_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fixture,
                    observation,
                    triggered,
                    parsed_minute,
                    normalized_league,
                    input_json,
                    observation,
                    triggered,
                    parsed_minute,
                    normalized_league,
                    input_json,
                ),
            )
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO triggers(
                    phase_id, rule_id, fixture_id, observation_id,
                    triggered_at_utc, minute, league, input_json,
                    latest_outcome_event_id, latest_outcome_version,
                    latest_outcome_status, latest_outcome_label,
                    latest_outcome_at_utc, latest_outcome_json, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 'pending',
                          NULL, NULL, NULL, ?)
                """,
                (
                    phase,
                    rule,
                    fixture,
                    observation,
                    triggered,
                    parsed_minute,
                    normalized_league,
                    input_json,
                    triggered,
                ),
            )
            inserted = cursor.rowcount == 1
            if inserted:
                self._apply_latest_outcome_to_trigger(
                    connection,
                    phase_id=phase,
                    rule_id=rule,
                    fixture_id=fixture,
                    observation_id=observation,
                )
            return inserted

    @staticmethod
    def _normalize_outcome(record: Mapping[str, Any]) -> Dict[str, Any]:
        observation_id = _required_text(record.get("observation_id"), "observation_id")
        raw_version = record.get("outcome_version", record.get("version", 1))
        version = _outcome_version(raw_version)

        raw_status = record.get("outcome_status", record.get("status"))
        raw_label = record.get("outcome_label", record.get("label"))
        aliases = {
            "win": "win",
            "won": "win",
            "plus": "win",
            "+": "win",
            "1": "win",
            "true": "win",
            "loss": "loss",
            "lost": "loss",
            "minus": "loss",
            "-": "loss",
            "0": "loss",
            "false": "loss",
            "pending": "pending",
            "open": "pending",
            "unknown": "pending",
            "invalid": "invalid",
            "void": "invalid",
            "cancelled": "invalid",
            "canceled": "invalid",
        }
        if raw_status is None and raw_label is not None:
            if raw_label is True or raw_label == 1:
                status = "win"
            elif raw_label is False or raw_label == 0:
                status = "loss"
            else:
                raise ValueError("outcome label must be boolean/0/1")
        else:
            status = aliases.get(str(raw_status or "").strip().lower(), "")
        if status not in OUTCOME_STATUSES:
            raise ValueError(f"unsupported outcome status {raw_status!r}")
        label: Optional[int]
        if status == "win":
            label = 1
        elif status == "loss":
            label = 0
        else:
            label = None
        if raw_label is not None and label is not None:
            supplied = 1 if raw_label is True else 0 if raw_label is False else int(raw_label)
            if supplied != label:
                raise ValueError("outcome status and label disagree")

        outcome_at = _utc_timestamp(
            record.get("outcome_at_utc")
            or record.get("resolved_at_utc")
            or record.get("occurred_at_utc")
            or _utc_now(),
            "outcome_at_utc",
        )
        received_at = _utc_timestamp(
            record.get("received_at_utc") or _utc_now(), "received_at_utc"
        )
        if isinstance(record.get("payload"), Mapping):
            payload = dict(record["payload"])
        else:
            payload = {
                key: value
                for key, value in record.items()
                if key
                not in {
                    "event_id",
                    "received_at_utc",
                    "outcome_version",
                    "version",
                    "outcome_status",
                    "status",
                    "outcome_label",
                    "label",
                    "outcome_at_utc",
                    "resolved_at_utc",
                    "occurred_at_utc",
                }
            }
        payload_json = _canonical_json(payload)
        identity_json = _canonical_json(
            {
                "observation_id": observation_id,
                "version": version,
                "status": status,
                "label": label,
                "outcome_at_utc": outcome_at,
                "payload": json.loads(payload_json),
            }
        )
        event_id = str(record.get("event_id") or "").strip() or _json_hash(
            identity_json
        )
        return {
            "event_id": event_id,
            "observation_id": observation_id,
            "version": version,
            "outcome_status": status,
            "outcome_label": label,
            "outcome_at_utc": outcome_at,
            "received_at_utc": received_at,
            "payload_json": payload_json,
        }

    def attach_outcomes(
        self,
        outcomes: Union[Mapping[str, Any], Iterable[Mapping[str, Any]]],
    ) -> Dict[str, int]:
        """Attach versioned outcomes to every trigger sharing observation_id.

        Outcome events are retained even when no trigger exists yet.  If a
        delayed trigger is subsequently claimed, its newest event is attached
        immediately.  Consequently old phase/rule versions remain resolvable
        after a restart or promotion.
        """

        source: Iterable[Mapping[str, Any]]
        if isinstance(outcomes, Mapping):
            source = [outcomes]
        else:
            source = outcomes
        normalized = [self._normalize_outcome(record) for record in source]
        result = {"inserted": 0, "duplicates": 0, "updated_triggers": 0, "unmatched": 0}
        if not normalized:
            return result
        projected = sum(len(item["payload_json"]) + 1024 for item in normalized)
        with self._write(projected_bytes=projected) as connection:
            for item in normalized:
                by_event = connection.execute(
                    "SELECT * FROM outcome_events WHERE event_id=?",
                    (item["event_id"],),
                ).fetchone()
                if by_event is not None:
                    self._assert_same_outcome(by_event, item)
                    result["duplicates"] += 1
                    continue
                by_version = connection.execute(
                    """
                    SELECT * FROM outcome_events
                    WHERE observation_id=? AND outcome_version=?
                    """,
                    (item["observation_id"], item["version"]),
                ).fetchone()
                if by_version is not None:
                    self._assert_same_outcome(by_version, item, ignore_event_id=True)
                    result["duplicates"] += 1
                    continue
                connection.execute(
                    """
                    INSERT INTO outcome_events(
                        event_id, observation_id, outcome_version,
                        outcome_status, outcome_label, outcome_at_utc,
                        received_at_utc, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item["event_id"],
                        item["observation_id"],
                        item["version"],
                        item["outcome_status"],
                        item["outcome_label"],
                        item["outcome_at_utc"],
                        item["received_at_utc"],
                        item["payload_json"],
                    ),
                )
                result["inserted"] += 1
                cursor = connection.execute(
                    """
                    UPDATE triggers
                    SET latest_outcome_event_id=?, latest_outcome_version=?,
                        latest_outcome_status=?, latest_outcome_label=?,
                        latest_outcome_at_utc=?, latest_outcome_json=?,
                        updated_at_utc=?
                    WHERE observation_id=?
                      AND (latest_outcome_version IS NULL
                           OR CASE WHEN latest_outcome_version < 1000000000
                                   THEN latest_outcome_version * 1000000000
                                   ELSE latest_outcome_version END < ?)
                    """,
                    (
                        item["event_id"],
                        item["version"],
                        item["outcome_status"],
                        item["outcome_label"],
                        item["outcome_at_utc"],
                        item["payload_json"],
                        item["received_at_utc"],
                        item["observation_id"],
                        _outcome_version_rank(item["version"]),
                    ),
                )
                result["updated_triggers"] += max(0, cursor.rowcount)
                if connection.execute(
                    "SELECT 1 FROM triggers WHERE observation_id=? LIMIT 1",
                    (item["observation_id"],),
                ).fetchone() is None:
                    result["unmatched"] += 1
        return result

    def list_triggers(
        self,
        *,
        phase_id: Optional[str] = None,
        rule_id: Optional[str] = None,
    ) -> list[Dict[str, Any]]:
        clauses: list[str] = []
        params: list[str] = []
        if phase_id is not None:
            clauses.append("phase_id=?")
            params.append(_required_text(phase_id, "phase_id"))
        if rule_id is not None:
            clauses.append("rule_id=?")
            params.append(_required_text(rule_id, "rule_id"))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM triggers"
                + where
                + " ORDER BY triggered_at_utc, phase_id, rule_id, fixture_id",
                params,
            ).fetchall()
            return [self._decode_row(row) for row in rows]

    def freeze_phase_look(
        self,
        phase_id: str,
        milestone: int,
        *,
        created_at_utc: Any = None,
    ) -> Optional[Dict[str, Any]]:
        """Freeze the first ``milestone`` trigger identities for one phase.

        A look is created as soon as enough triggers exist, before their
        outcomes are known.  Pending outcomes can delay evaluation but later
        results can never substitute a more favorable fixture into the look.
        """

        phase = _required_text(phase_id, "phase_id")
        if type(milestone) is not int or milestone < 1:
            raise ValueError("milestone must be a positive integer")
        created = _utc_timestamp(
            created_at_utc or _utc_now(), "created_at_utc"
        )
        with self._write(projected_bytes=milestone * 160 + 512) as connection:
            if connection.execute(
                "SELECT 1 FROM phases WHERE phase_id=?", (phase,)
            ).fetchone() is None:
                raise KeyError(f"unknown phase {phase}")
            existing = connection.execute(
                "SELECT * FROM phase_looks WHERE phase_id=? AND milestone=?",
                (phase, milestone),
            ).fetchone()
            if existing is not None:
                return self._decode_row(existing)
            rows = connection.execute(
                """
                SELECT fixture_id, observation_id
                FROM triggers
                WHERE phase_id=?
                ORDER BY triggered_at_utc, fixture_id
                LIMIT ?
                """,
                (phase, milestone),
            ).fetchall()
            if len(rows) < milestone:
                return None
            identities = [
                {
                    "fixture_id": str(row["fixture_id"]),
                    "observation_id": str(row["observation_id"]),
                }
                for row in rows
            ]
            identities_json = _canonical_json(identities)
            identities_hash = _json_hash(identities_json)
            connection.execute(
                """
                INSERT INTO phase_looks(
                    phase_id, milestone, trigger_ids_json,
                    trigger_ids_hash, created_at_utc
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    phase,
                    milestone,
                    identities_json,
                    identities_hash,
                    created,
                ),
            )
            row = connection.execute(
                "SELECT * FROM phase_looks WHERE phase_id=? AND milestone=?",
                (phase, milestone),
            ).fetchone()
            assert row is not None
            return self._decode_row(row)

    def triggers_for_phase_look(
        self,
        phase_id: str,
        milestone: int,
    ) -> list[Dict[str, Any]]:
        phase = _required_text(phase_id, "phase_id")
        if type(milestone) is not int or milestone < 1:
            raise ValueError("milestone must be a positive integer")
        with self._read() as connection:
            look = connection.execute(
                "SELECT * FROM phase_looks WHERE phase_id=? AND milestone=?",
                (phase, milestone),
            ).fetchone()
            if look is None:
                raise KeyError(f"phase look {phase}:{milestone} is not frozen")
            identities_json = str(look["trigger_ids_json"])
            if _json_hash(identities_json) != str(look["trigger_ids_hash"]):
                raise WideResearchStoreError(
                    f"phase look checksum mismatch for {phase}:{milestone}"
                )
            identities = json.loads(identities_json)
            if not isinstance(identities, list) or len(identities) != milestone:
                raise WideResearchStoreError(
                    f"phase look identity count mismatch for {phase}:{milestone}"
                )
            rows = connection.execute(
                "SELECT * FROM triggers WHERE phase_id=?",
                (phase,),
            ).fetchall()
            by_fixture = {str(row["fixture_id"]): row for row in rows}
            result: list[Dict[str, Any]] = []
            for identity in identities:
                if not isinstance(identity, Mapping):
                    raise WideResearchStoreError("invalid phase look identity")
                fixture = str(identity.get("fixture_id") or "")
                observation = str(identity.get("observation_id") or "")
                row = by_fixture.get(fixture)
                if row is None or str(row["observation_id"]) != observation:
                    raise WideResearchStoreError(
                        f"phase look trigger mismatch for {phase}:{milestone}"
                    )
                result.append(self._decode_row(row))
            return result

    def matching_trigger_observation_ids(
        self, observation_ids: Iterable[Any], *, include_queued: bool = False
    ) -> set[str]:
        """Return only identities already claimed by at least one rule phase."""

        normalized = sorted(
            {
                _required_text(value, "observation_id")
                for value in observation_ids
                if value is not None and str(value).strip()
            }
        )
        matched: set[str] = set()
        with self._read() as connection:
            for offset in range(0, len(normalized), 500):
                chunk = normalized[offset : offset + 500]
                if not chunk:
                    continue
                rows = connection.execute(
                    "SELECT DISTINCT observation_id FROM triggers WHERE observation_id IN ("
                    + ",".join("?" for _ in chunk)
                    + ")",
                    chunk,
                ).fetchall()
                matched.update(str(row[0]) for row in rows)
                if include_queued:
                    queued = connection.execute(
                        "SELECT observation_id FROM live_evaluation_inbox "
                        "WHERE observation_id IN (" + ",".join("?" for _ in chunk) + ")",
                        chunk,
                    ).fetchall()
                    matched.update(str(row[0]) for row in queued)
        return matched

    @staticmethod
    def _assert_same_outcome(
        existing: sqlite3.Row,
        proposed: Mapping[str, Any],
        *,
        ignore_event_id: bool = False,
    ) -> None:
        comparisons: Dict[str, Any] = {
            "observation_id": proposed["observation_id"],
            "outcome_version": proposed["version"],
            "outcome_status": proposed["outcome_status"],
            "outcome_label": proposed["outcome_label"],
            "payload_json": proposed["payload_json"],
        }
        if not ignore_event_id:
            comparisons["event_id"] = proposed["event_id"]
            comparisons["outcome_at_utc"] = proposed["outcome_at_utc"]
        if any(existing[key] != value for key, value in comparisons.items()):
            raise ImmutableRecordError(
                "outcome event/version already exists with different content"
            )

    @staticmethod
    def _apply_latest_outcome_to_trigger(
        connection: sqlite3.Connection,
        *,
        phase_id: str,
        rule_id: str,
        fixture_id: str,
        observation_id: str,
    ) -> bool:
        trigger = connection.execute(
            """
            SELECT latest_outcome_event_id, latest_outcome_version,
                   latest_outcome_status, latest_outcome_label,
                   latest_outcome_at_utc, latest_outcome_json,
                   triggered_at_utc
            FROM triggers
            WHERE phase_id=? AND rule_id=? AND fixture_id=?
            """,
            (phase_id, rule_id, fixture_id),
        ).fetchone()
        if trigger is None:
            return False
        event = connection.execute(
            """
            SELECT * FROM outcome_events
            WHERE observation_id=?
            ORDER BY CASE WHEN outcome_version < 1000000000
                          THEN outcome_version * 1000000000
                          ELSE outcome_version END DESC,
                     received_at_utc DESC, event_id DESC
            LIMIT 1
            """,
            (observation_id,),
        ).fetchone()
        if event is None:
            desired = (None, None, "pending", None, None, None)
            updated_at = str(trigger["triggered_at_utc"])
        else:
            desired = (
                event["event_id"],
                event["outcome_version"],
                event["outcome_status"],
                event["outcome_label"],
                event["outcome_at_utc"],
                event["payload_json"],
            )
            updated_at = str(event["received_at_utc"])
        current = (
            trigger["latest_outcome_event_id"],
            trigger["latest_outcome_version"],
            trigger["latest_outcome_status"],
            trigger["latest_outcome_label"],
            trigger["latest_outcome_at_utc"],
            trigger["latest_outcome_json"],
        )
        if current == desired:
            return False
        connection.execute(
            """
            UPDATE triggers
            SET latest_outcome_event_id=?, latest_outcome_version=?,
                latest_outcome_status=?, latest_outcome_label=?,
                latest_outcome_at_utc=?, latest_outcome_json=?,
                updated_at_utc=?
            WHERE phase_id=? AND rule_id=? AND fixture_id=?
            """,
            (
                *desired,
                updated_at,
                phase_id,
                rule_id,
                fixture_id,
            ),
        )
        return True

    @staticmethod
    def _metric_counts(rows: Sequence[sqlite3.Row]) -> Dict[str, Any]:
        counts = {"win": 0, "loss": 0, "pending": 0, "invalid": 0}
        for row in rows:
            status = str(row["outcome_status"] or "pending")
            if status not in counts:
                status = "invalid"
            counts[status] += int(row["count"])
        resolved = counts["win"] + counts["loss"]
        total = resolved + counts["pending"] + counts["invalid"]
        return {
            "total": total,
            "win": counts["win"],
            "loss": counts["loss"],
            "pending": counts["pending"],
            "invalid": counts["invalid"],
            "resolved": resolved,
            "accuracy": counts["win"] / resolved if resolved else None,
        }

    def metrics_for_phase(self, phase_id: str) -> Dict[str, Any]:
        phase = _required_text(phase_id, "phase_id")
        with self._read() as connection:
            definition = connection.execute(
                "SELECT * FROM phases WHERE phase_id=?", (phase,)
            ).fetchone()
            if definition is None:
                raise KeyError(f"unknown phase {phase}")
            overall_rows = connection.execute(
                """
                SELECT COALESCE(latest_outcome_status, 'pending') AS outcome_status,
                       COUNT(*) AS count
                FROM triggers WHERE phase_id=?
                GROUP BY COALESCE(latest_outcome_status, 'pending')
                """,
                (phase,),
            ).fetchall()
            result = self._metric_counts(overall_rows)
            bounds = connection.execute(
                """
                SELECT MIN(triggered_at_utc) AS first_triggered_at_utc,
                       MAX(triggered_at_utc) AS last_triggered_at_utc,
                       COUNT(DISTINCT fixture_id) AS unique_fixtures
                FROM triggers WHERE phase_id=?
                """,
                (phase,),
            ).fetchone()
            result.update(
                {
                    "phase_id": phase,
                    "rule_id": definition["rule_id"],
                    "phase_status": definition["status"],
                    "first_triggered_at_utc": bounds["first_triggered_at_utc"],
                    "last_triggered_at_utc": bounds["last_triggered_at_utc"],
                    "unique_fixtures": int(bounds["unique_fixtures"] or 0),
                    "by_date": self._metric_breakdown(
                        connection,
                        phase,
                        "substr(triggered_at_utc, 1, 10)",
                        "date",
                    ),
                    "by_league": self._metric_breakdown(
                        connection,
                        phase,
                        "COALESCE(NULLIF(league, ''), '<unknown>')",
                        "league",
                    ),
                }
            )
            return result

    get_phase_metrics = metrics_for_phase

    def _metric_breakdown(
        self,
        connection: sqlite3.Connection,
        phase_id: str,
        sql_key: str,
        output_key: str,
    ) -> list[Dict[str, Any]]:
        rows = connection.execute(
            f"""
            SELECT {sql_key} AS bucket,
                   COALESCE(latest_outcome_status, 'pending') AS outcome_status,
                   COUNT(*) AS count
            FROM triggers
            WHERE phase_id=?
            GROUP BY bucket, COALESCE(latest_outcome_status, 'pending')
            ORDER BY bucket
            """,
            (phase_id,),
        ).fetchall()
        grouped: Dict[str, list[sqlite3.Row]] = {}
        for row in rows:
            grouped.setdefault(str(row["bucket"]), []).append(row)
        return [
            {output_key: key, **self._metric_counts(grouped[key])}
            for key in sorted(grouped)
        ]

    @staticmethod
    def _insert_lifecycle_event(
        connection: sqlite3.Connection,
        *,
        phase_id: str,
        from_status: Optional[str],
        to_status: str,
        reason: str,
        actor: str,
        metadata: Mapping[str, Any],
        created_at_utc: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO lifecycle_events(
                event_id, phase_id, from_status, to_status, reason,
                actor, metadata_json, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                uuid.uuid4().hex,
                phase_id,
                from_status,
                to_status,
                reason,
                actor,
                _canonical_json(dict(metadata)),
                created_at_utc,
            ),
        )

    def transition_phase(
        self,
        phase_id: str,
        to_status: str,
        *,
        expected_status: Optional[str] = None,
        reason: str,
        actor: str = "automation",
        metadata: Optional[Mapping[str, Any]] = None,
        changed_at_utc: Any = None,
    ) -> Dict[str, Any]:
        phase = _required_text(phase_id, "phase_id")
        target = _required_text(to_status, "to_status").lower()
        if target not in PHASE_STATUSES:
            raise ValueError(f"unsupported phase status {target}")
        normalized_reason = _required_text(reason, "reason")
        normalized_actor = _required_text(actor, "actor")
        changed = _utc_timestamp(changed_at_utc or _utc_now(), "changed_at_utc")
        metadata_json = _canonical_json(dict(metadata or {}))
        with self._write(projected_bytes=len(metadata_json) + 1024) as connection:
            row = connection.execute(
                "SELECT * FROM phases WHERE phase_id=?", (phase,)
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown phase {phase}")
            current = str(row["status"])
            if expected_status is not None and current != expected_status:
                raise ConcurrentUpdateError(
                    f"phase {phase} status is {current}, expected {expected_status}"
                )
            if current == target:
                return self._decode_row(row)
            if current in TERMINAL_PHASE_STATUSES:
                raise InvalidLifecycleTransition(
                    f"terminal phase {phase} cannot move from {current} to {target}"
                )
            connection.execute(
                "UPDATE phases SET status=?, updated_at_utc=? WHERE phase_id=?",
                (target, changed, phase),
            )
            self._insert_lifecycle_event(
                connection,
                phase_id=phase,
                from_status=current,
                to_status=target,
                reason=normalized_reason,
                actor=normalized_actor,
                metadata=json.loads(metadata_json),
                created_at_utc=changed,
            )
            updated = connection.execute(
                "SELECT * FROM phases WHERE phase_id=?", (phase,)
            ).fetchone()
            assert updated is not None
            return self._decode_row(updated)

    @staticmethod
    def _pointer_checksum(
        *,
        pointer_name: str,
        phase_id: Optional[str],
        rule_id: Optional[str],
        generation: int,
        updated_at_utc: str,
        payload_json: str,
    ) -> str:
        material = _canonical_json(
            {
                "pointer_name": pointer_name,
                "phase_id": phase_id,
                "rule_id": rule_id,
                "generation": generation,
                "updated_at_utc": updated_at_utc,
                "payload": json.loads(payload_json),
            }
        )
        return _json_hash(material)

    def set_active_pointer(
        self,
        pointer_name: str,
        *,
        phase_id: Optional[str],
        expected_generation: Optional[int],
        metadata: Optional[Mapping[str, Any]] = None,
        updated_at_utc: Any = None,
    ) -> Dict[str, Any]:
        """Atomically swap/clear a named production pointer using CAS."""

        pointer = _required_text(pointer_name, "pointer_name")
        phase = _required_text(phase_id, "phase_id") if phase_id is not None else None
        updated = _utc_timestamp(updated_at_utc or _utc_now(), "updated_at_utc")
        payload_json = _canonical_json(dict(metadata or {}))
        with self._write(projected_bytes=len(payload_json) + 1024) as connection:
            existing = connection.execute(
                "SELECT * FROM active_pointer WHERE pointer_name=?", (pointer,)
            ).fetchone()
            current_generation = int(existing["generation"]) if existing else 0
            if expected_generation is not None and int(expected_generation) != current_generation:
                raise ConcurrentUpdateError(
                    f"pointer {pointer} generation is {current_generation}, "
                    f"expected {expected_generation}"
                )
            rule: Optional[str] = None
            if phase is not None:
                phase_row = connection.execute(
                    "SELECT rule_id, status FROM phases WHERE phase_id=?", (phase,)
                ).fetchone()
                if phase_row is None:
                    raise KeyError(f"unknown phase {phase}")
                if phase_row["status"] != "active":
                    raise InvalidLifecycleTransition(
                        "active pointer may reference only an active phase"
                    )
                rule = str(phase_row["rule_id"])
            generation = current_generation + 1
            checksum = self._pointer_checksum(
                pointer_name=pointer,
                phase_id=phase,
                rule_id=rule,
                generation=generation,
                updated_at_utc=updated,
                payload_json=payload_json,
            )
            connection.execute(
                """
                INSERT INTO active_pointer(
                    pointer_name, phase_id, rule_id, generation,
                    payload_json, checksum, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pointer_name) DO UPDATE SET
                    phase_id=excluded.phase_id,
                    rule_id=excluded.rule_id,
                    generation=excluded.generation,
                    payload_json=excluded.payload_json,
                    checksum=excluded.checksum,
                    updated_at_utc=excluded.updated_at_utc
                """,
                (pointer, phase, rule, generation, payload_json, checksum, updated),
            )
            row = connection.execute(
                "SELECT * FROM active_pointer WHERE pointer_name=?", (pointer,)
            ).fetchone()
            assert row is not None
            return self._decode_row(row)

    def transition_phase_and_set_pointer(
        self,
        phase_id: str,
        to_status: str,
        *,
        expected_status: str,
        pointer_name: str,
        pointer_phase_id: Optional[str],
        expected_generation: Optional[int],
        reason: str,
        actor: str = "automation",
        metadata: Optional[Mapping[str, Any]] = None,
        changed_at_utc: Any = None,
    ) -> Dict[str, Any]:
        """Commit a lifecycle transition and production-pointer CAS together."""

        phase = _required_text(phase_id, "phase_id")
        target = _required_text(to_status, "to_status").lower()
        expected = _required_text(expected_status, "expected_status").lower()
        pointer = _required_text(pointer_name, "pointer_name")
        pointer_phase = (
            _required_text(pointer_phase_id, "pointer_phase_id")
            if pointer_phase_id is not None
            else None
        )
        if target not in PHASE_STATUSES:
            raise ValueError(f"unsupported phase status {target}")
        normalized_reason = _required_text(reason, "reason")
        normalized_actor = _required_text(actor, "actor")
        changed = _utc_timestamp(
            changed_at_utc or _utc_now(), "changed_at_utc"
        )
        metadata_value = dict(metadata or {})
        payload_json = _canonical_json(metadata_value)
        with self._write(projected_bytes=len(payload_json) + 2048) as connection:
            phase_row = connection.execute(
                "SELECT * FROM phases WHERE phase_id=?", (phase,)
            ).fetchone()
            if phase_row is None:
                raise KeyError(f"unknown phase {phase}")
            current = str(phase_row["status"])
            if current != expected:
                raise ConcurrentUpdateError(
                    f"phase {phase} status is {current}, expected {expected}"
                )
            if current in TERMINAL_PHASE_STATUSES:
                raise InvalidLifecycleTransition(
                    f"terminal phase {phase} cannot move from {current} to {target}"
                )
            pointer_row = connection.execute(
                "SELECT * FROM active_pointer WHERE pointer_name=?", (pointer,)
            ).fetchone()
            current_generation = (
                int(pointer_row["generation"]) if pointer_row else 0
            )
            if (
                expected_generation is not None
                and int(expected_generation) != current_generation
            ):
                raise ConcurrentUpdateError(
                    f"pointer {pointer} generation is {current_generation}, "
                    f"expected {expected_generation}"
                )
            connection.execute(
                "UPDATE phases SET status=?, updated_at_utc=? WHERE phase_id=?",
                (target, changed, phase),
            )
            self._insert_lifecycle_event(
                connection,
                phase_id=phase,
                from_status=current,
                to_status=target,
                reason=normalized_reason,
                actor=normalized_actor,
                metadata=metadata_value,
                created_at_utc=changed,
            )
            pointer_rule: Optional[str] = None
            if pointer_phase is not None:
                pointed = connection.execute(
                    "SELECT rule_id, status FROM phases WHERE phase_id=?",
                    (pointer_phase,),
                ).fetchone()
                if pointed is None:
                    raise KeyError(f"unknown phase {pointer_phase}")
                if str(pointed["status"]) != "active":
                    raise InvalidLifecycleTransition(
                        "active pointer may reference only an active phase"
                    )
                pointer_rule = str(pointed["rule_id"])
            generation = current_generation + 1
            checksum = self._pointer_checksum(
                pointer_name=pointer,
                phase_id=pointer_phase,
                rule_id=pointer_rule,
                generation=generation,
                updated_at_utc=changed,
                payload_json=payload_json,
            )
            connection.execute(
                """
                INSERT INTO active_pointer(
                    pointer_name, phase_id, rule_id, generation,
                    payload_json, checksum, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pointer_name) DO UPDATE SET
                    phase_id=excluded.phase_id,
                    rule_id=excluded.rule_id,
                    generation=excluded.generation,
                    payload_json=excluded.payload_json,
                    checksum=excluded.checksum,
                    updated_at_utc=excluded.updated_at_utc
                """,
                (
                    pointer,
                    pointer_phase,
                    pointer_rule,
                    generation,
                    payload_json,
                    checksum,
                    changed,
                ),
            )
            updated_phase = connection.execute(
                "SELECT * FROM phases WHERE phase_id=?", (phase,)
            ).fetchone()
            updated_pointer = connection.execute(
                "SELECT * FROM active_pointer WHERE pointer_name=?", (pointer,)
            ).fetchone()
            assert updated_phase is not None and updated_pointer is not None
            return {
                "phase": self._decode_row(updated_phase),
                "pointer": self._decode_row(updated_pointer),
            }

    def get_active_pointer(self, pointer_name: str) -> Optional[Dict[str, Any]]:
        pointer = _required_text(pointer_name, "pointer_name")
        with self._read() as connection:
            row = connection.execute(
                "SELECT * FROM active_pointer WHERE pointer_name=?", (pointer,)
            ).fetchone()
            if row is None:
                return None
            expected = self._pointer_checksum(
                pointer_name=str(row["pointer_name"]),
                phase_id=row["phase_id"],
                rule_id=row["rule_id"],
                generation=int(row["generation"]),
                updated_at_utc=str(row["updated_at_utc"]),
                payload_json=str(row["payload_json"]),
            )
            if expected != row["checksum"]:
                raise WideResearchStoreError(
                    f"active pointer checksum mismatch for {pointer}"
                )
            return self._decode_row(row)

    def recover(self) -> Dict[str, int]:
        """Verify durable state and repair denormalized latest-outcome columns."""

        with self._write(projected_bytes=0) as connection:
            check = connection.execute("PRAGMA quick_check").fetchone()
            if check is None or str(check[0]).lower() != "ok":
                raise WideResearchStoreError(
                    f"SQLite quick_check failed: {check[0] if check else 'no result'}"
                )
            repaired = 0
            triggers = connection.execute(
                "SELECT phase_id, rule_id, fixture_id, observation_id FROM triggers"
            ).fetchall()
            for trigger in triggers:
                changed = self._apply_latest_outcome_to_trigger(
                    connection,
                    phase_id=str(trigger["phase_id"]),
                    rule_id=str(trigger["rule_id"]),
                    fixture_id=str(trigger["fixture_id"]),
                    observation_id=str(trigger["observation_id"]),
                )
                if changed:
                    repaired += 1
            pointers = connection.execute(
                "SELECT pointer_name FROM active_pointer"
            ).fetchall()
            looks = connection.execute(
                "SELECT * FROM phase_looks ORDER BY phase_id, milestone"
            ).fetchall()
            for look in looks:
                identities_json = str(look["trigger_ids_json"])
                if _json_hash(identities_json) != str(look["trigger_ids_hash"]):
                    raise WideResearchStoreError(
                        "phase look checksum mismatch for "
                        f"{look['phase_id']}:{look['milestone']}"
                    )
                identities = json.loads(identities_json)
                if (
                    not isinstance(identities, list)
                    or len(identities) != int(look["milestone"])
                ):
                    raise WideResearchStoreError(
                        "phase look identity count mismatch for "
                        f"{look['phase_id']}:{look['milestone']}"
                    )
        for pointer in pointers:
            self.get_active_pointer(str(pointer["pointer_name"]))
        with self._read() as connection:
            counts = {
                "rules": int(connection.execute("SELECT COUNT(*) FROM rules").fetchone()[0]),
                "phases": int(connection.execute("SELECT COUNT(*) FROM phases").fetchone()[0]),
                "fixtures": int(
                    connection.execute("SELECT COUNT(*) FROM universe_fixtures").fetchone()[0]
                ),
                "triggers": int(connection.execute("SELECT COUNT(*) FROM triggers").fetchone()[0]),
                "outcome_events": int(
                    connection.execute("SELECT COUNT(*) FROM outcome_events").fetchone()[0]
                ),
                "phase_looks": int(
                    connection.execute("SELECT COUNT(*) FROM phase_looks").fetchone()[0]
                ),
                "alpha_allocations": int(
                    connection.execute("SELECT COUNT(*) FROM alpha_allocations").fetchone()[0]
                ),
                "repaired_triggers": repaired,
            }
        self._secure_files()
        return counts


_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS live_evaluation_inbox (
        observation_id TEXT PRIMARY KEY,
        observed_at_utc TEXT NOT NULL,
        payload_hash TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
        last_error TEXT,
        created_at_utc TEXT NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_live_evaluation_inbox_order
    ON live_evaluation_inbox(observed_at_utc, created_at_utc, observation_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS store_profile (
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        profile_id TEXT NOT NULL,
        created_at_utc TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS research_runs (
        run_id TEXT PRIMARY KEY,
        status TEXT NOT NULL CHECK(status IN ('running','completed','failed','cancelled')),
        started_at_utc TEXT NOT NULL,
        completed_at_utc TEXT,
        config_json TEXT NOT NULL,
        created_at_utc TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS alpha_allocations (
        run_id TEXT PRIMARY KEY,
        sequence INTEGER NOT NULL UNIQUE CHECK(sequence >= 1),
        global_alpha REAL NOT NULL CHECK(global_alpha > 0 AND global_alpha <= 1),
        alpha_budget REAL NOT NULL CHECK(alpha_budget > 0 AND alpha_budget <= 1),
        created_at_utc TEXT NOT NULL,
        FOREIGN KEY (run_id) REFERENCES research_runs(run_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS rules (
        rule_id TEXT PRIMARY KEY,
        manifest_hash TEXT NOT NULL,
        manifest_json TEXT NOT NULL,
        created_at_utc TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS phases (
        phase_id TEXT PRIMARY KEY,
        rule_id TEXT NOT NULL,
        run_id TEXT,
        status TEXT NOT NULL CHECK(status IN (
            'candidate','shadow','ready','active','degraded','paused',
            'retired','rejected','failed'
        )),
        starts_at_utc TEXT NOT NULL,
        stops_at_utc TEXT,
        policy_hash TEXT NOT NULL,
        policy_json TEXT NOT NULL,
        created_at_utc TEXT NOT NULL,
        updated_at_utc TEXT NOT NULL,
        UNIQUE (phase_id, rule_id),
        FOREIGN KEY (rule_id) REFERENCES rules(rule_id),
        FOREIGN KEY (run_id) REFERENCES research_runs(run_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS universe_fixtures (
        fixture_id TEXT PRIMARY KEY,
        first_observation_id TEXT NOT NULL,
        first_seen_at_utc TEXT NOT NULL,
        first_minute REAL,
        first_league TEXT,
        first_input_json TEXT NOT NULL,
        latest_observation_id TEXT NOT NULL,
        latest_observed_at_utc TEXT NOT NULL,
        latest_minute REAL,
        latest_league TEXT,
        latest_input_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS triggers (
        phase_id TEXT NOT NULL,
        rule_id TEXT NOT NULL,
        fixture_id TEXT NOT NULL,
        observation_id TEXT NOT NULL,
        triggered_at_utc TEXT NOT NULL,
        minute REAL,
        league TEXT,
        input_json TEXT NOT NULL,
        latest_outcome_event_id TEXT,
        latest_outcome_version INTEGER CHECK(
            latest_outcome_version IS NULL OR latest_outcome_version >= 1
        ),
        latest_outcome_status TEXT NOT NULL DEFAULT 'pending' CHECK(
            latest_outcome_status IN ('win','loss','pending','invalid')
        ),
        latest_outcome_label INTEGER CHECK(
            latest_outcome_label IS NULL OR latest_outcome_label IN (0,1)
        ),
        latest_outcome_at_utc TEXT,
        latest_outcome_json TEXT,
        updated_at_utc TEXT NOT NULL,
        PRIMARY KEY (phase_id, rule_id, fixture_id),
        UNIQUE (phase_id, rule_id, fixture_id),
        FOREIGN KEY (phase_id, rule_id) REFERENCES phases(phase_id, rule_id),
        FOREIGN KEY (fixture_id) REFERENCES universe_fixtures(fixture_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_wide_triggers_observation
    ON triggers(observation_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_wide_triggers_phase_time
    ON triggers(phase_id, triggered_at_utc)
    """,
    """
    CREATE TABLE IF NOT EXISTS phase_looks (
        phase_id TEXT NOT NULL,
        milestone INTEGER NOT NULL CHECK(milestone >= 1),
        trigger_ids_json TEXT NOT NULL,
        trigger_ids_hash TEXT NOT NULL,
        created_at_utc TEXT NOT NULL,
        PRIMARY KEY (phase_id, milestone),
        FOREIGN KEY (phase_id) REFERENCES phases(phase_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS outcome_events (
        event_id TEXT PRIMARY KEY,
        observation_id TEXT NOT NULL,
        outcome_version INTEGER NOT NULL CHECK(outcome_version >= 1),
        outcome_status TEXT NOT NULL CHECK(
            outcome_status IN ('win','loss','pending','invalid')
        ),
        outcome_label INTEGER CHECK(outcome_label IS NULL OR outcome_label IN (0,1)),
        outcome_at_utc TEXT NOT NULL,
        received_at_utc TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        UNIQUE (observation_id, outcome_version)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_wide_outcomes_observation_version
    ON outcome_events(observation_id, outcome_version DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS lifecycle_events (
        event_id TEXT PRIMARY KEY,
        phase_id TEXT NOT NULL,
        from_status TEXT,
        to_status TEXT NOT NULL,
        reason TEXT NOT NULL,
        actor TEXT NOT NULL,
        metadata_json TEXT NOT NULL,
        created_at_utc TEXT NOT NULL,
        FOREIGN KEY (phase_id) REFERENCES phases(phase_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_wide_lifecycle_phase_time
    ON lifecycle_events(phase_id, created_at_utc)
    """,
    """
    CREATE TABLE IF NOT EXISTS active_pointer (
        pointer_name TEXT PRIMARY KEY,
        phase_id TEXT,
        rule_id TEXT,
        generation INTEGER NOT NULL CHECK(generation >= 1),
        payload_json TEXT NOT NULL,
        checksum TEXT NOT NULL,
        updated_at_utc TEXT NOT NULL,
        FOREIGN KEY (phase_id, rule_id) REFERENCES phases(phase_id, rule_id)
    )
    """,
)


__all__ = [
    "ConcurrentUpdateError",
    "DatabaseSizeLimitError",
    "DEFAULT_MAX_DB_BYTES",
    "ImmutableRecordError",
    "InvalidLifecycleTransition",
    "UnsafeDatabasePathError",
    "WideResearchStore",
    "WideResearchStoreError",
    "safe_database_path",
]
