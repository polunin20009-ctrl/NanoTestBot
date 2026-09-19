from __future__ import annotations

import contextlib
import gzip
import json
import logging
import os
import shutil
import sqlite3
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping, Optional, Union

try:  # Linux production path; tests remain usable on platforms without fcntl.
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]


PathLike = Union[str, os.PathLike[str]]
logger = logging.getLogger(__name__)


def _absolute(path: PathLike) -> str:
    return os.path.realpath(
        os.path.abspath(os.path.expanduser(os.fspath(path)))
    )


def _timestamp_archive_name(candidate: str, filename: str) -> bool:
    stem, extension = os.path.splitext(filename)
    if not candidate.startswith(stem + "."):
        return False
    if candidate.endswith(extension + ".gz"):
        suffix = extension + ".gz"
    elif candidate.endswith(extension):
        suffix = extension
    else:
        return False
    stamp = candidate[len(stem) + 1 : -len(suffix)]
    for pattern in ("%Y%m%dT%H%M%S%fZ", "%Y%m%dT%H%M%SZ"):
        try:
            datetime.strptime(stamp, pattern)
            return True
        except ValueError:
            continue
    return False


def jsonl_paths(path: PathLike) -> list[str]:
    """Return timestamp archives oldest-first, followed by the active file."""
    active = _absolute(path)
    parent = os.path.dirname(active) or os.curdir
    filename = os.path.basename(active)
    archives: list[str] = []
    try:
        for candidate in os.listdir(parent):
            if candidate == filename:
                continue
            if _timestamp_archive_name(candidate, filename):
                archives.append(os.path.join(parent, candidate))
    except FileNotFoundError:
        return []
    archives.sort()
    if os.path.exists(active):
        archives.append(active)
    return archives


def _iter_one_jsonl(path: str) -> Iterator[Dict[str, Any]]:
    opener = gzip.open if path.endswith(".gz") else open
    try:
        with opener(path, "rt", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                try:
                    payload = json.loads(line)
                except (TypeError, ValueError):
                    logger.warning(
                        "[SHADOW_CANDIDATE_INVALID_LINE] file=%s line=%s",
                        path,
                        line_number,
                    )
                    continue
                if isinstance(payload, dict):
                    yield payload
    except OSError:
        logger.exception("[SHADOW_CANDIDATE_READ_ERROR] file=%s", path)


def iter_jsonl_records(path: PathLike) -> Iterator[Dict[str, Any]]:
    """Stream valid objects from active JSONL and timestamp gzip archives."""
    for candidate in jsonl_paths(path):
        yield from _iter_one_jsonl(candidate)


def _event_id(record: Mapping[str, Any]) -> str:
    return str(record.get("event_id") or "").strip()


class AppendOnlyCandidateJournal:
    """Append-only candidate journal with an exact disk-backed dedupe index.

    SQLite is a derived acceleration index only. JSONL and its gzip archives
    remain the source of truth. The index is incrementally repaired whenever a
    file signature changes, so a crash after fsync but before index update does
    not create a duplicate after restart. No all-event set is held in RAM.
    """

    def __init__(
        self,
        path: PathLike,
        *,
        rotate_max_bytes: int = 10 * 1024 * 1024,
        index_path: Optional[PathLike] = None,
    ) -> None:
        self.path = _absolute(path)
        self.rotate_max_bytes = max(0, int(rotate_max_bytes))
        self.index_path = _absolute(index_path or (self.path + ".index.sqlite3"))
        self.lock_path = self.path + ".lock"
        if self.index_path in {self.path, self.lock_path}:
            raise ValueError(
                "candidate journal, lock, and derived index must use "
                "different paths"
            )
        self._thread_lock = threading.RLock()

    @contextlib.contextmanager
    def _exclusive(self) -> Iterator[None]:
        parent = os.path.dirname(self.path) or os.curdir
        os.makedirs(parent, exist_ok=True)
        with self._thread_lock:
            with open(self.lock_path, "a+b") as lock_handle:
                if fcntl is not None:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    if fcntl is not None:
                        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    def _connect(self) -> sqlite3.Connection:
        os.makedirs(os.path.dirname(self.index_path) or os.curdir, exist_ok=True)
        connection = sqlite3.connect(self.index_path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS indexed_files (
                path TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                record_type TEXT,
                ruleset_version TEXT,
                observation_id TEXT,
                fixture_id TEXT,
                arm_id TEXT,
                created_at_utc TEXT
            );
            CREATE TABLE IF NOT EXISTS ruleset_boundaries (
                ruleset_version TEXT PRIMARY KEY,
                prospective_start_utc TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cohorts (
                ruleset_version TEXT NOT NULL,
                fixture_id TEXT NOT NULL,
                first_seen_at_utc TEXT,
                first_seen_observation_id TEXT,
                first_control_allow_at_utc TEXT,
                first_control_allow_observation_id TEXT,
                first_control_allow_event_id TEXT,
                PRIMARY KEY (ruleset_version, fixture_id)
            );
            CREATE TABLE IF NOT EXISTS triggers (
                ruleset_version TEXT NOT NULL,
                fixture_id TEXT NOT NULL,
                arm_id TEXT NOT NULL,
                observation_id TEXT NOT NULL,
                decision_event_id TEXT NOT NULL,
                created_at_utc TEXT,
                PRIMARY KEY (ruleset_version, fixture_id, arm_id)
            );
            CREATE INDEX IF NOT EXISTS idx_triggers_observation
                ON triggers (ruleset_version, observation_id);
            """
        )
        return connection

    @staticmethod
    def _signature(path: str) -> tuple[int, int]:
        stat = os.stat(path)
        return int(stat.st_size), int(stat.st_mtime_ns)

    def _index_record(
        self, connection: sqlite3.Connection, record: Mapping[str, Any]
    ) -> None:
        event_id = _event_id(record)
        if not event_id:
            return
        ruleset_version = str(record.get("ruleset_version") or "")
        observation_id = str(record.get("observation_id") or "")
        fixture_id = str(record.get("fixture_id") or "")
        record_type = str(record.get("record_type") or "")
        arm_id = str(record.get("arm_id") or "")
        created_at = str(record.get("created_at_utc") or "")
        self._validate_ruleset_boundary(connection, record)
        connection.execute(
            """
            INSERT OR IGNORE INTO events
                (event_id, record_type, ruleset_version, observation_id,
                 fixture_id, arm_id, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                record_type,
                ruleset_version,
                observation_id,
                fixture_id,
                arm_id,
                created_at,
            ),
        )
        if record_type != "shadow_candidate_decision":
            return

        cohort = record.get("cohort")
        cohort_map = cohort if isinstance(cohort, Mapping) else {}
        if ruleset_version and fixture_id:
            connection.execute(
                """
                INSERT OR IGNORE INTO cohorts
                    (ruleset_version, fixture_id, first_seen_at_utc,
                     first_seen_observation_id)
                VALUES (?, ?, ?, ?)
                """,
                (
                    ruleset_version,
                    fixture_id,
                    cohort_map.get("first_seen_at_utc") or created_at,
                    cohort_map.get("first_seen_observation_id") or observation_id,
                ),
            )
            if cohort_map.get("cohort_claimed") is True:
                connection.execute(
                    """
                    UPDATE cohorts
                    SET first_control_allow_at_utc = COALESCE(
                            first_control_allow_at_utc, ?),
                        first_control_allow_observation_id = COALESCE(
                            first_control_allow_observation_id, ?),
                        first_control_allow_event_id = COALESCE(
                            first_control_allow_event_id, ?)
                    WHERE ruleset_version = ? AND fixture_id = ?
                    """,
                    (
                        cohort_map.get("first_control_allow_at_utc") or created_at,
                        cohort_map.get("first_control_allow_observation_id")
                        or observation_id,
                        event_id,
                        ruleset_version,
                        fixture_id,
                    ),
                )

        arms = record.get("arms")
        if not isinstance(arms, Mapping):
            return
        for current_arm_id, evaluation in arms.items():
            if not isinstance(evaluation, Mapping):
                continue
            if evaluation.get("first_allow") is not True:
                continue
            connection.execute(
                """
                INSERT OR IGNORE INTO triggers
                    (ruleset_version, fixture_id, arm_id, observation_id,
                     decision_event_id, created_at_utc)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    ruleset_version,
                    fixture_id,
                    str(current_arm_id),
                    observation_id,
                    event_id,
                    created_at,
                ),
            )

    @staticmethod
    def _validate_ruleset_boundary(
        connection: sqlite3.Connection,
        record: Mapping[str, Any],
    ) -> None:
        ruleset_version = str(record.get("ruleset_version") or "")
        prospective_start = str(record.get("prospective_start_utc") or "")
        if not ruleset_version or not prospective_start:
            return
        existing = connection.execute(
            """
            SELECT prospective_start_utc FROM ruleset_boundaries
            WHERE ruleset_version = ?
            """,
            (ruleset_version,),
        ).fetchone()
        if existing is not None and str(existing[0]) != prospective_start:
            raise ValueError(
                "prospective_start_utc mismatch for immutable ruleset "
                f"{ruleset_version}: {existing[0]} != {prospective_start}"
            )
        connection.execute(
            """
            INSERT OR IGNORE INTO ruleset_boundaries
                (ruleset_version, prospective_start_utc)
            VALUES (?, ?)
            """,
            (ruleset_version, prospective_start),
        )

    def _sync_index(self, connection: sqlite3.Connection) -> None:
        current_paths = jsonl_paths(self.path)
        stored = {
            str(row["path"]): (int(row["size"]), int(row["mtime_ns"]))
            for row in connection.execute(
                "SELECT path, size, mtime_ns FROM indexed_files"
            )
        }
        current_set = set(current_paths)
        for candidate in current_paths:
            signature = self._signature(candidate)
            if stored.get(candidate) != signature:
                for record in _iter_one_jsonl(candidate):
                    self._index_record(connection, record)
                connection.execute(
                    """
                    INSERT INTO indexed_files(path, size, mtime_ns)
                    VALUES (?, ?, ?)
                    ON CONFLICT(path) DO UPDATE SET
                        size=excluded.size, mtime_ns=excluded.mtime_ns
                    """,
                    (candidate, signature[0], signature[1]),
                )
        for missing in set(stored) - current_set:
            connection.execute("DELETE FROM indexed_files WHERE path = ?", (missing,))
        connection.commit()

    def _open_synced(self) -> sqlite3.Connection:
        connection: Optional[sqlite3.Connection] = None
        try:
            connection = self._connect()
            self._sync_index(connection)
            return connection
        except sqlite3.DatabaseError:
            logger.exception(
                "[SHADOW_CANDIDATE_INDEX_REBUILD] path=%s", self.index_path
            )
            try:
                if connection is not None:
                    connection.close()
            except Exception:
                pass
            if os.path.exists(self.index_path):
                stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
                os.replace(self.index_path, self.index_path + f".broken.{stamp}")
            connection = self._connect()
            self._sync_index(connection)
            return connection
        except Exception:
            if connection is not None:
                connection.close()
            raise

    def _archive_path(self) -> str:
        parent = os.path.dirname(self.path) or os.curdir
        filename = os.path.basename(self.path)
        stem, extension = os.path.splitext(filename)
        now = datetime.now(timezone.utc)
        for offset in range(10_000):
            stamp = (now + timedelta(microseconds=offset)).strftime(
                "%Y%m%dT%H%M%S%fZ"
            )
            candidate = os.path.join(parent, f"{stem}.{stamp}{extension}.gz")
            if not os.path.exists(candidate) and not os.path.exists(candidate + ".tmp"):
                return candidate
        raise FileExistsError("could not allocate candidate journal archive")

    def _rotate_if_needed(self, incoming_bytes: int) -> Optional[str]:
        if (
            self.rotate_max_bytes <= 0
            or not os.path.exists(self.path)
            or os.path.getsize(self.path) + incoming_bytes <= self.rotate_max_bytes
        ):
            return None
        archive = self._archive_path()
        temporary = archive + ".tmp"
        try:
            with open(self.path, "rb") as source:
                with open(temporary, "wb") as raw_target:
                    with gzip.GzipFile(
                        filename="",
                        mode="wb",
                        compresslevel=6,
                        fileobj=raw_target,
                        mtime=0,
                    ) as compressed:
                        shutil.copyfileobj(source, compressed, length=1024 * 1024)
                    raw_target.flush()
                    os.fsync(raw_target.fileno())
            os.replace(temporary, archive)
            os.unlink(self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return archive

    @staticmethod
    def _serialize(record: Mapping[str, Any]) -> str:
        return json.dumps(
            dict(record),
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n"

    def _write_locked(
        self,
        connection: sqlite3.Connection,
        record: Mapping[str, Any],
    ) -> bool:
        event_id = _event_id(record)
        if not event_id:
            return False
        exists = connection.execute(
            "SELECT 1 FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        if exists is not None:
            return False
        self._validate_ruleset_boundary(connection, record)
        line = self._serialize(record)
        self._rotate_if_needed(len(line.encode("utf-8")))
        with open(self.path, "a", encoding="utf-8", newline="\n") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
        self._index_record(connection, record)
        for candidate in jsonl_paths(self.path):
            size, mtime_ns = self._signature(candidate)
            connection.execute(
                """
                INSERT INTO indexed_files(path, size, mtime_ns)
                VALUES (?, ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                    size=excluded.size, mtime_ns=excluded.mtime_ns
                """,
                (candidate, size, mtime_ns),
            )
        connection.commit()
        return True

    def append(self, record: Mapping[str, Any]) -> bool:
        """Append a generic event exactly once by ``event_id``."""
        if not isinstance(record, Mapping) or not _event_id(record):
            return False
        # Validate before taking the I/O lock.
        self._serialize(record)
        with self._exclusive():
            connection = self._open_synced()
            try:
                return self._write_locked(connection, record)
            finally:
                connection.close()

    def append_evaluation(
        self, base_record: Mapping[str, Any]
    ) -> tuple[bool, Optional[Dict[str, Any]]]:
        """Atomically claim/freeze the first-control cohort and append an audit.

        Every raw arm evaluation is retained. Candidate decisions are made only
        once, at the fixture's first prospective control ALLOW. Missing data at
        that snapshot is a permanent reject for this ruleset cohort.
        """
        if not isinstance(base_record, Mapping) or not _event_id(base_record):
            return False, None
        record = json.loads(self._serialize(base_record))
        ruleset_version = str(record.get("ruleset_version") or "")
        fixture_id = str(record.get("fixture_id") or "")
        observation_id = str(record.get("observation_id") or "")
        if not ruleset_version or not fixture_id or not observation_id:
            return False, None

        with self._exclusive():
            connection = self._open_synced()
            try:
                if connection.execute(
                    "SELECT 1 FROM events WHERE event_id = ?",
                    (_event_id(record),),
                ).fetchone() is not None:
                    return False, None

                row = connection.execute(
                    """
                    SELECT * FROM cohorts
                    WHERE ruleset_version = ? AND fixture_id = ?
                    """,
                    (ruleset_version, fixture_id),
                ).fetchone()
                source = record.get("source")
                source_map = source if isinstance(source, Mapping) else {}
                created_at = str(record.get("created_at_utc") or "")
                source_created = str(source_map.get("created_at_utc") or created_at)
                if row is None:
                    first_seen_at = source_created
                    first_seen_observation = observation_id
                    first_control_observation = None
                    first_control_event = None
                    first_control_at = None
                else:
                    first_seen_at = row["first_seen_at_utc"]
                    first_seen_observation = row["first_seen_observation_id"]
                    first_control_observation = row[
                        "first_control_allow_observation_id"
                    ]
                    first_control_event = row["first_control_allow_event_id"]
                    first_control_at = row["first_control_allow_at_utc"]

                arms = record.get("arms")
                if not isinstance(arms, dict):
                    return False, None
                control = arms.get("control_current_filter")
                control_passed = bool(
                    isinstance(control, Mapping)
                    and control.get("evaluation_status") == "pass"
                )
                cohort_claimed = first_control_observation is None and control_passed
                if cohort_claimed:
                    first_control_observation = observation_id
                    first_control_event = _event_id(record)
                    first_control_at = source_created

                is_first_control_snapshot = bool(
                    first_control_observation
                    and first_control_observation == observation_id
                    and cohort_claimed
                )
                record["cohort"] = {
                    "semantics": "candidate_at_first_control_allow",
                    "ruleset_version": ruleset_version,
                    "prospective_start_utc": record.get(
                        "prospective_start_utc"
                    ),
                    "first_seen_at_utc": first_seen_at,
                    "first_seen_observation_id": first_seen_observation,
                    "cohort_claimed": cohort_claimed,
                    "is_first_control_allow_snapshot": is_first_control_snapshot,
                    "first_control_allow_at_utc": first_control_at,
                    "first_control_allow_observation_id": first_control_observation,
                    "first_control_allow_event_id": first_control_event,
                    "unavailable_at_first_control_is_permanent_reject": True,
                }

                for evaluation in arms.values():
                    if not isinstance(evaluation, dict):
                        continue
                    evaluation["shadow_only"] = True
                    evaluation["production_applied"] = False
                    evaluation["first_allow"] = False
                    evaluation["candidate_at_first_control_allow"] = (
                        is_first_control_snapshot
                    )
                    if is_first_control_snapshot:
                        status = evaluation.get("evaluation_status")
                        if status == "pass":
                            evaluation["candidate_decision"] = "ALLOW"
                            evaluation["first_allow"] = True
                            evaluation["candidate_reason"] = "pass_at_first_control_allow"
                        elif status == "unavailable":
                            evaluation["candidate_decision"] = "REJECT_UNAVAILABLE"
                            evaluation["candidate_reason"] = (
                                "unavailable_at_first_control_allow_reject_forever"
                            )
                        else:
                            evaluation["candidate_decision"] = "BLOCK"
                            evaluation["candidate_reason"] = (
                                "failed_at_first_control_allow"
                            )
                    elif first_control_observation:
                        evaluation["candidate_decision"] = "FROZEN"
                        evaluation["candidate_reason"] = (
                            "cohort_already_frozen_at_first_control_allow"
                        )
                    else:
                        evaluation["candidate_decision"] = "WAITING_FOR_CONTROL"
                        evaluation["candidate_reason"] = (
                            "no_control_allow_seen_for_fixture"
                        )

                appended = self._write_locked(connection, record)
                return appended, record if appended else None
            finally:
                connection.close()

    def trigger_rows_for_observation(
        self, ruleset_version: str, observation_id: str
    ) -> list[Dict[str, Any]]:
        """Return the bounded, disk-indexed first ALLOW arms for one source."""
        return self.trigger_rows_for_observations(
            ruleset_version, [observation_id]
        ).get(str(observation_id), [])

    def trigger_rows_for_observations(
        self, ruleset_version: str, observation_ids: list[str]
    ) -> Dict[str, list[Dict[str, Any]]]:
        """Resolve first-ALLOW arms for many outcomes with one index sync.

        Queries are chunked below SQLite's common variable limit. Memory is
        proportional only to the requested fixture-resolution batch, never to
        the complete journal.
        """
        normalized = sorted({str(item).strip() for item in observation_ids if str(item).strip()})
        if not normalized:
            return {}
        result: Dict[str, list[Dict[str, Any]]] = {item: [] for item in normalized}
        with self._exclusive():
            connection = self._open_synced()
            try:
                for offset in range(0, len(normalized), 500):
                    chunk = normalized[offset : offset + 500]
                    placeholders = ",".join("?" for _ in chunk)
                    rows = connection.execute(
                        f"""
                        SELECT * FROM triggers
                        WHERE ruleset_version = ?
                          AND observation_id IN ({placeholders})
                        ORDER BY observation_id, arm_id
                        """,
                        (str(ruleset_version), *chunk),
                    ).fetchall()
                    for row in rows:
                        materialized = dict(row)
                        result[str(materialized["observation_id"])].append(
                            materialized
                        )
                return result
            finally:
                connection.close()

    def iter_records(self) -> Iterator[Dict[str, Any]]:
        yield from iter_jsonl_records(self.path)


def atomic_write_json(path: PathLike, payload: Mapping[str, Any]) -> None:
    """Atomically write a JSON report; callers must validate its target first."""
    target = _absolute(path)
    parent = os.path.dirname(target) or os.curdir
    os.makedirs(parent, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{Path(target).stem}.", suffix=".tmp", dir=parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(
                dict(payload),
                handle,
                allow_nan=False,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
