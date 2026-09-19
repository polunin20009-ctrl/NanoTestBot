from __future__ import annotations

import contextlib
import gzip
import json
import logging
import os
import shutil
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional, Sequence, Union

from .normalization import SNAPSHOT_RECORD_TYPE

try:  # Linux production path; tests remain portable.
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]


PathLike = Union[str, os.PathLike[str]]
logger = logging.getLogger(__name__)


def _absolute(path: PathLike) -> str:
    return os.path.realpath(
        os.path.abspath(os.path.expanduser(os.fspath(path)))
    )


def _archive_name(candidate: str, filename: str) -> bool:
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


def _archive_sort_key(path: str, filename: str) -> tuple[datetime, str]:
    """Sort mixed second/microsecond archive names by actual UTC time."""

    candidate = os.path.basename(path)
    stem, extension = os.path.splitext(filename)
    suffix = (
        extension + ".gz"
        if candidate.endswith(extension + ".gz")
        else extension
    )
    stamp = candidate[len(stem) + 1 : -len(suffix)]
    for pattern in ("%Y%m%dT%H%M%S%fZ", "%Y%m%dT%H%M%SZ"):
        try:
            return datetime.strptime(stamp, pattern).replace(
                tzinfo=timezone.utc
            ), candidate
        except ValueError:
            continue
    return datetime.max.replace(tzinfo=timezone.utc), candidate


def market_journal_paths(path: PathLike) -> list[str]:
    """Return timestamp archives oldest-first, followed by active JSONL."""

    active = _absolute(path)
    parent = os.path.dirname(active) or os.curdir
    filename = os.path.basename(active)
    archives: list[str] = []
    try:
        for candidate in os.listdir(parent):
            if candidate == filename:
                continue
            if _archive_name(candidate, filename):
                archives.append(os.path.join(parent, candidate))
    except FileNotFoundError:
        return []
    archives.sort(key=lambda path: _archive_sort_key(path, filename))
    if os.path.exists(active):
        archives.append(active)
    return archives


def _iter_file(path: str) -> Iterator[dict[str, Any]]:
    opener = gzip.open if path.endswith(".gz") else open
    try:
        with opener(path, "rt", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                try:
                    payload = json.loads(line)
                except (TypeError, ValueError):
                    logger.warning(
                        "[MARKET_BENCHMARK_INVALID_LINE] file=%s line=%s",
                        path,
                        line_number,
                    )
                    continue
                if isinstance(payload, dict):
                    yield payload
    except OSError:
        logger.exception("[MARKET_BENCHMARK_READ_ERROR] file=%s", path)


def iter_market_records(path: PathLike) -> Iterator[dict[str, Any]]:
    """Stream valid objects across active JSONL and timestamp archives."""

    for candidate in market_journal_paths(path):
        yield from _iter_file(candidate)


def _timestamp_epoch(value: object) -> Optional[float]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc).timestamp()


class AppendOnlyMarketJournal:
    """Append-only JSONL journal with a derived disk-backed dedupe index.

    JSONL and gzip archives remain the source of truth.  SQLite contains no
    authoritative state; it is rebuilt whenever journal file signatures do
    not match.  Therefore no unbounded set of record keys is retained in RAM.
    """

    def __init__(
        self,
        path: PathLike,
        *,
        rotate_max_bytes: int = 50 * 1024 * 1024,
        index_path: Optional[PathLike] = None,
    ) -> None:
        self.path = _absolute(path)
        self.rotate_max_bytes = max(0, int(rotate_max_bytes))
        self.index_path = _absolute(index_path or (self.path + ".index.sqlite3"))
        self.lock_path = self.path + ".lock"
        if self.index_path in {self.path, self.lock_path}:
            raise ValueError("journal, lock, and index paths must differ")
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
            CREATE TABLE IF NOT EXISTS records (
                record_key TEXT PRIMARY KEY,
                record_type TEXT,
                fixture_id TEXT,
                captured_at_epoch REAL,
                captured_at_utc TEXT,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_market_fixture_time
                ON records (fixture_id, record_type, captured_at_epoch DESC);
            """
        )
        return connection

    @staticmethod
    def _signature(path: str) -> tuple[int, int]:
        stat = os.stat(path)
        return int(stat.st_size), int(stat.st_mtime_ns)

    def _file_signatures(self) -> dict[str, tuple[int, int]]:
        return {
            path: self._signature(path)
            for path in market_journal_paths(self.path)
        }

    @staticmethod
    def _indexed_signatures(
        connection: sqlite3.Connection,
    ) -> dict[str, tuple[int, int]]:
        return {
            str(row["path"]): (int(row["size"]), int(row["mtime_ns"]))
            for row in connection.execute(
                "SELECT path, size, mtime_ns FROM indexed_files"
            )
        }

    @staticmethod
    def _index_record(
        connection: sqlite3.Connection,
        record: Mapping[str, Any],
        *,
        payload_json: Optional[str] = None,
    ) -> None:
        record_key = str(record.get("record_key") or "").strip()
        if not record_key:
            return
        serialized = payload_json or json.dumps(
            dict(record),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO records
                (record_key, record_type, fixture_id, captured_at_epoch,
                 captured_at_utc, payload_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                record_key,
                str(record.get("record_type") or ""),
                str(record.get("fixture_id") or ""),
                _timestamp_epoch(record.get("captured_at_utc")),
                str(record.get("captured_at_utc") or ""),
                serialized,
            ),
        )

    def _replace_signatures(
        self,
        connection: sqlite3.Connection,
        signatures: Mapping[str, tuple[int, int]],
    ) -> None:
        connection.execute("DELETE FROM indexed_files")
        connection.executemany(
            "INSERT INTO indexed_files (path, size, mtime_ns) VALUES (?, ?, ?)",
            [
                (path, signature[0], signature[1])
                for path, signature in signatures.items()
            ],
        )

    def _synchronize_index(self, connection: sqlite3.Connection) -> None:
        signatures = self._file_signatures()
        if signatures == self._indexed_signatures(connection):
            return
        connection.execute("DELETE FROM records")
        for record in iter_market_records(self.path):
            self._index_record(connection, record)
        self._replace_signatures(connection, signatures)
        connection.commit()

    def _archive_path(self) -> str:
        parent = os.path.dirname(self.path) or os.curdir
        filename = os.path.basename(self.path)
        stem, extension = os.path.splitext(filename)
        now = datetime.now(timezone.utc)
        for offset in range(10_000):
            stamp = (now + timedelta(microseconds=offset)).strftime(
                "%Y%m%dT%H%M%S%fZ"
            )
            candidate = os.path.join(
                parent,
                f"{stem}.{stamp}{extension}.gz",
            )
            if not os.path.exists(candidate) and not os.path.exists(
                candidate + ".tmp"
            ):
                return candidate
        raise FileExistsError("could not allocate a unique market archive")

    def _rotate_if_needed(self, incoming_bytes: int) -> Optional[str]:
        if (
            self.rotate_max_bytes <= 0
            or not os.path.exists(self.path)
            or os.path.getsize(self.path) == 0
            or os.path.getsize(self.path) + max(0, int(incoming_bytes))
            <= self.rotate_max_bytes
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
    def _prepare(record: object) -> Optional[tuple[dict[str, Any], str]]:
        if not isinstance(record, Mapping):
            return None
        payload = dict(record)
        if not str(payload.get("record_key") or "").strip():
            return None
        if payload.get("shadow_only") is not True:
            return None
        if payload.get("production_applied") is not False:
            return None
        serialized = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return payload, serialized

    def append(self, record: object) -> bool:
        """Append one valid shadow record exactly once per ``record_key``."""

        return self.append_many((record,))[0]

    def append_many(self, records: Sequence[object]) -> tuple[bool, ...]:
        """Append a batch under one cross-process lock.

        The returned tuple mirrors the input: ``True`` means a new JSONL line,
        while ``False`` means invalid shadow metadata or an existing key.
        """

        prepared: list[Optional[tuple[dict[str, Any], str]]] = [
            self._prepare(record) for record in records
        ]
        results = [False] * len(prepared)
        if not any(item is not None for item in prepared):
            return tuple(results)

        with self._exclusive():
            connection = self._connect()
            try:
                self._synchronize_index(connection)
                seen_in_batch: set[str] = set()
                for index, item in enumerate(prepared):
                    if item is None:
                        continue
                    payload, serialized = item
                    record_key = str(payload["record_key"]).strip()
                    if record_key in seen_in_batch:
                        continue
                    seen_in_batch.add(record_key)
                    exists = connection.execute(
                        "SELECT 1 FROM records WHERE record_key = ? LIMIT 1",
                        (record_key,),
                    ).fetchone()
                    if exists is not None:
                        continue
                    line = serialized + "\n"
                    self._rotate_if_needed(len(line.encode("utf-8")))
                    with open(
                        self.path,
                        "a",
                        encoding="utf-8",
                        newline="\n",
                    ) as handle:
                        handle.write(line)
                        handle.flush()
                        os.fsync(handle.fileno())
                    self._index_record(
                        connection,
                        payload,
                        payload_json=serialized,
                    )
                    results[index] = True
                self._replace_signatures(connection, self._file_signatures())
                connection.commit()
            finally:
                connection.close()
        return tuple(results)

    def latest_for_fixture(
        self,
        fixture_id: object,
        *,
        at_or_before_utc: object,
        max_age_seconds: float,
        record_type: str = SNAPSHOT_RECORD_TYPE,
    ) -> Optional[dict[str, Any]]:
        """Return the latest causal quote without loading history into RAM."""

        fixture = str(fixture_id).strip()
        if not fixture:
            raise ValueError("fixture_id must not be empty")
        at_epoch = _timestamp_epoch(at_or_before_utc)
        if at_epoch is None:
            raise ValueError("at_or_before_utc must be timezone-aware ISO-8601")
        age = float(max_age_seconds)
        if not age >= 0.0:
            raise ValueError("max_age_seconds must be non-negative")
        earliest = at_epoch - age

        with self._exclusive():
            connection = self._connect()
            try:
                self._synchronize_index(connection)
                row = connection.execute(
                    """
                    SELECT payload_json
                    FROM records
                    WHERE fixture_id = ?
                      AND record_type = ?
                      AND captured_at_epoch IS NOT NULL
                      AND captured_at_epoch <= ?
                      AND captured_at_epoch >= ?
                    ORDER BY captured_at_epoch DESC, record_key DESC
                    LIMIT 1
                    """,
                    (fixture, str(record_type), at_epoch, earliest),
                ).fetchone()
            finally:
                connection.close()
        if row is None:
            return None
        try:
            payload = json.loads(str(row["payload_json"]))
        except (TypeError, ValueError):  # pragma: no cover - derived DB corruption
            return None
        return payload if isinstance(payload, dict) else None

    def memory_stats(self) -> dict[str, Any]:
        """Expose that dedupe keys live on disk, plus current index size."""

        with self._exclusive():
            connection = self._connect()
            try:
                self._synchronize_index(connection)
                count = int(
                    connection.execute("SELECT COUNT(*) FROM records").fetchone()[0]
                )
            finally:
                connection.close()
        return {
            "dedupe_backend": "sqlite",
            "indexed_records": count,
            "in_memory_record_keys": 0,
        }
