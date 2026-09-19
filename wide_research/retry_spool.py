"""Bounded, fsynced fallback for research writes while SQLite is unavailable.

Only this module owns these files. Files are removed after their idempotent
SQLite operation commits; a crash between commit and removal repeats safely.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import stat
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping


class RetrySpoolError(RuntimeError):
    pass


class RetrySpool:
    def __init__(self, path: str, *, max_bytes: int = 256 * 1024 * 1024,
                 max_records: int = 20_000, max_record_bytes: int = 2 * 1024 * 1024):
        self.path = os.path.abspath(path)
        self.max_bytes = max(1, int(max_bytes))
        self.max_records = max(1, int(max_records))
        self.max_record_bytes = max(1, int(max_record_bytes))
        self._lock = threading.RLock()
        self._lock_depth = 0
        self._lock_fd: int | None = None
        Path(self.path).mkdir(mode=0o700, parents=True, exist_ok=True)
        if os.path.realpath(self.path) != self.path or not stat.S_ISDIR(os.lstat(self.path).st_mode):
            raise RetrySpoolError("retry spool must be a real directory")
        os.chmod(self.path, 0o700, follow_symlinks=False)
        # A process can die after mkstemp() but before linking the completed
        # record into its durable name.  Such private temporaries were never
        # accepted by enqueue(), and retaining them would bypass both spool
        # capacity counters.  Serialize cleanup with live writers so a second
        # layer/process can safely reopen the same spool.
        with self.locked():
            self._cleanup_temporaries_unlocked()

    @contextlib.contextmanager
    def locked(self) -> Iterator[None]:
        # Covers intake plus replay, also serializing two processes/layers for
        # the same database. The lock file never contains observations.
        with self._lock:
            outermost = self._lock_depth == 0
            if outermost:
                fd = os.open(os.path.join(self.path, ".lock"),
                             os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
                try:
                    if not stat.S_ISREG(os.fstat(fd).st_mode):
                        raise RetrySpoolError(
                            "retry spool lock must be a regular file"
                        )
                    os.fchmod(fd, 0o600)
                    fcntl.flock(fd, fcntl.LOCK_EX)
                except Exception:
                    os.close(fd)
                    raise
                self._lock_fd = fd
            self._lock_depth += 1
            try:
                yield
            finally:
                self._lock_depth -= 1
                if outermost:
                    fd = self._lock_fd
                    self._lock_fd = None
                    if fd is not None:
                        fcntl.flock(fd, fcntl.LOCK_UN)
                        os.close(fd)

    def _sync_directory(self) -> None:
        fd = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _entries(self, kind: str | None = None):
        with os.scandir(self.path) as entries:
            for entry in entries:
                if not entry.name.endswith(".json") or (kind and not entry.name.startswith(kind + "-")):
                    continue
                try:
                    metadata = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    # Be defensive against an older/non-cooperating process
                    # acknowledging a file while health status is scanning.
                    continue
                if not stat.S_ISREG(metadata.st_mode):
                    raise RetrySpoolError("retry spool entry is not a regular file")
                yield entry.name, metadata

    def _cleanup_temporaries_unlocked(self) -> None:
        removed = False
        with os.scandir(self.path) as entries:
            for entry in entries:
                if not entry.name.startswith(".pending-"):
                    continue
                try:
                    metadata = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if not stat.S_ISREG(metadata.st_mode):
                    raise RetrySpoolError(
                        "retry spool temporary is not a regular file"
                    )
                try:
                    os.unlink(os.path.join(self.path, entry.name))
                except FileNotFoundError:
                    continue
                removed = True
        if removed:
            self._sync_directory()

    def _status_unlocked(self) -> dict[str, Any]:
        count = total = 0
        snapshots = outcomes = 0
        oldest = None
        for name, metadata in self._entries():
            count += 1
            total += metadata.st_size
            snapshots += int(name.startswith("snapshot-"))
            outcomes += int(name.startswith("outcomes-"))
            oldest = metadata.st_mtime if oldest is None else min(oldest, metadata.st_mtime)
        return {"pending": count, "pending_snapshots": snapshots,
                "pending_outcomes": outcomes, "bytes": total,
                "capacity_records": self.max_records, "capacity_bytes": self.max_bytes,
                "oldest_at_utc": datetime.fromtimestamp(oldest, timezone.utc).isoformat()
                if oldest is not None else None}

    def status(self) -> dict[str, Any]:
        with self.locked():
            return self._status_unlocked()

    def _read_encoded_unlocked(self, name: str) -> bytes:
        if os.path.basename(name) != name or not name.endswith(".json"):
            raise RetrySpoolError("invalid retry spool filename")
        fd = os.open(os.path.join(self.path, name), os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as handle:
            encoded = handle.read(self.max_record_bytes + 1)
        if len(encoded) > self.max_record_bytes:
            raise RetrySpoolError("oversized retry spool entry")
        return encoded

    def _encode_event(
        self, kind: str, identity: str, payload: Mapping[str, Any]
    ) -> tuple[str, bytes]:
        if kind not in {"snapshot", "outcomes"} or not str(identity).strip():
            raise ValueError("invalid retry event identity")
        name = kind + "-" + hashlib.sha256(str(identity).encode()).hexdigest() + ".json"
        frozen_payload = dict(payload)
        encoded = json.dumps(frozen_payload, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":"), allow_nan=False).encode()
        if len(encoded) > self.max_record_bytes:
            raise RetrySpoolError("retry event exceeds record size limit")
        return name, encoded

    def _publish_unlocked(self, name: str, encoded: bytes) -> None:
        target = os.path.join(self.path, name)
        fd, temporary = tempfile.mkstemp(prefix=".pending-", dir=self.path)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, target, follow_symlinks=False)
            except FileExistsError:
                # The file lock protects cooperating writers.  Preserve
                # immutability even if an older writer did not take it.
                if self._read_encoded_unlocked(name) != encoded:
                    raise RetrySpoolError(
                        "retry event identity reused with different evidence"
                    )
            self._sync_directory()
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            else:
                self._sync_directory()

    def enqueue_many(
        self, events: Iterable[tuple[str, str, Mapping[str, Any]]]
    ) -> list[str]:
        """Accept one logical intake without a mid-batch capacity failure.

        Every payload and identity is validated before the lock is taken.  The
        capacity check then accounts for all not-yet-present records before
        publishing the first one.  A process crash can still interrupt any
        filesystem operation, but replaying the same immutable intake safely
        completes it.
        """

        encoded_events: list[tuple[str, bytes]] = []
        by_name: dict[str, bytes] = {}
        for kind, identity, payload in events:
            name, encoded = self._encode_event(kind, identity, payload)
            previous = by_name.get(name)
            if previous is not None and previous != encoded:
                raise RetrySpoolError(
                    "retry event identity reused with different evidence"
                )
            if previous is None:
                by_name[name] = encoded
                encoded_events.append((name, encoded))
        with self.locked():
            missing: list[tuple[str, bytes]] = []
            for name, encoded in encoded_events:
                try:
                    existing = self._read_encoded_unlocked(name)
                except FileNotFoundError:
                    missing.append((name, encoded))
                    continue
                if existing != encoded:
                    raise RetrySpoolError(
                        "retry event identity reused with different evidence"
                    )
            status = self._status_unlocked()
            if (
                status["pending"] + len(missing) > self.max_records
                or status["bytes"] + sum(len(encoded) for _, encoded in missing)
                > self.max_bytes
            ):
                raise RetrySpoolError(
                    "research retry spool is full; event was not accepted"
                )
            for name, encoded in missing:
                self._publish_unlocked(name, encoded)
            return [name for name, _ in encoded_events]

    def enqueue(self, kind: str, identity: str, payload: Mapping[str, Any]) -> str:
        return self.enqueue_many([(kind, identity, payload)])[0]

    def pending(self, kind: str, *, limit: int = 32) -> list[str]:
        # Ingestion order is immaterial: all snapshot files must reach the
        # SQLite inbox before its observation-time ordered evaluation starts.
        bounded = max(1, min(256, int(limit)))
        with self.locked():
            return sorted(name for name, _ in self._entries(kind))[:bounded]

    def read(self, name: str) -> dict[str, Any]:
        with self.locked():
            encoded = self._read_encoded_unlocked(name)
        value = json.loads(encoded)
        if not isinstance(value, dict):
            raise RetrySpoolError("invalid retry spool payload")
        return value

    def acknowledge(self, name: str) -> bool:
        if os.path.basename(name) != name or not name.endswith(".json"):
            raise RetrySpoolError("invalid retry spool filename")
        with self.locked():
            try:
                os.unlink(os.path.join(self.path, name))
            except FileNotFoundError:
                return False
            self._sync_directory()
            return True
