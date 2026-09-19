from __future__ import annotations

import gzip
import json
import logging
import os
import re
import shutil
import tempfile
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from .parser import SECOND_HALF_HISTORY_SCHEMA_VERSION, compute_second_half_features_from_fixture


DEFAULT_HISTORY_PATH = os.environ.get("SECOND_HALF_HISTORY_PATH", os.path.join("data", "second_half_history.jsonl"))
HISTORY_ROTATE_MAX_BYTES = int(os.environ.get("SECOND_HALF_HISTORY_ROTATE_MAX_BYTES", str(10 * 1024 * 1024)))

LOGGER = logging.getLogger(__name__)
_history_lock = threading.RLock()
_history_id_cache: Dict[str, Set[int]] = {}


def _ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)


def _history_paths(path: str = DEFAULT_HISTORY_PATH) -> List[str]:
    active = os.path.abspath(path)
    parent = os.path.dirname(active) or os.curdir
    filename = os.path.basename(active)
    stem, extension = os.path.splitext(filename)
    archives: List[str] = []
    try:
        for candidate in os.listdir(parent):
            is_archive = candidate.endswith(extension) or candidate.endswith(extension + ".gz")
            is_backup = ".pre_migration." in candidate or ".pre_v2." in candidate
            if candidate != filename and candidate.startswith(stem + ".") and is_archive and not is_backup:
                archives.append(os.path.join(parent, candidate))
    except FileNotFoundError:
        pass
    archives.sort()
    if os.path.exists(active):
        archives.append(active)
    return archives


def iter_second_half_history_records(path: str = DEFAULT_HISTORY_PATH) -> Iterator[Dict[str, Any]]:
    for source_path in _history_paths(path):
        opener = gzip.open if source_path.endswith(".gz") else open
        try:
            with opener(source_path, "rt", encoding="utf-8") as handle:
                for line_number, raw_line in enumerate(handle, 1):
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except Exception:
                        LOGGER.warning(
                            "[2H_HISTORY_INVALID_LINE] file=%s line=%s", source_path, line_number
                        )
                        continue
                    if isinstance(payload, dict):
                        yield payload
        except OSError:
            LOGGER.exception("[2H_HISTORY_READ_ERROR] file=%s", source_path)


def reset_second_half_history_cache(path: Optional[str] = None) -> None:
    with _history_lock:
        if path is None:
            _history_id_cache.clear()
            return
        _history_id_cache.pop(os.path.abspath(path), None)


def load_second_half_history_records(path: str = DEFAULT_HISTORY_PATH) -> List[Dict[str, Any]]:
    """Load the latest revision of every valid fixture across active and archives."""
    latest: Dict[int, Dict[str, Any]] = {}
    order: List[int] = []
    with _history_lock:
        for payload in iter_second_half_history_records(path):
            try:
                fixture_id = int(payload.get("fixture_id"))
            except Exception:
                continue
            if fixture_id <= 0:
                continue
            if fixture_id not in latest:
                order.append(fixture_id)
            # Paths and lines are chronological revisions; the latest wins.
            latest[fixture_id] = payload
    return [latest[fixture_id] for fixture_id in order]


def load_second_half_fixture_ids(path: str = DEFAULT_HISTORY_PATH) -> Set[int]:
    file_path = os.path.abspath(path)
    with _history_lock:
        cached = _history_id_cache.get(file_path)
        if cached is not None:
            return set(cached)
        fixture_ids = {int(record["fixture_id"]) for record in load_second_half_history_records(path)}
        _history_id_cache[file_path] = set(fixture_ids)
        return fixture_ids


def _quality_rank(record: Dict[str, Any]) -> int:
    quality = str(record.get("events_quality") or "")
    return {"unavailable": 0, "incomplete": 1, "complete": 2}.get(quality, 0)


def _semantic_signature(record: Dict[str, Any]) -> str:
    payload = dict(record)
    payload.pop("collected_at_utc", None)
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _parser_version_key(record: Dict[str, Any]) -> Tuple[int, ...]:
    return tuple(int(value) for value in re.findall(r"\d+", str(record.get("parser_version") or "")))


def _rotate_history_if_needed(path: str, incoming_bytes: int) -> Optional[str]:
    limit = int(HISTORY_ROTATE_MAX_BYTES)
    if limit <= 0 or not os.path.exists(path):
        return None
    size = os.path.getsize(path)
    if size <= 0 or size + incoming_bytes <= limit:
        return None
    stem, extension = os.path.splitext(path)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    archive = f"{stem}.{stamp}{extension}.gz"
    counter = 1
    while os.path.exists(archive):
        archive = f"{stem}.{stamp}.{counter}{extension}.gz"
        counter += 1
    temporary = archive + ".tmp"
    try:
        with open(path, "rb") as source, gzip.open(temporary, "wb", compresslevel=6) as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)
        os.replace(temporary, archive)
        os.remove(path)
    except Exception:
        if os.path.exists(temporary):
            os.remove(temporary)
        raise
    LOGGER.info(
        "[2H_HISTORY_ROTATE] source=%s archive=%s bytes=%s compressed_bytes=%s",
        path, archive, size, os.path.getsize(archive),
    )
    return archive


def append_second_half_history_record(record: Dict[str, Any], path: str = DEFAULT_HISTORY_PATH) -> bool:
    """Append a fixture or a higher-quality/newer-schema correction revision."""
    if not isinstance(record, dict):
        return False
    try:
        fixture_id = int(record.get("fixture_id"))
    except Exception:
        return False
    if fixture_id <= 0:
        return False

    file_path = os.path.abspath(path)
    _ensure_parent_dir(file_path)
    with _history_lock:
        current = next(
            (item for item in load_second_half_history_records(file_path) if int(item.get("fixture_id", 0)) == fixture_id),
            None,
        )
        if current is not None:
            current_schema = int(current.get("schema_version") or 1)
            incoming_schema = int(record.get("schema_version") or 1)
            if _semantic_signature(current) == _semantic_signature(record):
                return False
            if incoming_schema < current_schema:
                return False
            if (
                incoming_schema == current_schema
                and _quality_rank(record) <= _quality_rank(current)
                and _parser_version_key(record) <= _parser_version_key(current)
            ):
                return False

        payload = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        _rotate_history_if_needed(file_path, len(payload.encode("utf-8")))
        with open(file_path, "a", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
        ids = load_second_half_fixture_ids(file_path)
        ids.add(fixture_id)
        _history_id_cache[file_path] = ids
        return True


def compact_second_half_history(path: str = DEFAULT_HISTORY_PATH) -> Dict[str, Any]:
    """Atomically rewrite the active file with one latest revision per fixture."""
    file_path = os.path.abspath(path)
    _ensure_parent_dir(file_path)
    records = load_second_half_history_records(file_path)
    fd, temporary = tempfile.mkstemp(prefix=".second_half_", suffix=".jsonl.tmp", dir=os.path.dirname(file_path) or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
            handle.flush()
        os.replace(temporary, file_path)
    finally:
        if os.path.exists(temporary):
            os.remove(temporary)
    reset_second_half_history_cache(file_path)
    return {"records": len(records), "path": file_path, "schema_version": SECOND_HALF_HISTORY_SCHEMA_VERSION}


def collect_and_store_second_half_history(
    fixture_payload: Any,
    events_payload: Optional[Any],
    path: str = DEFAULT_HISTORY_PATH,
    observed_finished_at_utc: Optional[str] = None,
) -> Tuple[bool, Dict[str, Any]]:
    record = compute_second_half_features_from_fixture(
        fixture_payload,
        events_payload,
        observed_finished_at_utc=observed_finished_at_utc,
    )
    stored = append_second_half_history_record(record, path=path)
    return stored, record
