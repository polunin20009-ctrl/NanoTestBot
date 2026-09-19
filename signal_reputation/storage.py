from __future__ import annotations

import gzip
import json
import os
import shutil
import tempfile
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Mapping


_shadow_lock = threading.RLock()
_shadow_keys: Dict[str, set[str]] = {}


def save_reputation_model(path: str, model: Mapping[str, Any]) -> None:
    parent = os.path.dirname(os.path.abspath(path)) or os.curdir
    os.makedirs(parent, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=".signal-reputation-", suffix=".json", dir=parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(dict(model), handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def load_reputation_model(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def _shadow_paths(path: str) -> list[str]:
    active = os.path.abspath(path)
    parent = os.path.dirname(active) or os.curdir
    filename = os.path.basename(active)
    stem, extension = os.path.splitext(filename)
    paths: list[str] = []
    try:
        for name in os.listdir(parent):
            if name == filename or (
                name.startswith(stem + ".")
                and (name.endswith(extension) or name.endswith(extension + ".gz"))
            ):
                paths.append(os.path.join(parent, name))
    except FileNotFoundError:
        return []
    return sorted(paths, key=lambda item: (item == active, item))


def iter_shadow_records(path: str) -> Iterator[Dict[str, Any]]:
    for candidate in _shadow_paths(path):
        opener = gzip.open if candidate.endswith(".gz") else open
        try:
            with opener(candidate, "rt", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        payload = json.loads(line)
                    except (ValueError, TypeError):
                        continue
                    if isinstance(payload, dict):
                        yield payload
        except OSError:
            continue


def _archive_path(path: str) -> str:
    parent = os.path.dirname(os.path.abspath(path)) or os.curdir
    filename = os.path.basename(path)
    stem, extension = os.path.splitext(filename)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return os.path.join(parent, f"{stem}.{stamp}{extension}.gz")


def _rotate(path: str, incoming_bytes: int, max_bytes: int) -> None:
    if max_bytes <= 0 or not os.path.exists(path):
        return
    if os.path.getsize(path) + incoming_bytes <= max_bytes:
        return
    archive = _archive_path(path)
    with open(path, "rb") as source, gzip.open(archive, "wb") as target:
        shutil.copyfileobj(source, target)
    os.unlink(path)


def _load_keys(path: str) -> set[str]:
    cached = _shadow_keys.get(os.path.abspath(path))
    if cached is not None:
        return cached
    keys: set[str] = set()
    parent = os.path.dirname(os.path.abspath(path)) or os.curdir
    filename = os.path.basename(path)
    stem, extension = os.path.splitext(filename)
    candidates: list[str] = []
    try:
        for name in os.listdir(parent):
            if name == filename or (
                name.startswith(stem + ".")
                and (name.endswith(extension) or name.endswith(extension + ".gz"))
            ):
                candidates.append(os.path.join(parent, name))
    except FileNotFoundError:
        pass
    for candidate in sorted(candidates):
        opener = gzip.open if candidate.endswith(".gz") else open
        try:
            with opener(candidate, "rt", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        payload = json.loads(line)
                    except (ValueError, TypeError):
                        continue
                    if isinstance(payload, dict) and payload.get("shadow_key"):
                        keys.add(str(payload["shadow_key"]))
        except OSError:
            continue
    _shadow_keys[os.path.abspath(path)] = keys
    return keys


def append_shadow_record(
    path: str,
    record: Mapping[str, Any],
    *,
    rotate_max_bytes: int = 10 * 1024 * 1024,
) -> bool:
    payload = dict(record)
    shadow_key = str(payload.get("shadow_key") or "").strip()
    if not shadow_key:
        return False
    line = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
    encoded_size = len(line.encode("utf-8"))
    parent = os.path.dirname(os.path.abspath(path)) or os.curdir
    os.makedirs(parent, exist_ok=True)
    with _shadow_lock:
        keys = _load_keys(path)
        if shadow_key in keys:
            return False
        _rotate(path, encoded_size, int(rotate_max_bytes))
        with open(path, "a", encoding="utf-8", newline="\n") as handle:
            handle.write(line)
            handle.flush()
        keys.add(shadow_key)
    return True
