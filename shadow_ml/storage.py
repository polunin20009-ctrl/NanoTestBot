from __future__ import annotations

import gzip
import json
import logging
import os
import shutil
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping, Optional, Union


PathLike = Union[str, os.PathLike[str]]

logger = logging.getLogger(__name__)

_model_lock = threading.RLock()
_prediction_lock = threading.RLock()
_prediction_keys_by_file: Dict[str, set[str]] = {}


def _absolute_path(path: PathLike) -> str:
    return os.path.abspath(os.fspath(path))


def save_model(path: PathLike, model: Mapping[str, Any]) -> None:
    """Atomically persist a shadow model as a JSON object."""
    if not isinstance(model, Mapping):
        raise TypeError("model must be a mapping")

    target = _absolute_path(path)
    parent = os.path.dirname(target) or os.curdir
    os.makedirs(parent, exist_ok=True)

    with _model_lock:
        descriptor, temporary = tempfile.mkstemp(
            prefix=f".{Path(target).stem}.",
            suffix=".tmp",
            dir=parent,
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
                json.dump(
                    dict(model),
                    handle,
                    allow_nan=False,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            with open(temporary, "r", encoding="utf-8") as verification:
                verified = json.load(verification)
            if not isinstance(verified, dict):
                raise ValueError("serialized model is not a JSON object")
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


def load_model(path: PathLike) -> Dict[str, Any]:
    """Load a model JSON object, returning an empty mapping when unavailable."""
    target = _absolute_path(path)
    with _model_lock:
        try:
            with open(target, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, ValueError, TypeError):
            return {}
    return payload if isinstance(payload, dict) else {}


def _is_timestamp_archive_name(
    candidate: str,
    *,
    stem: str,
    extension: str,
) -> bool:
    prefix = stem + "."
    if not candidate.startswith(prefix):
        return False

    if candidate.endswith(extension + ".gz"):
        suffix = extension + ".gz"
    elif candidate.endswith(extension):
        suffix = extension
    else:
        return False

    timestamp = candidate[len(prefix):-len(suffix)]
    if not timestamp:
        return False
    for pattern in ("%Y%m%dT%H%M%S%fZ", "%Y%m%dT%H%M%SZ"):
        try:
            datetime.strptime(timestamp, pattern)
            return True
        except ValueError:
            continue
    return False


def _prediction_paths(path: PathLike) -> list[str]:
    """Return timestamp archives oldest-first, followed by the active JSONL."""
    active = _absolute_path(path)
    parent = os.path.dirname(active) or os.curdir
    filename = os.path.basename(active)
    stem, extension = os.path.splitext(filename)
    archives: list[str] = []
    try:
        for candidate in os.listdir(parent):
            if candidate == filename:
                continue
            if _is_timestamp_archive_name(
                candidate,
                stem=stem,
                extension=extension,
            ):
                archives.append(os.path.join(parent, candidate))
    except FileNotFoundError:
        return []

    archives.sort()
    if os.path.exists(active):
        archives.append(active)
    return archives


def iter_prediction_records(path: PathLike) -> Iterator[Dict[str, Any]]:
    """Yield valid prediction objects across legacy, gzip, and active files."""
    for candidate in _prediction_paths(path):
        opener = gzip.open if candidate.endswith(".gz") else open
        try:
            with opener(candidate, "rt", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, 1):
                    try:
                        payload = json.loads(line)
                    except (ValueError, TypeError):
                        logger.warning(
                            "[SHADOW_ML_PREDICTION_INVALID_LINE] file=%s line=%s",
                            candidate,
                            line_number,
                        )
                        continue
                    if isinstance(payload, dict):
                        yield payload
        except OSError:
            logger.exception(
                "[SHADOW_ML_PREDICTION_READ_ERROR] file=%s",
                candidate,
            )


def _load_prediction_keys(path: PathLike) -> set[str]:
    active = _absolute_path(path)
    cached = _prediction_keys_by_file.get(active)
    if cached is not None:
        return cached

    keys: set[str] = set()
    for record in iter_prediction_records(active):
        prediction_key = str(record.get("prediction_key") or "").strip()
        if prediction_key:
            keys.add(prediction_key)
    _prediction_keys_by_file[active] = keys
    return keys


def _prediction_archive_path(path: PathLike) -> str:
    active = _absolute_path(path)
    parent = os.path.dirname(active) or os.curdir
    filename = os.path.basename(active)
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
        if not os.path.exists(candidate) and not os.path.exists(candidate + ".tmp"):
            return candidate
    raise FileExistsError("could not allocate a unique prediction archive name")


def _rotate_predictions_if_needed(
    path: PathLike,
    incoming_bytes: int,
    max_bytes: int,
) -> Optional[str]:
    active = _absolute_path(path)
    if max_bytes <= 0 or not os.path.exists(active):
        return None
    if os.path.getsize(active) + max(0, int(incoming_bytes)) <= max_bytes:
        return None

    archive = _prediction_archive_path(active)
    temporary_archive = archive + ".tmp"
    try:
        with open(active, "rb") as source:
            with open(temporary_archive, "wb") as raw_target:
                with gzip.GzipFile(
                    filename="",
                    mode="wb",
                    compresslevel=6,
                    fileobj=raw_target,
                    mtime=0,
                ) as compressed_target:
                    shutil.copyfileobj(
                        source,
                        compressed_target,
                        length=1024 * 1024,
                    )
                raw_target.flush()
                os.fsync(raw_target.fileno())
        os.replace(temporary_archive, archive)
        os.unlink(active)
    finally:
        if os.path.exists(temporary_archive):
            os.unlink(temporary_archive)
    return archive


def append_prediction_record(
    path: PathLike,
    record: Mapping[str, Any],
    *,
    max_bytes: int = 50 * 1024 * 1024,
    rotate_max_bytes: Optional[int] = None,
) -> bool:
    """Append one prediction exactly once per ``prediction_key``."""
    if not isinstance(record, Mapping):
        return False
    payload = dict(record)
    prediction_key = str(payload.get("prediction_key") or "").strip()
    if not prediction_key:
        return False

    line = (
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + "\n"
    )
    incoming_bytes = len(line.encode("utf-8"))
    active = _absolute_path(path)
    parent = os.path.dirname(active) or os.curdir

    with _prediction_lock:
        os.makedirs(parent, exist_ok=True)
        keys = _load_prediction_keys(active)
        if prediction_key in keys:
            return False

        _rotate_predictions_if_needed(
            active,
            incoming_bytes,
            int(
                max_bytes
                if rotate_max_bytes is None
                else rotate_max_bytes
            ),
        )
        with open(active, "a", encoding="utf-8", newline="\n") as handle:
            handle.write(line)
            handle.flush()
        keys.add(prediction_key)
    return True


def reset_prediction_cache(path: Optional[PathLike] = None) -> None:
    """Reset in-memory deduplication state, primarily for tests and reloads."""
    with _prediction_lock:
        if path is None:
            _prediction_keys_by_file.clear()
        else:
            _prediction_keys_by_file.pop(_absolute_path(path), None)
