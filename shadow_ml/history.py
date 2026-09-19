"""Memory-bounded preparation of joined shadow-ML training rows.

The observation journal is intentionally append-only and can be much larger
than the actual ML cohort.  Keeping every decoded JSON object in dictionaries
made the isolated trainer consume several gigabytes.  This module preserves
the journal's latest-schema/latest-outcome semantics while spilling the join
index to a temporary SQLite database.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from outcome_revision import outcome_revision as get_outcome_revision


_OBSERVATION_UPSERT = """
INSERT INTO observations (
    observation_id,
    schema_version,
    created_at_utc,
    sort_observation_id,
    training_shape,
    payload_json
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(observation_id) DO UPDATE SET
    schema_version = excluded.schema_version,
    created_at_utc = excluded.created_at_utc,
    sort_observation_id = excluded.sort_observation_id,
    training_shape = excluded.training_shape,
    payload_json = excluded.payload_json
WHERE excluded.schema_version > observations.schema_version
   OR (
       excluded.schema_version = observations.schema_version
       AND excluded.created_at_utc >= observations.created_at_utc
   )
"""

_OUTCOME_UPSERT = """
INSERT INTO outcomes (
    observation_id,
    schema_version,
    outcome_revision,
    created_at_utc,
    payload_json
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(observation_id) DO UPDATE SET
    schema_version = excluded.schema_version,
    outcome_revision = excluded.outcome_revision,
    created_at_utc = excluded.created_at_utc,
    payload_json = excluded.payload_json
WHERE excluded.schema_version > outcomes.schema_version
   OR (
       excluded.schema_version = outcomes.schema_version
       AND (
           excluded.outcome_revision > outcomes.outcome_revision
           OR (
               excluded.outcome_revision = outcomes.outcome_revision
               AND excluded.created_at_utc >= outcomes.created_at_utc
           )
       )
   )
"""


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return int(default)
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return int(default)


def _observation_id(record: Mapping[str, Any]) -> str:
    return str(
        record.get("observation_key") or record.get("observation_id") or ""
    )


def _has_training_shape(record: Mapping[str, Any]) -> bool:
    """Cheap pre-outcome allow-list matching all structural ML requirements.

    Timestamp and outcome quality remain validated by the model itself.  The
    filter is deliberately conservative: it may retain a malformed candidate,
    but it must never discard a row that the trainer could accept.
    """

    if str(record.get("stage") or "") != "decision_pipeline":
        return False
    if _safe_int(record.get("fixture_id"), -1) <= 0:
        return False
    minute = _safe_int(record.get("minute"), -1)
    return 46 <= minute <= 60


def _json_payload(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _configure_spool(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("PRAGMA temp_store=FILE")
    connection.execute("PRAGMA mmap_size=0")
    connection.execute("PRAGMA cache_size=-32768")
    connection.executescript(
        """
        CREATE TABLE observations (
            observation_id TEXT PRIMARY KEY,
            schema_version INTEGER NOT NULL,
            created_at_utc TEXT NOT NULL,
            sort_observation_id TEXT NOT NULL,
            training_shape INTEGER NOT NULL,
            payload_json TEXT
        ) WITHOUT ROWID;
        CREATE TABLE outcomes (
            observation_id TEXT PRIMARY KEY,
            schema_version INTEGER NOT NULL,
            outcome_revision INTEGER NOT NULL,
            created_at_utc TEXT NOT NULL,
            payload_json TEXT NOT NULL
        ) WITHOUT ROWID;
        """
    )


def materialize_training_history(
    records: Iterable[Mapping[str, Any]],
    *,
    include_pending: bool = False,
    spool_parent: Optional[os.PathLike[str] | str] = None,
    commit_every: int = 2000,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Join an append-only journal without retaining the raw journal in RAM.

    Only structurally possible 46--60 minute decision-pipeline observations
    keep their JSON payload.  Rank rows for every observation and outcome are
    stored on disk so that a newer incompatible revision correctly supersedes
    an older compatible one, exactly like the legacy in-memory joiner.
    """

    batch_size = max(1, int(commit_every))
    stats = {
        "records_scanned": 0,
        "observation_records": 0,
        "outcome_records": 0,
        "candidate_payloads_seen": 0,
        "joined_training_rows": 0,
    }
    parent = os.fspath(spool_parent) if spool_parent is not None else None
    with tempfile.TemporaryDirectory(
        prefix="goalbot-shadow-ml-spool-",
        dir=parent,
    ) as temporary:
        database_path = os.path.join(temporary, "history.sqlite3")
        connection = sqlite3.connect(database_path)
        try:
            _configure_spool(connection)
            pending_writes = 0
            for raw in records:
                stats["records_scanned"] += 1
                if not isinstance(raw, Mapping):
                    continue
                observation_id = _observation_id(raw)
                if not observation_id:
                    continue
                record_type = str(raw.get("record_type") or "")
                created_at = str(raw.get("created_at_utc") or "")
                if record_type == "observation":
                    stats["observation_records"] += 1
                    training_shape = _has_training_shape(raw)
                    if training_shape:
                        stats["candidate_payloads_seen"] += 1
                    connection.execute(
                        _OBSERVATION_UPSERT,
                        (
                            observation_id,
                            _safe_int(raw.get("schema_version"), 1),
                            created_at,
                            str(raw.get("observation_id") or ""),
                            int(training_shape),
                            _json_payload(raw) if training_shape else None,
                        ),
                    )
                    pending_writes += 1
                elif record_type == "observation_outcome":
                    stats["outcome_records"] += 1
                    outcome = raw.get("outcome")
                    connection.execute(
                        _OUTCOME_UPSERT,
                        (
                            observation_id,
                            _safe_int(raw.get("outcome_schema_version"), 1),
                            get_outcome_revision(raw),
                            created_at,
                            _json_payload(outcome if isinstance(outcome, Mapping) else {}),
                        ),
                    )
                    pending_writes += 1
                if pending_writes >= batch_size:
                    connection.commit()
                    pending_writes = 0
            connection.commit()

            joined: List[Dict[str, Any]] = []
            cursor = connection.execute(
                """
                SELECT
                    observations.observation_id,
                    observations.payload_json,
                    outcomes.schema_version,
                    outcomes.outcome_revision,
                    outcomes.created_at_utc,
                    outcomes.payload_json
                FROM observations
                LEFT JOIN outcomes USING (observation_id)
                WHERE observations.training_shape = 1
                  AND observations.payload_json IS NOT NULL
                ORDER BY
                    observations.created_at_utc,
                    observations.sort_observation_id
                """
            )
            for (
                _observation_key,
                observation_json,
                outcome_schema,
                outcome_revision,
                outcome_created_at,
                outcome_json,
            ) in cursor:
                try:
                    item = json.loads(observation_json)
                except (TypeError, ValueError):
                    continue
                if not isinstance(item, dict):
                    continue
                if outcome_json is not None:
                    try:
                        outcome = json.loads(outcome_json)
                    except (TypeError, ValueError):
                        outcome = {}
                    item["outcome"] = outcome if isinstance(outcome, dict) else {}
                    item["outcome_schema_version"] = _safe_int(outcome_schema, 1)
                    parsed_revision = _safe_int(outcome_revision, 0)
                    if parsed_revision > 0:
                        item["outcome_revision"] = parsed_revision
                    item["outcome_record_created_at_utc"] = outcome_created_at
                status = str(
                    (item.get("outcome") or {}).get("status")
                    if isinstance(item.get("outcome"), Mapping)
                    else ""
                )
                if include_pending or status != "pending":
                    joined.append(item)
            stats["joined_training_rows"] = len(joined)
            return joined, stats
        finally:
            connection.close()
