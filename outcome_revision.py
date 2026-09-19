"""Helpers for append-only, revision-aware outcome records.

Legacy journals used only ``outcome_schema_version`` as their identity.  That
made a corrected result with the same schema indistinguishable from the first
result.  New records keep schema compatibility and add a monotonically
increasing revision for corrections.
"""

from __future__ import annotations

import json
from typing import Any, Mapping, Optional, Tuple


OUTCOME_STORE_VERSION_MULTIPLIER = 1_000_000_000


def _safe_non_negative_int(value: Any, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return int(default)
    return parsed if parsed >= 0 else int(default)


def outcome_schema_version(record: Optional[Mapping[str, Any]]) -> int:
    if not isinstance(record, Mapping):
        return 0
    return _safe_non_negative_int(record.get("outcome_schema_version"), 0)


def outcome_revision(record: Optional[Mapping[str, Any]]) -> int:
    """Return zero for legacy records and a positive revision for new ones."""

    if not isinstance(record, Mapping):
        return 0
    value = record.get("outcome_revision")
    if value is None:
        nested = record.get("outcome")
        if isinstance(nested, Mapping):
            value = nested.get("outcome_revision")
    return _safe_non_negative_int(value, 0)


def outcome_rank(record: Optional[Mapping[str, Any]]) -> Tuple[int, int, str]:
    """Canonical rank for selecting the newest append-only outcome."""

    if not isinstance(record, Mapping):
        return (-1, -1, "")
    return (
        outcome_schema_version(record),
        outcome_revision(record),
        str(record.get("created_at_utc") or ""),
    )


def semantic_outcome_payload(outcome: Any) -> Mapping[str, Any]:
    """Remove delivery metadata before deciding whether a label changed."""

    if not isinstance(outcome, Mapping):
        return {}
    return {
        str(key): value
        for key, value in outcome.items()
        if key not in {"resolved_at_utc", "outcome_revision"}
    }


def outcomes_semantically_equal(left: Any, right: Any) -> bool:
    return json.dumps(
        semantic_outcome_payload(left),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ) == json.dumps(
        semantic_outcome_payload(right),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def next_outcome_revision(
    previous_record: Optional[Mapping[str, Any]],
    proposed_outcome: Mapping[str, Any],
    *,
    schema_version: int,
) -> Optional[int]:
    """Return the revision to append, or ``None`` when nothing changed."""

    previous = previous_record if isinstance(previous_record, Mapping) else {}
    previous_outcome = previous.get("outcome")
    previous_outcome = previous_outcome if isinstance(previous_outcome, Mapping) else {}
    previous_status = str(previous_outcome.get("status") or "").lower()
    has_terminal_previous = previous_status in {"resolved", "void", "quarantine"}
    previous_schema = outcome_schema_version(previous)
    next_schema = max(1, int(schema_version))

    if (
        has_terminal_previous
        and previous_schema == next_schema
        and outcomes_semantically_equal(previous_outcome, proposed_outcome)
    ):
        return None
    if has_terminal_previous and previous_schema == next_schema:
        return outcome_revision(previous) + 1
    return 1


def outcome_store_version(record: Mapping[str, Any]) -> int:
    """Map ``schema + revision`` onto the integer version used by SQLite.

    Legacy records keep their historical small version.  Revision-aware
    records use a disjoint large range while preserving schema/revision order.
    """

    schema = max(1, outcome_schema_version(record))
    revision = outcome_revision(record)
    if revision <= 0:
        return schema
    if revision >= OUTCOME_STORE_VERSION_MULTIPLIER:
        raise ValueError("outcome revision exceeds supported range")
    return schema * OUTCOME_STORE_VERSION_MULTIPLIER + revision
