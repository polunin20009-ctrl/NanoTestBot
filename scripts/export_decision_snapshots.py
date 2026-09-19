from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import NanoTest as nanotest


class ExportValidationError(ValueError):
    """Raised when a joined export cannot be produced without ambiguity."""


def _record_identifier(record: Mapping[str, Any], row_number: int) -> str:
    decision_id = str(record.get("decision_id") or "").strip()
    decision_key = str(record.get("decision_key") or "").strip()
    if decision_id and decision_key and decision_id != decision_key:
        raise ExportValidationError(
            f"row {row_number} has mismatched decision_id={decision_id!r} "
            f"and decision_key={decision_key!r}"
        )
    canonical_id = decision_id or decision_key
    if not canonical_id:
        raise ExportValidationError(
            f"row {row_number} has no non-empty decision_id or decision_key"
        )
    return canonical_id


def _prepare_export_records(
    records: Iterable[Mapping[str, Any]],
    *,
    resolved_only: bool = False,
    compact: bool = False,
) -> list[Dict[str, Any]]:
    """Validate joined rows and return normalized records ready for export."""
    normalized: list[Dict[str, Any]] = []
    seen: Dict[str, int] = {}
    for row_number, record in enumerate(records, 1):
        if not isinstance(record, Mapping):
            raise ExportValidationError(f"row {row_number} is not a JSON object")
        canonical_id = _record_identifier(record, row_number)
        previous_row = seen.get(canonical_id)
        if previous_row is not None:
            raise ExportValidationError(
                f"duplicate canonical decision ID {canonical_id!r} "
                f"in rows {previous_row} and {row_number}"
            )
        seen[canonical_id] = row_number

        item = dict(record)
        item["decision_id"] = canonical_id
        item["decision_key"] = canonical_id
        if compact:
            # New records retain their compact summary. Only the duplicated
            # full projection embedded by legacy snapshots is removed.
            item.pop("shadow_reputation", None)
        normalized.append(item)

    if resolved_only:
        normalized = [
            record
            for record in normalized
            if (record.get("outcome") or {}).get("status") == "resolved"
        ]
    return normalized


def _normalized_path(path: os.PathLike[str] | str) -> str:
    return os.path.normcase(os.path.realpath(os.path.abspath(os.fspath(path))))


def _assert_safe_output(input_path: os.PathLike[str] | str, output: Path) -> None:
    """Refuse to overwrite the active append-only journal or one of its archives."""
    protected_paths = {_normalized_path(input_path)}
    source_path_resolver = getattr(nanotest, "_decision_snapshot_paths", None)
    if callable(source_path_resolver):
        protected_paths.update(
            _normalized_path(path) for path in source_path_resolver(str(input_path))
        )
    normalized_output = _normalized_path(output)
    if normalized_output in protected_paths:
        raise ExportValidationError(
            f"output path {str(output)!r} is an input decision journal or archive"
        )


def _write_jsonl(records: Iterable[Dict[str, Any]], output: Path) -> int:
    """Atomically replace an export after its complete contents reach disk."""
    output.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output.name}.",
        suffix=".tmp",
        dir=str(output.parent),
    )
    temporary_path = Path(temporary_name)
    count = 0
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8", newline="\n") as handle:
            for record in records:
                handle.write(
                    json.dumps(record, ensure_ascii=False, separators=(",", ":"))
                    + "\n"
                )
                count += 1
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, output)
        return count
    except Exception:
        try:
            os.close(file_descriptor)
        except OSError:
            pass
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass
        raise


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Join decision and newest outcome records from decision snapshot JSONL archives."
    )
    parser.add_argument("--input", default=nanotest.DECISION_SNAPSHOTS_FILE)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--resolved-only", action="store_true")
    parser.add_argument(
        "--compact",
        action="store_true",
        help="remove the duplicated full legacy shadow_reputation projection",
    )
    args = parser.parse_args(argv)

    try:
        if args.output:
            _assert_safe_output(args.input, args.output)
        records = _prepare_export_records(
            nanotest.load_joined_decision_snapshots(
                args.input,
                strict_ids=True,
            ),
            resolved_only=args.resolved_only,
            compact=args.compact,
        )
    except (ExportValidationError, ValueError) as exc:
        parser.error(str(exc))

    statuses = Counter(str((record.get("outcome") or {}).get("status") or "missing") for record in records)
    decisions = Counter(str((record.get("decision") or {}).get("final_decision") or "missing") for record in records)
    print(json.dumps({
        "records": len(records),
        "fixtures": len({record.get("fixture_id") for record in records}),
        "outcome_statuses": dict(statuses),
        "decisions": dict(decisions),
        "compact": bool(args.compact),
    }, ensure_ascii=False, indent=2))
    if args.output:
        written = _write_jsonl(records, args.output)
        print(f"wrote {written} joined records to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
