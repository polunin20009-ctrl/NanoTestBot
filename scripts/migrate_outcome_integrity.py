from __future__ import annotations

import argparse
import gzip
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from outcome_integrity import resolve_normal_time_outcome


def _integer(payload: dict[str, Any], name: str) -> int | None:
    value = payload.get(name)
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _latest_records(path: Path) -> tuple[list[dict[str, Any]], int]:
    latest: dict[str, dict[str, Any]] = {}
    invalid = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                payload = json.loads(line)
            except (TypeError, ValueError):
                invalid += 1
                continue
            if not isinstance(payload, dict):
                invalid += 1
                continue
            key = str(payload.get("signal_id") or "").strip()
            if key:
                latest[key] = payload
    return list(latest.values()), invalid


def _correction(payload: dict[str, Any]) -> dict[str, Any] | None:
    signal_home = _integer(payload, "signal_score_home")
    signal_away = _integer(payload, "signal_score_away")
    final_home = _integer(payload, "normal_time_final_score_home")
    final_away = _integer(payload, "normal_time_final_score_away")
    if None in {signal_home, signal_away, final_home, final_away}:
        return None
    integrity = resolve_normal_time_outcome(
        (signal_home, signal_away),
        (final_home, final_away),
        normal_time_event_count=0,
        previously_confirmed_win=(
            str(payload.get("normal_time_result") or "").upper() == "WIN"
            or payload.get("goal_to90_normal_time") is True
        ),
    )
    recorded_loss = (
        str(payload.get("normal_time_result") or "").upper() == "LOSS"
        or payload.get("goal_to90_normal_time") is False
        or payload.get("goal_after_signal_normal_time") is False
    )
    if not recorded_loss or integrity.goal_to90_normal_time is not True:
        return None

    corrected = dict(payload)
    corrected.update(
        {
            "goal_after_signal": 1,
            "goal_after_signal_normal_time": True,
            "goal_to90_normal_time": True,
            "goal_by_90": 1,
            "normal_time_result": "WIN",
            "goal_result_source": "score_delta",
            "normal_time_goal_count_after_signal": int(
                integrity.normal_time_goal_count_after_signal or 1
            ),
            "outcome_integrity_migrated": True,
            "outcome_integrity_previous_result": payload.get(
                "normal_time_result"
            ),
            "outcome_integrity_migrated_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
        }
    )
    # A score delta proves a goal, not its minute.
    if not payload.get("first_goal_after_signal_minute"):
        corrected["first_goal_after_signal_minute"] = None
    return corrected


def migrate(path: Path, *, dry_run: bool = False) -> dict[str, Any]:
    records, invalid = _latest_records(path)
    corrections = [
        corrected
        for payload in records
        if (corrected := _correction(payload)) is not None
    ]
    summary: dict[str, Any] = {
        "path": str(path),
        "checked": len(records),
        "corrected": len(corrections),
        "suspicious": invalid,
        "fixture_ids": sorted(
            {int(item["fixture_id"]) for item in corrections}
        ),
        "backup": None,
        "dry_run": bool(dry_run),
    }
    if dry_run or not corrections:
        return summary

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    backup = path.with_name(f"{path.stem}.pre_outcome_integrity.{stamp}.jsonl.gz")
    with path.open("rb") as source, backup.open("wb") as raw_target:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            compresslevel=6,
            fileobj=raw_target,
            mtime=0,
        ) as target:
            shutil.copyfileobj(source, target)

    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND)
    try:
        for payload in corrections:
            line = (
                json.dumps(payload, ensure_ascii=False, allow_nan=False)
                + "\n"
            ).encode("utf-8")
            view = memoryview(line)
            while view:
                written = os.write(descriptor, view)
                view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    summary["backup"] = str(backup)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        type=Path,
        default=ROOT_DIR / "test_zzz.json" / "test_match_outcomes.jsonl",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(
        json.dumps(
            migrate(args.path.resolve(), dry_run=args.dry_run),
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
