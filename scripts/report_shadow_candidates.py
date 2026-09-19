#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shadow_candidates.storage import iter_jsonl_records, jsonl_paths
from outcome_revision import outcome_revision as get_outcome_revision


def _schema_number(value: Any) -> int:
    if isinstance(value, bool):
        return -1
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def _outcome_rank(record: Mapping[str, Any]) -> tuple[int, int, str]:
    return (
        _schema_number(record.get("outcome_schema_version")),
        get_outcome_revision(record),
        str(record.get("created_at_utc") or ""),
    )


def load_latest_observation_outcomes(
    path: str,
    *,
    observation_ids: Optional[set[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Stream only requested source outcomes, never the complete label corpus."""
    latest: Dict[str, Dict[str, Any]] = {}
    for record in iter_jsonl_records(path):
        if record.get("record_type") != "observation_outcome":
            continue
        observation_id = str(record.get("observation_id") or "")
        if not observation_id:
            continue
        if observation_ids is not None and observation_id not in observation_ids:
            continue
        previous = latest.get(observation_id)
        if previous is None or _outcome_rank(record) > _outcome_rank(previous):
            latest[observation_id] = dict(record)
    return latest


def _candidate_outcome_rank(record: Mapping[str, Any]) -> tuple[int, int, str]:
    return (
        _schema_number(record.get("source_outcome_schema_version")),
        max(0, _schema_number(record.get("source_outcome_revision"))),
        str(record.get("source_outcome_created_at_utc") or ""),
    )


def load_latest_candidate_outcomes(
    journal_path: str,
    selected_keys: set[tuple[str, str, str]],
) -> Dict[tuple[str, str, str], Dict[str, Any]]:
    """Stream journal outcomes for selected ruleset+observation+arm keys only."""
    latest: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    for record in iter_jsonl_records(journal_path):
        if record.get("record_type") != "shadow_candidate_outcome":
            continue
        key = (
            str(record.get("ruleset_version") or ""),
            str(record.get("observation_id") or ""),
            str(record.get("arm_id") or ""),
        )
        if key not in selected_keys:
            continue
        previous = latest.get(key)
        if previous is None or _candidate_outcome_rank(record) > _candidate_outcome_rank(
            previous
        ):
            latest[key] = dict(record)
    return latest


def load_first_control_cohorts(
    journal_path: str,
    *,
    ruleset_version: Optional[str] = None,
) -> Dict[tuple[str, str], Dict[str, Any]]:
    cohorts: Dict[tuple[str, str], Dict[str, Any]] = {}
    for record in iter_jsonl_records(journal_path):
        if record.get("record_type") != "shadow_candidate_decision":
            continue
        ruleset = str(record.get("ruleset_version") or "")
        if ruleset_version and ruleset != ruleset_version:
            continue
        cohort = record.get("cohort")
        cohort_map = cohort if isinstance(cohort, Mapping) else {}
        if cohort_map.get("cohort_claimed") is not True:
            continue
        fixture_id = str(record.get("fixture_id") or "")
        key = (ruleset, fixture_id)
        if not ruleset or not fixture_id:
            continue
        previous = cohorts.get(key)
        if previous is None or str(record.get("created_at_utc") or "") < str(
            previous.get("created_at_utc") or ""
        ):
            cohorts[key] = dict(record)
    return cohorts


def wilson_interval(wins: int, losses: int, z: float = 1.959963984540054) -> Dict[str, Any]:
    n = int(wins) + int(losses)
    if n <= 0:
        return {"sample_size": 0, "rate_pct": None, "low_pct": None, "high_pct": None}
    proportion = wins / n
    denominator = 1.0 + z * z / n
    center = (proportion + z * z / (2.0 * n)) / denominator
    margin = (
        z
        * math.sqrt(
            proportion * (1.0 - proportion) / n + z * z / (4.0 * n * n)
        )
        / denominator
    )
    return {
        "sample_size": n,
        "rate_pct": round(proportion * 100.0, 6),
        "low_pct": round(max(0.0, center - margin) * 100.0, 6),
        "high_pct": round(min(1.0, center + margin) * 100.0, 6),
    }


def build_report(
    journal_path: str,
    observation_path: str,
    *,
    ruleset_version: Optional[str] = None,
) -> Dict[str, Any]:
    cohorts = load_first_control_cohorts(
        journal_path, ruleset_version=ruleset_version
    )
    selected_keys: set[tuple[str, str, str]] = set()
    for (ruleset, _fixture_id), record in cohorts.items():
        observation_id = str(record.get("observation_id") or "")
        arms = record.get("arms")
        if not isinstance(arms, Mapping):
            continue
        for arm_id, evaluation in arms.items():
            if (
                isinstance(evaluation, Mapping)
                and evaluation.get("candidate_decision") == "ALLOW"
            ):
                selected_keys.add((ruleset, observation_id, str(arm_id)))
    candidate_outcomes = load_latest_candidate_outcomes(
        journal_path, selected_keys
    )
    selected_observation_ids = {
        observation_id for _ruleset, observation_id, _arm_id in selected_keys
    }
    outcomes = (
        load_latest_observation_outcomes(
            observation_path,
            observation_ids=selected_observation_ids,
        )
        if selected_observation_ids
        else {}
    )
    grouped: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    cohort_counts: Dict[str, int] = defaultdict(int)

    for (ruleset, _fixture_id), record in cohorts.items():
        cohort_counts[ruleset] += 1
        observation_id = str(record.get("observation_id") or "")
        arms = record.get("arms")
        if not isinstance(arms, Mapping):
            continue
        for arm_id, evaluation in arms.items():
            if not isinstance(evaluation, Mapping):
                continue
            stats = grouped[ruleset].setdefault(
                str(arm_id),
                {
                    "arm_id": str(arm_id),
                    "arm_version": evaluation.get("arm_version"),
                    "first_control_cohort": 0,
                    "selected": 0,
                    "rejected_failed": 0,
                    "rejected_unavailable": 0,
                    "wins": 0,
                    "losses": 0,
                    "pending": 0,
                    "void": 0,
                    "invalid_outcome": 0,
                },
            )
            stats["first_control_cohort"] += 1
            candidate_decision = evaluation.get("candidate_decision")
            if candidate_decision == "ALLOW":
                stats["selected"] += 1
            elif candidate_decision == "REJECT_UNAVAILABLE":
                stats["rejected_unavailable"] += 1
                continue
            else:
                stats["rejected_failed"] += 1
                continue

            candidate_outcome_record = candidate_outcomes.get(
                (ruleset, observation_id, str(arm_id))
            )
            fallback_outcome_record = outcomes.get(observation_id)
            if candidate_outcome_record and fallback_outcome_record:
                outcome_record = (
                    fallback_outcome_record
                    if _outcome_rank(fallback_outcome_record)
                    > _candidate_outcome_rank(candidate_outcome_record)
                    else candidate_outcome_record
                )
            else:
                outcome_record = candidate_outcome_record or fallback_outcome_record
            outcome = (
                outcome_record.get("outcome")
                if isinstance(outcome_record, Mapping)
                else None
            )
            outcome_map = outcome if isinstance(outcome, Mapping) else {}

            status = str(outcome_map.get("status") or "").lower()
            if status == "quarantine":
                stats["invalid_outcome"] += 1
                continue
            if not outcome_map or status not in {"resolved", "void"}:
                stats["pending"] += 1
                continue
            if status == "void":
                stats["void"] += 1
                continue
            if outcome_map.get("outcome_integrity_conflict") is True:
                stats["invalid_outcome"] += 1
                continue
            label = outcome_map.get("goal_to90_normal_time")
            if label is True:
                stats["wins"] += 1
            elif label is False:
                stats["losses"] += 1
            else:
                stats["invalid_outcome"] += 1

    rulesets: Dict[str, Any] = {}
    for ruleset, arms in sorted(grouped.items()):
        finalized = []
        for arm_id, stats in sorted(arms.items()):
            row = dict(stats)
            row["wilson_95"] = wilson_interval(row["wins"], row["losses"])
            finalized.append(row)
        rulesets[ruleset] = {
            "evaluation_semantics": "candidate_at_first_control_allow",
            "first_control_fixture_count": cohort_counts[ruleset],
            "arms": finalized,
        }
    return {
        "report_schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "journal_path": os.path.abspath(journal_path),
        "observation_history_path": os.path.abspath(observation_path),
        "shadow_only": True,
        "production_applied": False,
        "outcome_source_policy": (
            "latest_shadow_candidate_or_filtered_observation_outcome"
        ),
        "rulesets": rulesets,
    }


def render_human(report: Mapping[str, Any]) -> str:
    lines = [
        "Shadow candidates — prospective first-control cohort",
        f"Сформировано: {report.get('generated_at_utc')}",
        "Влияние на production: отсутствует",
    ]
    rulesets = report.get("rulesets")
    ruleset_map = rulesets if isinstance(rulesets, Mapping) else {}
    if not ruleset_map:
        lines.append("Нет зафиксированных first control ALLOW cohort.")
        return "\n".join(lines) + "\n"
    for ruleset, payload in ruleset_map.items():
        payload_map = payload if isinstance(payload, Mapping) else {}
        lines.extend(
            [
                "",
                f"Ruleset: {ruleset}",
                f"Матчей в first-control cohort: {payload_map.get('first_control_fixture_count', 0)}",
            ]
        )
        arms = payload_map.get("arms")
        for arm in arms if isinstance(arms, list) else []:
            if not isinstance(arm, Mapping):
                continue
            wilson = arm.get("wilson_95")
            wilson_map = wilson if isinstance(wilson, Mapping) else {}
            rate = wilson_map.get("rate_pct")
            interval = (
                "нет завершённых"
                if rate is None
                else (
                    f"{float(rate):.2f}% "
                    f"(Wilson 95%: {float(wilson_map['low_pct']):.2f}–"
                    f"{float(wilson_map['high_pct']):.2f}%)"
                )
            )
            lines.append(
                f"- {arm.get('arm_id')}: selected={arm.get('selected')}, "
                f"W/L/P/V/I={arm.get('wins')}/{arm.get('losses')}/"
                f"{arm.get('pending')}/{arm.get('void')}/"
                f"{arm.get('invalid_outcome')}, результат={interval}, "
                f"reject_failed={arm.get('rejected_failed')}, "
                f"reject_unavailable={arm.get('rejected_unavailable')}"
            )
    return "\n".join(lines) + "\n"


def _validate_output_target(output: str, source_paths: Iterable[str]) -> str:
    normalize = lambda value: os.path.normcase(  # noqa: E731
        os.path.realpath(os.path.abspath(os.path.expanduser(value)))
    )
    target = normalize(output)
    protected = {normalize(path) for path in source_paths}
    if target in protected:
        raise ValueError(f"refusing to overwrite source journal: {target}")
    return target


def _atomic_write_text(path: str, text: str) -> None:
    parent = os.path.dirname(path) or os.curdir
    os.makedirs(parent, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{Path(path).stem}.", suffix=".tmp", dir=parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only first-control shadow-candidate report."
    )
    parser.add_argument(
        "--journal",
        default=str(PROJECT_ROOT / "data" / "shadow_candidates.jsonl"),
    )
    parser.add_argument(
        "--observation-file",
        default=str(PROJECT_ROOT / "data" / "observation_history.jsonl"),
    )
    parser.add_argument("--ruleset")
    parser.add_argument("--format", choices=("human", "json"), default="human")
    parser.add_argument("--output")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    report = build_report(
        args.journal,
        args.observation_file,
        ruleset_version=args.ruleset,
    )
    rendered = (
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        if args.format == "json"
        else render_human(report)
    )
    if args.output:
        protected = [args.journal, args.observation_file]
        protected.extend(jsonl_paths(args.journal))
        protected.extend(jsonl_paths(args.observation_file))
        protected.extend(
            [
                args.journal + ".index.sqlite3",
                args.journal + ".lock",
                args.observation_file + ".index.sqlite3",
                args.observation_file + ".lock",
            ]
        )
        target = _validate_output_target(args.output, protected)
        _atomic_write_text(target, rendered)
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
