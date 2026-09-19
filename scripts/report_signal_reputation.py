from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Mapping

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from signal_reputation import (
    EXPANDED_BLEND_COHORT,
    build_expanded_blend_cohort,
    iter_shadow_records,
)
from outcome_revision import outcome_revision as get_outcome_revision


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _outcome_is_usable(outcome: Mapping[str, Any]) -> bool:
    # Legacy shadow outcomes did not always persist an explicit status, so a
    # missing status remains readable.  An explicit invalid terminal update,
    # however, must remove the old label instead of turning a quarantine into
    # a synthetic loss in the calibration report.
    status = str(outcome.get("status") or "").strip().lower()
    if status and status != "resolved":
        return False
    if outcome.get("outcome_integrity_conflict") is True:
        return False
    scope = str(outcome.get("outcome_scope") or "").strip().upper()
    if scope and scope not in {
        "TO_90_NORMAL_TIME",
        "NORMAL_TIME",
        "TO90_NORMAL_TIME",
    }:
        return False
    return True


def _label(outcome: Mapping[str, Any], target: str):
    if not _outcome_is_usable(outcome):
        return None
    key = "goal_within_15" if target == "next15" else "goal_to90_normal_time"
    value = outcome.get(key)
    if value is True or value == 1:
        return 1.0
    if value is False or value == 0:
        return 0.0
    return None


def _projection_rank(record: Mapping[str, Any]) -> tuple[int, str]:
    projection = record.get("shadow_reputation")
    projection = projection if isinstance(projection, Mapping) else {}
    try:
        schema = int(projection.get("schema_version") or 1)
    except (TypeError, ValueError):
        schema = 1
    return schema, str(record.get("created_at_utc") or "")


def _outcome_rank(record: Mapping[str, Any]) -> tuple[int, int, str]:
    try:
        schema = int(record.get("outcome_schema_version") or 1)
    except (TypeError, ValueError):
        schema = 1
    return (
        schema,
        get_outcome_revision(record),
        str(record.get("created_at_utc") or ""),
    )


def _projection_caps(cohorts: Mapping[str, Any]) -> tuple[float, ...]:
    caps: set[float] = set()
    for cohort_name in ("all_decisions", "telegram_signals"):
        cohort = cohorts.get(cohort_name)
        cohort = cohort if isinstance(cohort, Mapping) else {}
        targets = cohort.get("targets")
        targets = targets if isinstance(targets, Mapping) else {}
        for target in targets.values():
            target = target if isinstance(target, Mapping) else {}
            for cap in (target.get("probability_by_cap") or {}):
                try:
                    caps.add(abs(float(cap)))
                except (TypeError, ValueError):
                    continue
    return tuple(sorted(caps)) or (2.0, 3.0, 5.0)


def build_report(
    path: str,
    *,
    blend_telegram_weight: float = 0.75,
    blend_next15_telegram_weight: float = 1.00,
) -> Dict[str, Any]:
    blend_telegram_weight = max(0.0, min(1.0, float(blend_telegram_weight)))
    blend_next15_telegram_weight = max(
        0.0,
        min(1.0, float(blend_next15_telegram_weight)),
    )
    projections: Dict[str, Dict[str, Any]] = {}
    outcomes: Dict[str, Dict[str, Any]] = {}
    for record in iter_shadow_records(path):
        decision_id = str(record.get("decision_id") or "")
        if not decision_id:
            continue
        if record.get("record_type") == "projection":
            current = projections.get(decision_id)
            if current is None or _projection_rank(record) >= _projection_rank(current):
                projections[decision_id] = record
        elif record.get("record_type") == "outcome":
            current = outcomes.get(decision_id)
            if current is None or _outcome_rank(record) >= _outcome_rank(current):
                outcomes[decision_id] = record

    metric_rows: Dict[tuple[str, str, str], list[tuple[float, float]]] = defaultdict(list)
    baseline_rows: Dict[tuple[str, str], list[tuple[float, float]]] = defaultdict(list)
    signal_metric_rows: Dict[
        tuple[str, str, str], list[tuple[float, float]]
    ] = defaultdict(list)
    signal_baseline_rows: Dict[
        tuple[str, str], list[tuple[float, float]]
    ] = defaultdict(list)
    signal_fixture_ids: set[int] = set()
    signal_projection_count = 0
    decision_rows: Dict[tuple[str, str], Dict[str, float]] = defaultdict(
        lambda: {
            "resolved": 0,
            "changed": 0,
            "shadow_allow": 0,
            "shadow_allow_wins_to90": 0,
            "shadow_allow_known_to90": 0,
        }
    )
    joined_count = 0
    for decision_id, projection_record in projections.items():
        outcome_record = outcomes.get(decision_id)
        if not outcome_record:
            continue
        joined_count += 1
        outcome = outcome_record.get("outcome")
        outcome = outcome if isinstance(outcome, Mapping) else {}
        projection = projection_record.get("shadow_reputation")
        projection = projection if isinstance(projection, Mapping) else {}
        baseline_decision = str(projection.get("baseline_decision") or "BLOCK")
        telegram = projection_record.get("telegram")
        telegram = telegram if isinstance(telegram, Mapping) else {}
        record_decision = projection_record.get("decision")
        record_decision = (
            record_decision if isinstance(record_decision, Mapping) else {}
        )
        is_published_signal = bool(telegram.get("send_ok")) or (
            not telegram
            and str(record_decision.get("final_decision") or "").upper()
            == "ALLOW"
        )
        if is_published_signal:
            signal_projection_count += 1
            try:
                signal_fixture_ids.add(int(projection_record.get("fixture_id")))
            except (TypeError, ValueError):
                pass
        projection_cohorts = dict(projection.get("cohorts") or {})
        persisted_expanded = projection_cohorts.get(EXPANDED_BLEND_COHORT)
        persisted_expanded = (
            persisted_expanded
            if isinstance(persisted_expanded, Mapping)
            else {}
        )
        persisted_weights = persisted_expanded.get("configured_telegram_weights")
        persisted_weights = (
            persisted_weights if isinstance(persisted_weights, Mapping) else {}
        )
        persisted_caps = persisted_expanded.get("recommended_caps_pp")
        persisted_caps = (
            persisted_caps if isinstance(persisted_caps, Mapping) else {}
        )
        persisted_matches_config = (
            persisted_expanded.get("blend_version") == "convex_v1"
            and abs(
                _safe_float(persisted_weights.get("next15"), -1.0)
                - blend_next15_telegram_weight
            )
            < 1e-9
            and abs(
                _safe_float(persisted_weights.get("to90"), -1.0)
                - blend_telegram_weight
            )
            < 1e-9
            and _safe_float(persisted_caps.get("next15"), -1.0) == 1.0
            and _safe_float(persisted_caps.get("to90"), -1.0) == 2.0
        )
        if (
            "all_decisions" in projection_cohorts
            or "telegram_signals" in projection_cohorts
        ):
            recomputed_expanded = build_expanded_blend_cohort(
                projection_cohorts,
                caps_pp=_projection_caps(projection_cohorts),
                telegram_weight=blend_telegram_weight,
                next15_telegram_weight=blend_next15_telegram_weight,
            )
            if persisted_matches_config:
                for decision_key in (
                    "decision_by_cap",
                    "recommended_decision",
                    "recommended_channel_filter_passed",
                ):
                    if decision_key in persisted_expanded:
                        recomputed_expanded[decision_key] = dict(
                            persisted_expanded.get(decision_key) or {}
                    ) if decision_key == "decision_by_cap" else persisted_expanded.get(
                        decision_key
                    )
            projection_cohorts[EXPANDED_BLEND_COHORT] = recomputed_expanded
        if not _outcome_is_usable(outcome):
            # Do not let a corrected VOID/quarantine record remain in decision
            # denominators after its old metric labels have been removed.
            continue
        for cohort, cohort_payload in projection_cohorts.items():
            targets = cohort_payload.get("targets") or {}
            for target in ("next15", "to90"):
                target_payload = targets.get(target) or {}
                label = _label(outcome, target)
                if label is None:
                    continue
                base = _safe_float(target_payload.get("base_probability"), 0.0) / 100.0
                baseline_rows[(cohort, target)].append((base, label))
                if is_published_signal:
                    signal_baseline_rows[(cohort, target)].append((base, label))
                for cap, probability in (target_payload.get("probability_by_cap") or {}).items():
                    row = (_safe_float(probability, 0.0) / 100.0, label)
                    metric_rows[(cohort, target, str(cap))].append(row)
                    if is_published_signal:
                        signal_metric_rows[(cohort, target, str(cap))].append(row)
            to90_label = _label(outcome, "to90")
            for cap, shadow_decision in (cohort_payload.get("decision_by_cap") or {}).items():
                stats = decision_rows[(cohort, str(cap))]
                stats["resolved"] += 1
                if str(shadow_decision) != baseline_decision:
                    stats["changed"] += 1
                if str(shadow_decision) == "ALLOW":
                    stats["shadow_allow"] += 1
                    if to90_label is not None:
                        stats["shadow_allow_known_to90"] += 1
                        stats["shadow_allow_wins_to90"] += to90_label
            recommended_decision = cohort_payload.get("recommended_decision")
            if recommended_decision in {"ALLOW", "BLOCK"}:
                stats = decision_rows[(cohort, "recommended")]
                stats["resolved"] += 1
                if str(recommended_decision) != baseline_decision:
                    stats["changed"] += 1
                if str(recommended_decision) == "ALLOW":
                    stats["shadow_allow"] += 1
                    if to90_label is not None:
                        stats["shadow_allow_known_to90"] += 1
                        stats["shadow_allow_wins_to90"] += to90_label

    def summarize(rows: list[tuple[float, float]]) -> Dict[str, Any]:
        if not rows:
            return {
                "count": 0,
                "brier": None,
                "logloss": None,
                "avg_probability": None,
                "actual_rate": None,
            }
        clipped = [
            (max(1e-6, min(1.0 - 1e-6, prob)), label)
            for prob, label in rows
        ]
        return {
            "count": len(rows),
            "brier": round(sum((prob - label) ** 2 for prob, label in rows) / len(rows), 8),
            "logloss": round(
                -sum(
                    label * math.log(prob)
                    + (1.0 - label) * math.log(1.0 - prob)
                    for prob, label in clipped
                )
                / len(clipped),
                8,
            ),
            "avg_probability": round(sum(prob for prob, _ in rows) / len(rows), 8),
            "actual_rate": round(sum(label for _, label in rows) / len(rows), 8),
        }

    def summarize_target_rows(
        source_baseline: Mapping[tuple[str, str], list[tuple[float, float]]],
        source_metrics: Mapping[
            tuple[str, str, str], list[tuple[float, float]]
        ],
    ) -> Dict[str, Any]:
        output: Dict[str, Any] = {}
        for cohort, target in source_baseline:
            cohort_payload = output.setdefault(
                cohort,
                {"targets": {}, "decisions": {}},
            )
            target_payload = {
                "baseline": summarize(source_baseline[(cohort, target)]),
                "caps": {},
            }
            for cap in ("1", "2", "3", "5"):
                target_payload["caps"][cap] = summarize(
                    source_metrics.get((cohort, target, cap), [])
                )
            cohort_payload["targets"][target] = target_payload
        return output

    cohorts = summarize_target_rows(baseline_rows, metric_rows)
    for (cohort, cap), values in decision_rows.items():
        payload = dict(values)
        known = payload["shadow_allow_known_to90"]
        payload["shadow_allow_winrate_to90"] = (
            round(payload["shadow_allow_wins_to90"] / known, 8) if known else None
        )
        cohorts.setdefault(cohort, {"targets": {}, "decisions": {}})["decisions"][cap] = payload

    return {
        "shadow_only": True,
        "expanded_blend": {
            "enabled": True,
            "production_apply": False,
            "version": "convex_v1",
            "telegram_weights": {
                "next15": round(float(blend_next15_telegram_weight), 6),
                "to90": round(float(blend_telegram_weight), 6),
            },
            "recommended_caps_pp": {"next15": 1.0, "to90": 2.0},
            "historical_projection_fallback": True,
        },
        "source": os.path.abspath(path),
        "projection_records": len(projections),
        "outcome_records": len(outcomes),
        "joined_records": joined_count,
        "cohorts": cohorts,
        "published_signals": {
            "projection_records": signal_projection_count,
            "fixtures": len(signal_fixture_ids),
            "selection": "telegram.send_ok; historical fallback=decision.final_decision=ALLOW",
            "cohorts": summarize_target_rows(
                signal_baseline_rows,
                signal_metric_rows,
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Report shadow reputation quality.")
    parser.add_argument(
        "--input",
        default=os.environ.get(
            "SIGNAL_REPUTATION_SHADOW_FILE",
            os.path.join("data", "signal_reputation_shadow.jsonl"),
        ),
    )
    parser.add_argument("--output", default="")
    parser.add_argument(
        "--blend-telegram-weight",
        type=float,
        default=float(os.environ.get("SIGNAL_REPUTATION_BLEND_TELEGRAM_WEIGHT", "0.75")),
    )
    parser.add_argument(
        "--blend-next15-telegram-weight",
        type=float,
        default=float(
            os.environ.get(
                "SIGNAL_REPUTATION_BLEND_NEXT15_TELEGRAM_WEIGHT",
                "1.00",
            )
        ),
    )
    args = parser.parse_args()
    report = build_report(
        args.input,
        blend_telegram_weight=args.blend_telegram_weight,
        blend_next15_telegram_weight=args.blend_next15_telegram_weight,
    )
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
