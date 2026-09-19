from __future__ import annotations

import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from second_half.aggregate import build_stats_from_history, sort_history_records
from second_half.factors import compute_league_2h_factor, compute_score_state_factor, compute_team_2h_factor
from second_half.smoothing import clamp
from second_half.storage import DEFAULT_HISTORY_PATH, load_second_half_history_records


DEFAULT_SNAPSHOTS_PATH = os.environ.get(
    "MATCH_SNAPSHOTS_JSONL_PATH",
    os.path.join("test_zzz.json", "test_match_snapshots.jsonl"),
)
DEFAULT_OUTCOMES_PATH = os.environ.get(
    "MATCH_OUTCOMES_JSONL_PATH",
    os.path.join("test_zzz.json", "test_match_outcomes.jsonl"),
)


def _load_jsonl(path: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    if not os.path.exists(path):
        return records
    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            if isinstance(payload, dict):
                records.append(payload)
    return records


def _parse_dt(value: Any) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    raw = str(value).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except Exception:
        return float(default)


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


def _optional_binary(value: Any) -> int | None:
    """Keep an unresolved outcome unknown instead of turning it into a loss."""
    if value is None or value == "":
        return None
    parsed = _safe_int(value, -1)
    return parsed if parsed in {0, 1} else None


def _join_signals(snapshots: Sequence[Mapping[str, Any]], outcomes: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    outcome_map: Dict[str, Dict[str, Any]] = {}
    for outcome in outcomes:
        signal_id = str(outcome.get("signal_id") or "").strip()
        if signal_id:
            outcome_map[signal_id] = dict(outcome)

    joined: List[Dict[str, Any]] = []
    for snapshot in snapshots:
        signal_id = str(snapshot.get("signal_id") or "").strip()
        if not signal_id:
            continue
        outcome = outcome_map.get(signal_id)
        if not outcome:
            continue
        row = dict(snapshot)
        row.update(
            {
                "goal_after_signal": _safe_int(outcome.get("goal_after_signal"), 0),
                "goal_in_next_15": _optional_binary(outcome.get("goal_in_next_15")),
            }
        )
        joined.append(row)

    joined.sort(key=lambda item: (_parse_dt(item.get("timestamp_utc")), _safe_int(item.get("fixture_id"), 0)))
    return joined


def _compute_context_multiplier(signal: Mapping[str, Any], history_before_signal: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    payloads = build_stats_from_history(history_before_signal)
    team_payload = payloads["team_payload"]
    league_payload = payloads["league_payload"]

    home_team_id = _safe_int(signal.get("home_team_id"), 0)
    away_team_id = _safe_int(signal.get("away_team_id"), 0)
    league_id = _safe_int(signal.get("league_id"), 0)
    score_home = _safe_int(signal.get("score_home"), 0)
    score_away = _safe_int(signal.get("score_away"), 0)

    team_ctx = compute_team_2h_factor(
        home_team_id,
        away_team_id,
        league_id,
        team_stats_data=team_payload,
        league_stats_data=league_payload,
    )
    league_ctx = compute_league_2h_factor(league_id, league_stats_data=league_payload)
    score_ctx = compute_score_state_factor(
        score_home,
        score_away,
        home_team_id,
        away_team_id,
        league_id,
        team_stats_data=team_payload,
        league_stats_data=league_payload,
    )

    context_multiplier = 1.0
    context_multiplier += 0.20 * (float(team_ctx.get("factor", 1.0)) - 1.0)
    context_multiplier += 0.10 * (float(league_ctx.get("factor", 1.0)) - 1.0)
    context_multiplier += 0.15 * (float(score_ctx.get("factor", 1.0)) - 1.0)
    context_multiplier = clamp(context_multiplier, 0.94, 1.08)
    return {
        "team_ctx": team_ctx,
        "league_ctx": league_ctx,
        "score_ctx": score_ctx,
        "context_multiplier": context_multiplier,
    }


def _calibration_by_decile(rows: Sequence[Mapping[str, Any]], prob_key: str) -> List[Dict[str, Any]]:
    buckets: List[List[Mapping[str, Any]]] = [[] for _ in range(10)]
    for row in rows:
        probability = clamp(_safe_float(row.get(prob_key), 0.0), 0.0, 100.0)
        index = min(9, int(probability // 10))
        buckets[index].append(row)

    report: List[Dict[str, Any]] = []
    for index, bucket in enumerate(buckets):
        if not bucket:
            report.append(
                {
                    "decile": f"{index * 10:02d}-{index * 10 + 9:02d}",
                    "count": 0,
                    "avg_pred": 0.0,
                    "actual_rate": 0.0,
                }
            )
            continue

        avg_pred = sum(_safe_float(row.get(prob_key), 0.0) for row in bucket) / len(bucket)
        actual_rate = sum(_safe_int(row.get("goal_after_signal"), 0) for row in bucket) / len(bucket)
        report.append(
            {
                "decile": f"{index * 10:02d}-{index * 10 + 9:02d}",
                "count": len(bucket),
                "avg_pred": round(avg_pred, 4),
                "actual_rate": round(actual_rate, 4),
            }
        )
    return report


def _compute_metrics(rows: Sequence[Mapping[str, Any]], prob_key: str, pred_key: str) -> Dict[str, Any]:
    if not rows:
        return {
            "precision": 0.0,
            "hit_rate": 0.0,
            "brier_score": 0.0,
            "log_loss": 0.0,
            "predicted_positives": 0,
            "samples": 0,
            "calibration_by_decile": [],
        }

    true_positives = 0
    false_positives = 0
    brier_total = 0.0
    log_loss_total = 0.0

    for row in rows:
        label = _safe_int(row.get("goal_after_signal"), 0)
        probability = clamp(_safe_float(row.get(prob_key), 0.0) / 100.0, 1e-6, 1.0 - 1e-6)
        predicted_positive = bool(row.get(pred_key, False))
        if predicted_positive and label == 1:
            true_positives += 1
        elif predicted_positive and label == 0:
            false_positives += 1

        brier_total += (probability - label) ** 2
        log_loss_total += -(label * math.log(probability) + (1 - label) * math.log(1 - probability))

    predicted_positives = true_positives + false_positives
    precision = true_positives / predicted_positives if predicted_positives else 0.0
    hit_rate = true_positives / len(rows)

    return {
        "precision": round(precision, 6),
        "hit_rate": round(hit_rate, 6),
        "brier_score": round(brier_total / len(rows), 6),
        "log_loss": round(log_loss_total / len(rows), 6),
        "predicted_positives": predicted_positives,
        "samples": len(rows),
        "calibration_by_decile": _calibration_by_decile(rows, prob_key),
    }


def main() -> int:
    history = sort_history_records(load_second_half_history_records(DEFAULT_HISTORY_PATH))
    snapshots = _load_jsonl(DEFAULT_SNAPSHOTS_PATH)
    outcomes = _load_jsonl(DEFAULT_OUTCOMES_PATH)
    joined = _join_signals(snapshots, outcomes)

    evaluated: List[Dict[str, Any]] = []
    for signal in joined:
        signal_ts = _parse_dt(signal.get("timestamp_utc"))
        history_before_signal = [
            record
            for record in history
            if _parse_dt(record.get("finished_at")) < signal_ts and _safe_int(record.get("fixture_id"), 0) != _safe_int(signal.get("fixture_id"), 0)
        ]

        context = _compute_context_multiplier(signal, history_before_signal)
        base_probability = clamp(_safe_float(signal.get("prob_second_half_remain"), 0.0), 0.0, 100.0)
        threshold = clamp(_safe_float(signal.get("threshold_remain"), 50.0), 0.0, 100.0)
        adjusted_probability = clamp(base_probability * float(context.get("context_multiplier", 1.0)), 0.0, 100.0)

        row = dict(signal)
        row["baseline_probability"] = round(base_probability, 6)
        row["adjusted_probability"] = round(adjusted_probability, 6)
        row["context_multiplier"] = round(float(context.get("context_multiplier", 1.0)), 6)
        row["baseline_predicted"] = bool(base_probability >= threshold)
        row["adjusted_predicted"] = bool(adjusted_probability >= threshold)
        evaluated.append(row)

    baseline_metrics = _compute_metrics(evaluated, "baseline_probability", "baseline_predicted")
    adjusted_metrics = _compute_metrics(evaluated, "adjusted_probability", "adjusted_predicted")

    uplift = {
        "precision_delta": round(adjusted_metrics["precision"] - baseline_metrics["precision"], 6),
        "hit_rate_delta": round(adjusted_metrics["hit_rate"] - baseline_metrics["hit_rate"], 6),
        "brier_score_delta": round(adjusted_metrics["brier_score"] - baseline_metrics["brier_score"], 6),
        "log_loss_delta": round(adjusted_metrics["log_loss"] - baseline_metrics["log_loss"], 6),
        "predicted_positives_delta": int(adjusted_metrics["predicted_positives"] - baseline_metrics["predicted_positives"]),
    }

    report = {
        "samples": len(evaluated),
        "history_records": len(history),
        "baseline": baseline_metrics,
        "baseline_plus_2h": adjusted_metrics,
        "uplift_summary": uplift,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
