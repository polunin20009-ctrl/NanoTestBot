from __future__ import annotations

import json
import os
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .parser import derive_halftime_state
from .smoothing import (
    WEIGHT_RECENT,
    blend_with_league,
    build_two_tier_weights,
    compute_weighted_average,
    effective_sample_size,
    smooth_metric,
)
from .storage import DEFAULT_HISTORY_PATH, load_second_half_history_records


# These defaults intentionally preserve the current smoothing behavior used by
# the live model: 20 team matches, 200 league matches, and the existing cup
# fallback window.
TEAM_2H_MATCHES = int(os.environ.get("TEAM_2H_MATCHES", "20"))
LEAGUE_2H_MATCHES = int(os.environ.get("LEAGUE_2H_MATCHES", "200"))
CUP_2H_MATCHES = int(os.environ.get("CUP_2H_MATCHES", "50"))
CUP_2H_FALLBACK_MIN = int(os.environ.get("CUP_2H_FALLBACK_MIN", "30"))
DEFAULT_TEAM_STATS_PATH = os.environ.get("TEAM_2H_STATS_PATH", os.path.join("stats", "team_2h_stats.json"))
DEFAULT_LEAGUE_STATS_PATH = os.environ.get("LEAGUE_2H_STATS_PATH", os.path.join("stats", "league_2h_stats.json"))
SCORE_BUCKETS = ["DRAW", "LEAD_1", "TRAIL_1", "LEAD_2_PLUS", "TRAIL_2_PLUS"]


def _ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)


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


def sort_history_records(records: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    normalized = [dict(record) for record in records if isinstance(record, Mapping)]
    normalized.sort(key=lambda item: (_parse_dt(item.get("finished_at")), _safe_int(item.get("fixture_id"), 0)))
    return normalized


def _is_cup_record(record: Mapping[str, Any]) -> bool:
    if record.get("is_cup") is not None:
        return bool(record.get("is_cup"))
    league_type = str(record.get("league_type") or "").strip().lower()
    return league_type == "cup"


def _team_bucket_for_side(record: Mapping[str, Any], side: str) -> str:
    ht_home = _safe_int(record.get("ht_home"), 0)
    ht_away = _safe_int(record.get("ht_away"), 0)
    if side == "home":
        return derive_halftime_state(ht_home, ht_away)
    return derive_halftime_state(ht_away, ht_home)


def _team_row_from_record(record: Mapping[str, Any], side: str) -> Dict[str, Any]:
    if side == "home":
        team_id = _safe_int(record.get("home_team_id"), 0)
        team_name = str(record.get("home_team_name") or "")
        goals_for = _safe_int(record.get("goals_2h_home"), 0)
        goals_against = _safe_int(record.get("goals_2h_away"), 0)
        scored = bool(record.get("home_scored_in_2h", False))
        conceded = bool(record.get("home_conceded_in_2h", False))
    else:
        team_id = _safe_int(record.get("away_team_id"), 0)
        team_name = str(record.get("away_team_name") or "")
        goals_for = _safe_int(record.get("goals_2h_away"), 0)
        goals_against = _safe_int(record.get("goals_2h_home"), 0)
        scored = bool(record.get("away_scored_in_2h", False))
        conceded = bool(record.get("away_conceded_in_2h", False))

    return {
        "team_id": team_id,
        "team_name": team_name,
        "league_id": _safe_int(record.get("league_id"), 0),
        "league_name": str(record.get("league_name") or ""),
        "league_type": str(record.get("league_type") or ""),
        "finished_at": str(record.get("finished_at") or ""),
        "fixture_id": _safe_int(record.get("fixture_id"), 0),
        "goals_2h_for": goals_for,
        "goals_2h_against": goals_against,
        "goals_after_60_total": record.get("goals_after_60_total"),
        "goals_after_75_total": record.get("goals_after_75_total"),
        "two_h_goal_match": bool(_safe_int(record.get("goals_2h_total"), 0) > 0),
        "scored_in_2h": bool(scored),
        "conceded_in_2h": bool(conceded),
        "events_available": bool(record.get("events_available", False)),
        "halftime_bucket": _team_bucket_for_side(record, side),
    }


def _build_global_meta(records: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    if not records:
        return {
            "global_avg_2h_goals": 1.0,
            "global_avg_goals_after_60": 0.5,
            "global_avg_goals_after_75": 0.25,
            "global_two_h_goal_match_rate": 0.5,
            "global_team_scored_in_2h_rate": 0.35,
            "global_team_conceded_in_2h_rate": 0.35,
            "history_records": 0,
        }

    goals_2h_values = [_safe_float(record.get("goals_2h_total"), 0.0) for record in records]
    after_60_values = [_safe_float(record.get("goals_after_60_total"), 0.0) for record in records if record.get("goals_after_60_total") is not None]
    after_75_values = [_safe_float(record.get("goals_after_75_total"), 0.0) for record in records if record.get("goals_after_75_total") is not None]
    goal_match_values = [1.0 if _safe_int(record.get("goals_2h_total"), 0) > 0 else 0.0 for record in records]
    team_rows = [_team_row_from_record(record, side) for record in records for side in ("home", "away")]

    return {
        "global_avg_2h_goals": round(sum(goals_2h_values) / max(1, len(goals_2h_values)), 6),
        "global_avg_goals_after_60": round(sum(after_60_values) / max(1, len(after_60_values)), 6) if after_60_values else 0.0,
        "global_avg_goals_after_75": round(sum(after_75_values) / max(1, len(after_75_values)), 6) if after_75_values else 0.0,
        "global_two_h_goal_match_rate": round(sum(goal_match_values) / max(1, len(goal_match_values)), 6),
        "global_team_scored_in_2h_rate": round(sum(1.0 if row["scored_in_2h"] else 0.0 for row in team_rows) / max(1, len(team_rows)), 6),
        "global_team_conceded_in_2h_rate": round(sum(1.0 if row["conceded_in_2h"] else 0.0 for row in team_rows) / max(1, len(team_rows)), 6),
        "history_records": len(records),
    }


def _aggregate_leagues(records: Sequence[Mapping[str, Any]], global_meta: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        league_key = str(_safe_int(record.get("league_id"), 0))
        grouped[league_key].append(dict(record))

    aggregated: Dict[str, Dict[str, Any]] = {}
    for league_key, league_records in grouped.items():
        ordered = sorted(league_records, key=lambda item: (_parse_dt(item.get("finished_at")), _safe_int(item.get("fixture_id"), 0)), reverse=True)
        cup_votes = sum(1 for record in ordered if _is_cup_record(record))
        is_cup = bool(ordered and cup_votes > len(ordered) / 2.0)
        target_matches = CUP_2H_MATCHES if is_cup else LEAGUE_2H_MATCHES
        window = ordered[:target_matches]
        fallback_used = False
        if is_cup and len(window) < CUP_2H_MATCHES:
            fallback_used = len(window) >= CUP_2H_FALLBACK_MIN

        total_goals = [_safe_float(record.get("goals_2h_total"), 0.0) for record in window]
        after_60 = [_safe_float(record.get("goals_after_60_total"), 0.0) for record in window if record.get("goals_after_60_total") is not None]
        after_75 = [_safe_float(record.get("goals_after_75_total"), 0.0) for record in window if record.get("goals_after_75_total") is not None]
        goal_match = [1.0 if _safe_int(record.get("goals_2h_total"), 0) > 0 else 0.0 for record in window]
        fulltime_goals = [_safe_float(record.get("ft_home"), 0.0) + _safe_float(record.get("ft_away"), 0.0) for record in window]
        team_rows = [_team_row_from_record(record, side) for record in window for side in ("home", "away")]

        avg_2h_goals = sum(total_goals) / max(1, len(total_goals)) if total_goals else _safe_float(global_meta.get("global_avg_2h_goals"), 1.0)
        avg_after_60 = sum(after_60) / max(1, len(after_60)) if after_60 else _safe_float(global_meta.get("global_avg_goals_after_60"), 0.0)
        avg_after_75 = sum(after_75) / max(1, len(after_75)) if after_75 else _safe_float(global_meta.get("global_avg_goals_after_75"), 0.0)
        goal_match_rate = sum(goal_match) / max(1, len(goal_match)) if goal_match else _safe_float(global_meta.get("global_two_h_goal_match_rate"), 0.5)
        second_half_share = sum(total_goals) / max(1.0, sum(fulltime_goals)) if fulltime_goals else 0.0
        team_scored_rate_mean = sum(1.0 if row["scored_in_2h"] else 0.0 for row in team_rows) / max(1, len(team_rows)) if team_rows else _safe_float(global_meta.get("global_team_scored_in_2h_rate"), 0.35)
        team_conceded_rate_mean = sum(1.0 if row["conceded_in_2h"] else 0.0 for row in team_rows) / max(1, len(team_rows)) if team_rows else _safe_float(global_meta.get("global_team_conceded_in_2h_rate"), 0.35)
        events_coverage = sum(1.0 if row["events_available"] else 0.0 for row in team_rows) / max(1, len(team_rows)) if team_rows else 0.0

        aggregated[league_key] = {
            "league_id": _safe_int(ordered[0].get("league_id"), 0) if ordered else 0,
            "league_name": str(ordered[0].get("league_name") or "") if ordered else "",
            "league_type": str(ordered[0].get("league_type") or "") if ordered else "",
            "sample_matches": len(window),
            "window_target": target_matches,
            "fallback_used": bool(fallback_used),
            "avg_2h_goals": round(avg_2h_goals, 6),
            "avg_goals_after_60": round(avg_after_60, 6),
            "avg_goals_after_75": round(avg_after_75, 6),
            "two_h_goal_match_rate": round(goal_match_rate, 6),
            "second_half_share": round(second_half_share, 6),
            "team_scored_in_2h_rate_mean": round(team_scored_rate_mean, 6),
            "team_conceded_in_2h_rate_mean": round(team_conceded_rate_mean, 6),
            "events_coverage": round(events_coverage, 6),
        }

    return aggregated


def _aggregate_score_state(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[Mapping[str, Any]]] = {bucket: [] for bucket in SCORE_BUCKETS}
    for row in rows:
        grouped.setdefault(str(row.get("halftime_bucket") or "DRAW"), []).append(row)

    score_state: Dict[str, Dict[str, Any]] = {}
    for bucket in SCORE_BUCKETS:
        bucket_rows = grouped.get(bucket, [])
        count = len(bucket_rows)
        if count <= 0:
            score_state[bucket] = {
                "n": 0,
                "team_scored_again_rate": 0.0,
                "team_conceded_rate": 0.0,
            }
            continue
        score_state[bucket] = {
            "n": count,
            "team_scored_again_rate": round(sum(1.0 if row.get("scored_in_2h") else 0.0 for row in bucket_rows) / count, 6),
            "team_conceded_rate": round(sum(1.0 if row.get("conceded_in_2h") else 0.0 for row in bucket_rows) / count, 6),
        }
    return score_state


def _aggregate_teams(
    records: Sequence[Mapping[str, Any]],
    leagues: Mapping[str, Mapping[str, Any]],
    global_meta: Mapping[str, Any],
) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        for side in ("home", "away"):
            row = _team_row_from_record(record, side)
            grouped[str(row["team_id"])].append(row)

    aggregated: Dict[str, Dict[str, Any]] = {}
    for team_key, rows in grouped.items():
        ordered = sorted(rows, key=lambda item: (_parse_dt(item.get("finished_at")), _safe_int(item.get("fixture_id"), 0)), reverse=True)
        window = ordered[:TEAM_2H_MATCHES]
        # The two-tier weights and n_eff telemetry are part of the existing
        # smoothing contract; keep them explicit here for auditability.
        weights = build_two_tier_weights(len(window), recent_share=WEIGHT_RECENT)
        league_key = str(_safe_int(window[0].get("league_id"), 0)) if window else "0"
        league_rec = dict(leagues.get(league_key, {}))

        league_avg_2h = _safe_float(league_rec.get("avg_2h_goals"), _safe_float(global_meta.get("global_avg_2h_goals"), 1.0))
        league_avg_after_60 = _safe_float(league_rec.get("avg_goals_after_60"), _safe_float(global_meta.get("global_avg_goals_after_60"), 0.0))
        league_avg_after_75 = _safe_float(league_rec.get("avg_goals_after_75"), _safe_float(global_meta.get("global_avg_goals_after_75"), 0.0))
        league_goal_match = _safe_float(league_rec.get("two_h_goal_match_rate"), _safe_float(global_meta.get("global_two_h_goal_match_rate"), 0.5))
        league_team_scored = _safe_float(league_rec.get("team_scored_in_2h_rate_mean"), _safe_float(global_meta.get("global_team_scored_in_2h_rate"), 0.35))
        league_team_conceded = _safe_float(league_rec.get("team_conceded_in_2h_rate_mean"), _safe_float(global_meta.get("global_team_conceded_in_2h_rate"), 0.35))

        scored_values = [_safe_float(row.get("goals_2h_for"), 0.0) for row in window]
        conceded_values = [_safe_float(row.get("goals_2h_against"), 0.0) for row in window]
        event_rows = [
            row for row in window
            if row.get("events_available")
            and row.get("goals_after_60_total") is not None
            and row.get("goals_after_75_total") is not None
        ]
        event_weights = build_two_tier_weights(len(event_rows), recent_share=WEIGHT_RECENT)
        after_60_values = [_safe_float(row.get("goals_after_60_total"), 0.0) for row in event_rows]
        after_75_values = [_safe_float(row.get("goals_after_75_total"), 0.0) for row in event_rows]
        goal_match_values = [1.0 if row.get("two_h_goal_match") else 0.0 for row in window]
        scored_rate_values = [1.0 if row.get("scored_in_2h") else 0.0 for row in window]
        conceded_rate_values = [1.0 if row.get("conceded_in_2h") else 0.0 for row in window]
        events_values = [1.0 if row.get("events_available") else 0.0 for row in window]

        raw_scored = compute_weighted_average(scored_values, weights)
        raw_conceded = compute_weighted_average(conceded_values, weights)
        raw_after_60 = compute_weighted_average(after_60_values, event_weights) if event_weights else league_avg_after_60
        raw_after_75 = compute_weighted_average(after_75_values, event_weights) if event_weights else league_avg_after_75
        raw_goal_match = compute_weighted_average(goal_match_values, weights)
        raw_scored_rate = compute_weighted_average(scored_rate_values, weights)
        raw_conceded_rate = compute_weighted_average(conceded_rate_values, weights)
        events_coverage = compute_weighted_average(events_values, weights) if weights else 0.0

        smoothed_scored = smooth_metric(raw_scored, weights, league_avg_2h / 2.0)
        smoothed_conceded = smooth_metric(raw_conceded, weights, league_avg_2h / 2.0)
        smoothed_after_60 = smooth_metric(raw_after_60, event_weights, league_avg_after_60)
        smoothed_after_75 = smooth_metric(raw_after_75, event_weights, league_avg_after_75)
        smoothed_goal_match = smooth_metric(raw_goal_match, weights, league_goal_match)
        smoothed_scored_rate = smooth_metric(raw_scored_rate, weights, league_team_scored)
        smoothed_conceded_rate = smooth_metric(raw_conceded_rate, weights, league_team_conceded)

        sample_matches = len(window)
        aggregated[team_key] = {
            "team_id": _safe_int(window[0].get("team_id"), 0) if window else 0,
            "team_name": str(window[0].get("team_name") or "") if window else "",
            "league_id": _safe_int(window[0].get("league_id"), 0) if window else 0,
            "league_name": str(window[0].get("league_name") or "") if window else "",
            "league_type": str(window[0].get("league_type") or "") if window else "",
            "sample_matches": sample_matches,
            "n_eff": round(effective_sample_size(weights), 6),
            "events_coverage": round(events_coverage, 6),
            "weighted_2h_scored_avg_raw": round(raw_scored, 6),
            "weighted_2h_conceded_avg_raw": round(raw_conceded, 6),
            "weighted_goals_after_60_raw": round(raw_after_60, 6),
            "weighted_goals_after_75_raw": round(raw_after_75, 6),
            "weighted_2h_goal_match_rate_raw": round(raw_goal_match, 6),
            "weighted_scored_in_2h_rate_raw": round(raw_scored_rate, 6),
            "weighted_conceded_in_2h_rate_raw": round(raw_conceded_rate, 6),
            "weighted_2h_scored_avg_smoothed": round(smoothed_scored, 6),
            "weighted_2h_conceded_avg_smoothed": round(smoothed_conceded, 6),
            "weighted_goals_after_60_smoothed": round(smoothed_after_60, 6),
            "weighted_goals_after_75_smoothed": round(smoothed_after_75, 6),
            "weighted_2h_goal_match_rate_smoothed": round(smoothed_goal_match, 6),
            "weighted_scored_in_2h_rate_smoothed": round(smoothed_scored_rate, 6),
            "weighted_conceded_in_2h_rate_smoothed": round(smoothed_conceded_rate, 6),
            "weighted_2h_scored_avg_final": round(blend_with_league(smoothed_scored, league_avg_2h / 2.0, sample_matches), 6),
            "weighted_2h_conceded_avg_final": round(blend_with_league(smoothed_conceded, league_avg_2h / 2.0, sample_matches), 6),
            "weighted_goals_after_60_final": round(blend_with_league(smoothed_after_60, league_avg_after_60, sample_matches), 6),
            "weighted_goals_after_75_final": round(blend_with_league(smoothed_after_75, league_avg_after_75, sample_matches), 6),
            "weighted_2h_goal_match_rate_final": round(blend_with_league(smoothed_goal_match, league_goal_match, sample_matches), 6),
            "weighted_scored_in_2h_rate_final": round(blend_with_league(smoothed_scored_rate, league_team_scored, sample_matches), 6),
            "weighted_conceded_in_2h_rate_final": round(blend_with_league(smoothed_conceded_rate, league_team_conceded, sample_matches), 6),
            "score_state": _aggregate_score_state(window),
        }

    return aggregated


def build_stats_from_history(records: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    ordered = sort_history_records(records)
    global_meta = _build_global_meta(ordered)
    leagues = _aggregate_leagues(ordered, global_meta)
    teams = _aggregate_teams(ordered, leagues, global_meta)
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    league_payload = {
        "_meta": {
            **global_meta,
            "history_schema_version": 2,
            "generated_at": generated_at,
            "league_window": LEAGUE_2H_MATCHES,
            "cup_window": CUP_2H_MATCHES,
            "cup_fallback_min": CUP_2H_FALLBACK_MIN,
        },
        "leagues": leagues,
    }
    team_payload = {
        "_meta": {
            **global_meta,
            "history_schema_version": 2,
            "generated_at": generated_at,
            "team_window": TEAM_2H_MATCHES,
            "weight_recent": WEIGHT_RECENT,
        },
        "teams": teams,
    }
    return {
        "team_payload": team_payload,
        "league_payload": league_payload,
    }


def aggregate_history_to_files(
    history_path: str = DEFAULT_HISTORY_PATH,
    team_stats_path: str = DEFAULT_TEAM_STATS_PATH,
    league_stats_path: str = DEFAULT_LEAGUE_STATS_PATH,
) -> Dict[str, Any]:
    records = load_second_half_history_records(history_path)
    payloads = build_stats_from_history(records)
    _ensure_parent_dir(team_stats_path)
    _ensure_parent_dir(league_stats_path)

    def _atomic_json(path: str, payload: Dict[str, Any]) -> None:
        parent = os.path.dirname(os.path.abspath(path)) or "."
        fd, temporary = tempfile.mkstemp(prefix=".2h_stats_", suffix=".tmp", dir=parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
                handle.flush()
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.remove(temporary)

    _atomic_json(team_stats_path, payloads["team_payload"])
    _atomic_json(league_stats_path, payloads["league_payload"])

    return {
        "history_records": len(records),
        "team_count": len(payloads["team_payload"].get("teams", {})),
        "league_count": len(payloads["league_payload"].get("leagues", {})),
        "team_stats_path": team_stats_path,
        "league_stats_path": league_stats_path,
    }
