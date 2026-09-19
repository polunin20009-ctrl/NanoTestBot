from __future__ import annotations

import json
import os
from typing import Any, Dict, Mapping, Optional

from .aggregate import DEFAULT_LEAGUE_STATS_PATH, DEFAULT_TEAM_STATS_PATH
from .parser import derive_halftime_state
from .smoothing import blend_with_league, clamp


_JSON_CACHE: Dict[str, Dict[str, Any]] = {
    "team": {"path": None, "mtime": None, "data": {"_meta": {}, "teams": {}}},
    "league": {"path": None, "mtime": None, "data": {"_meta": {}, "leagues": {}}},
}


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


def _load_json_cached(kind: str, path: str) -> Dict[str, Any]:
    abs_path = os.path.abspath(path)
    try:
        mtime = os.path.getmtime(abs_path)
    except OSError:
        return {"_meta": {}, "teams": {}} if kind == "team" else {"_meta": {}, "leagues": {}}

    cache = _JSON_CACHE[kind]
    if cache.get("path") == abs_path and cache.get("mtime") == mtime:
        return dict(cache.get("data") or {})

    with open(abs_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    cache["path"] = abs_path
    cache["mtime"] = mtime
    cache["data"] = data
    return dict(data)


def load_team_2h_stats(path: str = DEFAULT_TEAM_STATS_PATH) -> Dict[str, Any]:
    return _load_json_cached("team", path)


def load_league_2h_stats(path: str = DEFAULT_LEAGUE_STATS_PATH) -> Dict[str, Any]:
    return _load_json_cached("league", path)


def _team_record(team_stats_data: Mapping[str, Any], team_id: int) -> Dict[str, Any]:
    teams = team_stats_data.get("teams") if isinstance(team_stats_data, Mapping) else {}
    if isinstance(teams, Mapping):
        record = teams.get(str(int(team_id)))
        if isinstance(record, Mapping):
            return dict(record)
    return {}


def _league_record(league_stats_data: Mapping[str, Any], league_id: int) -> Dict[str, Any]:
    leagues = league_stats_data.get("leagues") if isinstance(league_stats_data, Mapping) else {}
    if isinstance(leagues, Mapping):
        record = leagues.get(str(int(league_id)))
        if isinstance(record, Mapping):
            return dict(record)
    return {}


def _league_global(league_stats_data: Mapping[str, Any]) -> Dict[str, Any]:
    meta = league_stats_data.get("_meta") if isinstance(league_stats_data, Mapping) else {}
    if isinstance(meta, Mapping):
        return dict(meta)
    return {}


def _team_global(team_stats_data: Mapping[str, Any]) -> Dict[str, Any]:
    meta = team_stats_data.get("_meta") if isinstance(team_stats_data, Mapping) else {}
    if isinstance(meta, Mapping):
        return dict(meta)
    return {}


def _metric_or_default(record: Mapping[str, Any], key: str, default: float) -> float:
    return _safe_float(record.get(key), default)


def compute_team_2h_factor(
    home_team_id: int,
    away_team_id: int,
    league_id: int,
    team_stats_data: Optional[Mapping[str, Any]] = None,
    league_stats_data: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    team_payload = dict(team_stats_data or load_team_2h_stats())
    league_payload = dict(league_stats_data or load_league_2h_stats())

    home_record = _team_record(team_payload, home_team_id)
    away_record = _team_record(team_payload, away_team_id)
    league_record = _league_record(league_payload, league_id)
    league_meta = _league_global(league_payload)

    league_avg_2h_goals = _safe_float(league_record.get("avg_2h_goals"), _safe_float(league_meta.get("global_avg_2h_goals"), 1.0))
    default_side_mean = league_avg_2h_goals / 2.0

    home_scored = _metric_or_default(home_record, "weighted_2h_scored_avg_final", default_side_mean)
    home_conceded = _metric_or_default(home_record, "weighted_2h_conceded_avg_final", default_side_mean)
    away_scored = _metric_or_default(away_record, "weighted_2h_scored_avg_final", default_side_mean)
    away_conceded = _metric_or_default(away_record, "weighted_2h_conceded_avg_final", default_side_mean)

    expected_home_2h = 0.5 * (home_scored + away_conceded)
    expected_away_2h = 0.5 * (away_scored + home_conceded)
    team_2h_total = expected_home_2h + expected_away_2h
    factor = clamp(team_2h_total / max(0.01, league_avg_2h_goals), 0.90, 1.12)

    home_sample = _safe_int(home_record.get("sample_matches"), 0)
    away_sample = _safe_int(away_record.get("sample_matches"), 0)
    events_coverage = (
        _safe_float(home_record.get("events_coverage"), _safe_float(league_record.get("events_coverage"), 0.0))
        + _safe_float(away_record.get("events_coverage"), _safe_float(league_record.get("events_coverage"), 0.0))
        + _safe_float(league_record.get("events_coverage"), 0.0)
    ) / 3.0

    return {
        "factor": round(factor, 6),
        "expected_home_2h": round(expected_home_2h, 6),
        "expected_away_2h": round(expected_away_2h, 6),
        "team_2h_total": round(team_2h_total, 6),
        "league_avg_2h_goals": round(league_avg_2h_goals, 6),
        "home_2h_scored_smoothed": round(home_scored, 6),
        "home_2h_conceded_smoothed": round(home_conceded, 6),
        "away_2h_scored_smoothed": round(away_scored, 6),
        "away_2h_conceded_smoothed": round(away_conceded, 6),
        "team_sample_home": home_sample,
        "team_sample_away": away_sample,
        "events_coverage": round(events_coverage, 6),
    }


def compute_league_2h_factor(
    league_id: int,
    league_stats_data: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    league_payload = dict(league_stats_data or load_league_2h_stats())
    league_record = _league_record(league_payload, league_id)
    league_meta = _league_global(league_payload)

    league_avg_2h_goals = _safe_float(league_record.get("avg_2h_goals"), _safe_float(league_meta.get("global_avg_2h_goals"), 1.0))
    global_avg_2h_goals = _safe_float(league_meta.get("global_avg_2h_goals"), league_avg_2h_goals or 1.0)
    factor = clamp(league_avg_2h_goals / max(0.01, global_avg_2h_goals), 0.95, 1.08)

    return {
        "factor": round(factor, 6),
        "league_avg_2h_goals": round(league_avg_2h_goals, 6),
        "global_avg_2h_goals": round(global_avg_2h_goals, 6),
        "avg_goals_after_60": round(_safe_float(league_record.get("avg_goals_after_60"), _safe_float(league_meta.get("global_avg_goals_after_60"), 0.0)), 6),
        "league_sample": _safe_int(league_record.get("sample_matches"), 0),
    }


def compute_score_state_factor(
    current_home_goals: int,
    current_away_goals: int,
    home_team_id: int,
    away_team_id: int,
    league_id: int,
    team_stats_data: Optional[Mapping[str, Any]] = None,
    league_stats_data: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    team_payload = dict(team_stats_data or load_team_2h_stats())
    league_payload = dict(league_stats_data or load_league_2h_stats())
    home_record = _team_record(team_payload, home_team_id)
    away_record = _team_record(team_payload, away_team_id)
    league_record = _league_record(league_payload, league_id)
    league_meta = _league_global(league_payload)

    home_bucket = derive_halftime_state(int(current_home_goals), int(current_away_goals))
    away_bucket = derive_halftime_state(int(current_away_goals), int(current_home_goals))
    league_goal_rate = _safe_float(league_record.get("two_h_goal_match_rate"), _safe_float(league_meta.get("global_two_h_goal_match_rate"), 0.5))
    league_scored_rate = _safe_float(league_record.get("team_scored_in_2h_rate_mean"), _safe_float(league_meta.get("global_team_scored_in_2h_rate"), 0.35))
    league_conceded_rate = _safe_float(league_record.get("team_conceded_in_2h_rate_mean"), _safe_float(league_meta.get("global_team_conceded_in_2h_rate"), 0.35))

    home_state = dict((home_record.get("score_state") or {}).get(home_bucket, {})) if isinstance(home_record.get("score_state"), Mapping) else {}
    away_state = dict((away_record.get("score_state") or {}).get(away_bucket, {})) if isinstance(away_record.get("score_state"), Mapping) else {}

    home_state_n = _safe_int(home_state.get("n"), 0)
    away_state_n = _safe_int(away_state.get("n"), 0)
    league_sample = _safe_int(league_record.get("sample_matches"), 0)
    if league_sample <= 0 and home_state_n <= 0 and away_state_n <= 0:
        return {
            "factor": 1.0,
            "home_bucket": home_bucket,
            "away_bucket": away_bucket,
            "home_bucket_n": home_state_n,
            "away_bucket_n": away_state_n,
            "expected_home_rate": round(league_scored_rate, 6),
            "expected_away_rate": round(league_scored_rate, 6),
            "league_goal_rate": round(league_goal_rate, 6),
        }

    home_scored_rate = blend_with_league(_safe_float(home_state.get("team_scored_again_rate"), league_scored_rate), league_scored_rate, home_state_n)
    home_conceded_rate = blend_with_league(_safe_float(home_state.get("team_conceded_rate"), league_conceded_rate), league_conceded_rate, home_state_n)
    away_scored_rate = blend_with_league(_safe_float(away_state.get("team_scored_again_rate"), league_scored_rate), league_scored_rate, away_state_n)
    away_conceded_rate = blend_with_league(_safe_float(away_state.get("team_conceded_rate"), league_conceded_rate), league_conceded_rate, away_state_n)

    expected_home_rate = 0.5 * (home_scored_rate + away_conceded_rate)
    expected_away_rate = 0.5 * (away_scored_rate + home_conceded_rate)
    score_state_total = expected_home_rate + expected_away_rate
    # The numerator is a sum of two team-level scoring probabilities, so its
    # neutral baseline must use the same unit.  Dividing by the match-level
    # probability of any second-half goal made almost every result hit 1.12.
    baseline_score_state_total = league_scored_rate + league_conceded_rate
    factor = clamp(
        score_state_total / max(0.01, baseline_score_state_total),
        0.90,
        1.12,
    )

    return {
        "factor": round(factor, 6),
        "home_bucket": home_bucket,
        "away_bucket": away_bucket,
        "home_bucket_n": home_state_n,
        "away_bucket_n": away_state_n,
        "expected_home_rate": round(expected_home_rate, 6),
        "expected_away_rate": round(expected_away_rate, 6),
        "league_goal_rate": round(league_goal_rate, 6),
        "baseline_score_state_total": round(baseline_score_state_total, 6),
    }
