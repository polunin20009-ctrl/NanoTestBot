from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence

from match_period import normalize_score_blocks


ScoreBucket = str
SECOND_HALF_HISTORY_SCHEMA_VERSION = 2
SECOND_HALF_PARSER_VERSION = "normal_time_v2.3"


class IncompleteSecondHalfDataError(ValueError):
    """The provider has not exposed enough terminal data for a safe 2H label."""


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def _safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _get_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _fixture_root(payload: Any) -> Mapping[str, Any]:
    if isinstance(payload, Mapping):
        response = payload.get("response")
        if isinstance(response, Sequence) and not isinstance(response, (str, bytes)) and response:
            first = response[0]
            if isinstance(first, Mapping):
                return first
        return payload
    return {}


def _parse_iso(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        raw = str(value).strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _derive_finished_at(payload: Mapping[str, Any]) -> str:
    fixture_block = _get_mapping(payload.get("fixture"))
    date_raw = fixture_block.get("date") or payload.get("date")
    kickoff_dt = _parse_iso(date_raw)
    if kickoff_dt is not None:
        return (kickoff_dt + timedelta(hours=2)).replace(microsecond=0).isoformat()
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _kickoff_at(payload: Mapping[str, Any]) -> Optional[str]:
    fixture_block = _get_mapping(payload.get("fixture"))
    kickoff = _parse_iso(fixture_block.get("date") or payload.get("date"))
    return kickoff.replace(microsecond=0).isoformat() if kickoff else None


def _fixture_status(payload: Mapping[str, Any]) -> str:
    status = _get_mapping(_get_mapping(payload.get("fixture")).get("status"))
    return _safe_str(status.get("short") or status.get("value")).upper()


def _extract_normal_time_scores(payload: Mapping[str, Any]) -> tuple[int, int, str]:
    blocks = normalize_score_blocks(payload)
    normal = _get_mapping(blocks.get("normal_time"))
    home = _safe_int(normal.get("home"), None)
    away = _safe_int(normal.get("away"), None)
    if home is not None and away is not None:
        return int(home), int(away), _safe_str(normal.get("source"), "score.fulltime")
    fallback_home, fallback_away = _extract_ft_scores(payload)
    return fallback_home, fallback_away, "fallback_current_goals"


def _infer_league_type(league: Mapping[str, Any]) -> tuple[str, bool, str]:
    explicit = _safe_str(league.get("type")).strip()
    if explicit:
        is_cup = explicit.lower() == "cup"
        return explicit, is_cup, "api"
    name = _safe_str(league.get("name")).strip().lower()
    cup_tokens = ("cup", "copa", "pokal", "trophy")
    is_cup = any(token in name for token in cup_tokens)
    return ("Cup" if is_cup else "League"), is_cup, "name_heuristic"


def _extract_ft_scores(payload: Mapping[str, Any]) -> tuple[int, int]:
    goals_block = _get_mapping(payload.get("goals"))
    ft_block = _get_mapping(_get_mapping(payload.get("score")).get("fulltime"))
    ft_home = _safe_int(goals_block.get("home"), None)
    ft_away = _safe_int(goals_block.get("away"), None)
    if ft_home is None:
        ft_home = _safe_int(ft_block.get("home"), 0)
    if ft_away is None:
        ft_away = _safe_int(ft_block.get("away"), 0)
    return int(ft_home or 0), int(ft_away or 0)


def _extract_ht_scores(payload: Mapping[str, Any]) -> tuple[Optional[int], Optional[int]]:
    halftime_block = _get_mapping(_get_mapping(payload.get("score")).get("halftime"))
    return _safe_int(halftime_block.get("home"), None), _safe_int(halftime_block.get("away"), None)


def _derive_halftime_scores_from_events(
    goal_events: Sequence[Mapping[str, Any]],
    home_team_id: Optional[int],
    away_team_id: Optional[int],
) -> tuple[int, int]:
    home_goals = 0
    away_goals = 0
    for event in goal_events:
        minute = _safe_int(event.get("minute"), None)
        if minute is None or minute > 45:
            continue
        team_side = _safe_str(event.get("team_side")).lower()
        team_id = _safe_int(event.get("team_id"), None)
        if team_side == "home" or (team_id is not None and home_team_id is not None and team_id == home_team_id):
            home_goals += 1
        elif team_side == "away" or (team_id is not None and away_team_id is not None and team_id == away_team_id):
            away_goals += 1
    return home_goals, away_goals


def _score_bucket(team_goals: int, opp_goals: int) -> ScoreBucket:
    diff = int(team_goals) - int(opp_goals)
    if diff == 0:
        return "DRAW"
    if diff == 1:
        return "LEAD_1"
    if diff == -1:
        return "TRAIL_1"
    if diff >= 2:
        return "LEAD_2_PLUS"
    return "TRAIL_2_PLUS"


def derive_halftime_state(ht_home: int, ht_away: int) -> ScoreBucket:
    """Return the home-team halftime bucket."""
    return _score_bucket(int(ht_home), int(ht_away))


def parse_goal_events_from_fixture_events(events: Any) -> List[Dict[str, Any]]:
    """Parse goal events from API-Football /fixtures/events payloads.

    Only events with ``type=Goal`` are accepted. Injury time is preserved as
    ``minute`` + ``extra`` and expanded into ``abs_minute``.
    """
    parsed: List[Dict[str, Any]] = []
    if not isinstance(events, Sequence) or isinstance(events, (str, bytes)):
        return parsed

    for raw_event in events:
        if not isinstance(raw_event, Mapping):
            continue

        event_type = _safe_str(raw_event.get("type")).strip()
        if event_type.lower() != "goal":
            continue

        detail = _safe_str(raw_event.get("detail"))
        detail_lower = detail.lower()
        invalid_tokens = (
            "missed penalty", "penalty missed", "cancel", "disallow",
            "no goal", "ruled out", "offside", "var no goal",
        )
        if any(token in detail_lower for token in invalid_tokens):
            continue

        time_block = _get_mapping(raw_event.get("time"))
        minute = _safe_int(time_block.get("elapsed"), _safe_int(raw_event.get("elapsed"), _safe_int(raw_event.get("minute"), None)))
        if minute is None:
            continue
        # This dataset is explicitly scoped to normal time. Minute 90 with
        # stoppage is valid; elapsed values above 90 are extra time/shootout.
        if int(minute) > 90:
            continue

        extra = _safe_int(time_block.get("extra"), 0) or 0
        team_block = _get_mapping(raw_event.get("team"))
        player_block = _get_mapping(raw_event.get("player"))
        assist_block = _get_mapping(raw_event.get("assist"))

        parsed.append(
            {
                "minute": int(minute),
                "extra": int(extra),
                "abs_minute": int(minute) + int(extra),
                "team_id": _safe_int(team_block.get("id"), None),
                "team_side": _safe_str(raw_event.get("team_side") or raw_event.get("side") or "").lower() or None,
                "type": event_type,
                "detail": detail,
                "player_id": _safe_int(player_block.get("id"), None),
                "player_name": _safe_str(player_block.get("name")),
                "assist_id": _safe_int(assist_block.get("id"), None),
                "assist_name": _safe_str(assist_block.get("name")),
            }
        )

    parsed.sort(key=lambda item: (int(item.get("abs_minute") or 0), int(item.get("minute") or 0), int(item.get("extra") or 0)))
    return parsed


def derive_state_transitions(goal_events: Sequence[Mapping[str, Any]], ht_home: int, ht_away: int) -> List[Dict[str, Any]]:
    """Build second-half state transitions from halftime score and parsed goals."""
    current_home = int(ht_home)
    current_away = int(ht_away)
    transitions: List[Dict[str, Any]] = []

    ordered_events = sorted(
        [event for event in goal_events if isinstance(event, Mapping)],
        key=lambda item: (int(item.get("abs_minute") or 0), int(item.get("minute") or 0), int(item.get("extra") or 0)),
    )
    for event in ordered_events:
        minute = _safe_int(event.get("minute"), None)
        if minute is None or minute <= 45:
            continue

        team_side = _safe_str(event.get("team_side")).lower()
        if team_side not in {"home", "away"}:
            continue

        before_home = current_home
        before_away = current_away
        before_state = derive_halftime_state(before_home, before_away)

        if team_side == "home":
            current_home += 1
        else:
            current_away += 1

        after_state = derive_halftime_state(current_home, current_away)
        transitions.append(
            {
                "minute": int(minute),
                "extra": int(_safe_int(event.get("extra"), 0) or 0),
                "abs_minute": int(_safe_int(event.get("abs_minute"), int(minute)) or int(minute)),
                "team_id": _safe_int(event.get("team_id"), None),
                "team_side": team_side,
                "score_before_home": before_home,
                "score_before_away": before_away,
                "score_after_home": current_home,
                "score_after_away": current_away,
                "state_before": before_state,
                "state_after": after_state,
            }
        )

    return transitions


def compute_second_half_features_from_fixture(
    fixture_payload: Any,
    events_payload: Optional[Any],
    observed_finished_at_utc: Optional[str] = None,
) -> Dict[str, Any]:
    """Compute a finished-match second-half analytics record."""
    fixture_root = _fixture_root(fixture_payload)
    fixture_block = _get_mapping(fixture_root.get("fixture"))
    league_block = _get_mapping(fixture_root.get("league"))
    teams_block = _get_mapping(fixture_root.get("teams"))
    home_block = _get_mapping(teams_block.get("home"))
    away_block = _get_mapping(teams_block.get("away"))

    fixture_id = _safe_int(fixture_block.get("id"), _safe_int(fixture_root.get("fixture_id"), _safe_int(fixture_root.get("id"), 0))) or 0
    season = _safe_int(league_block.get("season"), 0) or 0
    league_id = _safe_int(league_block.get("id"), 0) or 0
    home_team_id = _safe_int(home_block.get("id"), 0) or 0
    away_team_id = _safe_int(away_block.get("id"), 0) or 0

    if fixture_id <= 0 or league_id <= 0 or home_team_id <= 0 or away_team_id <= 0:
        raise ValueError(
            "second-half history requires positive fixture, league, home-team and away-team ids"
        )

    goal_events = parse_goal_events_from_fixture_events(events_payload or [])
    for event in goal_events:
        if event.get("team_side") in {"home", "away"}:
            continue
        team_id = _safe_int(event.get("team_id"), None)
        if team_id is not None and team_id == home_team_id:
            event["team_side"] = "home"
        elif team_id is not None and team_id == away_team_id:
            event["team_side"] = "away"

    ft_home, ft_away, normal_time_score_source = _extract_normal_time_scores(fixture_root)
    fixture_status = _fixture_status(fixture_root)
    if fixture_status in {"AET", "PEN"} and normal_time_score_source == "fallback_current_goals":
        raise IncompleteSecondHalfDataError(
            "normal-time score is unavailable for extra-time fixture"
        )
    events_response_available = events_payload is not None
    raw_event_count = (
        len(events_payload)
        if isinstance(events_payload, Sequence) and not isinstance(events_payload, (str, bytes))
        else 0
    )
    expected_normal_time_goals = int(ft_home) + int(ft_away)
    events_complete = bool(
        events_response_available and len(goal_events) == expected_normal_time_goals
    )

    ht_home, ht_away = _extract_ht_scores(fixture_root)
    halftime_score_source = "fixture_score_halftime"
    if ht_home is None or ht_away is None:
        if not events_complete:
            raise IncompleteSecondHalfDataError(
                "halftime score is unavailable and goal events are incomplete"
            )
        derived_ht_home, derived_ht_away = _derive_halftime_scores_from_events(goal_events, home_team_id, away_team_id)
        ht_home = derived_ht_home if ht_home is None else ht_home
        ht_away = derived_ht_away if ht_away is None else ht_away
        halftime_score_source = "complete_goal_events"

    ht_home = int(ht_home or 0)
    ht_away = int(ht_away or 0)

    goals_2h_home = max(0, int(ft_home) - int(ht_home))
    goals_2h_away = max(0, int(ft_away) - int(ht_away))
    second_half_goal_events = [event for event in goal_events if int(event.get("minute") or 0) > 45]

    goals_after_60_total: Optional[int]
    goals_after_75_total: Optional[int]
    if events_complete:
        goals_after_60_total = sum(1 for event in second_half_goal_events if int(event.get("abs_minute") or 0) > 60)
        goals_after_75_total = sum(1 for event in second_half_goal_events if int(event.get("abs_minute") or 0) > 75)
    else:
        goals_after_60_total = None
        goals_after_75_total = None

    league_type, is_cup, league_type_source = _infer_league_type(league_block)
    collected_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    observed_finished = _parse_iso(observed_finished_at_utc)
    finished_at = (
        observed_finished.replace(microsecond=0).isoformat()
        if observed_finished else _derive_finished_at(fixture_root)
    )
    return {
        "schema_version": SECOND_HALF_HISTORY_SCHEMA_VERSION,
        "parser_version": SECOND_HALF_PARSER_VERSION,
        "outcome_scope": "TO_90_NORMAL_TIME",
        "fixture_id": int(fixture_id),
        "finished_at": finished_at,
        "finished_at_source": "observed_terminal_status" if observed_finished else "kickoff_plus_2h_estimate",
        "kickoff_at": _kickoff_at(fixture_root),
        "collected_at_utc": collected_at,
        "fixture_status": fixture_status,
        "season": int(season),
        "league_id": int(league_id),
        "league_name": _safe_str(league_block.get("name")),
        "league_type": league_type,
        "league_type_source": league_type_source,
        "is_cup": is_cup,
        "round": _safe_str(league_block.get("round")),
        "home_team_id": int(home_team_id),
        "home_team_name": _safe_str(home_block.get("name")),
        "away_team_id": int(away_team_id),
        "away_team_name": _safe_str(away_block.get("name")),
        "ft_home": int(ft_home),
        "ft_away": int(ft_away),
        "normal_time_score_source": normal_time_score_source,
        "ht_home": int(ht_home),
        "ht_away": int(ht_away),
        "halftime_score_source": halftime_score_source,
        "goals_2h_home": int(goals_2h_home),
        "goals_2h_away": int(goals_2h_away),
        "goals_2h_total": int(goals_2h_home + goals_2h_away),
        "goals_after_60_total": goals_after_60_total,
        "goals_after_75_total": goals_after_75_total,
        "home_scored_in_2h": bool(goals_2h_home > 0),
        "away_scored_in_2h": bool(goals_2h_away > 0),
        "home_conceded_in_2h": bool(goals_2h_away > 0),
        "away_conceded_in_2h": bool(goals_2h_home > 0),
        "events_available": events_complete,
        "events_response_available": events_response_available,
        "events_complete": events_complete,
        "events_quality": (
            "complete" if events_complete
            else "incomplete" if events_response_available
            else "unavailable"
        ),
        "raw_event_count": raw_event_count,
        "valid_normal_time_goal_event_count": len(goal_events),
        "goal_events": goal_events,
        "halftime_state": derive_halftime_state(ht_home, ht_away),
        "state_transitions_2h": derive_state_transitions(goal_events, ht_home, ht_away) if events_complete else [],
    }
