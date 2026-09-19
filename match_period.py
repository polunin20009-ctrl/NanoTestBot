from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, Mapping, Optional, Tuple


class MatchPeriod(str, Enum):
    PRE_MATCH = "PRE_MATCH"
    FIRST_HALF = "FIRST_HALF"
    FIRST_HALF_STOPPAGE = "FIRST_HALF_STOPPAGE"
    HALFTIME = "HALFTIME"
    SECOND_HALF = "SECOND_HALF"
    NORMAL_TIME_STOPPAGE = "NORMAL_TIME_STOPPAGE"
    NORMAL_TIME_FINISHED = "NORMAL_TIME_FINISHED"
    EXTRA_TIME_FIRST_HALF = "EXTRA_TIME_FIRST_HALF"
    EXTRA_TIME_FIRST_HALF_STOPPAGE = "EXTRA_TIME_FIRST_HALF_STOPPAGE"
    EXTRA_TIME_BREAK = "EXTRA_TIME_BREAK"
    EXTRA_TIME_SECOND_HALF = "EXTRA_TIME_SECOND_HALF"
    EXTRA_TIME_SECOND_HALF_STOPPAGE = "EXTRA_TIME_SECOND_HALF_STOPPAGE"
    EXTRA_TIME_FINISHED = "EXTRA_TIME_FINISHED"
    PENALTY_SHOOTOUT = "PENALTY_SHOOTOUT"
    FINISHED = "FINISHED"
    SUSPENDED = "SUSPENDED"
    UNKNOWN = "UNKNOWN"


class GoalScope(str, Enum):
    FIRST_HALF = "FIRST_HALF"
    SECOND_HALF_NORMAL_TIME = "SECOND_HALF_NORMAL_TIME"
    EXTRA_TIME = "EXTRA_TIME"
    PENALTY_SHOOTOUT = "PENALTY_SHOOTOUT"
    INVALID_OR_CANCELLED = "INVALID_OR_CANCELLED"
    UNKNOWN = "UNKNOWN"


class OutcomeScope(str, Enum):
    NEXT_15 = "NEXT_15"
    NEXT_25 = "NEXT_25"
    TO_75_NORMAL_TIME = "TO_75_NORMAL_TIME"
    TO_90_NORMAL_TIME = "TO_90_NORMAL_TIME"
    EXTRA_TIME = "EXTRA_TIME"
    FULL_MATCH_EXCLUDING_SHOOTOUT = "FULL_MATCH_EXCLUDING_SHOOTOUT"


LIVE_NORMAL_TIME_STATUSES = frozenset({"1H", "HT", "2H", "LIVE"})
NORMAL_TIME_BOUNDARY_STATUSES = frozenset({"BT", "ET", "P", "PEN", "AET", "FT"})
EXTRA_TIME_STATUSES = frozenset({"BT", "ET", "AET"})
SHOOTOUT_STATUSES = frozenset({"P", "PEN"})
FULLY_FINISHED_STATUSES = frozenset({"FT", "AET", "PEN", "CANC", "ABD", "AWD", "WO"})
INTERRUPTED_STATUSES = frozenset({"SUSP", "INT", "PST"})


def _int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _status(value: Any) -> str:
    if isinstance(value, Mapping):
        value = value.get("short") or value.get("value") or value.get("status")
        if isinstance(value, Mapping):
            value = value.get("short") or value.get("value")
    return str(value or "").strip().upper()


def classify_match_period(
    *,
    fixture_status_short: Optional[str],
    elapsed: Optional[int],
    extra: Optional[int] = None,
    event_period: Optional[str] = None,
) -> MatchPeriod:
    status = _status(fixture_status_short)
    period_hint = str(event_period or "").strip().upper().replace("-", "_").replace(" ", "_")
    minute = _int(elapsed)
    added = _int(extra) or 0

    if any(token in period_hint for token in ("SHOOTOUT", "PENALTY_SHOOTOUT")) or status in SHOOTOUT_STATUSES:
        return MatchPeriod.PENALTY_SHOOTOUT
    if period_hint in {"ET1", "EXTRA_TIME_FIRST_HALF", "FIRST_EXTRA_TIME"}:
        return MatchPeriod.EXTRA_TIME_FIRST_HALF_STOPPAGE if added > 0 else MatchPeriod.EXTRA_TIME_FIRST_HALF
    if period_hint in {"ET2", "EXTRA_TIME_SECOND_HALF", "SECOND_EXTRA_TIME"}:
        return MatchPeriod.EXTRA_TIME_SECOND_HALF_STOPPAGE if added > 0 else MatchPeriod.EXTRA_TIME_SECOND_HALF
    if period_hint in {"2H", "SECOND_HALF", "NORMAL_TIME_SECOND_HALF"}:
        return MatchPeriod.NORMAL_TIME_STOPPAGE if minute == 90 and added > 0 else MatchPeriod.SECOND_HALF
    if period_hint in {"1H", "FIRST_HALF"}:
        return MatchPeriod.FIRST_HALF_STOPPAGE if minute == 45 and added > 0 else MatchPeriod.FIRST_HALF

    if status in INTERRUPTED_STATUSES:
        return MatchPeriod.SUSPENDED
    if status in {"NS", "TBD", "PST"}:
        return MatchPeriod.PRE_MATCH
    if status == "HT":
        return MatchPeriod.HALFTIME
    if status == "BT":
        return MatchPeriod.EXTRA_TIME_BREAK
    if status == "AET":
        return MatchPeriod.EXTRA_TIME_FINISHED
    if status == "FT":
        return MatchPeriod.NORMAL_TIME_FINISHED
    if status == "1H":
        return MatchPeriod.FIRST_HALF_STOPPAGE if minute == 45 and added > 0 else MatchPeriod.FIRST_HALF
    if status == "2H":
        return MatchPeriod.NORMAL_TIME_STOPPAGE if minute == 90 and added > 0 else MatchPeriod.SECOND_HALF
    if status == "ET":
        if minute is None:
            return MatchPeriod.UNKNOWN
        if minute <= 105:
            return MatchPeriod.EXTRA_TIME_FIRST_HALF_STOPPAGE if minute == 105 and added > 0 else MatchPeriod.EXTRA_TIME_FIRST_HALF
        return MatchPeriod.EXTRA_TIME_SECOND_HALF_STOPPAGE if minute == 120 and added > 0 else MatchPeriod.EXTRA_TIME_SECOND_HALF
    if status in {"CANC", "ABD", "AWD", "WO"}:
        return MatchPeriod.FINISHED
    if minute is None:
        return MatchPeriod.UNKNOWN
    if minute <= 45:
        return MatchPeriod.FIRST_HALF_STOPPAGE if minute == 45 and added > 0 else MatchPeriod.FIRST_HALF
    if minute <= 90:
        return MatchPeriod.NORMAL_TIME_STOPPAGE if minute == 90 and added > 0 else MatchPeriod.SECOND_HALF
    return MatchPeriod.UNKNOWN


@dataclass(frozen=True)
class EventTime:
    elapsed: Optional[int]
    extra: Optional[int]
    clock_minute: Optional[int]
    display_minute: str
    period: MatchPeriod

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["period"] = self.period.value
        return data


def event_time_from_event(event: Mapping[str, Any], fixture_context: Optional[Mapping[str, Any]] = None) -> EventTime:
    raw = event.get("raw") if isinstance(event.get("raw"), Mapping) else event
    time_block = raw.get("time") if isinstance(raw.get("time"), Mapping) else {}
    elapsed = _int(time_block.get("elapsed"))
    if elapsed is None:
        elapsed = _int(event.get("elapsed"))
    if elapsed is None:
        elapsed = _int(event.get("minute"))
    extra = _int(time_block.get("extra"))
    if extra is None:
        extra = _int(event.get("extra"))
    if extra is None:
        extra = _int(event.get("second"))

    context = fixture_context or {}
    status = context.get("status_short") or context.get("fixture_status_short") or context.get("status")
    event_period = raw.get("period") or event.get("period") or context.get("event_period")
    period = classify_match_period(
        fixture_status_short=_status(status), elapsed=elapsed, extra=extra, event_period=event_period
    )
    clock = None if elapsed is None else elapsed + max(0, extra or 0)
    display = "?" if elapsed is None else (f"{elapsed}+{extra}" if extra and extra > 0 else str(elapsed))
    return EventTime(elapsed=elapsed, extra=extra, clock_minute=clock, display_minute=display, period=period)


def format_event_minute(event_time: EventTime, suffix: str = "'") -> str:
    return f"{event_time.display_minute}{suffix}"


def _goal_text(event: Mapping[str, Any]) -> Tuple[str, str]:
    raw = event.get("raw") if isinstance(event.get("raw"), Mapping) else {}
    event_type = str(event.get("type") or raw.get("type") or event.get("event_type") or "").lower()
    fields = (
        event.get("detail"), event.get("details"), event.get("comment"), event.get("comments"),
        event.get("note"), raw.get("detail"), raw.get("comments"), raw.get("period"),
    )
    return event_type, " ".join(str(value or "") for value in fields).lower()


def classify_goal_scope(event: Mapping[str, Any], fixture_context: Optional[Mapping[str, Any]] = None) -> GoalScope:
    event_type, detail = _goal_text(event)
    cancelled = (
        "cancel" in detail or "disallow" in detail or "no goal" in detail
        or "ruled out" in detail or "offside" in detail or "var no goal" in detail
    )
    missed_penalty = "missed penalty" in detail or "penalty missed" in detail
    is_goal = "goal" in event_type or event_type == "own_goal"
    if cancelled or missed_penalty or not is_goal:
        return GoalScope.INVALID_OR_CANCELLED

    event_time = event_time_from_event(event, fixture_context)
    hint = detail + " " + str(event.get("period") or "").lower()
    if "shootout" in hint or "penalty shootout" in hint:
        return GoalScope.PENALTY_SHOOTOUT

    status = _status((fixture_context or {}).get("status_short") or (fixture_context or {}).get("status"))
    if status in SHOOTOUT_STATUSES and event_time.elapsed is not None and event_time.elapsed >= 120:
        return GoalScope.PENALTY_SHOOTOUT
    if event_time.elapsed is not None and event_time.elapsed <= 45:
        return GoalScope.FIRST_HALF
    if event_time.elapsed is not None and event_time.elapsed <= 90:
        return GoalScope.SECOND_HALF_NORMAL_TIME
    if event_time.elapsed is not None and 90 < event_time.elapsed <= 120:
        if status in EXTRA_TIME_STATUSES or status in SHOOTOUT_STATUSES or event_time.period in {
            MatchPeriod.EXTRA_TIME_FIRST_HALF,
            MatchPeriod.EXTRA_TIME_FIRST_HALF_STOPPAGE,
            MatchPeriod.EXTRA_TIME_SECOND_HALF,
            MatchPeriod.EXTRA_TIME_SECOND_HALF_STOPPAGE,
            MatchPeriod.EXTRA_TIME_FINISHED,
        }:
            return GoalScope.EXTRA_TIME
        return GoalScope.UNKNOWN
    if event_time.period == MatchPeriod.PENALTY_SHOOTOUT:
        return GoalScope.PENALTY_SHOOTOUT
    return GoalScope.UNKNOWN


def is_normal_time_goal(event: Mapping[str, Any], fixture_context: Optional[Mapping[str, Any]] = None) -> bool:
    return classify_goal_scope(event, fixture_context) in {GoalScope.FIRST_HALF, GoalScope.SECOND_HALF_NORMAL_TIME}


def is_extra_time_goal(event: Mapping[str, Any], fixture_context: Optional[Mapping[str, Any]] = None) -> bool:
    return classify_goal_scope(event, fixture_context) == GoalScope.EXTRA_TIME


def is_penalty_shootout_event(event: Mapping[str, Any], fixture_context: Optional[Mapping[str, Any]] = None) -> bool:
    return classify_goal_scope(event, fixture_context) == GoalScope.PENALTY_SHOOTOUT


def fixture_status_elapsed_extra(fixture: Mapping[str, Any]) -> Tuple[str, Optional[int], Optional[int]]:
    root = fixture.get("fixture") if isinstance(fixture.get("fixture"), Mapping) else fixture
    status_obj = root.get("status") or fixture.get("status") or {}
    status = _status(status_obj)
    elapsed = _int(status_obj.get("elapsed")) if isinstance(status_obj, Mapping) else _int(root.get("elapsed"))
    extra = _int(status_obj.get("extra")) if isinstance(status_obj, Mapping) else _int(root.get("extra"))
    if elapsed is None and isinstance(root.get("elapsed"), Mapping):
        elapsed = _int(root["elapsed"].get("value"))
    if extra is None and isinstance(root.get("extra_time"), Mapping):
        extra = _int(root["extra_time"].get("value"))
    return status, elapsed, extra


def is_extra_time_active(fixture: Mapping[str, Any]) -> bool:
    status, elapsed, extra = fixture_status_elapsed_extra(fixture)
    return classify_match_period(fixture_status_short=status, elapsed=elapsed, extra=extra) in {
        MatchPeriod.EXTRA_TIME_FIRST_HALF,
        MatchPeriod.EXTRA_TIME_FIRST_HALF_STOPPAGE,
        MatchPeriod.EXTRA_TIME_BREAK,
        MatchPeriod.EXTRA_TIME_SECOND_HALF,
        MatchPeriod.EXTRA_TIME_SECOND_HALF_STOPPAGE,
    }


def is_shootout_active(fixture: Mapping[str, Any]) -> bool:
    status, elapsed, extra = fixture_status_elapsed_extra(fixture)
    return classify_match_period(
        fixture_status_short=status, elapsed=elapsed, extra=extra
    ) == MatchPeriod.PENALTY_SHOOTOUT


def is_match_fully_finished(fixture: Mapping[str, Any]) -> bool:
    status, _, _ = fixture_status_elapsed_extra(fixture)
    return status in FULLY_FINISHED_STATUSES


def has_normal_time_finished(fixture: Mapping[str, Any], previous_state: Optional[Mapping[str, Any]] = None) -> bool:
    status, elapsed, extra = fixture_status_elapsed_extra(fixture)
    if status in NORMAL_TIME_BOUNDARY_STATUSES or status in FULLY_FINISHED_STATUSES:
        return True
    period = classify_match_period(fixture_status_short=status, elapsed=elapsed, extra=extra)
    return period in {
        MatchPeriod.NORMAL_TIME_FINISHED,
        MatchPeriod.EXTRA_TIME_FIRST_HALF,
        MatchPeriod.EXTRA_TIME_FIRST_HALF_STOPPAGE,
        MatchPeriod.EXTRA_TIME_BREAK,
        MatchPeriod.EXTRA_TIME_SECOND_HALF,
        MatchPeriod.EXTRA_TIME_SECOND_HALF_STOPPAGE,
        MatchPeriod.EXTRA_TIME_FINISHED,
        MatchPeriod.PENALTY_SHOOTOUT,
    }


def _score_pair(value: Any) -> Optional[Tuple[int, int]]:
    if not isinstance(value, Mapping):
        return None
    home, away = _int(value.get("home")), _int(value.get("away"))
    return (home, away) if home is not None and away is not None else None


def normalize_score_blocks(raw_fixture: Mapping[str, Any]) -> Dict[str, Any]:
    score = raw_fixture.get("score") if isinstance(raw_fixture.get("score"), Mapping) else {}
    goals = raw_fixture.get("goals") if isinstance(raw_fixture.get("goals"), Mapping) else {}
    status, _, _ = fixture_status_elapsed_extra(raw_fixture)
    halftime = _score_pair(score.get("halftime"))
    fulltime = _score_pair(score.get("fulltime"))
    extratime = _score_pair(score.get("extratime"))
    penalty = _score_pair(score.get("penalty"))
    current = _score_pair(goals)

    normal_time = fulltime
    normal_source = "score.fulltime" if fulltime is not None else "unknown"
    if normal_time is None and status == "FT" and current is not None:
        normal_time, normal_source = current, "general_goals"

    return {
        "halftime": {"home": halftime[0], "away": halftime[1]} if halftime else None,
        "normal_time": (
            {"home": normal_time[0], "away": normal_time[1], "source": normal_source}
            if normal_time else None
        ),
        "after_extra_time": (
            {"home": extratime[0], "away": extratime[1], "source": "score.extratime"}
            if extratime else None
        ),
        "penalty": (
            {"home": penalty[0], "away": penalty[1], "source": "score.penalty"}
            if penalty else None
        ),
        "current": {"home": current[0], "away": current[1]} if current else None,
        "normal_time_source": normal_source,
    }
