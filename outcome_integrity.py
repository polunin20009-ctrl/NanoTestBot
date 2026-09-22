from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional, Tuple


Score = Tuple[int, int]


@dataclass(frozen=True)
class OutcomeIntegrityResult:
    status: str
    goal_to90_normal_time: Optional[bool]
    normal_time_result: str
    goal_result_source: str
    normal_time_goal_count_after_signal: Optional[int]
    score_delta: Optional[int]
    conflict: bool
    conflict_details: Optional[Mapping[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def is_previously_confirmed_win(payload: Optional[Mapping[str, Any]]) -> bool:
    """Read the existing confirmed-WIN flag from a stored outcome or record."""
    if not isinstance(payload, Mapping):
        return False
    nested = payload.get("outcome")
    outcome = nested if isinstance(nested, Mapping) else payload
    if not isinstance(outcome, Mapping):
        return False
    status = str(outcome.get("status") or "").strip().lower()
    if status in {"void", "quarantine", "pending"}:
        return False
    if str(
        outcome.get("normal_time_result") or payload.get("normal_time_result") or ""
    ).upper() == "WIN":
        return True
    if outcome.get("goal_to90_normal_time") is True:
        return True
    return payload.get("goal_to90_normal_time") is True


def resolve_normal_time_outcome(
    signal_score: Score,
    normal_time_final_score: Optional[Score],
    *,
    normal_time_event_count: int = 0,
    previously_confirmed_win: bool = False,
) -> OutcomeIntegrityResult:
    """Resolve TO_90 outcome with a single fail-safe evidence policy.

    A trustworthy normal-time scoreboard is authoritative. Events may prove a
    win when the score is unavailable, but they cannot turn a trustworthy
    unchanged score into a win. A prior confirmed win is never downgraded.
    """
    signal_home, signal_away = (int(signal_score[0]), int(signal_score[1]))
    event_count = max(0, int(normal_time_event_count))
    score_delta: Optional[int] = None
    score_reliable = False
    score_increased = False

    if normal_time_final_score is not None:
        final_home, final_away = (
            int(normal_time_final_score[0]),
            int(normal_time_final_score[1]),
        )
        score_reliable = (
            signal_home >= 0
            and signal_away >= 0
            and final_home >= 0
            and final_away >= 0
            and final_home >= signal_home
            and final_away >= signal_away
        )
        if score_reliable:
            score_delta = (
                final_home + final_away - signal_home - signal_away
            )
            score_increased = score_delta > 0

    event_win = event_count > 0
    prior_win = bool(previously_confirmed_win)
    conflict = bool(
        score_reliable
        and (
            (score_increased and not event_win)
            or (not score_increased and event_win)
        )
    )
    conflict_details = (
        {
            "score_reliable": score_reliable,
            "score_delta": score_delta,
            "normal_time_event_count": event_count,
            "previously_confirmed_win": prior_win,
        }
        if conflict
        else None
    )

    if score_reliable:
        won = bool(score_increased or prior_win)
        source = (
            "previously_confirmed_win"
            if prior_win and not score_increased
            else "score_and_events"
            if score_increased and event_win
            else "score_delta"
            if score_increased
            else "score"
        )
        count = max(event_count, int(score_delta or 0)) if won else 0
        return OutcomeIntegrityResult(
            status="resolved",
            goal_to90_normal_time=won,
            normal_time_result="WIN" if won else "LOSS",
            goal_result_source=source,
            normal_time_goal_count_after_signal=count,
            score_delta=score_delta,
            conflict=conflict,
            conflict_details=conflict_details,
        )

    if event_win or prior_win:
        source = "events" if event_win else "previously_confirmed_win"
        return OutcomeIntegrityResult(
            status="resolved",
            goal_to90_normal_time=True,
            normal_time_result="WIN",
            goal_result_source=source,
            normal_time_goal_count_after_signal=max(event_count, 1),
            score_delta=None,
            conflict=False,
            conflict_details=None,
        )

    return OutcomeIntegrityResult(
        status="quarantine",
        goal_to90_normal_time=None,
        normal_time_result="UNKNOWN",
        goal_result_source="unknown",
        normal_time_goal_count_after_signal=None,
        score_delta=None,
        conflict=False,
        conflict_details={
            "reason": "normal_time_score_unreliable_and_no_positive_evidence"
        },
    )
