from __future__ import annotations

import NanoTest as nanotest
from scripts.backtest_second_half import _join_signals


def _resolve(
    *,
    clocks=None,
    timeline=None,
    final=(2, 0),
    exact_count=None,
):
    return nanotest.resolve_goal_within_horizon(
        signal_minute=54,
        snapshot_score=(1, 0),
        horizon_minutes=15,
        exact_goal_clocks=clocks or [],
        score_timeline=timeline or [],
        normal_time_score=final,
        exact_goal_count_after_signal=exact_count,
    )


def test_exact_goal_inside_horizon_is_true() -> None:
    label = _resolve(clocks=[63], exact_count=1)

    assert label["value"] is True
    assert label["source"] == "event_exact"
    assert label["quality"] == "exact"


def test_complete_exact_events_after_horizon_are_false() -> None:
    label = _resolve(clocks=[72], exact_count=1)

    assert label["value"] is False
    assert label["source"] == "event_exact_complete"


def test_timeline_score_growth_by_horizon_is_true() -> None:
    label = _resolve(
        timeline=[
            {"minute": 54, "home": 1, "away": 0},
            {"minute": 60, "home": 1, "away": 0},
            {"minute": 63, "home": 2, "away": 0},
        ]
    )

    assert label["value"] is True
    assert label["source"] == "score_timeline"
    assert label["goal_interval_start_minute"] == 60
    assert label["goal_interval_end_minute"] == 63


def test_timeline_baseline_through_horizon_is_false() -> None:
    label = _resolve(
        timeline=[
            {"minute": 54, "home": 1, "away": 0},
            {"minute": 69, "home": 1, "away": 0},
            {"minute": 72, "home": 2, "away": 0},
        ]
    )

    assert label["value"] is False
    assert label["source"] == "score_timeline_no_change"


def test_score_growth_across_unobserved_boundary_is_unknown() -> None:
    label = _resolve(
        timeline=[
            {"minute": 54, "home": 1, "away": 0},
            {"minute": 71, "home": 2, "away": 0},
        ]
    )

    assert label["value"] is None
    assert label["source"] == "score_timeline_boundary_gap"


def test_final_unchanged_score_overrides_transient_score_growth() -> None:
    label = _resolve(
        timeline=[
            {"minute": 54, "home": 1, "away": 0},
            {"minute": 63, "home": 2, "away": 0},
        ],
        final=(1, 0),
    )

    assert label["value"] is False
    assert label["source"] == "final_score_no_change"


def test_final_score_growth_without_timing_evidence_is_unknown() -> None:
    label = _resolve()

    assert label["value"] is None
    assert label["source"] == "unknown_timing"


def test_backtest_preserves_unknown_next_15_label() -> None:
    joined = _join_signals(
        [{"signal_id": "s1", "fixture_id": 1}],
        [{"signal_id": "s1", "goal_after_signal": 1, "goal_in_next_15": None}],
    )

    assert joined[0]["goal_after_signal"] == 1
    assert joined[0]["goal_in_next_15"] is None
