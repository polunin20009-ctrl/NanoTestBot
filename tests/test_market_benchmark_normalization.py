from __future__ import annotations

import math

import pytest

from market_benchmark import (
    LiveGoalMarketSpec,
    normalize_live_goal_markets,
    remove_vig_proportional,
)


CAPTURED = "2026-09-08T12:00:00+00:00"


def _values(
    line: str = "2.5",
    *,
    over: str = "1.40",
    under: str = "3.20",
    suspended_over: bool = False,
    suspended_under: bool = False,
) -> list[dict]:
    return [
        {
            "value": "Over",
            "odd": over,
            "handicap": line,
            "main": None,
            "suspended": suspended_over,
        },
        {
            "value": "Under",
            "odd": under,
            "handicap": line,
            "main": None,
            "suspended": suspended_under,
        },
    ]


def _entry(
    fixture_id: int = 123,
    *,
    score: tuple[int, int] = (1, 1),
    status: dict | None = None,
    odds: list[dict] | None = None,
    update: str = "2026-09-08T11:59:58+00:00",
) -> dict:
    return {
        "fixture": {
            "id": fixture_id,
            "status": {"long": "Second Half", "elapsed": 55},
        },
        "league": {"id": 39, "season": 2026},
        "teams": {
            "home": {"id": 1, "goals": score[0]},
            "away": {"id": 2, "goals": score[1]},
        },
        "status": status
        if status is not None
        else {"stopped": False, "blocked": False, "finished": False},
        "update": update,
        "odds": odds
        if odds is not None
        else [{"id": 25, "name": "Match Goals", "values": _values()}],
    }


def _payload(*entries: dict, errors=None) -> dict:
    return {
        "get": "odds/live",
        "errors": [] if errors is None else errors,
        "results": len(entries),
        "paging": {"current": 1, "total": 1},
        "response": list(entries),
    }


def test_normalizes_exact_one_more_goal_pair_and_removes_vig() -> None:
    result = normalize_live_goal_markets(
        _payload(_entry()),
        captured_at_utc=CAPTURED,
        fixture_id=123,
        current_goals=2,
    )

    assert result.accepted_count == 1
    assert result.rejections == ()
    record = result.records[0]
    assert record["record_type"] == "market_odds_snapshot"
    assert record["fixture_id"] == 123
    assert record["current_goals"] == 2
    assert record["line"] == 2.5
    assert record["bet_id"] == 25
    assert record["target_event"] == "at_least_one_more_goal_to90_normal_time"
    assert record["shadow_only"] is True
    assert record["production_applied"] is False
    assert record["market"]["shadow_only"] is True
    assert record["source_status"] == {
        "blocked": False,
        "stopped": False,
        "finished": False,
    }
    expected = (1 / 1.4) / ((1 / 1.4) + (1 / 3.2))
    assert record["no_vig_over_probability"] == pytest.approx(expected)
    assert record["no_vig_under_probability"] == pytest.approx(1 - expected)
    assert record["market"]["fair_probability_goal_to90"] == pytest.approx(
        expected
    )


def test_match_goals_is_preferred_to_over_under_line_deterministically() -> None:
    odds = [
        {"id": 36, "name": "Over/Under Line", "values": _values(over="1.2")},
        {"id": 25, "name": "Match Goals", "values": _values(over="1.8")},
    ]

    record = normalize_live_goal_markets(
        _payload(_entry(odds=odds)), captured_at_utc=CAPTURED
    ).records[0]

    assert record["bet_id"] == 25
    assert record["over_decimal"] == 1.8
    assert record["eligible_market_candidate_count"] == 2


def test_main_quote_wins_duplicate_values_within_selected_market() -> None:
    values = _values()
    values.extend(
        [
            {
                "value": "Over",
                "odd": "1.75",
                "handicap": "2.5",
                "main": True,
                "suspended": False,
            },
            {
                "value": "Under",
                "odd": "2.10",
                "handicap": "2.5",
                "main": True,
                "suspended": False,
            },
        ]
    )
    entry = _entry(odds=[{"id": 25, "name": "Match Goals", "values": values}])

    record = normalize_live_goal_markets(
        _payload(entry), captured_at_utc=CAPTURED
    ).records[0]

    assert record["over_decimal"] == 1.75
    assert record["under_decimal"] == 2.10
    assert record["selection_main_quote_count"] == 2


@pytest.mark.parametrize("flag", ["blocked", "stopped", "finished"])
def test_rejects_non_live_market_status_fail_closed(flag: str) -> None:
    status = {"stopped": False, "blocked": False, "finished": False}
    status[flag] = True

    result = normalize_live_goal_markets(
        _payload(_entry(status=status)), captured_at_utc=CAPTURED
    )

    assert result.records == ()
    assert result.rejections[0]["reason"] == f"market_{flag}"
    assert result.rejections[0]["shadow_only"] is True
    assert result.rejections[0]["production_applied"] is False


def test_rejects_missing_or_non_boolean_status_flags() -> None:
    result = normalize_live_goal_markets(
        _payload(
            _entry(status={"stopped": False, "blocked": "false", "finished": False})
        ),
        captured_at_utc=CAPTURED,
    )

    assert result.records == ()
    assert result.rejections[0]["reason"] == "market_status_blocked_invalid"


def test_suspended_preferred_market_falls_back_to_complete_active_market() -> None:
    odds = [
        {
            "id": 25,
            "name": "Match Goals",
            "values": _values(suspended_over=True),
        },
        {"id": 36, "name": "Over/Under Line", "values": _values(over="1.7")},
    ]

    result = normalize_live_goal_markets(
        _payload(_entry(odds=odds)), captured_at_utc=CAPTURED
    )

    assert len(result.records) == 1
    assert result.records[0]["bet_id"] == 36
    assert result.records[0]["over_decimal"] == 1.7
    assert result.rejections == ()


def test_never_pairs_different_lines_or_a_suspended_side() -> None:
    mixed_lines = [
        {**_values("2.5")[0]},
        {**_values("3.5")[1]},
    ]
    suspended = _values("2.5", suspended_under=True)

    mixed_result = normalize_live_goal_markets(
        _payload(
            _entry(
                odds=[
                    {"id": 25, "name": "Match Goals", "values": mixed_lines}
                ]
            )
        ),
        captured_at_utc=CAPTURED,
    )
    suspended_result = normalize_live_goal_markets(
        _payload(
            _entry(
                odds=[{"id": 25, "name": "Match Goals", "values": suspended}]
            )
        ),
        captured_at_utc=CAPTURED,
    )

    assert mixed_result.records == ()
    assert "target_under_quote_missing" in {
        item["reason"] for item in mixed_result.rejections
    }
    assert suspended_result.records == ()
    assert "target_under_suspended" in {
        item["reason"] for item in suspended_result.rejections
    }


def test_ambiguous_duplicate_quotes_are_rejected_without_guessing() -> None:
    values = _values()
    values.append(
        {
            "value": "Over",
            "odd": "1.75",
            "handicap": "2.5",
            "main": False,
            "suspended": False,
        }
    )
    result = normalize_live_goal_markets(
        _payload(
            _entry(
                odds=[{"id": 25, "name": "Match Goals", "values": values}]
            )
        ),
        captured_at_utc=CAPTURED,
    )

    assert result.records == ()
    assert result.rejections[0]["reason"] == (
        "target_over_ambiguous_duplicate_quotes"
    )


def test_full_page_normalization_and_optional_fixture_score_guard() -> None:
    result = normalize_live_goal_markets(
        _payload(
            _entry(123, score=(1, 1)),
            _entry(456, score=(0, 0), odds=[
                {"id": 25, "name": "Match Goals", "values": _values("0.5")}
            ]),
        ),
        captured_at_utc=CAPTURED,
    )
    mismatch = normalize_live_goal_markets(
        _payload(_entry(123, score=(1, 1))),
        captured_at_utc=CAPTURED,
        fixture_id=123,
        current_goals=1,
    )

    assert {record["fixture_id"] for record in result.records} == {123, 456}
    assert mismatch.records == ()
    assert mismatch.rejections[0]["reason"] == "current_goals_mismatch"


def test_identical_provider_snapshot_has_stable_key_across_poll_times() -> None:
    first = normalize_live_goal_markets(
        _payload(_entry()), captured_at_utc="2026-09-08T12:00:00Z"
    ).records[0]
    second = normalize_live_goal_markets(
        _payload(_entry()), captured_at_utc="2026-09-08T12:00:10Z"
    ).records[0]

    assert first["record_key"] == second["record_key"]


def test_live_market_catalog_is_explicitly_configurable() -> None:
    custom = (LiveGoalMarketSpec(999, "Verified Goal Total", 0),)
    entry = _entry(
        odds=[{"id": 999, "name": "Verified Goal Total", "values": _values()}]
    )

    result = normalize_live_goal_markets(
        _payload(entry), captured_at_utc=CAPTURED, market_specs=custom
    )

    assert result.records[0]["bet_id"] == 999


def test_api_errors_and_invalid_odds_are_fail_closed() -> None:
    api_error = normalize_live_goal_markets(
        _payload(errors={"rateLimit": "too many"}), captured_at_utc=CAPTURED
    )
    invalid_odds = normalize_live_goal_markets(
        _payload(
            _entry(
                odds=[
                    {
                        "id": 25,
                        "name": "Match Goals",
                        "values": _values(over="1"),
                    }
                ]
            )
        ),
        captured_at_utc=CAPTURED,
    )

    assert api_error.records == ()
    assert api_error.rejections[0]["reason"] == "api_error"
    assert invalid_odds.records == ()
    assert invalid_odds.rejections[0]["reason"] == "target_over_odds_invalid"


def test_missing_provider_update_is_rejected_for_causal_alignment() -> None:
    result = normalize_live_goal_markets(
        _payload(_entry(update="not-a-timestamp")),
        captured_at_utc=CAPTURED,
    )

    assert result.records == ()
    assert result.rejections[0]["reason"] == (
        "provider_update_missing_or_invalid"
    )


def test_remove_vig_validates_inputs_and_sums_to_one() -> None:
    probabilities = remove_vig_proportional(1.4, 3.2)

    assert probabilities["fair_over"] + probabilities["fair_under"] == pytest.approx(1)
    assert probabilities["overround"] == pytest.approx(
        probabilities["raw_implied_sum"] - 1
    )
    with pytest.raises(ValueError):
        remove_vig_proportional(1, 2)
    with pytest.raises(ValueError):
        remove_vig_proportional(math.nan, 2)
