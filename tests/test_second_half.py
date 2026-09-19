from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from second_half.aggregate import build_stats_from_history
from second_half.factors import compute_league_2h_factor, compute_score_state_factor, compute_team_2h_factor
from second_half.migrate import migrate_history_file, migrate_legacy_record
from second_half.parser import (
    IncompleteSecondHalfDataError,
    compute_second_half_features_from_fixture,
    parse_goal_events_from_fixture_events,
)
from second_half.smoothing import effective_sample_size, smooth_metric
from second_half import storage
from second_half.storage import append_second_half_history_record, collect_and_store_second_half_history, load_second_half_history_records, reset_second_half_history_cache


def _sample_fixture() -> dict:
    return {
        "fixture": {
            "id": 1001,
            "date": "2026-04-22T16:00:00+00:00",
            "status": {"short": "FT"},
        },
        "league": {
            "id": 218,
            "name": "Bundesliga",
            "type": "League",
            "season": 2026,
            "round": "Regular Season - 1",
        },
        "teams": {
            "home": {"id": 571, "name": "Red Bull Salzburg"},
            "away": {"id": 601, "name": "Austria Vienna"},
        },
        "goals": {"home": 2, "away": 2},
        "score": {
            "halftime": {"home": 1, "away": 1},
            "fulltime": {"home": 2, "away": 2},
        },
    }


def _sample_events() -> list[dict]:
    return [
        {
            "time": {"elapsed": 12, "extra": 0},
            "team": {"id": 571},
            "type": "Goal",
            "detail": "Normal Goal",
            "player": {"id": 1, "name": "Alpha"},
            "assist": {"id": 2, "name": "Beta"},
        },
        {
            "time": {"elapsed": 45, "extra": 2},
            "team": {"id": 601},
            "type": "Goal",
            "detail": "Normal Goal",
            "player": {"id": 3, "name": "Gamma"},
            "assist": {"id": 4, "name": "Delta"},
        },
        {
            "time": {"elapsed": 61, "extra": 0},
            "team": {"id": 571},
            "type": "Goal",
            "detail": "Normal Goal",
            "player": {"id": 5, "name": "Epsilon"},
            "assist": {"id": 6, "name": "Zeta"},
        },
        {
            "time": {"elapsed": 90, "extra": 4},
            "team": {"id": 601},
            "type": "Goal",
            "detail": "Normal Goal",
            "player": {"id": 7, "name": "Eta"},
            "assist": {"id": 8, "name": "Theta"},
        },
        {
            "time": {"elapsed": 70, "extra": 0},
            "team": {"id": 571},
            "type": "Card",
            "detail": "Yellow Card",
            "player": {"id": 9, "name": "Iota"},
        },
    ]


def test_parse_goal_events_from_fixture_events() -> None:
    parsed = parse_goal_events_from_fixture_events(_sample_events())
    assert len(parsed) == 4
    assert parsed[-1]["minute"] == 90
    assert parsed[-1]["extra"] == 4
    assert parsed[-1]["abs_minute"] == 94
    assert all(event["type"] == "Goal" for event in parsed)


def test_compute_second_half_features_from_fixture() -> None:
    features = compute_second_half_features_from_fixture(_sample_fixture(), _sample_events())
    assert features["fixture_id"] == 1001
    assert features["halftime_state"] == "DRAW"
    assert features["goals_2h_home"] == 1
    assert features["goals_2h_away"] == 1
    assert features["goals_2h_total"] == 2
    assert features["goals_after_60_total"] == 2
    assert features["goals_after_75_total"] == 1
    assert features["halftime_score_source"] == "fixture_score_halftime"
    assert len(features["state_transitions_2h"]) == 2
    assert features["state_transitions_2h"][0]["state_before"] == "DRAW"
    assert features["state_transitions_2h"][0]["state_after"] == "LEAD_1"


def test_missing_halftime_requires_complete_events() -> None:
    fixture = _sample_fixture()
    fixture["score"].pop("halftime")

    with pytest.raises(
        IncompleteSecondHalfDataError,
        match="halftime score is unavailable and goal events are incomplete",
    ):
        compute_second_half_features_from_fixture(fixture, None)


def test_missing_halftime_is_derived_from_complete_events() -> None:
    fixture = _sample_fixture()
    fixture["score"].pop("halftime")

    features = compute_second_half_features_from_fixture(fixture, _sample_events())

    assert features["ht_home"] == 1
    assert features["ht_away"] == 1
    assert features["halftime_score_source"] == "complete_goal_events"
    assert features["goals_2h_total"] == 2


def test_shrinkage_formula() -> None:
    weights = [0.6, 0.4]
    n_eff = effective_sample_size(weights)
    raw_metric = 1.5
    league_mean = 1.0
    smoothed = smooth_metric(raw_metric, weights, league_mean, alpha=8.0)
    expected = (n_eff * raw_metric + 8.0 * league_mean) / (n_eff + 8.0)
    assert round(smoothed, 8) == round(expected, 8)


def test_team_2h_factor_bounds() -> None:
    team_payload = {
        "_meta": {"global_avg_2h_goals": 1.2},
        "teams": {
            "571": {
                "sample_matches": 20,
                "weighted_2h_scored_avg_final": 2.4,
                "weighted_2h_conceded_avg_final": 2.0,
                "events_coverage": 1.0,
            },
            "601": {
                "sample_matches": 20,
                "weighted_2h_scored_avg_final": 2.2,
                "weighted_2h_conceded_avg_final": 2.3,
                "events_coverage": 1.0,
            },
        },
    }
    league_payload = {
        "_meta": {"global_avg_2h_goals": 1.2},
        "leagues": {
            "218": {
                "avg_2h_goals": 1.0,
                "avg_goals_after_60": 0.6,
                "sample_matches": 200,
                "events_coverage": 1.0,
            }
        },
    }
    result = compute_team_2h_factor(571, 601, 218, team_stats_data=team_payload, league_stats_data=league_payload)
    assert 0.90 <= result["factor"] <= 1.12
    assert result["factor"] == 1.12


def test_league_2h_factor_bounds() -> None:
    league_payload = {
        "_meta": {"global_avg_2h_goals": 1.0, "global_avg_goals_after_60": 0.4},
        "leagues": {
            "218": {
                "avg_2h_goals": 2.5,
                "avg_goals_after_60": 1.1,
                "sample_matches": 200,
            }
        },
    }
    result = compute_league_2h_factor(218, league_stats_data=league_payload)
    assert 0.95 <= result["factor"] <= 1.08
    assert result["factor"] == 1.08


def test_score_state_factor_bounds() -> None:
    team_payload = {
        "_meta": {},
        "teams": {
            "571": {
                "score_state": {
                    "DRAW": {"n": 20, "team_scored_again_rate": 0.95, "team_conceded_rate": 0.85}
                }
            },
            "601": {
                "score_state": {
                    "DRAW": {"n": 20, "team_scored_again_rate": 0.9, "team_conceded_rate": 0.9}
                }
            },
        },
    }
    league_payload = {
        "_meta": {
            "global_two_h_goal_match_rate": 0.55,
            "global_team_scored_in_2h_rate": 0.4,
            "global_team_conceded_in_2h_rate": 0.4,
        },
        "leagues": {
            "218": {
                "two_h_goal_match_rate": 0.55,
                "team_scored_in_2h_rate_mean": 0.4,
                "team_conceded_in_2h_rate_mean": 0.4,
            }
        },
    }
    result = compute_score_state_factor(1, 1, 571, 601, 218, team_stats_data=team_payload, league_stats_data=league_payload)
    assert 0.90 <= result["factor"] <= 1.12
    assert result["factor"] == 1.12


def test_score_state_factor_neutral_without_samples() -> None:
    team_payload = {"_meta": {}, "teams": {}}
    league_payload = {
        "_meta": {
            "global_two_h_goal_match_rate": 0.55,
            "global_team_scored_in_2h_rate": 0.4,
            "global_team_conceded_in_2h_rate": 0.4,
        },
        "leagues": {},
    }
    result = compute_score_state_factor(0, 0, 571, 601, 218, team_stats_data=team_payload, league_stats_data=league_payload)
    assert result["factor"] == 1.0


def test_score_state_factor_is_neutral_at_league_team_rates() -> None:
    team_payload = {
        "_meta": {},
        "teams": {
            "571": {
                "score_state": {
                    "DRAW": {
                        "n": 20,
                        "team_scored_again_rate": 0.4,
                        "team_conceded_rate": 0.4,
                    }
                }
            },
            "601": {
                "score_state": {
                    "DRAW": {
                        "n": 20,
                        "team_scored_again_rate": 0.4,
                        "team_conceded_rate": 0.4,
                    }
                }
            },
        },
    }
    league_payload = {
        "_meta": {
            "global_two_h_goal_match_rate": 0.55,
            "global_team_scored_in_2h_rate": 0.4,
            "global_team_conceded_in_2h_rate": 0.4,
        },
        "leagues": {
            "218": {
                "sample_matches": 100,
                "two_h_goal_match_rate": 0.55,
                "team_scored_in_2h_rate_mean": 0.4,
                "team_conceded_in_2h_rate_mean": 0.4,
            }
        },
    }

    result = compute_score_state_factor(
        1,
        1,
        571,
        601,
        218,
        team_stats_data=team_payload,
        league_stats_data=league_payload,
    )

    assert result["factor"] == 1.0
    assert result["baseline_score_state_total"] == 0.8


def test_dedup_second_half_history(tmp_path: Path) -> None:
    history_path = tmp_path / "second_half_history.jsonl"
    reset_second_half_history_cache(str(history_path))

    stored_first, _ = collect_and_store_second_half_history(_sample_fixture(), _sample_events(), path=str(history_path))
    stored_second, _ = collect_and_store_second_half_history(_sample_fixture(), _sample_events(), path=str(history_path))
    records = load_second_half_history_records(str(history_path))

    assert stored_first is True
    assert stored_second is False
    assert len(records) == 1


def test_missed_penalty_is_not_a_goal() -> None:
    events = [
        {"time": {"elapsed": 10}, "team": {"id": 571}, "type": "Goal", "detail": "Missed Penalty"},
        {"time": {"elapsed": 70}, "team": {"id": 571}, "type": "Goal", "detail": "Normal Goal"},
    ]
    fixture = _sample_fixture()
    fixture["goals"] = {"home": 1, "away": 0}
    fixture["score"] = {"halftime": {"home": 0, "away": 0}, "fulltime": {"home": 1, "away": 0}}

    features = compute_second_half_features_from_fixture(fixture, events)

    assert len(features["goal_events"]) == 1
    assert features["events_complete"] is True
    assert features["goals_after_60_total"] == 1


def test_extra_time_is_excluded_and_normal_time_score_is_used() -> None:
    fixture = _sample_fixture()
    fixture["fixture"]["status"] = {"short": "AET"}
    fixture["goals"] = {"home": 3, "away": 2}
    fixture["score"] = {
        "halftime": {"home": 1, "away": 0},
        "fulltime": {"home": 2, "away": 2},
        "extratime": {"home": 3, "away": 2},
    }
    events = [
        {"time": {"elapsed": 2}, "team": {"id": 571}, "type": "Goal", "detail": "Penalty"},
        {"time": {"elapsed": 48}, "team": {"id": 601}, "type": "Goal", "detail": "Normal Goal"},
        {"time": {"elapsed": 63}, "team": {"id": 601}, "type": "Goal", "detail": "Normal Goal"},
        {"time": {"elapsed": 90}, "team": {"id": 571}, "type": "Goal", "detail": "Normal Goal"},
        {"time": {"elapsed": 116}, "team": {"id": 571}, "type": "Goal", "detail": "Normal Goal"},
    ]

    features = compute_second_half_features_from_fixture(fixture, events)

    assert (features["ft_home"], features["ft_away"]) == (2, 2)
    assert features["goals_2h_total"] == 3
    assert [event["minute"] for event in features["goal_events"]] == [2, 48, 63, 90]
    assert features["goals_after_60_total"] == 2


def test_incomplete_events_produce_unknown_late_goal_metrics() -> None:
    features = compute_second_half_features_from_fixture(_sample_fixture(), _sample_events()[:1])

    assert features["events_response_available"] is True
    assert features["events_complete"] is False
    assert features["events_available"] is False
    assert features["goals_after_60_total"] is None
    assert features["goals_after_75_total"] is None
    assert features["state_transitions_2h"] == []


def test_invalid_zero_identifiers_are_rejected() -> None:
    fixture = _sample_fixture()
    fixture["fixture"]["id"] = 0

    with pytest.raises(ValueError, match="positive"):
        compute_second_half_features_from_fixture(fixture, _sample_events())


def test_observed_finish_time_replaces_estimate_for_live_collection() -> None:
    features = compute_second_half_features_from_fixture(
        _sample_fixture(),
        _sample_events(),
        observed_finished_at_utc="2026-04-22T17:55:00+00:00",
    )

    assert features["finished_at"] == "2026-04-22T17:55:00+00:00"
    assert features["finished_at_source"] == "observed_terminal_status"


def test_migration_sanitizes_legacy_record() -> None:
    legacy = compute_second_half_features_from_fixture(_sample_fixture(), _sample_events())
    legacy.pop("schema_version")
    legacy["league_type"] = ""
    legacy["goal_events"].append({
        "minute": 116, "extra": 0, "abs_minute": 116,
        "team_id": 571, "team_side": "home", "type": "Goal", "detail": "Normal Goal",
    })
    legacy["goal_events"].append({
        "minute": 73, "extra": 0, "abs_minute": 73,
        "team_id": 571, "team_side": "home", "type": "Goal", "detail": "Missed Penalty",
    })

    migrated = migrate_legacy_record(legacy)

    assert migrated is not None
    assert migrated["schema_version"] == 2
    assert all(event["minute"] <= 90 for event in migrated["goal_events"])
    assert all("missed" not in event["detail"].lower() for event in migrated["goal_events"])
    assert migrated["league_type"] == "League"


def test_new_schema_can_correct_legacy_fixture(tmp_path: Path) -> None:
    path = tmp_path / "second_half_history.jsonl"
    legacy = compute_second_half_features_from_fixture(_sample_fixture(), _sample_events())
    legacy.pop("schema_version")
    current = compute_second_half_features_from_fixture(_sample_fixture(), _sample_events())

    assert append_second_half_history_record(legacy, str(path)) is True
    assert append_second_half_history_record(current, str(path)) is True
    records = load_second_half_history_records(str(path))

    assert len(records) == 1
    assert records[0]["schema_version"] == 2


def test_history_rotation_uses_gzip_and_reader_is_transparent(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / "second_half_history.jsonl"
    monkeypatch.setattr(storage, "HISTORY_ROTATE_MAX_BYTES", 300)
    first = compute_second_half_features_from_fixture(_sample_fixture(), _sample_events())
    second_fixture = deepcopy(_sample_fixture())
    second_fixture["fixture"]["id"] = 1002
    second = compute_second_half_features_from_fixture(second_fixture, _sample_events())

    assert append_second_half_history_record(first, str(path)) is True
    assert append_second_half_history_record(second, str(path)) is True

    assert list(tmp_path.glob("second_half_history.*.jsonl.gz"))
    assert len(load_second_half_history_records(str(path))) == 2


def test_missing_event_metrics_do_not_become_zero_in_team_aggregation() -> None:
    complete = compute_second_half_features_from_fixture(_sample_fixture(), _sample_events())
    complete["goals_after_60_total"] = 2
    complete["goals_after_75_total"] = 1
    incomplete = deepcopy(complete)
    incomplete["fixture_id"] = 1002
    incomplete["events_available"] = False
    incomplete["events_complete"] = False
    incomplete["goals_after_60_total"] = None
    incomplete["goals_after_75_total"] = None

    payloads = build_stats_from_history([complete, incomplete])
    home = payloads["team_payload"]["teams"]["571"]

    assert home["weighted_goals_after_60_raw"] == 2.0
    assert home["weighted_goals_after_75_raw"] == 1.0


def test_file_migration_creates_gzip_backup_and_is_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "second_half_history.jsonl"
    legacy = compute_second_half_features_from_fixture(_sample_fixture(), _sample_events())
    legacy.pop("schema_version")
    invalid = dict(legacy)
    invalid["fixture_id"] = 0
    path.write_text(
        "\n".join(json.dumps(item) for item in (legacy, invalid)) + "\n",
        encoding="utf-8",
    )

    first = migrate_history_file(str(path))
    second = migrate_history_file(str(path))
    records = load_second_half_history_records(str(path))

    assert first["source_records"] == 2
    assert first["migrated_records"] == 1
    assert first["dropped_records"] == 1
    assert Path(first["backup"]).suffix == ".gz"
    assert len(records) == 1
    assert records[0]["schema_version"] == 2
    assert second["already_current"] is True
