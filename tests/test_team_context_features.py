"""Contracts for team / competition class-of-situation research features."""
from __future__ import annotations

import pytest

from wide_research.features import ALLOWED_FEATURES, extract_features
from wide_research.rules import Clause
from wide_research.team_context import (
    TEAM_CONTEXT_FEATURE_NAMES,
    build_team_context_payload,
    compute_team_context_values,
)


def _snapshot(
    *,
    score_home: int = 0,
    score_away: int = 1,
    is_cup: bool | None = False,
    league_type: str = "League",
    league_name: str = "Premier League",
    home_attack: float = 1.15,
    home_defense: float = 0.95,
    away_attack: float = 1.05,
    away_defense: float = 1.10,
    home_matches: int = 10,
    away_matches: int = 10,
) -> dict:
    return {
        "observation_id": "1:55:WIDE_MONITOR:v1",
        "fixture_id": 1,
        "created_at_utc": "2026-11-04T18:00:00+00:00",
        "minute": 55,
        "match": {
            "score_home": score_home,
            "score_away": score_away,
            "is_cup": is_cup,
            "league_type": league_type,
            "league_name": league_name,
        },
        "team_context": build_team_context_payload(
            home_attack_factor=home_attack,
            home_defense_factor=home_defense,
            away_attack_factor=away_attack,
            away_defense_factor=away_defense,
            team_mix_factor=(home_attack * away_defense + away_attack * home_defense) / 2.0,
            home_matches=home_matches,
            away_matches=away_matches,
            league_factor=1.0,
        ),
        "features": {},
        "probabilities": {},
        "raw_metrics": {},
        "availability": {},
    }


def test_team_context_feature_names_are_allowlisted() -> None:
    for name in TEAM_CONTEXT_FEATURE_NAMES:
        assert name in ALLOWED_FEATURES
        Clause(name, ">=", 1.0)


def test_attack_mix_and_quality_gap_from_strength_inputs() -> None:
    values = compute_team_context_values(_snapshot())
    assert values["team_context.v1.attack_mix"] == pytest.approx(1.13125)
    assert values["matchup.v1.quality_gap"] == pytest.approx(
        abs(1.15 * 1.10 - 1.05 * 0.95)
    )


def test_trailing_favorite_when_stronger_side_is_behind() -> None:
    # Home edge 1.15*1.10=1.265 > away edge 1.05*0.95=0.9975, home trails 0-1.
    values = compute_team_context_values(_snapshot(score_home=0, score_away=1))
    assert values["team_context.v1.trailing_favorite"] == 1.0
    assert values["team_context.v1.away_urgency"] == 0.0

    values_leading = compute_team_context_values(_snapshot(score_home=2, score_away=1))
    assert values_leading["team_context.v1.trailing_favorite"] == 0.0


def test_away_urgency_when_away_side_trails() -> None:
    values = compute_team_context_values(_snapshot(score_home=2, score_away=1))
    assert values["team_context.v1.away_urgency"] == 1.0


def test_cup_or_europe_from_flag_and_league_name() -> None:
    cup = compute_team_context_values(_snapshot(is_cup=True, league_type="Cup"))
    assert cup["competition.v1.cup_or_europe"] == 1.0

    ucl = compute_team_context_values(
        _snapshot(
            is_cup=None,
            league_type="Unknown",
            league_name="UEFA Champions League",
        )
    )
    assert ucl["competition.v1.cup_or_europe"] == 1.0

    league = compute_team_context_values(
        _snapshot(is_cup=False, league_type="League", league_name="Serie A")
    )
    assert league["competition.v1.cup_or_europe"] == 0.0


def test_strength_features_unavailable_without_sample() -> None:
    values = compute_team_context_values(
        _snapshot(home_matches=1, away_matches=10)
    )
    assert values["team_context.v1.attack_mix"] is None
    assert values["team_context.v1.trailing_favorite"] is None
    assert values["matchup.v1.quality_gap"] is None
    assert values["competition.v1.cup_or_europe"] == 0.0
    assert values["team_context.v1.away_urgency"] == 0.0


def test_extract_features_includes_team_context_atoms() -> None:
    vector = extract_features(_snapshot(is_cup=True, league_type="Cup", league_name="Cup"))
    assert vector["values"]["team_context.v1.trailing_favorite"] == 1.0
    assert vector["values"]["competition.v1.cup_or_europe"] == 1.0
    assert vector["values"]["team_context.v1.attack_mix"] is not None
