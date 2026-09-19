from __future__ import annotations

import pytest

import NanoTest as nanotest


def _identity_fixture(
    *,
    league_id: int | None = 77,
    league_name: str = "Neutral Championship",
    league_type: str | None = None,
) -> dict:
    payload = {
        "league_id": league_id,
        "league_name": league_name,
        "league_country": "Testland",
        "team_home_id": 1,
        "team_home_name": "Home",
        "team_away_id": 2,
        "team_away_name": "Away",
    }
    if league_type is not None:
        payload["league_type"] = league_type
    return payload


@pytest.mark.parametrize(
    ("api_type", "persisted_type", "expected_type", "expected_is_cup"),
    [
        ("Cup", "League", "Cup", True),
        ("League", "Cup", "League", False),
    ],
)
def test_explicit_api_league_type_has_priority_over_persisted_type(
    monkeypatch,
    api_type: str,
    persisted_type: str,
    expected_type: str,
    expected_is_cup: bool,
) -> None:
    monkeypatch.setattr(
        nanotest,
        "leagues_state",
        {"77": {"id": 77, "type": persisted_type}},
    )

    identity = nanotest.build_match_identity_context(
        _identity_fixture(league_type=api_type)
    )

    assert identity["league_type"] == expected_type
    assert identity["is_cup"] is expected_is_cup
    assert identity["is_cup_source"] == "api_type"


@pytest.mark.parametrize(
    ("persisted_type", "expected_is_cup"),
    [("League", False), ("Cup", True)],
)
def test_missing_api_type_uses_persisted_league_type(
    monkeypatch,
    persisted_type: str,
    expected_is_cup: bool,
) -> None:
    monkeypatch.setattr(
        nanotest,
        "leagues_state",
        {"77": {"id": 77, "type": persisted_type}},
    )

    identity = nanotest.build_match_identity_context(_identity_fixture())

    assert identity["league_type"] == persisted_type
    assert identity["is_cup"] is expected_is_cup
    assert identity["is_cup_source"] == "persisted_league"


def test_persisted_type_has_priority_over_name_heuristic(monkeypatch) -> None:
    monkeypatch.setattr(
        nanotest,
        "leagues_state",
        {"77": {"id": 77, "type": "League"}},
    )

    identity = nanotest.build_match_identity_context(
        _identity_fixture(league_name="Cup-shaped League Name")
    )

    assert (
        identity["league_type"],
        identity["is_cup"],
        identity["is_cup_source"],
    ) == ("League", False, "persisted_league")


def test_name_heuristic_and_genuinely_unknown_identity_remain_distinct(
    monkeypatch,
) -> None:
    monkeypatch.setattr(nanotest, "leagues_state", {})

    cup = nanotest.build_match_identity_context(
        _identity_fixture(league_id=None, league_name="Regional Cup")
    )
    unknown = nanotest.build_match_identity_context(
        _identity_fixture(league_id=None, league_name="unknown")
    )

    assert (cup["league_type"], cup["is_cup"], cup["is_cup_source"]) == (
        "Cup",
        True,
        "name_heuristic",
    )
    assert (
        unknown["league_type"],
        unknown["is_cup"],
        unknown["is_cup_source"],
    ) == ("Unknown", None, "unknown")


def test_decision_snapshot_keeps_runtime_hashes_and_cup_source() -> None:
    snapshot = nanotest.build_decision_snapshot(
        fixture_id=101,
        minute=46,
        window_name="WINDOW_1",
        match_identity={
            "league_type": "League",
            "is_cup": False,
            "is_cup_source": "persisted_league",
        },
        score_home=0,
        score_away=0,
        probability_result={"prob_next_15": 50.0, "prob_to90": 80.0},
        threshold_result={"threshold": 80.0, "fallback_threshold": 75.0},
        threshold_next15=40.0,
        readiness_result={"passed": True},
        live_gate_result={"required": False},
        anti_garbage_passed=True,
        final_decision="ALLOW",
        block_reason=None,
        factor_context={},
    )

    assert snapshot["model_version"] == "45_plus_v2"
    assert snapshot["runtime"]["model_version"] == "45_plus_v2"
    assert len(snapshot["runtime"]["config_hash"]) == 16
    assert len(snapshot["runtime"]["code_hash"]) == 16
    assert snapshot["runtime"]["factor_versions"]["pressure"] == "v3_smooth"
    assert snapshot["runtime"]["factor_versions"]["second_half_parser"]
    assert snapshot["match"]["is_cup_source"] == "persisted_league"
    assert snapshot["runtime"]["values"]["channel_signal_min_prob_to90"] == 75.0
    assert snapshot["runtime"]["values"][
        "channel_signal_min_reputation_delta_to90_pp"
    ] == 1.5
    assert snapshot["runtime"]["values"][
        "channel_signal_min_adjusted_intensity"
    ] == 0.55
    assert snapshot["runtime"]["values"][
        "channel_signal_min_season_context_factor"
    ] == 1.02


def test_decision_snapshot_keeps_channel_signal_filter_audit() -> None:
    channel_filter = nanotest.evaluate_channel_signal_filter(
        75.0,
        1,
        1,
        reputation_base_prob_to90=73.5,
        reputation_adjusted_prob_to90=75.0,
        adjusted_intensity=0.55,
        season_context_factor=1.02,
    )
    snapshot = nanotest.build_decision_snapshot(
        fixture_id=102,
        minute=47,
        window_name="WINDOW_1",
        match_identity={},
        score_home=1,
        score_away=1,
        probability_result={
            "prob_next_15": 50.0,
            "prob_to90": 80.0,
            "channel_signal_filter": channel_filter,
        },
        threshold_result={"threshold": 82.0, "fallback_threshold": 75.0},
        threshold_next15=32.0,
        readiness_result={"passed": True},
        live_gate_result={"required": False},
        anti_garbage_passed=True,
        final_decision="ALLOW",
        block_reason=None,
        factor_context={},
    )

    assert snapshot["channel_signal_filter"] == channel_filter
    assert snapshot["decision"]["channel_signal_filter_passed"] is True
    assert snapshot["gates"]["channel_signal_filter_passed"] is True
    assert snapshot["decision"]["active_publication_allow"] is True
    assert snapshot["decision"]["legacy_thresholds_publication_active"] is False
    assert snapshot["publication_policy"] == {
        "name": "base_p90_75_channel_gate",
        "active": True,
        "filter_version": nanotest.CHANNEL_SIGNAL_FILTER_VERSION,
        "filter_evaluated": True,
        "filter_contract_current": True,
        "publication_context_passed": True,
        "publication_allow": True,
        "readiness_policy": "snapshot_integrity",
        "pre_send_validation_required": True,
        "channel_signal_filter_required": True,
        "next15_probability_required": False,
        "dynamic_to90_threshold_required": False,
        "live_gate_required": False,
        "anti_garbage_required": False,
        "rescue_route_active": False,
        "score_limit_required_for_send": False,
        "score_limit_presentation_only": True,
    }

    observation = nanotest.build_observation_from_decision(
        snapshot,
        {},
        {
            "prob_next_15": 50.0,
            "prob_to90": 80.0,
        },
    )

    assert observation["publication_policy"] == snapshot["publication_policy"]
    assert observation["channel_signal_filter"] == channel_filter
