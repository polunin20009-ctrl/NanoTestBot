from __future__ import annotations

import pytest

import NanoTest as bot


@pytest.mark.parametrize("correction_status", ["quarantine", "void"])
def test_removed_training_label_notifies_both_ml_trainers(
    monkeypatch, correction_status
) -> None:
    observation = {
        "record_type": "observation",
        "observation_id": "501:50:test",
        "fixture_id": 501,
        "stage": "decision_pipeline",
        "minute": 50,
        "match": {"score_home": 0, "score_away": 0},
        "outcome_schema_version": bot.OBSERVATION_OUTCOME_SCHEMA_VERSION,
        "outcome_revision": 1,
        "outcome": {"status": "resolved", "goal_to90_normal_time": True},
        "rolling_dynamics": {
            "schema_version": bot.ROLLING_DYNAMICS_SCHEMA_VERSION,
            "mode": "shadow_collection",
            "production_applied": False,
            "windows": {},
        },
    }
    notifications = []
    written = []
    monkeypatch.setattr(bot, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(bot, "get_score_timeline", lambda fixture_id: [])
    monkeypatch.setattr(
        bot, "append_observation_history", lambda row: written.append(row) or True
    )
    monkeypatch.setattr(
        bot, "note_shadow_ml_outcome",
        lambda fixture_id, **kwargs: notifications.append((fixture_id, kwargs)),
    )
    for name in (
        "append_shadow_candidate_outcomes",
        "append_wide_research_outcomes",
        "append_market_benchmark_outcomes",
    ):
        monkeypatch.setattr(bot, name, lambda rows: 0)
    if correction_status == "void":
        monkeypatch.setattr(
            bot, "pending_observations_by_fixture",
            lambda *args, **kwargs: {501: [observation]},
        )
        count = bot.void_observation_history_outcomes(
            501, "fixture_status_cancelled", "2026-09-12T12:00:00+00:00",
            include_terminal_records=True,
        )
    else:
        count = bot.resolve_observation_history_outcomes(
            501, [], {"status_short": "FT"}, None,
            "2026-09-12T12:00:00+00:00", observations=[observation],
        )
    assert count == 1
    assert written[0]["outcome"]["status"] == correction_status
    assert written[0]["outcome_revision"] == 2
    assert notifications == [
        (501, {"data_revision": True, "static_eligible": True, "rolling_eligible": True})
    ]


@pytest.mark.parametrize("resolver", ["resolved", "void"])
def test_correction_sweep_never_labels_rolling_seed(
    monkeypatch, resolver
) -> None:
    seed = {
        "record_type": "observation",
        "observation_id": "502:ROLLING_SEED_5M:rv1:v1",
        "fixture_id": 502,
        "stage": "rolling_seed",
        "minute": 41,
        "match": {"score_home": 0, "score_away": 0},
        "outcome": {
            "status": "void",
            "void_reason": "rolling_dynamics_seed",
        },
    }
    written = []
    monkeypatch.setattr(bot, "ENABLE_OBSERVATION_HISTORY", True)
    monkeypatch.setattr(
        bot, "append_observation_history", lambda row: written.append(row) or True
    )
    monkeypatch.setattr(
        bot, "pending_observations_by_fixture", lambda *args, **kwargs: {502: [seed]}
    )
    for name in (
        "append_shadow_candidate_outcomes",
        "append_wide_research_outcomes",
        "append_market_benchmark_outcomes",
    ):
        monkeypatch.setattr(bot, name, lambda rows: 0)

    if resolver == "resolved":
        count = bot.resolve_observation_history_outcomes(
            502,
            [],
            {"status_short": "FT"},
            (0, 0),
            "2026-09-12T12:00:00+00:00",
            observations=[seed],
        )
    else:
        count = bot.void_observation_history_outcomes(
            502,
            "fixture_status_cancelled",
            "2026-09-12T12:00:00+00:00",
            include_terminal_records=True,
        )

    assert count == 0
    assert written == []
