from __future__ import annotations

import gzip
from datetime import datetime, timedelta, timezone

import NanoTest as bot
import signal_reputation.model as reputation_model
from signal_reputation.model import (
    EXPANDED_BLEND_COHORT,
    ReputationConfig,
    build_expanded_blend_cohort,
    build_reputation_model,
    evaluate_shadow_decision,
)
from signal_reputation.storage import append_shadow_record
from scripts.report_signal_reputation import build_report


NOW = datetime(2026, 7, 24, tzinfo=timezone.utc)


def _record(
    decision_id: str,
    *,
    fixture_id: int,
    minute: int = 54,
    probability_next15: float = 50.0,
    probability_to90: float = 70.0,
    next15=True,
    to90=True,
    resolved_at: datetime = NOW,
    send_ok: bool = False,
) -> dict:
    return {
        "record_type": "decision",
        "decision_id": decision_id,
        "decision_key": decision_id,
        "fixture_id": fixture_id,
        "created_at_utc": (resolved_at - timedelta(hours=2)).isoformat(),
        "minute": minute,
        "match": {
            "league_id": 10,
            "home_team_id": 100,
            "away_team_id": 200,
            "score_home": 1,
            "score_away": 1,
        },
        "probabilities": {
            "prob_next_15": probability_next15,
            "prob_to90": probability_to90,
        },
        "decision": {
            "final_decision": "BLOCK",
            "threshold_next15": 55.0,
            "selected_prob_to90_threshold": 75.0,
        },
        "gates": {
            "readiness_passed": True,
            "anti_garbage_passed": True,
            "other_hard_gates_passed": True,
            "live_gate_required": False,
            "live_gate_passed": True,
        },
        "telegram": {"send_ok": send_ok},
        "outcome": {
            "status": "resolved",
            "goal_within_15": next15,
            "goal_within_15_quality": "exact" if next15 is not None else "unknown",
            "goal_within_15_source": "event_exact" if next15 is not None else "unknown_timing",
            "goal_to90_normal_time": to90,
            "goal_result_source": "events",
            "resolved_at_utc": resolved_at.isoformat(),
        },
    }


def _config(**changes) -> ReputationConfig:
    values = {
        "half_life_days": 90.0,
        "prior_global": 0.0,
        "prior_league": 0.0,
        "prior_team": 0.0,
        "prior_team_league": 0.0,
        "prior_role": 0.0,
        "prior_pair": 0.0,
    }
    values.update(changes)
    return ReputationConfig(**values)


def test_repeated_decisions_from_one_fixture_have_total_weight_one() -> None:
    records = [
        _record("1:54", fixture_id=1, minute=54),
        _record("1:55", fixture_id=1, minute=55),
        _record("1:56", fixture_id=1, minute=56),
    ]

    model = build_reputation_model(records, _config(), now=NOW)
    target = model["cohorts"]["all_decisions"]["targets"]["to90"]

    assert target["rows"] == 3
    assert target["fixtures"] == 1
    assert target["overall"]["effective_weight"] == 1.0


def test_unknown_next15_is_excluded_but_known_to90_remains() -> None:
    model = build_reputation_model(
        [_record("1:54", fixture_id=1, next15=None, to90=True)],
        _config(),
        now=NOW,
    )

    assert model["cohorts"]["all_decisions"]["targets"]["next15"]["rows"] == 0
    assert model["cohorts"]["all_decisions"]["targets"]["to90"]["rows"] == 1


def test_telegram_cohort_contains_only_successfully_sent_signals() -> None:
    records = [
        _record("1:54", fixture_id=1, send_ok=True),
        _record("2:54", fixture_id=2, send_ok=False),
    ]

    model = build_reputation_model(records, _config(), now=NOW)

    assert model["cohorts"]["all_decisions"]["targets"]["to90"]["rows"] == 2
    assert model["cohorts"]["telegram_signals"]["targets"]["to90"]["rows"] == 1


def test_ninety_day_old_result_has_half_weight() -> None:
    model = build_reputation_model(
        [
            _record("1:54", fixture_id=1, resolved_at=NOW),
            _record("2:54", fixture_id=2, resolved_at=NOW - timedelta(days=90)),
        ],
        _config(),
        now=NOW,
    )

    weight = model["cohorts"]["all_decisions"]["targets"]["to90"]["overall"]["effective_weight"]

    assert weight == 1.5


def test_prior_shrinks_small_sample_delta() -> None:
    model = build_reputation_model(
        [_record("1:54", fixture_id=1, probability_to90=50.0, to90=True)],
        _config(prior_global=9.0),
        now=NOW,
    )
    overall = model["cohorts"]["all_decisions"]["targets"]["to90"]["overall"]

    assert overall["confidence"] == 0.1
    assert overall["delta_pp"] == 5.0


def test_projection_calculates_caps_without_changing_baseline_snapshot() -> None:
    history = [
        _record(str(index), fixture_id=index, probability_next15=20.0, probability_to90=20.0)
        for index in range(1, 11)
    ]
    model = build_reputation_model(history, _config(), now=NOW)
    snapshot = _record(
        "current",
        fixture_id=99,
        probability_next15=54.0,
        probability_to90=74.0,
        next15=None,
        to90=None,
    )
    snapshot["outcome"] = {"status": "pending"}

    projection = evaluate_shadow_decision(snapshot, model, _config())
    all_decisions = projection["cohorts"]["all_decisions"]

    assert snapshot["probabilities"]["prob_next_15"] == 54.0
    assert all_decisions["targets"]["next15"]["probability_by_cap"]["2"] == 56.0
    assert all_decisions["targets"]["next15"]["probability_by_cap"]["3"] == 57.0
    assert all_decisions["targets"]["next15"]["probability_by_cap"]["5"] == 59.0
    assert all_decisions["decision_by_cap"]["2"] == "ALLOW"
    assert projection["shadow_only"] is True
    assert EXPANDED_BLEND_COHORT not in projection["cohorts"]


def _projected_target(
    *,
    base: float,
    raw_delta: float,
    fixtures: int,
    deltas: dict[str, float],
) -> dict:
    return {
        "base_probability": base,
        "raw_delta_pp": raw_delta,
        "applied_delta_pp_by_cap": dict(deltas),
        "probability_by_cap": {
            cap: base + delta for cap, delta in deltas.items()
        },
        "sample": {
            "target_rows": fixtures,
            "target_fixtures": fixtures,
        },
    }


def test_expanded_blend_is_convex_and_never_adds_cohort_deltas() -> None:
    all_target = _projected_target(
        base=60.0,
        raw_delta=4.0,
        fixtures=400,
        deltas={"2": 2.0, "3": 3.0},
    )
    telegram_target = _projected_target(
        base=60.0,
        raw_delta=-4.0,
        fixtures=200,
        deltas={"2": -2.0, "3": -3.0},
    )
    cohorts = {
        "all_decisions": {
            "targets": {"next15": all_target, "to90": all_target}
        },
        "telegram_signals": {
            "targets": {"next15": telegram_target, "to90": telegram_target}
        },
    }

    blended = build_expanded_blend_cohort(
        cohorts,
        caps_pp=(2.0, 3.0),
        telegram_weight=0.10,
    )
    target = blended["targets"]["to90"]

    assert target["raw_delta_pp"] == 3.2
    assert target["applied_delta_pp_by_cap"]["2"] == 2.0
    assert target["applied_delta_pp_by_cap"]["3"] == 3.0
    assert target["probability_by_cap"]["2"] == 62.0
    assert target["blend"]["mode"] == "convex_blend"
    assert blended["production_apply"] is False


def test_expanded_blend_applies_one_cap_after_mixing_opposite_deltas() -> None:
    all_target = _projected_target(
        base=60.0,
        raw_delta=10.0,
        fixtures=400,
        deltas={"2": 2.0},
    )
    telegram_target = _projected_target(
        base=60.0,
        raw_delta=-1.0,
        fixtures=200,
        deltas={"2": -1.0},
    )
    cohorts = {
        "all_decisions": {
            "targets": {"next15": all_target, "to90": all_target}
        },
        "telegram_signals": {
            "targets": {"next15": telegram_target, "to90": telegram_target}
        },
    }

    blended = build_expanded_blend_cohort(
        cohorts,
        caps_pp=(2.0,),
        telegram_weight=0.75,
    )
    target = blended["targets"]["to90"]

    assert target["raw_delta_pp"] == 1.75
    assert target["applied_delta_pp_by_cap"]["2"] == 1.75
    assert target["probability_by_cap"]["2"] == 61.75


def test_expanded_recommended_probability_is_independent_of_caps_list() -> None:
    all_target = _projected_target(
        base=60.0,
        raw_delta=4.0,
        fixtures=400,
        deltas={"1": 1.0, "2": 2.0},
    )
    telegram_target = _projected_target(
        base=60.0,
        raw_delta=-4.0,
        fixtures=200,
        deltas={"1": -1.0, "2": -2.0},
    )
    cohorts = {
        "all_decisions": {
            "targets": {"next15": all_target, "to90": all_target}
        },
        "telegram_signals": {
            "targets": {"next15": telegram_target, "to90": telegram_target}
        },
    }

    without_cap_one = build_expanded_blend_cohort(
        cohorts,
        caps_pp=(2.0,),
        telegram_weight=0.75,
        next15_telegram_weight=0.75,
    )
    with_cap_one = build_expanded_blend_cohort(
        cohorts,
        caps_pp=(1.0, 2.0),
        telegram_weight=0.75,
        next15_telegram_weight=0.75,
    )

    for target in ("next15", "to90"):
        assert without_cap_one["targets"][target]["recommended_probability"] == (
            with_cap_one["targets"][target]["recommended_probability"]
        )


def test_expanded_blend_has_explicit_single_cohort_fallbacks() -> None:
    all_target = _projected_target(
        base=70.0,
        raw_delta=4.0,
        fixtures=400,
        deltas={"2": 2.0},
    )
    telegram_target = _projected_target(
        base=70.0,
        raw_delta=-4.0,
        fixtures=200,
        deltas={"2": -2.0},
    )

    all_only = build_expanded_blend_cohort(
        {"all_decisions": {"targets": {"next15": all_target, "to90": all_target}}},
        caps_pp=(2.0,),
    )
    telegram_only = build_expanded_blend_cohort(
        {
            "telegram_signals": {
                "targets": {"next15": telegram_target, "to90": telegram_target}
            }
        },
        caps_pp=(2.0,),
    )

    assert all_only["targets"]["to90"]["probability_by_cap"]["2"] == 72.0
    assert all_only["targets"]["to90"]["blend"]["mode"] == "all_decisions_only"
    assert telegram_only["targets"]["to90"]["probability_by_cap"]["2"] == 68.0
    assert telegram_only["targets"]["to90"]["blend"]["mode"] == "legacy_telegram_only"
    assert telegram_only["targets"]["to90"]["sample"]["target_fixtures"] == 200


def test_shadow_projection_includes_expanded_candidate_but_keeps_it_shadow_only() -> None:
    records = [
        _record(str(index), fixture_id=index, send_ok=index <= 5)
        for index in range(1, 11)
    ]
    config = _config(
        expanded_blend_enabled=True,
        expanded_blend_telegram_weight=0.10,
    )
    model = build_reputation_model(records, config, now=NOW)
    snapshot = _record("current", fixture_id=99, next15=None, to90=None)
    snapshot["outcome"] = {"status": "pending"}

    projection = evaluate_shadow_decision(snapshot, model, config)
    expanded = projection["cohorts"][EXPANDED_BLEND_COHORT]

    assert expanded["shadow_only"] is True
    assert expanded["production_apply"] is False
    assert expanded["blend_version"] == "convex_v1"
    assert expanded["targets"]["to90"]["blend"]["effective_telegram_weight"] == 0.10
    assert set(expanded["decision_by_cap"]) == {"2", "3", "5"}
    assert expanded["recommended_decision"] in {"ALLOW", "BLOCK"}


def test_expanded_recommended_decision_rechecks_channel_probability_gate() -> None:
    records = [
        _record(str(index), fixture_id=index, send_ok=True)
        for index in range(1, 11)
    ]
    config = _config(expanded_blend_enabled=True)
    model = build_reputation_model(records, config, now=NOW)
    snapshot = _record("current", fixture_id=99, next15=None, to90=None)
    snapshot["outcome"] = {"status": "pending"}
    snapshot["probabilities"]["reputation_base_prob_to90"] = 78.0
    snapshot["channel_signal_filter"] = {
        "min_prob_to90": 100.0,
        "min_reputation_delta_to90_pp": 1.5,
        "min_adjusted_intensity": 0.55,
        "min_season_context_factor": 1.02,
        "adjusted_intensity": 0.55,
        "season_context_factor": 1.02,
    }

    projection = evaluate_shadow_decision(snapshot, model, config)
    expanded = projection["cohorts"][EXPANDED_BLEND_COHORT]

    assert expanded["recommended_channel_filter_passed"] is False
    assert expanded["recommended_decision"] == "BLOCK"


def test_expanded_recommended_decision_accepts_new_channel_boundaries() -> None:
    config = _config(expanded_blend_enabled=True)
    model = build_reputation_model(
        [
            _record(
                str(index),
                fixture_id=index,
                probability_to90=78.5,
                to90=True,
                send_ok=True,
            )
            for index in range(1, 11)
        ],
        config,
        now=NOW,
    )
    snapshot = _record(
        "new-channel-boundaries",
        fixture_id=99,
        probability_next15=90.0,
        probability_to90=80.0,
        next15=None,
        to90=None,
    )
    snapshot["outcome"] = {"status": "pending"}
    snapshot["probabilities"]["reputation_base_prob_to90"] = 78.5
    snapshot["channel_signal_filter"] = {
        "min_prob_to90": 75.0,
        "min_reputation_delta_to90_pp": 1.5,
        "min_adjusted_intensity": 0.55,
        "min_season_context_factor": 1.02,
        "reputation_base_prob_to90": 78.5,
        "adjusted_intensity": 0.55,
        "season_context_factor": 1.02,
    }

    projection = evaluate_shadow_decision(snapshot, model, config)

    assert projection["cohorts"][EXPANDED_BLEND_COHORT][
        "recommended_channel_filter_passed"
    ] is True
    assert projection["cohorts"][EXPANDED_BLEND_COHORT][
        "recommended_decision"
    ] == "ALLOW"


def test_expanded_recommended_decision_fails_closed_without_channel_features() -> None:
    config = _config(expanded_blend_enabled=True)
    model = build_reputation_model([], config, now=NOW)

    low_probability = _record(
        "low-p90",
        fixture_id=98,
        probability_next15=90.0,
        probability_to90=80.0,
        next15=None,
        to90=None,
    )
    low_probability["outcome"] = {"status": "pending"}
    low_probability.pop("channel_signal_filter", None)
    high_probability = _record(
        "missing-channel-features",
        fixture_id=99,
        probability_next15=90.0,
        probability_to90=90.0,
        next15=None,
        to90=None,
    )
    high_probability["outcome"] = {"status": "pending"}
    high_probability.pop("channel_signal_filter", None)

    low_projection = evaluate_shadow_decision(low_probability, model, config)
    missing_projection = evaluate_shadow_decision(high_probability, model, config)

    assert low_projection["cohorts"][EXPANDED_BLEND_COHORT][
        "recommended_decision"
    ] == "BLOCK"
    assert missing_projection["cohorts"][EXPANDED_BLEND_COHORT][
        "recommended_decision"
    ] == "BLOCK"


def test_expanded_recommended_decision_uses_probability_and_factor_fallback() -> None:
    config = _config(expanded_blend_enabled=True)
    model = build_reputation_model(
        [
            _record(
                str(index),
                fixture_id=index,
                probability_to90=88.5,
                to90=True,
                send_ok=True,
            )
            for index in range(1, 11)
        ],
        config,
        now=NOW,
    )
    snapshot = _record(
        "live-metric-fallback",
        fixture_id=99,
        probability_next15=90.0,
        probability_to90=90.0,
        next15=None,
        to90=None,
    )
    snapshot["outcome"] = {"status": "pending"}
    snapshot["probabilities"]["reputation_base_prob_to90"] = 88.5
    snapshot["factors"] = {
        "adjusted_intensity": 0.55,
        "season_context_factor_45p": 1.02,
    }
    snapshot.pop("channel_signal_filter", None)

    projection = evaluate_shadow_decision(snapshot, model, config)
    expanded = projection["cohorts"][EXPANDED_BLEND_COHORT]

    assert expanded["recommended_channel_filter_passed"] is True
    assert expanded["recommended_decision"] == "ALLOW"


def test_reputation_model_uses_pre_reputation_probability_as_baseline() -> None:
    record = _record(
        "base-probability",
        fixture_id=1,
        probability_next15=80.0,
        probability_to90=90.0,
        next15=True,
        to90=True,
    )
    record["probabilities"]["reputation_base_prob_next_15"] = 50.0
    record["probabilities"]["reputation_base_prob_to90"] = 60.0

    model = build_reputation_model(
        [record],
        _config(prior_global=0.0),
        now=NOW,
    )

    next15 = model["cohorts"]["all_decisions"]["targets"]["next15"]["overall"]
    to90 = model["cohorts"]["all_decisions"]["targets"]["to90"]["overall"]
    assert next15["expected_rate"] == 0.5
    assert to90["expected_rate"] == 0.6


def test_auto_reputation_stages_use_unique_fixture_thresholds(monkeypatch) -> None:
    monkeypatch.setattr(bot, "SIGNAL_REPUTATION_AUTO_STAGE_1_FIXTURES", 50)
    monkeypatch.setattr(bot, "SIGNAL_REPUTATION_AUTO_STAGE_2_FIXTURES", 150)
    monkeypatch.setattr(bot, "SIGNAL_REPUTATION_AUTO_STAGE_3_FIXTURES", 300)

    assert bot._signal_reputation_auto_stage(49)["cap_pp"] == 0.0
    assert bot._signal_reputation_auto_stage(50)["cap_pp"] == 1.0
    assert bot._signal_reputation_auto_stage(149)["cap_pp"] == 1.0
    assert bot._signal_reputation_auto_stage(150)["cap_pp"] == 2.0
    assert bot._signal_reputation_auto_stage(299)["cap_pp"] == 2.0
    assert bot._signal_reputation_auto_stage(300)["cap_pp"] == 3.0


def test_auto_reputation_applies_each_target_only_at_its_ready_stage(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_AUTO_APPLY", True)
    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_EXPANDED_AUTO_APPLY", False)
    monkeypatch.setattr(bot, "SIGNAL_REPUTATION_AUTO_STAGE_1_FIXTURES", 50)
    monkeypatch.setattr(bot, "SIGNAL_REPUTATION_AUTO_STAGE_2_FIXTURES", 150)
    monkeypatch.setattr(bot, "SIGNAL_REPUTATION_AUTO_STAGE_3_FIXTURES", 300)
    monkeypatch.setattr(bot, "refresh_signal_reputation_model", lambda: {"model": True})

    def projection(*_args, **_kwargs):
        return {
            "model_generated_at_utc": "2026-07-24T00:00:00+00:00",
            "cohorts": {
                "telegram_signals": {
                    "targets": {
                        "next15": {
                            "raw_delta_pp": 5.0,
                            "probability_by_cap": {
                                "1": 61.0,
                                "2": 62.0,
                                "3": 63.0,
                                "5": 65.0,
                            },
                            "sample": {"target_fixtures": 49},
                        },
                        "to90": {
                            "raw_delta_pp": -5.0,
                            "probability_by_cap": {
                                "1": 79.0,
                                "2": 78.0,
                                "3": 77.0,
                                "5": 75.0,
                            },
                            "sample": {"target_fixtures": 150},
                        },
                    }
                }
            },
        }

    monkeypatch.setattr(bot, "evaluate_shadow_decision", projection)

    result = bot.apply_signal_reputation_auto(
        fixture_id=1,
        minute=54,
        match_identity={
            "league_id": 10,
            "home_team_id": 100,
            "away_team_id": 200,
        },
        score_home=0,
        score_away=0,
        prob_next_15=60.0,
        prob_to90=80.0,
    )

    assert result["targets"]["next15"]["status"] == "collecting"
    assert result["targets"]["next15"]["adjusted_probability"] == 60.0
    assert result["targets"]["to90"]["stage"] == 2
    assert result["targets"]["to90"]["cap_pp"] == 2.0
    assert result["targets"]["to90"]["adjusted_probability"] == 78.0
    assert result["cohort"] == "telegram_signals"


def test_auto_reputation_applies_expanded_recommended_probabilities(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_AUTO_APPLY", True)
    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_EXPANDED_AUTO_APPLY", True)
    monkeypatch.setattr(bot, "SIGNAL_REPUTATION_AUTO_STAGE_1_FIXTURES", 50)
    monkeypatch.setattr(bot, "SIGNAL_REPUTATION_AUTO_STAGE_2_FIXTURES", 150)
    monkeypatch.setattr(bot, "SIGNAL_REPUTATION_AUTO_STAGE_3_FIXTURES", 300)
    monkeypatch.setattr(bot, "refresh_signal_reputation_model", lambda: {"model": True})

    def projection(*_args, **kwargs):
        assert kwargs["config"].expanded_blend_enabled is True
        return {
            "model_generated_at_utc": "2026-08-14T00:00:00+00:00",
            "cohorts": {
                "expanded_blend": {
                    "targets": {
                        "next15": {
                            "raw_delta_pp": 4.0,
                            "recommended_cap_pp": 1.0,
                            "recommended_probability": 61.0,
                            "sample": {"target_fixtures": 1111},
                        },
                        "to90": {
                            "raw_delta_pp": -4.0,
                            "recommended_cap_pp": 2.0,
                            "recommended_probability": 78.0,
                            "sample": {"target_fixtures": 1112},
                        },
                    }
                }
            },
        }

    monkeypatch.setattr(bot, "evaluate_shadow_decision", projection)

    result = bot.apply_signal_reputation_auto(
        fixture_id=1,
        minute=54,
        match_identity={
            "league_id": 10,
            "home_team_id": 100,
            "away_team_id": 200,
        },
        score_home=0,
        score_away=0,
        prob_next_15=60.0,
        prob_to90=80.0,
    )

    assert result["active"] is True
    assert result["cohort"] == "expanded_blend"
    assert result["fallback_cohort"] is None
    assert result["targets"]["next15"]["cap_pp"] == 1.0
    assert result["targets"]["next15"]["adjusted_probability"] == 61.0
    assert result["targets"]["to90"]["cap_pp"] == 2.0
    assert result["targets"]["to90"]["adjusted_probability"] == 78.0


def test_expanded_production_failure_falls_back_to_telegram_reputation(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_AUTO_APPLY", True)
    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_EXPANDED_AUTO_APPLY", True)
    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_EXPANDED_SHADOW", True)
    monkeypatch.setattr(bot, "SIGNAL_REPUTATION_AUTO_STAGE_1_FIXTURES", 50)
    records = [
        _record(str(index), fixture_id=index, send_ok=True)
        for index in range(1, 51)
    ]
    model = build_reputation_model(records, _config(), now=NOW)
    monkeypatch.setattr(bot, "refresh_signal_reputation_model", lambda: model)

    def fail_expanded(*_args, **_kwargs):
        raise AssertionError("expanded calculation failed")

    monkeypatch.setattr(
        reputation_model,
        "build_expanded_blend_cohort",
        fail_expanded,
    )

    result = bot.apply_signal_reputation_auto(
        fixture_id=99,
        minute=54,
        match_identity={
            "league_id": 10,
            "home_team_id": 100,
            "away_team_id": 200,
        },
        score_home=1,
        score_away=1,
        prob_next_15=60.0,
        prob_to90=80.0,
    )

    assert result["active"] is True
    assert result["targets"]["to90"]["status"] == "active"
    assert result["cohort"] == "telegram_signals"
    assert result["expanded_requested"] is True
    assert result["fallback_cohort"] == "telegram_signals"


def test_reputation_result_application_is_idempotent(monkeypatch) -> None:
    fixture = {
        "fixture_id": {"value": 99},
        "score_home": {"value": 1},
        "score_away": {"value": 0},
    }
    received_bases = []
    monkeypatch.setattr(bot, "get_fixture_id", lambda _fixture: 99)
    monkeypatch.setattr(
        bot,
        "build_match_identity_context",
        lambda _fixture: {
            "league_id": 10,
            "home_team_id": 100,
            "away_team_id": 200,
        },
    )

    def apply_auto(**kwargs):
        received_bases.append((kwargs["prob_next_15"], kwargs["prob_to90"]))
        return {
            "active": True,
            "targets": {
                "next15": {
                    "adjusted_probability": kwargs["prob_next_15"] + 1.0,
                },
                "to90": {
                    "adjusted_probability": kwargs["prob_to90"] - 1.0,
                },
            },
        }

    monkeypatch.setattr(bot, "apply_signal_reputation_auto", apply_auto)
    base = {"prob_next_15": 60.0, "prob_to90": 80.0}

    first = bot.apply_signal_reputation_to_probability_result(
        fixture,
        54,
        base,
        application_context="send",
    )
    second = bot.apply_signal_reputation_to_probability_result(
        fixture,
        55,
        first,
        application_context="update",
    )

    assert received_bases == [(60.0, 80.0), (60.0, 80.0)]
    assert first["prob_next_15"] == second["prob_next_15"] == 61.0
    assert first["prob_to90"] == second["prob_to90"] == 79.0
    assert second["reputation_application_context"] == "update"


def _fixture_at_status(status: str, *, elapsed: int, extra: int = 0):
    return {
        "fixture_id": {"value": 99},
        "status": {
            "value": {
                "short": status,
                "elapsed": elapsed,
                "extra": extra,
            }
        },
        "elapsed": {"value": elapsed},
        "extra_time": {"value": extra},
        "score_home": {"value": 1},
        "score_away": {"value": 0},
    }


def test_reputation_is_not_applied_after_normal_time_finished(monkeypatch) -> None:
    def fail_apply(**_kwargs):
        raise AssertionError("reputation must not run after normal time")

    monkeypatch.setattr(bot, "apply_signal_reputation_auto", fail_apply)

    result = bot.apply_signal_reputation_to_probability_result(
        _fixture_at_status("FT", elapsed=90, extra=7),
        90,
        {"prob_next_15": 50.0, "prob_to90": 0.0},
        application_context="update",
    )

    assert result["prob_next_15"] == 50.0
    assert result["prob_to90"] == 0.0
    assert result["reputation_application"]["active"] is False
    assert result["reputation_application"]["skip_reason"] == "normal_time_finished"
    assert result["reputation_application"]["targets"]["next15"]["applied_delta_pp"] == 0.0
    assert result["reputation_application"]["targets"]["to90"]["applied_delta_pp"] == 0.0


def test_reputation_is_not_applied_in_extra_time(monkeypatch) -> None:
    def fail_apply(**_kwargs):
        raise AssertionError("reputation must not run in extra time")

    monkeypatch.setattr(bot, "apply_signal_reputation_auto", fail_apply)

    result = bot.apply_signal_reputation_to_probability_result(
        _fixture_at_status("ET", elapsed=97),
        97,
        {"prob_next_15": 42.0, "prob_to90": 0.0},
        application_context="update",
    )

    assert result["prob_next_15"] == 42.0
    assert result["prob_to90"] == 0.0
    assert result["reputation_application"]["skip_reason"] == "normal_time_finished"


def test_reputation_still_applies_during_normal_time_stoppage(monkeypatch) -> None:
    def apply_auto(**kwargs):
        return {
            "active": True,
            "targets": {
                "next15": {
                    "adjusted_probability": kwargs["prob_next_15"] + 1.0,
                },
                "to90": {
                    "adjusted_probability": kwargs["prob_to90"] - 1.0,
                },
            },
        }

    monkeypatch.setattr(bot, "apply_signal_reputation_auto", apply_auto)

    result = bot.apply_signal_reputation_to_probability_result(
        _fixture_at_status("2H", elapsed=90, extra=7),
        90,
        {"prob_next_15": 20.0, "prob_to90": 5.0},
        application_context="update",
    )

    assert result["prob_next_15"] == 21.0
    assert result["prob_to90"] == 4.0
    assert result["reputation_application"]["active"] is True


def test_send_and_update_use_same_reputation_aware_probability_path(monkeypatch) -> None:
    fixture = {"fixture_id": {"value": 99}}
    monkeypatch.setattr(
        bot,
        "compute_probability_45_plus",
        lambda _fixture, _minute: {
            "prob_next_15": 60.0,
            "prob_to90": 80.0,
            "prob_next_25": 70.0,
        },
    )

    contexts = []

    def apply_result(_fixture, _minute, result, *, application_context):
        contexts.append(application_context)
        adjusted = dict(result)
        adjusted["prob_next_15"] = 61.0
        adjusted["prob_to90"] = 79.0
        return adjusted

    monkeypatch.setattr(
        bot,
        "apply_signal_reputation_to_probability_result",
        apply_result,
    )

    sent = bot.compute_probability_45_plus_with_reputation(
        fixture,
        54,
        application_context="send",
    )
    updated = bot.compute_probability_45_plus_with_reputation(
        fixture,
        55,
        application_context="update",
    )

    assert contexts == ["send", "update"]
    assert sent["prob_next_15"] == updated["prob_next_15"] == 61.0
    assert sent["prob_to90"] == updated["prob_to90"] == 79.0


def test_failed_hard_gate_cannot_be_overridden_by_shadow_probability() -> None:
    model = build_reputation_model(
        [_record("1", fixture_id=1, probability_next15=20.0, probability_to90=20.0)],
        _config(),
        now=NOW,
    )
    snapshot = _record(
        "current", fixture_id=99, probability_next15=54.0, probability_to90=74.0
    )
    snapshot["outcome"] = {"status": "pending"}
    snapshot["gates"]["other_hard_gates_passed"] = False

    projection = evaluate_shadow_decision(snapshot, model, _config())

    assert projection["cohorts"]["all_decisions"]["decision_by_cap"]["5"] == "BLOCK"


def test_shadow_storage_rotates_to_gzip_and_deduplicates(tmp_path) -> None:
    path = tmp_path / "shadow.jsonl"
    first = {"shadow_key": "projection:1:v1", "payload": "x" * 200}
    second = {"shadow_key": "projection:2:v1", "payload": "y" * 200}

    assert append_shadow_record(str(path), first, rotate_max_bytes=250) is True
    assert append_shadow_record(str(path), second, rotate_max_bytes=250) is True
    assert append_shadow_record(str(path), first, rotate_max_bytes=250) is False

    archives = list(tmp_path.glob("shadow.*.jsonl.gz"))
    assert len(archives) == 1
    with gzip.open(archives[0], "rt", encoding="utf-8") as handle:
        assert "projection:1:v1" in handle.read()


def test_shadow_report_joins_projection_with_later_outcome(tmp_path) -> None:
    path = tmp_path / "shadow.jsonl"
    projection = {
        "record_type": "projection",
        "shadow_key": "projection:d1:v1",
        "decision_id": "d1",
        "shadow_reputation": {
            "baseline_decision": "BLOCK",
            "cohorts": {
                "all_decisions": {
                    "targets": {
                        "next15": {
                            "base_probability": 40.0,
                            "probability_by_cap": {"2": 42.0, "3": 43.0, "5": 45.0},
                        },
                        "to90": {
                            "base_probability": 70.0,
                            "probability_by_cap": {"2": 72.0, "3": 73.0, "5": 75.0},
                        },
                    },
                    "decision_by_cap": {"2": "BLOCK", "3": "ALLOW", "5": "ALLOW"},
                }
            },
        },
    }
    outcome = {
        "record_type": "outcome",
        "shadow_key": "outcome:d1:v4",
        "decision_id": "d1",
        "outcome": {"goal_within_15": True, "goal_to90_normal_time": True},
    }
    assert append_shadow_record(str(path), projection) is True
    assert append_shadow_record(str(path), outcome) is True

    report = build_report(str(path))

    assert report["joined_records"] == 1
    assert report["cohorts"]["all_decisions"]["targets"]["next15"]["baseline"]["count"] == 1
    assert report["cohorts"]["all_decisions"]["decisions"]["3"]["changed"] == 1
    assert report["cohorts"]["all_decisions"]["decisions"]["3"]["shadow_allow_winrate_to90"] == 1.0
    assert report["cohorts"][EXPANDED_BLEND_COHORT]["targets"]["to90"][
        "caps"
    ]["2"]["avg_probability"] == 0.72


def test_shadow_report_backfills_expanded_blend_from_historical_sources(tmp_path) -> None:
    path = tmp_path / "historical-shadow.jsonl"

    def target(delta: float) -> dict:
        return {
            "base_probability": 70.0,
            "raw_delta_pp": delta,
            "applied_delta_pp_by_cap": {"2": delta},
            "probability_by_cap": {"2": 70.0 + delta},
            "sample": {"target_rows": 200, "target_fixtures": 200},
        }

    projection = {
        "record_type": "projection",
        "shadow_key": "projection:d1:v1",
        "decision_id": "d1",
        "fixture_id": 1,
        "decision": {"final_decision": "ALLOW"},
        "shadow_reputation": {
            "baseline_decision": "BLOCK",
            "cohorts": {
                "all_decisions": {
                    "targets": {"next15": target(2.0), "to90": target(2.0)},
                    "decision_by_cap": {"2": "BLOCK"},
                },
                "telegram_signals": {
                    "targets": {"next15": target(-2.0), "to90": target(-2.0)},
                    "decision_by_cap": {"2": "BLOCK"},
                },
                EXPANDED_BLEND_COHORT: {
                    "targets": {
                        "next15": target(29.0),
                        "to90": target(29.0),
                    },
                    "blend_version": "convex_v1",
                    "configured_telegram_weights": {
                        "next15": 0.10,
                        "to90": 0.10,
                    },
                    "recommended_caps_pp": {"next15": 1.0, "to90": 2.0},
                    "production_apply": False,
                },
            },
        },
    }
    outcome = {
        "record_type": "outcome",
        "shadow_key": "outcome:d1:v4",
        "decision_id": "d1",
        "outcome": {"goal_within_15": True, "goal_to90_normal_time": True},
    }
    assert append_shadow_record(str(path), projection) is True
    assert append_shadow_record(str(path), outcome) is True

    report = build_report(
        str(path),
        blend_telegram_weight=0.75,
        blend_next15_telegram_weight=1.0,
    )

    expanded = report["cohorts"][EXPANDED_BLEND_COHORT]["targets"]
    assert expanded["to90"]["caps"]["2"]["avg_probability"] == 0.69
    assert expanded["next15"]["caps"]["2"]["avg_probability"] == 0.68
    assert report["expanded_blend"]["production_apply"] is False
    assert report["published_signals"]["fixtures"] == 1
    assert report["published_signals"]["cohorts"][EXPANDED_BLEND_COHORT][
        "targets"
    ]["to90"]["caps"]["2"]["avg_probability"] == 0.69


def test_shadow_report_ranks_schema_before_file_order(tmp_path) -> None:
    path = tmp_path / "ranked-shadow.jsonl"

    def projection(schema: int, created: str, probability: float) -> dict:
        return {
            "record_type": "projection",
            "shadow_key": f"projection:d1:v{schema}",
            "decision_id": "d1",
            "created_at_utc": created,
            "shadow_reputation": {
                "schema_version": schema,
                "baseline_decision": "BLOCK",
                "cohorts": {
                    "all_decisions": {
                        "targets": {
                            "next15": {
                                "base_probability": probability,
                                "probability_by_cap": {"3": probability},
                            },
                            "to90": {
                                "base_probability": probability,
                                "probability_by_cap": {"3": probability},
                            },
                        },
                        "decision_by_cap": {"3": "BLOCK"},
                    }
                },
            },
        }

    high_projection = projection(2, "2026-07-25T01:00:00+00:00", 80.0)
    later_low_projection = projection(1, "2026-07-25T02:00:00+00:00", 20.0)
    current_outcome = {
        "record_type": "outcome",
        "shadow_key": "outcome:d1:v4",
        "decision_id": "d1",
        "outcome_schema_version": 4,
        "created_at_utc": "2026-07-25T01:00:00+00:00",
        "outcome": {"goal_within_15": False, "goal_to90_normal_time": False},
    }
    later_old_outcome = {
        **current_outcome,
        "shadow_key": "outcome:d1:v3",
        "outcome_schema_version": 3,
        "created_at_utc": "2026-07-25T02:00:00+00:00",
        "outcome": {"goal_within_15": True, "goal_to90_normal_time": True},
    }
    for record in (
        high_projection,
        later_low_projection,
        current_outcome,
        later_old_outcome,
    ):
        assert append_shadow_record(str(path), record) is True

    report = build_report(str(path))
    baseline = report["cohorts"]["all_decisions"]["targets"]["next15"][
        "baseline"
    ]

    assert baseline["avg_probability"] == 0.8
    assert baseline["actual_rate"] == 0.0


def test_shadow_report_quarantine_revision_removes_previous_label(tmp_path) -> None:
    path = tmp_path / "quarantine-revision-shadow.jsonl"
    projection = {
        "record_type": "projection",
        "shadow_key": "projection:d-quarantine:v1",
        "decision_id": "d-quarantine",
        "created_at_utc": "2026-09-12T00:00:00+00:00",
        "shadow_reputation": {
            "schema_version": 1,
            "baseline_decision": "ALLOW",
            "cohorts": {
                "all_decisions": {
                    "targets": {
                        "next15": {
                            "base_probability": 50.0,
                            "probability_by_cap": {"3": 50.0},
                        },
                        "to90": {
                            "base_probability": 80.0,
                            "probability_by_cap": {"3": 80.0},
                        },
                    },
                    "decision_by_cap": {"3": "ALLOW"},
                }
            },
        },
    }
    resolved = {
        "record_type": "outcome",
        "shadow_key": "outcome:d-quarantine:v4:r1",
        "decision_id": "d-quarantine",
        "outcome_schema_version": 4,
        "outcome_revision": 1,
        "created_at_utc": "2026-09-12T01:00:00+00:00",
        "outcome": {
            "status": "resolved",
            "goal_within_15": True,
            "goal_to90_normal_time": True,
        },
    }
    quarantine = {
        **resolved,
        "shadow_key": "outcome:d-quarantine:v4:r2",
        "outcome_revision": 2,
        "created_at_utc": "2026-09-12T02:00:00+00:00",
        # Integrity resolver can retain boolean fields on an invalid record.
        "outcome": {
            "status": "quarantine",
            "goal_within_15": False,
            "goal_to90_normal_time": False,
            "outcome_integrity_conflict": True,
        },
    }
    for record in (projection, resolved, quarantine):
        assert append_shadow_record(str(path), record) is True

    report = build_report(str(path))
    assert report["joined_records"] == 1
    assert report["cohorts"] == {}


def _full_shadow_projection() -> dict:
    target = {
        "base_probability": 70.0,
        "components_pp": {"league": 1.25, "home_team": -0.5},
        "raw_delta_pp": 0.75,
        "applied_delta_pp_by_cap": {"1": 0.75, "2": 0.75, "3": 0.75},
        "probability_by_cap": {"1": 70.75, "2": 70.75, "3": 70.75},
        "sample": {"target_rows": 100, "target_fixtures": 80},
    }
    return {
        "schema_version": 1,
        "shadow_only": True,
        "model_generated_at_utc": "2026-07-25T00:00:00+00:00",
        "baseline_decision": "BLOCK",
        "cohorts": {
            "all_decisions": {
                "targets": {
                    "next15": dict(target),
                    "to90": dict(target),
                },
                "decision_by_cap": {"1": "BLOCK", "2": "BLOCK", "3": "ALLOW"},
            }
        },
    }


def test_attach_shadow_writes_full_journal_and_embeds_only_summary(
    monkeypatch,
) -> None:
    projection = _full_shadow_projection()
    captured: list[dict] = []
    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_SHADOW", True)
    monkeypatch.setattr(bot, "refresh_signal_reputation_model", lambda: {})
    monkeypatch.setattr(
        bot,
        "evaluate_shadow_decision",
        lambda snapshot, model, config: projection,
    )

    def capture_append(path, record, *, rotate_max_bytes):
        captured.append(record)
        return True

    monkeypatch.setattr(bot, "append_shadow_record", capture_append)
    snapshot = {
        "decision_id": "101:46:WINDOW_1:v2",
        "fixture_id": 101,
        "created_at_utc": "2026-07-25T01:00:00+00:00",
        "match": {},
        "decision": {},
        "probabilities": {},
    }

    result = bot.attach_shadow_reputation(snapshot)

    assert captured[0]["shadow_reputation"] is projection
    assert "components_pp" in captured[0]["shadow_reputation"]["cohorts"][
        "all_decisions"
    ]["targets"]["to90"]
    assert "shadow_reputation" not in result
    assert "shadow_reputation_fallback" not in result
    summary = result["shadow_reputation_summary"]
    assert summary["journal_status"] == "written"
    assert summary["shadow_key"] == "projection:101:46:WINDOW_1:v2:v1"
    compact_target = summary["cohorts"]["all_decisions"]["targets"]["to90"]
    assert compact_target["raw_delta_pp"] == 0.75
    assert "components_pp" not in compact_target
    assert "sample" not in compact_target


def test_attach_shadow_keeps_full_fallback_when_journal_write_fails(
    monkeypatch,
) -> None:
    projection = _full_shadow_projection()
    monkeypatch.setattr(bot, "ENABLE_SIGNAL_REPUTATION_SHADOW", True)
    monkeypatch.setattr(bot, "refresh_signal_reputation_model", lambda: {})
    monkeypatch.setattr(
        bot,
        "evaluate_shadow_decision",
        lambda snapshot, model, config: projection,
    )

    def fail_append(path, record, *, rotate_max_bytes):
        raise OSError("disk unavailable")

    monkeypatch.setattr(bot, "append_shadow_record", fail_append)
    snapshot = {
        "decision_id": "101:46:WINDOW_1:v2",
        "fixture_id": 101,
        "match": {},
        "decision": {},
        "probabilities": {},
    }

    result = bot.attach_shadow_reputation(snapshot)

    assert result["shadow_reputation_summary"]["journal_status"] == (
        "fallback_embedded"
    )
    assert result["shadow_reputation_fallback"] is projection
