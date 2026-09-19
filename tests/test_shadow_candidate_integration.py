from __future__ import annotations

import copy

import NanoTest as bot
import pytest


def _observation(*, control_allow: bool = True) -> dict:
    return {
        "record_type": "observation",
        "observation_id": "99001:50:WINDOW_1:ALLOW:v3",
        "observation_key": "99001:50:WINDOW_1:ALLOW:v3",
        "fixture_id": 99001,
        "created_at_utc": "2026-08-22T19:00:00+00:00",
        "schema_version": 3,
        "stage": "decision_pipeline",
        "minute": 50,
        "decision": {"active_publication_allow": control_allow},
        "outcome": {
            "status": "pending",
            "outcome_scope": "TO_90_NORMAL_TIME",
        },
    }


def test_persist_runs_candidate_last_on_the_same_frozen_record(monkeypatch) -> None:
    source = _observation()
    source_before = copy.deepcopy(source)
    order: list[tuple[str, dict]] = []

    def freeze(record):
        frozen = copy.deepcopy(record)
        frozen["rolling_dynamics"] = {
            "schema_version": 1,
            "production_applied": False,
        }
        return frozen

    monkeypatch.setattr(bot, "freeze_observation_rolling_dynamics", freeze)
    monkeypatch.setattr(bot, "append_observation_history", lambda record: True)
    monkeypatch.setattr(
        bot,
        "register_observation_rolling_baseline",
        lambda record: order.append(("baseline", record)),
    )
    monkeypatch.setattr(
        bot,
        "predict_and_append_shadow_ml",
        lambda record: order.append(("static", record)),
    )
    monkeypatch.setattr(
        bot,
        "predict_and_append_shadow_rolling_ml",
        lambda record: order.append(("rolling", record)),
    )
    monkeypatch.setattr(
        bot,
        "evaluate_and_append_shadow_candidates",
        lambda record: order.append(("candidate", record)),
    )
    monkeypatch.setattr(
        bot,
        "evaluate_and_store_wide_research",
        lambda record: order.append(("wide", record)),
    )

    assert bot.persist_observation_and_score_shadow(source) is True
    assert source == source_before
    assert [name for name, _record in order] == [
        "baseline",
        "static",
        "rolling",
        "candidate",
        "wide",
    ]
    assert all(record is order[0][1] for _name, record in order)
    assert order[-1][1]["rolling_dynamics"]["production_applied"] is False


def test_live_candidate_wrapper_is_disabled_without_touching_storage(
    monkeypatch,
) -> None:
    monkeypatch.setattr(bot, "ENABLE_SHADOW_CANDIDATE_LAYER", False)
    monkeypatch.setattr(
        bot,
        "_get_shadow_candidate_layer",
        lambda: (_ for _ in ()).throw(
            AssertionError("disabled layer must not touch its journal")
        ),
    )

    assert bot.evaluate_and_append_shadow_candidates(_observation()) is False


def test_live_candidate_paths_cannot_overlap_observation_history(
    monkeypatch,
    tmp_path,
) -> None:
    source = tmp_path / "observation_history.jsonl"
    monkeypatch.setattr(bot, "OBSERVATION_HISTORY_FILE", str(source))
    monkeypatch.setattr(bot, "SHADOW_CANDIDATE_JOURNAL_FILE", str(source))
    monkeypatch.setattr(
        bot,
        "SHADOW_CANDIDATE_INDEX_FILE",
        str(tmp_path / "candidate.index.sqlite3"),
    )

    with pytest.raises(ValueError, match="overlaps live source"):
        bot._assert_shadow_candidate_runtime_paths()


def test_live_candidate_failure_is_best_effort_and_does_not_mutate_input(
    monkeypatch,
) -> None:
    source = _observation(control_allow=False)
    source_before = copy.deepcopy(source)

    class BrokenLayer:
        def process_snapshot(self, *args, **kwargs):
            raise RuntimeError("synthetic candidate failure")

    monkeypatch.setattr(bot, "ENABLE_SHADOW_CANDIDATE_LAYER", True)
    monkeypatch.setattr(bot, "_get_shadow_candidate_layer", lambda: BrokenLayer())
    monkeypatch.setattr(
        bot,
        "_build_shadow_candidate_prediction_record",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("BLOCK observation must not run duplicate ML inference")
        ),
    )

    assert bot.evaluate_and_append_shadow_candidates(source) is False
    assert source == source_before


def test_control_candidate_passes_both_causal_predictions_to_layer(
    monkeypatch,
) -> None:
    source = _observation(control_allow=True)
    calls: list[bool] = []
    captured: list[tuple[dict, dict, dict]] = []

    class CapturingLayer:
        def process_snapshot(
            self,
            observation,
            *,
            static_prediction=None,
            rolling_prediction=None,
        ):
            captured.append(
                (observation, static_prediction, rolling_prediction)
            )
            return {"arms": {}, "fixture_id": observation["fixture_id"]}

    def build_prediction(_observation, *, rolling):
        calls.append(rolling)
        return {"source": "rolling" if rolling else "static"}

    monkeypatch.setattr(bot, "ENABLE_SHADOW_CANDIDATE_LAYER", True)
    monkeypatch.setattr(bot, "_get_shadow_candidate_layer", lambda: CapturingLayer())
    monkeypatch.setattr(
        bot,
        "_build_shadow_candidate_prediction_record",
        build_prediction,
    )

    assert bot.evaluate_and_append_shadow_candidates(source) is True
    assert calls == [False, True]
    assert captured == [
        (source, {"source": "static"}, {"source": "rolling"})
    ]


def test_outcome_hook_is_batch_bounded_and_failure_is_isolated(monkeypatch) -> None:
    outcomes = [
        {
            "record_type": "observation_outcome",
            "observation_id": "99001:50:WINDOW_1:ALLOW:v3",
            "fixture_id": 99001,
            "outcome": {"status": "resolved"},
        }
    ]

    class WorkingLayer:
        def process_outcomes(self, supplied):
            assert supplied is outcomes
            return [{"event_id": "one"}, {"event_id": "two"}]

    monkeypatch.setattr(bot, "ENABLE_SHADOW_CANDIDATE_LAYER", True)
    monkeypatch.setattr(bot, "_get_shadow_candidate_layer", lambda: WorkingLayer())
    assert bot.append_shadow_candidate_outcomes(outcomes) == 2

    class BrokenLayer:
        def process_outcomes(self, supplied):
            raise RuntimeError("synthetic outcome failure")

    monkeypatch.setattr(bot, "_get_shadow_candidate_layer", lambda: BrokenLayer())
    assert bot.append_shadow_candidate_outcomes(outcomes) == 0
