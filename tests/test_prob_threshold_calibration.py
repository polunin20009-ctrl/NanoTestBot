from __future__ import annotations

from wide_research.prob_threshold_calibration import (
    ProbThresholdCalibrationPolicy,
    apply_dual_prob_threshold_gates,
    apply_publication_prob_gate,
    calibrate_dual_publication_floors,
    calibrate_publication_min_prob,
    calibration_inputs_from_feature_values,
    extract_bot_prob_to90,
    extract_rolling_ml_prob_to90,
    hit_rate_lcb,
    iter_threshold_grid,
    rolling_ml_prob_from_prediction,
)


def _samples_at(prob: float, wins: int, losses: int) -> list[tuple[float, bool]]:
    return [(prob, True)] * wins + [(prob, False)] * losses


def test_hit_rate_lcb_penalizes_small_samples() -> None:
    dense = hit_rate_lcb(118, 130)
    sparse = hit_rate_lcb(37, 40)
    assert dense > sparse


def test_calibrator_prefers_stable_high_hit_rate_over_tiny_perfect_pocket() -> None:
    # Dense 91% at >=81 should beat sparse 100% at >=86 under LCB.
    samples = (
        _samples_at(80.0, wins=80, losses=20)
        + _samples_at(81.0, wins=90, losses=10)
        + _samples_at(86.0, wins=8, losses=0)
    )
    policy = ProbThresholdCalibrationPolicy(
        min_resolved=40,
        min_threshold=80.0,
        max_threshold=88.0,
        grid_step=1.0,
        hysteresis_lcb_eps=0.0,
    )
    result = calibrate_publication_min_prob(samples, policy=policy)
    assert result["active"] is True
    assert result["selected_threshold"] == 81.0
    assert result["selected"]["resolved"] == 108
    assert result["selected"]["hit_rate"] == 98 / 108


def test_calibrator_stays_inactive_when_no_threshold_beats_baseline() -> None:
    samples = _samples_at(82.0, wins=90, losses=10)
    policy = ProbThresholdCalibrationPolicy(
        min_resolved=40,
        min_threshold=80.0,
        max_threshold=88.0,
        grid_step=1.0,
        require_improvement_vs_baseline=True,
        hysteresis_lcb_eps=0.0,
    )
    result = calibrate_publication_min_prob(samples, policy=policy)
    # Raising the floor only shrinks N while keeping the same hit rate, so the
    # baseline LCB remains best and no extra gate is armed.
    assert result["active"] is False
    assert result["selected_threshold"] is None
    assert result["selection_reason"] == "no_threshold_beats_baseline_lcb"


def test_calibrator_hysteresis_keeps_previous_threshold() -> None:
    samples = (
        _samples_at(80.0, wins=40, losses=20)
        + _samples_at(81.0, wins=80, losses=10)
        + _samples_at(82.0, wins=80, losses=8)
    )
    policy = ProbThresholdCalibrationPolicy(
        min_resolved=40,
        min_threshold=80.0,
        max_threshold=85.0,
        grid_step=1.0,
        # Large enough that a nearby previous floor is retained.
        hysteresis_lcb_eps=0.20,
        require_improvement_vs_baseline=False,
    )
    fresh = calibrate_publication_min_prob(samples, policy=policy)
    assert fresh["active"] is True
    assert fresh["selected_threshold"] in {81.0, 82.0}
    previous = 81.0 if fresh["selected_threshold"] == 82.0 else 82.0
    kept = calibrate_publication_min_prob(
        samples,
        policy=policy,
        previous_threshold=previous,
    )
    assert kept["selected_threshold"] == previous
    assert kept["selection_reason"] == "hysteresis_keep_previous"


def test_grid_includes_bounds() -> None:
    policy = ProbThresholdCalibrationPolicy(
        min_threshold=80.0,
        max_threshold=81.5,
        grid_step=0.5,
    )
    assert iter_threshold_grid(policy) == (80.0, 80.5, 81.0, 81.5)


def test_extract_and_persist_calibration_inputs() -> None:
    values = {
        "bot.prob_to90": 84.25,
        "bot.baseline_prob_to90": 70.0,
        "ml.rolling.prob_to90": 86.5,
    }
    payload = {"calibration_inputs": calibration_inputs_from_feature_values(values)}
    assert extract_bot_prob_to90(payload) == 84.25
    assert extract_rolling_ml_prob_to90(payload) == 86.5
    assert extract_bot_prob_to90(
        {"source_input": {"matched_values": {"bot.prob_to90": 79.5}}}
    ) == 79.5


def test_rolling_ml_prob_from_prediction_record() -> None:
    assert (
        rolling_ml_prob_from_prediction(
            {
                "predictions": {
                    "to90": {
                        "status": "ok",
                        "calibrated_probability_pct": 83.25,
                        "production_applied": False,
                    }
                }
            }
        )
        == 83.25
    )
    assert (
        rolling_ml_prob_from_prediction(
            {
                "predictions": {
                    "to90": {
                        "status": "ok",
                        "calibrated_probability_pct": 83.25,
                        "production_applied": True,
                    }
                }
            }
        )
        is None
    )


def test_dual_tracks_publish_bot_and_shadow_rolling_ml() -> None:
    samples = []
    for idx in range(60):
        samples.append(
            {
                "calibration_inputs": {
                    "bot.prob_to90": 80.0 + (idx % 5),
                    "ml.rolling.prob_to90": 82.0 + (idx % 5),
                },
                "outcome_status": "win" if idx % 10 else "loss",
            }
        )
    # Dense high-quality rolling-ML pocket above 85.
    for _ in range(50):
        samples.append(
            {
                "calibration_inputs": {
                    "bot.prob_to90": 81.0,
                    "ml.rolling.prob_to90": 86.0,
                },
                "outcome_status": "win",
            }
        )
    policy = ProbThresholdCalibrationPolicy(
        min_resolved=40,
        min_threshold=80.0,
        max_threshold=88.0,
        grid_step=1.0,
        hysteresis_lcb_eps=0.0,
        require_improvement_vs_baseline=False,
    )
    dual = calibrate_dual_publication_floors(samples, policy=policy)
    assert dual["publication_track"] == "bot"
    assert "bot" in dual["tracks"]
    assert "rolling_ml" in dual["tracks"]
    assert dual["selected_threshold"] == dual["tracks"]["bot"]["selected_threshold"]
    assert dual["shadow_comparison"]["comparable_resolved"] > 0

    gate = apply_dual_prob_threshold_gates(
        combination_allow=True,
        bot_prob_to90=81.0,
        rolling_ml_prob_to90=86.0,
        calibration=dual,
    )
    assert gate["publication_track"] == "bot"
    assert "publication" in gate
    assert "rolling_ml" in gate["shadow"]
    assert gate["allow"] == gate["publication"]["allow"]


def test_publication_gate_and_fail_closed_without_prob() -> None:
    calibration = {
        "active": True,
        "selected_threshold": 81.5,
    }
    blocked = apply_publication_prob_gate(
        combination_allow=True,
        bot_prob_to90=81.0,
        calibration=calibration,
    )
    assert blocked["allow"] is False
    assert blocked["reason"] == "prob_below_calibrated_threshold"

    missing = apply_publication_prob_gate(
        combination_allow=True,
        bot_prob_to90=None,
        calibration=calibration,
    )
    assert missing["allow"] is False
    assert missing["reason"] == "bot_prob_unavailable_for_calibrated_gate"

    allowed = apply_publication_prob_gate(
        combination_allow=True,
        bot_prob_to90=81.5,
        calibration=calibration,
    )
    assert allowed["allow"] is True
    assert allowed["reason"] == "combination_and_calibrated_prob"
