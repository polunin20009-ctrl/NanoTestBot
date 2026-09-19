from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone

from shadow_ml.rolling import (
    RollingDynamicsTracker,
    freeze_rolling_dynamics_batch,
)


START = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)
PAIR_METRICS = {
    "xg_total": "xg",
    "shots_on_target_total": "shots_on_target",
    "shots_in_box_total": "shots_in_box",
    "total_shots_total": "total_shots",
    "corners_total": "corners",
}


def _observation(
    fixture_id: int = 101,
    minute: int = 46,
    *,
    created_at: datetime | None = None,
    observation_id: str | None = None,
    totals: dict[str, float] | None = None,
    pressure: float | None = 0.0,
    score: tuple[float, float] = (0.0, 0.0),
    unavailable: tuple[str, ...] = (),
    xg_source: str = "api",
    stage: str = "decision_pipeline",
) -> dict:
    values = {name: 0.0 for name in PAIR_METRICS}
    values.update(totals or {})
    raw_metrics: dict[str, float] = {}
    availability: dict[str, bool] = {}
    for output_name, raw_name in PAIR_METRICS.items():
        raw_metrics[f"{raw_name}_home"] = values[output_name]
        raw_metrics[f"{raw_name}_away"] = 0.0
        availability[f"{raw_name}_home"] = True
        availability[f"{raw_name}_away"] = True
    for key in unavailable:
        availability[key] = False

    timestamp = created_at or START + timedelta(minutes=minute)
    identity = observation_id or f"{fixture_id}:{minute}:{timestamp.timestamp()}"
    return {
        "record_type": "observation",
        "observation_id": identity,
        "observation_key": identity,
        "fixture_id": fixture_id,
        "created_at_utc": timestamp.isoformat(),
        "stage": stage,
        "minute": minute,
        "match": {
            "score_home": score[0],
            "score_away": score[1],
            "score_state": f"{score[0]}-{score[1]}",
        },
        "raw_metrics": raw_metrics,
        "availability": availability,
        "data_quality": {"xg_source": xg_source},
        "features": {"pressure_index": pressure},
        "decision": {"final_decision": "BLOCK"},
        "telegram": {"send_ok": False},
        "outcome": {"status": "pending"},
    }


def _window(
    tracker: RollingDynamicsTracker,
    observation: dict,
    name: str,
) -> dict:
    return tracker.compute(observation)["windows"][name]


def test_exact_5_and_10_minute_deltas_use_real_observation_schema() -> None:
    tracker = RollingDynamicsTracker()
    minute_36 = _observation(
        minute=36,
        totals={
            "xg_total": 0.3,
            "shots_on_target_total": 1,
            "shots_in_box_total": 2,
            "total_shots_total": 4,
            "corners_total": 1,
        },
        pressure=10,
    )
    minute_41 = _observation(
        minute=41,
        totals={
            "xg_total": 0.6,
            "shots_on_target_total": 2,
            "shots_in_box_total": 3,
            "total_shots_total": 7,
            "corners_total": 2,
        },
        pressure=14,
    )
    current = _observation(
        minute=46,
        totals={
            "xg_total": 1.1,
            "shots_on_target_total": 4,
            "shots_in_box_total": 6,
            "total_shots_total": 11,
            "corners_total": 4,
        },
        pressure=12,
        score=(1, 0),
    )
    tracker.ingest(minute_36)
    tracker.ingest(
        _observation(
            fixture_id=202,
            minute=41,
            totals={"total_shots_total": 100},
        )
    )
    tracker.ingest(minute_41)

    five = _window(tracker, current, "5m")
    ten = _window(tracker, current, "10m")

    assert five["baseline_observation_id"] == minute_41["observation_id"]
    assert five["actual_span_minutes"] == 5
    assert five["crosses_halftime"] is True
    assert five["deltas"] == {
        "xg_total": 0.5,
        "shots_on_target_total": 2.0,
        "shots_in_box_total": 3.0,
        "total_shots_total": 4.0,
        "corners_total": 2.0,
        "score_total": 1.0,
        "pressure_index": -2.0,
    }
    assert ten["baseline_observation_id"] == minute_36["observation_id"]
    assert ten["actual_span_minutes"] == 10
    assert ten["deltas"]["total_shots_total"] == 7.0
    assert ten["rates_per_minute"]["total_shots_total"] == 0.7


def test_sparse_history_uses_nearest_span_only_within_tolerance() -> None:
    exact_tracker = RollingDynamicsTracker(windows=(5,), max_extra_minutes=2)
    minute_40 = _observation(minute=40)
    minute_42 = _observation(minute=42)
    exact_tracker.ingest(minute_40)
    exact_tracker.ingest(minute_42)

    exact = _window(exact_tracker, _observation(minute=47), "5m")

    assert exact["baseline_minute"] == 42
    assert exact["actual_span_minutes"] == 5

    sparse_tracker = RollingDynamicsTracker(windows=(5,), max_extra_minutes=2)
    sparse_tracker.ingest(minute_40)

    tolerated = _window(sparse_tracker, _observation(minute=47), "5m")
    too_old = _window(sparse_tracker, _observation(minute=48), "5m")

    assert tolerated["status"] == "ok"
    assert tolerated["actual_span_minutes"] == 7
    assert too_old["status"] == "unavailable"
    assert too_old["reason"] == "baseline_missing"


def test_richer_baseline_wins_within_tolerance_over_sparse_exact_point() -> None:
    tracker = RollingDynamicsTracker(windows=(5,), max_extra_minutes=2)
    rich = _observation(
        minute=41,
        totals={"total_shots_total": 4},
        pressure=3,
    )
    pair_keys = tuple(
        f"{raw_name}_{side}"
        for raw_name in PAIR_METRICS.values()
        for side in ("home", "away")
    )
    sparse_exact = _observation(
        minute=42,
        pressure=5,
        unavailable=pair_keys,
    )
    tracker.ingest(rich)
    tracker.ingest(sparse_exact)

    window = _window(
        tracker,
        _observation(
            minute=47,
            totals={"total_shots_total": 8},
            pressure=7,
        ),
        "5m",
    )

    assert window["baseline_minute"] == 41
    assert window["actual_span_minutes"] == 6
    assert window["deltas"]["total_shots_total"] == 4.0


def test_missing_values_are_not_confused_with_real_zero() -> None:
    tracker = RollingDynamicsTracker(windows=(5,))
    baseline = _observation(
        minute=41,
        totals={"xg_total": 9, "shots_on_target_total": 0},
        unavailable=("xg_home",),
    )
    current = _observation(
        minute=46,
        totals={
            "xg_total": 10,
            "shots_on_target_total": 3,
            "shots_in_box_total": 4,
        },
        unavailable=("shots_in_box_away",),
    )
    assert tracker.ingest(baseline) is True

    window = _window(tracker, current, "5m")

    assert window["deltas"]["shots_on_target_total"] == 3.0
    assert window["availability"]["shots_on_target_total"] is True
    assert window["deltas"]["xg_total"] is None
    assert window["invalid_reasons"]["xg_total"] == "baseline_missing"
    assert window["deltas"]["shots_in_box_total"] is None
    assert window["invalid_reasons"]["shots_in_box_total"] == "current_missing"


def test_xg_source_mismatch_invalidates_only_xg_delta() -> None:
    tracker = RollingDynamicsTracker(windows=(5,))
    tracker.ingest(
        _observation(
            minute=41,
            totals={"xg_total": 0.5, "total_shots_total": 4},
            xg_source="estimated_fallback",
        )
    )

    window = _window(
        tracker,
        _observation(
            minute=46,
            totals={"xg_total": 1.0, "total_shots_total": 7},
            xg_source="api",
        ),
        "5m",
    )

    assert window["deltas"]["xg_total"] is None
    assert window["invalid_reasons"]["xg_total"] == "source_mismatch"
    assert window["deltas"]["total_shots_total"] == 3.0


def test_counter_reset_is_invalid_but_negative_pressure_is_valid() -> None:
    tracker = RollingDynamicsTracker(windows=(5,))
    tracker.ingest(
        _observation(
            minute=41,
            totals={"total_shots_total": 10},
            pressure=20,
            score=(1, 0),
        )
    )

    window = _window(
        tracker,
        _observation(
            minute=46,
            totals={"total_shots_total": 8},
            pressure=15,
            score=(0, 0),
        ),
        "5m",
    )

    assert window["deltas"]["total_shots_total"] is None
    assert window["invalid_reasons"]["total_shots_total"] == "counter_reset"
    assert window["deltas"]["score_total"] is None
    assert window["invalid_reasons"]["score_total"] == "counter_reset"
    assert window["deltas"]["pressure_index"] == -5.0
    assert window["availability"]["pressure_index"] is True


def test_score_delta_uses_snapshot_and_never_outcome_final_score() -> None:
    tracker = RollingDynamicsTracker(windows=(5,))
    tracker.ingest(_observation(minute=41, score=(0, 0)))
    current = _observation(minute=46, score=(1, 0))
    resolved = copy.deepcopy(current)
    resolved["outcome"] = {
        "status": "resolved",
        "normal_time_final_score_home": 9,
        "normal_time_final_score_away": 9,
        "goals_after_snapshot": 17,
    }

    pending_window = _window(tracker, current, "5m")
    resolved_window = _window(tracker, resolved, "5m")

    assert pending_window == resolved_window
    assert pending_window["deltas"]["score_total"] == 1.0


def test_future_and_late_replay_rows_cannot_change_past_features() -> None:
    tracker = RollingDynamicsTracker(windows=(5,))
    tracker.ingest(_observation(minute=46, totals={"total_shots_total": 4}))
    current = _observation(minute=51, totals={"total_shots_total": 7})
    before = tracker.compute(current)

    tracker.ingest(_observation(minute=56, totals={"total_shots_total": 99}))
    after = tracker.compute(current)

    assert after == before
    assert after["windows"]["5m"]["deltas"]["total_shots_total"] == 3.0

    late_tracker = RollingDynamicsTracker(windows=(5,))
    late_tracker.ingest(
        _observation(
            minute=46,
            created_at=START + timedelta(minutes=52),
            observation_id="late-replay-46",
        )
    )
    late_window = _window(late_tracker, current, "5m")

    assert late_window["status"] == "unavailable"
    assert late_window["reason"] == "baseline_missing"


def test_ingest_is_idempotent_for_replayed_observation() -> None:
    tracker = RollingDynamicsTracker(windows=(5,))
    baseline = _observation(
        minute=41,
        observation_id="stable-baseline",
        totals={"total_shots_total": 4},
    )
    current = _observation(minute=46, totals={"total_shots_total": 6})

    assert tracker.ingest(baseline) is True
    first = tracker.compute(current)
    assert tracker.ingest(copy.deepcopy(baseline)) is False
    second = tracker.compute(current)

    assert second == first
    assert len(tracker._history[101]) == 1


def test_invalid_and_non_finite_values_fail_closed_and_are_json_safe() -> None:
    tracker = RollingDynamicsTracker(windows=(5,))
    invalid = {
        "observation_id": "invalid",
        "fixture_id": "not-an-id",
        "minute": 46,
        "created_at_utc": "not-a-time",
    }

    assert tracker.ingest(invalid) is False
    invalid_payload = tracker.compute(invalid)
    assert invalid_payload["windows"]["5m"]["reason"] == "invalid_current"

    baseline = _observation(minute=41, pressure=float("inf"))
    baseline["raw_metrics"]["shots_on_target_home"] = float("nan")
    current = _observation(
        minute=46,
        totals={"shots_on_target_total": 3},
        pressure=10,
    )
    assert tracker.ingest(baseline) is True

    window = _window(tracker, current, "5m")

    assert window["invalid_reasons"]["shots_on_target_total"] == "baseline_missing"
    assert window["invalid_reasons"]["pressure_index"] == "baseline_missing"
    json.dumps(window, allow_nan=False)


def test_score_only_empty_seed_never_makes_activity_window_available() -> None:
    tracker = RollingDynamicsTracker(windows=(5,))
    unavailable = tuple(
        f"{raw_name}_{side}"
        for raw_name in PAIR_METRICS.values()
        for side in ("home", "away")
    )
    empty_seed = _observation(
        minute=41,
        pressure=None,
        unavailable=unavailable,
        stage="rolling_seed",
    )

    assert tracker.ingest(empty_seed) is False
    payload = tracker.compute(_observation(minute=46))

    assert payload["available_windows"] == 0
    assert payload["windows"]["5m"]["status"] == "unavailable"
    assert payload["windows"]["5m"]["reason"] == "baseline_missing"


def test_bridge_seeds_keep_both_windows_available_from_46_through_60() -> None:
    tracker = RollingDynamicsTracker()
    for minute in (36, 39, 41, 44):
        tracker.ingest(_observation(minute=minute, stage="rolling_seed"))

    payloads = {}
    for minute in range(46, 61):
        current = _observation(minute=minute)
        payload = tracker.compute(current)
        payloads[minute] = payload
        assert payload["available_windows"] == 2
        for window_name, target_span in (("5m", 5), ("10m", 10)):
            window = payload["windows"][window_name]
            assert window["status"] == "ok"
            assert target_span <= window["actual_span_minutes"] <= target_span + 2
        tracker.ingest(current)

    assert payloads[49]["windows"]["5m"]["baseline_minute"] == 44
    assert payloads[49]["windows"]["10m"]["baseline_minute"] == 39
    assert payloads[54]["windows"]["10m"]["baseline_minute"] == 44
    assert payloads[55]["windows"]["10m"]["baseline_minute"] == 44


def test_batch_freeze_is_deterministic_in_observation_time_order() -> None:
    rows = [
        _observation(
            minute=36,
            totals={"total_shots_total": 2},
            stage="rolling_seed",
        ),
        _observation(
            minute=41,
            totals={"total_shots_total": 5},
            stage="rolling_seed",
        ),
        _observation(minute=46, totals={"total_shots_total": 9}),
    ]

    chronological = freeze_rolling_dynamics_batch(rows)
    reversed_input = freeze_rolling_dynamics_batch(reversed(rows))

    assert reversed_input == chronological
    current = next(row for row in chronological if row["minute"] == 46)
    assert current["rolling_dynamics"]["windows"]["5m"][
        "deltas"
    ]["total_shots_total"] == 4.0
    assert current["rolling_dynamics"]["windows"]["10m"][
        "deltas"
    ]["total_shots_total"] == 7.0
