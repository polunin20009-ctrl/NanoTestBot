from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import report_shadow_ml
from scripts.report_shadow_ml import build_report


def _observation(fixture_id: int, label: bool) -> tuple[dict, dict]:
    observation_id = f"{fixture_id}:46:WINDOW_1:v2"
    observation = {
        "record_type": "observation",
        "observation_id": observation_id,
        "observation_key": observation_id,
        "fixture_id": fixture_id,
        "schema_version": 1,
        "stage": "decision_pipeline",
        "created_at_utc": "2026-07-01T12:00:00+00:00",
        "minute": 46,
        "outcome": {"status": "pending"},
    }
    outcome = {
        "record_type": "observation_outcome",
        "observation_id": observation_id,
        "observation_key": observation_id,
        "fixture_id": fixture_id,
        "outcome_schema_version": 1,
        "created_at_utc": "2026-07-02T12:00:00+00:00",
        "outcome": {
            "status": "resolved",
            "resolved_at_utc": "2026-07-02T12:00:00+00:00",
            "goal_within_15": label,
            "goal_within_15_quality": "exact",
            "goal_to90_normal_time": label,
        },
    }
    return observation, outcome


def _prediction(
    fixture_id: int,
    *,
    candidate: float,
    baseline: float,
    cutoff: str = "2026-06-30T12:00:00+00:00",
    model_created_at: str = "2026-06-30T12:01:00+00:00",
    prediction_created_at: str = "2026-07-01T12:01:00+00:00",
) -> dict:
    observation_id = f"{fixture_id}:46:WINDOW_1:v2"
    target = {
        "status": "ok",
        "calibrated_probability_pct": candidate,
        "base_probability_pct": baseline,
        "production_applied": False,
    }
    return {
        "record_type": "shadow_ml_prediction",
        "prediction_key": f"prediction:{observation_id}:model-1",
        "observation_id": observation_id,
        "fixture_id": fixture_id,
        "created_at_utc": prediction_created_at,
        "model_created_at_utc": model_created_at,
        "model_data_cutoff_utc": cutoff,
        "model_id": "model-1",
        "algorithm_version": "residual_logistic_stdlib_v1",
        "production_applied": False,
        "predictions": {"next15": dict(target), "to90": dict(target)},
    }


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )


@pytest.mark.parametrize(
    ("percentage", "expected"),
    [
        (0.0, 0.000001),
        (0.5, 0.005),
        (1.0, 0.01),
        (80.0, 0.8),
        (100.0, 0.999999),
    ],
)
def test_probability_pct_is_never_misread_as_fraction(
    percentage: float,
    expected: float,
) -> None:
    assert report_shadow_ml._probability_pct(percentage) == expected


def test_paired_bootstrap_requires_real_improvement() -> None:
    identical: list[dict] = []
    improved: list[dict] = []
    for fixture_id in range(1, 41):
        label = fixture_id % 2
        identical.append(
            {
                "fixture_id": fixture_id,
                "label": label,
                "candidate": 0.5,
                "baseline": 0.5,
                "quality_weight": 1.0,
            }
        )
        improved.append(
            {
                "fixture_id": fixture_id,
                "label": label,
                "candidate": 0.8 if label else 0.2,
                "baseline": 0.5,
                "quality_weight": 1.0,
            }
        )

    no_gain = report_shadow_ml._paired_fixture_improvement(
        identical,
        seed=1,
    )
    real_gain = report_shadow_ml._paired_fixture_improvement(
        improved,
        seed=1,
    )

    assert no_gain["log_loss"] == 0.0
    assert no_gain["brier"] == 0.0
    assert no_gain["log_loss_ci95"] == [0.0, 0.0]
    assert real_gain["log_loss_ci95"][0] > 0.0
    assert real_gain["brier_ci95"][0] > 0.0


def test_report_uses_only_prospective_predictions_and_fixture_weights(
    tmp_path: Path,
) -> None:
    observations = tmp_path / "observation_history.jsonl"
    predictions = tmp_path / "shadow_ml_predictions.jsonl"
    model = tmp_path / "shadow_ml_model.json"
    observation_records: list[dict] = []
    for fixture_id, label in ((1, True), (2, False)):
        observation_records.extend(_observation(fixture_id, label))
    _write_jsonl(observations, observation_records)
    _write_jsonl(
        predictions,
        [
            _prediction(1, candidate=80.0, baseline=60.0),
            _prediction(2, candidate=20.0, baseline=40.0),
        ],
    )
    model.write_text('{"status":"collecting"}\n', encoding="utf-8")

    report = build_report(
        str(observations),
        str(predictions),
        str(model),
    )
    target = report["by_algorithm"]["residual_logistic_stdlib_v1"][
        "targets"
    ]["next15"]

    assert report["prospective_records_joined"] == 2
    assert target["candidate"]["fixtures"] == 2
    assert target["candidate"]["actual_rate"] == 0.5
    assert target["candidate"]["brier"] == 0.04
    assert target["baseline"]["brier"] == 0.16
    assert target["status"] == "collecting"


def test_report_rejects_prediction_trained_after_observation(
    tmp_path: Path,
) -> None:
    observations = tmp_path / "observation_history.jsonl"
    predictions = tmp_path / "shadow_ml_predictions.jsonl"
    model = tmp_path / "shadow_ml_model.json"
    _write_jsonl(observations, list(_observation(1, True)))
    _write_jsonl(
        predictions,
        [
            _prediction(
                1,
                candidate=90.0,
                baseline=50.0,
                cutoff="2026-07-01T13:00:00+00:00",
            )
        ],
    )
    model.write_text("{}\n", encoding="utf-8")

    report = build_report(
        str(observations),
        str(predictions),
        str(model),
    )

    assert report["prospective_records_joined"] == 0
    assert report["skipped"]["non_prospective_timing"] == 1


@pytest.mark.parametrize(
    ("model_created_at", "prediction_created_at"),
    [
        (
            "2026-07-01T12:00:01+00:00",
            "2026-07-01T12:01:00+00:00",
        ),
        (
            "2026-06-30T12:01:00+00:00",
            "2026-07-01T11:59:59+00:00",
        ),
    ],
)
def test_report_rejects_retrospective_prediction_timestamps(
    tmp_path: Path,
    model_created_at: str,
    prediction_created_at: str,
) -> None:
    observations = tmp_path / "observation_history.jsonl"
    predictions = tmp_path / "shadow_ml_predictions.jsonl"
    model = tmp_path / "shadow_ml_model.json"
    _write_jsonl(observations, list(_observation(1, True)))
    _write_jsonl(
        predictions,
        [
            _prediction(
                1,
                candidate=90.0,
                baseline=50.0,
                model_created_at=model_created_at,
                prediction_created_at=prediction_created_at,
            )
        ],
    )
    model.write_text("{}\n", encoding="utf-8")

    report = build_report(
        str(observations),
        str(predictions),
        str(model),
    )

    assert report["prospective_records_joined"] == 0
    assert report["skipped"]["non_prospective_timing"] == 1


def test_report_deduplicates_prediction_key_across_active_and_archive(
    tmp_path: Path,
) -> None:
    observations = tmp_path / "observation_history.jsonl"
    predictions = tmp_path / "shadow_ml_predictions.jsonl"
    archive = tmp_path / "shadow_ml_predictions.20260701T120100Z.jsonl"
    model = tmp_path / "shadow_ml_model.json"
    _write_jsonl(observations, list(_observation(1, True)))
    prediction = _prediction(1, candidate=80.0, baseline=60.0)
    _write_jsonl(archive, [prediction])
    _write_jsonl(predictions, [prediction])
    model.write_text("{}\n", encoding="utf-8")

    report = build_report(
        str(observations),
        str(predictions),
        str(model),
    )

    assert report["prediction_records_seen"] == 2
    assert report["prospective_records_joined"] == 1
    assert report["skipped"]["duplicate_prediction_key"] == 1


@pytest.mark.parametrize("quality", ["", "unknown", "future_v9"])
def test_report_rejects_unknown_next15_label_quality(
    tmp_path: Path,
    quality: str,
) -> None:
    observations = tmp_path / "observation_history.jsonl"
    predictions = tmp_path / "shadow_ml_predictions.jsonl"
    model = tmp_path / "shadow_ml_model.json"
    observation, outcome = _observation(1, True)
    outcome["outcome"]["goal_within_15_quality"] = quality
    _write_jsonl(observations, [observation, outcome])
    _write_jsonl(
        predictions,
        [_prediction(1, candidate=80.0, baseline=60.0)],
    )
    model.write_text("{}\n", encoding="utf-8")

    report = build_report(
        str(observations),
        str(predictions),
        str(model),
    )

    targets = report["by_model"]["model-1"]["targets"]
    assert targets["next15"]["candidate"]["rows"] == 0
    assert targets["to90"]["candidate"]["rows"] == 1


def test_report_output_is_atomic_and_cannot_replace_sources(
    monkeypatch,
    tmp_path: Path,
) -> None:
    observation = tmp_path / "observation_history.jsonl"
    prediction = tmp_path / "shadow_ml_predictions.jsonl"
    model = tmp_path / "shadow_ml_model.json"
    output = tmp_path / "report.json"
    for path in (observation, prediction, model):
        path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(
        report_shadow_ml.bot,
        "_observation_history_paths",
        lambda path: [str(observation)],
    )
    monkeypatch.setattr(
        report_shadow_ml.prediction_storage,
        "_prediction_paths",
        lambda path: [str(prediction)],
    )

    for protected in (observation, prediction, model):
        with pytest.raises(ValueError, match="cannot replace"):
            report_shadow_ml._assert_safe_output(
                protected,
                observation_path=str(observation),
                prediction_path=str(prediction),
                model_path=str(model),
            )

    report_shadow_ml._assert_safe_output(
        output,
        observation_path=str(observation),
        prediction_path=str(prediction),
        model_path=str(model),
    )
    report_shadow_ml._write_text_atomic(output, '{"complete":true}\n')
    assert json.loads(output.read_text(encoding="utf-8")) == {
        "complete": True
    }
    assert list(tmp_path.glob(f".{output.name}.*.tmp")) == []
