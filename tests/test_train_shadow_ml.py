from __future__ import annotations

from pathlib import Path

import pytest

from scripts import train_shadow_ml


def test_model_output_cannot_replace_observation_source(
    monkeypatch,
    tmp_path: Path,
) -> None:
    observation = tmp_path / "observation_history.jsonl"
    archive = tmp_path / "observation_history.20260725T010203Z.jsonl.gz"
    model = tmp_path / "shadow_ml_model.json"
    observation.write_text("", encoding="utf-8")
    archive.write_bytes(b"archive")
    monkeypatch.setattr(
        train_shadow_ml.bot,
        "_observation_history_paths",
        lambda path: [str(archive), str(observation)],
    )

    with pytest.raises(ValueError, match="cannot replace"):
        train_shadow_ml._assert_safe_output(str(observation), observation)
    with pytest.raises(ValueError, match="cannot replace"):
        train_shadow_ml._assert_safe_output(str(observation), archive)

    train_shadow_ml._assert_safe_output(str(observation), model)


def test_rolling_worker_cannot_replace_static_artifacts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    observation = tmp_path / "observation_history.jsonl"
    static_model = tmp_path / "shadow_ml_model.json"
    static_predictions = tmp_path / "shadow_ml_predictions.jsonl"
    rolling_model = tmp_path / "shadow_ml_rolling_model.json"
    observation.write_text("", encoding="utf-8")
    monkeypatch.setattr(
        train_shadow_ml.bot,
        "_observation_history_paths",
        lambda path: [str(observation)],
    )
    monkeypatch.setattr(
        train_shadow_ml.bot, "SHADOW_ML_MODEL_FILE", str(static_model)
    )
    monkeypatch.setattr(
        train_shadow_ml.bot,
        "SHADOW_ML_PREDICTIONS_FILE",
        str(static_predictions),
    )
    monkeypatch.setattr(
        train_shadow_ml.bot,
        "SHADOW_ML_ROLLING_PREDICTIONS_FILE",
        str(tmp_path / "shadow_ml_rolling_predictions.jsonl"),
    )

    with pytest.raises(ValueError, match="cannot replace"):
        train_shadow_ml._assert_safe_output(
            str(observation), static_model, variant="rolling"
        )
    with pytest.raises(ValueError, match="cannot replace"):
        train_shadow_ml._assert_safe_output(
            str(observation), static_predictions, variant="rolling"
        )
    with pytest.raises(ValueError, match="cannot replace"):
        train_shadow_ml._assert_safe_output(
            str(observation),
            Path(train_shadow_ml.bot.SHADOW_ML_ROLLING_PREDICTIONS_FILE),
            variant="rolling",
        )
    with pytest.raises(ValueError, match="cannot replace"):
        train_shadow_ml._assert_safe_output(
            str(observation), static_predictions, variant="static"
        )
    train_shadow_ml._assert_safe_output(
        str(observation), rolling_model, variant="rolling"
    )
