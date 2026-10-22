from __future__ import annotations

from shadow_ml.publication_confirm import (
    apply_rolling_to90_publication_confirm,
    extract_rolling_to90_pct,
)


def _rolling_prediction(
    *,
    to90_pct: float | None = 80.0,
    status: str = "ok",
    model_status: str = "offline_ready",
) -> dict:
    predictions = {}
    if to90_pct is not None:
        predictions["to90"] = {
            "status": status,
            "calibrated_probability_pct": to90_pct,
        }
    return {
        "model_id": "shadow_ml_rolling:test",
        "model_status": model_status,
        "predictions": predictions,
    }


def test_extract_rolling_to90_pct() -> None:
    assert extract_rolling_to90_pct(_rolling_prediction(to90_pct=81.5)) == 81.5
    assert extract_rolling_to90_pct(None) is None
    assert (
        extract_rolling_to90_pct(_rolling_prediction(to90_pct=81.5, status="error"))
        is None
    )


def test_confirm_disabled_passes_champion_through() -> None:
    result = apply_rolling_to90_publication_confirm(
        champion_allow=True,
        rolling_prediction=_rolling_prediction(to90_pct=10.0),
        enabled=False,
    )
    assert result["allow"] is True
    assert result["applied"] is False
    assert result["reason"] == "confirm_disabled"


def test_confirm_blocks_low_rolling_to90() -> None:
    result = apply_rolling_to90_publication_confirm(
        champion_allow=True,
        rolling_prediction=_rolling_prediction(to90_pct=70.0),
        enabled=True,
        min_to90_pct=75.0,
    )
    assert result["allow"] is False
    assert result["applied"] is True
    assert result["reason"] == "confirm_below_threshold"
    assert result["rolling_to90_pct"] == 70.0


def test_confirm_passes_high_rolling_to90() -> None:
    result = apply_rolling_to90_publication_confirm(
        champion_allow=True,
        rolling_prediction=_rolling_prediction(to90_pct=76.0),
        enabled=True,
        min_to90_pct=75.0,
    )
    assert result["allow"] is True
    assert result["reason"] == "confirm_pass"


def test_confirm_fail_open_when_prediction_missing() -> None:
    result = apply_rolling_to90_publication_confirm(
        champion_allow=True,
        rolling_prediction=None,
        enabled=True,
    )
    assert result["allow"] is True
    assert result["applied"] is True
    assert result["reason"] == "fail_open_unavailable"


def test_confirm_fail_open_when_not_offline_ready() -> None:
    result = apply_rolling_to90_publication_confirm(
        champion_allow=True,
        rolling_prediction=_rolling_prediction(
            to90_pct=90.0, model_status="collecting"
        ),
        enabled=True,
    )
    assert result["allow"] is True
    assert result["reason"] == "fail_open_not_offline_ready"


def test_confirm_does_not_create_pass_when_champion_blocked() -> None:
    result = apply_rolling_to90_publication_confirm(
        champion_allow=False,
        rolling_prediction=_rolling_prediction(to90_pct=99.0),
        enabled=True,
    )
    assert result["allow"] is False
    assert result["applied"] is False
    assert result["reason"] == "champion_blocked"
