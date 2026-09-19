import json

from shadow_ml.champion import compare_holdout_metrics, save_candidate


def _artifact(value: float) -> dict:
    target = {
        "metrics": {
            "holdout": {
                "candidate": {
                    "log_loss": value,
                    "brier": value,
                    "ece": value,
                }
            }
        }
    }
    return {
        "shadow_only": True,
        "production_applied": False,
        "targets": {"next15": target, "to90": target},
    }


def test_worse_candidate_is_not_promotable() -> None:
    comparison = compare_holdout_metrics(_artifact(0.2), _artifact(0.3))
    assert comparison.promotable is False
    assert comparison.reasons


def test_candidate_save_is_separate_and_forces_shadow_flags(tmp_path) -> None:
    champion_path = tmp_path / "champion.json"
    candidate_path = tmp_path / "candidate.json"
    champion_path.write_text('{"model_id":"champion"}', encoding="utf-8")
    save_candidate(candidate_path, {"model_id": "candidate", "production_applied": True})
    assert json.loads(champion_path.read_text(encoding="utf-8"))["model_id"] == "champion"
    candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
    assert candidate["artifact_role"] == "candidate"
    assert candidate["shadow_only"] is True
    assert candidate["production_applied"] is False
