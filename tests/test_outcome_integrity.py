from outcome_integrity import resolve_normal_time_outcome
from scripts.migrate_outcome_integrity import migrate

import gzip
import json


def test_known_false_losses_are_score_delta_wins() -> None:
    for signal, final, count in (
        ((0, 1), (1, 5), 5),
        ((2, 0), (5, 1), 4),
        ((1, 2), (2, 2), 1),
    ):
        result = resolve_normal_time_outcome(signal, final)
        assert result.normal_time_result == "WIN"
        assert result.goal_result_source == "score_delta"
        assert result.normal_time_goal_count_after_signal == count


def test_unchanged_score_is_loss() -> None:
    result = resolve_normal_time_outcome((1, 2), (1, 2))
    assert result.normal_time_result == "LOSS"
    assert result.goal_to90_normal_time is False


def test_missing_events_score_increase_is_win() -> None:
    result = resolve_normal_time_outcome((0, 0), (1, 0), normal_time_event_count=0)
    assert result.normal_time_result == "WIN"
    assert result.goal_result_source == "score_delta"


def test_extra_time_goal_does_not_count_without_normal_time_evidence() -> None:
    result = resolve_normal_time_outcome((1, 1), (1, 1), normal_time_event_count=0)
    assert result.normal_time_result == "LOSS"


def test_event_score_conflict_uses_trustworthy_score_and_records_conflict() -> None:
    result = resolve_normal_time_outcome((1, 1), (1, 1), normal_time_event_count=1)
    assert result.normal_time_result == "LOSS"
    assert result.conflict is True
    assert result.conflict_details


def test_unreliable_score_is_quarantined() -> None:
    result = resolve_normal_time_outcome((2, 0), (1, 0))
    assert result.status == "quarantine"
    assert result.goal_to90_normal_time is None


def test_migration_is_idempotent_and_creates_valid_backup(tmp_path) -> None:
    path = tmp_path / "outcomes.jsonl"
    payload = {
        "signal_id": "fixture:signal",
        "fixture_id": 1490328,
        "signal_score_home": 0,
        "signal_score_away": 1,
        "normal_time_final_score_home": 1,
        "normal_time_final_score_away": 5,
        "goal_after_signal_normal_time": False,
        "goal_to90_normal_time": False,
        "normal_time_result": "LOSS",
        "first_goal_after_signal_minute": None,
    }
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    first = migrate(path)
    second = migrate(path)

    assert first["corrected"] == 1
    assert second["corrected"] == 0
    with gzip.open(first["backup"], "rt", encoding="utf-8") as handle:
        assert json.loads(handle.readline())["normal_time_result"] == "LOSS"
    latest = json.loads(path.read_text(encoding="utf-8").splitlines()[-1])
    assert latest["normal_time_result"] == "WIN"
    assert latest["first_goal_after_signal_minute"] is None
