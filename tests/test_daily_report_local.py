from __future__ import annotations

import json
from datetime import datetime

import NanoTest as bot


def _write_jsonl(path, records: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records),
        encoding="utf-8",
    )


def _snapshot(fixture_id: int, timestamp_utc: str) -> dict:
    return {
        "signal_id": f"signal-{fixture_id}",
        "fixture_id": fixture_id,
        "timestamp_utc": timestamp_utc,
        "signal_sent": True,
    }


def _outcome(
    fixture_id: int,
    timestamp_utc: str,
    result: str,
    first_goal_minute: int | None,
) -> dict:
    return {
        "signal_id": f"signal-{fixture_id}",
        "fixture_id": fixture_id,
        "timestamp_utc": timestamp_utc,
        "normal_time_result": result,
        "first_goal_after_signal_minute": first_goal_minute,
        "normal_time_resolved_at_utc": "2026-07-25T00:30:00+00:00",
    }


def _use_local_files(monkeypatch, tmp_path, snapshots: list[dict], outcomes: list[dict]) -> None:
    snapshots_path = tmp_path / "snapshots.jsonl"
    outcomes_path = tmp_path / "outcomes.jsonl"
    _write_jsonl(snapshots_path, snapshots)
    _write_jsonl(outcomes_path, outcomes)
    monkeypatch.setattr(bot, "MATCH_SNAPSHOTS_JSONL_PATH", str(snapshots_path))
    monkeypatch.setattr(bot, "MATCH_OUTCOMES_JSONL_PATH", str(outcomes_path))
    monkeypatch.setattr(bot, "_get_relevant_report_match_ids", lambda report_date: [])
    with bot.state_lock:
        monkeypatch.setitem(bot.state, "training_signal_records", {})


def test_local_daily_report_uses_moscow_signal_date_and_deduplicates_fixture(
    monkeypatch, tmp_path
) -> None:
    snapshots = [
        _snapshot(101, "2026-07-24T20:00:00+00:00"),  # 23:00 MSK on July 24
        _snapshot(102, "2026-07-24T20:15:00+00:00"),
        _snapshot(103, "2026-07-24T22:30:00+00:00"),  # 01:30 MSK on July 25
    ]
    outcomes = [
        _outcome(101, "2026-07-24T20:00:00+00:00", "WIN", 58),
        _outcome(101, "2026-07-24T20:00:00+00:00", "WIN", 58),
        _outcome(102, "2026-07-24T20:15:00+00:00", "LOSS", None),
        _outcome(103, "2026-07-24T22:30:00+00:00", "WIN", 70),
    ]
    _use_local_files(monkeypatch, tmp_path, snapshots, outcomes)

    collected = bot.collect_local_report_matches("2026-07-24")
    payload = bot.build_daily_report("2026-07-24", collected["matches"])

    assert collected["candidate_match_ids"] == [101, 102]
    assert collected["all_finished"] is True
    assert len(collected["matches"]) == 2
    assert payload["final_matches_count"] == 2
    assert payload["plus"] == 1
    assert payload["minus"] == 1
    assert payload["goals_u60"] == 1
    assert payload["success_pct"] == 50.0


def test_local_daily_report_waits_for_missing_outcome(monkeypatch, tmp_path) -> None:
    snapshots = [
        _snapshot(201, "2026-07-24T12:00:00+00:00"),
        _snapshot(202, "2026-07-24T13:00:00+00:00"),
    ]
    outcomes = [_outcome(201, "2026-07-24T12:00:00+00:00", "WIN", 62)]
    _use_local_files(monkeypatch, tmp_path, snapshots, outcomes)

    collected = bot.collect_local_report_matches("2026-07-24")

    assert collected["all_finished"] is False
    assert collected["unresolved_ids"] == [202]
    assert bot.format_daily_stats_message("2026-07-24") is None


def test_daily_formatter_does_not_access_google_sheets(monkeypatch, tmp_path) -> None:
    snapshots = [_snapshot(301, "2026-07-24T12:00:00+00:00")]
    outcomes = [_outcome(301, "2026-07-24T12:00:00+00:00", "WIN", 76)]
    _use_local_files(monkeypatch, tmp_path, snapshots, outcomes)
    monkeypatch.setattr(
        bot,
        "gsheets_fetch_recent_rows_as_dicts",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Google Sheets accessed")),
    )

    message = bot.format_daily_stats_message("2026-07-24")

    assert message
    assert "1" in message


def test_daily_sender_uses_only_local_report_data(monkeypatch, tmp_path) -> None:
    snapshots = [_snapshot(401, "2026-07-24T12:00:00+00:00")]
    outcomes = [_outcome(401, "2026-07-24T12:00:00+00:00", "LOSS", None)]
    _use_local_files(monkeypatch, tmp_path, snapshots, outcomes)
    monkeypatch.setattr(
        bot,
        "get_msk_datetime",
        lambda: datetime(2026, 7, 25, 8, 0, tzinfo=bot._MSK_TZ),
    )
    monkeypatch.setattr(
        bot,
        "collect_report_matches",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("legacy Sheets collector used")),
    )
    monkeypatch.setattr(bot, "save_persistent_state", lambda: None)
    monkeypatch.setattr(bot, "TELEGRAM_TOKEN", "test-token")
    monkeypatch.setattr(bot, "TELEGRAM_CHAT_ID", -100123)
    monkeypatch.setattr(bot, "STATS_MESSAGE_ID", None)

    sent_payloads: list[dict] = []

    class _Response:
        ok = True
        text = ""

        @staticmethod
        def json():
            return {"ok": True, "result": {"message_id": 777}}

    def _post(url, data, timeout):
        sent_payloads.append(dict(data))
        return _Response()

    monkeypatch.setattr(bot.requests, "post", _post)
    rotated_ids: list[int] = []
    monkeypatch.setattr(
        bot,
        "rotate_pinned_daily_report",
        lambda message_id: rotated_ids.append(int(message_id)) or True,
    )
    with bot.persistent_state_lock:
        monkeypatch.setitem(bot.persistent_state, "daily_report_last_sent_date", "")
        monkeypatch.setitem(bot.persistent_state, "last_daily_stats_sent_date_msk", "")
        monkeypatch.setitem(bot.persistent_state, "pinned_daily_msg_id", None)
        monkeypatch.setitem(bot.persistent_state, "pinned_daily_stats_message_id", None)

    bot.maybe_send_daily_report()

    assert len(sent_payloads) == 1
    assert sent_payloads[0]["chat_id"] == -100123
    assert rotated_ids == [777]
    with bot.persistent_state_lock:
        assert bot.persistent_state["daily_report_last_sent_date"] == "2026-07-24"


def test_daily_pin_rotation_keeps_instruction_and_replaces_previous_report(monkeypatch) -> None:
    actions: list[tuple[str, int]] = []
    monkeypatch.setattr(
        bot,
        "pin_chat_message",
        lambda message_id: actions.append(("pin", int(message_id))) or True,
    )
    monkeypatch.setattr(
        bot,
        "unpin_chat_message",
        lambda message_id: actions.append(("unpin", int(message_id))) or True,
    )
    monkeypatch.setattr(bot, "save_persistent_state", lambda: None)
    with bot.persistent_state_lock:
        monkeypatch.setitem(bot.persistent_state, "pinned_daily_msg_id", 700)
        monkeypatch.setitem(bot.persistent_state, "pinned_daily_stats_message_id", 700)
        monkeypatch.setitem(bot.persistent_state, "instruction_message_id", 1439)

    assert bot.rotate_pinned_daily_report(701) is True
    assert actions == [("pin", 701), ("unpin", 700)]
    with bot.persistent_state_lock:
        assert bot.persistent_state["pinned_daily_msg_id"] == 701
        assert bot.persistent_state["pinned_daily_stats_message_id"] == 701
        assert bot.persistent_state["instruction_message_id"] == 1439
