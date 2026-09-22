"""PR-1: Admin Review wiring removed from main_loop, startup, and callbacks.

Legacy review helpers remain defined until PR-2; this module locks behavior.
"""

from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import NanoTest as bot


def _main_loop_function_source() -> str:
    path = Path(bot.__file__).resolve()
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "main_loop":
            segment = ast.get_source_segment(text, node)
            assert segment is not None
            return segment
    pytest.fail("main_loop() not found in NanoTest.py")


def test_main_loop_does_not_call_send_review_to_admin() -> None:
    source = _main_loop_function_source()
    assert "send_review_to_admin" not in source
    assert "ENABLE_ADMIN_REVIEW_SIGNALS" not in source


def test_main_startup_does_not_start_review_timeout_daemon() -> None:
    source = _main_loop_function_source()
    assert "start_review_timeout_daemon" not in source


def test_legacy_review_send_callback_does_not_publish(monkeypatch) -> None:
    send_channel = MagicMock(return_value=999)
    monkeypatch.setattr(bot, "send_to_telegram", send_channel)
    monkeypatch.setattr(bot, "TELEGRAM_TOKEN", "test-token")
    monkeypatch.setattr(bot.requests, "post", lambda *args, **kwargs: MagicMock(ok=True))

    fixture_id = 424242
    with bot.state_lock:
        bot.state.setdefault("review_queue", {})[str(fixture_id)] = {
            "review_sent": True,
            "review_decision": None,
            "review_ts": 1.0,
            "data": {"fixture": {"fixture_id": fixture_id}},
            "prob": 55.0,
            "minute": 50,
        }

    bot.handle_callback_query(
        {
            "id": "cb-1",
            "data": f"review_send:{fixture_id}",
            "from": {"id": 1},
            "message": {"message_id": 10, "chat": {"id": 2}},
        }
    )

    send_channel.assert_not_called()


def test_ordinary_resolve_signal_header_unchanged() -> None:
    title = bot.resolve_signal_header_title(is_admin_approved=False, signal_route="primary")
    assert title == "🚨 Сигнал"


def test_ordinary_telegram_delivery_decision_unchanged() -> None:
    assert bot.resolve_final_decision_after_telegram_delivery(100) == ("ALLOW", None)
    assert bot.resolve_final_decision_after_telegram_delivery(None) == (
        "BLOCK",
        "telegram-send-failed",
    )


def test_historical_admin_header_still_renders() -> None:
    title = bot.resolve_signal_header_title(is_admin_approved=True)
    assert title == "🚨 Сигнал от админа"
