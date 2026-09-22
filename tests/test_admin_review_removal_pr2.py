"""PR-2: Admin Review definitions and config removed from NanoTest."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import NanoTest as bot

REMOVED_SYMBOLS = (
    "send_review_to_admin",
    "publish_signal_to_channel",
    "can_auto_post_admin",
    "build_review_card",
    "review_timeout_daemon",
    "start_review_timeout_daemon",
    "admin_review_daemon",
    "ENABLE_ADMIN_REVIEW_SIGNALS",
    "PROB_SEND_THRESHOLD",
    "REVIEW_MIN_THRESHOLD",
    "ADMIN_USER_ID",
)


@pytest.mark.parametrize("name", REMOVED_SYMBOLS)
def test_admin_review_symbols_absent_from_module(name: str) -> None:
    assert not hasattr(bot, name), f"{name} should be removed in PR-2"


def test_legacy_bot_state_keys_still_loadable(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "legacy_state.json"
    payload = {
        "review_queue": {
            "1001": {
                "review_sent": True,
                "review_decision": "sent",
                "approved_by_admin": True,
            }
        },
        "review_tracking": {"1001": {"done": True}},
        "admin_reviews": {"1001": {"finished": True}},
        "admin_auto_posted": {"1001": True},
        "approved_matches_auto": [1001],
        "match_goal_status": {
            "1001": {"approved_by_admin": True, "finalized": False, "score": [1, 0]}
        },
        "sent_matches": {},
        "monitored_matches": [],
        "excluded_matches": [],
    }
    state_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(bot, "STATE_FILE", str(state_path))

    with bot.state_lock:
        bot.state.clear()
    bot.load_state()

    with bot.state_lock:
        assert "1001" in bot.state.get("review_queue", {})
        assert bot.state["match_goal_status"]["1001"]["approved_by_admin"] is True

    title = bot.resolve_signal_header_title(is_admin_approved=True)
    assert title == "🚨 Сигнал от админа"


def test_env_example_has_no_admin_review_flags() -> None:
    text = Path(bot.__file__).resolve().parent.joinpath(".env.example").read_text(encoding="utf-8")
    assert "ENABLE_ADMIN_REVIEW_SIGNALS" not in text
    assert "REVIEW_TARGET_CHAT" not in text
