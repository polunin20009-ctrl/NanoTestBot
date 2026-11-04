from __future__ import annotations

from datetime import datetime, timedelta, timezone

import signal_roi as roi


def test_pnl_units_win_loss_push():
    assert abs(roi.pnl_units(outcome="WIN", over_decimal=2.35) - 1.35) < 1e-9
    assert roi.pnl_units(outcome="LOSS", over_decimal=2.35) == -1.0
    assert roi.pnl_units(outcome="PUSH", over_decimal=2.35) == 0.0


def test_asian_outcome_from_goals():
    assert roi.asian_outcome_from_goals(goals_at_signal=5, goals_final=5) == "LOSS"
    assert roi.asian_outcome_from_goals(goals_at_signal=5, goals_final=6) == "PUSH"
    assert roi.asian_outcome_from_goals(goals_at_signal=5, goals_final=7) == "WIN"
    assert roi.asian_outcome_from_goals(goals_at_signal=0, goals_final=2) == "WIN"


def test_open_settle_asian_and_period_roi(tmp_path):
    path = tmp_path / "signal_roi_ledger.json"
    epoch = datetime(2026, 11, 3, 21, 0, tzinfo=timezone.utc)
    ledger = roi.empty_ledger(epoch_utc=epoch.isoformat())

    roi.open_published_signal_bet(
        ledger,
        fixture_id=101,
        sent_at_utc=epoch + timedelta(minutes=5),
        signal_date_msk="2026-11-03",
        signal_minute=51,
        score_home=2,
        score_away=3,
        over_decimal=2.35,
        quote_status="available",
        line=6.0,
    )
    roi.open_published_signal_bet(
        ledger,
        fixture_id=102,
        sent_at_utc=epoch + timedelta(hours=1),
        signal_date_msk="2026-11-03",
        signal_minute=55,
        score_home=0,
        score_away=0,
        over_decimal=1.55,
        quote_status="available",
        line=1.0,
    )
    roi.open_published_signal_bet(
        ledger,
        fixture_id=104,
        sent_at_utc=epoch + timedelta(hours=1, minutes=30),
        signal_date_msk="2026-11-03",
        signal_minute=60,
        score_home=1,
        score_away=0,
        over_decimal=1.80,
        quote_status="available",
        line=2.0,
    )
    # No quote -> excluded from ROI
    roi.open_published_signal_bet(
        ledger,
        fixture_id=103,
        sent_at_utc=epoch + timedelta(hours=2),
        signal_date_msk="2026-11-03",
        signal_minute=48,
        score_home=2,
        score_away=1,
        over_decimal=None,
        quote_status="no_recent_quote",
    )

    # 2-3 -> final 2-5 = +2 goals after signal => WIN +(2.35-1)
    roi.settle_published_signal_bet(ledger, fixture_id=101, goals_final=7)
    # 0-0 -> final 0-1 = +1 => PUSH 0
    roi.settle_published_signal_bet(ledger, fixture_id=102, goals_final=1)
    # 1-0 -> final 1-0 = +0 => LOSS -1
    roi.settle_published_signal_bet(ledger, fixture_id=104, goals_final=1)
    roi.save_ledger(ledger, path)

    loaded = roi.load_ledger(path)
    assert loaded["bets"]["101"]["outcome"] == "WIN"
    assert loaded["bets"]["102"]["outcome"] == "PUSH"
    assert loaded["bets"]["104"]["outcome"] == "LOSS"
    assert abs(loaded["bets"]["101"]["pnl_units"] - 1.35) < 1e-9
    assert loaded["bets"]["102"]["pnl_units"] == 0.0

    summary = roi.summarize_period(
        loaded,
        "all",
        now_utc=epoch + timedelta(days=1),
    )
    assert summary.signals == 4
    assert summary.settled == 3
    assert summary.plus == 1
    assert summary.minus == 1
    assert summary.push == 1
    assert summary.excluded == 1
    assert abs(summary.pnl_units - (1.35 - 1.0)) < 1e-9
    assert abs(summary.roi_pct - ((1.35 - 1.0) / 3.0 * 100.0)) < 1e-9

    message = roi.format_roi_message(summary)
    assert "ROI:" in message
    assert "азиат" in message.lower() or "Азиат" in message or "счёт+1" in message


def test_open_is_idempotent(tmp_path):
    ledger = roi.empty_ledger(epoch_utc="2026-11-03T21:00:00+00:00")
    first = roi.open_published_signal_bet(
        ledger,
        fixture_id=7,
        sent_at_utc="2026-11-03T22:00:00+00:00",
        signal_date_msk="2026-11-03",
        signal_minute=50,
        score_home=0,
        score_away=0,
        over_decimal=1.55,
        quote_status="available",
        line=1.0,
    )
    second = roi.open_published_signal_bet(
        ledger,
        fixture_id=7,
        sent_at_utc="2026-11-03T22:05:00+00:00",
        signal_date_msk="2026-11-03",
        signal_minute=55,
        score_home=1,
        score_away=0,
        over_decimal=1.80,
        quote_status="available",
        line=2.0,
    )
    assert first["market"]["over_decimal"] == 1.55
    assert second["market"]["over_decimal"] == 1.55
    assert first["market"]["selection"] == "over_current_goals_plus_1_0"
    assert first["status"] == "open"
    assert len(ledger["bets"]) == 1
