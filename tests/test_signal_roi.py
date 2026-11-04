from __future__ import annotations

from datetime import datetime, timedelta, timezone

import signal_roi as roi


def test_pnl_units_win_loss():
    assert abs(roi.pnl_units(outcome="WIN", over_decimal=1.13) - 0.13) < 1e-9
    assert roi.pnl_units(outcome="LOSS", over_decimal=1.13) == -1.0


def test_open_settle_and_period_roi(tmp_path):
    path = tmp_path / "signal_roi_ledger.json"
    epoch = datetime(2026, 11, 3, 21, 0, tzinfo=timezone.utc)
    ledger = roi.empty_ledger(epoch_utc=epoch.isoformat())

    roi.open_published_signal_bet(
        ledger,
        fixture_id=101,
        sent_at_utc=epoch + timedelta(minutes=5),
        signal_date_msk="2026-11-03",
        signal_minute=51,
        score_home=1,
        score_away=0,
        over_decimal=1.20,
        quote_status="available",
    )
    roi.open_published_signal_bet(
        ledger,
        fixture_id=102,
        sent_at_utc=epoch + timedelta(hours=1),
        signal_date_msk="2026-11-03",
        signal_minute=55,
        score_home=0,
        score_away=0,
        over_decimal=1.10,
        quote_status="available",
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
    # Pre-epoch ignored for ROI
    roi.open_published_signal_bet(
        ledger,
        fixture_id=100,
        sent_at_utc=epoch - timedelta(hours=1),
        signal_date_msk="2026-11-03",
        signal_minute=50,
        score_home=0,
        score_away=0,
        over_decimal=1.50,
        quote_status="available",
    )

    roi.settle_published_signal_bet(ledger, fixture_id=101, outcome="WIN")
    roi.settle_published_signal_bet(ledger, fixture_id=102, outcome="LOSS")
    roi.save_ledger(ledger, path)

    loaded = roi.load_ledger(path)
    summary = roi.summarize_period(
        loaded,
        "all",
        now_utc=epoch + timedelta(days=1),
    )
    assert summary.signals == 3  # pre-epoch excluded from period by start bound? 
    # fixture 100 sent before epoch: period start=epoch so not included
    assert summary.signals == 3
    assert summary.settled == 2
    assert summary.plus == 1
    assert summary.minus == 1
    assert summary.excluded == 1
    assert abs(summary.pnl_units - (0.20 - 1.0)) < 1e-9
    assert abs(summary.roi_pct - ((0.20 - 1.0) / 2.0 * 100.0)) < 1e-9

    message = roi.format_roi_message(summary)
    assert "ROI:" in message
    assert "1 юнит" in message


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
        over_decimal=1.25,
        quote_status="available",
    )
    second = roi.open_published_signal_bet(
        ledger,
        fixture_id=7,
        sent_at_utc="2026-11-03T22:05:00+00:00",
        signal_date_msk="2026-11-03",
        signal_minute=55,
        score_home=1,
        score_away=0,
        over_decimal=1.40,
        quote_status="available",
    )
    assert first["market"]["over_decimal"] == 1.25
    assert second["market"]["over_decimal"] == 1.25
    assert first["status"] == "open"
    assert len(ledger["bets"]) == 1
