from __future__ import annotations

from datetime import datetime, timedelta, timezone

import signal_roi as roi


def test_pnl_units_win_loss_push():
    assert abs(roi.pnl_units(outcome="WIN", over_decimal=2.35) - 1.35) < 1e-9
    assert roi.pnl_units(outcome="LOSS", over_decimal=2.35) == -1.0
    assert roi.pnl_units(outcome="PUSH", over_decimal=2.35) == 0.0


def test_outcome_helpers():
    assert roi.european_outcome_from_goals(goals_at_signal=5, goals_final=5) == "LOSS"
    assert roi.european_outcome_from_goals(goals_at_signal=5, goals_final=6) == "WIN"
    assert roi.asian_outcome_from_goals(goals_at_signal=5, goals_final=5) == "LOSS"
    assert roi.asian_outcome_from_goals(goals_at_signal=5, goals_final=6) == "PUSH"
    assert roi.asian_outcome_from_goals(goals_at_signal=5, goals_final=7) == "WIN"


def test_dual_open_settle_and_compare(tmp_path):
    path = tmp_path / "signal_roi_ledger.json"
    epoch = datetime(2026, 11, 3, 21, 0, tzinfo=timezone.utc)
    ledger = roi.empty_ledger(epoch_utc=epoch.isoformat())

    # Signal at 2-3: EU line 5.5 @ 1.20, Asian line 6.0 @ 2.35
    roi.open_published_signal_bet(
        ledger,
        fixture_id=101,
        sent_at_utc=epoch + timedelta(minutes=5),
        signal_date_msk="2026-11-03",
        signal_minute=55,
        score_home=2,
        score_away=3,
        eu_over_decimal=1.20,
        eu_quote_status="available",
        eu_line=5.5,
        asian_over_decimal=2.35,
        asian_quote_status="available",
        asian_line=6.0,
    )
    # Only EU quote available
    roi.open_published_signal_bet(
        ledger,
        fixture_id=102,
        sent_at_utc=epoch + timedelta(hours=1),
        signal_date_msk="2026-11-03",
        signal_minute=50,
        score_home=0,
        score_away=0,
        eu_over_decimal=1.40,
        eu_quote_status="available",
        eu_line=0.5,
        asian_over_decimal=None,
        asian_quote_status="no_recent_quote",
    )

    # Final 2-5 = +2 goals after signal
    # EU: WIN +0.20 ; Asian: WIN +1.35
    roi.settle_published_signal_bet(ledger, fixture_id=101, goals_final=7)
    # Final 0-1 = +1 goal
    # EU: WIN +0.40 ; Asian: excluded (no quote)
    roi.settle_published_signal_bet(ledger, fixture_id=102, goals_final=1)
    roi.save_ledger(ledger, path)

    loaded = roi.load_ledger(path)
    legs101 = loaded["bets"]["101"]["legs"]
    assert legs101[roi.LEG_EU]["outcome"] == "WIN"
    assert legs101[roi.LEG_ASIAN]["outcome"] == "WIN"
    assert abs(legs101[roi.LEG_EU]["pnl_units"] - 0.20) < 1e-9
    assert abs(legs101[roi.LEG_ASIAN]["pnl_units"] - 1.35) < 1e-9
    assert loaded["bets"]["102"]["legs"][roi.LEG_ASIAN]["status"] == "excluded"
    assert loaded["bets"]["102"]["legs"][roi.LEG_EU]["outcome"] == "WIN"

    eu = roi.summarize_period(loaded, "all", leg=roi.LEG_EU, now_utc=epoch + timedelta(days=1))
    asian = roi.summarize_period(
        loaded, "all", leg=roi.LEG_ASIAN, now_utc=epoch + timedelta(days=1)
    )
    assert eu.settled == 2 and eu.plus == 2
    assert abs(eu.pnl_units - 0.60) < 1e-9
    assert asian.settled == 1 and asian.plus == 1 and asian.excluded == 1
    assert abs(asian.pnl_units - 1.35) < 1e-9

    message = roi.format_dual_roi_message(eu, asian)
    assert "Over +0.5" in message
    assert "азиат" in message.lower() or "Азиат" in message
    assert "Сравнение ROI" in message


def test_push_only_on_asian(tmp_path):
    ledger = roi.empty_ledger(epoch_utc="2026-11-03T21:00:00+00:00")
    roi.open_published_signal_bet(
        ledger,
        fixture_id=7,
        sent_at_utc="2026-11-03T22:00:00+00:00",
        signal_date_msk="2026-11-03",
        signal_minute=50,
        score_home=1,
        score_away=0,
        eu_over_decimal=1.25,
        eu_quote_status="available",
        eu_line=1.5,
        asian_over_decimal=1.80,
        asian_quote_status="available",
        asian_line=2.0,
    )
    # Exactly +1 goal after signal
    roi.settle_published_signal_bet(ledger, fixture_id=7, goals_final=2)
    legs = ledger["bets"]["7"]["legs"]
    assert legs[roi.LEG_EU]["outcome"] == "WIN"
    assert abs(legs[roi.LEG_EU]["pnl_units"] - 0.25) < 1e-9
    assert legs[roi.LEG_ASIAN]["outcome"] == "PUSH"
    assert legs[roi.LEG_ASIAN]["pnl_units"] == 0.0


def test_open_is_idempotent():
    ledger = roi.empty_ledger(epoch_utc="2026-11-03T21:00:00+00:00")
    first = roi.open_published_signal_bet(
        ledger,
        fixture_id=7,
        sent_at_utc="2026-11-03T22:00:00+00:00",
        signal_date_msk="2026-11-03",
        signal_minute=50,
        score_home=0,
        score_away=0,
        eu_over_decimal=1.25,
        eu_quote_status="available",
        asian_over_decimal=1.55,
        asian_quote_status="available",
    )
    second = roi.open_published_signal_bet(
        ledger,
        fixture_id=7,
        sent_at_utc="2026-11-03T22:05:00+00:00",
        signal_date_msk="2026-11-03",
        signal_minute=55,
        score_home=1,
        score_away=0,
        eu_over_decimal=1.40,
        eu_quote_status="available",
        asian_over_decimal=1.80,
        asian_quote_status="available",
    )
    assert first["legs"][roi.LEG_EU]["over_decimal"] == 1.25
    assert second["legs"][roi.LEG_EU]["over_decimal"] == 1.25
    assert len(ledger["bets"]) == 1
