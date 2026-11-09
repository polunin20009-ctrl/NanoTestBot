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
    # Asian leg for 102 had no quote, so it is imputed from the known 2.35 average
    # and settles as PUSH on exactly +1 goal after signal.
    asian102 = loaded["bets"]["102"]["legs"][roi.LEG_ASIAN]
    assert asian102["status"] == "settled"
    assert asian102["odds_source"] == roi.ODDS_SOURCE_IMPUTED_AVG
    assert abs(float(asian102["over_decimal"]) - 2.35) < 1e-9
    assert asian102["outcome"] == "PUSH"
    assert asian102["pnl_units"] == 0.0
    assert loaded["bets"]["102"]["legs"][roi.LEG_EU]["outcome"] == "WIN"

    eu = roi.summarize_period(loaded, "all", leg=roi.LEG_EU, now_utc=epoch + timedelta(days=1))
    asian = roi.summarize_period(
        loaded, "all", leg=roi.LEG_ASIAN, now_utc=epoch + timedelta(days=1)
    )
    assert eu.settled == 2 and eu.plus == 2
    assert abs(eu.pnl_units - 0.60) < 1e-9
    assert asian.settled == 2 and asian.plus == 1 and asian.push == 1 and asian.excluded == 0
    assert asian.imputed == 1
    assert abs(asian.pnl_units - 1.35) < 1e-9

    message = roi.format_dual_roi_message(eu, asian)
    assert "Over +0.5" in message
    assert "азиат" in message.lower() or "Азиат" in message
    assert "Сравнение ROI" in message
    assert "средним кэфом" in message.lower()


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


def test_backfill_imputes_average_without_touching_real_odds():
    ledger = roi.empty_ledger(epoch_utc="2026-11-03T21:00:00+00:00")
    roi.open_published_signal_bet(
        ledger,
        fixture_id=1,
        sent_at_utc="2026-11-03T22:00:00+00:00",
        signal_date_msk="2026-11-03",
        signal_minute=50,
        score_home=1,
        score_away=0,
        eu_over_decimal=1.20,
        eu_quote_status="available",
        asian_over_decimal=1.40,
        asian_quote_status="available",
        asian_line=2.0,
    )
    roi.open_published_signal_bet(
        ledger,
        fixture_id=2,
        sent_at_utc="2026-11-03T22:10:00+00:00",
        signal_date_msk="2026-11-03",
        signal_minute=55,
        score_home=0,
        score_away=0,
        eu_over_decimal=1.15,
        eu_quote_status="available",
        asian_over_decimal=1.20,
        asian_quote_status="available",
        asian_line=1.0,
    )
    roi.open_published_signal_bet(
        ledger,
        fixture_id=3,
        sent_at_utc="2026-11-03T22:20:00+00:00",
        signal_date_msk="2026-11-03",
        signal_minute=60,
        score_home=2,
        score_away=1,
        eu_over_decimal=1.10,
        eu_quote_status="available",
        asian_over_decimal=None,
        asian_quote_status="quote_asian_line_mismatch",
        asian_line=4.0,
    )
    roi.settle_published_signal_bet(ledger, fixture_id=1, goals_final=3)  # +2 WIN
    roi.settle_published_signal_bet(ledger, fixture_id=2, goals_final=0)  # +0 LOSS
    # Fixture 3 already has final goals but was left excluded historically.
    ledger["bets"]["3"]["goals_final"] = 5
    ledger["bets"]["3"]["goals_after_signal"] = 2
    ledger["bets"]["3"]["legs"][roi.LEG_EU]["status"] = "settled"
    ledger["bets"]["3"]["legs"][roi.LEG_EU]["outcome"] = "WIN"
    ledger["bets"]["3"]["legs"][roi.LEG_EU]["pnl_units"] = 0.10
    ledger["bets"]["3"]["legs"][roi.LEG_EU]["settled_at_utc"] = "2026-11-03T23:00:00+00:00"

    avg, known_n = roi.average_known_leg_odds(ledger, roi.LEG_ASIAN)
    assert known_n == 2
    assert abs(float(avg) - 1.30) < 1e-9

    changed = roi.backfill_imputed_odds_and_settle(ledger)
    assert changed >= 1
    asian3 = ledger["bets"]["3"]["legs"][roi.LEG_ASIAN]
    assert asian3["status"] == "settled"
    assert asian3["odds_source"] == roi.ODDS_SOURCE_IMPUTED_AVG
    assert abs(float(asian3["over_decimal"]) - 1.30) < 1e-9
    assert asian3["outcome"] == "WIN"
    assert abs(float(asian3["pnl_units"]) - 0.30) < 1e-9
    # Real odds stay untouched.
    assert ledger["bets"]["1"]["legs"][roi.LEG_ASIAN]["over_decimal"] == 1.40
    assert ledger["bets"]["1"]["legs"][roi.LEG_ASIAN]["odds_source"] == roi.ODDS_SOURCE_REAL
