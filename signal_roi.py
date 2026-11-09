"""Forward-looking dual ROI ledger for published Telegram signals.

Same signal, two parallel 1u stakes:

1) European Over (current goals + 0.5)
   - 0 more goals => LOSS => -1
   - 1+ more      => WIN  => +(odds - 1)

2) Asian Over (current goals + 1.0)
   - 0 more goals => LOSS => -1
   - exactly 1   => PUSH => 0
   - 2+ more     => WIN  => +(odds - 1)

Odds frozen from API-Football live quotes at successful send time.
When a leg has no usable quote, it is settled with the mean of already-known
real odds for that same leg (never overwriting a real quote). Only signals
at/after epoch count.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 3
DEFAULT_STATE_FILE = Path("stats") / "signal_roi_ledger.json"
UNIT_STAKE = 1.0
MODEL_NAME = "dual_over_plus_0_5_and_asian_plus_1_0"

LEG_EU = "eu_plus_0_5"
LEG_ASIAN = "asian_plus_1_0"
LEG_KEYS = (LEG_EU, LEG_ASIAN)

SELECTION_EU = "over_current_goals_plus_0_5"
SELECTION_ASIAN = "over_current_goals_plus_1_0"

ODDS_SOURCE_REAL = "real_quote"
ODDS_SOURCE_IMPUTED_AVG = "imputed_avg_known"

_MSK = timezone(timedelta(hours=3))
_LOCK = threading.RLock()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fmt_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _safe_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _safe_int(value: Any) -> Optional[int]:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number


def default_state_path() -> Path:
    raw = os.environ.get("SIGNAL_ROI_LEDGER_FILE", "").strip()
    return Path(raw) if raw else DEFAULT_STATE_FILE


def empty_ledger(*, epoch_utc: Optional[str] = None) -> Dict[str, Any]:
    epoch = epoch_utc or _fmt_utc(_utc_now())
    return {
        "schema_version": SCHEMA_VERSION,
        "epoch_utc": epoch,
        "updated_at_utc": epoch,
        "model": MODEL_NAME,
        "bets": {},
    }


def _coerce_path(path: Optional[Any] = None) -> Path:
    if path is None or path == "":
        return default_state_path()
    return Path(path)


def load_ledger(path: Optional[Any] = None) -> Dict[str, Any]:
    target = _coerce_path(path)
    if not target.exists():
        return empty_ledger()
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("[SIGNAL_ROI] failed_to_read path=%s", target)
        return empty_ledger()
    if not isinstance(payload, dict):
        return empty_ledger()
    bets = payload.get("bets")
    if not isinstance(bets, dict):
        bets = {}
    epoch = payload.get("epoch_utc") or _fmt_utc(_utc_now())
    return {
        "schema_version": int(payload.get("schema_version") or SCHEMA_VERSION),
        "epoch_utc": str(epoch),
        "updated_at_utc": str(payload.get("updated_at_utc") or epoch),
        "model": str(payload.get("model") or MODEL_NAME),
        "bets": bets,
    }


def save_ledger(ledger: Mapping[str, Any], path: Optional[Any] = None) -> Path:
    target = _coerce_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = deepcopy(dict(ledger))
    payload["schema_version"] = SCHEMA_VERSION
    payload["model"] = MODEL_NAME
    payload["updated_at_utc"] = _fmt_utc(_utc_now())
    tmp = target.with_suffix(target.suffix + ".tmp")
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    tmp.write_text(text + "\n", encoding="utf-8")
    os.replace(tmp, target)
    return target


def ensure_epoch(ledger: Dict[str, Any], *, epoch_utc: Optional[str] = None) -> str:
    existing = _parse_utc(ledger.get("epoch_utc"))
    if existing is not None:
        return _fmt_utc(existing)
    epoch = epoch_utc or _fmt_utc(_utc_now())
    ledger["epoch_utc"] = epoch
    return epoch


def extract_over_decimal(quote: Optional[Mapping[str, Any]]) -> Optional[float]:
    if not isinstance(quote, Mapping):
        return None
    for candidate in (
        quote.get("over_decimal"),
        (quote.get("decimal_odds") or {}).get("over")
        if isinstance(quote.get("decimal_odds"), Mapping)
        else None,
        (quote.get("market") or {}).get("over_decimal")
        if isinstance(quote.get("market"), Mapping)
        else None,
        ((quote.get("market") or {}).get("decimal_odds") or {}).get("over")
        if isinstance(quote.get("market"), Mapping)
        and isinstance((quote.get("market") or {}).get("decimal_odds"), Mapping)
        else None,
    ):
        odds = _safe_float(candidate)
        if odds is not None and odds > 1.0:
            return odds
    return None


def european_outcome_from_goals(*, goals_at_signal: int, goals_final: int) -> str:
    return "WIN" if int(goals_final) - int(goals_at_signal) >= 1 else "LOSS"


def asian_outcome_from_goals(*, goals_at_signal: int, goals_final: int) -> str:
    delta = int(goals_final) - int(goals_at_signal)
    if delta <= 0:
        return "LOSS"
    if delta == 1:
        return "PUSH"
    return "WIN"


def pnl_units(
    *,
    outcome: str,
    over_decimal: float,
    stake_units: float = UNIT_STAKE,
) -> float:
    result = str(outcome or "").upper()
    stake = float(stake_units)
    odds = float(over_decimal)
    if result == "WIN":
        return stake * (odds - 1.0)
    if result == "LOSS":
        return -stake
    if result == "PUSH":
        return 0.0
    raise ValueError(f"unsupported outcome: {outcome}")


def _leg_odds_source(leg: Mapping[str, Any]) -> str:
    explicit = str(leg.get("odds_source") or "").strip().lower()
    if explicit:
        return explicit
    if str(leg.get("quote_status") or "") == ODDS_SOURCE_IMPUTED_AVG:
        return ODDS_SOURCE_IMPUTED_AVG
    return ODDS_SOURCE_REAL


def leg_has_real_odds(leg: Optional[Mapping[str, Any]]) -> bool:
    """True when the leg carries a real market quote (not an imputed average)."""
    if not isinstance(leg, Mapping):
        return False
    if _leg_odds_source(leg) == ODDS_SOURCE_IMPUTED_AVG:
        return False
    if str(leg.get("quote_status") or "") != "available":
        return False
    odds = _safe_float(leg.get("over_decimal"))
    return odds is not None and odds > 1.0


def average_known_leg_odds(
    ledger: Mapping[str, Any],
    leg_key: str,
) -> Tuple[Optional[float], int]:
    """Mean over_decimal of real quotes for ``leg_key`` across the ledger."""
    if leg_key not in LEG_KEYS:
        raise ValueError(f"unknown leg: {leg_key}")
    values: list[float] = []
    for bet in (ledger.get("bets") or {}).values():
        if not isinstance(bet, Mapping):
            continue
        legs = bet.get("legs")
        if not isinstance(legs, Mapping):
            continue
        leg = legs.get(leg_key)
        if not leg_has_real_odds(leg if isinstance(leg, Mapping) else None):
            continue
        odds = _safe_float((leg or {}).get("over_decimal"))
        if odds is not None and odds > 1.0:
            values.append(float(odds))
    if not values:
        return None, 0
    return sum(values) / len(values), len(values)


def _apply_imputed_odds_to_leg(
    leg: Dict[str, Any],
    *,
    avg_odds: float,
    known_n: int,
    goals_at_signal: Optional[int],
    line_offset: float,
) -> bool:
    """Fill a missing-quote leg from the known-odds average. Never touches real quotes."""
    if leg.get("status") == "settled":
        return False
    if leg_has_real_odds(leg):
        return False
    existing = _safe_float(leg.get("over_decimal"))
    if (
        existing is not None
        and existing > 1.0
        and leg.get("status") == "open"
        and _leg_odds_source(leg) != ODDS_SOURCE_IMPUTED_AVG
    ):
        return False
    if avg_odds <= 1.0 or known_n <= 0:
        return False

    basis = (
        leg.get("impute_basis_reason")
        or leg.get("exclude_reason")
        or leg.get("quote_status")
        or "quote_unavailable"
    )
    leg["over_decimal"] = float(avg_odds)
    leg["stake_units"] = float(UNIT_STAKE)
    leg["status"] = "open"
    leg["outcome"] = None
    leg["pnl_units"] = None
    leg["settled_at_utc"] = None
    leg["exclude_reason"] = None
    leg["odds_source"] = ODDS_SOURCE_IMPUTED_AVG
    leg["quote_status"] = ODDS_SOURCE_IMPUTED_AVG
    leg["imputed_from_known_n"] = int(known_n)
    leg["imputed_over_decimal"] = float(avg_odds)
    leg["impute_basis_reason"] = str(basis)
    if leg.get("line") is None and goals_at_signal is not None:
        leg["line"] = float(goals_at_signal) + float(line_offset)
    if not leg.get("selection"):
        leg["selection"] = (
            SELECTION_ASIAN if abs(line_offset - 1.0) < 1e-9 else SELECTION_EU
        )
    return True


def impute_missing_leg_odds(ledger: Dict[str, Any], *, leg_key: str) -> int:
    """Impute excluded/missing odds for one leg from the real-odds average.

    Returns the number of legs changed. Real ``available`` quotes are never
    overwritten. Already-settled real legs are left alone; settled imputed legs
    keep their frozen imputed odds.
    """
    avg, known_n = average_known_leg_odds(ledger, leg_key)
    if avg is None or known_n <= 0:
        return 0
    line_offset = 0.5 if leg_key == LEG_EU else 1.0
    changed = 0
    for bet in (ledger.get("bets") or {}).values():
        if not isinstance(bet, dict):
            continue
        legs = bet.get("legs")
        if not isinstance(legs, dict):
            continue
        leg = legs.get(leg_key)
        if not isinstance(leg, dict):
            continue
        if leg.get("status") == "settled":
            continue
        goals_at = _safe_int(bet.get("goals_at_signal"))
        if goals_at is None:
            goals_at = int(bet.get("score_home") or 0) + int(bet.get("score_away") or 0)
        if _apply_imputed_odds_to_leg(
            leg,
            avg_odds=float(avg),
            known_n=int(known_n),
            goals_at_signal=goals_at,
            line_offset=line_offset,
        ):
            changed += 1
    return changed


def backfill_imputed_odds_and_settle(ledger: Dict[str, Any]) -> int:
    """Impute missing odds, then settle any open legs that already have finals."""
    ensure_epoch(ledger)
    changed = 0
    for leg_key in LEG_KEYS:
        changed += impute_missing_leg_odds(ledger, leg_key=leg_key)

    for fixture_key, bet in list((ledger.get("bets") or {}).items()):
        if not isinstance(bet, dict):
            continue
        goals_final = _safe_int(bet.get("goals_final"))
        if goals_final is None:
            continue
        legs = bet.get("legs") if isinstance(bet.get("legs"), dict) else {}
        needs_settle = any(
            isinstance(legs.get(leg_key), dict)
            and legs[leg_key].get("status") == "open"
            for leg_key in LEG_KEYS
        )
        if not needs_settle:
            continue
        prior_settled = None
        for leg_key in LEG_KEYS:
            leg = legs.get(leg_key)
            if isinstance(leg, dict) and leg.get("settled_at_utc"):
                prior_settled = leg.get("settled_at_utc")
                break
        settle_published_signal_bet(
            ledger,
            fixture_id=int(bet.get("fixture_id") or fixture_key),
            goals_final=int(goals_final),
            settled_at_utc=prior_settled,
            impute_missing_odds=False,
        )
        changed += 1
    return changed


def _empty_leg(
    *,
    selection: str,
    line: Optional[float],
    over_decimal: Optional[float],
    quote_status: str,
    quote_record_key: Optional[str],
    usable: bool,
    exclude_reason: Optional[str],
    stake_units: float,
) -> Dict[str, Any]:
    return {
        "selection": selection,
        "line": line if usable else None,
        "over_decimal": over_decimal if usable else None,
        "quote_status": "available" if usable else str(quote_status or "unavailable"),
        "quote_record_key": quote_record_key,
        "stake_units": float(stake_units) if usable else None,
        "status": "open" if usable else "excluded",
        "outcome": None,
        "pnl_units": None,
        "exclude_reason": exclude_reason,
        "settled_at_utc": None,
    }


def open_published_signal_bet(
    ledger: Dict[str, Any],
    *,
    fixture_id: int,
    sent_at_utc: Any,
    signal_date_msk: str,
    signal_minute: Optional[int],
    score_home: int,
    score_away: int,
    eu_over_decimal: Optional[float] = None,
    eu_quote_status: str = "unavailable",
    eu_quote_record_key: Optional[str] = None,
    eu_line: Optional[float] = None,
    asian_over_decimal: Optional[float] = None,
    asian_quote_status: str = "unavailable",
    asian_quote_record_key: Optional[str] = None,
    asian_line: Optional[float] = None,
    # backward-compat single-leg kwargs (treated as asian if dual missing)
    over_decimal: Optional[float] = None,
    quote_status: Optional[str] = None,
    quote_record_key: Optional[str] = None,
    line: Optional[float] = None,
    message_id: Optional[int] = None,
    stake_units: float = UNIT_STAKE,
) -> Dict[str, Any]:
    """Register one published signal with dual ROI legs. Idempotent per fixture."""
    ensure_epoch(ledger)
    fixture_key = str(int(fixture_id))
    existing = ledger.setdefault("bets", {}).get(fixture_key)
    if isinstance(existing, dict):
        return existing

    sent_at = _parse_utc(sent_at_utc) or _utc_now()
    epoch = _parse_utc(ledger.get("epoch_utc")) or sent_at
    goals_at_signal = int(score_home) + int(score_away)

    if asian_over_decimal is None and over_decimal is not None:
        asian_over_decimal = over_decimal
        asian_quote_status = str(quote_status or asian_quote_status)
        asian_quote_record_key = quote_record_key
        asian_line = line

    def _usable(odds: Optional[float], status: str) -> Tuple[bool, Optional[str]]:
        ok = (
            str(status) == "available"
            and odds is not None
            and odds > 1.0
            and sent_at >= epoch
        )
        if sent_at < epoch:
            return False, "pre_epoch"
        if not ok:
            return False, str(status or "quote_unavailable")
        return True, None

    eu_odds = _safe_float(eu_over_decimal)
    asian_odds = _safe_float(asian_over_decimal)
    eu_ok, eu_reason = _usable(eu_odds, eu_quote_status)
    asian_ok, asian_reason = _usable(asian_odds, asian_quote_status)

    eu_target = _safe_float(eu_line)
    if eu_target is None:
        eu_target = float(goals_at_signal) + 0.5
    asian_target = _safe_float(asian_line)
    if asian_target is None:
        asian_target = float(goals_at_signal) + 1.0

    eu_leg = _empty_leg(
        selection=SELECTION_EU,
        line=eu_target,
        over_decimal=eu_odds,
        quote_status=eu_quote_status,
        quote_record_key=eu_quote_record_key,
        usable=eu_ok,
        exclude_reason=eu_reason,
        stake_units=stake_units,
    )
    asian_leg = _empty_leg(
        selection=SELECTION_ASIAN,
        line=asian_target,
        over_decimal=asian_odds,
        quote_status=asian_quote_status,
        quote_record_key=asian_quote_record_key,
        usable=asian_ok,
        exclude_reason=asian_reason,
        stake_units=stake_units,
    )
    if eu_ok:
        eu_leg["odds_source"] = ODDS_SOURCE_REAL
    if asian_ok:
        asian_leg["odds_source"] = ODDS_SOURCE_REAL
    bet = {
        "fixture_id": int(fixture_id),
        "sent_at_utc": _fmt_utc(sent_at),
        "signal_date_msk": str(signal_date_msk or ""),
        "signal_minute": _safe_int(signal_minute),
        "score_home": int(score_home),
        "score_away": int(score_away),
        "goals_at_signal": goals_at_signal,
        "message_id": _safe_int(message_id),
        "goals_final": None,
        "goals_after_signal": None,
        "legs": {
            LEG_EU: eu_leg,
            LEG_ASIAN: asian_leg,
        },
    }
    ledger.setdefault("bets", {})[fixture_key] = bet
    return bet


def _settle_leg(
    leg: Dict[str, Any],
    *,
    outcome: str,
    settled_at_utc: str,
) -> None:
    if leg.get("status") == "settled":
        return
    if leg.get("status") != "open":
        return
    odds = _safe_float(leg.get("over_decimal"))
    stake = _safe_float(leg.get("stake_units")) or UNIT_STAKE
    if odds is None or odds <= 1.0:
        leg["status"] = "excluded"
        leg["exclude_reason"] = "quote_unavailable"
        return
    result = str(outcome).upper()
    leg["outcome"] = result
    leg["pnl_units"] = pnl_units(outcome=result, over_decimal=odds, stake_units=stake)
    leg["status"] = "settled"
    leg["settled_at_utc"] = settled_at_utc
    leg["exclude_reason"] = None


def settle_published_signal_bet(
    ledger: Dict[str, Any],
    *,
    fixture_id: int,
    goals_final: Optional[int] = None,
    outcome: Optional[str] = None,
    settled_at_utc: Optional[Any] = None,
    impute_missing_odds: bool = True,
) -> Optional[Dict[str, Any]]:
    fixture_key = str(int(fixture_id))
    bet = ledger.setdefault("bets", {}).get(fixture_key)
    if not isinstance(bet, dict):
        return None

    settled_at = _fmt_utc(_parse_utc(settled_at_utc) or _utc_now())
    legs = bet.get("legs")
    # Legacy single-leg records (schema v1/v2): migrate in-place then settle.
    if not isinstance(legs, dict):
        legs = _migrate_legacy_bet_to_legs(bet)
        bet["legs"] = legs

    goals_at = _safe_int(bet.get("goals_at_signal"))
    if goals_at is None:
        goals_at = int(bet.get("score_home") or 0) + int(bet.get("score_away") or 0)

    if goals_final is not None:
        bet["goals_final"] = int(goals_final)
        bet["goals_after_signal"] = int(goals_final) - int(goals_at)
        eu_out = european_outcome_from_goals(
            goals_at_signal=int(goals_at), goals_final=int(goals_final)
        )
        asian_out = asian_outcome_from_goals(
            goals_at_signal=int(goals_at), goals_final=int(goals_final)
        )
    elif outcome is not None:
        # Legacy: apply same WIN/LOSS to EU only; asian needs goal delta.
        eu_out = str(outcome).upper()
        asian_out = str(outcome).upper()
        if asian_out not in {"WIN", "LOSS", "PUSH"}:
            return bet
    else:
        return bet

    if impute_missing_odds:
        for leg_key, offset in ((LEG_EU, 0.5), (LEG_ASIAN, 1.0)):
            leg = legs.get(leg_key)
            if not isinstance(leg, dict) or leg.get("status") == "settled":
                continue
            avg, known_n = average_known_leg_odds(ledger, leg_key)
            if avg is None:
                continue
            _apply_imputed_odds_to_leg(
                leg,
                avg_odds=float(avg),
                known_n=int(known_n),
                goals_at_signal=goals_at,
                line_offset=offset,
            )

    eu_leg = legs.get(LEG_EU)
    asian_leg = legs.get(LEG_ASIAN)
    if isinstance(eu_leg, dict):
        _settle_leg(eu_leg, outcome=eu_out, settled_at_utc=settled_at)
    if isinstance(asian_leg, dict) and goals_final is not None:
        _settle_leg(asian_leg, outcome=asian_out, settled_at_utc=settled_at)
    elif isinstance(asian_leg, dict) and asian_out in {"WIN", "LOSS", "PUSH"}:
        _settle_leg(asian_leg, outcome=asian_out, settled_at_utc=settled_at)
    return bet


def _migrate_legacy_bet_to_legs(bet: Dict[str, Any]) -> Dict[str, Any]:
    market = bet.get("market") if isinstance(bet.get("market"), dict) else {}
    selection = str(market.get("selection") or "")
    odds = _safe_float(market.get("over_decimal"))
    status = str(bet.get("status") or "excluded")
    leg = {
        "selection": selection or SELECTION_ASIAN,
        "line": _safe_float(market.get("line")),
        "over_decimal": odds,
        "quote_status": str(market.get("quote_status") or "unavailable"),
        "quote_record_key": market.get("quote_record_key"),
        "stake_units": _safe_float(bet.get("stake_units")),
        "status": status,
        "outcome": bet.get("outcome"),
        "pnl_units": _safe_float(bet.get("pnl_units")),
        "exclude_reason": bet.get("exclude_reason"),
        "settled_at_utc": bet.get("settled_at_utc"),
    }
    empty = _empty_leg(
        selection=SELECTION_EU if selection != SELECTION_EU else SELECTION_ASIAN,
        line=None,
        over_decimal=None,
        quote_status="legacy_missing",
        quote_record_key=None,
        usable=False,
        exclude_reason="legacy_single_leg",
        stake_units=UNIT_STAKE,
    )
    if selection == SELECTION_EU:
        return {LEG_EU: leg, LEG_ASIAN: empty}
    return {LEG_EU: empty, LEG_ASIAN: leg}


@dataclass(frozen=True)
class RoiPeriodSummary:
    period: str
    title: str
    leg: str
    signals: int
    settled: int
    plus: int
    minus: int
    push: int
    excluded: int
    open_count: int
    stake_units: float
    pnl_units: float
    roi_pct: Optional[float]
    avg_odds: Optional[float]
    pass_rate_pct: Optional[float]
    epoch_utc: str
    imputed: int = 0
    avg_real_odds: Optional[float] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "period": self.period,
            "title": self.title,
            "leg": self.leg,
            "signals": self.signals,
            "settled": self.settled,
            "plus": self.plus,
            "minus": self.minus,
            "push": self.push,
            "excluded": self.excluded,
            "open": self.open_count,
            "imputed": self.imputed,
            "stake_units": self.stake_units,
            "pnl_units": self.pnl_units,
            "roi_pct": self.roi_pct,
            "avg_odds": self.avg_odds,
            "avg_real_odds": self.avg_real_odds,
            "pass_rate_pct": self.pass_rate_pct,
            "epoch_utc": self.epoch_utc,
        }


def _in_period(bet: Mapping[str, Any], *, start_utc: datetime, end_utc: datetime) -> bool:
    sent = _parse_utc(bet.get("sent_at_utc"))
    if sent is None:
        return False
    return start_utc <= sent < end_utc


def _period_bounds(
    period: str,
    *,
    now_utc: Optional[datetime] = None,
    epoch_utc: Optional[datetime] = None,
) -> Tuple[datetime, datetime, str]:
    now = (now_utc or _utc_now()).astimezone(timezone.utc)
    now_msk = now.astimezone(_MSK)
    if period == "week":
        start_msk = now_msk - timedelta(days=7)
        title = "📅 ROI за неделю (последние 7 дней)"
    elif period == "month":
        start_msk = now_msk.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        month_names = {
            1: "Январь",
            2: "Февраль",
            3: "Март",
            4: "Апрель",
            5: "Май",
            6: "Июнь",
            7: "Июль",
            8: "Август",
            9: "Сентябрь",
            10: "Октябрь",
            11: "Ноябрь",
            12: "Декабрь",
        }
        title = f"🗓 ROI за месяц ({month_names.get(now_msk.month, now_msk.month)} {now_msk.year})"
    elif period == "all":
        start_msk = (epoch_utc or now).astimezone(_MSK)
        title = "📈 ROI за всё время"
    else:
        raise ValueError(f"unknown period: {period}")
    start_utc = start_msk.astimezone(timezone.utc)
    if epoch_utc is not None and start_utc < epoch_utc:
        start_utc = epoch_utc
    return start_utc, now, title


def _leg_title(leg: str) -> str:
    if leg == LEG_EU:
        return "🇪🇺 Over +0.5 (евро)"
    if leg == LEG_ASIAN:
        return "🌏 Over +1.0 (азиат, возврат при ровно +1)"
    return leg


def summarize_period(
    ledger: Mapping[str, Any],
    period: str,
    *,
    leg: str = LEG_ASIAN,
    now_utc: Optional[datetime] = None,
) -> RoiPeriodSummary:
    if leg not in LEG_KEYS:
        raise ValueError(f"unknown leg: {leg}")
    epoch = _parse_utc(ledger.get("epoch_utc")) or (now_utc or _utc_now())
    start_utc, end_utc, title = _period_bounds(period, now_utc=now_utc, epoch_utc=epoch)
    bets: Iterable[Mapping[str, Any]] = [
        bet
        for bet in (ledger.get("bets") or {}).values()
        if isinstance(bet, Mapping) and _in_period(bet, start_utc=start_utc, end_utc=end_utc)
    ]

    signals = 0
    settled = 0
    plus = 0
    minus = 0
    push = 0
    excluded = 0
    open_count = 0
    imputed = 0
    stake_sum = 0.0
    pnl_sum = 0.0
    odds_values = []
    real_odds_values = []

    for bet in bets:
        signals += 1
        legs = bet.get("legs") if isinstance(bet.get("legs"), Mapping) else None
        leg_data: Optional[Mapping[str, Any]] = None
        if legs is None:
            # legacy flat bet: attribute to asian or eu by selection
            market = bet.get("market") if isinstance(bet.get("market"), Mapping) else {}
            selection = str(market.get("selection") or "")
            mapped = LEG_EU if selection == SELECTION_EU else LEG_ASIAN
            if mapped != leg:
                excluded += 1
                continue
            status = str(bet.get("status") or "")
            outcome = str(bet.get("outcome") or "").upper()
            odds = _safe_float(market.get("over_decimal"))
            stake = _safe_float(bet.get("stake_units")) or UNIT_STAKE
            pnl = _safe_float(bet.get("pnl_units")) or 0.0
            odds_source = ODDS_SOURCE_REAL
        else:
            leg_data = legs.get(leg)
            if not isinstance(leg_data, Mapping):
                excluded += 1
                continue
            status = str(leg_data.get("status") or "")
            outcome = str(leg_data.get("outcome") or "").upper()
            odds = _safe_float(leg_data.get("over_decimal"))
            stake = _safe_float(leg_data.get("stake_units")) or UNIT_STAKE
            pnl = _safe_float(leg_data.get("pnl_units")) or 0.0
            odds_source = _leg_odds_source(leg_data)

        if status == "open":
            open_count += 1
            continue
        if status == "excluded":
            excluded += 1
            continue
        if status != "settled":
            excluded += 1
            continue
        settled += 1
        if odds_source == ODDS_SOURCE_IMPUTED_AVG:
            imputed += 1
        if outcome == "WIN":
            plus += 1
        elif outcome == "LOSS":
            minus += 1
        elif outcome == "PUSH":
            push += 1
        stake_sum += stake
        pnl_sum += pnl
        if odds is not None:
            odds_values.append(odds)
            if odds_source != ODDS_SOURCE_IMPUTED_AVG:
                real_odds_values.append(odds)

    roi_pct = (pnl_sum / stake_sum * 100.0) if stake_sum > 0 else None
    avg_odds = (sum(odds_values) / len(odds_values)) if odds_values else None
    avg_real_odds = (
        sum(real_odds_values) / len(real_odds_values) if real_odds_values else None
    )
    decided = plus + minus
    pass_rate = (plus / decided * 100.0) if decided > 0 else None
    return RoiPeriodSummary(
        period=period,
        title=f"{title}\n{_leg_title(leg)}",
        leg=leg,
        signals=signals,
        settled=settled,
        plus=plus,
        minus=minus,
        push=push,
        excluded=excluded,
        open_count=open_count,
        stake_units=stake_sum,
        pnl_units=pnl_sum,
        roi_pct=roi_pct,
        avg_odds=avg_odds,
        pass_rate_pct=pass_rate,
        epoch_utc=_fmt_utc(epoch),
        imputed=imputed,
        avg_real_odds=avg_real_odds,
    )


def format_roi_message(summary: RoiPeriodSummary) -> str:
    lines = [summary.title, ""]
    lines.append(f"🚨 Сигналов: {summary.signals}")
    if summary.open_count:
        lines.append(f"⏳ Ещё в игре: {summary.open_count}")
    if summary.excluded:
        lines.append(f"➖ Без кэфа / вне ROI: {summary.excluded}")
    if summary.imputed:
        lines.append(
            f"🧮 С средним кэфом известных: {summary.imputed}"
        )
    lines.append("")
    if summary.settled <= 0:
        lines.append("Пока нет рассчитанных сигналов с коэффициентом.")
        lines.append("ROI считается только с момента включения учёта.")
        return "\n".join(lines)

    lines.extend(
        [
            f"✅ Плюсов: {summary.plus}",
            f"❌ Минусов: {summary.minus}",
        ]
    )
    if summary.leg == LEG_ASIAN or summary.push:
        lines.append(f"↩ Возвратов: {summary.push}")
    if summary.pass_rate_pct is not None:
        label = "Проходимость (без возвратов)" if summary.leg == LEG_ASIAN else "Проходимость"
        lines.append(f"📈 {label}: {summary.pass_rate_pct:.0f}%")
    lines.append("")
    if summary.avg_real_odds is not None:
        odds_label = "Over +0.5" if summary.leg == LEG_EU else "азиат Over +1"
        lines.append(f"💸 Средний кэф известных ({odds_label}): {summary.avg_real_odds:.3f}")
    elif summary.avg_odds is not None:
        odds_label = "Over +0.5" if summary.leg == LEG_EU else "азиат Over +1"
        lines.append(f"💸 Средний кэф ({odds_label}): {summary.avg_odds:.3f}")
    lines.append(f"📦 Ставок (юниты): {summary.stake_units:.0f}")
    sign = "+" if summary.pnl_units >= 0 else ""
    lines.append(f"💰 PnL: {sign}{summary.pnl_units:.2f} юнита")
    if summary.roi_pct is not None:
        roi_sign = "+" if summary.roi_pct >= 0 else ""
        lines.append(f"📊 ROI: {roi_sign}{summary.roi_pct:.1f}%")
    return "\n".join(lines)


def format_dual_roi_message(
    eu: RoiPeriodSummary,
    asian: RoiPeriodSummary,
) -> str:
    parts = [
        format_roi_message(eu),
        "",
        "────────",
        "",
        format_roi_message(asian),
    ]
    if eu.roi_pct is not None and asian.roi_pct is not None:
        parts.extend(["", "⚖️ Сравнение ROI:"])
        parts.append(f"• Over +0.5: {eu.roi_pct:+.1f}%")
        parts.append(f"• Азиат +1: {asian.roi_pct:+.1f}%")
        if asian.roi_pct > eu.roi_pct + 1e-9:
            parts.append("Пока лучше: азиат +1")
        elif eu.roi_pct > asian.roi_pct + 1e-9:
            parts.append("Пока лучше: Over +0.5")
        else:
            parts.append("Пока примерно равны")
    parts.extend(
        [
            "",
            "Один сигнал = две параллельные ставки по 1 юниту.",
            "Нет живого кэфа → берётся средний кэф уже известных сигналов этой же ветки.",
        ]
    )
    return "\n".join(parts)


def record_open_and_save(
    *,
    fixture_id: int,
    sent_at_utc: Any,
    signal_date_msk: str,
    signal_minute: Optional[int],
    score_home: int,
    score_away: int,
    eu_over_decimal: Optional[float] = None,
    eu_quote_status: str = "unavailable",
    eu_quote_record_key: Optional[str] = None,
    eu_line: Optional[float] = None,
    asian_over_decimal: Optional[float] = None,
    asian_quote_status: str = "unavailable",
    asian_quote_record_key: Optional[str] = None,
    asian_line: Optional[float] = None,
    over_decimal: Optional[float] = None,
    quote_status: Optional[str] = None,
    quote_record_key: Optional[str] = None,
    line: Optional[float] = None,
    message_id: Optional[int] = None,
    path: Optional[Any] = None,
) -> Dict[str, Any]:
    with _LOCK:
        ledger = load_ledger(path)
        ensure_epoch(ledger)
        bet = open_published_signal_bet(
            ledger,
            fixture_id=fixture_id,
            sent_at_utc=sent_at_utc,
            signal_date_msk=signal_date_msk,
            signal_minute=signal_minute,
            score_home=score_home,
            score_away=score_away,
            eu_over_decimal=eu_over_decimal,
            eu_quote_status=eu_quote_status,
            eu_quote_record_key=eu_quote_record_key,
            eu_line=eu_line,
            asian_over_decimal=asian_over_decimal,
            asian_quote_status=asian_quote_status,
            asian_quote_record_key=asian_quote_record_key,
            asian_line=asian_line,
            over_decimal=over_decimal,
            quote_status=quote_status,
            quote_record_key=quote_record_key,
            line=line,
            message_id=message_id,
        )
        save_ledger(ledger, path)
        return bet


def record_settle_and_save(
    *,
    fixture_id: int,
    goals_final: Optional[int] = None,
    outcome: Optional[str] = None,
    settled_at_utc: Optional[Any] = None,
    path: Optional[Any] = None,
) -> Optional[Dict[str, Any]]:
    with _LOCK:
        ledger = load_ledger(path)
        bet = settle_published_signal_bet(
            ledger,
            fixture_id=fixture_id,
            goals_final=goals_final,
            outcome=outcome,
            settled_at_utc=settled_at_utc,
            impute_missing_odds=True,
        )
        if bet is not None:
            save_ledger(ledger, path)
        return bet


def initialize_ledger(path: Optional[Any] = None) -> str:
    """Create/persist ledger; reset when upgrading to dual ROI schema."""
    with _LOCK:
        target = _coerce_path(path)
        raw_schema = 0
        raw_model = ""
        if target.exists():
            try:
                raw = json.loads(target.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    raw_schema = int(raw.get("schema_version") or 1)
                    raw_model = str(raw.get("model") or "")
            except Exception:
                logger.exception("[SIGNAL_ROI] failed_to_inspect path=%s", target)
        if target.exists() and (
            raw_schema < SCHEMA_VERSION or (raw_model and raw_model != MODEL_NAME)
        ):
            logger.info(
                "[SIGNAL_ROI] resetting ledger for dual ROI model "
                "prior_schema=%s prior_model=%s",
                raw_schema,
                raw_model or "unset",
            )
            ledger = empty_ledger()
        else:
            ledger = load_ledger(target)
        epoch = ensure_epoch(ledger)
        save_ledger(ledger, target)
        return epoch


def build_period_message(period: str, *, path: Optional[Any] = None) -> str:
    with _LOCK:
        target = _coerce_path(path)
        ledger = load_ledger(target)
        ensure_epoch(ledger)
        repaired = backfill_imputed_odds_and_settle(ledger)
        if repaired or not target.exists():
            save_ledger(ledger, target)
        eu = summarize_period(ledger, period, leg=LEG_EU)
        asian = summarize_period(ledger, period, leg=LEG_ASIAN)
        return format_dual_roi_message(eu, asian)
