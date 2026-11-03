"""Forward-looking ROI ledger for published Telegram signals.

Model:
  - 1 unit stake on Over (current goals + 0.5) / goal until end of normal time
  - odds frozen from API-Football live quote at successful send time
  - WIN  => +(odds - 1)
  - LOSS => -1
  - signals without a usable quote are tracked but excluded from ROI
  - only signals at/after ``epoch_utc`` participate
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

SCHEMA_VERSION = 1
DEFAULT_STATE_FILE = Path("stats") / "signal_roi_ledger.json"
UNIT_STAKE = 1.0

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
        "bets": bets,
    }


def save_ledger(ledger: Mapping[str, Any], path: Optional[Any] = None) -> Path:
    target = _coerce_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = deepcopy(dict(ledger))
    payload["schema_version"] = SCHEMA_VERSION
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


def pnl_units(*, outcome: str, over_decimal: float, stake_units: float = UNIT_STAKE) -> float:
    result = str(outcome or "").upper()
    stake = float(stake_units)
    odds = float(over_decimal)
    if result == "WIN":
        return stake * (odds - 1.0)
    if result == "LOSS":
        return -stake
    raise ValueError(f"unsupported outcome: {outcome}")


def open_published_signal_bet(
    ledger: Dict[str, Any],
    *,
    fixture_id: int,
    sent_at_utc: Any,
    signal_date_msk: str,
    signal_minute: Optional[int],
    score_home: int,
    score_away: int,
    over_decimal: Optional[float],
    quote_status: str,
    quote_record_key: Optional[str] = None,
    message_id: Optional[int] = None,
    stake_units: float = UNIT_STAKE,
) -> Dict[str, Any]:
    """Register one published signal. Idempotent per fixture_id."""
    ensure_epoch(ledger)
    fixture_key = str(int(fixture_id))
    existing = ledger.setdefault("bets", {}).get(fixture_key)
    if isinstance(existing, dict):
        return existing

    sent_at = _parse_utc(sent_at_utc) or _utc_now()
    epoch = _parse_utc(ledger.get("epoch_utc")) or sent_at
    odds = _safe_float(over_decimal)
    usable_quote = (
        str(quote_status) == "available"
        and odds is not None
        and odds > 1.0
        and sent_at >= epoch
    )
    status = "open" if usable_quote else "excluded"
    exclude_reason = None
    if sent_at < epoch:
        status = "excluded"
        exclude_reason = "pre_epoch"
    elif not usable_quote:
        exclude_reason = str(quote_status or "quote_unavailable")

    bet = {
        "fixture_id": int(fixture_id),
        "sent_at_utc": _fmt_utc(sent_at),
        "signal_date_msk": str(signal_date_msk or ""),
        "signal_minute": _safe_int(signal_minute),
        "score_home": int(score_home),
        "score_away": int(score_away),
        "message_id": _safe_int(message_id),
        "market": {
            "provider": "api_football",
            "selection": "over_current_goals_plus_0_5",
            "over_decimal": odds if usable_quote else None,
            "quote_status": "available" if usable_quote else str(quote_status or "unavailable"),
            "quote_record_key": quote_record_key,
        },
        "stake_units": float(stake_units) if usable_quote else None,
        "status": status,
        "outcome": None,
        "pnl_units": None,
        "settled_at_utc": None,
        "exclude_reason": exclude_reason,
    }
    ledger.setdefault("bets", {})[fixture_key] = bet
    return bet


def settle_published_signal_bet(
    ledger: Dict[str, Any],
    *,
    fixture_id: int,
    outcome: str,
    settled_at_utc: Optional[Any] = None,
) -> Optional[Dict[str, Any]]:
    fixture_key = str(int(fixture_id))
    bet = ledger.setdefault("bets", {}).get(fixture_key)
    if not isinstance(bet, dict):
        return None
    if bet.get("status") == "settled":
        return bet
    if bet.get("status") != "open":
        return bet

    result = str(outcome or "").upper()
    if result not in {"WIN", "LOSS"}:
        return bet
    market = bet.get("market") if isinstance(bet.get("market"), dict) else {}
    odds = _safe_float(market.get("over_decimal"))
    stake = _safe_float(bet.get("stake_units")) or UNIT_STAKE
    if odds is None or odds <= 1.0:
        bet["status"] = "excluded"
        bet["exclude_reason"] = "quote_unavailable"
        return bet

    bet["outcome"] = result
    bet["pnl_units"] = pnl_units(outcome=result, over_decimal=odds, stake_units=stake)
    bet["status"] = "settled"
    bet["settled_at_utc"] = _fmt_utc(_parse_utc(settled_at_utc) or _utc_now())
    bet["exclude_reason"] = None
    return bet


@dataclass(frozen=True)
class RoiPeriodSummary:
    period: str
    title: str
    signals: int
    settled: int
    plus: int
    minus: int
    excluded: int
    open_count: int
    stake_units: float
    pnl_units: float
    roi_pct: Optional[float]
    avg_odds: Optional[float]
    pass_rate_pct: Optional[float]
    epoch_utc: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "period": self.period,
            "title": self.title,
            "signals": self.signals,
            "settled": self.settled,
            "plus": self.plus,
            "minus": self.minus,
            "excluded": self.excluded,
            "open": self.open_count,
            "stake_units": self.stake_units,
            "pnl_units": self.pnl_units,
            "roi_pct": self.roi_pct,
            "avg_odds": self.avg_odds,
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


def summarize_period(
    ledger: Mapping[str, Any],
    period: str,
    *,
    now_utc: Optional[datetime] = None,
) -> RoiPeriodSummary:
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
    excluded = 0
    open_count = 0
    stake_sum = 0.0
    pnl_sum = 0.0
    odds_values = []

    for bet in bets:
        signals += 1
        status = str(bet.get("status") or "")
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
        outcome = str(bet.get("outcome") or "").upper()
        if outcome == "WIN":
            plus += 1
        elif outcome == "LOSS":
            minus += 1
        stake = _safe_float(bet.get("stake_units")) or UNIT_STAKE
        pnl = _safe_float(bet.get("pnl_units")) or 0.0
        stake_sum += stake
        pnl_sum += pnl
        market = bet.get("market") if isinstance(bet.get("market"), Mapping) else {}
        odds = _safe_float(market.get("over_decimal"))
        if odds is not None:
            odds_values.append(odds)

    roi_pct = (pnl_sum / stake_sum * 100.0) if stake_sum > 0 else None
    avg_odds = (sum(odds_values) / len(odds_values)) if odds_values else None
    decided = plus + minus
    pass_rate = (plus / decided * 100.0) if decided > 0 else None
    return RoiPeriodSummary(
        period=period,
        title=title,
        signals=signals,
        settled=settled,
        plus=plus,
        minus=minus,
        excluded=excluded,
        open_count=open_count,
        stake_units=stake_sum,
        pnl_units=pnl_sum,
        roi_pct=roi_pct,
        avg_odds=avg_odds,
        pass_rate_pct=pass_rate,
        epoch_utc=_fmt_utc(epoch),
    )


def format_roi_message(summary: RoiPeriodSummary) -> str:
    lines = [summary.title, ""]
    lines.append(f"🚨 Сигналов: {summary.signals}")
    if summary.open_count:
        lines.append(f"⏳ Ещё в игре: {summary.open_count}")
    if summary.excluded:
        lines.append(f"➖ Без кэфа / вне ROI: {summary.excluded}")
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
    if summary.pass_rate_pct is not None:
        lines.append(f"📈 Проходимость: {summary.pass_rate_pct:.0f}%")
    lines.append("")
    if summary.avg_odds is not None:
        lines.append(f"💸 Средний кэф (Over +0.5): {summary.avg_odds:.3f}")
    lines.append(f"📦 Ставок (юниты): {summary.stake_units:.0f}")
    sign = "+" if summary.pnl_units >= 0 else ""
    lines.append(f"💰 PnL: {sign}{summary.pnl_units:.2f} юнита")
    if summary.roi_pct is not None:
        roi_sign = "+" if summary.roi_pct >= 0 else ""
        lines.append(f"📊 ROI: {roi_sign}{summary.roi_pct:.1f}%")
    lines.append("")
    lines.append("Модель: 1 юнит на гол до конца / Over (счёт+0.5) по кэфу API на момент сигнала.")
    return "\n".join(lines)


def record_open_and_save(
    *,
    fixture_id: int,
    sent_at_utc: Any,
    signal_date_msk: str,
    signal_minute: Optional[int],
    score_home: int,
    score_away: int,
    over_decimal: Optional[float],
    quote_status: str,
    quote_record_key: Optional[str] = None,
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
            over_decimal=over_decimal,
            quote_status=quote_status,
            quote_record_key=quote_record_key,
            message_id=message_id,
        )
        save_ledger(ledger, path)
        return bet


def record_settle_and_save(
    *,
    fixture_id: int,
    outcome: str,
    settled_at_utc: Optional[Any] = None,
    path: Optional[Any] = None,
) -> Optional[Dict[str, Any]]:
    with _LOCK:
        ledger = load_ledger(path)
        bet = settle_published_signal_bet(
            ledger,
            fixture_id=fixture_id,
            outcome=outcome,
            settled_at_utc=settled_at_utc,
        )
        if bet is not None:
            save_ledger(ledger, path)
        return bet


def initialize_ledger(path: Optional[Any] = None) -> str:
    """Create/persist ledger and freeze epoch on first enablement."""
    with _LOCK:
        target = _coerce_path(path)
        ledger = load_ledger(target)
        epoch = ensure_epoch(ledger)
        save_ledger(ledger, target)
        return epoch


def build_period_message(period: str, *, path: Optional[Any] = None) -> str:
    with _LOCK:
        target = _coerce_path(path)
        ledger = load_ledger(target)
        ensure_epoch(ledger)
        # Persist epoch on first report request so "all time" starts from enablement.
        if not target.exists():
            save_ledger(ledger, target)
        summary = summarize_period(ledger, period)
        return format_roi_message(summary)
