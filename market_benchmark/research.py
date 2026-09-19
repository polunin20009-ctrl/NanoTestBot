"""Causal market inputs shared by live collection and offline rule discovery.

Only normalized snapshots are accepted. No later movement, delivery or outcome
record is ever an input. Persist selected quote evidence, not just its numbers.
"""
from __future__ import annotations

import math
import threading
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

CONTRACT_VERSION = "causal_market_features_v1"
MAX_AGE_SECONDS = 120
HISTORY_SECONDS = 720


def _time(value: Any) -> datetime | None:
    try:
        t = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return t.astimezone(timezone.utc) if t.utcoffset() is not None else None
    except (ValueError, TypeError, OverflowError):
        return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        n = float(value)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError, OverflowError):
        return None


def valid_quote(quote: Mapping[str, Any], observation: Mapping[str, Any],
                at: datetime) -> bool:
    match = observation.get("match") or {}
    if not isinstance(match, Mapping):
        return False
    home, away = _number(match.get("score_home")), _number(match.get("score_away"))
    captured = _time(quote.get("captured_at_utc"))
    provider_time = _time(quote.get("provider_update_utc"))
    market = quote.get("market") or {}
    status = quote.get("source_status") or {}
    if not isinstance(market, Mapping) or not isinstance(status, Mapping):
        return False
    over = _number(quote.get("over_decimal"))
    under = _number(quote.get("under_decimal"))
    if (
        quote.get("record_type") != "market_odds_snapshot"
        or not quote.get("record_key")
        or quote.get("provider") != "api_football"
        or quote.get("shadow_only") is not True
        or quote.get("production_applied") is not False
        or quote.get("settlement_scope") != "normal_time"
        or quote.get("bet_id") not in (25, 36)
        or any(status.get(k) is not False for k in ("blocked", "stopped", "finished"))
        or quote.get("fixture_id") != observation.get("fixture_id")
        or home is None or away is None or home < 0 or away < 0
        or not home.is_integer() or not away.is_integer()
        or _number(quote.get("score_home")) != home
        or _number(quote.get("score_away")) != away
        or _number(quote.get("current_goals")) != home + away
        or _number(quote.get("line")) != home + away + 0.5
        or captured is None or provider_time is None
        or not 0 <= (at - captured).total_seconds() <= MAX_AGE_SECONDS
        or not 0 <= (at - provider_time).total_seconds() <= MAX_AGE_SECONDS
        or (provider_time - captured).total_seconds() > 5
        or over is None or under is None or over <= 1 or under <= 1
    ):
        return False
    p = (1 / over) / (1 / over + 1 / under)
    recorded = _number(market.get("fair_probability_goal_to90"))
    return recorded is not None and math.isclose(p, recorded, abs_tol=1e-7)


def freeze_market_context(observation: Mapping[str, Any],
                          quotes: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    at = _time(observation.get("created_at_utc"))
    context: dict[str, Any] = {
        "version": CONTRACT_VERSION,
        "observation_id": observation.get("observation_id") or observation.get("observation_key"),
        "fixture_id": observation.get("fixture_id"),
        "observation_created_at_utc": observation.get("created_at_utc"),
        "status": "unavailable", "quotes": {},
    }
    if at is None:
        return context
    # Keep the earliest known capture for identical provider quotes.
    unique: dict[str, Mapping[str, Any]] = {}
    for quote in quotes:
        if not isinstance(quote, Mapping):
            continue
        captured = _time(quote.get("captured_at_utc"))
        key = str(quote.get("record_key") or "")
        if not key or captured is None or captured > at:
            continue
        previous = unique.get(key)
        if previous is None or captured < _time(previous.get("captured_at_utc")):
            unique[key] = quote
    usable = [q for q in unique.values() if valid_quote(q, observation, at)]
    if not usable:
        return context
    latest = max(usable, key=lambda q: (_time(q["captured_at_utc"]), q["record_key"]))
    context["quotes"]["now"] = deepcopy(dict(latest))
    context["status"] = "available"
    for minutes in (5, 10):
        boundary = at - timedelta(minutes=minutes)
        prior = [q for q in unique.values()
                 if valid_quote(q, observation, boundary)
                 and q.get("bet_id") == latest.get("bet_id")
                 and _time(q.get("provider_update_utc")) < _time(latest.get("provider_update_utc"))]
        if prior:
            context["quotes"][f"{minutes}m"] = deepcopy(dict(max(
                prior, key=lambda q: (_time(q["captured_at_utc"]), q["record_key"])))
            )
    return context


def market_values(observation: Mapping[str, Any]) -> dict[str, float]:
    context = observation.get("market_research")
    if not isinstance(context, Mapping) or context.get("version") != CONTRACT_VERSION:
        return {}
    if context.get("status") != "available":
        return {}
    if (context.get("observation_id") != (observation.get("observation_id") or observation.get("observation_key"))
        or context.get("fixture_id") != observation.get("fixture_id")
        or context.get("observation_created_at_utc") != observation.get("created_at_utc")):
        return {}
    quotes = context.get("quotes")
    if not isinstance(quotes, Mapping):
        return {}
    # Revalidate evidence on extraction; never trust supplied feature numbers.
    checked = freeze_market_context(observation, quotes.values())
    now = checked["quotes"].get("now")
    if now is None:
        return {}
    def probability(q: Mapping[str, Any]) -> float:
        a, b = float(q["over_decimal"]), float(q["under_decimal"])
        return 100 * (1 / a) / (1 / a + 1 / b)
    values = {
        "market.v1.prob_to90": probability(now),
        "market.v1.over_decimal": float(now["over_decimal"]),
        "market.v1.overround": 1 / float(now["over_decimal"]) + 1 / float(now["under_decimal"]) - 1,
    }
    for minutes in (5, 10):
        old = checked["quotes"].get(f"{minutes}m")
        if old is not None:
            values[f"market.v1.delta_{minutes}m_pp"] = probability(now) - probability(old)
            values[f"market.v1.odds_ratio_{minutes}m"] = float(now["over_decimal"]) / float(old["over_decimal"])
    return values


class CausalQuoteCache:
    """Small independent 12-minute cache; old benchmark cache is unchanged."""
    def __init__(self, max_fixtures: int = 512, per_fixture: int = 64):
        if type(max_fixtures) is not int or not 1 <= max_fixtures <= 512:
            raise ValueError("max_fixtures must be an integer in [1,512]")
        if type(per_fixture) is not int or not 1 <= per_fixture <= 64:
            raise ValueError("per_fixture must be an integer in [1,64]")
        self.max_fixtures = max_fixtures
        self.per_fixture = per_fixture
        self._quotes: dict[int, list[dict]] = {}
        self._lock = threading.Lock()

    def update(self, records: Iterable[Mapping[str, Any]], captured_at_utc: str) -> None:
        at = _time(captured_at_utc)
        if at is None:
            return
        cutoff = at - timedelta(seconds=HISTORY_SECONDS)
        with self._lock:
            for quote in records:
                if not isinstance(quote, Mapping):
                    continue
                fid = quote.get("fixture_id")
                captured = _time(quote.get("captured_at_utc"))
                if type(fid) is not int or fid < 1 or captured is None or not cutoff <= captured <= at:
                    continue
                old = self._quotes.setdefault(fid, [])
                same = next((q for q in old if q.get("record_key") == quote.get("record_key")), None)
                if same is not None and captured < _time(same["captured_at_utc"]):
                    old.remove(same)
                    same = None
                if same is None:
                    old.append(deepcopy(dict(quote)))
            for fid, rows in list(self._quotes.items()):
                rows = [q for q in rows if _time(q["captured_at_utc"]) >= cutoff]
                rows.sort(key=lambda q: (_time(q["captured_at_utc"]), q["record_key"]))
                if rows:
                    self._quotes[fid] = rows[-self.per_fixture:]
                else:
                    del self._quotes[fid]
            while len(self._quotes) > self.max_fixtures:
                oldest = min(self._quotes, key=lambda fid: _time(self._quotes[fid][-1]["captured_at_utc"]))
                del self._quotes[oldest]

    def freeze(self, observation: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            quotes = deepcopy(self._quotes.get(observation.get("fixture_id"), []))
        return freeze_market_context(observation, quotes)
