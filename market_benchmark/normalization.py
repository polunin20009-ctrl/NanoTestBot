from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional, Sequence, Tuple, Union


SCHEMA_VERSION = 1
SNAPSHOT_RECORD_TYPE = "market_odds_snapshot"
REJECTION_RECORD_TYPE = "market_odds_rejection"
PROVIDER = "api_football"
SOURCE_ENDPOINT = "/odds/live"

TimestampLike = Union[str, datetime]


@dataclass(frozen=True)
class LiveGoalMarketSpec:
    """One verified API-Football *live* total-goals market.

    Live bet identifiers are deliberately kept separate from pre-match bet
    identifiers.  Callers may replace this tuple after checking
    ``/odds/live/bets`` if API-Football changes its catalogue.
    """

    bet_id: int
    name: str
    priority: int


DEFAULT_LIVE_GOAL_MARKETS: Tuple[LiveGoalMarketSpec, ...] = (
    LiveGoalMarketSpec(bet_id=25, name="Match Goals", priority=0),
    LiveGoalMarketSpec(bet_id=36, name="Over/Under Line", priority=1),
)


@dataclass(frozen=True)
class NormalizationResult:
    """Immutable result of normalizing one API response page."""

    records: Tuple[dict[str, Any], ...]
    rejections: Tuple[dict[str, Any], ...]

    @property
    def accepted_count(self) -> int:
        return len(self.records)


@dataclass(frozen=True)
class _Quote:
    side: str
    line: Decimal
    decimal_odds: float
    main: bool


@dataclass(frozen=True)
class _Candidate:
    record: dict[str, Any]
    priority: tuple[int, int, int]


_SELECTION_PATTERN = re.compile(
    r"^\s*(over|under)(?:\s+([+-]?\d+(?:[.,]\d+)?))?\s*$",
    re.IGNORECASE,
)


def _normalized_name(value: object) -> str:
    return " ".join(
        re.sub(r"[^a-z0-9]+", " ", str(value or "").casefold()).split()
    )


def _coerce_int(value: object, *, minimum: int = 0) -> Optional[int]:
    if isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError):
        return None
    if not number.is_finite() or number != number.to_integral_value():
        return None
    result = int(number)
    return result if result >= minimum else None


def _coerce_decimal(value: object) -> Optional[Decimal]:
    if isinstance(value, bool) or value is None:
        return None
    text = str(value).strip().replace(",", ".")
    if not text or "/" in text:
        return None
    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError, TypeError):
        return None
    return number if number.is_finite() else None


def _coerce_decimal_odds(value: object) -> Optional[float]:
    number = _coerce_decimal(value)
    if number is None or number <= Decimal("1"):
        return None
    result = float(number)
    return result if math.isfinite(result) else None


def _utc_timestamp(value: Optional[TimestampLike]) -> str:
    if value is None:
        parsed = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("timestamp must not be empty")
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError("timestamp must be valid ISO-8601") from exc
    else:
        raise TypeError("timestamp must be a datetime, ISO-8601 string, or None")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include a UTC offset")
    return parsed.astimezone(timezone.utc).isoformat(
        timespec="microseconds"
    ).replace("+00:00", "Z")


def _optional_utc_timestamp(value: object) -> Optional[str]:
    if value is None or not str(value).strip():
        return None
    try:
        return _utc_timestamp(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def remove_vig_proportional(
    over_decimal_odds: object,
    under_decimal_odds: object,
) -> dict[str, float]:
    """Remove two-way market vig using proportional normalization.

    The returned probabilities are fractions in ``[0, 1]``.  ``overround``
    is the excess over 100%, not the raw probability sum.
    """

    over = _coerce_decimal_odds(over_decimal_odds)
    under = _coerce_decimal_odds(under_decimal_odds)
    if over is None or under is None:
        raise ValueError("both decimal odds must be finite numbers greater than 1")

    raw_over = 1.0 / over
    raw_under = 1.0 / under
    raw_sum = raw_over + raw_under
    if not math.isfinite(raw_sum) or raw_sum <= 0.0:
        raise ValueError("implied probability sum must be positive and finite")

    fair_over = raw_over / raw_sum
    fair_under = raw_under / raw_sum
    return {
        "raw_implied_over": raw_over,
        "raw_implied_under": raw_under,
        "raw_implied_sum": raw_sum,
        "overround": raw_sum - 1.0,
        "fair_over": fair_over,
        "fair_under": fair_under,
    }


def _spec_for_market(
    market: Mapping[str, Any],
    market_specs: Sequence[LiveGoalMarketSpec],
) -> Optional[LiveGoalMarketSpec]:
    bet_id = _coerce_int(market.get("id"), minimum=1)
    name = _normalized_name(market.get("name"))
    if bet_id is None or not name:
        return None
    for spec in market_specs:
        if bet_id == int(spec.bet_id) and name == _normalized_name(spec.name):
            return spec
    return None


def live_goal_market_preference_key(
    market: Mapping[str, Any],
    *,
    main_quote_count: int = 0,
    source_index: int = 0,
    market_specs: Sequence[LiveGoalMarketSpec] = DEFAULT_LIVE_GOAL_MARKETS,
) -> tuple[int, int, int]:
    """Return a stable, lowest-first preference key for live goal markets."""

    spec = _spec_for_market(market, market_specs)
    if spec is None:
        return (1_000_000, 0, int(source_index))
    return (
        int(spec.priority),
        -max(0, min(2, int(main_quote_count))),
        int(source_index),
    )


def _selection(value: Mapping[str, Any]) -> tuple[Optional[str], Optional[Decimal]]:
    label = str(value.get("value") or "")
    match = _SELECTION_PATTERN.fullmatch(label)
    if match is None:
        return None, None
    line_source = value.get("handicap")
    if line_source is None or not str(line_source).strip():
        line_source = match.group(2)
    return match.group(1).casefold(), _coerce_decimal(line_source)


def _pick_quote(quotes: Sequence[_Quote]) -> tuple[Optional[_Quote], str]:
    if not quotes:
        return None, "quote_missing"
    main = [quote for quote in quotes if quote.main]
    if len(main) == 1:
        return main[0], "ok"
    if len(main) > 1:
        return None, "multiple_main_quotes"
    if len(quotes) == 1:
        return quotes[0], "ok"
    return None, "ambiguous_duplicate_quotes"


def _shadow_metadata() -> dict[str, Any]:
    return {
        "mode": "shadow_collection",
        "shadow_only": True,
        "production_applied": False,
    }


def _rejection(
    *,
    captured_at_utc: str,
    reason: str,
    fixture_id: Optional[int] = None,
    details: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "record_type": REJECTION_RECORD_TYPE,
        "created_at_utc": captured_at_utc,
        "captured_at_utc": captured_at_utc,
        "fixture_id": fixture_id,
        "reason": str(reason),
        **_shadow_metadata(),
    }
    if details:
        record["details"] = dict(details)
    return record


def _record_key(record: Mapping[str, Any]) -> str:
    identity = {
        "schema_version": record["schema_version"],
        "fixture_id": record["fixture_id"],
        "score_home": record["score_home"],
        "score_away": record["score_away"],
        "provider_update_utc": record.get("provider_update_utc")
        or record["captured_at_utc"],
        "bet_id": record["bet_id"],
        "bet_name": record["bet_name"],
        "line": record["line"],
        "over_decimal": record["over_decimal"],
        "under_decimal": record["under_decimal"],
    }
    canonical = json.dumps(
        identity,
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    digest = hashlib.sha256(canonical).hexdigest()[:32]
    return f"market-odds-snapshot:v{SCHEMA_VERSION}:{digest}"


def _entry_status_reason(entry: Mapping[str, Any]) -> Optional[str]:
    status = entry.get("status")
    if not isinstance(status, Mapping):
        return "market_status_missing"
    for flag in ("blocked", "stopped", "finished"):
        value = status.get(flag)
        if not isinstance(value, bool):
            return f"market_status_{flag}_invalid"
        if value:
            return f"market_{flag}"
    return None


def _entry_score(entry: Mapping[str, Any]) -> tuple[Optional[int], Optional[int]]:
    teams = entry.get("teams")
    if not isinstance(teams, Mapping):
        return None, None
    home = teams.get("home")
    away = teams.get("away")
    if not isinstance(home, Mapping) or not isinstance(away, Mapping):
        return None, None
    return (
        _coerce_int(home.get("goals"), minimum=0),
        _coerce_int(away.get("goals"), minimum=0),
    )


def _normalize_entry(
    entry: Mapping[str, Any],
    *,
    captured_at_utc: str,
    expected_fixture_id: Optional[int],
    expected_current_goals: Optional[int],
    market_specs: Sequence[LiveGoalMarketSpec],
) -> tuple[Optional[dict[str, Any]], list[dict[str, Any]]]:
    fixture = entry.get("fixture")
    fixture_map = fixture if isinstance(fixture, Mapping) else {}
    fixture_id = _coerce_int(fixture_map.get("id"), minimum=1)
    if fixture_id is None:
        return None, [
            _rejection(
                captured_at_utc=captured_at_utc,
                reason="fixture_id_missing_or_invalid",
            )
        ]
    if expected_fixture_id is not None and fixture_id != expected_fixture_id:
        return None, []

    status_reason = _entry_status_reason(entry)
    if status_reason is not None:
        return None, [
            _rejection(
                captured_at_utc=captured_at_utc,
                fixture_id=fixture_id,
                reason=status_reason,
            )
        ]

    score_home, score_away = _entry_score(entry)
    if score_home is None or score_away is None:
        return None, [
            _rejection(
                captured_at_utc=captured_at_utc,
                fixture_id=fixture_id,
                reason="score_missing_or_invalid",
            )
        ]
    current_goals = score_home + score_away
    if (
        expected_current_goals is not None
        and current_goals != expected_current_goals
    ):
        return None, [
            _rejection(
                captured_at_utc=captured_at_utc,
                fixture_id=fixture_id,
                reason="current_goals_mismatch",
                details={
                    "expected_current_goals": expected_current_goals,
                    "provider_current_goals": current_goals,
                },
            )
        ]

    target_line = Decimal(current_goals) + Decimal("0.5")
    odds = entry.get("odds")
    if not isinstance(odds, Sequence) or isinstance(odds, (str, bytes)):
        odds = ()

    candidates: list[_Candidate] = []
    market_rejections: list[dict[str, Any]] = []
    recognized_market_count = 0
    for source_index, raw_market in enumerate(odds):
        if not isinstance(raw_market, Mapping):
            continue
        spec = _spec_for_market(raw_market, market_specs)
        if spec is None:
            continue
        recognized_market_count += 1
        quotes: dict[str, list[_Quote]] = {"over": [], "under": []}
        suspended_sides: set[str] = set()
        invalid_sides: set[str] = set()
        values = raw_market.get("values")
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
            values = ()
        for raw_value in values:
            if not isinstance(raw_value, Mapping):
                continue
            side, line = _selection(raw_value)
            if side not in quotes or line != target_line:
                continue
            if raw_value.get("suspended") is not False:
                suspended_sides.add(side)
                continue
            decimal_odds = _coerce_decimal_odds(raw_value.get("odd"))
            if decimal_odds is None:
                invalid_sides.add(side)
                continue
            quotes[side].append(
                _Quote(
                    side=side,
                    line=line,
                    decimal_odds=decimal_odds,
                    main=raw_value.get("main") is True,
                )
            )

        over, over_reason = _pick_quote(quotes["over"])
        under, under_reason = _pick_quote(quotes["under"])
        if over is None or under is None:
            if over is None and "over" in suspended_sides:
                reason = "target_over_suspended"
            elif under is None and "under" in suspended_sides:
                reason = "target_under_suspended"
            elif over is None and "over" in invalid_sides:
                reason = "target_over_odds_invalid"
            elif under is None and "under" in invalid_sides:
                reason = "target_under_odds_invalid"
            elif over is None:
                reason = f"target_over_{over_reason}"
            else:
                reason = f"target_under_{under_reason}"
            market_rejections.append(
                _rejection(
                    captured_at_utc=captured_at_utc,
                    fixture_id=fixture_id,
                    reason=reason,
                    details={
                        "bet_id": spec.bet_id,
                        "bet_name": spec.name,
                        "target_line": float(target_line),
                    },
                )
            )
            continue

        probabilities = remove_vig_proportional(
            over.decimal_odds,
            under.decimal_odds,
        )
        fixture_status = fixture_map.get("status")
        fixture_status_map = (
            fixture_status if isinstance(fixture_status, Mapping) else {}
        )
        league = entry.get("league")
        league_map = league if isinstance(league, Mapping) else {}
        main_quote_count = int(over.main) + int(under.main)
        provider_update_utc = _optional_utc_timestamp(entry.get("update"))
        if provider_update_utc is None:
            return None, [
                _rejection(
                    captured_at_utc=captured_at_utc,
                    fixture_id=fixture_id,
                    reason="provider_update_missing_or_invalid",
                )
            ]
        record: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "record_type": SNAPSHOT_RECORD_TYPE,
            "created_at_utc": captured_at_utc,
            "captured_at_utc": captured_at_utc,
            "fixture_id": fixture_id,
            "minute": _coerce_int(fixture_status_map.get("elapsed"), minimum=0),
            "score_home": score_home,
            "score_away": score_away,
            "current_goals": current_goals,
            "provider": PROVIDER,
            "source_endpoint": SOURCE_ENDPOINT,
            "provider_update_utc": provider_update_utc,
            "league_id": _coerce_int(league_map.get("id"), minimum=1),
            "league_season": _coerce_int(league_map.get("season"), minimum=1),
            "market_type": "one_more_goal_to90",
            "source_market_type": "match_total_goals",
            "target_event": "at_least_one_more_goal_to90_normal_time",
            "settlement_scope": "normal_time",
            "bet_id": spec.bet_id,
            "bet_name": spec.name,
            "line": float(target_line),
            "over_decimal": over.decimal_odds,
            "under_decimal": under.decimal_odds,
            "raw_implied_over": probabilities["raw_implied_over"],
            "raw_implied_under": probabilities["raw_implied_under"],
            "raw_implied_sum": probabilities["raw_implied_sum"],
            "overround": probabilities["overround"],
            "no_vig_over_probability": probabilities["fair_over"],
            "no_vig_under_probability": probabilities["fair_under"],
            "no_vig_over_probability_pct": probabilities["fair_over"] * 100.0,
            "selection_main_quote_count": main_quote_count,
            "source_status": {
                "blocked": False,
                "stopped": False,
                "finished": False,
            },
            **_shadow_metadata(),
        }
        record["market"] = {
            "market_type": record["market_type"],
            "source_market_type": record["source_market_type"],
            "target_event": record["target_event"],
            "settlement_scope": record["settlement_scope"],
            "scope": "normal_time",
            "api_updated_at_utc": provider_update_utc,
            "current_goals": current_goals,
            "target_line": record["line"],
            "bet_id": record["bet_id"],
            "bet_name": record["bet_name"],
            "decimal_odds": {
                "over": record["over_decimal"],
                "under": record["under_decimal"],
            },
            "raw_implied_probability": {
                "over": record["raw_implied_over"],
                "under": record["raw_implied_under"],
                "sum": record["raw_implied_sum"],
            },
            "overround": record["overround"],
            "fair_probability": {
                "over": record["no_vig_over_probability"],
                "under": record["no_vig_under_probability"],
            },
            "fair_probability_goal_to90": record["no_vig_over_probability"],
            "fair_probability_goal_to90_pct": record[
                "no_vig_over_probability_pct"
            ],
            **_shadow_metadata(),
        }
        record["record_key"] = _record_key(record)
        record["snapshot_id"] = record["record_key"]
        candidates.append(
            _Candidate(
                record=record,
                priority=live_goal_market_preference_key(
                    raw_market,
                    main_quote_count=main_quote_count,
                    source_index=source_index,
                    market_specs=market_specs,
                ),
            )
        )

    if candidates:
        selected = min(candidates, key=lambda item: item.priority).record
        selected["eligible_market_candidate_count"] = len(candidates)
        return selected, []

    if market_rejections:
        return None, market_rejections
    reason = (
        "target_line_pair_missing"
        if recognized_market_count
        else "supported_live_goal_market_missing"
    )
    return None, [
        _rejection(
            captured_at_utc=captured_at_utc,
            fixture_id=fixture_id,
            reason=reason,
            details={"target_line": float(target_line)},
        )
    ]


def normalize_live_goal_markets(
    payload: object,
    *,
    captured_at_utc: Optional[TimestampLike] = None,
    fixture_id: Optional[object] = None,
    current_goals: Optional[object] = None,
    market_specs: Sequence[LiveGoalMarketSpec] = DEFAULT_LIVE_GOAL_MARKETS,
) -> NormalizationResult:
    """Normalize API-Football ``/odds/live`` into shadow quote snapshots.

    With no ``fixture_id`` the complete response page is processed.  When a
    fixture is requested, ``current_goals`` may be supplied as a strict
    race-condition guard: provider score and caller score must agree.
    Exactly one preferred paired market is returned per fixture.
    """

    captured = _utc_timestamp(captured_at_utc)
    expected_fixture_id: Optional[int] = None
    if fixture_id is not None:
        expected_fixture_id = _coerce_int(fixture_id, minimum=1)
        if expected_fixture_id is None:
            raise ValueError("fixture_id must be a positive integer")
    expected_current_goals: Optional[int] = None
    if current_goals is not None:
        if expected_fixture_id is None:
            raise ValueError("current_goals requires fixture_id")
        expected_current_goals = _coerce_int(current_goals, minimum=0)
        if expected_current_goals is None:
            raise ValueError("current_goals must be a non-negative integer")

    if not market_specs:
        raise ValueError("at least one verified live goal market is required")
    normalized_specs: list[LiveGoalMarketSpec] = []
    identities: set[tuple[int, str]] = set()
    for spec in market_specs:
        if not isinstance(spec, LiveGoalMarketSpec):
            raise TypeError("market_specs must contain LiveGoalMarketSpec values")
        identity = (int(spec.bet_id), _normalized_name(spec.name))
        if identity[0] <= 0 or not identity[1] or identity in identities:
            raise ValueError("live goal market specs must be unique and valid")
        identities.add(identity)
        normalized_specs.append(spec)

    errors: object = None
    if isinstance(payload, Mapping):
        errors = payload.get("errors")
        response = payload.get("response")
    elif isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
        response = payload
    else:
        response = None
    if errors:
        return NormalizationResult(
            records=(),
            rejections=(
                _rejection(
                    captured_at_utc=captured,
                    fixture_id=expected_fixture_id,
                    reason="api_error",
                    details={"errors": errors},
                ),
            ),
        )
    if not isinstance(response, Sequence) or isinstance(response, (str, bytes)):
        return NormalizationResult(
            records=(),
            rejections=(
                _rejection(
                    captured_at_utc=captured,
                    fixture_id=expected_fixture_id,
                    reason="api_response_missing_or_invalid",
                ),
            ),
        )

    records: list[dict[str, Any]] = []
    rejections: list[dict[str, Any]] = []
    matched_fixture = False
    for entry in response:
        if not isinstance(entry, Mapping):
            rejections.append(
                _rejection(
                    captured_at_utc=captured,
                    fixture_id=expected_fixture_id,
                    reason="api_response_entry_invalid",
                )
            )
            continue
        raw_fixture = entry.get("fixture")
        entry_fixture_id = (
            _coerce_int(raw_fixture.get("id"), minimum=1)
            if isinstance(raw_fixture, Mapping)
            else None
        )
        if expected_fixture_id is None or entry_fixture_id == expected_fixture_id:
            matched_fixture = True
        record, entry_rejections = _normalize_entry(
            entry,
            captured_at_utc=captured,
            expected_fixture_id=expected_fixture_id,
            expected_current_goals=expected_current_goals,
            market_specs=tuple(normalized_specs),
        )
        if record is not None:
            records.append(record)
        rejections.extend(entry_rejections)

    if expected_fixture_id is not None and not matched_fixture:
        rejections.append(
            _rejection(
                captured_at_utc=captured,
                fixture_id=expected_fixture_id,
                reason="requested_fixture_missing",
            )
        )

    unique_records: dict[str, dict[str, Any]] = {}
    for record in records:
        unique_records.setdefault(str(record["record_key"]), record)
    return NormalizationResult(
        records=tuple(unique_records.values()),
        rejections=tuple(rejections),
    )
