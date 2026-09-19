#!/usr/bin/env python3
"""Report prospective bot/ML quality against frozen live-market quotes.

The report intentionally consumes only the append-only market benchmark
journal.  Probabilities must have been frozen in a ``market_benchmark_decision``
record at observation time; this command never recomputes a historical model
prediction.  Outcomes are joined afterwards by ``observation_id``.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from market_benchmark import market_journal_paths
from outcome_revision import outcome_revision as get_outcome_revision


SNAPSHOT_TYPES = {"market_benchmark_snapshot", "market_odds_snapshot"}
DECISION_TYPES = {"market_benchmark_decision", "market_odds_decision"}
DELIVERY_TYPES = {"market_benchmark_delivery"}
OUTCOME_TYPES = {
    "market_benchmark_outcome",
    "market_odds_outcome",
    "observation_outcome",
}
SOURCE_NAMES = ("bot", "static_ml", "rolling_ml", "market")
MODEL_SOURCE_NAMES = ("static_ml", "rolling_ml")
EPSILON = 1e-6


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _parse_utc(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _finite_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _fraction(value: Any) -> Optional[float]:
    """Read a field whose schema explicitly defines a probability fraction."""
    number = _finite_float(value)
    if number is None or not 0.0 <= number <= 1.0:
        return None
    return min(1.0 - EPSILON, max(EPSILON, number))


def _percentage(value: Any) -> Optional[float]:
    """Read a field whose schema explicitly defines percentage points."""
    number = _finite_float(value)
    if number is None or not 0.0 <= number <= 100.0:
        return None
    return min(1.0 - EPSILON, max(EPSILON, number / 100.0))


def _schema_number(value: Any) -> int:
    if isinstance(value, bool):
        return -1
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def _open_jsonl(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rb")
    return path.open("rb")


def _iter_jsonl(path: os.PathLike[str] | str) -> Iterable[dict[str, Any]]:
    source = Path(path)
    if not source.exists():
        return
    with _open_jsonl(source) as handle:
        for line_number, line in enumerate(handle, 1):
            try:
                text = line.decode("utf-8").strip()
                if not text:
                    continue
                record = json.loads(text)
            except (json.JSONDecodeError, UnicodeDecodeError):
                yield {
                    "record_type": "__invalid_json__",
                    "_line_number": line_number,
                }
                continue
            if not isinstance(record, dict):
                yield {
                    "record_type": "__invalid_json__",
                    "_line_number": line_number,
                }
                continue
            yield record


def _iter_journal(path: os.PathLike[str] | str) -> Iterable[dict[str, Any]]:
    """Read the full append-only history, including rotated gzip files."""

    candidates = market_journal_paths(path)
    if not candidates:
        candidates = [os.fspath(path)]
    for candidate in candidates:
        yield from _iter_jsonl(candidate)


def _record_time(record: Mapping[str, Any]) -> Optional[datetime]:
    for key in (
        "captured_at_utc",
        "created_at_utc",
        "decision_created_at_utc",
    ):
        parsed = _parse_utc(record.get(key))
        if parsed is not None:
            return parsed
    return None


def _decision_time(record: Mapping[str, Any]) -> Optional[datetime]:
    # A decision may denormalize the quote's ``captured_at_utc``.  Its own
    # immutable creation timestamp is the boundary for frozen predictions.
    if "decision_created_at_utc" in record:
        return _parse_utc(record.get("decision_created_at_utc"))
    for key in ("created_at_utc", "captured_at_utc"):
        parsed = _parse_utc(record.get(key))
        if parsed is not None:
            return parsed
    return None


def _observation_time(record: Mapping[str, Any]) -> Optional[datetime]:
    for key in ("observation_created_at_utc", "observed_at_utc"):
        parsed = _parse_utc(record.get(key))
        if parsed is not None:
            return parsed
    observation = _mapping(record.get("observation"))
    return _parse_utc(observation.get("created_at_utc"))


def _snapshot_key(record: Mapping[str, Any]) -> str:
    for key in ("record_key", "snapshot_key", "quote_record_key"):
        value = str(record.get(key) or "").strip()
        if value:
            return value
    return ""


def _decision_key(record: Mapping[str, Any]) -> str:
    for key in ("observation_id", "decision_key", "observation_key"):
        value = str(record.get(key) or "").strip()
        if value:
            return value
    return ""


def _quote_reference(record: Mapping[str, Any]) -> str:
    market = _mapping(record.get("market"))
    for container in (market, record):
        for key in (
            "quote_record_key",
            "snapshot_key",
            "market_snapshot_key",
        ):
            value = str(container.get(key) or "").strip()
            if value:
                return value
    return ""


def _outcome_rank(
    record: Mapping[str, Any],
) -> tuple[int, int, int, datetime]:
    outcome = _mapping(record.get("outcome"))
    timestamp = _parse_utc(record.get("created_at_utc"))
    if timestamp is None:
        timestamp = _parse_utc(outcome.get("resolved_at_utc"))
    return (
        max(
            _schema_number(record.get("outcome_schema_version")),
            _schema_number(outcome.get("outcome_schema_version")),
        ),
        get_outcome_revision(record),
        int(timestamp is not None),
        timestamp or datetime.min.replace(tzinfo=timezone.utc),
    )


def _probability_from_mapping(value: Any) -> Optional[float]:
    """Read a frozen bot/model source with explicit unit-aware field names."""
    if not isinstance(value, Mapping):
        return _percentage(value)
    source = value
    status = source.get("status", source.get("prediction_status", "ok"))
    if str(status or "ok").lower() not in {"ok", "available"}:
        return None
    target = _mapping(source.get("to90")) or _mapping(
        _mapping(source.get("predictions")).get("to90")
    )
    candidates = (source, target)
    for container in candidates:
        for key in (
            "probability_pct",
            "to90_probability_pct",
            "p90_pct",
            "prob_to90_pct",
            "calibrated_probability_pct",
            "final_probability_pct",
            "prob_to90",
        ):
            if key in container:
                return _percentage(container.get(key))
    for container in candidates:
        for key in ("probability", "to90_probability", "p90"):
            if key in container:
                return _fraction(container.get(key))
    return None


def _source_mapping(
    decision: Mapping[str, Any], source_name: str
) -> Mapping[str, Any]:
    probabilities = _mapping(decision.get("probabilities"))
    aliases = {
        "bot": ("bot", "base", "production"),
        "static_ml": ("static_ml", "static", "shadow_ml"),
        "rolling_ml": ("rolling_ml", "rolling", "shadow_ml_rolling"),
    }[source_name]
    for alias in aliases:
        value = probabilities.get(alias)
        if isinstance(value, Mapping):
            return value
    return {}


def _source_probability(
    decision: Mapping[str, Any], source_name: str
) -> Optional[float]:
    probabilities = _mapping(decision.get("probabilities"))
    aliases = {
        "bot": ("bot", "base", "production"),
        "static_ml": ("static_ml", "static", "shadow_ml"),
        "rolling_ml": ("rolling_ml", "rolling", "shadow_ml_rolling"),
    }[source_name]
    for alias in aliases:
        if alias in probabilities:
            parsed = _probability_from_mapping(probabilities.get(alias))
            if parsed is not None:
                return parsed

    direct_aliases = {
        "bot": (
            "bot_probability_pct",
            "bot_p90_pct",
            "bot_to90_pct",
            "prob_to90",
        ),
        "static_ml": (
            "static_ml_probability_pct",
            "static_ml_p90_pct",
            "static_ml_to90_pct",
        ),
        "rolling_ml": (
            "rolling_ml_probability_pct",
            "rolling_ml_p90_pct",
            "rolling_ml_to90_pct",
        ),
    }[source_name]
    for key in direct_aliases:
        if key in probabilities:
            return _percentage(probabilities.get(key))
        if key in decision:
            return _percentage(decision.get(key))
    return None


def _timing_value(
    decision: Mapping[str, Any], source_name: str, key: str
) -> Optional[datetime]:
    source = _source_mapping(decision, source_name)
    metadata = _mapping(decision.get("prediction_metadata"))
    source_metadata = _mapping(metadata.get(source_name))
    models = _mapping(decision.get("models"))
    model_metadata = _mapping(models.get(source_name))
    for container in (
        source,
        _mapping(source.get("metadata")),
        _mapping(source.get("model")),
        source_metadata,
        model_metadata,
    ):
        parsed = _parse_utc(container.get(key))
        if parsed is not None:
            return parsed
    return None


def _model_timing_reason(
    decision: Mapping[str, Any],
    source_name: str,
    *,
    observation_id: str,
    fixture_id: int,
    minute: float,
    observation_time: datetime,
    decision_time: datetime,
    future_tolerance_seconds: float,
) -> Optional[str]:
    source = _source_mapping(decision, source_name)
    input_observation_id = str(
        source.get("prediction_input_observation_id") or ""
    ).strip()
    input_fixture_id = _fixture_id(
        {"fixture_id": source.get("prediction_input_fixture_id")}
    )
    input_minute = _finite_float(source.get("prediction_input_minute"))
    input_fingerprint = str(
        source.get("prediction_input_fingerprint") or ""
    ).strip()
    expected_fingerprint = str(
        _mapping(decision.get("prediction_input_fingerprints")).get(
            source_name
        )
        or ""
    ).strip()
    if not input_observation_id:
        return "prediction_input_observation_id_missing"
    if input_fixture_id != fixture_id:
        return "prediction_input_fixture_mismatch"
    if input_minute is None or not math.isclose(input_minute, minute, abs_tol=1e-9):
        return "prediction_input_minute_mismatch"
    if not input_fingerprint or not expected_fingerprint:
        return "prediction_input_fingerprint_missing"
    if input_fingerprint != expected_fingerprint:
        return "prediction_input_fingerprint_mismatch"
    if input_observation_id != observation_id and str(
        source.get("input_identity_method") or ""
    ) != "predictive_input_fingerprint_v1":
        return "prediction_input_observation_id_mismatch"
    cutoff = _timing_value(decision, source_name, "model_data_cutoff_utc")
    model_created = _timing_value(decision, source_name, "model_created_at_utc")
    predicted = _timing_value(decision, source_name, "prediction_created_at_utc")
    if predicted is None:
        predicted = _timing_value(decision, source_name, "created_at_utc")
    if cutoff is None or model_created is None or predicted is None:
        return "missing_prospective_timestamps"
    if cutoff >= observation_time:
        return "model_cutoff_not_before_observation"
    if cutoff > model_created:
        return "model_cutoff_after_model_creation"
    if model_created > observation_time:
        return "model_created_after_observation"
    if predicted < observation_time:
        return "prediction_before_observation"
    if predicted > decision_time:
        return "prediction_after_frozen_decision"
    return None


def _market_probability(market: Mapping[str, Any]) -> Optional[float]:
    # Canonical core fields are fractions.  Percentage aliases stay explicit.
    for key in ("fair_probability_goal_to90", "fair_probability"):
        if key in market:
            value = market.get(key)
            if isinstance(value, Mapping):
                value = value.get("over")
            parsed = _fraction(value)
            if parsed is not None:
                return parsed
    fair = _mapping(market.get("fair_probability"))
    parsed = _fraction(fair.get("over"))
    if parsed is not None:
        return parsed
    for key in ("fair_probability_goal_to90_pct", "fair_probability_pct"):
        if key in market:
            parsed = _percentage(market.get(key))
            if parsed is not None:
                return parsed
    return None


def _market_status(decision: Mapping[str, Any]) -> str:
    market = _mapping(decision.get("market"))
    status = str(market.get("status") or "").strip().lower()
    if status:
        return status
    return "available" if _market_probability(market) is not None else "unavailable"


def _fixture_id(record: Mapping[str, Any]) -> Optional[int]:
    value = record.get("fixture_id")
    if value is None:
        value = _mapping(record.get("match")).get("fixture_id")
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _outcome_label(
    record: Mapping[str, Any],
    *,
    observation_time: Optional[datetime],
    quote_time: Optional[datetime],
    decision_time: Optional[datetime],
) -> tuple[Optional[int], str]:
    outcome = _mapping(record.get("outcome"))
    status = str(outcome.get("status") or record.get("status") or "").lower()
    if status == "void":
        return None, "void"
    if status != "resolved":
        return None, "pending" if status == "pending" else "invalid_status"
    if (
        outcome.get("outcome_integrity_conflict") is True
        or record.get("outcome_integrity_conflict") is True
    ):
        return None, "integrity_conflict"
    raw_scope = outcome.get("outcome_scope") or record.get("outcome_scope")
    if not str(raw_scope or "").strip():
        return None, "missing_outcome_scope"
    scope = str(raw_scope).upper()
    if scope not in {"TO_90_NORMAL_TIME", "NORMAL_TIME", "TO90_NORMAL_TIME"}:
        return None, "wrong_scope"
    raw_label = outcome.get("goal_to90_normal_time")
    if raw_label is None:
        raw_label = record.get("goal_to90_normal_time")
    if raw_label is None:
        raw_label = outcome.get("goal_to90", record.get("goal_to90"))
    if raw_label is True or raw_label == 1:
        label = 1
    elif raw_label is False or raw_label == 0:
        label = 0
    else:
        return None, "missing_label"
    resolved = _parse_utc(
        outcome.get("resolved_at_utc") or record.get("resolved_at_utc")
    )
    if resolved is None:
        return None, "missing_resolved_timestamp"
    boundary = max(
        (
            value
            for value in (observation_time, quote_time, decision_time)
            if value is not None
        ),
        default=None,
    )
    if boundary is None or resolved <= boundary:
        return None, "non_prospective_outcome_timing"
    return label, "resolved"


def _metrics(rows: Sequence[Mapping[str, Any]], source: str) -> dict[str, Any]:
    usable = [row for row in rows if row.get(source) is not None]
    if not usable:
        return {
            "rows": 0,
            "fixtures": 0,
            "log_loss": None,
            "brier": None,
            "average_probability": None,
            "actual_rate": None,
        }
    per_fixture = Counter(int(row["fixture_id"]) for row in usable)
    weighted: list[tuple[float, int, float]] = []
    for row in usable:
        fixture_id = int(row["fixture_id"])
        probability = float(row[source])
        label = int(row["label"])
        weighted.append((probability, label, 1.0 / per_fixture[fixture_id]))
    total_weight = sum(weight for _, _, weight in weighted)
    log_loss = -sum(
        weight
        * (
            label * math.log(probability)
            + (1 - label) * math.log1p(-probability)
        )
        for probability, label, weight in weighted
    ) / total_weight
    brier = sum(
        weight * (probability - label) ** 2
        for probability, label, weight in weighted
    ) / total_weight
    return {
        "rows": len(usable),
        "fixtures": len(per_fixture),
        "log_loss": round(log_loss, 9),
        "brier": round(brier, 9),
        "average_probability": round(
            sum(probability * weight for probability, _, weight in weighted)
            / total_weight,
            9,
        ),
        "actual_rate": round(
            sum(label * weight for _, label, weight in weighted) / total_weight,
            9,
        ),
    }


def _source_comparison(
    rows: Sequence[Mapping[str, Any]], source: str
) -> dict[str, Any]:
    paired = [
        row
        for row in rows
        if row.get(source) is not None and row.get("market") is not None
    ]
    source_metrics = _metrics(paired, source)
    market_metrics = _metrics(paired, "market")
    if not paired:
        log_loss_gain = None
        brier_gain = None
        winner = None
    else:
        log_loss_gain = round(
            float(market_metrics["log_loss"]) - float(source_metrics["log_loss"]),
            9,
        )
        brier_gain = round(
            float(market_metrics["brier"]) - float(source_metrics["brier"]),
            9,
        )
        winner = source if log_loss_gain > 0 else "market" if log_loss_gain < 0 else "tie"
    return {
        "aligned_rows": len(paired),
        "aligned_fixtures": len({row["fixture_id"] for row in paired}),
        "source": source_metrics,
        "market": market_metrics,
        "source_improvement_vs_market": {
            "log_loss": log_loss_gain,
            "brier": brier_gain,
        },
        "lower_log_loss": winner,
    }


def _market_dimensions(
    decision: Mapping[str, Any], quote: Mapping[str, Any]
) -> dict[str, Optional[str]]:
    decision_market = _mapping(decision.get("market"))
    quote_market = _mapping(quote.get("market"))
    provider = str(
        quote.get("provider") or decision.get("provider") or "unknown"
    )
    bet_id = quote_market.get("bet_id", decision_market.get("bet_id"))
    bet_name = quote_market.get("bet_name", decision_market.get("bet_name"))
    bet = f"{bet_id}:{bet_name}" if bet_id is not None else str(bet_name or "unknown")
    bookmaker_id = quote_market.get(
        "bookmaker_id", decision_market.get("bookmaker_id")
    )
    bookmaker_name = quote_market.get(
        "bookmaker_name", decision_market.get("bookmaker_name")
    )
    bookmaker = None
    if bookmaker_id is not None or bookmaker_name:
        bookmaker = (
            f"{bookmaker_id}:{bookmaker_name}"
            if bookmaker_id is not None
            else str(bookmaker_name)
        )
    return {"provider": provider, "bet": bet, "bookmaker": bookmaker}


def _market_decimal_odds(
    record: Mapping[str, Any], side: str
) -> Optional[float]:
    market = _mapping(record.get("market"))
    nested = _mapping(market.get("decimal_odds"))
    value = nested.get(side)
    if value is None:
        value = record.get(f"{side}_decimal")
    number = _finite_float(value)
    return number if number is not None and number > 1.0 else None


def _market_identity_value(record: Mapping[str, Any], key: str) -> Any:
    market = _mapping(record.get("market"))
    aliases = {
        "current_goals": ("current_goals",),
        "target_line": ("target_line", "line"),
        "bet_id": ("bet_id",),
    }[key]
    for alias in aliases:
        if alias in market:
            return market.get(alias)
        if alias in record:
            return record.get(alias)
    return None


def _movement_for_decision(
    evaluated: Mapping[str, Any],
    snapshots: Mapping[str, Mapping[str, Any]],
    snapshots_by_fixture: Mapping[int, Sequence[Mapping[str, Any]]],
    *,
    horizon_seconds: float,
    future_tolerance_seconds: float,
    max_alignment_seconds: float,
) -> tuple[Optional[dict[str, Any]], str]:
    """Measure a later quote only while score, line, and market stay equal."""

    row = _mapping(evaluated.get("row"))
    if row.get("market") is None:
        return None, "initial_market_unavailable"
    fixture_id = evaluated.get("fixture_id")
    movement_start_time = evaluated.get("movement_start_time")
    quote_key = str(evaluated.get("quote_key") or "")
    initial = snapshots.get(quote_key)
    if (
        not isinstance(fixture_id, int)
        or not isinstance(movement_start_time, datetime)
        or not isinstance(initial, Mapping)
    ):
        if not isinstance(movement_start_time, datetime):
            return None, str(
                evaluated.get("delivery_problem")
                or "movement_boundary_missing"
            )
        return None, "initial_quote_missing"
    initial_time = _record_time(initial)
    initial_probability = _market_probability(_mapping(initial.get("market")))
    if initial_time is None or initial_probability is None:
        return None, "initial_quote_invalid"
    initial_goals = _finite_float(
        _market_identity_value(initial, "current_goals")
    )
    initial_line = _finite_float(_market_identity_value(initial, "target_line"))
    initial_bet = _market_identity_value(initial, "bet_id")
    initial_provider = str(initial.get("provider") or "").strip()
    initial_provider_time = _parse_utc(
        _mapping(initial.get("market")).get("api_updated_at_utc")
        or initial.get("provider_update_utc")
    )
    if initial_goals is None or initial_line is None:
        return None, "initial_market_identity_missing"
    if not initial_provider or initial_provider_time is None:
        return None, "initial_provider_identity_missing"

    deadline = movement_start_time.timestamp() + float(horizon_seconds)
    candidates: list[tuple[datetime, Mapping[str, Any], float]] = []
    for candidate in snapshots_by_fixture.get(fixture_id, ()):
        candidate_key = _snapshot_key(candidate)
        if not candidate_key or candidate_key == quote_key:
            continue
        candidate_time = _record_time(candidate)
        if candidate_time is None or candidate_time <= movement_start_time:
            continue
        if candidate_time.timestamp() > deadline:
            continue
        if str(candidate.get("provider") or "").strip() != initial_provider:
            continue
        provider_time = _parse_utc(
            _mapping(candidate.get("market")).get("api_updated_at_utc")
            or candidate.get("provider_update_utc")
        )
        if provider_time is None:
            continue
        if provider_time <= movement_start_time:
            continue
        if (provider_time - candidate_time).total_seconds() > future_tolerance_seconds:
            continue
        if (candidate_time - provider_time).total_seconds() > max_alignment_seconds:
            continue
        if provider_time <= initial_provider_time:
            continue
        goals = _finite_float(
            _market_identity_value(candidate, "current_goals")
        )
        line = _finite_float(_market_identity_value(candidate, "target_line"))
        bet = _market_identity_value(candidate, "bet_id")
        if goals != initial_goals or line != initial_line or bet != initial_bet:
            continue
        probability = _market_probability(_mapping(candidate.get("market")))
        if probability is None:
            continue
        candidates.append((candidate_time, candidate, probability))
    if not candidates:
        return None, "no_later_same_state_quote"

    later_time, later, later_probability = max(
        candidates, key=lambda item: item[0]
    )
    initial_over = _market_decimal_odds(initial, "over")
    later_over = _market_decimal_odds(later, "over")
    probability_delta_pp = (later_probability - initial_probability) * 100.0
    if probability_delta_pp > 1e-9:
        direction = "market_probability_up"
    elif probability_delta_pp < -1e-9:
        direction = "market_probability_down"
    else:
        direction = "flat"
    return {
        "observation_id": row.get("observation_id"),
        "fixture_id": fixture_id,
        "initial_quote_record_key": quote_key,
        "later_quote_record_key": _snapshot_key(later),
        "initial_captured_at_utc": initial_time.isoformat(),
        "movement_started_at_utc": movement_start_time.isoformat(),
        "later_captured_at_utc": later_time.isoformat(),
        "elapsed_seconds": round(
            (later_time - movement_start_time).total_seconds(), 6
        ),
        "initial_fair_probability": initial_probability,
        "later_fair_probability": later_probability,
        "fair_probability_delta_pp": round(probability_delta_pp, 9),
        "initial_over_decimal": initial_over,
        "later_over_decimal": later_over,
        "over_decimal_delta": (
            round(later_over - initial_over, 9)
            if initial_over is not None and later_over is not None
            else None
        ),
        "direction": direction,
    }, "available"


def _breakdown(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, Any]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("market") is None:
            continue
        value = row.get(field)
        if value is not None:
            grouped[str(value)].append(row)
    result: dict[str, Any] = {}
    for value, group in sorted(grouped.items()):
        result[value] = {
            "rows": len(group),
            "fixtures": len({row["fixture_id"] for row in group}),
            "metrics": {source: _metrics(group, source) for source in SOURCE_NAMES},
        }
    return result


def _evaluate_decision(
    observation_id: str,
    decision: Mapping[str, Any],
    snapshots: Mapping[str, Mapping[str, Any]],
    *,
    delivery: Optional[Mapping[str, Any]] = None,
    max_alignment_seconds: float,
    future_tolerance_seconds: float,
) -> dict[str, Any]:
    """Evaluate only information frozen before an outcome is consulted."""
    fixture_id = _fixture_id(decision)
    observation_time = _observation_time(decision)
    decision_time = _decision_time(decision)
    observation = _mapping(decision.get("observation"))
    decision_minute = _finite_float(
        decision.get("minute", observation.get("minute"))
    )
    decision_problem: Optional[str] = None
    if observation_time is None or decision_time is None:
        decision_problem = "missing_decision_timestamps"
    elif decision_time < observation_time:
        decision_problem = "decision_before_observation"
    elif (decision_time - observation_time).total_seconds() > max_alignment_seconds:
        decision_problem = "decision_alignment_too_late"
    decision_details = _mapping(decision.get("decision"))
    publication_policy = _mapping(decision.get("publication_policy"))
    publication_intent = bool(
        str(decision_details.get("final_decision") or "").upper() == "ALLOW"
        or decision_details.get("active_publication_allow") is True
        or publication_policy.get("publication_allow") is True
    )
    legacy_telegram = _mapping(decision.get("telegram"))
    telegram = (
        _mapping(delivery.get("telegram"))
        if isinstance(delivery, Mapping)
        else legacy_telegram
    )
    send_started: Optional[datetime] = None
    send_finished: Optional[datetime] = None
    delivery_problem: Optional[str] = None
    if isinstance(delivery, Mapping) and _fixture_id(delivery) != fixture_id:
        delivery_problem = "delivery_fixture_id_mismatch"
    elif telegram.get("send_attempted") is True:
        message_id = _finite_float(telegram.get("message_id"))
        if telegram.get("send_ok") is not True:
            delivery_problem = "telegram_send_failed"
        elif message_id is None or message_id <= 0 or not message_id.is_integer():
            delivery_problem = "telegram_message_id_missing"
        else:
            send_started = _parse_utc(telegram.get("send_started_at_utc"))
            send_finished = _parse_utc(telegram.get("send_finished_at_utc"))
        if delivery_problem is None:
            if send_started is None or send_finished is None:
                delivery_problem = "missing_telegram_timestamps"
            elif decision_time is None or decision_time > send_started:
                delivery_problem = "decision_after_telegram_send_started"
            elif send_finished < send_started:
                delivery_problem = "telegram_finished_before_started"
            elif isinstance(delivery, Mapping):
                delivery_recorded = _record_time(delivery)
                if delivery_recorded is None:
                    delivery_problem = "missing_delivery_record_timestamp"
                elif delivery_recorded < send_finished:
                    delivery_problem = "delivery_recorded_before_send_finished"
    elif isinstance(delivery, Mapping):
        delivery_problem = "delivery_not_attempted"
    elif publication_intent:
        delivery_problem = "delivery_missing_for_publication"

    publication_confirmed = bool(
        publication_intent
        and delivery_problem is None
        and telegram.get("send_attempted") is True
        and telegram.get("send_ok") is True
    )
    movement_start_time: Optional[datetime] = decision_time
    if (
        delivery_problem is None
        and telegram.get("send_attempted") is True
        and send_finished is not None
    ):
        movement_start_time = send_finished
    elif publication_intent:
        movement_start_time = None

    quote_key = _quote_reference(decision)
    quote = snapshots.get(quote_key, {}) if quote_key else {}
    market_block = _mapping(quote.get("market")) or _mapping(decision.get("market"))
    quote_time = (
        _record_time(quote)
        if quote
        else _parse_utc(_mapping(decision.get("market")).get("captured_at_utc"))
    )
    row: dict[str, Any] = {
        "observation_id": observation_id,
        "fixture_id": fixture_id,
        "label": None,
        "bot": None,
        "static_ml": None,
        "rolling_ml": None,
        "market": None,
    }
    row.update(_market_dimensions(decision, quote))
    source_reasons: dict[str, str] = {}

    bot = _source_probability(decision, "bot")
    if decision_problem is not None:
        source_reasons["bot"] = "invalid_decision_timing"
    elif bot is None:
        source_reasons["bot"] = "missing_probability"
    else:
        row["bot"] = bot
        source_reasons["bot"] = "available"

    for source_name in MODEL_SOURCE_NAMES:
        probability = _source_probability(decision, source_name)
        if decision_problem is not None:
            reason: Optional[str] = "invalid_decision_timing"
        elif probability is None:
            reason = "missing_probability"
        else:
            assert observation_time is not None and decision_time is not None
            reason = _model_timing_reason(
                decision,
                source_name,
                observation_id=observation_id,
                fixture_id=fixture_id,
                minute=float(decision_minute),
                observation_time=observation_time,
                decision_time=decision_time,
                future_tolerance_seconds=future_tolerance_seconds,
            )
        if reason is None:
            row[source_name] = probability
            source_reasons[source_name] = "available"
        else:
            source_reasons[source_name] = reason

    market_status = _market_status(decision)
    market_probability = _market_probability(market_block)
    market_reason: Optional[str] = None
    if market_status not in {"ok", "available", "matched"}:
        market_reason = market_status or "unavailable"
    elif not quote_key:
        market_reason = "quote_reference_missing"
    elif not quote and quote_key:
        market_reason = "referenced_quote_missing"
    elif _fixture_id(quote) is None:
        market_reason = "quote_fixture_id_missing"
    elif _fixture_id(quote) != fixture_id:
        market_reason = "quote_fixture_id_mismatch"
    elif not str(
        quote.get("provider") or decision.get("provider") or ""
    ).strip():
        market_reason = "provider_missing"
    elif decision_problem is not None:
        market_reason = "invalid_decision_timing"
    elif quote_time is None or observation_time is None:
        market_reason = "missing_alignment_timestamp"
    elif quote_time > observation_time:
        market_reason = "quote_after_observation"
    elif (observation_time - quote_time).total_seconds() > max_alignment_seconds:
        market_reason = "quote_outside_alignment_window"
    else:
        api_updated = _parse_utc(market_block.get("api_updated_at_utc"))
        if api_updated is None:
            market_reason = "provider_update_timestamp_missing"
        elif api_updated > observation_time:
            market_reason = "api_update_after_observation"
        elif (
            api_updated - quote_time
        ).total_seconds() > future_tolerance_seconds:
            market_reason = "api_update_after_capture"
        elif (quote_time - api_updated).total_seconds() > max_alignment_seconds:
            market_reason = "provider_quote_outside_alignment_window"
        elif market_probability is None:
            market_reason = "missing_fair_probability"
        else:
            market_type = str(market_block.get("market_type") or "").lower()
            scope = str(market_block.get("scope") or "").lower()
            if market_type != "one_more_goal_to90":
                market_reason = "wrong_market_type"
            elif scope not in {"normal_time", "to_90_normal_time"}:
                market_reason = "wrong_market_scope"
            else:
                current_goals = _finite_float(market_block.get("current_goals"))
                target_line = _finite_float(market_block.get("target_line"))
                if (
                    current_goals is None
                    or target_line is None
                ):
                    market_reason = "market_identity_missing"
                elif not math.isclose(
                        target_line, current_goals + 0.5, abs_tol=1e-9
                    ):
                    market_reason = "line_not_next_goal"
                if market_reason is None:
                    decision_match = _mapping(decision.get("match"))
                    decision_home = _finite_float(
                        decision_match.get("score_home")
                    )
                    decision_away = _finite_float(
                        decision_match.get("score_away")
                    )
                    quote_home = _finite_float(quote.get("score_home"))
                    quote_away = _finite_float(quote.get("score_away"))
                    if decision_home is None or decision_away is None:
                        market_reason = "decision_score_missing"
                    elif quote_home is None or quote_away is None:
                        market_reason = "quote_score_missing"
                    elif (
                        decision_home != quote_home
                        or decision_away != quote_away
                    ):
                        market_reason = "quote_score_mismatch"
                    elif (
                        current_goals is not None
                        and decision_home is not None
                        and decision_away is not None
                        and current_goals != decision_home + decision_away
                    ):
                        market_reason = "quote_total_mismatch"
    if market_reason is None:
        row["market"] = market_probability
        source_reasons["market"] = "available"
    else:
        source_reasons["market"] = market_reason

    return {
        "fixture_id": fixture_id,
        "observation_time": observation_time,
        "decision_time": decision_time,
        "movement_start_time": movement_start_time,
        "decision_problem": decision_problem,
        "delivery_problem": delivery_problem,
        "publication_intent": publication_intent,
        "publication_confirmed": publication_confirmed,
        "quote_key": quote_key,
        "quote_time": quote_time,
        "source_reasons": source_reasons,
        "row": row,
    }


def _movement_summary(
    movements: Sequence[Mapping[str, Any]],
    movement_reasons: Mapping[str, int],
) -> dict[str, Any]:
    probability_deltas = [
        float(movement["fair_probability_delta_pp"])
        for movement in movements
    ]
    odds_deltas = [
        float(movement["over_decimal_delta"])
        for movement in movements
        if movement.get("over_decimal_delta") is not None
    ]
    return {
        "eligible_decisions": sum(
            count
            for reason, count in movement_reasons.items()
            if reason != "initial_market_unavailable"
        ),
        "observed_movements": len(movements),
        "fixtures": len(
            {
                movement["fixture_id"]
                for movement in movements
                if movement.get("fixture_id") is not None
            }
        ),
        "coverage_reasons": dict(sorted(movement_reasons.items())),
        "average_fair_probability_delta_pp": (
            round(sum(probability_deltas) / len(probability_deltas), 9)
            if probability_deltas
            else None
        ),
        "average_over_decimal_delta": (
            round(sum(odds_deltas) / len(odds_deltas), 9)
            if odds_deltas
            else None
        ),
        "directions": dict(
            sorted(Counter(movement["direction"] for movement in movements).items())
        ),
        "rows": list(movements),
    }


def build_report(
    journal_path: os.PathLike[str] | str,
    outcome_path: Optional[os.PathLike[str] | str] = None,
    *,
    max_alignment_seconds: float = 120.0,
    future_tolerance_seconds: float = 5.0,
    movement_horizon_seconds: float = 600.0,
) -> dict[str, Any]:
    """Build a leakage-resistant prospective comparison report.

    ``market_benchmark_decision`` is the coverage denominator.  The earliest
    append for an observation wins even when it says that market data was
    unavailable; a later retry is never selected with knowledge of the result.
    """
    timing_values = (
        float(max_alignment_seconds),
        float(future_tolerance_seconds),
        float(movement_horizon_seconds),
    )
    if any(not math.isfinite(value) or value < 0.0 for value in timing_values):
        raise ValueError("timing tolerances must be finite and non-negative")

    raw_records = list(_iter_journal(journal_path))
    outcome_records = list(raw_records)
    external_malformed = 0
    if (
        outcome_path is not None
        and os.path.realpath(os.path.abspath(os.fspath(outcome_path)))
        != os.path.realpath(os.path.abspath(os.fspath(journal_path)))
    ):
        for record in _iter_journal(outcome_path):
            if record.get("record_type") == "__invalid_json__":
                external_malformed += 1
            elif record.get("record_type") in OUTCOME_TYPES:
                outcome_records.append(record)
    malformed = sum(
        record.get("record_type") == "__invalid_json__" for record in raw_records
    )
    malformed += external_malformed

    snapshots: dict[str, dict[str, Any]] = {}
    snapshot_duplicates = 0
    for record in raw_records:
        if record.get("record_type") not in SNAPSHOT_TYPES:
            continue
        key = _snapshot_key(record)
        if not key:
            continue
        previous = snapshots.get(key)
        if previous is not None:
            snapshot_duplicates += 1
            continue
        snapshots[key] = dict(record)
    snapshots_by_fixture: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for snapshot in snapshots.values():
        fixture_id = _fixture_id(snapshot)
        if fixture_id is not None:
            snapshots_by_fixture[fixture_id].append(snapshot)
    for fixture_snapshots in snapshots_by_fixture.values():
        fixture_snapshots.sort(
            key=lambda record: _record_time(record)
            or datetime.min.replace(tzinfo=timezone.utc)
        )

    decisions: dict[str, dict[str, Any]] = {}
    decision_duplicates = 0
    missing_decision_key = 0
    decision_records_raw = 0
    decision_keys_seen: set[str] = set()
    decision_scope_excluded: Counter[str] = Counter()
    for record in raw_records:
        if record.get("record_type") not in DECISION_TYPES:
            continue
        decision_records_raw += 1
        key = _decision_key(record)
        if not key:
            missing_decision_key += 1
            continue
        if key in decision_keys_seen:
            decision_duplicates += 1
            continue
        decision_keys_seen.add(key)
        observation = _mapping(record.get("observation"))
        stage = str(record.get("stage") or observation.get("stage") or "").lower()
        if stage != "decision_pipeline":
            decision_scope_excluded[
                "missing_stage" if not stage else "stage"
            ] += 1
            continue
        raw_minute = record.get("minute", observation.get("minute"))
        minute = _finite_float(raw_minute)
        if raw_minute is None:
            decision_scope_excluded["missing_minute"] += 1
            continue
        if minute is None:
            decision_scope_excluded["invalid_minute"] += 1
            continue
        if not 46.0 <= minute <= 60.0:
            decision_scope_excluded["minute_outside_46_60"] += 1
            continue
        decisions[key] = dict(record)

    deliveries: dict[str, dict[str, Any]] = {}
    delivery_duplicates = 0
    delivery_records_raw = 0
    for record in raw_records:
        if record.get("record_type") not in DELIVERY_TYPES:
            continue
        delivery_records_raw += 1
        key = _decision_key(record)
        if not key:
            continue
        if key in deliveries:
            delivery_duplicates += 1
            continue
        deliveries[key] = dict(record)

    outcomes: dict[str, dict[str, Any]] = {}
    outcomes_by_snapshot: dict[str, dict[str, Any]] = {}
    for record in outcome_records:
        if record.get("record_type") not in OUTCOME_TYPES:
            continue
        key = _decision_key(record)
        snapshot_key = _quote_reference(record) or _snapshot_key(record)
        if key:
            previous = outcomes.get(key)
            if previous is None or _outcome_rank(record) > _outcome_rank(previous):
                outcomes[key] = dict(record)
        if snapshot_key:
            previous = outcomes_by_snapshot.get(snapshot_key)
            if previous is None or _outcome_rank(record) > _outcome_rank(previous):
                outcomes_by_snapshot[snapshot_key] = dict(record)

    counters: Counter[str] = Counter()
    reasons: dict[str, Counter[str]] = {
        source: Counter() for source in SOURCE_NAMES
    }
    all_decision_reasons: dict[str, Counter[str]] = {
        source: Counter() for source in SOURCE_NAMES
    }
    all_decision_available: Counter[str] = Counter()
    outcome_reasons: Counter[str] = Counter()
    publication_outcome_reasons: Counter[str] = Counter()
    delivery_reasons: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []
    publication_rows: list[dict[str, Any]] = []
    movements: list[dict[str, Any]] = []
    movement_reasons: Counter[str] = Counter()
    publication_movements: list[dict[str, Any]] = []
    publication_movement_reasons: Counter[str] = Counter()
    publication_intent_decisions = 0
    confirmed_publication_decisions = 0

    for observation_id, decision in decisions.items():
        counters["decisions_total"] += 1
        evaluated = _evaluate_decision(
            observation_id,
            decision,
            snapshots,
            delivery=deliveries.get(observation_id),
            max_alignment_seconds=max_alignment_seconds,
            future_tolerance_seconds=future_tolerance_seconds,
        )
        fixture_id = evaluated["fixture_id"]
        if fixture_id is None:
            counters["invalid_fixture_id"] += 1
            continue
        publication_intent = bool(evaluated["publication_intent"])
        publication_confirmed = bool(evaluated["publication_confirmed"])
        publication_intent_decisions += int(publication_intent)
        confirmed_publication_decisions += int(publication_confirmed)
        decision_problem = evaluated["decision_problem"]
        if decision_problem is not None:
            counters[str(decision_problem)] += 1
        delivery_problem = evaluated["delivery_problem"]
        delivery_reasons[
            str(delivery_problem or "available_or_not_required")
        ] += 1
        source_reasons = evaluated["source_reasons"]
        for source, source_reason in source_reasons.items():
            all_decision_reasons[source][source_reason] += 1
            if source_reason == "available":
                all_decision_available[source] += 1
        movement, movement_reason = _movement_for_decision(
            evaluated,
            snapshots,
            snapshots_by_fixture,
            horizon_seconds=movement_horizon_seconds,
            future_tolerance_seconds=future_tolerance_seconds,
            max_alignment_seconds=max_alignment_seconds,
        )
        movement_reasons[movement_reason] += 1
        if movement is not None:
            movements.append(movement)
        if publication_confirmed:
            publication_movement_reasons[movement_reason] += 1
            if movement is not None:
                publication_movements.append(movement)

        outcome_record = outcomes.get(observation_id)
        quote_key = str(evaluated["quote_key"] or "")
        if outcome_record is None and quote_key:
            outcome_record = outcomes_by_snapshot.get(quote_key)
        if outcome_record is None:
            outcome_reasons["missing"] += 1
            if publication_confirmed:
                publication_outcome_reasons["missing"] += 1
            continue
        outcome_fixture_id = _fixture_id(outcome_record)
        if outcome_fixture_id is None:
            outcome_reasons["missing_fixture_id"] += 1
            if publication_confirmed:
                publication_outcome_reasons["missing_fixture_id"] += 1
            continue
        if outcome_fixture_id != fixture_id:
            outcome_reasons["fixture_id_mismatch"] += 1
            if publication_confirmed:
                publication_outcome_reasons["fixture_id_mismatch"] += 1
            continue
        label, outcome_reason = _outcome_label(
            outcome_record,
            observation_time=evaluated["observation_time"],
            quote_time=evaluated["quote_time"],
            decision_time=evaluated["decision_time"],
        )
        outcome_reasons[outcome_reason] += 1
        if publication_confirmed:
            publication_outcome_reasons[outcome_reason] += 1
        if label is None:
            continue

        row = dict(evaluated["row"])
        row["label"] = label
        for source, source_reason in source_reasons.items():
            reasons[source][source_reason] += 1
        rows.append(row)
        if publication_confirmed:
            publication_rows.append(row)

    all_aligned = [
        row for row in rows if all(row.get(source) is not None for source in SOURCE_NAMES)
    ]
    publication_aligned = [
        row
        for row in publication_rows
        if all(row.get(source) is not None for source in SOURCE_NAMES)
    ]
    resolved_fixtures = {row["fixture_id"] for row in rows}
    decision_fixtures = {
        fixture_id
        for fixture_id in (_fixture_id(record) for record in decisions.values())
        if fixture_id is not None
    }
    report = {
        "schema_version": 1,
        "report_type": "market_benchmark_report",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": {
            "journal_path": os.path.abspath(os.fspath(journal_path)),
            "outcome_path": os.path.abspath(
                os.fspath(outcome_path if outcome_path is not None else journal_path)
            ),
        },
        "methodology": {
            "target": "goal_to90_normal_time",
            "coverage_denominator": "unique market_benchmark_decision observation_id",
            "eligible_decision_scope": "decision_pipeline minutes 46-60",
            "deduplication": "earliest decision append per observation_id",
            "outcome_join": (
                "latest outcome schema, revision, then timestamp per observation_id"
            ),
            "selection_uses_outcome": False,
            "historical_predictions_recomputed": False,
            "metric_weighting": "equal fixture weight; equal rows within fixture",
            "max_alignment_seconds": max_alignment_seconds,
            "future_tolerance_seconds": future_tolerance_seconds,
            "movement_horizon_seconds": movement_horizon_seconds,
            "movement_same_state_only": True,
            "movement_starts_after": (
                "telegram send_finished for publication decisions; frozen "
                "decision only when no Telegram delivery is required"
            ),
            "confirmed_publication_definition": (
                "publication intent plus send_attempted=true, send_ok=true, "
                "positive Telegram message_id, and valid causal timestamps"
            ),
            "ml_timing_policy": (
                "model cutoff and creation precede observation; frozen prediction "
                "timestamp lies between observation and decision"
            ),
        },
        "coverage": {
            "quote_records": len(snapshots),
            "raw_decision_records": decision_records_raw,
            "decision_records": len(decisions),
            "delivery_records": len(deliveries),
            "raw_delivery_records": delivery_records_raw,
            "decisions_with_delivery": sum(
                observation_id in deliveries for observation_id in decisions
            ),
            "publication_intent_decisions": publication_intent_decisions,
            "confirmed_publications": confirmed_publication_decisions,
            "resolved_confirmed_publications": len(publication_rows),
            "all_sources_aligned_confirmed_publications": len(
                publication_aligned
            ),
            "decision_fixtures": len(decision_fixtures),
            "resolved_decisions": len(rows),
            "resolved_fixtures": len(resolved_fixtures),
            "all_sources_aligned_decisions": len(all_aligned),
            "all_sources_aligned_fixtures": len(
                {row["fixture_id"] for row in all_aligned}
            ),
            "source_available_on_resolved": {
                source: sum(row.get(source) is not None for row in rows)
                for source in SOURCE_NAMES
            },
            "source_available_on_all_decisions": {
                source: all_decision_available[source] for source in SOURCE_NAMES
            },
            "source_coverage_pct_of_all_decisions": {
                source: round(
                    100.0
                    * all_decision_available[source]
                    / len(decisions),
                    6,
                )
                if decisions
                else None
                for source in SOURCE_NAMES
            },
            "outcomes": dict(sorted(outcome_reasons.items())),
            "confirmed_publication_outcomes": dict(
                sorted(publication_outcome_reasons.items())
            ),
            "delivery_timing": dict(sorted(delivery_reasons.items())),
        },
        "metrics": {
            "all_sources_same_cohort": {
                "rows": len(all_aligned),
                "fixtures": len({row["fixture_id"] for row in all_aligned}),
                "sources": {
                    source: _metrics(all_aligned, source) for source in SOURCE_NAMES
                },
            },
            "paired_vs_market": {
                source: _source_comparison(rows, source)
                for source in ("bot", "static_ml", "rolling_ml")
            },
            "confirmed_publications_same_cohort": {
                "rows": len(publication_aligned),
                "fixtures": len(
                    {row["fixture_id"] for row in publication_aligned}
                ),
                "sources": {
                    source: _metrics(publication_aligned, source)
                    for source in SOURCE_NAMES
                },
            },
            "paired_confirmed_publications_vs_market": {
                source: _source_comparison(publication_rows, source)
                for source in ("bot", "static_ml", "rolling_ml")
            },
        },
        "market_movement": {
            **_movement_summary(movements, movement_reasons),
            "confirmed_publications": _movement_summary(
                publication_movements,
                publication_movement_reasons,
            ),
        },
        "breakdown": {
            "by_provider": _breakdown(rows, "provider"),
            "by_market": _breakdown(rows, "bet"),
            # API-Football live odds normally has no bookmaker dimension.  This
            # compatibility view is populated only if a provider supplies it.
            "by_bookmaker": _breakdown(rows, "bookmaker"),
            "bookmaker_dimension_available": any(
                row.get("bookmaker") is not None for row in rows
            ),
        },
        "skipped": {
            "malformed_json": malformed,
            "snapshot_duplicates": snapshot_duplicates,
            "decision_duplicates": decision_duplicates,
            "delivery_duplicates": delivery_duplicates,
            "missing_decision_key": missing_decision_key,
            "decision_scope_excluded": dict(sorted(decision_scope_excluded.items())),
            "decision": dict(sorted(counters.items())),
            "source_reasons_on_resolved": {
                source: dict(sorted(counter.items()))
                for source, counter in reasons.items()
            },
            "source_reasons_on_all_decisions": {
                source: dict(sorted(counter.items()))
                for source, counter in all_decision_reasons.items()
            },
        },
    }
    return report


def render_text(report: Mapping[str, Any]) -> str:
    coverage = _mapping(report.get("coverage"))
    report_metrics = _mapping(report.get("metrics"))
    cohort = _mapping(report_metrics.get("all_sources_same_cohort"))
    sources = _mapping(cohort.get("sources"))
    publication_cohort = _mapping(
        report_metrics.get("confirmed_publications_same_cohort")
    )
    publication_sources = _mapping(publication_cohort.get("sources"))
    movement = _mapping(report.get("market_movement"))
    publication_movement = _mapping(movement.get("confirmed_publications"))
    lines = [
        "Market benchmark (prospective, no historical recomputation)",
        (
            "Coverage: decisions={decisions}, resolved={resolved}, "
            "all-sources-aligned={aligned}"
        ).format(
            decisions=coverage.get("decision_records", 0),
            resolved=coverage.get("resolved_decisions", 0),
            aligned=coverage.get("all_sources_aligned_decisions", 0),
        ),
        "Same-cohort metrics (lower is better):",
    ]
    for source in SOURCE_NAMES:
        metrics = _mapping(sources.get(source))
        log_loss = metrics.get("log_loss")
        brier = metrics.get("brier")
        lines.append(
            f"  {source}: rows={metrics.get('rows', 0)}, "
            f"logloss={log_loss if log_loss is not None else 'n/a'}, "
            f"brier={brier if brier is not None else 'n/a'}"
        )
    lines.extend(
        [
            (
                "Confirmed Telegram publications: sent={sent}, resolved={resolved}, "
                "all-sources-aligned={aligned}"
            ).format(
                sent=coverage.get("confirmed_publications", 0),
                resolved=coverage.get("resolved_confirmed_publications", 0),
                aligned=coverage.get(
                    "all_sources_aligned_confirmed_publications", 0
                ),
            ),
            "Confirmed-publication metrics (lower is better):",
        ]
    )
    for source in SOURCE_NAMES:
        metrics = _mapping(publication_sources.get(source))
        log_loss = metrics.get("log_loss")
        brier = metrics.get("brier")
        lines.append(
            f"  {source}: rows={metrics.get('rows', 0)}, "
            f"logloss={log_loss if log_loss is not None else 'n/a'}, "
            f"brier={brier if brier is not None else 'n/a'}"
        )
    if not _mapping(report.get("breakdown")).get("bookmaker_dimension_available"):
        lines.append(
            "Bookmaker breakdown: unavailable in canonical API-Football live odds; "
            "use provider/market breakdowns."
        )
    lines.append(
        "Same-score market movement: observed={observed}, fixtures={fixtures}, "
        "average_probability_delta_pp={delta}".format(
            observed=movement.get("observed_movements", 0),
            fixtures=movement.get("fixtures", 0),
            delta=(
                movement.get("average_fair_probability_delta_pp")
                if movement.get("average_fair_probability_delta_pp") is not None
                else "n/a"
            ),
        )
    )
    lines.append(
        "Confirmed-publication movement: observed={observed}, fixtures={fixtures}, "
        "average_probability_delta_pp={delta}".format(
            observed=publication_movement.get("observed_movements", 0),
            fixtures=publication_movement.get("fixtures", 0),
            delta=(
                publication_movement.get("average_fair_probability_delta_pp")
                if publication_movement.get("average_fair_probability_delta_pp")
                is not None
                else "n/a"
            ),
        )
    )
    return "\n".join(lines) + "\n"


def _normalized_path(path: os.PathLike[str] | str) -> str:
    return os.path.normcase(os.path.realpath(os.path.abspath(os.fspath(path))))


def _write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--journal", default="data/market_benchmark.jsonl", help="Market journal"
    )
    parser.add_argument(
        "--outcomes",
        default=None,
        help=(
            "Optional canonical outcome JSONL; when omitted, a sibling "
            "observation_history.jsonl is used if present"
        ),
    )
    parser.add_argument("--output", default=None, help="Optional output path")
    parser.add_argument("--json", action="store_true", help="Render JSON")
    parser.add_argument("--max-alignment-seconds", type=float, default=120.0)
    parser.add_argument("--future-tolerance-seconds", type=float, default=5.0)
    parser.add_argument("--movement-horizon-seconds", type=float, default=600.0)
    args = parser.parse_args(argv)

    outcome_path = args.outcomes
    if outcome_path is None:
        configured = os.environ.get(
            "MARKET_BENCHMARK_CANONICAL_OUTCOME_FILE", ""
        ).strip()
        candidate = Path(configured) if configured else Path(
            args.journal
        ).with_name("observation_history.jsonl")
        if candidate.exists():
            outcome_path = os.fspath(candidate)

    report = build_report(
        args.journal,
        outcome_path,
        max_alignment_seconds=args.max_alignment_seconds,
        future_tolerance_seconds=args.future_tolerance_seconds,
        movement_horizon_seconds=args.movement_horizon_seconds,
    )
    rendered = (
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        if args.json
        else render_text(report)
    )
    if args.output:
        protected: set[str] = set()
        for input_path in (
            path for path in (args.journal, outcome_path) if path
        ):
            protected.add(_normalized_path(input_path))
            protected.update(
                _normalized_path(candidate)
                for candidate in market_journal_paths(input_path)
            )
        if _normalized_path(args.output) in protected:
            parser.error("output cannot replace an input journal")
        _write_atomic(Path(args.output), rendered)
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
