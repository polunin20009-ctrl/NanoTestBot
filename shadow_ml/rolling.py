from __future__ import annotations

import copy
import math
from collections import OrderedDict, defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Optional


ROLLING_DYNAMICS_SCHEMA_VERSION = 1
DEFAULT_WINDOWS = (5, 10)
DEFAULT_MAX_EXTRA_MINUTES = 2
DEFAULT_MAX_CACHED_PAYLOADS = 10_000
DEFAULT_FIXTURE_RETENTION_HOURS = 12

# These are cumulative match counters. A negative difference means that the
# provider corrected/reset a counter; it is not negative attacking activity.
_CUMULATIVE_PAIR_METRICS = {
    "xg_total": "xg",
    "shots_on_target_total": "shots_on_target",
    "shots_in_box_total": "shots_in_box",
    "total_shots_total": "total_shots",
    "corners_total": "corners",
}
_BASELINE_STAGES = {"rolling_seed", "decision_pipeline"}
_ACTIVITY_METRICS = (*_CUMULATIVE_PAIR_METRICS, "pressure_index")


def _finite_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _safe_int(value: Any) -> Optional[int]:
    number = _finite_float(value)
    if number is None:
        return None
    try:
        return int(number)
    except (OverflowError, ValueError):
        return None


def _parse_timestamp(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _canonical_xg_source(value: Any) -> str:
    source = str(value or "").strip().lower()
    if not source or source in {"missing", "unknown", "none"}:
        return "missing"
    if "fallback" in source or "estimate" in source or "derived" in source:
        return "estimated"
    if "api" in source:
        return "api"
    return source


def _available_pair_total(
    observation: Mapping[str, Any],
    metric: str,
) -> Optional[float]:
    raw = _as_mapping(observation.get("raw_metrics"))
    availability = _as_mapping(observation.get("availability"))
    values = []
    for side in ("home", "away"):
        key = f"{metric}_{side}"
        if availability.get(key) is not True:
            return None
        number = _finite_float(raw.get(key))
        if number is None:
            return None
        values.append(number)
    return float(sum(values))


def _observation_snapshot(
    observation: Mapping[str, Any],
) -> Optional[dict[str, Any]]:
    fixture_id = _safe_int(observation.get("fixture_id"))
    minute = _safe_int(observation.get("minute"))
    created_at = _parse_timestamp(observation.get("created_at_utc"))
    observation_id = str(
        observation.get("observation_key")
        or observation.get("observation_id")
        or ""
    ).strip()
    if (
        fixture_id is None
        or fixture_id <= 0
        or minute is None
        or minute <= 0
        or created_at is None
        or not observation_id
    ):
        return None

    values: dict[str, Optional[float]] = {}
    for output_name, raw_name in _CUMULATIVE_PAIR_METRICS.items():
        values[output_name] = _available_pair_total(observation, raw_name)

    match = _as_mapping(observation.get("match"))
    score_home = _finite_float(match.get("score_home"))
    score_away = _finite_float(match.get("score_away"))
    values["score_total"] = (
        score_home + score_away
        if score_home is not None and score_away is not None
        else None
    )

    features = _as_mapping(observation.get("features"))
    values["pressure_index"] = _finite_float(features.get("pressure_index"))

    return {
        "fixture_id": fixture_id,
        "minute": minute,
        "created_at": created_at,
        "created_at_utc": created_at.isoformat(),
        "observation_id": observation_id,
        "schema_version": _safe_int(observation.get("schema_version")) or 1,
        "stage": str(observation.get("stage") or "unknown"),
        "values": values,
        "available_value_count": sum(
            value is not None for value in values.values()
        ),
        "xg_source": _canonical_xg_source(
            _as_mapping(observation.get("data_quality")).get("xg_source")
        ),
    }


def _unavailable_window(window: int, reason: str) -> dict[str, Any]:
    return {
        "status": "unavailable",
        "reason": reason,
        "requested_window_minutes": int(window),
        "baseline_observation_id": None,
        "baseline_minute": None,
        "actual_span_minutes": None,
        "sample_count": 0,
        "crosses_halftime": None,
        "deltas": {
            name: None
            for name in (*_CUMULATIVE_PAIR_METRICS, "score_total", "pressure_index")
        },
        "rates_per_minute": {
            name: None
            for name in (*_CUMULATIVE_PAIR_METRICS, "score_total", "pressure_index")
        },
        "availability": {
            name: False
            for name in (*_CUMULATIVE_PAIR_METRICS, "score_total", "pressure_index")
        },
        "invalid_reasons": {},
    }


class RollingDynamicsTracker:
    """Prospective, fixture-local rolling dynamics for immutable observations.

    The tracker only uses observations that existed before the current row.
    It never reads decisions, Telegram state, or outcomes.
    """

    def __init__(
        self,
        *,
        windows: tuple[int, ...] = DEFAULT_WINDOWS,
        max_extra_minutes: int = DEFAULT_MAX_EXTRA_MINUTES,
        max_cached_payloads: int = DEFAULT_MAX_CACHED_PAYLOADS,
        fixture_retention_hours: int = DEFAULT_FIXTURE_RETENTION_HOURS,
    ) -> None:
        normalized_windows = tuple(
            sorted({int(window) for window in windows if int(window) > 0})
        )
        if not normalized_windows:
            raise ValueError("at least one positive rolling window is required")
        self.windows = normalized_windows
        self.max_extra_minutes = max(0, int(max_extra_minutes))
        self.max_cached_payloads = max(1, int(max_cached_payloads))
        self.fixture_retention_hours = max(1, int(fixture_retention_hours))
        self._history: dict[int, dict[str, dict[str, Any]]] = defaultdict(dict)
        self._fixture_latest_at: dict[int, datetime] = {}
        self._latest_at: Optional[datetime] = None
        self._computed: OrderedDict[
            tuple[int, str, int], dict[str, Any]
        ] = OrderedDict()

    def memory_stats(self) -> dict[str, int]:
        """Return constant-cost size telemetry without copying payloads."""
        return {
            "fixtures": len(self._history),
            "history_records": sum(
                len(fixture_history)
                for fixture_history in self._history.values()
            ),
            "computed_payloads": len(self._computed),
        }

    @staticmethod
    def _cache_key(snapshot: Mapping[str, Any]) -> tuple[int, str, int]:
        return (
            int(snapshot["fixture_id"]),
            str(snapshot["observation_id"]),
            int(snapshot.get("schema_version") or 1),
        )

    def _cache_payload(
        self,
        snapshot: Mapping[str, Any],
        payload: Mapping[str, Any],
    ) -> None:
        key = self._cache_key(snapshot)
        self._computed[key] = copy.deepcopy(dict(payload))
        self._computed.move_to_end(key)
        while len(self._computed) > self.max_cached_payloads:
            self._computed.popitem(last=False)

    def ingest(self, observation: Mapping[str, Any]) -> bool:
        snapshot = _observation_snapshot(observation)
        if snapshot is None:
            return False
        frozen_payload = observation.get("rolling_dynamics")
        if (
            isinstance(frozen_payload, Mapping)
            and _safe_int(frozen_payload.get("schema_version"))
            == ROLLING_DYNAMICS_SCHEMA_VERSION
            and frozen_payload.get("production_applied") is False
        ):
            self._cache_payload(snapshot, frozen_payload)
        if str(snapshot.get("stage") or "") not in _BASELINE_STAGES:
            return False
        # A score-only row is not evidence of attacking dynamics. Empty/error
        # seeds are persisted for request dedupe but never become baselines.
        if not any(
            _as_mapping(snapshot.get("values")).get(name) is not None
            for name in _ACTIVITY_METRICS
        ):
            return False
        fixture_id = int(snapshot["fixture_id"])
        created_at = snapshot["created_at"]
        observation_id = str(snapshot["observation_id"])
        history_key = (
            f"{observation_id}:v{int(snapshot.get('schema_version') or 1)}"
        )
        if history_key in self._history.get(fixture_id, {}):
            return False
        if (
            self._latest_at is not None
            and created_at
            < self._latest_at - timedelta(hours=self.fixture_retention_hours)
        ):
            return False
        self._latest_at = (
            max(self._latest_at, created_at)
            if self._latest_at is not None
            else created_at
        )
        previous_fixture_at = self._fixture_latest_at.get(fixture_id)
        self._fixture_latest_at[fixture_id] = (
            max(previous_fixture_at, created_at)
            if previous_fixture_at is not None
            else created_at
        )
        self._prune_stale_fixtures(self._latest_at)

        fixture_history = self._history[fixture_id]
        fixture_history[history_key] = snapshot
        self._prune_fixture(fixture_id, int(snapshot["minute"]))
        return True

    def _prune_stale_fixtures(self, reference_time: datetime) -> None:
        cutoff = reference_time - timedelta(hours=self.fixture_retention_hours)
        stale_fixture_ids = [
            fixture_id
            for fixture_id, latest_at in self._fixture_latest_at.items()
            if latest_at < cutoff
        ]
        for fixture_id in stale_fixture_ids:
            self._history.pop(fixture_id, None)
            self._fixture_latest_at.pop(fixture_id, None)

    def _prune_fixture(self, fixture_id: int, newest_minute: int) -> None:
        history = self._history.get(fixture_id)
        if not history:
            return
        oldest_minute = newest_minute - max(self.windows) - self.max_extra_minutes
        stale = [
            observation_id
            for observation_id, snapshot in history.items()
            if int(snapshot.get("minute") or 0) < oldest_minute
        ]
        for observation_id in stale:
            history.pop(observation_id, None)

    def _baseline_for(
        self,
        current: Mapping[str, Any],
        window: int,
    ) -> Optional[dict[str, Any]]:
        fixture_history = self._history.get(int(current["fixture_id"]), {})
        candidates = []
        current_values = _as_mapping(current.get("values"))
        for snapshot in fixture_history.values():
            if str(snapshot.get("observation_id")) == str(
                current.get("observation_id")
            ):
                continue
            if snapshot.get("created_at") >= current.get("created_at"):
                continue
            span = int(current["minute"]) - int(snapshot.get("minute") or 0)
            if window <= span <= window + self.max_extra_minutes:
                baseline_values = _as_mapping(snapshot.get("values"))
                usable_activity_count = 0
                for name in _ACTIVITY_METRICS:
                    current_value = _finite_float(current_values.get(name))
                    baseline_value = _finite_float(baseline_values.get(name))
                    if current_value is None or baseline_value is None:
                        continue
                    if name == "xg_total" and (
                        current.get("xg_source") == "missing"
                        or snapshot.get("xg_source") == "missing"
                        or current.get("xg_source") != snapshot.get("xg_source")
                    ):
                        continue
                    if name != "pressure_index" and current_value < baseline_value:
                        continue
                    usable_activity_count += 1
                candidates.append((usable_activity_count, span, snapshot))
        if not candidates:
            return None
        candidates.sort(
            key=lambda item: (
                -item[0],
                abs(item[1] - window),
                -int(item[2].get("available_value_count") or 0),
                -item[2]["created_at"].timestamp(),
                str(item[2].get("observation_id") or ""),
            )
        )
        return candidates[0][2]

    def _window_payload(
        self,
        current: Mapping[str, Any],
        window: int,
    ) -> dict[str, Any]:
        baseline = self._baseline_for(current, window)
        if baseline is None:
            return _unavailable_window(window, "baseline_missing")

        span = int(current["minute"]) - int(baseline["minute"])
        current_values = _as_mapping(current.get("values"))
        baseline_values = _as_mapping(baseline.get("values"))
        names = (*_CUMULATIVE_PAIR_METRICS, "score_total", "pressure_index")
        deltas: dict[str, Optional[float]] = {}
        rates: dict[str, Optional[float]] = {}
        availability: dict[str, bool] = {}
        invalid_reasons: dict[str, str] = {}

        for name in names:
            current_value = _finite_float(current_values.get(name))
            baseline_value = _finite_float(baseline_values.get(name))
            reason = ""
            if current_value is None:
                reason = "current_missing"
            elif baseline_value is None:
                reason = "baseline_missing"
            elif (
                name == "xg_total"
                and (
                    current.get("xg_source") == "missing"
                    or baseline.get("xg_source") == "missing"
                    or current.get("xg_source") != baseline.get("xg_source")
                )
            ):
                reason = "source_mismatch"

            delta = (
                current_value - baseline_value
                if not reason
                and current_value is not None
                and baseline_value is not None
                else None
            )
            if (
                delta is not None
                and name != "pressure_index"
                and delta < -1e-9
            ):
                reason = "counter_reset"
                delta = None
            if delta is not None and abs(delta) < 1e-9:
                delta = 0.0

            available = delta is not None
            availability[name] = available
            deltas[name] = round(delta, 6) if delta is not None else None
            rates[name] = (
                round(delta / span, 6)
                if delta is not None and span > 0
                else None
            )
            if reason:
                invalid_reasons[name] = reason

        history = self._history.get(int(current["fixture_id"]), {})
        sample_count = sum(
            1
            for snapshot in history.values()
            if snapshot.get("created_at") < current.get("created_at")
            and int(baseline["minute"])
            <= int(snapshot.get("minute") or 0)
            < int(current["minute"])
        )
        available_count = sum(1 for available in availability.values() if available)
        available_activity_count = sum(
            1 for name in _ACTIVITY_METRICS if availability.get(name) is True
        )
        return {
            "status": "ok" if available_activity_count else "unavailable",
            "reason": (
                None if available_activity_count else "activity_metrics_unavailable"
            ),
            "requested_window_minutes": int(window),
            "baseline_observation_id": baseline.get("observation_id"),
            "baseline_minute": int(baseline["minute"]),
            "baseline_stage": baseline.get("stage"),
            "actual_span_minutes": span,
            "sample_count": sample_count,
            "crosses_halftime": bool(
                int(baseline["minute"]) <= 45 < int(current["minute"])
            ),
            "deltas": deltas,
            "rates_per_minute": rates,
            "availability": availability,
            "available_metric_count": available_count,
            "available_activity_metric_count": available_activity_count,
            "invalid_reasons": invalid_reasons,
        }

    def compute(self, observation: Mapping[str, Any]) -> dict[str, Any]:
        current = _observation_snapshot(observation)
        if current is None:
            windows = {
                f"{window}m": _unavailable_window(window, "invalid_current")
                for window in self.windows
            }
        else:
            cached = self._computed.get(self._cache_key(current))
            if cached is not None:
                self._computed.move_to_end(self._cache_key(current))
                return copy.deepcopy(cached)
            windows = {
                f"{window}m": self._window_payload(current, window)
                for window in self.windows
            }
        payload = {
            "schema_version": ROLLING_DYNAMICS_SCHEMA_VERSION,
            "mode": "shadow_collection",
            "production_applied": False,
            "windows": windows,
            "available_windows": sum(
                1 for payload in windows.values() if payload["status"] == "ok"
            ),
        }
        if current is not None:
            self._cache_payload(current, payload)
        return payload

    def freeze(self, observation: Mapping[str, Any]) -> dict[str, Any]:
        frozen = copy.deepcopy(dict(observation))
        frozen["rolling_dynamics"] = self.compute(frozen)
        return frozen


def freeze_rolling_dynamics_batch(
    observations: Iterable[Mapping[str, Any]],
    *,
    windows: tuple[int, ...] = DEFAULT_WINDOWS,
    max_extra_minutes: int = DEFAULT_MAX_EXTRA_MINUTES,
) -> list[dict[str, Any]]:
    """Freeze prospective rolling blocks in actual observation-time order."""
    materialized = [
        row for row in observations if isinstance(row, Mapping)
    ]
    materialized.sort(
        key=lambda row: (
            _parse_timestamp(row.get("created_at_utc"))
            or datetime.min.replace(tzinfo=timezone.utc),
            str(
                row.get("observation_key")
                or row.get("observation_id")
                or ""
            ),
        )
    )
    tracker = RollingDynamicsTracker(
        windows=windows,
        max_extra_minutes=max_extra_minutes,
    )
    result = []
    for row in materialized:
        frozen = tracker.freeze(row)
        result.append(frozen)
        tracker.ingest(frozen)
    return result


def extract_rolling_deltas(
    observations: Iterable[Mapping[str, Any]],
    *,
    fields: tuple[str, ...],
    windows: tuple[int, ...] = DEFAULT_WINDOWS,
) -> list[dict[str, Any]]:
    """Legacy generic helper retained for offline callers.

    New live code uses :class:`RollingDynamicsTracker` with the real
    ``raw_metrics`` observation schema.
    """
    history: dict[int, deque[Mapping[str, Any]]] = defaultdict(deque)
    result: list[dict[str, Any]] = []
    ordered = sorted(
        (row for row in observations if isinstance(row, Mapping)),
        key=lambda row: (
            int(row.get("fixture_id") or 0),
            str(row.get("created_at_utc") or ""),
            int(row.get("minute") or 0),
        ),
    )
    for row in ordered:
        fixture_id = int(row.get("fixture_id") or 0)
        minute = int(row.get("minute") or 0)
        if fixture_id <= 0 or minute <= 0:
            continue
        queue = history[fixture_id]
        item: dict[str, Any] = {
            "fixture_id": fixture_id,
            "minute": minute,
            "observation_id": row.get("observation_id"),
        }
        metrics = row.get("metrics") if isinstance(row.get("metrics"), Mapping) else {}
        for window in windows:
            baseline = next(
                (
                    old
                    for old in reversed(queue)
                    if minute - int(old.get("minute") or 0) == int(window)
                ),
                None,
            )
            old_metrics = (
                baseline.get("metrics")
                if baseline and isinstance(baseline.get("metrics"), Mapping)
                else {}
            )
            for field in fields:
                current = _finite_float(metrics.get(field))
                previous = _finite_float(old_metrics.get(field))
                key = f"{field}_delta_{window}m"
                item[key] = (
                    current - previous
                    if current is not None and previous is not None
                    else None
                )
        result.append(item)
        queue.append(row)
        while queue and minute - int(queue[0].get("minute") or 0) > max(windows):
            queue.popleft()
    return result
