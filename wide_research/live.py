"""Live, bounded evaluation for wide prospective research.

This module has no Telegram dependency.  It records the broad technical
universe once per fixture, freezes only the first matching snapshot for each
immutable phase, and exposes an opt-in production router backed by a
checksummed last-known-good manifest.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Any, Iterator, Mapping, Optional, Sequence

from .features import FEATURE_SCHEMA_VERSION, extract_features, evaluate_universe
from .lifecycle import ActiveManifestCache
from .rules import evaluate_rule, evaluate_snapshot, rule_manifest_from_dict
from .schema import PASS
from .store import WideResearchStore
from .retry_spool import RetrySpool, RetrySpoolError
from outcome_revision import outcome_store_version


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _source_id(snapshot: Mapping[str, Any]) -> str:
    return str(
        snapshot.get("observation_id")
        or snapshot.get("observation_key")
        or snapshot.get("decision_id")
        or ""
    ).strip()


def _league(snapshot: Mapping[str, Any]) -> Optional[str]:
    match = _mapping(snapshot.get("match"))
    value = match.get("league_id") or match.get("league_name")
    normalized = str(value or "").strip()
    return normalized or None


def _parse_utc(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


class WideShadowLayer:
    """Evaluate a bounded set of prospective phases without changing signals."""

    def __init__(
        self,
        store: WideResearchStore,
        *,
        max_active_rules: int = 10,
        refresh_seconds: float = 60.0,
        max_prediction_lag_seconds: float = 300.0,
        retry_spool: Optional[RetrySpool] = None,
    ) -> None:
        self.store = store
        self.include_extended = getattr(store, "profile_id", None) == "rare_precision_shadow"
        self.max_active_rules = max(1, min(64, int(max_active_rules)))
        self.refresh_seconds = max(0.0, float(refresh_seconds))
        self.max_prediction_lag_seconds = max(
            0.0, float(max_prediction_lag_seconds)
        )
        self._lock = threading.RLock()
        self._processing_lock = threading.RLock()
        self._phases: list[tuple[dict[str, Any], Any]] = []
        self._last_refresh_monotonic = 0.0
        self.retry_spool = retry_spool or RetrySpool(
            self.store.path + ".retry",
            max_bytes=self.store.max_live_retry_bytes,
            max_records=self.store.max_live_retry_records,
            max_record_bytes=self.store.max_live_retry_record_bytes,
        )

    def invalidate(self) -> None:
        with self._lock:
            self._last_refresh_monotonic = 0.0

    def refresh(
        self,
        *,
        at_utc: Any = None,
        force: bool = False,
    ) -> list[tuple[dict[str, Any], Any]]:
        now = time.monotonic()
        with self._lock:
            if (
                not force
                and self._last_refresh_monotonic > 0.0
                and now - self._last_refresh_monotonic < self.refresh_seconds
            ):
                return list(self._phases)
        rows = self.store.list_active_phases(at_utc=at_utc)
        priority = {
            "active": 0,
            "ready": 1,
            "shadow": 2,
            "candidate": 3,
            "degraded": 4,
        }
        rows.sort(
            key=lambda row: (
                priority.get(str(row.get("status") or ""), 9),
                str(row.get("created_at_utc") or ""),
                str(row.get("phase_id") or ""),
            )
        )
        parsed: list[tuple[dict[str, Any], Any]] = []
        for row in rows:
            if len(parsed) >= self.max_active_rules:
                break
            try:
                manifest = rule_manifest_from_dict(_mapping(row.get("manifest")))
            except (TypeError, ValueError):
                continue
            if str(row.get("rule_id") or "") != manifest.rule_id:
                continue
            # Live evaluation is stricter than historical replay: an unpinned
            # legacy extractor is not safe to execute against today's vector.
            if manifest.feature_schema_version != FEATURE_SCHEMA_VERSION:
                continue
            parsed.append((dict(row), manifest))
        with self._lock:
            self._phases = parsed
            self._last_refresh_monotonic = now
            return list(self._phases)

    def _process_snapshot_once(
        self,
        snapshot: Mapping[str, Any],
        *,
        static_prediction: Optional[Mapping[str, Any]] = None,
        rolling_prediction: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        if not isinstance(snapshot, Mapping):
            return {"status": "UNAVAILABLE", "reason": "snapshot_not_mapping"}
        universe = evaluate_universe(snapshot)
        if universe.get("status") != PASS:
            return {
                "status": str(universe.get("status") or "UNAVAILABLE"),
                "reason": str(universe.get("reason") or "universe"),
                "universe": universe,
                "evaluated_phases": 0,
                "claimed": [],
            }
        observation_id = _source_id(snapshot)
        fixture_id = snapshot.get("fixture_id")
        observed_at = snapshot.get("created_at_utc")
        minute = snapshot.get("minute")
        league = _league(snapshot)
        vector = extract_features(
            snapshot,
            static_prediction=static_prediction,
            rolling_prediction=rolling_prediction,
            max_prediction_lag_seconds=self.max_prediction_lag_seconds,
            include_extended=self.include_extended,
        )
        self.store.record_universe(
            fixture_id=fixture_id,
            observation_id=observation_id,
            observed_at_utc=observed_at,
            minute=minute,
            league=league,
            input_data={
                "stage": snapshot.get("stage"),
                "universe_id": universe.get("universe_id"),
                "universe_manifest_hash": universe.get(
                    "universe_manifest_hash"
                ),
                "available_feature_count": vector.get("available_count"),
                "contracts": dict(_mapping(vector.get("contracts"))),
            },
        )
        # Replay may span phase boundaries; a cache created for another
        # observation timestamp cannot decide historical eligibility.
        phases = self.refresh(at_utc=observed_at, force=True)
        claimed: list[dict[str, Any]] = []
        evaluations: list[dict[str, Any]] = []
        values = _mapping(vector.get("values"))
        for phase, manifest in phases:
            phase_universe = evaluate_universe(snapshot, manifest.universe)
            evaluation = evaluate_rule(
                manifest,
                vector,
                universe_evaluation=phase_universe,
            )
            evaluations.append(
                {
                    "phase_id": phase.get("phase_id"),
                    "rule_id": manifest.rule_id,
                    "status": evaluation.get("status"),
                    "reason": evaluation.get("reason"),
                }
            )
            if evaluation.get("status") != PASS:
                continue
            matched_values = {
                clause.feature: values.get(clause.feature)
                for clause in manifest.clauses
            }
            inserted = self.store.claim_first_trigger(
                phase_id=str(phase.get("phase_id") or ""),
                rule_id=manifest.rule_id,
                fixture_id=fixture_id,
                observation_id=observation_id,
                triggered_at_utc=observed_at,
                minute=minute,
                league=league,
                input_data={
                    "manifest_hash": manifest.manifest_hash,
                    "matched_values": matched_values,
                    "feature_contracts": dict(
                        _mapping(vector.get("contracts"))
                    ),
                    "stage": snapshot.get("stage"),
                },
            )
            if inserted:
                claimed.append(
                    {
                        "phase_id": phase.get("phase_id"),
                        "rule_id": manifest.rule_id,
                        "fixture_id": fixture_id,
                        "observation_id": observation_id,
                        "minute": minute,
                    }
                )
        return {
            "status": PASS,
            "reason": "pass",
            "universe": universe,
            "evaluated_phases": len(phases),
            "evaluations": evaluations,
            "claimed": claimed,
        }

    @staticmethod
    def _is_transient_sqlite_error(exc: BaseException) -> bool:
        if not isinstance(exc, sqlite3.OperationalError):
            return False
        message = str(exc).lower()
        return any(
            token in message
            for token in (
                "locked",
                "busy",
                "temporarily unavailable",
                "unable to open database file",
                "disk i/o error",
            )
        )

    def _retry_sqlite(self, operation: Any) -> Any:
        delays = (0.0, 0.025, 0.100)
        for attempt, delay in enumerate(delays):
            if delay:
                time.sleep(delay)
            try:
                return operation()
            except Exception as exc:
                if attempt + 1 >= len(delays) or not self._is_transient_sqlite_error(exc):
                    raise
        raise AssertionError("unreachable")

    def _drain_live_evaluations(
        self, *, target_observation_id: str, limit: int = 32
    ) -> dict[str, Any]:
        target_result: Optional[dict[str, Any]] = None
        deferred_result: Optional[dict[str, Any]] = None
        processed = 0
        rows = self._retry_sqlite(
            lambda: self.store.pending_live_evaluations(limit=limit)
        )
        for row in rows:
            observation_id = str(row.get("observation_id") or "")
            payload = _mapping(row.get("payload"))
            snapshot = _mapping(payload.get("snapshot"))
            static_prediction = _mapping(payload.get("static_prediction")) or None
            rolling_prediction = _mapping(payload.get("rolling_prediction")) or None
            try:
                result = self._retry_sqlite(
                    lambda: self._process_snapshot_once(
                        snapshot,
                        static_prediction=static_prediction,
                        rolling_prediction=rolling_prediction,
                    )
                )
                self._retry_sqlite(
                    lambda: self.store.acknowledge_live_evaluation(observation_id)
                )
            except Exception as exc:
                try:
                    self._retry_sqlite(
                        lambda: self.store.note_live_evaluation_failure(
                            observation_id, exc
                        )
                    )
                except Exception:
                    pass
                deferred_result = {
                    "status": "DEFERRED",
                    "reason": "live_evaluation_queued_for_retry",
                    "observation_id": observation_id,
                    "error_type": type(exc).__name__,
                    "evaluated_phases": 0,
                    "claimed": [],
                }
                if observation_id == target_observation_id:
                    return deferred_result
                # FIFO is deliberate: a later minute must never overtake an
                # earlier frozen snapshot for first-trigger ownership.
                break
            processed += 1
            if observation_id == target_observation_id:
                target_result = result
        if target_result is not None:
            return target_result
        if deferred_result is not None:
            return deferred_result
        if target_observation_id:
            return {
                "status": "DEFERRED",
                "reason": "earlier_live_evaluation_pending",
                "observation_id": target_observation_id,
                "evaluated_phases": 0,
                "claimed": [],
            }
        return {
            "status": "OK",
            "reason": "live_evaluation_queue_drained",
            "processed": processed,
            "evaluated_phases": 0,
            "claimed": [],
        }

    def process_snapshot(
        self,
        snapshot: Mapping[str, Any],
        *,
        static_prediction: Optional[Mapping[str, Any]] = None,
        rolling_prediction: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        """Queue frozen evidence, then replay it in causal FIFO order.

        The durable inbox makes every store operation idempotently retryable.
        A partially applied observation remains queued, so the next pass
        resumes all phases without changing the frozen input evidence.
        """

        if not isinstance(snapshot, Mapping):
            return {"status": "UNAVAILABLE", "reason": "snapshot_not_mapping"}
        universe = evaluate_universe(snapshot)
        if universe.get("status") != PASS:
            return self._process_snapshot_once(
                snapshot,
                static_prediction=static_prediction,
                rolling_prediction=rolling_prediction,
            )
        observation_id = _source_id(snapshot)
        observed_at = snapshot.get("created_at_utc")
        frozen = {
            "snapshot": dict(snapshot),
            "static_prediction": dict(static_prediction or {}),
            "rolling_prediction": dict(rolling_prediction or {}),
        }
        if not observation_id or _parse_utc(observed_at) is None:
            raise ValueError("live snapshot requires an identity and UTC timestamp")
        with self._processing_lock, self.retry_spool.locked():
            # The independent fsynced file closes the enqueue-failure hole.
            # Never return DEFERRED unless evidence is durable somewhere.
            self.retry_spool.enqueue("snapshot", observation_id, frozen)
            replay = self._replay_pending_locked(target_observation_id=observation_id)
            return replay.get("snapshot_result") or {
                "status": "DEFERRED", "reason": "durable_live_retry_pending",
                "observation_id": observation_id,
                "error_type": replay.get("error_type"),
                "evaluated_phases": 0, "claimed": [],
            }

    def pending_retry_status(self) -> dict[str, Any]:
        """Bounded health summary, with an explicit error if SQLite is down."""
        spool = self.retry_spool.status()
        result = {"spool": spool, "pending": int(spool["pending"]),
                  "oldest_at_utc": spool.get("oldest_at_utc")}
        try:
            inbox = self.store.live_retry_status()
        except Exception as exc:
            result.update(status="UNAVAILABLE", error_type=type(exc).__name__)
            return result
        result["inbox"] = inbox
        result["pending"] += int(inbox["pending"])
        timestamps = [_parse_utc(value) for value in
                      (spool.get("oldest_at_utc"), inbox.get("oldest_at_utc"))]
        timestamps = [value for value in timestamps if value is not None]
        result["oldest_at_utc"] = min(timestamps).isoformat() if timestamps else None
        result["status"] = "PENDING" if result["pending"] else "OK"
        return result

    def replay_pending(self, *, limit: int = 32) -> dict[str, Any]:
        """Periodic/restart hook; no new live match is required for recovery."""
        with self._processing_lock, self.retry_spool.locked():
            return self._replay_pending_locked(limit=limit)

    def _replay_pending_locked(
        self, *, target_observation_id: str = "", limit: int = 32,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "status": "OK", "imported_snapshots": 0, "outcome_batches": 0,
            "inserted": 0, "duplicates": 0, "updated_triggers": 0,
            "unmatched": 0, "ignored_unmatched": 0,
        }
        try:
            for name in self.retry_spool.pending("snapshot", limit=limit):
                payload = self.retry_spool.read(name)
                snapshot = _mapping(payload.get("snapshot"))
                self._retry_sqlite(lambda: self.store.enqueue_live_evaluation(
                    observation_id=_source_id(snapshot),
                    observed_at_utc=snapshot.get("created_at_utc"), payload=payload))
                self.retry_spool.acknowledge(name)
                result["imported_snapshots"] += 1
            if self.retry_spool.pending("snapshot", limit=1):
                # A not-yet-imported snapshot may precede every inbox row.
                result.update(status="DEFERRED", reason="snapshot_spool_ingestion_pending")
                return result
            result["snapshot_result"] = self._drain_live_evaluations(
                target_observation_id=target_observation_id, limit=limit)
            for name in self.retry_spool.pending("outcomes", limit=limit):
                payload = self.retry_spool.read(name)
                normalized = payload["outcomes"]
                # An earlier queued snapshot can still claim a trigger.
                # Store its outcome now; claim_first_trigger attaches it later.
                matched = self._retry_sqlite(lambda: self.store.matching_trigger_observation_ids(
                    (row.get("observation_id") for row in normalized), include_queued=True))
                selected = [row for row in normalized if str(row.get("observation_id")) in matched]
                attached = self._retry_sqlite(lambda: self.store.attach_outcomes(selected))
                self.retry_spool.acknowledge(name)
                for key, value in attached.items():
                    result[key] += int(value)
                result["ignored_unmatched"] += len(normalized) - len(selected)
                result["outcome_batches"] += 1
            pending = self.pending_retry_status()
            result["pending"] = pending["pending"]
            if pending["status"] != "OK":
                result["status"] = "DEFERRED"
        except Exception as exc:
            # The file/inbox stays in place, including on a non-transient
            # corruption/size-limit error. Health exposes a stalled queue.
            result.update(status="DEFERRED", error_type=type(exc).__name__)
        return result

    @staticmethod
    def _normalize_live_outcome(record: Mapping[str, Any]) -> dict[str, Any]:
        outcome = _mapping(record.get("outcome"))
        raw_status = str(outcome.get("status") or "").strip().lower()
        label = outcome.get("goal_to90_normal_time")
        if raw_status == "resolved" and (label is True or label == 1):
            status = "win"
        elif raw_status == "resolved" and (label is False or label == 0):
            status = "loss"
        elif raw_status in {"void", "invalid", "cancelled", "canceled"}:
            status = "invalid"
        elif raw_status in {"pending", ""}:
            status = "pending"
        else:
            status = "invalid"
        return {
            "observation_id": record.get("observation_id")
            or record.get("observation_key"),
            "outcome_version": outcome_store_version(record),
            "outcome_status": status,
            "outcome_label": 1 if status == "win" else 0 if status == "loss" else None,
            "outcome_at_utc": outcome.get("resolved_at_utc")
            or record.get("created_at_utc"),
            "payload": dict(outcome),
        }

    def process_outcomes(
        self, outcomes: Sequence[Mapping[str, Any]]
    ) -> dict[str, Any]:
        normalized = [
            self._normalize_live_outcome(record)
            for record in outcomes
            if isinstance(record, Mapping)
        ]
        # Validate and size the complete intake before the first durable write;
        # one poison/oversized row must not cause a partially accepted call.
        for row in normalized:
            self.store._normalize_outcome(row)
        batches = list(self._outcome_retry_batches(normalized))
        with self._processing_lock, self.retry_spool.locked():
            durable_events: list[tuple[str, str, Mapping[str, Any]]] = []
            for batch in batches:
                payload = {"outcomes": batch}
                identity = hashlib.sha256(json.dumps(
                    payload, sort_keys=True, separators=(",", ":"),
                    ensure_ascii=False, allow_nan=False).encode()).hexdigest()
                durable_events.append(("outcomes", identity, payload))
            # Preflight aggregate capacity so a normal capacity error cannot
            # accept only the first half of one caller's outcome intake.
            self.retry_spool.enqueue_many(durable_events)
            return self._replay_pending_locked()

    def _outcome_retry_batches(
        self, normalized: Sequence[Mapping[str, Any]]
    ) -> Iterator[list[Mapping[str, Any]]]:
        """Bound batches by both count and the actual durable JSON size."""

        current: list[Mapping[str, Any]] = []
        for row in normalized:
            candidate = [*current, row]
            payload = {"outcomes": candidate}
            encoded_size = len(
                json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8")
            )
            if current and (
                len(candidate) > 128
                or encoded_size > self.retry_spool.max_record_bytes
            ):
                yield current
                current = [row]
                payload = {"outcomes": current}
                encoded_size = len(
                    json.dumps(
                        payload,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                        allow_nan=False,
                    ).encode("utf-8")
                )
            else:
                current = candidate
            if encoded_size > self.retry_spool.max_record_bytes:
                raise RetrySpoolError(
                    "single outcome exceeds retry record size limit"
                )
        if current:
            yield current


class ActiveRuleRouter:
    """Apply only a valid, explicitly production-enabled champion manifest."""

    def __init__(self, manifest_path: str) -> None:
        self.cache = ActiveManifestCache(manifest_path)

    def route(
        self,
        snapshot: Mapping[str, Any],
        *,
        current_filter_allow: bool,
        static_prediction: Optional[Mapping[str, Any]] = None,
        rolling_prediction: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        fallback = {
            "applied": False,
            "allow": bool(current_filter_allow),
            "source": "current_filter",
            "reason": "no_active_production_rule",
            "rule_id": None,
            "phase_id": None,
        }
        active = self.cache.load()
        if not active or active.get("production_enabled") is not True:
            return fallback
        rule_payload = active.get("rule")
        if not isinstance(rule_payload, Mapping):
            return fallback
        effective = _parse_utc(active.get("effective_from_utc"))
        observed = _parse_utc(snapshot.get("created_at_utc"))
        if effective is None or observed is None or observed < effective:
            return fallback
        try:
            manifest_payload = rule_payload.get("manifest")
            manifest = rule_manifest_from_dict(
                manifest_payload
                if isinstance(manifest_payload, Mapping)
                else rule_payload
            )
            if manifest.feature_schema_version != FEATURE_SCHEMA_VERSION:
                return {
                    **fallback,
                    "reason": "incompatible_active_rule_feature_schema",
                }
            evaluation = evaluate_snapshot(
                manifest,
                snapshot,
                static_prediction=static_prediction,
                rolling_prediction=rolling_prediction,
            )
        except (TypeError, ValueError):
            return fallback
        if evaluation.get("reason") == "feature_schema_version_mismatch":
            return {
                **fallback,
                "reason": "incompatible_active_rule_feature_schema",
            }
        return {
            "applied": True,
            "allow": evaluation.get("status") == PASS,
            "source": "wide_research_champion",
            "reason": evaluation.get("reason"),
            "status": evaluation.get("status"),
            "rule_id": manifest.rule_id,
            "phase_id": rule_payload.get("phase_id"),
            "generation": active.get("generation"),
            "manifest_hash": manifest.manifest_hash,
        }


__all__ = ["ActiveRuleRouter", "WideShadowLayer"]
