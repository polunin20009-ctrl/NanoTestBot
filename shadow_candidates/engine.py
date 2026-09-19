from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Union

from outcome_revision import outcome_revision as get_outcome_revision

from .rules import DEFAULT_RULESET, RuleSet, evaluate_candidate_arms
from .storage import AppendOnlyCandidateJournal


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _aware_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.utcoffset() is not None else None


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _digest(value: Any) -> str:
    encoded = json.dumps(
        _json_safe(value),
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:24]


def source_observation_id(record: Mapping[str, Any]) -> str:
    """Normalize observations and decision snapshots onto one source identity."""
    return str(record.get("observation_id") or record.get("decision_id") or "").strip()


def build_prediction_index(
    records: Iterable[Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Select the first recorded prediction per observation.

    Choosing the first append prevents a later retrained model from silently
    rescoring an old prospective snapshot.
    """
    selected: Dict[str, Dict[str, Any]] = {}
    selected_times: Dict[str, tuple[int, str]] = {}
    for record in records:
        if not isinstance(record, Mapping):
            continue
        observation_id = source_observation_id(record)
        if not observation_id:
            continue
        created = str(record.get("created_at_utc") or "")
        valid_time = 0 if _aware_datetime(created) is not None else 1
        rank = (valid_time, created)
        if observation_id not in selected or rank < selected_times[observation_id]:
            selected[observation_id] = dict(record)
            selected_times[observation_id] = rank
    return selected


class CandidateLayer:
    """Evaluate and journal prospective candidate arms without production effects."""

    def __init__(
        self,
        journal: AppendOnlyCandidateJournal,
        *,
        prospective_start_utc: Union[str, datetime],
        ruleset: RuleSet = DEFAULT_RULESET,
        settle_seconds: float = 0.0,
        max_live_prediction_lag_seconds: float = 300.0,
        now_factory: Callable[[], datetime] = _now_utc,
    ) -> None:
        self.journal = journal
        self.ruleset = ruleset
        if isinstance(prospective_start_utc, datetime):
            parsed_start = prospective_start_utc
        else:
            parsed_start = _aware_datetime(prospective_start_utc)
        if parsed_start is None or parsed_start.utcoffset() is None:
            raise ValueError("prospective_start_utc must be an aware ISO timestamp")
        self.prospective_start = parsed_start.astimezone(timezone.utc)
        self.prospective_start_utc = self.prospective_start.isoformat()
        self.settle_seconds = max(0.0, float(settle_seconds))
        self.max_live_prediction_lag_seconds = max(
            0.0, float(max_live_prediction_lag_seconds)
        )
        self.now_factory = now_factory

    def _is_settled(self, snapshot: Mapping[str, Any]) -> bool:
        created_at = _aware_datetime(snapshot.get("created_at_utc"))
        now = self.now_factory()
        if created_at is None or now.utcoffset() is None:
            return False
        try:
            age = (now - created_at).total_seconds()
        except TypeError:
            return False
        return age >= self.settle_seconds

    def _is_prospective(self, snapshot: Mapping[str, Any]) -> bool:
        created_at = _aware_datetime(snapshot.get("created_at_utc"))
        if created_at is None:
            return False
        return created_at.astimezone(timezone.utc) >= self.prospective_start

    @staticmethod
    def _valid_snapshot(snapshot: Mapping[str, Any]) -> bool:
        source_id = source_observation_id(snapshot)
        record_type = str(snapshot.get("record_type") or "")
        fixture_id = snapshot.get("fixture_id")
        minute = snapshot.get("minute")
        return bool(
            source_id
            and record_type in {"observation", "decision"}
            and fixture_id is not None
            and not isinstance(fixture_id, bool)
            and minute is not None
            and not isinstance(minute, bool)
        )

    def process_snapshot(
        self,
        snapshot: Mapping[str, Any],
        *,
        static_prediction: Optional[Mapping[str, Any]] = None,
        rolling_prediction: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Append one complete all-arm audit after the live-settlement grace."""
        if not isinstance(snapshot, Mapping) or not self._valid_snapshot(snapshot):
            return None
        if not self._is_prospective(snapshot) or not self._is_settled(snapshot):
            return None

        source_id = source_observation_id(snapshot)
        fixture_id = snapshot.get("fixture_id")
        evaluations = evaluate_candidate_arms(
            snapshot,
            static_prediction=static_prediction,
            rolling_prediction=rolling_prediction,
            ruleset=self.ruleset,
            max_live_prediction_lag_seconds=(
                self.max_live_prediction_lag_seconds
            ),
        )
        manifest = self.ruleset.manifest()
        input_fingerprint = _digest(
            {
                "ruleset_manifest_hash": manifest["manifest_hash"],
                "source_id": source_id,
                "source_created_at_utc": snapshot.get("created_at_utc"),
                "evaluations": evaluations,
            }
        )
        event_id = (
            f"shadow_candidate_decision:{self.ruleset.version}:"
            f"{source_id}:{input_fingerprint}"
        )
        now = self.now_factory().astimezone(timezone.utc).isoformat()
        match = snapshot.get("match")
        match_map = match if isinstance(match, Mapping) else {}
        base_record: Dict[str, Any] = {
            "record_type": "shadow_candidate_decision",
            "schema_version": 1,
            "event_id": event_id,
            "candidate_key": f"{self.ruleset.version}:{source_id}",
            "ruleset_version": self.ruleset.version,
            "ruleset_manifest_hash": manifest["manifest_hash"],
            "evaluation_semantics": "candidate_at_first_control_allow",
            "prospective_start_utc": self.prospective_start_utc,
            "observation_id": source_id,
            "fixture_id": fixture_id,
            "minute": snapshot.get("minute"),
            "created_at_utc": now,
            "source": {
                "record_type": snapshot.get("record_type"),
                "observation_id": source_id,
                "source_schema_version": snapshot.get("schema_version"),
                "created_at_utc": snapshot.get("created_at_utc"),
                "stage": snapshot.get("stage"),
                "window_name": snapshot.get("window_name"),
            },
            "match": {
                "home_team_name": match_map.get("home_team_name"),
                "away_team_name": match_map.get("away_team_name"),
                "league_name": match_map.get("league_name"),
                "score_home": match_map.get("score_home"),
                "score_away": match_map.get("score_away"),
            },
            "inputs_fingerprint": input_fingerprint,
            "ruleset": manifest,
            "arms": _json_safe(evaluations),
            "shadow_only": True,
            "production_applied": False,
            "production_effect": "none",
            "outcome": {
                "status": "pending",
                "scope": "TO_90_NORMAL_TIME",
            },
        }
        appended, written = self.journal.append_evaluation(base_record)
        return written if appended else None

    def _materialize_outcome_records(
        self,
        source_outcome: Mapping[str, Any],
        triggers: list[Mapping[str, Any]],
    ) -> list[Dict[str, Any]]:
        observation_id = source_observation_id(source_outcome)
        outcome = source_outcome.get("outcome")
        outcome_map = outcome if isinstance(outcome, Mapping) else {}
        status = str(outcome_map.get("status") or "").lower()

        scope = str(
            outcome_map.get("outcome_scope")
            or source_outcome.get("outcome_scope")
            or ""
        )
        label = outcome_map.get("goal_to90_normal_time")
        if not isinstance(label, bool) and scope == "TO_90_NORMAL_TIME":
            fallback = outcome_map.get("goal_to90")
            label = fallback if isinstance(fallback, bool) else None
        integrity_conflict = outcome_map.get("outcome_integrity_conflict") is True
        if status == "void":
            label = None
            result = "VOID"
        elif status == "quarantine":
            # A corrected invalid outcome must supersede an earlier win/loss.
            label = None
            result = "UNAVAILABLE"
        elif integrity_conflict:
            label = None
            result = "UNAVAILABLE"
        elif label is True:
            result = "WIN"
        elif label is False:
            result = "LOSS"
        else:
            result = "UNAVAILABLE"

        source_schema = source_outcome.get("outcome_schema_version")
        source_revision = get_outcome_revision(source_outcome)
        compact_outcome = {
            "status": status,
            "scope": scope or None,
            "goal_to90_normal_time": label if isinstance(label, bool) else None,
            "result": result,
            "goals_after_snapshot": outcome_map.get("goals_after_snapshot"),
            "first_goal_minute_after_snapshot": outcome_map.get(
                "first_goal_minute_after_snapshot"
            ),
            "normal_time_result": outcome_map.get("normal_time_result"),
            "goal_result_source": outcome_map.get("goal_result_source"),
            "outcome_integrity_conflict": integrity_conflict,
            "resolved_at_utc": outcome_map.get("resolved_at_utc"),
        }
        outcome_fingerprint = _digest(
            {
                "observation_id": observation_id,
                "source_outcome_schema_version": source_schema,
                "source_outcome_revision": source_revision,
                "source_created_at_utc": source_outcome.get("created_at_utc"),
                "outcome": compact_outcome,
            }
        )
        materialized: list[Dict[str, Any]] = []
        for trigger in triggers:
            arm_id = str(trigger.get("arm_id") or "")
            if not arm_id:
                continue
            event_id = (
                f"shadow_candidate_outcome:{self.ruleset.version}:"
                f"{observation_id}:{arm_id}:{outcome_fingerprint}"
            )
            record: Dict[str, Any] = {
                "record_type": "shadow_candidate_outcome",
                "schema_version": 1,
                "event_id": event_id,
                "ruleset_version": self.ruleset.version,
                "evaluation_semantics": "candidate_at_first_control_allow",
                "prospective_start_utc": self.prospective_start_utc,
                "observation_id": observation_id,
                "fixture_id": trigger.get("fixture_id"),
                "arm_id": arm_id,
                "candidate_decision_event_id": trigger.get("decision_event_id"),
                "created_at_utc": self.now_factory().astimezone(
                    timezone.utc
                ).isoformat(),
                "source_outcome_schema_version": source_schema,
                "source_outcome_revision": source_revision,
                "source_outcome_created_at_utc": source_outcome.get(
                    "created_at_utc"
                ),
                "outcome": compact_outcome,
                "shadow_only": True,
                "production_applied": False,
                "production_effect": "none",
            }
            materialized.append(record)
        return materialized

    def process_outcomes(
        self, source_outcomes: Iterable[Mapping[str, Any]]
    ) -> list[Dict[str, Any]]:
        """Batch-resolve a fixture's source outcomes with one trigger lookup."""
        terminal_by_observation: Dict[str, Mapping[str, Any]] = {}

        def rank(record: Mapping[str, Any]) -> tuple[int, int, str]:
            value = record.get("outcome_schema_version")
            if isinstance(value, bool):
                schema = -1
            else:
                try:
                    schema = int(value)
                except (TypeError, ValueError):
                    schema = -1
            return (
                schema,
                get_outcome_revision(record),
                str(record.get("created_at_utc") or ""),
            )

        for source_outcome in source_outcomes:
            if not isinstance(source_outcome, Mapping):
                continue
            observation_id = source_observation_id(source_outcome)
            outcome = source_outcome.get("outcome")
            outcome_map = outcome if isinstance(outcome, Mapping) else {}
            status = str(outcome_map.get("status") or "").lower()
            if not observation_id or status not in {"resolved", "void", "quarantine"}:
                continue
            previous = terminal_by_observation.get(observation_id)
            if previous is None or rank(source_outcome) > rank(previous):
                terminal_by_observation[observation_id] = source_outcome
        if not terminal_by_observation:
            return []
        trigger_map = self.journal.trigger_rows_for_observations(
            self.ruleset.version, list(terminal_by_observation)
        )
        written: list[Dict[str, Any]] = []
        for source_outcome in terminal_by_observation.values():
            triggers = trigger_map.get(source_observation_id(source_outcome), [])
            for record in self._materialize_outcome_records(
                source_outcome, triggers
            ):
                if self.journal.append(record):
                    written.append(record)
        return written

    def process_outcome(
        self, source_outcome: Mapping[str, Any]
    ) -> list[Dict[str, Any]]:
        """Compatibility wrapper for a single terminal source update."""
        return self.process_outcomes([source_outcome])
