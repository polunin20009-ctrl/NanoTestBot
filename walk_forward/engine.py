"""Leakage-resistant offline evaluation of fixed signal-selection arms.

The module is deliberately disconnected from Telegram and the live bot state.
It can either train fresh fold-local shadow models at every Moscow-day cutoff,
or replay immutable prediction journals under strict prospective timing checks.
Outcomes are used only after a trigger has been frozen.
"""

from __future__ import annotations

import gzip
import json
import math
import os
import re
import sqlite3
import tempfile
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from shadow_ml import (
    ShadowMLConfig,
    predict_shadow,
    predict_shadow_rolling,
    train_shadow_model,
    train_shadow_rolling_model,
)
from shadow_ml import model as shadow_model
from shadow_candidates.rules import (
    DEFAULT_RULESET as LIVE_CANDIDATE_RULESET,
    evaluate_candidate_arms,
)


REPORT_SCHEMA_VERSION = 1
ENGINE_VERSION = "walk_forward_daily_v1"
MOSCOW = ZoneInfo("Europe/Moscow")
UTC = timezone.utc
ARM_CONTROL = "control"
ARM_BOTH_WINDOWS = "both_windows"
ARM_BOTH_WINDOWS_GOALS_LE2 = "both_windows_goals_le2"
ARM_BOTH_WINDOWS_GOALS_LE2_CLOSE = "both_windows_goals_le2_close"
ARM_ML_CONFIRM = "ml_confirm"
ARM_ORDER = (
    ARM_CONTROL,
    ARM_BOTH_WINDOWS,
    ARM_BOTH_WINDOWS_GOALS_LE2,
    ARM_BOTH_WINDOWS_GOALS_LE2_CLOSE,
    ARM_ML_CONFIRM,
)
ML_MODES = {"fold-local", "journal-replay", "disabled"}
ML_SOURCES = {"rolling", "static", "consensus"}
JOURNAL_TIMING_POLICIES = {"day-frozen", "prospective"}
TRIGGER_POLICIES = {"first-control", "independent-exploratory"}
CONTROL_CONTRACT_MODES = {"recorded-production", "recomputed-historical"}
_ARCHIVE_STAMP = re.compile(r"^\d{8}T\d{6}(?:\d{6})?Z$")
_WILSON_Z_95 = 1.959963984540054


@dataclass(frozen=True)
class EvaluationConfig:
    """Pre-registered thresholds and temporal evaluation policy."""

    timezone_name: str = "Europe/Moscow"
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    contract_key: Optional[str] = None
    min_minute: int = 46
    max_minute: int = 60
    min_prob_to90: float = 75.0
    min_reputation_delta_to90_pp: float = 1.5
    min_adjusted_intensity: float = 0.55
    min_season_context_factor: float = 1.02
    max_goals: int = 2
    max_score_difference_abs: int = 1
    ml_mode: str = "fold-local"
    ml_source: str = "rolling"
    ml_confirm_threshold_pct: float = 75.0
    journal_timing_policy: str = "day-frozen"
    max_prediction_lag_seconds: float = 300.0
    trigger_policy: str = "first-control"
    control_contract_mode: str = "recorded-production"

    def __post_init__(self) -> None:
        ZoneInfo(self.timezone_name)
        if self.from_date is not None:
            date.fromisoformat(self.from_date)
        if self.to_date is not None:
            date.fromisoformat(self.to_date)
        if self.from_date and self.to_date and self.from_date > self.to_date:
            raise ValueError("from_date cannot be later than to_date")
        if not 0 < int(self.min_minute) <= int(self.max_minute):
            raise ValueError("invalid minute window")
        for name, value in (
            ("min_prob_to90", self.min_prob_to90),
            ("min_reputation_delta_to90_pp", self.min_reputation_delta_to90_pp),
            ("min_adjusted_intensity", self.min_adjusted_intensity),
            ("min_season_context_factor", self.min_season_context_factor),
            ("ml_confirm_threshold_pct", self.ml_confirm_threshold_pct),
            ("max_prediction_lag_seconds", self.max_prediction_lag_seconds),
        ):
            if not math.isfinite(float(value)):
                raise ValueError(f"{name} must be finite")
        if not 0.0 <= float(self.min_prob_to90) <= 100.0:
            raise ValueError("min_prob_to90 must be in [0, 100]")
        if not 0.0 <= float(self.ml_confirm_threshold_pct) <= 100.0:
            raise ValueError("ml_confirm_threshold_pct must be in [0, 100]")
        if int(self.max_goals) < 0:
            raise ValueError("max_goals must be non-negative")
        if int(self.max_score_difference_abs) < 0:
            raise ValueError("max_score_difference_abs must be non-negative")
        if self.ml_mode not in ML_MODES:
            raise ValueError(f"ml_mode must be one of {sorted(ML_MODES)}")
        if self.ml_source not in ML_SOURCES:
            raise ValueError(f"ml_source must be one of {sorted(ML_SOURCES)}")
        if self.journal_timing_policy not in JOURNAL_TIMING_POLICIES:
            raise ValueError(
                "journal_timing_policy must be one of "
                f"{sorted(JOURNAL_TIMING_POLICIES)}"
            )
        if float(self.max_prediction_lag_seconds) < 0.0:
            raise ValueError("max_prediction_lag_seconds must be non-negative")
        if self.trigger_policy not in TRIGGER_POLICIES:
            raise ValueError(
                f"trigger_policy must be one of {sorted(TRIGGER_POLICIES)}"
            )
        if self.control_contract_mode not in CONTROL_CONTRACT_MODES:
            raise ValueError(
                "control_contract_mode must be one of "
                f"{sorted(CONTROL_CONTRACT_MODES)}"
            )


def _parse_utc(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    else:
        text_value = str(value or "").strip()
        if not text_value:
            return None
        if text_value.endswith("Z"):
            text_value = text_value[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text_value)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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
    if number is None or not number.is_integer():
        return None
    try:
        return int(number)
    except (OverflowError, ValueError):
        return None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _probability_pct(value: Any) -> Optional[float]:
    number = _finite_float(value)
    if number is None or not 0.0 <= number <= 100.0:
        return None
    return number


def _label(value: Any) -> Optional[int]:
    if value is True or value == 1:
        return 1
    if value is False or value == 0:
        return 0
    return None


def _local_day(value: datetime, tz: ZoneInfo) -> date:
    return value.astimezone(tz).date()


def _day_cutoff(day: date, tz: ZoneInfo) -> datetime:
    return datetime.combine(day, time.min, tzinfo=tz).astimezone(UTC)


def discover_journal_paths(active_path: os.PathLike[str] | str) -> list[Path]:
    """Return exact timestamp archives oldest-first and the active file last."""

    active = Path(active_path).expanduser().resolve()
    parent = active.parent
    filename = active.name
    if filename.endswith(".jsonl"):
        stem = filename[:-len(".jsonl")]
    else:
        stem = active.stem
    archives: list[tuple[str, Path]] = []
    try:
        candidates = list(parent.iterdir())
    except OSError:
        candidates = []
    prefix = stem + "."
    for candidate in candidates:
        name = candidate.name
        if name == filename or not name.startswith(prefix):
            continue
        if name.endswith(".jsonl.gz"):
            stamp = name[len(prefix):-len(".jsonl.gz")]
        elif name.endswith(".jsonl"):
            stamp = name[len(prefix):-len(".jsonl")]
        else:
            continue
        if _ARCHIVE_STAMP.fullmatch(stamp):
            archives.append((stamp, candidate.resolve()))
    result = [path for _, path in sorted(archives)]
    if active.exists():
        result.append(active)
    return result


def _stream_journal(
    active_path: os.PathLike[str] | str,
    diagnostics: dict[str, Any],
) -> Iterator[dict[str, Any]]:
    """Stream a journal without retaining its raw records in memory."""

    files = discover_journal_paths(active_path)
    diagnostics.update(
        {
            "active_path": str(Path(active_path).expanduser().resolve()),
            "files": [],
            "records": 0,
            "lines": 0,
            "invalid_json_lines": 0,
            "non_object_lines": 0,
            "read_errors": 0,
            "snapshot_consistent": True,
        }
    )
    for source in files:
        before = None
        try:
            stat = source.stat()
            before = (stat.st_ino, stat.st_size, stat.st_mtime_ns)
        except OSError:
            pass
        opener: Callable[..., Any] = gzip.open if source.suffix == ".gz" else open
        file_lines = 0
        try:
            with opener(source, "rt", encoding="utf-8") as handle:
                for line in handle:
                    diagnostics["lines"] += 1
                    file_lines += 1
                    try:
                        payload = json.loads(line)
                    except (TypeError, ValueError):
                        diagnostics["invalid_json_lines"] += 1
                        continue
                    if not isinstance(payload, dict):
                        diagnostics["non_object_lines"] += 1
                        continue
                    diagnostics["records"] += 1
                    yield payload
        except (OSError, UnicodeError):
            diagnostics["read_errors"] += 1
        after = None
        try:
            stat = source.stat()
            after = (stat.st_ino, stat.st_size, stat.st_mtime_ns)
        except OSError:
            pass
        changed = before is not None and before != after
        diagnostics["files"].append(
            {
                "path": str(source),
                "lines": file_lines,
                "changed_while_reading": changed,
            }
        )
        if changed:
            diagnostics["snapshot_consistent"] = False


class _ObservationStore:
    """Ephemeral canonical observation/outcome join backed by SQLite."""

    def __init__(self) -> None:
        descriptor, name = tempfile.mkstemp(
            prefix="goalbot-walk-forward-", suffix=".sqlite3"
        )
        os.close(descriptor)
        self.path = Path(name)
        self.connection = sqlite3.connect(str(self.path))
        self.connection.execute("PRAGMA journal_mode=OFF")
        self.connection.execute("PRAGMA synchronous=OFF")
        self.connection.execute("PRAGMA temp_store=FILE")
        self.connection.execute("PRAGMA cache_size=-32768")
        self.connection.execute("PRAGMA mmap_size=0")
        self.connection.executescript(
            """
            CREATE TABLE observations (
                observation_id TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                created_at_utc TEXT NOT NULL,
                created_epoch REAL,
                fixture_id INTEGER,
                minute INTEGER,
                stage TEXT,
                contract_key TEXT,
                contract_payload_json TEXT,
                payload_json TEXT NOT NULL
            );
            CREATE TABLE outcomes (
                observation_id TEXT PRIMARY KEY,
                outcome_schema_version INTEGER NOT NULL,
                created_at_utc TEXT NOT NULL,
                resolved_epoch REAL,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX observations_candidate_idx
                ON observations(stage, minute, contract_key, created_epoch);
            CREATE INDEX observations_fixture_idx ON observations(fixture_id);
            CREATE INDEX outcomes_resolved_idx ON outcomes(resolved_epoch);
            """
        )
        self.counts = Counter()
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.connection.close()
        finally:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass

    def __del__(self) -> None:
        self.close()

    @staticmethod
    def _json(payload: Mapping[str, Any]) -> str:
        return json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    def ingest(
        self,
        records: Iterable[Mapping[str, Any]],
        *,
        config: EvaluationConfig,
    ) -> None:
        total_observation_records = 0
        candidate_observation_records = 0
        outcome_records = 0
        with self.connection:
            for record in records:
                observation_id = _record_id(record)
                if not observation_id:
                    self.counts["missing_observation_id"] += 1
                    continue
                record_type = str(record.get("record_type") or "")
                if record_type == "observation":
                    total_observation_records += 1
                    minute = _safe_int(record.get("minute"))
                    if str(record.get("stage") or "") != "decision_pipeline":
                        self.counts["non_candidate_observation_stage"] += 1
                        continue
                    if (
                        minute is None
                        or not config.min_minute <= minute <= config.max_minute
                    ):
                        self.counts["non_candidate_observation_minute"] += 1
                        continue
                    candidate_observation_records += 1
                    try:
                        payload_json = self._json(record)
                    except (TypeError, ValueError, OverflowError):
                        self.counts["non_json_payload"] += 1
                        continue
                    created_at = _parse_utc(record.get("created_at_utc"))
                    contract = _feature_contract(record)
                    try:
                        contract_payload_json = self._json(
                            _mapping(contract.get("payload"))
                        )
                    except (TypeError, ValueError, OverflowError):
                        contract_payload_json = "{}"
                    self.connection.execute(
                        """
                        INSERT INTO observations(
                            observation_id, schema_version, created_at_utc,
                            created_epoch, fixture_id, minute, stage,
                            contract_key, contract_payload_json, payload_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(observation_id) DO UPDATE SET
                            schema_version=excluded.schema_version,
                            created_at_utc=excluded.created_at_utc,
                            created_epoch=excluded.created_epoch,
                            fixture_id=excluded.fixture_id,
                            minute=excluded.minute,
                            stage=excluded.stage,
                            contract_key=excluded.contract_key,
                            contract_payload_json=excluded.contract_payload_json,
                            payload_json=excluded.payload_json
                        WHERE excluded.schema_version > observations.schema_version
                           OR (
                                excluded.schema_version = observations.schema_version
                                AND excluded.created_at_utc >= observations.created_at_utc
                           )
                        """,
                        (
                            observation_id,
                            _safe_int(record.get("schema_version")) or 0,
                            str(record.get("created_at_utc") or ""),
                            created_at.timestamp() if created_at else None,
                            _safe_int(record.get("fixture_id")),
                            minute,
                            str(record.get("stage") or ""),
                            str(contract.get("key") or ""),
                            contract_payload_json,
                            payload_json,
                        ),
                    )
                elif record_type == "observation_outcome":
                    outcome_records += 1
                    outcome = _mapping(record.get("outcome"))
                    try:
                        # Only the immutable label block is needed for joining;
                        # retaining the full outcome envelope would waste disk.
                        payload_json = self._json(outcome)
                    except (TypeError, ValueError, OverflowError):
                        self.counts["non_json_payload"] += 1
                        continue
                    resolved_at = _parse_utc(
                        outcome.get("resolved_at_utc")
                        or record.get("created_at_utc")
                    )
                    self.connection.execute(
                        """
                        INSERT INTO outcomes(
                            observation_id, outcome_schema_version,
                            created_at_utc, resolved_epoch, payload_json
                        ) VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(observation_id) DO UPDATE SET
                            outcome_schema_version=excluded.outcome_schema_version,
                            created_at_utc=excluded.created_at_utc,
                            resolved_epoch=excluded.resolved_epoch,
                            payload_json=excluded.payload_json
                        WHERE excluded.outcome_schema_version > outcomes.outcome_schema_version
                           OR (
                                excluded.outcome_schema_version = outcomes.outcome_schema_version
                                AND excluded.created_at_utc >= outcomes.created_at_utc
                           )
                        """,
                        (
                            observation_id,
                            _safe_int(record.get("outcome_schema_version")) or 0,
                            str(record.get("created_at_utc") or ""),
                            resolved_at.timestamp() if resolved_at else None,
                            payload_json,
                        ),
                    )
                else:
                    self.counts["other_record_type"] += 1
        unique_observations = int(
            self.connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        )
        unique_outcomes = int(
            self.connection.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0]
        )
        self.counts.update(
            {
                "total_observation_records": total_observation_records,
                "candidate_observation_records": candidate_observation_records,
                "outcome_records": outcome_records,
                "observations": unique_observations,
                "outcomes": unique_outcomes,
                "duplicate_observations": (
                    candidate_observation_records - unique_observations
                ),
                "duplicate_outcomes": outcome_records - unique_outcomes,
            }
        )

    def join_diagnostics(self) -> dict[str, Any]:
        joined = int(
            self.connection.execute(
                """
                SELECT COUNT(*)
                FROM observations AS o
                INNER JOIN outcomes AS x USING(observation_id)
                """
            ).fetchone()[0]
        )
        return {
            "observations": int(self.counts["observations"]),
            "outcomes": int(self.counts["outcomes"]),
            "joined": joined,
            "observations_without_outcome": int(
                self.counts["observations"]
            )
            - joined,
            "total_observation_records": int(
                self.counts["total_observation_records"]
            ),
            "stored_candidate_observation_records": int(
                self.counts["candidate_observation_records"]
            ),
            "stored_candidate_observations": int(self.counts["observations"]),
            "total_outcome_records": int(self.counts["outcome_records"]),
            "duplicates": {
                "observation": int(self.counts["duplicate_observations"]),
                "outcome": int(self.counts["duplicate_outcomes"]),
            },
            "skipped": {
                key: int(value)
                for key, value in self.counts.items()
                if key
                in {
                    "missing_observation_id",
                    "non_json_payload",
                    "other_record_type",
                    "non_candidate_observation_stage",
                    "non_candidate_observation_minute",
                }
            },
            "canonical_store": "ephemeral_sqlite",
        }

    def select_contract(
        self,
        config: EvaluationConfig,
    ) -> tuple[str, Mapping[str, Any], dict[str, Any]]:
        tz = ZoneInfo(config.timezone_name)
        end_epoch = None
        if config.to_date:
            end_epoch = _day_cutoff(
                date.fromisoformat(config.to_date) + timedelta(days=1), tz
            ).timestamp()
        where = [
            "stage='decision_pipeline'",
            "minute BETWEEN ? AND ?",
            "created_epoch IS NOT NULL",
            "contract_key <> ''",
        ]
        parameters: list[Any] = [config.min_minute, config.max_minute]
        if end_epoch is not None:
            where.append("created_epoch < ?")
            parameters.append(end_epoch)
        where_sql = " AND ".join(where)
        cohort_rows = self.connection.execute(
            f"""
            SELECT contract_key, COUNT(*), COUNT(DISTINCT fixture_id),
                   MIN(created_epoch), MAX(created_epoch)
            FROM observations
            WHERE {where_sql}
            GROUP BY contract_key
            ORDER BY MAX(created_epoch), contract_key
            """,
            parameters,
        ).fetchall()
        selected_key = str(config.contract_key or "")
        if not selected_key:
            newest = self.connection.execute(
                f"""
                SELECT contract_key
                FROM observations
                WHERE {where_sql}
                ORDER BY created_epoch DESC, observation_id DESC
                LIMIT 1
                """,
                parameters,
            ).fetchone()
            selected_key = str(newest[0] if newest else "")
        cohorts = []
        selected_payload: Mapping[str, Any] = {}
        for key, rows, fixtures, first_epoch, last_epoch in cohort_rows:
            payload_row = self.connection.execute(
                """
                SELECT contract_payload_json
                FROM observations
                WHERE contract_key=?
                ORDER BY created_epoch DESC, observation_id DESC
                LIMIT 1
                """,
                (key,),
            ).fetchone()
            try:
                payload = json.loads(payload_row[0]) if payload_row else {}
            except (TypeError, ValueError):
                payload = {}
            if key == selected_key:
                selected_payload = payload if isinstance(payload, dict) else {}
            cohorts.append(
                {
                    "key": key,
                    "selected": key == selected_key,
                    "rows": int(rows),
                    "fixtures": int(fixtures),
                    "first_observation_utc": datetime.fromtimestamp(
                        first_epoch, UTC
                    ).isoformat(),
                    "last_observation_utc": datetime.fromtimestamp(
                        last_epoch, UTC
                    ).isoformat(),
                    "payload": payload,
                }
            )
        contract = {"key": selected_key, "payload": dict(selected_payload)}
        return selected_key, contract, {
            "selection": "explicit" if config.contract_key else "latest_semantic_cohort",
            "cohort_count": len(cohorts),
            "cohorts": cohorts,
        }

    def compatible_rows(
        self,
        *,
        contract_key: str,
        config: EvaluationConfig,
    ) -> Iterator[dict[str, Any]]:
        tz = ZoneInfo(config.timezone_name)
        parameters: list[Any] = [
            config.min_minute,
            config.max_minute,
            contract_key,
        ]
        end_clause = ""
        if config.to_date:
            end_epoch = _day_cutoff(
                date.fromisoformat(config.to_date) + timedelta(days=1), tz
            ).timestamp()
            end_clause = " AND o.created_epoch < ?"
            parameters.append(end_epoch)
        cursor = self.connection.execute(
            f"""
            SELECT o.payload_json, x.payload_json, x.created_at_utc,
                   x.outcome_schema_version
            FROM observations AS o
            LEFT JOIN outcomes AS x USING(observation_id)
            WHERE o.stage='decision_pipeline'
              AND o.minute BETWEEN ? AND ?
              AND o.contract_key=?
              AND o.created_epoch IS NOT NULL
              {end_clause}
            ORDER BY o.created_epoch, o.observation_id
            """,
            parameters,
        )
        for observation_json, outcome_json, outcome_created, outcome_schema in cursor:
            try:
                observation = json.loads(observation_json)
            except (TypeError, ValueError):
                continue
            if not isinstance(observation, dict):
                continue
            if outcome_json:
                try:
                    outcome_record = json.loads(outcome_json)
                except (TypeError, ValueError):
                    outcome_record = {}
                if isinstance(outcome_record, dict):
                    observation["outcome"] = dict(outcome_record)
                    observation["_outcome_record_created_at_utc"] = outcome_created
                    observation["_outcome_schema_version"] = outcome_schema
            yield observation


def _record_id(record: Mapping[str, Any]) -> str:
    return str(
        record.get("observation_key") or record.get("observation_id") or ""
    ).strip()


def _outcome_resolved_at(record: Mapping[str, Any]) -> Optional[datetime]:
    outcome = _mapping(record.get("outcome"))
    return _parse_utc(
        outcome.get("resolved_at_utc")
        or record.get("_outcome_record_created_at_utc")
    )


def _resolved_label(record: Mapping[str, Any]) -> tuple[Optional[int], str]:
    outcome = _mapping(record.get("outcome"))
    status = str(outcome.get("status") or "pending").strip().lower()
    if status != "resolved":
        return None, status or "pending"
    label = _label(outcome.get("goal_to90_normal_time"))
    if label is None:
        return None, "invalid_label"
    observation_at = _parse_utc(record.get("created_at_utc"))
    resolved_at = _outcome_resolved_at(record)
    if observation_at is None or resolved_at is None or resolved_at <= observation_at:
        return None, "invalid_outcome_timing"
    return label, "resolved"


def _feature_contract(record: Mapping[str, Any]) -> Mapping[str, Any]:
    return shadow_model._feature_contract(record)


def _candidate_observation(
    record: Mapping[str, Any],
    config: EvaluationConfig,
) -> tuple[bool, Optional[datetime], str]:
    if str(record.get("record_type") or "") != "observation":
        return False, None, "record_type"
    if str(record.get("stage") or "") != "decision_pipeline":
        return False, None, "stage"
    fixture_id = _safe_int(record.get("fixture_id"))
    if fixture_id is None or fixture_id <= 0:
        return False, None, "fixture_id"
    minute = _safe_int(record.get("minute"))
    if minute is None or not config.min_minute <= minute <= config.max_minute:
        return False, None, "minute"
    created_at = _parse_utc(record.get("created_at_utc"))
    if created_at is None:
        return False, None, "timestamp"
    return True, created_at, ""


def _snapshot_values(record: Mapping[str, Any]) -> tuple[Optional[dict[str, Any]], str]:
    probabilities = _mapping(record.get("probabilities"))
    features = _mapping(record.get("features"))
    match = _mapping(record.get("match"))
    stored_filter = _mapping(record.get("channel_signal_filter"))
    probability = _probability_pct(
        probabilities.get("prob_to90")
        if probabilities.get("prob_to90") is not None
        else probabilities.get("final_prob_to90")
    )
    base = _probability_pct(
        probabilities.get("reputation_base_prob_to90")
        if probabilities.get("reputation_base_prob_to90") is not None
        else stored_filter.get("reputation_base_prob_to90")
    )
    adjusted = _probability_pct(
        probabilities.get("reputation_adjusted_prob_to90")
        if probabilities.get("reputation_adjusted_prob_to90") is not None
        else stored_filter.get("reputation_adjusted_prob_to90")
    )
    intensity = _finite_float(
        features.get("adjusted_intensity")
        if features.get("adjusted_intensity") is not None
        else stored_filter.get("adjusted_intensity")
    )
    season = _finite_float(
        features.get("season_context_factor")
        if features.get("season_context_factor") is not None
        else features.get("season_context_factor_45p")
    )
    if season is None:
        season = _finite_float(stored_filter.get("season_context_factor"))
    score_home = _safe_int(
        match.get("score_home")
        if match.get("score_home") is not None
        else stored_filter.get("score_home")
    )
    score_away = _safe_int(
        match.get("score_away")
        if match.get("score_away") is not None
        else stored_filter.get("score_away")
    )
    required = {
        "prob_to90": probability,
        "reputation_base_prob_to90": base,
        "reputation_adjusted_prob_to90": adjusted,
        "adjusted_intensity": intensity,
        "season_context_factor": season,
        "score_home": score_home,
        "score_away": score_away,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        return None, "missing_" + "+".join(missing)
    assert probability is not None and base is not None and adjusted is not None
    assert intensity is not None and season is not None
    assert score_home is not None and score_away is not None
    if intensity < 0.0 or season < 0.0 or score_home < 0 or score_away < 0:
        return None, "invalid_value"
    return {
        "prob_to90": probability,
        "reputation_base_prob_to90": base,
        "reputation_adjusted_prob_to90": adjusted,
        "reputation_delta_to90_pp": round(adjusted - base, 6),
        "adjusted_intensity": intensity,
        "season_context_factor": season,
        "score_home": score_home,
        "score_away": score_away,
        "goals_at_snapshot": score_home + score_away,
    }, ""


def _walk_ruleset(config: EvaluationConfig):
    """Use exactly the same control/slice contract as the prospective layer."""

    return replace(
        LIVE_CANDIDATE_RULESET,
        min_prob_to90=float(config.min_prob_to90),
        min_reputation_delta_to90_pp=float(
            config.min_reputation_delta_to90_pp
        ),
        min_adjusted_intensity=float(config.min_adjusted_intensity),
        min_season_context_factor=float(config.min_season_context_factor),
        max_goals_at_snapshot=int(config.max_goals),
        max_score_difference_abs=int(config.max_score_difference_abs),
        rolling_ml_min_probability_pct=float(
            config.ml_confirm_threshold_pct
        ),
    )


def _candidate_contract_record(
    record: Mapping[str, Any],
    values: Mapping[str, Any],
    *,
    config: EvaluationConfig,
) -> Mapping[str, Any]:
    """Return exact live evidence or an explicitly hypothetical envelope."""

    if config.control_contract_mode == "recorded-production":
        return record
    filter_passed = bool(
        values["prob_to90"] >= config.min_prob_to90
        and values["reputation_delta_to90_pp"]
        >= config.min_reputation_delta_to90_pp
        and values["adjusted_intensity"] >= config.min_adjusted_intensity
        and values["season_context_factor"]
        >= config.min_season_context_factor
    )
    synthetic = dict(record)
    synthetic["publication_policy"] = {
        "filter_version": LIVE_CANDIDATE_RULESET.control_filter_version,
        "publication_context_passed": True,
        "publication_allow": filter_passed,
    }
    synthetic["decision"] = {"active_publication_allow": filter_passed}
    synthetic["channel_signal_filter"] = {
        "version": LIVE_CANDIDATE_RULESET.control_filter_version,
        "passed": filter_passed,
        "prob_to90": values["prob_to90"],
        "reputation_base_prob_to90": values[
            "reputation_base_prob_to90"
        ],
        "reputation_adjusted_prob_to90": values[
            "reputation_adjusted_prob_to90"
        ],
        "adjusted_intensity": values["adjusted_intensity"],
        "season_context_factor": values["season_context_factor"],
        "score_home": values["score_home"],
        "score_away": values["score_away"],
    }
    return synthetic


def _prediction_value(prediction: Mapping[str, Any]) -> Optional[float]:
    target = _mapping(_mapping(prediction.get("predictions")).get("to90"))
    if str(target.get("status") or "") != "ok":
        return None
    return _probability_pct(target.get("calibrated_probability_pct"))


def _read_prediction_index(
    path: Optional[os.PathLike[str] | str],
    *,
    selected_observation_ids: Optional[set[str]] = None,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    if path is None:
        return {}, {"active_path": None, "files": [], "records": 0}
    diagnostics: dict[str, Any] = {}
    records = _stream_journal(path, diagnostics)
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: set[str] = set()
    skipped = Counter()
    for record in records:
        observation_id = str(record.get("observation_id") or "").strip()
        if not observation_id:
            skipped["missing_observation_id"] += 1
            continue
        if (
            selected_observation_ids is not None
            and observation_id not in selected_observation_ids
        ):
            skipped["not_selected_observation"] += 1
            continue
        key = str(record.get("prediction_key") or "").strip()
        if not key:
            skipped["missing_prediction_key"] += 1
            continue
        if key in seen:
            skipped["duplicate_prediction_key"] += 1
            continue
        seen.add(key)
        index[observation_id].append(record)
    for values in index.values():
        values.sort(key=lambda item: str(item.get("created_at_utc") or ""))
    diagnostics = {**diagnostics, "index_skipped": dict(skipped)}
    return dict(index), diagnostics


def _prospective_journal_prediction(
    candidates: Sequence[Mapping[str, Any]],
    observation: Mapping[str, Any],
    *,
    config: EvaluationConfig,
    source: str,
) -> tuple[Optional[dict[str, Any]], Counter[str]]:
    reasons: Counter[str] = Counter()
    observation_at = _parse_utc(observation.get("created_at_utc"))
    if observation_at is None:
        reasons["observation_timestamp"] += 1
        return None, reasons
    tz = ZoneInfo(config.timezone_name)
    fold_start = _day_cutoff(_local_day(observation_at, tz), tz)
    outcome_at = _outcome_resolved_at(observation)
    expected_type = (
        "shadow_ml_rolling_prediction"
        if source == "rolling"
        else "shadow_ml_prediction"
    )
    for candidate in candidates:
        if str(candidate.get("record_type") or "") != expected_type:
            reasons["record_type"] += 1
            continue
        if candidate.get("shadow_only") is not True:
            reasons["shadow_only"] += 1
            continue
        if candidate.get("production_applied") is not False:
            reasons["production_applied"] += 1
            continue
        if str(candidate.get("prediction_status") or "") not in {"ok", "partial"}:
            reasons["prediction_status"] += 1
            continue
        if _safe_int(candidate.get("fixture_id")) != _safe_int(
            observation.get("fixture_id")
        ) or _safe_int(candidate.get("minute")) != _safe_int(
            observation.get("minute")
        ) or str(candidate.get("observation_id") or "") != _record_id(
            observation
        ):
            reasons["identity_mismatch"] += 1
            continue
        recorded_observation_at = _parse_utc(
            candidate.get("observation_created_at_utc")
        )
        if recorded_observation_at is None:
            reasons["observation_timestamp_missing"] += 1
            continue
        if recorded_observation_at != observation_at:
            reasons["observation_timestamp_mismatch"] += 1
            continue
        prediction_at = _parse_utc(candidate.get("created_at_utc"))
        model_at = _parse_utc(candidate.get("model_created_at_utc"))
        cutoff_at = _parse_utc(candidate.get("model_data_cutoff_utc"))
        if prediction_at is None or model_at is None or cutoff_at is None:
            reasons["missing_timing"] += 1
            continue
        if cutoff_at > model_at:
            reasons["invalid_model_timeline"] += 1
            continue
        if model_at >= observation_at or cutoff_at >= observation_at:
            reasons["trained_after_observation"] += 1
            continue
        if config.journal_timing_policy == "day-frozen" and (
            model_at >= fold_start or cutoff_at >= fold_start
        ):
            reasons["not_day_frozen"] += 1
            continue
        lag = (prediction_at - observation_at).total_seconds()
        if lag < 0.0 or lag > config.max_prediction_lag_seconds:
            reasons["prediction_lag"] += 1
            continue
        if outcome_at is not None and prediction_at >= outcome_at:
            reasons["prediction_after_outcome"] += 1
            continue
        target = _mapping(_mapping(candidate.get("predictions")).get("to90"))
        if target.get("production_applied") is not False:
            reasons["target_production_applied"] += 1
            continue
        probability = _prediction_value(candidate)
        if probability is None:
            reasons["missing_to90_prediction"] += 1
            continue
        return {
            "probability_pct": probability,
            "model_id": candidate.get("model_id"),
            "algorithm_version": candidate.get("algorithm_version"),
            "model_created_at_utc": model_at.isoformat(),
            "model_data_cutoff_utc": cutoff_at.isoformat(),
            "prediction_created_at_utc": prediction_at.isoformat(),
            "origin": "journal-replay",
        }, reasons
    return None, reasons


def _fold_training_rows(
    observations: Sequence[Mapping[str, Any]],
    *,
    cutoff: datetime,
    contract_key: str,
    config: EvaluationConfig,
) -> tuple[list[dict[str, Any]], Counter[str]]:
    rows: list[dict[str, Any]] = []
    excluded: Counter[str] = Counter()
    for record in observations:
        valid, observation_at, reason = _candidate_observation(record, config)
        if not valid or observation_at is None:
            excluded[reason] += 1
            continue
        if str(_feature_contract(record).get("key") or "") != contract_key:
            excluded["feature_contract"] += 1
            continue
        outcome_at = _outcome_resolved_at(record)
        label, outcome_status = _resolved_label(record)
        if label is None:
            excluded[f"outcome_{outcome_status}"] += 1
            continue
        if observation_at >= cutoff:
            excluded["observation_not_before_cutoff"] += 1
            continue
        if outcome_at is None or outcome_at >= cutoff:
            excluded["outcome_not_before_cutoff"] += 1
            continue
        rows.append(dict(record))
    return rows, excluded


def _prediction_from_inference(
    prediction: Mapping[str, Any],
    artifact: Mapping[str, Any],
    *,
    cutoff: datetime,
    observation_at: datetime,
) -> Optional[dict[str, Any]]:
    targets = _mapping(prediction.get("targets"))
    target = _mapping(targets.get("to90"))
    probability = _probability_pct(target.get("calibrated_probability_pct"))
    if str(target.get("status") or "") != "ok" or probability is None:
        return None
    artifact_cutoff = _parse_utc(artifact.get("data_cutoff_utc"))
    artifact_created = _parse_utc(artifact.get("created_at_utc"))
    if artifact_cutoff is None or artifact_created is None:
        return None
    if (
        artifact_cutoff >= cutoff
        or artifact_created > cutoff
        or artifact_cutoff > artifact_created
        or artifact_created >= observation_at
    ):
        return None
    return {
        "probability_pct": probability,
        "model_id": artifact.get("model_id"),
        "algorithm_version": artifact.get("algorithm_version"),
        "model_created_at_utc": artifact.get("created_at_utc"),
        "model_data_cutoff_utc": artifact.get("data_cutoff_utc"),
        "prediction_created_at_utc": observation_at.isoformat(),
        "origin": "fold-local",
    }


def _build_fold_local_predictions(
    observations: Sequence[Mapping[str, Any]],
    test_rows_by_day: Mapping[date, Sequence[Mapping[str, Any]]],
    *,
    contract_key: str,
    config: EvaluationConfig,
    training_config: Optional[ShadowMLConfig],
) -> tuple[
    dict[str, dict[str, dict[str, Any]]],
    list[dict[str, Any]],
]:
    predictions = {"static": {}, "rolling": {}}
    fold_models: list[dict[str, Any]] = []
    tz = ZoneInfo(config.timezone_name)
    for day in sorted(test_rows_by_day):
        cutoff = _day_cutoff(day, tz)
        training_rows, excluded = _fold_training_rows(
            observations,
            cutoff=cutoff,
            contract_key=contract_key,
            config=config,
        )
        fold_payload: dict[str, Any] = {
            "day": day.isoformat(),
            "cutoff_utc": cutoff.isoformat(),
            "training_rows": len(training_rows),
            "training_fixtures": len(
                {int(row["fixture_id"]) for row in training_rows}
            ),
            "training_excluded": dict(excluded),
            "models": {},
        }
        artifacts: dict[str, Mapping[str, Any]] = {}
        trainers = {
            "static": train_shadow_model,
            "rolling": train_shadow_rolling_model,
        }
        for source, trainer in trainers.items():
            try:
                artifact = trainer(
                    training_rows,
                    config=training_config,
                    now=cutoff,
                )
                artifacts[source] = artifact
                artifact_cutoff = _parse_utc(artifact.get("data_cutoff_utc"))
                artifact_created = _parse_utc(artifact.get("created_at_utc"))
                cutoff_valid = bool(
                    artifact_cutoff is not None
                    and artifact_created is not None
                    and artifact_cutoff < cutoff
                    and artifact_created <= cutoff
                    and artifact_cutoff <= artifact_created
                )
                fold_payload["models"][source] = {
                    "model_id": artifact.get("model_id"),
                    "status": artifact.get("status"),
                    "created_at_utc": artifact.get("created_at_utc"),
                    "data_cutoff_utc": artifact.get("data_cutoff_utc"),
                    "cutoff_strictly_respected": cutoff_valid,
                    "trained_targets": {
                        name: bool(_mapping(payload).get("trained"))
                        for name, payload in _mapping(artifact.get("targets")).items()
                    },
                }
            except Exception as exc:  # offline report must isolate one fold
                fold_payload["models"][source] = {
                    "status": "error",
                    "error_type": type(exc).__name__,
                    "cutoff_strictly_respected": False,
                }
        for record in test_rows_by_day[day]:
            observation_id = _record_id(record)
            observation_at = _parse_utc(record.get("created_at_utc"))
            if observation_at is None:
                continue
            inference_row = dict(record)
            # The model allow-list already excludes outcomes, but removing the
            # block here makes that no-leakage boundary structural and auditable.
            inference_row["outcome"] = {"status": "pending"}
            inference_row.pop("_outcome_record_created_at_utc", None)
            inference_row.pop("_outcome_schema_version", None)
            for source, predictor in (
                ("static", predict_shadow),
                ("rolling", predict_shadow_rolling),
            ):
                artifact = artifacts.get(source)
                if not artifact:
                    continue
                try:
                    prediction = predictor(artifact, inference_row)
                except Exception:
                    continue
                materialized = _prediction_from_inference(
                    prediction,
                    artifact,
                    cutoff=cutoff,
                    observation_at=observation_at,
                )
                if materialized is not None:
                    predictions[source][observation_id] = materialized
        fold_models.append(fold_payload)
    return predictions, fold_models


def _ml_confirmation_probability(
    static_prediction: Optional[Mapping[str, Any]],
    rolling_prediction: Optional[Mapping[str, Any]],
    source: str,
) -> Optional[float]:
    static = (
        _probability_pct(static_prediction.get("probability_pct"))
        if static_prediction
        else None
    )
    rolling = (
        _probability_pct(rolling_prediction.get("probability_pct"))
        if rolling_prediction
        else None
    )
    if source == "static":
        return static
    if source == "rolling":
        return rolling
    if static is None or rolling is None:
        return None
    # Consensus is intentionally conservative: both models must clear the
    # threshold, so the effective confirmation is their lower probability.
    return min(static, rolling)


def _trigger_payload(
    record: Mapping[str, Any],
    values: Mapping[str, Any],
    *,
    day: date,
    both_windows_available: bool,
    static_prediction: Optional[Mapping[str, Any]],
    rolling_prediction: Optional[Mapping[str, Any]],
    ml_confirmation_probability: Optional[float],
) -> dict[str, Any]:
    label, outcome_status = _resolved_label(record)
    match = _mapping(record.get("match"))
    return {
        "fixture_id": int(record["fixture_id"]),
        "observation_id": _record_id(record),
        "created_at_utc": str(record.get("created_at_utc") or ""),
        "day_msk": day.isoformat(),
        "minute": int(record["minute"]),
        "home_team_name": match.get("home_team_name"),
        "away_team_name": match.get("away_team_name"),
        **dict(values),
        "both_windows_available": bool(both_windows_available),
        "static_ml_probability_pct": (
            static_prediction.get("probability_pct")
            if static_prediction
            else None
        ),
        "rolling_ml_probability_pct": (
            rolling_prediction.get("probability_pct")
            if rolling_prediction
            else None
        ),
        "ml_confirmation_probability_pct": ml_confirmation_probability,
        "static_ml_model_id": (
            static_prediction.get("model_id") if static_prediction else None
        ),
        "rolling_ml_model_id": (
            rolling_prediction.get("model_id") if rolling_prediction else None
        ),
        "outcome_status": outcome_status,
        "goal_to90_normal_time": bool(label) if label is not None else None,
        "label": label,
    }


def _wilson_interval(hits: int, total: int) -> Optional[list[float]]:
    if total <= 0:
        return None
    proportion = hits / total
    z2 = _WILSON_Z_95**2
    denominator = 1.0 + z2 / total
    center = (proportion + z2 / (2.0 * total)) / denominator
    margin = (
        _WILSON_Z_95
        * math.sqrt(
            proportion * (1.0 - proportion) / total
            + z2 / (4.0 * total**2)
        )
        / denominator
    )
    return [round(max(0.0, center - margin), 9), round(min(1.0, center + margin), 9)]


def _probability_metrics(
    triggers: Sequence[Mapping[str, Any]], probability_key: str
) -> dict[str, Any]:
    samples: list[tuple[float, int]] = []
    for trigger in triggers:
        probability_pct = _probability_pct(trigger.get(probability_key))
        label = _label(trigger.get("label"))
        if probability_pct is None or label is None:
            continue
        probability = min(1.0 - 1e-6, max(1e-6, probability_pct / 100.0))
        samples.append((probability, label))
    if not samples:
        return {
            "rows": 0,
            "coverage": 0.0 if triggers else None,
            "average_probability": None,
            "actual_rate": None,
            "log_loss": None,
            "brier": None,
        }
    return {
        "rows": len(samples),
        "coverage": round(len(samples) / max(1, len(triggers)), 9),
        "average_probability": round(
            sum(probability for probability, _ in samples) / len(samples), 9
        ),
        "actual_rate": round(sum(label for _, label in samples) / len(samples), 9),
        "log_loss": round(
            -sum(
                label * math.log(probability)
                + (1 - label) * math.log(1.0 - probability)
                for probability, label in samples
            )
            / len(samples),
            9,
        ),
        "brier": round(
            sum((probability - label) ** 2 for probability, label in samples)
            / len(samples),
            9,
        ),
    }


def _summarize_triggers(
    triggers: Sequence[Mapping[str, Any]],
    *,
    control_count: int,
) -> dict[str, Any]:
    resolved = [trigger for trigger in triggers if _label(trigger.get("label")) is not None]
    hits = sum(int(trigger["label"]) for trigger in resolved)
    total = len(resolved)
    outcome_statuses = Counter(str(item.get("outcome_status") or "unknown") for item in triggers)
    bot_metrics = _probability_metrics(resolved, "prob_to90")
    static_metrics = _probability_metrics(resolved, "static_ml_probability_pct")
    rolling_metrics = _probability_metrics(resolved, "rolling_ml_probability_pct")
    return {
        "signals": len(triggers),
        "resolved": total,
        "hits": hits,
        "misses": total - hits,
        "hit_rate": round(hits / total, 9) if total else None,
        "wilson_ci95": _wilson_interval(hits, total),
        "coverage_vs_control": (
            round(len(triggers) / control_count, 9) if control_count else None
        ),
        "outcome_coverage": (
            round(total / len(triggers), 9) if triggers else None
        ),
        "outcome_statuses": dict(sorted(outcome_statuses.items())),
        "log_loss": bot_metrics["log_loss"],
        "brier": bot_metrics["brier"],
        "probability_metrics": {
            "bot": bot_metrics,
            "static_ml": static_metrics,
            "rolling_ml": rolling_metrics,
        },
    }


def _date_range(first: date, last: date) -> Iterator[date]:
    current = first
    while current <= last:
        yield current
        current += timedelta(days=1)


def build_walk_forward_report(
    observation_path: os.PathLike[str] | str,
    *,
    static_prediction_path: Optional[os.PathLike[str] | str] = None,
    rolling_prediction_path: Optional[os.PathLike[str] | str] = None,
    config: Optional[EvaluationConfig] = None,
    training_config: Optional[ShadowMLConfig] = None,
) -> dict[str, Any]:
    """Build a read-only chronological report without touching live state."""

    selected_config = config or EvaluationConfig()
    observation_source: dict[str, Any] = {}
    store = _ObservationStore()
    try:
        return _build_walk_forward_report_with_store(
            store,
            observation_source,
            observation_path,
            static_prediction_path=static_prediction_path,
            rolling_prediction_path=rolling_prediction_path,
            config=selected_config,
            training_config=training_config,
        )
    finally:
        store.close()


def _build_walk_forward_report_with_store(
    store: _ObservationStore,
    observation_source: dict[str, Any],
    observation_path: os.PathLike[str] | str,
    *,
    static_prediction_path: Optional[os.PathLike[str] | str],
    rolling_prediction_path: Optional[os.PathLike[str] | str],
    config: EvaluationConfig,
    training_config: Optional[ShadowMLConfig],
) -> dict[str, Any]:
    selected_config = config
    store.ingest(
        _stream_journal(observation_path, observation_source),
        config=selected_config,
    )
    join_diagnostics = store.join_diagnostics()
    contract_key, contract, contract_diagnostics = store.select_contract(
        selected_config
    )
    tz = ZoneInfo(selected_config.timezone_name)
    start_day = (
        date.fromisoformat(selected_config.from_date)
        if selected_config.from_date
        else None
    )
    end_day = (
        date.fromisoformat(selected_config.to_date)
        if selected_config.to_date
        else None
    )
    rows_by_day: dict[date, list[dict[str, Any]]] = defaultdict(list)
    rows_count_by_day: Counter[date] = Counter()
    eligibility_skipped = Counter()
    eligibility_skipped["feature_contract"] = sum(
        int(cohort.get("rows") or 0)
        for cohort in contract_diagnostics.get("cohorts", [])
        if not cohort.get("selected")
    )
    materialized_rows: list[dict[str, Any]] = []
    selected_observation_ids: set[str] = set()
    compatible_fixtures: set[int] = set()
    resolved_metadata: list[tuple[int, datetime]] = []
    compatible_row_count = 0
    observed_days: set[date] = set()
    for record in store.compatible_rows(
        contract_key=contract_key,
        config=selected_config,
    ):
        valid, created_at, reason = _candidate_observation(record, selected_config)
        if not valid or created_at is None:
            eligibility_skipped[reason] += 1
            continue
        day = _local_day(created_at, tz)
        compatible_row_count += 1
        compatible_fixtures.add(int(record["fixture_id"]))
        selected_observation_ids.add(_record_id(record))
        observed_days.add(day)
        outcome_at = _outcome_resolved_at(record)
        if outcome_at is not None:
            resolved_metadata.append((int(record["fixture_id"]), outcome_at))
        if start_day is None or day >= start_day:
            rows_count_by_day[day] += 1
            if selected_config.ml_mode == "fold-local":
                rows_by_day[day].append(record)
        if selected_config.ml_mode == "fold-local":
            materialized_rows.append(record)

    if start_day is None and observed_days:
        start_day = min(observed_days)
    if end_day is None and observed_days:
        end_day = max(observed_days)

    prediction_maps: dict[str, dict[str, dict[str, Any]]] = {
        "static": {},
        "rolling": {},
    }
    fold_models: list[dict[str, Any]] = []
    prediction_sources: dict[str, Any] = {}
    prediction_skipped: dict[str, dict[str, int]] = {}
    if selected_config.ml_mode == "fold-local":
        prediction_maps, fold_models = _build_fold_local_predictions(
            materialized_rows,
            rows_by_day,
            contract_key=contract_key,
            config=selected_config,
            training_config=training_config,
        )
        prediction_sources = {
            "mode": "fold-local",
            "static": "trained_from_joined_observations",
            "rolling": "trained_from_joined_observations",
        }
    elif selected_config.ml_mode == "journal-replay":
        static_index, static_source = _read_prediction_index(
            static_prediction_path,
            selected_observation_ids=selected_observation_ids,
        )
        rolling_index, rolling_source = _read_prediction_index(
            rolling_prediction_path,
            selected_observation_ids=selected_observation_ids,
        )
        prediction_sources = {
            "mode": "journal-replay",
            "static": static_source,
            "rolling": rolling_source,
        }
        reason_counters = {"static": Counter(), "rolling": Counter()}
        for record in store.compatible_rows(
            contract_key=contract_key,
            config=selected_config,
        ):
            observation_id = _record_id(record)
            for source, index in (
                ("static", static_index),
                ("rolling", rolling_index),
            ):
                prediction, reasons = _prospective_journal_prediction(
                    index.get(observation_id, []),
                    record,
                    config=selected_config,
                    source=source,
                )
                reason_counters[source].update(reasons)
                if prediction is not None:
                    prediction_maps[source][observation_id] = prediction
        prediction_skipped = {
            source: dict(counter) for source, counter in reason_counters.items()
        }

    triggers: dict[str, list[dict[str, Any]]] = {arm: [] for arm in ARM_ORDER}
    seen: dict[str, set[int]] = {arm: set() for arm in ARM_ORDER}
    first_control_seen: set[int] = set()
    candidate_diagnostics = Counter()
    candidate_ruleset = _walk_ruleset(selected_config)
    trigger_records: Iterable[dict[str, Any]] = (
        materialized_rows
        if selected_config.ml_mode == "fold-local"
        else store.compatible_rows(
            contract_key=contract_key,
            config=selected_config,
        )
    )
    for record in trigger_records:
        created_at = _parse_utc(record.get("created_at_utc"))
        if created_at is None:
            continue
        day = _local_day(created_at, tz)
        values, reason = _snapshot_values(record)
        if values is None:
            candidate_diagnostics[reason] += 1
            continue
        fixture_id = int(record["fixture_id"])
        contract_record = _candidate_contract_record(
            record,
            values,
            config=selected_config,
        )
        contract_arms = evaluate_candidate_arms(
            contract_record,
            ruleset=candidate_ruleset,
            max_live_prediction_lag_seconds=(
                selected_config.max_prediction_lag_seconds
            ),
        )
        control_evaluation = _mapping(
            contract_arms.get("control_current_filter")
        )
        slices_evaluation = _mapping(
            contract_arms.get("full_slices_5m_10m")
        )
        goals_evaluation = _mapping(
            contract_arms.get("full_slices_goals_le2")
        )
        close_evaluation = _mapping(
            contract_arms.get("full_slices_goals_le2_close")
        )
        base_pass = control_evaluation.get("evaluation_status") == "pass"
        both_windows = slices_evaluation.get("evaluation_status") == "pass"
        goals_pass = goals_evaluation.get("evaluation_status") == "pass"
        close_pass = close_evaluation.get("evaluation_status") == "pass"
        if control_evaluation.get("evaluation_status") == "unavailable":
            candidate_diagnostics[
                "control_unavailable:"
                + str(control_evaluation.get("reason") or "unknown")
            ] += 1
        observation_id = _record_id(record)
        static_prediction = prediction_maps["static"].get(observation_id)
        rolling_prediction = prediction_maps["rolling"].get(observation_id)
        ml_probability = _ml_confirmation_probability(
            static_prediction,
            rolling_prediction,
            selected_config.ml_source,
        )
        conditions = {
            ARM_CONTROL: base_pass,
            ARM_BOTH_WINDOWS: both_windows,
            ARM_BOTH_WINDOWS_GOALS_LE2: goals_pass,
            ARM_BOTH_WINDOWS_GOALS_LE2_CLOSE: close_pass,
            ARM_ML_CONFIRM: bool(
                selected_config.ml_mode != "disabled"
                and goals_pass
                and ml_probability is not None
                and ml_probability >= selected_config.ml_confirm_threshold_pct
            ),
        }
        if selected_config.trigger_policy == "first-control":
            if fixture_id in first_control_seen or not base_pass:
                continue
            # Production-faithful policy: the first control snapshot is frozen
            # once. Every stricter arm is judged only on this exact row; a
            # missing slice or ML prediction can never become available later.
            first_control_seen.add(fixture_id)
            payload = _trigger_payload(
                record,
                values,
                day=day,
                both_windows_available=both_windows,
                static_prediction=static_prediction,
                rolling_prediction=rolling_prediction,
                ml_confirmation_probability=ml_probability,
            )
            if start_day is None or day >= start_day:
                for arm in ARM_ORDER:
                    if conditions[arm]:
                        triggers[arm].append(dict(payload))
            continue

        for arm in ARM_ORDER:
            if fixture_id in seen[arm] or not conditions[arm]:
                continue
            # Explicitly exploratory alternative: each arm may wait for its
            # own later first eligible row. It is never the default report.
            seen[arm].add(fixture_id)
            payload = _trigger_payload(
                record,
                values,
                day=day,
                both_windows_available=both_windows,
                static_prediction=static_prediction,
                rolling_prediction=rolling_prediction,
                ml_confirmation_probability=ml_probability,
            )
            if start_day is None or day >= start_day:
                triggers[arm].append(payload)

    control_count = len(triggers[ARM_CONTROL])
    arms = {
        arm: {
            "definition": {
                ARM_CONTROL: (
                    f"BASE + p90>={selected_config.min_prob_to90:g} + "
                    "reputation_delta>="
                    f"{selected_config.min_reputation_delta_to90_pp:g}pp + "
                    f"intensity>={selected_config.min_adjusted_intensity:g} + "
                    "season>="
                    f"{selected_config.min_season_context_factor:g}"
                ),
                ARM_BOTH_WINDOWS: "control + both frozen 5m/10m windows status=ok",
                ARM_BOTH_WINDOWS_GOALS_LE2: "both_windows + goals_at_snapshot<=2",
                ARM_BOTH_WINDOWS_GOALS_LE2_CLOSE: (
                    "both_windows_goals_le2 + abs(score_home-score_away)<="
                    f"{selected_config.max_score_difference_abs:g}"
                ),
                ARM_ML_CONFIRM: (
                    "both_windows_goals_le2 + "
                    f"{selected_config.ml_source} ML>="
                    f"{selected_config.ml_confirm_threshold_pct:g}%"
                ),
            }[arm],
            **_summarize_triggers(
                triggers[arm],
                control_count=control_count,
            ),
            "triggers": triggers[arm],
        }
        for arm in ARM_ORDER
    }

    folds: list[dict[str, Any]] = []
    if start_day is not None and end_day is not None and start_day <= end_day:
        for day in _date_range(start_day, end_day):
            cutoff = _day_cutoff(day, tz)
            prior_resolved_fixtures = {
                fixture_id
                for fixture_id, outcome_at in resolved_metadata
                if outcome_at < cutoff
            }
            day_arms = {}
            for arm in ARM_ORDER:
                daily_triggers = [
                    trigger
                    for trigger in triggers[arm]
                    if trigger["day_msk"] == day.isoformat()
                ]
                summary = _summarize_triggers(
                    daily_triggers,
                    control_count=len(
                        [
                            item
                            for item in triggers[ARM_CONTROL]
                            if item["day_msk"] == day.isoformat()
                        ]
                    ),
                )
                summary.pop("outcome_statuses", None)
                summary.pop("probability_metrics", None)
                day_arms[arm] = summary
            folds.append(
                {
                    "day_msk": day.isoformat(),
                    "cutoff_utc": cutoff.isoformat(),
                    "next_cutoff_utc": (cutoff + timedelta(days=1)).isoformat(),
                    "prior_resolved_fixtures": len(prior_resolved_fixtures),
                    "evaluated_rows": int(rows_count_by_day.get(day, 0)),
                    "arms": day_arms,
                }
            )

    ml_base_triggers = triggers[ARM_BOTH_WINDOWS_GOALS_LE2]
    source_available = sum(
        _ml_confirmation_probability(
            (
                {"probability_pct": item["static_ml_probability_pct"]}
                if item.get("static_ml_probability_pct") is not None
                else None
            ),
            (
                {"probability_pct": item["rolling_ml_probability_pct"]}
                if item.get("rolling_ml_probability_pct") is not None
                else None
            ),
            selected_config.ml_source,
        )
        is not None
        for item in ml_base_triggers
    )
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "engine_version": ENGINE_VERSION,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "read_only": True,
        "shadow_only": True,
        "production_applied": False,
        "methodology": {
            "fold_timezone": selected_config.timezone_name,
            "fold_unit": "calendar_day",
            "first_trigger_policy": (
                "freeze first control-eligible observation per fixture; "
                "evaluate every derived arm only on that same snapshot"
                if selected_config.trigger_policy == "first-control"
                else (
                    "exploratory: first eligible observation per fixture "
                    "independently for each arm"
                )
            ),
            "later_data_can_rescue_derived_arm": (
                selected_config.trigger_policy == "independent-exploratory"
            ),
            "ml_unavailable_at_first_control": (
                "fixture is permanently rejected by ml_confirm"
                if selected_config.trigger_policy == "first-control"
                else "ml_confirm may wait for a later observation (exploratory only)"
            ),
            "label": "outcome.goal_to90_normal_time",
            "selection_uses_outcome": False,
            "ml_mode": selected_config.ml_mode,
            "ml_leakage_boundary": (
                "fresh model per fold; observation_at and "
                "outcome_resolved_at strictly before fold cutoff; outcomes "
                "removed from test inference"
                if selected_config.ml_mode == "fold-local"
                else (
                    "stored prediction replay; model/cutoff before observation, "
                    "bounded prediction lag, optional day-frozen timing"
                    if selected_config.ml_mode == "journal-replay"
                    else "ML arm disabled"
                )
            ),
            "contract_selection_uses_labels": False,
            "production_contract": (
                (
                    "requires the recorded publication context, filter "
                    "version, embedded filter result, and active allow; "
                    "rolling eligibility uses the live versioned ruleset"
                )
                if selected_config.control_contract_mode
                == "recorded-production"
                else (
                    "explicit historical hypothesis: recompute control from "
                    "frozen numeric inputs; rolling eligibility still uses "
                    "the live versioned contract"
                )
            ),
        },
        "config": asdict(selected_config),
        "candidate_ruleset": candidate_ruleset.manifest(),
        "sources": {
            "observations": observation_source,
            "predictions": prediction_sources,
        },
        "join": join_diagnostics,
        "contract": {
            "selected_key": contract_key or None,
            "selected_payload": dict(_mapping(contract.get("payload"))),
            **contract_diagnostics,
        },
        "eligibility": {
            "compatible_rows_through_to_date": compatible_row_count,
            "compatible_fixtures_through_to_date": len(compatible_fixtures),
            "rows_in_report_period": sum(rows_count_by_day.values()),
            "skipped": dict(eligibility_skipped),
            "candidate_value_skipped": dict(candidate_diagnostics),
        },
        "prediction_skipped": prediction_skipped,
        "fold_models": fold_models,
        "ml_gate_coverage": {
            "denominator_arm": ARM_BOTH_WINDOWS_GOALS_LE2,
            "denominator_signals": len(ml_base_triggers),
            "source": selected_config.ml_source,
            "available": source_available,
            "coverage": (
                round(source_available / len(ml_base_triggers), 9)
                if ml_base_triggers
                else None
            ),
        },
        "arms": arms,
        "folds": folds,
    }
    return report


def _fmt_pct(value: Any) -> str:
    number = _finite_float(value)
    return "—" if number is None else f"{100.0 * number:.1f}%"


def _fmt_metric(value: Any) -> str:
    number = _finite_float(value)
    return "—" if number is None else f"{number:.4f}"


def render_text_report(report: Mapping[str, Any]) -> str:
    """Render a compact human-readable companion to the JSON audit."""

    config = _mapping(report.get("config"))
    lines = [
        "WALK-FORWARD — фиксированные правила сигналов",
        (
            f"Период MSK: {config.get('from_date') or 'начало'} .. "
            f"{config.get('to_date') or 'конец'} | ML: {config.get('ml_mode')} / "
            f"{config.get('ml_source')} >= {config.get('ml_confirm_threshold_pct')}%"
        ),
        f"Триггерная политика: {config.get('trigger_policy')}",
        f"Контрольный контракт: {config.get('control_contract_mode')}",
        (
            "Контракт: "
            f"{_mapping(report.get('contract')).get('selected_key') or 'не найден'}"
        ),
        "",
        "arm                         сигн.  реш.    +    -   hit rate       "
        "Wilson 95%   coverage   logloss    Brier",
    ]
    arms = _mapping(report.get("arms"))
    for arm in ARM_ORDER:
        payload = _mapping(arms.get(arm))
        interval = payload.get("wilson_ci95")
        interval_text = (
            f"{100 * interval[0]:.1f}–{100 * interval[1]:.1f}%"
            if isinstance(interval, list) and len(interval) == 2
            else "—"
        )
        lines.append(
            f"{arm:<27} {int(payload.get('signals') or 0):>5} "
            f"{int(payload.get('resolved') or 0):>5} "
            f"{int(payload.get('hits') or 0):>4} "
            f"{int(payload.get('misses') or 0):>4} "
            f"{_fmt_pct(payload.get('hit_rate')):>10} "
            f"{interval_text:>16} "
            f"{_fmt_pct(payload.get('coverage_vs_control')):>10} "
            f"{_fmt_metric(payload.get('log_loss')):>9} "
            f"{_fmt_metric(payload.get('brier')):>8}"
        )
    lines.extend(
        [
            "",
            "По дням MSK (control / both+goals<=2 / close-score / ML):",
        ]
    )
    for fold in report.get("folds") or []:
        fold_arms = _mapping(_mapping(fold).get("arms"))
        cells = []
        for arm in (
            ARM_CONTROL,
            ARM_BOTH_WINDOWS_GOALS_LE2,
            ARM_BOTH_WINDOWS_GOALS_LE2_CLOSE,
            ARM_ML_CONFIRM,
        ):
            payload = _mapping(fold_arms.get(arm))
            cells.append(
                f"{int(payload.get('signals') or 0)}/"
                f"{int(payload.get('hits') or 0)}+"
                f"{int(payload.get('misses') or 0)}-"
            )
        lines.append(
            f"{_mapping(fold).get('day_msk')}: " + " | ".join(cells)
        )
    fold_models = report.get("fold_models") or []
    if fold_models:
        lines.extend(["", "Fold-local модели:"])
        for fold_model in fold_models:
            payload = _mapping(fold_model)
            models = _mapping(payload.get("models"))
            cells = []
            for source in ("static", "rolling"):
                model = _mapping(models.get(source))
                trained = _mapping(model.get("trained_targets")).get("to90")
                cells.append(
                    f"{source}={model.get('status') or 'missing'}"
                    f"/to90_trained={bool(trained)}"
                )
            lines.append(
                f"{payload.get('day')}: train_rows="
                f"{payload.get('training_rows', 0)}, " + ", ".join(cells)
            )
    ml_coverage = _mapping(report.get("ml_gate_coverage"))
    lines.extend(
        [
            "",
            (
                "ML coverage на базе both_windows_goals_le2: "
                f"{ml_coverage.get('available', 0)}/"
                f"{ml_coverage.get('denominator_signals', 0)} "
                f"({_fmt_pct(ml_coverage.get('coverage'))})"
            ),
            "Важно: малая выборка отражается широким Wilson-интервалом; "
            "hit rate не является гарантией будущего результата.",
        ]
    )
    return "\n".join(lines) + "\n"
