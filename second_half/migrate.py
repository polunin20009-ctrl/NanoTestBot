from __future__ import annotations

import gzip
import json
import os
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional

from .parser import (
    SECOND_HALF_HISTORY_SCHEMA_VERSION,
    SECOND_HALF_PARSER_VERSION,
    _infer_league_type,
    derive_halftime_state,
    derive_state_transitions,
)
from .storage import DEFAULT_HISTORY_PATH, iter_second_half_history_records, reset_second_half_history_cache


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _valid_goal(event: Mapping[str, Any]) -> bool:
    detail = str(event.get("detail") or "").lower()
    invalid = (
        "missed penalty", "penalty missed", "cancel", "disallow",
        "no goal", "ruled out", "offside", "var no goal",
    )
    minute = _int(event.get("minute"), -1)
    return 0 <= minute <= 90 and not any(token in detail for token in invalid)


def _parse_dt(value: Any) -> Optional[datetime]:
    try:
        raw = str(value or "").replace("Z", "+00:00")
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def migrate_legacy_record(record: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    fixture_id = _int(record.get("fixture_id"))
    league_id = _int(record.get("league_id"))
    home_team_id = _int(record.get("home_team_id"))
    away_team_id = _int(record.get("away_team_id"))
    if min(fixture_id, league_id, home_team_id, away_team_id) <= 0:
        return None

    migrated = dict(record)
    original_events = [dict(event) for event in (record.get("goal_events") or []) if isinstance(event, Mapping)]
    non_cancelled_all = [
        event for event in original_events
        if not any(token in str(event.get("detail") or "").lower() for token in (
            "missed penalty", "penalty missed", "cancel", "disallow",
            "no goal", "ruled out", "offside", "var no goal",
        ))
    ]
    normal_events = [event for event in non_cancelled_all if _valid_goal(event)]
    normal_events.sort(key=lambda event: (_int(event.get("abs_minute")), _int(event.get("minute"))))

    ft_home = _int(record.get("ft_home"))
    ft_away = _int(record.get("ft_away"))
    original_total = ft_home + ft_away
    extra_time_events = [event for event in non_cancelled_all if _int(event.get("minute"), -1) > 90]
    score_source = "legacy_score_preserved"
    if extra_time_events and len(non_cancelled_all) == original_total:
        normal_home = sum(event.get("team_side") == "home" for event in normal_events)
        normal_away = sum(event.get("team_side") == "away" for event in normal_events)
        if normal_home + normal_away == len(normal_events):
            ft_home, ft_away = normal_home, normal_away
            score_source = "legacy_events_normal_time_reconstructed"

    ht_home = _int(record.get("ht_home"))
    ht_away = _int(record.get("ht_away"))
    goals_2h_home = max(0, ft_home - ht_home)
    goals_2h_away = max(0, ft_away - ht_away)
    events_response_available = bool(
        record.get("events_response_available")
        if record.get("events_response_available") is not None
        else record.get("events_available")
    )
    events_complete = events_response_available and len(normal_events) == ft_home + ft_away
    second_half_events = [event for event in normal_events if _int(event.get("minute")) > 45]

    existing_type_source = str(record.get("league_type_source") or "")
    is_legacy_fixture = str(record.get("fixture_status") or "").startswith("LEGACY")
    explicit_type = (
        record.get("league_type")
        if not is_legacy_fixture and existing_type_source in {"", "api"}
        else ""
    )
    league_type, is_cup, league_type_source = _infer_league_type({
        "name": record.get("league_name"),
        "type": explicit_type,
        "round": record.get("round"),
    })
    finished_at = str(record.get("finished_at") or "")
    finished_dt = _parse_dt(finished_at)
    kickoff_at = (finished_dt - timedelta(hours=2)).isoformat() if finished_dt else None

    migrated.update({
        "schema_version": SECOND_HALF_HISTORY_SCHEMA_VERSION,
        "parser_version": SECOND_HALF_PARSER_VERSION,
        "outcome_scope": "TO_90_NORMAL_TIME",
        "fixture_id": fixture_id,
        "finished_at_source": "legacy_kickoff_plus_2h_estimate",
        "kickoff_at": kickoff_at,
        "collected_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "fixture_status": str(record.get("fixture_status") or "LEGACY_FINISHED"),
        "league_type": league_type,
        "league_type_source": league_type_source,
        "is_cup": is_cup,
        "ft_home": ft_home,
        "ft_away": ft_away,
        "normal_time_score_source": score_source,
        "goals_2h_home": goals_2h_home,
        "goals_2h_away": goals_2h_away,
        "goals_2h_total": goals_2h_home + goals_2h_away,
        "goals_after_60_total": (
            sum(_int(event.get("abs_minute")) > 60 for event in second_half_events)
            if events_complete else None
        ),
        "goals_after_75_total": (
            sum(_int(event.get("abs_minute")) > 75 for event in second_half_events)
            if events_complete else None
        ),
        "home_scored_in_2h": goals_2h_home > 0,
        "away_scored_in_2h": goals_2h_away > 0,
        "home_conceded_in_2h": goals_2h_away > 0,
        "away_conceded_in_2h": goals_2h_home > 0,
        "events_available": events_complete,
        "events_response_available": events_response_available,
        "events_complete": events_complete,
        "events_quality": "complete" if events_complete else "incomplete" if events_response_available else "unavailable",
        "raw_event_count": len(original_events),
        "valid_normal_time_goal_event_count": len(normal_events),
        "goal_events": normal_events,
        "halftime_state": derive_halftime_state(ht_home, ht_away),
        "state_transitions_2h": (
            derive_state_transitions(normal_events, ht_home, ht_away) if events_complete else []
        ),
    })
    return migrated


def migrate_history_file(path: str = DEFAULT_HISTORY_PATH) -> Dict[str, Any]:
    active = os.path.abspath(path)
    latest: Dict[int, Dict[str, Any]] = {}
    legacy: Dict[int, Dict[str, Any]] = {}
    order = []
    for record in iter_second_half_history_records(active):
        fixture_id = _int(record.get("fixture_id"), -1)
        if _int(record.get("schema_version"), 1) < SECOND_HALF_HISTORY_SCHEMA_VERSION:
            legacy.setdefault(fixture_id, record)
        if fixture_id not in latest:
            order.append(fixture_id)
        latest[fixture_id] = record
    source_records = [latest[fixture_id] for fixture_id in order]
    valid_source_records = [record for record in source_records if _int(record.get("fixture_id")) > 0]
    invalid_source_count = len(source_records) - len(valid_source_records)
    if valid_source_records and all(
        _int(record.get("schema_version"), 1) >= SECOND_HALF_HISTORY_SCHEMA_VERSION
        and str(record.get("parser_version") or "") == SECOND_HALF_PARSER_VERSION
        for record in valid_source_records
    ):
        return {
            "source_records": len(source_records),
            "migrated_records": len(valid_source_records),
            "dropped_records": invalid_source_count,
            "backup": None,
            "path": active,
            "schema_version": SECOND_HALF_HISTORY_SCHEMA_VERSION,
            "already_current": True,
        }
    migrated = []
    dropped = 0
    for record in source_records:
        candidate = dict(record)
        fixture_id = _int(candidate.get("fixture_id"), -1)
        legacy_record = legacy.get(fixture_id)
        if legacy_record is not None:
            candidate["events_response_available"] = bool(legacy_record.get("events_available"))
        converted = migrate_legacy_record(candidate)
        if converted is None:
            dropped += 1
        else:
            migrated.append(converted)

    backup = None
    if os.path.exists(active):
        stem, extension = os.path.splitext(active)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        backup = f"{stem}.pre_migration.{stamp}{extension}.gz"
        with open(active, "rb") as source, gzip.open(backup, "wb", compresslevel=6) as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)

    parent = os.path.dirname(active) or "."
    os.makedirs(parent, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".2h_migrate_", suffix=".tmp", dir=parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            for record in migrated:
                handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
            handle.flush()
        os.replace(temporary, active)
    finally:
        if os.path.exists(temporary):
            os.remove(temporary)
    reset_second_half_history_cache(active)
    return {
        "source_records": len(source_records),
        "migrated_records": len(migrated),
        "dropped_records": dropped,
        "backup": backup,
        "path": active,
        "schema_version": SECOND_HALF_HISTORY_SCHEMA_VERSION,
    }
