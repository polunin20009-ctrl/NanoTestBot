from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .aggregate import aggregate_history_to_files
from .storage import DEFAULT_HISTORY_PATH, collect_and_store_second_half_history


FINISHED_STATUSES = {"FT", "AET", "PEN"}


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def _safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _parse_dt(value: Any) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    raw = str(value).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def _fixture_id(raw_fixture: Mapping[str, Any]) -> Optional[int]:
    fixture = raw_fixture.get("fixture") if isinstance(raw_fixture, Mapping) else {}
    if isinstance(fixture, Mapping):
        return _safe_int(fixture.get("id"), _safe_int(raw_fixture.get("fixture_id"), _safe_int(raw_fixture.get("id"), None)))
    return _safe_int(raw_fixture.get("fixture_id"), _safe_int(raw_fixture.get("id"), None))


def _fixture_status(raw_fixture: Mapping[str, Any]) -> str:
    fixture = raw_fixture.get("fixture") if isinstance(raw_fixture, Mapping) else {}
    status = fixture.get("status") if isinstance(fixture, Mapping) else raw_fixture.get("status")
    if isinstance(status, Mapping):
        return _safe_str(status.get("short") or status.get("value"), "").upper()
    return _safe_str(status, "").upper()


def _fixture_date(raw_fixture: Mapping[str, Any]) -> datetime:
    fixture = raw_fixture.get("fixture") if isinstance(raw_fixture, Mapping) else {}
    date_raw = fixture.get("date") if isinstance(fixture, Mapping) else raw_fixture.get("date")
    return _parse_dt(date_raw)


def _api_error_text(errors: Any) -> str:
    if not errors:
        return ""
    if isinstance(errors, Mapping):
        return "; ".join(f"{key}: {value}" for key, value in errors.items())
    if isinstance(errors, Sequence) and not isinstance(errors, (str, bytes)):
        return "; ".join(str(item) for item in errors)
    return str(errors)


def fetch_finished_league_season_fixtures(
    client: Any,
    league_id: int,
    season: int,
    max_pages: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Fetch all available fixtures for a league season and keep only finished rows.

    Some plans return the whole season in one response, and ``page`` is not accepted for
    this endpoint in the current integration. ``max_pages`` is accepted only for call-site
    compatibility and is ignored.
    """
    finished: List[Dict[str, Any]] = []
    seen_fixture_ids: set[int] = set()
    data = client._get(
        "fixtures",
        params={
            "league": int(league_id),
            "season": int(season),
        },
        cache=False,
    )
    error_text = _api_error_text(data.get("errors"))
    if error_text:
        raise RuntimeError(error_text)

    response = data.get("response") or []
    for raw_fixture in response:
        if not isinstance(raw_fixture, Mapping):
            continue
        if _fixture_status(raw_fixture) not in FINISHED_STATUSES:
            continue

        fixture_id = _fixture_id(raw_fixture)
        if fixture_id is None or fixture_id in seen_fixture_ids:
            continue

        seen_fixture_ids.add(fixture_id)
        finished.append(dict(raw_fixture))

    finished.sort(key=_fixture_date)
    return finished


def backfill_second_half_history(
    client: Any,
    league_ids: Sequence[int],
    seasons: Sequence[int],
    history_path: str = DEFAULT_HISTORY_PATH,
    max_pages: Optional[int] = None,
    limit_fixtures: Optional[int] = None,
    sleep_seconds: float = 0.0,
    aggregate_after: bool = False,
    logger: Any = None,
) -> Dict[str, Any]:
    processed = 0
    stored = 0
    duplicates = 0
    failures = 0
    leagues_summary: List[Dict[str, Any]] = []

    for league_id in league_ids:
        for season in seasons:
            league_processed = 0
            league_stored = 0
            league_duplicates = 0
            league_failures = 0
            league_error: Optional[str] = None

            try:
                fixtures = fetch_finished_league_season_fixtures(
                    client=client,
                    league_id=int(league_id),
                    season=int(season),
                    max_pages=max_pages,
                )
            except Exception as exc:
                fixtures = []
                league_error = str(exc)
                failures += 1
                league_failures += 1
                if logger is not None:
                    logger.exception("[2H_BACKFILL_LEAGUE_ERR] league_id=%s season=%s error=%s", league_id, season, exc)

            if limit_fixtures is not None:
                fixtures = fixtures[-max(0, int(limit_fixtures)):]

            for raw_fixture in fixtures:
                fixture_id = _fixture_id(raw_fixture)
                if fixture_id is None:
                    continue

                try:
                    raw_events = client.fetch_fixture_events_response(int(fixture_id))
                    saved, record = collect_and_store_second_half_history(
                        raw_fixture,
                        raw_events,
                        path=history_path,
                    )
                    processed += 1
                    league_processed += 1

                    if saved:
                        stored += 1
                        league_stored += 1
                    else:
                        duplicates += 1
                        league_duplicates += 1

                    if logger is not None:
                        logger.info(
                            "[2H_DATA] fixture_id=%s events_available=%s ht=%s-%s ft=%s-%s goals_2h_total=%s after60=%s after75=%s",
                            record.get("fixture_id"),
                            record.get("events_available"),
                            record.get("ht_home"),
                            record.get("ht_away"),
                            record.get("ft_home"),
                            record.get("ft_away"),
                            record.get("goals_2h_total"),
                            record.get("goals_after_60_total"),
                            record.get("goals_after_75_total"),
                        )
                except Exception as exc:
                    processed += 1
                    league_processed += 1
                    failures += 1
                    league_failures += 1
                    if logger is not None:
                        logger.exception("[2H_BACKFILL_ERR] fixture_id=%s error=%s", fixture_id, exc)

                if sleep_seconds > 0:
                    time.sleep(float(sleep_seconds))

            leagues_summary.append(
                {
                    "league_id": int(league_id),
                    "season": int(season),
                    "fixtures_seen": len(fixtures),
                    "processed": league_processed,
                    "stored": league_stored,
                    "duplicates": league_duplicates,
                    "failures": league_failures,
                    "error": league_error,
                }
            )

    result: Dict[str, Any] = {
        "history_path": history_path,
        "processed": processed,
        "stored": stored,
        "duplicates": duplicates,
        "failures": failures,
        "leagues": leagues_summary,
    }
    if aggregate_after:
        result["aggregate"] = aggregate_history_to_files(history_path=history_path)
    return result
