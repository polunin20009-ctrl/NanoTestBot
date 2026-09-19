from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, Optional


TARGETS = ("next15", "to90")
COHORTS = ("all_decisions", "telegram_signals")
EXPANDED_BLEND_COHORT = "expanded_blend"


@dataclass(frozen=True)
class ReputationConfig:
    schema_version: int = 1
    half_life_days: float = 90.0
    prior_global: float = 12.0
    prior_league: float = 20.0
    prior_team: float = 30.0
    prior_team_league: float = 40.0
    prior_role: float = 45.0
    prior_pair: float = 60.0
    inferred_quality_weight: float = 0.90
    score_quality_weight: float = 1.00
    caps_pp: tuple[float, ...] = (2.0, 3.0, 5.0)
    expanded_blend_enabled: bool = False
    expanded_blend_telegram_weight: float = 0.75
    expanded_blend_next15_telegram_weight: float = 1.00
    expanded_blend_next15_recommended_cap_pp: float = 1.0
    expanded_blend_to90_recommended_cap_pp: float = 2.0
    expanded_channel_min_prob_to90: float = 75.0
    expanded_channel_min_reputation_delta_to90_pp: float = 1.5
    expanded_channel_min_adjusted_intensity: float = 0.55
    expanded_channel_min_season_context_factor: float = 1.02


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return int(default)
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return int(default)


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))


def _parse_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    raw = str(value).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def minute_bucket(minute: Any) -> str:
    value = _safe_int(minute, 0)
    if 46 <= value <= 49:
        return "46_49"
    if 50 <= value <= 53:
        return "50_53"
    if 54 <= value <= 57:
        return "54_57"
    if 58 <= value <= 60:
        return "58_60"
    if value > 60:
        return "61_PLUS"
    return "PRE_46"


def score_bucket(home: Any, away: Any) -> str:
    diff = _safe_int(home, 0) - _safe_int(away, 0)
    if diff == 0:
        return "DRAW"
    if diff == 1:
        return "HOME_LEAD_1"
    if diff == -1:
        return "AWAY_LEAD_1"
    return "HOME_LEAD_2_PLUS" if diff > 1 else "AWAY_LEAD_2_PLUS"


def _context_key(row: Mapping[str, Any]) -> str:
    return f"{minute_bucket(row.get('minute'))}|{score_bucket(row.get('score_home'), row.get('score_away'))}"


def _entity_key(value: Any) -> Optional[str]:
    parsed = _safe_int(value, 0)
    return str(parsed) if parsed > 0 else None


def _team_league_key(team_id: Any, league_id: Any) -> Optional[str]:
    team = _entity_key(team_id)
    league = _entity_key(league_id)
    return f"{team}:{league}" if team and league else None


def _pair_key(home_id: Any, away_id: Any) -> Optional[str]:
    home = _entity_key(home_id)
    away = _entity_key(away_id)
    return f"{home}:{away}" if home and away else None


def _is_telegram_signal(record: Mapping[str, Any]) -> bool:
    telegram = record.get("telegram")
    return bool(isinstance(telegram, Mapping) and telegram.get("send_ok"))


def _target_label(outcome: Mapping[str, Any], target: str) -> Optional[float]:
    key = "goal_within_15" if target == "next15" else "goal_to90_normal_time"
    value = outcome.get(key)
    if value is True or value == 1:
        return 1.0
    if value is False or value == 0:
        return 0.0
    return None


def _target_probability(record: Mapping[str, Any], target: str) -> float:
    probabilities = record.get("probabilities")
    probabilities = probabilities if isinstance(probabilities, Mapping) else {}
    key = "prob_next_15" if target == "next15" else "prob_to90"
    reputation_base_key = (
        "reputation_base_prob_next_15"
        if target == "next15"
        else "reputation_base_prob_to90"
    )
    value = probabilities.get(reputation_base_key)
    if value is None:
        value = probabilities.get(key)
    return _clamp(_safe_float(value, 0.0) / 100.0, 0.001, 0.999)


def _quality_weight(outcome: Mapping[str, Any], target: str, config: ReputationConfig) -> float:
    if target == "next15":
        quality = str(outcome.get("goal_within_15_quality") or "").lower()
        source = str(outcome.get("goal_within_15_source") or "").lower()
        if quality == "unknown" or source in {"unknown_timing", "score_timeline_boundary_gap"}:
            return 0.0
        if quality == "inferred":
            return config.inferred_quality_weight
        return 1.0
    source = str(outcome.get("goal_result_source") or "").lower()
    if source == "score_delta":
        return config.score_quality_weight
    return 1.0


def _time_weight(resolved_at: Optional[datetime], now: datetime, half_life_days: float) -> float:
    if resolved_at is None:
        return 1.0
    age_days = max(0.0, (now - resolved_at).total_seconds() / 86400.0)
    return math.pow(0.5, age_days / max(1.0, float(half_life_days)))


def _normalized_rows(
    records: Iterable[Mapping[str, Any]],
    target: str,
    cohort: str,
    config: ReputationConfig,
    now: datetime,
) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    fixture_counts: Dict[int, int] = defaultdict(int)
    provisional: list[Dict[str, Any]] = []
    for record in records:
        if not isinstance(record, Mapping):
            continue
        outcome = record.get("outcome")
        outcome = outcome if isinstance(outcome, Mapping) else {}
        if outcome.get("status") != "resolved":
            continue
        if cohort == "telegram_signals" and not _is_telegram_signal(record):
            continue
        label = _target_label(outcome, target)
        quality = _quality_weight(outcome, target, config)
        if label is None or quality <= 0.0:
            continue
        match = record.get("match")
        match = match if isinstance(match, Mapping) else {}
        fixture_id = _safe_int(record.get("fixture_id"), 0)
        resolved_at = _parse_dt(outcome.get("resolved_at_utc") or record.get("created_at_utc"))
        row = {
            "fixture_id": fixture_id,
            "minute": _safe_int(record.get("minute"), 0),
            "score_home": _safe_int(match.get("score_home"), 0),
            "score_away": _safe_int(match.get("score_away"), 0),
            "league_id": _safe_int(match.get("league_id"), 0),
            "home_team_id": _safe_int(match.get("home_team_id"), 0),
            "away_team_id": _safe_int(match.get("away_team_id"), 0),
            "label": label,
            "probability": _target_probability(record, target),
            "quality_weight": quality,
            "time_weight": _time_weight(resolved_at, now, config.half_life_days),
            "resolved_at": resolved_at.isoformat() if resolved_at else None,
        }
        provisional.append(row)
        fixture_counts[fixture_id] += 1

    for row in provisional:
        repeated = max(1, fixture_counts[row["fixture_id"]])
        row["weight"] = row["quality_weight"] * row["time_weight"] / repeated
        rows.append(row)
    return rows


class _Accumulator:
    def __init__(self) -> None:
        self.weight = 0.0
        self.error = 0.0
        self.observed = 0.0
        self.expected = 0.0
        self.raw_count = 0
        self.fixtures: set[int] = set()
        self.first_resolved_at: Optional[str] = None
        self.last_resolved_at: Optional[str] = None

    def add(self, row: Mapping[str, Any], residual: float, multiplier: float = 1.0) -> None:
        weight = _safe_float(row.get("weight"), 0.0) * float(multiplier)
        if weight <= 0.0:
            return
        self.weight += weight
        self.error += weight * float(residual)
        self.observed += weight * _safe_float(row.get("label"), 0.0)
        self.expected += weight * _safe_float(row.get("probability"), 0.0)
        self.raw_count += 1
        self.fixtures.add(_safe_int(row.get("fixture_id"), 0))
        resolved_at = row.get("resolved_at")
        if resolved_at:
            value = str(resolved_at)
            self.first_resolved_at = min(self.first_resolved_at, value) if self.first_resolved_at else value
            self.last_resolved_at = max(self.last_resolved_at, value) if self.last_resolved_at else value

    def payload(self, prior: float) -> Dict[str, Any]:
        denominator = self.weight + max(0.0, float(prior))
        delta = self.error / denominator if denominator > 0.0 else 0.0
        return {
            "raw_count": self.raw_count,
            "fixture_count": len(self.fixtures),
            "effective_weight": round(self.weight, 6),
            "confidence": round(self.weight / denominator, 6) if denominator > 0.0 else 0.0,
            "observed_rate": round(self.observed / self.weight, 6) if self.weight > 0.0 else None,
            "expected_rate": round(self.expected / self.weight, 6) if self.weight > 0.0 else None,
            "residual_delta": round(delta, 8),
            "delta_pp": round(delta * 100.0, 6),
            "first_resolved_at": self.first_resolved_at,
            "last_resolved_at": self.last_resolved_at,
        }


def _payload_delta(container: Mapping[str, Any], key: Optional[str]) -> float:
    if not key:
        return 0.0
    payload = container.get(key)
    return _safe_float(payload.get("residual_delta"), 0.0) if isinstance(payload, Mapping) else 0.0


def _aggregate_target(rows: list[Dict[str, Any]], config: ReputationConfig) -> Dict[str, Any]:
    overall_acc = _Accumulator()
    context_acc: Dict[str, _Accumulator] = defaultdict(_Accumulator)
    for row in rows:
        residual = row["label"] - row["probability"]
        overall_acc.add(row, residual)
        context_acc[_context_key(row)].add(row, residual)
    overall = overall_acc.payload(config.prior_global)
    contexts = {key: acc.payload(config.prior_global) for key, acc in context_acc.items()}

    league_acc: Dict[str, _Accumulator] = defaultdict(_Accumulator)
    for row in rows:
        global_delta = _payload_delta(contexts, _context_key(row))
        residual = row["label"] - _clamp(row["probability"] + global_delta, 0.001, 0.999)
        league_key = _entity_key(row["league_id"])
        if league_key:
            league_acc[league_key].add(row, residual)
    leagues = {key: acc.payload(config.prior_league) for key, acc in league_acc.items()}

    team_acc: Dict[str, _Accumulator] = defaultdict(_Accumulator)
    for row in rows:
        base = row["probability"]
        base += _payload_delta(contexts, _context_key(row))
        base += _payload_delta(leagues, _entity_key(row["league_id"]))
        residual = row["label"] - _clamp(base, 0.001, 0.999)
        for team_id in (row["home_team_id"], row["away_team_id"]):
            key = _entity_key(team_id)
            if key:
                team_acc[key].add(row, residual)
    teams = {key: acc.payload(config.prior_team) for key, acc in team_acc.items()}

    team_league_acc: Dict[str, _Accumulator] = defaultdict(_Accumulator)
    for row in rows:
        team_deltas = [
            _payload_delta(teams, _entity_key(row["home_team_id"])),
            _payload_delta(teams, _entity_key(row["away_team_id"])),
        ]
        base = row["probability"]
        base += _payload_delta(contexts, _context_key(row))
        base += _payload_delta(leagues, _entity_key(row["league_id"]))
        base += sum(team_deltas) / 2.0
        residual = row["label"] - _clamp(base, 0.001, 0.999)
        for team_id in (row["home_team_id"], row["away_team_id"]):
            key = _team_league_key(team_id, row["league_id"])
            if key:
                team_league_acc[key].add(row, residual)
    team_leagues = {
        key: acc.payload(config.prior_team_league)
        for key, acc in team_league_acc.items()
    }

    role_acc: Dict[str, _Accumulator] = defaultdict(_Accumulator)
    pair_acc: Dict[str, _Accumulator] = defaultdict(_Accumulator)
    for row in rows:
        team_ids = (row["home_team_id"], row["away_team_id"])
        team_deltas = [_payload_delta(teams, _entity_key(team_id)) for team_id in team_ids]
        team_league_deltas = [
            _payload_delta(team_leagues, _team_league_key(team_id, row["league_id"]))
            for team_id in team_ids
        ]
        base = row["probability"]
        base += _payload_delta(contexts, _context_key(row))
        base += _payload_delta(leagues, _entity_key(row["league_id"]))
        base += sum(team_deltas) / 2.0
        base += sum(team_league_deltas) / 2.0
        residual = row["label"] - _clamp(base, 0.001, 0.999)
        home_key = _entity_key(row["home_team_id"])
        away_key = _entity_key(row["away_team_id"])
        if home_key:
            role_acc[f"{home_key}:home"].add(row, residual)
        if away_key:
            role_acc[f"{away_key}:away"].add(row, residual)
        pair_key = _pair_key(row["home_team_id"], row["away_team_id"])
        if pair_key:
            pair_acc[pair_key].add(row, residual)

    return {
        "rows": len(rows),
        "fixtures": len({row["fixture_id"] for row in rows}),
        "overall": overall,
        "contexts": contexts,
        "leagues": leagues,
        "teams": teams,
        "team_leagues": team_leagues,
        "roles": {key: acc.payload(config.prior_role) for key, acc in role_acc.items()},
        # Pair statistics are collected for research, but never applied.
        "pairs": {key: acc.payload(config.prior_pair) for key, acc in pair_acc.items()},
    }


def build_reputation_model(
    records: Iterable[Mapping[str, Any]],
    config: Optional[ReputationConfig] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    config = config or ReputationConfig()
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    records_list = [record for record in records if isinstance(record, Mapping)]
    cohorts: Dict[str, Any] = {}
    for cohort in COHORTS:
        targets: Dict[str, Any] = {}
        for target in TARGETS:
            rows = _normalized_rows(records_list, target, cohort, config, current)
            targets[target] = _aggregate_target(rows, config)
        cohorts[cohort] = {"targets": targets}
    return {
        "schema_version": config.schema_version,
        "generated_at_utc": current.astimezone(timezone.utc).isoformat(),
        "shadow_only": True,
        "config": asdict(config),
        "cohorts": cohorts,
    }


def _target_projection(
    snapshot: Mapping[str, Any],
    target_model: Mapping[str, Any],
    target: str,
    caps_pp: tuple[float, ...],
) -> Dict[str, Any]:
    match = snapshot.get("match")
    match = match if isinstance(match, Mapping) else {}
    base = _target_probability(snapshot, target) * 100.0
    contexts = target_model.get("contexts") if isinstance(target_model.get("contexts"), Mapping) else {}
    leagues = target_model.get("leagues") if isinstance(target_model.get("leagues"), Mapping) else {}
    teams = target_model.get("teams") if isinstance(target_model.get("teams"), Mapping) else {}
    team_leagues = target_model.get("team_leagues") if isinstance(target_model.get("team_leagues"), Mapping) else {}
    roles = target_model.get("roles") if isinstance(target_model.get("roles"), Mapping) else {}

    row = {
        "minute": snapshot.get("minute"),
        "score_home": match.get("score_home"),
        "score_away": match.get("score_away"),
    }
    league_key = _entity_key(match.get("league_id"))
    home_key = _entity_key(match.get("home_team_id"))
    away_key = _entity_key(match.get("away_team_id"))
    global_delta = _payload_delta(contexts, _context_key(row)) * 100.0
    league_delta = _payload_delta(leagues, league_key) * 100.0
    home_team_delta = _payload_delta(teams, home_key) * 100.0
    away_team_delta = _payload_delta(teams, away_key) * 100.0
    home_league_delta = _payload_delta(
        team_leagues, _team_league_key(match.get("home_team_id"), match.get("league_id"))
    ) * 100.0
    away_league_delta = _payload_delta(
        team_leagues, _team_league_key(match.get("away_team_id"), match.get("league_id"))
    ) * 100.0
    home_role_delta = _payload_delta(roles, f"{home_key}:home" if home_key else None) * 100.0
    away_role_delta = _payload_delta(roles, f"{away_key}:away" if away_key else None) * 100.0
    raw_delta = (
        global_delta
        + league_delta
        + (home_team_delta + away_team_delta) / 2.0
        + (home_league_delta + away_league_delta) / 2.0
        + (home_role_delta + away_role_delta) / 2.0
    )
    projections: Dict[str, float] = {}
    applied: Dict[str, float] = {}
    for cap in caps_pp:
        cap_key = str(int(cap)) if float(cap).is_integer() else str(cap)
        applied_delta = _clamp(raw_delta, -float(cap), float(cap))
        applied[cap_key] = round(applied_delta, 6)
        projections[cap_key] = round(_clamp(base + applied_delta, 0.0, 100.0), 6)
    return {
        "available": bool(_safe_int(target_model.get("fixtures"), 0) > 0),
        "base_probability": round(base, 6),
        "components_pp": {
            "global_context": round(global_delta, 6),
            "league": round(league_delta, 6),
            "home_team": round(home_team_delta, 6),
            "away_team": round(away_team_delta, 6),
            "home_team_league": round(home_league_delta, 6),
            "away_team_league": round(away_league_delta, 6),
            "home_role": round(home_role_delta, 6),
            "away_role": round(away_role_delta, 6),
        },
        "raw_delta_pp": round(raw_delta, 6),
        "applied_delta_pp_by_cap": applied,
        "probability_by_cap": projections,
        "sample": {
            "target_rows": _safe_int(target_model.get("rows"), 0),
            "target_fixtures": _safe_int(target_model.get("fixtures"), 0),
            "league_effective_weight": _safe_float(
                (leagues.get(league_key) or {}).get("effective_weight") if league_key else 0.0, 0.0
            ),
            "home_team_effective_weight": _safe_float(
                (teams.get(home_key) or {}).get("effective_weight") if home_key else 0.0, 0.0
            ),
            "away_team_effective_weight": _safe_float(
                (teams.get(away_key) or {}).get("effective_weight") if away_key else 0.0, 0.0
            ),
        },
    }


def build_expanded_blend_cohort(
    cohorts: Mapping[str, Any],
    *,
    caps_pp: tuple[float, ...] = (2.0, 3.0, 5.0),
    telegram_weight: float = 0.75,
    next15_telegram_weight: float = 1.00,
    next15_recommended_cap_pp: float = 1.0,
    to90_recommended_cap_pp: float = 2.0,
) -> Dict[str, Any]:
    """Build a shadow-only convex blend without double-counting corrections.

    Telegram signals are a subset of all decisions, so their deltas must never
    be added together. The raw estimates are blended first and a single cap is
    applied afterwards. Missing cohorts fall back to the cohort that is present.
    """
    all_cohort = cohorts.get("all_decisions")
    all_cohort = all_cohort if isinstance(all_cohort, Mapping) else {}
    telegram_cohort = cohorts.get("telegram_signals")
    telegram_cohort = telegram_cohort if isinstance(telegram_cohort, Mapping) else {}
    all_targets = all_cohort.get("targets")
    all_targets = all_targets if isinstance(all_targets, Mapping) else {}
    telegram_targets = telegram_cohort.get("targets")
    telegram_targets = telegram_targets if isinstance(telegram_targets, Mapping) else {}
    configured_weights = {
        "next15": _clamp(
            _safe_float(next15_telegram_weight, 1.00),
            0.0,
            1.0,
        ),
        "to90": _clamp(_safe_float(telegram_weight, 0.75), 0.0, 1.0),
    }
    recommended_caps = {
        "next15": max(0.0, _safe_float(next15_recommended_cap_pp, 1.0)),
        "to90": max(0.0, _safe_float(to90_recommended_cap_pp, 2.0)),
    }

    blended_targets: Dict[str, Any] = {}
    modes: set[str] = set()

    def target_available(target_payload: Mapping[str, Any]) -> bool:
        if "available" in target_payload:
            return bool(target_payload.get("available"))
        sample_payload = target_payload.get("sample")
        sample_payload = (
            sample_payload if isinstance(sample_payload, Mapping) else {}
        )
        if "target_fixtures" in sample_payload:
            return _safe_int(sample_payload.get("target_fixtures"), 0) > 0
        return bool(target_payload) and "base_probability" in target_payload

    def target_raw_delta(target_payload: Mapping[str, Any]) -> float:
        if "raw_delta_pp" in target_payload:
            return _safe_float(target_payload.get("raw_delta_pp"), 0.0)
        applied = target_payload.get("applied_delta_pp_by_cap")
        applied = applied if isinstance(applied, Mapping) else {}
        probabilities = target_payload.get("probability_by_cap")
        probabilities = probabilities if isinstance(probabilities, Mapping) else {}
        cap_keys: set[str] = {str(key) for key in applied}
        cap_keys.update(str(key) for key in probabilities)
        if not cap_keys:
            return 0.0
        largest_key = max(
            cap_keys,
            key=lambda key: abs(_safe_float(key, 0.0)),
        )
        if largest_key in applied:
            return _safe_float(applied.get(largest_key), 0.0)
        base_probability = _safe_float(
            target_payload.get("base_probability"),
            0.0,
        )
        return (
            _safe_float(probabilities.get(largest_key), base_probability)
            - base_probability
        )

    for target in TARGETS:
        configured_weight = configured_weights[target]
        all_target = all_targets.get(target)
        all_target = all_target if isinstance(all_target, Mapping) else {}
        telegram_target = telegram_targets.get(target)
        telegram_target = telegram_target if isinstance(telegram_target, Mapping) else {}
        all_sample = all_target.get("sample")
        all_sample = all_sample if isinstance(all_sample, Mapping) else {}
        telegram_sample = telegram_target.get("sample")
        telegram_sample = telegram_sample if isinstance(telegram_sample, Mapping) else {}
        all_fixtures = _safe_int(all_sample.get("target_fixtures"), 0)
        telegram_fixtures = _safe_int(telegram_sample.get("target_fixtures"), 0)
        all_available = target_available(all_target)
        telegram_available = target_available(telegram_target)

        if all_available and telegram_available:
            mode = "convex_blend"
            effective_telegram_weight = configured_weight
        elif all_available:
            mode = "all_decisions_only"
            effective_telegram_weight = 0.0
        elif telegram_available:
            mode = "legacy_telegram_only"
            effective_telegram_weight = 1.0
        else:
            mode = "neutral"
            effective_telegram_weight = 0.0
        modes.add(mode)

        source_target = (
            all_target
            if all_available
            else telegram_target if telegram_available else all_target or telegram_target
        )
        base = _safe_float(source_target.get("base_probability"), 0.0)
        all_raw = target_raw_delta(all_target)
        telegram_raw = target_raw_delta(telegram_target)
        if mode == "convex_blend":
            raw_delta = (
                (1.0 - effective_telegram_weight) * all_raw
                + effective_telegram_weight * telegram_raw
            )
        elif mode == "all_decisions_only":
            raw_delta = all_raw
        elif mode == "legacy_telegram_only":
            raw_delta = telegram_raw
        else:
            raw_delta = 0.0

        applied_by_cap: Dict[str, float] = {}
        probability_by_cap: Dict[str, float] = {}
        for cap in caps_pp:
            cap_value = abs(float(cap))
            cap_key = str(int(cap_value)) if cap_value.is_integer() else str(cap_value)
            applied_delta = _clamp(raw_delta, -cap_value, cap_value)
            applied_by_cap[cap_key] = round(applied_delta, 6)
            probability_by_cap[cap_key] = round(
                _clamp(base + applied_delta, 0.0, 100.0),
                6,
            )

        recommended_cap = recommended_caps[target]
        recommended_delta = _clamp(
            raw_delta,
            -recommended_cap,
            recommended_cap,
        )
        recommended_probability = _clamp(
            base + recommended_delta,
            0.0,
            100.0,
        )

        blended_targets[target] = {
            "base_probability": round(base, 6),
            "raw_delta_pp": round(raw_delta, 6),
            "applied_delta_pp_by_cap": applied_by_cap,
            "probability_by_cap": probability_by_cap,
            "recommended_cap_pp": round(recommended_cap, 6),
            "recommended_applied_delta_pp": round(recommended_delta, 6),
            "recommended_probability": round(recommended_probability, 6),
            "sample": {
                "target_rows": _safe_int(
                    (all_sample if all_available else telegram_sample).get(
                        "target_rows"
                    ),
                    0,
                ),
                "target_fixtures": (
                    all_fixtures if all_available else telegram_fixtures
                ),
                "all_decisions_fixtures": all_fixtures,
                "telegram_signals_fixtures": telegram_fixtures,
            },
            "blend": {
                "version": "convex_v1",
                "mode": mode,
                "configured_telegram_weight": round(configured_weight, 6),
                "effective_telegram_weight": round(effective_telegram_weight, 6),
                "all_decisions_raw_delta_pp": round(all_raw, 6),
                "telegram_signals_raw_delta_pp": round(telegram_raw, 6),
            },
        }

    return {
        "targets": blended_targets,
        "decision_by_cap": {},
        "shadow_only": True,
        "production_apply": False,
        "blend_version": "convex_v1",
        "mode": next(iter(modes)) if len(modes) == 1 else "target_specific",
        "configured_telegram_weights": {
            target: round(weight, 6)
            for target, weight in configured_weights.items()
        },
        "recommended_caps_pp": {
            target: round(cap, 6) for target, cap in recommended_caps.items()
        },
    }


def evaluate_shadow_decision(
    snapshot: Mapping[str, Any],
    model: Mapping[str, Any],
    config: Optional[ReputationConfig] = None,
) -> Dict[str, Any]:
    config = config or ReputationConfig()
    decision = snapshot.get("decision")
    decision = decision if isinstance(decision, Mapping) else {}
    gates = snapshot.get("gates")
    gates = gates if isinstance(gates, Mapping) else {}
    cohorts_model = model.get("cohorts") if isinstance(model.get("cohorts"), Mapping) else {}
    output_cohorts: Dict[str, Any] = {}
    for cohort in COHORTS:
        cohort_model = cohorts_model.get(cohort)
        cohort_model = cohort_model if isinstance(cohort_model, Mapping) else {}
        target_models = cohort_model.get("targets")
        target_models = target_models if isinstance(target_models, Mapping) else {}
        target_output = {
            target: _target_projection(
                snapshot,
                target_models.get(target) if isinstance(target_models.get(target), Mapping) else {},
                target,
                config.caps_pp,
            )
            for target in TARGETS
        }
        decisions: Dict[str, str] = {}
        common_gates_passed = (
            bool(gates.get("readiness_passed", True))
            and bool(gates.get("anti_garbage_passed", True))
            and (not bool(gates.get("live_gate_required", False)) or bool(gates.get("live_gate_passed", False)))
            and bool(gates.get("other_hard_gates_passed", True))
        )
        threshold_next15 = _safe_float(decision.get("threshold_next15"), 0.0)
        threshold_to90 = _safe_float(decision.get("selected_prob_to90_threshold"), 0.0)
        for cap in config.caps_pp:
            cap_key = str(int(cap)) if float(cap).is_integer() else str(cap)
            passes = (
                target_output["next15"]["probability_by_cap"][cap_key] >= threshold_next15
                and target_output["to90"]["probability_by_cap"][cap_key] >= threshold_to90
                and common_gates_passed
            )
            decisions[cap_key] = "ALLOW" if passes else "BLOCK"
        output_cohorts[cohort] = {
            "targets": target_output,
            "decision_by_cap": decisions,
        }
    if config.expanded_blend_enabled:
        expanded = build_expanded_blend_cohort(
            output_cohorts,
            caps_pp=config.caps_pp,
            telegram_weight=config.expanded_blend_telegram_weight,
            next15_telegram_weight=(
                config.expanded_blend_next15_telegram_weight
            ),
            next15_recommended_cap_pp=(
                config.expanded_blend_next15_recommended_cap_pp
            ),
            to90_recommended_cap_pp=(
                config.expanded_blend_to90_recommended_cap_pp
            ),
        )
        expanded_targets = expanded.get("targets") or {}
        expanded_decisions: Dict[str, str] = {}
        for cap in config.caps_pp:
            cap_key = str(int(cap)) if float(cap).is_integer() else str(cap)
            passes = (
                ((expanded_targets.get("next15") or {}).get("probability_by_cap") or {}).get(cap_key, 0.0)
                >= threshold_next15
                and ((expanded_targets.get("to90") or {}).get("probability_by_cap") or {}).get(cap_key, 0.0)
                >= threshold_to90
                and common_gates_passed
            )
            expanded_decisions[cap_key] = "ALLOW" if passes else "BLOCK"
        expanded["decision_by_cap"] = expanded_decisions
        recommended_next15 = _safe_float(
            (expanded_targets.get("next15") or {}).get(
                "recommended_probability"
            ),
            0.0,
        )
        recommended_to90 = _safe_float(
            (expanded_targets.get("to90") or {}).get(
                "recommended_probability"
            ),
            0.0,
        )
        channel_filter = snapshot.get("channel_signal_filter")
        channel_filter = (
            channel_filter if isinstance(channel_filter, Mapping) else {}
        )
        probabilities = snapshot.get("probabilities")
        probabilities = (
            probabilities if isinstance(probabilities, Mapping) else {}
        )
        factors = snapshot.get("factors")
        factors = factors if isinstance(factors, Mapping) else {}
        min_probability = _safe_float(
            channel_filter.get("min_prob_to90"),
            config.expanded_channel_min_prob_to90,
        )
        min_reputation_delta = _safe_float(
            channel_filter.get("min_reputation_delta_to90_pp"),
            config.expanded_channel_min_reputation_delta_to90_pp,
        )
        min_adjusted_intensity = _safe_float(
            channel_filter.get("min_adjusted_intensity"),
            config.expanded_channel_min_adjusted_intensity,
        )
        min_season_context_factor = _safe_float(
            channel_filter.get("min_season_context_factor"),
            config.expanded_channel_min_season_context_factor,
        )
        reputation_base = channel_filter.get("reputation_base_prob_to90")
        if reputation_base is None:
            reputation_base = probabilities.get("reputation_base_prob_to90")
        adjusted_intensity = channel_filter.get("adjusted_intensity")
        if adjusted_intensity is None:
            adjusted_intensity = factors.get("adjusted_intensity")
        season_context_factor = channel_filter.get("season_context_factor")
        if season_context_factor is None:
            season_context_factor = factors.get("season_context_factor_45p")
        reputation_base_available = reputation_base is not None
        adjusted_intensity_available = adjusted_intensity is not None
        season_context_available = season_context_factor is not None
        projected_reputation_delta = (
            recommended_to90 - _safe_float(reputation_base, recommended_to90)
        )
        recommended_channel_filter_passed = (
            recommended_to90 >= min_probability
            and reputation_base_available
            and projected_reputation_delta >= min_reputation_delta
            and adjusted_intensity_available
            and _safe_float(adjusted_intensity, -1.0)
            >= min_adjusted_intensity
            and season_context_available
            and _safe_float(season_context_factor, -1.0)
            >= min_season_context_factor
        )
        publication_context_passed = (
            bool(gates.get("readiness_passed", True))
            and bool(gates.get("other_hard_gates_passed", True))
        )
        recommended_passes = (
            publication_context_passed
            and recommended_channel_filter_passed
        )
        expanded["recommended_decision"] = (
            "ALLOW" if recommended_passes else "BLOCK"
        )
        expanded["recommended_channel_filter_passed"] = bool(
            recommended_channel_filter_passed
        )
        expanded["recommended_reputation_delta_to90_pp"] = round(
            projected_reputation_delta,
            6,
        )
        output_cohorts[EXPANDED_BLEND_COHORT] = expanded
    return {
        "schema_version": config.schema_version,
        "shadow_only": True,
        "model_generated_at_utc": model.get("generated_at_utc"),
        "baseline_decision": str(decision.get("final_decision") or "BLOCK"),
        "cohorts": output_cohorts,
    }
