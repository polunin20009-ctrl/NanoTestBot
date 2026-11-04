"""Causal team / competition context features for prospective research.

These are class-of-situation signals (strength mix, trailing favourite, cup
context), never club-name or H2H identity features.  Raw inputs are persisted
on the observation; derived numeric atoms are allowlisted for discovery.
"""
from __future__ import annotations

import math
import re
from typing import Any, Mapping, Optional

TEAM_CONTEXT_SCHEMA_VERSION = 1
MIN_TEAM_SAMPLE_MATCHES = 3
FAVORITE_EDGE_EPS = 0.02

TEAM_CONTEXT_FEATURE_NAMES = (
    "team_context.v1.attack_mix",
    "team_context.v1.trailing_favorite",
    "team_context.v1.away_urgency",
    "competition.v1.cup_or_europe",
    "matchup.v1.quality_gap",
)
TEAM_CONTEXT_FEATURES = frozenset(TEAM_CONTEXT_FEATURE_NAMES)

_EURO_OR_CONTINENTAL_RE = re.compile(
    r"("
    r"champions\s+league|"
    r"europa\s+league|"
    r"conference\s+league|"
    r"\buefa\b|"
    r"afc\s+champions|"
    r"copa\s+libertadores|"
    r"copa\s+sudamericana"
    r")",
    re.IGNORECASE,
)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _finite(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return 0.0 if number == 0.0 else number


def _nonneg_int(value: Any) -> Optional[int]:
    number = _finite(value)
    if number is None or number < 0.0 or not float(number).is_integer():
        return None
    return int(number)


def _truthy_cup(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "cup"}:
        return True
    if text in {"0", "false", "no", "league"}:
        return False
    return None


def build_team_context_payload(
    *,
    home_attack_factor: Any = None,
    home_defense_factor: Any = None,
    away_attack_factor: Any = None,
    away_defense_factor: Any = None,
    team_mix_factor: Any = None,
    home_matches: Any = None,
    away_matches: Any = None,
    league_factor: Any = None,
) -> dict[str, Any]:
    """Persist only causal pre-outcome inputs used by team-context atoms."""

    return {
        "schema_version": TEAM_CONTEXT_SCHEMA_VERSION,
        "home_attack_factor": _finite(home_attack_factor),
        "home_defense_factor": _finite(home_defense_factor),
        "away_attack_factor": _finite(away_attack_factor),
        "away_defense_factor": _finite(away_defense_factor),
        "team_mix_factor": _finite(team_mix_factor),
        "home_matches": _nonneg_int(home_matches),
        "away_matches": _nonneg_int(away_matches),
        "league_factor": _finite(league_factor),
    }


def _resolve_inputs(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    block = _mapping(snapshot.get("team_context"))
    features = _mapping(snapshot.get("features"))
    match = _mapping(snapshot.get("match"))

    def pick(*keys: str) -> Any:
        for key in keys:
            if key in block and block.get(key) is not None:
                return block.get(key)
            if key in features and features.get(key) is not None:
                return features.get(key)
        return None

    return {
        "home_attack": _finite(pick("home_attack_factor")),
        "home_defense": _finite(pick("home_defense_factor")),
        "away_attack": _finite(pick("away_attack_factor")),
        "away_defense": _finite(pick("away_defense_factor")),
        "team_mix": _finite(pick("team_mix_factor")),
        "home_matches": _nonneg_int(pick("home_matches")),
        "away_matches": _nonneg_int(pick("away_matches")),
        "score_home": _nonneg_int(match.get("score_home")),
        "score_away": _nonneg_int(match.get("score_away")),
        "is_cup": _truthy_cup(match.get("is_cup")),
        "league_type": str(match.get("league_type") or "").strip(),
        "league_name": str(match.get("league_name") or "").strip(),
    }


def _sample_ok(home_matches: Optional[int], away_matches: Optional[int]) -> bool:
    return (
        home_matches is not None
        and away_matches is not None
        and home_matches >= MIN_TEAM_SAMPLE_MATCHES
        and away_matches >= MIN_TEAM_SAMPLE_MATCHES
    )


def _cup_or_europe(
    *,
    is_cup: Optional[bool],
    league_type: str,
    league_name: str,
) -> Optional[float]:
    if is_cup is True or league_type.lower() == "cup":
        return 1.0
    if league_name and _EURO_OR_CONTINENTAL_RE.search(league_name):
        return 1.0
    if is_cup is False and league_type.lower() == "league":
        # Explicit non-cup league with no continental name hit.
        if league_name and not _EURO_OR_CONTINENTAL_RE.search(league_name):
            return 0.0
        if not league_name:
            return 0.0
    if is_cup is None and not league_type and not league_name:
        return None
    if league_type.lower() == "league" and not _EURO_OR_CONTINENTAL_RE.search(league_name):
        return 0.0
    return None


def compute_team_context_values(snapshot: Mapping[str, Any]) -> dict[str, Optional[float]]:
    """Derive allowlisted team/competition atoms from a causal snapshot."""

    values: dict[str, Optional[float]] = {name: None for name in TEAM_CONTEXT_FEATURE_NAMES}
    if not isinstance(snapshot, Mapping):
        return values

    inputs = _resolve_inputs(snapshot)
    values["competition.v1.cup_or_europe"] = _cup_or_europe(
        is_cup=inputs["is_cup"],
        league_type=inputs["league_type"],
        league_name=inputs["league_name"],
    )

    score_home = inputs["score_home"]
    score_away = inputs["score_away"]
    if score_home is not None and score_away is not None and score_away < score_home:
        values["team_context.v1.away_urgency"] = 1.0
    elif score_home is not None and score_away is not None:
        values["team_context.v1.away_urgency"] = 0.0

    if not _sample_ok(inputs["home_matches"], inputs["away_matches"]):
        return values

    home_attack = inputs["home_attack"]
    home_defense = inputs["home_defense"]
    away_attack = inputs["away_attack"]
    away_defense = inputs["away_defense"]
    if None in (home_attack, home_defense, away_attack, away_defense):
        return values

    home_edge = float(home_attack) * float(away_defense)
    away_edge = float(away_attack) * float(home_defense)
    attack_mix = inputs["team_mix"]
    if attack_mix is None:
        attack_mix = (home_edge + away_edge) / 2.0
    values["team_context.v1.attack_mix"] = float(attack_mix)
    values["matchup.v1.quality_gap"] = abs(home_edge - away_edge)

    if score_home is None or score_away is None:
        return values

    if home_edge > away_edge + FAVORITE_EDGE_EPS:
        values["team_context.v1.trailing_favorite"] = (
            1.0 if score_home < score_away else 0.0
        )
    elif away_edge > home_edge + FAVORITE_EDGE_EPS:
        values["team_context.v1.trailing_favorite"] = (
            1.0 if score_away < score_home else 0.0
        )
    else:
        values["team_context.v1.trailing_favorite"] = 0.0
    return values


def extend_team_context_values(
    snapshot: Mapping[str, Any], values: dict[str, Optional[float]]
) -> None:
    """Write team-context atoms into an extract_features values dict."""

    computed = compute_team_context_values(snapshot)
    for name in TEAM_CONTEXT_FEATURE_NAMES:
        values[name] = computed.get(name)
