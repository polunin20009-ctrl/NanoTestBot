"""Versioned finite expression library; no eval, fitted transform or labels.

Thresholds and conjunctions of these expressions are learned by discovery.
Existing schema-2 features retain their definitions and allowlists unchanged.
"""
from itertools import combinations
import math
from typing import Mapping

from market_benchmark.research import market_values

PRIMITIVES = {
    "pressure": ("feature.pressure_index", 100.0),
    "intensity": ("feature.adjusted_intensity", 1.0),
    "season": ("feature.season_context_factor", 1.0),
    "game_state": ("feature.game_state_factor", 1.0),
    "xg": ("raw.xg_total", 10.0),
    "box_shots": ("raw.shots_in_box_total", 30.0),
    "pace5": ("rolling.5m.rate_per_minute.total_shots_total", 1.0),
    "pace10": ("rolling.10m.rate_per_minute.total_shots_total", 1.0),
    "bot_p90": ("bot.prob_to90", 100.0),
    "static_p90": ("ml.static.prob_to90", 100.0),
    "rolling_p90": ("ml.rolling.prob_to90", 100.0),
    "market_p90": ("market.v1.prob_to90", 100.0),
}
PRODUCTS = {f"nonlinear.v1.product.{a}.{b}": (a, b)
            for a, b in combinations(sorted(PRIMITIVES), 2)}
SIDE_METRICS = ("xg", "shots_on_target", "shots_in_box", "total_shots", "corners", "dangerous_attacks")
MARKET_FEATURE_NAMES = tuple([
    "market.v1.prob_to90", "market.v1.over_decimal", "market.v1.overround",
    "market.v1.bot_gap_pp", "market.v1.static_gap_pp", "market.v1.rolling_gap_pp",
    *[f"market.v1.{kind}_{m}{suffix}" for m in (5, 10)
      for kind, suffix in (("delta", "m_pp"), ("odds_ratio", "m"))],
])
NONLINEAR_FEATURE_NAMES = tuple([
    *PRODUCTS,
    *[f"nonlinear.v1.{op}.{m}" for m in SIDE_METRICS
      for op in ("home_share", "away_share", "home_away_balance", "trailing_share")],
    "nonlinear.v1.pace5_over_pace10", "nonlinear.v1.box_share",
    "nonlinear.v1.on_target_share", "nonlinear.v1.xg_per_shot",
])
EXTENDED_FEATURE_NAMES = MARKET_FEATURE_NAMES + NONLINEAR_FEATURE_NAMES
EXTENDED_FEATURES = frozenset(EXTENDED_FEATURE_NAMES)


def has_market_dependency(name: str) -> bool:
    return name.startswith("market.v1.") or name in PRODUCTS and "market_p90" in PRODUCTS[name]


def extend_values(snapshot: Mapping, values: dict) -> None:
    values.update({name: None for name in EXTENDED_FEATURE_NAMES})
    values.update(market_values(snapshot))
    def ratio(a, b):
        return a / b if a is not None and b is not None and b > 1e-9 else None
    for label, source in (("bot", "bot.prob_to90"), ("static", "ml.static.prob_to90"), ("rolling", "ml.rolling.prob_to90")):
        a, b = values.get(source), values.get("market.v1.prob_to90")
        if a is not None and b is not None:
            values[f"market.v1.{label}_gap_pp"] = a - b
    for name, (a, b) in PRODUCTS.items():
        ka, sa = PRIMITIVES[a]; kb, sb = PRIMITIVES[b]
        va, vb = values.get(ka), values.get(kb)
        if va is not None and vb is not None:
            values[name] = va / sa * (vb / sb)
    goal_diff = values.get("score.goal_difference")
    for metric in SIDE_METRICS:
        home, away = values.get(f"raw.{metric}_home"), values.get(f"raw.{metric}_away")
        if home is None or away is None or home < 0 or away < 0:
            continue
        total = home + away
        values[f"nonlinear.v1.home_share.{metric}"] = ratio(home, total)
        values[f"nonlinear.v1.away_share.{metric}"] = ratio(away, total)
        values[f"nonlinear.v1.home_away_balance.{metric}"] = ratio(home - away, total)
        if goal_diff is not None and goal_diff != 0:
            values[f"nonlinear.v1.trailing_share.{metric}"] = ratio(home if goal_diff < 0 else away, total)
    for name, a, b in (
        ("pace5_over_pace10", "rolling.5m.rate_per_minute.total_shots_total", "rolling.10m.rate_per_minute.total_shots_total"),
        ("box_share", "raw.shots_in_box_total", "raw.total_shots_total"),
        ("on_target_share", "raw.shots_on_target_total", "raw.total_shots_total"),
        ("xg_per_shot", "raw.xg_total", "raw.total_shots_total"),
    ):
        values[f"nonlinear.v1.{name}"] = ratio(values.get(a), values.get(b))
    for name in EXTENDED_FEATURE_NAMES:
        value = values[name]
        if value is not None and not math.isfinite(value):
            values[name] = None
