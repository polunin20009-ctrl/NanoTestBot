"""Rare-precision worker summary must match the parent profile validator."""
from __future__ import annotations

from pathlib import Path

import NanoTest as bot
from scripts import run_wide_research_cycle as cycle_cli
from wide_research.controller import (
    POOL_SELECTION_MIN_HIT_RATE,
    WideResearchController,
)
from wide_research.lifecycle import LifecyclePolicy
from wide_research.store import WideResearchStore


def test_rare_precision_worker_terminal_review_matches_parent_validator(
    tmp_path: Path,
) -> None:
    store = WideResearchStore(
        str(tmp_path / "rare.sqlite3"),
        allowed_root=tmp_path,
    )
    store.bind_profile("rare_precision_shadow")
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc=bot.WIDE_RESEARCH_RARE_PRECISION_PROSPECTIVE_START_UTC,
        production_enabled=False,
        max_shadow_rules=bot.WIDE_RESEARCH_RARE_PRECISION_MAX_SHADOW_RULES,
        policy=LifecyclePolicy(
            min_triggers_per_week=0.0,
            allowed_looks=(
                50,
                100,
                200,
                300,
                400,
                500,
                600,
                750,
                800,
                1000,
            ),
        ),
        terminal_review_enabled=True,
        terminal_review_min_hit_rate=POOL_SELECTION_MIN_HIT_RATE,
        pool_lifecycle_policy={
            "version": "wide_pool_lifecycle_v2",
            "selection_look": 200,
            "selection_min_hit_rate": POOL_SELECTION_MIN_HIT_RATE,
            "terminal_look": 1000,
            "max_age_days": 365,
        },
    )

    terminal_review = controller._terminal_review_manifest()
    lifecycle_policy = controller.policy.manifest()
    lifecycle_policy["allowed_looks"] = list(controller.policy.allowed_looks)
    profile_contract = {
        "max_shadow_rules": int(
            bot.WIDE_RESEARCH_RARE_PRECISION_MAX_SHADOW_RULES
        ),
        "lifecycle_policy": lifecycle_policy,
        "pool_lifecycle": dict(controller.pool_lifecycle_policy or {}),
        "terminal_review": {
            "enabled": bool(terminal_review["enabled"]),
            "final_look": int(terminal_review["final_look"]),
            "min_point_hit_rate": float(terminal_review["min_point_hit_rate"]),
        },
    }

    assert profile_contract["terminal_review"]["min_point_hit_rate"] == (
        POOL_SELECTION_MIN_HIT_RATE
    )
    assert profile_contract["terminal_review"]["min_point_hit_rate"] != 0.90
    assert profile_contract["terminal_review"]["final_look"] == 200

    # Guard the CLI source itself: the old hardcoded 0.90 must stay gone.
    source = Path(cycle_cli.__file__).read_text(encoding="utf-8")
    assert '"min_point_hit_rate": 0.90' not in source
    assert "controller._terminal_review_manifest()" in source
