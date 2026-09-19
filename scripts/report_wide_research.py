#!/usr/bin/env python3
"""Print a compact operational report for the wide rule factory."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from wide_research.lifecycle import wilson_interval  # noqa: E402
from wide_research.store import WideResearchStore  # noqa: E402


def frozen_review_summary(
    store: WideResearchStore, phase: Mapping[str, Any]
) -> dict[str, Any] | None:
    """Read the frozen terminal cohort, including later outcome corrections."""

    contract = (phase.get("policy") or {}).get("terminal_review") or {}
    if contract.get("enabled") is not True:
        return None
    horizon = int(contract["final_look"])
    try:
        rows = store.triggers_for_phase_look(str(phase["phase_id"]), horizon)
    except KeyError:
        return {"final_look": horizon, "closed": False, "reviewable": None}
    wins = sum(row.get("latest_outcome_status") == "win" for row in rows)
    losses = sum(row.get("latest_outcome_status") == "loss" for row in rows)
    invalid = sum(row.get("latest_outcome_status") == "invalid" for row in rows)
    pending = horizon - wins - losses - invalid
    closed = pending == 0
    rate = wins / horizon if closed else None
    lower, upper = wilson_interval(wins, horizon)
    return {
        "final_look": horizon,
        "closed": closed,
        "wins": wins,
        "losses": losses,
        "invalid_counted_as_losses": invalid,
        "pending": pending,
        "hit_rate_pct": round(100 * rate, 2) if rate is not None else None,
        "wilson_95_pct": (
            [round(100 * lower, 2), round(100 * upper, 2)] if closed else None
        ),
        "reviewable": (
            rate >= float(contract["min_point_hit_rate"]) if closed else None
        ),
        "production_applied": False,
        "evidence": "first_frozen_triggers_with_latest_outcomes",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db", type=Path, default=ROOT_DIR / "data" / "wide_research.sqlite3"
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    store = WideResearchStore(args.db, allowed_root=ROOT_DIR)
    phases = store.list_phases()
    payload = {"store": store.recover(), "phases": []}
    for phase in phases:
        metrics = store.metrics_for_phase(str(phase["phase_id"]))
        resolved = int(metrics.get("resolved") or 0)
        wins = int(metrics.get("win") or 0)
        lower, upper = wilson_interval(wins, resolved)
        payload["phases"].append(
            {
                "phase_id": phase["phase_id"],
                "rule_id": phase["rule_id"],
                "status": phase["status"],
                "starts_at_utc": phase["starts_at_utc"],
                "refinement_lineage": (phase.get("policy") or {}).get("discovery", {}).get("refinement_lineage"),
                "wins": wins,
                "losses": int(metrics.get("loss") or 0),
                "pending": int(metrics.get("pending") or 0),
                "hit_rate_pct": round(100.0 * wins / resolved, 2)
                if resolved
                else None,
                "wilson_95_pct": [round(100 * lower, 2), round(100 * upper, 2)],
                "terminal_review": frozen_review_summary(store, phase),
            }
        )
    payload["active_pointer"] = store.get_active_pointer("production")
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    print(
        "Wide research: "
        f"{payload['store']['rules']} rules, "
        f"{payload['store']['fixtures']} universe fixtures, "
        f"{payload['store']['triggers']} first triggers"
    )
    for row in payload["phases"]:
        rate = f"{row['hit_rate_pct']:.2f}%" if row["hit_rate_pct"] is not None else "n/a"
        print(
            f"- {row['status']:>8} {row['rule_id']} "
            f"{row['wins']}+/{row['losses']}- pending={row['pending']} "
            f"rate={rate} Wilson={row['wilson_95_pct'][0]:.2f}–{row['wilson_95_pct'][1]:.2f}%"
        )
        review = row.get("terminal_review")
        lineage = row.get("refinement_lineage")
        if lineage:
            comparison = lineage["train_comparison"]
            print(
                f"  offspring via {lineage['method']} of {lineage['parent_candidate_id']}: "
                f"train excluded losses={comparison['losses_excluded']}, "
                f"lost wins={comparison['wins_lost']}, "
                f"shifted triggers={comparison['shifted_triggers']} (historical only)"
            )
        if review and review["closed"]:
            print(
                f"  frozen first {review['final_look']}: "
                f"{review['hit_rate_pct']:.2f}%, "
                f"reviewable={review['reviewable']} (manual review only)"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
