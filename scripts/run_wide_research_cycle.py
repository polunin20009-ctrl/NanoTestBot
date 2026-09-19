#!/usr/bin/env python3
"""Run one isolated discovery/lifecycle cycle for the wide rule factory."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from shadow_ml.worker_runtime import (  # noqa: E402
    apply_worker_memory_limit,
    worker_memory_limit_mb,
)

_WORKER_MEMORY_LIMIT_REQUESTED_MB = worker_memory_limit_mb(
    env_name="GOALBOT_WIDE_RESEARCH_WORKER_MEMORY_LIMIT_MB"
)
_WORKER_MEMORY_LIMIT_MB = apply_worker_memory_limit(
    env_name="GOALBOT_WIDE_RESEARCH_WORKER_MEMORY_LIMIT_MB"
)

from wide_research.controller import WideResearchController  # noqa: E402
from wide_research.discovery import (  # noqa: E402
    DiscoveryConfig,
    JournalJoinStore,
    assert_safe_outputs,
    discover_rules,
    discover_extended_scopes,
    stream_jsonl,
    write_immutable_atomic,
)
from wide_research.lifecycle import LifecyclePolicy  # noqa: E402
from wide_research.store import WideResearchStore  # noqa: E402


DEFAULT_OBSERVATIONS = ROOT_DIR / "data" / "observation_history.jsonl"
DEFAULT_STATIC = ROOT_DIR / "data" / "shadow_ml_predictions.jsonl"
DEFAULT_ROLLING = ROOT_DIR / "data" / "shadow_ml_rolling_predictions.jsonl"
DEFAULT_DB = ROOT_DIR / "data" / "wide_research.sqlite3"
DEFAULT_ACTIVE = ROOT_DIR / "stats" / "wide_research_active.json"
DEFAULT_LATEST = ROOT_DIR / "stats" / "wide_research_discovery.json"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "reports" / "wide_research"


def _depth_budgets(value: str) -> tuple[int, ...]:
    try:
        parsed = tuple(int(item.strip()) for item in value.split(","))
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            "depth budgets must be comma-separated integers"
        ) from exc
    if not parsed or any(item <= 0 for item in parsed):
        raise argparse.ArgumentTypeError("depth budgets must be positive")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observations", type=Path, default=DEFAULT_OBSERVATIONS)
    parser.add_argument("--static-predictions", type=Path, default=DEFAULT_STATIC)
    parser.add_argument("--rolling-predictions", type=Path, default=DEFAULT_ROLLING)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--active-manifest", type=Path, default=DEFAULT_ACTIVE)
    parser.add_argument("--latest-output", type=Path, default=DEFAULT_LATEST)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--prospective-start-utc", required=True)
    parser.add_argument("--max-shadow-rules", type=int, default=10)
    parser.add_argument("--max-db-bytes", type=int, default=2 * 1024 * 1024 * 1024)
    parser.add_argument(
        "--store-profile",
        choices=(
            "primary",
            "exact_four_shadow",
            "precision_shadow",
            "rare_precision_shadow",
        ),
        default="primary",
    )
    parser.add_argument("--production-enabled", action="store_true")
    parser.add_argument(
        "--require-memory-limit",
        action="store_true",
        help="Refuse to run if the requested worker RLIMIT_AS was not applied",
    )
    parser.add_argument(
        "--shadow-only",
        action="store_true",
        help=(
            "Hard-disable lifecycle promotion for an isolated research profile; "
            "cannot be combined with --production-enabled"
        ),
    )
    parser.add_argument("--lifecycle-only", action="store_true")
    parser.add_argument(
        "--min-conjunction-size", type=int, choices=range(1, 9), default=1
    )
    parser.add_argument(
        "--max-conjunction-size", type=int, choices=range(1, 9), default=3
    )
    parser.add_argument("--beam-width", type=int, default=64)
    parser.add_argument("--evaluation-budget", type=int, default=12_000)
    parser.add_argument(
        "--depth-evaluation-budgets",
        type=_depth_budgets,
        default=(),
        help="comma-separated absolute quotas for depths 1..max depth",
    )
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--min-train-support", type=int, default=40)
    parser.add_argument("--min-validation-support", type=int, default=15)
    parser.add_argument("--min-holdout-support", type=int, default=15)
    parser.add_argument(
        "--selection-mode",
        choices=("standard", "precision_first", "rare_precision"),
        default="standard",
    )
    parser.add_argument("--allow-feature-ranges", action="store_true")
    parser.add_argument("--validation-window-count", type=int, default=1)
    parser.add_argument(
        "--temporal-purge", action="store_true",
        help="Exclude fixtures whose observations or labels cross historical split cutoffs",
    )
    parser.add_argument("--temporal-embargo-seconds", type=float, default=300.0)
    parser.add_argument("--extended-features", action="store_true")
    parser.add_argument("--error-refinement", action="store_true")
    parser.add_argument("--market-quotes", type=Path, default=None)
    parser.add_argument("--min-signals-per-week", type=float, default=0.0)
    parser.add_argument("--preferred-signals-per-week", type=float, default=0.0)
    parser.add_argument("--max-signals-per-week", type=float, default=0.0)
    parser.add_argument("--portfolio-max-rules", type=int, default=1)
    parser.add_argument("--portfolio-beam-width", type=int, default=32)
    return parser


def _write_latest_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent)
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(
                payload,
                handle,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary_name, 0o600)
        os.replace(temporary_name, destination)
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


def _compact_metrics(value: Any) -> Any:
    if not isinstance(value, Mapping):
        return value
    excluded = {"first_triggers", "league_counts", "daily", "weekly"}
    return {key: item for key, item in value.items() if key not in excluded}


def _compact_portfolio(value: Any) -> Any:
    if not isinstance(value, Mapping):
        return value
    compact = dict(value)
    for split in ("training", "validation", "holdout"):
        if split in compact:
            compact[split] = _compact_metrics(compact[split])
    return compact


def _compact_splits(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {
        name: {key: item for key, item in details.items() if key != "fixture_ids"}
        if isinstance(details, Mapping) else details
        for name, details in value.items()
    }


def _assert_shadow_only_discovery_complete(
    report: Mapping[str, Any],
    *,
    min_conjunction_size: int,
    max_conjunction_size: int,
) -> None:
    search = report["manifests"].get("search", {})
    completed_levels = {
        int(row.get("size") or 0)
        for row in search.get("levels", [])
        if isinstance(row, Mapping)
    }
    if (
        max_conjunction_size not in completed_levels
        or bool(search.get("budget_exhausted"))
    ):
        raise RuntimeError(
            "shadow-only discovery did not complete the requested "
            f"conjunction depth {max_conjunction_size}; refusing to import "
            "a partial, order-biased search"
        )
    for candidate in report["manifests"].get("candidates", []):
        clauses = candidate.get("clauses", [])
        if len(clauses) < min_conjunction_size or len(clauses) > (
            max_conjunction_size
        ):
            raise RuntimeError(
                "shadow-only discovery returned a candidate outside the "
                "requested conjunction-size range"
            )


def _assert_precision_discovery_complete(
    report: Mapping[str, Any], *, max_conjunction_size: int
) -> None:
    search = report["manifests"].get("search", {})
    levels = [
        row for row in search.get("levels", []) if isinstance(row, Mapping)
    ]
    completed = {int(row.get("size") or 0) for row in levels}
    depth_one = next(
        (row for row in levels if int(row.get("size") or 0) == 1), None
    )
    if int(max_conjunction_size) not in completed:
        raise RuntimeError(
            "precision discovery did not reach conjunction depth "
            f"{int(max_conjunction_size)}"
        )
    if depth_one is None or bool(depth_one.get("truncated")):
        raise RuntimeError(
            "precision discovery must evaluate every available atomic clause"
        )
    if bool(search.get("budget_exhausted")):
        raise RuntimeError(
            "precision discovery exhausted the global budget before its "
            "planned depth quotas completed"
        )


def _assert_rare_precision_discovery_complete(
    report: Mapping[str, Any],
    *,
    max_conjunction_size: int,
    evaluation_budget: int,
) -> None:
    """Accept only the registered depth-complete rare search plan.

    Unlike the legacy precision profile, the rare plan intentionally assigns
    the entire global budget to deterministic per-depth quotas.  Reaching the
    global limit is therefore valid only while sampling the final requested
    depth; an earlier global truncation still fails closed.
    """

    search = report["manifests"].get("search", {})
    levels = [
        row for row in search.get("levels", []) if isinstance(row, Mapping)
    ]
    completed = {int(row.get("size") or 0) for row in levels}
    expected = set(range(1, int(max_conjunction_size) + 1))
    if completed != expected:
        raise RuntimeError(
            "rare precision discovery did not reach every requested "
            f"conjunction depth through {int(max_conjunction_size)}"
        )
    depth_one = next(
        row for row in levels if int(row.get("size") or 0) == 1
    )
    if bool(depth_one.get("truncated")):
        raise RuntimeError(
            "rare precision discovery must evaluate every atomic clause"
        )
    globally_truncated = {
        int(value)
        for value in search.get("global_budget_truncated_depths", [])
    }
    if globally_truncated and (
        globally_truncated != {int(max_conjunction_size)}
        or int(search.get("evaluations_used") or 0)
        != int(evaluation_budget)
    ):
        raise RuntimeError(
            "rare precision discovery exhausted its global budget before "
            "the registered final-depth quota"
        )
    rare = search.get("rare_precision")
    if not isinstance(rare, Mapping):
        raise RuntimeError("rare precision discovery diagnostics are missing")
    if int(rare.get("validation_window_count") or 0) != 4:
        raise RuntimeError(
            "rare precision discovery did not evaluate four validation windows"
        )


def _discovery(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    config = DiscoveryConfig(
        min_conjunction_size=args.min_conjunction_size,
        max_conjunction_size=args.max_conjunction_size,
        beam_width=args.beam_width,
        evaluation_budget=args.evaluation_budget,
        depth_evaluation_budgets=args.depth_evaluation_budgets,
        top_n=args.top_n,
        min_train_support=args.min_train_support,
        min_validation_support=args.min_validation_support,
        min_holdout_support=args.min_holdout_support,
        selection_mode=args.selection_mode,
        min_signals_per_week=args.min_signals_per_week,
        preferred_signals_per_week=args.preferred_signals_per_week,
        max_signals_per_week=args.max_signals_per_week,
        portfolio_max_rules=args.portfolio_max_rules,
        portfolio_beam_width=args.portfolio_beam_width,
        allow_feature_ranges=args.allow_feature_ranges,
        validation_window_count=args.validation_window_count,
        extended_features=args.extended_features,
        error_refinement=args.error_refinement,
        temporal_purge=args.temporal_purge,
        temporal_embargo_seconds=args.temporal_embargo_seconds,
    )
    observation_diagnostics: dict[str, Any] = {}
    static_diagnostics: dict[str, Any] = {}
    rolling_diagnostics: dict[str, Any] = {}
    market_diagnostics: dict[str, Any] = {}
    with JournalJoinStore(config=config) as join:
        join.ingest_observation_records(
            stream_jsonl(args.observations, observation_diagnostics)
        )
        if args.static_predictions.exists():
            join.ingest_predictions(
                stream_jsonl(args.static_predictions, static_diagnostics),
                source="static",
            )
        if args.rolling_predictions.exists():
            join.ingest_predictions(
                stream_jsonl(args.rolling_predictions, rolling_diagnostics),
                source="rolling",
            )
        if args.extended_features and args.market_quotes is not None and args.market_quotes.exists():
            join.ingest_market_quotes(stream_jsonl(args.market_quotes, market_diagnostics))
        join_diagnostics = join.diagnostics()
        report = (discover_extended_scopes(join, config=config) if args.extended_features
                  else discover_rules(join.joined_observations(), config=config))
    if args.store_profile == "rare_precision_shadow":
        _assert_rare_precision_discovery_complete(
            report,
            max_conjunction_size=args.max_conjunction_size,
            evaluation_budget=args.evaluation_budget,
        )
        market_scope = report["manifests"]["search"].get("market_scope", {})
        if market_scope.get("status") == "completed" and market_scope.get("search", {}).get("selected"):
            _assert_rare_precision_discovery_complete(
                {"manifests": {"search": market_scope["search"]}},
                max_conjunction_size=args.max_conjunction_size,
                evaluation_budget=args.evaluation_budget,
            )
    elif args.store_profile == "precision_shadow":
        _assert_precision_discovery_complete(
            report,
            max_conjunction_size=args.max_conjunction_size,
        )
    elif args.shadow_only:
        _assert_shadow_only_discovery_complete(
            report,
            min_conjunction_size=args.min_conjunction_size,
            max_conjunction_size=args.max_conjunction_size,
        )
    report["manifests"]["sources"] = {
        "observations": observation_diagnostics,
        "static_predictions": static_diagnostics,
        "rolling_predictions": rolling_diagnostics,
        "market_quotes": market_diagnostics,
        "join": join_diagnostics,
    }
    manifest_path = args.output_dir / f"{report['run_id']}.manifests.json"
    results_path = args.output_dir / f"{report['run_id']}.results.json"
    assert_safe_outputs(
        (manifest_path, results_path, args.latest_output),
        sources=(
            args.observations,
            args.static_predictions,
            args.rolling_predictions,
            *([args.market_quotes] if args.market_quotes is not None else []),
        ),
    )
    write_immutable_atomic(manifest_path, report["manifests"])
    write_immutable_atomic(results_path, report["results"])
    return report, {
        "manifest_path": str(manifest_path.resolve()),
        "results_path": str(results_path.resolve()),
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    if args.error_refinement and not args.extended_features:
        raise SystemExit("error refinement requires isolated extended features")
    if args.extended_features and (not args.shadow_only or args.store_profile != "rare_precision_shadow"):
        raise SystemExit("extended features require the hard-shadow rare profile")
    if args.market_quotes is not None and not args.extended_features:
        raise SystemExit("market quotes require extended features")
    if args.shadow_only and args.production_enabled:
        raise SystemExit(
            "--shadow-only cannot be combined with --production-enabled"
        )
    if args.shadow_only and args.store_profile not in {
        "exact_four_shadow",
        "precision_shadow",
        "rare_precision_shadow",
    }:
        raise SystemExit(
            "--shadow-only requires an isolated shadow store profile"
        )
    if args.selection_mode == "rare_precision" and (
        args.store_profile != "rare_precision_shadow" or not args.shadow_only
    ):
        raise SystemExit(
            "rare_precision selection mode requires "
            "--store-profile rare_precision_shadow and --shadow-only"
        )
    if args.store_profile == "exact_four_shadow" and (
        not args.shadow_only
        or args.min_conjunction_size != 4
        or args.max_conjunction_size != 4
    ):
        raise SystemExit(
            "exact_four_shadow profile requires --shadow-only and "
            "--min-conjunction-size 4 --max-conjunction-size 4"
        )
    if args.store_profile == "precision_shadow" and (
        not args.shadow_only
        or args.selection_mode != "precision_first"
        or args.min_conjunction_size != 2
        or not 4 <= args.max_conjunction_size <= 8
        or not args.depth_evaluation_budgets
    ):
        raise SystemExit(
            "precision_shadow profile requires --shadow-only, "
            "--selection-mode precision_first, minimum conjunction size 2, "
            "maximum conjunction size in [4, 8], "
            "and explicit per-depth budgets"
        )
    if args.store_profile == "rare_precision_shadow" and (
        not args.shadow_only
        or args.selection_mode != "rare_precision"
        or args.min_conjunction_size != 2
        or args.max_conjunction_size != 8
        or args.beam_width != 128
        or args.evaluation_budget != 180000
        or args.depth_evaluation_budgets
        != (4000, 28000, 30000, 28000, 25000, 23000, 22000, 20000)
        or args.top_n != 8
        or args.max_shadow_rules != 64
        or float(args.min_signals_per_week) != 0.0
        or float(args.preferred_signals_per_week) != 0.0
        or float(args.max_signals_per_week) != 0.0
        or not args.allow_feature_ranges
        or args.validation_window_count != 4
        or not args.extended_features
    ):
        raise SystemExit(
            "rare_precision_shadow profile requires its fixed hard-shadow "
            "search contract (rare_precision, ranges, four validation "
            "windows, cadence 0, top 8, pool 64, beam 128, depth 8, "
            "and the registered 180000 evaluation budget)"
        )
    if args.require_memory_limit and _WORKER_MEMORY_LIMIT_MB is None:
        raise SystemExit(
            "required wide-research worker memory limit was not applied"
        )
    if args.market_quotes is not None:
        assert_safe_outputs(
            (args.db, args.active_manifest, args.latest_output),
            sources=(args.observations, args.static_predictions, args.rolling_predictions, args.market_quotes),
        )
    store = WideResearchStore(
        args.db,
        allowed_root=ROOT_DIR,
        max_db_bytes=args.max_db_bytes,
    )
    store.bind_profile(args.store_profile)
    controller_kwargs: dict[str, Any] = {}
    if args.store_profile == "rare_precision_shadow":
        controller_kwargs.update(
            {
                "policy": LifecyclePolicy(
                    min_resolved=200,
                    min_triggers_per_week=0.0,
                    allowed_looks=(50, 100, 200),
                ),
                "terminal_review_enabled": True,
                "terminal_review_min_hit_rate": 0.90,
            }
        )
    controller = WideResearchController(
        store,
        active_manifest_path=str(args.active_manifest),
        prospective_start_utc=args.prospective_start_utc,
        production_enabled=args.production_enabled,
        max_shadow_rules=args.max_shadow_rules,
        **controller_kwargs,
    )
    summary: dict[str, Any] = {
        "schema_version": 1,
        "cycle_type": "lifecycle" if args.lifecycle_only else "discovery",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "production_enabled": bool(args.production_enabled),
        "store_profile": str(args.store_profile),
        "shadow_only": bool(args.shadow_only),
        "prospective_start_utc": str(args.prospective_start_utc),
        "worker_memory_limit_requested_mb": (
            _WORKER_MEMORY_LIMIT_REQUESTED_MB
        ),
        "worker_memory_limit_mb": _WORKER_MEMORY_LIMIT_MB,
        "search_config": {
            "min_conjunction_size": int(args.min_conjunction_size),
            "max_conjunction_size": int(args.max_conjunction_size),
            "beam_width": int(args.beam_width),
            "evaluation_budget": int(args.evaluation_budget),
            "depth_evaluation_budgets": list(args.depth_evaluation_budgets),
            "top_n": int(args.top_n),
            "min_train_support": int(args.min_train_support),
            "min_validation_support": int(args.min_validation_support),
            "min_holdout_support": int(args.min_holdout_support),
            "selection_mode": str(args.selection_mode),
            "extended_features": bool(args.extended_features),
            "error_refinement": bool(args.error_refinement),
            "min_signals_per_week": float(args.min_signals_per_week),
            "preferred_signals_per_week": float(
                args.preferred_signals_per_week
            ),
            "max_signals_per_week": float(args.max_signals_per_week),
            "portfolio_max_rules": int(args.portfolio_max_rules),
            "portfolio_beam_width": int(args.portfolio_beam_width),
            "allow_feature_ranges": bool(args.allow_feature_ranges),
            "validation_window_count": int(args.validation_window_count),
            "temporal_purge": bool(args.temporal_purge),
            "temporal_embargo_seconds": float(args.temporal_embargo_seconds),
        },
    }
    if args.store_profile == "rare_precision_shadow":
        lifecycle_policy = controller.policy.manifest()
        lifecycle_policy["allowed_looks"] = list(
            controller.policy.allowed_looks
        )
        summary["profile_contract"] = {
            "max_shadow_rules": int(args.max_shadow_rules),
            "lifecycle_policy": lifecycle_policy,
            "terminal_review": {
                "enabled": True,
                "final_look": max(controller.policy.allowed_looks),
                "min_point_hit_rate": 0.90,
            },
        }
    if not args.lifecycle_only:
        report, paths = _discovery(args)
        summary.update(
            {
                "run_id": report["run_id"],
                "discovery_engine_version": report["manifests"].get(
                    "engine_version"
                ),
                "feature_schema_version": report["manifests"].get(
                    "feature_schema_version"
                ),
                "threshold_grid_version": report["manifests"].get(
                    "threshold_grid_version"
                ),
                "split_policy": report["manifests"].get("split_policy"),
                "splits": _compact_splits(report["manifests"].get("splits")),
                "artifacts": paths,
                "eligible_observations": report["manifests"]["input"].get(
                    "eligible_observations"
                ),
                "eligible_fixtures": report["manifests"]["input"].get(
                    "eligible_fixtures"
                ),
                "candidate_count": len(
                    report["manifests"].get("candidates", [])
                ),
                "error_refinement": {
                    "general": report["manifests"]["search"].get("error_refinement"),
                    "market": report["manifests"]["search"].get("market_scope", {}).get("search", {}).get("error_refinement"),
                } if args.error_refinement else None,
                "candidate_results": [
                    {
                        "candidate_id": row.get("candidate_id"),
                        "rank": row.get("rank_at_discovery"),
                        "research_scope": row.get("research_scope"),
                        "refinement_lineage": row.get("refinement_lineage"),
                        "holdout": _compact_metrics(row.get("holdout")),
                        "statistical_test": row.get("statistical_test"),
                    }
                    for row in report["results"].get("results", [])
                ],
                "selected_portfolio": _compact_portfolio(
                    report["results"].get("selected_portfolio")
                ),
                "best_available_portfolio": _compact_portfolio(
                    report["results"].get("best_available_portfolio")
                ),
                "registry": controller.import_report(report),
            }
        )
    if args.shadow_only:
        summary["lifecycle"] = {
            "status": "skipped",
            "reason": "hard_shadow_only",
            "production_enabled": False,
        }
    else:
        summary["lifecycle"] = controller.reconcile()
    summary["store"] = store.recover()
    summary["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    _write_latest_atomic(args.latest_output, summary)
    print(
        json.dumps(
            summary,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
