#!/usr/bin/env python3
"""Stream journals and discover frozen wide-universe rule candidates."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from wide_research.discovery import (  # noqa: E402
    DiscoveryConfig,
    JournalJoinStore,
    assert_safe_outputs,
    discover_rules,
    stream_jsonl,
    write_immutable_atomic,
)


DEFAULT_OBSERVATIONS = ROOT_DIR / "data" / "observation_history.jsonl"
DEFAULT_STATIC_PREDICTIONS = ROOT_DIR / "data" / "shadow_ml_predictions.jsonl"
DEFAULT_ROLLING_PREDICTIONS = (
    ROOT_DIR / "data" / "shadow_ml_rolling_predictions.jsonl"
)
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
    parser = argparse.ArgumentParser(
        description=(
            "Leakage-resistant rule discovery over every eligible 46-60 minute "
            "observation. Production signals are never changed."
        )
    )
    parser.add_argument("--observations", type=Path, default=DEFAULT_OBSERVATIONS)
    parser.add_argument("--static-predictions", type=Path)
    parser.add_argument("--rolling-predictions", type=Path)
    parser.add_argument(
        "--with-default-predictions",
        action="store_true",
        help="read the default static and rolling prediction journals",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--manifest-output", type=Path)
    parser.add_argument("--results-output", type=Path)
    parser.add_argument("--train-fraction", type=float, default=0.60)
    parser.add_argument("--validation-fraction", type=float, default=0.20)
    parser.add_argument(
        "--min-conjunction-size", type=int, choices=range(1, 9), default=1
    )
    parser.add_argument(
        "--max-conjunction-size", type=int, choices=range(1, 9), default=3
    )
    parser.add_argument("--beam-width", type=int, default=64)
    parser.add_argument("--evaluation-budget", type=int, default=12_000)
    parser.add_argument(
        "--depth-evaluation-budgets", type=_depth_budgets, default=()
    )
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--min-train-support", type=int, default=40)
    parser.add_argument("--min-validation-support", type=int, default=15)
    parser.add_argument("--min-holdout-support", type=int, default=15)
    parser.add_argument("--target-hit-rate", type=float, default=0.95)
    parser.add_argument("--null-hit-rate", type=float, default=0.90)
    parser.add_argument("--family-alpha", type=float, default=0.05)
    parser.add_argument(
        "--selection-mode",
        choices=("standard", "precision_first"),
        default="standard",
    )
    parser.add_argument("--min-signals-per-week", type=float, default=0.0)
    parser.add_argument("--preferred-signals-per-week", type=float, default=0.0)
    parser.add_argument("--max-signals-per-week", type=float, default=0.0)
    parser.add_argument("--portfolio-max-rules", type=int, default=1)
    parser.add_argument("--portfolio-beam-width", type=int, default=32)
    parser.add_argument("--max-prediction-lag-seconds", type=float, default=300.0)
    parser.add_argument(
        "--stdout",
        choices=("summary", "json", "none"),
        default="summary",
    )
    return parser


def _summary(report: dict) -> str:
    manifests = report["manifests"]
    results = report["results"]
    lines = [
        f"run_id: {report['run_id']}",
        f"eligible fixtures: {manifests['input']['eligible_fixtures']}",
        f"eligible observations: {manifests['input']['eligible_observations']}",
        f"candidates: {len(manifests['candidates'])}",
    ]
    for row in results["results"]:
        holdout = row["holdout"]
        rate = holdout["hit_rate_pct"]
        rendered_rate = f"{rate:.2f}%" if rate is not None else "n/a"
        lines.append(
            f"{row['rank_at_discovery']:>2}. {row['candidate_id']} "
            f"holdout={holdout['wins']}+/{holdout['losses']}- "
            f"({rendered_rate})"
        )
    return "\n".join(lines) + "\n"


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    static_path = args.static_predictions
    rolling_path = args.rolling_predictions
    if args.with_default_predictions:
        static_path = static_path or DEFAULT_STATIC_PREDICTIONS
        rolling_path = rolling_path or DEFAULT_ROLLING_PREDICTIONS
    try:
        config = DiscoveryConfig(
            train_fraction=args.train_fraction,
            validation_fraction=args.validation_fraction,
            min_conjunction_size=args.min_conjunction_size,
            max_conjunction_size=args.max_conjunction_size,
            beam_width=args.beam_width,
            evaluation_budget=args.evaluation_budget,
            depth_evaluation_budgets=args.depth_evaluation_budgets,
            top_n=args.top_n,
            min_train_support=args.min_train_support,
            min_validation_support=args.min_validation_support,
            min_holdout_support=args.min_holdout_support,
            target_hit_rate=args.target_hit_rate,
            null_hit_rate=args.null_hit_rate,
            family_alpha=args.family_alpha,
            selection_mode=args.selection_mode,
            min_signals_per_week=args.min_signals_per_week,
            preferred_signals_per_week=args.preferred_signals_per_week,
            max_signals_per_week=args.max_signals_per_week,
            portfolio_max_rules=args.portfolio_max_rules,
            portfolio_beam_width=args.portfolio_beam_width,
            max_prediction_lag_seconds=args.max_prediction_lag_seconds,
        )
        # Explicit outputs can be checked before spending time on ingestion.
        assert_safe_outputs(
            (args.manifest_output, args.results_output),
            sources=(args.observations, static_path, rolling_path),
        )
    except ValueError as exc:
        parser.error(str(exc))

    observation_diagnostics: dict = {}
    static_diagnostics: dict = {}
    rolling_diagnostics: dict = {}
    with JournalJoinStore(config=config) as store:
        store.ingest_observation_records(
            stream_jsonl(args.observations, observation_diagnostics)
        )
        if static_path is not None:
            store.ingest_predictions(
                stream_jsonl(static_path, static_diagnostics), source="static"
            )
        if rolling_path is not None:
            store.ingest_predictions(
                stream_jsonl(rolling_path, rolling_diagnostics), source="rolling"
            )
        join_diagnostics = store.diagnostics()
        try:
            report = discover_rules(store.joined_observations(), config=config)
        except ValueError as exc:
            parser.error(str(exc))

    report["manifests"]["sources"] = {
        "observations": observation_diagnostics,
        "static_predictions": static_diagnostics if static_path is not None else None,
        "rolling_predictions": rolling_diagnostics if rolling_path is not None else None,
        "join": join_diagnostics,
    }
    # Sources are diagnostics only and deliberately excluded from artifact hashes:
    # archive filenames do not change candidate identity or statistical results.
    manifest_output = args.manifest_output or (
        args.output_dir / f"{report['run_id']}.manifests.json"
    )
    results_output = args.results_output or (
        args.output_dir / f"{report['run_id']}.results.json"
    )
    try:
        assert_safe_outputs(
            (manifest_output, results_output),
            sources=(args.observations, static_path, rolling_path),
        )
        write_immutable_atomic(manifest_output, report["manifests"])
        write_immutable_atomic(results_output, report["results"])
    except (ValueError, FileExistsError, OSError) as exc:
        parser.error(str(exc))

    if args.stdout == "json":
        print(json.dumps(report, ensure_ascii=False, allow_nan=False, indent=2, sort_keys=True))
    elif args.stdout == "summary":
        print(_summary(report), end="")
        print(f"manifests: {manifest_output.resolve()}")
        print(f"results: {results_output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
