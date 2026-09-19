from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from walk_forward import (  # noqa: E402
    EvaluationConfig,
    build_walk_forward_report,
    discover_journal_paths,
    render_text_report,
)


DEFAULT_OBSERVATIONS = ROOT_DIR / "data" / "observation_history.jsonl"
DEFAULT_STATIC_PREDICTIONS = ROOT_DIR / "data" / "shadow_ml_predictions.jsonl"
DEFAULT_ROLLING_PREDICTIONS = (
    ROOT_DIR / "data" / "shadow_ml_rolling_predictions.jsonl"
)


def _normalized(path: os.PathLike[str] | str) -> str:
    return os.path.normcase(os.path.realpath(os.path.abspath(os.fspath(path))))


def _assert_safe_outputs(
    outputs: Sequence[Optional[Path]],
    *,
    sources: Sequence[Optional[Path]],
) -> None:
    protected: set[str] = set()
    for source in sources:
        if source is None:
            continue
        protected.add(_normalized(source))
        protected.add(_normalized(str(source) + ".lock"))
        protected.add(_normalized(str(source) + ".index.sqlite3"))
        protected.update(_normalized(path) for path in discover_journal_paths(source))
    selected = [output for output in outputs if output is not None]
    normalized_outputs = [_normalized(output) for output in selected]
    if len(normalized_outputs) != len(set(normalized_outputs)):
        raise ValueError("JSON and text outputs must be different files")
    if any(output in protected for output in normalized_outputs):
        raise ValueError("report output cannot replace a source journal or archive")


def _write_atomic(path: Path, content: str) -> None:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only MSK daily walk-forward for fixed signal arms. "
            "Default ML mode retrains fresh fold-local models."
        )
    )
    parser.add_argument("--observations", type=Path, default=DEFAULT_OBSERVATIONS)
    parser.add_argument(
        "--static-predictions", type=Path, default=DEFAULT_STATIC_PREDICTIONS
    )
    parser.add_argument(
        "--rolling-predictions", type=Path, default=DEFAULT_ROLLING_PREDICTIONS
    )
    parser.add_argument("--from-date", help="first MSK date, YYYY-MM-DD")
    parser.add_argument("--to-date", help="last MSK date, YYYY-MM-DD")
    parser.add_argument("--contract-key")
    parser.add_argument(
        "--ml-mode",
        choices=("fold-local", "journal-replay", "disabled"),
        default="fold-local",
    )
    parser.add_argument(
        "--ml-source", choices=("rolling", "static", "consensus"), default="rolling"
    )
    parser.add_argument("--ml-confirm-threshold", type=float, default=75.0)
    parser.add_argument("--max-score-difference-abs", type=int, default=1)
    parser.add_argument(
        "--journal-timing",
        choices=("day-frozen", "prospective"),
        default="day-frozen",
        help="applies only to journal-replay; day-frozen is stricter",
    )
    parser.add_argument("--max-prediction-lag-seconds", type=float, default=300.0)
    parser.add_argument(
        "--trigger-policy",
        choices=("first-control", "independent-exploratory"),
        default="first-control",
        help=(
            "first-control mirrors live selection; independent-exploratory "
            "allows stricter arms to wait for later rows"
        ),
    )
    parser.add_argument(
        "--control-contract",
        choices=("recorded-production", "recomputed-historical"),
        default="recorded-production",
        help=(
            "recorded-production requires the actual saved publication "
            "contract; recomputed-historical is an explicitly hypothetical "
            "backtest over older compatible rows"
        ),
    )
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--text-output", type=Path)
    parser.add_argument(
        "--stdout", choices=("text", "json", "none"), default="text"
    )
    args = parser.parse_args(argv)
    try:
        config = EvaluationConfig(
            from_date=args.from_date,
            to_date=args.to_date,
            contract_key=args.contract_key,
            ml_mode=args.ml_mode,
            ml_source=args.ml_source,
            ml_confirm_threshold_pct=args.ml_confirm_threshold,
            max_score_difference_abs=args.max_score_difference_abs,
            journal_timing_policy=args.journal_timing,
            max_prediction_lag_seconds=args.max_prediction_lag_seconds,
            trigger_policy=args.trigger_policy,
            control_contract_mode=args.control_contract,
        )
        _assert_safe_outputs(
            (args.json_output, args.text_output),
            sources=(
                args.observations,
                args.static_predictions,
                args.rolling_predictions,
            ),
        )
    except ValueError as exc:
        parser.error(str(exc))
    report = build_walk_forward_report(
        args.observations,
        static_prediction_path=(
            args.static_predictions if args.ml_mode == "journal-replay" else None
        ),
        rolling_prediction_path=(
            args.rolling_predictions if args.ml_mode == "journal-replay" else None
        ),
        config=config,
    )
    rendered_json = json.dumps(
        report,
        ensure_ascii=False,
        allow_nan=False,
        indent=2,
        sort_keys=True,
    ) + "\n"
    rendered_text = render_text_report(report)
    if args.json_output:
        _write_atomic(args.json_output, rendered_json)
    if args.text_output:
        _write_atomic(args.text_output, rendered_text)
    if args.stdout == "json":
        print(rendered_json, end="")
    elif args.stdout == "text":
        print(rendered_text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
