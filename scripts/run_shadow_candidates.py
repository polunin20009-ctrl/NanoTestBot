#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shadow_candidates.engine import CandidateLayer, build_prediction_index
from shadow_candidates.storage import (
    AppendOnlyCandidateJournal,
    iter_jsonl_records,
    jsonl_paths,
)


logger = logging.getLogger("shadow_candidates.runner")


def _normalized(path: str) -> str:
    return os.path.normcase(os.path.realpath(os.path.abspath(path)))


def _assert_safe_journal(args: argparse.Namespace) -> None:
    journal = str(args.journal)
    if journal.endswith(".gz"):
        raise ValueError("active candidate journal must not be a gzip archive")
    protected: set[str] = set()
    for source in (
        args.observation_file,
        args.decision_file,
        args.static_predictions,
        args.rolling_predictions,
    ):
        protected.add(_normalized(str(source)))
        protected.update(_normalized(path) for path in jsonl_paths(str(source)))
    outputs = {
        _normalized(journal),
        _normalized(journal + ".index.sqlite3"),
        _normalized(journal + ".lock"),
    }
    collision = sorted(outputs & protected)
    if collision:
        raise ValueError(
            "candidate journal/index/lock cannot overlap a source journal: "
            + collision[0]
        )


def _existing_records(path: str) -> Iterator[Dict[str, Any]]:
    if not os.path.exists(path) and not list(Path(path).parent.glob(
        f"{Path(path).stem}.*{Path(path).suffix}*"
    )):
        return
    yield from iter_jsonl_records(path)


def _snapshots(records: Iterable[Mapping[str, Any]], source: str) -> Iterator[Mapping[str, Any]]:
    expected = "observation" if source == "observations" else "decision"
    for record in records:
        if record.get("record_type") != expected:
            continue
        if expected == "observation" and record.get("stage") != "decision_pipeline":
            continue
        yield record


def _outcomes(records: Iterable[Mapping[str, Any]], source: str) -> Iterator[Mapping[str, Any]]:
    expected = "observation_outcome" if source == "observations" else "outcome"
    for record in records:
        if record.get("record_type") == expected:
            yield record


def run_once(args: argparse.Namespace, layer: CandidateLayer) -> Dict[str, Any]:
    static_index = build_prediction_index(_existing_records(args.static_predictions))
    rolling_index = build_prediction_index(_existing_records(args.rolling_predictions))
    source_path = (
        args.observation_file
        if args.source == "observations"
        else args.decision_file
    )
    snapshots_seen = 0
    decisions_appended = 0
    for snapshot in _snapshots(_existing_records(source_path), args.source):
        snapshots_seen += 1
        source_id = str(
            snapshot.get("observation_id") or snapshot.get("decision_id") or ""
        )
        written = layer.process_snapshot(
            snapshot,
            static_prediction=static_index.get(source_id),
            rolling_prediction=rolling_index.get(source_id),
        )
        if written is not None:
            decisions_appended += 1

    outcomes_seen = 0
    outcomes_appended = 0
    # A second streaming pass guarantees reconciliation even if an archive is
    # not in strict decision-before-outcome order.
    outcome_batch: list[Mapping[str, Any]] = []
    for outcome in _outcomes(_existing_records(source_path), args.source):
        outcomes_seen += 1
        outcome_batch.append(outcome)
        if len(outcome_batch) >= 500:
            outcomes_appended += len(layer.process_outcomes(outcome_batch))
            outcome_batch.clear()
    if outcome_batch:
        outcomes_appended += len(layer.process_outcomes(outcome_batch))

    return {
        "source": args.source,
        "snapshots_seen": snapshots_seen,
        "decision_records_appended": decisions_appended,
        "outcomes_seen": outcomes_seen,
        "outcome_records_appended": outcomes_appended,
        "static_predictions_indexed": len(static_index),
        "rolling_predictions_indexed": len(rolling_index),
        "shadow_only": True,
        "production_applied": False,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "One-shot recovery/offline replay for the prospective shadow-candidate journal. "
            "It has no Telegram or production-probability side effects."
        )
    )
    parser.add_argument(
        "--source",
        choices=("observations", "decisions"),
        default="observations",
    )
    parser.add_argument(
        "--observation-file",
        default=str(PROJECT_ROOT / "data" / "observation_history.jsonl"),
    )
    parser.add_argument(
        "--decision-file",
        default=str(PROJECT_ROOT / "data" / "decision_snapshots.jsonl"),
    )
    parser.add_argument(
        "--static-predictions",
        default=str(PROJECT_ROOT / "data" / "shadow_ml_predictions.jsonl"),
    )
    parser.add_argument(
        "--rolling-predictions",
        default=str(PROJECT_ROOT / "data" / "shadow_ml_rolling_predictions.jsonl"),
    )
    parser.add_argument(
        "--journal",
        default=str(PROJECT_ROOT / "data" / "shadow_candidates.jsonl"),
    )
    parser.add_argument("--rotate-max-bytes", type=int, default=10 * 1024 * 1024)
    parser.add_argument(
        "--prospective-start-utc",
        required=True,
        help="Immutable aware ISO boundary; older source snapshots are ignored.",
    )
    parser.add_argument("--settle-seconds", type=float, default=15.0)
    parser.add_argument("--max-live-prediction-lag-seconds", type=float, default=300.0)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        _assert_safe_journal(args)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    journal = AppendOnlyCandidateJournal(
        args.journal,
        rotate_max_bytes=args.rotate_max_bytes,
    )
    layer = CandidateLayer(
        journal,
        prospective_start_utc=args.prospective_start_utc,
        settle_seconds=args.settle_seconds,
        max_live_prediction_lag_seconds=args.max_live_prediction_lag_seconds,
    )
    summary = run_once(args, layer)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
