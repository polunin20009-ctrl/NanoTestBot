from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import NanoTest as bot
from shadow_ml import iter_prediction_records, load_model
from shadow_ml import storage as prediction_storage


PROSPECTIVE_MIN_LOGLOSS_IMPROVEMENT = 0.001
PROSPECTIVE_MIN_BRIER_IMPROVEMENT = 0.0005
PROSPECTIVE_BOOTSTRAP_ITERATIONS = 1000


def _parse_utc(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalized_path(path: os.PathLike[str] | str) -> str:
    return os.path.normcase(os.path.realpath(os.path.abspath(os.fspath(path))))


def _assert_safe_output(
    output: Path,
    *,
    observation_path: str,
    prediction_path: str,
    model_path: str,
) -> None:
    protected = {
        _normalized_path(observation_path),
        _normalized_path(prediction_path),
        _normalized_path(model_path),
    }
    protected.update(
        _normalized_path(path)
        for path in bot._observation_history_paths(observation_path)
    )
    protected.update(
        _normalized_path(path)
        for path in prediction_storage._prediction_paths(prediction_path)
    )
    if _normalized_path(output) in protected:
        raise ValueError(
            "report output cannot replace a source journal, archive, or model"
        )


def _write_text_atomic(output: Path, text: str) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output.name}.",
        suffix=".tmp",
        dir=str(output.parent),
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            temporary.unlink()


def _probability_pct(value: Any) -> Optional[float]:
    """Normalize an explicitly percentage-point field to ``[0, 1]``."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    if not 0.0 <= number <= 100.0:
        return None
    number /= 100.0
    return min(1.0 - 1e-6, max(1e-6, number))


def _label(outcome: Mapping[str, Any], target: str) -> Optional[int]:
    value = (
        outcome.get("goal_within_15")
        if target == "next15"
        else outcome.get("goal_to90_normal_time")
    )
    if value is True or value == 1:
        return 1
    if value is False or value == 0:
        return 0
    return None


def _weighted_metrics(samples: list[dict], probability_key: str) -> dict:
    if not samples:
        return {
            "rows": 0,
            "fixtures": 0,
            "log_loss": None,
            "brier": None,
            "ece": None,
            "average_probability": None,
            "actual_rate": None,
        }
    per_fixture = Counter(int(sample["fixture_id"]) for sample in samples)
    weighted: list[tuple[float, int, float]] = []
    for sample in samples:
        probability = float(sample[probability_key])
        label = int(sample["label"])
        quality_weight = float(sample.get("quality_weight", 1.0))
        weight = (
            max(0.0, min(1.0, quality_weight))
            / max(1, per_fixture[int(sample["fixture_id"])])
        )
        weighted.append((probability, label, weight))
    total_weight = sum(weight for _, _, weight in weighted)
    if total_weight <= 0.0:
        return {
            "rows": len(samples),
            "fixtures": len(per_fixture),
            "log_loss": None,
            "brier": None,
            "ece": None,
            "average_probability": None,
            "actual_rate": None,
        }
    log_loss = -sum(
        weight
        * (
            label * math.log(probability)
            + (1 - label) * math.log(1.0 - probability)
        )
        for probability, label, weight in weighted
    ) / total_weight
    brier = sum(
        weight * (probability - label) ** 2
        for probability, label, weight in weighted
    ) / total_weight

    ece = 0.0
    for start in range(0, 10):
        lower = start / 10.0
        upper = (start + 1) / 10.0
        bucket = [
            item
            for item in weighted
            if lower <= item[0] < upper or (start == 9 and item[0] == 1.0)
        ]
        bucket_weight = sum(item[2] for item in bucket)
        if not bucket_weight:
            continue
        average_probability = sum(
            probability * weight for probability, _, weight in bucket
        ) / bucket_weight
        actual_rate = sum(
            label * weight for _, label, weight in bucket
        ) / bucket_weight
        ece += (
            bucket_weight
            / total_weight
            * abs(average_probability - actual_rate)
        )
    return {
        "rows": len(samples),
        "fixtures": len(per_fixture),
        "log_loss": round(log_loss, 8),
        "brier": round(brier, 8),
        "ece": round(ece, 8),
        "average_probability": round(
            sum(probability * weight for probability, _, weight in weighted)
            / total_weight,
            8,
        ),
        "actual_rate": round(
            sum(label * weight for _, label, weight in weighted) / total_weight,
            8,
        ),
    }


def _paired_fixture_improvement(
    samples: list[dict],
    *,
    seed: int,
) -> dict:
    """Measure paired gains and a deterministic fixture-level bootstrap CI."""
    grouped: Dict[int, list[dict]] = defaultdict(list)
    for sample in samples:
        grouped[int(sample["fixture_id"])].append(sample)

    fixture_values: list[tuple[float, float, float]] = []
    for fixture_samples in grouped.values():
        weighted_rows: list[tuple[float, float, float]] = []
        for sample in fixture_samples:
            quality_weight = max(
                0.0,
                min(1.0, float(sample.get("quality_weight", 1.0))),
            )
            if quality_weight <= 0.0:
                continue
            label = int(sample["label"])
            candidate = float(sample["candidate"])
            baseline = float(sample["baseline"])
            candidate_logloss = -(
                label * math.log(candidate)
                + (1 - label) * math.log(1.0 - candidate)
            )
            baseline_logloss = -(
                label * math.log(baseline)
                + (1 - label) * math.log(1.0 - baseline)
            )
            weighted_rows.append(
                (
                    baseline_logloss - candidate_logloss,
                    (baseline - label) ** 2
                    - (candidate - label) ** 2,
                    quality_weight,
                )
            )
        total = sum(row[2] for row in weighted_rows)
        if total <= 0.0:
            continue
        fixture_values.append(
            (
                sum(row[0] * row[2] for row in weighted_rows) / total,
                sum(row[1] * row[2] for row in weighted_rows) / total,
                total / max(1, len(fixture_samples)),
            )
        )

    def weighted_mean(indexes: Sequence[int], value_index: int) -> float:
        total_weight = sum(fixture_values[index][2] for index in indexes)
        return sum(
            fixture_values[index][value_index]
            * fixture_values[index][2]
            for index in indexes
        ) / total_weight

    count = len(fixture_values)
    if not count:
        return {
            "fixtures": 0,
            "log_loss": None,
            "brier": None,
            "bootstrap_iterations": 0,
            "log_loss_ci95": [None, None],
            "brier_ci95": [None, None],
        }
    indexes = list(range(count))
    point_logloss = weighted_mean(indexes, 0)
    point_brier = weighted_mean(indexes, 1)
    if count < 30:
        return {
            "fixtures": count,
            "log_loss": round(point_logloss, 9),
            "brier": round(point_brier, 9),
            "bootstrap_iterations": 0,
            "log_loss_ci95": [None, None],
            "brier_ci95": [None, None],
        }

    randomizer = random.Random(int(seed))
    logloss_samples: list[float] = []
    brier_samples: list[float] = []
    for _ in range(PROSPECTIVE_BOOTSTRAP_ITERATIONS):
        draw = [randomizer.randrange(count) for _ in range(count)]
        logloss_samples.append(weighted_mean(draw, 0))
        brier_samples.append(weighted_mean(draw, 1))
    logloss_samples.sort()
    brier_samples.sort()
    lower_index = int(
        math.floor(0.025 * (PROSPECTIVE_BOOTSTRAP_ITERATIONS - 1))
    )
    upper_index = int(
        math.ceil(0.975 * (PROSPECTIVE_BOOTSTRAP_ITERATIONS - 1))
    )
    return {
        "fixtures": count,
        "log_loss": round(point_logloss, 9),
        "brier": round(point_brier, 9),
        "bootstrap_iterations": PROSPECTIVE_BOOTSTRAP_ITERATIONS,
        "log_loss_ci95": [
            round(logloss_samples[lower_index], 9),
            round(logloss_samples[upper_index], 9),
        ],
        "brier_ci95": [
            round(brier_samples[lower_index], 9),
            round(brier_samples[upper_index], 9),
        ],
    }


def _summarize_group(records: Iterable[dict]) -> dict:
    records = list(records)
    target_samples: Dict[str, list[dict]] = {"next15": [], "to90": []}
    fixture_ids = {int(record["fixture_id"]) for record in records}
    timestamps = [
        timestamp
        for timestamp in (
            _parse_utc(record.get("created_at_utc")) for record in records
        )
        if timestamp is not None
    ]
    for record in records:
        outcome = record["outcome"]
        predictions = record["predictions"]
        for target in ("next15", "to90"):
            target_prediction = predictions.get(target)
            if not isinstance(target_prediction, Mapping):
                continue
            if str(target_prediction.get("status") or "") != "ok":
                continue
            label = _label(outcome, target)
            if label is None:
                continue
            quality_weight = 1.0
            if target == "next15":
                quality = str(
                    outcome.get("goal_within_15_quality") or ""
                ).strip().lower()
                if quality not in {"exact", "score_confirmed", "inferred"}:
                    continue
                if quality == "inferred":
                    quality_weight = 0.5
            candidate = _probability_pct(
                target_prediction.get("calibrated_probability_pct")
            )
            baseline = _probability_pct(
                target_prediction.get("base_probability_pct")
            )
            if candidate is None or baseline is None:
                continue
            target_samples[target].append(
                {
                    "fixture_id": int(record["fixture_id"]),
                    "label": label,
                    "candidate": candidate,
                    "baseline": baseline,
                    "quality_weight": quality_weight,
                }
            )

    targets: Dict[str, Any] = {}
    for target, samples in target_samples.items():
        baseline = _weighted_metrics(samples, "baseline")
        candidate = _weighted_metrics(samples, "candidate")
        improvement = _paired_fixture_improvement(
            samples,
            seed=1501 if target == "next15" else 9001,
        )
        positive_fixtures = {
            sample["fixture_id"] for sample in samples if sample["label"] == 1
        }
        negative_fixtures = {
            sample["fixture_id"] for sample in samples if sample["label"] == 0
        }
        span_days = (
            (max(timestamps) - min(timestamps)).total_seconds() / 86400.0
            if len(timestamps) >= 2
            else 0.0
        )
        reasons: list[str] = []
        if candidate["fixtures"] < 200:
            reasons.append("prospective_min_fixtures")
        if span_days < 28.0:
            reasons.append("prospective_min_span_days")
        if len(positive_fixtures) < 30:
            reasons.append("prospective_positive_class")
        if len(negative_fixtures) < 30:
            reasons.append("prospective_negative_class")
        if (
            improvement["log_loss"] is None
            or improvement["log_loss"]
            < PROSPECTIVE_MIN_LOGLOSS_IMPROVEMENT
        ):
            reasons.append("prospective_logloss_improvement")
        if (
            improvement["brier"] is None
            or improvement["brier"]
            < PROSPECTIVE_MIN_BRIER_IMPROVEMENT
        ):
            reasons.append("prospective_brier_improvement")
        if candidate["fixtures"] >= 200:
            logloss_lower = improvement["log_loss_ci95"][0]
            brier_lower = improvement["brier_ci95"][0]
            if logloss_lower is None or logloss_lower <= 0.0:
                reasons.append("prospective_logloss_confidence")
            if brier_lower is None or brier_lower <= 0.0:
                reasons.append("prospective_brier_confidence")
        targets[target] = {
            "status": "ready" if not reasons else "collecting",
            "readiness_reasons": reasons,
            "span_days": round(span_days, 6),
            "positive_fixtures": len(positive_fixtures),
            "negative_fixtures": len(negative_fixtures),
            "baseline": baseline,
            "candidate": candidate,
            "paired_improvement": improvement,
            "delta_log_loss": (
                round(candidate["log_loss"] - baseline["log_loss"], 8)
                if candidate["log_loss"] is not None
                and baseline["log_loss"] is not None
                else None
            ),
            "delta_brier": (
                round(candidate["brier"] - baseline["brier"], 8)
                if candidate["brier"] is not None
                and baseline["brier"] is not None
                else None
            ),
        }
    return {
        "records": len(records),
        "fixtures": len(fixture_ids),
        "first_prediction_utc": (
            min(timestamps).isoformat() if timestamps else None
        ),
        "last_prediction_utc": (
            max(timestamps).isoformat() if timestamps else None
        ),
        "targets": targets,
    }


def build_report(
    observation_path: str,
    prediction_path: str,
    model_path: str,
) -> Dict[str, Any]:
    observations = {
        str(row.get("observation_key") or row.get("observation_id") or ""): row
        for row in bot.load_joined_observation_history(
            observation_path,
            include_pending=False,
        )
    }
    prediction_records = list(iter_prediction_records(prediction_path))
    joined: list[dict] = []
    skipped = Counter()
    seen_prediction_keys: set[str] = set()
    for prediction in prediction_records:
        prediction_key = str(prediction.get("prediction_key") or "").strip()
        if not prediction_key:
            skipped["missing_prediction_key"] += 1
            continue
        if prediction_key in seen_prediction_keys:
            skipped["duplicate_prediction_key"] += 1
            continue
        seen_prediction_keys.add(prediction_key)
        if bool(prediction.get("production_applied", False)):
            skipped["production_applied"] += 1
            continue
        observation_id = str(prediction.get("observation_id") or "")
        observation = observations.get(observation_id)
        if not observation:
            skipped["missing_observation_or_outcome"] += 1
            continue
        outcome = (
            observation.get("outcome")
            if isinstance(observation.get("outcome"), dict)
            else {}
        )
        if str(outcome.get("status") or "") != "resolved":
            skipped["not_resolved"] += 1
            continue
        prediction_at = _parse_utc(prediction.get("created_at_utc"))
        outcome_at = _parse_utc(outcome.get("resolved_at_utc"))
        observation_at = _parse_utc(observation.get("created_at_utc"))
        model_created_at = _parse_utc(
            prediction.get("model_created_at_utc")
        )
        cutoff_at = _parse_utc(prediction.get("model_data_cutoff_utc"))
        if (
            prediction_at is None
            or outcome_at is None
            or observation_at is None
            or model_created_at is None
            or cutoff_at is None
            or model_created_at >= observation_at
            or prediction_at < observation_at
            or prediction_at > outcome_at
            or cutoff_at >= observation_at
        ):
            skipped["non_prospective_timing"] += 1
            continue
        predictions = prediction.get("predictions")
        if not isinstance(predictions, dict):
            skipped["missing_predictions"] += 1
            continue
        joined.append(
            {
                **prediction,
                "outcome": outcome,
                "predictions": predictions,
            }
        )

    by_model: Dict[str, list[dict]] = defaultdict(list)
    earliest_by_family_observation: Dict[tuple[str, str], dict] = {}
    for record in joined:
        model_id = str(record.get("model_id") or "unknown")
        algorithm = str(record.get("algorithm_version") or "unknown")
        by_model[model_id].append(record)
        key = (algorithm, str(record.get("observation_id") or ""))
        current = earliest_by_family_observation.get(key)
        if current is None or str(record.get("created_at_utc") or "") < str(
            current.get("created_at_utc") or ""
        ):
            earliest_by_family_observation[key] = record
    by_algorithm: Dict[str, list[dict]] = defaultdict(list)
    for (algorithm, _), record in earliest_by_family_observation.items():
        by_algorithm[algorithm].append(record)

    model = load_model(model_path)
    return {
        "shadow_only": True,
        "production_applied": False,
        "observation_source": os.path.abspath(observation_path),
        "prediction_source": os.path.abspath(prediction_path),
        "model_source": os.path.abspath(model_path),
        "current_model": {
            "model_id": model.get("model_id"),
            "status": model.get("status") or "missing",
            "created_at_utc": model.get("created_at_utc"),
            "data_cutoff_utc": model.get("data_cutoff_utc"),
            "training_summary": model.get("training_summary"),
            "targets": {
                name: {
                    "trained": payload.get("trained"),
                    "status": payload.get("status"),
                    "readiness_reasons": payload.get("readiness_reasons"),
                }
                for name, payload in (model.get("targets") or {}).items()
                if isinstance(payload, dict)
            },
        },
        "prediction_records_seen": len(prediction_records),
        "prospective_records_joined": len(joined),
        "skipped": dict(skipped),
        "by_model": {
            key: _summarize_group(records)
            for key, records in sorted(by_model.items())
        },
        "by_algorithm": {
            key: _summarize_group(records)
            for key, records in sorted(by_algorithm.items())
        },
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Report strictly prospective shadow-ML quality."
    )
    parser.add_argument("--observations", default=bot.OBSERVATION_HISTORY_FILE)
    parser.add_argument("--predictions", default=bot.SHADOW_ML_PREDICTIONS_FILE)
    parser.add_argument("--model", default=bot.SHADOW_ML_MODEL_FILE)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    report = build_report(args.observations, args.predictions, args.model)
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        try:
            _assert_safe_output(
                args.output,
                observation_path=args.observations,
                prediction_path=args.predictions,
                model_path=args.model,
            )
        except ValueError as exc:
            parser.error(str(exc))
        _write_text_atomic(args.output, rendered + "\n")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
