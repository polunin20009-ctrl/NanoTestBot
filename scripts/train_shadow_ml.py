from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Importing the bot from this utility must never start its persistence daemon or
# run state migrations.  The live process owns all mutable runtime state.
os.environ["GOALBOT_LIBRARY_MODE"] = "1"

from shadow_ml.worker_runtime import apply_worker_memory_limit

_WORKER_MEMORY_LIMIT_MB = apply_worker_memory_limit()

import NanoTest as bot
from shadow_ml import save_model, train_shadow_model


def _normalized_path(path: os.PathLike[str] | str) -> str:
    return os.path.normcase(os.path.realpath(os.path.abspath(os.fspath(path))))


def _assert_safe_output(
    input_path: str,
    output: Path,
    *,
    variant: str = "static",
) -> None:
    protected = {_normalized_path(input_path)}
    protected.update(
        _normalized_path(path)
        for path in bot._observation_history_paths(input_path)
    )
    protected.update(
        {
            _normalized_path(bot.SHADOW_ML_PREDICTIONS_FILE),
            _normalized_path(bot.SHADOW_ML_ROLLING_PREDICTIONS_FILE),
        }
    )
    protected.add(
        _normalized_path(
            bot.SHADOW_ML_MODEL_FILE
            if variant == "rolling"
            else bot.SHADOW_ML_ROLLING_MODEL_FILE
        )
    )
    if _normalized_path(output) in protected:
        raise ValueError(
            "model output cannot replace the observation journal or an archive"
        )


def _summary(artifact: dict, *, output: Optional[Path], written: bool) -> dict:
    targets = artifact.get("targets") if isinstance(artifact.get("targets"), dict) else {}
    return {
        "model_id": artifact.get("model_id"),
        "artifact_role": artifact.get("artifact_role"),
        "feature_profile": artifact.get("feature_profile"),
        "status": artifact.get("status"),
        "shadow_only": True,
        "production_applied": False,
        "created_at_utc": artifact.get("created_at_utc"),
        "data_cutoff_utc": artifact.get("data_cutoff_utc"),
        "training_summary": artifact.get("training_summary"),
        "targets": {
            name: {
                "trained": (payload or {}).get("trained"),
                "status": (payload or {}).get("status"),
                "readiness_reasons": (payload or {}).get("readiness_reasons"),
                "counts": (payload or {}).get("counts"),
                "holdout_metrics": (
                    ((payload or {}).get("metrics") or {}).get("holdout")
                ),
            }
            for name, payload in targets.items()
            if isinstance(payload, dict)
        },
        "written": bool(written),
        "output": str(output.resolve()) if output else None,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Train the strictly shadow-only residual goal model from joined "
            "decision_pipeline observations."
        )
    )
    parser.add_argument("--input", default=bot.OBSERVATION_HISTORY_FILE)
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
    )
    parser.add_argument(
        "--variant",
        choices=("static", "rolling"),
        default="static",
        help="train the incumbent static family or the rolling challenger",
    )
    parser.add_argument(
        "--current-model",
        type=Path,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="train and print readiness without replacing the model artifact",
    )
    parser.add_argument(
        "--managed-worker",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--force", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--trigger", default="cli", help=argparse.SUPPRESS)
    parser.add_argument(
        "--data-revision-hint",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args(argv)
    output = args.output or Path(
        bot.SHADOW_ML_ROLLING_MODEL_FILE
        if args.variant == "rolling"
        else bot.SHADOW_ML_MODEL_FILE
    )

    if args.managed_worker and args.dry_run:
        parser.error("--managed-worker cannot be combined with --dry-run")
    if not args.dry_run:
        try:
            _assert_safe_output(
                args.input,
                output,
                variant=str(args.variant),
            )
        except ValueError as exc:
            parser.error(str(exc))

    if args.managed_worker:
        # The parent gives us a point-in-time journal snapshot.  Only this
        # short-lived process materializes it and fits the model, so released
        # Python arenas disappear when the worker exits.
        bot.OBSERVATION_HISTORY_FILE = os.path.abspath(args.input)
        current_model = os.path.abspath(
            os.fspath(args.current_model or output)
        )
        if args.variant == "rolling":
            bot.SHADOW_ML_ROLLING_MODEL_FILE = current_model
        else:
            bot.SHADOW_ML_MODEL_FILE = current_model
        result = bot._train_shadow_ml_once_local(
            force=bool(args.force),
            trigger=str(args.trigger),
            data_revision_hint=bool(args.data_revision_hint),
            history_path=bot.OBSERVATION_HISTORY_FILE,
            output_path=os.path.abspath(os.fspath(output)),
            variant=str(args.variant),
        )
        result["worker_memory_limit_mb"] = _WORKER_MEMORY_LIMIT_MB
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
        return 0

    records, history_spool = bot.load_shadow_ml_training_history(args.input)
    trainer = (
        bot.train_shadow_rolling_model
        if args.variant == "rolling"
        else train_shadow_model
    )
    artifact = trainer(
        records,
        config=bot._shadow_ml_config(),
        now=datetime.now(timezone.utc),
    )
    written = False
    if not args.dry_run:
        save_model(output, artifact)
        written = True
    print(
        json.dumps(
            {
                **_summary(artifact, output=output, written=written),
                "history_spool": history_spool,
                "worker_memory_limit_mb": _WORKER_MEMORY_LIMIT_MB,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
