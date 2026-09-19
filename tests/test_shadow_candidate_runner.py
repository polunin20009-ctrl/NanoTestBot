from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from scripts.run_shadow_candidates import _assert_safe_journal


def _args(tmp_path: Path, journal: Path) -> argparse.Namespace:
    return argparse.Namespace(
        journal=str(journal),
        observation_file=str(tmp_path / "observations.jsonl"),
        decision_file=str(tmp_path / "decisions.jsonl"),
        static_predictions=str(tmp_path / "static.jsonl"),
        rolling_predictions=str(tmp_path / "rolling.jsonl"),
    )


@pytest.mark.parametrize(
    "source_name",
    (
        "observation_file",
        "decision_file",
        "static_predictions",
        "rolling_predictions",
    ),
)
def test_recovery_runner_rejects_output_source_collision(
    tmp_path: Path,
    source_name: str,
) -> None:
    args = _args(tmp_path, tmp_path / "candidate.jsonl")
    setattr(args, source_name, args.journal)

    with pytest.raises(ValueError, match="cannot overlap"):
        _assert_safe_journal(args)


def test_recovery_runner_rejects_gzip_active_output(tmp_path: Path) -> None:
    args = _args(tmp_path, tmp_path / "candidate.jsonl.gz")

    with pytest.raises(ValueError, match="must not be a gzip"):
        _assert_safe_journal(args)
