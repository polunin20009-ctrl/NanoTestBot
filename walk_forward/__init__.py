"""Offline, read-only walk-forward evaluation for channel signal rules."""

from .engine import (
    EvaluationConfig,
    build_walk_forward_report,
    discover_journal_paths,
    render_text_report,
)

__all__ = [
    "EvaluationConfig",
    "build_walk_forward_report",
    "discover_journal_paths",
    "render_text_report",
]
