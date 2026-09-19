"""Prospective, shadow-only candidate rule evaluation.

The package deliberately has no dependency on :mod:`NanoTest` and never sends
Telegram messages or changes production probabilities.
"""

from .engine import CandidateLayer, build_prediction_index
from .rules import (
    ARM_CONTROL,
    ARM_FULL_SLICES,
    ARM_FULL_SLICES_GOALS_LE2,
    ARM_FULL_SLICES_GOALS_LE2_CLOSE,
    ARM_ROLLING_ML_CONFIRM_75,
    DEFAULT_RULESET,
    evaluate_candidate_arms,
)
from .storage import AppendOnlyCandidateJournal, iter_jsonl_records

__all__ = [
    "ARM_CONTROL",
    "ARM_FULL_SLICES",
    "ARM_FULL_SLICES_GOALS_LE2",
    "ARM_FULL_SLICES_GOALS_LE2_CLOSE",
    "ARM_ROLLING_ML_CONFIRM_75",
    "AppendOnlyCandidateJournal",
    "CandidateLayer",
    "DEFAULT_RULESET",
    "build_prediction_index",
    "evaluate_candidate_arms",
    "iter_jsonl_records",
]
