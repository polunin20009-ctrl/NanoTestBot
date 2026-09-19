"""Shadow-only reputation calibration for signal decisions."""

from .model import (
    EXPANDED_BLEND_COHORT,
    ReputationConfig,
    build_expanded_blend_cohort,
    build_reputation_model,
    evaluate_shadow_decision,
)
from .storage import (
    append_shadow_record,
    iter_shadow_records,
    load_reputation_model,
    save_reputation_model,
)

__all__ = [
    "ReputationConfig",
    "EXPANDED_BLEND_COHORT",
    "append_shadow_record",
    "build_expanded_blend_cohort",
    "build_reputation_model",
    "evaluate_shadow_decision",
    "load_reputation_model",
    "iter_shadow_records",
    "save_reputation_model",
]
