from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping


SCHEMA_VERSION = 1

PASS = "PASS"
FAIL = "FAIL"
UNAVAILABLE = "UNAVAILABLE"
EVALUATION_STATUSES = (PASS, FAIL, UNAVAILABLE)

DECISION_PIPELINE = "decision_pipeline"
WIDE_MONITOR = "wide_monitor"
SUPPORTED_UNIVERSE_STAGES = (DECISION_PIPELINE, WIDE_MONITOR)


def canonical_json(payload: Mapping[str, Any]) -> str:
    """Return the single byte-for-byte representation used for manifests."""
    return json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def canonical_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class UniverseSpec:
    """Immutable contract for observations visible to broad research.

    Publication context and readiness are technical data-quality gates here.
    The production signal filter is deliberately not part of this contract.
    """

    universe_id: str = "all_technically_eligible_46_60_v1"
    schema_version: int = SCHEMA_VERSION
    stages: tuple[str, ...] = SUPPORTED_UNIVERSE_STAGES
    min_minute: int = 46
    max_minute: int = 60
    require_readiness_passed: bool = True
    require_publication_context_passed: bool = True
    current_filter_policy: str = "ignored"

    def __post_init__(self) -> None:
        universe_id = str(self.universe_id or "").strip()
        if not universe_id:
            raise ValueError("universe_id must be non-empty")
        object.__setattr__(self, "universe_id", universe_id)

        if type(self.schema_version) is not int or self.schema_version < 1:
            raise ValueError("schema_version must be a positive integer")

        stages = tuple(sorted({str(stage).strip() for stage in self.stages}))
        if not stages:
            raise ValueError("at least one universe stage is required")
        unsupported = set(stages) - set(SUPPORTED_UNIVERSE_STAGES)
        if unsupported:
            raise ValueError(
                "unsupported universe stages: " + ", ".join(sorted(unsupported))
            )
        object.__setattr__(self, "stages", stages)

        if (
            type(self.min_minute) is not int
            or type(self.max_minute) is not int
            or self.min_minute < 1
            or self.max_minute < self.min_minute
        ):
            raise ValueError("minute bounds must be ordered positive integers")
        if type(self.require_readiness_passed) is not bool:
            raise ValueError("require_readiness_passed must be bool")
        if type(self.require_publication_context_passed) is not bool:
            raise ValueError("require_publication_context_passed must be bool")
        if self.current_filter_policy != "ignored":
            raise ValueError("wide research must ignore the current signal filter")

    def as_dict(self) -> dict[str, Any]:
        return {
            "universe_id": self.universe_id,
            "schema_version": self.schema_version,
            "stages": list(self.stages),
            "minute_range": {
                "min_inclusive": self.min_minute,
                "max_inclusive": self.max_minute,
            },
            "technical_eligibility": {
                "require_readiness_passed": self.require_readiness_passed,
                "require_publication_context_passed": (
                    self.require_publication_context_passed
                ),
            },
            "current_filter_policy": self.current_filter_policy,
        }

    @property
    def manifest_hash(self) -> str:
        return canonical_hash(self.as_dict())


DEFAULT_UNIVERSE = UniverseSpec()

