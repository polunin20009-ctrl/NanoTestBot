from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .storage import save_model


@dataclass(frozen=True)
class CandidateComparison:
    promotable: bool
    reasons: tuple[str, ...]


def save_candidate(path: str | Path, artifact: Mapping[str, Any]) -> None:
    """Atomically save a shadow-only candidate without touching champion."""
    payload = dict(artifact)
    payload["shadow_only"] = True
    payload["production_applied"] = False
    payload["artifact_role"] = "candidate"
    if "checksum_sha256" in payload:
        payload.pop("checksum_sha256", None)
        canonical = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        payload["checksum_sha256"] = hashlib.sha256(canonical).hexdigest()
    save_model(path, payload)


def compare_holdout_metrics(
    champion: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> CandidateComparison:
    """Conservatively accept only candidates better on every target/metric."""
    reasons: list[str] = []
    for target in ("next15", "to90"):
        champion_target = ((champion.get("targets") or {}).get(target) or {})
        candidate_target = ((candidate.get("targets") or {}).get(target) or {})
        champion_metrics = (
            ((champion_target.get("metrics") or {}).get("holdout") or {}).get(
                "candidate"
            )
            or {}
        )
        candidate_metrics = (
            ((candidate_target.get("metrics") or {}).get("holdout") or {}).get(
                "candidate"
            )
            or {}
        )
        for metric in ("log_loss", "brier", "ece"):
            old = champion_metrics.get(metric)
            new = candidate_metrics.get(metric)
            if old is None or new is None:
                reasons.append(f"{target}:{metric}:missing")
            elif float(new) >= float(old):
                reasons.append(f"{target}:{metric}:not_improved")
    if candidate.get("production_applied") is not False:
        reasons.append("candidate:production_applied_must_be_false")
    if candidate.get("shadow_only") is not True:
        reasons.append("candidate:shadow_only_must_be_true")
    return CandidateComparison(not reasons, tuple(reasons))
