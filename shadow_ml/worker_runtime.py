"""Runtime safety helpers for short-lived shadow-ML worker processes."""

from __future__ import annotations

import os
from typing import Mapping, Optional


WORKER_MEMORY_LIMIT_ENV = "GOALBOT_SHADOW_ML_WORKER_MEMORY_LIMIT_MB"


def worker_memory_limit_mb(
    environment: Mapping[str, str] = os.environ,
    *,
    env_name: str = WORKER_MEMORY_LIMIT_ENV,
) -> Optional[int]:
    raw = str(environment.get(env_name) or "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def apply_worker_memory_limit(
    environment: Mapping[str, str] = os.environ,
    *,
    env_name: str = WORKER_MEMORY_LIMIT_ENV,
) -> Optional[int]:
    """Apply a hard address-space ceiling before the heavyweight bot import.

    The helper is Linux/Unix specific by design.  If ``resource`` or
    ``RLIMIT_AS`` is unavailable, it fails open and process isolation still
    protects the live bot from retained Python arenas.
    """

    limit_mb = worker_memory_limit_mb(environment, env_name=env_name)
    if limit_mb is None:
        return None
    try:
        import resource

        requested = int(limit_mb) * 1024 * 1024
        current_soft, current_hard = resource.getrlimit(resource.RLIMIT_AS)
        infinity = resource.RLIM_INFINITY
        effective = requested
        if current_hard != infinity:
            effective = min(effective, int(current_hard))
        if current_soft != infinity:
            effective = min(effective, int(current_soft))
        resource.setrlimit(resource.RLIMIT_AS, (effective, effective))
        return int(effective // (1024 * 1024))
    except (AttributeError, ImportError, OSError, ValueError):
        return None
