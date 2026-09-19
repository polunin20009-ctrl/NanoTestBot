from __future__ import annotations

import json

from wide_research.lifecycle import (
    ActiveManifestCache,
    LifecyclePolicy,
    binomial_upper_tail,
    build_active_manifest,
    evaluate_degradation,
    evaluate_readiness,
    validate_active_manifest,
    wilson_interval,
    write_active_manifest_atomic,
)


def _ready_metrics() -> dict:
    return {
        "wins": 285,
        "losses": 15,
        "span_days": 35.0,
        "trigger_days": 24,
        "leagues": 12,
        "max_league_share": 0.20,
        "triggers_per_week": 6.0,
        "weekly": [
            {"wins": 68, "losses": 4},
            {"wins": 72, "losses": 3},
            {"wins": 70, "losses": 4},
            {"wins": 75, "losses": 4},
        ],
    }


def test_wilson_prevents_small_perfect_sample_from_looking_certain():
    lower, upper = wilson_interval(20, 20)
    assert 0.83 < lower < 0.85
    assert upper == 1.0


def test_binomial_tail_is_stable_at_largest_registered_look():
    tail = binomial_upper_tail(950, 1000, 0.9)
    assert 0.0 < tail < 1.0
    assert tail < binomial_upper_tail(949, 1000, 0.9)


def test_readiness_requires_prospective_volume_and_closed_milestone():
    small = _ready_metrics()
    small.update({"wins": 19, "losses": 1})
    decision = evaluate_readiness(small)
    assert not decision.eligible
    assert "insufficient_resolved" in decision.reasons
    assert "not_closed_milestone" in decision.reasons


def test_readiness_accepts_strong_diverse_stable_candidate():
    decision = evaluate_readiness(
        _ready_metrics(),
        family_size=1,
        family_rank=1,
    )
    assert decision.eligible
    assert decision.target_state == "READY"
    assert decision.metrics["resolved"] == 300


def test_readiness_rejects_league_concentration():
    metrics = _ready_metrics()
    metrics["max_league_share"] = 0.8
    decision = evaluate_readiness(metrics)
    assert not decision.eligible
    assert "league_concentration" in decision.reasons


def test_degradation_uses_hysteresis_and_tenure():
    healthy = evaluate_degradation(
        recent_fast={"wins": 48, "losses": 2},
        recent_slow={"wins": 143, "losses": 7},
        tenure_days=30,
    )
    assert not healthy.eligible
    too_early = evaluate_degradation(
        recent_fast={"wins": 35, "losses": 15},
        recent_slow={"wins": 120, "losses": 30},
        tenure_days=2,
    )
    assert not too_early.eligible
    severe = evaluate_degradation(
        recent_fast={"wins": 35, "losses": 15},
        recent_slow={"wins": 120, "losses": 30},
        tenure_days=30,
    )
    assert severe.eligible
    assert severe.target_state == "DEGRADED"


def test_active_manifest_checksum_atomic_and_last_known_good(tmp_path):
    path = tmp_path / "active.json"
    first = build_active_manifest(
        generation=1,
        rule={"rule_id": "rule:one", "conditions": []},
        effective_from_utc="2026-08-27T00:00:00+00:00",
        previous_rule_id=None,
        production_enabled=False,
    )
    assert validate_active_manifest(first)
    write_active_manifest_atomic(path, first)
    cache = ActiveManifestCache(path)
    assert cache.load()["generation"] == 1

    path.write_text('{"schema_version":1,"generation":2}', encoding="utf-8")
    assert cache.load(force=True)["generation"] == 1

    same_generation_mutation = build_active_manifest(
        generation=1,
        rule=None,
        effective_from_utc="2026-08-27T01:00:00+00:00",
        previous_rule_id="rule:one",
        production_enabled=False,
    )
    path.write_text(json.dumps(same_generation_mutation), encoding="utf-8")
    assert cache.load(force=True)["generation"] == 1


def test_manifest_generation_is_strict_and_validation_fails_closed():
    for invalid_generation in (0, True, 1.5, "1"):
        try:
            build_active_manifest(
                generation=invalid_generation,
                rule=None,
                effective_from_utc="2026-08-27T00:00:00+00:00",
                previous_rule_id=None,
                production_enabled=False,
            )
        except ValueError:
            pass
        else:
            raise AssertionError("invalid generation accepted")
    assert not validate_active_manifest(
        {
            "schema_version": 1,
            "generation": 1,
            "fallback": "current_filter",
            "checksum": "invalid",
            "nan": float("nan"),
        }
    )

def test_policy_rejects_invalid_rate():
    try:
        LifecyclePolicy(target_hit_rate=1.1)
    except ValueError:
        pass
    else:
        raise AssertionError("invalid rate accepted")
