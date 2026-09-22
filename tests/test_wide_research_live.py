from __future__ import annotations

from datetime import datetime, timedelta, timezone

from wide_research import FEATURE_SCHEMA_VERSION, Clause, RuleManifest
from wide_research.lifecycle import build_active_manifest, write_active_manifest_atomic
from wide_research.live import ActiveRuleRouter, WideShadowLayer
from wide_research.store import WideResearchStore


UTC = timezone.utc


def _snapshot(*, probability: float = 82.0, fixture_id: int = 10) -> dict:
    observed = datetime(2026, 8, 28, 12, tzinfo=UTC)
    return {
        "record_type": "observation",
        "observation_id": f"{fixture_id}:50:BLOCK:v3",
        "fixture_id": fixture_id,
        "created_at_utc": observed.isoformat(),
        "stage": "decision_pipeline",
        "minute": 50,
        "match": {"score_home": 0, "score_away": 0, "league_id": 7},
        "probabilities": {"prob_to90": probability},
        "features": {},
        "raw_metrics": {},
        "availability": {},
        "gates": {
            "readiness_passed": True,
            "publication_context_passed": True,
        },
        "publication_policy": {"publication_context_passed": True},
    }


def _store(tmp_path):
    return WideResearchStore(
        tmp_path / "wide.sqlite3", allowed_root=tmp_path
    )


def _register(store, manifest, *, phase_id="phase-1"):
    store.register_rule_and_phase(
        rule_id=manifest.rule_id,
        manifest=manifest.as_dict(),
        phase_id=phase_id,
        starts_at_utc="2026-08-28T00:00:00+00:00",
        created_at_utc="2026-08-28T00:00:00+00:00",
        status="shadow",
    )


def test_live_layer_records_universe_and_only_first_trigger(tmp_path) -> None:
    store = _store(tmp_path)
    manifest = RuleManifest(
        "prob-80",
        "v1",
        (Clause("bot.prob_to90", ">=", 80),),
        feature_schema_version=FEATURE_SCHEMA_VERSION,
    )
    _register(store, manifest)
    layer = WideShadowLayer(store, refresh_seconds=0)
    first = layer.process_snapshot(_snapshot())
    second = layer.process_snapshot(_snapshot())
    assert len(first["claimed"]) == 1
    assert second["claimed"] == []
    assert store.metrics_for_phase("phase-1")["total"] == 1
    assert store.get_universe_fixture(10) is not None


def test_live_outcome_is_attached_to_every_matching_phase(tmp_path) -> None:
    store = _store(tmp_path)
    for index in (1, 2):
        manifest = RuleManifest(
            f"prob-{index}",
            "v1",
            (Clause("bot.prob_to90", ">=", 70),),
            feature_schema_version=FEATURE_SCHEMA_VERSION,
        )
        _register(store, manifest, phase_id=f"phase-{index}")
    layer = WideShadowLayer(store, refresh_seconds=0)
    snapshot = _snapshot()
    layer.process_snapshot(snapshot)
    result = layer.process_outcomes(
        [
            {
                "observation_id": snapshot["observation_id"],
                "outcome_schema_version": 1,
                "created_at_utc": "2026-08-28T14:00:00+00:00",
                "outcome": {
                    "status": "resolved",
                    "goal_to90_normal_time": True,
                    "resolved_at_utc": "2026-08-28T14:00:00+00:00",
                },
            }
        ]
    )
    assert result["updated_triggers"] == 2
    assert store.metrics_for_phase("phase-1")["win"] == 1
    assert store.metrics_for_phase("phase-2")["win"] == 1


def test_router_falls_back_until_manifest_is_production_enabled(tmp_path) -> None:
    path = tmp_path / "active.json"
    router = ActiveRuleRouter(str(path))
    assert router.route(_snapshot(), current_filter_allow=False)["allow"] is False
    disabled = build_active_manifest(
        generation=1,
        rule=None,
        effective_from_utc="2026-08-28T00:00:00+00:00",
        previous_rule_id=None,
        production_enabled=False,
    )
    write_active_manifest_atomic(path, disabled)
    assert router.route(_snapshot(), current_filter_allow=True)["source"] == "current_filter"


def test_router_applies_valid_champion_and_missing_rejects(tmp_path) -> None:
    manifest = RuleManifest(
        "prob-80",
        "v1",
        (Clause("bot.prob_to90", ">=", 80),),
        feature_schema_version=FEATURE_SCHEMA_VERSION,
    )
    active = build_active_manifest(
        generation=1,
        rule={"phase_id": "active-phase", "manifest": manifest.as_dict()},
        effective_from_utc="2026-08-28T00:00:00+00:00",
        previous_rule_id=None,
        production_enabled=True,
    )
    path = tmp_path / "active.json"
    write_active_manifest_atomic(path, active)
    router = ActiveRuleRouter(str(path))
    assert router.route(_snapshot(), current_filter_allow=False)["allow"] is True
    missing = _snapshot()
    missing["probabilities"].clear()
    decision = router.route(missing, current_filter_allow=True)
    assert decision["applied"] is True
    assert decision["allow"] is False

    path.unlink()
    fallback = router.route(_snapshot(), current_filter_allow=True)
    assert fallback["applied"] is False
    assert fallback["allow"] is True
    assert fallback["source"] == "current_filter"
    assert fallback["reason"] == "no_active_production_rule"


def test_router_falls_back_for_incompatible_feature_schema(tmp_path) -> None:
    manifest = RuleManifest(
        "future-schema",
        "v2",
        (Clause("bot.prob_to90", ">=", 80),),
        schema_version=2,
        feature_schema_version=FEATURE_SCHEMA_VERSION + 1,
    )
    active = build_active_manifest(
        generation=1,
        rule={"phase_id": "future-phase", "manifest": manifest.as_dict()},
        effective_from_utc="2026-08-28T00:00:00+00:00",
        previous_rule_id=None,
        production_enabled=True,
    )
    path = tmp_path / "active.json"
    write_active_manifest_atomic(path, active)

    decision = ActiveRuleRouter(str(path)).route(
        _snapshot(), current_filter_allow=True
    )

    assert decision["applied"] is False
    assert decision["allow"] is True
    assert decision["source"] == "current_filter"
    assert decision["reason"] == "incompatible_active_rule_feature_schema"


def test_live_paths_fail_closed_for_unpinned_legacy_schema(tmp_path) -> None:
    manifest = RuleManifest(
        "legacy-unpinned", "v1", (Clause("bot.prob_to90", ">=", 80),)
    )
    store = _store(tmp_path)
    _register(store, manifest)

    shadow = WideShadowLayer(store, refresh_seconds=0)
    result = shadow.process_snapshot(_snapshot())
    assert result["evaluated_phases"] == 0
    assert result["claimed"] == []

    active = build_active_manifest(
        generation=1,
        rule={"phase_id": "legacy-phase", "manifest": manifest.as_dict()},
        effective_from_utc="2026-08-28T00:00:00+00:00",
        previous_rule_id=None,
        production_enabled=True,
    )
    path = tmp_path / "legacy-active.json"
    write_active_manifest_atomic(path, active)
    decision = ActiveRuleRouter(str(path)).route(
        _snapshot(), current_filter_allow=True
    )
    assert decision["applied"] is False
    assert decision["allow"] is True
    assert decision["reason"] == "incompatible_active_rule_feature_schema"
