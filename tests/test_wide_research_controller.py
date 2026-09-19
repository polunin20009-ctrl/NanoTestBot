from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from dataclasses import replace

import pytest

import wide_research.controller as controller_module
from wide_research.controller import WideResearchController
from wide_research.lifecycle import LifecyclePolicy
from wide_research.live import WideShadowLayer
from wide_research.schema import canonical_hash
from wide_research.store import WideResearchStore


UTC = timezone.utc


def _report() -> dict:
    candidate_body = {
        "schema_version": 1,
        "manifest_version": "wide_rule_manifest_v2",
        "candidate_id": "wide-test-rule",
        "rank_at_discovery": 1,
        "engine_version": "wide_rule_discovery_v1",
        "feature_schema_version": 2,
        "threshold_grid_version": "wide_atomic_grid_v1",
        "universe": {
            "stages": ["decision_pipeline", "wide_monitor"],
            "minute_min": 46,
            "minute_max": 60,
        },
        "trigger_policy": "first_matching_observation_per_fixture",
        "clauses": [
            {"feature": "bot.prob_to90", "operator": ">=", "threshold": 80.0}
        ],
        "selection_data": {},
        "production_applied": False,
        "state": "RESEARCH",
    }
    candidate = {
        **candidate_body,
        "manifest_sha256": canonical_hash(candidate_body),
    }
    manifests_body = {
        "schema_version": 1,
        "engine_version": "wide_rule_discovery_v1",
        "feature_schema_version": 2,
        "threshold_grid_version": "wide_atomic_grid_v1",
        "dataset_as_of_utc": "2026-08-27T12:00:00+00:00",
        "config": {},
        "split_policy": "chronological_fixture_group_v1",
        "splits": {},
        "input": {},
        "threshold_manifest": {},
        "search": {},
        "artifact_type": "wide_rule_candidate_manifests",
        "candidates": [candidate],
    }
    results_body = {
        "schema_version": 1,
        "engine_version": "wide_rule_discovery_v1",
        "feature_schema_version": 2,
        "artifact_type": "wide_rule_candidate_results",
        "dataset_as_of_utc": "2026-08-27T12:00:00+00:00",
        "split_policy": "chronological_fixture_group_v1",
        "splits": {},
        "null_hit_rate": 0.9,
        "family_alpha": 0.05,
        "results": [
            {
                "candidate_id": "wide-test-rule",
                "rank_at_discovery": 1,
                "holdout": {"wins": 19, "losses": 1},
                "statistical_test": {"reject_null_at_alpha": False},
            }
        ],
    }
    return {
        "run_id": "wide-discovery-test",
        "manifests": {
            **manifests_body,
            "artifact_sha256": canonical_hash(manifests_body),
        },
        "results": {
            **results_body,
            "artifact_sha256": canonical_hash(results_body),
        },
    }


def _generation_report(
    run_id: str,
    generation: str,
    candidate_count: int,
    *,
    portfolio_members: tuple[int, ...] = (),
    candidate_namespace: str | None = None,
) -> dict:
    engine = f"wide_rule_discovery_{generation}"
    grid = f"wide_atomic_grid_{generation}"
    namespace = candidate_namespace or generation
    candidates = []
    results = []
    for index in range(candidate_count):
        candidate_id = f"wide-{namespace}-{index}"
        body = {
            "schema_version": 2,
            "manifest_version": "wide_rule_manifest_v2",
            "candidate_id": candidate_id,
            "rank_at_discovery": index + 1,
            "engine_version": engine,
            "feature_schema_version": 2,
            "threshold_grid_version": grid,
            "universe": {
                "stages": ["decision_pipeline", "wide_monitor"],
                "minute_min": 46,
                "minute_max": 60,
            },
            "trigger_policy": "first_matching_observation_per_fixture",
            "clauses": [
                {
                    "feature": "bot.prob_to90",
                    "operator": ">=",
                    "threshold": 70.0 + index,
                }
            ],
            "selection_data": {},
            "production_applied": False,
            "state": "RESEARCH",
        }
        candidates.append({**body, "manifest_sha256": canonical_hash(body)})
        results.append(
            {
                "candidate_id": candidate_id,
                "rank_at_discovery": index + 1,
                "holdout": {"wins": 9, "losses": 1},
                "statistical_test": {"reject_null_at_alpha": False},
            }
        )
    portfolio = None
    if portfolio_members:
        portfolio = {
            "portfolio_id": f"portfolio-{generation}",
            "member_candidate_ids": [
                f"wide-{namespace}-{index}" for index in portfolio_members
            ],
            "trigger_policy": "first_matching_observation_per_fixture",
            "selection_split": "validation",
        }
    manifests_body = {
        "schema_version": 2,
        "engine_version": engine,
        "feature_schema_version": 2,
        "threshold_grid_version": grid,
        "dataset_as_of_utc": "2026-09-01T00:00:00+00:00",
        "config": {},
        "split_policy": "chronological_fixture_group_v1",
        "splits": {},
        "input": {},
        "threshold_manifest": {},
        "search": {},
        "artifact_type": "wide_rule_candidate_manifests",
        "candidates": candidates,
        "selected_portfolio": portfolio,
        "best_available_portfolio": None,
    }
    results_body = {
        "schema_version": 2,
        "engine_version": engine,
        "feature_schema_version": 2,
        "threshold_grid_version": grid,
        "artifact_type": "wide_rule_candidate_results",
        "dataset_as_of_utc": "2026-09-01T00:00:00+00:00",
        "split_policy": "chronological_fixture_group_v1",
        "splits": {},
        "null_hit_rate": 0.9,
        "family_alpha": 0.05,
        "results": results,
        "selected_portfolio": None,
        "best_available_portfolio": None,
    }
    return {
        "run_id": run_id,
        "manifests": {
            **manifests_body,
            "artifact_sha256": canonical_hash(manifests_body),
        },
        "results": {
            **results_body,
            "artifact_sha256": canonical_hash(results_body),
        },
    }


def _purged_successor(report: dict, *, run_id: str) -> dict:
    successor = copy.deepcopy(report)
    successor["run_id"] = run_id
    manifests = successor["manifests"]
    manifests.pop("artifact_sha256", None)
    manifests["engine_version"] += controller_module.TEMPORAL_PURGED_ENGINE_SUFFIX
    manifests["split_policy"] = controller_module.PURGED_SPLIT_POLICY
    manifests["config"] = {
        **manifests.get("config", {}),
        "temporal_purge": True,
        "temporal_embargo_seconds": 300.0,
    }
    split_specs = {
        "train": ([101], "2026-09-01T00:00:00+00:00"),
        "validation": ([102], "2026-09-01T02:00:00+00:00"),
        "holdout": ([103], "2026-09-01T04:00:00+00:00"),
    }
    splits = {
        name: {
            "fixture_count": len(ids),
            "observation_count": len(ids),
            "fixture_ids": ids,
            "fixture_ids_sha256": canonical_hash(ids),
            "first_observation_utc": observed_at,
            "last_observation_utc": observed_at,
        }
        for name, (ids, observed_at) in split_specs.items()
    }
    splits["temporal_purge"] = {
        "version": controller_module.TEMPORAL_PURGE_VERSION,
        "embargo_seconds": 300.0,
        "no_fixture_reallocation": True,
        "boundaries": {
            "train_to_validation": {
                "next_split_first_observation_utc": "2026-09-01T02:00:00+00:00",
                "information_cutoff_utc": "2026-09-01T01:55:00+00:00",
                "nominal_fixture_count": 1,
                "retained_fixture_count": 1,
                "purged_fixture_count": 0,
                "purged_fixture_ids_sha256": canonical_hash([]),
                "purge_reasons": {},
            },
            "validation_to_holdout": {
                "next_split_first_observation_utc": "2026-09-01T04:00:00+00:00",
                "information_cutoff_utc": "2026-09-01T03:55:00+00:00",
                "nominal_fixture_count": 1,
                "retained_fixture_count": 1,
                "purged_fixture_count": 0,
                "purged_fixture_ids_sha256": canonical_hash([]),
                "purge_reasons": {},
            },
        },
    }
    manifests["splits"] = splits
    results = successor["results"]
    results.pop("artifact_sha256", None)
    results["engine_version"] = manifests["engine_version"]
    results["split_policy"] = manifests["split_policy"]
    results["splits"] = copy.deepcopy(splits)
    result_rows = results["results"]
    for index, (candidate, result) in enumerate(
        zip(manifests["candidates"], result_rows)
    ):
        candidate.pop("manifest_sha256", None)
        candidate["candidate_id"] = f"wide-purged-{index}"
        candidate["engine_version"] = manifests["engine_version"]
        candidate["clauses"][0]["threshold"] = 80.0 + index
        candidate["manifest_sha256"] = canonical_hash(candidate)
        result["candidate_id"] = candidate["candidate_id"]
    manifests["artifact_sha256"] = canonical_hash(manifests)
    results["artifact_sha256"] = canonical_hash(results)
    return successor


def _snapshot(fixture_id: int, observed: datetime) -> dict:
    return {
        "record_type": "observation",
        "observation_id": f"{fixture_id}:50:BLOCK:v3",
        "fixture_id": fixture_id,
        "created_at_utc": observed.isoformat(),
        "stage": "decision_pipeline",
        "minute": 50,
        "match": {"score_home": 0, "score_away": 0, "league_id": fixture_id},
        "probabilities": {"prob_to90": 85.0},
        "features": {},
        "raw_metrics": {},
        "availability": {},
        "gates": {
            "readiness_passed": True,
            "publication_context_passed": True,
        },
        "publication_policy": {"publication_context_passed": True},
    }


def _policy() -> LifecyclePolicy:
    return LifecyclePolicy(
        target_hit_rate=0.5,
        null_hit_rate=0.4,
        min_resolved=2,
        min_span_days=0.0,
        min_trigger_days=1,
        min_leagues=1,
        max_league_share=1.0,
        min_wilson_lower=0.0,
        min_weekly_windows=1,
        min_weekly_hit_rate=0.5,
        min_triggers_per_week=0.1,
        family_alpha=1.0,
        allowed_looks=(2,),
    )


def test_import_starts_new_phase_now_and_never_backfills_history(tmp_path) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-08-28T00:00:00+00:00",
    )
    imported = controller.import_report(
        _report(), imported_at_utc="2026-08-29T12:00:00+00:00"
    )
    assert imported["prospective_start_utc"] == "2026-08-29T12:00:00+00:00"
    phase_id = imported["imported"][0]["phase_id"]
    assert store.metrics_for_phase(phase_id)["total"] == 0
    repeated = controller.import_report(
        _report(), imported_at_utc="2026-08-30T12:00:00+00:00"
    )
    assert repeated["imported"] == []
    assert repeated["reused"][0]["phase_id"] == phase_id


def test_new_search_generation_replaces_old_shadow_pool_without_outcomes(
    tmp_path,
) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-09-01T00:00:00+00:00",
        max_shadow_rules=4,
    )
    first = controller.import_report(
        _generation_report("run-v1", "v1", 2),
        imported_at_utc="2026-09-01T01:00:00+00:00",
    )
    old_phase_ids = {row["phase_id"] for row in first["imported"]}

    second = controller.import_report(
        _generation_report(
            "run-v2", "v2", 4, portfolio_members=(0, 1)
        ),
        imported_at_utc="2026-09-02T01:00:00+00:00",
    )

    assert len(second["imported"]) == 4
    assert set(second["retired"]) == old_phase_ids
    assert second["pool"]["outcome_adaptive_eviction"] is False
    phases = store.list_phases()
    assert sum(row["status"] == "shadow" for row in phases) == 4
    assert sum(row["status"] == "retired" for row in phases) == 2
    selected = [
        row
        for row in phases
        if row["status"] == "shadow"
        and row["policy"]["discovery"]["selected_portfolio_member"]
    ]
    assert len(selected) == 2
    assert all(
        row["policy"]["discovery"]["search_generation"][
            "generation_id"
        ]
        == second["search_generation"]["generation_id"]
        for row in selected
    )


def test_purge_transition_preserves_clocks_and_admits_one_new_cohort(
    tmp_path,
) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-09-01T00:00:00+00:00",
        max_shadow_rules=10,
    )
    first = controller.import_report(
        _generation_report("pre-purge", "v3", 10),
        imported_at_utc="2026-09-01T01:00:00+00:00",
    )
    old_phase_ids = {row["phase_id"] for row in first["imported"]}
    old_phase_id = first["imported"][0]["phase_id"]
    WideShadowLayer(store, refresh_seconds=0).process_snapshot(
        _snapshot(9001, datetime(2026, 9, 2, tzinfo=UTC))
    )
    assert store.metrics_for_phase(old_phase_id)["total"] == 1

    successor = _purged_successor(
        _generation_report("placeholder", "v3", 10),
        run_id="first-purged",
    )
    migrated = controller.import_report(
        successor,
        imported_at_utc="2026-09-03T01:00:00+00:00",
    )

    assert migrated["retired"] == []
    assert len(migrated["imported"]) == 10
    assert set(migrated["purge_transition_preserved_phase_ids"]) == old_phase_ids
    assert migrated["pool"]["limit"] == 10
    assert migrated["pool"]["effective_limit"] == 20
    assert migrated["pool"]["purge_transition_reserve"] == 10
    assert store.metrics_for_phase(old_phase_id)["total"] == 1
    assert {
        row["phase_id"]
        for row in store.list_phases(statuses=("shadow",))
    }.issuperset(old_phase_ids)
    assert len(store.list_phases(statuses=("shadow",))) == 20

    # The bounded overlap remains valid on a repeated current-generation
    # import; it neither resets old clocks nor grows beyond the reserve.
    repeated = controller.import_report(
        successor,
        imported_at_utc="2026-09-04T01:00:00+00:00",
    )
    assert repeated["imported"] == []
    assert len(store.list_phases(statuses=("shadow",))) == 20
    assert store.metrics_for_phase(old_phase_id)["total"] == 1

    # The runtime evaluator is configured for the same bounded migration
    # capacity, so neither the preserved evidence nor the new purged cohort is
    # silently starved during the overlap.
    evaluated = WideShadowLayer(
        store, max_active_rules=20, refresh_seconds=0
    ).process_snapshot(
        _snapshot(9002, datetime(2026, 9, 5, tzinfo=UTC))
    )
    active_phase_ids = {
        row["phase_id"] for row in store.list_phases(statuses=("shadow",))
    }
    assert evaluated["evaluated_phases"] == 20
    assert {
        row["phase_id"] for row in evaluated["evaluations"]
    } == active_phase_ids


def test_purged_report_requires_matching_partition_artifacts(tmp_path) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-09-01T00:00:00+00:00",
    )
    report = _purged_successor(
        _generation_report("placeholder", "v3", 1),
        run_id="invalid-purged",
    )
    report["results"].pop("artifact_sha256")
    report["results"]["splits"]["holdout"]["fixture_ids"] = [999]
    report["results"]["artifact_sha256"] = canonical_hash(report["results"])

    with pytest.raises(ValueError, match="discovery splits mismatch"):
        controller.import_report(
            report, imported_at_utc="2026-09-03T01:00:00+00:00"
        )
    assert store.list_phases() == []


def test_same_generation_portfolio_is_never_partially_admitted(tmp_path) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-09-01T00:00:00+00:00",
        max_shadow_rules=4,
    )
    controller.import_report(
        _generation_report("run-v2-a", "v2", 3),
        imported_at_utc="2026-09-01T01:00:00+00:00",
    )

    with pytest.raises(RuntimeError, match="does not fit atomically"):
        controller.import_report(
            _generation_report(
                "run-v2-b",
                "v2",
                3,
                portfolio_members=(0, 1),
                candidate_namespace="v2b",
            ),
            imported_at_utc="2026-09-02T01:00:00+00:00",
        )

    assert len(store.list_phases(statuses=("shadow",))) == 3
    assert all(
        row["run_id"] == "run-v2-a"
        for row in store.list_phases(statuses=("shadow",))
    )


def test_two_prospective_wins_promote_only_with_opt_in(tmp_path) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-08-28T00:00:00+00:00",
        production_enabled=True,
        policy=_policy(),
    )
    imported = controller.import_report(
        _report(), imported_at_utc="2026-08-28T00:00:00+00:00"
    )
    phase_id = imported["imported"][0]["phase_id"]
    layer = WideShadowLayer(store, refresh_seconds=0)
    for index in range(2):
        observed = datetime(2026, 8, 28, 12, tzinfo=UTC) + timedelta(hours=index)
        snapshot = _snapshot(100 + index, observed)
        layer.process_snapshot(snapshot)
        layer.process_outcomes(
            [
                {
                    "observation_id": snapshot["observation_id"],
                    "outcome_schema_version": 1,
                    "created_at_utc": (observed + timedelta(hours=2)).isoformat(),
                    "outcome": {
                        "status": "resolved",
                        "goal_to90_normal_time": True,
                        "resolved_at_utc": (observed + timedelta(hours=2)).isoformat(),
                    },
                }
            ]
        )
    result = controller.reconcile(now_utc="2026-08-29T00:00:00+00:00")
    assert result["promoted_phase_id"] == phase_id
    assert result["active_pointer"]["phase_id"] == phase_id
    assert result["active_manifest"]["production_enabled"] is True
    assert result["active_manifest"]["rule"]["phase_id"] == phase_id


def test_reconcile_uses_each_phase_frozen_lifecycle_policy(tmp_path) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    importing = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-08-28T00:00:00+00:00",
        production_enabled=False,
        policy=_policy(),
    )
    imported = importing.import_report(
        _report(), imported_at_utc="2026-08-28T00:00:00+00:00"
    )
    phase_id = imported["imported"][0]["phase_id"]
    layer = WideShadowLayer(store, refresh_seconds=0)
    for index in range(2):
        observed = datetime(2026, 8, 28, 12, tzinfo=UTC) + timedelta(hours=index)
        snapshot = _snapshot(150 + index, observed)
        layer.process_snapshot(snapshot)
        layer.process_outcomes(
            [
                {
                    "observation_id": snapshot["observation_id"],
                    "outcome_schema_version": 1,
                    "created_at_utc": (observed + timedelta(hours=2)).isoformat(),
                    "outcome": {
                        "status": "resolved",
                        "goal_to90_normal_time": True,
                        "resolved_at_utc": (
                            observed + timedelta(hours=2)
                        ).isoformat(),
                    },
                }
            ]
        )

    # The runtime controller is deliberately much stricter.  It must not
    # retroactively alter the gates pre-registered with this phase.
    strict_runtime = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-08-28T00:00:00+00:00",
        production_enabled=False,
        policy=LifecyclePolicy(),
    )
    strict_runtime.reconcile(now_utc="2026-08-29T00:00:00+00:00")

    assert store.list_phases(rule_id="wide-test-rule")[0]["status"] == "ready"


def test_incompatible_active_schema_is_atomically_removed_from_pointer(
    tmp_path, monkeypatch
) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-08-28T00:00:00+00:00",
        production_enabled=True,
        policy=_policy(),
    )
    imported = controller.import_report(
        _report(), imported_at_utc="2026-08-28T00:00:00+00:00"
    )
    phase_id = imported["imported"][0]["phase_id"]
    store.transition_phase(
        phase_id,
        "ready",
        expected_status="shadow",
        reason="test_ready",
    )
    store.transition_phase_and_set_pointer(
        phase_id,
        "active",
        expected_status="ready",
        pointer_name="production",
        pointer_phase_id=phase_id,
        expected_generation=0,
        reason="test_active",
    )
    monkeypatch.setattr(
        controller_module,
        "FEATURE_SCHEMA_VERSION",
        controller_module.FEATURE_SCHEMA_VERSION + 1,
    )

    result = controller.reconcile(now_utc="2026-08-29T00:00:00+00:00")

    assert store.list_phases(rule_id="wide-test-rule")[0]["status"] == "degraded"
    assert result["active_pointer"]["phase_id"] is None
    assert result["active_manifest"]["rule"] is None


def test_candidate_id_collision_with_another_universe_fails_closed(
    tmp_path,
) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-09-01T00:00:00+00:00",
        max_shadow_rules=4,
    )
    controller.import_report(
        _generation_report(
            "run-collision-a", "collision", 1, candidate_namespace="same-id"
        ),
        imported_at_utc="2026-09-01T01:00:00+00:00",
    )
    changed = _generation_report(
        "run-collision-b", "collision", 1, candidate_namespace="same-id"
    )
    manifests = changed["manifests"]
    candidate = manifests["candidates"][0]
    candidate.pop("manifest_sha256")
    candidate["universe"]["minute_min"] = 50
    candidate["manifest_sha256"] = canonical_hash(candidate)
    manifests.pop("artifact_sha256")
    manifests["artifact_sha256"] = canonical_hash(manifests)

    with pytest.raises(ValueError, match="collides"):
        controller.import_report(
            changed,
            imported_at_utc="2026-09-02T01:00:00+00:00",
        )


def test_pending_early_trigger_cannot_be_replaced_by_later_win(tmp_path) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-08-28T00:00:00+00:00",
        production_enabled=True,
        policy=_policy(),
    )
    imported = controller.import_report(
        _report(), imported_at_utc="2026-08-28T00:00:00+00:00"
    )
    phase_id = imported["imported"][0]["phase_id"]
    layer = WideShadowLayer(store, refresh_seconds=0)
    snapshots = []
    for index in range(3):
        observed = datetime(2026, 8, 28, 12, tzinfo=UTC) + timedelta(hours=index)
        snapshot = _snapshot(200 + index, observed)
        snapshots.append((snapshot, observed))
        layer.process_snapshot(snapshot)
        if index > 0:
            layer.process_outcomes(
                [
                    {
                        "observation_id": snapshot["observation_id"],
                        "outcome_schema_version": 1,
                        "created_at_utc": (observed + timedelta(hours=2)).isoformat(),
                        "outcome": {
                            "status": "resolved",
                            "goal_to90_normal_time": True,
                            "resolved_at_utc": (observed + timedelta(hours=2)).isoformat(),
                        },
                    }
                ]
            )
    blocked = controller.reconcile(now_utc="2026-08-29T00:00:00+00:00")
    assert blocked["promoted_phase_id"] is None
    assert store.list_phases(rule_id="wide-test-rule")[0]["status"] == "shadow"

    first_snapshot, first_observed = snapshots[0]
    layer.process_outcomes(
        [
            {
                "observation_id": first_snapshot["observation_id"],
                "outcome_schema_version": 1,
                "created_at_utc": (first_observed + timedelta(hours=4)).isoformat(),
                "outcome": {
                    "status": "resolved",
                    "goal_to90_normal_time": True,
                    "resolved_at_utc": (first_observed + timedelta(hours=4)).isoformat(),
                },
            }
        ]
    )
    promoted = controller.reconcile(now_utc="2026-08-29T01:00:00+00:00")
    assert promoted["promoted_phase_id"] == phase_id


def test_ready_phase_is_revalidated_after_outcome_correction(tmp_path) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-08-28T00:00:00+00:00",
        production_enabled=False,
        policy=_policy(),
    )
    controller.import_report(
        _report(), imported_at_utc="2026-08-28T00:00:00+00:00"
    )
    layer = WideShadowLayer(store, refresh_seconds=0)
    snapshots = []
    for index in range(2):
        observed = datetime(2026, 8, 28, 12, tzinfo=UTC) + timedelta(hours=index)
        snapshot = _snapshot(300 + index, observed)
        snapshots.append((snapshot, observed))
        layer.process_snapshot(snapshot)
        layer.process_outcomes(
            [
                {
                    "observation_id": snapshot["observation_id"],
                    "outcome_schema_version": 1,
                    "created_at_utc": (observed + timedelta(hours=2)).isoformat(),
                    "outcome": {
                        "status": "resolved",
                        "goal_to90_normal_time": True,
                        "resolved_at_utc": (observed + timedelta(hours=2)).isoformat(),
                    },
                }
            ]
        )
    controller.reconcile(now_utc="2026-08-29T00:00:00+00:00")
    assert store.list_phases(rule_id="wide-test-rule")[0]["status"] == "ready"

    corrected, observed = snapshots[0]
    layer.process_outcomes(
        [
            {
                "observation_id": corrected["observation_id"],
                "outcome_schema_version": 2,
                "created_at_utc": (observed + timedelta(hours=5)).isoformat(),
                "outcome": {
                    "status": "resolved",
                    "goal_to90_normal_time": False,
                    "resolved_at_utc": (observed + timedelta(hours=5)).isoformat(),
                },
            }
        ]
    )
    controller.reconcile(now_utc="2026-08-29T01:00:00+00:00")
    assert store.list_phases(rule_id="wide-test-rule")[0]["status"] == "shadow"


def test_terminal_shadow_review_freezes_final_look_without_production(
    tmp_path,
) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-08-28T00:00:00+00:00",
        production_enabled=False,
        policy=_policy(),
        terminal_review_enabled=True,
        terminal_review_min_hit_rate=0.90,
    )
    imported = controller.import_report(
        _report(), imported_at_utc="2026-08-28T00:00:00+00:00"
    )
    phase_id = imported["imported"][0]["phase_id"]
    phase = store.list_phases(rule_id="wide-test-rule")[0]
    assert phase["policy"]["terminal_review"] == {
        "version": "wide_terminal_review_v1",
        "enabled": True,
        "final_look": 2,
        "min_point_hit_rate": 0.9,
    }

    layer = WideShadowLayer(store, refresh_seconds=0)
    snapshots = []
    for index in range(2):
        observed = datetime(2026, 8, 28, 12, tzinfo=UTC) + timedelta(
            hours=index
        )
        snapshot = _snapshot(400 + index, observed)
        snapshots.append((snapshot, observed))
        layer.process_snapshot(snapshot)
        layer.process_outcomes(
            [
                {
                    "observation_id": snapshot["observation_id"],
                    "outcome_schema_version": 1,
                    "created_at_utc": (observed + timedelta(hours=2)).isoformat(),
                    "outcome": {
                        "status": "resolved",
                        "goal_to90_normal_time": True,
                        "resolved_at_utc": (
                            observed + timedelta(hours=2)
                        ).isoformat(),
                    },
                }
            ]
        )

    result = controller.reconcile(now_utc="2026-08-29T00:00:00+00:00")
    review = next(
        row for row in result["readiness"] if row["phase_id"] == phase_id
    )
    assert review["terminal_review"]["closed"] is True
    assert review["terminal_review"]["reviewable"] is True
    assert review["terminal_review"]["strict_readiness_passed"] is True
    assert store.list_phases(rule_id="wide-test-rule")[0]["status"] == "paused"
    assert result["promoted_phase_id"] is None
    assert result["active_pointer"] is None

    # A closed research identity is not restarted if weekly discovery finds
    # the exact same rule again, and it no longer claims new live fixtures.
    repeated = controller.import_report(
        _report(), imported_at_utc="2026-08-30T00:00:00+00:00"
    )
    assert repeated["imported"] == []
    assert repeated["reused"] == [
        {"rule_id": "wide-test-rule", "phase_id": phase_id}
    ]
    layer.process_snapshot(
        _snapshot(999, datetime(2026, 8, 30, 12, tzinfo=UTC))
    )
    assert len(store.list_triggers(phase_id=phase_id)) == 2

    # Outcome corrections remain visible in the frozen review evidence.
    corrected, observed = snapshots[0]
    layer.process_outcomes(
        [
            {
                "observation_id": corrected["observation_id"],
                "outcome_schema_version": 2,
                "created_at_utc": (observed + timedelta(hours=5)).isoformat(),
                "outcome": {
                    "status": "resolved",
                    "goal_to90_normal_time": False,
                    "resolved_at_utc": (
                        observed + timedelta(hours=5)
                    ).isoformat(),
                },
            }
        ]
    )
    corrected_result = controller.reconcile(
        now_utc="2026-08-30T01:00:00+00:00"
    )
    corrected_review = next(
        row
        for row in corrected_result["readiness"]
        if row["phase_id"] == phase_id
    )
    assert corrected_review["terminal_review"]["hit_rate"] == 0.5
    assert corrected_review["terminal_review"]["reviewable"] is False
    assert store.list_phases(rule_id="wide-test-rule")[0]["status"] == "paused"

    layer.process_outcomes([{
        "observation_id": corrected["observation_id"],
        "outcome_schema_version": 3,
        "created_at_utc": (observed + timedelta(hours=6)).isoformat(),
        "outcome": {"status": "pending"},
    }])
    pending = controller.reconcile(now_utc="2026-08-30T02:00:00+00:00")
    disposition = pending["readiness"][0]["terminal_review"]
    assert disposition["closed"] is False
    assert disposition["pending"] == 1
    assert disposition["reviewable"] is None


def test_terminal_review_rate_is_validated(tmp_path) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    with pytest.raises(ValueError, match="terminal_review_min_hit_rate"):
        WideResearchController(
            store,
            active_manifest_path=str(tmp_path / "active.json"),
            prospective_start_utc="2026-08-28T00:00:00+00:00",
            terminal_review_enabled=True,
            terminal_review_min_hit_rate=1.01,
        )
    with pytest.raises(ValueError, match="hard-shadow"):
        WideResearchController(
            store,
            active_manifest_path=str(tmp_path / "active.json"),
            prospective_start_utc="2026-08-28T00:00:00+00:00",
            terminal_review_enabled=True,
            production_enabled=True,
        )


def test_empty_weekly_shortlist_preserves_ongoing_prospective_phases(tmp_path) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-09-01T00:00:00+00:00",
        terminal_review_enabled=True,
    )
    first = controller.import_report(
        _generation_report("initial", "rare-v1", 2),
        imported_at_utc="2026-09-01T01:00:00+00:00",
    )
    empty = controller.import_report(
        _generation_report("empty-week", "rare-v1", 0),
        imported_at_utc="2026-09-08T01:00:00+00:00",
    )
    assert empty["retired"] == []
    assert {row["phase_id"] for row in store.list_phases(statuses=("shadow",))} == {
        row["phase_id"] for row in first["imported"]
    }


def test_terminal_review_uses_first_200_and_waits_for_pending_outcome(tmp_path) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-08-28T00:00:00+00:00",
        policy=LifecyclePolicy(allowed_looks=(50, 100, 200), min_triggers_per_week=0),
        max_shadow_rules=1,
        terminal_review_enabled=True,
    )
    imported = controller.import_report(
        _report(), imported_at_utc="2026-08-28T00:00:00+00:00"
    )
    phase_id = imported["imported"][0]["phase_id"]
    layer = WideShadowLayer(store, refresh_seconds=0)
    records = []
    for index in range(201):
        observed = datetime(2026, 8, 28, 12, tzinfo=UTC) + timedelta(hours=index)
        snapshot = _snapshot(1000 + index, observed)
        layer.process_snapshot(snapshot)
        record = {
            "observation_id": snapshot["observation_id"],
            "outcome_schema_version": 1,
            "created_at_utc": (observed + timedelta(hours=2)).isoformat(),
            "outcome": {
                "status": "resolved",
                "goal_to90_normal_time": index >= 20,
                "resolved_at_utc": (observed + timedelta(hours=2)).isoformat(),
            },
        }
        records.append(record)
        if index:
            layer.process_outcomes([record])
        if index == 99:
            controller.reconcile(now_utc="2026-09-02T00:00:00+00:00")
            assert store.list_phases()[0]["status"] == "shadow"
    blocked = controller.reconcile(now_utc="2026-09-07T00:00:00+00:00")
    assert blocked["readiness"][0]["milestone"] is None
    assert blocked["readiness"][0]["terminal_review"]["closed"] is False
    assert blocked["readiness"][0]["terminal_review"]["pending"] == 1
    assert store.list_phases()[0]["status"] == "paused"
    assert len(store.triggers_for_phase_look(phase_id, 50)) == 50
    assert len(store.triggers_for_phase_look(phase_id, 200)) == 200
    layer.process_outcomes([records[0]])
    closed = controller.reconcile(now_utc="2026-09-07T01:00:00+00:00")
    review = closed["readiness"][0]["terminal_review"]
    assert review["resolved"] == 200
    assert review["hit_rate"] == 0.90  # excludes the 201st win
    assert review["reviewable"] is True
    assert review["strict_readiness_passed"] is False
    assert store.list_phases()[0]["status"] == "paused"
    assert closed["promoted_phase_id"] is None
    from scripts.report_wide_research import frozen_review_summary

    reported = frozen_review_summary(store, store.list_phases()[0])
    assert reported["hit_rate_pct"] == 90.0
    assert reported["wins"] == 180
    assert reported["losses"] == 20
    assert reported["reviewable"] is True

    # The frozen terminal phase frees the one executable slot; evidence stays.
    replacement = controller.import_report(
        _generation_report("new-candidate", "rare-v1", 1),
        imported_at_utc="2026-09-08T00:00:00+00:00",
    )
    assert len(replacement["imported"]) == 1
    assert len(store.list_phases(statuses=("paused",))) == 1
    assert len(store.list_triggers(phase_id=phase_id)) == 200


def test_terminal_execution_keeps_evidence_across_search_version_change(tmp_path) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-09-01T00:00:00+00:00",
        terminal_review_enabled=True,
    )
    first = controller.import_report(
        _generation_report("run1", "rare-v1", 2),
        imported_at_utc="2026-09-01T01:00:00+00:00",
    )
    first_phase = first["imported"][0]["phase_id"]
    store.transition_phase(
        first_phase, "paused", reason="frozen_terminal_cohort", expected_status="shadow"
    )
    changed = controller.import_report(
        _generation_report("run2", "rare-v2", 3),
        imported_at_utc="2026-09-08T01:00:00+00:00",
    )
    assert len(changed["reused"]) == 2
    assert len(changed["imported"]) == 1
    assert changed["retired"] == []
    assert first_phase in {row["phase_id"] for row in changed["reused"]}


def test_terminal_import_cannot_silently_change_frozen_review_policy(tmp_path) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    original = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-08-28T00:00:00+00:00",
    )
    original.import_report(_report(), imported_at_utc="2026-08-28T00:00:00+00:00")
    terminal = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-08-28T00:00:00+00:00",
        terminal_review_enabled=True,
    )
    with pytest.raises(ValueError, match="different frozen terminal review policy"):
        terminal.import_report(_report(), imported_at_utc="2026-08-29T00:00:00+00:00")


def test_frozen_terminal_phase_never_promotes_under_changed_runtime_flags(tmp_path) -> None:
    store = WideResearchStore(tmp_path / "wide.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-08-28T00:00:00+00:00",
        policy=replace(_policy(), allowed_looks=(2, 4)),
        terminal_review_enabled=True,
    )
    controller.import_report(_report(), imported_at_utc="2026-08-28T00:00:00+00:00")
    layer = WideShadowLayer(store, refresh_seconds=0)
    for index in range(2):
        observed = datetime(2026, 8, 28, 12, tzinfo=UTC) + timedelta(hours=index)
        snapshot = _snapshot(2000 + index, observed)
        layer.process_snapshot(snapshot)
        layer.process_outcomes([{
            "observation_id": snapshot["observation_id"],
            "outcome_schema_version": 1,
            "created_at_utc": (observed + timedelta(hours=2)).isoformat(),
            "outcome": {
                "status": "resolved", "goal_to90_normal_time": True,
                "resolved_at_utc": (observed + timedelta(hours=2)).isoformat(),
            },
        }])
    changed_runtime = WideResearchController(
        store,
        active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-08-28T00:00:00+00:00",
        production_enabled=True,
    )
    result = changed_runtime.reconcile(now_utc="2026-08-29T00:00:00+00:00")
    assert result["readiness"][0]["eligible"] is True
    assert store.list_phases()[0]["status"] == "ready"
    assert result["promoted_phase_id"] is None
    assert result["active_manifest"]["rule"] is None
