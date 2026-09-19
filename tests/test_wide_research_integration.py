from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone

import NanoTest as bot
import pytest
from scripts import run_wide_research_cycle as cycle_cli


def _with_temporal_contract(payload: dict) -> dict:
    if bot.WIDE_RESEARCH_TEMPORAL_PURGE:
        payload["discovery_engine_version"] += bot.WIDE_RESEARCH_PURGED_ENGINE_SUFFIX
        payload["split_policy"] = bot.WIDE_RESEARCH_PURGED_SPLIT_POLICY
        payload["splits"] = {
            "train": {
                "fixture_count": 1,
                "observation_count": 2,
                "fixture_ids_sha256": "1" * 64,
                "first_observation_utc": "2026-09-01T10:00:00+00:00",
                "last_observation_utc": "2026-09-01T11:59:00+00:00",
            },
            "validation": {
                "fixture_count": 1,
                "observation_count": 2,
                "fixture_ids_sha256": "2" * 64,
                "first_observation_utc": "2026-09-01T12:05:00+00:00",
                "last_observation_utc": "2026-09-01T14:04:00+00:00",
            },
            "holdout": {
                "fixture_count": 1,
                "observation_count": 2,
                "fixture_ids_sha256": "3" * 64,
                "first_observation_utc": "2026-09-01T14:10:00+00:00",
                "last_observation_utc": "2026-09-01T15:00:00+00:00",
            },
            "temporal_purge": {
            "version": bot.WIDE_RESEARCH_TEMPORAL_PURGE_VERSION,
            "embargo_seconds": bot.WIDE_RESEARCH_TEMPORAL_EMBARGO_SECONDS,
            "no_fixture_reallocation": True,
            "boundaries": {
                "train_to_validation": {
                    "next_split_first_observation_utc": "2026-09-01T12:05:00+00:00",
                    "information_cutoff_utc": "2026-09-01T12:00:00+00:00",
                    "nominal_fixture_count": 2,
                    "retained_fixture_count": 1,
                    "purged_fixture_count": 1,
                    "purged_fixture_ids_sha256": "0" * 64,
                    "purge_reasons": {"label_not_available_before_cutoff": 1},
                },
                "validation_to_holdout": {
                    "next_split_first_observation_utc": "2026-09-01T14:10:00+00:00",
                    "information_cutoff_utc": "2026-09-01T14:05:00+00:00",
                    "nominal_fixture_count": 2,
                    "retained_fixture_count": 1,
                    "purged_fixture_count": 1,
                    "purged_fixture_ids_sha256": "0" * 64,
                    "purge_reasons": {"label_not_available_before_cutoff": 1},
                },
            },
        }}
    payload.setdefault("search_config", {}).update({
        "temporal_purge": bot.WIDE_RESEARCH_TEMPORAL_PURGE,
        "temporal_embargo_seconds": bot.WIDE_RESEARCH_TEMPORAL_EMBARGO_SECONDS,
    })
    return payload


def _primary_search_config() -> dict:
    return {
        "min_conjunction_size": 1,
        "max_conjunction_size": 3,
        "beam_width": 64,
        "evaluation_budget": 12000,
        "depth_evaluation_budgets": [],
        "top_n": 10,
        "min_train_support": 40,
        "min_validation_support": 15,
        "min_holdout_support": 15,
        "selection_mode": "standard",
        "extended_features": False,
        "error_refinement": False,
        "min_signals_per_week": 0.0,
        "preferred_signals_per_week": 0.0,
        "max_signals_per_week": 0.0,
        "portfolio_max_rules": 1,
        "portfolio_beam_width": 32,
        "allow_feature_ranges": False,
        "validation_window_count": 1,
    }


def _observation() -> dict:
    return {
        "record_type": "observation",
        "observation_id": "99100:50:WINDOW_1:BLOCK:v3",
        "observation_key": "99100:50:WINDOW_1:BLOCK:v3",
        "fixture_id": 99100,
        "created_at_utc": "2026-08-28T12:00:00+00:00",
        "schema_version": 3,
        "stage": "decision_pipeline",
        "minute": 50,
    }


@pytest.mark.parametrize("profile", [{}, {"four_factor": True}, {"precision": True}, {"rare_precision": True}])
def test_temporal_worker_flags_apply_to_every_profile(monkeypatch, profile) -> None:
    monkeypatch.setattr(bot, "WIDE_RESEARCH_TEMPORAL_PURGE", True)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_TEMPORAL_EMBARGO_SECONDS", 300)
    command = bot._wide_research_worker_command(
        {"observations": "o", "static": "s", "rolling": "r"}, **profile,
    )
    parsed = cycle_cli._parser().parse_args(command[2:])
    assert parsed.temporal_purge is True
    assert parsed.temporal_embargo_seconds == 300


def test_temporal_freshness_requires_purge_engine_policy_and_diagnostics(monkeypatch) -> None:
    monkeypatch.setattr(bot, "WIDE_RESEARCH_TEMPORAL_PURGE", True)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_TEMPORAL_EMBARGO_SECONDS", 300)
    engine = bot.WIDE_RESEARCH_DISCOVERY_ENGINE_VERSION
    payload = _with_temporal_contract({"discovery_engine_version": engine})
    assert bot._wide_research_temporal_summary_is_current(payload, engine)
    for name, value in (
        ("discovery_engine_version", engine),
        ("split_policy", "chronological_fixture_group_v1"),
        ("splits", {}),
        ("search_config", {"temporal_purge": True, "temporal_embargo_seconds": 0}),
    ):
        changed = {**payload, name: value}
        assert not bot._wide_research_temporal_summary_is_current(changed, engine)
    incomplete = copy.deepcopy(payload)
    del incomplete["splits"]["temporal_purge"]["boundaries"][
        "validation_to_holdout"
    ]
    assert not bot._wide_research_temporal_summary_is_current(incomplete, engine)


def test_cycle_summary_compacts_split_ids_but_keeps_purge_audit() -> None:
    splits = {
        "train": {"fixture_ids": [1, 2], "fixture_count": 2},
        "temporal_purge": {"embargo_seconds": 300, "boundaries": {"test": {"purged_fixture_count": 1}}},
    }
    compact = cycle_cli._compact_splits(splits)
    assert compact["train"] == {"fixture_count": 2}
    assert compact["temporal_purge"] == splits["temporal_purge"]


def test_existing_observation_retry_does_not_create_unpersisted_wide_trigger(
    monkeypatch,
) -> None:
    source = _observation()
    calls: list[str] = []
    monkeypatch.setattr(
        bot, "freeze_observation_rolling_dynamics", lambda value: copy.deepcopy(value)
    )
    monkeypatch.setattr(bot, "append_observation_history", lambda value: False)
    monkeypatch.setattr(bot, "observation_history_record_exists", lambda value: True)
    monkeypatch.setattr(bot, "register_observation_rolling_baseline", lambda value: None)
    monkeypatch.setattr(bot, "predict_and_append_shadow_ml", lambda value: None)
    monkeypatch.setattr(bot, "predict_and_append_shadow_rolling_ml", lambda value: None)
    monkeypatch.setattr(bot, "evaluate_and_append_shadow_candidates", lambda value: None)
    monkeypatch.setattr(
        bot,
        "evaluate_and_store_wide_research",
        lambda value: calls.append("wide"),
    )
    assert bot.persist_observation_and_score_shadow(source) is False
    assert calls == []


def test_wide_output_paths_cannot_overlap_live_observation_journal(
    monkeypatch, tmp_path
) -> None:
    source = tmp_path / "observation_history.jsonl"
    monkeypatch.setattr(bot, "OBSERVATION_HISTORY_FILE", str(source))
    monkeypatch.setattr(bot, "WIDE_RESEARCH_DB_FILE", str(source))
    monkeypatch.setattr(
        bot,
        "WIDE_RESEARCH_ACTIVE_MANIFEST_FILE",
        str(tmp_path / "active.json"),
    )
    monkeypatch.setattr(
        bot,
        "WIDE_RESEARCH_DISCOVERY_FILE",
        str(tmp_path / "latest.json"),
    )
    with pytest.raises(ValueError, match="overlaps a live source"):
        bot._assert_wide_research_runtime_paths()


def test_four_factor_paths_must_be_isolated_from_primary(monkeypatch, tmp_path) -> None:
    shared = tmp_path / "shared.sqlite3"
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_FOUR_FACTOR", True)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_DB_FILE", str(shared))
    monkeypatch.setattr(bot, "WIDE_RESEARCH_FOUR_FACTOR_DB_FILE", str(shared))
    with pytest.raises(ValueError, match="overlaps primary"):
        bot._assert_wide_research_runtime_paths()


def test_precision_paths_must_be_isolated_from_every_other_profile(
    monkeypatch, tmp_path
) -> None:
    shared = tmp_path / "shared.sqlite3"
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_PRECISION", True)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_DB_FILE", str(shared))
    monkeypatch.setattr(bot, "WIDE_RESEARCH_PRECISION_DB_FILE", str(shared))
    with pytest.raises(ValueError, match="precision research output overlaps"):
        bot._assert_wide_research_runtime_paths()


def test_rare_precision_paths_must_be_isolated_from_all_profiles(
    monkeypatch, tmp_path
) -> None:
    shared = tmp_path / "shared.sqlite3"
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_RARE_PRECISION", True)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_PRECISION", True)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_PRECISION_DB_FILE", str(shared))
    monkeypatch.setattr(
        bot, "WIDE_RESEARCH_RARE_PRECISION_DB_FILE", str(shared)
    )
    with pytest.raises(ValueError, match="rare precision research output overlaps"):
        bot._assert_wide_research_runtime_paths()


def test_rare_precision_artifact_directory_must_be_pairwise_isolated(
    monkeypatch, tmp_path
) -> None:
    parent = tmp_path / "precision-reports"
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_PRECISION", True)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_RARE_PRECISION", True)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_PRECISION_OUTPUT_DIR", str(parent))
    monkeypatch.setattr(
        bot,
        "WIDE_RESEARCH_RARE_PRECISION_OUTPUT_DIR",
        str(parent / "rare"),
    )
    with pytest.raises(ValueError, match="artifact directories overlap"):
        bot._assert_wide_research_runtime_paths()


def test_unsafe_four_factor_paths_fail_before_worker_start(monkeypatch, tmp_path) -> None:
    shared = tmp_path / "shared.sqlite3"
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_FOUR_FACTOR", True)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_FOUR_FACTOR_AUTO_DISCOVERY", True)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_DB_FILE", str(shared))
    monkeypatch.setattr(bot, "WIDE_RESEARCH_FOUR_FACTOR_DB_FILE", str(shared))
    monkeypatch.setattr(
        bot.subprocess,
        "Popen",
        lambda *args, **kwargs: pytest.fail("worker must not start"),
    )
    result = bot.run_wide_research_four_factor_discovery_once(trigger="test")
    assert result["status"] == "error"
    assert result["reason"] == "unsafe_output_paths"


def test_four_factor_worker_is_exact_four_and_never_production(monkeypatch, tmp_path) -> None:
    values = {
        "WIDE_RESEARCH_FOUR_FACTOR_DB_FILE": tmp_path / "four.sqlite3",
        "WIDE_RESEARCH_FOUR_FACTOR_ACTIVE_MANIFEST_FILE": tmp_path / "four-active.json",
        "WIDE_RESEARCH_FOUR_FACTOR_DISCOVERY_FILE": tmp_path / "four-latest.json",
        "WIDE_RESEARCH_FOUR_FACTOR_OUTPUT_DIR": tmp_path / "four-reports",
        "WIDE_RESEARCH_FOUR_FACTOR_PROSPECTIVE_START_UTC": "2026-09-03T17:05:31+00:00",
        "WIDE_RESEARCH_FOUR_FACTOR_MAX_SHADOW_RULES": 7,
        "WIDE_RESEARCH_FOUR_FACTOR_MAX_DB_BYTES": 99999999,
        "WIDE_RESEARCH_FOUR_FACTOR_BEAM_WIDTH": 24,
        "WIDE_RESEARCH_FOUR_FACTOR_EVALUATION_BUDGET": 30000,
        "WIDE_RESEARCH_FOUR_FACTOR_MIN_TRAIN_SUPPORT": 60,
        "WIDE_RESEARCH_FOUR_FACTOR_MIN_VALIDATION_SUPPORT": 20,
        "WIDE_RESEARCH_FOUR_FACTOR_MIN_HOLDOUT_SUPPORT": 20,
    }
    for name, value in values.items():
        monkeypatch.setattr(bot, name, value)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_PRODUCTION_APPLY", True)
    command = bot._wide_research_worker_command(
        {"observations": "o", "static": "s", "rolling": "r"},
        four_factor=True,
    )

    assert "--shadow-only" in command
    assert "--require-memory-limit" in command
    assert "--production-enabled" not in command
    assert command[command.index("--min-conjunction-size") + 1] == "4"
    assert command[command.index("--max-conjunction-size") + 1] == "4"
    assert command[command.index("--beam-width") + 1] == "24"
    assert command[command.index("--evaluation-budget") + 1] == "30000"
    assert command[command.index("--db") + 1] == str(values["WIDE_RESEARCH_FOUR_FACTOR_DB_FILE"])
    assert command[command.index("--store-profile") + 1] == "exact_four_shadow"


def test_precision_worker_has_deep_budget_cadence_and_never_production(
    monkeypatch, tmp_path
) -> None:
    values = {
        "WIDE_RESEARCH_PRECISION_DB_FILE": tmp_path / "precision.sqlite3",
        "WIDE_RESEARCH_PRECISION_ACTIVE_MANIFEST_FILE": tmp_path / "precision-active.json",
        "WIDE_RESEARCH_PRECISION_DISCOVERY_FILE": tmp_path / "precision-latest.json",
        "WIDE_RESEARCH_PRECISION_OUTPUT_DIR": tmp_path / "precision-reports",
        "WIDE_RESEARCH_PRECISION_PROSPECTIVE_START_UTC": "2026-09-04T12:54:07+00:00",
        "WIDE_RESEARCH_PRECISION_MAX_SHADOW_RULES": 40,
        "WIDE_RESEARCH_PRECISION_DISCOVERY_TOP_N": 20,
        "WIDE_RESEARCH_PRECISION_MAX_DB_BYTES": 99999999,
        "WIDE_RESEARCH_PRECISION_BEAM_WIDTH": 96,
        "WIDE_RESEARCH_PRECISION_MAX_CONJUNCTION_SIZE": 8,
        "WIDE_RESEARCH_PRECISION_EVALUATION_BUDGET": 100000,
        "WIDE_RESEARCH_PRECISION_DEPTH_BUDGETS": (
            1000,
            15000,
            16000,
            16000,
            14000,
            13000,
            13000,
            12000,
        ),
        "WIDE_RESEARCH_PRECISION_MIN_TRAIN_SUPPORT": 12,
        "WIDE_RESEARCH_PRECISION_MIN_VALIDATION_SUPPORT": 4,
        "WIDE_RESEARCH_PRECISION_MIN_HOLDOUT_SUPPORT": 4,
        "WIDE_RESEARCH_PRECISION_MIN_SIGNALS_PER_WEEK": 10.0,
        "WIDE_RESEARCH_PRECISION_PREFERRED_SIGNALS_PER_WEEK": 12.5,
        "WIDE_RESEARCH_PRECISION_MAX_SIGNALS_PER_WEEK": 15.0,
        "WIDE_RESEARCH_PRECISION_PORTFOLIO_MAX_RULES": 10,
        "WIDE_RESEARCH_PRECISION_PORTFOLIO_BEAM_WIDTH": 128,
    }
    for name, value in values.items():
        monkeypatch.setattr(bot, name, value)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_PRODUCTION_APPLY", True)
    command = bot._wide_research_worker_command(
        {"observations": "o", "static": "s", "rolling": "r"},
        precision=True,
    )

    assert "--shadow-only" in command
    assert "--production-enabled" not in command
    assert command[command.index("--store-profile") + 1] == "precision_shadow"
    assert command[command.index("--selection-mode") + 1] == "precision_first"
    assert command[command.index("--min-conjunction-size") + 1] == "2"
    assert command[command.index("--max-conjunction-size") + 1] == "8"
    assert command[command.index("--evaluation-budget") + 1] == "100000"
    assert command[command.index("--max-shadow-rules") + 1] == "40"
    assert command[command.index("--top-n") + 1] == "20"
    assert command[command.index("--depth-evaluation-budgets") + 1] == (
        "1000,15000,16000,16000,14000,13000,13000,12000"
    )
    assert command[command.index("--min-signals-per-week") + 1] == "10.0"
    assert command[command.index("--preferred-signals-per-week") + 1] == "12.5"
    assert command[command.index("--max-signals-per-week") + 1] == "15.0"


def test_rare_precision_worker_has_fixed_deep_range_contract_and_no_production(
    monkeypatch, tmp_path
) -> None:
    values = {
        "WIDE_RESEARCH_RARE_PRECISION_DB_FILE": tmp_path / "rare.sqlite3",
        "WIDE_RESEARCH_RARE_PRECISION_ACTIVE_MANIFEST_FILE": (
            tmp_path / "rare-active.json"
        ),
        "WIDE_RESEARCH_RARE_PRECISION_DISCOVERY_FILE": (
            tmp_path / "rare-latest.json"
        ),
        "WIDE_RESEARCH_RARE_PRECISION_OUTPUT_DIR": tmp_path / "rare-reports",
        "WIDE_RESEARCH_RARE_PRECISION_PROSPECTIVE_START_UTC": (
            "2026-09-10T08:16:59+00:00"
        ),
        "WIDE_RESEARCH_RARE_PRECISION_MAX_DB_BYTES": 99999999,
    }
    for name, value in values.items():
        monkeypatch.setattr(bot, name, value)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_PRODUCTION_APPLY", True)
    command = bot._wide_research_worker_command(
        {"observations": "o", "static": "s", "rolling": "r"},
        rare_precision=True,
    )

    assert "--shadow-only" in command
    assert "--production-enabled" not in command
    assert command[command.index("--store-profile") + 1] == (
        "rare_precision_shadow"
    )
    assert command[command.index("--selection-mode") + 1] == "rare_precision"
    assert command[command.index("--min-conjunction-size") + 1] == "2"
    assert command[command.index("--max-conjunction-size") + 1] == "8"
    assert command[command.index("--beam-width") + 1] == "128"
    assert command[command.index("--evaluation-budget") + 1] == "180000"
    assert command[command.index("--max-shadow-rules") + 1] == "64"
    assert command[command.index("--top-n") + 1] == "8"
    assert command[command.index("--depth-evaluation-budgets") + 1] == (
        "4000,28000,30000,28000,25000,23000,22000,20000"
    )
    assert command[command.index("--min-signals-per-week") + 1] == "0.0"
    assert "--allow-feature-ranges" in command
    assert "--extended-features" in command
    assert command[command.index("--validation-window-count") + 1] == "4"


def test_four_factor_components_have_no_router_and_hard_disable_production(
    monkeypatch, tmp_path
) -> None:
    captured = {}

    class DummyStore:
        def __init__(self, path, **kwargs):
            self.path = path

        def bind_profile(self, profile_id):
            self.profile_id = profile_id
            return {"profile_id": profile_id}

    class DummyLayer:
        def __init__(self, store, **kwargs):
            self.store = store

    class DummyController:
        def __init__(self, store, **kwargs):
            self.store = store
            self.production_enabled = kwargs["production_enabled"]
            captured.update(kwargs)

    monkeypatch.setattr(bot, "WideResearchStore", DummyStore)
    monkeypatch.setattr(bot, "WideShadowLayer", DummyLayer)
    monkeypatch.setattr(bot, "WideResearchController", DummyController)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_FOUR_FACTOR", True)
    monkeypatch.setattr(
        bot, "WIDE_RESEARCH_FOUR_FACTOR_DB_FILE", str(tmp_path / "four.sqlite3")
    )
    monkeypatch.setattr(
        bot,
        "WIDE_RESEARCH_FOUR_FACTOR_ACTIVE_MANIFEST_FILE",
        str(tmp_path / "four-active.json"),
    )
    monkeypatch.setattr(
        bot,
        "WIDE_RESEARCH_FOUR_FACTOR_DISCOVERY_FILE",
        str(tmp_path / "four-latest.json"),
    )
    monkeypatch.setattr(
        bot,
        "WIDE_RESEARCH_FOUR_FACTOR_OUTPUT_DIR",
        str(tmp_path / "four-reports"),
    )
    monkeypatch.setattr(bot, "_wide_research_4f_store", None)
    monkeypatch.setattr(bot, "_wide_research_4f_layer", None)
    monkeypatch.setattr(bot, "_wide_research_4f_controller", None)
    monkeypatch.setattr(bot, "_wide_research_4f_signature", ())

    components = bot._get_wide_research_four_factor_components()
    assert len(components) == 3
    assert components[0].profile_id == "exact_four_shadow"
    assert captured["production_enabled"] is False
    assert components[2].production_enabled is False


def test_precision_components_have_no_router_and_hard_disable_production(
    monkeypatch, tmp_path
) -> None:
    captured = {}

    class DummyStore:
        def __init__(self, path, **kwargs):
            self.path = path

        def bind_profile(self, profile_id):
            self.profile_id = profile_id
            return {"profile_id": profile_id}

    class DummyLayer:
        def __init__(self, store, **kwargs):
            self.store = store

    class DummyController:
        def __init__(self, store, **kwargs):
            self.store = store
            self.production_enabled = kwargs["production_enabled"]
            captured.update(kwargs)

    monkeypatch.setattr(bot, "WideResearchStore", DummyStore)
    monkeypatch.setattr(bot, "WideShadowLayer", DummyLayer)
    monkeypatch.setattr(bot, "WideResearchController", DummyController)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_PRECISION", True)
    monkeypatch.setattr(
        bot,
        "WIDE_RESEARCH_PRECISION_DB_FILE",
        str(tmp_path / "precision.sqlite3"),
    )
    monkeypatch.setattr(
        bot,
        "WIDE_RESEARCH_PRECISION_ACTIVE_MANIFEST_FILE",
        str(tmp_path / "precision-active.json"),
    )
    monkeypatch.setattr(
        bot,
        "WIDE_RESEARCH_PRECISION_DISCOVERY_FILE",
        str(tmp_path / "precision-latest.json"),
    )
    monkeypatch.setattr(
        bot,
        "WIDE_RESEARCH_PRECISION_OUTPUT_DIR",
        str(tmp_path / "precision-reports"),
    )
    monkeypatch.setattr(bot, "_wide_research_precision_store", None)
    monkeypatch.setattr(bot, "_wide_research_precision_layer", None)
    monkeypatch.setattr(bot, "_wide_research_precision_controller", None)
    monkeypatch.setattr(bot, "_wide_research_precision_signature", ())

    components = bot._get_wide_research_precision_components()
    assert len(components) == 3
    assert components[0].profile_id == "precision_shadow"
    assert captured["production_enabled"] is False
    assert components[2].production_enabled is False


def test_rare_precision_components_are_isolated_and_terminal_shadow_only(
    monkeypatch, tmp_path
) -> None:
    captured = {}

    class DummyStore:
        def __init__(self, path, **kwargs):
            self.path = path

        def bind_profile(self, profile_id):
            self.profile_id = profile_id
            return {"profile_id": profile_id}

    class DummyLayer:
        def __init__(self, store, **kwargs):
            self.store = store

    class DummyController:
        def __init__(self, store, **kwargs):
            self.store = store
            self.production_enabled = kwargs["production_enabled"]
            captured.update(kwargs)

    monkeypatch.setattr(bot, "WideResearchStore", DummyStore)
    monkeypatch.setattr(bot, "WideShadowLayer", DummyLayer)
    monkeypatch.setattr(bot, "WideResearchController", DummyController)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_RARE_PRECISION", True)
    monkeypatch.setattr(
        bot,
        "WIDE_RESEARCH_RARE_PRECISION_DB_FILE",
        str(tmp_path / "rare.sqlite3"),
    )
    monkeypatch.setattr(
        bot,
        "WIDE_RESEARCH_RARE_PRECISION_ACTIVE_MANIFEST_FILE",
        str(tmp_path / "rare-active.json"),
    )
    monkeypatch.setattr(
        bot,
        "WIDE_RESEARCH_RARE_PRECISION_DISCOVERY_FILE",
        str(tmp_path / "rare-latest.json"),
    )
    monkeypatch.setattr(
        bot,
        "WIDE_RESEARCH_RARE_PRECISION_OUTPUT_DIR",
        str(tmp_path / "rare-reports"),
    )
    monkeypatch.setattr(bot, "_wide_research_rare_precision_store", None)
    monkeypatch.setattr(bot, "_wide_research_rare_precision_layer", None)
    monkeypatch.setattr(bot, "_wide_research_rare_precision_controller", None)
    monkeypatch.setattr(bot, "_wide_research_rare_precision_signature", ())

    components = bot._get_wide_research_rare_precision_components()
    assert len(components) == 3
    assert components[0].profile_id == "rare_precision_shadow"
    assert captured["production_enabled"] is False
    assert captured["terminal_review_enabled"] is True
    assert captured["terminal_review_min_hit_rate"] == 0.90
    assert captured["policy"].allowed_looks == (50, 100, 200)
    assert captured["policy"].min_resolved == 200
    assert captured["policy"].min_triggers_per_week == 0.0


def test_shadow_only_cli_rejects_production_before_opening_store() -> None:
    with pytest.raises(SystemExit, match="cannot be combined"):
        cycle_cli.main(
            [
                "--prospective-start-utc",
                "2026-09-03T17:05:31+00:00",
                "--shadow-only",
                "--production-enabled",
            ]
        )


def test_precision_cli_requires_hard_shadow_contract() -> None:
    with pytest.raises(SystemExit, match="precision_shadow profile requires"):
        cycle_cli.main(
            [
                "--prospective-start-utc",
                "2026-09-04T12:54:07+00:00",
                "--store-profile",
                "precision_shadow",
                "--selection-mode",
                "precision_first",
                "--min-conjunction-size",
                "2",
                "--max-conjunction-size",
                "4",
                "--depth-evaluation-budgets",
                "2000,18000,20000,20000",
            ]
        )


def test_rare_precision_cli_requires_fixed_hard_shadow_contract() -> None:
    with pytest.raises(
        SystemExit, match="rare_precision_shadow profile requires"
    ):
        cycle_cli.main(
            [
                "--prospective-start-utc",
                "2026-09-10T08:16:59+00:00",
                "--store-profile",
                "rare_precision_shadow",
                "--shadow-only",
                "--selection-mode",
                "rare_precision",
                "--min-conjunction-size",
                "2",
                "--max-conjunction-size",
                "8",
                "--beam-width",
                "128",
                "--evaluation-budget",
                "180000",
                "--depth-evaluation-budgets",
                "2000,30000,30000,28000,25000,23000,22000,20000",
                "--top-n",
                "8",
                "--max-shadow-rules",
                "64",
            ]
        )


def test_rare_precision_selection_mode_cannot_target_primary_store() -> None:
    with pytest.raises(
        SystemExit, match="rare_precision selection mode requires"
    ):
        cycle_cli.main(
            [
                "--prospective-start-utc",
                "2026-09-10T08:16:59+00:00",
                "--selection-mode",
                "rare_precision",
            ]
        )


def test_shadow_only_rejects_incomplete_depth() -> None:
    report = {
        "manifests": {
            "search": {
                "levels": [{"size": 1}, {"size": 2}, {"size": 3}],
                "budget_exhausted": True,
            },
            "candidates": [],
        }
    }
    with pytest.raises(RuntimeError, match="did not complete"):
        cycle_cli._assert_shadow_only_discovery_complete(
            report,
            min_conjunction_size=4,
            max_conjunction_size=4,
        )


def test_rare_precision_accepts_only_registered_final_depth_budget_exhaustion() -> None:
    report = {
        "manifests": {
            "search": {
                "levels": [
                    {"size": depth, "truncated": depth > 1}
                    for depth in range(1, 9)
                ],
                "evaluations_used": 180000,
                "global_budget_truncated_depths": [8],
                "rare_precision": {"validation_window_count": 4},
            }
        }
    }
    cycle_cli._assert_rare_precision_discovery_complete(
        report,
        max_conjunction_size=8,
        evaluation_budget=180000,
    )

    report["manifests"]["search"]["global_budget_truncated_depths"] = [7]
    with pytest.raises(RuntimeError, match="before the registered final-depth"):
        cycle_cli._assert_rare_precision_discovery_complete(
            report,
            max_conjunction_size=8,
            evaluation_budget=180000,
        )


def test_stale_non_four_factor_summary_does_not_delay_exact_four_search(
    monkeypatch, tmp_path
) -> None:
    latest = tmp_path / "four-latest.json"
    latest.write_text(
        json.dumps(
            {
                "cycle_type": "discovery",
                "run_id": "old-wrong-profile",
                "registry": {},
                "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                "shadow_only": False,
                "production_enabled": False,
                "search_config": {
                    "min_conjunction_size": 1,
                    "max_conjunction_size": 3,
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(bot, "WIDE_RESEARCH_FOUR_FACTOR_DISCOVERY_INTERVAL_SECONDS", 100)
    assert (
        bot._wide_research_initial_discovery_delay_seconds(
            discovery_file=str(latest),
            interval_seconds=100,
            require_four_factor_profile=True,
        )
        == 0.0
    )


def test_four_factor_summary_accepts_a_stricter_applied_memory_limit(
    monkeypatch,
) -> None:
    monkeypatch.setattr(bot, "WIDE_RESEARCH_WORKER_MEMORY_LIMIT_MB", 3072)
    payload = {
        "cycle_type": "discovery",
        "store_profile": "exact_four_shadow",
        "shadow_only": True,
        "production_enabled": False,
        "discovery_engine_version": bot.WIDE_RESEARCH_DISCOVERY_ENGINE_VERSION,
        "feature_schema_version": bot.WIDE_RESEARCH_FEATURE_SCHEMA_VERSION,
        "threshold_grid_version": bot.WIDE_RESEARCH_ATOMIC_GRID_VERSION,
        "prospective_start_utc": bot.WIDE_RESEARCH_FOUR_FACTOR_PROSPECTIVE_START_UTC,
        "worker_memory_limit_requested_mb": 3072,
        "worker_memory_limit_mb": 2048,
        "search_config": {
            "selection_mode": "standard",
            "extended_features": False,
            "error_refinement": False,
            "min_conjunction_size": 4,
            "max_conjunction_size": 4,
            "beam_width": bot.WIDE_RESEARCH_FOUR_FACTOR_BEAM_WIDTH,
            "evaluation_budget": bot.WIDE_RESEARCH_FOUR_FACTOR_EVALUATION_BUDGET,
            "depth_evaluation_budgets": [],
            "top_n": bot.WIDE_RESEARCH_FOUR_FACTOR_MAX_SHADOW_RULES,
            "min_train_support": bot.WIDE_RESEARCH_FOUR_FACTOR_MIN_TRAIN_SUPPORT,
            "min_validation_support": bot.WIDE_RESEARCH_FOUR_FACTOR_MIN_VALIDATION_SUPPORT,
            "min_holdout_support": bot.WIDE_RESEARCH_FOUR_FACTOR_MIN_HOLDOUT_SUPPORT,
            "min_signals_per_week": 0.0,
            "preferred_signals_per_week": 0.0,
            "max_signals_per_week": 0.0,
            "portfolio_max_rules": 1,
            "portfolio_beam_width": 32,
            "allow_feature_ranges": False,
            "validation_window_count": 1,
        },
    }
    _with_temporal_contract(payload)
    assert bot._wide_research_four_factor_summary_is_current(payload) is True


def test_precision_summary_requires_the_exact_new_search_contract(monkeypatch) -> None:
    monkeypatch.setattr(bot, "WIDE_RESEARCH_WORKER_MEMORY_LIMIT_MB", 3072)
    payload = {
        "cycle_type": "discovery",
        "store_profile": "precision_shadow",
        "shadow_only": True,
        "production_enabled": False,
        "discovery_engine_version": bot.WIDE_RESEARCH_DISCOVERY_ENGINE_VERSION,
        "feature_schema_version": bot.WIDE_RESEARCH_FEATURE_SCHEMA_VERSION,
        "threshold_grid_version": bot.WIDE_RESEARCH_ATOMIC_GRID_VERSION,
        "prospective_start_utc": bot.WIDE_RESEARCH_PRECISION_PROSPECTIVE_START_UTC,
        "worker_memory_limit_requested_mb": 3072,
        "worker_memory_limit_mb": 2048,
        "search_config": {
            "selection_mode": "precision_first",
            "extended_features": False,
            "error_refinement": False,
            "min_conjunction_size": 2,
            "max_conjunction_size": bot.WIDE_RESEARCH_PRECISION_MAX_CONJUNCTION_SIZE,
            "beam_width": bot.WIDE_RESEARCH_PRECISION_BEAM_WIDTH,
            "evaluation_budget": bot.WIDE_RESEARCH_PRECISION_EVALUATION_BUDGET,
            "depth_evaluation_budgets": list(bot.WIDE_RESEARCH_PRECISION_DEPTH_BUDGETS),
            "top_n": bot.WIDE_RESEARCH_PRECISION_DISCOVERY_TOP_N,
            "min_train_support": bot.WIDE_RESEARCH_PRECISION_MIN_TRAIN_SUPPORT,
            "min_validation_support": bot.WIDE_RESEARCH_PRECISION_MIN_VALIDATION_SUPPORT,
            "min_holdout_support": bot.WIDE_RESEARCH_PRECISION_MIN_HOLDOUT_SUPPORT,
            "min_signals_per_week": bot.WIDE_RESEARCH_PRECISION_MIN_SIGNALS_PER_WEEK,
            "preferred_signals_per_week": bot.WIDE_RESEARCH_PRECISION_PREFERRED_SIGNALS_PER_WEEK,
            "max_signals_per_week": bot.WIDE_RESEARCH_PRECISION_MAX_SIGNALS_PER_WEEK,
            "portfolio_max_rules": bot.WIDE_RESEARCH_PRECISION_PORTFOLIO_MAX_RULES,
            "portfolio_beam_width": bot.WIDE_RESEARCH_PRECISION_PORTFOLIO_BEAM_WIDTH,
            "allow_feature_ranges": False,
            "validation_window_count": 1,
        },
    }
    _with_temporal_contract(payload)
    assert bot._wide_research_precision_summary_is_current(payload) is True
    stale_contract = dict(payload)
    stale_contract["feature_schema_version"] = (
        bot.WIDE_RESEARCH_FEATURE_SCHEMA_VERSION - 1
    )
    assert bot._wide_research_precision_summary_is_current(
        stale_contract
    ) is False
    del payload["search_config"]["depth_evaluation_budgets"]
    assert bot._wide_research_precision_summary_is_current(payload) is False


def test_rare_precision_summary_requires_mode_specific_frozen_contract(
    monkeypatch,
) -> None:
    monkeypatch.setattr(bot, "WIDE_RESEARCH_WORKER_MEMORY_LIMIT_MB", 3072)
    lifecycle_policy = (
        bot.WIDE_RESEARCH_RARE_PRECISION_LIFECYCLE_POLICY.manifest()
    )
    lifecycle_policy["allowed_looks"] = [50, 100, 200]
    payload = {
        "cycle_type": "discovery",
        "store_profile": "rare_precision_shadow",
        "shadow_only": True,
        "production_enabled": False,
        "discovery_engine_version": (
            bot.WIDE_RESEARCH_RARE_PRECISION_DISCOVERY_ENGINE_VERSION
        ),
        "feature_schema_version": bot.WIDE_RESEARCH_FEATURE_SCHEMA_VERSION,
        "threshold_grid_version": (
            bot.WIDE_RESEARCH_RARE_PRECISION_ATOMIC_GRID_VERSION
        ),
        "prospective_start_utc": (
            bot.WIDE_RESEARCH_RARE_PRECISION_PROSPECTIVE_START_UTC
        ),
        "worker_memory_limit_requested_mb": 3072,
        "worker_memory_limit_mb": 2048,
        "profile_contract": {
            "max_shadow_rules": 64,
            "lifecycle_policy": lifecycle_policy,
            "terminal_review": {
                "enabled": True,
                "final_look": 200,
                "min_point_hit_rate": 0.90,
            },
        },
        "search_config": {
            "selection_mode": "rare_precision",
            "extended_features": True,
            "error_refinement": True,
            "min_conjunction_size": 2,
            "max_conjunction_size": 8,
            "beam_width": 128,
            "evaluation_budget": 180000,
            "depth_evaluation_budgets": [
                4000,
                28000,
                30000,
                28000,
                25000,
                23000,
                22000,
                20000,
            ],
            "top_n": 8,
            "min_train_support": (
                bot.WIDE_RESEARCH_RARE_PRECISION_MIN_TRAIN_SUPPORT
            ),
            "min_validation_support": (
                bot.WIDE_RESEARCH_RARE_PRECISION_MIN_VALIDATION_SUPPORT
            ),
            "min_holdout_support": (
                bot.WIDE_RESEARCH_RARE_PRECISION_MIN_HOLDOUT_SUPPORT
            ),
            "min_signals_per_week": 0.0,
            "preferred_signals_per_week": 0.0,
            "max_signals_per_week": 0.0,
            "portfolio_max_rules": 1,
            "portfolio_beam_width": 128,
            "allow_feature_ranges": True,
            "validation_window_count": 4,
        },
    }
    _with_temporal_contract(payload)
    assert bot._wide_research_rare_precision_summary_is_current(payload) is True

    stale_ranges = copy.deepcopy(payload)
    stale_ranges["search_config"]["allow_feature_ranges"] = False
    assert (
        bot._wide_research_rare_precision_summary_is_current(stale_ranges)
        is False
    )
    stale_windows = copy.deepcopy(payload)
    stale_windows["search_config"]["validation_window_count"] = 3
    assert (
        bot._wide_research_rare_precision_summary_is_current(stale_windows)
        is False
    )
    stale_policy = copy.deepcopy(payload)
    stale_policy["profile_contract"]["lifecycle_policy"][
        "min_triggers_per_week"
    ] = 2.0
    assert (
        bot._wide_research_rare_precision_summary_is_current(stale_policy)
        is False
    )
    stale_engine = copy.deepcopy(payload)
    stale_engine["discovery_engine_version"] = (
        bot.WIDE_RESEARCH_DISCOVERY_ENGINE_VERSION
    )
    assert (
        bot._wide_research_rare_precision_summary_is_current(stale_engine)
        is False
    )


def test_recent_rare_precision_summary_controls_its_own_schedule(
    monkeypatch, tmp_path
) -> None:
    latest = tmp_path / "rare-latest.json"
    latest.write_text(
        json.dumps(
            {
                "cycle_type": "discovery",
                "run_id": "rare-run",
                "registry": {},
                "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    checks = []
    monkeypatch.setattr(
        bot,
        "_wide_research_rare_precision_summary_is_current",
        lambda payload: checks.append(payload["run_id"]) or True,
    )
    monkeypatch.setattr(
        bot,
        "_wide_research_primary_summary_is_current",
        lambda payload: pytest.fail("primary checker must not be used"),
    )

    delay = bot._wide_research_initial_discovery_delay_seconds(
        discovery_file=str(latest),
        interval_seconds=100,
        require_rare_precision_profile=True,
    )
    assert 95.0 <= delay <= 100.0
    assert checks == ["rare-run"]


def test_rare_precision_collects_snapshots_and_outcomes_without_routing(
    monkeypatch,
) -> None:
    class RareLayer:
        def __init__(self) -> None:
            self.snapshot_calls = 0
            self.outcome_calls = 0

        def process_snapshot(self, *args, **kwargs):
            self.snapshot_calls += 1
            return {"status": "PASS", "claimed": []}

        def process_outcomes(self, outcomes):
            self.outcome_calls += 1
            return {
                "inserted": 1,
                "updated_triggers": 1,
                "ignored_unmatched": 0,
            }

    rare = RareLayer()
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH", False)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_FOUR_FACTOR", False)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_PRECISION", False)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_RARE_PRECISION", True)
    monkeypatch.setattr(
        bot, "_get_wide_research_rare_precision_layer", lambda: rare
    )
    monkeypatch.setattr(
        bot,
        "_build_shadow_candidate_prediction_record",
        lambda observation, rolling: {"rolling": rolling},
    )
    wakeups = []
    monkeypatch.setattr(
        bot._wide_research_wakeup, "set", lambda: wakeups.append(1)
    )

    assert bot.evaluate_and_store_wide_research(_observation()) is False
    assert bot.append_wide_research_outcomes([{"observation_id": "x"}]) == 0
    assert rare.snapshot_calls == 1
    assert rare.outcome_calls == 1
    assert wakeups == [1]


def test_rare_precision_daemon_runs_recovery_discovery_and_lifecycle(
    monkeypatch,
) -> None:
    class StopAfterOneLoop:
        def __init__(self) -> None:
            self.checks = 0

        def is_set(self):
            self.checks += 1
            return self.checks > 1

    class Wakeup:
        def wait(self, timeout):
            return None

        def clear(self):
            return None

    class Store:
        def __init__(self) -> None:
            self.recover_calls = 0

        def recover(self):
            self.recover_calls += 1
            return {"status": "ok"}

    class Controller:
        def __init__(self) -> None:
            self.reconcile_calls = 0

        def reconcile(self):
            self.reconcile_calls += 1
            return {"readiness": [], "promoted_phase_id": None}

    class Layer:
        def replay_pending(self, *, limit):
            assert limit == 256
            return {"status": "OK", "pending": 0}

        def pending_retry_status(self):
            return {"status": "OK", "pending": 0}

    store = Store()
    controller = Controller()
    discoveries = []
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH", False)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_FOUR_FACTOR", False)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_PRECISION", False)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_RARE_PRECISION", True)
    monkeypatch.setattr(
        bot, "WIDE_RESEARCH_RARE_PRECISION_AUTO_DISCOVERY", True
    )
    monkeypatch.setattr(
        bot, "WIDE_RESEARCH_RARE_PRECISION_AUTO_LIFECYCLE", True
    )
    monkeypatch.setattr(
        bot, "_wide_research_initial_discovery_delay_seconds", lambda **kwargs: 0.0
    )
    monkeypatch.setattr(
        bot,
        "_get_wide_research_rare_precision_components",
        lambda: (store, Layer(), controller),
    )
    monkeypatch.setattr(
        bot,
        "run_wide_research_rare_precision_discovery_once",
        lambda *, trigger: discoveries.append(trigger)
        or {"status": "completed", "reason": None},
    )
    monkeypatch.setattr(bot, "_wide_research_stop", StopAfterOneLoop())
    monkeypatch.setattr(bot, "_wide_research_wakeup", Wakeup())

    bot.wide_research_daemon()

    assert store.recover_calls == 1
    assert controller.reconcile_calls == 2
    assert discoveries == ["startup"]


def test_four_factor_snapshot_and_outcome_errors_do_not_change_primary(
    monkeypatch,
) -> None:
    class PrimaryLayer:
        def __init__(self) -> None:
            self.snapshot_calls = 0
            self.outcome_calls = 0

        def process_snapshot(self, *args, **kwargs):
            self.snapshot_calls += 1
            return {"status": "PASS", "claimed": []}

        def process_outcomes(self, outcomes):
            self.outcome_calls += 1
            return {"inserted": 1, "updated_triggers": 1, "ignored_unmatched": 0}

    class BrokenFourFactorLayer:
        def __init__(self) -> None:
            self.snapshot_calls = 0
            self.outcome_calls = 0

        def process_snapshot(self, *args, **kwargs):
            self.snapshot_calls += 1
            raise RuntimeError("four-factor snapshot failure")

        def process_outcomes(self, outcomes):
            self.outcome_calls += 1
            raise RuntimeError("four-factor outcome failure")

    class BrokenPrecisionLayer:
        def __init__(self) -> None:
            self.snapshot_calls = 0
            self.outcome_calls = 0

        def process_snapshot(self, *args, **kwargs):
            self.snapshot_calls += 1
            raise RuntimeError("precision snapshot failure")

        def process_outcomes(self, outcomes):
            self.outcome_calls += 1
            raise RuntimeError("precision outcome failure")

    primary = PrimaryLayer()
    four_factor = BrokenFourFactorLayer()
    precision = BrokenPrecisionLayer()
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH", True)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_FOUR_FACTOR", True)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_PRECISION", True)
    monkeypatch.setattr(bot, "_get_wide_research_layer", lambda: primary)
    monkeypatch.setattr(
        bot,
        "_get_wide_research_four_factor_layer",
        lambda: four_factor,
    )
    monkeypatch.setattr(
        bot,
        "_get_wide_research_precision_layer",
        lambda: precision,
    )
    monkeypatch.setattr(
        bot,
        "_build_shadow_candidate_prediction_record",
        lambda observation, rolling: {"rolling": rolling},
    )

    assert bot.evaluate_and_store_wide_research(_observation()) is True
    assert bot.append_wide_research_outcomes([{"observation_id": "x"}]) == 1
    assert primary.snapshot_calls == 1
    assert primary.outcome_calls == 1
    assert four_factor.snapshot_calls == 1
    assert four_factor.outcome_calls == 1
    assert precision.snapshot_calls == 1
    assert precision.outcome_calls == 1


def test_primary_errors_do_not_prevent_four_factor_collection(
    monkeypatch,
) -> None:
    class BrokenPrimaryLayer:
        def process_snapshot(self, *args, **kwargs):
            raise RuntimeError("primary snapshot failure")

        def process_outcomes(self, outcomes):
            raise RuntimeError("primary outcome failure")

    class FourFactorLayer:
        def __init__(self) -> None:
            self.snapshot_calls = 0
            self.outcome_calls = 0

        def process_snapshot(self, *args, **kwargs):
            self.snapshot_calls += 1
            return {"status": "PASS", "claimed": []}

        def process_outcomes(self, outcomes):
            self.outcome_calls += 1
            return {"inserted": 1, "updated_triggers": 1, "ignored_unmatched": 0}

    four_factor = FourFactorLayer()
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH", True)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_FOUR_FACTOR", True)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_PRECISION", False)
    monkeypatch.setattr(
        bot, "_get_wide_research_layer", lambda: BrokenPrimaryLayer()
    )
    monkeypatch.setattr(
        bot,
        "_get_wide_research_four_factor_layer",
        lambda: four_factor,
    )
    monkeypatch.setattr(
        bot,
        "_build_shadow_candidate_prediction_record",
        lambda observation, rolling: {"rolling": rolling},
    )
    wakeups = []
    monkeypatch.setattr(bot._wide_research_wakeup, "set", lambda: wakeups.append(1))

    assert bot.evaluate_and_store_wide_research(_observation()) is False
    assert bot.append_wide_research_outcomes([{"observation_id": "x"}]) == 0
    assert four_factor.snapshot_calls == 1
    assert four_factor.outcome_calls == 1
    assert wakeups == []


def test_compact_portfolio_removes_large_trigger_details_from_every_split() -> None:
    payload = {
        "portfolio_id": "portfolio-test",
        "rule_ids": ["rule-a"],
        "training": {"wins": 4, "first_triggers": [1], "daily": {"x": 1}},
        "validation": {"wins": 3, "weekly": {"x": 1}},
        "holdout": {"wins": 2, "league_counts": {"x": 1}},
    }

    compact = cycle_cli._compact_portfolio(payload)

    assert compact["portfolio_id"] == "portfolio-test"
    assert compact["rule_ids"] == ["rule-a"]
    assert compact["training"] == {"wins": 4}
    assert compact["validation"] == {"wins": 3}
    assert compact["holdout"] == {"wins": 2}


def test_router_flag_off_is_exact_current_filter_fallback(monkeypatch) -> None:
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH", True)
    monkeypatch.setattr(bot, "WIDE_RESEARCH_PRODUCTION_APPLY", False)
    decision = bot.route_publication_with_wide_research(
        fixture_id=1,
        minute=50,
        fixture_metrics={},
        probability_result={},
        current_filter_allow=True,
    )
    assert decision["applied"] is False
    assert decision["allow"] is True
    assert decision["source"] == "current_filter"


def test_recent_discovery_survives_service_restart_without_duplicate_search(
    monkeypatch, tmp_path
) -> None:
    latest = tmp_path / "latest.json"
    latest.write_text(
        json.dumps(
            _with_temporal_contract({
                "cycle_type": "discovery",
                "run_id": "wide-run",
                "registry": {},
                "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                "store_profile": "primary",
                "shadow_only": False,
                "production_enabled": bot.WIDE_RESEARCH_PRODUCTION_APPLY,
                "discovery_engine_version": (
                    bot.WIDE_RESEARCH_DISCOVERY_ENGINE_VERSION
                ),
                "feature_schema_version": bot.WIDE_RESEARCH_FEATURE_SCHEMA_VERSION,
                "threshold_grid_version": bot.WIDE_RESEARCH_ATOMIC_GRID_VERSION,
                "prospective_start_utc": bot.WIDE_RESEARCH_PROSPECTIVE_START_UTC,
                "worker_memory_limit_requested_mb": (
                    bot.WIDE_RESEARCH_WORKER_MEMORY_LIMIT_MB
                ),
                "worker_memory_limit_mb": bot.WIDE_RESEARCH_WORKER_MEMORY_LIMIT_MB,
                "search_config": _primary_search_config(),
            })
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(bot, "WIDE_RESEARCH_DISCOVERY_FILE", str(latest))
    monkeypatch.setattr(bot, "WIDE_RESEARCH_DISCOVERY_INTERVAL_SECONDS", 100)
    delay = bot._wide_research_initial_discovery_delay_seconds()
    assert 95.0 <= delay <= 100.0

    latest.write_text(
        json.dumps(
            _with_temporal_contract({
                "cycle_type": "discovery",
                "run_id": "wide-run",
                "registry": {},
                "completed_at_utc": (
                    datetime.now(timezone.utc) - timedelta(seconds=101)
                ).isoformat(),
                "store_profile": "primary",
                "shadow_only": False,
                "production_enabled": bot.WIDE_RESEARCH_PRODUCTION_APPLY,
                "discovery_engine_version": (
                    bot.WIDE_RESEARCH_DISCOVERY_ENGINE_VERSION
                ),
                "feature_schema_version": bot.WIDE_RESEARCH_FEATURE_SCHEMA_VERSION,
                "threshold_grid_version": bot.WIDE_RESEARCH_ATOMIC_GRID_VERSION,
                "prospective_start_utc": bot.WIDE_RESEARCH_PROSPECTIVE_START_UTC,
                "worker_memory_limit_requested_mb": (
                    bot.WIDE_RESEARCH_WORKER_MEMORY_LIMIT_MB
                ),
                "worker_memory_limit_mb": bot.WIDE_RESEARCH_WORKER_MEMORY_LIMIT_MB,
                "search_config": _primary_search_config(),
            })
        ),
        encoding="utf-8",
    )
    assert bot._wide_research_initial_discovery_delay_seconds() == 0.0


def test_discovery_failure_backoff_does_not_create_fifteen_minute_retry_loop() -> None:
    delay, failures = bot._wide_research_discovery_next_delay(
        {"status": "error", "reason": "worker_exit"},
        success_interval_seconds=604800,
        consecutive_failures=0,
    )
    assert (delay, failures) == (3600.0, 1)
    delay, failures = bot._wide_research_discovery_next_delay(
        {"status": "error", "reason": "worker_exit"},
        success_interval_seconds=604800,
        consecutive_failures=failures,
    )
    assert (delay, failures) == (21600.0, 2)
    delay, failures = bot._wide_research_discovery_next_delay(
        {"status": "error", "reason": "worker_exit"},
        success_interval_seconds=604800,
        consecutive_failures=failures,
    )
    assert (delay, failures) == (86400.0, 3)


def test_decision_snapshot_records_champion_as_authoritative() -> None:
    probability = {
        "prob_next_15": 40.0,
        "prob_to90": 82.0,
        "prob_until_end_decision": 82.0,
        "channel_signal_filter": {
            "version": bot.CHANNEL_SIGNAL_FILTER_VERSION,
            "passed": False,
            "reason": "prob_to90",
        },
        "wide_research_router": {
            "applied": True,
            "allow": True,
            "source": "wide_research_champion",
            "reason": "pass",
            "rule_id": "wide-rule",
            "phase_id": "wide-phase",
            "generation": 2,
        },
    }
    snapshot = bot.build_decision_snapshot(
        fixture_id=1,
        minute=50,
        window_name="WINDOW_1",
        match_identity={},
        score_home=0,
        score_away=0,
        probability_result=probability,
        threshold_result={"threshold": 75.0, "fallback_threshold": 75.0},
        threshold_next15=30.0,
        readiness_result={"passed": True, "other_hard_gates_passed": True},
        live_gate_result={"required": False, "passed": True},
        anti_garbage_passed=True,
        final_decision="ALLOW",
        block_reason=None,
        factor_context=probability,
    )
    assert snapshot["decision"]["active_publication_allow"] is True
    assert snapshot["publication_policy"]["name"] == "wide_research_champion"
    assert snapshot["publication_policy"]["channel_signal_filter_required"] is False
    assert snapshot["gates"]["wide_research_router_passed"] is True
