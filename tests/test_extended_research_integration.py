from copy import deepcopy

import pytest

import NanoTest as bot
from scripts import run_wide_research_cycle as cycle_cli
from wide_research.features import ALLOWED_FEATURE_NAMES, extract_features
from wide_research.extended_features import EXTENDED_FEATURE_NAMES


def test_market_evidence_frozen_before_durable_observation_and_ml(monkeypatch):
    source = {"record_type": "observation", "observation_id": "1:50", "fixture_id": 1,
              "minute": 50, "created_at_utc": "2026-09-11T12:00:00Z",
              "match": {"score_home": 0, "score_away": 0},
              "probabilities": {"prob_to90": 80}}
    original = deepcopy(source)
    frozen = {"version": "causal_market_features_v1", "status": "unavailable", "quotes": {}}
    calls = []
    class Cache:
        def freeze(self, observation):
            calls.append("freeze")
            return deepcopy(frozen)
    monkeypatch.setattr(bot, "ENABLE_WIDE_RESEARCH_RARE_PRECISION", True)
    monkeypatch.setattr(bot, "freeze_observation_rolling_dynamics", lambda x: deepcopy(x))
    monkeypatch.setattr(bot, "_market_research_quote_cache", Cache())
    def append(record):
        calls.append("append")
        assert record["market_research"] == frozen
        assert extract_features(record) == extract_features(original)
        return True
    monkeypatch.setattr(bot, "append_observation_history", append)
    for name in ("register_observation_rolling_baseline", "predict_and_append_shadow_ml",
                 "predict_and_append_shadow_rolling_ml", "capture_market_benchmark_decision",
                 "evaluate_and_append_shadow_candidates", "evaluate_and_store_wide_research"):
        monkeypatch.setattr(bot, name, lambda *args, **kwargs: None)
    assert bot.persist_observation_and_score_shadow(source)
    assert calls == ["freeze", "append"]
    assert source == original
    calls.clear()
    source["market_research"] = deepcopy(frozen)
    assert bot.persist_observation_and_score_shadow(source)
    assert calls == ["append"]  # existing unavailable evidence cannot be replaced


def test_extended_cli_cannot_run_against_production():
    with pytest.raises(SystemExit, match="hard-shadow rare profile"):
        cycle_cli.main(["--prospective-start-utc", "2026-09-11T00:00:00Z",
                        "--extended-features", "--production-enabled"])


def test_worker_uses_stable_market_snapshot_only_in_rare_profile():
    sources = {"observations": "/tmp/o", "static": "/tmp/s", "rolling": "/tmp/r", "market": "/tmp/frozen-market"}
    rare = bot._wide_research_worker_command(sources, rare_precision=True)
    primary = bot._wide_research_worker_command(sources)
    assert rare[rare.index("--market-quotes") + 1] == sources["market"]
    assert "--extended-features" in rare
    assert "--market-quotes" not in primary and "--extended-features" not in primary


def test_primary_grid_stays_original_and_extended_is_opt_in():
    from wide_research.discovery import DiscoveryConfig, _threshold_grids
    legacy = _threshold_grids(DiscoveryConfig(selection_mode="precision_first"))
    assert set(legacy) == set(ALLOWED_FEATURE_NAMES)
    assert not set(legacy).intersection(EXTENDED_FEATURE_NAMES)
    extended = _threshold_grids(DiscoveryConfig(selection_mode="rare_precision", allow_feature_ranges=True,
                                               validation_window_count=4, extended_features=True))
    assert set(EXTENDED_FEATURE_NAMES).issubset(extended)


def test_extended_registry_rejects_production_and_runs_only_shadow(tmp_path, monkeypatch):
    from test_extended_research_discovery import _config, _rows, PRODUCT
    import wide_research.discovery as discovery
    from wide_research.store import WideResearchStore
    from wide_research.controller import WideResearchController
    from wide_research.live import WideShadowLayer
    monkeypatch.setattr(discovery, "_threshold_grids", lambda config: {PRODUCT: (">=", ())})
    rows = _rows(64)
    report = discovery.discover_rules(rows, config=_config(max_conjunction_size=1))
    assert report["manifests"]["candidates"]
    store = WideResearchStore(tmp_path / "shadow.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(store, active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-01-01T00:00:00Z", production_enabled=True)
    with pytest.raises(ValueError, match="isolated terminal shadow"):
        controller.import_report(report)
    assert store.list_phases() == []
    store.bind_profile("rare_precision_shadow")
    controller = WideResearchController(store, active_manifest_path=str(tmp_path / "shadow-active.json"),
        prospective_start_utc="2026-01-01T00:00:00Z", terminal_review_enabled=True)
    result = controller.import_report(report, imported_at_utc="2026-01-02T00:00:00Z")
    assert result["imported"]
    layer = WideShadowLayer(store, refresh_seconds=0)
    hits = [hit for row in rows for hit in layer.process_snapshot(row)["claimed"]]
    assert hits
    assert all(phase["status"] == "shadow" for phase in store.list_phases())
    assert not (tmp_path / "shadow-active.json").exists()


def test_malformed_market_quote_does_not_abort_join():
    from wide_research.discovery import JournalJoinStore
    with JournalJoinStore() as join:
        join.ingest_market_quotes([{"record_type": "market_odds_snapshot", "record_key": "bad",
            "fixture_id": 1, "captured_at_utc": "2026-09-11T12:00:00Z", "value": float("nan")}])
        assert join.counts["market_invalid_payload"] == 1
        assert join.connection.execute("SELECT count(*) FROM market_quotes").fetchone()[0] == 0
