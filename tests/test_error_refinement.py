from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from wide_research import discovery as d
from wide_research.refinement import POLICY, comparison, conjoin, refine_candidates


def atom(feature, op, value):
    return d.AtomicClause(feature, op, value, _allow_alternate_operator=True)


A = atom("feature.season_context_factor", ">=", 1.02)
B = atom("raw.shots_on_target_total", ">=", 3)
C = atom("feature.adjusted_intensity", ">=", .5)


def config(**values):
    return d.DiscoveryConfig(**dict({"selection_mode": "rare_precision",
        "extended_features": True, "error_refinement": True,
        "allow_feature_ranges": True, "validation_window_count": 4,
        "max_conjunction_size": 4, "min_train_support": 12,
        "min_validation_support": 8, "min_holdout_support": 4,
        "beam_width": 16, "evaluation_budget": 2000, "top_n": 2}, **values))


def rows(count=120, start=0, *, break_validation=False):
    result = {}
    for i in range(count):
        j = i % 10
        label = int(j < 8)
        b = j < 7 if not break_validation else j >= 7
        fid = start + i + 1
        r = d._Row(str(fid), fid, datetime(2026, 1, 1, tzinfo=timezone.utc)
            + timedelta(minutes=fid * 120), 50, "1", label,
            {A.feature: 1.04, B.feature: 4 if b else 1, C.feature: .8 if b else .2})
        result[fid] = [r]
    return result


def test_error_guided_child_removes_losses_and_preserves_wins():
    train, val = rows(), rows(80, 120)
    before = deepcopy((train, val))
    children, report = refine_candidates(train, val, config(), [A, B], [(A,)])
    assert children and report["parents"]
    child = children[0]
    assert child["rule"] == conjoin((A,), (B,))
    assert child["lineage"]["train_comparison"]["losses_excluded"] == 24
    assert child["lineage"]["train_comparison"]["wins_lost"] == 12
    assert child["lineage"]["train_comparison"]["win_retention"] == .875
    assert child["lineage"]["validation_comparison"]["hit_rate_gain"] == pytest.approx(.2)
    assert len(child["lineage"]["validation_windows"]) == 4
    assert before == (train, val)


def test_validation_rejects_train_only_pattern():
    children, report = refine_candidates(rows(), rows(80, 120, break_validation=True),
        config(), [A, B], [(A,)])
    assert not children
    assert report["train_finalists"] > 0
    assert report["rejected"]["validation_tradeoff_or_stability"] > 0


def test_crossover_borrows_complete_donor_and_keeps_parent():
    train, val = rows(), rows(80, 120)
    for group in (train, val):
        for i, rr in enumerate(group.values()):
            rr[0].features[B.feature] = 4 if i % 10 not in (7, 8) else 1
            rr[0].features[C.feature] = .8 if i % 10 != 9 else .2
    children, report = refine_candidates(train, val, config(), [A, B, C],
        [(A,), (B, C)])
    assert report["methods_evaluated"]["crossover"] > 0
    crossovers = [x for x in children if x["lineage"]["method"] == "crossover"]
    assert crossovers
    assert crossovers[0]["lineage"]["donor_candidate_id"]
    assert A in crossovers[0]["rule"]


def test_delayed_loss_is_not_counted_as_excluded():
    parent = rows(10)
    first = parent[9][0]  # losing match
    delayed = replace(first, observation_id="9:later", minute=55,
                      created_at=first.created_at + timedelta(minutes=5))
    delta = comparison([first], [delayed])
    assert delta["losses_excluded"] == 0
    assert delta["shifted_triggers"] == 1


def test_delayed_win_becoming_loss_is_a_lost_plus():
    first = rows(1)[1][0]
    delayed = replace(first, label=0, observation_id="later")
    delta = comparison([first], [delayed])
    assert delta["wins_lost"] == 1
    assert delta["parent_win_to_child_loss"] == 1
    with pytest.raises(ValueError, match="subset"):
        comparison([first], [replace(delayed, fixture_id=999)])


def test_replay_catches_delayed_losing_triggers():
    train, val = rows(), rows(80, 120)
    for group in (train, val):
        for fixture_rows in group.values():
            r = fixture_rows[0]
            if r.label == 0:
                fixture_rows.append(replace(r, observation_id=r.observation_id+":later",
                    minute=55, created_at=r.created_at+timedelta(minutes=5),
                    features={**r.features, B.feature: 5}))
    children, _ = refine_candidates(train, val, config(), [A, B], [(A,)])
    assert not children


def test_tightening_and_conflicting_ranges():
    tighter = atom(A.feature, ">=", 1.05)
    assert conjoin((A,), (tighter,)) == (tighter,)
    assert conjoin((A,), (atom(A.feature, "<=", 1.0),)) is None


def test_zero_losses_do_not_create_error_parents():
    children, report = refine_candidates(rows(), rows(80, 120), config(), [A, B], [(B,)])
    assert not children and not report["parents"]


def test_excluded_rule_is_not_republished_as_new_child():
    children, _ = refine_candidates(rows(), rows(80, 120), config(), [A, B], [(A,)],
        excluded_rules=[conjoin((A,), (B,))])
    assert not children


def test_budget_is_enforced(monkeypatch):
    monkeypatch.setitem(POLICY, "proposal_budget", 1)
    children, report = refine_candidates(rows(), rows(80, 120), config(), [A, B, C], [(A,)])
    assert report["proposals_evaluated"] <= 1
    assert report["budget_exhausted"]


def test_market_dependency_cannot_disappear():
    market = atom("market.v1.prob_to90", ">=", 70)
    train, val = rows(), rows(80, 120)
    for group in (train, val):
        for fixture_rows in group.values():
            fixture_rows[0].features[market.feature] = 80
    children, _ = refine_candidates(train, val, config(market_only=True),
        [market, B], [(market,)])
    assert children and all(market in c["rule"] for c in children)


def test_refinement_cannot_enable_in_legacy_profile():
    with pytest.raises(ValueError, match="isolated extended"):
        d.DiscoveryConfig(error_refinement=True)


def discovery_report(monkeypatch, *, flip_holdout=False):
    # Exercise the real discovery/manifest boundary with a controlled search
    # pool, keeping fixture splitting, refinement and holdout evaluation real.
    from test_wide_research_discovery import _observation
    records = []
    for i in range(240):
        j = i % 10
        r = _observation(i+1, 50, datetime(2026, 1, 1, tzinfo=timezone.utc)
            + timedelta(hours=2*i), label=(j < 8), probability=70)
        r["features"].update(season_context_factor=1.04, adjusted_intensity=.8 if j < 7 else .2)
        r["raw_metrics"].update(shots_on_target_home=4 if j < 7 else 1, shots_on_target_away=0)
        r["availability"] = {"shots_on_target_home": True, "shots_on_target_away": True}
        records.append(r)
    cfg = config(train_fraction=.5, validation_fraction=1/3)
    monkeypatch.setattr(d, "_threshold_grids", lambda _: {
        A.feature: (">=", (1.02,)), B.feature: (">=", (3,))})
    if flip_holdout:
        for r in records[200:]:
            r["outcome"]["goal_to90_normal_time"] = not r["outcome"]["goal_to90_normal_time"]
    return d.discover_rules(records, config=cfg)


def test_lineage_round_trip_and_no_production_admission(monkeypatch, tmp_path):
    from wide_research.controller import WideResearchController
    from wide_research.store import WideResearchStore
    # Hold the generic beam constant to exercise child-only admission and
    # immutable lineage independently of the old beam's choice of aliases.
    def controlled_search(train, validation, cfg, atoms):
        children, diag = refine_candidates(train, validation, cfg, [A, B], [(A,)])
        assert children
        selected = [x["rule"] for x in children]
        ids = [d._candidate_id(x, config=cfg) for x in selected]
        return selected, {"error_refinement": diag}, {
            "train": {cid:d._metrics(d._first_triggers(train, rule), len(train)) for cid, rule in zip(ids, selected)},
            "validation": {cid:d._metrics(d._first_triggers(validation, rule), len(validation)) for cid, rule in zip(ids, selected)},
            "validation_stability": {cid:x["stability"] for cid,x in zip(ids,children)},
            "selection_lanes": {cid:"error_refinement" for cid in ids},
            "offspring_lineage": {cid:x["lineage"] for cid,x in zip(ids,children)}}
    monkeypatch.setattr(d, "_search_candidates", controlled_search)
    report = discovery_report(monkeypatch)
    changed = discovery_report(monkeypatch, flip_holdout=True)
    assert report["manifests"]["candidates"] == changed["manifests"]["candidates"]
    assert report["manifests"]["candidates"][0]["refinement_lineage"]
    store = WideResearchStore(tmp_path / "rules.sqlite3", allowed_root=tmp_path)
    controller = WideResearchController(store, active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-01-01T00:00:00Z", production_enabled=True)
    with pytest.raises(ValueError, match="isolated terminal shadow"):
        controller.import_report(report)
    assert not store.list_phases()
    store.bind_profile("rare_precision_shadow")
    controller = WideResearchController(store, active_manifest_path=str(tmp_path / "active.json"),
        prospective_start_utc="2026-01-01T00:00:00Z", terminal_review_enabled=True)
    imported = controller.import_report(report, imported_at_utc="2026-03-01T00:00:00Z")
    phase = store.list_phases()[0]
    assert phase["policy"]["discovery"]["refinement_lineage"] == report["manifests"]["candidates"][0]["refinement_lineage"]
    assert phase["starts_at_utc"] == "2026-03-01T00:00:00.000000Z"
    again = controller.import_report(report, imported_at_utc="2026-03-02T00:00:00Z")
    assert not again["imported"] and again["reused"]
    assert store.list_phases()[0]["starts_at_utc"] == phase["starts_at_utc"]


def test_holdout_labels_cannot_change_selected_children(monkeypatch):
    a = discovery_report(monkeypatch)
    b = discovery_report(monkeypatch, flip_holdout=True)
    assert a["manifests"]["candidates"] == b["manifests"]["candidates"]
    assert a["manifests"]["search"]["error_refinement"] == b["manifests"]["search"]["error_refinement"]
    assert a["results"]["results"] != b["results"]["results"]


def test_worker_flags_and_cli_guard():
    import NanoTest as bot
    from scripts.run_wide_research_cycle import main
    paths = {k: "/tmp/unused" for k in ("observations", "static", "rolling", "market")}
    assert "--error-refinement" in bot._wide_research_worker_command(paths, rare_precision=True)
    assert "--error-refinement" not in bot._wide_research_worker_command(paths)
    with pytest.raises(SystemExit, match="isolated extended"):
        main(["--error-refinement", "--prospective-start-utc", "2026-09-11T00:00:00Z"])
