"""Bounded error-guided offspring search. Receives train/validation, never holdout.

Parents/donors and proposals are chosen exclusively on train. A descendant
remains a standalone first-trigger rule, not a retrospectively edited parent.
"""
from __future__ import annotations

from collections import Counter
import math
from itertools import combinations
from typing import Any, Sequence

REFINEMENT_VERSION = "error_guided_offspring_v1"
POLICY = {
    "version": REFINEMENT_VERSION,
    "proposal_budget": 20000,
    "max_parents": 8,
    "max_donors": 8,
    "max_additions_per_parent": 8,
    "max_train_finalists_per_parent": 4,
    "max_children": 4,
    "max_children_per_parent": 2,
    "min_parent_losses": 3,
    "min_parent_hit_rate": 0.70,
    "min_train_win_retention": 0.60,
    "min_train_loss_rejection": 0.20,
    "min_train_discrimination": 0.10,
    "min_train_hit_rate_gain": 0.02,
    "min_validation_win_retention": 0.50,
    "min_validation_hit_rate_gain": 0.01,
    "min_noninferior_validation_windows": 3,
    "parent_selection_split": "train",
    "proposal_selection_split": "train",
    "selection_uses_holdout": False,
    "production_applied": False,
}


def comparison(parent_rows: Sequence, child_rows: Sequence) -> dict[str, Any]:
    """Count actual exclusions, not merely a delayed first observation."""
    parent = {r.fixture_id: r for r in parent_rows}
    child = {r.fixture_id: r for r in child_rows}
    if not set(child).issubset(parent):
        raise ValueError("offspring must be a subset of parent fixtures")
    wins = sum(r.label for r in parent.values())
    losses = len(parent) - wins
    kept_wins = sum(r.label == 1 and fid in child and child[fid].label == 1
                    for fid, r in parent.items())
    excluded_losses = sum(r.label == 0 and fid not in child for fid, r in parent.items())
    parent_rate = wins / len(parent) if parent else 0.0
    child_rate = sum(r.label for r in child.values()) / len(child) if child else 0.0
    retention = kept_wins / wins if wins else 0.0
    rejection = excluded_losses / losses if losses else 0.0
    return {
        "parent_wins": wins, "parent_losses": losses,
        "child_wins": sum(r.label for r in child.values()),
        "child_losses": sum(1 - r.label for r in child.values()),
        "wins_retained": kept_wins, "wins_lost": wins - kept_wins,
        "losses_excluded": excluded_losses,
        "win_retention": retention, "loss_rejection": rejection,
        "discrimination": rejection - (1 - retention),
        "hit_rate_gain": child_rate - parent_rate,
        "shifted_triggers": sum(r.observation_id != parent[fid].observation_id
                                for fid, r in child.items()),
        "parent_win_to_child_loss": sum(parent[fid].label == 1 and r.label == 0
                                        for fid, r in child.items()),
    }


def conjoin(*rules):
    """Tighten redundant same-direction bounds; never loosen the parent."""
    from .discovery import _clause_key, _clauses_are_compatible
    bounds = {}
    for rule in rules:
        for clause in rule:
            key = clause.feature, clause.operator
            old = bounds.get(key)
            if old is None or (clause.threshold > old.threshold if clause.operator == ">="
                               else clause.threshold < old.threshold):
                bounds[key] = clause
    result = tuple(sorted(bounds.values(), key=_clause_key))
    return result if _clauses_are_compatible(result) else None


def refine_candidates(train, validation, config, atoms, parent_pool, *, excluded_rules=()):
    # Lazy import keeps the core discovery module independent at import time.
    from . import discovery as d
    from .extended_features import has_market_dependency

    if not config.error_refinement or not config.extended_features or config.selection_mode != "rare_precision":
        raise ValueError("error refinement requires the isolated extended rare search")
    matcher = d._FirstTriggerBitsetMatcher(train)
    span = d._evaluation_span_days(train)
    val_span = d._evaluation_span_days(validation)
    rejected = Counter()
    unique = {d._rule_key(r): r for r in parent_pool}
    training = {key: d._ranking_metrics(matcher.first_triggers(rule), len(train),
                evaluation_span_days=span) for key, rule in unique.items()}
    ranked = sorted(unique.values(), key=lambda r: d._rank_key(training[d._rule_key(r)], r))

    def diverse(rules, limit):
        result, seen = [], set()
        for rule in rules:
            signature = tuple((r.fixture_id, r.observation_id) for r in matcher.first_triggers(rule))
            if signature in seen:
                continue
            seen.add(signature); result.append(rule)
            if len(result) >= limit:
                break
        return result

    donors = diverse(ranked, POLICY["max_donors"])
    parents = diverse([r for r in ranked
        if training[d._rule_key(r)]["losses"] >= POLICY["min_parent_losses"]
        and training[d._rule_key(r)]["hit_rate"] >= POLICY["min_parent_hit_rate"]
        and len(r) < config.max_conjunction_size], POLICY["max_parents"])
    excluded = {d._rule_key(r) for r in excluded_rules}
    attempts = 0
    train_finalists = []
    parent_reports = []
    methods = Counter()
    for parent in parents:
        parent_key = d._rule_key(parent)
        parent_id = d._candidate_id(parent, config=config)
        parent_rows = matcher.first_triggers(parent)
        accepted, tried, additions = [], set(), []

        def evaluate(addition, method, donor=None, contrast=None):
            nonlocal attempts
            child = conjoin(parent, addition)
            if child is None or len(child) > config.max_conjunction_size or len(child) < config.min_conjunction_size:
                rejected["incompatible_or_depth"] += 1; return
            key = d._rule_key(child)
            if key == parent_key or key in tried:
                return
            tried.add(key)
            if attempts >= POLICY["proposal_budget"]:
                rejected["budget"] += 1; return
            attempts += 1; methods[method] += 1
            if config.market_only and not any(has_market_dependency(c.feature) for c in child):
                raise ValueError("market descendant lost market dependency")
            child_rows = matcher.first_triggers(child)
            metrics = d._ranking_metrics(child_rows, len(train), evaluation_span_days=span)
            delta = comparison(parent_rows, child_rows)
            if (metrics["resolved"] < config.min_train_support
                or delta["win_retention"] < POLICY["min_train_win_retention"]
                or delta["loss_rejection"] < POLICY["min_train_loss_rejection"]
                or delta["discrimination"] + 1e-12 < POLICY["min_train_discrimination"]
                or delta["hit_rate_gain"] + 1e-12 < POLICY["min_train_hit_rate_gain"]):
                rejected["train_tradeoff"] += 1; return
            accepted.append({"rule": child, "parent": parent, "train": metrics,
                "lineage": {"version": REFINEMENT_VERSION, "method": method,
                    "parent_candidate_id": parent_id,
                    "parent_clauses": [c.as_dict() for c in parent],
                    "donor_candidate_id": d._candidate_id(donor, config=config) if donor else None,
                    "donor_clauses": [c.as_dict() for c in donor] if donor else [],
                    "added_conditions": [c.as_dict() for c in addition],
                    "parent_selection_split": "train", "train_comparison": delta,
                    "atom_contrast": contrast, "selection_uses_holdout": False}})

        # Evaluate complete donor intersections first so they have a reserved
        # opportunity even when the atomic search consumes its full budget.
        for donor in donors:
            if d._rule_key(donor) != parent_key:
                evaluate(donor, "crossover", donor=donor)
        for atom in atoms:
            if attempts >= POLICY["proposal_budget"]:
                break
            if d._clause_key(atom) in parent_key:
                continue
            good = [r for r in parent_rows if d._matches(r, (atom,))]
            wp = sum(r.label for r in good)
            lp = len(good) - wp
            pw = training[parent_key]["wins"]; pl = training[parent_key]["losses"]
            contrast = {"win_pass_fraction": wp / pw, "loss_pass_fraction": lp / pl,
                "wins_missing_feature": sum(r.label == 1 and atom.feature not in r.features for r in parent_rows),
                "losses_missing_feature": sum(r.label == 0 and atom.feature not in r.features for r in parent_rows)}
            # This measures separability on the original parent triggers;
            # actual first-trigger replay below is still mandatory.
            gain = contrast["win_pass_fraction"] - contrast["loss_pass_fraction"]
            if contrast["win_pass_fraction"] >= POLICY["min_train_win_retention"] and gain >= POLICY["min_train_discrimination"]:
                additions.append((gain, atom, contrast))
                donor = next((r for r in donors if atom in r), None)
                evaluate((atom,), "donor_condition" if donor else "error_condition", donor, contrast)
        additions.sort(key=lambda x: (-x[0], d._clause_key(x[1])))
        for a, b in combinations(additions[:POLICY["max_additions_per_parent"]], 2):
            evaluate((a[1], b[1]), "error_pair")
        accepted.sort(key=lambda x: (d._rank_key(x["train"], x["rule"]),
            -x["lineage"]["train_comparison"]["discrimination"]))
        shortlisted = diverse([x["rule"] for x in accepted], POLICY["max_train_finalists_per_parent"])
        shortlist_keys = {d._rule_key(r) for r in shortlisted}
        train_finalists.extend(x for x in accepted if d._rule_key(x["rule"]) in shortlist_keys)
        parent_reports.append({"parent_candidate_id": parent_id,
            "clauses": [c.as_dict() for c in parent], "train": training[parent_key],
            "distinctive_atoms": len(additions), "train_supported_children": len(accepted),
            "top_contrasts": [{"condition": a.as_dict(), **c} for _, a, c in additions[:8]]})

    windows, _ = d._chronological_validation_windows(validation, config.validation_window_count)
    min_window = max(1, math.ceil(config.min_validation_support / config.validation_window_count))
    supported = []
    for child in train_finalists:
        rule, parent = child["rule"], child["parent"]
        rows = d._first_triggers(validation, rule)
        metrics = d._ranking_metrics(rows, len(validation), evaluation_span_days=val_span)
        delta = comparison(d._first_triggers(validation, parent), rows)
        wm = [d._ranking_metrics(d._first_triggers(w, rule), len(w)) for w in windows]
        stability = d._rare_validation_stability(metrics, wm,
            min_total_support=config.min_validation_support, min_window_support=min_window)
        comparisons = [comparison(d._first_triggers(w, parent), d._first_triggers(w, rule)) for w in windows]
        noninferior = sum(x["hit_rate_gain"] >= -1e-12 for x in comparisons)
        if (not stability["support_gate_passed"]
            or delta["win_retention"] < POLICY["min_validation_win_retention"]
            or delta["hit_rate_gain"] + 1e-12 < POLICY["min_validation_hit_rate_gain"]
            or delta["discrimination"] < -1e-12
            or noninferior < POLICY["min_noninferior_validation_windows"]):
            rejected["validation_tradeoff_or_stability"] += 1; continue
        child.update(validation=metrics, stability=stability, validation_rows=rows)
        child["lineage"].update(validation_comparison=delta, validation_windows=comparisons)
        supported.append(child)
    supported.sort(key=lambda x: (-(x["stability"].get("minimum_window_combined_score") or 0),
        d._rank_key(x["validation"], x["rule"]), d._rule_key(x["rule"])))
    selected, seen, parent_counts = [], set(), Counter()
    for child in supported:
        key = d._rule_key(child["rule"])
        pid = child["lineage"]["parent_candidate_id"]
        signature = tuple((r.fixture_id, r.observation_id) for r in child["validation_rows"])
        if key in excluded or signature in seen or parent_counts[pid] >= POLICY["max_children_per_parent"]:
            continue
        seen.add(signature); excluded.add(key); parent_counts[pid] += 1
        selected.append(child)
        if len(selected) >= POLICY["max_children"]:
            break
    diagnostics = {"policy": dict(POLICY), "parents": parent_reports,
        "donor_count": len(donors), "proposals_evaluated": attempts,
        "budget_exhausted": attempts >= POLICY["proposal_budget"],
        "methods_evaluated": dict(methods), "rejected": dict(rejected),
        "train_finalists": len(train_finalists), "validation_supported": len(supported),
        "selected": len(selected)}
    return selected, diagnostics
