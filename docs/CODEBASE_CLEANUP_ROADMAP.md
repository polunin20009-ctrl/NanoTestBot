# Codebase cleanup roadmap

Date: 2026-09-23

## Goal

Turn the repository into a codebase that is easy to navigate and maintain
without changing production behavior, publication policy, persisted schemas,
Telegram output, research causality, or operational entry points.

This is an execution plan, not permission to redesign product behavior. Policy
changes and correctness fixes must remain separate from refactoring changes.

## Baseline

- `NanoTest.py`: 30,738 lines, 570 top-level functions, 507 module-level
  assignments.
- Largest functions:
  - `main_loop`: 1,267 lines
  - `compute_probability_45_plus`: 667 lines
  - `perform_monitor_update`: 636 lines
  - `wide_research_daemon`: 537 lines
- Existing package boundaries are useful and should be preserved:
  `second_half`, `signal_reputation`, `shadow_ml`, `shadow_candidates`,
  `market_benchmark`, `wide_research`, and `walk_forward`.
- Current regression baseline: 895 tests pass.
- `docs/ARCHITECTURE_AUDIT.md` describes the live topology.
- `docs/FUNCTIONAL_CONTRACT.md` defines behavior that refactoring must preserve.

## Non-negotiable invariants

Every cleanup change must preserve:

1. The 46–60 production decision window and current BASE gate semantics.
2. Wide-research router fallback and champion behavior, including current
   fail-open/fail-closed details documented by characterization tests.
3. At most one ordinary Telegram send per fixture and the exact message
   contract.
4. Decision and observation identities, schemas, ordering, deduplication, and
   append-only outcome revisions.
5. First-trigger and causal-join behavior for every research layer.
6. Existing environment variable names, defaults, CLI commands, script entry
   points, and `NanoTest` import surface until a compatibility layer exists.
7. Importing `NanoTest` in library mode must not load or overwrite live state.

## Verification gates

Every code PR:

```bash
PYTHONPATH=. python3 -m pytest tests/ -q --tb=short
git diff --check
```

Any change touching publication, snapshots, observations, Telegram, outcomes,
or the router also runs:

```bash
PYTHONPATH=. python3 -m pytest \
  tests/test_production_flow_characterization.py \
  tests/test_decision_snapshots.py \
  tests/test_observation_history.py \
  tests/test_probability_message_format.py -q
```

Before moving orchestration code, add characterization coverage for:

- one mocked `main_loop` ALLOW path and one BLOCK path;
- `perform_monitor_update` and its Telegram/wide-monitor side effects;
- router exception fallback through the `NanoTest` wrapper;
- `--dump-match` CLI startup;
- feature-flag startup logging.

## Dead-code classification

### High-confidence leaf candidates

Static analysis found no repository call sites for these symbols. Each cluster
still requires a final symbol search and the full test suite before removal.

- Legacy probability leaf:
  `calculate_goal_probability` and its private constants.
- Unused render wrappers:
  `render_live_main_text`, `format_signal_report_from_metrics`.
- Shadowed duplicate:
  the first `_utc_now_iso` definition.
- Duplicate unused league updaters:
  `update_league_stats`, `refresh_league_factor`,
  `_get_persisted_league_factor`.
- Superseded cleanup helpers:
  `cleanup_expired_no_stats_fixtures`,
  `cleanup_expired_no_stats_blocked`, `has_active_matches`, `cleanup_now`,
  `_set_pending_cleanup`.
- Unused cache/index helpers:
  `invalidate_shadow_ml_model_cache`,
  `invalidate_shadow_rolling_model_cache`,
  `_invalidate_decision_snapshot_index`.
- Unused state/display leaves:
  `add_ignored_match`, `get_signal_snapshot_state`, `get_bot_username`, and
  legacy formatting/scoring helpers identified by the audit.
- `match_period.is_extra_time_goal`.

These should be removed by coherent cluster, never as one large deletion.

### Requires runtime or operator evidence

Do not delete solely from static analysis:

- Google Sheets integration: disabled in current code but still wired as no-op
  startup and send calls.
- Rescue publication/state: retired from production, but shadow/audit fields
  remain part of historical comparability.
- Admin-review legacy state keys: inactive but intentionally tolerated while
  loading old state.
- Shadow ML champion utilities: may be invoked manually outside the bot.
- Any CLI, Telegram callback, feature-flag branch, persisted-state reader, or
  dynamically dispatched handler.

## Execution sequence

### PR 1 — Characterization gaps

Add tests for `main_loop`, monitor orchestration, router failure fallback, CLI,
and startup flags. No production code changes.

### PR 2 — Proven dead leaves

Remove the smallest high-confidence clusters one at a time. Keep unrelated
formatting out of these diffs. Use structural tests where a removed feature
must not return.

### PR 3 — Package skeleton and compatibility surface

Introduce a small application package for extracted runtime code. Keep
`NanoTest.py` re-exporting existing public names so tests, scripts, and
operator commands continue to work.

Suggested shape:

```text
goalbot/
  config/
  runtime/
  publication/
  telegram/
  observations/
  persistence/
```

Do not move `main_loop` in this PR.

### PR 4 — Pure publication helpers

Move pure threshold, gate, badge, and rendering helpers while preserving
function signatures and constants through compatibility re-exports.

### PR 5 — Low-risk research runtime glue

Move shadow-candidate and market-benchmark runtime wrappers next to their
existing packages. Preserve journal ordering and path assertions exactly.

### PR 6 — Wide-research runtime registry

Replace the repeated profile setup with one parameterized internal factory,
while retaining the primary-only production router distinction.

### PR 7 — Shadow ML and reputation runtime

Move caches, worker invocation, and daemon scheduling without changing
retraining thresholds, subprocess environment, artifact paths, or application
order.

### PR 8 — Observation and outcome spine

Move builders and reconciliation only after golden JSONL fixtures exist.
Byte-level or normalized-record comparisons must demonstrate equivalent
decision, observation, and outcome output.

### PR 9 — Telegram and monitor

Extract queueing, formatting, edit behavior, and monitor orchestration after
fixture-level characterization coverage is complete.

### PR 10 — Decompose `main_loop`

Extract named pipeline stages without changing their order:

```text
poll -> collect -> score -> readiness -> BASE -> router
     -> delivery -> decision snapshot -> research fan-out
```

The final `NanoTest.py` remains the executable compatibility entry point and
should contain configuration wiring and process startup rather than business
logic.

### Final pass

- Remove compatibility re-exports only after all internal and operator callers
  have migrated.
- Update architecture, functional-contract, and operator documentation.
- Apply consistent formatting in dedicated mechanical PRs, not mixed with
  moves or deletions.
- Re-run replay/golden comparisons against recorded fixtures before deploying.

## What not to combine

- Refactoring with threshold, router, or publication-policy changes.
- File moves with JSONL/SQLite schema changes.
- Dead-code removal with broad formatting.
- `main_loop` extraction with monitor extraction.
- Shared storage deduplication across journals until each journal's rotation,
  locking, fsync, and deduplication semantics are characterized independently.

## Completion criteria

The cleanup is complete when:

- `NanoTest.py` is a small documented entry point rather than a god module;
- each subsystem has one clear owner and dependency direction;
- no proven dead code remains;
- runtime and operator entry points are documented;
- production and replay outputs remain equivalent;
- the full regression suite stays green throughout the migration.
