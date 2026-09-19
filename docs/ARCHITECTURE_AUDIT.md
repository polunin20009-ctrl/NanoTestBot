# Architecture audit — Goal Predictor Telegram Bot

Audit date: 2026-09-19  
Scope: repository source as of Git baseline `3d3da6d` plus local runtime wiring.  
Constraint: read-only analysis. No application logic was changed.

This document describes how the live bot publishes signals, how research layers attach without (normally) replacing publication, where state lives, what is duplicated or leftover, and the main operational risks.

---

## 1. Executive summary

The system is a **live second-half goal-signal bot** wrapped in a **shadow research factory**.

- **Production** is a single process (`NanoTest.py` `main_loop`): poll API-Football live fixtures → Poisson/intensity 45+ model → reputation calibration → BASE channel gate `base_rep15_int055_season102_p90_75_v1` → optional wide-research champion router → Telegram → post-send monitor edits.
- **Research** is many parallel shadow pipelines that consume the same observations: rolling 5/10-minute dynamics, static and rolling ML residuals, market odds, hand-written shadow candidate arms, and four wide-research discovery profiles (primary, 4f, precision, rare_precision). Only the **primary** wide-research profile is allowed to become a production champion, and only when `WIDE_RESEARCH_PRODUCTION_APPLY` is true **and** an active checksummed manifest exists. Today there is no champion; 4f / precision / rare are hard shadow-only.
- **`NanoTest.py` is the god object** (~31 800 lines). Packages under `wide_research/`, `shadow_ml/`, `shadow_candidates/`, `signal_reputation/`, `market_benchmark/`, `second_half/`, `walk_forward/` hold the newer contracts. The bot file still owns the live loop, persistence, Telegram, daemons, Google Sheets leftovers, and most glue.
- The architecture is **fail-closed and shadow-first by design** (causal ML join, first-trigger freeze, Wilson/Holm lifecycle, `production_applied=false` on ML/market artifacts). The remaining risks are operational: a **hardcoded API key**, **env flags that can enable champion override**, **one process / many writers on shared journals**, **memory (OOM on full journal reports)**, and **history still containing secrets**.

---

## 2. Layered map

```
                    API-Football (live fixtures, stats, events, odds)
                                      |
                                      v
                         NanoTest.APISportsMetricsClient
                                      |
                 +--------------------+--------------------+
                 |                    |                    |
           main_loop            monitor_daemon      market_benchmark_daemon
           (46–60 eval)         (sent matches)      (odds/live quotes)
                 |                    |
                 v                    v
        compute_probability_45_plus
        + 2H factors (soft apply)
        + signal reputation auto-apply
                 |
                 v
        BASE channel filter  ------------ optional ActiveRuleRouter
        (production gate)                (primary wide champion only)
                 |
                 +-- ALLOW --> Telegram send + persist state
                 +-- BLOCK --> snapshot still recorded
                 |
                 v
        persist_observation_and_score_shadow
                 |
     observation_history.jsonl  (canonical research fact table)
                 |
     +-----------+-----------+-----------+-----------+-----------+
     |           |           |           |           |           |
  shadow_ml  rolling_ml  shadow_cand  market_bm  wide x4   reputation
  predict    predict     arms         snapshot   triggers  shadow journal
     |           |           |           |           |
     +-----------+-----------+-----------+-----------+
                 |
        outcomes (normal-time integrity + revisions)
                 |
        attach to snapshots / wide sqlite / ML retrain wakeups
```

**Offline / worker processes** (spawned with `GOALBOT_LIBRARY_MODE=1`):

- `scripts/train_shadow_ml.py` — static and rolling residual models
- `scripts/discover_wide_rules.py` — four discovery profiles
- `scripts/build_signal_reputation.py`
- `scripts/run_wide_research_cycle.py`, `run_shadow_candidates.py`
- report CLIs: `report_*.py`, `walk_forward_evaluation.py`

Workers copy journal trees into temp dirs for isolation; they must not overwrite live persist.

---

## 3. Production flow

### 3.1 Process and daemons

Entry: `if __name__ == "__main__"` → `setup_logging()` → `main_loop()`.

On import as `__main__` (and not `GOALBOT_LIBRARY_MODE`), the process loads persist files and starts the state-saver daemon. `main_loop` then starts:

| Daemon | Role in production vs research |
|--------|--------------------------------|
| `start_market_benchmark_daemon` | Research: freeze live odds quotes |
| `start_shadow_ml_daemon` | Research: train/load residual models |
| `start_wide_research_daemon` | Research: discovery + lifecycle; primary can promote |
| `start_research_health_monitor` | Research ops |
| `start_monitor_daemon` | Production: edit sent Telegram messages; wide_monitor snapshots |
| `start_callback_handler_daemon` | Production: inline analysis UI |
| `start_review_timeout_daemon` | Production only if `ENABLE_ADMIN_REVIEW_SIGNALS` |
| `start_daily_stats_checker_daemon` | Production: 23:59 report |
| `start_gsheets_labeler_daemon` | Dead: `GSHEETS_AVAILABLE = False` |
| `start_daily_cleanup_daemon` | Ops: 04:00 MSK cleanup |
| `start_state_saver_daemon` | Persist `test_zzz.json` / state |

`start_admin_review_daemon` exists but is **not started** (commented as retired).

### 3.2 Live evaluation (main loop)

1. `GET fixtures?live=all`.
2. Skip ignored / excluded / no-stats / already monitored.
3. Minutes **&lt; 46**: optional rolling-dynamics seed fetches; prefilter observation; no full 45+ send path (unless admin review is on).
4. Minutes **&gt; 60**: skip ordinary evaluation (review can still run if enabled).
5. `collect_match_all` + normalize statistics. Coverage gate at minute 46 (`coverage &lt; 0.85` skip).
6. Legacy `compute_lambda_and_probability` still runs for review-range logging (`prob_goal_either_to75`).
7. Authoritative send path: `compute_probability_45_plus_with_reputation(..., application_context="send")`.
   - Poisson/intensity 45+ model (`lambda_2h`, pressure, shots, xG confidence, game-state).
   - Second-half team/league factors with **soft apply** (`ENABLE_2H_SOFT_APPLY`, capped delta).
   - **Signal reputation auto-apply** (default on): Telegram-cohort calibration of `prob_next_15` / `prob_to90` after unique-fixture readiness. Expanded blend auto-apply is a separate flag (default off in code).
8. Window tags `WINDOW_1` (46–53) and `WINDOW_2` (54–60) still compute **legacy** next-15 / to90 / live-gate / anti-garbage / Rescue scores. Logs mark `legacy_selection_publication_active=false`. They **do not** gate Telegram.
9. First-signal **readiness** snapshot check (`is_first_signal_snapshot_ready`).
10. `validate_match_context_before_send`.
11. **Authoritative gate:** `evaluate_channel_signal_filter`  
    version `base_rep15_int055_season102_p90_75_v1`:
    - `prob_to90 ≥ 75`
    - reputation Δ to90 ≥ 1.5 pp
    - adjusted intensity ≥ 0.55
    - season context factor ≥ 1.02  
    Score is **not** a pass/fail condition; goals ≤ 2 only freeze the **Premium** title.
12. `route_publication_with_wide_research`: if primary production-apply is off or no champion, `allow` = BASE filter. If a champion manifest is active, **the champion replaces the BASE allow** (`allow` = rule PASS, `applied=true`).
13. ALLOW → send Telegram, persist `sent_matches` / tracking, freeze Premium header. BLOCK → still write a decision snapshot.
14. `record_current_decision` always (on persist):
    - `build_decision_snapshot` → `data/decision_snapshots.jsonl`
    - `attach_shadow_reputation` → `data/signal_reputation_shadow.jsonl`
    - `build_observation_from_decision` → `persist_observation_and_score_shadow`

### 3.3 After send (monitor)

`perform_monitor_update` refreshes stats/events, edits the channel message, records score timeline, resolves normal-time outcomes (`outcome_integrity` + `outcome_revision`), and while the match is still in 46–60 writes **`wide_monitor`** observations so research is not censored by the send.

### 3.4 What production does *not* use

- Shadow ML probabilities (static or rolling) — never copied into `res_45`.
- Market implied probabilities.
- Shadow candidate arms.
- Wide-research 4f / precision / rare_precision rules.
- Dynamic to90 buckets and Rescue — shadow/audit only (`ENABLE_RESCUE_SIGNALS` default false; even if on, send path hard-codes `rescue_publication_enabled=false`).

---

## 4. Research flow

Research is **prospective**: a rule or model may only claim the **first** eligible snapshot per fixture (or per phase/rule), with outcomes attached later by observation id. Historical holdout cannot promote a rule to production.

### 4.1 Canonical event: observation history

`persist_observation_and_score_shadow`:

1. Freeze rolling 5/10-minute dynamics on the observation.
2. Rare-precision may freeze a `market_research` quote.
3. Append `data/observation_history.jsonl` (deduped).
4. If the row exists: register rolling baseline, append ML predictions, market decision row, shadow-candidate arms, then (on **new** writes) evaluate four wide-research layers.

Stages:

- `decision_pipeline` — ordinary 46–60 evaluations (ALLOW and BLOCK).
- `wide_monitor` — post-send continued universe.
- Prefilter stages (`pre_46_out_of_window`, `no_normalized_live_metrics`, …) are logged but **out of the research universe**.

Universe contract (`all_technically_eligible_46_60_v1`): stage in `{decision_pipeline, wide_monitor}`, minute 46–60, valid ids/timestamps, readiness + publication-context gates. Telegram ALLOW/BLOCK is **not** part of eligibility.

### 4.2 Shadow ML

- Residual logistic (`residual_logistic_stdlib_v1`) on bot logits for `next15` and `to90`.
- Static champion vs rolling challenger; artifacts in `stats/shadow_ml_model.json` and `stats/shadow_ml_rolling_model.json`.
- Predictions journaled with causal identity (observation id, fixture, minute, timestamps, model created-before-observation).
- Retrain in a subprocess with memory limit; champion compare requires holdout improvement on every target/metric.
- Status `collecting` until span/availability gates (e.g. 56 days, core metric availability). `production_applied` stays false.

### 4.3 Shadow candidates

Hand-specified arms vs the same BASE control (`control_current_filter`, rolling-slice arms, close-score, rolling-ML confirm). Journal `data/shadow_candidates.jsonl` + sqlite index. Shadow-only; used for A/B evidence, not routing.

### 4.4 Market benchmark

Daemon polls live odds, `CausalQuoteCache` freezes a quote **before** Telegram side effects. Journal compared later to bot / static ML / rolling ML logloss. Failures must not change publication.

### 4.5 Wide research (four profiles)

| Profile | Store | Production | Search |
|---------|-------|------------|--------|
| primary | `data/wide_research.sqlite3` | Can champion if `WIDE_RESEARCH_PRODUCTION_APPLY` | Broader conjunction search |
| 4f | `data/wide_research_4f.sqlite3` | Hard shadow | Exact four-factor |
| precision | `data/wide_research_precision.sqlite3` | Hard shadow | High-precision portfolio |
| rare_precision | `data/wide_research_rare_precision.sqlite3` | Hard shadow | Nonlinear/extended + market features; looks 50/100/200 |

Shared mechanics (`wide_research/`):

- Discovery worker joins observations + causal ML predictions; train/val/holdout by fixture; first-trigger bitsets; Holm / Wilson lifecycle (`min_resolved=200`, `min_span_days=28`, `min_wilson_lower=0.90`, target 0.95).
- Live `WideShadowLayer.process_snapshot` claims first trigger into sqlite; outcomes attached via retry spool.
- `WideResearchController.reconcile` may write an active manifest **only** for primary when production_apply and gates pass.
- `ActiveRuleRouter.route` fail-closed: missing/incompatible manifest → current BASE filter.

### 4.6 Signal reputation (dual role)

- **Production:** `ENABLE_SIGNAL_REPUTATION_AUTO_APPLY` (default true) adjusts send/update probabilities from the telegram cohort (and optionally expanded blend).
- **Research:** full projection journal for shadow cohorts; does not by itself change ALLOW except through the adjusted `prob_to90` that BASE consumes.

### 4.7 Walk-forward

Offline engine (`walk_forward/`) replays journals with day-frozen folds. Not on the live path. Reports under `reports/` are generated artifacts.

### 4.8 Second-half history

Separate journal `data/second_half_history.jsonl` → aggregated `stats/team_2h_stats.json` / `league_2h_stats.json` → **does** enter production via 2H factors (soft apply). This is research-derived production input, unlike ML.

---

## 5. Component catalog

| Component | Location | Approx. size | Production effect |
|-----------|----------|--------------|-------------------|
| Live bot, Telegram, persist, daemons, 45+ model | `NanoTest.py` | ~31.8k lines | Yes |
| Analysis UI (deeplink screens) | `telegram_analysis_ui.py` | ~0.4k | Optional display |
| Period / goal scope | `match_period.py` | ~0.3k | Yes (labels, FT vs AET) |
| Normal-time outcome integrity | `outcome_integrity.py` | ~0.1k | Yes (labels) |
| Outcome revision ranks | `outcome_revision.py` | ~0.1k | Yes (corrections) |
| 2H parse/store/aggregate/factors | `second_half/` | ~several k | Soft-apply factors |
| Reputation model + journal | `signal_reputation/` | ~1k | Auto-apply on |
| Shadow ML | `shadow_ml/` | ~several k | Journal only |
| Shadow candidate arms | `shadow_candidates/` | ~1k | Journal only |
| Market quotes | `market_benchmark/` | ~1k | Journal only |
| Wide factory | `wide_research/` | ~12.3k | Primary champion only |
| Walk-forward | `walk_forward/` | ~1.9k | Offline |
| CLI | `scripts/` | ~5.4k | Workers / reports |
| Tests | `tests/` | ~23k | — |
| Docs | `docs/` | — | — |

---

## 6. Dependencies

**Python packages (`requirements.txt`):** `python-dotenv`, `requests`, `urllib3`, `pytest`. No numpy/sklearn; ML is stdlib logistic JSON.

**External systems:**

- API-Football (`v3.football.api-sports.io`) — fixtures, statistics, events, odds.
- Telegram Bot API — publish, edit, pin, callbacks.
- Local filesystem — persist JSON, JSONL journals, sqlite, model JSON.
- Optional tzdata / `ZoneInfo` (fallback fixed UTC+3).

**Internal coupling (high):**

- Almost every research package is imported **at module load** by `NanoTest.py`.
- Tests and several scripts `import NanoTest as bot` (mitigated by `GOALBOT_LIBRARY_MODE` in `tests/conftest.py` and worker env).
- Wide discovery and live scoring share `wide_research.features.extract_features` / `evaluate_universe` (discovery wrapper delegates to the live extractor — good).
- Shadow candidates, market benchmark, and wide router all call `_build_shadow_candidate_prediction_record` for causal ML payloads.

**Internal coupling (intentional isolation):**

- `wide_research.live` has no Telegram import.
- Lifecycle policy is a pure dataclass; frozen per phase.
- ML `save_candidate` forces `shadow_only=true`, `production_applied=false`.

---

## 7. Shared state

### 7.1 In-process

- Global `state` + `state_lock` (sent matches, tracked matches, score timelines, rescue leftovers, ignored lists, …).
- Cached ML / reputation models, wide-research component singletons, market quote cache, rolling dynamics trackers.
- Thread pools: live loop, monitor (8 workers), rolling seed, outcome reconcile, research workers as subprocesses.

### 7.2 On disk (runtime — not source)

| Path | Owner | Writers |
|------|--------|---------|
| `test_zzz.json/persist_*.json` | Bot persist | Main + state saver |
| `test_bot_state.json` | Bot state | Main |
| `data/observation_history.jsonl` (+ gz, reconcile sqlite) | Research spine | Main loop + monitor |
| `data/decision_snapshots.jsonl` | Audit of gates | Main loop |
| `data/shadow_ml*_predictions.jsonl` | ML | Predict path + train worker |
| `data/shadow_candidates.jsonl` + index sqlite | Candidates | Main |
| `data/market_benchmark.jsonl*` | Market | Daemon + capture |
| `data/signal_reputation_shadow.jsonl` | Reputation shadow | Main |
| `data/second_half_history.jsonl` | 2H | Collector / backfill |
| `data/wide_research*.sqlite3` (+ wal/shm/retry) | Four profiles | Live layer + discovery import + lifecycle |
| `stats/*.json` | Models, discovery, 2H aggregates, health | Daemons / scripts |
| `reports/` | Generated | Scripts / discovery |
| `*.log` | Logging | Process |

**Contention:** the live loop, monitor thread pool, market daemon, ML trainer subprocess, and four wide-research sqlite files all touch overlapping fixture ids. Journals are append-only with locks; sqlite uses WAL. Outcome corrections must update snapshots, ML labels, candidates, and four stores without changing frozen first-trigger identities.

### 7.3 Dual outcome worlds

1. **Legacy training JSONL** in `PERSIST_DIR` (`test_match_snapshots.jsonl` / `test_match_outcomes.jsonl`) — signal-centric, `_training_jsonl` flag.
2. **Decision/observation outcomes** in `data/` with schema versions, revision ranks, and wide-research attach.

Both are fed from monitor/event classification. Consumers must not mix ids casually (`signal_id` vs `observation_id` / `decision_id`).

---

## 8. Duplication

| Duplicate | Where | Risk |
|-----------|--------|------|
| Append-only JSONL + gzip rotate + lock | `NanoTest._append_jsonl_record`, `shadow_ml.storage`, `shadow_candidates.storage`, `market_benchmark.storage`, `signal_reputation.storage` | Drift in rotate/dedupe semantics |
| Feature extraction | Live `wide_research.features` vs discovery wrapper (thin — OK); extra flattening in `NanoTest` observation builder | Observation schema drift vs extractor allowlist |
| Channel filter vs shadow control arm | `evaluate_channel_signal_filter` vs `shadow_candidates.rules` control | Tests exist; version string must stay aligned |
| Probability horizons | `prob_to90`, `prob_second_half_remain`, `prob_next_25`, `prob_until_end_decision` aliases | Easy to log the wrong field |
| Four near-identical wide-research component getters / daemon loops | `NanoTest.py` | Bugfix applied to one profile only |
| Persist vs observation journals | `test_zzz.json` vs `data/` | Two sources of “what happened” |
| Legacy vs BASE gates | Full WINDOW_1/2 live-gate/Rescue still computed every cycle | CPU + confusion if logs are read as production |

---

## 9. Legacy and dead code

**Dead or inert at runtime**

- Google Sheets: `GSHEETS_AVAILABLE = False`; init/labeler/finalize still compiled and daemons still started.
- `start_admin_review_daemon` / review message editing — commented unused.
- Rescue publication and dynamic to90 **as send gates** — retired; functions remain for tests and shadow logs.
- Early strict mode 25–30 — not on the ordinary 46–60 send path (review-only remnant).
- Mid-loop `compute_lambda_and_probability` (to75) — not the 45+ send model.
- `ENABLE_DYNAMIC_PROB_TO90_THRESHOLD` env still documented in `.env.example` though it cannot publish.

**Alive but legacy-shaped**

- `NanoTest.py` still named “test bot”; persist dir `test_zzz.json`.
- `NORMAL_TIME_BOUNDARY_MODE=legacy` still selectable.
- Training JSONL snapshot/outcome pair predates decision snapshots.
- Premium / Rescue title branches remain in formatters.

**Not dead**

- Shadow Rescue scoring and legacy gate fields inside snapshots — required for comparability and walk-forward.

---

## 10. Principal risks

### P0 — Security

- **API-Football key is hardcoded** in `NanoTest.py` (`API_FOOTBALL_KEY = "..."`), not only in `.env`. Source baseline still contains a live credential. Telegram tokens were removed from the tree but remain in **git history**.
- `config_test.json` stores an admin user id.

### P0 — Production override path

- If `WIDE_RESEARCH_PRODUCTION_APPLY=true` **and** a champion manifest is written, `ActiveRuleRouter` **replaces** the BASE filter (champion FAIL blocks even a BASE pass; champion PASS publishes even if BASE would fail). 4f/precision/rare cannot do this; primary can.
- Live `.env` has historically set `WIDE_RESEARCH_PRODUCTION_APPLY=true` while code default is `False`. No champion today, so behavior is BASE-only, but the switch is armed.

### P1 — God object and import cost

- Importing `NanoTest` loads every research stack, env, and constants. Scripts/tests depend on `GOALBOT_LIBRARY_MODE` to avoid clobbering persist. A missed flag can overwrite live `persist_state.json`.

### P1 — Memory and disk

- Four sqlite research DBs, observation JSONL + gz rotations, reconcile sqlite (~GB), ML journals. Full `report_shadow_ml.py` on complete journals has been OOM-killed. Discovery workers copy journal trees and cap RSS (`WIDE_RESEARCH_WORKER_MEMORY_LIMIT_MB`).
- WAL/shm/retry spools: unclean process kill can stall lifecycle (`retry_spool` / health DEFERRED).

### P1 — Shared-state races

- Main loop and monitor both write observations, timelines, and Telegram. Outcome corrections must not move first-trigger minutes; the store is designed for that, but bugs here silently bias hit rates.

### P2 — Causality / leakage (mitigated, still a review surface)

- ML join requires exact observation identity and max 300s lag; missing ML **fails** rules that need it (good).
- Reputation auto-apply **does** change production probabilities; expanded blend auto-apply is a second production lever (`ENABLE_SIGNAL_REPUTATION_EXPANDED_AUTO_APPLY`).
- 2H stats aggregated from history **do** move lambda (soft-capped). A bad aggregate is a silent production change unlike ML.

### P2 — Operational complexity

- Four weekly discovery jobs + lifecycle + health + ML retrain + market daemon on one host.
- Holdout 90% hit rates in discovery JSON are **not** lifecycle proof (Holm / Wilson / 200 / 28d still fail).
- Dual logging of “would pass dynamic threshold” can be misread as the live rule.

### P2 — Test surface vs runtime

- Strong package tests (lifecycle, causality, fail-closed). Production path still lives in one huge function (`main_loop` inner fixture loop) that is hard to test as a unit.

---

## 11. Architectural judgment

**What is sound**

- Clear split: BASE filter is the live product; research is prospective and mostly shadow.
- Causal contracts (first trigger, observation-id join, frozen manifests, `production_applied=false`).
- Outcome revisions with ranks instead of silent overwrite.
- Worker isolation via `GOALBOT_LIBRARY_MODE` and temp journal copies.
- Universe wider than sent signals (`wide_monitor`) avoids selection bias.

**What is strained**

- New research was extracted into packages, but **orchestration, model, Telegram, and persist remain one file**.
- Four cloned wide-research profiles multiply state and daemon complexity for one product gate.
- Legacy gates and Sheets code inflate the production file and the cognitive load of logs.

**Effective production contract today**

```
API live stats
  → 45+ Poisson/intensity + 2H soft factors
  → reputation auto-apply (telegram cohort)
  → BASE p90/reputation-delta/intensity/season gate
  → Telegram
```

Research watches that stream. It should not publish unless a primary champion is explicitly promoted **and** `WIDE_RESEARCH_PRODUCTION_APPLY` is on.

---

## 12. Suggested follow-ups (not done in this audit)

These are recommendations only; nothing was implemented.

1. Remove the hardcoded API key; load only from environment. Rotate the key and any tokens still in git history.
2. Keep `WIDE_RESEARCH_PRODUCTION_APPLY` false until a champion actually passes lifecycle; treat “replace BASE” as an explicit product decision, or change the router to AND with BASE.
3. Split `main_loop` send path from research glue (observation persist already almost is a facade).
4. Deduplicate JSONL journals into one storage helper.
5. Delete or isolate Google Sheets and unused review daemons.
6. Cap report jobs so they never scan full gz history in-process (already a known OOM).
7. Document env-vs-code defaults (`PRODUCTION_APPLY`, expanded reputation) in an operator runbook.

---

## 13. File index for readers

- Production gate: `NanoTest.py` `evaluate_channel_signal_filter`, `main_loop`, `compute_probability_45_plus_with_reputation`
- Champion hook: `NanoTest.py` `route_publication_with_wide_research`; `wide_research/live.py` `ActiveRuleRouter`
- Observation spine: `persist_observation_and_score_shadow`
- Wide factory: `wide_research/{discovery,features,store,controller,lifecycle,live}.py`
- ML: `shadow_ml/`, `docs/shadow_ml.md`
- Candidates: `shadow_candidates/`, `docs/shadow_candidates.md`
- Market: `market_benchmark/`, `docs/MARKET_BENCHMARK.md`
- Reputation: `signal_reputation/`, `docs/signal_reputation_shadow.md`
- Outcomes: `outcome_integrity.py`, `outcome_revision.py`, `docs/OUTCOME_REVISIONS.md`
- Wide research product doc: `docs/WIDE_RESEARCH.md`
