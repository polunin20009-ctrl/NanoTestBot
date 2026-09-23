# Functional contract — current system

Date: 2026-09-19  
Source: live code and package docs. Application logic was not changed.

This document is the **capability contract**: what production and research can do *now*, what they refuse to do, and which flags turn a capability on. It is not a design proposal.

Convention: **Production** = anything that can change Telegram publication, live message text, or the inputs used by `balanced-two-rule`. **Research** = observe, score, train, discover, report; default `production_applied=false` / `shadow_only=true`.

---

## 1. Product identity

The running product is a **Goal Predictor Telegram Bot** (`NanoTest.py`).

It watches **live football fixtures** from API-Football, computes a **second-half (minutes 46–60) probability that another goal arrives before the end of normal time**, and may **publish one ordinary channel signal per fixture** when the immutable `balanced-two-rule` portfolio passes.

A parallel **research factory** records every technically eligible snapshot, residual ML, market quotes, hand-written candidate arms, and four wide-research rule profiles. Historical search and primary wide-research champions no longer publish. The BASE filter is retained as an audit/control feature but is not the Telegram gate.

CLI besides the bot: `python3 NanoTest.py --dump-match <fixture_id>` dumps collected metrics and exits.

---

## 2. Shared facts (both systems)

### 2.1 Time and match clock

- Operator calendar for daily reports: **Europe/Moscow** (`MSK`), with fallback UTC+3.
- Decision window for ordinary signals: **elapsed minute 46–60 inclusive**.
- Sub-windows (logging / legacy only): WINDOW_1 = 46–53, WINDOW_2 = 54–60.
- Polling: live loop and monitor default **60 s**.
- Target label for research and market: **`goal_to90_normal_time`** — at least one additional **normal-time** goal after the snapshot, **excluding extra time and penalty shootouts**.
- Secondary labels: goal in next 15 minutes, next 25 minutes, before minute 75 (training JSONL / reputation / ML `next15`).
- Outcome quality: `exact` / `score_confirmed` / `inferred`; inferred `next15` may be down-weighted (ML weight 0.5).
- Outcome journals are **append-only revisions** (`outcome_revision` ≥ 1). Readers rank by schema version, then revision, then `created_at_utc`. Corrections change labels, not Telegram history.

### 2.2 Identity

- Fixture: API-Football `fixture_id`.
- Decision: `decision_id` ≈ `{fixture}:{minute}:{WINDOW_1|WINDOW_2}:v{schema}`.
- Observation: `observation_id` / `observation_key` (canonical research join key).
- Signal (legacy training JSONL): `signal_id`.
- One ordinary **channel send per fixture** (`state.sent_matches`). Later minutes update the same message; they do not send a second ordinary signal.

### 2.3 Data sources

- API-Football: live fixtures, statistics, events, league/team context, **odds/live** (research).
- Persist: `PERSIST_DIR` (default `test_zzz.json`) — leagues, teams, matches, core state.
- Bot state file: `STATE_FILE` (default `test_bot_state.json`).
- Research journals: `data/*.jsonl` (+ gzip rotate), sqlite indexes, `stats/*.json`.

---

## 3. Production contract

### 3.1 What production **can** do

1. **Ingest live matches** (`fixtures?live=all`), discover leagues, cache league/team stats, refresh stale/cup leagues in a bounded queue.
2. **Skip** ignored, excluded, no-stats, already-monitored, and too-early (`< MATCH_MIN_MINUTE`, default 30) fixtures cheaply.
3. **Collect full match packets** (`collect_match_all`): fixture, stats, events; recover identity if the fixture endpoint is thin.
4. **Normalize live metrics** (xG with fallback, shots, box shots, saves, corners, possession, cards, attacks, score).
5. **Seed rolling 5/10-minute dynamics** before minute 46 (shadow collection for later research; extra API fetches bounded).
6. **Compute the 45+ model** (`compute_probability_45_plus`):
   - intensity `lambda_2h` from xG (confidence-weighted), shots on target, box shots, pressure, save-stress, tempo;
   - Poisson-style horizons: `prob_next_15`, `prob_next_25`, `prob_to75`, `prob_to90` (%);
   - `prob_until_end_decision` = `prob_to90`;
   - `prob_second_half_remain` is a **legacy alias** of `prob_next_25`, not the publication metric.
7. **Apply second-half context factors** from `stats/team_2h_stats.json` and `stats/league_2h_stats.json` (and score-state). With `ENABLE_2H_SOFT_APPLY` (default true) the probability move is **capped** (`SOFT_APPLY_MAX_ANTIBOOST_PCT` 4 pp default, `SOFT_APPLY_MAX_BOOST_PCT` 5 pp).
8. **Apply signal reputation** to send/update probabilities when `ENABLE_SIGNAL_REPUTATION_AUTO_APPLY` (default true):
   - production cohort = `telegram_signals`, unless expanded auto-apply is on;
   - unique-fixture staged caps: 50 / 150 / 300 fixtures → ±1 / ±2 / ±3 pp;
   - half-life 90 days; hierarchical priors (global/league/team/…);
   - not applied after normal time has finished or in extra time;
   - expanded blend (`ENABLE_SIGNAL_REPUTATION_EXPANDED_AUTO_APPLY`, **default false**) can mix telegram + other cohorts with configured weights; evaluation failure **falls back** to telegram cohort.
9. **Gate first snapshot quality** (`is_first_signal_snapshot_ready`) only before the first ordinary send: reject HT/1H, not-yet-2H, empty 2H start, post-goal xG lag, all-zero mid-game stats, extreme probability with weak live metrics.
10. **Require publication context** (`validate_match_context_before_send`): real team names, league name/id, team ids, non-default league/team factor source.
11. **Evaluate the legacy BASE control** `base_rep15_int055_season102_p90_75_v1` (`evaluate_channel_signal_filter`) for snapshots, comparison, and research:

    | Condition | Default threshold | Env override |
    |-----------|-------------------|--------------|
    | `prob_to90` | ≥ 75 | `BASE_SIGNAL_MIN_PROB_TO90` |
    | reputation Δ to90 (adjusted − base) | ≥ 1.5 pp | `BASE_SIGNAL_MIN_REPUTATION_DELTA_TO90_PP` |
    | `adjusted_intensity` | ≥ 0.55 | `BASE_SIGNAL_MIN_ADJUSTED_INTENSITY` |
    | `season_context_factor` | ≥ 1.02 | `BASE_SIGNAL_MIN_SEASON_CONTEXT_FACTOR` |

    Invalid/non-finite inputs fail closed (`passed=false`). Its result is recorded but does not allow or block Telegram publication.
12. **Apply the only production gate: frozen `balanced-two-rule` v1.** It is an OR portfolio: at least one of its two immutable members must PASS. Missing features, prediction errors, market unavailability affecting a required member, or evaluation exceptions cannot create a signal; if neither member passes, publication is blocked. The rule ignores the BASE result.
    - `wide-1f89b379f5d04f3ceb3f`: `prob_to90≤79`, away corner share `≥0.5`, home-away total-shot balance `≤-0.076923`, box-shots × causal static-ML p90 `≥0.228077`, trailing-team SOT share `≥0.333333`.
    - `wide-0bed4a7f285d9f108840`: causal market p90 × season factor `≥0.832204` and 10-minute pressure delta `≥1.82`.
13. **Publish to Telegram** (Markdown, rate-limited edits):
    - every ordinary signal uses the title «Сигнал»; the Premium title/badge no longer exists;
    - live score, minute, `prob_to90` (and next-15 display), xG, shots, saves, box, corners, possession;
    - optional analysis UI (`ENABLE_SIGNAL_ANALYSIS_UI`, default **false**): private deeplink / snapshot screens (overview, live, teams, season, 2h, technical);
    - edit the same message as the match progresses; freeze the **header/title** at send;
    - final badge when the match is fully finished (FT/AET/PEN/…);
    - pin rotation for the daily stats message; never unpin the permanent instruction message;
    - send a **permanent instruction** message once per channel lifetime.
14. **Monitor sent fixtures** every `MONITOR_INTERVAL`: refresh stats/events, edit channel text, record score timeline, collect 2H history on finish, resolve outcomes, write `wide_monitor` observations while still 46–60. For fixtures already sent with `approved_by_admin` in persisted state, the frozen header may still display «Сигнал от админа» (historical admin-approved sends only; **Admin Review is not a live path**).
15. **Resolve outcomes at the normal-time boundary** without waiting for AET (`process_normal_time_outcomes_for_jsonl` + `resolve_normal_time_outcome`). Recheck a bounded recent cohort for API corrections (`ENABLE_OUTCOME_CORRECTION_RECHECK`, default 30 min / 168 h lookback / 5 fixtures).
16. **Persist** decision snapshots, observation history, training JSONL (if `_training_jsonl`), score timelines, sent/tracked maps. Periodic state save (`SAVE_INTERVAL` 30 s). Legacy keys such as `review_queue` / `admin_reviews` may remain in loaded state files and are tolerated on load; they are not written by current production logic.
17. **Daily MSK report** near 23:59: counts of sent signals and results for that Moscow date; pin as the stats message.
18. **Maintenance** (if `ENABLE_CLEANUP`): 04:00 MSK bounded cleanup of expired no-stats / persist matches.
19. **Memory telemetry** (RSS counters, no journal scans) on an interval.
20. **League/team factor refresh** from API samples (TTL, cup heuristic, clamps).

### 3.2 Production windows and skips

| Situation | Production behavior |
|-----------|---------------------|
| Minute &lt; 46 | No ordinary send; may seed rolling dynamics; prefilter observation |
| Minute 46, coverage &lt; 0.85 | Skip this cycle (wait for stats) |
| Minute 46–60, `balanced-two-rule` FAIL/UNAVAILABLE | No send; **decision snapshot still written** |
| Minute 46–60, `balanced-two-rule` PASS | Send, regardless of BASE result |
| Minute &gt; 60, already sent | Monitor updates only |
| Duplicate fixture already in `sent_matches` | No second ordinary send |
| Missing stats / unknown fixture id | Long no-stats block |
| Telegram token/chat missing | Bot continues; sends fail closed |

### 3.3 What production **cannot** do (hard)

- Publish from **dynamic minute-bucket to90 thresholds** (46–49: 82, … 58–60: 79). Computed and logged only (`legacy_selection_publication_active=false`).
- Publish from **WINDOW_1/WINDOW_2 next-15 + remain + live-gate + anti-garbage**. Shadow comparison only.
- Publish from **Rescue** even if `ENABLE_RESCUE_SIGNALS=true`. Send path forces `rescue_publication_enabled=false`.
- Copy **static or rolling ML** probabilities into `res_45`, thresholds, or Telegram text.
- Use **shadow candidate arms** or **4f / precision / rare_precision** rules for publication.
- Use a primary wide-research champion or BASE result for publication.
- Promote a wide-research rule from **holdout hit rate** alone.
- Send Google Sheets rows (`GSHEETS_AVAILABLE=false`).
- Treat extra-time / shootout goals as `goal_to90_normal_time` (strict mode default).
- Confirm goals with `GOAL_CONFIRM_ACTIVE` (hardcoded **false**; delay constant unused).

`WIDE_RESEARCH_PRODUCTION_APPLY` and an active primary champion no longer affect Telegram publication.

---

## 4. Research contract

Every research layer below is **fail-closed**: missing features, missing ML, or schema mismatch → UNAVAILABLE / skip, never silent impute into a pass.

### 4.1 Observation spine (shared input)

**Can:**

- Append `data/observation_history.jsonl` for `decision_pipeline` and `wide_monitor` (and prefilter rows that are **out of universe**).
- Freeze rolling 5/10-minute windows on the observation (`ENABLE_ROLLING_DYNAMICS_SHADOW`, default true).
- Freeze a rare-precision `market_research` quote on the same row when that profile is enabled.
- Deduplicate; rotate at size cap; gzip archives; optional reconcile sqlite.

**Universe `all_technically_eligible_46_60_v1`:** stage ∈ `{decision_pipeline, wide_monitor}`, minute 46–60, valid fixture/observation/timezone, readiness passed, publication-context passed. **Ignores** BASE ALLOW/BLOCK and whether Telegram was sent.

`decision_pipeline` = ordinary 46–60 evaluations.  
`wide_monitor` = post-send continued snapshots so a send does not censor later minutes.

### 4.2 Decision snapshots (audit)

**Can:** write one `decision` per fixture/minute/window/schema and later `outcome` rows to `data/decision_snapshots.jsonl`. Embed gates, BASE filter, legacy selection, telegram result, reputation summary, wide-router decision. Join by `decision_id`. Schema ≥ 3 by default.

### 4.3 Signal reputation (shadow journal)

**Can:**

- Rebuild `stats/signal_reputation.json` (isolated subprocess, default every 15 min / min rebuild 5 min).
- Append full projections to `data/signal_reputation_shadow.jsonl` (`shadow_only=true`) for telegram and expanded-blend cohorts.
- Report join of projection + later outcome (`scripts/report_signal_reputation.py`, `scripts/build_signal_reputation.py`).

**Cannot:** change publication except through the **production auto-apply** path in §3.1.8.

### 4.4 Shadow ML

**Can:**

- Train residual logistic `residual_logistic_stdlib_v1` (static) and `residual_logistic_stdlib_rolling_v2` (rolling 5/10 features) with **stdlib only**.
- Targets: `next15`, `to90`. Bot probability is a frozen logit offset; model learns a residual.
- Auto-retrain in a memory-capped subprocess (`SHADOW_ML_AUTO_RETRAIN` / rolling equivalent, default true).
- Write `stats/shadow_ml_model.json`, `stats/shadow_ml_rolling_model.json`, prediction journals.
- Champion vs candidate: promote artifact only if holdout improves **every** target/metric (logloss min +0.001, brier); always `production_applied=false`.
- Offline-ready gates (per target, then both): ≥500 fixtures, ≥3000 rows, ≥56 day span, ≥75 holdout/calibration fixtures, core metric availability ≥ 0.85, next15 unknown fraction ≤ 0.02, class balance floors. Until then status **`collecting`**.
- Train only on `decision_pipeline` 46–60 resolved rows; ALLOW and BLOCK both kept; no outcome/Telegram/league-id features.
- Causal live predict: exact observation identity; model created before observation; append lag ≤ 300 s.

**Production boundary:** static ML may affect publication only through the
source-controlled `balanced-two-rule` member described in §3.1.12. It is never
copied into displayed probabilities. Missing model data makes that member
UNAVAILABLE and cannot create a pass.

CLI: `scripts/train_shadow_ml.py`, `scripts/report_shadow_ml.py`.

### 4.5 Rolling dynamics (shadow collection)

**Can:** track per-fixture 5- and 10-minute deltas/rates for xG, SOT, box shots, shots, corners, pressure; seed extra fetches at ~36/39/41/44; persist on observations. Schema `ROLLING_DYNAMICS_SCHEMA_VERSION`.

**Cannot:** alter BASE. A rule that **requires** a window fails if the window is not `ok`.

### 4.6 Shadow candidates

Ruleset `candidate_at_first_control_allow_v3`.

**Can:** on each observation, evaluate five arms and freeze **the first snapshot where control passes** for all arms:

| Arm | Meaning |
|-----|---------|
| `control_current_filter` | Same BASE thresholds + publication context |
| `full_slices_5m_10m` | Control + both rolling windows `status=ok` |
| `full_slices_goals_le2` | Previous + goals ≤ 2 |
| `full_slices_goals_le2_close` | Previous + `|home−away| ≤ 1` |
| `rolling_ml_confirm_75` | `goals_le2` + causal rolling ML calibrated to90 ≥ 75% |

Failed/unavailable derived arm at that freeze is **permanent** for the fixture/ruleset. Journal `data/shadow_candidates.jsonl` + sqlite index. `prospective_start_utc` is mandatory.

**Cannot:** send Telegram, train ML, or change BASE.

CLI: `scripts/run_shadow_candidates.py`, `scripts/report_shadow_candidates.py`.

### 4.7 Market benchmark

**Can:**

- Poll `GET /odds/live` (~60 s) on a **separate** thread from the send path.
- Keep quotes for Match Goals / O-U, **line = current total goals + 0.5**, no-vig probability of ≥1 more normal-time goal.
- Record raw quotes minutes 35–90; for 46–60 observations freeze bot p90, static ML p90, rolling ML p90, and the newest quote **already known** at observation time.
- For publication decisions: fsync market evidence **before** Telegram HTTP; append delivery result after.
- Reject suspended/stale/wrong-line quotes. Provider = API-Football (no bookmaker id).
- Report logloss vs bot / static ML / rolling ML (`scripts/report_market_benchmark.py`).

**Cannot:** change probability, badge, or allow/block.

### 4.8 Wide research — four profiles

All profiles: conjunction DSL (`>=`, `<=`, `==`) on an allowlisted numeric feature set; UNAVAILABLE cannot PASS; first trigger per `(phase, rule, fixture)`; outcome `goal_to90_normal_time`; pending does not close a look; invalid/void counts as **loss** for READY; corrections cannot unfreeze the fixture list.

**Discovery (isolated subprocess, journal snapshot copy):**

- Join observations + causal static/rolling predictions.
- Fixture-grouped chronological split (default 60/20/20 train/val/holdout).
- Thresholds/quantiles from **train only**.
- Bounded beam search; validation selects a shortlist; identities frozen **before** holdout.
- Holdout: one-sided binomial vs 90% + Holm. Stored as diagnostics (`historical_metrics_are_evidence=false`) — **not READY**.
- Optional temporal purge / embargo (`WIDE_RESEARCH_TEMPORAL_PURGE`).

| Profile | Default role | Search character | Lifecycle looks | Can become lifecycle champion |
|---------|--------------|------------------|-----------------|---------------------|
| primary | `ENABLE_WIDE_RESEARCH` | up to 3 clauses typical; max 10 shadow rules | 50…1000 incl. 200 | **Yes**, as research state only |
| 4f | four-factor | exact depth 4; beam 32; budget 40k | same family | **No** (hard shadow) |
| precision | high-precision | up to 8 clauses; portfolio; frequency bands | same | **No** |
| rare_precision | rare high precision | extended/nonlinear + market features | **50, 100, 200** | **No** |

**Live:** `WideShadowLayer` claims first matching snapshot into sqlite; retry spool for crashes; health pending/DEFERRED.

**Lifecycle (primary & precision/rare reconcile; 4f discovery-only pool):** `LifecyclePolicy` defaults:

- target hit rate **95%**, null **90%**, Wilson lower **≥ 90%**
- **200** resolved in the closed look, **28** span days, **14** trigger-days
- ≥ 8 leagues, max league share 35%, ≥ 4 stable weekly windows, ≥ 2 triggers/week
- Holm α = 0.05
- degradation: fast n=50 rate 0.80; slow n=150 vs null 0.90

Phases: candidate → shadow → ready → active/champion; also paused/degraded. This lifecycle state does not grant Telegram publication authority.

The historical primary champion router is not called by `main_loop`.

CLI: `scripts/discover_wide_rules.py`, `scripts/run_wide_research_cycle.py`, `scripts/report_wide_research.py`.

### 4.9 Research health

**Can:** snapshot observations/outcomes/discovery/pools/retries/disk; warn vs critical with hysteresis; persist `stats/research_health.json` and alert state. **Cannot** score rules or promote.

### 4.10 Second-half history (feeds production factors)

**Can:** one normal-time analytics record per finished fixture (`second_half_history` schema v2: no ET/shootout/disallowed). Aggregate last 20 team / 200 league matches into JSON stats. Backfill/migrate/aggregate CLIs.

**Does** affect production via 2H soft-apply (capped). It is research-derived **input**, not a publication rule.

### 4.11 Walk-forward (offline only)

**Can:** MSK day folds; train fold-local static/rolling **only** on observations and outcomes strictly before cutoff; hide test outcomes; evaluate control / both_windows / goals≤2 / ml_confirm. Modes: fold-local train, `journal-replay`, `recomputed-historical` (labelled hypothetical).

**Cannot:** import live `NanoTest` side effects, write live journals, or replace production artifacts.

CLI: `scripts/walk_forward_evaluation.py`.

### 4.12 Other research CLIs

| Script | Capability |
|--------|------------|
| `export_decision_snapshots.py` | Compact/joined export from snapshots |
| `migrate_outcome_integrity.py` | One-shot outcome schema migration |
| `migrate_second_half_history.py` | 2H schema migration |
| `backfill_second_half_history.py` | Historical 2H fill |
| `aggregate_second_half_stats.py` | Rebuild team/league JSON |
| `backtest_second_half.py` | Offline 2H factor backtest |

---

## 5. Capability matrix

| Capability | Production effect | Research record |
|------------|-------------------|-----------------|
| 45+ Poisson/intensity + 2H soft-apply | Yes (probabilities) | Frozen on observations |
| Reputation auto-apply (telegram) | Yes (probabilities) | Shadow journal |
| Reputation expanded auto-apply | Optional (default off) | Shadow journal (default on) |
| BASE `p90≥75` + Δrep + intensity + season | Audit/control only | Copied into snapshots / control arm |
| Frozen `balanced-two-rule` | **Only publication gate** | Frozen portfolio sqlite |
| Wide primary champion | None | sqlite + manifest |
| Wide 4f / precision / rare | None | sqlite + reports |
| Shadow ML static/rolling | Static p90 is one input to `balanced-two-rule`; rolling ML has no production effect | artifacts + prediction JSONL |
| Shadow candidate arms | None | JSONL |
| Market O/U +0.5 | Causal market p90 is one input to `balanced-two-rule`; not displayed | JSONL |
| Dynamic to90 / Rescue / live-gate | None | Snapshot `legacy_selection_gates` |
| Decision snapshots | Audit (not a gate) | JSONL |
| Observation history | Input to all research | JSONL |
| Outcome revisions | Labels only | All consumers |
| Telegram send/edit/pin/daily | Yes | telegram block on snapshot |
| Analysis UI | Optional display | — |
| Google Sheets | Disabled | — |
| Walk-forward | None | `reports/` |
| Health monitor | Alerts only | `stats/research_health.json` |

---

## 6. Default feature flags (code)

On unless noted:

- Observation history, decision snapshots, outcome recheck
- Shadow ML + rolling challenger + auto-retrain
- Rolling dynamics shadow
- Market benchmark
- Shadow candidates
- Wide research ×4 + auto discovery; primary/precision/rare auto lifecycle
- Research health
- Reputation shadow + auto-apply; expanded **shadow on**, expanded **auto-apply off**
- 2H collection, aggregate, factors, soft-apply
- Dynamic to90 **logging** on; Rescue **off**; analysis UI **off**; cleanup **off**
- Wide production-apply flag is ignored by Telegram routing
- 4f/precision/rare **hard shadow** regardless of env

---

## 7. Outputs operators can read

| Output | Meaning |
|--------|---------|
| Telegram channel message | Live product |
| `data/decision_snapshots.jsonl` | Why ALLOW/BLOCK |
| `data/observation_history.jsonl` | Research fact table |
| `stats/shadow_ml*.json` | Residual model + `collecting`/`offline_ready` |
| `data/shadow_ml*_predictions.jsonl` | Causal ML scores |
| `data/shadow_candidates.jsonl` | Five-arm freeze |
| `data/market_benchmark.jsonl` | Odds vs bot/ML |
| `data/wide_research*.sqlite3` | Prospective triggers |
| `stats/wide_research*_discovery.json` | Last search (not READY) |
| `stats/wide_research*_active.json` | Champion pointer (empty = none) |
| `reports/wide_research*/` | Immutable discovery artifacts |
| `reports/walk_forward_*` | Offline fold reports |
| `stats/research_health.json` | Ops, not hit-rate claims |
| `test_goal_predictor.log` | Runtime log |

---

## 8. Contract summary

**Production is allowed to:** score 46–60 live matches with the 45+ model, nudge probabilities with capped 2H factors and staged telegram reputation, publish **at most one** channel signal per fixture only when frozen `balanced-two-rule` passes, update that message until full time, label normal-time outcomes, and emit a daily MSK digest.

**Research is allowed to:** record the broader eligible universe, train shadow residuals, freeze candidate arms, store market quotes, search and prospectively validate four rule families, and report walk-forward/health/logloss. It does not publish.

**Neither system is allowed to:** treat holdout 90% as production proof, use ET/PEN goals as to90 wins, impute missing ML/windows as passes, or let Sheets/Rescue/dynamic thresholds send to the channel.
