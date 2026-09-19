# Correctness findings

Date: 2026-09-19  
Constraint: **read-only**. No production or test logic was changed for this audit.  
Scope: live publication, BASE/router, Telegram/state, outcomes/2H retry, reputation/wide-research glue, second-half parser fallbacks. Primary file: `NanoTest.py` plus `wide_research/`, `outcome_integrity.py`, `second_half/`, `signal_reputation/`.  
Related: `docs/FUNCTIONAL_CONTRACT.md`, `docs/ARCHITECTURE_AUDIT.md`, `tests/test_production_flow_characterization.py` (`PRODUCTION_FLOW_KNOWN_GAPS`).

Severity:

| Level | Meaning |
|-------|---------|
| **critical** | Condition is always false or always wrong under current constants; a named feature cannot work. |
| **high** | Can publish the wrong signal, attach the wrong outcome, or permanently drop retries / 2H history. |
| **medium** | Silent fallback, dual-path drift, or state hole that is wrong in edge/restart cases. |
| **low** | Dead config, duplicate definitions, misleading docs/logs; low runtime impact. |

**Fix without product policy** means: can be corrected as a bug/hardening while keeping the same publication rule (BASE four conditions, one ordinary send per fixture, champion-replace-when-armed, shadow-only research). Changing who is allowed to send, or AND-ing champion with BASE, is a policy change.

---

## Summary

| ID | Severity | Topic | Fix without policy? |
|----|----------|--------|---------------------|
| C1 | critical | Admin-review band `80 ≤ p < 30` is unreachable | Yes |
| H1 | high | Champion **replaces** BASE allow (armed risk) | No (documented policy) |
| H2 | high | Champion/router errors **fail open** to BASE allow | Yes |
| H3 | high | Active manifest cache stays on `_last_good` after delete/invalid | Yes |
| H4 | high | Admin `publish_signal_to_channel` skips readiness, router, snapshots | Partial |
| H5 | high | Decision snapshot records `ALLOW` even when Telegram send fails | Yes |
| H6 | high | Ordinary send is not gated on `sent_matches` | Yes |
| H7 | high | 2H store `False` / generic exception does not schedule retry | Yes |
| H8 | high | `_prune_second_half_incomplete_retries` drops backoff when `collectable_2h` empties | Yes |
| H9 | high | `previously_confirmed_win` never passed into integrity | Yes |
| H10 | high | Hardcoded API-Football key | Yes |
| M1 | medium | Dual probability models (lambda to75 vs 45+ to90) | Partial |
| M2 | medium | `prob_second_half_remain` alias of `prob_next_25`, used as to90 fallback | Yes |
| M3 | medium | Reputation Δ / season ≥1.02 starve send if reputation cold/off | No (relaxing = policy) |
| M4 | medium | Expanded reputation silently falls back to telegram and still applies | Yes |
| M5 | medium | Expanded blend stages on `all_decisions` fixture count | Yes |
| M6 | medium | `match_sent_at` not written inside `send_to_telegram`; restart bootstrap | Yes |
| M7 | medium | In-memory 2H/outcome schedulers not restored on restart | Yes |
| M8 | medium | Outcome correction sweep max 5 fixtures / 30 min | Yes (ops) |
| M9 | medium | `collectable_2h` pending_only vs cutoff asymmetry | Yes |
| M10 | medium | Second-half parser missing FT → 0–0 | Yes |
| M11 | medium | xG estimated fallback feeds BASE intensity | Partial |
| M12 | medium | Coverage gate 0.85 vs `FALLBACK_MIN_COVERAGE` 0.35; incomplete key aliases | Yes |
| M13 | medium | Premium badge still requires BASE pass after champion ALLOW | Partial |
| M14 | medium | Shadow-ML load does not require `shadow_only is True` | Yes |
| M15 | medium | Observation id fallback to `decision_id` | Yes |
| M16 | medium | `ENABLE_RESCUE_SIGNALS` / dynamic to90 look live but do not publish | Partial (docs/warn) |
| L1 | low | `MATCH_MAX_MINUTE` unused; `CHANNEL_ID` assigned twice | Yes |
| L2 | low | `get_prob_to90_decision_threshold` / 45+ docstring call to90 “legacy” | Yes |
| L3 | low | MID_STRICT 25–30 and `check_early_strict_mode` dead for ordinary send | Yes |
| L4 | low | Readiness comment “minute 47” vs `REGULAR_SIGNAL_MIN_MINUTE` 46 | Yes |
| L5 | low | Metric key aliases (`shots_inside_box` vs `shots_insidebox`) inconsistent | Yes |
| L6 | low | UTC+3 timezone fallback; `_parse_env_bool` unknown values → default | Yes |
| L7 | low | Duplicate outcome writers / 2H collect paths | Yes |
| L8 | low | Google Sheets daemons still started; `GSHEETS_AVAILABLE=false` | Yes |
| L9 | low | WINDOW remain 70/75 vs BASE 75 vs DYNAMIC 79–82 | Yes (log/docs) |

---

## Findings

### C1. Admin-review inequality is unreachable

- **Severity:** critical  
- **Where:** `NanoTest.py` constants `461–463`; `main_loop` `30764–30774`  
- **Actual:** `REVIEW_MIN_THRESHOLD = 80.0`, `PROB_SEND_THRESHOLD` default `30`, condition `REVIEW_MIN_THRESHOLD <= prob_actual < PROB_SEND_THRESHOLD` → `80 ≤ p < 30`. Always false. `REVIEW_MAX_THRESHOLD = 79.999` is unused. Comment says “50–79.999%”. `ENABLE_ADMIN_REVIEW_SIGNALS` therefore never queues a review card from this branch.  
- **Risk:** Operators who enable admin review get a silent no-op; no DM, no `publish_signal_to_channel` from this gate.  
- **Fix without product policy:** **Yes** — restore a coherent open interval matching the comment / unused `REVIEW_MAX_THRESHOLD`. That is a bugfix, not a new send rule. (Whether review should exist at all is already a flag.)

---

### H1. Wide-research champion fully replaces BASE allow

- **Severity:** high (only when `ENABLE_WIDE_RESEARCH` and `WIDE_RESEARCH_PRODUCTION_APPLY` and a `production_enabled` manifest)  
- **Where:** `wide_research/live.py` `ActiveRuleRouter.route` `622–625`; `NanoTest.py` `31256–31271`, `31315–31338`  
- **Actual:** `effective_publication_allow = router_decision["allow"]`. Champion PASS can publish when BASE failed; champion FAIL blocks when BASE passed. Default apply flag is **false**.  
- **Risk:** Quality bypass or total drop of BASE-passing fixtures once a champion is armed. Documented in the functional contract.  
- **Fix without product policy:** **No** — replace semantics are current product. Changing to AND-with-BASE would be a policy change.

---

### H2. Champion/router errors fail open to BASE

- **Severity:** high (when production apply is on)  
- **Where:** `NanoTest.py` `route_publication_with_wide_research` `24572–24580`; `wide_research/live.py` `615–616`  
- **Actual:** Outer `except Exception` returns `allow = current_filter_allow` with `reason=router_error_fallback`. Prediction-prep exceptions continue with `static_prediction`/`rolling_prediction` unset. Router `TypeError`/`ValueError` also return BASE allow.  
- **Risk:** A tightening champion that would BLOCK is silently skipped; BASE-pass fixtures still send.  
- **Fix without product policy:** **Yes** — fail closed (do not apply champion, or BLOCK) when a production champion is supposed to be active and evaluation errors. Distinguishing “no rule” vs “rule error” does not change the intended champion rule.

---

### H3. Active manifest cache sticks to `_last_good`

- **Severity:** high (when production apply is on)  
- **Where:** `wide_research/lifecycle.py` `ActiveManifestCache.load` `434–470`  
- **Actual:** Missing file, corrupt JSON, failed checksum, or same-generation checksum mismatch all **return in-memory `_last_good`**. Deleting the pointer file does not disable a previously loaded `production_enabled=true` rule until process restart.  
- **Risk:** Operators cannot turn a champion off by deleting/invalidating the file in a live process; stale production routing continues.  
- **Fix without product policy:** **Yes** — treat missing/invalid as empty (fail closed / no champion), which matches “checksummed active manifest required”.

---

### H4. Admin publish path diverges from ordinary send

- **Severity:** high (latent until C1 is fixed or review is triggered another way)  
- **Where:** `publish_signal_to_channel` `12453–12769` vs `main_loop` `31100–31557`; callback `14512–14522`  
- **Actual:** Admin path still runs BASE filter and Telegram, but **does not** call `is_first_signal_snapshot_ready`, `route_publication_with_wide_research`, or `record_current_decision`. Uses frozen `review_info["data"]`. Callback thread can race `main_loop`.  
- **Risk:** Weaker quality than ordinary send; no decision JSONL; possible duplicate channel posts.  
- **Fix without product policy:** **Partial** — sharing readiness/snapshots/idempotency is hardening. Whether admin must obey the champion is a product call.

---

### H5. `ALLOW` is persisted when Telegram did not deliver

- **Severity:** high (journals / ops)  
- **Where:** `send_to_telegram` `9947–9949`, `10032–10037`; `main_loop` `31501–31557`  
- **Actual:** Missing credentials or failed HTTP → `None`, `send_ok=False`, no `sent_matches`. Nested `record_current_decision("ALLOW", …, telegram_result=…)` still runs. Fixture is **not** in `monitored_matches`, so the next loop can retry send (good) while journals already have ALLOW (bad). `send_to_telegram` almost never raises (it swallows), so the `except` around the call is mostly unused.  
- **Risk:** Research/reputation treat undelivered signals as published; credential misconfig floods ALLOW rows.  
- **Fix without product policy:** **Yes** — keep eligibility logic; persist `BLOCK` / `SEND_FAILED` when `send_ok` is false.

---

### H6. One-signal-per-fixture is not enforced on `sent_matches`

- **Severity:** high (contract hole); medium in a healthy lifecycle  
- **Where:** `main_loop` skip `30605–30609`; `first_signal` `31096–31115` (readiness only); `send_to_telegram` `10005–10027`  
- **Actual:** Loop `continue`s on `monitored_matches` / `excluded_matches`, **not** on `sent_matches`. `first_signal` only skips readiness, not send. `send_to_telegram` does not check an existing message id before POST. If `sent_matches` has the fixture but monitored/excluded do not (partial finish, crashed post-send, manual state), a **second ordinary send** can occur. Contract: one ordinary channel signal per fixture.  
- **Risk:** Duplicate channel posts.  
- **Fix without product policy:** **Yes** — `continue` if `str(fixture_id) in sent_matches` before send; claim-under-lock before HTTP.

---

### H7. 2H store returning `False` never schedules retry

- **Severity:** high (default `pending_only` 2H cohort)  
- **Where:** `store_second_half_history_payload` `5973–6034`; `reconcile_pending_decision_outcomes` `28496–28526`  
- **Actual:** Only `IncompleteSecondHalfDataError` calls `_defer_second_half_incomplete_retry`. Append reject, generic exception (logged, returns `False`), or mocked `False` neither defers nor clears. `_decision_outcome_last_checked` is stamped **before** the store. After the same pass terminalizes the observation, default index drops the fixture from `collectable_2h` (see H8/M9). Locked in by `test_second_half_collection_failure_is_retried_after_observation_resolves` after characterization (current behavior: **no** second store).  
- **Risk:** Permanent missing 2H history for observation-only fixtures (signalled matches may still collect via monitor).  
- **Fix without product policy:** **Yes** — retry on `False`/IO; do not treat terminal observation as “2H done”.

---

### H8. Incomplete 2H backoff is pruned when history is still missing

- **Severity:** high without `SECOND_HALF_ALL_MATCHES_SINCE_UTC`; medium with cutoff  
- **Where:** `_prune_second_half_incomplete_retries` `28134–28139`; call `28264–28269`  
- **Actual:** Retry map keeps only fixtures in `missing_second_half_ids = collectable_2h − history`. After outcome resolve, default collectable set is empty → backoff entry deleted while JSONL still has no 2H row. Restart also loses the in-memory map.  
- **Risk:** Incomplete HT/events never retried unless a new pending observation appears or monitor collects.  
- **Fix without product policy:** **Yes** — prune only after history is stored (or persist backoff).

---

### H9. Prior confirmed WIN is never wired on re-resolve

- **Severity:** high  
- **Where:** `outcome_integrity.resolve_normal_time_outcome` (docstring: prior confirmed win is never downgraded); `_resolve_outcome_integrity` `3609–3621`; callers `3719`, `3837`, `20131`, `27921` all omit `previously_confirmed_win` (default `False`). Migration script is the only non-default caller.  
- **Actual:** Correction / `include_terminal_records` can append a LOSS revision over a prior WIN when the scoreboard looks reliable and flat.  
- **Risk:** Training, reputation, and reports flip WIN→LOSS on API corrections.  
- **Fix without product policy:** **Yes** — pass prior terminal WIN into the existing flag (implements documented integrity policy).

---

### H10. API-Football key is hardcoded

- **Severity:** high (security / ops; not a probability bug)  
- **Where:** `NanoTest.py` ~237 (`API_FOOTBALL_KEY = "…"`). Host is env (`API_FOOTBALL_HOST`).  
- **Actual:** Key is embedded “per user request”; not read from env.  
- **Risk:** Leak via repo, logs, process listing; forks inherit a live credential.  
- **Fix without product policy:** **Yes** — env-only, fail closed if missing.

---

### M1. Dual probability models on the same loop

- **Severity:** medium  
- **Where:** `compute_lambda_and_probability` `16012+` vs `compute_probability_45_plus` `16473+`; `main_loop` `30756–30808`; admin `12496–12503`  
- **Actual:** Review/eval logs use lambda **`prob_goal_either_to75`**. Publication uses 45+ **`prob_to90` / `prob_next_15`**. Admin recomputes both.  
- **Risk:** Wrong review band (also C1); operators tune the wrong metric.  
- **Fix without product policy:** **Partial** — stop using lambda for review/logging without changing BASE. Unifying review onto 45+ is product.

---

### M2. `prob_second_half_remain` is not “until FT”

- **Severity:** medium  
- **Where:** `compute_probability_45_plus` `16716–16721`, `16738–16742`; `main_loop` `30805–30809`  
- **Actual:** `prob_second_half_remain = prob_next_25`, and `prob_next_25` uses `horizon_minutes = min(90-minute, 25)`. Authoritative remain is `prob_to90` (full `90-minute`). Fallback `prob_to90_45 = res_45.get("prob_to90", prob_second_half_remain)` silently substitutes next-25 if `prob_to90` is missing. Exception path returns zeros (fail closed). Docstring still labels `prob_to90` as “legacy compatibility”.  
- **Risk:** Any consumer of the alias or fallback gates/displays the wrong horizon.  
- **Fix without product policy:** **Yes** — no fallback to next-25; rename/docs.

---

### M3. BASE Δrep ≥ 1.5 and season ≥ 1.02 with cold/disabled reputation

- **Severity:** medium–high (config footgun; named into filter version)  
- **Where:** `evaluate_channel_signal_filter` `29674–29684`; reputation disabled targets `26907–26918`; season soften `16649–16652`  
- **Actual:** All four BASE conditions must pass. Disabled/collecting reputation → Δ=0 → always fail `reputation_delta_to90_pp`. Neutral `season_context_factor_45p` is 1.0 after `1.0 + (combined_m-1)*0.6`, so **combined_m must be ≥ ~1.033** to pass 1.02. Turning reputation off does not “send raw 45+”; it **blocks all ordinary sends**.  
- **Risk:** Total publication starvation; easy to misread as a code bug.  
- **Fix without product policy:** **No** for relaxing thresholds. **Yes** for fail-loud logs when reputation is inactive (if product expects zero sends).

---

### M4–M5. Expanded reputation: silent telegram fallback and wrong stage sample

- **Severity:** medium (high if `ENABLE_SIGNAL_REPUTATION_EXPANDED_AUTO_APPLY=true`; default false)  
- **Where:** `apply_signal_reputation_auto` `26940–26990`; `signal_reputation/model.py` expanded cohort flags `shadow_only` / `production_apply=false`  
- **Actual:** Env flag applies expanded blend to **live** `prob_*` used by BASE despite cohort flags. Eval error or missing cohort → **telegram_signals** and still mutates probabilities (`fallback_cohort` set). Staging uses `target_fixtures` which for blend can be `all_decisions`, opening ±1/2/3 pp caps with little telegram evidence.  
- **Risk:** Unintended cohort; operators think expanded failed closed.  
- **Fix without product policy:** **Yes** for fallback (neutral/error, not telegram) and staging on telegram fixtures. Default-on auto-apply of telegram cohort is existing policy (M3).

---

### M6. `match_sent_at` vs `send_to_telegram`

- **Severity:** medium  
- **Where:** `send_to_telegram` `9940–10037` (writes `sent_matches` / `monitored_matches`, not `match_sent_at`); post-send `31692`; consumers `_event_happened_after_signal` `28560+`  
- **Actual:** If an exception hits after TG success and before `match_sent_at`, or on restart `bootstrap_tracking_from_state` uses `time.time()` as `sent_at_ts`, pre-signal events can count as post-signal (`return True` when sent minute/ts missing).  
- **Risk:** Wrong goal-after-signal labels and daily plus/minus.  
- **Fix without product policy:** **Yes** — write `match_sent_at` atomically with `sent_matches`; restore stored timestamps.

---

### M7. Retry maps are process-memory only

- **Severity:** medium  
- **Where:** `_second_half_incomplete_retry`, `_decision_outcome_last_checked`, `_outcome_correction_last_checked`  
- **Actual:** Not part of `STATE_FILE`. Restart forgets backoff and recheck stamps (then last_checked empty → immediate recheck, but H8 may have already dropped 2H intent). Dirty state flush every `SAVE_INTERVAL` — crash after TG OK before save can duplicate (H6).  
- **Risk:** Lost 2H retries; burst re-queries; duplicate send.  
- **Fix without product policy:** **Yes**.

---

### M8. Outcome correction is tightly bounded

- **Severity:** medium  
- **Where:** `ENABLE_OUTCOME_CORRECTION_RECHECK` (~729–739); sweep `28240–28314`  
- **Actual:** Default ~30 min sweep, 168 h lookback, **5 fixtures per sweep**, plus per-fixture throttle.  
- **Risk:** Wrong labels linger for days on large terminal cohorts.  
- **Fix without product policy:** **Yes** (raise limits); not a send-rule change.

---

### M9. `collectable_2h` depends on cutoff env

- **Severity:** medium  
- **Where:** index populate `18570–18594`; stream `19759–19773`; default `SECOND_HALF_ALL_MATCHES_SINCE_UTC=""`  
- **Actual:** No cutoff: resolved/void/quarantine **excluded** from collectable. Cutoff set: resolved **kept**. Index and stream agree. Tests/default behave like H7/H8; a live env with cutoff can still retry 2H after resolve.  
- **Risk:** Same code, different recovery; tests understate production-with-cutoff and overstate default holes.  
- **Fix without product policy:** **Yes** — collectable until 2H history exists, independent of outcome status.

---

### M10. Second-half parser silent 0–0 / kickoff+2h

- **Severity:** medium  
- **Where:** `second_half/parser.py` `_extract_ft_scores` `110–119`; `_extract_normal_time_scores` `88–96`; `_derive_finished_at` `68–74`; `_infer_league_type` `99–107`; `storage.iter_second_half_history_records` skips bad JSON  
- **Actual:** Missing FT goals become **0**. Incomplete is raised for some AET/PEN cases, not all missing FT. League type falls back to name tokens.  
- **Risk:** Biased 2H factors and aggregates; 0–0 FT stored as real.  
- **Fix without product policy:** **Yes** for Incomplete-on-unreliable FT; cup name heuristic is product-adjacent.

---

### M11. Estimated xG still drives BASE intensity

- **Severity:** medium  
- **Where:** `get_xg_with_fallback` `15951–15978`; 45+ weights estimated xG at 0.70 confidence (`16505`)  
- **Actual:** Missing API xG is replaced by `estimate_xg_from_metrics_combined`. BASE uses `adjusted_intensity` from this mix. Readiness can still fire on estimated xG.  
- **Risk:** Synthetic xG inflates `prob_to90` / intensity vs true API-zero.  
- **Fix without product policy:** **Partial** — fail-loud / down-weight further is hardening; forbidding estimated xG for BASE is policy.

---

### M12. Coverage thresholds and key lists disagree

- **Severity:** medium  
- **Where:** minute-46 skip `coverage < 0.85` (`30737–30754`); `FALLBACK_MIN_COVERAGE = 0.35` (`1677`, `15798`); `compute_coverage_score` box keys `shots_insidebox`/`inside_box` only (`15418–15419`)  
- **Actual:** First 46' cycle uses a **hardcoded 0.85**, not the named constant. Coverage can miss canonical `shots_inside_box`. Later 47–60 can evaluate with weaker coverage.  
- **Risk:** Skip/allow oscillation at HT; under-counted coverage.  
- **Fix without product policy:** **Yes** — one named threshold and one alias list.

---

### M13. Premium title ignores champion-only ALLOW

- **Severity:** medium  
- **Where:** `qualifies_for_premium_badge` `29716–29736` requires BASE `passed` and version match  
- **Actual:** If champion publishes a BASE-fail snapshot (H1), title is not Premium even though the channel message was sent. Premium is “presentation only” in contract, defined as BASE pass + goals ≤ 2.  
- **Risk:** Inconsistent channel branding vs send decision.  
- **Fix without product policy:** **Partial** — documenting is enough if Premium must stay BASE-tied; aligning Premium to `effective_publication_allow` would be policy.

---

### M14–M15. Shadow load / identity fallbacks

- **Severity:** medium  
- **Where:** `_shadow_ml_artifact_is_safe` `20846–20867` (rejects `production_applied` truthy only); `build_observation_from_decision` reuses `decision_id`; `wide_research/live.py` `_source_id` can fall back to `decision_id`. Decision ids include `ALLOW`/`BLOCK` (`17214–17236`); contract sketch omits that.  
- **Actual:** Hand-edited artifact with `shadow_only=false` can still load into cache (not copied into `res_45` today). Blank observation ids join on decision ids.  
- **Risk:** Future leak if someone copies cache into production math; wrong trigger/outcome joins.  
- **Fix without product policy:** **Yes** — require `shadow_only is True`; fail closed without `observation_id`.

---

### M16. Rescue and dynamic to90 look live

- **Severity:** medium (ops trap)  
- **Where:** `ENABLE_RESCUE_SIGNALS` `504`; send path hard-sets `rescue_publication_enabled=False` / `retired_by_base_p90_75` `31155–31173`; `ENABLE_DYNAMIC_PROB_TO90_THRESHOLD` default **true**; `get_prob_to90_decision_threshold` still called `30849+` for logs; docstring `512–513` says it is the ordinary gate.  
- **Actual:** WINDOW_*, DYNAMIC_TO90_*, Rescue **do not** gate Telegram. Logs still emit `[DYNAMIC_TO90_THRESHOLD]`, window tags, live-gate.  
- **Risk:** Tuning the wrong knobs.  
- **Fix without product policy:** **Partial** — warn/ignore flags and fix docstring without re-enabling Rescue (re-enable = policy).

---

### L1–L9. Dead, duplicate, inconsistent leftovers

- **L1** `MATCH_MAX_MINUTE` (default 45) is **never read**. `CHANNEL_ID` / `STATS_MESSAGE_ID` assigned at `247–249` and again `447–448`.  
- **L2** 45+ docstring: `prob_to90` “legacy”; it is the BASE metric.  
- **L3** `eval_mode = MID_STRICT` for minutes 25–30; ordinary send starts at 46. `check_early_strict_mode` returns True outside 25–30, so the 45+ `legacy_early_strict_passed` call is a no-op. Early-strict uses `get_any_metric(..., ["shots_inside_box"])` only (`30213`), unlike 45+ (`shots_insidebox`, `shots_inside_box`).  
- **L4** Readiness check 7 comment “minute 47”; code `minute == REGULAR_SIGNAL_MIN_MINUTE` (46).  
- **L5** Box-shot aliases differ across coverage, 45+, MID_STRICT, Sheets-era formatters (`12637`, `29538`).  
- **L6** `ZoneInfo` miss → fixed UTC+3 (`35–44`); `_parse_env_bool` unknown string → **default**, not error (`197–206`). Chat `getChat` failure **does not** fail startup (`9931–9932`).  
- **L7** `process_normal_time_outcomes_for_jsonl` vs `process_match_outcomes_for_jsonl` vs snapshot/observation resolve; reconcile store vs monitor `collect_second_half_history_for_fixture`. Drift risk, not a single current contradiction.  
- **L8** `GSHEETS_AVAILABLE = False`; `start_gsheets_labeler_daemon` still started. Writers early-return.  
- **L9** WINDOW remain 75/70, DYNAMIC 82→79, BASE 75. Shadow-only but easy to confuse.  
- **Fix without product policy:** **Yes** for all of the above (cleanup/docs/aliases).

---

## Intentional (not defects)

These look contradictory in isolation but match the written contract:

1. **Champion replaces BASE** when apply+manifest (H1) — `FUNCTIONAL_CONTRACT.md` §3.1.12.  
2. **WINDOW / live-gate / anti-garbage / Rescue do not publish** — BASE is the only ordinary quality gate. Anti-garbage cools `prob_next_25` only, not `prob_to90` (`16726–16742`).  
3. **Score total is not a BASE pass/fail input**; goals ≤ 2 only affect Premium.  
4. **Research layers** (ML, market, 4f/precision/rare, shadow candidates) are not copied into `res_45` when flags stay shadow.  
5. **Fail-closed BASE** on non-finite inputs (`29645–29669`).

---

## How this was audited

- Read production send path (`main_loop`, BASE filter, router, Telegram, admin publish).  
- Read reconcile/2H retry, observation collectable index vs stream, `outcome_integrity`.  
- Read reputation auto-apply, wide-research cache/router, second-half parser fallbacks.  
- Cross-check contract + characterization `PRODUCTION_FLOW_KNOWN_GAPS` (2H False-store / prune).  
- Did **not** run a live bot cycle; pytest was not re-run for this document.

Highest-priority **bug-class** items (policy-stable): **C1**, **H2–H3**, **H5–H9**, **H10**. Highest-priority **policy-armed** item: **H1** (do not “fix” unless product wants AND-with-BASE).
