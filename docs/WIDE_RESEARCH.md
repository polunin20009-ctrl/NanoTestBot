# Wide research rule factory

Wide research is the bot's autonomous rule-discovery and prospective-validation
layer. It searches the broad technically eligible match universe, freezes a
small set of candidate rules, and measures those rules on future matches.
Primary champions remain research artifacts and no longer route Telegram
publication.

It is deliberately separate from the existing publication filter and from ML
training. Discovery does not edit an ML model, and historical performance can
never promote a rule directly into production.

## What "all matches" means

The universe contract is `all_technically_eligible_46_60_v1`. A snapshot is in
that universe only when all of the following are true:

- its stage is `decision_pipeline` or `wide_monitor`;
- its minute is between 46 and 60, inclusive;
- it has a valid fixture ID, observation ID, and timezone-aware creation time;
- the normal readiness gate passed;
- the publication-context gate passed.

The current signal filter, its final `ALLOW`/`BLOCK` decision, and whether a
Telegram message was sent are explicitly ignored. Thus the research population
is much wider than sent signals. It is not literally every fixture returned by
the API: a match that never reaches a complete technical snapshot, is outside
minutes 46--60, or fails readiness/publication context is not eligible.

`decision_pipeline` captures ordinary pre-publication observations.
`wide_monitor` captures deduplicated observations while a sent match continues
to be monitored, so sending a signal does not censor its later eligible state.
The store retains first/latest universe metadata per fixture rather than every
failed rule evaluation.

## Causality and leakage controls

Research rules are conjunctions: every clause must pass. An absent, malformed,
or non-finite feature is `UNAVAILABLE` and cannot pass the rule. The rule DSL is
limited to the reviewed numeric allowlist and the operators `>=`, `<=`, and
`==`. Outcome, result, final decision, Telegram, send, win/loss, and current
filter fields are not flattened into candidate features.

Available feature families include:

- minute and current score;
- the bot's frozen probabilities;
- reputation base, adjusted probability, and delta;
- engineered intensity, pressure, game-state, season/team/league, confidence,
  and xG features;
- available raw match metrics;
- validated frozen 5- and 10-minute deltas/rates;
- causal static-ML and rolling-ML probabilities.

A 5/10-minute window is usable only when its frozen shadow contract is valid,
its requested and actual spans are consistent, and it has available activity
data. An ML value is usable only when the prediction matches the exact
observation ID, fixture, minute, and observation timestamp; was appended no
more than 300 seconds after the observation; came from a model created before
the observation with an earlier data cutoff; and is explicitly shadow-only
with no production effect. Missing rolling or ML data therefore rejects a rule
that requires it instead of being silently imputed or recovered from a later
minute.

For every `(phase, rule, fixture)`, only the first matching observation is
claimed. Later snapshots cannot move the trigger to a more favorable minute.
Its eventual `goal_to90_normal_time` outcome is attached by observation ID.
Pending outcomes do not close a statistical look. The fixture identities for
each milestone are frozen before their outcomes are known; later wins cannot
replace an earlier pending match. For the READY proof, a terminal invalid/void
outcome is conservatively counted as a loss. Outcome corrections can change a
label but cannot change the frozen fixture cohort.

## Discovery

The isolated discovery worker joins observation history with matching static
and rolling prediction journals, then:

1. filters rows through the same universe and feature contracts used live;
2. groups by fixture and sorts fixtures chronologically;
3. splits fixtures into train/validation/holdout (60%/20%/20% by default);
4. builds fixed thresholds plus quantiles calculated from train only;
5. runs a bounded beam search over AND rules (up to three clauses in the
   production-capable profile and up to eight in the isolated precision
   profile);
6. uses validation to select at most ten distinct candidates;
7. freezes candidate identities before opening holdout;
8. evaluates holdout once and reports a one-sided binomial test against 90%,
   with Holm correction across the selected family.

Default search limits are a beam width of 64, 12,000 rule evaluations, support
of at least 40 train / 15 validation / 15 holdout fixtures, and ten selected
candidates. The thresholds, splits, fixture-ID digests, rule clauses, and
checksums are recorded in immutable artifacts under
`reports/wide_research/`.

Train, validation, and holdout results are retrospective diagnostics only.
They may justify starting a candidate's prospective phase, but they are stored
with `historical_metrics_are_evidence=false` and never contribute to READY.

Discovery runs in a subprocess against stable journal snapshots. It shares the
bot's heavy-worker slot with model training, has a timeout, and is collected
after completion so its search memory does not accumulate in the long-lived bot
process. By default it runs once on daemon startup and then weekly. A failed
worker leaves production unchanged and is retried on the lifecycle cadence.

## Phases and prospective evidence

Each immutable rule is measured in a versioned phase. A newly imported phase
starts at the later of the configured prospective boundary and its import time.
No earlier match can enter it, even if that match appears in the discovery
history.

Every discovery run receives an immutable telescoping alpha budget: run `n`
gets `5% / (n·(n+1))`. The infinite sum across continuous weekly searches is
bounded by 5%, while later runs retain more statistical power than with
exponential decay. Within a run, the candidate family size is frozen and Holm
step-down uses current prospective p-values; retired siblings remain part of
that statistical family. This prevents endless automatic searching from
eventually promoting a lucky rule merely through repeated testing.
The run, alpha allocation, complete phase family, and generation retirements
are committed by one SQLite transaction against the exact pool snapshot used
for admission. A crash or competing worker therefore leaves either the whole
family or no part of it. Each phase also stores its lifecycle policy; later
configuration changes cannot retroactively loosen its statistical gates.

The normal lifecycle is:

```text
SHADOW -> READY -> ACTIVE -> DEGRADED
                  |            |
                  +-- champion +-- pointer cleared / replacement selected
```

`RETIRED` is used for outcome-independent generation or contract replacement;
same-generation candidates that do not fit are left unadmitted. Other stored statuses
(`candidate`, `paused`, `rejected`, and `failed`) support explicit lifecycle
bookkeeping. Retired and rejected phases are terminal.

The live layer evaluates at most `WIDE_RESEARCH_MAX_SHADOW_RULES` phases. The
SQLite transaction atomically enforces one first trigger per fixture and phase,
including when multiple workers race. Outcome events are versioned, and startup
recovery runs SQLite integrity checks and repairs denormalized latest-outcome
fields.

### READY gates

Readiness is checked only at pre-registered resolved-sample milestones:
`50, 100, 150, 200, 300, 500, 750, 1000`. The default policy cannot pass before
200 resolved triggers. At a closed look, all of these gates must pass:

- observed hit rate is at least 95%;
- the lower bound of the 95% Wilson interval is at least 90%;
- the prospective span is at least 28 days;
- triggers occurred on at least 14 distinct days;
- at least 8 leagues are represented;
- no league contributes more than 35% of resolved triggers;
- frequency is at least 2 resolved triggers per week;
- at least 4 weekly windows have a hit rate of at least 90%;
- a one-sided binomial test beats the 90% null after correction for both the
  fixed discovery-run candidate family, repeated milestone looks, and the
  telescoping alpha budget spent across successive discovery runs.

If a rule fails at 200, extra results do not continuously move that same test;
the next fresh decision point is the next configured milestone. This avoids
repeatedly checking after every favorable result.

Historical holdout, historical Telegram signals, and outcomes before the phase
boundary are never added to these gates.

A READY phase is revalidated under its frozen policy on every lifecycle pass and immediately before
promotion. If a versioned outcome correction invalidates its certificate, it
returns to SHADOW. Lifecycle transitions and production-pointer swaps are one
SQLite transaction, so a crash cannot leave a degraded pointer live or an
orphan champion published. Before a destructive pointer clear, the checksummed
fallback file is written first; a crash between disk and DB can therefore only
fall back early, never keep a degraded research rule routing signals.

### Promotion, degradation, and replacement

`WIDE_RESEARCH_PRODUCTION_APPLY=true` grants permission to promote; it does not
bypass READY. When no champion exists, the strongest READY phase is promoted
atomically, the production pointer generation increments, and a checksummed
active manifest is written with `fsync` plus atomic replace.

An active champion has a minimum tenure of 14 days before automatic
degradation. After that, it is degraded when either:

- its last 50 resolved triggers have a hit rate below 80%; or
- for its last 150 resolved triggers, the 95% Wilson upper bound is below 90%.

The pointer is then cleared. The router immediately falls back to the current
filter; if another READY phase exists, the controller may promote it. Weekly
discovery continues creating potential replacements, subject to the bounded
shadow pool and the same prospective gates.

## Historical production router

The active file is `stats/wide_research_active.json`. The controller and router
remain available for historical evidence and compatibility, but `main_loop`
does not call this router for Telegram publication. `balanced-two-rule` is now
the only publication gate.

- wide research and production application are enabled;
- the active manifest is structurally valid and its checksum is valid;
- `production_enabled` is true;
- an active rule and phase are present;
- the snapshot is not earlier than the manifest's effective timestamp;
- the immutable rule manifest can be parsed and checksum-verified.

The validation rules below describe how the dormant router verifies a manifest;
they do not grant publication authority.

The manifest cache never accepts a lower generation or a corrupt update. While
the process is alive it also rejects different content under the same
generation. Generations are strict integers starting at 1. After a clean start
with no valid manifest, routing falls back to the current filter. A manifest
with `production_enabled=true` but `rule=null` is therefore safe and does not
create signals.

## Configuration

Set the prospective boundary explicitly in `.env` and keep it timezone-aware.
Changing the environment value later does not rewrite the immutable start time
of an already registered phase.

| Variable | Default | Meaning / enforced bound |
|---|---:|---|
| `ENABLE_WIDE_RESEARCH` | `true` | Enable collection and the controller daemon |
| `WIDE_RESEARCH_DB_FILE` | `data/wide_research.sqlite3` | SQLite source of truth |
| `WIDE_RESEARCH_ACTIVE_MANIFEST_FILE` | `stats/wide_research_active.json` | Atomic production pointer manifest |
| `WIDE_RESEARCH_DISCOVERY_FILE` | `stats/wide_research_discovery.json` | Latest compact cycle summary |
| `WIDE_RESEARCH_OUTPUT_DIR` | `reports/wide_research` | Immutable discovery artifacts |
| `WIDE_RESEARCH_PROSPECTIVE_START_UTC` | `2026-08-28T00:00:00+00:00` | Earliest allowed prospective trigger time |
| `WIDE_RESEARCH_AUTO_DISCOVERY` | `true` | Run isolated discovery automatically |
| `WIDE_RESEARCH_AUTO_LIFECYCLE` | `true` | Reconcile READY/ACTIVE/DEGRADED automatically |
| `WIDE_RESEARCH_PRODUCTION_APPLY` | `false` | Retained for lifecycle compatibility; ignored by Telegram routing |
| `WIDE_RESEARCH_DISCOVERY_INTERVAL_SECONDS` | `604800` | Discovery cadence; minimum 86400 seconds |
| `WIDE_RESEARCH_LIFECYCLE_INTERVAL_SECONDS` | `900` | Lifecycle cadence; minimum 300 seconds |
| `WIDE_RESEARCH_PHASE_REFRESH_SECONDS` | `60` | Live phase-cache refresh; minimum 5 seconds |
| `WIDE_RESEARCH_WORKER_TIMEOUT_SECONDS` | `1800` | Discovery subprocess timeout; minimum 300 seconds |
| `WIDE_RESEARCH_WORKER_MEMORY_LIMIT_MB` | `3072` | Hard address-space ceiling for each isolated discovery worker |
| `WIDE_RESEARCH_MAX_SHADOW_RULES` | `10` | Simultaneously measured phases; clamped to 1--20 |
| `WIDE_RESEARCH_MAX_DB_BYTES` | `2147483648` | SQLite/WAL storage budget; minimum 10 MiB |

### Exact four-factor research profile

The optional four-factor profile is a physically separate experiment. It
searches only conjunctions containing exactly four different features. Lower
depths are used only as beam-search steps and can never be emitted as this
profile's candidates.

It receives the same already collected eligible observations, causal static
and rolling ML predictions, and terminal outcomes as the primary profile. It
does not make additional API requests. Its failures are isolated from the
primary profile.

The safety boundary is deliberately stronger than an environment toggle:

- its SQLite database, latest summary, immutable artifacts, and placeholder
  manifest path are separate;
- each SQLite file is permanently bound to either `primary` or
  `exact_four_shadow`, so even a mistaken manual command cannot open the other
  profile's registry;
- its controller is constructed with `production_enabled=false`;
- no `ActiveRuleRouter` exists for this profile;
- the worker runs with `--shadow-only`, and lifecycle reconciliation is never
  called, so phases remain `SHADOW`;
- an incomplete or budget-exhausted depth-four search is rejected before any
  candidates are imported. The previous prospective phases remain intact.

| Variable | Default | Meaning / enforced bound |
|---|---:|---|
| `ENABLE_WIDE_RESEARCH_FOUR_FACTOR` | `true` | Enable exact-four collection and discovery |
| `WIDE_RESEARCH_FOUR_FACTOR_DB_FILE` | `data/wide_research_4f.sqlite3` | Isolated prospective evidence store |
| `WIDE_RESEARCH_FOUR_FACTOR_ACTIVE_MANIFEST_FILE` | `stats/wide_research_4f_active.json` | Reserved isolated path; never used for routing |
| `WIDE_RESEARCH_FOUR_FACTOR_DISCOVERY_FILE` | `stats/wide_research_4f_discovery.json` | Latest compact four-factor cycle summary |
| `WIDE_RESEARCH_FOUR_FACTOR_OUTPUT_DIR` | `reports/wide_research_4f` | Immutable four-factor artifacts |
| `WIDE_RESEARCH_FOUR_FACTOR_PROSPECTIVE_START_UTC` | `2026-09-03T17:05:31+00:00` | Earliest boundary; each phase actually starts no earlier than its import time |
| `WIDE_RESEARCH_FOUR_FACTOR_AUTO_DISCOVERY` | `true` | Run the isolated search automatically |
| `WIDE_RESEARCH_FOUR_FACTOR_DISCOVERY_INTERVAL_SECONDS` | `604800` | Independent discovery cadence |
| `WIDE_RESEARCH_FOUR_FACTOR_MAX_SHADOW_RULES` | `10` | Bounded prospective candidate pool |
| `WIDE_RESEARCH_FOUR_FACTOR_BEAM_WIDTH` | `32` | Search beam used at depths one through four |
| `WIDE_RESEARCH_FOUR_FACTOR_EVALUATION_BUDGET` | `40000` | Maximum rule evaluations per run |
| `WIDE_RESEARCH_FOUR_FACTOR_MIN_TRAIN_SUPPORT` | `60` | Minimum historical train fixtures |
| `WIDE_RESEARCH_FOUR_FACTOR_MIN_VALIDATION_SUPPORT` | `20` | Minimum historical validation fixtures |
| `WIDE_RESEARCH_FOUR_FACTOR_MIN_HOLDOUT_SUPPORT` | `20` | Minimum historical holdout fixtures |
| `WIDE_RESEARCH_FOUR_FACTOR_MAX_DB_BYTES` | `2147483648` | Separate SQLite/WAL storage budget |

The beam keeps only compact ranking statistics while exploring thousands of
rules. Full fixture identities plus daily, weekly, and league breakdowns are
recomputed for the final candidates, preserving auditability without retaining
large intermediate structures in the long-lived bot. Automatic workers also
use `--require-memory-limit`: a requested but unapplied address-space ceiling
stops the run before it opens the research database. Summaries record requested
and actually applied limits separately; a stricter host limit is accepted.
Deterministic worker failures back off for 1 hour, then 6 hours, then 24 hours,
instead of repeatedly competing with ML workers every lifecycle interval.

### Precision-first portfolio profile

`precision_shadow` is a third, physically isolated research profile for the
quality-first objective. It does not require a bot or ML probability anchor.
It builds train-only thresholds for every available feature on the reviewed
causal allowlist, searches conjunctions containing two through eight different
search atoms, and ranks them by validation Wilson lower bound before raw hit
rate.

Two reviewed causal shortcuts are available only inside this hard-shadow
profile:

- `composite.base_quality_v1` is the exact current BASE conjunction
  (`p90>=75`, reputation delta `>=1.5pp`, adjusted intensity `>=0.55`, season
  context `>=1.02`);
- `rolling.both_windows_available_v1` requires valid frozen 5m and 10m
  windows under the normal rolling contract.

They let the search use the remaining clause budget for genuine refinements.
They are intentionally absent from the production-capable primary profile and
cannot affect Telegram routing. The beam also deduplicates candidates with the
same train first-trigger signature so syntactic aliases do not crowd deeper,
materially different candidates out of the search.

The 100,000-evaluation budget is split across depths as
`1000,15000,16000,16000,14000,13000,13000,12000`. A busy shallow level
therefore cannot consume the budgets reserved for depths five through eight.
Depth one must be complete; later depth quotas may truthfully report
deterministic sampling while still reaching depth eight. This is a broad
bounded search over all available factors, not an exhaustive enumeration of
every mathematical combination. The worker remains under the shared 3 GiB
address-space ceiling.

Each rule manifest pins the feature-extractor schema. Candidate identity also
includes the discovery engine, threshold grid, trigger policy, minute window,
stages, current-filter policy, and clauses. Each discovery run records a search
generation made from these stable execution contracts. A changed generation retires only older SHADOW/candidate phases,
without consulting their wins or losses. Within one generation, existing
prospective phases are never evicted because of observed performance: new rules
use genuinely free pool slots. Every member of the validation-selected
portfolio (or the best available portfolio when the cadence gate is not met)
must fit as one unit, otherwise the import fails before changing the registry.
This avoids survivor bias and prevents a portfolio from being tested only in
part. The precision pool holds 40 phases while each run proposes at most 20, so
the first two same-generation searches can coexist.

After individual validation ranking, a second validation-only beam combines
rules with OR semantics. Multiple rules triggering the same fixture count as
one signal, at the earliest causal trigger. A portfolio is selected only when
its union reaches at least 10 calendar-normalized signals per week. The
preferred reporting band is 10--15 (midpoint 12.5), but a more frequent
portfolio is not rejected when its confirmed precision is better. Portfolio
membership is frozen before holdout is opened; train, validation, and holdout
union metrics are then reported separately. If no portfolio reaches the
minimum cadence, `selected_portfolio` is null and the strongest insufficient
alternative is recorded as `best_available_portfolio`.
The portfolio result also records an explicit holdout support gate, raw 95%
target-rate gate, and a one-sided confirmation test against the 90% null; this
metadata is descriptive only and cannot activate publication.

The low 12/4/4 historical support floors only admit exploratory candidates;
Wilson ranking penalizes tiny samples, and no historical result can become
production evidence. `precision_shadow` has a separate bound SQLite database,
no `ActiveRuleRouter`, and hard-rejects production mode. Its lifecycle does run
the ordinary prospective readiness checks (at least 200 resolved triggers,
28 days, 14 trigger days, 8 leagues, Wilson lower bound at least 90%, weekly
stability and corrected significance). A passing phase may be marked READY in
the laboratory database, but `production_enabled=false` makes promotion or
Telegram routing impossible until a separately reviewed policy is implemented.

| Variable | Default |
|---|---:|
| `ENABLE_WIDE_RESEARCH_PRECISION` | `true` |
| `WIDE_RESEARCH_PRECISION_AUTO_LIFECYCLE` | `true` |
| `WIDE_RESEARCH_PRECISION_DB_FILE` | `data/wide_research_precision.sqlite3` |
| `WIDE_RESEARCH_PRECISION_DISCOVERY_FILE` | `stats/wide_research_precision_discovery.json` |
| `WIDE_RESEARCH_PRECISION_OUTPUT_DIR` | `reports/wide_research_precision` |
| `WIDE_RESEARCH_PRECISION_MAX_SHADOW_RULES` | `40` |
| `WIDE_RESEARCH_PRECISION_DISCOVERY_TOP_N` | `20` |
| `WIDE_RESEARCH_PRECISION_BEAM_WIDTH` | `96` |
| `WIDE_RESEARCH_PRECISION_MAX_CONJUNCTION_SIZE` | `8` |
| `WIDE_RESEARCH_PRECISION_EVALUATION_BUDGET` | `100000` |
| `WIDE_RESEARCH_PRECISION_DEPTH_BUDGETS` | `1000,15000,16000,16000,14000,13000,13000,12000` |
| `WIDE_RESEARCH_PRECISION_MIN_SIGNALS_PER_WEEK` | `10.0` |
| `WIDE_RESEARCH_PRECISION_PREFERRED_SIGNALS_PER_WEEK` | `12.5` |
| `WIDE_RESEARCH_PRECISION_MAX_SIGNALS_PER_WEEK` | `15.0` |
| `WIDE_RESEARCH_PRECISION_PORTFOLIO_MAX_RULES` | `10` |
| `WIDE_RESEARCH_PRECISION_PORTFOLIO_BEAM_WIDTH` | `128` |

Database and artifact paths must remain below the project root, must not be
symlinks where prohibited, and must not overlap live observation/prediction
journals. SQLite uses WAL, foreign keys, `synchronous=FULL`, transactional
first-trigger and milestone-cohort claims, an immutable inter-run alpha ledger,
file mode `0600`, and an explicit size ceiling. The system does not silently
delete prospective evidence when that ceiling is reached.

### Rare precision with intervals and temporal validation

`rare_precision_shadow` is a fourth, isolated laboratory. It searches the same
broad, technically eligible 46--60 minute universe, including matches never
sent to Telegram. Neither BASE nor a particular bot/ML probability is a
mandatory anchor. All available numeric features on the causal allowlist can
participate. Existing profiles and their prospective evidence remain intact.

The new language can express a closed range with two ordinary AND clauses,
for example `bot.prob_to90 >= 80 AND bot.prob_to90 <= 87`. These are two bounds
on one feature, not two independent sources of evidence. Both directions are
searched; thresholds come from pre-registered grids and train-only quantiles.
Intervals are explicitly seeded at depth two, so both separate bounds need
not survive the single-factor beam. Redundant same-direction bounds and
contradictory intervals are rejected.

Validation is divided into four chronological, disjoint fixture groups.
Every selected candidate must have support in all four groups; the minimum
per group is `ceil(min_validation_support / 4)`. The deployed support floors
are 12 train, 8 validation (at least two per window), and 4 holdout matches.
Holdout support is reported after selection, and never used to choose rules.
Small windows provide exploratory checks, not evidence of long-term stability.

Ranking rewards the weakest validation window, using raw hit rate together
with the Wilson lower bound. A quarter of shortlist slots are reserved for
leaders in raw precision, so rare high-percentage candidates can survive
alongside candidates with stronger sample support. There is no minimum
signals-per-week gate and no OR portfolio in this laboratory. The desired
future channel volume remains a later, separate evaluation.

The extended worker runs two pre-registered scopes: general nonlinear research
and causal-market research. Each scope selects at most eight finalists, with a
shared pool of at most 64 active candidates. Each uses beam 128, up to eight
clauses, and 180,000 evaluations (at most 360,000 total per weekly worker).
Depth quotas per scope are `4000,28000,30000,28000,25000,23000,22000,20000`.
Diagnostics record actual depth coverage and truncation. The worker uses the
shared memory-limited process slot. This remains a bounded search, not every
possible conjunction.

The same 25% raw-precision reserve is applied during train exploration, before
validation. The general extended scope also reserves 25% of its beam and
finalist slots for supported nonlinear rules, when such rules exist. Repeated
train evaluations use cached atomic bitsets and exact
first-trigger matching; the final report recomputes its full fixture lists
with the original evaluator. This speeds up the larger search without sampling
or dropping observations.

Each admitted rule automatically starts an immutable prospective phase at
import time. Reviews use the first 50, 100 and 200 triggered fixtures. Poor
interim results do not replace or restart the candidate. Once the first 200
fixtures are claimed, further admission is capped atomically and the lifecycle
pauses the phase, freeing its executable pool slot. Pending outcomes still
resolve against that fixed cohort. The evidence remains on disk. Rediscovery
of the same executable rule reuses that identity across search-version
changes, and corrections to its outcomes remain
visible in subsequent reports. A run with no finalists preserves ongoing
phases. If all 64 slots are occupied, admission explicitly reports the
unadmitted candidates; it does not evict rules based on wins and losses.

At the final review, observed hit rate of at least 90% is labelled
`reviewable`; this is a request for manual examination, not proof of 93%.
The ordinary stricter readiness gates are also reported separately. Every
terminal phase stays paused even when those gates pass. This profile has no
production router and cannot publish a Telegram signal.

#### Extended expressions and causal market scope (September 11)

The expression library `nonlinear_v1` enumerates all 66 pairwise products of
12 pre-registered scaled primitives: intensity, pressure, season, game state,
xG, box shots, 5m/10m shot pace, and bot/static/rolling/market p90. It also
includes ratios (shot quality and pace acceleration), team-side shares,
signed home-away balances, and the trailing team's share for six metrics.
Products can be combined with other products, ranges and ordinary clauses.
This is an automatic bounded nonlinear rule search, not an unrestricted
symbolic-expression search or a new neural/tree ML model. Missing inputs and
zero denominators stay unavailable; they are never filled with zero.

Versioned market features include the no-vig p90 estimate, Over decimal odds,
overround, gaps versus the bot and both ML models, and probability/odds changes
over the previous 5 and 10 wall-clock minutes. A shared extractor checks
normalized snapshot identity, provider, normal-time settlement, exact score,
half-goal line, provider timestamp, capture timestamp, and freshness. The
baseline quote must already be known at `observation_time - window`, within
120 seconds of that boundary, and use the same bet/score/line as the current
quote. Nothing observed after the prediction is a feature.

Live observations persist `market_research` evidence before the observation is
appended or scored. The independent bounded cache retains up to 64 quotes per
fixture for 12 minutes, and at most 512 fixtures. A restart initially makes
long-window changes unavailable; no old context is fabricated to fill the gap.
Older observations without a frozen block can be joined to the append-only
quote journal using the same causal extractor. Explicitly frozen unavailable
blocks are never backfilled. The worker uses a locked stable journal copy and
a disk-backed timestamp index; it does not load all quotes into bot RAM.

Because market collection began later than gameplay collection, the market
scope chronologically splits only observations with valid contemporaneous
quotes. It has its own train, four validation groups and untouched holdout.
Every market candidate must include a direct or transformed market feature;
therefore historical and live eligibility agree. At least 40 eligible market
fixtures are needed to attempt the default 60/20/20 split and 12/8/4 support
floors. Below this count the scope reports `collecting`. This is a feasibility
threshold, not a readiness or accuracy guarantee.

Both scopes freeze their selections without holdout ranking. Their combined
admission family uses one prospective alpha allocation, and retrospective
Holm correction is recomputed across all finalists. Each candidate records its
own research scope and selection period. A general-scope holdout is not claimed
to be the market scope's holdout: these are different declared populations.

The extended engine/grid IDs are `wide_rule_discovery_extended_v1` and
`wide_extended_grid_v1`. Base feature schema 2 and all old feature definitions
are preserved; new expressions have separately versioned names and explicit
opt-in extraction. Legacy three-profile search grids remain unchanged. Old
rare phases keep their original boundaries and evidence. Extended rules can
only be imported into the terminal hard-shadow profile; the production router
and both ML training feature sets do not receive these inputs.

#### Error-guided offspring and crossover (September 11)

The rare worker now passes `--error-refinement`, using engine
`wide_rule_discovery_refinement_v1` with the unchanged extended feature grid.
The additional algorithm/policy is versioned `error_guided_offspring_v1`.
It runs independently in both general and causal-market cohorts. Old three
profiles and the ordinary beam finalists are unchanged by this additional lane.

Parents are freshly ranked on **train only** from the current beam's candidate
pool, not chosen using their prospective scoreboard or holdout. Up to eight
distinct parents with at least three losses and 70% train precision are
examined, with up to eight strong train donors. This is not an automatic
analysis of every active registered rule, nor a claim that parents were
already prospectively proven. Each parent is recorded with its exact clauses.

For every allowed train-derived atomic threshold, compare its passing rate
among the parent's wins and losses, including missing-feature counts. Try
discriminating single conditions, pairs of the top eight additions, and
intersections with strong donor rules. Crossover tightens redundant bounds
and rejects contradictory ranges. Every child logically retains its parent
constraints and the existing eight-clause limit. Market descendants retain
their required market dependency.

Replay the child's **actual first matching observation** per fixture. A later
trigger is explicitly reported and is not called an excluded losing match.
A parent's winning match becoming a losing later trigger counts as a lost
plus. Train admission requires at least 60% win retention, 20% actual loss
exclusion, a 10 percentage-point advantage of loss exclusion over lost-win
fraction, and at least 2 percentage points of observed precision improvement.
The normal minimum-support gate also applies. These are search policies,
not predicted live success probabilities.

Up to four train-shortlisted children per parent reach validation. They must
pass all four chronological support windows, keep at least half the parent's
wins, improve aggregate validation precision by at least one percentage
point, and not lower precision in at least three of the four windows.
Holdout is not available to the offspring search and is evaluated only after
selection. Finalists are deduplicated and limited to four per scope, at most
two per parent, in addition to the ordinary eight per scope. Thus a cycle
can admit up to 24 candidates; the total frozen pool remains capped at 64.

Each scope has a separate limit of 20,000 offspring proposal replays (at most
40,000 extra, 400,000 combined with the existing beam search limits). Contrasts
are separately bounded by eight parents and the finite atomic library.
If no child improves on validation, none is admitted; no fallback relaxes the
gates. Full diagnostics, parent/donor identities and clauses, train/validation
comparisons and timing shifts are stored under `error_refinement` and
`refinement_lineage`. Historical Holm correction and prospective allocation
include children in the same combined family as ordinary finalists.

Accepted children receive their own immutable prospective phase. An identical
executable rule reuses its existing phase rather than resetting evidence.
Parents are never retrospectively edited or retired to make room for children.
The existing 50/100/200 review policy and hard prohibition on production
application apply, even when a child uses only old-style features. Neither
Telegram selection nor either ML model's training inputs change.

The enable flag is `ENABLE_WIDE_RESEARCH_RARE_PRECISION`. Paths use the
`WIDE_RESEARCH_RARE_PRECISION_*` prefix and default to:

- `data/wide_research_rare_precision.sqlite3`;
- `stats/wide_research_rare_precision_active.json`;
- `stats/wide_research_rare_precision_discovery.json`;
- `reports/wide_research_rare_precision/`.

The interval, support floors, prospective boundary and storage ceiling can be
configured with the same prefix. Search shape and terminal policy are fixed
in code and included in the cycle contract. Runtime paths are checked against
all three other profiles and the live data journals. Engine/grid identities
are specific to this profile (`wide_rule_discovery_rare_v1` and
`wide_atomic_range_grid_v1`).

Inspect its accumulated evidence with:

```bash
python3.12 scripts/report_wide_research.py \
  --db data/wide_research_rare_precision.sqlite3 --json
```

`terminal_review` reports the frozen 200-fixture result and whether its outcomes
have all closed. Unknown outcomes are never replaced by later wins.

## Commands and reports

Run a full one-shot discovery plus lifecycle cycle:

```bash
GOALBOT_WIDE_RESEARCH_WORKER_MEMORY_LIMIT_MB=3072 \
python3.12 scripts/run_wide_research_cycle.py \
  --prospective-start-utc 2026-08-28T00:00:00+00:00
```

Permit automatic production promotion for this invocation (READY gates still
apply):

```bash
python3.12 scripts/run_wide_research_cycle.py \
  --prospective-start-utc 2026-08-28T00:00:00+00:00 \
  --production-enabled
```

Reconcile existing phases without a new search:

```bash
python3.12 scripts/run_wide_research_cycle.py \
  --prospective-start-utc 2026-08-28T00:00:00+00:00 \
  --production-enabled \
  --lifecycle-only
```

The CLI also exposes bounded search controls such as
`--min-conjunction-size`, `--max-conjunction-size`, `--beam-width`,
`--evaluation-budget`, `--top-n`, and split support minimums. Changing these creates a different auditable
discovery run; it does not alter an existing rule manifest.

Run an explicit isolated exact-four cycle (the daemon normally supplies all
paths and limits):

```bash
python3.12 scripts/run_wide_research_cycle.py \
  --prospective-start-utc 2026-09-03T17:05:31+00:00 \
  --db data/wide_research_4f.sqlite3 \
  --active-manifest stats/wide_research_4f_active.json \
  --latest-output stats/wide_research_4f_discovery.json \
  --output-dir reports/wide_research_4f \
  --store-profile exact_four_shadow \
  --require-memory-limit --shadow-only \
  --min-conjunction-size 4 --max-conjunction-size 4 \
  --beam-width 32 --evaluation-budget 40000 \
  --min-train-support 60 --min-validation-support 20 \
  --min-holdout-support 20
```

Run the isolated precision-first cycle explicitly:

```bash
python3.12 scripts/run_wide_research_cycle.py \
  --prospective-start-utc 2026-09-04T12:54:07+00:00 \
  --db data/wide_research_precision.sqlite3 \
  --active-manifest stats/wide_research_precision_active.json \
  --latest-output stats/wide_research_precision_discovery.json \
  --output-dir reports/wide_research_precision \
  --store-profile precision_shadow --require-memory-limit --shadow-only \
  --selection-mode precision_first \
  --min-conjunction-size 2 --max-conjunction-size 8 \
  --beam-width 96 --evaluation-budget 100000 \
  --depth-evaluation-budgets 1000,15000,16000,16000,14000,13000,13000,12000 \
  --max-shadow-rules 40 --top-n 20 --min-train-support 12 \
  --min-validation-support 4 --min-holdout-support 4 \
  --min-signals-per-week 10 --preferred-signals-per-week 12.5 \
  --max-signals-per-week 15 --portfolio-max-rules 10 \
  --portfolio-beam-width 128
```

`--shadow-only` and `--production-enabled` are mutually exclusive.

Print the operational phase report:

```bash
python3.12 scripts/report_wide_research.py
python3.12 scripts/report_wide_research.py --json
python3.12 scripts/report_wide_research.py \
  --db data/wide_research_4f.sqlite3 --json
```

Inspect the latest cycle and active manifest without modifying them:

```bash
python3.12 -m json.tool stats/wide_research_discovery.json
python3.12 -m json.tool stats/wide_research_active.json
```

Useful service checks:

```bash
systemctl status goalbot-test.service
journalctl -u goalbot-test.service --since today --no-pager -o cat \
  | rg 'WIDE_RESEARCH|ERROR|Traceback'
```

Expected startup/runtime records include `WIDE_RESEARCH_CONFIG`,
`WIDE_RESEARCH_4F_CONFIG`,
`WIDE_RESEARCH_DAEMON`, `WIDE_RESEARCH_RECOVERY`, and
`WIDE_RESEARCH_WORKER`. Four-factor records use the `WIDE_RESEARCH_4F_*`
prefix. Trigger, outcome, monitor, lifecycle, cache, and router
events have corresponding `WIDE_RESEARCH_*` tags. Errors explicitly report
that production is unchanged or that the current-filter fallback was used.

For backup or migration, preserve the SQLite database together with consistent
WAL state (use SQLite's backup mechanism or stop the writer first), the active
manifest, latest cycle summary, and immutable discovery artifacts. Do not edit
the DB, pointer, phase timestamps, checksums, or artifact files by hand.

## Limitations

The 95% target is a qualification policy, not a promise. Even a rule that
passes all gates can perform worse later because competitions, teams, API data,
market conditions, and match populations change. A high point estimate on a
small or concentrated sample is specifically not enough; that is why the
system requires a Wilson lower bound, time and league diversity, frequency,
weekly stability, multiple-testing correction, and genuinely future evidence.

The search is intentionally bounded. It explores reviewed threshold
conjunctions, not every possible program or causal explanation. Rare 100%
historical rules are usually fragile and may never collect 200 prospective
triggers. Conversely, a useful high-coverage rule can be rejected if its
uncertainty remains too large. Automatic replacement reduces stale-rule risk
but cannot guarantee a continuously available 95% champion; during gaps the
correct behavior is the existing-filter fallback.

Treat wide research as an auditable candidate factory and safety-controlled
publication layer, not proof of future profitability or a guarantee of signal
accuracy.
