# Shadow ML

The shadow-ML subsystem learns a small residual correction to the bot's
existing 45+ probabilities. It is deliberately isolated from production:

```text
current probability -> production decision and Telegram
                   \
                    -> shadow model -> prediction journal only
```

Every artifact and prediction contains `production_applied=false`. The runtime
does not copy an ML probability back into `res_45`, decision thresholds,
Rescue, Telegram text, or message updates. A missing, damaged, incompatible, or
untrained model therefore leaves the normal bot unchanged.

## What is trained

The first algorithm family is
`residual_logistic_stdlib_v1`. It uses two independent, L2-regularized logistic
models:

- `next15`: a goal within the next 15 minutes;
- `to90`: a goal before the end of normal time.

The current production probability is a fixed logit offset. The model learns
only a residual correction from the frozen metrics that were available at the
decision minute. With zero learned coefficients, the challenger falls back to
the current bot instead of behaving like an unrelated replacement model.

No third-party ML dependency is required. Training and inference use the Python
standard library, and the artifact is versioned JSON rather than pickle.

## Training contract and leakage protection

Training uses joined records from `observation_history.jsonl` only when all of
the following are true:

- `stage=decision_pipeline`;
- minute is between 46 and 60;
- `outcome.status=resolved`;
- `fixture_id` is valid;
- the target label and its pre-ML production probability are available.

`void`, pending, and prefilter observations are excluded. For `next15`, only
`exact`, `score_confirmed`, and `inferred` timing labels are accepted. Missing,
unknown, or future unrecognized quality values are excluded; inferred timing
receives weight `0.5`. `ALLOW` and `BLOCK` rows are both retained to avoid
selection bias.

The feature extractor uses an explicit allow-list:

- minute, window, and score at observation time;
- raw live metrics and their availability flags;
- xG/tempo data-quality fields;
- frozen pressure, intensity, lambda, season, and second-half factors.

It never reads final score fields, outcomes as features, Telegram state,
`final_decision`, gates, block reasons, team IDs, league IDs, or mutable current
team/league statistics.

## Rolling 5/10-minute dynamics

Rolling dynamics remain excluded from the incumbent
`residual_logistic_stdlib_v1` model. A separate shadow-only challenger,
`residual_logistic_stdlib_rolling_v2`, now trains and predicts in parallel.
This preserves the incumbent artifact byte-for-byte while allowing a fair
prospective comparison as rolling history accumulates.

The challenger has its own artifact family, semantic feature contract, cache,
candidate staging files, and durable outputs:

- `stats/shadow_ml_rolling_model.json`;
- `data/shadow_ml_rolling_predictions.jsonl`;
- feature profile `rolling_5m10m_v1`;
- artifact role `rolling_challenger`.

It requires a previously frozen rolling block with the supported schema and
`production_applied=false`. A row is not accepted when that whole block is
missing or incompatible. A missing individual 5- or 10-minute window remains
eligible and is represented with explicit availability and missing flags; it
is never silently converted to zero and is not filtered out.

In ordinary mode the bot schedules one lightweight `fixtures/statistics`
collection job in each of four stable seed slots:

- minute 36 through 38 for the early 10-minute baseline;
- minute 39 through 40 for its bridge baseline;
- minute 41 through 43 for the early 5-minute baseline;
- minute 44 through 45 for its bridge baseline.

The slot ID is durable, so a persisted empty response or restart does not cause
a request every minute. Each job makes one fetch and performs exactly one
immediate retry only after an empty response or exception. Therefore there are
at most eight top-level statistics fetches per fixture, in addition to the
common HTTP transport retry policy. Persisted seeds extend that guarantee
across restarts; a crash between the request and journal write can repeat that
one slot. Fetches run on eight bounded background workers, so a slow statistics
endpoint does not pause production fixture processing; a busy worker pool
defers the seed to a later loop in the same slot. Attempt statuses and durations
are stored with the seed. Errors and missing statistics remain fail-open.
Seed rows have `stage=rolling_seed`, a void outcome, no ML prediction, no
Telegram action, and no production decision.

Before a normal decision observation is persisted, it receives an immutable
top-level `rolling_dynamics` block with its own schema version. It contains
5- and 10-minute changes and per-minute rates for total xG, shots on target,
shots inside the box, total shots, corners, score, and pressure. Each window
also stores the baseline observation and minute, actual span, sample count,
half-time crossing, availability, and invalid reasons.

Only strictly earlier records from the same fixture can be baselines. The
actual span may exceed the requested window by at most two match minutes.
Within that tolerance the baseline yielding more valid activity deltas is
preferred, with temporal closeness used as the next tie-breaker.
Missing values stay distinct from real zeroes. A negative cumulative-counter
change is treated as an API correction/reset, while a negative pressure change
is valid. xG is compared only when both snapshots use the same source.
Restart hydration reads both the active observation journal and its rotated
gzip archives.

The challenger combines the incumbent feature allow-list with a strict rolling
allow-list. For each window it uses window status, actual span, half-time
crossing, per-metric availability, deltas, and rates for xG, shots, shots on
target, shots inside the box, corners, score, and pressure. It never consumes
baseline IDs, provider error text, fetch/retry metadata, decisions, gates,
Telegram state, or outcomes as features. A rolling-content revision changes
the challenger training hash and can trigger retraining even when labels and
fixture counts stay unchanged.

With successful anchors at minutes 36, 39, 41, and 44, both windows remain
available continuously from decision minute 46 through minute 60. The bridge
anchors cover the former gaps at 49–50 for both windows and at 54–55 for the
10-minute window. Full decision observations supply the later baselines.
If a fixture first appears after an anchor or the provider supplies no usable
statistics, the affected window honestly remains `baseline_missing`; the bot
never reconstructs an earlier snapshot from later data.

All minutes from one fixture stay in the same chronological split. The split is
70% train, 15% calibration, and 15% untouched holdout. Repeated minutes are
fixture-weighted so one match contributes approximately the same total weight
as another. Imputation, clipping, scaling, and category vocabularies are fitted
on the train split only. Platt calibration uses only the calibration split.

Observations are additionally grouped by a feature contract made from the
observation schema, production-model version, and factor versions. Training
uses the contract of the newest eligible observation and excludes incompatible
older cohorts. Runtime inference refuses a model whose feature contract differs
from the current observation. Deployment and config hashes are intentionally
not part of this contract, so a harmless code deployment does not discard
otherwise compatible history.

Feature-contract schema v2 also excludes the reviewed selection/presentation
versions `dynamic_threshold`, `rescue_controller`, `channel_signal_filter`, and
`premium_badge`. These versions remain in the immutable config and audit hash
of observations that record them, but none is read by the feature extractor or
base-probability offset. Changing a Telegram filter or badge therefore cannot
reset either ML cohort. All other and all previously unknown factor-version
keys remain in the contract by default, so an unreviewed predictive change
still fails safe by starting an incompatible cohort.

## Readiness

An experimental candidate is not trained before at least 150 resolved
fixtures and both target classes are represented. Training an experimental
candidate does not mean it is ready for production.

The default offline readiness gates additionally require, per target:

- at least 500 resolved fixtures;
- at least 56 days of history;
- at least 3,000 eligible rows;
- at least 100 independent fixtures in each class;
- at least 75 fixtures in calibration and 75 in holdout;
- both classes represented sufficiently in calibration and holdout;
- no more than 2% unknown or invalid `next15` labels;
- at least 85% availability across the core live-metric groups;
- holdout log loss improves over the production baseline by at least `0.001`;
- holdout Brier score improves by at least `0.0005`.

Even `offline_ready` remains shadow-only. The prospective report separately
requires predictions that were made before their outcomes existed. No code
path automatically promotes the model into production.

The incumbent and rolling challenger have independent readiness status. A
large historical rolling cohort can train an experimental model immediately,
but the challenger remains `collecting` until the same time-span, data-quality,
class-balance, and holdout-improvement gates are satisfied. Promotion is never
automatic. In addition, at least the configured offline-ready fixture count
must have both windows available, and each window must be available in at
least 60% of eligible rows. Thus a model trained only on missing-window flags
can never be labeled `offline_ready`.

## Runtime lifecycle

At startup, a background daemon loads the last valid artifact. If no artifact
exists, it writes a valid `collecting` artifact. Training never runs while an
observation or decision journal lock is held.

The same daemon orchestrates the incumbent and rolling workers sequentially.
They share one heavy-worker slot, so the two model fits and the reputation
model cannot create simultaneous memory spikes. Outcome/revision counters are
separate for the two families, so one completed or failed worker cannot consume
the other's retraining state.

When new observation outcomes are appended, they first update an in-memory
fixture counter. This deliberately avoids reading and decompressing the full,
unbounded history after every finished match. A batch retrain occurs after the
configured minimum interval and growth:

- normally after 25 new resolved fixtures;
- as a weekly fallback after at least 5 new fixtures.

An updated outcome label with the same fixture count is detected through the
training-data hash and retrained without waiting for fixture growth. A changed
feature contract also forces a safe collecting artifact as soon as the first
compatible fixture resolves.

The model is saved atomically and swapped into the inference cache only after
validation. Live inference is then an in-memory dot product. Predictions are
made only after the production decision and observation have already been
saved. If prediction persistence fails temporarily, a replay of the already
persisted observation retries it; the prediction key keeps that retry
idempotent.

## Files and retention

- `stats/shadow_ml_model.json`: current atomic model artifact, checksum
  protected;
- `data/shadow_ml_predictions.jsonl`: immutable prospective predictions;
- `data/shadow_ml_predictions.<UTC timestamp>.jsonl.gz`: rotated prediction
  archives.

Prediction rotation defaults to 50 MiB. Archives are never deleted
automatically. A prediction key contains both `observation_id` and `model_id`,
so restart-safe deduplication does not overwrite predictions from another model
version.

## Commands

Inspect readiness without replacing the artifact:

```powershell
python scripts\train_shadow_ml.py --dry-run
```

Train and atomically save the current candidate:

```powershell
python scripts\train_shadow_ml.py
```

Report only genuinely prospective prediction quality:

```powershell
python scripts\report_shadow_ml.py
```

The report rejects a prediction unless both the model creation time and its
data cutoff are earlier than the observation. The prediction itself must be
created no earlier than the observation and no later than the outcome.
Percentage-point fields are always interpreted as percentages, including
values at or below `1%`.

Prospective readiness needs at least 200 fixtures over 28 days, both classes,
the same minimum LogLoss/Brier improvements as the offline gate, and a
positive lower bound in a deterministic 95% fixture-level bootstrap interval.
An unchanged copy of the production baseline therefore cannot be marked ready.

## Configuration

Important environment variables:

- `ENABLE_SHADOW_ML=true`;
- `ENABLE_ROLLING_DYNAMICS_SHADOW=true`;
- `ROLLING_DYNAMICS_MAX_EXTRA_MINUTES=2`;
- `SHADOW_ML_AUTO_RETRAIN=true`;
- `SHADOW_ML_MODEL_FILE`;
- `SHADOW_ML_PREDICTIONS_FILE`;
- `SHADOW_ML_PREDICTION_ROTATE_MAX_BYTES`;
- `SHADOW_ML_RETRAIN_CHECK_SECONDS`;
- `SHADOW_ML_MIN_NEW_FIXTURES`;
- `SHADOW_ML_CANDIDATE_MIN_FIXTURES`;
- `SHADOW_ML_OFFLINE_READY_MIN_FIXTURES`;
- `SHADOW_ML_OFFLINE_READY_MIN_HISTORY_DAYS`;
- `SHADOW_ML_OFFLINE_READY_MIN_SPLIT_FIXTURES`;
- `SHADOW_ML_OFFLINE_READY_MIN_ROWS`;
- `SHADOW_ML_OFFLINE_READY_MIN_CLASS_FIXTURES`;
- `SHADOW_ML_OFFLINE_READY_MAX_NEXT15_UNKNOWN_FRACTION`;
- `SHADOW_ML_OFFLINE_READY_MIN_CORE_AVAILABILITY`;
- `SHADOW_ML_READINESS_MIN_LOGLOSS_IMPROVEMENT`;
- `SHADOW_ML_READINESS_MIN_BRIER_IMPROVEMENT`.

Useful log markers are `[SHADOW_ML_CONFIG]`, `[SHADOW_ML_TRAIN]`,
`[SHADOW_ML_PREDICTION]`, `[SHADOW_ML_MODEL_INVALID]`, and
`[SHADOW_ML_*_ERROR]`. Rolling collection uses
`[ROLLING_DYNAMICS_CONFIG]`, `[ROLLING_DYNAMICS_SEED]`, and
`[ROLLING_DYNAMICS_HYDRATE]`.
