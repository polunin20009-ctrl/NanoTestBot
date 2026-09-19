# Walk-forward evaluation

This is a read-only, offline layer. It never imports `NanoTest.py`, writes to
the live journals, sends Telegram messages, replaces an ML artifact, or changes
the production probability.

The default mode is a real MSK calendar-day walk-forward. For every test day it
trains fresh static and rolling artifacts only from observations satisfying both
of these strict conditions:

- `observation.created_at_utc < fold_cutoff_utc`;
- `outcome.resolved_at_utc < fold_cutoff_utc`.

Before test inference, the outcome block is replaced with `pending`. Models and
rows must share the selected semantic feature contract. The newest semantic
contract through the requested end date is selected by default; it can be
pinned with `--contract-key`.

The default `recorded-production` control contract requires the saved current
filter version, publication context, embedded filter result, and active
publication `ALLOW`. It measures only decisions the current rule could really
have published. The explicit `recomputed-historical` mode instead recomputes
numeric BASE over older compatible frozen observations and is always labelled
as a hypothetical backtest.

The default `first-control` trigger policy freezes the first observation that
passes `control`. Every stricter arm is evaluated only on that exact snapshot.
If either rolling window or the required ML prediction is unavailable then, the
fixture is permanently rejected by that derived arm; later data cannot rescue
it. This mirrors a one-signal live publication decision.

Four pre-registered arms are evaluated, taking at most one frozen snapshot per
fixture:

1. `control`: current BASE rule, `p90 >= 75`, reputation delta `>= 1.5 pp`,
   adjusted intensity `>= 0.55`, season context `>= 1.02`;
2. `both_windows`: control plus frozen 5/10-minute windows both `status=ok`;
3. `both_windows_goals_le2`: previous arm plus at most two current goals;
4. `ml_confirm`: previous arm plus the configured ML probability (fresh
   fold-local prediction by default, or a strictly validated journal replay).

Run a real fold-local report:

```bash
python3 scripts/walk_forward_evaluation.py \
  --from-date 2026-08-14 \
  --to-date 2026-08-22 \
  --ml-mode fold-local \
  --ml-source rolling \
  --ml-confirm-threshold 75 \
  --json-output reports/walk_forward_2026-08-14_2026-08-22.json \
  --text-output reports/walk_forward_2026-08-14_2026-08-22.txt
```

Run the separately labelled historical hypothesis across older rows:

```bash
python3 scripts/walk_forward_evaluation.py \
  --from-date 2026-08-14 \
  --to-date 2026-08-22 \
  --control-contract recomputed-historical \
  --ml-mode disabled
```

`journal-replay` is a different, explicitly labelled mode. It does not retrain
ML inside folds. It accepts only immutable predictions whose model and data
cutoff predate the observation, whose prediction was appended within the
configured lag, whose observation identity/timestamp and shadow-only contract
match exactly, and (by default) whose model was frozen before that MSK day.
This mode is useful for auditing what the shadow model actually emitted, but it
must not be described as fold-local retraining.

Audit the predictions that were actually written prospectively:

```bash
python3 scripts/walk_forward_evaluation.py \
  --from-date 2026-08-14 \
  --to-date 2026-08-22 \
  --ml-mode journal-replay \
  --journal-timing day-frozen \
  --ml-source rolling
```

`--trigger-policy independent-exploratory` is available for research only. It
lets each stricter arm wait for a later eligible observation and therefore can
look materially better than the production-faithful default. Its output is
explicitly marked exploratory.

Every JSON report includes signals, resolved outcomes, hits/misses, hit rate,
95% Wilson interval, coverage versus control, bot/static/rolling log loss and
Brier score, per-day folds, prediction coverage, source diagnostics, and the
exact selected feature contract.
