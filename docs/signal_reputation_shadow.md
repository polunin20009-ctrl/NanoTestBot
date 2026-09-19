# Signal reputation and automatic readiness

The signal reputation subsystem starts in shadow mode. It changes live
probabilities only after enough unique resolved Telegram fixtures exist for the
target.

## Data flow

1. `decision_snapshots.jsonl` remains the reproducible source of decisions and
   resolved outcomes.
2. `stats/signal_reputation.json` is an atomically rebuilt aggregate model.
3. `data/signal_reputation_shadow.jsonl` stores the projection made at decision
   time and the later outcome. It rotates at 10 MiB by default and keeps all
   gzip archives.
4. The report script joins projection and outcome records by `decision_id`.

The model has independent `next15` and `to90` targets and independent
`all_decisions` and `telegram_signals` cohorts. Repeated decisions for one
fixture share a total weight of one. Unknown target labels are excluded.

## Reputation layers

The correction is the sum of shrunk residual layers:

- minute and score-state calibration;
- league;
- both teams;
- both teams in the current league;
- home/away roles.

Team-pair statistics are collected but not applied. Results decay with a
90-day half-life. Capped projections are stored concurrently.

## Automatic readiness

Readiness is target-specific for `next15` and `to90` and uses the
`telegram_signals` cohort:

- fewer than 50 unique resolved fixtures: shadow only;
- 50-149 fixtures: apply at most +/-1 percentage point;
- 150-299 fixtures: apply at most +/-2 percentage points;
- 300 or more fixtures: apply at most +/-3 percentage points.

Repeated snapshots from one fixture cannot advance readiness. Automatic
application never advances beyond +/-3 percentage points. The unadjusted base
probability is stored separately and remains the learning baseline, preventing
the correction from training on itself.

## Configuration

```text
ENABLE_DECISION_SNAPSHOTS=true
ENABLE_SIGNAL_REPUTATION_SHADOW=true
ENABLE_SIGNAL_REPUTATION_AUTO_APPLY=true
SIGNAL_REPUTATION_AUTO_STAGE_1_FIXTURES=50
SIGNAL_REPUTATION_AUTO_STAGE_2_FIXTURES=150
SIGNAL_REPUTATION_AUTO_STAGE_3_FIXTURES=300
SIGNAL_REPUTATION_MODEL_FILE=stats/signal_reputation.json
SIGNAL_REPUTATION_SHADOW_FILE=data/signal_reputation_shadow.jsonl
SIGNAL_REPUTATION_REFRESH_SECONDS=900
SIGNAL_REPUTATION_ROTATE_MAX_BYTES=10485760
SIGNAL_REPUTATION_HALF_LIFE_DAYS=90
SIGNAL_REPUTATION_PRIOR_GLOBAL=12
SIGNAL_REPUTATION_PRIOR_LEAGUE=20
SIGNAL_REPUTATION_PRIOR_TEAM=30
SIGNAL_REPUTATION_PRIOR_TEAM_LEAGUE=40
SIGNAL_REPUTATION_PRIOR_ROLE=45
```

## Commands

Rebuild the aggregate model:

```powershell
python scripts/build_signal_reputation.py
```

Report recorded projections and outcomes:

```powershell
python scripts/report_signal_reputation.py
```

The live readiness/application marker is `[SIGNAL_REPUTATION_AUTO]`. Shadow
projections remain available under `[SIGNAL_REPUTATION_SHADOW]`.
