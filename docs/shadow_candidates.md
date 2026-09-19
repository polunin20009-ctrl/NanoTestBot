# Prospective shadow candidate layer

`shadow_candidates` is an autonomous, audit-only comparison layer. It does not
import `NanoTest.py`, send Telegram messages, change the bot probability, train
ML, or mutate any production decision. Every record carries:

```json
{"shadow_only":true,"production_applied":false,"production_effect":"none"}
```

The layer can consume either a `decision_pipeline` observation or a decision
snapshot. Observations are preferred because they contain the frozen 5/10
minute rolling windows.

## Immutable prospective semantics

The ruleset version is `candidate_at_first_control_allow_v3`, with semantics
`candidate_at_first_control_allow`.

For every fixture and ruleset:

1. all snapshot/arm evaluations are written to the audit journal;
2. the first prospective snapshot where the control arm passes is frozen as
   the only candidate snapshot for all five arms;
3. a derived arm that is failed or unavailable at that exact snapshot is
   rejected permanently for this cohort/ruleset;
4. later snapshots are still auditable, but their candidate decision is
   `FROZEN` and they cannot repair or replace the first snapshot;
5. reports count the first `ALLOW` per fixture/arm, never raw JSONL lines.

This prevents hindsight from moving a losing or incomplete candidate to a more
convenient later minute.

`prospective_start_utc` is mandatory and is copied into decisions, outcomes,
and cohort metadata. A source timestamp equal to the boundary is accepted; an
older timestamp is ignored. Keep the same aware UTC boundary across restarts.

## Arms

The first three derived steps are cumulative. After `goals<=2`, close-score
and rolling-ML are two parallel alternatives; ML does not implicitly require
the close-score condition.

| Arm | Exact requirement at the first control snapshot |
|---|---|
| `control_current_filter` | Current `base_rep15_int055_season102_p90_75_v1` publication context and thresholds |
| `full_slices_5m_10m` | Control plus real 5m and 10m window blocks with `status=ok` |
| `full_slices_goals_le2` | Previous arm plus score total `<=2` |
| `full_slices_goals_le2_close` | Previous arm plus `abs(score_home-score_away)<=1` |
| `rolling_ml_confirm_75` | `full_slices_goals_le2` plus causal rolling-ML calibrated `to90 >=75%` |

The full-slice contract also requires schema v1, `mode=shadow_collection`,
`production_applied=false`, both declared windows, valid target/span ranges,
and at least one available activity metric in each window. A missing or
malformed window is `unavailable`, not a pass.

Rolling ML is usable only when all of these are true:

- observation ID, fixture ID, minute, and observation timestamp exactly match;
- prediction append lag is between zero and the configured maximum;
- model creation and data cutoff are valid and strictly before the observation;
- data cutoff is not later than model creation;
- record and target both have `production_applied=false`, and the record is
  explicitly `shadow_only=true`;
- target status is `ok` and calibrated probability is finite.

Missing ML is `unavailable` and therefore a permanent reject at the first
control snapshot. The static ML prediction is recorded for comparison but does
not gate any arm.

## In-process API

Direct integration receives already-created prediction records, so its default
settlement delay is zero:

```python
from shadow_candidates import AppendOnlyCandidateJournal, CandidateLayer

journal = AppendOnlyCandidateJournal(
    "data/shadow_candidates.jsonl",
    rotate_max_bytes=10 * 1024 * 1024,
)
layer = CandidateLayer(
    journal,
    prospective_start_utc="2026-09-07T14:38:39+00:00",
)

layer.process_snapshot(
    observation,
    static_prediction=static_prediction,
    rolling_prediction=rolling_prediction,
)

# Call once with all observation_outcome rows produced for a resolved fixture.
layer.process_outcomes(fixture_observation_outcomes)
```

`process_outcomes` performs one batched trigger lookup and writes updates only
for the selected observation and its at most five arms. `process_outcome` is a
single-record compatibility wrapper.

## One-shot recovery/offline CLI

The CLI is deliberately one-shot. It is intended for recovery or an offline
replay after source and prediction records have settled:

```bash
python3 scripts/run_shadow_candidates.py \
  --prospective-start-utc 2026-09-07T14:38:39+00:00
```

It reads active and timestamped gzip archives, replays safely after a restart,
and writes only `data/shadow_candidates.jsonl` plus its derived index and
lock files. It may build an in-memory prediction lookup during this bounded
one-shot recovery, so it intentionally has no `--follow` mode. Live operation
uses the direct `CandidateLayer` API with prediction records already in hand.
Starting this command is a separate operational action; adding the code alone
has no runtime effect.

## Append-only journal and restart safety

The JSONL journal and gzip archives are the source of truth. Rotation is
lossless and archives are never deleted automatically. Appends are flushed and
`fsync`'d.

Exact event deduplication and first-trigger lookup use a bounded-memory SQLite
sidecar (`.index.sqlite3`). It is only a derived index. Changed JSONL file
signatures are streamed back into the index, which repairs a crash between the
JSONL fsync and index commit. A cross-process file lock serializes claims and
appends. No complete event-key set is retained in RAM.

Recovery procedure:

1. Keep the JSONL and every timestamp gzip archive intact.
2. Stop the single shadow-layer writer if it is running.
3. Move aside a damaged `.index.sqlite3` sidecar, never the JSONL.
4. Run the one-shot CLI with the original `prospective_start_utc`.
5. The index is rebuilt by streaming the journal, while event IDs prevent
   replay duplicates.

Outcome updates are independent append-only rows linked by:

- `ruleset_version`;
- `observation_id`;
- `arm_id`;
- `candidate_decision_event_id`.

Newer source outcome schemas append new update rows instead of rewriting older
history.

## Read-only report

The report can run from the layer's own `shadow_candidate_outcome` rows. When
active/gzip observation history is available, it streams only the selected
observation IDs and chooses the newer source-outcome version; it never
materializes the complete source outcome corpus in RAM. It reports selected,
win, loss, pending, void,
invalid, failed reject, unavailable reject, hit rate, and Wilson 95% interval
for each arm.

```bash
python3 scripts/report_shadow_candidates.py
python3 scripts/report_shadow_candidates.py --format json
python3 scripts/report_shadow_candidates.py \
  --format json --output exports/shadow_candidates.json
```

The reporter refuses to overwrite its active source files, any discovered
source archive, or journal index/lock sidecars. Requested output is written
atomically.
