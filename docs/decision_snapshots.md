# Decision snapshots

`data/decision_snapshots.jsonl` is the append-only technical audit log for the
45+ decision pipeline. Each line is one of:

- a `decision` captured during minutes 46–60;
- an `outcome` appended when the normal-time result becomes known.

The pending `outcome` embedded in a decision is only a placeholder. Consumers
must join records by their canonical `decision_id`; they must not count raw
JSONL lines as independent decisions or treat the placeholder as the final
label.

JSONL and its archives remain the source of truth. Runtime indexes, joined
exports, and compact exports are derived views and can always be rebuilt.

## Decision identity and versions

A canonical decision ID identifies one fixture, minute, decision window, and
decision schema, for example:

```text
1589165:54:WINDOW_2:v2
```

New decision schema v2 records use:

```json
{
  "schema_version": 2,
  "model_version": "45_plus_v2",
  "runtime": {
    "config_hash": "…",
    "code_hash": "…",
    "model_version": "45_plus_v2",
    "pressure_version": "v3_smooth",
    "factor_versions": {
      "pressure": "v3_smooth",
      "second_half_parser": "normal_time_v2.3",
      "dynamic_threshold": "minute_bucket_v1",
      "rescue_controller": "v1"
    },
    "values": {}
  }
}
```

`runtime.config_hash` fingerprints the selected decision settings stored in
`runtime.values` together with `runtime.factor_versions`.
`runtime.code_hash` fingerprints the running source version. Together with
`model_version`, these fields make later decisions reproducible and prevent
unrelated formula changes from all appearing as `45_plus_v1`.

Older records remain valid and may not contain `runtime`, may use an older
schema, or may report `model_version=45_plus_v1`. Readers must remain tolerant
of those records; the append-only history is not rewritten in place.

## League and cup identity

New snapshots store:

- `match.league_type`;
- `match.is_cup`;
- `match.is_cup_source`.

The identity resolver uses this precedence:

1. an explicit `Cup` or `League` type supplied with the fixture;
2. the type already registered for `league_id` in `persist_leagues.json`;
3. the existing cup-name heuristic;
4. `Unknown` when the league context is genuinely unavailable.

Typical source values are `api_type`, `persisted_league`, `name_heuristic`, and
`unknown`. An explicit fixture type always wins over persisted metadata. This
fallback fixes ordinary fixture responses that include league ID and name but
do not include the API league type, without silently converting a completely
missing league identity into `League`.

## Shadow reputation storage

The decision log stores only `shadow_reputation_summary` during normal
operation. The summary contains:

- `schema_version`, `shadow_key`, `journal_status`, and
  `model_generated_at_utc`;
- `baseline_decision`;
- for each cohort, `decision_by_cap`;
- compact target values: `base_probability`, `raw_delta_pp`,
  `applied_delta_pp_by_cap`, and `probability_by_cap`.

The complete reputation projection is written separately to
`data/signal_reputation_shadow.jsonl`. `shadow_key` links the compact summary to
that full journal record. This keeps the decision audit compact while retaining
the full diagnostic calculation in its dedicated journal.

If the separate journal write fails, the decision keeps
`shadow_reputation_fallback` so the full calculation is not lost. Legacy
decisions may still contain the old top-level full `shadow_reputation` block.

## Versioned outcomes

Outcome rows are versioned independently from decision rows. Recalculating an
outcome with a newer schema appends a new row; it does not overwrite or delete
the older result.

The joined reader selects the outcome with the highest
`outcome_schema_version`, using `created_at_utc` as the tie-breaker. Therefore,
multiple raw outcome rows for one decision are expected and must not be counted
as duplicate training examples.

- Outcome schema v3 aligns goal events with the score already visible in the
  snapshot and records `outcome.label_alignment_method`.
- Outcome schema v4 keeps normal-time WIN/LOSS resolvable from the score when
  event timing is incomplete, while uncertain 15/25-minute labels remain null
  and carry their source/quality metadata.
- Cancelled, abandoned, awarded, walkover, and postponed fixtures are closed as
  `status=void` and must be excluded from model-quality metrics.

The reconciler treats a decision as fully closed only when it has a terminal
`resolved` or `void` outcome from the current outcome schema. This allows an
older outcome to be upgraded safely after a deployment.

## Lazy runtime index

Fixture outcome reconciliation no longer needs to rescan every JSONL and gzip
archive separately for every fixture. On the first indexed lookup, the bot
builds a lazy in-memory index from all active and archived records. It tracks
decisions by fixture and the newest terminal outcome by canonical decision ID.

Important properties:

- the index is not a second database and is never the source of truth;
- it is updated only after an append to JSONL succeeds;
- rotation does not discard the already built index;
- after a restart it is rebuilt lazily from disk;
- deduplication state and the fixture index remain separate;
- a failed or invalid index can be discarded and rebuilt from JSONL.

This keeps outcome reconciliation proportional to the decisions for the target
fixture instead of repeatedly scanning the entire history.

## Safe joined export

Use the supported exporter instead of parsing only the active file:

```powershell
python scripts\export_decision_snapshots.py
python scripts\export_decision_snapshots.py --resolved-only --output exports\decision_snapshots_joined.jsonl
python scripts\export_decision_snapshots.py --resolved-only --compact --output exports\decision_snapshots_compact.jsonl
```

The exporter always creates a joined view: one row per canonical decision ID.
Different decision minutes for the same fixture intentionally remain separate
rows. It reads the active file, legacy uncompressed archives, and gzip archives,
then selects the newest decision and newest compatible outcome.

Before writing, it validates that:

- every row has a non-empty canonical decision ID;
- `decision_id` and `decision_key` agree when both are present;
- canonical decision IDs are unique in the export;
- the output path is not the active input file or any of its archives.

Output is written to a temporary file, flushed and synchronized, and then
atomically replaced. A failed export therefore cannot partially overwrite a
previous valid export.

`--compact` removes only the legacy full top-level `shadow_reputation` block.
It preserves `shadow_reputation_summary` and all joined outcome data. The
command is an export operation: it does not compact, rewrite, or delete the
source JSONL files and archives.

## Rotation and retention

- `ENABLE_DECISION_SNAPSHOTS` enables the subsystem.
- `DECISION_SNAPSHOTS_FILE` selects the active JSONL path.
- `DECISION_SNAPSHOT_ROTATE_MAX_BYTES` defaults to 10 MiB; zero disables
  rotation.
- On rotation, the old active file is compressed at gzip level 6 and stored as
  `decision_snapshots.<UTC timestamp>.jsonl.gz`.
- Legacy uncompressed `decision_snapshots.*.jsonl` archives remain readable.
- `DECISION_SNAPSHOT_DEDUPE_MAX_KEYS` defaults to zero, meaning complete
  restart-safe deduplication.
- `DECISION_OUTCOME_RECONCILE_EVERY_CYCLES` defaults to 5.
- `DECISION_OUTCOME_RECONCILE_LIMIT` defaults to 20 fixtures per reconciliation.
- `DECISION_OUTCOME_RECHECK_SECONDS` defaults to 300 seconds for unfinished
  fixtures.

Archives are never deleted automatically. The runtime rotator, joined exporter,
and compact exporter all preserve every source archive. Disk retention must be
managed manually if a deletion policy is ever desired.

The reconciler covers both `ALLOW` and `BLOCK` decisions. It skips fixtures
still present in the live feed and queries only pending fixtures that have left
it.
