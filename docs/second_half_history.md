# Second-half history

`data/second_half_history.jsonl` stores one normal-time analytics record per
finished fixture. Schema v2 excludes extra time, shootouts, missed penalties,
cancelled goals and disallowed goals.

The active file rotates at 10 MiB. Closed segments are gzip-compressed as
`second_half_history.<UTC timestamp>.jsonl.gz`; archives are not deleted. The
reader transparently merges archives and keeps the latest schema/quality
revision for each positive `fixture_id`.

## Event quality

- `events_response_available` means the API endpoint returned a response.
- `events_complete` means valid normal-time goal events match the normal-time
  final score.
- `events_available` is a compatibility alias for `events_complete`.
- `events_quality` is `complete`, `incomplete`, or `unavailable`.
- Late-goal metrics and state transitions are `null`/empty unless events are
  complete; unknown data is never treated as zero.

`ft_home` and `ft_away` are scoped to 90-minute normal time. AET/PEN fixtures
without a recoverable normal-time score are rejected.

For matches observed finishing live, `finished_at` is the observation timestamp.
Historical backfill has no API finish timestamp, so it keeps the conservative
kickoff-plus-two-hours estimate and labels it in `finished_at_source`.

## Aggregation

After a new or improved fixture revision is stored, the bot atomically rebuilds
`stats/team_2h_stats.json` and `stats/league_2h_stats.json` by default. Set
`AUTO_AGGREGATE_2H_STATS=false` to disable this. Manual rebuild remains:

```powershell
python scripts\aggregate_second_half_stats.py
```

Team windows contain 20 recent matches and league windows contain up to 200.
Missing event metrics are excluded rather than converted to zero. Cup windows
use explicit API type when available, otherwise a documented name/round
heuristic.

## Migration

Run the schema migration once after upgrading:

```powershell
python scripts\migrate_second_half_history.py
```

It creates a timestamped `pre_migration` gzip backup, removes invalid zero-ID rows,
sanitizes legacy events, reconstructs normal-time scores where safely possible,
writes schema v2 atomically, and rebuilds derived stats. Re-running it is
idempotent.

Configuration:

- `SECOND_HALF_HISTORY_PATH`
- `SECOND_HALF_HISTORY_ROTATE_MAX_BYTES` (default 10 MiB, zero disables)
- `AUTO_AGGREGATE_2H_STATS` (default true)
- `ENABLE_2H_FACTORS` (default true)
- `ENABLE_2H_SOFT_APPLY` (default true)
- `SOFT_APPLY_MAX_ANTIBOOST_PCT` (default 0.04)
- `SOFT_APPLY_MAX_BOOST_PCT` (default 0.05)
