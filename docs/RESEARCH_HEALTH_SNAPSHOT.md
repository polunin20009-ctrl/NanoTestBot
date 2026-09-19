# Read-only research health snapshot

`wide_research.health_snapshot` translates explicitly supplied runtime sources
into the input contract of `wide_research.health.evaluate_health`.

The collector has no defaults for project paths and does not import
`NanoTest.py`. Every discovery profile is described by a
`ProfileSnapshotSpec`: profile ID, SQLite path, latest-summary path, capacity,
interval and optional runtime counters. The active observation journal is
described separately by `ObservationJournalSpec`.

Safety properties:

- SQLite is opened with URI `mode=ro` and `PRAGMA query_only=ON`;
- the profile binding inside SQLite must match the requested profile;
- only `candidate`, `shadow`, `ready`, and `active` phases occupy the live pool;
- `occupied == effective_capacity` is reported as a normal bounded-pool state;
  health severity comes from actual `not_admitted` candidates, a deferred
  transition portfolio, or invalid/over-capacity counters rather than the
  occupancy percentage alone;
- the latest registry's `pool.effective_limit` is used during the bounded
  purge-policy migration; configured capacity and transition reserve remain
  separate report fields, so intentional one-generation overlap is not
  reported as corrupt/full capacity;
- JSON files are bounded and symlinks are rejected;
- only a bounded tail of the active observation journal is inspected;
- no SQLite database, journal, summary, state file, or retry record is written;
- source failures are returned under `sources.errors`; a failed database also
  produces invalid pool counters so the health evaluation fails closed;
- full-time outcomes and durable-retry state remain runtime-owned aggregates
  passed through `outcome_counters` and `retry_counters`.

Example:

```python
snapshot = build_health_snapshot(
    profiles=[
        ProfileSnapshotSpec(
            profile_id="rare_precision_shadow",
            database_path="data/wide_research_rare_precision.sqlite3",
            summary_path="stats/wide_research_rare_precision_discovery.json",
            capacity=64,
            interval_seconds=604800,
            failures=runtime_discovery_failures,
        )
    ],
    observation_journal=ObservationJournalSpec(
        "data/observation_history.jsonl",
        stages=("prefilter", "decision_pipeline", "wide_monitor"),
    ),
    expected_eligible=bool(eligible_fixture_ids),
    eligible_active=len(eligible_fixture_ids),
    outcome_counters=runtime_outcome_health,
    retry_counters=runtime_retry_health,
    disk_path="data",
)
report = evaluate_health(snapshot, now_utc)
```

`expected_eligible` must come from the current fixture loop. This is what
prevents a quiet football schedule from being mistaken for a stalled collector.
The runtime counter requires a valid fixture ID and isolates malformed provider
rows, so health diagnostics cannot interrupt the live decision loop.
