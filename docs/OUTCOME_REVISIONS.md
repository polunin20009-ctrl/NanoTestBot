# Outcome revisions

Outcome journals are append-only. A first terminal label is now written with
`outcome_revision=1`. If API-Football later corrects a score or event list, the
same observation/decision receives revision 2, 3, and so on. Repeated API
responses with identical semantic content do not create another record.

Readers rank outcomes by:

1. `outcome_schema_version`;
2. `outcome_revision` (legacy records are revision 0);
3. `created_at_utc` as a deterministic tie-breaker.

The live reconciler periodically rechecks a bounded recent terminal cohort.
Defaults:

- `ENABLE_OUTCOME_CORRECTION_RECHECK=true`;
- `OUTCOME_CORRECTION_RECHECK_SECONDS=1800`;
- `OUTCOME_CORRECTION_LOOKBACK_HOURS=168`;
- `OUTCOME_CORRECTION_RECHECK_LIMIT=5`.

Corrections are propagated to the static and rolling ML wake-up path, shadow
candidate journal, wide-research SQLite profiles, market benchmark and signal
reputation decision history. This changes data integrity only; it does not
change Telegram publication rules.
