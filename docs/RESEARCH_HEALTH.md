# Research health monitor

`wide_research.health` is an operational monitor for the prospective rule
pipeline. It does not score predictions, promote candidates, alter Telegram
signals, or claim that a rule has reached a target hit rate.

The runtime supplies a read-only aggregate snapshot:

```json
{
  "observations": {
    "expected_eligible": true,
    "eligible_active": 2,
    "last_seen_at": "2026-09-12T08:00:00Z"
  },
  "outcomes": {
    "pending": 3,
    "overdue": 0,
    "oldest_pending_at": "2026-09-12T06:30:00Z",
    "grace_seconds": 10800
  },
  "discovery": {
    "profiles": {
      "primary": {
        "enabled": true,
        "last_completed_at": "2026-09-11T08:00:00Z",
        "interval_seconds": 86400,
        "running_since": null,
        "failures": 0
      }
    }
  },
  "pools": {
    "profiles": {
      "rare_precision_shadow": {
        "occupied": 46,
        "capacity": 64,
        "not_admitted": 0
      }
    }
  },
  "retries": {
    "pending": 0,
    "failed": 0,
    "oldest_pending_at": null
  },
  "disk": {
    "free_bytes": 5000000000,
    "total_bytes": 10000000000
  }
}
```

For outcome-level integration, `outcomes.items` may be supplied instead of the
aggregate counters. Only unresolved items with `finished_at` are counted;
live fixtures and resolved items are ignored. The grace period starts at full
time, which prevents normal API settlement delay from producing an alert.

`evaluate_health(snapshot, now)` returns a JSON-safe report. Observation stalls
are checked only while at least one fixture is expected to be eligible. Each
enabled discovery profile is checked independently. Candidate pools are
intentionally bounded: reaching `occupied == effective_capacity` without any
`not_admitted` candidates is a healthy steady state, not an incident. A pool
warns when the latest discovery could not admit candidates, and becomes
critical when rejections reach the policy threshold, a selected purge
portfolio is deferred, counters are inconsistent, or occupancy exceeds the
effective capacity. Durable retries and optional disk statistics have separate
checks.

`AlertTracker` adds notification behavior:

- warnings require two consecutive unhealthy evaluations;
- critical conditions fire on the first evaluation;
- unchanged alerts are suppressed for six hours;
- recovery requires two consecutive healthy evaluations;
- escalation from warning to critical is emitted immediately.

Tracker state can be persisted using `save_atomic(path)` and restored with
`AlertTracker.load(path)`. This state is a small standalone JSON file and never
modifies research databases. A corrupt or absent state file starts a fresh
tracker, so it cannot permanently silence operational alerts.
