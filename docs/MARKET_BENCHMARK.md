# Live market benchmark

This subsystem records an independent, prospective market benchmark for the
bot's `goal_to90_normal_time` probability. It is permanently shadow-only: it
does not change a probability, publication rule, Telegram badge, or signal.

## What is collected

- One global uncached `GET /odds/live` poll, normally every 60 seconds.
- API-Football live market `25 / Match Goals`, with market `36 / Over/Under
  Line` as a fallback.
- Only an active Over/Under pair on the exact line
  `current score total + 0.5`.
- Decimal odds, raw implied probabilities, overround, and a proportional
  no-vig probability for at least one more normal-time goal.
- Raw market snapshots during minutes 35–90.
- For each full decision-pipeline observation during minutes 46–60: the frozen
  bot p90, static ML p90, rolling ML p90, and the newest quote that was already
  known at observation time. For publication decisions, the observation,
  quote, both ML predictions, and decision timestamp are frozen before the
  Telegram request starts. The decision is fsynced before that request, while
  its delivery result is appended as a separate record after Telegram returns.
- A deterministic fingerprint of the exact predictive ML input. A router
  preview may have a different observation ID, but the report accepts its
  probability only when fixture, minute, and predictive-input fingerprint all
  match the canonical decision.
- The canonical normal-time outcome when it is resolved.

Suspended, blocked, stopped, finished, malformed, stale, future, wrong-score,
and wrong-line quotes are rejected. The feed currently does not expose a
bookmaker identity, so records identify API-Football as the provider without
inventing a bookmaker.

## Runtime isolation

Network polling and durable writes run in separate daemons, so a slow live-odds
request cannot hold up decision or terminal-outcome writes. The main signal
path freezes a small bounded quote-cache entry. Publication decisions use one
small synchronous append before Telegram to close the send/crash survivorship
gap; all routine snapshots, blocked decisions, and outcomes use the isolated
writer. The bounded queue reserves capacity for outcomes; an outcome may evict
a lower-priority decision at hard capacity, but never the reverse. Failed
writes are requeued with backoff and outcome priority. The JSONL journal is
append-only, rotates to gzip, and uses a rebuildable SQLite deduplication index
instead of an unbounded RAM set.

The canonical observation history remains the durable outcome source. The
report automatically joins a sibling `observation_history.jsonl` when present,
so an outcome is still recoverable after a process crash before its derived
market-journal copy was flushed.

## Report

```bash
python3 scripts/report_market_benchmark.py \
  --journal data/market_benchmark.jsonl
```

JSON output:

```bash
python3 scripts/report_market_benchmark.py \
  --journal data/market_benchmark.jsonl \
  --json
```

The report keeps unavailable quotes in the coverage denominator, compares bot,
static ML, rolling ML, and market on the exact same resolved cohort using
LogLoss and Brier score. It exposes both the complete decision cohort and a
separate confirmed-publication cohort containing only `ALLOW` decisions with a
successful Telegram request, positive `message_id`, and valid causal timing.
The same split is present for subsequent same-score/same-line market movement,
which begins after Telegram finishes (or after the frozen decision when no
Telegram delivery is required). A publication with a missing or invalid
delivery record, a failed send, or no confirmed Telegram `message_id` has
unavailable movement rather than a guessed boundary. The report rejects
timestamps without an explicit time zone, counts torn JSON/UTF-8 lines as
malformed instead of aborting, and never reconstructs an old ML prediction
with a newer model.
You can explicitly select the canonical outcome source with
`--outcomes data/observation_history.jsonl`.

## Configuration

Since September 11, the isolated extended rule laboratory also consumes the
normalized quote journal and a separate bounded live cache. This is distinct
from the report's *subsequent* movement: rule features use only quotes known
before each observation, including 5m/10m prior same-score/same-market quotes.
The market scope searches combinations with gameplay, bot and ML probabilities
and automatically freezes candidates for future shadow testing. It does not
change either ML model's inputs or Telegram publication. See the extended
expressions section in `WIDE_RESEARCH.md` for timing and validation contracts.

- `ENABLE_MARKET_BENCHMARK=true`
- `MARKET_BENCHMARK_POLL_SECONDS=60`
- `MARKET_BENCHMARK_MAX_QUOTE_AGE_SECONDS=120`
- `MARKET_BENCHMARK_MIN_MINUTE=35`
- `MARKET_BENCHMARK_MAX_MINUTE=90`
- `MARKET_BENCHMARK_QUEUE_MAX=2000`
- `MARKET_BENCHMARK_OUTCOME_QUEUE_RESERVE=500`
- `MARKET_BENCHMARK_POLL_BUFFER_MAX=1`
- `MARKET_BENCHMARK_WRITE_BATCH=500`
- `MARKET_BENCHMARK_CACHE_QUOTES_PER_FIXTURE=4`
- `MARKET_BENCHMARK_JOURNAL_FILE=data/market_benchmark.jsonl`
- `MARKET_BENCHMARK_INDEX_FILE=data/market_benchmark.jsonl.index.sqlite3`
- `MARKET_BENCHMARK_ROTATE_MAX_BYTES=52428800`
- `MARKET_BENCHMARK_CANONICAL_OUTCOME_FILE=` (report-only override)
