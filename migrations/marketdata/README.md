# marketdata migrations

goose migrations for the TimescaleDB `marketdata` database (`00E` §12, task 05.02):

| Migration | Creates |
|---|---|
| `0001_ticks.sql` | `market_ticks`, a hypertable on `event_time`, unique on `(event_id, event_time)` (the processor's dedupe key) |
| `0002_aggregates.sql` | `market_aggregates`, a hypertable on `window_start`, one row per `(symbol, window_size, window_start)` |
| `0003_state_replay.sql` | `market_ingest_state` (per consumer group and partition) and `market_replay_jobs` |
| `0004_policies.sql` | Tick compression after 2 days and retention of 14 days |

Run them with `make migrate-up DB=marketdata` or `flowforge-migrate --db marketdata up`. The rules are in [`docs/architecture/database.md`](../../docs/architecture/database.md); the formulas the aggregates use are in [`docs/architecture/market-data-math.md`](../../docs/architecture/market-data-math.md).
