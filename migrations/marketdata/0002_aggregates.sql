-- 0002_aggregates.sql (task 05.02): one row per (symbol, window size, window start), recomputed from
-- the stored ticks and upserted only when input_hash changes (ADR-011, 05.05). Formulas:
-- docs/architecture/market-data-math.md.
-- Rollback: forward-only (00C §9).
-- +goose Up
CREATE TABLE market_aggregates (
  symbol          text NOT NULL,
  window_size     text NOT NULL CHECK (window_size IN ('1m', '5m')),
  window_start    timestamptz NOT NULL,
  open            numeric NOT NULL,
  high            numeric NOT NULL,
  low             numeric NOT NULL,
  close           numeric NOT NULL,
  volume          numeric NOT NULL CHECK (volume >= 0),
  vwap            numeric NOT NULL,                -- the close when volume is 0
  event_count     integer NOT NULL CHECK (event_count > 0),
  moving_avg      numeric,                         -- null until enough previous windows
  volatility      double precision,                -- a statistic of log returns, not a price
  anomaly         boolean NOT NULL DEFAULT false,
  status          text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'final')),
  version         bigint NOT NULL DEFAULT 1 CHECK (version > 0),
  input_hash      text NOT NULL,                   -- SHA-256 of the window's sorted event ids
  last_updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, window_size, window_start) -- also serves "the previous n windows" (indicators)
);
SELECT create_hypertable('market_aggregates', 'window_start', chunk_time_interval => interval '7 days');

-- +goose Down
-- Forward-only (00C §9): no down migration. Tests get a fresh database instead.
