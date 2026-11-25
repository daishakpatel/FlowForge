-- 0001_ticks.sql (task 05.02): the tick store of the TimescaleDB `marketdata` database (00E §12).
-- Prices and sizes are numeric, never floating point (00A amendment 05.02).
-- Rollback: forward-only (00C §9); drop the database to start again in development.
-- +goose Up
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Timescale requires the partitioning column in every unique index, so uniqueness, and with it the
-- processor's INSERT ... ON CONFLICT DO NOTHING dedupe (ADR-011), is on (event_id, event_time).
-- event_id is deterministic and event_time is part of the event, so one event always maps to one
-- pair. Two events with the same event_id but different event_times are stored as two rows: a
-- producer bug that the quality check reports (05.09).
CREATE TABLE market_ticks (
  event_time      timestamptz NOT NULL,
  symbol          text NOT NULL CHECK (symbol ~ '^[A-Z]{1,6}(\.[A-Z])?$'),
  event_id        text NOT NULL,
  price           numeric NOT NULL CHECK (price > 0),
  size            numeric NOT NULL CHECK (size >= 0),
  source          text NOT NULL,
  sequence        bigint,
  ingested_at     timestamptz NOT NULL,
  kafka_partition integer,
  kafka_offset    bigint,
  late            boolean NOT NULL DEFAULT false, -- stored after its window closed (LateRecompute, 05.04)
  PRIMARY KEY (event_id, event_time)
);
SELECT create_hypertable('market_ticks', 'event_time', chunk_time_interval => interval '1 day');

-- Window recompute reads one symbol's ticks in [start, end) (05.05).
CREATE INDEX ix_ticks_symbol_time ON market_ticks (symbol, event_time DESC);

-- +goose Down
-- Forward-only (00C §9): no down migration. Tests get a fresh database instead.
