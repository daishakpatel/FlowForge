-- 0003_state_replay.sql (task 05.02): processor bookkeeping and replay jobs. Neither is
-- authoritative for offsets: Kafka group commits are (ADR-011).
-- Rollback: forward-only (00C §9).
-- +goose Up

-- One row per consumer group and partition: the last offset handled and the latest event time seen,
-- for monitoring and the watermark (05.04).
CREATE TABLE market_ingest_state (
  consumer_group text NOT NULL,
  topic          text NOT NULL,
  partition      integer NOT NULL CHECK (partition >= 0),
  last_offset    bigint NOT NULL CHECK (last_offset >= 0),
  max_event_time timestamptz,                      -- null until the partition has an event
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, topic, partition)
);

-- Bounded replay and backfill jobs (05.07). The id is the replay id and its idempotency key:
-- re-submitting returns the existing job.
CREATE TABLE market_replay_jobs (
  id             text PRIMARY KEY,
  mode           text NOT NULL CHECK (mode IN ('offset', 'timestamp', 'time_range')),
  spec           jsonb NOT NULL,                   -- the ReplaySpec (05.07)
  status         text NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')),
  progress       jsonb NOT NULL DEFAULT '{}',      -- per-partition positions
  events_read    bigint NOT NULL DEFAULT 0 CHECK (events_read >= 0),
  events_written bigint NOT NULL DEFAULT 0 CHECK (events_written >= 0),
  error          text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  started_at     timestamptz,
  finished_at    timestamptz,
  updated_at     timestamptz NOT NULL DEFAULT now()
);
-- The runner and the API list the jobs still to do or in progress.
CREATE INDEX ix_replay_jobs_active ON market_replay_jobs (created_at) WHERE status IN ('pending', 'running');

-- +goose Down
-- Forward-only (00C §9): no down migration. Tests get a fresh database instead.
