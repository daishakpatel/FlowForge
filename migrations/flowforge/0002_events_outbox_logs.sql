-- 0002_events_outbox_logs.sql: copied verbatim from 00E §2; change the schema only with a new migration.
-- +goose Up
CREATE TABLE workflow_events (
  id             bigserial PRIMARY KEY,             -- ordering for SSE Last-Event-ID
  event_id       text NOT NULL UNIQUE,              -- evt_<ulid>
  run_id         text NOT NULL,
  task_id        text,
  event_type     text NOT NULL,
  event_version  integer NOT NULL DEFAULT 1,
  payload        jsonb NOT NULL,
  correlation_id text NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_events_run ON workflow_events(run_id, id);

CREATE TABLE outbox (
  id           bigserial PRIMARY KEY,
  event_id     text NOT NULL UNIQUE,
  topic        text NOT NULL,
  key          text NOT NULL,
  envelope     jsonb NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX ix_outbox_unpublished ON outbox(id) WHERE published_at IS NULL;

CREATE TABLE task_logs (
  id          bigserial PRIMARY KEY,
  attempt_id  text NOT NULL REFERENCES task_attempts(id) ON DELETE CASCADE,
  task_id     text NOT NULL,
  run_id      text NOT NULL,
  ts          timestamptz NOT NULL,
  level       text NOT NULL CHECK (level IN ('DEBUG','INFO','WARN','ERROR')),
  message     text NOT NULL,
  fields      jsonb
);
CREATE INDEX ix_logs_attempt ON task_logs(attempt_id, id);


-- +goose Down
-- Forward-only (00C §9): no down migration. Tests get a fresh database instead.
