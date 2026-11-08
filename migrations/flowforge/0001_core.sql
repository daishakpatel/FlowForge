-- 0001_core.sql: copied verbatim from 00E §2; change the schema only with a new migration.
-- +goose Up
CREATE TABLE workflow_definitions (
  id            text PRIMARY KEY,                  -- wf_<ulid>
  name          text NOT NULL,
  version       integer NOT NULL CHECK (version > 0),
  definition    jsonb NOT NULL,
  created_by    text NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (name, version)
);

CREATE TABLE cluster_state (                       -- single row; scheduler epoch fence
  id            smallint PRIMARY KEY CHECK (id = 1),
  epoch         bigint NOT NULL DEFAULT 0,
  leader_id     text,
  updated_at    timestamptz NOT NULL DEFAULT now()
);
INSERT INTO cluster_state(id) VALUES (1);

CREATE TABLE workflow_runs (
  id                 text PRIMARY KEY,             -- run_<ulid>
  definition_id      text NOT NULL REFERENCES workflow_definitions(id),
  status             text NOT NULL CHECK (status IN
    ('CREATED','RUNNING','PAUSED','SUCCEEDED','FAILED','CANCELLED','NEEDS_ATTENTION')),
  input              jsonb NOT NULL DEFAULT '{}',
  idempotency_key    text,
  trigger_id         text,                          -- cron trigger that fired it
  parent_run_id      text REFERENCES workflow_runs(id),
  deadline_at        timestamptz,                   -- workflow-level timeout
  cancel_requested   boolean NOT NULL DEFAULT false,
  created_by         text NOT NULL,
  created_at         timestamptz NOT NULL DEFAULT now(),
  started_at         timestamptz,
  completed_at       timestamptz,
  leader_epoch       bigint,
  version            bigint NOT NULL DEFAULT 0      -- optimistic concurrency
);
CREATE UNIQUE INDEX uq_runs_idem ON workflow_runs(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX ix_runs_status_created ON workflow_runs(status, created_at DESC);

CREATE TABLE workflow_tasks (
  id               text PRIMARY KEY,                -- task_<ulid>
  run_id           text NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
  task_key         text NOT NULL,                   -- id inside definition
  task_type        text NOT NULL,
  queue_name       text NOT NULL DEFAULT 'default',
  status           text NOT NULL CHECK (status IN
    ('PENDING','READY','QUEUED','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED','TIMED_OUT','CANCELLED','SKIPPED')),
  depends_on       text[] NOT NULL DEFAULT '{}',    -- task_keys
  run_if           text NOT NULL DEFAULT 'on_success',
  run_if_expr      text,
  config           jsonb NOT NULL DEFAULT '{}',
  input            jsonb,                           -- resolved at READY time
  result           jsonb,
  retry_policy     jsonb NOT NULL,
  timeout_seconds  integer NOT NULL,
  attempt_count    integer NOT NULL DEFAULT 0,
  available_at     timestamptz NOT NULL DEFAULT now(),
  supersedes_task_id text REFERENCES workflow_tasks(id), -- rerun linkage
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  version          bigint NOT NULL DEFAULT 0,
  UNIQUE (run_id, task_key, supersedes_task_id)
);
CREATE INDEX ix_tasks_dispatch ON workflow_tasks(queue_name, task_type, available_at) WHERE status = 'QUEUED';
CREATE INDEX ix_tasks_run ON workflow_tasks(run_id, status);
CREATE UNIQUE INDEX uq_tasks_original_key ON workflow_tasks(run_id, task_key) WHERE supersedes_task_id IS NULL; -- NULLs are distinct in the UNIQUE above (02.01)

CREATE SEQUENCE attempt_fencing_seq;
CREATE TABLE task_attempts (
  id               text PRIMARY KEY,                -- att_<ulid>
  task_id          text NOT NULL REFERENCES workflow_tasks(id) ON DELETE CASCADE,
  attempt_number   integer NOT NULL,
  worker_id        text,
  status           text NOT NULL CHECK (status IN
    ('RUNNING','SUCCEEDED','FAILED','TIMED_OUT','ABANDONED','CANCELLED')),
  fencing_token    bigint NOT NULL DEFAULT nextval('attempt_fencing_seq'),
  idempotency_key  text NOT NULL,                   -- = task_id + ':' + attempt_number
  started_at       timestamptz NOT NULL DEFAULT now(),
  deadline_at      timestamptz NOT NULL,
  last_heartbeat_at timestamptz,
  finished_at      timestamptz,
  error_code       text,
  error_message    text,
  retryable        boolean,
  result           jsonb,
  UNIQUE (task_id, attempt_number)
);
CREATE INDEX ix_attempts_running ON task_attempts(status, deadline_at) WHERE status = 'RUNNING';
CREATE INDEX ix_attempts_worker ON task_attempts(worker_id) WHERE status = 'RUNNING';


-- +goose Down
-- Forward-only (00C §9): no down migration. Tests get a fresh database instead.
