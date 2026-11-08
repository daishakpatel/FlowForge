-- 0004_triggers_approvals.sql: copied verbatim from 00E §2; change the schema only with a new migration.
-- +goose Up
CREATE TABLE cron_triggers (
  id text PRIMARY KEY, name text NOT NULL UNIQUE, workflow_name text NOT NULL,
  workflow_version integer,                          -- null = latest
  schedule text NOT NULL, timezone text NOT NULL DEFAULT 'UTC', input jsonb NOT NULL DEFAULT '{}',
  missed_policy text NOT NULL DEFAULT 'skip' CHECK (missed_policy IN ('skip','catch_up_one')),
  enabled boolean NOT NULL DEFAULT true, next_fire_at timestamptz, last_fired_at timestamptz,
  created_by text NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE approval_requests (
  id text PRIMARY KEY,                               -- apr_<ulid>
  run_id text NOT NULL REFERENCES workflow_runs(id), investigation_id text,
  action_type text NOT NULL CHECK (action_type IN ('retry_task','replay_range','cancel_run','escalate')),
  proposal jsonb NOT NULL, risk text NOT NULL CHECK (risk IN ('low','medium','high')),
  status text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','APPROVED','REJECTED','EXECUTED','FAILED','EXPIRED')),
  requested_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz NOT NULL,
  decided_at timestamptz, decided_by text, decision_reason text,
  executed_at timestamptz, execution_result jsonb
);
CREATE TABLE investigations (                         -- FlowForge-side record of AI results
  id text PRIMARY KEY, run_id text, task_id text, question text NOT NULL,
  status text NOT NULL, result jsonb, created_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz
);

-- +goose Down
-- Forward-only (00C §9): no down migration. Tests get a fresh database instead.
