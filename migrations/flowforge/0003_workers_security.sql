-- 0003_workers_security.sql: copied verbatim from 00E §2; change the schema only with a new migration.
-- +goose Up
CREATE TABLE worker_observations (          -- operational snapshot; liveness authority is Raft
  worker_id text PRIMARY KEY, status text NOT NULL, capacity integer NOT NULL,
  active_tasks integer NOT NULL, task_types text[] NOT NULL, queues text[] NOT NULL,
  version text, metadata jsonb, first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE api_keys (
  id text PRIMARY KEY, name text NOT NULL, role text NOT NULL CHECK (role IN
   ('viewer','operator','admin','worker','ai-readonly')),
  secret_hash bytea NOT NULL, salt bytea NOT NULL, created_by text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz, revoked_at timestamptz
);
CREATE TABLE audit_log (
  id bigserial PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now(), actor_key_id text,
  actor_role text, action text NOT NULL, resource_type text, resource_id text,
  request_id text NOT NULL, details jsonb, outcome text NOT NULL
);


-- +goose Down
-- Forward-only (00C §9): no down migration. Tests get a fresh database instead.
