# 00E — Canonical Contracts Reference

Single source of truth for names, schemas, enums and wire contracts. **Tasks must conform to this file.** If a task needs to change anything here it MUST update this file in the same change and record a deviation in its completion report. Section numbers are referenced by the master spec and by task files.

---

## 1. Conventions

- Go module path: `github.com/daishakpatel/flowforge`.
- IDs: ULID with prefix. DB columns are `text` holding the prefixed form (`run_01J…`) unless stated; generated in Go (`internal/ids`). Worker/attempt/fencing integers are `bigint`.
- Time: `timestamptz`, UTC, microsecond precision. Go code takes a `clock.Clock` interface (real + fake).
- JSON field naming: `snake_case`. Enum values: UPPER_SNAKE for states; lower_snake for event types (`task.attempt.failed`).
- Env var prefix `FF_`; nested with `_`. See §6.

## 2. PostgreSQL `flowforge` — core DDL (migration `0001`–`0004`)

```sql
-- 0001_core.sql
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

-- 0002_events_outbox_logs.sql
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

-- 0003_workers_security.sql
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

-- 0004_triggers_approvals.sql
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
```

Indexing/retention: `workflow_events`, `task_logs`, `outbox` (published) are pruned by a leader-run retention job (task 03.07) — defaults 30 d / 14 d / 3 d.

## 3. State machines

### 3.1 Task state transitions (only these are legal)

| From | To | Trigger / actor |
|---|---|---|
| PENDING | READY | all `depends_on` terminal and `run_if` satisfied (scheduler) |
| PENDING | SKIPPED | `run_if` not satisfied (scheduler) |
| PENDING | CANCELLED | run cancelled |
| READY | QUEUED | input resolved, concurrency permits (scheduler) |
| READY / QUEUED | CANCELLED | run cancelled |
| QUEUED | RUNNING | worker claim (API/PollTask) — creates attempt |
| RUNNING | SUCCEEDED | `CompleteTask` with valid fencing token |
| RUNNING | RETRY_WAIT | attempt FAILED/TIMED_OUT/ABANDONED and retries remain and error retryable |
| RUNNING | FAILED | attempt failed non-retryable or attempts exhausted |
| RUNNING | TIMED_OUT | attempt timed out and attempts exhausted (terminal) |
| RUNNING | CANCELLED | forced cancel / cancel acknowledged |
| RETRY_WAIT | QUEUED | `available_at <= now` (scheduler) |
| RETRY_WAIT | CANCELLED | run cancelled |
| FAILED / TIMED_OUT | — | terminal; remediation creates a *new* task row (`supersedes_task_id`) |

`SUCCEEDED`, `FAILED`, `TIMED_OUT`, `CANCELLED`, `SKIPPED` are terminal.

### 3.2 Attempt states
`RUNNING → SUCCEEDED | FAILED | TIMED_OUT | ABANDONED | CANCELLED`. Terminal; completion succeeds only when `status='RUNNING' AND fencing_token = $token`.

### 3.3 Run states
CREATED → RUNNING (first task queued) → SUCCEEDED (all tasks SUCCEEDED or SKIPPED, no non-handled failure) | FAILED (a task failed with no `on_failure` branch handling it, or workflow timeout) | CANCELLED | NEEDS_ATTENTION (an `AI_INVESTIGATION`/approval is pending or workflow policy `on_failure: needs_attention`). RUNNING ⇄ PAUSED. NEEDS_ATTENTION → RUNNING (rerun/approval executed) | FAILED | CANCELLED.

### 3.4 `run_if` semantics
`on_success` (all deps SUCCEEDED), `on_failure` (≥1 dep FAILED/TIMED_OUT, all deps terminal), `always` (all deps terminal), `expr` (all deps terminal and boolean expression over `tasks.<key>.status|result` evaluates true; expression grammar in task 03.06). A failed task whose failure is "handled" by at least one `on_failure`/`always` dependent does not by itself fail the run; the run result is FAILED iff some terminal-failed task has no handling dependent that SUCCEEDED.

### 3.5 Workflow definition JSON (v1)

```json
{
  "name": "market-data-hourly", "version": 1,
  "timeout_seconds": 3600,
  "on_failure": "fail",                       // fail | needs_attention
  "concurrency": {"max_parallel_tasks": 8},
  "tasks": [{
    "id": "fetch", "type": "market-data-ingest", "queue": "market-data",
    "depends_on": [], "timeout_seconds": 120,
    "config": {"symbols": "${run.input.symbols}", "window_minutes": 5},
    "retry": {"max_attempts": 3, "strategy": "exponential",
              "initial_delay_ms": 1000, "max_delay_ms": 60000, "multiplier": 2.0, "jitter": 0.2,
              "non_retryable_errors": ["INVALID_INPUT"]},
    "run_if": "on_success"
  }]
}
```
Limits: ≤ 500 tasks, ids `^[a-z][a-z0-9_-]{0,62}$`, `timeout_seconds` 1–86400, `max_attempts` 1–20.

## 4. Error code catalog (REST/gRPC)

| Code | HTTP | gRPC | Meaning |
|---|---|---|---|
| INVALID_ARGUMENT | 400 | INVALID_ARGUMENT | Malformed/invalid request |
| INVALID_WORKFLOW | 422 | INVALID_ARGUMENT | DAG validation failed (`details[]` list) |
| UNAUTHENTICATED | 401 | UNAUTHENTICATED | Missing/invalid key |
| PERMISSION_DENIED | 403 | PERMISSION_DENIED | Role insufficient |
| NOT_FOUND | 404 | NOT_FOUND | |
| CONFLICT | 409 | ALREADY_EXISTS / ABORTED | Duplicate or version conflict |
| INVALID_STATE | 409 | FAILED_PRECONDITION | Illegal state transition |
| STALE_ATTEMPT | 409 | FAILED_PRECONDITION | Fencing token mismatch / attempt no longer RUNNING |
| PAYLOAD_TOO_LARGE | 413 | RESOURCE_EXHAUSTED | |
| RATE_LIMITED | 429 | RESOURCE_EXHAUSTED | `Retry-After` set |
| UNAVAILABLE | 503 | UNAVAILABLE | Dependency down; `Retry-After` set |
| INTERNAL | 500 | INTERNAL | Bug; message sanitized |
| NOT_LEADER | — | FAILED_PRECONDITION | Raft/leader redirect (internal; carries `leader_hint`) |
| NO_QUORUM | — | UNAVAILABLE | Raft write without quorum |

Error envelope: `{"error":{"code":"…","message":"…","details":[…],"request_id":"req_…"}}`.

## 5. Raft KV contract

### 5.1 Operations (proto `coordination/v1`)
`Get(key, consistency=LINEARIZABLE|SERIALIZABLE)`, `Put(key,value,expected_version?,lease_id?)`, `Delete(key,expected_version?)`, `List(prefix,limit,consistency)`, `CAS(key, expected_version, value|delete)` (expected_version 0 = must not exist), `Txn(compare[], success[], failure[])` (≤ 8 ops), `LeaseGrant(ttl_ms, owner) → lease_id, fencing`, `LeaseKeepAlive(lease_id, token)`, `LeaseRevoke`, `IncrCounter(name) → value`, `ClusterStatus`, `Health`.
Every mutating command carries `{client_id, request_id, ts_unix_ms}`; `ts` is stamped by the **receiving node** before proposal. The FSM dedupes by `(client_id, request_id)` (bounded LRU persisted in snapshot) so retries after timeout are safe. Every key has `version` (per-key modification counter) and `mod_index` (Raft log index).

### 5.2 Key layout

| Key | Value (JSON) | Lease-bound |
|---|---|---|
| `/cluster/scheduler/leader` | `{"node_id","epoch","since_ms"}` | yes (TTL default 10 s, renew 3 s) |
| `/counters/scheduler_epoch` | int (via IncrCounter) | no |
| `/workers/{worker_id}` | `{"capabilities":[],"queues":[],"capacity":n,"version","registered_ms"}` | yes (TTL default 15 s) |
| `/cluster/config` | versioned JSON | no |

Lease semantics: `expires_at_ms = cmd.ts_ms + ttl_ms` stored in FSM; keep-alive extends from its own `cmd.ts_ms`; **the FSM never calls `time.Now()`**. A key bound to an expired lease is *reported* as expired by leader-evaluated reads (`now` = leader wall clock at ReadIndex completion) and physically deleted by a leader-proposed `LeaseExpire(lease_id, ts_ms)` command. Safety never depends on lease timing: all state mutations are fenced in Postgres (§2 `cluster_state.epoch`, `task_attempts.fencing_token`).

### 5.3 Raft defaults
Election tick 10 × 100 ms = 1 s heartbeat tick 1 (100 ms); `PreVote` and `CheckQuorum` enabled; snapshot every 10 000 applied entries (`FF_RAFT_SNAPSHOT_ENTRIES`); trailing entries kept 1 000; max inflight msgs 256; max size per msg 1 MiB; static peers via config.

## 6. Configuration / environment variables

| Var | Default | Used by |
|---|---|---|
| `FF_ENV` | `dev` (one of `dev`, `test`, `prod`) | all |
| `FF_LOG_LEVEL` / `FF_LOG_FORMAT` | `info` / `json` | all |
| `FF_PG_DSN` | `postgres://flowforge:flowforge@localhost:5432/flowforge?sslmode=disable` | api, scheduler, flowforge-migrate |
| `FF_API_HTTP_ADDR` / `FF_API_GRPC_ADDR` | `:8080` / `:9090` | api |
| `FF_ADMIN_BOOTSTRAP_KEY` | (required in prod) | api |
| `FF_RAFT_ENDPOINTS` | `raft-1:7000,raft-2:7000,raft-3:7000` | api, scheduler |
| `FF_RAFT_NODE_ID`, `FF_RAFT_PEERS`, `FF_RAFT_DATA_DIR`, `FF_RAFT_CLIENT_ADDR`, `FF_RAFT_PEER_ADDR` | — | raft |
| `FF_SCHED_TICK` | `500ms` | scheduler |
| `FF_LEADER_TTL` / `FF_LEADER_RENEW` | `10s` / `3s` | scheduler |
| `FF_WORKER_LEASE_TTL` / `FF_WORKER_HEARTBEAT` | `15s` / `5s` | api, worker |
| `FF_WORKER_ID`, `FF_WORKER_QUEUES`, `FF_WORKER_CAPACITY`, `FF_API_GRPC_TARGET`, `FF_WORKER_TOKEN` | — | worker |
| `FF_KAFKA_BROKERS` | `localhost:9092` | relay, worker, processor |
| `FF_MD_PG_DSN` | `postgres://md:md@localhost:5433/marketdata?sslmode=disable` | processor, api(analytics), flowforge-migrate |
| `FF_AI_URL`, `FF_AI_TOKEN` | `http://ai-ops:8000` | worker |
| `AI_*` (Python: `AI_LLM_PROVIDER=mock|…`, `AI_EMBED_PROVIDER`, `AI_FLOWFORGE_URL`, `AI_FLOWFORGE_TOKEN`, `AI_PROM_URL`, `AI_DB_URL`) | — | ai-ops |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4317` | all |
| `FF_DEV_PG_PORT` / `FF_DEV_MD_PORT` | `5432` / `5433` | `docker-compose.yml` host ports (local dev only) |
| `FF_TEST_PG_DSN` / `FF_TEST_MD_DSN` | unset (start containers) | `internal/testutil`: admin DSN (URL form) of an existing server (tests only) |
| `FF_TEST_REQUIRE_DOCKER` | unset (skip without Docker) | `internal/testutil`: fail instead of skip when containers can't start (tests only; CI sets it) |

Config precedence: defaults < config file (optional `FF_CONFIG_FILE`) < env. Secrets never logged (§ redaction). Loader, file format and validation rules: `docs/configuration.md` (task 01.03).

## 7. Kafka contract

| Topic | Key | Partitions (dev) | Retention | Purpose |
|---|---|---|---|---|
| `market.ticks` | symbol | 6 | 7 d | validated ticks |
| `market.invalid` | event_id or symbol | 3 | 14 d | DLQ (reason header) |
| `market.aggregates` | symbol | 6 | 7 d | emitted aggregate updates (informational) |
| `flowforge.workflow-events` | run_id | 3 | 7 d | run lifecycle |
| `flowforge.task-events` | run_id | 3 | 7 d | task/attempt lifecycle (keyed by run for ordering) |

Envelope (JSON Schema `schemas/events/envelope.v1.schema.json`):
```json
{"event_id":"evt_…","event_type":"task.attempt.failed","event_version":1,
 "occurred_at":"2026-11-02T14:30:00Z","producer":"flowforge-api",
 "correlation_id":"run_…","aggregate_id":"task_…","trace":"00-<traceid>-<spanid>-01",
 "payload":{…}}
```
Event types: `run.created|started|paused|resumed|succeeded|failed|cancelled|needs_attention`, `task.ready|queued|started|succeeded|failed|retry_scheduled|timed_out|cancelled|skipped`, `task.attempt.started|succeeded|failed|timed_out|abandoned`, `approval.requested|decided|executed`, `worker.registered|lost`, `scheduler.leader_changed`. Compatibility: additive optional fields only within a version; breaking change = new `event_version` and parallel schema file.

Market tick payload (`market.ticks` value, schema `market_tick.v1`): `{event_id,symbol,event_time,ingested_at,price,size,source,sequence}`; `event_id` for synthetic data = `sha1(source|symbol|sequence|event_time)` hex; symbol regex `^[A-Z]{1,6}(\.[A-Z])?$`; price > 0; size > 0; `event_time` not > now+5 s.

## 8. Worker gRPC (`proto/worker/v1/worker.proto`) — shape

```proto
service WorkerService {
  rpc Register(RegisterRequest) returns (RegisterResponse);          // worker_id, task_types[], queues[], capacity, version
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);       // worker_id, running[{attempt_id, fencing_token, progress}] → {cancel_attempt_ids[], lease_ttl_ms}
  rpc PollTask(PollTaskRequest) returns (PollTaskResponse);          // worker_id, free_capacity, wait_ms (≤30000) → optional Task
  rpc CompleteTask(CompleteTaskRequest) returns (CompleteTaskResponse); // attempt_id, fencing_token, result_json
  rpc FailTask(FailTaskRequest) returns (FailTaskResponse);          // attempt_id, fencing_token, error_code, message, retryable
  rpc ShipLogs(stream LogLine) returns (ShipLogsResponse);           // attempt_id, fencing_token, ts, level, message, fields
  rpc Deregister(DeregisterRequest) returns (DeregisterResponse);
}
message Task { string task_id; string attempt_id; int32 attempt_number; string task_type;
  bytes input_json; google.protobuf.Timestamp deadline; int64 fencing_token;
  string idempotency_key; string correlation_id; string traceparent; }
```
Auth: `authorization: Bearer <worker token>` metadata (role `worker`). Stale token → `FAILED_PRECONDITION` with `STALE_ATTEMPT` detail; workers MUST abort the attempt locally.

## 9. REST routes (OpenAPI source of truth: `api/openapi/flowforge.v1.yaml`)

| Method | Path | Min role |
|---|---|---|
| POST/GET | `/api/v1/workflows` | operator / viewer |
| GET | `/api/v1/workflows/{id}` ; `/api/v1/workflows/by-name/{name}?version=` | viewer |
| POST | `/api/v1/workflows/{id}/runs` (header `Idempotency-Key`) | operator |
| GET | `/api/v1/runs` (filters: status, workflow, since, until, cursor) ; `/api/v1/runs/{run_id}` | viewer |
| POST | `/api/v1/runs/{run_id}/cancel` `…/pause` `…/resume` `…/retry` `…/rerun` | operator |
| GET | `/api/v1/runs/{run_id}/tasks` ; `/api/v1/runs/{run_id}/events` | viewer |
| GET | `/api/v1/tasks/{task_id}` ; `/attempts` ; `/api/v1/attempts/{attempt_id}/logs` | viewer |
| GET | `/api/v1/workers` ; `/api/v1/cluster/status` | viewer |
| CRUD | `/api/v1/triggers` | admin (read: viewer) |
| GET/POST | `/api/v1/approvals` ; `/api/v1/approvals/{id}` ; `POST …/{id}/approve|reject` | viewer / operator |
| POST/GET | `/api/v1/investigations` ; `/{id}` (proxy to AI + record) | operator / viewer |
| GET | `/api/v1/marketdata/ohlcv` ; `/freshness` ; `/lag` | viewer |
| CRUD | `/api/v1/api-keys` | admin |
| GET | `/api/v1/events/stream` (SSE) | viewer |
| GET | `/healthz` `/readyz` `/metrics` `/openapi.json` | none (metrics: network-restricted) |
| GET | `/internal/v1/runs/{id}` `/runs/{id}/tasks` `/tasks/{id}/attempts` `/attempts/{id}/logs` `/workers/{id}` `/cluster/status` `/runs?since=&until=&status=&workflow=` | ai-readonly |

Pagination: `?limit=50&cursor=<opaque>` → `{items:[…], next_cursor}`.

## 10. AI service REST (`/ai/v1`)
`POST /investigations` `{investigation_id?, question, run_id?, workflow_name?, time_range?, requested_by}` → 202 `{investigation_id,status}` (idempotent by id); `GET /investigations/{id}` → status + result; `POST /knowledge/ingest` `{paths[]|documents[]}`; `GET /knowledge/search?q=&tags=&limit=`; `GET /healthz`. **No approvals endpoint** (FlowForge owns approvals — resolution R11).

Investigation result JSON Schema is `services/aiops/schemas/investigation_result.schema.json` (task 06.05); shape = v1.0 §11.7 plus `evidence[]` (`{ref_id, tool, params, retrieved_at, excerpt_hash}`), every `observed_facts[].evidence_ref` ∈ `evidence[].ref_id`, `status ∈ {completed, insufficient_evidence, failed}`, `proposals[]` (`{action_type, params, risk, rationale, requires_approval}` — `requires_approval` is always `true` for write-capable actions and enforced server-side by FlowForge regardless of AI output).

## 11. Observability contract

Metric names (prefix per component; labels low-cardinality only: `queue`, `task_type`, `status`, `outcome`, `node`, `topic`, `partition`, `group`, plus the labels the metrics below use: `route`, `method`, `code`, `peer`, `reason`, `stage`, `kind`, `tool`, `disposition`, `result`, `symbol`; enforced by `obs.AllowedLabels()`, task 01.04):
- API: `flowforge_http_requests_total{route,method,code}`, `flowforge_http_request_duration_seconds`.
- Scheduler: `flowforge_scheduler_is_leader`, `flowforge_scheduler_epoch`, `flowforge_scheduler_tick_duration_seconds`, `flowforge_scheduler_queue_depth{queue}`, `flowforge_tasks_total{status}`, `flowforge_tasks_retried_total`, `flowforge_workflows_started_total`, `flowforge_workflows_completed_total{status}`, `flowforge_workflow_duration_seconds`, `flowforge_task_duration_seconds{task_type}`, `flowforge_outbox_backlog`, `flowforge_outbox_publish_lag_seconds`, `flowforge_stale_attempt_rejections_total`.
- Workers: `flowforge_workers_registered`, `flowforge_worker_active_tasks`, `flowforge_worker_lease_renewal_failures_total`, `flowforge_worker_lease_expired_total`.
- Raft: `flowforge_raft_is_leader`, `_term`, `_commit_index`, `_applied_index`, `_proposals_pending`, `_peer_unreachable{peer}`, `_snapshot_total`, `_apply_duration_seconds`, `_write_duration_seconds`.
- Streaming: `marketdata_events_consumed_total`, `marketdata_events_invalid_total{reason}`, `marketdata_events_deduplicated_total`, `marketdata_events_late_total`, `marketdata_processing_latency_seconds`, `marketdata_consumer_lag{topic,partition,group}`, `marketdata_batch_size`, `marketdata_channel_queue_depth{stage}`, `marketdata_db_write_latency_seconds`, `marketdata_last_event_timestamp_seconds`, `marketdata_consumer_paused`.
- AI: `flowforge_ai_investigations_total{outcome}`, `flowforge_ai_investigation_duration_seconds`, `flowforge_ai_tool_calls_total{tool,outcome}`, `flowforge_ai_retrieval_results`, `flowforge_ai_llm_tokens_total{kind}`.

### 11.1 Metric additions and canonical names (supersede any conflicting name above)
- Streaming metrics use the prefix `flowforge_stream_*` (replacing the `marketdata_*` names): `flowforge_stream_duplicates_total`, `flowforge_stream_late_events_total{disposition}`, `flowforge_stream_lag{partition}`, `flowforge_stream_freshness_seconds{symbol}` (symbol label only when the configured universe is at most 50), `flowforge_stream_backpressure_active`, `flowforge_stream_db_write_seconds`; producer: `flowforge_producer_events_total{result}`, `flowforge_producer_publish_latency_seconds`.
- Outbox: `flowforge_outbox_backlog`, `flowforge_outbox_oldest_age_seconds`, `flowforge_outbox_publish_failures_total`.
- Coordination: `flowforge_leader_changes_total`, `flowforge_raft_leader_changes_total`.
- Tracing: dropped-span counter `flowforge_otel_dropped_spans_total`.
- `metrics/catalogue.yaml` (task 08.02) is generated from this section and is the machine-checked source after that task.

Logs: JSON, fields `ts, level, msg, service, version, request_id, run_id, task_id, attempt_id, worker_id, trace_id, span_id, err`. Spans: `http.<route>`, `scheduler.tick`, `worker.execute`, `kafka.publish`, `kafka.consume`, `processor.batch`, `ai.investigation`, `ai.tool.<name>`.

## 12. Market data DDL (database `marketdata`, migrations `migrations/marketdata/`)

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE market_ticks (
  event_time timestamptz NOT NULL, symbol text NOT NULL, event_id text NOT NULL,
  price double precision NOT NULL, size double precision NOT NULL, source text NOT NULL,
  sequence bigint, ingested_at timestamptz NOT NULL, kafka_partition int, kafka_offset bigint,
  PRIMARY KEY (event_id, event_time)               -- hypertable requires partition col in unique keys
);
SELECT create_hypertable('market_ticks','event_time', chunk_time_interval => interval '1 day');
CREATE INDEX ix_ticks_symbol_time ON market_ticks(symbol, event_time DESC);
-- Dedupe note: event_id embeds event_time determinism for synthetic data; processor uses
-- INSERT … ON CONFLICT (event_id, event_time) DO NOTHING and counts RowsAffected()==0 as duplicate.

CREATE TABLE market_ohlcv (
  symbol text NOT NULL, window_size text NOT NULL CHECK (window_size IN ('1m','5m')),
  window_start timestamptz NOT NULL, open double precision, high double precision, low double precision,
  close double precision, volume double precision, vwap double precision, event_count integer NOT NULL,
  moving_avg double precision, volatility double precision, anomaly boolean NOT NULL DEFAULT false,
  input_hash text NOT NULL, last_updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, window_size, window_start)
);
SELECT create_hypertable('market_ohlcv','window_start', chunk_time_interval => interval '7 days');

CREATE TABLE ingest_watermarks (
  symbol text NOT NULL, partition int NOT NULL, max_event_time timestamptz NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (symbol, partition)
);
```
Formulas: VWAP = Σ(price·size)/Σsize; moving average = mean of last 5 closes of the same window size; volatility = population std-dev of log returns of last 20 closes (null if < 3); anomaly = |z-score(close vs last 20 closes)| > 4 (configurable).

## 13. AI database (`aiops`)
`kb_documents(doc_id pk, title, source_path, version, content_hash, scope, tags text[], updated_at)`, `kb_chunks(chunk_id pk, doc_id fk, section, chunk_idx, content, embedding vector(<dim>), metadata jsonb)` with HNSW index; `investigations(id pk, request jsonb, status, result jsonb, evidence jsonb, created_at, completed_at)`; embedding dimension is config (`AI_EMBED_DIM`, default 384 for the deterministic local embedder).
