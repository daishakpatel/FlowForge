# 00A — Architecture Decision Records

Status of all ADRs below: **Accepted** (2026-11-02). Agents MUST NOT silently deviate; a deviation requires (1) a note in the task completion report, (2) an ADR amendment appended here under "Amendments", and (3) updating `00E`/spec if contracts change.

Format: Context → Decision → Alternatives considered → Consequences.

---

## ADR-001 — Go core, single Go module, monorepo
**Context.** Components are tightly coupled by contracts (proto, schemas, DDL) and the project is reviewed as one system.
**Decision.** One repository, one `go.mod`; Python service and React app live in subdirectories with their own toolchains. Services are separate binaries under `cmd/`; shared code under `internal/` and (sparingly) `pkg/`.
**Alternatives.** Multi-repo (contract drift, painful for agents); Go workspaces with many modules (overhead without benefit).
**Consequences.** Atomic cross-component changes; single CI; need strict package boundaries (enforced by `depguard` lint rules, task 01.01).

## ADR-002 — Use the etcd Raft library (`go.etcd.io/raft/v3`), own the state machine
**Context.** Writing Raft is a high-risk distraction; the value is in storage, FSM semantics, client semantics and failure testing. v1.0 named `go.etcd.io/etcd/raft/v3`; the library has since been published as a standalone module.
**Decision.** Use `go.etcd.io/raft/v3`. Task 04.02 verifies the module path/version via `go list -m`/pkg.go.dev and records it; if only the legacy path is resolvable, use it and note it. Documentation states plainly that the consensus algorithm is the library's.
**Alternatives.** From-scratch Raft (separate learning branch only); embed etcd server (weaker demonstration of depth).
**Consequences.** We must honor the Ready/Advance contract (persist entries+hard state → send messages → apply committed → Advance), implement `Storage`, transport, snapshots, and conf-state ourselves.

## ADR-003 — Authority split: Postgres owns state; Raft owns coordination; fencing guarantees safety
**Context.** Two durable systems invite divergence (v1.0 risk table).
**Decision.** Postgres is the single source of truth for definitions, runs, tasks, attempts. Raft stores only leader/epoch, worker liveness leases, fencing counters, cluster config. Correctness never relies on lease timing: (a) scheduler writes verify `cluster_state.epoch` (row-share lock) equals the leader's epoch; (b) worker results verify `task_attempts.fencing_token` and `status='RUNNING'` in the `UPDATE … WHERE`. Reconciliation rule: if Raft says a worker is lost, the leader marks its RUNNING attempts ABANDONED in Postgres; if Raft is unavailable nothing is reassigned.
**Alternatives.** Store task state in Raft (throughput and size problems); Postgres-only advisory locks for election (loses the Raft demonstration, and Postgres is a SPOF for liveness).
**Consequences.** Two-step recovery (observe in Raft → decide in Postgres) with clear idempotent transitions; a deposed leader cannot corrupt state.

## ADR-004 — Pull-based dispatch with Postgres `SKIP LOCKED` claims
**Context.** v1.0 had both "dispatcher" push and worker poll.
**Decision.** Workers long-poll `PollTask` (≤ 30 s). The API claims one `QUEUED` task matching the worker's advertised task types/queues with `SELECT … FOR UPDATE SKIP LOCKED`, inserts the attempt (assigning fencing token), sets RUNNING, all in one transaction (amended by 03.01: a poll claims up to `free_capacity` tasks; see "ADR-004 amendment — 03.01" below). The scheduler only moves tasks PENDING→READY→QUEUED and handles retry/timeouts/recovery; it never talks to workers.
**Alternatives.** Push dispatch (needs worker connectivity + delivery acks); Kafka-based dispatch (conflates data and control planes); Redis queue (extra source of truth).
**Consequences.** Simple horizontal scaling and natural backpressure; slight claim latency (mitigated by `LISTEN/NOTIFY` waking pollers). The API tier, not the scheduler, hosts the Worker gRPC service.

## ADR-005 — Deterministic Raft FSM; leader-evaluated lease expiry
**Context.** A replicated state machine that reads `time.Now()` diverges across replicas.
**Decision.** The receiving node stamps `ts_ms` into each command; the FSM computes `expires_at = ts + ttl` and never reads a clock. Expiry is *reported* by leader linearizable reads (`now` of the leader at ReadIndex completion) and *materialized* by leader-proposed `LeaseExpire(lease_id, ts)` commands. Clock skew only affects when a lease is *considered* expired, never FSM determinism; fencing covers safety (ADR-003).
**Alternatives.** Logical clocks/tick-counting (hard to tune, drifts under load); TTL via follower local timers (non-deterministic).
**Consequences.** Clients see consistent expiry judgments from the current leader; during leader change, expiry judgments may delay by ≤ election timeout.

## ADR-006 — KV API: CAS, small Txn, TTL leases, counters, ReadIndex reads
**Context.** Get/Put/Delete cannot build safe election/leases.
**Decision.** Implement per-key `version`, CAS, ≤ 8-op Txn, lease grant/keepalive/revoke, named monotonic counters, request-ID dedupe in the FSM, and linearizable reads via `ReadIndex` (default) with an explicit opt-in `SERIALIZABLE` stale read. Watch is **not** implemented (polling at ≤ 1 s is sufficient); list as optional.
**Alternatives.** Full etcd v3 API (scope creep); reads through the log (slow).
**Consequences.** Election and registry are ~100 lines of client code over CAS; read cost = one heartbeat round on the leader.

## ADR-007 — At-least-once execution + idempotency + fencing; never claim exactly-once
**Decision.** Each attempt has an ID, a monotonically increasing fencing token, and an idempotency key (`task_id:attempt_number`) given to handlers. Handlers with external effects MUST use the key. Docs and README state at-least-once semantics.
**Alternatives.** Two-phase commit with external systems (impractical).
**Consequences.** Market-data handlers are written idempotently (event-id dedupe, deterministic recompute).

## ADR-008 — Acyclic definitions; conditional edges and remediation rerun instead of loops
**Context.** v1.0 §9.2 drew Quality Check → AI → Retry → Quality Check, a cycle.
**Decision.** Definitions are strict DAGs. Branching uses `run_if` (`on_success|on_failure|always|expr`). Remediation (retry failed task, replay range) is a **rerun**: new task rows with `supersedes_task_id` pointing at originals; history is immutable. A run in `NEEDS_ATTENTION` resumes when a rerun executes.
**Alternatives.** Cyclic graphs with loop counters (breaks validation, termination proofs, UI); dynamic task generation in handlers (hidden control flow).
**Consequences.** Simple validation and visualisation; reruns appear as extra nodes in the UI chain.

## ADR-009 — Transactional outbox for event publication
**Decision.** State transitions insert `workflow_events` and `outbox` rows in the same transaction. A leader-gated relay publishes with franz-go (idempotent producer, `acks=all`), marks `published_at`, and may re-publish after crashes; `event_id` makes downstream dedupe trivial. SSE reads `workflow_events` directly (not Kafka) so the UI works without Kafka.
**Alternatives.** Dual write (loses events); CDC/Debezium (extra infra); publish-from-transition-code (not atomic).
**Consequences.** Event delivery is at-least-once, ordered per `run_id` key (topic partition by run).

## ADR-010 — Kafka via franz-go (KRaft); JSON envelopes with JSON Schema; no schema registry
**Decision.** `github.com/twmb/franz-go` (pure Go, no cgo) for producer/consumer/admin. Single-broker KRaft in Compose. JSON payloads validated against JSON Schemas stored in-repo; additive-only evolution within a version.
**Alternatives.** confluent-kafka-go (cgo, harder CI); Avro/Protobuf + registry (infra overhead for a demo; JSON aids debugging and AI evidence readability).
**Consequences.** Slightly larger messages; explicit schema validation code on both sides.

## ADR-011 — Stream correctness: persisted dedupe, recompute-from-ticks, commit-after-write
**Decision.** Ticks are inserted with `ON CONFLICT DO NOTHING` on `(event_id,event_time)`. For each affected `(symbol,window)` the processor *recomputes* the aggregate from stored ticks and upserts (`input_hash` stored). Offsets are committed only after the DB transaction commits. Late events within `allowed_lateness` simply trigger a recompute; later ones are not stored: they are counted (`flowforge_stream_late_events_total{disposition}` (`00E §11.1`)) and sent to the DLQ with `reason=late` (FR-KAFKA-09).
**Alternatives.** Incremental in-memory state (lost on crash; needs checkpointing = Flink territory); exactly-once Kafka transactions into a non-Kafka sink (not available).
**Consequences.** Slightly higher DB read cost per batch (bounded by distinct windows per batch); replay-safe by construction.

## ADR-012 — Processor is a long-running service; workflow tasks are bounded controllers
**Decision.** `marketdata-processor` runs continuously (own consumer group `md-live`). Tasks: `market-data-ingest` (generate/load events for a window and produce them, returns `{produced, window_start, window_end, partitions_high_watermarks}`), `market-data-process-window` (waits until the live group has consumed through the recorded offsets and the watermark passes `window_end + allowed_lateness`, or times out), `market-data-quality-check` (completeness/freshness assertions against Timescale). Backfill/replay tasks launch a bounded processor run in-process on a separate group `md-replay-<id>`.
**Alternatives.** Processor as a workflow task (loses streaming semantics); fully external orchestration (loses the story).
**Consequences.** Clear separation; the DAG demonstrates orchestration of a streaming system rather than pretending streaming is batch.

## ADR-013 — Two Postgres-family instances, three logical databases
**Decision.** `postgres` (image `pgvector/pgvector:pg16`) hosting `flowforge` and `aiops`; `timescaledb` (`timescale/timescaledb:latest-pg16` pinned by digest/tag at implementation time) hosting `marketdata`. No cross-database transactions or foreign keys.
**Alternatives.** One instance with all extensions (image coupling); separate vector DB (rejected per v1.0).
**Consequences.** Migrations in separate directories/tools per DB; connection pools sized separately.

## ADR-014 — Data access: pgx v5 + sqlc; migrations: goose
**Decision.** SQL-first with sqlc-generated typed queries (checked in; CI drift check). goose for forward-only SQL migrations (embedded via `embed`). No ORM.
**Alternatives.** GORM (hides locking semantics we must control); raw pgx everywhere (boilerplate).
**Consequences.** Locking and conditional updates are visible in SQL — important for fencing correctness.

## ADR-015 — AuthN/Z: hashed API keys, RBAC roles, service tokens, audit log
**Decision.** Opaque keys `ff_<id>_<secret>`; stored as SHA-256(salt‖secret); roles `viewer|operator|admin|worker|ai-readonly`. Worker and AI services use keys of service roles. All mutations audited. mTLS for Raft/gRPC arrives in hardening (task 09.01).
**Alternatives.** OIDC/JWT (appropriate for production, disproportionate here; documented as future work).
**Consequences.** Simple, testable; keys rotated by admin API.

## ADR-016 — AI boundary: read-only tools over FlowForge API; FlowForge owns approvals and execution
**Context.** v1.0 had the AI service reading Postgres directly, an approvals endpoint on the AI service, and a LangGraph `human_approval` pause — three overlapping owners.
**Decision.** AI tools call FlowForge's read-only internal API (service token `ai-readonly`), Prometheus HTTP API, and Kafka admin (lag) via FlowForge's internal API (single egress). The graph is bounded and **terminates** after emitting `proposals`; the node `human_approval` is renamed `emit_proposals`. FlowForge persists proposals as `approval_requests`, humans decide, FlowForge executes approved actions after re-validating them. No AI write tool exists.
**Alternatives.** Long-lived paused graph with checkpoint resume (stateful, hard to audit, fragile across deploys).
**Consequences.** Auditable, restart-safe; deviation from v1.0 wording is intentional and documented.

## ADR-017 — Frontend stack and contract generation
**Decision.** React + TypeScript (strict) + Vite; TanStack Query; React Flow for DAG; API client generated from OpenAPI (`openapi-typescript` + thin fetch wrapper); live updates via SSE. No global state library unless needed.
**Consequences.** Contract drift caught in CI; UI cannot call undocumented endpoints.

## ADR-018 — Observability stack: Prometheus + Grafana + OTel Collector + Jaeger; logs in `task_logs`
**Decision.** Metrics scraped directly by Prometheus; traces exported via OTLP to Collector → Jaeger; structured stdout logs; per-attempt task logs persisted in Postgres for UI and AI. No Loki/ELK.
**Alternatives.** Tempo/Loki (more components without added learning value here).
**Consequences.** AI log evidence comes from `task_logs` (bounded), not free-form infrastructure logs; documented limitation.

## ADR-019 — Testing approach: testcontainers, Porcupine, Toxiproxy, deterministic fakes
**Decision.** Integration tests use testcontainers-go; Raft histories are checked with Porcupine; network faults use Toxiproxy + `docker pause/kill`; in-process Raft transport has a fault-injecting wrapper (drop/delay/partition) for fast deterministic tests; time uses a fake `clock.Clock` in unit tests.
**Consequences.** CI needs Docker; fast unit suite remains Docker-free.

## ADR-020 — State machines as pure functions + conditional SQL
**Decision.** `internal/workflow/statemachine` exposes `CanTransition(from,to)` and event-producing pure functions; repositories apply transitions with `UPDATE … WHERE id=$1 AND status=$2 [AND version=$3]` and treat 0 rows as `INVALID_STATE`. No transition logic in handlers.
**Consequences.** Exhaustive matrix tests; races resolve in SQL.

## ADR-021 — Time and clock policy
**Decision.** All services run NTP-synced (documented); logic that compares times uses DB `now()` where a single clock is needed (retry `available_at`, deadlines) and the injected clock elsewhere. Raft leases tolerate skew via fencing (ADR-003/005). Workers never decide expiry by local clocks; they enforce deadlines locally *as a courtesy* but the server's decision is final.
**Consequences.** Fewer skew bugs; tests use fake clocks. The Raft FSM never reads a clock: the leader stamps each command with a timestamp and evaluates lease expiry at read time (ADR-005); a leader change can shift that clock base by the NTP skew, which lease TTLs (seconds) must dominate.

---

## Amendments
(Agents append here: `### ADR-0xx amendment — <task id> — <date>` with reason and impact.)

### ADR-013 / ADR-019 amendment — 01.02 — 2026-11-06
- **What:** five dev- and test-only environment variables added to `00E` §6. `FF_DEV_PG_PORT` and `FF_DEV_MD_PORT` override the Compose host ports (defaults stay 5432/5433). `FF_TEST_PG_DSN`, `FF_TEST_MD_DSN` and `FF_TEST_REQUIRE_DOCKER` configure `internal/testutil`; task 01.02 names the first and last, and `FF_TEST_MD_DSN` is the Timescale counterpart.
- **Why:** the task requires the test variables, and a fixed host port fails on machines where 5432 is already in use (it was on the implementing machine).
- **Impact:** compatible extension; no service reads these variables, and no existing contract changes. 01.03 must let `internal/testutil` read `FF_TEST_*` when it bans `os.Getenv` outside `internal/config`.

### ADR-008 / ADR-014 amendment — 02.01 — 2026-11-08
- **What:** migration `0001_core.sql` adds `CREATE UNIQUE INDEX uq_tasks_original_key ON workflow_tasks(run_id, task_key) WHERE supersedes_task_id IS NULL`, recorded in `00E` §2.
- **Why:** `UNIQUE (run_id, task_key, supersedes_task_id)` treats NULLs as distinct, so two original (non-rerun) tasks could share a key in one run. Task 02.01 §5 requires the index.
- **Impact:** compatible extension (a new index); rerun rows (ADR-008) still reuse a key once per superseded task. No column, table or existing constraint changes. Tested by `TestSchema_Constraints`.

### ADR-015 / ADR-017 amendment — 02.04 — 2026-11-11
- **What:** two additions recorded in `00E`.
  - `FF_AUTH_DISABLED` (`00E` §6, default `false`). It lets every request through as the dev admin until 02.05 ships API keys, and config validation rejects `true` when `FF_ENV=prod`.
  - The error envelope's `details[]` entries gain optional `code` and `task_id` fields (`00E` §4), so `INVALID_WORKFLOW` can return each DAG issue's stable code (task 02.04 §4 and §8: `details[].code=CYCLE`).
- **Why:** both are required by task 02.04. `apperrors.Detail` had only `field` and `message`.
- **Impact:** compatible extension. A new optional env var with a safe default, and two optional JSON fields. No existing field, code or route changes. 02.05 keeps `FF_AUTH_DISABLED` as a dev/test-only switch (its §5). Tested by `TestLoad_AuthDisabledRejectedInProd` and `TestContract_API`.

### ADR-015 amendment — 02.05 — 2026-11-11
- **What:** two additions recorded in `00E`.
  - `FF_API_RATE_LIMIT_RPS` (`00E` §6, default 50). It sets the per-key token bucket; the burst is 2×, and the `worker` and `ai-readonly` service roles get 4× both.
  - The metric `flowforge_audit_write_failures_total` (`00E` §11.1).
- **Why:**
  - Task 02.05 names the variable and the counter.
  - Deriving the burst and the service-role limits from one variable avoids three more variables for what the task describes as fixed relations.
- **Impact:** compatible extension: a new optional env var with the task's default, and a new counter. Audit writes are best-effort by the task's documented decision (availability over audit completeness; `docs/api/authentication.md`), so an audit outage never fails a request. Tested by `TestLimiter_*`, `TestRateLimit_*` and `TestAudit_WriteFailureIsCountedAndRequestProceeds`.

### ADR-004 amendment — 03.01 — 2026-11-11
- **What:** three changes recorded in `00E`.
  - `00E` §8: `PollTask` returns `tasks[]`, at most `free_capacity` (was "optional Task").
  - `00E` §8: `HeartbeatResponse` adds `server_time` and `stale_attempt_ids`.
  - `00E` §11.1: the metrics `flowforge_grpc_requests_total{method,code}` and `flowforge_grpc_request_duration_seconds{method}`.
- **Why:**
  - Task 03.01 claims `LIMIT $free` and guarantees "never more than `free_capacity` tasks per poll". 03.03's runtime polls with `free_capacity = capacity - active`, which only makes sense if a poll can return several tasks.
  - Task 03.01 §4 specifies the heartbeat fields and a metrics interceptor.
- **Impact:** compatible. The proto did not exist before this task, so no client depended on the singular shape. Fields are additive, and the metrics are new. Tested by `TestClaim_ExactlyOnceUnderRace` and `TestHeartbeat_StaleAndCancelled`.

### ADR-020 / ADR-004 amendment — 03.02 — 2026-11-14
- **What:** three additions recorded in `00E`.
  - `00E` §3.1: the task transition `READY → FAILED`, used when input resolution fails. It is non-retryable, creates no attempt, and its `task.failed` event carries `error_code: INVALID_INPUT`.
  - `00E` §6: `FF_SCHED_HTTP_ADDR` (default `:8081`), which serves the scheduler's `/metrics`, `/healthz` and `/readyz`. Also `FF_QUEUE_LIMITS` (default unset, meaning unlimited): `queue=n` caps on `QUEUED`+`RUNNING` tasks per queue across all runs.
- **Why:**
  - Task 03.02 §4 fails a task whose input cannot be resolved, but `00E` §3.1 had no move from `READY` to `FAILED`.
  - The task names `FF_QUEUE_LIMITS` and an obs server for `flowforge-scheduler`, and `00E` §6 had neither.
- **Impact:** compatible extension. One transition is added and none removed. The default resolver (identity) never fails, so the transition first matters with 03.06's templating. The two new env vars are optional with safe defaults. The `statemachine` transition-table test now includes the new row (a separate suggested commit). Tested by `TestScheduler_InputResolutionFailure`, `TestProcessRun_InputFailureAndLimits`, `TestLoad_QueueLimits` and `TestRun_CompletesDAGRuns`.

### ADR-004 / ADR-007 amendment — 03.03 — 2026-11-14
- **What:** additions recorded in `00E`.
  - `00E` §6: `FF_WORKER_SHUTDOWN_GRACE` (`30s`) and `FF_WORKER_ENABLE_CRASH_HANDLER` (`false`, and rejected when `FF_ENV=prod`), both named by task 03.03. Also `FF_WORKER_HTTP_ADDR` (`:8082`) for the worker's `/metrics`, `/healthz` and `/readyz`.
  - `00E` §11.1: `flowforge_worker_task_outcomes_total{outcome}` and `flowforge_worker_log_lines_dropped_total`.
- **Why:**
  - The task asks for "task outcome counters" and counted log drops, but `00E` named neither.
  - A worker's metrics need a listener, and `00E` §6 had none for the worker.
  - The crash handler kills the process on request, so it must never be enabled in production.
- **Impact:** compatible extension. The env vars are optional with safe defaults, and the metrics are new. The worker reports two new error codes: `WORKER_SHUTDOWN` (retryable) for attempts cancelled by its own shutdown, and `INVALID_RESULT` (not retryable) for a result that isn't valid JSON. Both are handler-level codes in `task_attempts.error_code`, not `00E` §4 API codes. Tested by `TestLoad_WorkerShutdownAndCrashHandler`, `TestWorker_GracefulShutdown`, `TestWorker_FailureClassification` and `TestRun_ExecutesAndShutsDownGracefully`.

### ADR-021 / ADR-007 amendment — 03.04 — 2026-11-15
- **What:**
  - **Clock for a retry's `available_at`.** It is set from the injected clock, as `clock.Now() + NextRetryDelay`, through a new optional `TaskMutation.AvailableAt`. The database's `now()` is no longer used for it. Attempt deadlines are unchanged: the database sets them at claim time.
  - **`00E` §11.1:** the metric `flowforge_loop_errors_total{loop}`, with the new allowed label `loop` (`scheduler`, `timeout`, `recovery`).
  - **`00E` §6:** `FF_FAILPOINTS`, for test builds only. `FF_WORKER_LEASE_TTL` and `FF_WORKER_HEARTBEAT` are now also read by the scheduler.
- **Why:**
  - **Clock.** ADR-021 lists retry `available_at` among the uses of the database clock. Task 03.04 sets it from `clock.Now()` and requires every time compared in SQL to be a parameter from the injected clock, with tests driving a `clock.Fake`. The task file outranks `00A` (`00C` §2.12). This also matches 03.01's claim query, which already compares `available_at <= $now` on the API clock, so promotion and claiming now share one time base.
  - **Metric and label.** The task names the metric, and `loop` was not an allowed label.
  - **Variables.** Recovery's fallback bound is 3×heartbeat + lease TTL.
- **Impact:**
  - Compatible. Without `AvailableAt` the old database-clock behavior is unchanged.
  - In production every service runs on an NTP-synced host clock, so the two time bases differ only by skew. Retry delays are in seconds, so that skew cannot reorder them.
  - Tested by `TestRetry_BackoffOnTheFakeClock`, `TestRetryOutcomeHandler`, `TestRunTicking_BacksOffAndRecovers` and `TestLoops_SurviveDatabaseOutage`.

### ADR-020 / ADR-007 amendment — 03.05 — 2026-11-15
- **What:**
  - **`00E` §3.3:** three run transitions, CREATED → CANCELLED, PAUSED → CANCELLED and PAUSED → FAILED.
  - **`00E` §6:** `FF_CANCEL_GRACE` (`30s`, read by the API).
  - **Event payload:** `run.failed` carries an optional `reason` (`WORKFLOW_TIMEOUT`).
  - **Worker-reported cancellation:** `FailTask(error_code=CANCELLED)` now ends the attempt `CANCELLED` instead of `FAILED`.
  - **Mechanism:** a cancel request shortens the run's existing `deadline_at` to `now + FF_CANCEL_GRACE`, or to `now` with `force`. `TimeoutLoop` force-cancels at the run deadline. No new column.
- **Why:**
  - Task 03.05 cancels from CREATED and PAUSED and enforces the workflow deadline while PAUSED, and `00E` had none of those moves.
  - It also specifies attempt and task `CANCELLED` for a worker that honours a cancel.
  - Reusing the deadline gives the timeout and the cancel grace a single enforcement point. It avoids a migration and a change to the tests that pin the migration count.
- **Impact:**
  - **Compatible:** transitions and an optional field are added and nothing is removed; existing runs behave as before.
  - **Breaking-looking but not breaking:** a cancelled run's `deadline_at` reads as the grace's end, not the original workflow deadline. Nothing depends on the original after a cancel.
  - **Changed tests:** `transitions_test.go` (the `00E` copy and one event case) and 03.03's `TestWorker_ServerCancellation`, which now expects an attempt CANCELLED instead of FAILED.
  - **Tested by:** `TestCancel_*`, `TestPause_RunningFinishesNothingNewUntilResume`, `TestWorkflowTimeout_FailsRunAndCancelsTasks`, `TestProcessRun_CancelRequested`, `TestTimeoutLoop_EndsOverdueRuns` and `TestContract_API`.


### ADR-008 / ADR-020 amendment — 03.06 — 2026-11-15
- **What:**
  - **`00E` §3.3:** run transition FAILED → RUNNING, used only by a remediation rerun. `completed_at` is cleared on any move back to RUNNING.
  - **`00E` §3.4 and validation:** a new issue code `INVALID_REFERENCE`, for a `${…}` placeholder that doesn't parse and for a placeholder or `run_if_expr` path naming an unknown or non-ancestor task. Expression references that were `INVALID_RUN_IF` (02.02) now use it; syntax errors stay `INVALID_RUN_IF`.
  - **`00E` §9:** `GET /runs/{run_id}/tasks` lists head instances by default; `?include_superseded=true` lists every generation.
  - **Event payload:** `run.resumed` gains optional `actor`, `superseded_task_ids` and `rerun_task_ids`, and reuses `reason`.
  - **Scheduler seams:** `InputResolver` and `ExprEvaluator` now receive the task's head **ancestors**, not only its direct dependencies (a superset, so `EvaluateReadiness` is unchanged). An optional `scheduler.RunBinder` lets an evaluator read the run input.
  - **Rerun restarts the workflow deadline** at `now + timeout_seconds`.
- **Why:**
  - Task 03.06 allows a rerun of a FAILED run, which `00E` §3.3 had as terminal.
  - The task names `INVALID_REFERENCE` for every bad reference.
  - It asks for head-only listing by default, and for templates and expressions that read any ancestor and `run.input`.
  - Without the deadline restart, a run that failed on its workflow timeout would fail again at once.
- **Impact:**
  - **Compatible:** a transition, an issue code and optional payload fields are added. Before this task no row could be superseded, so the head-only default lists exactly what it listed before.
  - **Changed existing behavior:**
    - A definition with a non-ancestor expression reference is still rejected, but with code `INVALID_REFERENCE` instead of `INVALID_RUN_IF`.
    - `IsTerminalRun(FAILED)` stays true: only an operator's rerun reopens a FAILED run, and the scheduler never does.
  - **Changed tests:**
    - `dag/validate_test.go`: two expectations now use `INVALID_REFERENCE`.
    - `transitions_test.go`: the `00E` copy, and one event case.
    - The scheduler fake store now copies `run_if_expr` and `config`.
  - **Tested by:** `TestResolve_*`, `TestEval_*`, `TestExpr_RandomInputsNeverPanic`, `TestValidate_Table`, `TestPlanRerun`, `TestProcessRun_ExprAndTemplatesSeeAncestors`, `TestTemplating_*`, `TestConditional_MarketDataFailureBranch`, `TestRerun_*` and `TestContract_API`.

### ADR-009 amendment — 03.07 — 2026-11-17
- **What:**
  - **Relay transaction:** `outbox.RelayLoop` locks a batch of unpublished rows with `FOR UPDATE SKIP LOCKED` and publishes them inside the same transaction, which also asserts the epoch. A `PublishTimeout` (10 s) bounds it. Only the prefix the `Publisher` reports delivered (`PartialError`) is marked.
  - **Publisher selection:** `FF_EVENT_PUBLISHER=log|none|kafka`. The default `log` writes debug lines; `none` discards events but marks them published; `kafka` is a startup error until 05.01.
  - **Event channel:** `events.Recorder.Record` sends `NOTIFY ff_events` (empty payload) for every run event, in its transaction.
  - **SSE stream:** `GET /api/v1/events/stream` reads `workflow_events`. It stays behind a commit horizon: a missing id holds the stream until it commits, or until it is missing for 1 s, or until the row after it is older than 30 s. The envelope comes from the outbox while that row exists and is rebuilt after.
  - **Retention:** a new metric `flowforge_retention_deleted_rows_total{kind}`; the existing allowed label `kind` is reused.
  - **Config:** `FF_SSE_MAX_CONNECTIONS` and `FF_RETENTION_*`, recorded in `00E` §6.
- **Why:**
  - **The relay transaction:** task 03.07 §4 asks for `FOR UPDATE SKIP LOCKED`, which only protects rows while the transaction is open. `00C` §9 says a transaction never spans a network call, but the task file ranks higher (`00C` §2.12). Asserting the epoch in that transaction also stops a deposed leader from marking rows.
  - **The commit horizon:** `workflow_events.id` comes from a sequence assigned at insert, so a cursor that only moves forward would lose an event that commits after a higher id. The task requires no gaps.
  - **The rebuilt envelope:** `workflow_events` stores no envelope, and outbox rows are deleted after 3 days.
- **Impact:**
  - **Compatible:** an endpoint, a NOTIFY channel, config keys, a metric and optional repository methods are added. `persistence.EventView` gains an optional `Envelope`. No table or column changes.
  - **Cost:**
    - A publish holds the outbox row locks and the epoch share lock for up to `PublishTimeout`, so a new leader's epoch raise can wait that long.
    - Transactions that send NOTIFY commit one at a time (a Postgres global lock).
  - **Changed tests:**
    - 02.05's `TestRBAC_Matrix` serves the streaming operation under a short deadline, because a 200 is an endless stream.
    - The 02.04 contract harness registers a `text/event-stream` decoder.
  - **Tested by:** `TestRelay_*`, `TestRelayPG_*` (including the failpoint crash test), `TestStream_*`, `TestStreamPG_*`, `TestHub_*`, `TestLoop_*`, `TestLoopPG_*` and `TestContract_API`.

### ADR-007 amendment — 03.08 — 2026-11-17
- **What:**
  - **Firing:** cron triggers fire through the idempotent run start, with `Idempotency-Key` = `cron:<trigger_id>:<fire time, Unix seconds>` and the run's `trigger_id` set.
  - **Fire loop:** `cron.Loop` locks due triggers with `FOR UPDATE SKIP LOCKED` in an epoch-fenced transaction. It advances `next_fire_at` conditionally on its old value and audits each fire as `system:cron`.
  - **Missed fires:** after `FF_CRON_MISSED_GRACE` (60 s), `skip` drops them and `catch_up_one` starts the oldest once. Catch-up is capped at 24 h.
  - **DST:** a schedule fires at each wall-clock time once. A time in a spring-forward gap fires at the gap's end; a fall-back repeat fires on its first occurrence only.
  - **Additions in `00E`:** `FF_CRON_MISSED_GRACE` (§6) and `flowforge_cron_fires_total{outcome}` (§11.1).
  - **Trigger validation:** errors are `400 INVALID_ARGUMENT` with the field.
- **Why:**
  - The task requires at most one run per fire across crashes and concurrent or failed-over loops. The idempotency key gives that without a new table.
  - Idempotency keys are cleared after 24 h (03.07), which is the catch-up cap.
  - `00E` §4 defines 422 only for `INVALID_WORKFLOW`, so the task's "422/400" uses the existing 400.
- **Impact:**
  - **Compatible:** a config key, a metric, endpoints already listed in `00E` §9, and a failpoint (`cron/after-start-before-advance`). No schema change; `cron_triggers` is from 0004.
  - **Changed tests:** the contract harness's sample path gains `{trigger_id}`.
  - **Tested by:** `TestNextFire`, `TestParse_Rejects`, `TestLoop_*`, `TestCronPG_*` (including the failpoint crash test) and `TestContract_API`.

### ADR-007 amendment — 03.09 — 2026-11-18
- **What:** recovery's staleness fallback (`ListOrphanedAttempts`) abandons any `RUNNING` attempt silent for 3×`FF_WORKER_HEARTBEAT` + `FF_WORKER_LEASE_TTL`, whatever its worker's lease. Before, it skipped attempts whose `worker_id` held a live lease. `RecoveryRepo.OrphanedAttempts` loses its `alive` parameter.
- **Why:** the 03.09 e2e suite found that a worker killed and restarted under the same id re-registers within its lease. The dead incarnation's attempts then belonged to a "live" worker. The lease never expired, so they stayed `RUNNING` until their deadline, which is an hour by default. A lost PollTask answer leaves the same gap. A live worker heartbeats every attempt it runs, so silence alone means no live process knows the attempt. That makes the lease check redundant for live work and wrong for these cases.
- **Impact:**
  - Compatible: no schema, API or configuration change, and the bound is unchanged.
  - A worker that is alive but cannot heartbeat for the bound loses its attempts, as it already did through lease expiry, which is shorter.
  - Tested by `TestWorkerLoss_FallbackForRestartedWorker` (it failed before the fix: the attempt stayed `RUNNING`), the other `TestWorkerLoss_*` tests, and the e2e scenarios `TestWorkerRestartedWithSameID` and `TestSoakChaos`.

### ADR-006 / ADR-005 amendment — 04.04 — 2026-11-21
- **What:** compatible extensions of the Raft KV contract.
  - **Proto fields:** `GetResponse` and `ListResponse` gain `applied_index`, `leader_now_ms` and `stale`. `ListRequest.start_key` and `ListResponse.next_key` page a listing.
  - **Error metadata:** KV errors carry ErrorInfo metadata `result_code`, the FSM outcome, for example `LEASE_EXPIRED`.
  - **Config:** `FF_RAFT_PROPOSAL_TIMEOUT` (`3s`) and `FF_RAFT_HTTP_ADDR` (`:8083`).
  - **Leadership transfer:** `node.Node.TransferLeadership`.
  - **Paged listing:** `fsm.FSM.ListFrom`.
  - **Server metrics:** `flowforge_raft_requests_total{method,code}`, `flowforge_raft_request_duration_seconds{method}`, `flowforge_raft_read_index_duration_seconds` and `flowforge_raft_leases_swept_total{outcome}`.
- **Why:** each one is something task 04.04 needs.
  - **`applied_index`, `leader_now_ms`, `stale`, `next_key`:** the task requires them on read responses, and the 04.01 proto lacked them.
  - **`result_code`:** separates `LEASE_EXPIRED` (re-grant) from `LEASE_NOT_FOUND` behind the single `NOT_FOUND` code that 00E §4 allows.
  - **`FF_RAFT_PROPOSAL_TIMEOUT`:** the task names it.
  - **`FF_RAFT_HTTP_ADDR`:** the binary must serve its `obs` endpoints somewhere, and 00E §6 had no port for them. `:8083` follows the API (8080), scheduler (8081) and worker (8082).
  - **`TransferLeadership`:** the binary's graceful shutdown needs it.
  - **`ListFrom`:** makes `next_key` possible.
- **Impact:**
  - **Compatible:** fields and methods are added only; no existing field, signature or behaviour changes.
  - **Unchanged config:** `FF_RAFT_PEERS` keeps its format. Nodes still know each other only by peer address, so `leader_hint` stays a node id. The client maps ids to endpoints with `Cluster.Status`.
  - **Tested by:** `TestTransferLeadership_*`, `TestListFrom_ContinuesAtStartKey`, `TestLoad_RaftServerDefaults` and the `internal/raftkv/server` and `internal/raftkv/client` suites.

### ADR-005 / ADR-003 amendment — 04.05 — 2026-11-22
- **What:**
  - **Keep-alive formula (defect fix in 04.03's FSM):** keep-alive now sets `expires_at = max(expires_at, ts + ttl)` instead of `max(expires_at, ts) + ttl`.
  - **Worker key value:** `/workers/{id}` gains `key_id` and `lease_token` (00E §5.2).
  - **Activation order:** the scheduler's leader election activates its epoch in Postgres (`EpochFence`) before announcing leadership.
- **Why:**
  - **Keep-alive:** 00E §5.2, which outranks the 04.03 task file, says keep-alive "extends from its own `cmd.ts_ms`". The old formula added a whole TTL per renewal whenever renewals came faster than the TTL, as they always do (leader renew 3 s against a 10 s TTL; worker heartbeat 5 s against 15 s). Expiry then ran ahead without bound, so a crashed scheduler or worker would keep its lease for minutes or hours after a long run. 04.05's leader-failover test exposed it.
  - **`key_id`:** keeps one worker id per API key.
  - **`lease_token`:** a worker's heartbeats may reach any API instance, which must renew from the key alone.
- **Impact:**
  - **Replicated state:** FSM behaviour changes only for keep-alive. Replicas stay deterministic and the snapshot format is unchanged. A cluster running older binaries must be upgraded together, since mixed versions would compute different expiries; nothing is deployed yet.
  - **Value fields:** the new ones are additive.
  - **Tests:** the 04.03 lease test's expected expiries changed with the formula, and 04.04's expiry test moved its boundary by 1 ms.
  - **Tested by:** `TestLeases_*` (fsm), `TestLeases_ExpiryJudgedByLeaderClockAndSwept`, `TestElector_StepsDownBeforeTheServerExpires`, `TestRegistry_*`.

### ADR-002 / ADR-019 amendment — 04.06 — 2026-11-22
- **What:** compatible extensions for operating the Raft cluster.
  - **`Cluster.Status`** gains `state`, `peers` (match index, lag, reachable), `state_hash`@`state_hash_index`, `snapshot_index`, `first_index` and `uptime_ms`.
  - **`Cluster.Health`** gains `status` (`READY`/`NOT_READY`: within 100 applied entries of the leader's commit, which the node records from appends and heartbeats) and the applied and commit indexes.
  - **A new `Admin` gRPC service** (`TransferLeader`, `SnapshotNow`, `Compact`), gated by `FF_RAFT_ADMIN_TOKEN`.
  - **Config:** `FF_RAFT_SNAPSHOT_ENTRIES` and `FF_RAFT_SNAPSHOT_TRAILING` are mapped.
  - **Node:** `node.Node` gains `SnapshotNow`, `CompactNow` and new `Status` fields. A heartbeat whose commit index is past the node's log is capped at the node's commit.
  - **Compose:** services `raft-1..3` (profile `raft`).
- **Why:**
  - **Status, Health, Admin and config:** task 04.06 asks for them.
  - **The heartbeat cap:** a node restarted with an empty data directory under its old id otherwise panics in the raft library ("tocommit out of range"). The cap only withholds a commit; it never commits anything the leader didn't.
  - **The wipe procedure:** recovering such a node needs a leadership transfer, because raft never lowers a match index (runbook `raft-quorum-loss.md`). It is not safe in general: a wiped node has forgotten its log and vote, which can lose a committed write. It is documented as a guarded last resort (one node, a live leader, the other follower at lag 0); the safe fix is dynamic membership (FR-RAFT-11).
- **Impact:**
  - **Compatibility:** proto fields and the service are added only; existing fields keep their meaning.
  - **Health:** it is now stricter than in 04.04 (it also requires the node to be within the lag bound).
  - **Ports:** the in-container client port is 00E §6's `7000`, published as host `7001`–`7003`; the peer port is `7100`, internal only. The task text had peer 7000 and client 7001–7003; 00E wins.
  - **Tested by:**
    - `TestNode_SnapshotNowAndCompactNow`, `_AutomaticSnapshotCompactsTheLog`, `_StatusReportsStateAndPeerProgress` and `_WipedFollowerSurvivesAndCatchesUp`;
    - `TestStatus_*`, `TestHealth_*`, `TestAdmin_*` and `TestSnapshot_WipedFollowerRecoversViaSnapshot`;
    - `TestAdminAndMetrics_AgainstACluster`;
    - `TestCompose_ClusterServesSurvivesALeaderKillAndTransfers`.


### ADR-003 / ADR-005 amendment — 04.07 — 2026-11-22
- **What:** the Raft-coordinated control plane, as compatible extensions.
  - **Config:** `FF_COORDINATION` (`local`, the default and the previous behaviour, or `raft`) for the API and scheduler; `FF_SCHED_NODE_ID` (default: the hostname). In raft mode `FF_WORKER_LEASE_TTL` must be ≥ 3 s, 3× the Raft election timeout.
  - **Fencing:** `coordination.PGFence` implements `EpochFence` with `ClusterRepo.SetEpochIfGreater` and records `scheduler.leader_changed` in the same transaction. Re-activating the stored epoch for the same node succeeds (a retried activation); any other non-increase is an error and the term does not start.
  - **Worker liveness:** `workerapi.RaftLeaseProvider` implements `LeaseProvider`. Register and Renew write Postgres, then Raft; Raft failures other than a conflict degrade (logged, counted, call succeeds), and Raft calls there are bounded at 2 s. `Expired` reports a worker only when Raft holds no live lease **and** Postgres has not seen it within the TTL; a failed Raft read is an error, so nothing is abandoned.
  - **Events:** `worker.registered` is recorded by `workerapi.Service.Register`.
  - **REST:** `GET /api/v1/cluster/status` (viewer). Its `/internal/v1` alias is left to 06.02, which owns that route group.
  - **Small additions:** `coordination.CurrentLeader` (a non-candidate read of the leader key); the test helper `internal/testutil/epochguard`; `pg.PoolOption`/`pg.SessionParam`.
  - **Bounded fencing (code review):** `PGFence.LockTimeout` (default 2 s, `SET LOCAL lock_timeout`) bounds the activation's wait for a deposed leader's share lock, and the scheduler's sessions set `idle_in_transaction_session_timeout = 10s`, so Postgres ends a frozen leader's open transaction.
  - **Mode switch:** a local-mode scheduler refuses to start when `cluster_state.epoch` ≠ 0 (raft mode ran on that database) and names the reset.
  - **Lease safety (code review):** a heartbeat never revives a worker deregistered meanwhile nor renews another key's lease; `WorkerRegistry`'s best-effort revokes have their own 1 s bound (a 04.05 defect: they ignored the caller's deadline).
  - **Compose:** `flowforge-migrate`, `flowforge-api`, `scheduler-1..3`, `worker-bootstrap`, `worker-1..2` in profiles `raft` and `full`; `postgres` also joins `raft`; `deploy/docker/Dockerfile.service` builds every Go service.
- **Why:**
  - **Task 04.07 §4.**
  - **The Postgres condition in `Expired`** is what keeps a Raft outage from looking like a worker outage. A worker that kept heartbeating has an expired Raft lease until its first heartbeat after quorum returns; without the condition the new leader would abandon it in that window (00_MASTER §6.2: "no spurious reassignment"). It only ever makes recovery more conservative: Raft expiry stays necessary.
  - **Postgres in the `raft` profile:** Compose cannot require two profiles, and a `raft`-only project with services depending on `postgres` is invalid, which would break 04.06's `make raft-up`.
- **Impact:**
  - **Defaults:** unchanged, since `local` is the default; the e2e suite passes in both modes.
  - **Deposed leader:** a scheduler deposed mid-decision exits on STALE_EPOCH, as the `Runner` contract already required, and needs a restart policy to come back as a candidate.
  - **Takeover time** is about `FF_LEADER_TTL` + `FF_LEADER_RENEW`, so the 10 s target needs `FF_LEADER_TTL` ≤ about 7 s; the 00E defaults (10 s / 3 s) are unchanged and the Compose demo uses 6 s / 2 s.
  - **Tested by:**
    - `TestRaftLease_*`, `TestFencing_DeposedLeaderCannotCommit`, `TestEpochGuard_*` and `TestClusterStatus_*`;
    - `TestRegister_RecordsWorkerRegistered` and `TestLoad_Coordination`;
    - the `TestRaft_*` e2e tests.
