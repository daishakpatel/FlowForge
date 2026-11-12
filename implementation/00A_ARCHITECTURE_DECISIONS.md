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
**Decision.** Workers long-poll `PollTask` (≤ 30 s). The API claims one `QUEUED` task matching the worker's advertised task types/queues with `SELECT … FOR UPDATE SKIP LOCKED`, inserts the attempt (assigning fencing token), sets RUNNING, all in one transaction. The scheduler only moves tasks PENDING→READY→QUEUED and handles retry/timeouts/recovery; it never talks to workers.
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
