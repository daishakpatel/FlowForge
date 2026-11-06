# 00 — FlowForge Master Project Specification (v2.0)

> Supersedes `original/FlowForge_Technical_Specification_and_Roadmap.md` (v1.0, 2026-11-02).
> Companion documents: `00A_ARCHITECTURE_DECISIONS.md` (ADRs), `00B_IMPLEMENTATION_INDEX.md` (roadmap), `00C_AGENT_OPERATING_RULES.md`, `00D_AUDIT_REPORT.md` (what was wrong/missing in v1.0), `00E_CONTRACTS_REFERENCE.md` (canonical schemas, state tables, key layouts, error codes, metric names).
> Date: 2026-11-02. Primary language Go; AI plane Python; dashboard React + TypeScript.

---

## 0. How to read this document

Every requirement has a stable ID (`FR-…`, `NFR-…`) and an **origin tag**:

| Tag | Meaning |
|---|---|
| **E** | *Existing* — present in the original v1.0 spec (preserved; may be sharpened) |
| **A** | *Added* — recommended addition, justified in §2 / `00D_AUDIT_REPORT.md` |
| **O** | *Optional* — enhancement; **not** required for Definition of Done |

Implementation tasks (in `implementation/phase-*/`) each list the requirement IDs they cover. The index (`00B`) proves every non-optional ID is covered by at least one task.

Conventions: all timestamps are UTC (`timestamptz` / RFC 3339). All IDs exposed externally are prefixed ULIDs (`run_…`, `task_…`, `att_…`, `evt_…`, `inv_…`, `apr_…`) except where noted in `00E`. "MUST/SHOULD/MAY" follow RFC 2119.

---

## 1. Summary and scope

**FlowForge** is a distributed workflow runtime and AI operations platform, built as one coherent system:

1. **Control plane (Go)** — REST API, DAG validation, scheduler, recovery manager, worker gRPC service; durable state in PostgreSQL; coordination (scheduler leadership, worker liveness leases, fencing epochs) in a Raft-backed KV store built on the etcd Raft library.
2. **Data plane (Go + Kafka)** — pull-based workers, market-data producer/handlers, a long-running stream processor, TimescaleDB aggregates, replay/backfill, backpressure.
3. **AI plane (Python)** — FastAPI service, RAG over runbooks (pgvector), LangGraph investigation graph with typed **read-only** tools, evidence-grounded structured reports. Approvals and recovery actions are executed only by FlowForge.
4. **Operations surface** — React dashboard, OpenTelemetry/Prometheus/Grafana/Jaeger, runbooks, CI.

**In scope:** everything in §5 tagged E or A. **Out of scope (v1 release):** replacing Temporal/Airflow/Kafka/Flink; building Raft from scratch; HFT or live paid feeds; autonomous production remediation; multi-region; Kubernetes operator; multi-tenancy.

**Honesty rules (portfolio integrity):** FlowForge provides *at-least-once task execution with idempotent handlers and fencing* — never "exactly-once". It uses the etcd Raft library — it does not claim to have invented Raft. No performance claim may be published without a reproducible measurement (`NFR-TEST-04`).

---

## 2. Key design resolutions (changes versus v1.0)

The v1.0 spec was strong on vision but contained internal contradictions and under-specified areas that would force coding agents to guess. These resolutions are binding; rationale lives in `00A` (ADR numbers shown) and `00D`.

| # | Problem in v1.0 | Resolution | ADR |
|---|---|---|---|
| R1 | DAG diagram contains a loop (Quality Check → AI Investigation → Retry → Quality Check) but workflows must be acyclic. | Definitions stay acyclic. Add **conditional tasks** (`run_if`) and a **remediation/rerun** operation that creates a *new* task instance (or child run) from an approved action. | 008 |
| R2 | Two durable stores (Postgres, Raft) with no divergence rule. | **Postgres is the sole authority for workflow/task/attempt state.** Raft holds only *coordination facts* (leader+epoch, worker leases, fencing counters). Safety is enforced by **fencing checks in Postgres writes**, never by trusting lease timing. | 003 |
| R3 | "Dispatcher" push vs. "Poll/Receive" ambiguity. | **Pull model**: workers long-poll `PollTask` over gRPC; control plane claims a task atomically (`FOR UPDATE SKIP LOCKED`) and returns attempt + fencing token. | 004 |
| R4 | Lease expiry in a Raft state machine needs time, but state machines must be deterministic. | Commands carry an absolute leader-assigned timestamp; the state machine never reads the wall clock. Expiry is *evaluated at read time* by the Raft leader (linearizable read). | 005 |
| R5 | KV API (Get/Put/Delete/List) cannot implement election or leases safely. | Add **CAS (compare-and-swap on version)**, **TTL leases**, monotonic **fencing counters**, and **linearizable reads (ReadIndex)**. | 006 |
| R6 | "Scheduler-N paired with Raft-N" implies schedulers are Raft nodes. | Schedulers are **Raft clients**; Raft nodes are separate processes. Leader election = CAS on `/cluster/scheduler/leader` with an epoch. DB writes carry the epoch and are rejected if stale. | 003, 006 |
| R7 | Worker/AI logs are queried by the copilot but no log store exists. | Add **task log storage** (bounded, structured, in Postgres `task_logs`) and use Prometheus HTTP API for metrics. No Loki. | 018 |
| R8 | Workflow events must reach Kafka reliably; DB write + Kafka publish is a dual-write hazard. | **Transactional outbox** table + publisher relay (at-least-once, dedupe by `event_id`). | 009 |
| R9 | Stream processor is a long-running service but "tasks" are bounded. | Processor is a **long-running consumer service**; workflow tasks `market-data-ingest`, `market-data-process-window`, `market-data-quality-check` are *bounded controllers* that produce a window of events, wait for lag-drain/watermark, and verify output. | 012 |
| R10 | In-memory dedupe loses state on restart; aggregate "versioning" undefined. | Dedupe is **persisted** (unique `event_id` on `market_ticks`); aggregates for touched windows are **recomputed from stored ticks** (deterministic, idempotent, replay-safe, never overwrites newer data with stale data). Offsets are committed to Kafka **after** DB commit. | 011 |
| R11 | AI service reads Postgres directly (diagram) but ownership table says FlowForge owns workflow state; approvals exist in both FlowForge and AI endpoints. | AI service accesses FlowForge **only via a read-only internal API with a service token**; AI owns only its `aiops` database (pgvector). **FlowForge owns approvals.** The v1.0 "LangGraph pauses for human" is replaced by the graph **emitting proposals**; FlowForge persists them as approval requests and executes approved actions. | 016 |
| R12 | "Scheduled run" is the headline use case but cron triggers were only a stretch goal. | Minimal **cron triggers** are required (`FR-WF-14`). | — |
| R13 | No authN/authZ, audit, rate limits, request limits designed. | API keys + RBAC (`viewer`/`operator`/`admin`) + service tokens + audit log + limits (`FR-SEC-*`). | 015 |
| R14 | No OpenAPI / event schemas; frontend and AI service would drift. | OpenAPI 3.1 is the REST contract (generated TS client); JSON Schemas for Kafka events; protobuf for gRPC. | 010, 017 |
| R15 | Repo layout lists `go.etcd.io/etcd/raft/v3`; the library now lives in a standalone module. | Use **`go.etcd.io/raft/v3`** (standalone). Task 04.02 must verify the module path/version and record it. | 002 |
| R16 | Multi-database ambiguity (Postgres/Timescale/pgvector). | Two Postgres-family instances: `postgres` (pgvector image; DBs `flowforge`, `aiops`) and `timescaledb` (DB `marketdata`). No cross-DB transactions. | 013 |

---

## 3. Architecture

### 3.1 System context

```mermaid
flowchart TB
  Operator(["Operator"]) --> UI["React Dashboard"]
  UI -->|REST + SSE| API
  CLI(["flowforgectl / curl"]) -->|REST| API

  subgraph CP["CONTROL PLANE (Go)"]
    API["flowforge-api<br/>REST + Worker gRPC + Internal read API"]
    SCH["flowforge-scheduler (N instances)<br/>leader-only loops: scheduler, recovery, cron, outbox relay"]
  end

  subgraph COORD["COORDINATION"]
    R1[("raft-1")] --- R2[("raft-2")] --- R3[("raft-3")]
    R1 --- R3
  end

  PG[("PostgreSQL<br/>db: flowforge")]
  API --> PG
  SCH --> PG
  SCH -->|CAS leader+epoch, worker leases| COORD
  API -->|worker heartbeat -> lease renew| COORD

  subgraph DP["DATA PLANE"]
    W["flowforge-worker (N)"]
    KAFKA[("Kafka (KRaft)")]
    PROC["marketdata-processor"]
    TS[("TimescaleDB<br/>db: marketdata")]
  end
  W -->|PollTask / Heartbeat / Complete (gRPC)| API
  W --> KAFKA
  KAFKA --> PROC --> TS
  SCH -->|outbox relay| KAFKA

  subgraph AI["AI PLANE (Python)"]
    AIAPI["ai-ops FastAPI"] --> LG["LangGraph graph"]
    LG --> RET["LangChain retriever"] --> VEC[("pgvector<br/>db: aiops")]
    LG --> TOOLS["typed read-only tools"]
  end
  W -->|AI_INVESTIGATION handler (HTTP)| AIAPI
  TOOLS -->|service token| API
  TOOLS --> PROM["Prometheus API"]
  TOOLS --> KAFKA
```

### 3.2 Component responsibilities and ownership

| Component | Owns (authoritative) | Must NOT |
|---|---|---|
| `flowforge-api` | REST surface, authN/Z, request validation, worker gRPC service (register/poll/heartbeat/complete/fail/log), internal read API, SSE | Make scheduling decisions; hold coordination state |
| `flowforge-scheduler` | Leader election client, DAG readiness evaluation, retry/timeout evaluation, recovery of lost workers, cron firing, outbox relay — **all leader-gated** | Accept external requests; run tasks |
| `flowforge-raft` (node) | Replicated KV (leader key, worker leases, fencing counters, cluster config) | Store workflow history, logs, market data |
| PostgreSQL `flowforge` | Definitions, runs, tasks, attempts, events, outbox, approvals, logs, API keys, audit, cron triggers | Coordination leases |
| `flowforge-worker` | Executing task handlers; heartbeats; reporting results | Global scheduling; deciding retries |
| Kafka | Event streams, offsets, replay log | DAG state |
| `marketdata-processor` | Validation, dedupe, windowing, writing ticks+aggregates | Workflow-level retries |
| TimescaleDB | Ticks, aggregates, ingestion watermark | Workflow state |
| `ai-ops` service | Investigations, KB, retrieval, graph execution, proposals | Executing recovery actions, approvals, durable workflow scheduling |
| Dashboard | Presentation only | Business logic |

### 3.3 Run lifecycle (request flow)

```mermaid
sequenceDiagram
  actor Op as Operator
  participant API as flowforge-api
  participant PG as Postgres
  participant S as Scheduler (leader)
  participant W as Worker
  Op->>API: POST /workflows/{id}/runs (Idempotency-Key)
  API->>PG: INSERT run + task rows (PENDING) + outbox(run.created) [1 tx]
  API-->>Op: 201 run_id
  loop every tick or NOTIFY
    S->>PG: tx: verify epoch; PENDING→READY when deps SUCCEEDED; READY→QUEUED
  end
  W->>API: PollTask(queues, capacity) [long-poll]
  API->>PG: tx: claim QUEUED task SKIP LOCKED; create attempt(fencing_token); QUEUED→RUNNING
  API-->>W: Task{task_id, attempt_id, fencing_token, deadline, input}
  W->>API: Heartbeat(attempt_id, token) ... (renews worker lease)
  W->>API: CompleteTask(attempt_id, token, result)
  API->>PG: tx: UPDATE ... WHERE attempt.id=? AND fencing_token=? AND status='RUNNING'; outbox(task.succeeded)
  S->>PG: downstream tasks become READY ... run SUCCEEDED
```

### 3.4 Failure and fencing (worker loss)

```mermaid
sequenceDiagram
  participant W1 as Worker-1
  participant API as API
  participant R as Raft KV
  participant S as Scheduler(leader)
  participant PG as Postgres
  participant W2 as Worker-2
  W1->>API: Heartbeat (renews /workers/w1 lease)
  Note over W1: process pauses / partition
  S->>R: linearizable List(/workers/) (leader-evaluated expiry)
  R-->>S: w1 expired
  S->>PG: tx(epoch ok): attempt RUNNING→ABANDONED; task RUNNING→RETRY_WAIT|FAILED per policy
  W2->>API: PollTask → new attempt (fencing_token N+1)
  W1->>API: CompleteTask(token N)  [zombie wakes up]
  API->>PG: UPDATE ... WHERE fencing_token=N AND status='RUNNING' → 0 rows
  API-->>W1: FAILED_PRECONDITION (STALE_ATTEMPT)
```

### 3.5 Scheduler leadership

Leader key `/cluster/scheduler/leader` = `{node_id, epoch}`. Candidate acquires via CAS with a TTL lease; `epoch` comes from the Raft-backed fencing counter `/counters/scheduler_epoch` (monotonic). On winning, the new leader runs `UPDATE cluster_state SET epoch=$e WHERE epoch < $e` in Postgres. Every scheduler transaction begins with `SELECT epoch FROM cluster_state FOR SHARE` and aborts if `epoch != own_epoch`. A deposed leader therefore cannot commit decisions even if it still *believes* it leads (GC pause, partition). Loops stop within one renew interval of losing the lease.

### 3.6 Deployment topology (local)

Compose services: `postgres` (pgvector), `timescaledb`, `kafka` (KRaft single broker), `raft-1..3`, `flowforge-api`, `flowforge-scheduler` (×2–3), `flowforge-worker` (×2+), `marketdata-processor`, `ai-ops`, `frontend`, `prometheus`, `grafana`, `otel-collector`, `jaeger`, `toxiproxy` (fault injection). Profiles: `core` (postgres+api+scheduler+worker), `raft`, `streaming`, `ai`, `ui`, `obs`, `full`.

---

## 4. Data architecture

| Database | Instance | Contents | Tooling |
|---|---|---|---|
| `flowforge` | `postgres` | Workflow/task/attempt state, events, outbox, approvals, task logs, API keys, audit, cron | goose migrations, pgx v5 + sqlc |
| `aiops` | `postgres` | KB documents/chunks (pgvector), investigation records, LangGraph checkpoints (if used) | Alembic |
| `marketdata` | `timescaledb` | `market_ticks`, `market_ohlcv`, `ingest_watermarks` | goose (separate dir) |

Canonical DDL, enums, indexes, and state-transition tables are in `00E_CONTRACTS_REFERENCE.md`. No task may change an enum value or table name defined there without updating `00E` and recording a deviation.

---

## 5. Functional requirements

### 5.1 Workflow engine (`FR-WF`)

| ID | Requirement | Org | Acceptance (measurable) |
|---|---|---|---|
| FR-WF-01 | Workflow definitions can be created, versioned (`(name,version)` unique, immutable once created), listed, fetched. | E | Re-posting same name/version → `409 CONFLICT`; new version succeeds. |
| FR-WF-02 | DAG validation rejects duplicate IDs, unknown/self deps, cycles, bad retry/timeout/queue values, missing types, and `run_if` references to non-ancestors. | E | ≥ 25 table-driven cases pass incl. 500-node DAG validated < 100 ms. |
| FR-WF-03 | Runs can be started with input, listed/filtered/paginated, fetched with task summary. | E | API contract tests pass. |
| FR-WF-04 | Task and run state machines are explicit; invalid transitions rejected in code *and* by conditional SQL. | E | Exhaustive transition-matrix test; concurrent double-transition test yields exactly one winner. |
| FR-WF-05 | Scheduler promotes tasks whose dependencies succeeded, enforces per-run and per-queue concurrency limits, never dispatches an active attempt twice. | E | 10k-task fuzz run: zero double-dispatch. |
| FR-WF-06 | Worker protocol: register, heartbeat, long-poll, ack, complete, fail, log, graceful deregister; capability + capacity routing by task type and queue. | E | Worker never receives task type it didn't advertise. |
| FR-WF-07 | Retry policy: max attempts, fixed/exponential backoff with jitter, max backoff, retryable vs non-retryable error classes. | E | Backoff schedule test within ±jitter bounds; non-retryable fails on first attempt. |
| FR-WF-08 | Per-task timeout and workflow-level timeout; timeout produces `TIMED_OUT` attempt and policy-driven retry. | E | Timeout enforced within `timeout + 2×tick`. |
| FR-WF-09 | Recovery: scheduler restart, API restart, worker restart/loss, DB blip do not lose or duplicate-commit work. | E | Crash-recovery E2E suite (task 03.09) green. |
| FR-WF-10 | Cancel (cooperative + forced), pause/resume, and retry-failed-run. Cancellation prevents new work and signals running attempts via heartbeat response. | E | Cancel observed by running worker within 2 heartbeat intervals. |
| FR-WF-11 | Task result passing: downstream tasks can reference upstream results/run input via a constrained template (`${tasks.<id>.result.<path>}`, `${run.input.<path>}`), size-limited (default 256 KiB). | A | Template unit tests incl. missing-path error; oversize rejected. |
| FR-WF-12 | Conditional tasks (`run_if`: `on_success`(default) / `on_failure` / `always` / expression on upstream result) and **remediation rerun** (`POST /runs/{id}/rerun` with `from_task`, creating new task instances linked to the originals) — resolves R1. | A | A workflow with quality-check failure branch executes the failure branch only; rerun creates linked instances, history preserved. |
| FR-WF-13 | At-least-once semantics documented and enforced: unique attempt IDs, per-attempt fencing tokens, idempotency keys passed to handlers, `Idempotency-Key` header on run creation (24 h dedupe window). | E | Stale-attempt completion rejected; duplicate POST returns original run. |
| FR-WF-14 | Cron triggers: create/list/enable/disable/delete triggers (5-field cron + timezone); fired by leader, missed-fire policy (`skip` default / `catch_up_one`), fire dedupe across leader changes. | A | Failover mid-minute does not double-fire or lose the fire. |
| FR-WF-15 | Append-only `workflow_events` history and **transactional outbox** to Kafka (`flowforge.workflow-events`, `flowforge.task-events`). | E+A | Kill relay mid-batch → no loss, duplicates deduped by `event_id` downstream. |
| FR-WF-16 | Task logs: workers ship bounded structured log lines (default ≤ 1000 lines/attempt, ≤ 8 KiB/line) stored in `task_logs`, queryable by attempt and time. | A | Over-limit truncation marker recorded; query p95 < 100 ms for 1000 lines. |

### 5.2 REST API (`FR-API`)

| ID | Requirement | Org | Acceptance |
|---|---|---|---|
| FR-API-01 | Endpoints per `00E §9` (workflows, runs, tasks, attempts, workers, cluster, triggers, approvals, investigations proxy, health). | E | All routes covered by contract tests. |
| FR-API-02 | Stable error envelope `{error:{code,message,details,request_id}}` with documented code catalog. | E | Every non-2xx matches schema. |
| FR-API-03 | OpenAPI 3.1 file is the contract; served at `/openapi.json`; CI fails on drift. | A | `make openapi-check` passes. |
| FR-API-04 | `/healthz` (liveness), `/readyz` (DB + coordination reachable). | E | Readiness flips to 503 when Postgres is down. |
| FR-API-05 | Cursor pagination, filtering, bounded page size (max 200). | A | Pagination tests. |
| FR-API-06 | Server-Sent Events stream `/api/v1/events/stream` for run/task/worker updates (from outbox/events, `Last-Event-ID` resume). | A | UI receives task transition ≤ 2 s. |

### 5.3 Security (`FR-SEC`)

| ID | Requirement | Org | Acceptance |
|---|---|---|---|
| FR-SEC-01 | AuthN: API keys (`ff_<id>_<secret>`, stored as SHA-256+salt hash, shown once), bootstrap admin key from env. | A | Wrong/expired key → 401. |
| FR-SEC-02 | AuthZ RBAC: `viewer` (read), `operator` (+run/cancel/retry/approve), `admin` (+definitions delete, keys, triggers). Service roles `worker`, `ai-readonly`. | A | Role × endpoint matrix test. |
| FR-SEC-03 | Audit log of all mutating calls and approval decisions (who, what, when, request_id). | A | Audit row per mutation, tamper-evident hash chain (O) |
| FR-SEC-04 | Service-to-service auth: worker↔API and AI↔API use per-service bearer tokens; Raft peer/client TLS available (mTLS in hardening). | A | Unauthenticated gRPC rejected. |
| FR-SEC-05 | Input limits: body ≤ 1 MiB, definition ≤ 512 KiB, per-key rate limits, request timeouts. | E+A | Limit tests. |
| FR-SEC-06 | Secrets only via env/secret files; log/event redaction of secret-shaped values; AI treats logs as **untrusted input** (prompt-injection hardening). | E+A | Redaction unit tests; injection eval cases pass. |

### 5.4 Raft coordination (`FR-RAFT`)

| ID | Requirement | Org | Acceptance |
|---|---|---|---|
| FR-RAFT-01 | Raft node using `go.etcd.io/raft/v3` with durable WAL + hard state + snapshots (bbolt behind a `Storage` interface); Ready-loop persists before sending/applying per library contract. | E | Restart-all test retains committed state. |
| FR-RAFT-02 | Deterministic KV state machine: Get, Put, Delete, List(prefix), **CAS**, **Txn-lite (multi-key CAS, ≤ 8 ops)**; applied strictly in log order. | E+A | Two replicas applying the same log are byte-identical. |
| FR-RAFT-03 | TTL leases + monotonic fencing counters (`IncrCounter`). Timestamps are command-carried; state machine never reads wall clock (R4). | A | Determinism test with injected clocks. |
| FR-RAFT-04 | Linearizable reads via ReadIndex (default); `Serializable` (stale) option explicit in API. | A | Porcupine linearizability check passes under faults. |
| FR-RAFT-05 | Snapshot creation (log-size threshold) and install on lagging/new node; log compaction. | E | Node wiped + rejoined recovers via snapshot. |
| FR-RAFT-06 | gRPC client API (`KV`, `Cluster` services) with leader redirect/retry, deadline propagation; typed Go client. | E | Client survives leader kill transparently (≤ 2× election timeout). |
| FR-RAFT-07 | 3-node cluster in Compose; `ClusterStatus` (leader, term, commit/applied index, peers); Prometheus metrics. | E | Status reflects failover. |
| FR-RAFT-08 | Scheduler leader election + epoch fencing integrated end-to-end with Postgres `cluster_state` guard (R6). | E | Deposed leader's writes rejected in test. |
| FR-RAFT-09 | Worker registry & liveness leases in Raft; recovery manager consumes expirations. | E | Killed worker's attempt reassigned ≤ lease TTL + 2 ticks. |
| FR-RAFT-10 | Fault suite: leader kill, follower kill/restart, 2-node loss (writes rejected), partition (minority rejects), stale node rejoin, delay, full restart, snapshot restore. | E | All 12 scenarios from v1.0 §8.7 automated. |
| FR-RAFT-11 | Static membership (3 voters) required; dynamic membership change. | E / **O** (dynamic) | Static required. |

### 5.5 Kafka and market data (`FR-KAFKA`)

| ID | Requirement | Org | Acceptance |
|---|---|---|---|
| FR-KAFKA-01 | Kafka (KRaft) in Compose; idempotent topic bootstrap with documented partitions/retention (`market.ticks` 6 partitions). | E | `make topics` idempotent. |
| FR-KAFKA-02 | Versioned event envelope + JSON Schemas in repo; consumers tolerate unknown optional fields; compatibility rules documented. | E | Schema validation tests; forward-compat test. |
| FR-KAFKA-03 | Outbox relay publishes workflow/task events to Kafka (see FR-WF-15). | A | See FR-WF-15. |
| FR-KAFKA-04 | Deterministic synthetic generator (seeded; configurable rate, symbols, dup/late/out-of-order/invalid injection) and historical CSV/JSON loader behind a `Source` interface. | E | Same seed → byte-identical stream. |
| FR-KAFKA-05 | Producer partitions by `symbol`; events validated before publish; per-event idempotent key. | E | Per-symbol ordering test. |
| FR-KAFKA-06 | Processor validates/normalizes; invalid events → `market.invalid` DLQ with reason; poison events never block a partition. | E | DLQ test with malformed payloads. |
| FR-KAFKA-07 | Persisted dedupe via unique `event_id` in `market_ticks` (`ON CONFLICT DO NOTHING`); duplicates counted. | E+A | Replay of same data → identical aggregates, N dup metric. |
| FR-KAFKA-08 | Event-time tumbling windows 1 m and 5 m: OHLCV, VWAP, event count; moving average and rolling volatility (documented formulas); anomaly flag (z-score > configurable). | E | Golden-file aggregate tests. |
| FR-KAFKA-09 | Late/out-of-order policy: watermark = max(event_time) − allowed_lateness (default 30 s); late-but-within-bound events trigger window recompute; beyond-bound events → `market.late` counter + DLQ(reason=late). | E+A | Policy test matrix. |
| FR-KAFKA-10 | Aggregate upsert keyed `(symbol, window_size, window_start)`, recompute-from-ticks so replays never regress data; `last_updated_at`, `input_hash` stored. | E | Replay overlapping live → final state equals single clean pass. |
| FR-KAFKA-11 | Backpressure: bounded channels, bounded batch, max in-flight, consumer pause/resume, bounded DB write concurrency; no unbounded buffers. | E | Slow-DB test: RSS growth bounded, lag grows, no OOM. |
| FR-KAFKA-12 | Offsets committed only after durable DB commit; graceful shutdown drains and commits; rebalance-safe. | E | Kill -9 mid-batch → no loss, duplicates deduped. |
| FR-KAFKA-13 | Replay/backfill by offset, timestamp, symbol set, time range, using a dedicated consumer group; overlapping with live is safe. | E | Replay determinism test. |
| FR-KAFKA-14 | Monitoring: consumer lag, freshness (now − last event_time), rates, queue depth, DB write latency; `market-data-quality-check` task asserts completeness/freshness thresholds. | E | Quality-check fails on injected gap. |
| FR-KAFKA-15 | Market-data workflow definition (`market-data-hourly`) executes end-to-end through FlowForge including failure branch to AI investigation. | E | E2E demo test. |
| FR-KAFKA-16 | Analytics read API (`/api/v1/marketdata/…` ohlcv, freshness, lag) for the dashboard. | A | Contract tests. |

### 5.6 AI Operations Copilot (`FR-AI`)

| ID | Requirement | Org | Acceptance |
|---|---|---|---|
| FR-AI-01 | FastAPI service with endpoints per `00E §10`; Pydantic schemas for request/result. | E | OpenAPI served; schema tests. |
| FR-AI-02 | Provider-agnostic LLM + embedding abstraction; deterministic **mock/fake** providers for tests; live mode behind env flag. | E+A | Entire test suite runs with no network. |
| FR-AI-03 | KB ingestion (markdown runbooks, docs, schemas, error catalog) → chunk → embed → pgvector with metadata (doc_id, title, path, version, section, chunk_idx, updated_at, scope, tags); idempotent re-ingest by content hash. | E | Re-ingest unchanged doc = 0 writes. |
| FR-AI-04 | Retrieval via LangChain retriever with metadata filters; results carry citations (doc_id, section). | E | Eval recall@3 ≥ 0.8 on the seed set (mock embeddings + lexical fallback). |
| FR-AI-05 | Typed, bounded, **read-only** tools (workflow run, attempts, logs, worker status, scheduler leader, Kafka lag, pipeline metrics, DB latency, runbook search); per-call timeout; per-investigation call budget (default 25). | E | Budget/timeout tests; no write tool exists. |
| FR-AI-06 | LangGraph investigation graph with nodes per v1.0 §11.5 (`parse_request … finalize_report`), bounded loop (max 2 evidence-gathering retries). | E | Graph runs offline with mocks. |
| FR-AI-07 | Structured result (JSON Schema validated): summary, observed_facts (each with `source` + `evidence_ref`), hypotheses (confidence), recommendations (risk, requires_approval), limitations, citations. Facts without evidence_ref are rejected by a post-validator. | E | Unsupported-claim rate = 0 on eval set. |
| FR-AI-08 | Explicit `insufficient_evidence` outcome. | E | Eval case "telemetry missing" passes. |
| FR-AI-09 | `AI_INVESTIGATION` task type: Go worker handler starts/polls the AI service; idempotent by `investigation_id = hash(run_id, task_id, question)`; completed results reused; result persisted via task result. | E+A | Retry after worker crash reuses completed result (no 2nd LLM call). |
| FR-AI-10 | Approvals: AI proposals become FlowForge `approval_requests`; approve/reject via API; only FlowForge executes approved actions (`retry_task`, `replay_range`), re-validating authorization and parameters at execution time. | E+A | Disruptive action without approval impossible (test at API + executor). |
| FR-AI-11 | Prompt-injection and abuse controls: logs/metrics/doc text delimited as untrusted data, tool allow-list, output schema enforcement, max tokens/time. | A | Injection eval cases (log line "ignore previous instructions…") do not alter tool use or proposals. |
| FR-AI-12 | Evaluation suite (≥ 12 labeled incidents) measuring run identification, tool selection, retrieval relevance, evidence coverage, unsupported-claim rate, recommendation safety, schema validity, approval enforcement. | E | Report generated by `make ai-eval`; thresholds in CI (mock mode). |
| FR-AI-13 | FlowForge **internal read-only API** (`/internal/v1/…`) for AI tools, role `ai-readonly`. | A | Write attempts → 403. |

### 5.7 Dashboard (`FR-UI`)

| ID | Requirement | Org | Acceptance |
|---|---|---|---|
| FR-UI-01 | Auth shell (API key login), role-aware navigation, error/empty/loading states. | A | Component + e2e smoke. |
| FR-UI-02 | Workflow definitions list/detail; start run; run list with filters and status. | E | Playwright smoke. |
| FR-UI-03 | DAG graph with live task states, attempts, errors, logs viewer. | E | Visual state matches API. |
| FR-UI-04 | Workers, scheduler leader, Raft cluster health. | E | Reflects failover within 5 s. |
| FR-UI-05 | Market-data freshness/lag/rates panels. | E | Reflects injected lag. |
| FR-UI-06 | AI investigation reports (facts vs hypotheses, evidence links), approval inbox with approve/reject + reason. | E | Approve triggers recorded decision. |
| FR-UI-07 | Live updates via SSE with reconnect. | A | Reconnect resumes with `Last-Event-ID`. |

### 5.8 Cross-cutting (`NFR`)

| ID | Requirement | Org | Acceptance |
|---|---|---|---|
| NFR-OBS-01 | Prometheus metrics per `00E §11` for API, scheduler, worker, raft, processor, AI. No high-cardinality or sensitive labels. | E | `/metrics` golden-name test. |
| NFR-OBS-02 | OpenTelemetry tracing; context propagated API→worker→Kafka envelope→processor→AI. | E | Single trace spans run→task→Kafka→DB in Jaeger. |
| NFR-OBS-03 | Structured JSON logs with `request_id`, `run_id`, `task_id`, `attempt_id`, `trace_id`; redaction. | E | Log-field test. |
| NFR-OBS-04 | Grafana dashboards (provisioned as code) for the 9 areas in v1.0 §17.3. | E | Provisioning test loads all dashboards. |
| NFR-OBS-05 | Prometheus alert rules for leader loss, no leader, lease-expiry spikes, lag growth, stale freshness, outbox backlog, DB latency. | A | `promtool test rules` passes. |
| NFR-TEST-01 | Unit tests for all pure logic; Go `-race` in CI; coverage gate ≥ 70 % on `internal/{dag,workflow,scheduler,raftkv,stream}`. | E | CI gate. |
| NFR-TEST-02 | Integration tests with real Postgres/Kafka/Timescale via testcontainers-go. | E | `make test-integration`. |
| NFR-TEST-03 | Fault-injection suites (workflow, raft, streaming, e2e chaos) with expected vs observed results documented. | E | `docs/testing/fault-report.md`. |
| NFR-TEST-04 | Load tests + benchmark methodology doc (hardware, versions, config, warm-up, percentile definitions). | E | Results file + method committed. |
| NFR-TEST-05 | AI evaluation harness (see FR-AI-12). | E | — |
| NFR-DEV-01 | One-command dev: `make dev-up`, `make test`, `make lint`, `make demo`. | E | Fresh clone works. |
| NFR-DEV-02 | CI (GitHub Actions) for Go, Python, frontend, proto/openapi drift, migrations, container build, security scan. | E | Green on main. |
| NFR-DEV-03 | Migrations forward-only, idempotently runnable, tested up-from-empty and up-from-previous. | A | Migration test. |
| NFR-DEV-04 | Docs: README, architecture docs, ADRs, runbooks (≥ 9), API docs, testing docs, limitations. | E | Docs checklist. |
| NFR-DEP-01 | Multi-stage, non-root, pinned-version container images for every service. | A | Trivy clean at HIGH+ (or documented exceptions). |
| NFR-DEP-02 | Full-stack Compose with profiles and healthchecks; `make demo` seeds data and runs the main scenario. | E | Demo script passes in CI nightly. |
| NFR-DEP-03 | Kubernetes manifests (Kustomize). | **O** | `kubectl apply --dry-run` passes. |
| NFR-REL-01 | Documented degraded behavior per dependency (§6.2) and tests for each row. | E+A | Matrix tests. |
| NFR-REL-02 | Graceful shutdown for all services (drain, release leases, commit offsets). | E | SIGTERM tests. |
| NFR-PERF-01 | Performance *targets* (§6.1), measured and reported honestly; failing a target requires documented analysis, not hidden. | E | Report. |

---

## 6. Non-functional detail

### 6.1 Performance and recovery targets (to be measured on the documented reference machine; not marketing claims)

| Metric | Target (single laptop-class host, Compose) |
|---|---|
| Task scheduling latency (READY→RUNNING), p95, 4 workers idle | < 250 ms |
| Sustained task throughput, trivial no-op tasks, 8 workers | ≥ 200 tasks/s |
| Worker-loss detection → reassignment | ≤ lease TTL (default 15 s) + 2 scheduler ticks |
| Scheduler leader failover (kill → new leader scheduling) | ≤ 10 s |
| Raft leader failover (kill → client writes succeed) | ≤ 3 s (election timeout 1 s, heartbeat 100 ms defaults) |
| Raft write latency p99 (3 nodes, fsync on) | < 50 ms at 200 writes/s |
| Stream throughput (processor, 1 instance, 6 partitions) | ≥ 5,000 events/s end-to-end to Timescale |
| Replay of 1 M events | completes; final aggregates identical to clean run |
| AI investigation (mock LLM) | < 5 s; (live LLM) report latency recorded, no target |

### 6.2 Degraded-behavior matrix (`NFR-REL-01`)

| Dependency down | Required behavior |
|---|---|
| Postgres `flowforge` | API `/readyz` 503; writes 503 with `Retry-After`; scheduler halts loops (no partial decisions); workers keep running attempts and retry reporting with backoff up to attempt deadline |
| Raft quorum lost | Schedulers lose leadership (stop scheduling); **running tasks continue**; API reads OK; heartbeats buffer-and-retry; worker lease expiry is *not* evaluated (no leader), so no spurious reassignment; recovers on quorum return |
| One Raft node down | No user-visible effect beyond failover latency |
| Kafka down | Outbox accumulates (alert); workflow progress continues; processor pauses; producer tasks fail retryably |
| TimescaleDB slow/down | Processor backpressure → lag grows, bounded memory; no offset commits past uncommitted writes |
| AI service down | `AI_INVESTIGATION` tasks fail retryably; parent workflow state `NEEDS_ATTENTION` per workflow policy; core engine unaffected |
| Prometheus down | AI metric tools return `unavailable` evidence → `limitations`, never fabricated |

---

## 7. Interfaces (summary — full contracts in `00E`)

- **REST (external)**: `/api/v1/*`, JSON, OpenAPI 3.1 (`00E §9`).
- **gRPC worker** (`proto/worker/v1`): `Register, Heartbeat, PollTask, CompleteTask, FailTask, ShipLogs, Deregister` (`00E §8`).
- **gRPC coordination** (`proto/coordination/v1`): `KV{Get,Put,Delete,List,CAS,Txn,Lease*,Counter}`, `Cluster{Status,Health}`, plus Raft peer transport (`raftpb` over gRPC streaming).
- **Internal read API** `/internal/v1/*` for AI tools (`ai-readonly`).
- **Kafka**: topics and envelopes in `00E §7`.
- **AI service** `/ai/v1/*` (`00E §10`).

---

## 8. Technology stack and pinning policy

Go ≥ 1.23 (pin in `go.mod` `toolchain`); gRPC + protobuf via `buf`; `chi` router; `pgx/v5` + `sqlc`; `goose`; `go.etcd.io/raft/v3`; `go.etcd.io/bbolt`; `franz-go` (Kafka); `robfig/cron/v3` parser only (scheduling logic is ours); `prometheus/client_golang`; OpenTelemetry Go SDK; `testcontainers-go`; `porcupine` (linearizability); `toxiproxy`. Python ≥ 3.12, FastAPI, Pydantic v2, LangChain + LangGraph (pin exact versions in `pyproject.toml` lockfile — agents MUST verify current stable versions at implementation time and record them), SQLAlchemy/psycopg, pgvector, pytest. Frontend: Node ≥ 20, React 18+, TypeScript strict, Vite, TanStack Query, React Flow, Vitest, Playwright. Every agent records the exact versions it selects in `docs/decisions/versions.md`.

---

## 9. Repository layout (authoritative)

```text
flowforge/
├── README.md  LICENSE  Makefile  go.work? (no; single go.mod)  go.mod
├── buf.yaml  buf.gen.yaml  sqlc.yaml  docker-compose.yml  .env.example  .golangci.yml
├── api/openapi/flowforge.v1.yaml
├── proto/{coordination,worker}/v1/*.proto
├── schemas/events/*.schema.json            # Kafka JSON Schemas
├── cmd/{flowforge-api,flowforge-scheduler,flowforge-worker,flowforge-raft,marketdata-processor,flowforgectl,mdgen}/
├── internal/
│   ├── config/ logging/ ids/ apperrors/ clock/ obs/
│   ├── dag/ workflow/{model,statemachine,template}/ persistence/{pg,sqlcgen}/
│   ├── api/{rest,auth,sse,internalapi}/ workerapi/ scheduler/ recovery/ cron/ outbox/
│   ├── coordination/ (client lib: election, leases, counters)
│   ├── raftkv/{node,storage,transport,fsm,server,client}/
│   ├── worker/{runtime,handlers}/ kafkax/ events/ stream/{domain,source,producer,processor,aggregator,store,replay,monitor,quality}/
│   └── testutil/
├── pkg/{retry,client}/
├── migrations/{flowforge,marketdata}/
├── services/aiops/{pyproject.toml,src/aiops/,schemas/,eval/,migrations/,tests/}   # package layout per task 06.01
├── web/
├── deploy/{docker,compose,k8s,kafka,prometheus,alertmanager,grafana,otel,toxiproxy}/
├── docs/{architecture,decisions,runbooks,api,testing,implementation-reports}/
├── test/{e2e,fault,chaos,load,fixtures}/   # integration tests live beside code under the `integration` build tag
├── scripts/{dev,chaos,demo}/
└── .github/workflows/
```

Single Go module: `github.com/daishakpatel/flowforge`.

---

## 10. Testing strategy

Pyramid: unit (pure logic, table-driven, `-race`) → component (repository-backed with testcontainers) → integration (multi-process, real deps) → fault (kill/pause/partition via Docker + Toxiproxy + in-process transport faults) → e2e demo → load. **Every implementation task carries its own tests** — tests are not deferred to a final phase. Gate tasks (03.09, 04.08, 05.10, 09.04, 10.01) verify cross-component behavior. Raft correctness uses Porcupine over client histories collected during fault runs. AI tests run fully offline with deterministic mocks; live-LLM evaluation is a separate manual target.

## 11. Observability

Metrics catalogue and span/log conventions in `00E §11`. Trace context travels in gRPC metadata, Kafka envelope `trace` field (W3C traceparent), and HTTP headers. Dashboards and alerts are code in `deploy/grafana` and `deploy/prometheus`.

## 12. Deployment and CI/CD

CI jobs: `go` (lint, vet, test -race, coverage), `go-integration` (testcontainers), `proto-openapi-drift`, `sqlc-drift`, `python` (ruff, mypy, pytest, ai-eval-mock), `frontend` (lint, tsc, vitest, build), `docker` (build all images, trivy), `nightly` (fault suite + demo). Releases: tagged images + changelog. Kubernetes (optional) via Kustomize.

## 13. Documentation deliverables

README (15 sections per v1.0 §21.2), `docs/architecture/*`, ADR mirror of `00A`, runbooks: worker-unavailable, scheduler-failover, raft-quorum-loss, stuck-workflow, kafka-consumer-lag, kafka-broker-outage, database-latency, replay-market-data, ai-investigation-failure, outbox-backlog; API docs; testing/fault report; benchmark methodology; limitations. Runbooks are also the AI knowledge base seed.

## 14. Definition of Done (release)

Maps v1.0 §24 to requirement IDs; the checklist in `00B` section "Project completion checklist" is authoritative. A release candidate additionally requires: clean-clone `make demo` passing; fault report with expected vs observed; no unsupported performance claims in README.

## 15. Optional enhancements (never block release)

Kubernetes manifests (`NFR-DEP-03`), dynamic Raft membership (`FR-RAFT-11`), audit hash-chain (`FR-SEC-03`), Flink variant, priority queues, worker autoscaling, multi-tenancy/quotas, workflow version migration, live market-data adapter, Redis cache (only with demonstrated need), from-scratch Raft learning branch (separate repo branch only).
