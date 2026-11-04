# 00D — Audit of the Original Specification (v1.0)

Method: read the whole v1.0 document; evaluated it against the 15 areas requested; cross-checked sections against each other for contradictions; asked of every component "what would an implementing agent have to guess?". Severity: **H** = would cause incorrect/unsafe implementation or blocked agents; **M** = material gap; **L** = polish.

Verdict: v1.0 is an excellent vision and risk analysis. Its weaknesses are (a) internal contradictions between diagrams, tables and prose, (b) missing wire/storage contracts, and (c) missing cross-cutting subsystems (security, task logs, outbox, schemas). All are resolved in the v2.0 master spec; nothing from v1.0 was dropped.

## A. Contradictions and inconsistencies

| # | Finding | Sev | Resolution (spec ref) |
|---|---|---|---|
| A1 | §9.2 DAG diagram has a cycle (retry → quality check) although §7.2 forbids cycles. | H | R1 / ADR-008 / FR-WF-12 |
| A2 | §5.3 pairs `scheduler-N` with `raft-N`, implying schedulers are Raft peers; §8 treats Raft as a separate KV service. | H | R6 / ADR-003, 006 |
| A3 | Dispatcher "push" (§5.1, §5.2) vs worker "poll/receive" (§7.4, §13.3). | H | R3 / ADR-004 |
| A4 | AI service reads PostgreSQL directly in §5.2 diagram but §5.4 says FlowForge owns workflow state. | H | R11 / ADR-016, FR-AI-13 |
| A5 | Approvals exist as a FlowForge table (§12.1) *and* an AI endpoint (§13.4) *and* a LangGraph pause (§11.5). | H | R11 / ADR-016, FR-AI-10 |
| A6 | `go.etcd.io/etcd/raft/v3` listed; library now has a standalone module path. | M | R15 / ADR-002 |
| A7 | Task states include `TIMED_OUT` separate from `FAILED`, but no rule for when each is terminal vs retried. | M | `00E §3` |
| A8 | `market-data-hourly` JSON has 4 tasks; §9.2 diagram has 8 stages (publish/process/write). Streaming stages are services, not tasks. | M | R9 / ADR-012 |
| A9 | Event topics keyed by `task_id` (§10.3) would destroy per-run ordering. | M | `00E §7` keyed by `run_id` |

## B. Missing or incomplete requirements

| # | Area | Finding | Sev | Added |
|---|---|---|---|---|
| B1 | Functional | No definition of task-to-task data passing or run input templating. | H | FR-WF-11 |
| B2 | Functional | Cancel/pause/resume semantics undefined (cooperative vs forced; how workers learn). | M | FR-WF-10 (heartbeat response carries cancels) |
| B3 | Functional | Headline use case is a *scheduled* run; cron triggers only a stretch goal. | H | FR-WF-14 |
| B4 | Functional | Workflow-level timeout, `NEEDS_ATTENTION` entry/exit undefined. | M | FR-WF-08, `00E §3.3` |
| B5 | API | No idempotency for run creation (client retries create duplicates). | M | FR-WF-13 |
| B6 | API | No OpenAPI/pagination/error envelope/SSE; frontend and AI would diverge. | H | FR-API-02..06 |
| B7 | DB | No fencing/epoch representation in the schema; `leader_epoch` on runs only. | H | `cluster_state`, `task_attempts.fencing_token` |
| B8 | DB | No indexes for the hot dispatch query, no retention, no logs/outbox tables. | M | `00E §2` |
| B9 | Consistency | Dual-write hazard publishing events to Kafka. | H | FR-WF-15 / ADR-009 |
| B10 | Distributed | KV API lacks CAS/TTL/counters/linearizable-read semantics; election impossible. | H | FR-RAFT-02..04 |
| B11 | Distributed | Lease clock model hand-waved ("clear clock model"). FSM determinism not addressed. | H | ADR-005 |
| B12 | Distributed | No retry-safety for KV client requests (duplicate Put after timeout). | M | request-ID dedupe in FSM |
| B13 | Distributed | No spec for behavior when Raft quorum is lost while tasks run. | M | §6.2 degraded matrix |
| B14 | Streaming | Dedup state described but persistence unspecified; restart loses it. | H | FR-KAFKA-07 / ADR-011 |
| B15 | Streaming | "Late event policy" deferred; aggregate "versioning" undefined. | H | FR-KAFKA-09/10 |
| B16 | Streaming | Late/duplicate/invalid injection requirements for generator missing → untestable. | M | FR-KAFKA-04 |
| B17 | AI | No log store for "inspect logs". | H | FR-WF-16 / R7 |
| B18 | AI | LLM/embedding providers unspecified; tests would need network. | H | FR-AI-02 |
| B19 | AI | No prompt-injection model although logs are attacker-influenced text. | H | FR-AI-11 |
| B20 | AI | Idempotency of `AI_INVESTIGATION` task under retry only mentioned as a bullet. | M | FR-AI-09 |
| B21 | Security | AuthN/Z, RBAC, audit, rate limits, service auth all unspecified. | H | FR-SEC-01..06 / ADR-015 |
| B22 | Frontend | Pages listed, no live-update mechanism, auth, or states. | M | FR-UI-01, 07 |
| B23 | Observability | No trace backend, no alert rules, no correlation contract. | M | NFR-OBS-02/05, `00E §11` |
| B24 | Testing | No linearizability checking; no coverage gates; no migration tests; no determinism tests. | M | NFR-TEST-01/03, NFR-DEV-03 |
| B25 | Deployment | No image hardening, no Compose profiles, no fault-injection tooling choice. | M | NFR-DEP-01/02, ADR-019 |
| B26 | Performance | Zero measurable targets; "highly scalable" warnings but no numbers to measure against. | M | §6.1 targets |
| B27 | DevEx | No toolchain pinning, no version-recording convention, no contract drift checks. | M | NFR-DEV-02, master §8 |
| B28 | Operability | No documented degraded modes; no runbook for outbox backlog/stuck workflow. | M | §6.2, §13 |
| B29 | Config | No configuration contract (env vars, defaults). | M | `00E §6` |
| B30 | Time | Clock skew policy absent although leases/timeouts rely on time. | M | ADR-021 |

## C. Bottlenecks and scalability observations

- **Heartbeat → Raft writes** scale as `workers / interval`; fine for tens of workers (documented), batching/lease-sharing is the first optimization if workers ≫ 100 (documented limitation, not implemented).
- **Single Postgres** is the throughput ceiling for scheduling; mitigated by `SKIP LOCKED`, partial indexes, bounded scans, batch promotion queries; target in §6.1 is measured in task 09.03.
- **Outbox relay** is single-leader; throughput bounded by one producer — adequate (targets documented), partitioned relays are future work.
- **Hot symbol skew** in Kafka partitions is a demonstrated scenario, not solved by repartitioning in v1.
- **Recompute-from-ticks** reads scale with distinct windows per batch; bounded batch size keeps it predictable.

## D. Security design concerns

Unauthenticated gRPC between worker/API and AI/API; AI agent consuming attacker-controllable log text; approval executor trusting AI-supplied parameters; API keys in logs; unbounded request sizes; metrics endpoint exposure. Addressed by FR-SEC-01..06, FR-AI-10/11, task 09.01.

## E. Opportunities to demonstrate stronger engineering (justified, not decorative)

| Addition | Why it earns its place |
|---|---|
| Porcupine linearizability checks on Raft histories | Turns "I tested failover" into verifiable evidence |
| Fencing at two levels (epoch + attempt token) | The actual safety argument of the system |
| Determinism test for FSM (two replicas, same log, injected clocks) | Catches the most common Raft-app bug class |
| Replay-equals-clean-run property test | Proves the idempotency design claim |
| AI eval harness with unsupported-claim metric | Makes the "no fabrication" claim measurable |
| Expected-vs-observed fault report | Reviewer-grade evidence |

## F. Things deliberately *not* added

Service mesh, Schema Registry, Loki/Tempo, Redis, Kubernetes operator, OIDC, multi-tenancy, Flink, Watch API in KV, dynamic Raft membership (optional only), exactly-once claims. Each lacks a concrete requirement in this project; adding them would be decoration.

## G. Coverage of the 15 audit areas

1 Functional (A,B1–B5) · 2 Architecture (A2–A5,B7) · 3 Backend/API (B5–B6) · 4 Frontend (B22) · 5 DB (B7–B8) · 6 Distributed (B10–B13) · 7 Concurrency/faults (B11–B13, fencing) · 8 Security (B21, D) · 9 Scalability (C) · 10 Errors/retries (A7, B2) · 11 Observability (B23) · 12 Testing (B24) · 13 Deployment/CI (B25, B27) · 14 Docs/DX (B27–B29) · 15 Production readiness (B26, B28, B30).
