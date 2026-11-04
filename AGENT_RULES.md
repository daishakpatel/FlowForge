# AGENT_RULES.md — Universal Instruction Manual for FlowForge Coding Agents

**Audience:** every AI coding agent (and human) implementing a numbered FlowForge task.
**Status:** binding. In the repository this file lives at the repo root; the blueprint copy is `implementation/AGENT_RULES.md`. Keep them identical.

How this file relates to the others (precedence, highest first):

1. Your task file (`phase-NN-*/NN.NN_*.md`) for *what to build in this task*.
2. `00E_CONTRACTS_REFERENCE.md` for exact schemas, routes, topics, env vars, error codes.
3. `00A_ARCHITECTURE_DECISIONS.md` (ADRs) for *why the architecture is the way it is*.
4. `00_MASTER_PROJECT_SPEC.md` for requirements and targets.
5. **This file** for *how* to write, test, commit, document and hand off.
6. `00C_AGENT_OPERATING_RULES.md` for the short procedural checklist (reading order, completion, escalation).

If two sources conflict, do not guess: follow the order above, record the conflict in your completion report (§12), and, if it blocks you, stop and escalate (§7.4). Rules marked **MUST/MUST NOT** have no "judgment" exception; **SHOULD** may be deviated from with a one-line justification in the PR.

---

## 0. The ten rules that matter most

1. Read the master spec, ADRs, contracts, your task file, and the previous task's completion report **before** writing code.
2. Never silently change the architecture. Use §7.
3. Never overwrite or "improve" working code that is outside your task's scope. Use §8.
4. Every behavior you add has a test; tests are written with the code, not after.
5. The code must pass `make lint test` locally before you hand off. Never weaken a test, linter or gate to make it pass.
6. Postgres is the authority for workflow state; Raft holds only coordination state; safety comes from fencing (ADR-003). Never add a path that bypasses either.
7. Execution is **at-least-once** with idempotency and fencing. Never claim or design for exactly-once (ADR-007).
8. All time comes from the injected `clock.Clock`; all randomness and IDs come from injected sources (§1.7).
9. No secrets, tokens, or payload contents in logs, metrics labels, span attributes, error messages, or commits.
10. Finish with a completion report and a handoff note (§12). A task without them is not done.

---

## 1. Coding conventions and naming standards

### 1.1 Repository layout (authoritative; do not invent new top-level directories)

```text
cmd/<binary>/main.go          # thin: parse config, wire dependencies, run; no business logic
internal/<package>/           # all non-public Go code
pkg/                          # only code intended for external import (events envelope, coordination client)
proto/<area>/v1/              # protobuf sources (buf); generated code committed under internal/gen/
api/openapi/                  # OpenAPI specs (REST); generated clients committed where specified
migrations/<db>/              # goose SQL migrations (flowforge, marketdata); aiops migrations under services/aiops
schemas/                      # JSON Schemas (events, logs)
services/aiops/               # Python AI service
web/                          # React dashboard
deploy/                       # Dockerfiles, compose fragments, grafana, prometheus, k8s
docs/{architecture,decisions,runbooks,api,testing,guides,security}/
test/{e2e,fault,chaos,load}/  # cross-component suites; unit tests live beside code
scripts/                      # dev, ci, demo, bench, chaos tooling
workflows/                    # workflow definition YAML
```

A new package goes under the directory your task's "Files to Create or Modify" section names. If it names none, put it under `internal/` using the package naming rules below and mention it in the completion report.

### 1.2 Go

- Go version and toolchain are pinned in `go.mod` / `.tool-versions` (task 01.01). Do not change them without an ADR amendment (§7).
- Formatting: `gofmt` + `goimports` (local prefix = module path). Linting: `golangci-lint` with the committed config. **Do not add `//nolint` without a reason comment** (`//nolint:rule // why`); CI rejects bare `nolint`.
- **Packages:** short, lowercase, singular, no underscores (`scheduler`, `dag`, `outbox`). No `util`, `common`, `helpers`, `base` packages. Package name ≠ directory stutter (`scheduler.Scheduler` is fine; `scheduler.SchedulerService` is not).
- **Files:** `lower_snake.go`; tests `*_test.go` beside the code; one primary type per file when types are large.
- **Identifiers:** exported `PascalCase`, unexported `camelCase`, acronyms uppercase (`ID`, `URL`, `HTTP`, `JSON`, `SQL`, `RPC`). Receivers are 1–2 letters and consistent. Constants for enums are typed (`type TaskStatus string`) with `UPPER_SNAKE` string values matching the DB/API (`"RUNNING"`).
- **Interfaces:** small, defined where they are **consumed**, named for behavior (`Publisher`, `LeaseProvider`, `Gate`, `Clock`). Accept interfaces, return concrete types. Do not create an interface for a single implementation unless it is a documented seam in the task file or is needed for a fake in tests.
- **Context:** the first parameter of anything that does I/O or can block is `ctx context.Context`. Never store contexts in structs. Respect cancellation and deadlines in every loop and blocking call.
- **Concurrency:** every goroutine has an owner, a stop signal, and a join (`errgroup`, `sync.WaitGroup`). No fire-and-forget goroutines. All channels are bounded unless the task explicitly documents otherwise. Document lock ordering when more than one mutex is involved. `goleak` is required in tests of long-running components (§3.5).
- **Global state:** none. No `init()` side effects, no package-level mutable variables, no `time.Now()`, `rand`, or `uuid.New()` calls outside the injection points (§1.7).
- **Size limits (SHOULD):** functions ≤ 60 lines; files ≤ 600 lines; cyclomatic complexity per the linter config. Split instead of raising limits.
- **Comments:** exported identifiers have doc comments starting with the identifier name. Comment *why*, not *what*. Every non-obvious invariant, ordering requirement, or safety argument is written next to the code (e.g. "assert epoch in the same tx as the state change — ADR-003").
- **Generics / reflection:** use sparingly; no reflection in hot paths.

### 1.3 Data access and SQL

- Queries are written in `.sql` files and compiled by **sqlc** (ADR-014). No SQL built by string concatenation with user or external input; no ORMs.
- State transitions follow ADR-020: pure transition functions in the domain layer + **conditional SQL** (`UPDATE … WHERE id=$1 AND status=$2 [AND fencing_token=$3]`), checking `RowsAffected`. Never read-modify-write a status without the condition.
- Scheduler decision transactions assert the leader epoch (`AssertEpoch`) in the same transaction (ADR-003).
- Transactions are short, never span network calls (Kafka, HTTP, gRPC), and use the repository/`TxManager` helpers from Phase 02.
- Table/column names: `snake_case`, plural tables, IDs `text` with prefixed ULIDs (`run_…`), timestamps `timestamptz`.
- Indexes and constraints are part of the migration that creates the table; every new query path has either an index or a written justification in the PR.

### 1.4 Migrations

- Tool: goose; **forward-only** in `main` (no editing an applied migration, ever). Fix mistakes with a new migration.
- Filename: `NNNN_short_description.sql` with the next free number *in that database's directory*. If two tasks might collide, the later task renumbers on merge and says so in its completion report.
- Each migration is idempotent where practical (`IF NOT EXISTS`), transactional unless it cannot be (document why), and has a rollback note in comments. Destructive changes (drop/rename/type change) are a **breaking change** → §7.
- Every migration is tested up-from-empty and up-from-previous (NFR-DEV-03).

### 1.5 API conventions (REST, gRPC, events)

- REST: `/api/v1/...`; resource nouns, plural; JSON `snake_case`; timestamps RFC 3339 UTC; list endpoints paginate (`limit`, `cursor`) with documented maxima; state-changing POSTs accept an `Idempotency-Key`.
- Errors use the single error envelope and the codes in `00E §4` (`{"error":{"code","message","request_id","details"}}`). Add a new code only by amending `00E` (§7) and its tests.
- Authorization is enforced by middleware using the route→minimum-role table in `00E §9`. A new route **MUST** be added to that table and to the RBAC test that iterates all routes.
- OpenAPI/proto are the source of truth; generated code and clients are committed and drift-checked in CI. Never hand-edit generated files (`// Code generated … DO NOT EDIT.`).
- gRPC/proto: fields are only ever added (never renumbered/removed/retyped); removed fields are `reserved`. `buf breaking` must pass.
- Events: envelope + JSON Schema per `00E §7`. Consumers ignore unknown fields. A breaking payload change = new `schema_version` + both versions published for a deprecation window.

### 1.6 Naming cheat-sheet

| Thing | Convention | Example |
|---|---|---|
| Env var | `FF_` + `UPPER_SNAKE` | `FF_RAFT_DATA_DIR` |
| Prometheus metric | `flowforge_<area>_<name>_<unit>`; counters end `_total`, durations `_seconds` | `flowforge_stream_duplicates_total` |
| Metric labels | low-cardinality enums only; never ids, users, keys, free text | `queue`, `task_type`, `status` |
| Event type | `lower.dot.snake` | `task.attempt.failed` |
| State value | `UPPER_SNAKE` | `ABANDONED` |
| JSON field | `snake_case` | `available_at` |
| Kafka topic | `domain.name` | `market.ticks`, `flowforge.events` |
| Go test | `Test<Unit>_<Scenario>` ; table cases have descriptive `name` | `TestClaim_SkipsLockedRows` |
| Branch | `task/NN.NN-short-slug` | `task/03.02-scheduler-core` |
| Python | PEP 8, `snake_case`, type hints everywhere in `core/api`; `ruff`+`mypy` clean | |
| TypeScript | `strict`; components `PascalCase.tsx`; hooks `useX`; no `any` without justification | |
| Doc files | `lower-kebab.md` under `docs/…`; runbooks `<symptom>.md` | `raft-quorum-loss.md` |

### 1.7 Determinism and injection points (non-negotiable)

- Time: `clock.Clock` (real + fake) from task 01.04. Only `cmd/*/main.go` and the real clock implementation may call `time.Now()`.
- IDs: `internal/ids` generators (injectable). Randomness: seeded `rand.Source` passed in; tests use fixed seeds and print the seed on failure.
- The Raft FSM (`internal/raftkv/fsm`) and the stream domain (`internal/stream/domain`) are **pure**: no clock, randomness, network, goroutines, or map-iteration-order dependence. The lint rules enforce this; do not exempt them.
- Config comes only from the config loader (task 01.03). No `os.Getenv` outside it.

---

## 2. Git workflow and commit expectations

### 2.1 Branching and scope

- One task = one branch (`task/NN.NN-short-slug`) = one pull request, branched from the latest `main` that contains all your task's dependencies. Verify dependencies are merged (check `00B` progress tracker) before starting.
- Parallel tasks (marked in `00B`) touch disjoint areas by design. If you discover an overlap (same file, same migration number, same Compose service), **stop and coordinate** via the escalation path (§7.4) rather than resolving it unilaterally.
- Keep the PR scoped to the task. Drive-by refactors, formatting sweeps, dependency bumps and unrelated fixes go in separate PRs (and need their own justification).
- Never force-push to `main`; never rewrite history that others have pulled. Rebase your own branch freely before opening the PR; after review begins, add commits instead.

### 2.2 Commits

Format (Conventional Commits, task id in scope):

```text
<type>(<NN.NN>): <imperative summary, ≤ 72 chars>

<why, not just what; reference ADR/requirement IDs>

Refs: FR-XXX-NN, ADR-0NN
```

Types: `feat`, `fix`, `test`, `docs`, `refactor`, `perf`, `build`, `ci`, `chore`. A `BREAKING CHANGE:` footer is required for anything that changes a public contract (§7).

- Commits are small and each builds and passes tests (bisectable). Separate commits for: migration, generated code, implementation, tests, docs when practical.
- Generated files are committed in the same PR as the change that requires them, in their own commit, with the generating command in the message.
- Never commit secrets, `.env` files with real values, large binaries, build output, or coverage files. Dev-only keys are clearly labeled and live in the documented dev fixtures only.
- If a project policy asks for attribution lines on commits/PRs, include exactly the lines the harness provides; do not invent your own.

### 2.3 Pull requests

The PR description MUST contain: task id and title; requirements covered; summary of the change; **deviations** (or "none"); migrations added; new env vars/config; new metrics/routes/events/topics; test evidence (commands + results); the completion report (§12) or a link to it; and a checklist ticking the Definition of Done (§3.7). PRs without a test plan are not reviewable.

### 2.4 Merge conditions

CI green (lint, unit with `-race`, integration, proto/OpenAPI/sqlc drift, migrations, security scans); review approved; branch up-to-date with `main`; completion report committed; `00B` progress checklist updated. Merge with squash only if the squashed message keeps the format above; otherwise rebase-merge.

---

## 3. Testing requirements

### 3.1 Principles

- **No behavior without a test.** Bug fixes start with a failing test that reproduces the bug.
- Tests are deterministic: fake clock, seeded randomness, no sleeps for synchronization (use channels, `Eventually` with bounded timeouts), no reliance on test order, no network access outside testcontainers/Compose.
- Tests are independent and hermetic: unique schemas/keys/topics per test; clean up in `t.Cleanup`.
- A flaky test is a bug. Quarantine only with a linked issue and an owner, and never within your own task's acceptance tests.

### 3.2 Required levels

| Level | Tooling | Required for |
|---|---|---|
| Unit | `go test -race`, table-driven; pytest; Vitest | All pure logic, state machines, validators, parsers, formulas |
| Property/fuzz | `testing/quick`, `rapid` or Go fuzz; hypothesis for Python | Parsers, DAG/validation, FSM determinism, ordering-independence |
| Integration | testcontainers-go (Postgres, Timescale, Kafka), real wiring | Repositories, SQL, API handlers, Kafka paths, outbox |
| Contract | OpenAPI/proto/JSON Schema validation | Every public endpoint/message |
| Concurrency/failure | goleak, fault injection (failpoints, Toxiproxy, FaultTransport) | Anything with retries, leases, fencing, leadership |
| E2E | Compose or in-process full stack under `test/e2e` | Phase-gate tasks and user-visible flows |
| Linearizability | Porcupine | Raft KV paths |
| Performance | Go benchmarks / load suite | Only where the task states a target |

### 3.3 Coverage and gates

- Coverage gate: **≥ 70 %** on `internal/{dag,workflow,scheduler,raftkv,stream}` (NFR-TEST-01). New code in those packages SHOULD target ≥ 80 %. Coverage is a floor, not a goal: assert behavior, not lines.
- `go test -race ./...` MUST pass. Long suites are tagged (`integration`, `fault`, `chaos`, `load`) and wired into the `make` targets named in the task.
- Every task's "Testing Requirements" section is the minimum; add tests for edge cases you discover.

### 3.4 Test data and fixtures

- Fixtures live in `testdata/` beside the code. Golden files are regenerated only with an explicit flag (`-update`) and reviewed in the diff; never auto-update in CI.
- Use the shared helpers (`testutil`, `dbtest`, `kafkatest`, `nodetest`) instead of re-implementing harnesses. If a helper is missing a capability, extend it compatibly (§8) and test the extension.

### 3.5 Concurrency hygiene in tests

- Use `goleak.VerifyTestMain` for packages that start goroutines.
- Race-prone code gets a stress test (`-count=20`, varied `GOMAXPROCS`) before handoff; record the command in the report.
- Time-dependent behavior is tested with the fake clock advanced explicitly.

### 3.6 Failure-path testing is mandatory

For every external dependency the code touches (Postgres, Kafka, Raft, AI service, worker), test at least: unavailable, slow (timeout), and returning an error. The expected degraded behavior is defined in the master spec §6.2 — test that, not an invented behavior.

### 3.7 Definition of Done (code)

- [ ] All acceptance criteria in the task file are met and demonstrated.
- [ ] `make lint test` (plus the task's integration/e2e targets) pass locally, from a clean checkout.
- [ ] No unrelated files changed; no commented-out code; no `TODO` without an issue reference.
- [ ] Docs updated (§6); completion report written (§12).
- [ ] No regression in previous tasks' tests (run the full suite, not just yours).

---

## 4. Error-handling standards

### 4.1 Go errors

- Return errors; do not panic for expected failures. `panic` is allowed only for programmer errors at startup (invalid static config) and must never occur on a request/task path. Every goroutine entry point has a documented recovery policy; recovered panics are logged with stack, counted in a metric, and treated as a failure of that unit of work.
- Wrap with context using `%w`: `fmt.Errorf("claim task %s: %w", id, err)`. Lower-case messages, no trailing punctuation, no redundant "failed to".
- Use sentinel errors (`var ErrNotLeader = errors.New(…)`) or typed errors for conditions callers branch on; callers use `errors.Is/As`, never string matching.
- Do not log **and** return the same error. Log at the boundary that handles it (handler, loop iteration, main); lower layers return.
- Never swallow errors. If ignoring is correct (e.g. `Close` on a read path), assign to `_` with a comment.
- Distinguish **retriable** from **permanent** errors in types or wrapper (`errs.Retriable(err)`); retry logic consults that, not ad-hoc checks.

### 4.2 Retries, timeouts and backoff

- Every outbound call has a deadline derived from the caller's context plus a per-call cap. No unbounded waits.
- Retries use bounded exponential backoff with jitter (from the shared helper), a max attempt count, and honor context cancellation. Only **idempotent** operations are retried automatically; non-idempotent writes carry an idempotency/request id first (e.g. Raft `request_id`, API `Idempotency-Key`).
- Retry storms are prevented with caps and (where specified) circuit breaking; never retry in a tight loop.

### 4.3 Error mapping at boundaries

- Domain/repository errors are translated to API error codes only in the API layer, via the table in `00E §4`. Internal details (SQL text, stack traces, hostnames) never reach clients; `request_id` always does.
- gRPC: use canonical status codes plus `ErrorInfo` details (`NOT_LEADER` with hint, `STALE_EPOCH`, `STALE_FENCING_TOKEN`, …) as defined in `00E`.
- Worker-facing errors are explicit about fencing: a rejected stale completion is a normal, logged outcome (counter), not a crash.

### 4.4 Safety-critical failure rules

- **Fail safe, not open.** If you cannot prove you are the leader, hold a valid lease, or have a current epoch/fencing token, you do nothing. If an authoritative read fails, do not act on a stale view (e.g. the recovery loop never abandons tasks when the lease read fails).
- Never mutate workflow state outside the conditional-update + epoch/fencing discipline (ADR-003, ADR-020).
- Degraded-mode behavior must match master spec §6.2; surface degradation via metrics, health endpoints and logs.

### 4.5 Logging and observability of errors

- Structured logs only (the logger from task 01.04); required fields where applicable: `request_id`, `run_id`, `task_id`, `attempt_id`, `worker_id`, `trace_id`. Levels: `error` = needs attention; `warn` = degraded/retrying; `info` = lifecycle; `debug` = detail.
- **Never log:** secrets, API keys, tokens, DSNs with passwords, task payload/result bodies, market prices at info level, full log lines from tasks. Use the redaction helper for anything user-supplied.
- Increment a metric for every error class that matters (use bounded `outcome`/`reason` labels). Record errors on the active span with truncated, redacted messages.

### 4.6 Python and TypeScript

- Python: custom exception hierarchy under `aiops.core.errors`; no bare `except`; tool and provider failures become structured results (`ToolResult.error`), never uncaught exceptions inside the graph. Type-check with `mypy --strict` on `core/api`.
- TypeScript: API errors are parsed into the typed `ApiError`; UI shows `code` and `request_id`; error boundaries around routes; no silent `catch {}`.

---

## 5. Dependency management rules

1. **Prefer the standard library.** A new dependency needs a reason in the PR: what it replaces, maintenance status (recent release, active repo), license (must be permissive: MIT/BSD/Apache-2.0/ISC/MPL; no GPL/AGPL/SSPL), and security record.
2. **Approved core stack** (do not substitute without an ADR amendment): Go: `pgx/v5`, `sqlc`, `goose`, `chi`, `franz-go`, `go.etcd.io/raft/v3`, `grpc-go`, `protobuf`/`buf`, `testcontainers-go`, `testify`/`go-cmp`, `goleak`, `porcupine`, OpenTelemetry, `prometheus/client_golang`. Python: FastAPI, Pydantic v2, LangChain, LangGraph, SQLAlchemy/psycopg, pgvector. Frontend: React, Vite, TypeScript, TanStack Query, React Flow, Vitest, Playwright.
3. **Pin everything.** `go.mod`/`go.sum`, Python lockfile, `package-lock.json` are committed; container base images are pinned by tag (digest for release builds); CI tool versions pinned. Record chosen versions and the verified import path of any library whose path varies (e.g. the Raft module) in `docs/decisions/versions.md`.
4. **Verify, don't assume.** Library APIs in task files are described from the design's point of view; confirm the exact signatures in the version you import. If reality differs, adapt and note it in the completion report (a *clarification*, §7.1), unless it changes the architecture (§7.3).
5. **Minimal blast radius.** Add dependencies only in the task that needs them. Do not upgrade existing dependencies unless the task requires it or a security fix demands it; upgrades are their own PR with the full suite green.
6. **Vendoring and tools:** developer tools (`sqlc`, `buf`, `golangci-lint`, `goose`) are installed via the pinned mechanism from task 01.01 (`tools.go`/`make tools`), never "whatever is on the machine".
7. **Security:** CI runs `govulncheck`, `pip-audit`, `npm audit` (high+), `gitleaks`, `trivy`. A finding blocks the PR unless added to the expiring allowlist with justification and owner.
8. **Transitive hygiene:** run `go mod tidy`; commit the result; no unused dependencies. Do not import `internal` packages across the Go/Python/TS boundaries; cross-language contracts go through OpenAPI/proto/JSON Schema.
9. **Package boundaries:** import direction is `cmd → internal/api|worker|scheduler|… → internal/domain/repository → internal/platform (clock, ids, config, obs)`. Lower layers never import higher ones; `internal/raftkv` and `internal/stream/domain` import no FlowForge application packages. A dependency-cycle or layering violation is a defect.

---

## 6. Documentation rules

### 6.1 What must be updated, and when

| You changed… | You MUST update, in the same PR |
|---|---|
| Public/REST/internal API | OpenAPI spec, generated client, `docs/api/*`, contract tests |
| Proto | `.proto`, generated code, `buf breaking` baseline note, docs |
| DB schema | Migration, `00E` DDL section if the contract changed (§7), sqlc queries, `docs/architecture/*` for the component |
| Env var / config | Config struct docs, `00E §6` table, `.env.example`, config reference |
| Metric / log field / span | `metrics/catalogue.yaml` (once it exists) or `00E §11`, dashboards/alerts that use it |
| Event/Kafka schema | `schemas/events`, baseline compat snapshot, `docs/architecture/event-schemas.md` |
| Failure behavior | Relevant runbook, degraded matrix (master spec §6.2) and its test |
| Architecture decision | ADR (§7) |
| Developer workflow (make targets, tools) | `README.md`/`docs/guides/quickstart.md` |

### 6.2 Documentation standards

- Every task produces the documents named in its "Documentation Requirements" section. Architecture docs describe the *implemented* behavior (not intentions), include a Mermaid diagram where flow or state matters, and list limitations honestly.
- Runbooks contain: Symptoms · Impact · Diagnosis (exact commands/queries/dashboard links) · Mitigation · Verification · Escalation. Commands are copied from tested scripts.
- Code examples and commands in docs must work; the docs QA job runs them where feasible.
- Do not claim guarantees you have not tested (no "exactly-once", no "highly available" without the corresponding fault test). Performance numbers appear only with their methodology (NFR-TEST-04).
- Update — never duplicate — existing documents. Link to the canonical source (`00E`, ADRs) instead of copying tables.
- Keep the per-task documents' "Previous/Next" links, `00B` progress tracker and `docs/traceability.md` (when it exists) accurate.

### 6.3 Docs QA

Links resolve, Mermaid renders, generated references have no drift, headings follow the style (sentence case), and every new `docs/**` file is reachable from an index (`README.md` or the relevant overview).

---

## 7. Handling architectural changes

Architecture is decided in `00A_ARCHITECTURE_DECISIONS.md` and the contracts in `00E`. **You are not permitted to change either implicitly.** Classify your situation first:

### 7.1 Clarification (allowed without approval)

The design is unchanged; you are filling in detail the documents leave open or correcting a wrong detail that doesn't alter any contract (e.g. an actual library function name, a default timeout value inside the documented range, an internal helper type). Record it under "Deviations / clarifications" in your completion report with a one-line rationale.

### 7.2 Compatible extension (allowed, with an ADR *amendment*)

You add something that does not alter existing behavior or contracts: a new optional field, new endpoint, new metric, new index, new internal package, new error code. Requirements:
- Update `00E` (and the master spec if a requirement is added, tagged *recommended addition*).
- Add an entry under **Amendments** in `00A` (date, task id, what, why, impact).
- Include tests; keep all existing tests unchanged and green.

### 7.3 Architectural change (NOT allowed without escalation)

Any of the following requires an approved ADR **before** implementation:
- Changing an ADR decision or the authority split (what is stored in Postgres vs Raft; who executes approvals; who may write to the DB).
- Changing a public contract incompatibly: renaming/removing/retyping an API field, route, proto field, event field, topic, metric used by dashboards/alerts, env var, DB column/table, state or state-transition.
- Changing delivery or consistency semantics (e.g. introducing exactly-once claims, changing commit-after-write ordering, weakening fencing, adding a new writer of workflow state).
- Replacing an approved core dependency (§5.2) or introducing a new datastore/queue/service.
- Changing the module/service boundaries or deployment topology.
- Relaxing any **MUST** in this file or a safety invariant (ADR-003, ADR-005, ADR-007, ADR-011).

Procedure:
1. **Stop** work on the affected part. Do not "just try it".
2. Write a proposed ADR in `docs/decisions/proposed/ADR-XXX-short-title.md`: context, problem, options considered (≥ 2, including "do nothing"), decision, consequences, migration/compat plan, impacted tasks (use `00B` to list dependents), test plan.
3. Escalate (§7.4) and wait for approval. Approved ADRs are added to `00A` with status *Accepted* and, if they supersede another, mark that one *Superseded by ADR-XXX*.
4. Implement in a dedicated change (or as the first commits of the PR), update the contracts, **and update every downstream task file** that assumed the old behavior (list them in the ADR; downstream tasks are not "fixed" silently by later agents).

### 7.4 Escalation path

Escalate (stop and report; do not improvise) when: documents conflict and the precedence order doesn't settle it; a dependency's output is missing or doesn't match its "Provides" list; a task's acceptance criteria are impossible or mutually contradictory; you need an architectural change (§7.3); a safety/security concern arises; or two parallel tasks overlap. Escalation = a `BLOCKED` completion report (§12.4) plus, if a human/orchestrator channel exists, a message containing: task id, what is blocked, what you tried, options, and your recommendation.

### 7.5 Backward-compatibility policy

- Public APIs, events, protos and DB schemas change by **addition**. Deprecate before removal (mark in docs/OpenAPI, keep for at least one release, add a removal task).
- Data migrations preserve existing data; destructive steps need a two-phase plan (add → backfill/dual-write → switch → remove in a later release).
- Feature flags (`FF_*`) gate risky behavior; the default is the safe/previous behavior. Flags have an owner and removal note.

---

## 8. Rules against overwriting existing working implementations

1. **Understand before touching.** For any file you did not create, read it, its tests, and `git log -p` for its recent history. Identify which earlier task owns it (`provides` list in the previous task files).
2. **Extend, don't replace.** Add via new files, new methods, new interface implementations, options structs with defaults, or additive columns. Replacing a working implementation is allowed only when your task file explicitly says "replace/refactor X" and names the reason.
3. **Preserve public surfaces.** Keep exported function signatures, interface methods, JSON/proto fields, config keys, metric names and CLI flags that earlier tasks introduced. If you must change one, that is a §7 change.
4. **Tests are protected.** Never delete, skip, loosen, or rewrite an existing test to make your change pass. If a test legitimately encodes behavior that your task changes **by instruction**, change the test in a separate commit with an explanation, and state it in the report. A test that fails after your change is information: find out why.
5. **No broad mechanical edits** (renames, reformatting, regenerating whole files, mass import rewrites) in feature PRs. Do them in a separate, behavior-neutral PR.
6. **Run the full suite before and after.** Capture the baseline failures on a clean `main`; do not attribute pre-existing failures to your change, and do not hide them. Fix only what is yours; report the rest.
7. **Seams over edits.** Earlier tasks deliberately expose seams (`LeaseProvider`, `Gate`, `OutcomeHandler`, `CancelSource`, `InputResolver`, `Publisher`, `WindowSink`, `Applier`, `EpochFence`). Integrate through them; do not reach around them into internals.
8. **Generated and vendored code is read-only** except by regenerating with the documented command.
9. **No destructive git or filesystem actions** outside your own branch/workspace (`git reset --hard`, `git clean -fd`, deleting directories, dropping databases/volumes that others use). Local Compose resets are fine (`make dev-reset`) but never run against shared environments.
10. **Don't "fix" what you don't own silently.** If you find a bug in earlier work, add a failing test, make the minimal fix in a separate commit referencing the owning task, and report it under "Defects found in earlier tasks". If the fix is not minimal, escalate.
11. **Migrations and contracts are append-only** (see §1.4, §7.5).
12. **Temporary scaffolding** (stubs, fakes, placeholder handlers, e.g. the `LOG_ONLY` stub for `ai_investigation` before 06.06) must be clearly marked, tracked in the report's "Known stubs", and removed/replaced by the task that owns the real implementation — not left to rot.

---

## 9. Security and data-handling rules (applies everywhere)

- Validate all external input (HTTP, gRPC, Kafka, files, tool outputs, LLM output) against schemas/limits before use. Enforce size limits.
- Parameterize all queries (sqlc). No dynamic SQL from external text.
- Authentication/authorization goes through the shared middleware; do not hand-roll checks in handlers. `ai-readonly` can never write (06.02).
- Treat logs, metrics, KB text, and workflow definitions as **untrusted** when they flow into prompts (06.04).
- Secrets only from env/files via the config loader; never default to a real credential; dev keys are marked dev-only. Do not print config containing secrets.
- Containers run as non-root with pinned images (09.02). TLS/mTLS options are honored where implemented (09.01).

---

## 10. Performance and resource rules

- Bounded memory and concurrency everywhere (queues, batches, caches, dirty sets, goroutine pools) with documented limits and metrics.
- No unbounded `SELECT` (always `LIMIT`/pagination); no N+1 query patterns in loops on hot paths; check `EXPLAIN` for new query paths expected to run frequently.
- Do not optimize without a measurement. When a task states a performance target, measure per the methodology (09.03) and report honestly, including misses.
- Do not spin: loops that wait use blocking primitives, `LISTEN/NOTIFY` wake-ups with a fallback poll interval, or tickers.

---

## 11. Python and frontend specifics

**Python (`services/aiops`)** — `ruff`, `ruff format`, `mypy --strict` on `core/api`; dependency injection of providers and clients; mock mode by default in tests, **no network** (`pytest-socket`); prompts are versioned files; the graph never executes actions; outputs validated against JSON Schema; every tool is GET-only.

**Frontend (`web/`)** — TypeScript strict; the API client is generated from OpenAPI (never hand-write request types); server state via TanStack Query; no secrets in `localStorage`; render model- or user-generated text as plain text; accessible by default (labels, keyboard, contrast); tests with Vitest/Testing Library and Playwright for flows.

---

## 12. Reporting completion and handing off

### 12.1 Before you declare completion

Run, from a clean checkout of your branch, and record the exact commands and outcomes:

```text
make lint
make test                       # unit, -race
make test-integration           # if your task touches Postgres/Kafka/Raft/AI
<task-specific targets listed in the task file>
make verify-generated           # drift checks for proto/openapi/sqlc (when present)
```

Then walk the task's **Acceptance Criteria** one by one and tie each to evidence (test name, command output, file path, screenshot, metric). A criterion without evidence is not met.

### 12.2 Completion report (mandatory)

Copy `00F_COMPLETION_REPORT_TEMPLATE.md` to `docs/implementation-reports/NN.NN.md` in the repo, fill every section, commit it in the PR. It MUST contain:

1. Status: `COMPLETE` / `COMPLETE_WITH_DEVIATIONS` / `PARTIAL` / `BLOCKED`.
2. What was implemented (by acceptance criterion, with evidence).
3. Files created/modified (grouped) and migrations added.
4. Tests added and results (commands, counts, coverage for gated packages, race/stress runs, seeds).
5. **Deviations and clarifications** from the task/ADR/contracts, with rationale and classification (§7.1–7.3). "None" is a valid answer; silence is not.
6. Contract/ADR amendments made (links).
7. Known stubs, limitations, technical debt, and open questions.
8. Defects found in earlier tasks (and how handled).
9. Performance/benchmark observations if relevant.
10. Docs updated (list).
11. **Handoff notes** for the next task(s) (§12.3).

### 12.3 Handoff notes (what the next agent needs)

- **Provides:** the exact packages, types, functions, endpoints, tables, make targets, env vars, metrics and events now available — names and paths, not prose. Reconcile with the task file's "Artifacts provided" list and say if anything differs.
- **How to run and verify** your work in one or two commands (including any Compose profile).
- **Gotchas:** non-obvious invariants, ordering requirements, test harness quirks, flaky areas, performance cliffs.
- **Open items the next task must handle** or decisions it must not reverse.
- **Parallel work:** if your task has parallel peers (`00B`), state which shared files/migration numbers/Compose services you touched so the peer can rebase cleanly.
- Update `00B_IMPLEMENTATION_INDEX.md` progress checklist (tick your task) and, when it exists, `docs/traceability.md`.

### 12.4 Blocked or partial work

If you cannot complete the task, do **not** mark it complete and do **not** leave the branch in a broken state. Commit only what is green, write a `PARTIAL` or `BLOCKED` report that states: what is done, what is not, exactly what blocked you, what you tried, the smallest decision or input needed, and a recommended path. Downstream tasks stay unstarted.

### 12.5 Final handoff message (template)

```text
TASK NN.NN — <title>: <COMPLETE | COMPLETE_WITH_DEVIATIONS | PARTIAL | BLOCKED>
PR/branch: <link/name>        Report: docs/implementation-reports/NN.NN.md
Acceptance: <k>/<n> met (evidence in report)
Deviations: <none | list with class>
Provides for next task (NN.NN+1): <key artifacts>
Verify with: <command(s)>
Watch out for: <gotchas>
Next: <next task id and file>  |  Parallel peers: <ids or none>
```

---

## 13. Quick pre-merge checklist (copy into every PR)

- [ ] Read spec, ADRs, contracts, task file, previous report
- [ ] Scope limited to this task; no unrelated edits
- [ ] No architectural change without an approved ADR; amendments recorded
- [ ] No existing tests weakened or removed; baseline vs. after recorded
- [ ] New behavior tested: unit + failure paths + (integration/e2e as required)
- [ ] `-race` clean; goleak clean for long-running components
- [ ] Errors wrapped, mapped, logged once; no secrets/payloads in logs/labels/spans
- [ ] Time/IDs/randomness injected; pure packages stay pure
- [ ] Dependencies justified, pinned, licensed, scanned
- [ ] Migrations forward-only, tested from empty and from previous
- [ ] Generated code regenerated and committed; drift checks pass
- [ ] Docs, runbooks, OpenAPI, `00E`, metrics catalogue updated as needed
- [ ] Completion report committed; `00B` progress updated
- [ ] Handoff note written for the next task and parallel peers
