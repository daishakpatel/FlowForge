# 00C — Agent Operating Rules (apply to EVERY task)

You are one of several AI coding agents building FlowForge incrementally. You may have no memory of earlier work. Everything you need is in the repository and in these documents.

Sections 1–6 are the procedure every task follows. Sections 7–14 are the conventions the code and the PR must meet. Rules written as **MUST** or **never** have no judgment exception. Rules written as **SHOULD** may be departed from, with a one-line reason in the PR.

## 1. Mandatory reading order (before writing any code)

1. `implementation/00_MASTER_PROJECT_SPEC.md` — requirements and architecture.
2. `implementation/00A_ARCHITECTURE_DECISIONS.md` — decisions you must not silently change.
3. `implementation/00E_CONTRACTS_REFERENCE.md` — canonical schemas, states, names.
4. Your task file (`implementation/phase-*/NN.NN_*.md`), including the *Context and Reference Files* section.
5. Every **predecessor** task file listed under *Hard dependencies*, **and** its completion report at `docs/implementation-reports/<TASK_ID>.md` in the repository.
6. The existing code in the packages your task touches.

If a predecessor's completion report is missing, **stop** and report `BLOCKED: predecessor <ID> has no completion report` — do not reimplement it.

## 2. Working rules

1. **Do not rewrite or duplicate** existing implementations. Extend through the interfaces already established; if an interface is inadequate, make the smallest backwards-compatible change and document it. The detailed rules are in §12.
2. **Preserve compatibility** with already-implemented components. Run the full existing test suite (`make test`) before and after your change; you must not leave it red. Record the failures that already exist before you start, report them as pre-existing, and fix only the ones your task owns.
3. Follow repository conventions (layout in master spec §9; lint config; error handling via `internal/apperrors`; logging via `internal/logging`; IDs via `internal/ids`; time via `internal/clock`). The detailed conventions are in §8–§10.
4. **Do not silently change architecture.** Classify every departure from the task file, `00E` or the ADRs at the moment you make it:
   - **Clarification** — you fill in a detail the documents leave open, or correct a wrong detail, without changing any contract. Examples: the actual library function name, a default inside the documented range, an internal helper type. Record it under *Deviations* in your report, with a one-line reason.
   - **Compatible extension** — you add something without altering existing behavior or contracts. Examples: an optional field, a new endpoint, metric, index, internal package or error code. Update `00E` (and the master spec if you add a requirement). Append an amendment to `00A` with the date, task ID, what, why and impact. Add tests, and keep every existing test unchanged and green.
   - **Architectural change** — any of these:
     - changing an ADR, or the Postgres/Raft authority split
     - changing a public contract incompatibly: renaming, removing or retyping a field, route, topic, dashboard metric, env var, column, state or transition
     - changing delivery or consistency semantics
     - replacing an approved dependency (§10.3), or adding a datastore, queue or service
     - changing service boundaries or topology
     - relaxing a safety invariant (ADR-003, ADR-005, ADR-007, ADR-011)

     Stop work on that part and write a proposed ADR at `docs/decisions/proposed/ADR-XXX-<slug>.md`. It covers: context, at least two options including "do nothing", the decision, consequences, a compatibility plan, the impacted tasks from `00B`, and a test plan. Then escalate (§5). Implement only after approval, and update every downstream task file the ADR lists.
5. **Tests are part of the task.** Write the tests listed in the task, run them, and report real results. Never mark a test as skipped to get green without recording it as a known limitation. The testing standards are in §11.
6. Pin and record any new dependency version in `docs/decisions/versions.md`. Verify package paths/versions at implementation time (do not trust memory for exact versions or module paths). Library APIs in task files are described from the design's point of view, so confirm the exact signatures in the version you import.
7. Keep changes scoped to the task. Out-of-scope discoveries go into the completion report under *Follow-ups*, not into drive-by edits.
8. No secrets in the repository. No real network calls in unit tests.
9. Every public function/type carrying a contract has a doc comment explaining invariants (especially concurrency and fencing).
10. Concurrency code: run with `-race`; no goroutine leaks (use `goleak` in packages that spawn goroutines).
11. Generated code (`sqlc`, `buf`, OpenAPI client) is committed; regeneration commands live in the Makefile; CI checks drift.
12. When sources conflict, precedence is (highest first): `00E`, your task file, `00A`, the master spec, this file. Record every conflict in your completion report under *Deviations*; if the order does not settle it, stop and escalate (§5).
13. If you are blocked by an environment limitation (e.g. no Docker available), implement everything possible, mark Docker-dependent tests with the repo's `integration` build tag, state clearly in the report what could not be executed, and do not claim they passed.

## 3. Definition of "task complete"

A task is complete when all of these hold:

- Every *Acceptance Criteria* checkbox is true and backed by evidence: a test name, command output or file path. A criterion without evidence is not met.
- The tests are green: `make lint test`, plus `make test-integration` when the task touches Postgres, Kafka, Raft or the AI service, plus the task-specific commands. They run from a clean checkout of your branch.
- No unrelated files changed, no code is commented out, and every `TODO` references an issue.
- The documentation is updated (§13).
- The completion report is written to `docs/implementation-reports/<TASK_ID>.md` using `00F_COMPLETION_REPORT_TEMPLATE.md`.
- The task's line in the `00B` index checklist is ticked. The human or orchestrator does this; agents may tick their own line.

The report starts with a status line: `COMPLETE`, `COMPLETE_WITH_DEVIATIONS`, `PARTIAL` or `BLOCKED`. Every section is filled in; "None" is a valid answer, silence is not. The report also lists:

- any temporary stubs left in place
- defects found in earlier tasks, and how you handled them

If you cannot finish:

- Leave the branch unbroken and commit only work that is green.
- Write a `PARTIAL` or `BLOCKED` report. It states what is done, what is not, the exact blocker, what you tried, the smallest decision or input needed, and your recommended path.
- Downstream tasks stay unstarted.

## 4. Handoff protocol

The next agent relies on your report's **Handoff** section. State precisely: interfaces/contracts created (package path + exported symbol names), configuration added, migrations added (numbers), Make targets added, invariants other code must preserve, known issues. Prefer facts over narrative. Also include:

- **A reconciliation** of what you provide against the task file's handoff list, naming anything that differs.
- **How to verify** your work, in one or two commands (with the Compose profile, if one is needed).
- **Gotchas:** non-obvious ordering requirements, quirks in the test harness, flaky areas, performance cliffs.
- **Shared resources touched**, if the task has parallel peers in `00B` §5: shared files, migration numbers and Compose services, so the peer can rebase cleanly.

## 5. Escalation

Stop and ask (return a `BLOCKED:` message, do not guess) when:

- a requirement is contradictory
- a security-sensitive decision is not covered by the ADRs
- a dependency's actual API makes the specified design impossible
- tests reveal a defect in a predecessor's contract that cannot be fixed compatibly within your scope (include a minimal reproduction)
- sources conflict and §2.12 does not settle it
- a dependency's output is missing or doesn't match its handoff list
- you need an architectural change (§2.4)
- two parallel tasks overlap: the same file, migration number or Compose service

An escalation is a `BLOCKED` report (§3) plus, when a human or orchestrator channel exists, a message giving the task ID, what is blocked, what you tried, the options, and your recommendation.

## 6. Code quality bar

Small functions; explicit error wrapping (`fmt.Errorf("…: %w", err)`); context propagation everywhere; table-driven tests; no `panic` in library code; deterministic tests (fake clock, seeded RNG); exported behavior documented in `docs/`.

## 7. Git workflow

- **Branch:** each task gets one branch, `task/NN.NN-short-slug`, and one PR, branched from the latest base that contains all of the task's dependencies.
- **Commit format** (Conventional Commits, with the task ID as the scope):

  ```text
  <type>(NN.NN): <imperative summary, ≤ 72 chars>

  <why, not just what>

  Refs: FR-XXX-NN, ADR-0NN
  ```

  Types are `feat`, `fix`, `test`, `docs`, `refactor`, `perf`, `build`, `ci` and `chore`. Any change to a public contract needs a `BREAKING CHANGE:` footer.
- **Commit size:** commits are small, and each one builds and passes tests. Use separate commits for migrations, generated code (with the generating command in the message), implementation, tests and docs when practical.
- **Scope:** refactors, formatting sweeps, dependency bumps and mass renames go in their own behavior-neutral PRs.
- **History:** never force-push to `main` or rewrite history others have pulled. Rebase your own branch freely until review starts, then add commits instead.
- **Never commit** secrets, `.env` files with real values, large binaries, build output or coverage files.
- **PR body:** the task ID and title; the requirements covered; a summary; the deviations (or "none"); migrations added; new env vars, metrics, routes, events or topics; test commands and results; a link to the completion report.
- **Merging:** needs green CI, an approved review, a branch up to date with its base, the report committed and `00B` updated. Squash-merge only if the squashed message keeps the commit format; otherwise rebase-merge.

## 8. Go conventions

- **Formatting:** `gofmt` + `goimports`, and `golangci-lint` with the committed config. Every `//nolint` carries a reason: `//nolint:rule // why`.
- **Packages:** short, lowercase and singular, with no `util`, `common`, `helpers` or `base` packages and no stutter (`scheduler.Scheduler`, not `scheduler.SchedulerService`).
- **Files:** `lower_snake.go`, with tests beside the code named `Test<Unit>_<Scenario>`.
- **Identifiers:** exported names are `PascalCase` and acronyms stay uppercase (`ID`, `URL`, `HTTP`). Enums are typed (`type TaskStatus string`), with `UPPER_SNAKE` values that match the DB and API.
- **Interfaces:** small, defined where they are consumed, and named for behavior (`Publisher`, `Gate`, `Clock`). Accept interfaces and return concrete types. Add an interface only for a documented seam or a test fake.
- **Context:** `ctx context.Context` is the first parameter of anything that does I/O or can block. Never store it in a struct, and respect cancellation in every loop and blocking call.
- **Concurrency:** every goroutine has an owner, a stop signal and a join (`errgroup`, `sync.WaitGroup`). Channels are bounded, and when a type holds more than one mutex, the lock order is documented.
- **No global state:** no `init()` side effects and no package-level mutable variables.
- **Size (SHOULD):** functions ≤ 60 lines and files ≤ 600 lines; split them rather than raising the limits.
- **Comments:** doc comments start with the identifier's name. Comment *why*; safety arguments and ordering requirements sit next to the code they protect, e.g. "assert epoch in the same tx as the state change — ADR-003".
- **Import direction:** `cmd → internal/{api,worker,scheduler,…} → domain/repository → platform` (clock, ids, config, obs). Lower layers never import higher ones. `cmd/*/main.go` stays thin: it parses config, wires dependencies and runs.

### 8.1 Determinism (non-negotiable)

- Only `cmd/*/main.go` and the real clock implementation call `time.Now()`; everything else takes a `clock.Clock`.
- IDs come from `internal/ids`. Randomness comes from an injected, seeded `rand.Source`; tests use fixed seeds and print the seed when they fail.
- `internal/raftkv/fsm` and `internal/stream/domain` are **pure**: no clock, randomness, network, goroutines, or dependence on map iteration order. They import no FlowForge application packages.
- Config comes only from `internal/config` (task 01.03), so no `os.Getenv` anywhere else.

## 9. Data, migrations, APIs and events

- **SQL:** queries live in `.sql` files compiled by sqlc. No SQL is built from strings, and there is no ORM.
- **State transitions:** use a pure transition function plus a conditional `UPDATE … WHERE id=$1 AND status=$2 [AND fencing_token=$3]`, and check `RowsAffected` (ADR-020). Scheduler transactions call `AssertEpoch` inside the same transaction (ADR-003). Transactions are short, never span a network call, and use the `TxManager` from Phase 02.
- **Naming:** tables are `snake_case` and plural, IDs are prefixed ULIDs stored as `text`, and timestamps are `timestamptz`.
- **Queries:**
  - Indexes and constraints go in the migration that creates the table.
  - A new frequent query path gets an index or a written justification.
  - Every `SELECT` has a `LIMIT` or pagination.
  - No N+1 patterns on hot paths.
- **Migrations:**
  - goose, forward-only once a migration is on `main`.
  - Files are named `NNNN_short_description.sql`, using the next free number in that database's directory. On a collision, the later task renumbers.
  - Each migration is idempotent where practical, transactional unless documented otherwise, and has a rollback note.
  - Each is tested both from an empty database and from the previous migration.
  - A destructive change is an architectural change (§2.4).
- **REST:**
  - Routes are `/api/v1/…` with plural nouns, `snake_case` JSON and RFC 3339 UTC times.
  - List endpoints paginate with `limit` and `cursor`, and state-changing POSTs accept `Idempotency-Key`.
  - Errors use the envelope and codes in `00E §4`.
  - Every new route goes into the role table in `00E §9` and into the RBAC test that iterates over all routes.
- **Contracts:** OpenAPI and proto files are the source of truth. Generated code is committed and never edited by hand. Proto fields are only ever added, and removed fields are `reserved`; `buf breaking` must pass.
- **Events:** use the envelope and JSON Schema in `00E §7`. Consumers ignore unknown fields. A breaking payload change needs a new `event_version`, with both versions published during a deprecation window.
- **Compatibility:** contracts change by addition. Deprecate before removing anything, and keep it for at least one release. Data migrations preserve data, and destructive steps go in two phases. Risky behavior sits behind an `FF_*` flag that defaults to the previous behavior.

## 10. Errors, logging, dependencies and security

### 10.1 Errors and retries

- Return errors. `panic` is only for programmer errors at startup. Goroutine entry points recover, log the stack, count a metric, and fail that unit of work.
- Wrap with `%w`, using lower-case messages without "failed to". Callers branch on sentinel or typed errors with `errors.Is/As`, never on strings. Log an error once, at the boundary that handles it. An intentionally ignored error is assigned to `_` with a comment.
- `internal/apperrors` classifies errors as retriable or permanent, and retry logic consults that classification.
- Every outbound call has a deadline. Retries use the shared helper: bounded exponential backoff with jitter, a maximum attempt count, and respect for cancellation. Only idempotent operations retry automatically; anything else carries a request ID or `Idempotency-Key` first.
- Domain errors become API codes only in the API layer. Internals (SQL, stack traces, hostnames) never reach clients; `request_id` always does. gRPC uses canonical codes plus `ErrorInfo`. A rejected stale completion is a normal outcome that gets counted.
- **Fail safe:**
  - Without proven leadership, a valid lease or a current epoch or fencing token, do nothing.
  - When an authoritative read fails, never act on a stale view.
  - Degraded behavior follows master spec §6.2 and shows up in metrics, health endpoints and logs.

### 10.2 Logging and metrics

- **Logs:** structured, using the logger from task 01.04, with `request_id`, `run_id`, `task_id`, `attempt_id`, `worker_id` and `trace_id` where they apply. Levels mean: `error` needs attention, `warn` is degraded or retrying, `info` is lifecycle, `debug` is detail.
- **Never put these in logs, metric labels or spans:** secrets, keys, tokens, DSNs with passwords, payload or result bodies, prices at info level, or raw task log lines. Pass user-supplied text through the redaction helper.
- **Metric names:** `flowforge_<area>_<name>_<unit>`. Counters end in `_total` and durations in `_seconds`, and names come from `00E §11`/`§11.1`. Labels are low-cardinality enums only, never IDs, users or free text.

### 10.3 Dependencies

- **Prefer the standard library.** A new dependency needs a reason, an active maintenance record, a clean security record and a permissive licence (MIT, BSD, Apache-2.0, ISC or MPL; never GPL, AGPL or SSPL).
- **Approved core stack** (replacing any of these is an architectural change):
  - Go: `pgx/v5`, `sqlc`, `goose`, `chi`, `franz-go`, `go.etcd.io/raft/v3`, `grpc-go`, `protobuf`/`buf`, `testcontainers-go`, `testify`/`go-cmp`, `goleak`, `porcupine`, OpenTelemetry, `prometheus/client_golang`
  - Python: FastAPI, Pydantic v2, LangChain, LangGraph, SQLAlchemy/psycopg, pgvector
  - Frontend: React, Vite, TypeScript, TanStack Query, React Flow, Vitest, Playwright
- **Pin everything:** `go.sum`, the Python lockfile, `package-lock.json`, image tags (digests for releases) and CI tool versions. Record them in `docs/decisions/versions.md`.
- **Add dependencies** only in the task that needs them, and do upgrades in their own PR. Commit the result of `go mod tidy`, and install developer tools through the pinned mechanism from 01.01.
- **CI scans** (`govulncheck`, `pip-audit`, `npm audit` at high and above, `gitleaks`, `trivy`) block the PR unless the finding is on the expiring allowlist with an owner.

### 10.4 Security

- Validate every external input against schemas and size limits: HTTP, gRPC, Kafka, files, tool output and LLM output.
- Authentication and authorization go only through the shared middleware. `ai-readonly` never writes.
- Logs, metrics, knowledge-base text and workflow definitions are **untrusted** once they flow into a prompt.
- Secrets come only from env or files through the config loader. There are no real default credentials, dev keys are labelled dev-only, and config containing secrets is never printed.
- Containers run as non-root with pinned images.

## 11. Testing

- **Behavior and tests:** every behavior has a test, and a bug fix starts with a failing test that reproduces the bug.
- **Determinism:** tests use a fake clock advanced explicitly, seeded randomness and no sleeps for synchronization (use channels, or `Eventually` with a bounded timeout). They don't depend on test order, and touch the network only through testcontainers or Compose.
- **Isolation:** tests are hermetic, with unique schemas, keys and topics per test, cleaned up in `t.Cleanup`.
- **Flakiness:** a flaky test is a bug. Quarantine one only with a linked issue and an owner, and never within your own task's acceptance tests.
- **Levels:**

  | Level | Required for |
  |---|---|
  | Unit | All pure logic |
  | Property or fuzz | Parsers, DAG validation, FSM determinism |
  | Integration | Repositories, SQL, handlers, Kafka paths and the outbox, using testcontainers under the `integration` build tag |
  | Contract | Every public endpoint and message |
  | Concurrency and failure | Anything with retries, leases, fencing or leadership (`goleak`, fault injection) |
  | End-to-end | Phase gates and user-visible flows, under `test/e2e` |
  | Porcupine | Raft KV paths |
  | Performance | Only where the task states a target; report misses honestly |

- **Coverage:** at least 70 % on `internal/{dag,workflow,scheduler,raftkv,stream}`, and new code there SHOULD reach 80 %. Assert behavior, not lines.
- **Stress:** race-prone code gets a stress run (`-count=20` with varied `GOMAXPROCS`), and the command goes in the report.
- **Failure paths:** for every external dependency you touch, test that it is unavailable, slow and returning errors. The expected behavior comes from master spec §6.2.
- **Fixtures:** fixtures live in `testdata/`. Golden files are regenerated only with `-update` and reviewed in the diff. Extend the shared helpers (`testutil`, `kafkatest`, `nodetest`) compatibly instead of re-implementing them.
- **Existing tests are protected:** never delete, skip, loosen or rewrite one to make your change pass. If your task changes the behavior a test encodes, change the test in its own commit and explain it in the report.

## 12. Working with existing code

- **Read first:** read any file you didn't create, its tests and its `git log -p`, and identify which task owns it.
- **Extend:** add new files, methods, interface implementations, options structs with defaults, or additive columns. Replace an implementation only when your task file says so and gives the reason.
- **Preserve public surfaces:** keep exported signatures, interface methods, JSON and proto fields, config keys, metric names and CLI flags. Changing one is a §2.4 change.
- **Use the seams:** integrate through the ones earlier tasks expose (`LeaseProvider`, `Gate`, `OutcomeHandler`, `CancelSource`, `InputResolver`, `Publisher`, `WindowSink`, `Applier`, `EpochFence`) rather than reaching into internals.
- **Generated and vendored code** changes only by regenerating it.
- **Destructive actions stay inside your own branch and workspace:** no `git reset --hard`, `git clean -fd`, deleting directories, or dropping databases and volumes that others use. `make dev-reset` on your own machine is fine.
- **Bugs in earlier work:** add a failing test, make the minimal fix in a separate commit that references the owning task, and report it under *Defects found in earlier tasks*. If the fix isn't minimal, escalate.
- **Temporary stubs and fakes** are clearly marked, listed in the report, and replaced by the task that owns the real implementation.

## 13. Documentation

| You changed… | Update in the same PR |
|---|---|
| REST or internal API | OpenAPI spec, generated client, `docs/api/*`, contract tests |
| Proto | `.proto`, generated code, docs |
| DB schema | Migration, sqlc queries, `00E` DDL if the contract changed, `docs/architecture/*` |
| Env var or config | Config struct docs, `00E §6`, `.env.example`, `docs/configuration.md` |
| Metric, log field or span | `metrics/catalogue.yaml` (from 08.02) or `00E §11`, plus the dashboards and alerts that use it |
| Event or Kafka schema | `schemas/events`, `docs/architecture/event-schemas.md` |
| Failure behavior | The relevant runbook, master spec §6.2 and its test |
| Make targets or tools | `README.md` |

- **What docs describe:** architecture docs describe the *implemented* behavior, with a Mermaid diagram where flow or state matters and honest limitations. Runbooks contain Symptoms, Impact, Diagnosis, Mitigation, Verification and Escalation, with commands copied from tested scripts.
- **Claims:** only claim guarantees a test proves (no "exactly-once"; no "highly available" without the fault test). Performance numbers appear only alongside their methodology.
- **Avoid duplication:** update existing documents rather than writing new ones, and link to `00E` and the ADRs instead of copying their tables.
- **Indexing and style:** every new `docs/**` file is linked from an index, and headings use sentence case.

## 14. Python and frontend

- **Python (`services/aiops`):**
  - Tooling: `ruff`, `ruff format`, and `mypy --strict` on `core` and `api`.
  - Errors use a custom exception hierarchy under `aiops.core.errors`, with no bare `except`.
  - Tool and provider failures become structured results (`ToolResult.error`), never uncaught exceptions inside the graph.
  - Providers and clients are injected. Tests run in mock mode with no network (`pytest-socket`).
  - Prompts are versioned files, and outputs are validated against JSON Schema.
  - The graph never executes actions, and every tool is GET-only.
- **Frontend (`web/`):**
  - TypeScript runs in strict mode, with no `any` unless justified.
  - The API client is generated from OpenAPI, server state goes through TanStack Query, and API errors are parsed into the typed `ApiError`, showing `code` and `request_id`.
  - Routes have error boundaries, and there is no silent `catch {}`.
  - Nothing secret goes in `localStorage`, and text from models or users renders as plain text.
  - Screens are accessible by default: labels, keyboard support and contrast.
  - Tests use Vitest with Testing Library, and Playwright for flows.
