# 00C — Agent Operating Rules (apply to EVERY task)

You are one of several AI coding agents building FlowForge incrementally. You may have no memory of earlier work. Everything you need is in the repository and in these documents.

## 1. Mandatory reading order (before writing any code)

1. `implementation/00_MASTER_PROJECT_SPEC.md` — requirements and architecture.
2. `implementation/00A_ARCHITECTURE_DECISIONS.md` — decisions you must not silently change.
3. `implementation/00E_CONTRACTS_REFERENCE.md` — canonical schemas, states, names.
4. Your task file (`implementation/phase-*/NN.NN_*.md`), including the *Context and Reference Files* section.
5. Every **predecessor** task file listed under *Hard dependencies*, **and** its completion report at `docs/implementation-reports/<TASK_ID>.md` in the repository.
6. The existing code in the packages your task touches.

If a predecessor's completion report is missing, **stop** and report `BLOCKED: predecessor <ID> has no completion report` — do not reimplement it.

## 2. Working rules

1. **Do not rewrite or duplicate** existing implementations. Extend through the interfaces already established; if an interface is inadequate, make the smallest backwards-compatible change and document it.
2. **Preserve compatibility** with already-implemented components. Run the full existing test suite (`make test`) before and after your change; you must not leave it red.
3. Follow repository conventions (layout in master spec §9; lint config; error handling via `internal/apperrors`; logging via `internal/logging`; IDs via `internal/ids`; time via `internal/clock`).
4. **Do not silently change architecture.** Deviation procedure: (a) implement the closest compliant solution if one exists; (b) if none does, make the minimal deviation, record it in your completion report under *Deviations*, and append an amendment to `00A` (and update `00E`/spec if a contract changes).
5. **Tests are part of the task.** Write the tests listed in the task, run them, and report real results. Never mark a test as skipped to get green without recording it as a known limitation.
6. Pin and record any new dependency version in `docs/decisions/versions.md`. Verify package paths/versions at implementation time (do not trust memory for exact versions or module paths).
7. Keep changes scoped to the task. Out-of-scope discoveries go into the completion report under *Follow-ups*, not into drive-by edits.
8. No secrets in the repository. No real network calls in unit tests.
9. Every public function/type carrying a contract has a doc comment explaining invariants (especially concurrency and fencing).
10. Concurrency code: run with `-race`; no goroutine leaks (use `goleak` in packages that spawn goroutines).
11. Generated code (`sqlc`, `buf`, OpenAPI client) is committed; regeneration commands live in the Makefile; CI checks drift.
12. If the task text conflicts with `00E`, `00E` wins; report the conflict.
13. If you are blocked by an environment limitation (e.g. no Docker available), implement everything possible, mark Docker-dependent tests with the repo's `integration` build tag, state clearly in the report what could not be executed, and do not claim they passed.

## 3. Definition of "task complete"

All *Acceptance Criteria* checkboxes true and evidenced; tests green (`make lint test` plus task-specific commands); documentation updated; completion report written to `docs/implementation-reports/<TASK_ID>.md` using `00F_COMPLETION_REPORT_TEMPLATE.md`; index checklist (`00B`) ticked by the human/orchestrator (agents may tick their own line).

## 4. Handoff protocol

The next agent relies on your report's **Handoff** section. State precisely: interfaces/contracts created (package path + exported symbol names), configuration added, migrations added (numbers), Make targets added, invariants other code must preserve, known issues. Prefer facts over narrative.

## 5. Escalation

Stop and ask (return a `BLOCKED:` message, do not guess) when: a requirement is contradictory, a security-sensitive decision is not covered by the ADRs, a dependency's actual API makes the specified design impossible, or tests reveal a defect in a predecessor's contract that cannot be fixed compatibly within your scope (include a minimal reproduction).

## 6. Code quality bar

Small functions; explicit error wrapping (`fmt.Errorf("…: %w", err)`); context propagation everywhere; table-driven tests; no `panic` in library code; deterministic tests (fake clock, seeded RNG); exported behavior documented in `docs/`.
