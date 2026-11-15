# prompt.md — FlowForge task runner

This is a reusable prompt for implementing one or more FlowForge task files. It covers **orchestration** only: which tasks to run, in what order, and when to stop. The agent writes code and suggests commits; **the human commits, pushes and merges** (`00C` §7). **How** to work is defined in `implementation/00C_AGENT_OPERATING_RULES.md` (`00C`). **What** to build is defined in each task file. If this prompt and `00C` ever disagree, `00C` wins.

## Invocation

```text
implement <TARGET> using prompt.md and follow rules defined in 00C_AGENT_OPERATING_RULES.md [options]
```

| `<TARGET>` form | Example | Resolves to |
|---|---|---|
| Task ID | `01.03` | `implementation/phase-*/01.03_*.md` |
| Task file | `phase-01-foundation/01.03_configuration_management.md` | that file |
| Phase | `phase-01-foundation` or `phase-01-foundation.md` | every task file in `implementation/phase-01-foundation/` |
| Range | `01.02-01.04` | every task ID in that range, across phases |

| Option | Default | Effect |
|---|---|---|
| `pause` | off | Stop after each task and wait for "continue" (for example, so you can commit it) |
| `agents` | off | Allow the agent-backed skills marked † in [Skills and plugins](#skills-and-plugins): review subagents, `code-review` at `max`, and `claude-security` |

## Step 0: Build the queue

1. Resolve `<TARGET>` into task files and sort them by task ID. That sorted list is the **queue**.
2. Mark each queued task **done** when both of these hold: its line in `implementation/00B_IMPLEMENTATION_INDEX.md` §7 is ticked, **and** `docs/implementation-reports/<ID>.md` exists with status `COMPLETE` or `COMPLETE_WITH_DEVIATIONS`. Done tasks are skipped. If only one of the two holds, stop and report the inconsistency.
3. For every task left to do, check each *Hard dependency* listed in task-file §1. It must be done already or appear earlier in the queue.
4. Print the queue as a table with these columns: ID, title, hard dependencies, done/todo.

**Step complete when:** the queue table is printed and every dependency check passes. If a dependency is neither done nor queued, stop with `BLOCKED: <ID> needs <dep>` before writing any code.

The tasks run **one at a time in queue order**, even pairs that `00B` §5 marks as parallel-safe. Spawn parallel agents only if the invocation asks for them.

## Per-task loop

Treat every task as a **cold start**. Base each decision on the files, the code and the predecessor reports, not on what you remember from the previous task in this run.

At each step, invoke the skills that [Skills and plugins](#skills-and-plugins) lists for that step and for the task's kind.

### Step 1: Preflight

1. Read in the order given in `00C` §1: the master spec, `00A`, `00E`, the task file, each hard-dependency task file **and** its completion report, then the existing code in the packages the task touches.
2. Check every item under *Artifacts that should already exist* in task-file §2: each path, symbol and make target must actually be in the tree. A missing artifact or predecessor report means `BLOCKED` (`00C` §1).
3. Check `git status`. Uncommitted changes from tasks marked done (their reports exist) or from earlier tasks in this run are expected, because the human commits later. If there are any other changes, stop and ask the human, and leave their work exactly as found.
4. Work on the current branch. Create no branch, and record the branch name in the report's *Branch / commit range* field (`00C` §7).
5. Record a **baseline**: run `make lint test` and note any failures that already exist (`00C` §2.2). For `01.01` there is no Makefile yet, so the baseline is an empty repository.
6. Post a five-line **brief** in chat:
   - the objective
   - the number of acceptance criteria
   - the files in scope, from task-file §7
   - the dependencies you verified
   - any ambiguity you found

**Step complete when:** the brief is posted and the baseline is recorded.

### Step 2: Implement

1. Work through task-file §6 (*Step-by-Step Implementation Instructions*) in order. Write each piece of logic's tests together with it, test first for logic and bug fixes (`00C` §2.5).
2. Treat task-file §7 (*Files to Create or Modify*) as the **scope fence**. A file outside it goes in the report with a reason, or under *Follow-ups* if it isn't needed for this task (`00C` §2.7).
3. Handle every departure from the task file, `00E` or the ADRs at the moment you make it. Resolve conflicts with the precedence in `00C` §2.12, and follow the deviation procedure in `00C` §2.4. Log each one as you go.
4. Check each library API and version against what you actually installed, using `go doc`, the module source or the package's types, and record new versions in `docs/decisions/versions.md` (`00C` §2.6).
5. As you go, group the changes into **suggested commits** that each build and pass tests, using the format in `00C` §7. Leave everything uncommitted.

**Step complete when:** every item in task-file §6 is done, and every file in §7 has been created or modified, or its absence is logged with a reason.

### Step 3: Verify

1. Run `make lint test`, the commands and tests listed in task-file §8, and the **full** suite. Code with concurrency also needs `-race` and `goleak` (`00C` §2.10). Record each exact command and its summary line.
2. Build an **evidence** table: each acceptance criterion in task-file §9 maps to a test name, a command output or a file path. A criterion with no evidence is not met.
3. If the environment prevents running something, such as Docker being unavailable, follow `00C` §2.13 and record it as not run, with the reason.
4. Review this task's changes (`git status` and `git diff`, including untracked files) against `00C` §2 and §6–§12. Fix every rule that fails before moving on.

**Step complete when:** every suite is green or each non-green result is listed with its reason, every acceptance criterion has evidence, and the `00C` review passes.

### Step 4: Document and report

1. Update the documents named in task-file §10 and every row of the `00C` §13 table that your change triggers. For each compatible extension, also update `00E` and add a `00A` amendment (`00C` §2.4).
2. Copy `implementation/00F_COMPLETION_REPORT_TEMPLATE.md` to `docs/implementation-reports/<ID>.md` and fill every section with real results.
3. Add the status line and the extra items that `00C` §3 requires.
4. Complete the report's *Handoff* section so it covers both task-file §12 and `00C` §4, including the suggested commits (the report and the `00B` tick go in a final `docs(<ID>): ...` commit).
5. Tick this task's line in `00B` §7. Do this only when the status is `COMPLETE` or `COMPLETE_WITH_DEVIATIONS`.

**Step complete when:** the report has no empty section, and every interface it lists is reconciled against the task file's handoff list.

### Step 5: Hand off

1. Print a short handoff message in chat: `TASK <ID> — <status>`, acceptance met as k/n, deviations, the verify command, the suggested commits (files and message for each), and the next task.
2. Decide what happens next:
   - If the status is `PARTIAL` or `BLOCKED`, end the run. Leave only green work in the tree. Downstream tasks stay unstarted.
   - If the invocation says `pause`, wait for "continue".
   - Otherwise, start Step 1 for the next task in the queue.

## Skills and plugins

Skills supply method; `00C` and this prompt supply the rules. Invoke them with the Skill tool by their exact name.

### Rules

1. **`00C` wins.** Ignore any part of a skill that conflicts with `00C`. In particular, ignore instructions to:
   - commit, push, stash, open a PR, or create a branch or worktree (`00C` §7);
   - write plan or spec files outside the task's scope fence;
   - stop mid-task to ask questions that the brief or a `BLOCKED` report should carry.
2. **Invoke each skill once per run.** If a skill's instructions are already in context from earlier in the run, follow them again without re-invoking. Re-invoke only after the context has been compacted and the instructions are gone.
3. **Use only what fits.** Invoke a skill only when the task actually has that kind of work. A skill that turns out not to apply is dropped, not forced.
4. **† = agent-backed:** it spawns subagents or a cloud review, and costs far more. Use it only when the invocation includes `agents`, or the human asks for it. Without that, use the inline alternative listed next to it.
5. **Record what you used.** List the skills you invoked in the report's *Agent/session identifier* line, e.g. `Claude Code (…); skills: superpowers:test-driven-development, code-review`.

### By step (every task)

| Step | When | Invoke |
|---|---|---|
| 1 Preflight | The task is Large, or §4 leaves real design choices open | `agent-skills:spec-driven-development` (settle the spec in the brief, not in new files) |
| 2 Implement | Logic, parsers, state machines, bug fixes | `superpowers:test-driven-development` |
| 2 Implement | A test fails unexpectedly, a test is flaky, or behavior surprises you | `superpowers:systematic-debugging`, before proposing any fix |
| 2 Implement | Choosing between designs of similar size | `ponytail:ponytail` (smallest complete change) |
| 3 Verify | Before claiming any suite is green or a criterion is met | `superpowers:verification-before-completion` |
| 3 Verify | Always, on the task's diff | `code-review` at `medium`; `high` for P0 tasks and phase gates. † `max` or `ultra` |
| 3 Verify | Go changes | `ecc:go-review`; † the `ecc:go-reviewer` agent |
| 3 Verify | Error paths, retries, fallbacks, goroutines | † the `ecc:silent-failure-hunter` agent; inline: check them in `code-review` |
| 3 Verify | Auth, input validation, secrets, network exposure | `security-review` |
| 3 Verify | Acting on review findings | `superpowers:receiving-code-review` (verify each finding before changing code) |
| 4 Document | A `00A` amendment or a proposed ADR (`00C` §2.4) | `agent-skills:documentation-and-adrs` |

### By task kind

| Kind | Tasks | Invoke |
|---|---|---|
| Tooling, CI, dev environment | 01.01–01.02 | `agent-skills:ci-cd-and-automation`, `ecc:docker-patterns` |
| Go platform code (config, logging, clock, ids) | 01.03–01.04 | `ecc:golang-patterns`, `ecc:error-handling` |
| Schema, migrations, SQL, repositories | 02.01, 02.03, 05.02 | `ecc:postgres-patterns`, `ecc:database-migrations`; † the `ecc:database-reviewer` agent |
| Pure domain logic, DAGs, parsers, state machines | 02.02, 03.06, 04.03 | `ecc:golang-testing` (table, fuzz and property tests), `superpowers:test-driven-development` |
| REST, gRPC and auth contracts | 02.04, 02.05, 03.01, 04.04, 05.09, 06.02 | `agent-skills:api-and-interface-design`, `ecc:api-design`; for auth also `agent-skills:security-and-hardening` |
| Scheduler, workers, recovery, events | 03.02–03.08 | `ecc:golang-patterns`; `superpowers:systematic-debugging` for any race or flake |
| Raft and coordination | 04.01–04.07 | `fullstack-dev-skills:golang-pro`, `ecc:golang-testing` |
| Fault injection and chaos | 04.08, 09.04 | `fullstack-dev-skills:chaos-engineer` |
| Kafka and stream processing | 05.01, 05.03–05.08 | `ecc:golang-patterns`; for backpressure and throughput, `agent-skills:performance-optimization` |
| Python AI service | 06.01–06.05, 06.07 | `python-development:python-testing-patterns`, `python-development:async-python-patterns`, `python-development:python-type-safety`, `ecc:fastapi-patterns`. For LLM providers and prompts, `claude-api`. For retrieval (06.03), `fullstack-dev-skills:rag-architect`. Review: `ecc:python-review` |
| Go side of AI integration | 06.06 | `agent-skills:api-and-interface-design`, `security-review` (approvals execute actions) |
| Dashboard UI | 07.01–07.03 | `frontend-design:frontend-design`, `ecc:react-patterns`, `ecc:react-testing`, `ecc:typescript-patterns`, `ecc:frontend-a11y`. For charts, `dataviz`. For flows, `ecc:e2e-testing`, and `run` to see the change working |
| Tracing, metrics, dashboards, alerts | 08.01–08.03 | `agent-skills:observability-and-instrumentation`, `fullstack-dev-skills:monitoring-expert` |
| Security hardening, TLS | 09.01 | `agent-skills:security-and-hardening`, `security-review`; † `claude-security` (full scan with patches) |
| Containers, Compose, Kubernetes | 09.02, 09.06 | `ecc:docker-patterns`, `ecc:kubernetes-patterns`, `ecc:deployment-patterns` |
| Load tests and benchmarks | 09.03 | `ecc:benchmark`, `agent-skills:performance-optimization` |
| Documentation and runbooks | 09.05 | `agent-skills:documentation-and-adrs` |
| Phase gates and release | 03.09, 04.08, 05.10, 06.07, 07.03, 10.01 | `superpowers:verification-before-completion`, `ecc:e2e-testing`, `code-review` at `high`, `security-review`; † `code-review` `ultra` and `claude-security` at 10.01 |

### Not for this runner

- `loop` and `schedule`.
- The artifact and doc-publishing skills.
- The domain packs: healthcare, Laravel, Django, Spring Boot and similar.
- The orchestration suites that run their own multi-agent flows: `ecc:orch-*`, `ecc:multi-*`, `ecc:gan-*`, `ecc:santa-loop`, `superpowers:subagent-driven-development`.

They don't fit one-task-at-a-time work under `00C`, so use them only when the human asks for them by name.

## Run summary

When the queue is finished, or the run stops, print:

1. A table with these columns: task, status, acceptance met as k/n, deviations.
2. The **commit order**: all suggested commits for one task before the next task's.
3. Anything the human must decide or do. This includes escalations, `00A` amendments and any environment gaps that left tests unrun.
4. The next task ID in `00B` §1 order, which is the next target to invoke.

## Stop conditions

Stop and report `BLOCKED: <ID>: <reason>` (`00C` §5) when any of these happens:

- A source conflict that the precedence in `00C` §2.12 doesn't settle.
- A missing predecessor artifact or report.
- A requirement that is contradictory, or acceptance criteria that can't be met.
- A security-sensitive decision that the ADRs don't cover.
- A dependency's actual API that makes the specified design impossible.
- A defect in a predecessor's contract that can't be fixed compatibly within this task.

The report must give what you tried, the smallest decision needed, and your recommended option.
