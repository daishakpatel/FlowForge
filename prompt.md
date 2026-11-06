# prompt.md — FlowForge task runner

This is a reusable prompt for implementing one or more FlowForge task files. It covers **orchestration** only: which tasks to run, in what order, on which branch, and when to stop. **How** to write, test, commit and report is defined in `AGENT_RULES.md`. **What** to build is defined in each task file. If this prompt and `AGENT_RULES.md` ever disagree, `AGENT_RULES.md` wins.

## Invocation

```text
implement <TARGET> using prompt.md and follow rules defined in AGENT_RULES.md [options]
```

| `<TARGET>` form | Example | Resolves to |
|---|---|---|
| Task ID | `01.03` | `implementation/phase-*/01.03_*.md` |
| Task file | `phase-01-foundation/01.03_configuration_management.md` | that file |
| Phase | `phase-01-foundation` or `phase-01-foundation.md` | every task file in `implementation/phase-01-foundation/` |
| Range | `01.02-01.04` | every task ID in that range, across phases |

| Option | Default | Effect |
|---|---|---|
| `base=<branch>` | `main` | Branch that task branches start from (AGENT_RULES §2.1) |
| `no-pr` | off | Commit locally only; skip push and PR |
| `pause` | off | Stop after each task and wait for "continue" |

## Step 0: Build the queue

1. Resolve `<TARGET>` into task files and sort them by task ID. That sorted list is the **queue**.
2. Mark each queued task **done** when both of these hold: its line in `implementation/00B_IMPLEMENTATION_INDEX.md` §7 is ticked, **and** `docs/implementation-reports/<ID>.md` exists with status `COMPLETE` or `COMPLETE_WITH_DEVIATIONS`. Done tasks are skipped. If only one of the two holds, stop and report the inconsistency.
3. For every task left to do, check each *Hard dependency* listed in task-file §1. It must be done already or appear earlier in the queue.
4. Print the queue as a table with these columns: ID, title, hard dependencies, done/todo.

**Step complete when:** the queue table is printed and every dependency check passes. If a dependency is neither done nor queued, stop with `BLOCKED: <ID> needs <dep>` before writing any code.

The tasks run **one at a time in queue order**, even pairs that `00B` §5 marks as parallel-safe. Spawn parallel agents only if the invocation asks for them.

## Per-task loop

Treat every task as a **cold start**. Base each decision on the files, the code and the predecessor reports, not on what you remember from the previous task in this run.

### Step 1: Preflight

1. Read in the order given in `00C` §1: the master spec, `00A`, `00E`, `00C`, the task file, then each hard-dependency task file **and** its completion report.
2. Check every item under *Artifacts that should already exist* in task-file §2: each path, symbol and make target must actually be in the tree. A missing artifact or predecessor report means `BLOCKED` (`00C` §1, AGENT_RULES §7.4).
3. Make sure the working tree is clean. If it has changes that are not yours, stop and ask the human; leave their work exactly as found.
4. Create the branch `task/NN.NN-short-slug`:
   - If every hard dependency is merged into `base`, branch from `base`.
   - Otherwise, branch from the branch of the latest dependency still unmerged in this run. This is a **stacked** branch. Record the parent branch in the report's *Branch / commit range* field.
5. Record a **baseline**: run `make lint test` and note any failures that already exist (AGENT_RULES §8.6). For `01.01` there is no Makefile yet, so the baseline is an empty repository.
6. Post a five-line **brief** in chat:
   - the objective
   - the number of acceptance criteria
   - the files in scope, from task-file §7
   - the dependencies you verified
   - any ambiguity you found

**Step complete when:** the brief is posted and the baseline is recorded.

### Step 2: Implement

1. Work through task-file §6 (*Step-by-Step Implementation Instructions*) in order. Write each piece of logic's tests together with it, test first for logic and bug fixes (AGENT_RULES §3.1).
2. Treat task-file §7 (*Files to Create or Modify*) as the **scope fence**. A file outside it goes in the report with a reason, or under *Follow-ups* if it isn't needed for this task.
3. Classify every departure from the task file, `00E` or the ADRs at the moment you make it: §7.1 clarification, §7.2 extension (needs a `00A` amendment) or §7.3 change. Log it as you go. For a §7.3 change, stop work on that part and follow §7.3.
4. Check each library API against the version you actually installed, using `go doc`, the module source or the package's types (AGENT_RULES §5.4).
5. Commit in small, bisectable steps using the format in AGENT_RULES §2.2.

**Step complete when:** every item in task-file §6 is done, and every file in §7 has been created or modified, or its absence is logged with a reason.

### Step 3: Verify

1. Run the commands in AGENT_RULES §12.1, the tests listed in task-file §8, and the **full** suite. Code with concurrency also needs `-race` and the stress run in §3.5. Record each exact command and its summary line.
2. Build an **evidence** table: each acceptance criterion in task-file §9 maps to a test name, a command output or a file path. A criterion with no evidence is not met.
3. If the environment prevents running something, such as Docker being unavailable, follow `00C` §2.13 and record it as not run, with the reason.
4. Review `git diff <parent>...HEAD` against the checklist in AGENT_RULES §13. Fix every box that fails before moving on.

**Step complete when:** every suite is green or each non-green result is listed with its reason, every acceptance criterion has evidence, and the §13 checklist passes.

### Step 4: Document and report

1. Update the documents named in task-file §10 and every row of the AGENT_RULES §6.1 table that your change triggers.
2. Copy `implementation/00F_COMPLETION_REPORT_TEMPLATE.md` to `docs/implementation-reports/<ID>.md` and fill every section with real results.
3. Add the AGENT_RULES §12.2 items that the template lacks:
   - the status line
   - the deviation classification
   - known stubs
   - defects found in earlier tasks

   "None" is a valid answer for any of these.
4. Complete the report's *Handoff* section so it covers both task-file §12 and AGENT_RULES §12.3.
5. Tick this task's line in `00B` §7. Do this only when the status is `COMPLETE` or `COMPLETE_WITH_DEVIATIONS`.
6. Commit the docs and the report with `docs(<ID>): ...`.

**Step complete when:** the report has no empty section, and every *Provides* item in it is reconciled against the task file's handoff list.

### Step 5: Hand off

1. Unless the invocation says `no-pr`: push the branch and open a PR against the parent branch, with the body required by AGENT_RULES §2.3. Merging is the human's call (§2.4). Leave `main` untouched and never force-push.
2. Print the handoff message from AGENT_RULES §12.5.
3. Decide what happens next:
   - If the status is `PARTIAL` or `BLOCKED`, end the run. Commit only work that is green (AGENT_RULES §12.4). Downstream tasks stay unstarted.
   - If the invocation says `pause`, wait for "continue".
   - Otherwise, start Step 1 for the next task in the queue.

## Run summary

When the queue is finished, or the run stops, print:

1. A table with these columns: task, status, branch (and its parent), PR, acceptance met as k/n, deviations.
2. The **merge order** for stacked PRs: parents before children.
3. Anything the human must decide or do. This includes escalations, proposed ADRs and any environment gaps that left tests unrun.
4. The next task ID in `00B` §1 order, which is the next target to invoke.

## Stop conditions

Stop and report `BLOCKED: <ID>: <reason>` (AGENT_RULES §7.4, `00C` §5) when any of these happens:

- A source conflict that the precedence order in AGENT_RULES doesn't settle.
- A missing predecessor artifact or report.
- Acceptance criteria that contradict each other or can't be met.
- A change that falls under §7.3.
- A security concern.
- An overlap with a parallel task's files.

The report must give what you tried, the smallest decision needed, and your recommended option.
