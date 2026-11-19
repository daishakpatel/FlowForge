# Phase 3 gate: workflow engine

**Result: PASS (2026-11-18).** Every Phase-1 acceptance criterion holds against real binaries running as subprocesses on Postgres 16 (Docker), measured with `make test-e2e` on the final tree. The suite is described in [e2e.md](e2e.md). The completion report is [`03.09`](../implementation-reports/03.09.md).

Timings come from one developer machine (Apple Silicon, macOS, Docker Desktop), with every binary built with `-race`. They are measurements, not service-level objectives.

## Phase-1 acceptance criteria

The task file (03.09 §5) maps the v1.0 §19 Phase-1 criteria and FR-WF-09 to tests:

| Criterion | Test | Result | Time |
|---|---|---|---|
| A multi-step DAG completes | `TestDiamondAcrossTwoWorkers` | pass | 4.6 s (run 1.0 s) |
| A failed task retries per its policy | `TestRetryThenSucceed` | pass | 2.3 s |
| A scheduler restart doesn't lose pending state | `TestSchedulerRestartMidRun` | pass | 6.4 s |
| A worker restart is handled without corruption | `TestWorkerKilledMidTask`, `TestWorkerRestartedWithSameID`, `TestZombieWorkerIsRejected` | pass | 10.3 s, 14.5 s, 10.4 s |
| Invalid DAGs are rejected | `TestCLIEndToEnd`: `flowforgectl workflows create` of `cyclic.json` exits 1 with `INVALID_WORKFLOW` / `CYCLE` | pass | 11.9 s |
| FR-WF-09: scheduler, API and worker restarts and a database blip neither lose nor double-commit work | the restart tests above, `TestAPIRestartMidRun`, `TestDatabasePause` and `TestSoakChaos`; `AssertHealthy` checks the invariants and that no task has two `SUCCEEDED` attempts | pass | 6.4 s, 10.0 s, 116 s |

## The rest of the scenario library

| Test | Result | Time |
|---|---|---|
| `TestFanOutRespectsCapacity` | pass | 2.9 s |
| `TestNonRetryableFailure` | pass | 2.0 s |
| `TestTaskTimeout` | pass | 1.7 s |
| `TestCancelRun` | pass | 2.9 s |
| `TestPauseResume` | pass | 8.3 s |
| `TestRerunAfterFailure` | pass | 8.9 s |
| `TestTemplatingChain` | pass | 1.9 s |
| `TestCronTriggerFires` | pass | 38.4 s (waits for the minute boundary) |
| `TestIdempotentStart` | pass | 2.7 s |
| `TestSSEConsumption` | pass | 3.7 s |
| `TestDemoScript` | pass | 22.2 s |

The whole suite, soak included, took 298 s.

## Recovery times

Timings use the suite's settings: lease TTL 3 s, heartbeat 1 s, scheduler tick 100 ms, recovery tick 2 s. Production defaults are TTL 15 s and heartbeat 5 s; the bounds scale with them.

| Event | Measured from → to | Two runs | Bound |
|---|---|---|---|
| **Worker loss** (SIGKILL) | kill → the task's next attempt `RUNNING` on another worker | 3.9 s, 4.0 s | lease TTL + recovery tick (≤ 5 s) |
| **Worker restarted under the same id** | kill → the dead incarnation's attempt replaced | 8.0 s, 8.1 s | 3×heartbeat + TTL + recovery tick (≤ 8 s); before the 03.09 fix, the task timeout (1 h by default) |
| **Scheduler restart** | restart → `/readyz` | 121 ms, 101 ms | — |
| | restart → the next task claimed | 126 ms, 104 ms | one scheduler tick |
| **API restart** | restart → `/readyz` | 124 ms, 127 ms | — |
| | restart → the next task claimed (the worker reconnects and reports the finished one) | 1.54 s, 1.54 s | — |
| **Postgres frozen 5 s** (`docker pause`) | unpause → run complete (3 tasks of 1.5 s left) | 3.1 s, 3.2 s | — |
| **Cancel** | `POST cancel` → run `CANCELLED` | 1.0 s, 1.0 s | one heartbeat |

## Soak and chaos

This is `TestSoakChaos` (03.09 §4). Each run starts 500 runs × 6 tasks on 4 workers of capacity 4. Chaos SIGKILLs a random worker every 10 s and restarts it under the same id, and SIGKILLs and restarts the scheduler every 30 s.

| Seed | Runs succeeded | Worker kills | Scheduler restarts | Abandoned attempts recovered | Tasks with 2+ successes | Invariants | Duration |
|---|---|---|---|---|---|---|---|
| 1 | 500/500 | 10 | 3 | 40 | 0 | hold | 1m49s |
| 2 | 500/500 | 11 | 3 | 44 | 0 | hold | 1m59s |
| 3 | 500/500 | 11 | 3 | 39 | 0 | hold | 1m51s |
| 1795058435002821000 | 500/500 | 10 | 3 | 40 | 0 | hold | 1m50s |
| 1795059466286506000 | 500/500 | 10 | 3 | 39 | 0 | hold | 1m49s |

Replay a seed with `FF_E2E_SEED=<seed> go test -tags e2e -run TestSoakChaos -v ./test/e2e/`.

## Defects the gate found

| Defect | Severity | Fixed in 03.09 |
|---|---|---|
| A worker killed and restarted under the same id left its previous incarnation's attempts `RUNNING` until their deadline, 1 h by default. Recovery's fallback skipped attempts whose worker id held a live lease. | High: such runs stall for an hour | `ListOrphanedAttempts` no longer checks leases (`00A`, "ADR-007 amendment — 03.09"). Regression tests: `TestWorkerLoss_FallbackForRestartedWorker` and `TestWorkerRestartedWithSameID` |
| `TestPollTask_LongPoll` flaked: the test environment didn't wait for LISTEN, so the first NOTIFY could be lost | Test only | The env waits for the listener (`internal/workerapi`) |

## Expected vs observed (for NFR-TEST-03)

| Fault | Expected | Observed |
|---|---|---|
| Worker SIGKILL | The attempt is `ABANDONED` (`WORKER_LOST`) after the lease expires and retried elsewhere; no double success | As expected |
| Worker SIGSTOP, then SIGCONT | The lease is lost and the task reassigned; on resume, the zombie's result is rejected as stale | As expected; the worker logs `attempt is stale; result dropped` |
| Scheduler SIGKILL | No state lost; decisions resume within a tick of restart | As expected |
| API SIGKILL | Workers reconnect; in-flight reports are retried | As expected |
| Postgres frozen 5 s | Loops back off and resume; no invariant broken | As expected |
| Task over its timeout | Attempt ends at the deadline; no retries with max_attempts 1 | The worker reports `DEADLINE` just before the deadline, so the task is `FAILED`, not `TIMED_OUT` (documented in `handlers.md`; see the 03.09 report, §7) |

The cross-phase fault report, `docs/testing/fault-report.md`, is compiled by later tasks; this section is the workflow engine's part of it.
