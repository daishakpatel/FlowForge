# Retries, timeouts and recovery

How FlowForge keeps work moving when attempts fail, overrun, or lose their worker, and how it stays consistent through crashes (task 03.04, ADR-003, ADR-007, ADR-021). Execution is **at least once**: a task can run more than once, and handlers must be idempotent ([handlers](handlers.md)).

Code: `workerapi.RetryOutcomeHandler` and `workerapi.AttemptEnder` (`internal/workerapi/outcome_retry.go`), `scheduler.TimeoutLoop`, `recovery.Loop` (`internal/recovery`), and the failpoints in `internal/testutil/failpoint`.

## Failure model

| Failure | Detected by | What happens |
|---|---|---|
| A handler fails | The worker reports `FailTask` with a code and a retryable flag | `RetryOutcomeHandler` applies the decision table below |
| An attempt overruns its deadline | `TimeoutLoop`, every 1 s: RUNNING attempts with `deadline_at <= now` | The attempt becomes `TIMED_OUT` (code `TIMEOUT`); the task follows the decision table |
| A worker dies, hangs or is partitioned | `recovery.Loop`, every 2 s: workers whose lease the `LeaseProvider` reports expired | Each RUNNING attempt becomes `ABANDONED` (code `WORKER_LOST`); the task follows the decision table; the worker becomes `LOST` (event `worker.lost`) |
| An attempt no live process heartbeats (a crash, a cut-off shutdown, a deregistered worker, a worker restarted under the same id, a lost PollTask answer) | `recovery.Loop`'s fallback | Abandoned like the above once silent for 3×`FF_WORKER_HEARTBEAT` + `FF_WORKER_LEASE_TTL`, whatever its worker's lease: a live worker heartbeats every attempt it runs (amended in 03.09) |
| A zombie wakes up and reports | The API's fenced `UPDATE … WHERE status = 'RUNNING' AND fencing_token = $t` | `STALE_ATTEMPT`; the result is dropped |
| The scheduler or API crashes mid-decision | Nothing to detect: every decision is one transaction | It rolls back, and the next pass redoes it |
| Postgres is unreachable | The loops' queries fail | Each loop logs and counts the failure (`flowforge_loop_errors_total{loop}`) and backs off: the tick doubles per failure, up to 30 s. Nothing is decided outside a transaction, and the loops resume on their own |

## Retry decision table

Applied in the transaction that ends the attempt (a worker's report, `TimeoutLoop` or `recovery.Loop`):

| Attempt ended | Retryable flag / code | Attempts left | Task becomes |
|---|---|---|---|
| Failed | retryable, and the code is not in `non_retryable_errors` | yes | `RETRY_WAIT` |
| Failed | not retryable, or the code is listed | — | `FAILED` |
| Failed | retryable | no | `FAILED` |
| Timed out | (always retryable) | yes | `RETRY_WAIT` |
| Timed out | — | no | `TIMED_OUT` |
| Abandoned | (always retryable) | yes | `RETRY_WAIT` |
| Abandoned | — | no | `FAILED`, code `WORKER_LOST` |

- **Attempts left** means `attempt_count < max_attempts`. Abandoned attempts count, because `attempt_count` rises at every claim.
- **`RETRY_WAIT`** sets `available_at = now + statemachine.NextRetryDelay(policy, attempt)`: the initial delay times multiplier^(attempt−1), capped at `max_delay_ms`, with ± `jitter` drawn from a seeded source. The event is `task.retry_scheduled`, carrying `retry_delay_ms`.
- **Cancel requests:** a task whose run has a cancel request is `CANCELLED` instead of retried. That is the seam for 03.05.
- **Requeueing:** the scheduler's decision loop moves `RETRY_WAIT → QUEUED` once `available_at <= now`, within the same concurrency limits as READY tasks ([scheduler](scheduler.md)). The input resolved before the first attempt is kept.

## Time

Every time comparison in SQL takes the injected clock as a parameter: `available_at <= $now`, `deadline_at <= $now`, and lease expiry. Tests drive one `clock.Fake` through the API and all the loops. That's why `available_at` is set from the clock rather than the database's `now()`, which differs from ADR-021's wording (see the 03.04 amendment in `00A`).

`deadline_at` itself is still set by the database at claim time (`now() + timeout_seconds`). In production both are host clocks kept in sync by NTP. Time a task spends QUEUED does not count: `timeout_seconds` bounds an attempt, not the wait for a worker. The server's decision is final, so a worker that overruns finds its report rejected.

## Worker loss

```mermaid
sequenceDiagram
  participant W1 as Worker-1
  participant API as API
  participant S as Scheduler (recovery.Loop)
  participant PG as Postgres
  participant W2 as Worker-2
  W1->>API: Heartbeat (renews the lease: worker_observations.last_seen_at)
  Note over W1: killed / paused / partitioned
  S->>S: LeaseProvider.Expired() → w1 (unseen for FF_WORKER_LEASE_TTL)
  S->>PG: tx(epoch ok): attempt RUNNING→ABANDONED (fenced); task RUNNING→RETRY_WAIT
  S->>PG: tx(epoch ok): w1 ACTIVE→LOST if still unseen; outbox worker.lost
  S->>PG: decision loop: RETRY_WAIT→QUEUED once the delay passes
  W2->>API: PollTask → new attempt, fencing token N+1
  W1->>API: CompleteTask(token N)  [zombie]
  API-->>W1: FAILED_PRECONDITION (STALE_ATTEMPT)
```

- **Detection time:** a worker is noticed within lease TTL + 2 recovery ticks of its last heartbeat. `TestWorkerLoss_RecoveredAndZombieRejected` measured 12 s with a 10 s TTL and a 2 s tick, inside the 14 s bound.
- **Liveness comes only from `LeaseProvider`.** `PGLeaseProvider` uses heartbeats now; 04.07 swaps in Raft lease expirations without touching `recovery.Loop`.
- **Marking a worker LOST:** the mark is conditional on the worker still being ACTIVE and unseen, so a heartbeat that arrives meanwhile wins. A worker that registers again is ACTIVE again.

## Crash consistency

Every decision is one transaction that asserts the scheduler epoch first (ADR-003). Every status change is a conditional `UPDATE` (ADR-020). Events and the outbox are written in the same transaction (ADR-009). So a crash at any point leaves either the state before the decision or the state after it. Repeating a decision is a no-op: abandoning an attempt that already ended matches 0 rows.

**Failpoints** prove this. With the build tag `failpoint` (`make test-crash`), a point is armed by `FF_FAILPOINTS=name=error|panic,…` or by `failpoint.Enable` in a test:

| Failpoint | Where |
|---|---|
| `claim/after-task-running` | `PollTask`: the task is RUNNING, its attempt not yet inserted |
| `task/after-update-before-event` | `TaskRepo.Transition`: the row is updated, its event not yet recorded |
| `scheduler/process-run-midway` | `processRun`: readiness decided, promotion not yet run |
| `outcome/after-attempt-ended` | A report or `AttemptEnder`: the attempt is terminal, the task not yet moved |

Without the tag, `failpoint.Inject` is a no-op that the compiler removes.

`TestCrashConsistency` arms each point in turn, once as `error` (a crash before commit) and once as `panic` (a process crash). It drives a run through scheduling, claiming, completing and finishing, and requires three things:

- **The point fired.** `failpoint.Hits` counts it.
- **The invariants hold right after the crash.** `testutil.AssertInvariants` checks:
  - no RUNNING task lacks a RUNNING attempt, and no RUNNING attempt belongs to a task that isn't RUNNING;
  - no task has more than one RUNNING attempt;
  - `attempt_count` equals the attempts recorded;
  - every task's and run's status matches its latest event.
- **The run converges after a restart,** meaning a restarted API and fresh scheduler loops, with exactly one succeeded attempt per task.

## Configuration and metrics

- `FF_WORKER_LEASE_TTL` and `FF_WORKER_HEARTBEAT` are now read by the scheduler too, for recovery.
- `FF_FAILPOINTS` applies to test builds only.
- `flowforge_loop_errors_total{loop}` counts failed loop iterations, where `loop` is `scheduler`, `timeout` or `recovery`.

## Limitations

- **Queue wait doesn't count toward the timeout.** A task never times out while QUEUED.
- **Recovery reacts in ticks.** Detection is bounded by the lease TTL plus two recovery ticks, not immediate.
- **Database outage test:** it cuts the scheduler's connection through a TCP proxy rather than stopping Postgres, because other tests share the container. The loops see the same refused and dropped connections.
- **Cancellation** is described in [run control](run-control.md): a cancel-requested run's failures are cancelled rather than retried, and its RUNNING attempts are forced when the grace runs out.
