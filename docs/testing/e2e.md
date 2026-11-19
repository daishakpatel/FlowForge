# End-to-end suite

`test/e2e` proves the workflow engine as it is deployed. It runs real `flowforge-api`, `flowforge-scheduler` and `flowforge-worker` binaries as subprocesses against Postgres in Docker. Then it kills, freezes and restarts them, and freezes the database. The results feed the Phase-3 gate ([phase3-gate.md](phase3-gate.md)).

```bash
make test-e2e                                   # every scenario + the 500-run chaos soak (~6–8 min)
go test -tags e2e -run TestCancelRun -v ./test/e2e/        # one scenario
go test -tags e2e -short -v ./test/e2e/                    # scenarios only, no soak
FF_E2E_SEED=42 FF_E2E_SOAK_RUNS=200 go test -tags e2e -run TestSoakChaos -v ./test/e2e/
```

The suite needs Docker; it fails, rather than skips, without it. CI runs it as the `e2e` job.

## How it works

- **Binaries:** `TestMain` builds the four binaries once per test run with `-race` into a temporary directory, so a data race in any process shows up in that process's log.
- **One deployment per test:** `New(t)` starts a fresh Postgres container (the pinned `testutil.PGImage`) and applies the migrations. `StartAPI`, `StartScheduler` and `StartWorker(id, capacity, queues…)` launch processes on free loopback ports. Each start waits for the process's `/readyz`, and a process that exits first fails the test with its log tail. `StartAll(n, capacity)` starts all three kinds.
- **Keys:** the harness generates the bootstrap admin key (`FF_ADMIN_BOOTSTRAP_KEY`). The first `StartAPI` creates a `worker`-role key for the workers.
- **Shortened timings:** the lease TTL is 3 s, the heartbeat 1 s, the scheduler tick 100 ms, and the cancel and shutdown grace 2 s, so recovery fits in a test. A worker's dead attempt is recovered within 3 s (lease expiry) plus the 2 s recovery tick. The fallback for silent attempts takes 3×heartbeat + TTL = 6 s, plus the tick.
- **Faults** (`faults.go`):

| Helper | Effect |
|---|---|
| `Kill(p)` | SIGKILL, then wait for the exit |
| `Stop(p)` | SIGTERM, a graceful exit |
| `Restart(p)` | Kill, then start again with the same ports and environment (a worker keeps its id) |
| `Pause(p)` / `Resume(p)` | SIGSTOP / SIGCONT: a zombie that keeps its sockets but does nothing |
| `PausePostgres(d)` | `docker pause` the database for d, then `docker unpause` |

- **Waiting:** scenarios wait on conditions (`WaitRun`, `WaitAttempt`, `testutil.WaitFor`) and never sleep for a fixed time. The only exceptions are the bounded window that shows a paused run starts nothing, the chaos clock, and `PausePostgres`.
- **Checks:** `AssertHealthy` runs `testutil.AssertInvariants` and checks that no task has more than one `SUCCEEDED` attempt.
- **Logs:** each process logs at debug level, as text, to `<name>.log`. On failure, the last 60 lines of each log are printed. Set `FF_E2E_LOG_DIR` to keep every log; CI uploads them as the `e2e-logs` artifact when the job fails. Failure messages also list every task and attempt, read from the database.
- **Measurements:** lines starting `METRIC name=duration` record the recovery times reported in the gate.

## Scenarios

Each scenario uses a definition in `test/fixtures/workflows/`.

| Test | Fixture | Proves |
|---|---|---|
| `TestDiamondAcrossTwoWorkers` | `diamond` | a→{b,c}→d completes; b and c run in parallel on both workers; d sees b's result |
| `TestFanOutRespectsCapacity` | `fanout` | 8 parallel tasks on 2 workers × capacity 2: the per-worker peak is ≤ 2, the total peak is 3–4 |
| `TestRetryThenSucceed` | `retry` | `flaky` fails twice (`FLAKY`) and succeeds on attempt 3 |
| `TestNonRetryableFailure` | `non-retryable` | a non-retryable code fails the task on its first attempt; its dependent never runs |
| `TestTaskTimeout` | `timeout` | a 1 s timeout ends a 30 s handler: `DEADLINE` from the worker, or `TIMED_OUT` from the scheduler |
| `TestWorkerKilledMidTask` | `chain` | SIGKILL of the worker running a task: the attempt is `ABANDONED` and the task reassigned |
| `TestWorkerRestartedWithSameID` | `chain` | a worker killed and restarted under the same id: its dead incarnation's attempt is recovered |
| `TestZombieWorkerIsRejected` | `chain` | a SIGSTOPped worker loses its task; after SIGCONT its result is dropped as stale |
| `TestSchedulerRestartMidRun` | `chain` | the scheduler dies mid-run; after a restart the run continues |
| `TestAPIRestartMidRun` | `chain` | the API dies while a worker runs a task; the worker reconnects |
| `TestDatabasePause` | `chain` | Postgres freezes for 5 s; every process rides it out |
| `TestCancelRun` | `long-sleep` | the running attempt is `CANCELLED`; the rest never runs |
| `TestPauseResume` | `chain` | a paused run finishes its running task, starts nothing new, and completes after resume |
| `TestRerunAfterFailure` | `rerun` | a worker crash fails a one-attempt task and its `on_failure` branch escalates. The rerun succeeds, skips the branch and runs the success path |
| `TestTemplatingChain` | `templating` | run input → task result → task result → config |
| `TestCronTriggerFires` | `diamond` | a trigger made with `flowforgectl triggers create` fires at the next minute boundary (up to 60 s) |
| `TestIdempotentStart` | `diamond` | REST, a REST replay and a CLI replay with one `Idempotency-Key` make one run |
| `TestSSEConsumption` | `diamond` | `flowforgectl runs watch` streams the run from its first event to `run.succeeded` |
| `TestCLIEndToEnd` | `cyclic`, `market-data-skeleton` | the CLI rejects a cycle with `CYCLE` and drives a run; every read command works |
| `TestDemoScript` | — | `scripts/demo/engine-demo.sh` runs cleanly |
| `TestSoakChaos` | `soak` | 500 runs × 6 tasks on 4 workers under chaos (below) |

## Soak

`TestSoakChaos` starts `FF_E2E_SOAK_RUNS` runs (default 500) of a 6-task DAG on 4 workers of capacity 4. The DAG is a→{b,c,d}→e→f, about 3.3 s of work per run. Until every run is terminal:

- Every 10 s, a random worker is SIGKILLed and restarted under the same id.
- Every 30 s, the scheduler is SIGKILLed and restarted.

The schedule comes from a seeded RNG: `FF_E2E_SEED`, which defaults to the clock and is logged as `SOAK seed=`. Pass the seed back to replay the same schedule. The test requires every run to succeed, no task to have two `SUCCEEDED` attempts, and the invariants to hold. It logs the total duration and the counts of kills, restarts and abandoned attempts. `-short` skips it.

## Running the demo

[`scripts/demo/engine-demo.sh`](../../scripts/demo/engine-demo.sh) needs a running deployment:

```bash
make dev-up && make migrate-up
go run ./cmd/flowforge-api &                      # prints an admin key once on first start
export FF_API_KEY=ff_...                          # that key
go run ./cmd/flowforge-scheduler &
go build -o bin/flowforgectl ./cmd/flowforgectl
FF_WORKER_TOKEN="$(bin/flowforgectl -o json api-keys create --name worker-1 --role worker | sed -n 's/^  "key": "\(.*\)".*/\1/p')" \
  FF_WORKER_ID=worker-1 FF_WORKER_QUEUES=default FF_WORKER_CAPACITY=4 FF_API_GRPC_TARGET=localhost:9090 \
  go run ./cmd/flowforge-worker &
scripts/demo/engine-demo.sh
```
