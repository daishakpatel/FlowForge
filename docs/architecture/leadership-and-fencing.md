# Leadership and fencing

This page covers how FlowForge runs several schedulers safely (task 04.07, FR-RAFT-08, FR-RAFT-09, ADR-003, ADR-005): one leader elected through the Raft KV, every scheduler decision fenced by an epoch in Postgres, and worker liveness from Raft leases. The building blocks are in [`coordination.md`](coordination.md) (04.05); the Raft cluster itself is in [`raft-operations.md`](raft-operations.md).

## Modes

`FF_COORDINATION` selects the mode for both `flowforge-api` and `flowforge-scheduler`.

| | `local` (default) | `raft` |
|---|---|---|
| Schedulers | One. `scheduler.AlwaysLeader`, epoch 0 | Any number. An `Elector` campaigns through the Raft KV (`FF_RAFT_ENDPOINTS`) under `FF_SCHED_NODE_ID` (default: the hostname) |
| Epoch fencing | Not needed: one scheduler, epoch 0 | `coordination.PGFence` activates each won epoch in Postgres before the term starts |
| Worker liveness | `workerapi.PGLeaseProvider` (`worker_observations`) | `workerapi.RaftLeaseProvider`: Raft leases, with every heartbeat still recorded in Postgres |
| Config check | — | `FF_WORKER_LEASE_TTL` ≥ 3 s (3× the Raft election timeout) |

`local` stays fully supported: the whole e2e suite runs in both modes (`make test-e2e`, `make test-e2e-raft`).

**Switching modes is one-way.** Raft mode raises `cluster_state.epoch` above 0, and local mode always decides at epoch 0, so none of its decisions could commit. A local-mode scheduler therefore refuses to start on a database a raft-mode leader has fenced, and names the reset: with **every** scheduler stopped, `UPDATE cluster_state SET epoch = 0, leader_id = NULL` (`TestRun_LocalModeRefusesARaftFencedDatabase`).

## The two-layer fence

```mermaid
sequenceDiagram
  participant A as scheduler A (leader, epoch 7)
  participant B as scheduler B
  participant R as Raft KV
  participant PG as Postgres cluster_state
  A->>R: keep-alive leader lease
  Note over A: SIGSTOP / GC pause / partition
  R-->>R: A's lease expires (leader clock)
  B->>R: grant lease, IncrCounter → 8, CAS leader key {B, 8}
  B->>PG: SetEpochIfGreater(8) + scheduler.leader_changed (one tx; waits for A's open tx)
  B->>B: term starts: loops run, each tx asserts epoch 8
  Note over A: wakes up, still believes it leads
  A->>PG: tx: AssertEpoch(7) → STALE_EPOCH, rolled back
  A->>A: loop returns STALE_EPOCH → process exits; restarted as a candidate
```

1. **The Raft layer.** Only the CAS winner holds `/cluster/scheduler/leader`, and its epoch comes from the monotonic counter `/counters/scheduler_epoch`. A leader stops believing it leads `SafetyMargin` (TTL/3) before the server could expire its lease (04.05).
2. **The Postgres layer.** The new leader raises `cluster_state.epoch` before announcing its term (`EpochFence.Activate`). `SetEpochIfGreater` waits for every transaction still holding `AssertEpoch`'s share lock. Every scheduler transaction then asserts `epoch == own` first.

So a deposed leader that wakes up after the takeover cannot commit, whatever it believes. Layer 1 makes this rare; layer 2 makes it impossible.

**A leader frozen inside a transaction** still holds `AssertEpoch`'s share lock, which the next leader's activation waits for. Two bounds keep that from stalling the cluster:

- **`lock_timeout`.** `PGFence` sets it (2 s) for the activation, so the activation fails and the candidate campaigns again rather than hang past its own lease (`TestFencing_ActivationWaitIsBounded`).
- **`idle_in_transaction_session_timeout`.** The scheduler sets it (10 s) on its sessions, so Postgres ends the frozen session and releases the lock.

A takeover after a leader froze mid-transaction can therefore take up to about 10–15 s; one after a crash or a clean pause is unaffected.

**When it notices.** The deposed leader either sees its term end first (its keep-alive fails, or its local deadline passes), so its loops stop with no decision, or its next decision fails with STALE_EPOCH. A loop that hits STALE_EPOCH stops the process (`scheduler.Runner`'s fail-safe contract); in Compose `restart: unless-stopped` brings it back as a candidate.

**Tests:**

- `TestFencing_DeposedLeaderCannotCommit` proves the STALE_EPOCH path deterministically. It uses an injected pause: two real Electors, `PGFence` and the decision loop.
- `TestRaft_PausedLeaderIsFencedWhenItWakes` (e2e, SIGSTOP) proves either path with real processes.

### Every loop asserts the epoch

Each leader loop's transaction starts with `ClusterRepo.AssertEpoch`: the decision, timeout, recovery and cron loops, and the outbox relay.

**The guard.** The test helper `internal/testutil/epochguard` wraps a `TxManager` and fails the test when a transaction's first statement is not the epoch lock. It wraps the loops in the scheduler, recovery, outbox and cron integration suites, so every path those suites exercise is checked. `TestEpochGuard_CatchesAWriteWithoutAssertEpoch` proves it catches an injected unfenced write.

**The exemption.** Retention is the one leader loop without a transaction: it only purges expired rows and makes no decision, so a deposed leader running it is harmless.

## Worker liveness in raft mode

The API's `RaftLeaseProvider` keeps each worker's `/workers/{id}` key bound to a TTL lease (`FF_WORKER_LEASE_TTL`, 15 s).

- **Register and Heartbeat** write Postgres first, then Raft. A Raft failure other than a conflict is degraded: logged, counted in `flowforge_worker_lease_renewal_failures_total`, and the call still succeeds, so workers keep working through a Raft outage. The first heartbeat after the outage restores the lease, re-registering an expired or missing one.
- **Raft calls on the heartbeat path** are bounded at 2 s.
- **Expired** reports a worker only when Raft holds no live lease for it **and** Postgres has not seen it within the TTL.
  - Raft expiry stays necessary: Raft is the liveness authority.
  - The Postgres check is what prevents spurious abandonment when quorum returns. A worker that kept heartbeating through the outage has an expired Raft lease until its next heartbeat; its fresh `last_seen_at` keeps recovery from abandoning it in that window.
- **Fail-safe.** If the linearizable list fails, `Expired` returns an error and recovery abandons nothing (00C §10.1).

**Recovery itself** is unchanged (03.04): only the leader runs it, every abandonment asserts the epoch, and an abandonment is idempotent.

## Events and status

| Event | Recorded by | When |
|---|---|---|
| `scheduler.leader_changed` `{node_id, epoch}` | `PGFence`, in the activation transaction | Each term, before its loops start |
| `worker.registered` `{worker_id}` | `workerapi.Service.Register` | Each successful registration |
| `worker.lost` `{worker_id, reason}` | `recovery.Loop` (03.04) | After a lost worker's attempts are abandoned |

All three are outbox-only cluster events (no `run_id`).

**`GET /api/v1/cluster/status`** (viewer) reports:

- `coordination`;
- `scheduler`: `leader_id`, `epoch` and `since` from the leader key, and `fenced_epoch` from Postgres;
- `raft`: `available`, `leader_id`, `term`, `commit_index` and the leader's view of every node; null in local mode;
- `workers.alive`;
- `fetched_at`.

Each part is read concurrently and bounded at about 2 s. A failed part is null (`raft.available` false), so the page answers during an outage. 06.02 adds the `/internal/v1/cluster/status` alias for the AI service.

## Timing

| Quantity | Bound | Defaults | Measured (e2e, raft mode) |
|---|---|---|---|
| Scheduler takeover after the leader dies | ≈ `FF_LEADER_TTL` + `FF_LEADER_RENEW` (the watch) + jitter + activation | 10 s + 3 s: about 10–13 s | **6.3 s** with 6 s / 2 s (`TestRaft_LeaderFailoverUnderLoad`) |
| A killed worker's attempt abandoned | `FF_WORKER_LEASE_TTL` + 2 recovery ticks (2 s each) | 15 s + 4 s | **3.9 s** with 3 s (`TestRaft_KilledWorkerIsRecoveredFromItsRaftLease`, bound 7 s) |
| Quorum loss noticed (leader gives up) | `FF_LEADER_TTL` − `SafetyMargin` | ≈ 6.7 s | **4.0 s** with 6 s |
| Leader after quorum returns | Raft election + leader-key TTL + campaign | — | **1.0 s** once the Raft nodes were back |

- **Keep the worker lease above failover.** `FF_WORKER_LEASE_TTL` must stay well above a Raft leader change (election timeout 1 s), which the config enforces as ≥ 3 s, so a Raft failover never expires the leases of workers that keep heartbeating.
- **The 10 s takeover target needs a shorter leader TTL.** It needs `FF_LEADER_TTL` ≤ about 7 s; the Compose demo uses 6 s / 2 s. The 00E defaults (10 s / 3 s) trade a slower takeover for fewer spurious leader changes.

## Failure matrix

| Failure | Required (00_MASTER §6.2, task §5) | Observed | Test |
|---|---|---|---|
| Leader scheduler SIGKILLed mid-workload | Takeover ≤ 10 s; runs finish; invariants hold | New leader with its epoch fenced in 6.3 s. 6 runs SUCCEEDED, invariants hold, epochs `[1 2]` | `TestRaft_LeaderFailoverUnderLoad` |
| Leader SIGSTOPped past its TTL, then SIGCONT | Its next write is rejected; no divergent decisions | Takeover in 6.4 s. On resume the old leader saw its term end and stopped its loops; runs completed once; epochs ascend | `TestRaft_PausedLeaderIsFencedWhenItWakes`; STALE_EPOCH path: `TestFencing_DeposedLeaderCannotCommit` |
| Worker killed | Abandoned and reassigned ≤ TTL + 2 ticks | 3.9 s (bound 7 s); `worker.lost` recorded once | `TestRaft_KilledWorkerIsRecoveredFromItsRaftLease` |
| Raft quorum lost | Schedulers step down; running tasks continue; no reassignment; API works | Leader gone in 4.0 s. Running 12 s tasks SUCCEEDED; no ABANDONED; next task stays PENDING; API `/readyz` 200 | `TestRaft_QuorumLossStopsSchedulingButNotRunningWork` |
| Quorum returns | One leader; no spurious abandonment | Leader in 1.0 s; runs SUCCEEDED; no ABANDONED, no `worker.lost` | Same test |
| Raft down at registration or heartbeat | API keeps accepting heartbeats (Postgres) | Heartbeats succeed and count renewal failures; the lease is restored after the outage | `TestRaftLease_*` |

## Limitations

- **A deposed leader that hits STALE_EPOCH exits** and relies on a restart policy to come back as a candidate. Demoting in place would need the Runner to wait for the next term instead; the exit matches the existing fail-safe contract.
- **The single-leader belief assumes bounded clock-rate drift** within `SafetyMargin` (04.05). The Postgres fence makes a violation harmless, never invisible.
- **The STALE_EPOCH path in e2e is timing-dependent.** A resumed leader usually notices its term ended before deciding, so the deterministic proof is the in-process test.
- **The epoch guard sees only transactions** the guarded `TxManager` opens; retention is exempt by design.
- **No TLS on the Raft client port** until 09.01.
