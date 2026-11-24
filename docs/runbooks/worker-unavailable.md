# Runbook: worker unavailable

This is the initial version (task 04.07); task 09.05 expands it. It covers a worker that died, hung or lost its network while running tasks. In `FF_COORDINATION=raft` its liveness is a Raft lease renewed by its heartbeats through the API; in `local` mode it is `worker_observations.last_seen_at`. How recovery works: [`recovery.md`](../architecture/recovery.md) and [`leadership-and-fencing.md`](../architecture/leadership-and-fencing.md#worker-liveness-in-raft-mode).

## Symptoms

- `GET /api/v1/workers` lists the worker as `LOST`, or `GET /api/v1/cluster/status` shows fewer `workers.alive` than expected.
- Its attempts become `ABANDONED` with error code `WORKER_LOST`, and the tasks retry on other workers (`task.retry_scheduled`).
- A `worker.lost` event is in the outbox.
- `flowforge_worker_lease_renewal_failures_total` rising on the API means Raft is not renewing leases (a Raft problem, not a worker one); heartbeats are still accepted.

## Impact

- **Its running attempts are abandoned and retried** within `FF_WORKER_LEASE_TTL` + 2 recovery ticks (15 s + 4 s by default; measured 3.9 s with a 3 s TTL). Execution is at least once (ADR-007): handlers must be idempotent.
- **If it comes back later,** its old results are rejected as stale (`STALE_ATTEMPT`) because of the fencing token, and it must register again.
- **Less capacity** until it is replaced.

## Diagnosis

```bash
KEY=ff_devcompose27_ZGV2LW9ubHktY29tcG9zZS1hZG1pbi1rZXktMDQuMDc   # the Compose demo's dev-only admin key
curl -fsS -H "Authorization: Bearer $KEY" http://127.0.0.1:8080/api/v1/workers
curl -fsS -H "Authorization: Bearer $KEY" http://127.0.0.1:8080/api/v1/cluster/status   # workers.alive, raft.available
docker compose --profile core --profile raft ps worker-1 worker-2
docker compose --profile core --profile raft logs --tail=50 worker-1
```

- **The process exited or the container stopped:** the worker died.
- **The process runs but is not seen:** it is hung, or it can't reach the API (gRPC `:9090`).
- **`raft.available` is false:** leases are not evaluated and nothing is abandoned (fail safe), so a "lost" worker in this state is not a worker problem. See [`raft-quorum-loss.md`](raft-quorum-loss.md).

## Mitigation

1. **Restart or replace the worker:**
   ```bash
   docker compose --profile core --profile raft up -d worker-1
   ```
   It registers again: a new lease, and `worker.registered`.
2. **Abandoned work needs nothing:** it is retried per each task's retry policy. Tasks whose attempts are used up go FAILED, and their runs follow `on_failure`. Rerun them if needed (`POST /api/v1/runs/{id}/rerun`).
3. **A worker restarted under the same id is fine:** its previous incarnation's attempts are still recovered (03.09).

## Verification

- The worker is `ACTIVE` in `GET /api/v1/workers`, and `workers.alive` is back to the expected count.
- No attempt stays RUNNING on the lost worker.
- `testutil.AssertInvariants` holds in tests: no task has two SUCCEEDED attempts.

## Escalation

Escalate to the worker runtime owner when:

- workers are lost repeatedly while their processes run (heartbeats not arriving: network or API problems);
- attempts of a live worker are abandoned while `raft.available` is true. That would be a spurious expiry: keep the API and scheduler logs and the timeline.
