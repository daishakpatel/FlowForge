# Runbook: scheduler failover

This is the initial version (task 04.07); task 09.05 expands it. It covers `FF_COORDINATION=raft`: several schedulers, one leader elected through the Raft KV, every decision fenced by an epoch in Postgres. How it works: [`leadership-and-fencing.md`](../architecture/leadership-and-fencing.md). The commands assume the Compose demo (`make failover-up`, API on `127.0.0.1:8080`).

```bash
KEY=ff_devcompose27_ZGV2LW9ubHktY29tcG9zZS1hZG1pbi1rZXktMDQuMDc   # the Compose demo's dev-only admin key
status() { curl -fsS -H "Authorization: Bearer $KEY" http://127.0.0.1:8080/api/v1/cluster/status; }
```

## Symptoms

- No run makes progress: tasks stay PENDING/READY, and nothing is QUEUED.
- `GET /api/v1/cluster/status` shows `scheduler.leader_id: null`, or a `fenced_epoch` different from `scheduler.epoch` for more than a few seconds.
- `flowforge_scheduler_is_leader` sums to 0 across the schedulers (it must be exactly 1).
- A scheduler log shows `scheduler epoch is stale; stopping … reason=STALE_EPOCH` (it was deposed mid-decision and exits; expected after a pause), or `leadership lost; loops stopped`.

## Impact

- **While no leader runs:** no new scheduling, retries, timeouts or worker-loss recovery, and no cron fires or outbox publishing.
- **Unaffected:** running tasks continue, workers report results through the API, and API reads and writes work (00_MASTER §6.2).
- **Safety:** none is at stake. A deposed leader cannot commit (ADR-003).

## Diagnosis

```bash
status                                              # scheduler leader/epoch, raft.available, workers.alive
docker compose --profile core --profile raft ps     # which schedulers are running or restarting
docker compose --profile core --profile raft logs --tail=50 scheduler-1 scheduler-2 scheduler-3
make raft-status                                    # the Raft KV itself
```

- **`raft.available` is false:** the Raft KV has no quorum. Follow [`raft-quorum-loss.md`](raft-quorum-loss.md); schedulers recover by themselves when quorum returns.
- **Raft is available but there is no leader:**
  - for up to about `FF_LEADER_TTL` + `FF_LEADER_RENEW` after the old leader died this is normal: its lease must expire (6 s + 2 s in the demo, 10 s + 3 s by default);
  - longer than that, look at the schedulers' logs for `activate epoch … postgres holds epoch` (Postgres unreachable, or a higher epoch stored) or Raft errors.
- **A scheduler keeps restarting with STALE_EPOCH:** it is being deposed repeatedly. Check for clock-rate problems or pauses (CPU starvation) on that host.

## Mitigation

1. **Make sure at least one scheduler runs:**
   ```bash
   docker compose --profile core --profile raft up -d scheduler-1 scheduler-2 scheduler-3
   ```
2. **If Raft has no quorum,** restore it first (`make raft-restart N=…`, see the quorum-loss runbook).
3. **If Postgres is down,** restore it: no scheduler can activate an epoch without it, by design.
4. **Never** lower `cluster_state.epoch` or edit the leader key while any raft-mode scheduler may run: a deposed leader could commit again.
5. **Switching back to `FF_COORDINATION=local`** (a local scheduler refuses to start, "needs scheduler epoch 0, but this database is at epoch N"): stop **every** scheduler first, then reset the fence:
   ```bash
   docker compose --profile core --profile raft stop scheduler-1 scheduler-2 scheduler-3
   psql "$FF_PG_DSN" -c 'UPDATE cluster_state SET epoch = 0, leader_id = NULL'
   ```
6. **No leader for up to about 10–15 s after a scheduler froze mid-transaction** is expected: Postgres ends its idle session (`idle_in_transaction_session_timeout`) before the new leader's fence can activate.

## Verification

```bash
status   # scheduler.leader_id set, epoch == fenced_epoch, raft.available true
```

- **One leader:** `flowforge_scheduler_is_leader` is 1 on exactly one scheduler.
- **Progress:** new runs reach SUCCEEDED.
- **Epoch history:** `scheduler.leader_changed` events (outbox) show strictly increasing epochs.

## Escalation

Escalate to the scheduler and Raft owners, with `status` output and every scheduler's log, when:

- no leader is elected within a minute while Raft is available and Postgres is up;
- two schedulers report `is_leader` 1 for longer than `FF_LEADER_TTL`. Their decisions are still fenced, but the clocks or the elector need investigation;
- epochs in `scheduler.leader_changed` ever decrease.
