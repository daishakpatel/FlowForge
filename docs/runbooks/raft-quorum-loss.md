# Runbook: Raft quorum loss

This is the initial version (task 04.06); task 09.05 expands it. It covers the local 3-node Compose cluster, whose layout and commands are in [`raft-operations.md`](../architecture/raft-operations.md). The degraded behavior follows master spec §6.2.

## Symptoms

- `flowforge-raft health` prints `NOT_READY` on the surviving nodes, and the Compose healthchecks turn unhealthy.
- `flowforge_raft_is_leader` is 0 on every node, and `flowforge_raft_peer_unreachable{peer}` is 1 for the lost peers.
- KV writes and linearizable reads fail with `UNAVAILABLE` / `NO_QUORUM`. The Go client returns `ErrUnavailable` after its retries.
- Schedulers lose leadership (once 04.07 wires the elector): `flowforge_scheduler_is_leader` is 0 everywhere.

## Impact

Per master spec §6.2, with quorum lost:

- **Stopped:** schedulers stop scheduling, and new workflow progress waits.
- **Continuing:** running tasks continue, and API reads still work.
- **Worker leases:** heartbeats buffer and retry. Worker lease expiry is not evaluated, because there is no leader, so nothing is reassigned spuriously.
- **Data:** nothing committed is lost. The cluster resumes when a majority (2 of 3) is back.

## Diagnosis

```bash
make raft-status                                    # each node's Cluster.Status (state, leader_id, term, indexes)
docker compose --profile raft ps raft-1 raft-2 raft-3   # which containers are down or unhealthy
make raft-logs                                      # errors such as "stopping" or a storage error
```

- **Two or three nodes down or unreachable:** this is quorum loss; continue with Mitigation.
- **One node down:** not quorum loss. The other two keep serving, with no user-visible effect beyond the failover; restart the node when convenient.
- **A node that exits at once with `ErrPeersMismatch`:** its data directory belongs to another voter set. Escalate.

## Mitigation

1. **Restart the stopped nodes**, one at a time:
   ```bash
   make raft-restart N=2
   make raft-restart N=3
   ```
   A node that keeps its data directory rejoins with its log. As soon as two nodes run, they elect a leader.
2. **If a node's data is lost** (its volume was deleted or corrupted), this is a last resort and needs care, because Raft assumes nodes never forget ([`raft-operations.md`](../architecture/raft-operations.md#a-wiped-node)). Check first:
   - the other two nodes run and have a leader;
   - the leader's `make raft-status` shows the other follower at `lag` 0.

   If either check fails, escalate instead: wiping now can lose committed writes. Otherwise restart the node with an empty volume, then move leadership:
   ```bash
   docker compose --profile raft rm --stop --force raft-3
   docker volume rm flowforge_raft3data
   make raft-restart N=3
   FF_RAFT_ADMIN_TOKEN=dev-only-raft-admin-token \
     docker compose --profile raft exec -e FF_RAFT_ADMIN_TOKEN raft-1 \
     /usr/local/bin/flowforge-raft transfer-leader --addr 127.0.0.1:7000 --to 2   # run it on the current leader
   ```
   The leader still trusts the wiped node's old progress, so only a new leader sends it the snapshot. Wait until the wiped node's `health` is `READY` before doing anything else. **Wipe at most one node at a time:** a wiped node has lost its vote record, and until it catches up the cluster runs with one copy fewer of every committed entry.

## Verification

```bash
make raft-status
docker compose --profile raft exec raft-1 /usr/local/bin/flowforge-raft dump-state-hash \
  --addr raft-1:7000,raft-2:7000,raft-3:7000
```

The cluster has recovered when:

- exactly one node reports `"state": "LEADER"`, and every `health` is `READY`;
- the leader's `peers` show `lag` 0 and `reachable: true`;
- `dump-state-hash` prints the same hash for the nodes at the same applied index;
- `scripts/dev/raft-smoke.sh` passes.

## Escalation

Escalate to the Raft owner, with `make raft-status` output and the logs of every node attached, when:

- a majority can't be restarted;
- a node stops on its own with a storage or snapshot error;
- a node refuses its data directory (`ErrPeersMismatch`, `ErrIdentityMismatch`);
- state hashes differ at the same applied index. That points to a determinism bug: keep every data directory for analysis.
