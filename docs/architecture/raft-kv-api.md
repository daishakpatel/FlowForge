# Raft KV API

This page describes the replicated KV as a service (task 04.04, FR-RAFT-04, FR-RAFT-06, ADR-005, ADR-006): the `flowforge-raft` binary, its gRPC API, its consistency and retry guarantees, and the Go client.

- **Server:** `internal/raftkv/server` implements the `KV` and `Cluster` services of `proto/coordination/v1/kv.proto` on top of the Raft node ([`raft-node.md`](raft-node.md)) and the state machine ([`raft-fsm.md`](raft-fsm.md)).
- **Client:** `internal/raftkv/client`, also exported as `pkg/client/coordination`.
- **Binary:** `cmd/flowforge-raft`. Compose and operator tooling arrive in 04.06.
- **Scope:** coordination state only (leader keys, epochs, worker leases). Postgres owns workflow state and fences every action (ADR-003).

## Request flow

```mermaid
sequenceDiagram
  participant C as Client
  participant F as Follower
  participant L as Leader
  C->>F: Get(key, LINEARIZABLE)
  F-->>C: NOT_LEADER (leader_hint = L's id)
  C->>L: Get(key, LINEARIZABLE)
  L->>L: ReadIndex, wait until applied ≥ index
  L->>L: now = leader clock
  L-->>C: kv (expiry judged at now), applied_index, leader_now_ms
  C->>L: Put(key, v, client_id=c, request_id=r1)
  L->>L: ts = leader clock; Propose(cmd{c, r1, ts}); wait for apply
  L-->>C: kv
  Note over C,L: a timeout and a retry with the same r1 return the original result
```

## Consistency

| Operation | Guarantee |
|---|---|
| Writes (`Put`, `Delete`, `CAS`, `Txn`, `Lease*`, `IncrCounter`) | **Linearizable.** Only the leader proposes. It answers after the entry has committed and applied, so the write is visible to every later linearizable read. |
| `Get`, `List` with `LINEARIZABLE` (the default) | **Linearizable.** Only the leader serves them. It confirms its leadership with a quorum (ReadIndex) and catches up through the confirmed commit index before reading. A leader cut off from its quorum can't complete the ReadIndex (CheckQuorum), so it never serves a stale read. The response carries `leader_now_ms` and `applied_index`. |
| `Get`, `List` with `SERIALIZABLE` | **Possibly stale, never torn.** Any node answers from its applied state at once, with `stale = true` and its `applied_index`. It answers even without a quorum, for example on the survivor of a 2-node outage. |
| A `List` of several pages | Each page is one consistent read. The pages together are **not** one snapshot. |

Followers answer leader-only requests with `NOT_LEADER` and the leader's id (`leader_hint`) instead of forwarding them; the client follows the hint.

**Evidence:**

- **Porcupine:** `TestLinearizability_UnderLeaderKills` (`test/fault/raft`) runs 8 clients doing Put, Get and CAS on 4 keys for 60 s while the leader is killed every 2–4 s, then checks the history with the `internal/testutil/porcupine` register model. One run recorded 690 299 operations, 12 leader kills and 30 writes of unknown outcome, and the history was linearizable.
- **Read after write:** `TestClient_LinearizableReadAfterWriteAcrossLeaderChanges` checks that a read after an acknowledged write sees it, over 10 000 write/read pairs across 3 leader kills.

## Writes and retries

The receiving leader stamps each command with its clock (`ts_unix_ms`, ADR-005). That timestamp is the only time the state machine ever sees.

**Request ids.** A write that carries `meta {client_id, request_id}` is deduplicated by the state machine. A second command with the same pair returns the first result, failures included, and changes nothing. This is what makes a retry after a timeout safe.

- **How the client uses them:** it sends a fresh pair on every call (`client_id` is one per `Client`; `request_id` counts up) and reuses that pair on every retry of the call.
- **Without a pair:** a command is not deduplicated. A `request_id` without a `client_id` is `INVALID_ARGUMENT`.
- **The window:** it holds the last 10 000 identities (within 8 MiB). An older retry runs again ([`raft-fsm.md`](raft-fsm.md#deduplication)).

**What the client retries** (`internal/raftkv/client/retry.go`):

| Answer | Retried? | How |
|---|---|---|
| `NOT_LEADER` | Yes, always | The node did not run the request. The client goes straight to the hinted leader, but backs off first if that endpoint already failed during this call: until an election ends, followers still name the dead leader. |
| `UNAVAILABLE` / `NO_QUORUM`, or the endpoint is down | Yes | After a backoff, on the next endpoint. Writes are safe to retry thanks to their request id. |
| Anything else (conflict, not found, invalid, quota) | No | Returned at once, typed. |

**Backoff.** The client makes at most `MaxAttempts` (5) tries within an overall deadline (the call's `ctx`, or `Timeout`, 10 s by default). Each wait is "equal jitter": uniform in [c/2, c], where c = min(`MaxBackoff`, `BaseBackoff`·2ⁿ⁻¹).

- **Why the defaults:** with 300 ms and 3 s, the four waits total at least 2.25 s. That is more than the 2 s maximum randomized election timeout (00E §5.3), so an ordinary failover never runs out of attempts.
- **Faster test clusters:** scale both values down with the election timeout.
- **Reconnects:** connections reconnect with a 100 ms to 1 s backoff. A hint that names an endpoint which just failed resets that connection's backoff.

**Unknown outcomes.** When the attempts or the deadline run out, the error is `ErrUnavailable`. For a write that means the outcome is unknown: it may have applied, at most once.

**Measured:** `TestClient_RetriesWritesAcrossFailoverWithoutDuplicates` kills the leader in the middle of a stream of counter increments. Every increment succeeds and the counter equals the number of increments, so nothing was applied twice. With the 10 ms test tick (election timeout 100–200 ms) the write that spans the kill took 246 ms. That is within twice the maximum election timeout plus the client's first backoff.

## Errors

Every error is a gRPC status with a `google.rpc.ErrorInfo` (domain `flowforge.dev`; the reason is a 00E §4 code). The client turns them into `*client.Error` values that match the `Err*` kinds with `errors.Is`.

| FSM / node outcome | gRPC code | Reason | Metadata | Client kind |
|---|---|---|---|---|
| `VERSION_MISMATCH` | `ABORTED` | `CONFLICT` | `current_version`, `result_code` | `ErrConflict` (`Error.CurrentVersion`) |
| `KEY_NOT_FOUND` (an expected version > 0 on an absent key) | `ABORTED` | `CONFLICT` | `current_version=0`, `result_code` | `ErrConflict` |
| `LEASE_NOT_FOUND` | `NOT_FOUND` | `NOT_FOUND` | `result_code` | `ErrLeaseNotFound` |
| `LEASE_EXPIRED` | `NOT_FOUND` | `NOT_FOUND` | `result_code` | `ErrLeaseExpired` |
| `TXN_TOO_LARGE`, `INVALID_ARGUMENT` | `INVALID_ARGUMENT` | `INVALID_ARGUMENT` | `result_code` | `ErrInvalidArgument` |
| `QUOTA_EXCEEDED`; an entry over 1 MiB | `RESOURCE_EXHAUSTED` | `PAYLOAD_TOO_LARGE` | `result_code` (quota) | `ErrResourceExhausted` |
| Absent key or expired lease on `Get` | `NOT_FOUND` | `NOT_FOUND` | — | `ErrNotFound` |
| Follower | `FAILED_PRECONDITION` | `NOT_LEADER` | `leader_hint` (node id) | retried |
| No leader | `UNAVAILABLE` | `NO_QUORUM` | — | retried, then `ErrUnavailable` |
| Proposal or ReadIndex timeout (`FF_RAFT_PROPOSAL_TIMEOUT`), dropped proposal, node stopping | `UNAVAILABLE` | `UNAVAILABLE` | — | retried, then `ErrUnavailable` |
| The caller's deadline ended | `DEADLINE_EXCEEDED` | — | — | `ErrUnavailable` |

A `CAS` that doesn't match is **not** an error. It returns `succeeded = false`, with `kv` holding the current key (unset when the key is absent).

## Leases and expiry (ADR-005)

The state machine stores only `expires_at_ms = ts + ttl`. Whether a lease has expired is judged by the **leader's clock**:

- **Reads:** a linearizable read takes `now` after its ReadIndex completes and hides every key bound to a lease with `expires_at ≤ now`. `Get` answers `NOT_FOUND` and `List` leaves the key out, even though the key is still stored. A serializable read judges by the serving node's own clock.
- **Keep-alive:** `LeaseKeepAlive` refuses a lease the leader's clock says has expired, with `LEASE_EXPIRED`, even before it is deleted. The holder must grant a new lease. The server checks after a ReadIndex and proposes with the same timestamp, so the state machine judges the request at the same instant. A concurrent keep-alive can only extend the lease.
- **Sweeper:** every 500 ms the leader proposes `LeaseExpire` for up to 64 leases its clock says have expired, which physically deletes them and their keys. A follower's sweeper does nothing, so the sweep follows leadership. `LeaseExpire` re-checks `expires_at ≤ ts`, so a sweep from a stale view is harmless. `LeaseExpire` is not an RPC.
- **Clock skew:** a leader change can shift the clock base by the NTP skew between nodes (milliseconds). Lease TTLs are seconds, which dominates that, and safety never depends on lease timing, because Postgres fences every action (ADR-003).

**Tests:**

- `TestLeases_ExpiryJudgedByLeaderClockAndSwept`, on a fake leader clock: a key disappears from reads exactly at `expires_at`, a keep-alive extends it, a keep-alive after expiry gets `LEASE_EXPIRED`, and the key is deleted only by the sweeper's tick.
- `TestSweeper_FollowsLeadership`: a new leader sweeps a lease granted under the old one.

## Client usage

```go
c, err := coordination.New(coordination.Config{Endpoints: cfg.RaftEndpoints}) // FF_RAFT_ENDPOINTS
if err != nil {
	return err
}
defer c.Close()

// Leader election (04.05): a lease-bound key created only if absent.
lease, err := c.LeaseGrant(ctx, 10*time.Second, nodeID)
res, err := c.CAS(ctx, "/cluster/scheduler/leader", 0, leaderJSON, coordination.WithLease(lease.GetLeaseId()))
if err == nil && !res.GetSucceeded() {
	// someone else leads; res.GetKv() is the current holder
}
if _, err := c.LeaseKeepAlive(ctx, lease.GetLeaseId(), lease.GetFencingToken()); errors.Is(err, coordination.ErrLeaseExpired) {
	// leadership lost: stop acting, grant a new lease
}

epoch, err := c.IncrCounter(ctx, "/counters/scheduler_epoch")
page, err := c.List(ctx, "/workers/", coordination.WithLimit(500))
next, err := c.List(ctx, "/workers/", coordination.WithStartKey(page.GetNextKey()))
stale, err := c.Get(ctx, "/cluster/config", coordination.Serializable())
```

## Limits

| Limit | Value |
|---|---|
| Key / value | 1–512 bytes / ≤ 256 KiB ([`raft-fsm.md`](raft-fsm.md#limits) has every FSM limit) |
| One request (a Raft entry) | ≤ 1 MiB encoded (`MaxSizePerMsg`), else `PAYLOAD_TOO_LARGE` |
| `Txn` | ≤ 8 compares, ≤ 8 ops across both branches |
| `List` page | ≤ 1000 keys (the default when `limit` is 0), and it stops early after ~2 MiB of keys and values; `next_key` continues it |
| Wait for a write to apply, or for a ReadIndex | `FF_RAFT_PROPOSAL_TIMEOUT` (3 s), inside the caller's deadline |
| Client | 5 attempts, a 10 s deadline when the call has none |

## Metrics

Besides the node's metrics ([`raft-node.md`](raft-node.md#metrics), including `flowforge_raft_write_duration_seconds` for a durable Ready write), the server exports:

| Metric | Labels | Meaning |
|---|---|---|
| `flowforge_raft_requests_total` | `method`, `code` | requests by RPC and gRPC code |
| `flowforge_raft_request_duration_seconds` | `method` | request latency |
| `flowforge_raft_read_index_duration_seconds` | — | the ReadIndex round behind a linearizable read |
| `flowforge_raft_leases_swept_total` | `outcome` (`expired`, `error`) | leases the sweeper deleted, or failed to |

## Running a 3-node cluster locally

Compose arrives in 04.06. Until then, run three processes by hand:

```bash
make build
for i in 1 2 3; do
  FF_RAFT_NODE_ID=$i \
  FF_RAFT_PEERS=1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 \
  FF_RAFT_DATA_DIR=/tmp/flowforge-raft/$i \
  FF_RAFT_CLIENT_ADDR=127.0.0.1:700$i FF_RAFT_PEER_ADDR=127.0.0.1:710$i FF_RAFT_HTTP_ADDR=127.0.0.1:818$i \
  bin/flowforge-raft &
done
curl -s 127.0.0.1:8181/readyz                                    # {"checks":{"raft":"ok"},"status":"ok"} once a leader is known
curl -s 127.0.0.1:8181/metrics | grep '^flowforge_raft_is_leader'
pkill -TERM -f bin/flowforge-raft                               # graceful shutdown
```

Clients use `FF_RAFT_ENDPOINTS=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003`. The cluster id is derived from the voter ids, so a data directory can't be reused with another voter set. `TestRun_ServesAClusterAndHandsOffLeadershipOnShutdown` runs the same three processes in-process and serves writes through them.

**Graceful shutdown** (SIGTERM or SIGINT) runs in this order:

1. **Hand off leadership:** a leader transfers to the most up-to-date follower while still serving. Requests in that window get `NOT_LEADER` with the new leader's id, so a client is redirected at most once (measured by the test above).
2. **Drain:** stop serving clients, letting in-flight requests finish (≤ 10 s).
3. **Stop:** stop the node, the peer transport and the operational server, then close the storage.

If the transfer can't finish within 5 s, the node stops anyway and the cluster elects a new leader. This always happens when every node is stopped at once: the followers are already gone.

## Limitations

- **No authentication or TLS yet:** the KV and peer ports are plaintext and unauthenticated until mTLS arrives in 09.01 (ADR-015). Run them on a private network only.
- **Client addresses:** a node knows only its own client address. `Cluster.Status` lists the other members by peer address, and the client maps a leader hint (a node id) to an endpoint by asking each endpoint's `Cluster.Status`.
- **No forwarding:** followers redirect linearizable reads and writes instead of forwarding them, which costs one round trip after a leader change.
- **Multi-page lists:** a listing of several pages is not atomic.
- **Re-executed retries:** a retry older than the dedupe window runs again.
- **Sweeper rate:** each sweep proposes one `LeaseExpire` per lease, at most 64 every 500 ms, so a burst of more than 128 expiries a second takes several sweeps to delete. Reads hide expired keys meanwhile.
- **Status and Health:** they report the basics; 04.06 adds peer progress, the state hash and the applied-lag bound.
- **No Watch:** watches are not implemented (ADR-006).
