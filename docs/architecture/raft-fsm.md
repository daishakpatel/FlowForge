# Raft KV state machine

This page describes the replicated state machine (FSM) behind the coordination KV (task 04.03, FR-RAFT-02, FR-RAFT-03, ADR-005, ADR-006).

- **Where it runs:** `internal/raftkv/fsm` applies the committed log that the Raft node ([`raft-node.md`](raft-node.md)) hands it, through `fsm.NewApplier`.
- **What it holds:** coordination state only, such as leader keys, worker leases, fencing counters and cluster config. Postgres owns workflow state (ADR-003).

## Determinism

Replicas that apply the same log reach **byte-identical** state, and `StateHash` (SHA-256 of the canonical encoding) proves it. Use these rules as a checklist for any change to the package:

1. **No clock.** All time is the command's `meta.ts_unix_ms`, stamped once by the node that received the request (ADR-005). The FSM imports no `time`.
2. **No randomness.** Lease ids and fencing tokens come from counters in the state.
3. **No map order in output.** Every iteration over a map goes through `sortedKeys`, and `TestNoMapIterationOutsideSortedKeys` type-checks the package to enforce it. Dedupe eviction follows an insertion-ordered slice.
4. **No I/O, network or goroutines.** The `raft-fsm` depguard rule in `.golangci.yml` denies `time`, `math/rand`, `crypto/rand`, `os` and `net` in non-test files, and `make lint-selftest` proves the rule fires.
5. **Failures are results, never Go errors.** A failed condition, a malformed command or even undecodable bytes become a `CommandResult` with a `ResultCode`, and the applied index advances. A Go error would mean replicas could disagree.
6. **The same configuration everywhere.** `Options.DedupeCapacity` decides when a retry re-executes, so every replica must use the same value. The default is 10 000.

Evidence: `TestDeterminism_ReplicasConverge` covers 5 seeds × 50 000 random commands with non-monotonic timestamps, garbage entries and dedupe collisions.

- **Equal state:** two replicas hash equal at every 1 000 entries.
- **Restored replicas:** replicas restored from snapshots taken at three random points converge with them.
- **Coverage of outcomes:** the log reaches OK, VERSION_MISMATCH, KEY_NOT_FOUND, LEASE_NOT_FOUND, LEASE_EXPIRED and INVALID_ARGUMENT.
- **Generator:** the command generator (`internal/raftkv/fsmtest`) is reusable, for example by 04.08.

## Commands

A Raft entry's data is a `Command` (`proto/coordination/v1/command.proto`): `meta{client_id, request_id, ts_unix_ms}` plus one op. Most ops reuse the KV API's request messages, and a request's own `meta` field is ignored.

| Op | Semantics |
|---|---|
| `Put` | Optional `expected_version` and optional `lease_id`. The lease must exist (`LEASE_NOT_FOUND`) and not be expired at `ts` (`LEASE_EXPIRED`). Returns `kv` (as written) and `prev` |
| `Delete` | Optional `expected_version`. An absent key is `OK` with `deleted=false` |
| `CAS` | `expected_version` (0 = must not exist), then write `value` (optionally on a lease) or delete. A delete needs `expected_version > 0` and no `lease_id` (`INVALID_ARGUMENT`) |
| `Txn` | Evaluates every compare (version, value or lease; `=`, `≠`, `>`, `<`), then runs `success` if all hold, else `failure`. **Absent keys:** a value compare never holds, as in etcd; compare the version with 0 to test absence. **Validation:** both branches are checked for shape and sizes whichever runs, and the chosen branch is also checked for leases and quotas before any op runs, so a Txn runs every op or none. Later ops see earlier ones' writes. **Limits:** at most 8 compares, and at most 8 ops across both branches (`TXN_TOO_LARGE`) |
| `LeaseGrant` | TTL from 1 ms to 24 h. `lease_id` comes from a counter, `fencing_token` from the **global fencing counter** (strictly increasing across every grant, and across snapshot/restore), and `expires_at = ts + ttl` |
| `LeaseKeepAlive` | The fencing token must match the grant's (`LEASE_NOT_FOUND`). `expires_at = max(expires_at, ts + ttl)`: it counts from the keep-alive's own timestamp (00E §5.2), so renewals more frequent than the TTL don't push expiry further out, and it never moves backwards. (04.05 fixed an earlier `max(expires_at, ts) + ttl`, which added a TTL per renewal.) A lease past its expiry that no `LeaseExpire` has removed yet can still be extended (task 04.03 rejects only an expired *and* removed lease). Safety doesn't depend on this, because Postgres fences every action a lease holder takes (ADR-003) |
| `LeaseRevoke` | Deletes the lease and every key bound to it |
| `LeaseExpire` | Proposed by the leader's sweeper (04.04). It deletes the lease and its keys only if `expires_at <= ts`. Otherwise, or for an unknown lease, it is `OK` with no change, so it is idempotent |
| `IncrCounter` | Returns the new value (1 on first use). Counters only grow and never wrap |
| `NoOp` | Changes only the applied index |

## Version mismatches

A command's `expected_version` is checked against the key's current version, where an absent key counts as 0. A mismatch is a result, not an error:

- **`expected_version > 0` on an absent key:** `KEY_NOT_FOUND`.
- **Any other mismatch:** `VERSION_MISMATCH`, and the result carries the current key in `kv`.

## Result codes

| Code | Meaning | 04.04 maps to |
|---|---|---|
| `OK` | Applied. A failed Txn compare is still `OK`, with `succeeded=false` | — |
| `VERSION_MISMATCH` | `expected_version` differs from the key's | `CONFLICT` |
| `KEY_NOT_FOUND` | `expected_version > 0`, but the key is absent | `CONFLICT` or `NOT_FOUND` |
| `LEASE_NOT_FOUND` | The lease was never granted, was revoked or expired, or the fencing token is wrong | `NOT_FOUND` |
| `LEASE_EXPIRED` | The lease exists, but expired by the command's timestamp | `NOT_FOUND` |
| `TXN_TOO_LARGE` | More than 8 compares or 8 ops | `INVALID_ARGUMENT` |
| `INVALID_ARGUMENT` | Malformed command, empty or oversized key, value or id, bad TTL | `INVALID_ARGUMENT` |
| `QUOTA_EXCEEDED` | 100 000 keys, counters or leases reached, or the 40 MiB state-bytes budget | `PAYLOAD_TOO_LARGE`/`RESOURCE_EXHAUSTED` |

## Limits

All limits are deterministic results, not errors:

| Limit | Value |
|---|---|
| Key or counter name | 1–512 bytes |
| Value | ≤ 256 KiB |
| Live keys | 100 000 |
| Counters | 100 000 |
| Live leases | 100 000 |
| `client_id`, `request_id`, lease owner | ≤ 256 bytes each |
| State bytes (keys + values + counter names + lease owners) | 40 MiB |
| Dedupe table (ids + encoded results) | 8 MiB |
| Snapshot | ≤ 64 MiB (`ErrSnapshotTooLarge`) |

The per-key limits alone would allow about 25 GiB, so the two byte budgets are what keep every state the FSM can reach under the snapshot limit. The worst case is about 56.5 MiB including protobuf framing. Without them, a large state would make every snapshot fail and the log would never compact.

## Lease timing (ADR-005)

```text
LeaseGrant@ts       → expires_at = ts + ttl
KeepAlive@ts        → expires_at = max(expires_at, ts + ttl)   (from its own ts; never backwards)
LeaseExpire(id)@ts  → if expires_at <= ts: delete the lease and its keys
IsExpired(id, now)  → expires_at <= now   (judged by the leader at read time)
```

The FSM only stores timestamps; it never decides on its own that a lease has expired.

- **Readers:** they apply their own clock through `IsExpired(id, nowMs)` and `ExpiredLeaseIDs(nowMs)`. 04.04 uses the leader's clock after a ReadIndex.
- **Physical deletion:** keys are deleted only by a committed `LeaseExpire` or `LeaseRevoke`.
- **Safety:** it never depends on lease timing, because Postgres fences every state change (ADR-003).

## Deduplication

A command with both `client_id` and `request_id` is remembered with its result, including failed results. A retry with the same pair returns the stored result without running again; only the applied index moves. This is what makes 04.04's retry after a timeout safe.

- **Window:** the table keeps the last `DedupeCapacity` (10 000) identities, within 8 MiB, and evicts the oldest first (FIFO). Results of large writes therefore shorten the window.
- **Snapshots:** the table is part of snapshots, in eviction order.
- **Limitation:** a retry older than the window runs again, so clients use unique request ids and short retry windows.

## Applied index

`Apply(index, data)` refuses `index <= AppliedIndex()`, and the state is unchanged:

- **Remembered command:** it returns the stored result.
- **Otherwise:** it returns `ErrAlreadyApplied`.

The node never re-delivers applied entries (it resumes after the snapshot it restored; see [`raft-node.md`](raft-node.md)), so this is defence in depth.

## Snapshot format

```text
byte 0      version (1)
bytes 1..   FSMState, protobuf, deterministic marshalling:
            last_applied, next_lease_id, fencing,
            keys (sorted by key), leases (sorted by id), counters (sorted by name),
            dedupe (oldest first)
```

How the format is checked:

- **Canonical encoding:** equal states encode to identical bytes. `TestSnapshot_RoundTripsByteForByte` compares 20 snapshots of one state and a restored copy.
- **Lease keys:** a lease's key set is not stored. It is rebuilt from the keys' `lease_id`, so the two can't disagree.
- **Validation on `Restore`:**
  - the version byte;
  - the size (≤ 64 MiB);
  - canonical order;
  - limits;
  - references: a key's lease exists, lease ids and fencing tokens stay within their counters (tokens increase with lease id), and indexes stay within `last_applied`;
  - the dedupe table: unique identities, coded results, and no more entries than this FSM would keep. A table from a replica with another capacity is refused, not trimmed, since trimming would make the replicas' retries diverge.

  Only then does it swap the state, so a corrupt snapshot leaves the state untouched (`TestRestore_RejectsBadInputWithoutChangingState`).
- **Unknown version:** `ErrSnapshotVersion`.

## Reads

`Get`, `List(prefix, limit)` (key order), `ListFrom(prefix, start, limit)` (a page from `start`, for 04.04's `next_key`), `GetLease`, `IsExpired`, `ExpiredLeaseIDs` and `AppliedIndex` read under a read lock and return copies. Each sees the state between two applies.

- **Linearizable reads:** a ReadIndex on the node first (04.04).
- **Lease-bound keys:** a key bound to an expired lease is still returned. The caller judges its expiry.

## Limitations

- **Fencing tokens order holders; they are not secrets.** A token is the global counter's value at grant time, which equals the lease id today. `LeaseRevoke` takes only the id. Who may call the KV is decided by the API's authentication (02.05), and fencing in Postgres decides whose actions count (ADR-003).

- **Whole-state snapshots:** the snapshot is the whole state in memory, up to 64 MiB, encoded on every snapshot and every `StateHash`.
- **Sorting cost:** `List` sorts the key set on every call, which is fine at coordination scale (≤ 100 000 keys).
- **Re-executed retries:** a retry older than the dedupe window runs again (see above).
- **No Watch:** watches are not implemented (ADR-006).
