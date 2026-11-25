The analysis below is written by hand and appended to the generated report by `make test-fault-raft` (`-notes`). Its numbers are those of the 5-run gate of 2026-11-24 (task 04.08); update it when a run changes the picture.

**Safety: no violation in 110 scenario runs** (22 scenarios × 5).

- **Linearizability:** every client history was linearizable, over 3–105 k operations per run.
- **Convergence:** every scenario ended with all members at the same state hash and applied index.
- **Fencing:** committed scheduler decisions never went back in epoch (12, X3).
- **Nothing was acknowledged without quorum:**
  - no write was acknowledged with two nodes down (4), with every node isolated (9), or through an isolated leader that still believed it led (7);
  - a linearizable read without quorum was refused (4).
- **The checker can fail:** the negative control was judged Illegal in every run (N).

**The Raft failover target (≤ 3 s, kill → client writes succeed) was met in every run** of the scenarios that measure what it defines:

- leader kill in process (1): at most 1.54 s;
- leader container kill (C1): at most 1.86 s, on the production binary and timing;
- partitions and pauses: 7 at most 2.07 s, C7 at most 2.03 s, C2 at most 1.97 s;
- leader transfer (X2): at most 0.12 s.

**Missed once: X1, rolling restart under load, 3.25 s in one of five runs** (the others 1.44–1.88 s).

- **What X1 records:** the worst of three back-to-back kills, each within about a second of the previous node's restart.
- **Why it can exceed 3 s:** when the node killed is the leader while the just-restarted node has not yet appended the latest entries, Raft's up-to-date vote check leaves only one survivor that can win. One randomized election timeout (1–2 s), a pre-vote round and the client's retry backoff can then add up to more than 3 s.
- **What it is:** a liveness margin under a compound fault, not a safety issue. No write was lost and the history stayed linearizable.
- **Operator guidance:** in a rolling restart, wait for the restarted node to be READY (`flowforge-raft health`, 04.06) before taking down the next.
- **The test was not changed to pass the target.**

**Recovery times without a target:**

- quorum restored by restarting one of two stopped nodes (4): 3.5–4.2 s, including the process start and an election;
- restarting all three nodes: 1.1–1.4 s in process (8), 1.5–3.4 s as containers (C8), including container restarts;
- healing a three-way partition (9): 0.3–1.2 s.

**The scheduler failover target (≤ 10 s) was met** (12): 6.3–8.0 s with the Compose demo's 6 s leader lease renewed every 2 s. With the 00E defaults (10 s / 3 s) a takeover takes about 10–13 s ([`leadership-and-fencing.md`](../architecture/leadership-and-fencing.md#timing)).

**Clock skew (X3):** the 2 s skew moved scheduler leadership in every run, as the test requires (at least two terms). No decision was rejected as STALE_EPOCH: deposed schedulers stopped at their own local deadline before deciding. The Postgres fence was the backstop and was not needed.

**Wiped and stale nodes (5, 11, X5):** a node restored from an old copy of its own data (5) stays behind under the same leader, because raft keeps a peer's acknowledged progress. It catches up after a leadership transfer; that is the wipe procedure of [`raft-quorum-loss.md`](../runbooks/raft-quorum-loss.md).

**Variance:** the spread of each time-to-recover over the 5 runs is in the table (min / median / max). The widest are X1 (1.44–3.25 s, above) and C8 (1.54–3.39 s, container restart timing).
