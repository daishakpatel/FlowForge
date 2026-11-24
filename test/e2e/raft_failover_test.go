//go:build e2e

package e2e

import (
	"bufio"
	"context"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/testutil"
)

// recoveryTick is the recovery loop's tick in flowforge-scheduler.
const recoveryTick = 2 * time.Second

// clusterView is the part of GET /api/v1/cluster/status the failover tests read.
type clusterView struct {
	Scheduler struct {
		LeaderID    *string `json:"leader_id"`
		Epoch       *int64  `json:"epoch"`
		FencedEpoch *int64  `json:"fenced_epoch"`
	} `json:"scheduler"`
	Raft *struct {
		Available bool `json:"available"`
	} `json:"raft"`
}

func (h *Harness) Cluster() clusterView {
	var v clusterView
	_, _, _ = h.Do("GET", "/api/v1/cluster/status", nil, &v) // polled; a failed call reads as no leader
	return v
}

// WaitLeader waits until a scheduler other than not holds the leader key and its epoch is the
// one fenced in Postgres (its term has started), and returns the scheduler's name.
func (h *Harness) WaitLeader(not string, d time.Duration) string {
	h.t.Helper()
	var lead string
	testutil.WaitFor(h.t, func() bool {
		s := h.Cluster().Scheduler
		if s.LeaderID == nil || *s.LeaderID == not || s.Epoch == nil || s.FencedEpoch == nil || *s.Epoch != *s.FencedEpoch {
			return false
		}
		lead = *s.LeaderID
		return true
	}, d)
	return lead
}

// leaders sums flowforge_scheduler_is_leader over the running schedulers.
func (h *Harness) leaders(scheds []*Proc) int {
	n := 0
	for _, p := range scheds {
		if !p.Running() {
			continue
		}
		resp, err := h.http.Get("http://" + p.httpAddr + "/metrics") //nolint:noctx // bounded by the client timeout
		if err != nil {
			return -1
		}
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			if v, ok := strings.CutPrefix(sc.Text(), "flowforge_scheduler_is_leader "); ok {
				f, _ := strconv.ParseFloat(v, 64)
				n += int(f)
			}
		}
		_ = resp.Body.Close()
	}
	return n
}

// waitOneLeader waits until exactly one running scheduler reports itself leader.
func (h *Harness) waitOneLeader(scheds []*Proc) {
	h.t.Helper()
	testutil.WaitFor(h.t, func() bool { return h.leaders(scheds) == 1 }, 30*time.Second)
}

func named(procs []*Proc, name string) *Proc {
	for _, p := range procs {
		if p.Name == name {
			return p
		}
	}
	return nil
}

func (h *Harness) count(sql string, args ...any) int {
	h.t.Helper()
	var n int
	if err := h.DB.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	return n
}

// assertEpochsAscend checks the leadership history: one scheduler.leader_changed per term with
// strictly increasing epochs, the last of which Postgres enforces.
func (h *Harness) assertEpochsAscend() {
	h.t.Helper()
	rows, err := h.DB.Query(context.Background(), `SELECT (envelope->'payload'->>'epoch')::bigint FROM outbox
		WHERE envelope->>'event_type' = 'scheduler.leader_changed' ORDER BY id`)
	if err != nil {
		h.t.Fatal(err)
	}
	var epochs []int64
	for rows.Next() {
		var e int64
		if err := rows.Scan(&e); err != nil {
			h.t.Fatal(err)
		}
		epochs = append(epochs, e)
	}
	if rows.Err() != nil || len(epochs) == 0 {
		h.t.Fatalf("leader_changed epochs %v, %v", epochs, rows.Err())
	}
	for i := 1; i < len(epochs); i++ {
		if epochs[i] <= epochs[i-1] {
			h.t.Errorf("leader_changed epochs not increasing: %v", epochs)
		}
	}
	if fenced := h.count(`SELECT epoch::int FROM cluster_state`); int64(fenced) != epochs[len(epochs)-1] {
		h.t.Errorf("cluster_state epoch %d, last leader_changed epoch %d", fenced, epochs[len(epochs)-1])
	}
	h.t.Logf("leader_changed epochs: %v", epochs)
}

// TestRaft_LeaderFailoverUnderLoad (task 04.07, FR-RAFT-08): three schedulers elect exactly one
// leader; killing it mid-workload hands leadership to another within 10 s, and every run still
// completes with the invariants intact.
func TestRaft_LeaderFailoverUnderLoad(t *testing.T) {
	h := NewRaft(t)
	h.StartAPI()
	scheds := h.StartSchedulers(3)
	h.StartWorker("worker-1", 4)
	h.StartWorker("worker-2", 4)
	lead := h.WaitLeader("", 30*time.Second)
	h.waitOneLeader(scheds)
	wf := h.CreateWorkflow(fixture("chain"))
	var runs []Run
	for range 6 {
		runs = append(runs, h.StartRun(wf, nil))
	}
	h.WaitAttempt(runs[0].ID, "b", 30*time.Second, running(""))
	h.Kill(named(scheds, lead))
	killed := time.Now()
	next := h.WaitLeader(lead, 30*time.Second)
	took := time.Since(killed)
	h.metric("scheduler_failover", took)
	if took > 10*time.Second {
		t.Errorf("leader %s killed; %s took over after %s, want ≤ 10 s", lead, next, took)
	}
	for _, r := range runs {
		h.mustStatus("run "+r.ID, h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	}
	h.waitOneLeader(scheds)
	h.AssertHealthy()
	h.assertEpochsAscend()
}

// TestRaft_PausedLeaderIsFencedWhenItWakes (FR-RAFT-08): the leader is frozen (SIGSTOP) past its
// lease; another scheduler takes over. When the old one resumes it is deposed: either its next
// decision fails with STALE_EPOCH (the process exits) or it notices its term ended and stops
// its loops first. Either way runs complete once, and the epochs only go up.
func TestRaft_PausedLeaderIsFencedWhenItWakes(t *testing.T) {
	h := NewRaft(t)
	h.StartAPI()
	scheds := h.StartSchedulers(3)
	h.StartWorker("worker-1", 4)
	lead := h.WaitLeader("", 30*time.Second)
	old := named(scheds, lead)
	wf := h.CreateWorkflow(fixture("chain"))
	var runs []Run
	for range 3 {
		runs = append(runs, h.StartRun(wf, nil))
	}
	h.WaitAttempt(runs[0].ID, "a", 30*time.Second, running(""))
	h.Pause(old)
	paused := time.Now()
	next := h.WaitLeader(lead, 30*time.Second)
	h.metric("paused_leader_takeover", time.Since(paused))
	h.Resume(old)
	var how string
	testutil.WaitFor(t, func() bool {
		raw, _ := os.ReadFile(old.logPath)
		switch {
		case strings.Contains(string(raw), "STALE_EPOCH"):
			how = "a decision failed with STALE_EPOCH and the process exited"
		case strings.Contains(string(raw), "leadership lost; loops stopped"):
			how = "it saw its term end and stopped its loops"
		}
		return how != ""
	}, 30*time.Second)
	t.Logf("old leader %s resumed after %s took over: %s", lead, next, how)
	for _, r := range runs {
		h.mustStatus("run "+r.ID, h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	}
	if got := h.WaitLeader("", 10*time.Second); got != next {
		t.Errorf("leader after the old one resumed = %s, want %s", got, next)
	}
	h.AssertHealthy()
	h.assertEpochsAscend()
}

// TestRaft_KilledWorkerIsRecoveredFromItsRaftLease (FR-RAFT-09): a killed worker's Raft lease
// expires and the leader abandons its attempt within the lease TTL plus two recovery ticks.
func TestRaft_KilledWorkerIsRecoveredFromItsRaftLease(t *testing.T) {
	h := NewRaft(t)
	h.StartAPI()
	h.StartScheduler()
	w1 := h.StartWorker("worker-1", 1)
	h.WaitLeader("", 30*time.Second)
	r := h.StartRun(h.CreateWorkflow(fixture("chain")), nil)
	first := h.WaitAttempt(r.ID, "a", 30*time.Second, running("worker-1"))
	h.Kill(w1)
	killed := time.Now()
	h.StartWorker("worker-2", 1)
	testutil.WaitFor(t, func() bool {
		return h.count(`SELECT count(*) FROM task_attempts WHERE id = $1 AND status = 'ABANDONED'`, first.ID) == 1
	}, 30*time.Second)
	took, bound := time.Since(killed), leaseTTL+2*recoveryTick
	h.metric("raft_worker_loss_recovery", took)
	if took > bound {
		t.Errorf("attempt abandoned %s after the kill, want ≤ lease TTL + 2 ticks = %s", took, bound)
	}
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	if n := h.count(`SELECT count(*) FROM outbox WHERE envelope->>'event_type' = 'worker.lost' AND envelope->>'aggregate_id' = 'worker-1'`); n != 1 {
		t.Errorf("worker.lost events for worker-1 = %d, want 1", n)
	}
	h.AssertHealthy()
}

// TestRaft_QuorumLossStopsSchedulingButNotRunningWork (00_MASTER §6.2): with two of three Raft
// nodes down the scheduler gives up leadership and nothing is reassigned, while running tasks
// finish and their workers keep heartbeating; once quorum returns a leader is elected, healthy
// workers are not abandoned, and the runs complete.
func TestRaft_QuorumLossStopsSchedulingButNotRunningWork(t *testing.T) {
	h := NewRaft(t)
	h.StartAPI()
	h.StartScheduler()
	h.StartWorker("worker-1", 2)
	h.StartWorker("worker-2", 2)
	h.WaitLeader("", 30*time.Second)
	wf := h.CreateWorkflow(map[string]any{"name": "quorum", "on_failure": "fail", "tasks": []map[string]any{
		{"id": "long", "type": "sleep", "config": map[string]any{"duration_ms": 12000}},
		{"id": "next", "type": "echo", "depends_on": []string{"long"}},
	}})
	r1, r2 := h.StartRun(wf, nil), h.StartRun(wf, nil)
	h.WaitAttempt(r1.ID, "long", 30*time.Second, running(""))
	h.WaitAttempt(r2.ID, "long", 30*time.Second, running(""))
	h.Kill(h.Raft[1])
	h.Kill(h.Raft[2])
	lost := time.Now()
	testutil.WaitFor(t, func() bool {
		v := h.Cluster()
		return v.Raft != nil && !v.Raft.Available && v.Scheduler.LeaderID == nil
	}, 20*time.Second)
	h.metric("quorum_loss_detected", time.Since(lost))
	if resp, err := h.http.Get(h.APIURL + "/readyz"); err != nil || resp.StatusCode != http.StatusOK { //nolint:noctx // bounded by the client timeout
		t.Errorf("API /readyz without Raft quorum: %v %v", resp, err)
	} else {
		_ = resp.Body.Close()
	}
	// Running work completes; nothing is reassigned; no new task is scheduled.
	for _, r := range []Run{r1, r2} {
		h.WaitAttempt(r.ID, "long", 30*time.Second, func(a Attempt) bool { return a.Status == "SUCCEEDED" })
	}
	time.Sleep(leaseTTL + 2*recoveryTick) //nolint:forbidigo // e2e: give a would-be spurious expiry its chance
	if n := h.count(`SELECT count(*) FROM task_attempts WHERE status = 'ABANDONED'`); n != 0 {
		t.Fatalf("%d attempts abandoned while Raft had no quorum; tasks:%s", n, h.describe(""))
	}
	h.mustStatus("task next while there is no leader", h.Task(r1.ID, "next").Status, "PENDING")

	h.Restart(h.Raft[1])
	h.Restart(h.Raft[2])
	back := time.Now()
	h.WaitLeader("", 30*time.Second)
	h.metric("quorum_return_new_leader", time.Since(back))
	for _, r := range []Run{r1, r2} {
		h.mustStatus("run "+r.ID, h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	}
	time.Sleep(leaseTTL + 2*recoveryTick) //nolint:forbidigo // e2e: recovery passes run after quorum returns
	if n := h.count(`SELECT count(*) FROM task_attempts WHERE status = 'ABANDONED'`); n != 0 {
		t.Errorf("%d attempts abandoned after quorum returned (healthy workers); tasks:%s", n, h.describe(""))
	}
	if n := h.count(`SELECT count(*) FROM outbox WHERE envelope->>'event_type' = 'worker.lost'`); n != 0 {
		t.Errorf("%d worker.lost events for healthy workers", n)
	}
	h.AssertHealthy()
}
