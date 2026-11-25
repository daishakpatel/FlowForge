//go:build fault

package raft_test

// Container-level scenarios (task 04.08): the Compose Raft cluster raft-1..3 (production binary
// and timing), driven by test/fault/dockerctl, measured through the host ports 7001-7003. They run
// only with FF_FAULT_CONTAINERS=1 (FF_FAULT_CONTAINERS=1 make test-fault-raft, the nightly job); the cluster
// is started here and left running.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pbc "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/test/fault/dockerctl"
)

const containerLevel = "container"

var containerEndpoints = []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"}

// containers skips unless container scenarios are enabled, then brings raft-1..3 up healthy.
func containers(t *testing.T) *dockerctl.Compose {
	t.Helper()
	if os.Getenv("FF_FAULT_CONTAINERS") != "1" {
		t.Skip("container-level scenario: run with FF_FAULT_CONTAINERS=1 and Docker (FF_FAULT_CONTAINERS=1 make test-fault-raft)")
	}
	_, file, _, _ := runtime.Caller(0)
	dc := dockerctl.New(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	if err := dc.Up(ctx, "raft-1", "raft-2", "raft-3"); err != nil {
		t.Fatal(err)
	}
	return dc
}

// status asks one node for its Cluster.Status.
func status(ctx context.Context, addr string) (*pbc.StatusResponse, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }() // one-shot
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return pbc.NewClusterClient(conn).Status(ctx, &pbc.StatusRequest{})
}

// containerLeader waits for a leader that every reachable node agrees on.
func containerLeader(t *testing.T, not uint64) uint64 {
	t.Helper()
	var lead uint64
	ok := testutil.Eventually(func() bool {
		lead = 0
		for _, ep := range containerEndpoints {
			st, err := status(t.Context(), ep)
			if err != nil {
				continue
			}
			if st.GetLeaderId() == 0 || (lead != 0 && st.GetLeaderId() != lead) {
				return false
			}
			lead = st.GetLeaderId()
		}
		return lead != 0 && lead != not
	}, 30*time.Second, 100*time.Millisecond)
	if !ok {
		t.Fatalf("no agreed leader (other than %d)", not)
	}
	return lead
}

// containersConverged waits until every node reports the same state hash at the same applied
// index (Cluster.Status, 04.06).
func (s *scenario) containersConverged() {
	s.t.Helper()
	var hash []byte
	var index uint64
	ok := testutil.Eventually(func() bool {
		for i, ep := range containerEndpoints {
			st, err := status(s.t.Context(), ep)
			if err != nil || (i > 0 && (st.GetStateHashIndex() != index || string(st.GetStateHash()) != string(hash))) {
				return false
			}
			hash, index = st.GetStateHash(), st.GetStateHashIndex()
		}
		return true
	}, 60*time.Second, 200*time.Millisecond)
	if !ok {
		s.t.Fatal("the containers did not converge to one state hash")
	}
	s.observe("raft-1..3 converged: state hash %x… at applied index %d", hash[:4], index)
}

func service(id uint64) string { return fmt.Sprintf("raft-%d", id) }

func TestFault_C1_ContainerKillLeader(t *testing.T) {
	dc := containers(t)
	s := newScenario(t, "C1", "Kill leader (container)", containerLevel, "docker compose kill (SIGKILL) of the leader's container",
		"writes through the other two succeed within 3 s; linearizable; converged after the container restarts")
	s.target("Raft failover", raftFailover)
	l := s.startLoad(containerEndpoints, clientsPer)
	probe := s.client(containerEndpoints, 50)
	defer closeClient(probe)
	s.run(2 * time.Second)
	lead := containerLeader(t, 0)
	s.inject(func() {
		if err := dc.Kill(t.Context(), service(lead)); err != nil {
			t.Fatal(err)
		}
	})
	ttr := s.recover(probe, 20*time.Second)
	next := containerLeader(t, lead)
	s.observe("%s killed; raft-%d leads; writes succeeded %.2f s after the kill", service(lead), next, ttr.Seconds())
	if err := dc.Up(t.Context(), service(lead)); err != nil {
		t.Fatal(err)
	}
	l.stop()
	s.check()
	s.containersConverged()
}

func TestFault_C2_ContainerPauseLeader(t *testing.T) {
	dc := containers(t)
	s := newScenario(t, "C2", "Pause leader (container)", containerLevel, "docker pause (SIGSTOP) of the leader's container for 5 s",
		"the other two elect a leader and writes succeed within 3 s; the resumed old leader follows; linearizable; converged")
	s.target("Raft failover", raftFailover)
	l := s.startLoad(containerEndpoints, clientsPer)
	probe := s.client(containerEndpoints, 50)
	defer closeClient(probe)
	s.run(2 * time.Second)
	lead := containerLeader(t, 0)
	s.inject(func() {
		if err := dc.Pause(t.Context(), service(lead)); err != nil {
			t.Fatal(err)
		}
	})
	ttr := s.recover(probe, 20*time.Second)
	s.run(5*time.Second - ttr)
	if err := dc.Unpause(t.Context(), service(lead)); err != nil {
		t.Fatal(err)
	}
	next := containerLeader(t, 0)
	s.observe("%s paused; writes succeeded %.2f s later; after resuming, raft-%d leads", service(lead), ttr.Seconds(), next)
	l.stop()
	s.check()
	s.containersConverged()
}

func TestFault_C7_ContainerPartition(t *testing.T) {
	dc := containers(t)
	s := newScenario(t, "C7", "Network partition 1|2 (container)", containerLevel, "docker network disconnect of the leader's container from the Compose network for 5 s",
		"the majority elects a leader and writes succeed within 3 s; after reconnecting, the old leader follows; linearizable; converged")
	s.target("Raft failover", raftFailover)
	l := s.startLoad(containerEndpoints, clientsPer)
	probe := s.client(containerEndpoints, 50)
	defer closeClient(probe)
	s.run(2 * time.Second)
	lead := containerLeader(t, 0)
	s.inject(func() {
		if err := dc.Disconnect(t.Context(), service(lead)); err != nil {
			t.Fatal(err)
		}
	})
	ttr := s.recover(probe, 20*time.Second)
	s.run(5*time.Second - ttr)
	if err := dc.Connect(t.Context(), service(lead)); err != nil {
		t.Fatal(err)
	}
	next := containerLeader(t, 0)
	s.observe("%s disconnected; writes succeeded %.2f s later; reconnected, raft-%d leads", service(lead), ttr.Seconds(), next)
	l.stop()
	s.check()
	s.containersConverged()
}

func TestFault_C8_ContainerRestartAll(t *testing.T) {
	dc := containers(t)
	s := newScenario(t, "C8", "Restart all nodes (container)", containerLevel, "docker compose restart of raft-1..3 (volumes kept)",
		"state is preserved: a value written before reads back; writes succeed again; linearizable; converged")
	l := s.startLoad(containerEndpoints, clientsPer)
	probe := s.client(containerEndpoints, 50)
	defer closeClient(probe)
	s.run(2 * time.Second)
	marker := s.prefix + "marker"
	if err := s.putMarker(probe, marker, "before-restart"); err != nil {
		t.Fatal(err)
	}
	s.inject(func() {
		if err := dc.Restart(t.Context(), "raft-1", "raft-2", "raft-3"); err != nil {
			t.Fatal(err)
		}
	})
	ttr := s.recover(probe, 60*time.Second)
	if got := s.readBack(probe, marker); got != "before-restart" {
		t.Fatalf("marker after restarting every container = %q", got)
	}
	s.observe("raft-1..3 restarted; writes succeeded %.2f s after the restart began; the marker reads back", ttr.Seconds())
	l.stop()
	s.check()
	s.containersConverged()
}
