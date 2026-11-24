package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/coordination"
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// clusterBody is the ClusterStatus schema, decoded loosely enough to see nulls.
type clusterBody struct {
	Coordination string `json:"coordination"`
	Scheduler    struct {
		LeaderID    *string    `json:"leader_id"`
		Epoch       *int64     `json:"epoch"`
		Since       *time.Time `json:"since"`
		FencedEpoch *int64     `json:"fenced_epoch"`
	} `json:"scheduler"`
	Raft *struct {
		Available bool    `json:"available"`
		LeaderID  *uint64 `json:"leader_id"`
		Term      *uint64 `json:"term"`
		Nodes     []struct {
			ID        uint64 `json:"id"`
			Reachable bool   `json:"reachable"`
			Lag       uint64 `json:"lag"`
		} `json:"nodes"`
	} `json:"raft"`
	Workers struct {
		Alive *int `json:"alive"`
	} `json:"workers"`
	FetchedAt time.Time `json:"fetched_at"`
}

func clusterStatus(t *testing.T, info ClusterInfo) clusterBody {
	t.Helper()
	d := testDeps(auth.NewStub(true), nil, nil, func(context.Context) error { return nil })
	d.Cluster = info
	r, err := NewRouter(d)
	if err != nil {
		t.Fatal(err)
	}
	rec := newContract(t).do(t, r, "GET", "/api/v1/cluster/status", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/cluster/status = %d %s", rec.Code, rec.Body)
	}
	var b clusterBody
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
		t.Fatal(err)
	}
	return b
}

func fence(epoch int64) func(context.Context) (int64, string, error) {
	return func(context.Context) (int64, string, error) { return epoch, "scheduler-2", nil }
}

func alive(n int) func(context.Context) ([]persistence.WorkerView, error) {
	return func(context.Context) ([]persistence.WorkerView, error) { return make([]persistence.WorkerView, n), nil }
}

func TestClusterStatus_RaftModeAggregatesLeaderEpochRaftAndWorkers(t *testing.T) {
	t.Parallel()
	since := time.Date(2026, 11, 22, 9, 0, 0, 0, time.UTC)
	b := clusterStatus(t, ClusterInfo{
		Coordination: "raft", Fence: fence(7), Alive: alive(2),
		Leader: func(context.Context) (coordination.LeaderInfo, error) {
			return coordination.LeaderInfo{NodeID: "scheduler-2", Epoch: 7, SinceMs: since.UnixMilli()}, nil
		},
		Raft: func(context.Context) (*pb.StatusResponse, error) {
			return &pb.StatusResponse{LeaderId: 1, Term: 3, CommitIndex: 100, Peers: []*pb.PeerStatus{
				{Id: 1, Addr: "raft-1:7100", Reachable: true, MatchIndex: 100},
				{Id: 2, Addr: "raft-2:7100", Reachable: true, MatchIndex: 100},
				{Id: 3, Addr: "raft-3:7100", Reachable: false, MatchIndex: 40, Lag: 60},
			}}, nil
		},
	})
	s := b.Scheduler
	if b.Coordination != "raft" || s.LeaderID == nil || *s.LeaderID != "scheduler-2" || s.Epoch == nil || *s.Epoch != 7 ||
		s.Since == nil || !s.Since.Equal(since) || s.FencedEpoch == nil || *s.FencedEpoch != 7 {
		t.Errorf("scheduler = %+v", s)
	}
	if b.Raft == nil || !b.Raft.Available || *b.Raft.LeaderID != 1 || *b.Raft.Term != 3 || len(b.Raft.Nodes) != 3 ||
		b.Raft.Nodes[2].Reachable || b.Raft.Nodes[2].Lag != 60 {
		t.Errorf("raft = %+v", b.Raft)
	}
	if b.Workers.Alive == nil || *b.Workers.Alive != 2 || b.FetchedAt.IsZero() {
		t.Errorf("workers = %+v, fetched_at %v", b.Workers, b.FetchedAt)
	}
}

// With Raft down (quorum lost) the status still answers: Raft unavailable, no known leader and
// no worker count (liveness comes from Raft), and the Postgres epoch still shown.
func TestClusterStatus_RaftDownIsReportedNotFailed(t *testing.T) {
	t.Parallel()
	down := errors.New("raft: unavailable")
	b := clusterStatus(t, ClusterInfo{
		Coordination: "raft", Fence: fence(7),
		Alive:  func(context.Context) ([]persistence.WorkerView, error) { return nil, down },
		Leader: func(context.Context) (coordination.LeaderInfo, error) { return coordination.LeaderInfo{}, down },
		Raft:   func(context.Context) (*pb.StatusResponse, error) { return nil, down },
	})
	if b.Raft == nil || b.Raft.Available || b.Raft.LeaderID != nil || b.Scheduler.LeaderID != nil || b.Workers.Alive != nil {
		t.Errorf("status with raft down = %+v", b)
	}
	if b.Scheduler.FencedEpoch == nil || *b.Scheduler.FencedEpoch != 7 {
		t.Errorf("fenced epoch = %v", b.Scheduler.FencedEpoch)
	}
	// No scheduler holds the leader key (between terms): leader null, Raft available.
	b = clusterStatus(t, ClusterInfo{
		Coordination: "raft", Fence: fence(7), Alive: alive(0),
		Leader: func(context.Context) (coordination.LeaderInfo, error) {
			return coordination.LeaderInfo{}, coordination.ErrNoLeader
		},
		Raft: func(context.Context) (*pb.StatusResponse, error) {
			return &pb.StatusResponse{LeaderId: 2, Term: 4}, nil
		},
	})
	if b.Scheduler.LeaderID != nil || b.Raft == nil || !b.Raft.Available || *b.Workers.Alive != 0 {
		t.Errorf("status between terms = %+v", b)
	}
}

func TestClusterStatus_LocalModeHasNoRaft(t *testing.T) {
	t.Parallel()
	b := clusterStatus(t, ClusterInfo{Coordination: "local", Fence: fence(0), Alive: alive(1)})
	if b.Coordination != "local" || b.Raft != nil || b.Scheduler.FencedEpoch == nil || *b.Scheduler.FencedEpoch != 0 || *b.Workers.Alive != 1 {
		t.Errorf("local status = %+v", b)
	}
}
