package rest

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/daishakpatel/flowforge/internal/coordination"
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// clusterSourceTimeout bounds each source of the cluster status, so a Raft outage answers in
// about this long rather than after the client's retries.
const clusterSourceTimeout = 2 * time.Second

// ClusterInfo is where GET /api/v1/cluster/status reads from (task 04.07). A nil source is
// reported as null; Leader and Raft are nil in local mode, which has no Raft.
type ClusterInfo struct {
	Coordination string                                                        // FF_COORDINATION: local or raft
	Fence        func(context.Context) (epoch int64, leader string, err error) // Postgres cluster_state
	Alive        func(context.Context) ([]persistence.WorkerView, error)       // the LeaseProvider's live workers
	Leader       func(context.Context) (coordination.LeaderInfo, error)        // the scheduler leader key (ErrNoLeader: none)
	Raft         func(context.Context) (*pb.StatusResponse, error)             // the Raft leader's Cluster.Status
}

// clusterStatusResponse is the ClusterStatus schema. A part whose source failed is null (and
// raft.available false); the failure is logged, not returned, so the page still answers while
// Raft or Postgres is down.
type clusterStatusResponse struct {
	Coordination string                  `json:"coordination"`
	Scheduler    schedulerStatusResponse `json:"scheduler"`
	Raft         *raftStatusResponse     `json:"raft"` // null in local mode
	Workers      workersStatusResponse   `json:"workers"`
	FetchedAt    time.Time               `json:"fetched_at"`
}

type schedulerStatusResponse struct {
	LeaderID    *string    `json:"leader_id"`    // the leader key's node; null with no leader or in local mode
	Epoch       *int64     `json:"epoch"`        // the leader key's epoch
	Since       *time.Time `json:"since"`        // when the leader campaigned
	FencedEpoch *int64     `json:"fenced_epoch"` // cluster_state.epoch: what scheduler transactions assert
}

type raftStatusResponse struct {
	Available   bool               `json:"available"` // a Raft leader answered
	LeaderID    *uint64            `json:"leader_id"`
	Term        *uint64            `json:"term"`
	CommitIndex *uint64            `json:"commit_index"`
	Nodes       []raftNodeResponse `json:"nodes"` // the leader's view of every voter
}

type raftNodeResponse struct {
	ID         uint64 `json:"id"`
	Addr       string `json:"addr"`
	Reachable  bool   `json:"reachable"`
	MatchIndex uint64 `json:"match_index"`
	Lag        uint64 `json:"lag"`
}

type workersStatusResponse struct {
	Alive *int `json:"alive"` // workers holding a live lease; null when liveness can't be read
}

// clusterStatus serves GET /api/v1/cluster/status (task 04.07): the scheduler leader and epoch,
// Raft health and the live worker count, each read concurrently and bounded.
func (h *handlers) clusterStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	c := h.cluster
	res := clusterStatusResponse{Coordination: c.Coordination, Workers: workersStatusResponse{}}
	var wg sync.WaitGroup
	source := func(name string, f func(context.Context) error) {
		wg.Go(func() {
			sctx, cancel := context.WithTimeout(ctx, clusterSourceTimeout)
			defer cancel()
			if err := f(sctx); err != nil && !errors.Is(err, coordination.ErrNoLeader) {
				logging.FromContext(ctx).WarnContext(ctx, "cluster status: source unavailable", "source", name, logging.Err(err))
			}
		})
	}
	if c.Fence != nil {
		source("postgres", func(ctx context.Context) error {
			epoch, _, err := c.Fence(ctx)
			if err == nil {
				res.Scheduler.FencedEpoch = &epoch
			}
			return err
		})
	}
	if c.Alive != nil {
		source("leases", func(ctx context.Context) error {
			ws, err := c.Alive(ctx)
			if err == nil {
				n := len(ws)
				res.Workers.Alive = &n
			}
			return err
		})
	}
	if c.Leader != nil {
		source("leader", func(ctx context.Context) error {
			l, err := c.Leader(ctx)
			if err == nil {
				since := time.UnixMilli(l.SinceMs).UTC()
				res.Scheduler.LeaderID, res.Scheduler.Epoch, res.Scheduler.Since = &l.NodeID, &l.Epoch, &since
			}
			return err
		})
	}
	if c.Raft != nil {
		res.Raft = &raftStatusResponse{Nodes: []raftNodeResponse{}}
		source("raft", func(ctx context.Context) error {
			st, err := c.Raft(ctx)
			if err == nil {
				fillRaft(res.Raft, st)
			}
			return err
		})
	}
	wg.Wait()
	res.FetchedAt = h.clock.Now().UTC()
	writeJSON(w, http.StatusOK, res)
}

func fillRaft(out *raftStatusResponse, st *pb.StatusResponse) {
	if st.GetLeaderId() == 0 {
		return
	}
	lead, term, commit := st.GetLeaderId(), st.GetTerm(), st.GetCommitIndex()
	out.Available, out.LeaderID, out.Term, out.CommitIndex = true, &lead, &term, &commit
	for _, p := range st.GetPeers() {
		out.Nodes = append(out.Nodes, raftNodeResponse{
			ID: p.GetId(), Addr: p.GetAddr(), Reachable: p.GetReachable(), MatchIndex: p.GetMatchIndex(), Lag: p.GetLag(),
		})
	}
}
