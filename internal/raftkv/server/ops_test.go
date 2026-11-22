package server_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/kvtest"
	fftest "github.com/daishakpatel/flowforge/internal/testutil"
)

const adminToken = "test-admin-token"

func admin(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

// TestStatus_ReportsRoleProgressAndStateHash (04.06): the leader reports every voter's progress;
// at equal applied indexes all nodes report the same state hash.
func TestStatus_ReportsRoleProgressAndStateHash(t *testing.T) {
	f := newFixture(t, kvtest.Options{})
	for i := range 10 {
		f.put(t, fmt.Sprint("k", i), "v")
	}
	var statuses []*pb.StatusResponse
	fftest.WaitFor(t, func() bool {
		statuses = statuses[:0]
		for _, id := range f.c.IDs() {
			st, err := pb.NewClusterClient(f.c.Conn(id)).Status(tctx(t), &pb.StatusRequest{})
			if err != nil {
				return false
			}
			statuses = append(statuses, st)
		}
		for _, st := range statuses {
			if st.GetStateHashIndex() != statuses[0].GetStateHashIndex() || st.GetAppliedIndex() != st.GetCommitIndex() {
				return false
			}
		}
		lead := statuses[f.lead-1]
		for _, p := range lead.GetPeers() {
			if p.GetLag() != 0 || !p.GetReachable() {
				return false
			}
		}
		return len(lead.GetPeers()) == 3
	}, leaderWait)
	for _, st := range statuses {
		want := "FOLLOWER"
		if st.GetNodeId() == f.lead {
			want = "LEADER"
		}
		if st.GetState() != want || len(st.GetStateHash()) != 32 || !bytes.Equal(st.GetStateHash(), statuses[0].GetStateHash()) ||
			st.GetUptimeMs() < 0 || st.GetFirstIndex() == 0 {
			t.Fatalf("node %d status = %v", st.GetNodeId(), st)
		}
	}
	for _, p := range statuses[f.lead-1].GetPeers() {
		if p.GetMatchIndex() != statuses[f.lead-1].GetCommitIndex() || p.GetAddr() == "" {
			t.Fatalf("peer %v", p)
		}
	}
}

func TestHealth_ReadyOnlyWithALeader(t *testing.T) {
	f := newFixture(t, kvtest.Options{})
	h, err := pb.NewClusterClient(f.c.Conn(f.lead)).Health(tctx(t), &pb.HealthRequest{})
	if err != nil || !h.GetHealthy() || h.GetStatus() != "READY" || h.GetLeaderId() != f.lead {
		t.Fatalf("Health = %v, %v", h, err)
	}
	survivor := f.c.Others(f.lead)[0]
	for _, id := range f.c.Others(survivor) {
		f.c.Kill(id)
	}
	cl := pb.NewClusterClient(f.c.Conn(survivor))
	fftest.WaitFor(t, func() bool {
		h, err := cl.Health(tctx(t), &pb.HealthRequest{})
		return err == nil && !h.GetHealthy() && h.GetStatus() == "NOT_READY"
	}, leaderWait)
}

func TestAdmin_RequiresTheToken(t *testing.T) {
	f := newFixture(t, kvtest.Options{AdminToken: adminToken})
	a := pb.NewAdminClient(f.c.Conn(f.lead))
	if _, err := a.SnapshotNow(tctx(t), &pb.SnapshotNowRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("no token: %v", err)
	}
	if _, err := a.SnapshotNow(admin(tctx(t), "wrong"), &pb.SnapshotNowRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token: %v", err)
	}
	disabled := newFixture(t, kvtest.Options{})
	if _, err := pb.NewAdminClient(disabled.c.Conn(disabled.lead)).SnapshotNow(admin(tctx(t), ""), &pb.SnapshotNowRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("admin without a configured token: %v", err)
	}
}

func TestAdmin_TransferSnapshotCompact(t *testing.T) {
	f := newFixture(t, kvtest.Options{AdminToken: adminToken})
	for i := range 20 {
		f.put(t, fmt.Sprint("k", i), "v")
	}
	a := pb.NewAdminClient(f.c.Conn(f.lead))
	snap, err := a.SnapshotNow(admin(tctx(t), adminToken), &pb.SnapshotNowRequest{})
	if err != nil || snap.GetSnapshotIndex() == 0 {
		t.Fatalf("SnapshotNow = %v, %v", snap, err)
	}
	cmp, err := a.Compact(admin(tctx(t), adminToken), &pb.CompactRequest{})
	if err != nil || cmp.GetFirstIndex() != snap.GetSnapshotIndex()+1 {
		t.Fatalf("Compact = %v, %v; want first index %d", cmp, err, snap.GetSnapshotIndex()+1)
	}
	to := f.c.Others(f.lead)[1]
	res, err := a.TransferLeader(admin(tctx(t), adminToken), &pb.TransferLeaderRequest{To: to})
	if err != nil {
		t.Fatalf("TransferLeader: %v", err)
	}
	if lead := f.c.WaitLeader(leaderWait); lead != to || res.GetLeaderId() != to {
		t.Fatalf("leader %d after transfer to %d (response %v)", lead, to, res)
	}
	if _, err := a.TransferLeader(admin(tctx(t), adminToken), &pb.TransferLeaderRequest{}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("TransferLeader on a follower = %v, want NOT_LEADER", err)
	}
}

// TestSnapshot_WipedFollowerRecoversViaSnapshot (04.06): with a snapshot every 50 entries, 500
// writes compact the log; a follower whose data is wiped can only catch up through the leader's
// snapshot, and ends with the same state hash as its peers.
func TestSnapshot_WipedFollowerRecoversViaSnapshot(t *testing.T) {
	f := newFixture(t, kvtest.Options{SnapshotEntries: 50, SnapshotTrailing: 10})
	for i := range 500 {
		f.put(t, fmt.Sprint("k", i%37), fmt.Sprint(i))
	}
	lead := f.c.Member(f.lead).Node.Status()
	if lead.FirstIndex <= 1 || lead.SnapshotIndex < 450 {
		t.Fatalf("leader log not compacted: first %d, snapshot %d", lead.FirstIndex, lead.SnapshotIndex)
	}
	wiped := f.c.Others(f.lead)[0]
	f.c.Kill(wiped)
	f.c.Wipe(wiped)
	f.c.Start(wiped)
	// Hearing the leader is not enough: the wiped node holds none of the state, so it is not
	// ready until it has caught up to the leader's commit.
	fftest.WaitFor(t, func() bool { return f.c.Member(wiped).Node.Status().Leader != 0 }, leaderWait)
	if h, err := pb.NewClusterClient(f.c.Conn(wiped)).Health(tctx(t), &pb.HealthRequest{}); err != nil || h.GetHealthy() {
		t.Fatalf("wiped follower health before catching up = %v, %v; want NOT_READY", h, err)
	}
	// The wipe procedure: move leadership, so a leader with fresh progress sends the snapshot.
	if err := f.c.Member(f.lead).Node.TransferLeadership(tctx(t), f.c.Others(f.lead, wiped)[0]); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	f.lead = f.c.WaitLeader(leaderWait)
	f.kv = pb.NewKVClient(f.c.Conn(f.lead))
	f.put(t, "after-wipe", "v")
	fftest.WaitFor(t, func() bool {
		m := f.c.Member(wiped)
		return m != nil && m.FSM.AppliedIndex() == f.c.Member(f.lead).FSM.AppliedIndex()
	}, leaderWait)
	st := f.c.Member(wiped).Node.Status()
	if st.SnapshotIndex == 0 || st.FirstIndex <= 1 {
		t.Fatalf("wiped follower did not install a snapshot: %+v", st)
	}
	h1, i1 := f.c.Member(wiped).FSM.StateHashAt()
	h2, i2 := f.c.Member(f.lead).FSM.StateHashAt()
	if i1 != i2 || h1 != h2 {
		t.Fatalf("state hash %x@%d on the wiped follower, %x@%d on the leader", h1, i1, h2, i2)
	}
}
