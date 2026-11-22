//go:build compose

// Package raft_test talks to the Compose Raft cluster (task 04.06). Run it through
// scripts/dev/raft-smoke.sh, which starts the cluster first (make raft-up); it is behind the
// compose build tag so make test and make test-integration never need the cluster.
package raft_test

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/client"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

// adminToken is the Compose file's dev-only default.
const adminToken = "dev-only-raft-admin-token"

func endpoint(id uint64) string { return fmt.Sprintf("127.0.0.1:700%d", id) }

func compose(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"compose", "--profile", "raft"}, args...)...) //nolint:gosec // G204: fixed docker compose arguments
	cmd.Dir = "../../.."
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
}

func status(id uint64) (*pb.StatusResponse, error) {
	conn, err := grpc.NewClient(endpoint(id), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return pb.NewClusterClient(conn).Status(ctx, &pb.StatusRequest{})
}

func health(id uint64) (*pb.HealthResponse, error) {
	conn, err := grpc.NewClient(endpoint(id), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return pb.NewClusterClient(conn).Health(ctx, &pb.HealthRequest{})
}

// leader returns the node every given node names as leader, and its term.
func leader(ids ...uint64) (uint64, uint64) {
	var lead, term uint64
	for _, id := range ids {
		st, err := status(id)
		if err != nil || st.GetLeaderId() == 0 || (lead != 0 && st.GetLeaderId() != lead) {
			return 0, 0
		}
		lead, term = st.GetLeaderId(), st.GetTerm()
	}
	return lead, term
}

func TestCompose_ClusterServesSurvivesALeaderKillAndTransfers(t *testing.T) {
	ids := []uint64{1, 2, 3}
	var lead, term uint64
	testutil.WaitFor(t, func() bool { lead, term = leader(ids...); return lead != 0 }, 30*time.Second)
	leaders := 0
	for _, id := range ids {
		if st, _ := status(id); st.GetState() == "LEADER" {
			leaders++
		}
		if h, err := health(id); err != nil || h.GetStatus() != "READY" {
			t.Fatalf("node %d health = %v, %v", id, h, err)
		}
	}
	if leaders != 1 {
		t.Fatalf("%d nodes report LEADER", leaders)
	}

	cl, err := client.New(client.Config{Endpoints: []string{endpoint(1), endpoint(2), endpoint(3)}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := cl.Put(ctx, "smoke", []byte("ok")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got, err := cl.Get(ctx, "smoke"); err != nil || string(got.GetKv().GetValue()) != "ok" {
		t.Fatalf("Get = %v, %v", got, err)
	}

	// Kill the leader's container: the survivors elect a new leader in a higher term within 5 s.
	compose(t, "kill", fmt.Sprintf("raft-%d", lead))
	var survivors []uint64
	for _, id := range ids {
		if id != lead {
			survivors = append(survivors, id)
		}
	}
	start := time.Now()
	var newLead, newTerm uint64
	testutil.WaitFor(t, func() bool {
		newLead, newTerm = leader(survivors...)
		return newLead != 0 && newLead != lead && newTerm > term
	}, 5*time.Second)
	t.Logf("leader %d killed; survivors elected %d (term %d -> %d) in %s", lead, newLead, term, newTerm, time.Since(start))
	compose(t, "up", "-d", "--wait", fmt.Sprintf("raft-%d", lead))

	// transfer-leader moves leadership back to the restarted node, once it has caught up.
	conn, err := grpc.NewClient(endpoint(newLead), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	actx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+adminToken)
	testutil.WaitFor(t, func() bool {
		if _, err := pb.NewAdminClient(conn).TransferLeader(actx, &pb.TransferLeaderRequest{To: lead}); err != nil {
			return false
		}
		l, _ := leader(ids...)
		return l == lead
	}, 30*time.Second)
	if _, err := cl.Put(ctx, "after-transfer", []byte("ok")); err != nil {
		t.Fatalf("Put after transfer: %v", err)
	}
	// dump-state-hash: equal on every node at the same applied index.
	testutil.WaitFor(t, func() bool {
		var hash []byte
		var index uint64
		for i, id := range ids {
			st, err := status(id)
			if err != nil || (i > 0 && (st.GetStateHashIndex() != index || !bytes.Equal(st.GetStateHash(), hash))) {
				return false
			}
			hash, index = st.GetStateHash(), st.GetStateHashIndex()
		}
		return true
	}, 10*time.Second)
}
