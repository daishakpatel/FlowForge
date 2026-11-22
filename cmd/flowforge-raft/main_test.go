package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/raftkv/client"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

func TestMain(m *testing.M) { testutil.LeakCheck(m) }

// proc is one flowforge-raft process run in-process. Its listeners are created up front, so the
// addresses are known before any node starts and no port can be taken in between.
type proc struct {
	id     uint64
	env    map[string]string
	lis    map[string]net.Listener
	cancel context.CancelFunc
	done   chan error
}

func (p *proc) listen(_ context.Context, _, addr string) (net.Listener, error) {
	if l, ok := p.lis[addr]; ok {
		return l, nil
	}
	return nil, fmt.Errorf("unexpected listen on %s", addr)
}

func loopback(t *testing.T) net.Listener {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// startCluster runs three processes and stops them in t.Cleanup.
func startCluster(t *testing.T) []*proc {
	t.Helper()
	procs := make([]*proc, 3)
	var peers []string
	for i := range procs {
		p := &proc{id: uint64(i + 1), lis: map[string]net.Listener{}, done: make(chan error, 1)}
		p.env = map[string]string{"FF_RAFT_NODE_ID": strconv.Itoa(i + 1), "FF_RAFT_DATA_DIR": t.TempDir(), "FF_LOG_LEVEL": "error"}
		for _, key := range []string{"FF_RAFT_CLIENT_ADDR", "FF_RAFT_PEER_ADDR", "FF_RAFT_HTTP_ADDR"} {
			l := loopback(t)
			p.lis[l.Addr().String()] = l
			p.env[key] = l.Addr().String()
		}
		peers = append(peers, fmt.Sprintf("%d=%s", p.id, p.env["FF_RAFT_PEER_ADDR"]))
		procs[i] = p
	}
	for _, p := range procs {
		p.env["FF_RAFT_PEERS"] = strings.Join(peers, ",")
		opts := config.Options{LookupEnv: func(k string) (string, bool) { v, ok := p.env[k]; return v, ok }}
		ctx, cancel := context.WithCancel(context.Background())
		p.cancel = cancel
		go func() { p.done <- run(ctx, opts, io.Discard, p.listen) }()
	}
	t.Cleanup(func() {
		for _, p := range procs {
			p.cancel()
			if err := <-p.done; err != nil {
				t.Errorf("node %d: run returned %v", p.id, err)
			}
		}
	})
	return procs
}

// TestRun_ServesAClusterAndHandsOffLeadershipOnShutdown runs three flowforge-raft processes,
// writes through the client, then shuts the leader down gracefully while a writer keeps going:
// leadership moves before the node stops, every write succeeds, and the writer is redirected at
// most once.
func TestRun_ServesAClusterAndHandsOffLeadershipOnShutdown(t *testing.T) {
	procs := startCluster(t)
	var redirects atomic.Int64
	countRedirects := grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		for _, d := range status.Convert(err).Details() {
			if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == "NOT_LEADER" {
				redirects.Add(1)
			}
		}
		return err
	})
	var eps []string
	for _, p := range procs {
		eps = append(eps, p.env["FF_RAFT_CLIENT_ADDR"])
	}
	cl, err := client.New(client.Config{
		Endpoints:   eps,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), countRedirects},
		Rand:        rand.New(rand.NewPCG(1, 1)), //nolint:gosec // G404: seeded jitter keeps the test reproducible
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var lead uint64
	testutil.WaitFor(t, func() bool {
		st, err := cl.Status(ctx)
		lead = st.GetLeaderId()
		return err == nil && lead != 0
	}, 20*time.Second)
	if _, err := cl.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got, err := cl.Get(ctx, "k"); err != nil || string(got.GetKv().GetValue()) != "v" {
		t.Fatalf("Get = %v, %v", got, err)
	}

	// The client now writes to the leader. Keep writing while the leader shuts down.
	redirects.Store(0)
	stop := make(chan struct{})
	var writes atomic.Int64
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := cl.Put(ctx, "w", []byte(strconv.Itoa(i))); err != nil {
				t.Errorf("write %d during the handoff: %v", i, err)
				return
			}
			writes.Add(1)
		}
	})
	old := procs[lead-1]
	old.cancel()
	if err := <-old.done; err != nil {
		t.Fatalf("the leader's run returned %v", err)
	}
	old.done <- nil // for the cleanup
	before := writes.Load()
	testutil.WaitFor(t, func() bool { return writes.Load() > before+20 }, 20*time.Second)
	close(stop)
	wg.Wait()

	st, err := cl.Status(ctx)
	if err != nil || st.GetLeaderId() == 0 || st.GetLeaderId() == lead {
		t.Fatalf("after the shutdown: Status = %v, %v (old leader %d)", st, err, lead)
	}
	if n := redirects.Load(); n > 1 {
		t.Fatalf("the writer was redirected %d times, want at most 1", n)
	}
	t.Logf("leader %d handed off to %d; %d writes, %d redirect(s)", lead, st.GetLeaderId(), writes.Load(), redirects.Load())
}

func TestRun_RejectsBadConfig(t *testing.T) {
	err := run(context.Background(), config.Options{LookupEnv: func(string) (string, bool) { return "", false }}, io.Discard,
		func(context.Context, string, string) (net.Listener, error) { return nil, errors.New("unused") })
	if err == nil {
		t.Fatal("run with no configuration succeeded")
	}
}

func TestClusterID_DependsOnVotersOnly(t *testing.T) {
	a := clusterID([]uint64{1, 2, 3})
	if a == 0 || a != clusterID([]uint64{3, 1, 2}) || a == clusterID([]uint64{1, 2, 4}) {
		t.Fatalf("clusterID not a stable function of the voter set: %x", a)
	}
}
