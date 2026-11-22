package server_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/daishakpatel/flowforge/internal/clock"
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/kvtest"
	fftest "github.com/daishakpatel/flowforge/internal/testutil"
)

func TestMain(m *testing.M) { fftest.LeakCheck(m) }

const leaderWait = 5 * time.Second

func tctx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func meta(client, req string) *pb.RequestMeta {
	return &pb.RequestMeta{ClientId: client, RequestId: req}
}

// errInfo returns a status's code, its ErrorInfo reason and metadata.
func errInfo(t *testing.T, err error) (codes.Code, string, map[string]string) {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok || err == nil {
		t.Fatalf("want a gRPC status error, got %v", err)
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return st.Code(), info.GetReason(), info.GetMetadata()
		}
	}
	return st.Code(), "", nil
}

func wantErr(t *testing.T, err error, code codes.Code, reason, resultCode string) map[string]string {
	t.Helper()
	c, r, md := errInfo(t, err)
	if c != code || r != reason || (resultCode != "" && md["result_code"] != resultCode) {
		t.Fatalf("error = %v (%s, reason %q, metadata %v); want %s, %q, result_code %q", err, c, r, md, code, reason, resultCode)
	}
	return md
}

type fixture struct {
	c    *kvtest.Cluster
	lead uint64
	kv   pb.KVClient
}

func newFixture(t *testing.T, opts kvtest.Options) *fixture {
	c := kvtest.New(t, opts)
	lead := c.WaitLeader(leaderWait)
	return &fixture{c: c, lead: lead, kv: pb.NewKVClient(c.Conn(lead))}
}

func (f *fixture) put(t *testing.T, key, value string) *pb.KeyValue {
	t.Helper()
	res, err := f.kv.Put(tctx(t), &pb.PutRequest{Key: key, Value: []byte(value)})
	if err != nil {
		t.Fatalf("Put(%s): %v", key, err)
	}
	return res.GetKv()
}

func TestWrites_MapResultsToResponses(t *testing.T) {
	f := newFixture(t, kvtest.Options{})
	ctx := tctx(t)

	kv := f.put(t, "a", "1")
	if kv.GetVersion() != 1 || kv.GetModIndex() == 0 || string(kv.GetValue()) != "1" {
		t.Fatalf("Put = %v", kv)
	}
	_, err := f.kv.Put(ctx, &pb.PutRequest{Key: "a", Value: []byte("2"), ExpectedVersion: proto.Uint64(7)})
	if md := wantErr(t, err, codes.Aborted, "CONFLICT", "VERSION_MISMATCH"); md["current_version"] != "1" {
		t.Fatalf("current_version = %q", md["current_version"])
	}
	_, err = f.kv.Put(ctx, &pb.PutRequest{Key: "absent", Value: []byte("x"), ExpectedVersion: proto.Uint64(3)})
	if md := wantErr(t, err, codes.Aborted, "CONFLICT", "KEY_NOT_FOUND"); md["current_version"] != "0" {
		t.Fatalf("current_version = %q", md["current_version"])
	}
	_, err = f.kv.Put(ctx, &pb.PutRequest{Key: "", Value: []byte("x")})
	wantErr(t, err, codes.InvalidArgument, "INVALID_ARGUMENT", "INVALID_ARGUMENT")
	_, err = f.kv.Put(ctx, &pb.PutRequest{Key: "b", Value: []byte("x"), LeaseId: 99})
	wantErr(t, err, codes.NotFound, "NOT_FOUND", "LEASE_NOT_FOUND")

	cas, err := f.kv.CAS(ctx, &pb.CASRequest{Key: "a", ExpectedVersion: 1, Action: &pb.CASRequest_Value{Value: []byte("3")}})
	if err != nil || !cas.GetSucceeded() || cas.GetKv().GetVersion() != 2 {
		t.Fatalf("CAS success = %v, %v", cas, err)
	}
	cas, err = f.kv.CAS(ctx, &pb.CASRequest{Key: "a", ExpectedVersion: 1, Action: &pb.CASRequest_Value{Value: []byte("4")}})
	if err != nil || cas.GetSucceeded() || cas.GetKv().GetVersion() != 2 || string(cas.GetKv().GetValue()) != "3" {
		t.Fatalf("CAS mismatch = %v, %v", cas, err)
	}
	cas, err = f.kv.CAS(ctx, &pb.CASRequest{Key: "a", ExpectedVersion: 0, Action: &pb.CASRequest_Value{Value: []byte("5")}})
	if err != nil || cas.GetSucceeded() {
		t.Fatalf("CAS must-not-exist on a present key = %v, %v", cas, err)
	}
	cas, err = f.kv.CAS(ctx, &pb.CASRequest{Key: "new", ExpectedVersion: 0, Action: &pb.CASRequest_Value{Value: []byte("n")}})
	if err != nil || !cas.GetSucceeded() || cas.GetKv().GetVersion() != 1 {
		t.Fatalf("CAS must-not-exist = %v, %v", cas, err)
	}
	cas, err = f.kv.CAS(ctx, &pb.CASRequest{Key: "gone", ExpectedVersion: 4, Action: &pb.CASRequest_Value{Value: []byte("g")}})
	if err != nil || cas.GetSucceeded() || cas.GetKv() != nil {
		t.Fatalf("CAS on an absent key = %v, %v", cas, err)
	}
	cas, err = f.kv.CAS(ctx, &pb.CASRequest{Key: "new", ExpectedVersion: 1, Action: &pb.CASRequest_Delete{Delete: &pb.CASDelete{}}})
	if err != nil || !cas.GetSucceeded() {
		t.Fatalf("CAS delete = %v, %v", cas, err)
	}

	txn, err := f.kv.Txn(ctx, &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: "a", Target: pb.Compare_TARGET_VERSION, Result: pb.Compare_RESULT_EQUAL, Operand: &pb.Compare_Version{Version: 2}}},
		Success: []*pb.Op{{Op: &pb.Op_Put{Put: &pb.PutOp{Key: "t", Value: []byte("yes")}}}, {Op: &pb.Op_Get{Get: &pb.GetOp{Key: "a"}}}},
		Failure: []*pb.Op{{Op: &pb.Op_Put{Put: &pb.PutOp{Key: "t", Value: []byte("no")}}}},
	})
	if err != nil || !txn.GetSucceeded() || len(txn.GetResults()) != 2 || string(txn.GetResults()[1].GetGet().GetValue()) != "3" {
		t.Fatalf("Txn success = %v, %v", txn, err)
	}
	txn, err = f.kv.Txn(ctx, &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: "a", Target: pb.Compare_TARGET_VERSION, Result: pb.Compare_RESULT_EQUAL, Operand: &pb.Compare_Version{Version: 9}}},
		Failure: []*pb.Op{{Op: &pb.Op_Delete{Delete: &pb.DeleteOp{Key: "t"}}}},
	})
	if err != nil || txn.GetSucceeded() || !txn.GetResults()[0].GetDeleted() {
		t.Fatalf("Txn failure branch = %v, %v", txn, err)
	}
	var many []*pb.Op
	for i := range 9 {
		many = append(many, &pb.Op{Op: &pb.Op_Delete{Delete: &pb.DeleteOp{Key: fmt.Sprint(i)}}})
	}
	_, err = f.kv.Txn(ctx, &pb.TxnRequest{Success: many})
	wantErr(t, err, codes.InvalidArgument, "INVALID_ARGUMENT", "TXN_TOO_LARGE")

	del, err := f.kv.Delete(ctx, &pb.DeleteRequest{Key: "a"})
	if err != nil || !del.GetDeleted() {
		t.Fatalf("Delete = %v, %v", del, err)
	}
	del, err = f.kv.Delete(ctx, &pb.DeleteRequest{Key: "a"})
	if err != nil || del.GetDeleted() {
		t.Fatalf("Delete absent = %v, %v", del, err)
	}

	g, err := f.kv.LeaseGrant(ctx, &pb.LeaseGrantRequest{TtlMs: 60_000, Owner: "w1"})
	if err != nil || g.GetLeaseId() == 0 || g.GetFencingToken() == 0 || g.GetTtlMs() != 60_000 {
		t.Fatalf("LeaseGrant = %v, %v", g, err)
	}
	_, err = f.kv.LeaseGrant(ctx, &pb.LeaseGrantRequest{TtlMs: 0})
	wantErr(t, err, codes.InvalidArgument, "INVALID_ARGUMENT", "INVALID_ARGUMENT")
	ka, err := f.kv.LeaseKeepAlive(ctx, &pb.LeaseKeepAliveRequest{LeaseId: g.GetLeaseId(), FencingToken: g.GetFencingToken()})
	if err != nil || ka.GetLeaseId() != g.GetLeaseId() || ka.GetTtlMs() != 60_000 {
		t.Fatalf("LeaseKeepAlive = %v, %v", ka, err)
	}
	_, err = f.kv.LeaseKeepAlive(ctx, &pb.LeaseKeepAliveRequest{LeaseId: g.GetLeaseId(), FencingToken: g.GetFencingToken() + 1})
	wantErr(t, err, codes.NotFound, "NOT_FOUND", "LEASE_NOT_FOUND")
	if _, err := f.kv.LeaseRevoke(ctx, &pb.LeaseRevokeRequest{LeaseId: g.GetLeaseId()}); err != nil {
		t.Fatalf("LeaseRevoke: %v", err)
	}
	_, err = f.kv.LeaseRevoke(ctx, &pb.LeaseRevokeRequest{LeaseId: g.GetLeaseId()})
	wantErr(t, err, codes.NotFound, "NOT_FOUND", "LEASE_NOT_FOUND")

	for want := uint64(1); want <= 2; want++ {
		c, err := f.kv.IncrCounter(ctx, &pb.IncrCounterRequest{Name: "n"})
		if err != nil || c.GetValue() != want {
			t.Fatalf("IncrCounter = %v, %v; want %d", c, err, want)
		}
	}
	_, err = f.kv.IncrCounter(ctx, &pb.IncrCounterRequest{Name: ""})
	wantErr(t, err, codes.InvalidArgument, "INVALID_ARGUMENT", "")
}

// TestWrites_RequestIDRetryAppliesOnce: a retry with the same (client_id, request_id) returns the
// original result and changes nothing (FSM dedupe, 00E §5.1).
func TestWrites_RequestIDRetryAppliesOnce(t *testing.T) {
	f := newFixture(t, kvtest.Options{})
	ctx := tctx(t)
	req := &pb.PutRequest{Meta: meta("c1", "r1"), Key: "k", Value: []byte("v")}
	first, err := f.kv.Put(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.kv.Put(ctx, req)
	if err != nil || !proto.Equal(first, again) {
		t.Fatalf("retry = %v, %v; want %v", again, err, first)
	}
	if kv, _ := f.c.Member(f.lead).FSM.Get("k"); kv.GetVersion() != 1 {
		t.Fatalf("version after retry = %d, want 1 (applied once)", kv.GetVersion())
	}
	for range 2 {
		c, err := f.kv.IncrCounter(ctx, &pb.IncrCounterRequest{Meta: meta("c1", "r2"), Name: "n"})
		if err != nil || c.GetValue() != 1 {
			t.Fatalf("IncrCounter retry = %v, %v", c, err)
		}
	}
	// A failed result is remembered too.
	bad := &pb.PutRequest{Meta: meta("c1", "r3"), Key: "k", Value: []byte("w"), ExpectedVersion: proto.Uint64(5)}
	for range 2 {
		_, err := f.kv.Put(ctx, bad)
		wantErr(t, err, codes.Aborted, "CONFLICT", "VERSION_MISMATCH")
	}
	// A request id without a client id can't be deduplicated.
	_, err = f.kv.Put(ctx, &pb.PutRequest{Meta: meta("", "r4"), Key: "k", Value: []byte("w")})
	wantErr(t, err, codes.InvalidArgument, "INVALID_ARGUMENT", "")
}

func TestFollower_RedirectsWritesAndLinearizableReads(t *testing.T) {
	f := newFixture(t, kvtest.Options{})
	f.put(t, "k", "v")
	follower := f.c.Others(f.lead)[0]
	kv := pb.NewKVClient(f.c.Conn(follower))
	ctx := tctx(t)

	_, err := kv.Put(ctx, &pb.PutRequest{Key: "k", Value: []byte("w")})
	if md := wantErr(t, err, codes.FailedPrecondition, "NOT_LEADER", ""); md["leader_hint"] != fmt.Sprint(f.lead) {
		t.Fatalf("leader_hint = %q, want %d", md["leader_hint"], f.lead)
	}
	_, err = kv.Get(ctx, &pb.GetRequest{Key: "k"})
	wantErr(t, err, codes.FailedPrecondition, "NOT_LEADER", "")

	// SERIALIZABLE is served locally once the follower has applied the write.
	var got *pb.GetResponse
	fftest.WaitFor(t, func() bool {
		got, err = kv.Get(ctx, &pb.GetRequest{Key: "k", Consistency: pb.Consistency_CONSISTENCY_SERIALIZABLE})
		return err == nil
	}, leaderWait)
	if !got.GetStale() || got.GetAppliedIndex() == 0 || got.GetLeaderNowMs() != 0 || string(got.GetKv().GetValue()) != "v" {
		t.Fatalf("serializable Get = %v", got)
	}
}

func TestReads_LinearizableResponseCarriesLeaderClock(t *testing.T) {
	clk := clock.NewFake(time.UnixMilli(1_000_000))
	f := newFixture(t, kvtest.Options{Clock: clk})
	f.put(t, "k", "v")
	got, err := f.kv.Get(tctx(t), &pb.GetRequest{Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStale() || got.GetLeaderNowMs() != 1_000_000 || got.GetAppliedIndex() < got.GetKv().GetModIndex() {
		t.Fatalf("linearizable Get = %v", got)
	}
	_, err = f.kv.Get(tctx(t), &pb.GetRequest{Key: "missing"})
	wantErr(t, err, codes.NotFound, "NOT_FOUND", "")
	_, err = f.kv.Get(tctx(t), &pb.GetRequest{Key: ""})
	wantErr(t, err, codes.InvalidArgument, "INVALID_ARGUMENT", "")
	_, err = f.kv.Get(tctx(t), &pb.GetRequest{Key: "k", Consistency: 9})
	wantErr(t, err, codes.InvalidArgument, "INVALID_ARGUMENT", "")
}

func TestList_PagesWithNextKey(t *testing.T) {
	f := newFixture(t, kvtest.Options{})
	for _, k := range []string{"w/a", "w/b", "w/c", "w/d", "x"} {
		f.put(t, k, k)
	}
	ctx := tctx(t)
	var keys []string
	req := &pb.ListRequest{Prefix: "w/", Limit: 3}
	for pages := 0; ; pages++ {
		res, err := f.kv.List(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		for _, kv := range res.GetKvs() {
			keys = append(keys, kv.GetKey())
		}
		if res.GetAppliedIndex() == 0 || res.GetStale() {
			t.Fatalf("List response = %v", res)
		}
		if !res.GetMore() {
			if res.GetNextKey() != "" {
				t.Fatalf("last page has next_key %q", res.GetNextKey())
			}
			break
		}
		if pages > 3 || res.GetNextKey() == "" {
			t.Fatalf("page %d: more without progress: %v", pages, res)
		}
		req.StartKey = res.GetNextKey()
	}
	if strings.Join(keys, ",") != "w/a,w/b,w/c,w/d" {
		t.Fatalf("listed %v", keys)
	}
	all, err := f.kv.List(ctx, &pb.ListRequest{Prefix: ""})
	if err != nil || len(all.GetKvs()) != 5 || all.GetMore() {
		t.Fatalf("List all = %v, %v", all, err)
	}
}

// TestLeases_ExpiryJudgedByLeaderClockAndSwept: on a fake leader clock a lease-bound key vanishes
// from reads at expires_at, is physically deleted by the sweeper's next tick, and keep-alive
// both prevents expiry and is refused once the leader's clock says the lease expired.
func TestLeases_ExpiryJudgedByLeaderClockAndSwept(t *testing.T) {
	clk := clock.NewFake(time.UnixMilli(1_000_000))
	// The first sweep is at t+10s, after the expiry checks.
	f := newFixture(t, kvtest.Options{Size: 1, Clock: clk, SweepInterval: 10 * time.Second})
	ctx := tctx(t)
	g, err := f.kv.LeaseGrant(ctx, &pb.LeaseGrantRequest{TtlMs: 1000, Owner: "w"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.kv.Put(ctx, &pb.PutRequest{Key: "bound", Value: []byte("v"), LeaseId: g.GetLeaseId()}); err != nil {
		t.Fatal(err)
	}
	long, err := f.kv.LeaseGrant(ctx, &pb.LeaseGrantRequest{TtlMs: 60_000, Owner: "w"}) // live at the t+10s sweep: must survive it
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(999 * time.Millisecond)
	if _, err := f.kv.Get(ctx, &pb.GetRequest{Key: "bound"}); err != nil {
		t.Fatalf("Get before expiry: %v", err)
	}
	// Keep-alive at t+999 extends to max(t+1000, t+999) + 1000 = t+2000.
	if _, err := f.kv.LeaseKeepAlive(ctx, &pb.LeaseKeepAliveRequest{LeaseId: g.GetLeaseId(), FencingToken: g.GetFencingToken()}); err != nil {
		t.Fatalf("keep-alive: %v", err)
	}
	clk.Advance(999 * time.Millisecond) // t+1998: alive thanks to the keep-alive
	if _, err := f.kv.Get(ctx, &pb.GetRequest{Key: "bound"}); err != nil {
		t.Fatalf("Get after keep-alive: %v", err)
	}
	fsm := f.c.Member(f.lead).FSM
	clk.Advance(2 * time.Millisecond) // t+2000 = expires_at
	_, err = f.kv.Get(ctx, &pb.GetRequest{Key: "bound"})
	wantErr(t, err, codes.NotFound, "NOT_FOUND", "")
	if list, err := f.kv.List(ctx, &pb.ListRequest{}); err != nil || len(list.GetKvs()) != 0 {
		t.Fatalf("List after expiry = %v, %v", list, err)
	}
	_, err = f.kv.LeaseKeepAlive(ctx, &pb.LeaseKeepAliveRequest{LeaseId: g.GetLeaseId(), FencingToken: g.GetFencingToken()})
	wantErr(t, err, codes.NotFound, "NOT_FOUND", "LEASE_EXPIRED")
	_, err = f.kv.Put(ctx, &pb.PutRequest{Key: "other", Value: []byte("v"), LeaseId: g.GetLeaseId()})
	wantErr(t, err, codes.NotFound, "NOT_FOUND", "LEASE_EXPIRED")
	if _, ok := fsm.Get("bound"); !ok {
		t.Fatal("key deleted before any sweep")
	}
	clk.Advance(8 * time.Second) // t+10s: the sweeper's first tick
	fftest.WaitFor(t, func() bool { _, ok := fsm.Get("bound"); return !ok }, leaderWait)
	if _, ok := fsm.GetLease(g.GetLeaseId()); ok {
		t.Fatal("lease still present after the sweep")
	}
	if _, ok := fsm.GetLease(long.GetLeaseId()); !ok {
		t.Fatal("the sweep deleted a live lease")
	}
	if n := testutil.ToFloat64(f.c.Member(f.lead).Metrics.LeasesSwept.WithLabelValues("expired")); n != 1 {
		t.Fatalf("swept leases counted %v, want 1", n)
	}
}

// TestSweeper_FollowsLeadership: only the leader sweeps, and a new leader takes over.
func TestSweeper_FollowsLeadership(t *testing.T) {
	clk := clock.NewFake(time.UnixMilli(1_000_000))
	f := newFixture(t, kvtest.Options{Clock: clk, SweepInterval: 500 * time.Millisecond})
	ctx := tctx(t)
	g, err := f.kv.LeaseGrant(ctx, &pb.LeaseGrantRequest{TtlMs: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.kv.Put(ctx, &pb.PutRequest{Key: "bound", Value: []byte("v"), LeaseId: g.GetLeaseId()}); err != nil {
		t.Fatal(err)
	}
	f.c.Kill(f.lead)
	lead := f.c.WaitLeader(leaderWait)
	clk.Advance(2 * time.Second)
	fsm := f.c.Member(lead).FSM
	fftest.WaitFor(t, func() bool {
		clk.Advance(500 * time.Millisecond)
		_, ok := fsm.Get("bound")
		return !ok
	}, leaderWait)
	for _, id := range f.c.Others(f.lead, lead) {
		fftest.WaitFor(t, func() bool { _, ok := f.c.Member(id).FSM.Get("bound"); return !ok }, leaderWait)
	}
}

// TestNoQuorum_WritesAndLinearizableReadsFailSerializableAnswers kills two of three members.
func TestNoQuorum_WritesAndLinearizableReadsFailSerializableAnswers(t *testing.T) {
	f := newFixture(t, kvtest.Options{ProposalTimeout: 300 * time.Millisecond})
	f.put(t, "k", "v")
	survivor := f.lead
	for _, id := range f.c.Others(survivor) {
		f.c.Kill(id)
	}
	ctx := tctx(t)
	// At once, while the survivor may still believe it leads (CheckQuorum steps it down within an
	// election timeout): a linearizable read must not be served from its unconfirmed state.
	if _, err := f.kv.Get(ctx, &pb.GetRequest{Key: "k"}); err == nil {
		t.Fatal("a leader without quorum served a linearizable read")
	}
	_, err := f.kv.Put(ctx, &pb.PutRequest{Key: "k", Value: []byte("w")})
	if c, r, _ := errInfo(t, err); c != codes.Unavailable || (r != "UNAVAILABLE" && r != "NO_QUORUM") {
		t.Fatalf("Put without quorum = %v", err)
	}
	_, err = f.kv.Get(ctx, &pb.GetRequest{Key: "k"})
	if c, _, _ := errInfo(t, err); c != codes.Unavailable && c != codes.FailedPrecondition {
		t.Fatalf("linearizable Get without quorum = %v", err)
	}
	got, err := f.kv.Get(ctx, &pb.GetRequest{Key: "k", Consistency: pb.Consistency_CONSISTENCY_SERIALIZABLE})
	if err != nil || !got.GetStale() || string(got.GetKv().GetValue()) != "v" {
		t.Fatalf("serializable Get on the survivor = %v, %v", got, err)
	}
}

// TestDeadlines_ClientDeadlineIsHonoured: a write whose client deadline ends while it can't
// commit returns DEADLINE_EXCEEDED, not the longer server-side timeout.
func TestDeadlines_ClientDeadlineIsHonoured(t *testing.T) {
	f := newFixture(t, kvtest.Options{ProposalTimeout: 5 * time.Second})
	for _, id := range f.c.Others(f.lead) {
		f.c.Kill(id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := f.kv.Put(ctx, &pb.PutRequest{Key: "k", Value: []byte("w")})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Put = %v, want DEADLINE_EXCEEDED", err)
	}
}

func TestCluster_StatusAndHealth(t *testing.T) {
	f := newFixture(t, kvtest.Options{})
	f.put(t, "k", "v")
	for _, id := range f.c.IDs() {
		cl := pb.NewClusterClient(f.c.Conn(id))
		st, err := cl.Status(tctx(t), &pb.StatusRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if st.GetNodeId() != id || st.GetLeaderId() != f.lead || st.GetTerm() == 0 || len(st.GetMembers()) != 3 {
			t.Fatalf("Status(%d) = %v", id, st)
		}
		h, err := cl.Health(tctx(t), &pb.HealthRequest{})
		if err != nil || !h.GetHealthy() || h.GetLeaderId() != f.lead {
			t.Fatalf("Health(%d) = %v, %v", id, h, err)
		}
	}
}

func TestMetrics_CountRequestsByMethodAndCode(t *testing.T) {
	f := newFixture(t, kvtest.Options{})
	f.put(t, "k", "v")
	_, err := f.kv.Get(tctx(t), &pb.GetRequest{Key: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatal(err)
	}
	m := f.c.Member(f.lead).Metrics
	if n := testutil.ToFloat64(m.Requests.WithLabelValues("Put", "OK")); n != 1 {
		t.Fatalf("Put OK requests = %v", n)
	}
	if n := testutil.ToFloat64(m.Requests.WithLabelValues("Get", "NotFound")); n != 1 {
		t.Fatalf("Get NotFound requests = %v", n)
	}
}

func TestReads_LeaderChangeDoesNotLoseAcknowledgedWrites(t *testing.T) {
	f := newFixture(t, kvtest.Options{})
	f.put(t, "k", "v1")
	f.c.Kill(f.lead)
	lead := f.c.WaitLeader(leaderWait)
	got, err := pb.NewKVClient(f.c.Conn(lead)).Get(tctx(t), &pb.GetRequest{Key: "k"})
	if err != nil || string(got.GetKv().GetValue()) != "v1" {
		t.Fatalf("Get on the new leader = %v, %v", got, err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// TestLeases_WritesTreatExpiredKeysAsAbsent: once the leader's clock says a key's lease expired,
// writes see the key as absent, as reads already do, even before a sweep: a CAS that "must not
// exist" right after a NOT_FOUND read succeeds (code review, 04.04). A keep-alive with the wrong
// token is LEASE_NOT_FOUND, not LEASE_EXPIRED.
func TestLeases_WritesTreatExpiredKeysAsAbsent(t *testing.T) {
	clk := clock.NewFake(time.UnixMilli(1_000_000))
	f := newFixture(t, kvtest.Options{Size: 1, Clock: clk, SweepInterval: time.Hour})
	ctx := tctx(t)
	g, err := f.kv.LeaseGrant(ctx, &pb.LeaseGrantRequest{TtlMs: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"lock", "txn", "del"} {
		if _, err := f.kv.Put(ctx, &pb.PutRequest{Key: k, Value: []byte("old"), LeaseId: g.GetLeaseId()}); err != nil {
			t.Fatal(err)
		}
	}
	clk.Advance(time.Second) // expired, not swept
	_, err = f.kv.LeaseKeepAlive(ctx, &pb.LeaseKeepAliveRequest{LeaseId: g.GetLeaseId(), FencingToken: g.GetFencingToken() + 1})
	wantErr(t, err, codes.NotFound, "NOT_FOUND", "LEASE_NOT_FOUND")
	_, err = f.kv.Get(ctx, &pb.GetRequest{Key: "lock"})
	wantErr(t, err, codes.NotFound, "NOT_FOUND", "")
	cas, err := f.kv.CAS(ctx, &pb.CASRequest{Key: "lock", ExpectedVersion: 0, Action: &pb.CASRequest_Value{Value: []byte("mine")}})
	if err != nil || !cas.GetSucceeded() || cas.GetKv().GetVersion() != 1 {
		t.Fatalf("CAS must-not-exist on an expired key = %v, %v", cas, err)
	}
	txn, err := f.kv.Txn(ctx, &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: "txn", Target: pb.Compare_TARGET_VERSION, Result: pb.Compare_RESULT_EQUAL, Operand: &pb.Compare_Version{Version: 0}}},
		Success: []*pb.Op{{Op: &pb.Op_Put{Put: &pb.PutOp{Key: "txn", Value: []byte("new")}}}},
	})
	if err != nil || !txn.GetSucceeded() {
		t.Fatalf("Txn version==0 on an expired key = %v, %v", txn, err)
	}
	// "lock" and "txn" were rewritten; "del" (same lease) went with the lease.
	if del, err := f.kv.Delete(ctx, &pb.DeleteRequest{Key: "del"}); err != nil || del.GetDeleted() {
		t.Fatalf("Delete of an expired key = %v, %v; want deleted=false", del, err)
	}
}
