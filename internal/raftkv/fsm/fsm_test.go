package fsm_test

import (
	"bytes"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/fsm"
)

// h applies commands at increasing indexes.
type h struct {
	t   *testing.T
	f   *fsm.FSM
	idx uint64
}

func newH(t *testing.T) *h { return &h{t: t, f: fsm.New(fsm.Options{})} }

// apply applies one command at the next index with timestamp ts and identity meta.
func (x *h) apply(ts int64, meta *pb.CommandMeta, cmd *pb.Command) *pb.CommandResult {
	x.t.Helper()
	if meta == nil {
		meta = &pb.CommandMeta{}
	}
	meta.TsUnixMs = ts
	cmd.Meta = meta
	b, err := proto.Marshal(cmd)
	if err != nil {
		x.t.Fatal(err)
	}
	x.idx++
	res, err := x.f.Apply(x.idx, b)
	if err != nil {
		x.t.Fatalf("Apply(%d): %v", x.idx, err)
	}
	return res
}

func (x *h) put(key, value string, opts ...func(*pb.PutRequest)) *pb.CommandResult {
	x.t.Helper()
	r := &pb.PutRequest{Key: key, Value: []byte(value)}
	for _, o := range opts {
		o(r)
	}
	return x.apply(1000, nil, &pb.Command{Op: &pb.Command_Put{Put: r}})
}

func expect(v uint64) func(*pb.PutRequest)     { return func(r *pb.PutRequest) { r.ExpectedVersion = &v } }
func withLease(id uint64) func(*pb.PutRequest) { return func(r *pb.PutRequest) { r.LeaseId = id } }

func (x *h) del(key string, expected *uint64) *pb.CommandResult {
	x.t.Helper()
	return x.apply(1000, nil, &pb.Command{Op: &pb.Command_Delete{Delete: &pb.DeleteRequest{Key: key, ExpectedVersion: expected}}})
}

func (x *h) grant(ts int64, ttl uint64) *pb.CommandResult {
	x.t.Helper()
	return x.apply(ts, nil, &pb.Command{Op: &pb.Command_LeaseGrant{LeaseGrant: &pb.LeaseGrantRequest{TtlMs: ttl, Owner: "w1"}}})
}

func code(t *testing.T, r *pb.CommandResult, want pb.ResultCode) {
	t.Helper()
	if r.GetCode() != want {
		t.Fatalf("code = %v, want %v (result %v)", r.GetCode(), want, r)
	}
}

func u64(v uint64) *uint64 { return &v }

func TestPut_VersionsAndIndexes(t *testing.T) {
	x := newH(t)
	r := x.put("a", "1")
	code(t, r, pb.ResultCode_RESULT_CODE_OK)
	if kv := r.GetKv(); kv.GetVersion() != 1 || kv.GetModIndex() != 1 || string(kv.GetValue()) != "1" || r.GetPrev() != nil {
		t.Fatalf("first put: %v", r)
	}
	r = x.put("a", "2")
	if kv := r.GetKv(); kv.GetVersion() != 2 || kv.GetModIndex() != 2 || string(r.GetPrev().GetValue()) != "1" {
		t.Fatalf("second put: %v", r)
	}
	kv, ok := x.f.Get("a")
	if !ok || kv.GetVersion() != 2 || string(kv.GetValue()) != "2" {
		t.Fatalf("Get = %v, %v", kv, ok)
	}
	if x.f.AppliedIndex() != 2 {
		t.Errorf("AppliedIndex = %d", x.f.AppliedIndex())
	}
}

func TestPut_ExpectedVersion(t *testing.T) {
	x := newH(t)
	code(t, x.put("a", "1", expect(0)), pb.ResultCode_RESULT_CODE_OK) // must not exist
	r := x.put("a", "2", expect(0))
	code(t, r, pb.ResultCode_RESULT_CODE_VERSION_MISMATCH)
	if r.GetKv().GetVersion() != 1 {
		t.Errorf("mismatch carries current %v", r.GetKv())
	}
	code(t, x.put("a", "2", expect(1)), pb.ResultCode_RESULT_CODE_OK)
	code(t, x.put("b", "x", expect(3)), pb.ResultCode_RESULT_CODE_KEY_NOT_FOUND)
	if _, ok := x.f.Get("b"); ok {
		t.Error("failed put created the key")
	}
}

func TestDelete(t *testing.T) {
	x := newH(t)
	x.put("a", "1")
	code(t, x.del("a", u64(5)), pb.ResultCode_RESULT_CODE_VERSION_MISMATCH)
	r := x.del("a", u64(1))
	code(t, r, pb.ResultCode_RESULT_CODE_OK)
	if !r.GetDeleted() || string(r.GetPrev().GetValue()) != "1" {
		t.Fatalf("delete: %v", r)
	}
	if r := x.del("a", nil); r.GetCode() != pb.ResultCode_RESULT_CODE_OK || r.GetDeleted() {
		t.Fatalf("delete of absent key: %v", r)
	}
	code(t, x.del("a", u64(1)), pb.ResultCode_RESULT_CODE_KEY_NOT_FOUND)
	// Re-creating starts a new life: version 1.
	if r := x.put("a", "again"); r.GetKv().GetVersion() != 1 {
		t.Errorf("recreated version = %d", r.GetKv().GetVersion())
	}
}

func TestCAS(t *testing.T) {
	x := newH(t)
	cas := func(key string, expected uint64, value *string) *pb.CommandResult {
		req := &pb.CASRequest{Key: key, ExpectedVersion: expected}
		if value == nil {
			req.Action = &pb.CASRequest_Delete{Delete: &pb.CASDelete{}}
		} else {
			req.Action = &pb.CASRequest_Value{Value: []byte(*value)}
		}
		return x.apply(1000, nil, &pb.Command{Op: &pb.Command_Cas{Cas: req}})
	}
	v := "v1"
	code(t, cas("k", 0, &v), pb.ResultCode_RESULT_CODE_OK) // must-not-exist succeeds
	code(t, cas("k", 0, &v), pb.ResultCode_RESULT_CODE_VERSION_MISMATCH)
	v2 := "v2"
	if r := cas("k", 1, &v2); r.GetCode() != pb.ResultCode_RESULT_CODE_OK || r.GetKv().GetVersion() != 2 {
		t.Fatalf("cas at version 1: %v", r)
	}
	code(t, cas("k", 1, nil), pb.ResultCode_RESULT_CODE_VERSION_MISMATCH)
	if r := cas("k", 2, nil); !r.GetDeleted() {
		t.Fatalf("cas delete: %v", r)
	}
	code(t, cas("k", 2, nil), pb.ResultCode_RESULT_CODE_KEY_NOT_FOUND)
	r := x.apply(1000, nil, &pb.Command{Op: &pb.Command_Cas{Cas: &pb.CASRequest{Key: "k"}}}) // no action
	code(t, r, pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT)
}

func txn(cmp []*pb.Compare, success, failure []*pb.Op) *pb.Command {
	return &pb.Command{Op: &pb.Command_Txn{Txn: &pb.TxnRequest{Compare: cmp, Success: success, Failure: failure}}}
}

func cmpVersion(key string, r pb.Compare_Result, v uint64) *pb.Compare {
	return &pb.Compare{Key: key, Target: pb.Compare_TARGET_VERSION, Result: r, Operand: &pb.Compare_Version{Version: v}}
}

func opPut(key, value string) *pb.Op {
	return &pb.Op{Op: &pb.Op_Put{Put: &pb.PutOp{Key: key, Value: []byte(value)}}}
}
func opDel(key string) *pb.Op { return &pb.Op{Op: &pb.Op_Delete{Delete: &pb.DeleteOp{Key: key}}} }
func opGet(key string) *pb.Op { return &pb.Op{Op: &pb.Op_Get{Get: &pb.GetOp{Key: key}}} }

func TestTxn_Branches(t *testing.T) {
	x := newH(t)
	x.put("a", "1")
	x.put("b", "x")
	// All compares hold: success runs, and later ops see earlier writes.
	r := x.apply(1000, nil, txn(
		[]*pb.Compare{
			cmpVersion("a", pb.Compare_RESULT_EQUAL, 1),
			{Key: "b", Target: pb.Compare_TARGET_VALUE, Result: pb.Compare_RESULT_EQUAL, Operand: &pb.Compare_Value{Value: []byte("x")}},
			cmpVersion("missing", pb.Compare_RESULT_EQUAL, 0),
			{Key: "a", Target: pb.Compare_TARGET_LEASE, Result: pb.Compare_RESULT_EQUAL, Operand: &pb.Compare_LeaseId{LeaseId: 0}},
			cmpVersion("a", pb.Compare_RESULT_LESS, 2),
			cmpVersion("a", pb.Compare_RESULT_NOT_EQUAL, 7),
		},
		[]*pb.Op{opPut("a", "2"), opGet("a"), opDel("b"), opGet("b")},
		[]*pb.Op{opPut("never", "x")},
	))
	code(t, r, pb.ResultCode_RESULT_CODE_OK)
	if !r.GetSucceeded() || len(r.GetResults()) != 4 {
		t.Fatalf("txn: %v", r)
	}
	res := r.GetResults()
	if res[0].GetPut().GetVersion() != 2 || string(res[1].GetGet().GetValue()) != "2" || !res[2].GetDeleted() || res[3].GetResult() != nil {
		t.Errorf("op results: %v", res)
	}
	// A compare fails: the failure branch runs and success's writes do not happen.
	r = x.apply(1000, nil, txn(
		[]*pb.Compare{cmpVersion("a", pb.Compare_RESULT_GREATER, 5)},
		[]*pb.Op{opPut("s", "1")},
		[]*pb.Op{opPut("f", "1")},
	))
	if r.GetCode() != pb.ResultCode_RESULT_CODE_OK || r.GetSucceeded() {
		t.Fatalf("failing compare: %v", r)
	}
	if _, ok := x.f.Get("s"); ok {
		t.Error("success branch ran")
	}
	if _, ok := x.f.Get("f"); !ok {
		t.Error("failure branch did not run")
	}
}

func TestTxn_LimitsAndInvalid(t *testing.T) {
	x := newH(t)
	nine := make([]*pb.Op, 9)
	for i := range nine {
		nine[i] = opPut("k", "v")
	}
	code(t, x.apply(1000, nil, txn(nil, nine[:5], nine[:4])), pb.ResultCode_RESULT_CODE_TXN_TOO_LARGE)
	cmps := make([]*pb.Compare, 9)
	for i := range cmps {
		cmps[i] = cmpVersion("k", pb.Compare_RESULT_EQUAL, 0)
	}
	code(t, x.apply(1000, nil, txn(cmps, nil, nil)), pb.ResultCode_RESULT_CODE_TXN_TOO_LARGE)
	code(t, x.apply(1000, nil, txn(nil, nine[:8], nil)), pb.ResultCode_RESULT_CODE_OK) // 8 is allowed
	for name, c := range map[string]*pb.Compare{
		"no target":  {Key: "k", Result: pb.Compare_RESULT_EQUAL, Operand: &pb.Compare_Version{Version: 1}},
		"no result":  {Key: "k", Target: pb.Compare_TARGET_VERSION, Operand: &pb.Compare_Version{Version: 1}},
		"wrong type": {Key: "k", Target: pb.Compare_TARGET_VERSION, Result: pb.Compare_RESULT_EQUAL, Operand: &pb.Compare_Value{Value: []byte("x")}},
		"empty key":  cmpVersion("", pb.Compare_RESULT_EQUAL, 0),
		"no operand": {Key: "k", Target: pb.Compare_TARGET_LEASE, Result: pb.Compare_RESULT_EQUAL},
	} {
		t.Run(name, func(t *testing.T) {
			code(t, x.apply(1000, nil, txn([]*pb.Compare{c}, nil, nil)), pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT)
		})
	}
	// An invalid op anywhere in the branch rejects the whole txn before any write.
	r := x.apply(1000, nil, txn(nil, []*pb.Op{opPut("ok", "1"), opPut("", "bad")}, nil))
	code(t, r, pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT)
	if _, ok := x.f.Get("ok"); ok {
		t.Error("txn partially applied")
	}
	code(t, x.apply(1000, nil, txn(nil, []*pb.Op{{}}, nil)), pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT)
	code(t, x.apply(1000, nil, txn(nil, []*pb.Op{{Op: &pb.Op_Put{Put: &pb.PutOp{Key: "k", LeaseId: 77}}}}, nil)), pb.ResultCode_RESULT_CODE_LEASE_NOT_FOUND)
}

func TestLimits(t *testing.T) {
	x := newH(t)
	code(t, x.put("", "v"), pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT)
	code(t, x.put(strings.Repeat("k", fsm.MaxKeyBytes+1), "v"), pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT)
	code(t, x.put(strings.Repeat("k", fsm.MaxKeyBytes), "v"), pb.ResultCode_RESULT_CODE_OK)
	code(t, x.put("big", strings.Repeat("v", fsm.MaxValueBytes+1)), pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT)
	code(t, x.put("big", strings.Repeat("v", fsm.MaxValueBytes)), pb.ResultCode_RESULT_CODE_OK)
	r := x.apply(1000, &pb.CommandMeta{ClientId: strings.Repeat("c", fsm.MaxIDBytes+1), RequestId: "r"}, &pb.Command{Op: &pb.Command_Noop{Noop: &pb.NoOp{}}})
	code(t, r, pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT)
	code(t, x.apply(1000, nil, &pb.Command{}), pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT) // no op
}

func TestLimits_KeyQuota(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 100 000 keys")
	}
	x := newH(t)
	for i := range fsm.MaxKeys {
		if r := x.put("k"+strconv.Itoa(i), ""); r.GetCode() != pb.ResultCode_RESULT_CODE_OK {
			t.Fatalf("key %d: %v", i, r)
		}
	}
	code(t, x.put("one-too-many", ""), pb.ResultCode_RESULT_CODE_QUOTA_EXCEEDED)
	code(t, x.put("k7", "overwriting is fine"), pb.ResultCode_RESULT_CODE_OK)
	code(t, x.apply(1000, nil, txn(nil, []*pb.Op{opDel("k1"), opPut("fits-after-delete", "")}, nil)), pb.ResultCode_RESULT_CODE_OK)
	code(t, x.apply(1000, nil, txn(nil, []*pb.Op{opPut("a-new-1", ""), opDel("k2")}, nil)), pb.ResultCode_RESULT_CODE_QUOTA_EXCEEDED)
}

func TestLeases_Lifecycle(t *testing.T) {
	x := newH(t)
	g := x.grant(10_000, 5_000)
	code(t, g, pb.ResultCode_RESULT_CODE_OK)
	id := g.GetLeaseId()
	if g.GetExpiresAtMs() != 15_000 || g.GetFencingToken() != 1 || g.GetTtlMs() != 5_000 {
		t.Fatalf("grant: %v", g)
	}
	code(t, x.put("w/2", "x", withLease(id)), pb.ResultCode_RESULT_CODE_OK)
	code(t, x.put("w/1", "x", withLease(id)), pb.ResultCode_RESULT_CODE_OK)
	if l, ok := x.f.GetLease(id); !ok || !slices.Equal(l.Keys, []string{"w/1", "w/2"}) || l.Owner != "w1" || l.ExpiresAtMs != 15_000 {
		t.Fatalf("GetLease = %+v", l)
	}
	keep := func(ts int64, fencing uint64) *pb.CommandResult {
		return x.apply(ts, nil, &pb.Command{Op: &pb.Command_LeaseKeepAlive{LeaseKeepAlive: &pb.LeaseKeepAliveRequest{LeaseId: id, FencingToken: fencing}}})
	}
	// Keep-alive extends from max(expiry, ts) and never moves expiry backwards.
	if r := keep(12_000, 1); r.GetExpiresAtMs() != 20_000 {
		t.Fatalf("keep-alive at 12s: %v", r)
	}
	if r := keep(1_000, 1); r.GetExpiresAtMs() != 25_000 { // an old timestamp still extends from the expiry
		t.Fatalf("keep-alive with an old ts: %v", r)
	}
	code(t, keep(13_000, 99), pb.ResultCode_RESULT_CODE_LEASE_NOT_FOUND) // wrong fencing token
	// Expiry is judged by the reader's clock, never the FSM's.
	if x.f.IsExpired(id, 24_999) || !x.f.IsExpired(id, 25_000) || !x.f.IsExpired(999, 0) {
		t.Error("IsExpired")
	}
	if got := x.f.ExpiredLeaseIDs(25_000); !slices.Equal(got, []uint64{id}) {
		t.Errorf("ExpiredLeaseIDs = %v", got)
	}
	expire := func(ts int64) *pb.CommandResult {
		return x.apply(ts, nil, &pb.Command{Op: &pb.Command_LeaseExpire{LeaseExpire: &pb.LeaseExpire{LeaseId: id}}})
	}
	if r := expire(24_999); r.GetCode() != pb.ResultCode_RESULT_CODE_OK || r.GetDeleted() {
		t.Fatalf("early expire changed something: %v", r)
	}
	if _, ok := x.f.Get("w/1"); !ok {
		t.Fatal("early expire deleted a key")
	}
	if r := expire(25_000); !r.GetDeleted() {
		t.Fatalf("expire at expiry: %v", r)
	}
	if _, ok := x.f.Get("w/1"); ok {
		t.Error("expired lease's key survived")
	}
	if r := expire(30_000); r.GetCode() != pb.ResultCode_RESULT_CODE_OK || r.GetDeleted() { // idempotent
		t.Errorf("second expire: %v", r)
	}
	code(t, keep(26_000, 1), pb.ResultCode_RESULT_CODE_LEASE_NOT_FOUND)
}

func TestLeases_DeadLeasesRejectWrites(t *testing.T) {
	x := newH(t)
	code(t, x.put("k", "v", withLease(42)), pb.ResultCode_RESULT_CODE_LEASE_NOT_FOUND)
	id := x.grant(500, 600).GetLeaseId()                                  // expires at 1100
	code(t, x.put("k", "v", withLease(id)), pb.ResultCode_RESULT_CODE_OK) // h.put's ts is 1000
	r := x.apply(1_100, nil, &pb.Command{Op: &pb.Command_Put{Put: &pb.PutRequest{Key: "k2", LeaseId: id}}})
	code(t, r, pb.ResultCode_RESULT_CODE_LEASE_EXPIRED)
	// Revoke deletes attached keys; moving a key off a lease detaches it.
	id2 := x.grant(1_000, 10_000).GetLeaseId()
	x.put("a", "1", withLease(id2))
	x.put("b", "1", withLease(id2))
	x.put("b", "2") // detached
	revoke := &pb.Command{Op: &pb.Command_LeaseRevoke{LeaseRevoke: &pb.LeaseRevokeRequest{LeaseId: id2}}}
	code(t, x.apply(1_000, nil, revoke), pb.ResultCode_RESULT_CODE_OK)
	if _, ok := x.f.Get("a"); ok {
		t.Error("revoke kept an attached key")
	}
	if _, ok := x.f.Get("b"); !ok {
		t.Error("revoke deleted a detached key")
	}
	code(t, x.apply(1_000, nil, &pb.Command{Op: &pb.Command_LeaseRevoke{LeaseRevoke: &pb.LeaseRevokeRequest{LeaseId: id2}}}), pb.ResultCode_RESULT_CODE_LEASE_NOT_FOUND)
	for _, ttl := range []uint64{0, fsm.MaxLeaseTTLMs + 1} {
		code(t, x.grant(1_000, ttl), pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT)
	}
}

func TestLeases_FencingStrictlyIncreasesAcrossRestore(t *testing.T) {
	x := newH(t)
	var last uint64
	for i := range 5 {
		f := x.grant(int64(i), 1_000).GetFencingToken()
		if f <= last {
			t.Fatalf("fencing %d after %d", f, last)
		}
		last = f
	}
	snap, err := x.f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	y := &h{t: t, f: fsm.New(fsm.Options{}), idx: x.idx}
	if err := y.f.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if f := y.grant(0, 1_000).GetFencingToken(); f <= last {
		t.Fatalf("fencing %d after restore, last before %d", f, last)
	}
}

func TestCounters(t *testing.T) {
	x := newH(t)
	incr := func(name string) *pb.CommandResult {
		return x.apply(1000, nil, &pb.Command{Op: &pb.Command_IncrCounter{IncrCounter: &pb.IncrCounterRequest{Name: name}}})
	}
	for want := uint64(1); want <= 3; want++ {
		if r := incr("epoch"); r.GetCounter() != want {
			t.Fatalf("counter = %v, want %d", r, want)
		}
	}
	code(t, incr(""), pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT)
	x.put("", "") // a failed command in between
	if r := incr("epoch"); r.GetCounter() != 4 {
		t.Fatalf("after failed commands counter = %d, want 4", r.GetCounter())
	}
	if r := incr("other"); r.GetCounter() != 1 {
		t.Errorf("counters are independent: %v", r)
	}
}

func TestDedupe_ReturnsStoredResultWithoutReexecuting(t *testing.T) {
	x := newH(t)
	meta := func() *pb.CommandMeta { return &pb.CommandMeta{ClientId: "c", RequestId: "r1"} }
	incr := func() *pb.Command {
		return &pb.Command{Op: &pb.Command_IncrCounter{IncrCounter: &pb.IncrCounterRequest{Name: "n"}}}
	}
	first := x.apply(1000, meta(), incr())
	again := x.apply(2000, meta(), incr())
	if !proto.Equal(first, again) || again.GetCounter() != 1 {
		t.Fatalf("retry result %v, first %v", again, first)
	}
	// Only the applied index moved: same state as one incr followed by a no-op.
	y := newH(t)
	y.apply(1000, meta(), incr())
	y.apply(1, nil, &pb.Command{Op: &pb.Command_Noop{Noop: &pb.NoOp{}}})
	if x.f.StateHash() != y.f.StateHash() {
		t.Error("a deduplicated retry changed more than the applied index")
	}
	// A failed command's result is deduplicated too.
	bad := x.apply(1000, &pb.CommandMeta{ClientId: "c", RequestId: "bad"}, &pb.Command{Op: &pb.Command_Put{Put: &pb.PutRequest{}}})
	again = x.apply(1000, &pb.CommandMeta{ClientId: "c", RequestId: "bad"}, &pb.Command{Op: &pb.Command_Put{Put: &pb.PutRequest{Key: "now-valid"}}})
	if !proto.Equal(bad, again) {
		t.Errorf("retry of a failed request re-executed: %v", again)
	}
}

func TestDedupe_EvictsOldestFirstAndSurvivesRestore(t *testing.T) {
	x := &h{t: t, f: fsm.New(fsm.Options{DedupeCapacity: 3})}
	incr := func(req string) *pb.CommandResult {
		return x.apply(1000, &pb.CommandMeta{ClientId: "c", RequestId: req}, &pb.Command{Op: &pb.Command_IncrCounter{IncrCounter: &pb.IncrCounterRequest{Name: "n"}}})
	}
	for _, r := range []string{"r1", "r2", "r3", "r4"} { // r1 is evicted
		incr(r)
	}
	snap, err := x.f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := x.f.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if r := incr("r2"); r.GetCounter() != 2 {
		t.Errorf("r2 after restore = %d, want its stored 2", r.GetCounter())
	}
	if r := incr("r1"); r.GetCounter() != 5 {
		t.Errorf("evicted r1 re-executed to %d, want 5", r.GetCounter())
	}
	// r1 is now newest and evicted r2 (capacity 3: r3, r4, r1).
	if r := incr("r2"); r.GetCounter() != 6 {
		t.Errorf("r2 after eviction = %d, want 6", r.GetCounter())
	}
}

func TestApply_RefusesAppliedIndexes(t *testing.T) {
	x := newH(t)
	meta := &pb.CommandMeta{ClientId: "c", RequestId: "r", TsUnixMs: 1}
	b, _ := proto.Marshal(&pb.Command{Meta: meta, Op: &pb.Command_IncrCounter{IncrCounter: &pb.IncrCounterRequest{Name: "n"}}})
	first, err := x.f.Apply(5, b)
	if err != nil {
		t.Fatal(err)
	}
	hash := x.f.StateHash()
	// Re-delivery of an applied entry: the cached result when there is one, no state change.
	if again, err := x.f.Apply(5, b); err != nil || !proto.Equal(again, first) {
		t.Fatalf("re-delivery = %v, %v", again, err)
	}
	other, _ := proto.Marshal(&pb.Command{Op: &pb.Command_Noop{Noop: &pb.NoOp{}}})
	if _, err := x.f.Apply(4, other); !errors.Is(err, fsm.ErrAlreadyApplied) {
		t.Fatalf("Apply(4) = %v, want ErrAlreadyApplied", err)
	}
	if x.f.StateHash() != hash || x.f.AppliedIndex() != 5 {
		t.Error("a refused apply changed state")
	}
}

func TestApply_GarbageIsADeterministicResult(t *testing.T) {
	x := newH(t)
	r, err := x.f.Apply(1, []byte{0xff, 0x01, 0x02})
	if err != nil || r.GetCode() != pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT || x.f.AppliedIndex() != 1 {
		t.Fatalf("garbage: %v, %v, applied %d", r, err, x.f.AppliedIndex())
	}
}

func TestList_SortedPrefixAndLimit(t *testing.T) {
	x := newH(t)
	for _, k := range []string{"w/c", "a", "w/a", "w/b", "x"} {
		x.put(k, k)
	}
	kvs, more := x.f.List("w/", 2)
	if len(kvs) != 2 || kvs[0].GetKey() != "w/a" || kvs[1].GetKey() != "w/b" || !more {
		t.Fatalf("List = %v more=%v", kvs, more)
	}
	if kvs, more := x.f.List("w/", 0); len(kvs) != 3 || more {
		t.Fatalf("List unlimited = %v %v", kvs, more)
	}
	// Returned values are copies.
	kv, _ := x.f.Get("a")
	kv.Value[0] = 'Z'
	if again, _ := x.f.Get("a"); !bytes.Equal(again.GetValue(), []byte("a")) {
		t.Error("Get exposed internal state")
	}
}

// TestLimits_StateBytesKeepSnapshotsWithinLimit: the per-key limits alone allow far more state than
// a snapshot may hold, so a total-bytes quota refuses writes before the state outgrows a snapshot.
func TestLimits_StateBytesKeepSnapshotsWithinLimit(t *testing.T) {
	x := newH(t)
	value := strings.Repeat("v", fsm.MaxValueBytes)
	n := 0
	for ; n < 1000; n++ {
		if r := x.put("big/"+strconv.Itoa(n), value); r.GetCode() != pb.ResultCode_RESULT_CODE_OK {
			code(t, r, pb.ResultCode_RESULT_CODE_QUOTA_EXCEEDED)
			break
		}
	}
	if n == 1000 || n*fsm.MaxValueBytes > fsm.MaxStateBytes {
		t.Fatalf("quota hit after %d values", n)
	}
	code(t, x.put("big/0", "small"), pb.ResultCode_RESULT_CODE_OK) // shrinking is allowed
	code(t, x.put("big/new", strings.Repeat("v", fsm.MaxValueBytes)), pb.ResultCode_RESULT_CODE_OK)
	code(t, x.apply(1000, nil, txn(nil, []*pb.Op{opPut("t1", value), opPut("t2", value)}, nil)), pb.ResultCode_RESULT_CODE_QUOTA_EXCEEDED)
	snap, err := x.f.Snapshot()
	if err != nil || len(snap) > fsm.MaxSnapshotBytes {
		t.Fatalf("snapshot at the quota: %d bytes, %v", len(snap), err)
	}
}

// TestDedupe_ByteBudgetKeepsSnapshotsWithinLimit: remembered results hold values, so the table
// is bounded by bytes as well as entries; the oldest are evicted first.
func TestDedupe_ByteBudgetKeepsSnapshotsWithinLimit(t *testing.T) {
	x := newH(t)
	value := strings.Repeat("v", fsm.MaxValueBytes)
	for i := range 200 { // each result holds kv and prev: about 512 KiB
		meta := &pb.CommandMeta{ClientId: "c", RequestId: "r" + strconv.Itoa(i)}
		code(t, x.apply(1000, meta, &pb.Command{Op: &pb.Command_Put{Put: &pb.PutRequest{Key: "k" + strconv.Itoa(i%10), Value: []byte(value)}}}), pb.ResultCode_RESULT_CODE_OK)
	}
	snap, err := x.f.Snapshot()
	if err != nil || len(snap) > fsm.MaxSnapshotBytes {
		t.Fatalf("snapshot: %d bytes, %v", len(snap), err)
	}
	// r0 was evicted by bytes long before the 10 000-entry capacity: its retry runs again.
	r := x.apply(1000, &pb.CommandMeta{ClientId: "c", RequestId: "r0"}, &pb.Command{Op: &pb.Command_Put{Put: &pb.PutRequest{Key: "k0", Value: []byte("x")}}})
	if r.GetKv().GetVersion() != 21 {
		t.Errorf("evicted retry: %v, want a new write at version 21", r.GetKv())
	}
}

func TestTxn_ValueCompareOnAbsentKeyNeverHolds(t *testing.T) {
	x := newH(t)
	x.put("empty", "")
	for _, tc := range []struct {
		key  string
		res  pb.Compare_Result
		want bool
	}{
		{"empty", pb.Compare_RESULT_EQUAL, true},
		{"absent", pb.Compare_RESULT_EQUAL, false},
		{"absent", pb.Compare_RESULT_NOT_EQUAL, false},
		{"absent", pb.Compare_RESULT_LESS, false},
	} {
		c := &pb.Compare{Key: tc.key, Target: pb.Compare_TARGET_VALUE, Result: tc.res, Operand: &pb.Compare_Value{Value: []byte{}}}
		if r := x.apply(1000, nil, txn([]*pb.Compare{c}, nil, nil)); r.GetSucceeded() != tc.want {
			t.Errorf("value compare %s %v \"\" = %v, want %v", tc.key, tc.res, r.GetSucceeded(), tc.want)
		}
	}
}

func TestTxn_BothBranchesAreValidated(t *testing.T) {
	x := newH(t)
	holds := []*pb.Compare{cmpVersion("a", pb.Compare_RESULT_EQUAL, 0)}
	r := x.apply(1000, nil, txn(holds, []*pb.Op{opPut("a", "1")}, []*pb.Op{opPut("", "bad")}))
	code(t, r, pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT)
	if _, ok := x.f.Get("a"); ok {
		t.Error("txn with an invalid failure branch ran its success branch")
	}
}

func TestCAS_DeleteRejectsMeaninglessArguments(t *testing.T) {
	x := newH(t)
	x.put("k", "v")
	del := func(expected, lease uint64) *pb.CommandResult {
		return x.apply(1000, nil, &pb.Command{Op: &pb.Command_Cas{Cas: &pb.CASRequest{Key: "k", ExpectedVersion: expected, LeaseId: lease,
			Action: &pb.CASRequest_Delete{Delete: &pb.CASDelete{}}}}})
	}
	code(t, del(0, 0), pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT) // "delete if it doesn't exist"
	code(t, del(1, 5), pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT) // a lease means nothing on a delete
	if r := del(1, 0); !r.GetDeleted() {
		t.Fatalf("valid CAS delete: %v", r)
	}
}
