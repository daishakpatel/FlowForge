package fsm_test

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/fsm"
	"github.com/daishakpatel/flowforge/internal/raftkv/fsmtest"
)

// populated returns an FSM with keys, leases, counters and dedupe entries.
func populated(t *testing.T, n int) *fsm.FSM {
	t.Helper()
	f := fsm.New(fsm.Options{})
	for i, c := range fsmtest.NewGenerator(7).Commands(n) {
		if _, err := f.Apply(uint64(i)+1, c); err != nil { //nolint:gosec // i ≥ 0
			t.Fatal(err)
		}
	}
	return f
}

func TestSnapshot_RoundTripsByteForByte(t *testing.T) {
	f := populated(t, 3000)
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// Map iteration is random in Go: repeated snapshots of one state must still be identical.
	for range 20 {
		again, _ := f.Snapshot()
		if !bytes.Equal(again, snap) {
			t.Fatal("two snapshots of the same state differ")
		}
	}
	g := fsm.New(fsm.Options{})
	if err := g.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	back, _ := g.Snapshot()
	if !bytes.Equal(back, snap) || g.StateHash() != f.StateHash() || g.AppliedIndex() != f.AppliedIndex() {
		t.Fatal("restored state differs")
	}
	a, _ := f.List("", 0)
	b, _ := g.List("", 0)
	if len(a) == 0 || len(a) != len(b) {
		t.Fatalf("List sizes %d and %d", len(a), len(b))
	}
	for i := range a {
		if !proto.Equal(a[i], b[i]) {
			t.Fatalf("key %d differs: %v vs %v", i, a[i], b[i])
		}
	}
}

// TestRestore_RefusesOversizedInput. Apply can't build a state beyond MaxSnapshotBytes (the
// state-bytes quota, TestLimits_StateBytesKeepSnapshotsWithinLimit); Restore still refuses one.
func TestRestore_RefusesOversizedInput(t *testing.T) {
	huge := make([]byte, fsm.MaxSnapshotBytes+2)
	huge[0] = 1
	if err := fsm.New(fsm.Options{}).Restore(huge); !errors.Is(err, fsm.ErrSnapshotTooLarge) {
		t.Fatalf("Restore of > 64 MiB = %v, want ErrSnapshotTooLarge", err)
	}
}

func TestRestore_RejectsBadInputWithoutChangingState(t *testing.T) {
	f := populated(t, 500)
	good, _ := f.Snapshot()
	hash := f.StateHash()
	state := func(mut func(*pb.FSMState)) []byte {
		s := &pb.FSMState{}
		if err := proto.Unmarshal(good[1:], s); err != nil {
			t.Fatal(err)
		}
		mut(s)
		b, _ := proto.Marshal(s)
		return append([]byte{1}, b...)
	}
	for name, tc := range map[string]struct {
		data []byte
		want error
	}{
		"empty":           {nil, fsm.ErrCorruptSnapshot},
		"unknown version": {append([]byte{9}, good[1:]...), fsm.ErrSnapshotVersion},
		"truncated":       {good[:len(good)/2], fsm.ErrCorruptSnapshot},
		"garbage":         {append([]byte{1}, 0xff, 0xff, 0xff), fsm.ErrCorruptSnapshot},
		"unsorted keys": {state(func(s *pb.FSMState) {
			s.Keys[0], s.Keys[1] = s.Keys[1], s.Keys[0]
		}), fsm.ErrCorruptSnapshot},
		"key on an unknown lease": {state(func(s *pb.FSMState) {
			s.Keys[0].LeaseId = 1 << 40
		}), fsm.ErrCorruptSnapshot},
		"lease id beyond next": {state(func(s *pb.FSMState) {
			s.NextLeaseId = 0
		}), fsm.ErrCorruptSnapshot},
		"version zero": {state(func(s *pb.FSMState) {
			s.Keys[0].Version = 0
		}), fsm.ErrCorruptSnapshot},
		"modified after applied": {state(func(s *pb.FSMState) {
			s.Keys[0].ModIndex = s.LastApplied + 1
		}), fsm.ErrCorruptSnapshot},
		"duplicate dedupe": {state(func(s *pb.FSMState) {
			s.Dedupe = append(s.Dedupe, s.Dedupe[0])
		}), fsm.ErrCorruptSnapshot},
		"dedupe result without a code": {state(func(s *pb.FSMState) {
			s.Dedupe[0].Result = &pb.CommandResult{}
		}), fsm.ErrCorruptSnapshot},
		"dedupe beyond capacity": {state(func(s *pb.FSMState) {
			for i := range fsm.DefaultDedupeCapacity {
				s.Dedupe = append(s.Dedupe, &pb.StoredDedupe{ClientId: "x", RequestId: strconv.Itoa(i), Result: &pb.CommandResult{Code: pb.ResultCode_RESULT_CODE_OK}})
			}
		}), fsm.ErrCorruptSnapshot},
		"fencing tokens not increasing": {state(func(s *pb.FSMState) {
			s.Leases[1].FencingToken = s.Leases[0].GetFencingToken()
		}), fsm.ErrCorruptSnapshot},
	} {
		t.Run(name, func(t *testing.T) {
			if err := f.Restore(tc.data); !errors.Is(err, tc.want) {
				t.Fatalf("Restore = %v, want %v", err, tc.want)
			}
			if f.StateHash() != hash {
				t.Fatal("a failed Restore changed the state")
			}
		})
	}
	if s := state(func(*pb.FSMState) {}); len(s) == 0 || f.Restore(s) != nil {
		t.Fatal("the unmodified state must restore")
	}
}

// TestStateHashAt_PairsTheHashWithItsIndex: the hash and the applied index come from one read.
func TestStateHashAt_PairsTheHashWithItsIndex(t *testing.T) {
	f := populated(t, 7)
	h, idx := f.StateHashAt()
	if idx != f.AppliedIndex() || h != f.StateHash() || idx == 0 {
		t.Fatalf("StateHashAt = %x@%d; StateHash %x, applied %d", h, idx, f.StateHash(), f.AppliedIndex())
	}
}
