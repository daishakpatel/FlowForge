package fsm_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/fsm"
	"github.com/daishakpatel/flowforge/internal/raftkv/fsmtest"
)

// FuzzApply: decoding and applying arbitrary bytes never panics, never returns a Go error, and
// is deterministic: two FSMs give equal results and equal state.
func FuzzApply(f *testing.F) {
	for _, c := range fsmtest.NewGenerator(1).Commands(64) {
		f.Add(c)
	}
	f.Add([]byte{})
	f.Add([]byte{0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		a, b := fsm.New(fsm.Options{}), fsm.New(fsm.Options{})
		ra, err := a.Apply(1, data)
		if err != nil {
			t.Fatalf("Apply returned an error: %v", err)
		}
		rb, _ := b.Apply(1, data)
		if ra.GetCode() == pb.ResultCode_RESULT_CODE_UNSPECIFIED || !proto.Equal(ra, rb) || a.StateHash() != b.StateHash() {
			t.Fatalf("non-deterministic or unset result: %v vs %v", ra, rb)
		}
		// The same bytes as a second entry, after a populated state, must not panic either.
		if _, err := a.Apply(2, data); err != nil {
			t.Fatal(err)
		}
	})
}

// FuzzRestore: Restore never panics on arbitrary bytes, and whatever it accepts round-trips.
func FuzzRestore(f *testing.F) {
	g := fsm.New(fsm.Options{})
	for i, c := range fsmtest.NewGenerator(2).Commands(200) {
		_, _ = g.Apply(uint64(i)+1, c) //nolint:gosec // i ≥ 0
	}
	snap, _ := g.Snapshot()
	f.Add(snap)
	f.Add([]byte{1})
	f.Fuzz(func(t *testing.T, data []byte) {
		x := fsm.New(fsm.Options{})
		if err := x.Restore(data); err != nil {
			return
		}
		out, err := x.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot after an accepted Restore: %v", err)
		}
		y := fsm.New(fsm.Options{})
		if err := y.Restore(out); err != nil || y.StateHash() != x.StateHash() {
			t.Fatalf("accepted state does not round-trip: %v", err)
		}
	})
}
