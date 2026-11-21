package fsm_test

import (
	"math/rand"
	"strconv"
	"testing"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/fsm"
	"github.com/daishakpatel/flowforge/internal/raftkv/fsmtest"
)

// TestDeterminism_ReplicasConverge is the FSM's central property (ADR-005, FR-RAFT-02): replicas
// applying the same log reach byte-identical state. Two FSMs apply 50 000 random commands (with
// non-monotonic timestamps, garbage entries and dedupe collisions) and must hash equal at every
// 1 000 entries; replicas restored from snapshots taken at random points must converge with them.
func TestDeterminism_ReplicasConverge(t *testing.T) {
	const commands, checkEvery = 50_000, 1_000
	for _, seed := range []int64{1, 2, 3, 4, 5} {
		t.Run("seed="+strconv.FormatInt(seed, 10), func(t *testing.T) {
			log := fsmtest.NewGenerator(seed).Commands(commands)
			rng := rand.New(rand.NewSource(seed)) //nolint:gosec // test randomness
			restoreAt := map[int]bool{rng.Intn(commands): true, rng.Intn(commands): true, rng.Intn(commands): true}
			a, b := fsm.New(fsm.Options{}), fsm.New(fsm.Options{})
			var restored []*fsm.FSM
			seen := map[pb.ResultCode]int{}
			for i, data := range log {
				idx := uint64(i) + 1 //nolint:gosec // i ≥ 0
				ra, errA := a.Apply(idx, data)
				rb, errB := b.Apply(idx, data)
				if errA != nil || errB != nil || ra.GetCode() != rb.GetCode() {
					t.Fatalf("seed %d entry %d: results %v / %v, errors %v / %v", seed, idx, ra, rb, errA, errB)
				}
				seen[ra.GetCode()]++
				for _, c := range restored {
					if _, err := c.Apply(idx, data); err != nil {
						t.Fatalf("seed %d entry %d on a restored replica: %v", seed, idx, err)
					}
				}
				if restoreAt[i] {
					snap, err := a.Snapshot()
					if err != nil {
						t.Fatal(err)
					}
					c := fsm.New(fsm.Options{})
					if err := c.Restore(snap); err != nil {
						t.Fatal(err)
					}
					restored = append(restored, c)
				}
				if idx%checkEvery == 0 {
					want := a.StateHash()
					if b.StateHash() != want {
						t.Fatalf("seed %d: replicas diverge at entry %d", seed, idx)
					}
					for j, c := range restored {
						if c.StateHash() != want {
							t.Fatalf("seed %d: restored replica %d diverges at entry %d", seed, j, idx)
						}
					}
				}
			}
			if len(restored) == 0 {
				t.Fatal("no snapshot was restored")
			}
			// The log must reach the interesting outcomes, not only OK.
			for _, c := range []pb.ResultCode{pb.ResultCode_RESULT_CODE_OK, pb.ResultCode_RESULT_CODE_VERSION_MISMATCH,
				pb.ResultCode_RESULT_CODE_KEY_NOT_FOUND, pb.ResultCode_RESULT_CODE_LEASE_NOT_FOUND,
				pb.ResultCode_RESULT_CODE_LEASE_EXPIRED, pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT} {
				if seen[c] == 0 {
					t.Errorf("seed %d: no %v result in %d commands (seen %v)", seed, c, commands, seen)
				}
			}
			if keys, _ := a.List("", 0); len(keys) == 0 {
				t.Fatal("the generated log left no keys: the test exercises nothing")
			}
		})
	}
}
