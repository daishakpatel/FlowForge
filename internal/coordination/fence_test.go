package coordination_test

import (
	"testing"

	"github.com/daishakpatel/flowforge/internal/coordination"
	"github.com/daishakpatel/flowforge/internal/scheduler"
)

type gateStub struct {
	leading bool
	epoch   int64
}

func (g gateStub) IsLeader() bool { return g.leading }
func (g gateStub) Epoch() int64   { return g.epoch }

func TestFenceFromLeader(t *testing.T) {
	if f, ok := coordination.FenceFromLeader(gateStub{leading: true, epoch: 7}); !ok || f.Epoch != 7 {
		t.Fatalf("leader fence = %+v, %v", f, ok)
	}
	if _, ok := coordination.FenceFromLeader(gateStub{epoch: 7}); ok {
		t.Fatal("a fence from a non-leader")
	}
	// The single-scheduler gate fences at the unfenced epoch 0 (00E §2).
	if f, ok := coordination.FenceFromLeader(scheduler.AlwaysLeader{}); !ok || f.Epoch != 0 {
		t.Fatalf("AlwaysLeader fence = %+v, %v", f, ok)
	}
}

func TestFence_Compare(t *testing.T) {
	cases := []struct {
		f, other   int64
		supersedes bool
	}{{5, 4, true}, {5, 5, false}, {4, 5, false}}
	for _, c := range cases {
		if got := (coordination.Fence{Epoch: c.f}).Supersedes(coordination.Fence{Epoch: c.other}); got != c.supersedes {
			t.Errorf("%d supersedes %d = %v", c.f, c.other, got)
		}
	}
	f := coordination.Fence{Epoch: 5}
	if !f.Current(5) || f.Current(6) || f.Current(4) {
		t.Error("Current must hold only for the stored epoch")
	}
}
