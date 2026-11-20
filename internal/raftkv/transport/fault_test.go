package transport

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// sink records what a Fault passes on, as "from>to#index".
type sink struct {
	mu  sync.Mutex
	got []string
}

func (s *sink) Send(msgs []*pb.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range msgs {
		s.got = append(s.got, key(m))
	}
}

func (s *sink) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.got
	s.got = nil
	return g
}

func key(m *pb.Message) string {
	return fmt.Sprintf("%d>%d#%c", m.GetFrom(), m.GetTo(), 'a'+byte(m.GetIndex())) //nolint:gosec // test indexes are tiny
}

func batch(from, to uint64, n int) []*pb.Message {
	var ms []*pb.Message
	for i := range n {
		ms = append(ms, app(from, to, uint64(i)))
	}
	return ms
}

func TestFault_PassesThroughByDefault(t *testing.T) {
	s := &sink{}
	f := NewFault(s, 1, nil)
	f.Send(batch(1, 2, 3))
	if got := s.take(); !slices.Equal(got, []string{"1>2#a", "1>2#b", "1>2#c"}) {
		t.Errorf("got %v", got)
	}
}

func TestFault_PartitionDropsAcrossGroupsAndReportsSnapshots(t *testing.T) {
	s := &sink{}
	var dropped []string
	f := NewFault(s, 1, func(m *pb.Message) { dropped = append(dropped, key(m)) })
	f.Partition([]uint64{1}, []uint64{2, 3})
	f.Send(append(batch(1, 2, 1), batch(2, 3, 1)...))
	snap := snapMsg(1)
	f.Send([]*pb.Message{snap})
	if got := s.take(); !slices.Equal(got, []string{"2>3#a"}) {
		t.Errorf("delivered %v, want only 2>3", got)
	}
	if !slices.Equal(dropped, []string{"1>2#a", key(snap)}) {
		t.Errorf("onDrop saw %v", dropped)
	}
	f.Heal()
	f.Send(batch(1, 2, 1))
	if got := s.take(); len(got) != 1 {
		t.Errorf("after Heal delivered %v", got)
	}
}

func TestFault_DelayHoldsUntilTicks(t *testing.T) {
	s := &sink{}
	f := NewFault(s, 1, nil)
	f.SetLink(Link{From: 1, To: 2}, Faults{Delay: 1, MaxDelayTicks: 3})
	f.Send(batch(1, 2, 5))
	if got := s.take(); len(got) != 0 {
		t.Fatalf("delayed messages delivered at once: %v", got)
	}
	var all []string
	for range 3 {
		f.Tick()
		all = append(all, s.take()...)
	}
	if len(all) != 5 {
		t.Errorf("after MaxDelayTicks ticks delivered %d of 5: %v", len(all), all)
	}
	f.Send(batch(2, 1, 1)) // the reverse link has no faults
	if got := s.take(); len(got) != 1 {
		t.Errorf("reverse link delivered %v", got)
	}
}

func TestFault_DuplicateDeliversIndependentCopies(t *testing.T) {
	var got []*pb.Message
	f := NewFault(senderFunc(func(ms []*pb.Message) { got = append(got, ms...) }), 1, nil)
	f.SetDefault(Faults{Duplicate: 1})
	m := app(1, 2, 0)
	f.Send([]*pb.Message{m})
	if len(got) != 2 || got[0] == got[1] || got[0] == m || !proto.Equal(got[0], got[1]) {
		t.Fatalf("got %v: want two equal, distinct copies", got)
	}
}

func TestFault_SeededRunsAreIdentical(t *testing.T) {
	run := func(seed int64) []string {
		s := &sink{}
		f := NewFault(s, seed, nil)
		f.SetDefault(Faults{Drop: 0.2, Duplicate: 0.2, Delay: 0.3, MaxDelayTicks: 4, Reorder: 0.5})
		var out []string
		for i := range 50 {
			f.Send(batch(uint64(1+i%3), uint64(1+(i+1)%3), 4))
			f.Tick()
			out = append(out, s.take()...)
		}
		return out
	}
	a, b, c := run(42), run(42), run(43)
	if !slices.Equal(a, b) {
		t.Error("same seed, different deliveries")
	}
	if slices.Equal(a, c) {
		t.Error("different seeds, identical deliveries")
	}
	if len(a) == 0 || len(a) == 200 {
		t.Errorf("faults had no effect: %d of 200 delivered", len(a))
	}
}

type senderFunc func([]*pb.Message)

func (f senderFunc) Send(ms []*pb.Message) { f(ms) }
