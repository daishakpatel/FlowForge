package transport

import (
	"math/rand"
	"sync"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Sender is anything that sends Raft messages: GRPC, an in-process network, or another Fault.
type Sender interface {
	Send(msgs []*pb.Message)
}

// Link is the direction from one node to another.
type Link struct{ From, To uint64 }

// Faults are per-message probabilities (0..1) applied on a link. Delay holds a message for 1 to
// MaxDelayTicks ticks of the Fault (Tick). Reorder shuffles the batch a Send forwards; held
// messages released by Tick are shuffled with the default Faults' Reorder, whatever their link.
type Faults struct {
	Drop, Duplicate, Delay, Reorder float64
	MaxDelayTicks                   int
}

// Fault wraps a Sender and injects faults (ADR-019): drops, delays, duplicates, reordering and
// partitions, per link or by default. It has no build tag; only tests and chaos tooling create
// one. With the same seed and the same sequence of calls it makes the same decisions
// (deterministic mode). Delays count Tick calls, not wall time, so a harness that ticks the
// cluster ticks the Fault too.
//
// Send runs on the sending node's loop; Fault forwards synchronously and never blocks beyond
// its own lock. A dropped MsgSnap is passed to onDrop, so the owner can report the failure to
// the sending node (raft waits for a snapshot report).
type Fault struct {
	next   Sender
	onDrop func(*pb.Message)

	mu     sync.Mutex
	rng    *rand.Rand
	def    Faults
	links  map[Link]Faults
	groups map[uint64]int // nil: no partition
	held   []held
	now    int
}

type held struct {
	m   *pb.Message
	due int
}

// NewFault wraps next with no faults configured. onDrop may be nil.
func NewFault(next Sender, seed int64, onDrop func(*pb.Message)) *Fault {
	return &Fault{
		next: next, onDrop: onDrop,
		rng:   rand.New(rand.NewSource(seed)), //nolint:gosec // fault injection, not security
		links: map[Link]Faults{},
	}
}

// SetDefault sets the faults of every link without its own.
func (f *Fault) SetDefault(x Faults) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.def = x
}

// SetLink sets the faults of one direction.
func (f *Fault) SetLink(l Link, x Faults) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.links[l] = x
}

// Partition splits the nodes into groups that can only reach their own group's members.
// A node listed in no group reaches nobody.
func (f *Fault) Partition(groups ...[]uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groups = map[uint64]int{}
	for i, g := range groups {
		for _, id := range g {
			f.groups[id] = i + 1
		}
	}
}

// Heal removes the partition and every configured fault. Held messages are still delivered on
// later ticks.
func (f *Fault) Heal() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groups, f.def, f.links = nil, Faults{}, map[Link]Faults{}
}

// Send applies the faults to msgs and forwards what survives.
func (f *Fault) Send(msgs []*pb.Message) {
	f.mu.Lock()
	var out, dropped []*pb.Message
	for _, m := range msgs {
		x, ok := f.links[Link{m.GetFrom(), m.GetTo()}]
		if !ok {
			x = f.def
		}
		if f.cut(m) || f.rng.Float64() < x.Drop {
			dropped = append(dropped, m)
			continue
		}
		copies := []*pb.Message{m}
		if f.rng.Float64() < x.Duplicate {
			copies = []*pb.Message{clone(m), clone(m)}
		}
		for _, c := range copies {
			if x.MaxDelayTicks > 0 && f.rng.Float64() < x.Delay {
				f.held = append(f.held, held{m: clone(c), due: f.now + 1 + f.rng.Intn(x.MaxDelayTicks)})
				continue
			}
			out = append(out, c)
		}
		if f.rng.Float64() < x.Reorder {
			f.shuffle(out)
		}
	}
	f.mu.Unlock()
	f.forward(out, dropped)
}

// Tick advances the Fault's clock one tick and delivers the held messages now due, unless a
// partition made since cuts them off.
func (f *Fault) Tick() {
	f.mu.Lock()
	f.now++
	var out, dropped []*pb.Message
	keep := f.held[:0]
	for _, h := range f.held {
		switch {
		case h.due > f.now:
			keep = append(keep, h)
		case f.cut(h.m):
			dropped = append(dropped, h.m)
		default:
			out = append(out, h.m)
		}
	}
	f.held = keep
	if f.rng.Float64() < f.def.Reorder {
		f.shuffle(out)
	}
	f.mu.Unlock()
	f.forward(out, dropped)
}

func (f *Fault) forward(out, dropped []*pb.Message) {
	if len(out) > 0 {
		f.next.Send(out)
	}
	if f.onDrop != nil {
		for _, m := range dropped {
			f.onDrop(m)
		}
	}
}

// cut reports whether a partition separates the link. Callers hold mu.
func (f *Fault) cut(m *pb.Message) bool {
	if f.groups == nil {
		return false
	}
	g := f.groups[m.GetFrom()]
	return g == 0 || g != f.groups[m.GetTo()]
}

func (f *Fault) shuffle(ms []*pb.Message) {
	f.rng.Shuffle(len(ms), func(i, j int) { ms[i], ms[j] = ms[j], ms[i] })
}

func clone(m *pb.Message) *pb.Message { return proto.Clone(m).(*pb.Message) } //nolint:forcetypeassert // Clone keeps the type
