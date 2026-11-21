// Package fsmtest generates random replicated-state-machine commands for determinism and chaos
// tests (task 04.03; reused by 04.08). Commands are drawn from a small key, lease and request-id
// space so that they collide: CAS mismatches, lease expiry and dedupe hits all occur. Timestamps
// are deliberately not monotonic. A small share of entries is garbage bytes, which the FSM must
// turn into a deterministic INVALID_ARGUMENT result.
package fsmtest

import (
	"fmt"
	"math/rand"

	"google.golang.org/protobuf/proto"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// Generator yields commands from a seeded source: the same seed always yields the same sequence.
type Generator struct {
	rng *rand.Rand
}

// NewGenerator returns a Generator for seed.
func NewGenerator(seed int64) *Generator {
	return &Generator{rng: rand.New(rand.NewSource(seed))} //nolint:gosec // test data, not security
}

// Next returns the next encoded command (the data of one Raft entry).
func (g *Generator) Next() []byte {
	if g.rng.Intn(50) == 0 {
		b := make([]byte, g.rng.Intn(16))
		_, _ = g.rng.Read(b) // math/rand's Read never fails
		return b
	}
	cmd := &pb.Command{Meta: g.meta()}
	g.op(cmd)
	b, err := proto.Marshal(cmd)
	if err != nil {
		panic(fmt.Sprintf("fsmtest: marshal: %v", err)) // a generated message always marshals
	}
	return b
}

// Commands returns the next n commands.
func (g *Generator) Commands(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = g.Next()
	}
	return out
}

func (g *Generator) meta() *pb.CommandMeta {
	m := &pb.CommandMeta{TsUnixMs: int64(g.rng.Intn(200_000)) - 10_000} // any order, some negative
	if g.rng.Intn(3) == 0 {
		m.ClientId = fmt.Sprint("c", g.rng.Intn(4))
		m.RequestId = fmt.Sprint("r", g.rng.Intn(400))
	}
	return m
}

func (g *Generator) key() string {
	return fmt.Sprintf("%s/%d", []string{"a", "b", "w"}[g.rng.Intn(3)], g.rng.Intn(40))
}
func (g *Generator) value() []byte      { return []byte(fmt.Sprint("v", g.rng.Intn(1000))) }
func (g *Generator) small(n int) uint64 { return uint64(g.rng.Intn(n)) } //nolint:gosec // non-negative

func (g *Generator) op(c *pb.Command) {
	switch g.rng.Intn(14) {
	case 0, 1, 2:
		r := &pb.PutRequest{Key: g.key(), Value: g.value()}
		if g.rng.Intn(3) == 0 {
			v := g.small(6)
			r.ExpectedVersion = &v
		}
		if g.rng.Intn(3) == 0 {
			r.LeaseId = g.small(25)
		}
		c.Op = &pb.Command_Put{Put: r}
	case 3:
		r := &pb.DeleteRequest{Key: g.key()}
		if g.rng.Intn(2) == 0 {
			v := g.small(6)
			r.ExpectedVersion = &v
		}
		c.Op = &pb.Command_Delete{Delete: r}
	case 4, 5:
		r := &pb.CASRequest{Key: g.key(), ExpectedVersion: g.small(6)}
		if g.rng.Intn(4) == 0 {
			r.Action = &pb.CASRequest_Delete{Delete: &pb.CASDelete{}}
		} else {
			r.Action = &pb.CASRequest_Value{Value: g.value()}
		}
		c.Op = &pb.Command_Cas{Cas: r}
	case 6, 7:
		c.Op = &pb.Command_Txn{Txn: g.txn()}
	case 8:
		c.Op = &pb.Command_LeaseGrant{LeaseGrant: &pb.LeaseGrantRequest{TtlMs: 1 + g.small(30_000), Owner: "o"}}
	case 9:
		c.Op = &pb.Command_LeaseKeepAlive{LeaseKeepAlive: &pb.LeaseKeepAliveRequest{LeaseId: g.small(25), FencingToken: g.small(25)}}
	case 10:
		c.Op = &pb.Command_LeaseRevoke{LeaseRevoke: &pb.LeaseRevokeRequest{LeaseId: g.small(25)}}
	case 11:
		c.Op = &pb.Command_LeaseExpire{LeaseExpire: &pb.LeaseExpire{LeaseId: g.small(25)}}
	case 12:
		c.Op = &pb.Command_IncrCounter{IncrCounter: &pb.IncrCounterRequest{Name: fmt.Sprint("n", g.rng.Intn(5))}}
	default:
		c.Op = &pb.Command_Noop{Noop: &pb.NoOp{}}
	}
}

func (g *Generator) txn() *pb.TxnRequest {
	t := &pb.TxnRequest{}
	for range g.rng.Intn(4) {
		t.Compare = append(t.Compare, &pb.Compare{
			Key: g.key(), Target: pb.Compare_TARGET_VERSION,
			Result:  pb.Compare_Result(1 + g.rng.Intn(4)), //nolint:gosec // 1..4
			Operand: &pb.Compare_Version{Version: g.small(6)},
		})
	}
	ops := func() []*pb.Op {
		var out []*pb.Op
		for range g.rng.Intn(5) {
			switch g.rng.Intn(3) {
			case 0:
				out = append(out, &pb.Op{Op: &pb.Op_Put{Put: &pb.PutOp{Key: g.key(), Value: g.value()}}})
			case 1:
				out = append(out, &pb.Op{Op: &pb.Op_Delete{Delete: &pb.DeleteOp{Key: g.key()}}})
			default:
				out = append(out, &pb.Op{Op: &pb.Op_Get{Get: &pb.GetOp{Key: g.key()}}})
			}
		}
		return out
	}
	t.Success, t.Failure = ops(), ops()
	return t
}
