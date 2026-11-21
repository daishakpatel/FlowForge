package fsm

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// Applier adapts an FSM to the Raft node's Applier interface (04.02's node.Applier; the
// compile-time assertion is in applier_test.go, so this package doesn't import the node).
// Apply's result bytes are the encoded CommandResult the proposer receives as node.Result.Data.
type Applier struct{ f *FSM }

// NewApplier returns the adapter for f.
func NewApplier(f *FSM) *Applier { return &Applier{f: f} }

// Apply implements node.Applier: it applies the entry and encodes its CommandResult.
func (a *Applier) Apply(entry []byte, index uint64) ([]byte, error) {
	res, err := a.f.Apply(index, entry)
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(res)
	if err != nil {
		return nil, fmt.Errorf("encode command result: %w", err)
	}
	return b, nil
}

// Snapshot implements node.Applier.
func (a *Applier) Snapshot() ([]byte, error) { return a.f.Snapshot() }

// Restore implements node.Applier.
func (a *Applier) Restore(data []byte) error { return a.f.Restore(data) }
