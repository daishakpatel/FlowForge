// Package porcupine checks Raft KV histories for linearizability with the Porcupine checker
// (task 04.04, ADR-019, 00C §11): a model of the KV's register semantics and a history recorder.
//
// The model treats each key as a register with a version (absent = version 0, every write +1,
// as the FSM's per-key version): Put writes, Get reads, CAS writes only at the expected version.
// A write whose outcome the client never learned (a timeout, a node killed mid-request) is
// recorded with Output.Unknown: it never returns, and the model lets it take effect once or not
// at all. Failed reads change nothing and are simply not recorded.
package porcupine

import (
	"cmp"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	pc "github.com/anishathalye/porcupine"

	"github.com/daishakpatel/flowforge/internal/clock"
)

// OpKind is the operation an Input performs.
type OpKind int

// The operations the model knows.
const (
	OpGet OpKind = iota
	OpPut
	OpCAS
)

// Input is an operation's call. Expected is CAS's expected version (0: the key must not exist).
type Input struct {
	Op       OpKind
	Key      string
	Value    string
	Expected uint64
}

// Output is an operation's result. Found, Value and Version describe a Get; Version is the
// written version for a Put or a successful CAS, and the current version for a failed CAS.
type Output struct {
	Unknown   bool // a write whose outcome is unknown
	Found     bool
	Value     string
	Version   uint64
	Succeeded bool
}

type register struct {
	value   string
	version uint64
}

// Model returns the KV register model, partitioned by key.
func Model() pc.Model {
	nm := pc.NondeterministicModel{
		Partition: func(h []pc.Operation) [][]pc.Operation {
			byKey := map[string][]pc.Operation{}
			for _, o := range h {
				k := o.Input.(Input).Key
				byKey[k] = append(byKey[k], o)
			}
			out := make([][]pc.Operation, 0, len(byKey))
			for _, k := range slices.Sorted(func(yield func(string) bool) {
				for k := range byKey {
					if !yield(k) {
						return
					}
				}
			}) {
				out = append(out, byKey[k])
			}
			return out
		},
		Init: func() []any { return []any{register{}} },
		Step: func(state, input, output any) []any {
			return step(state.(register), input.(Input), output.(Output))
		},
		Equal: func(a, b any) bool { return a.(register) == b.(register) },
		DescribeOperation: func(input, output any) string {
			in, out := input.(Input), output.(Output)
			switch {
			case out.Unknown:
				return fmt.Sprintf("%s → ?", describe(in))
			case in.Op == OpGet && !out.Found:
				return fmt.Sprintf("%s → absent", describe(in))
			case in.Op == OpGet:
				return fmt.Sprintf("%s → %q@%d", describe(in), out.Value, out.Version)
			case in.Op == OpCAS:
				return fmt.Sprintf("%s → %v@%d", describe(in), out.Succeeded, out.Version)
			}
			return fmt.Sprintf("%s → @%d", describe(in), out.Version)
		},
		DescribeState: func(state any) string {
			s := state.(register)
			return fmt.Sprintf("%q@%d", s.value, s.version)
		},
	}
	return nm.ToModel()
}

func describe(in Input) string {
	switch in.Op {
	case OpGet:
		return fmt.Sprintf("get(%s)", in.Key)
	case OpPut:
		return fmt.Sprintf("put(%s, %q)", in.Key, in.Value)
	}
	return fmt.Sprintf("cas(%s, %d, %q)", in.Key, in.Expected, in.Value)
}

// step returns every state s can move to by in producing out (none: impossible).
func step(s register, in Input, out Output) []any {
	written := register{value: in.Value, version: s.version + 1}
	switch in.Op {
	case OpGet:
		if out.Found == (s.version > 0) && (!out.Found || (out.Value == s.value && out.Version == s.version)) {
			return []any{s}
		}
	case OpPut:
		switch {
		case out.Unknown:
			return []any{s, written}
		case out.Version == written.version:
			return []any{written}
		}
	case OpCAS:
		matches := s.version == in.Expected
		switch {
		case out.Unknown && matches:
			return []any{s, written}
		case out.Unknown:
			return []any{s}
		case out.Succeeded && matches && out.Version == written.version:
			return []any{written}
		case !out.Succeeded && !matches && out.Version == s.version:
			return []any{s}
		}
	}
	return nil
}

// Recorder collects a concurrent history. Times are nanoseconds on the real monotonic clock since
// NewRecorder. Safe for concurrent use.
type Recorder struct {
	clk   clock.Clock
	start time.Time
	mu    sync.Mutex
	ops   []pc.Operation
}

// NewRecorder starts an empty history.
func NewRecorder() *Recorder {
	c := clock.Real{}
	return &Recorder{clk: c, start: c.Now()}
}

// Now is the timestamp to pass as an operation's call time, taken just before the request.
func (r *Recorder) Now() int64 { return int64(r.clk.Now().Sub(r.start)) }

// Record adds an operation that returned now. An Unknown output never returns.
func (r *Recorder) Record(client int, in Input, call int64, out Output) {
	ret := r.Now()
	if out.Unknown {
		ret = math.MaxInt64
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, pc.Operation{ClientId: client, Input: in, Call: call, Output: out, Return: ret})
}

// Operations returns the history so far, ordered by call time.
func (r *Recorder) Operations() []pc.Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	ops := slices.Clone(r.ops)
	slices.SortStableFunc(ops, func(a, b pc.Operation) int { return cmp.Compare(a.Call, b.Call) })
	return ops
}

// checkTimeout bounds the search; a history it can't decide in time is Unknown.
const checkTimeout = 5 * time.Minute

// Check checks ops against Model. Unless the result is Ok it writes the history (history.json)
// and Porcupine's visualization (visualization.html) to a new directory under dir and returns
// its path, which the failing test reports and keeps.
func Check(ops []pc.Operation, dir string) (pc.CheckResult, string) {
	model := Model()
	res, info := pc.CheckOperationsVerbose(model, ops, checkTimeout)
	if res == pc.Ok {
		return res, ""
	}
	out, err := os.MkdirTemp(dir, "porcupine-")
	if err != nil {
		return res, ""
	}
	if b, err := json.MarshalIndent(ops, "", " "); err == nil {
		_ = os.WriteFile(filepath.Join(out, "history.json"), b, 0o600) // best effort: the verdict stands either way
	}
	_ = pc.VisualizePath(model, info, filepath.Join(out, "visualization.html")) // best effort, as above
	return res, out
}
