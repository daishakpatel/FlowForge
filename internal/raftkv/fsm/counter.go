package fsm

import (
	"math"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// incr increments a named counter and returns its new value (1 on first use). Counters only
// grow: a failed command never touches them, and a counter at its maximum refuses with
// QUOTA_EXCEEDED rather than wrap.
func (s *state) incr(r *pb.IncrCounterRequest) *pb.CommandResult {
	name := r.GetName()
	if c := checkKey(name); c != codeOK {
		return result(c)
	}
	v, exists := s.counters[name]
	if (!exists && (len(s.counters) >= MaxCounters || !s.fits(len(name)))) || v == math.MaxUint64 {
		return result(codeQuota)
	}
	if !exists {
		s.dataBytes += len(name)
	}
	s.counters[name] = v + 1
	return &pb.CommandResult{Code: codeOK, Counter: v + 1}
}
