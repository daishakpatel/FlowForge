package fsm

import (
	"slices"

	"google.golang.org/protobuf/proto"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// dedupe remembers the results of the last cap identified commands, keyed by (client_id,
// request_id), so a client's retry after a timeout returns the stored result instead of running
// again (00E §5.1). It is bounded by entries and by MaxDedupeBytes of encoded results. Eviction is
// FIFO by insertion, tracked in a slice, never by map order: a retry older than the window
// re-executes (a documented limitation).
type dedupe struct {
	cap     int
	bytes   int         // encoded size of the remembered results (MaxDedupeBytes)
	order   []dedupeKey // insertion order; live entries are order[head:]
	head    int
	results map[dedupeKey]*pb.CommandResult
}

type dedupeKey struct{ client, request string }

func newDedupe(capacity int) *dedupe {
	return &dedupe{cap: capacity, results: map[dedupeKey]*pb.CommandResult{}}
}

func (d *dedupe) get(k dedupeKey) *pb.CommandResult { return d.results[k] }

// add remembers r for k (k must not be present) and evicts the oldest beyond capacity.
func (d *dedupe) add(k dedupeKey, r *pb.CommandResult) {
	d.results[k] = r
	d.bytes += entryBytes(k, r)
	d.order = append(d.order, k)
	for len(d.order)-d.head > d.cap || d.bytes > MaxDedupeBytes {
		old := d.order[d.head]
		d.bytes -= entryBytes(old, d.results[old])
		delete(d.results, old)
		d.head++
	}
	if d.head > d.cap { // compact so the slice doesn't grow without bound
		d.order = slices.Clone(d.order[d.head:])
		d.head = 0
	}
}

func entryBytes(k dedupeKey, r *pb.CommandResult) int {
	return len(k.client) + len(k.request) + proto.Size(r)
}

// live returns the remembered keys, oldest first.
func (d *dedupe) live() []dedupeKey { return d.order[d.head:] }
