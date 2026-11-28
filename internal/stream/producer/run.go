package producer

import (
	"context"
	"errors"
	"io"

	"github.com/daishakpatel/flowforge/internal/stream/domain"
	"github.com/daishakpatel/flowforge/internal/stream/source"
)

// DefaultBatch is how many ticks Run publishes per batch: the bound on producer memory.
const DefaultBatch = 500

const maxBadLines = 20 // Summary keeps the first bad rows' line numbers

// RunOptions bounds a Run.
type RunOptions struct {
	Count int // stop after this many ticks; 0: until the source ends
	Batch int // ticks per publish; 0: DefaultBatch. Realtime sources want small batches
}

// Summary counts a Run. Read = Published + Rejected + ForcedInvalid.
type Summary struct {
	Read          int   `json:"read"`
	Published     int   `json:"published"`
	Rejected      int   `json:"rejected"`
	ForcedInvalid int   `json:"forced_invalid"`
	BadRows       int   `json:"bad_rows"`            // input rows the source could not parse
	BadLines      []int `json:"bad_lines,omitempty"` // the first 20 of them
}

// Run reads ticks from src and publishes them in batches until the source ends, Count ticks are
// read, or ctx ends (then it returns ctx's error and the summary so far; the last partial batch
// is not published). Rows the source cannot parse are counted and skipped. A publish error stops
// the run: Kafka retries inside the client, so an error here means the broker stayed unreachable.
func Run(ctx context.Context, src source.Source, p *Producer, opts RunOptions) (Summary, error) {
	batch := opts.Batch
	if batch <= 0 {
		batch = DefaultBatch
	}
	var sum Summary
	buf := make([]domain.Tick, 0, batch)
	flush := func() error {
		res, err := p.Publish(ctx, buf)
		sum.Published += res.Published
		sum.Rejected += res.Rejected
		sum.ForcedInvalid += res.ForcedInvalid
		buf = buf[:0]
		return err
	}
	for opts.Count == 0 || sum.Read < opts.Count {
		t, err := src.Next(ctx)
		var pe *source.ParseError
		switch {
		case errors.As(err, &pe):
			sum.BadRows++
			if len(sum.BadLines) < maxBadLines {
				sum.BadLines = append(sum.BadLines, pe.Line)
			}
			continue
		case errors.Is(err, io.EOF):
			return sum, flush()
		case err != nil:
			return sum, err
		}
		sum.Read++
		buf = append(buf, t)
		if len(buf) == batch {
			if err := flush(); err != nil {
				return sum, err
			}
		}
	}
	return sum, flush()
}
