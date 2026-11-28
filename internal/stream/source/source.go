// Package source produces market-data ticks (task 05.03, FR-KAFKA-04): a seeded synthetic
// generator with fault injection and a CSV/JSONL file loader, behind one Source interface. The
// producer (internal/stream/producer) validates and publishes what a Source returns. Formats,
// flags and injection semantics: docs/architecture/market-data-generator.md.
package source

import (
	"context"
	"crypto/sha1" //nolint:gosec // G505: 00E §7 fixes sha1 for event ids; a dedupe key, not a security boundary
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/daishakpatel/flowforge/internal/stream/domain"
)

// Source returns ticks one at a time. Next returns io.EOF when the source is exhausted, a
// *ParseError for an input row it could not read (the next call continues with the following
// row), and ctx.Err() when ctx ends. A Source is used from one goroutine.
type Source interface {
	Next(ctx context.Context) (domain.Tick, error)
}

// EventID is the deterministic event id of a tick (00E §7): the hex SHA-1 of
// "source|symbol|sequence|event_time", with event_time in Unix nanoseconds. The same tick always
// gets the same id, which the processor dedupes on.
func EventID(source, symbol string, seq int64, eventTime time.Time) string {
	sum := sha1.Sum([]byte(source + "|" + symbol + "|" + strconv.FormatInt(seq, 10) + "|" + strconv.FormatInt(eventTime.UnixNano(), 10))) //nolint:gosec // G401: see the import
	return hex.EncodeToString(sum[:])
}

// ParseError is an input row a FileSource could not read. It is not fatal: the next Next call
// reads the following row.
type ParseError struct {
	Line int // 1-based line of the row in the input
	Err  error
}

func (e *ParseError) Error() string { return fmt.Sprintf("line %d: %v", e.Line, e.Err) }

func (e *ParseError) Unwrap() error { return e.Err }
