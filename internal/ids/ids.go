// Package ids generates and validates FlowForge identifiers: a short prefix, an underscore and a
// ULID, such as run_01JC7Z8Q3V6N2X4RTMB9WKD5HF (task 01.04, 00E §1). ULIDs sort by creation time,
// and IDs from one Generator are strictly increasing.
package ids

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/oklog/ulid/v2"

	"github.com/daishakpatel/flowforge/internal/clock"
)

// ID prefixes (00E §1 and the master spec conventions).
const (
	PrefixWorkflow      = "wf"
	PrefixRun           = "run"
	PrefixTask          = "task"
	PrefixAttempt       = "att"
	PrefixEvent         = "evt"
	PrefixInvestigation = "inv"
	PrefixApproval      = "apr"
	PrefixRequest       = "req"
	PrefixTrigger       = "trg"
)

// ErrInvalid is wrapped by every Parse and Validate error.
var ErrInvalid = errors.New("invalid id")

// Generator creates prefixed IDs. Services take a Generator so tests can pass a Fake.
type Generator interface {
	New(prefix string) string
}

// ULIDGenerator is the real Generator. IDs it returns are strictly increasing across all
// goroutines: the timestamp read and the entropy increment happen under one mutex, and the
// timestamp never goes backwards even if the clock does.
type ULIDGenerator struct {
	mu      sync.Mutex
	clock   clock.Clock
	entropy *ulid.MonotonicEntropy
	lastMS  uint64
}

// NewGenerator returns a ULIDGenerator reading time from c and randomness from entropy
// (crypto/rand.Reader in production).
func NewGenerator(c clock.Clock, entropy io.Reader) *ULIDGenerator {
	return &ULIDGenerator{clock: c, entropy: ulid.Monotonic(entropy, 0)}
}

// New returns prefix + "_" + a new ULID. It panics only if the entropy source fails, which
// crypto/rand.Reader never does.
func (g *ULIDGenerator) New(prefix string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	ms := max(ulid.Timestamp(g.clock.Now()), g.lastMS)
	for {
		id, err := ulid.New(ms, g.entropy)
		switch {
		case err == nil:
			g.lastMS = ms
			return prefix + "_" + id.String()
		case errors.Is(err, ulid.ErrMonotonicOverflow):
			ms++ // this millisecond's entropy is used up; borrow the next one to stay ordered
		default:
			panic(fmt.Sprintf("ids: entropy source failed: %v", err))
		}
	}
}

// defaultGenerator backs New. It is the package's one piece of shared state (a deliberate
// exception to 00C §8): a single mutex-guarded generator is what makes New monotonic across
// every caller in the process.
var defaultGenerator = NewGenerator(clock.Real{}, rand.Reader)

// New returns a new ID from the process-wide generator. Code with an injected Generator should
// use that instead, so tests stay deterministic.
func New(prefix string) string { return defaultGenerator.New(prefix) }

// Parse checks that s is prefix + "_" + a valid ULID and returns the ULID.
func Parse(prefix, s string) (ulid.ULID, error) {
	rest, ok := strings.CutPrefix(s, prefix+"_")
	if !ok {
		return ulid.ULID{}, fmt.Errorf("%w: want prefix %q", ErrInvalid, prefix+"_")
	}
	id, err := ulid.ParseStrict(rest)
	if err != nil {
		return ulid.ULID{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return id, nil
}

// Validate reports whether s is a valid ID with the given prefix.
func Validate(prefix, s string) error {
	_, err := Parse(prefix, s)
	return err
}
