package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/stream/domain"
)

// GeneratorVersion identifies the synthetic stream a seed produces. Bump it with any change to the
// generator's output, and regenerate the golden hash (go test -run Golden -update).
const GeneratorVersion = 1

// ClockMode chooses where event times come from.
type ClockMode string

// The clock modes.
const (
	// ModeSimulated runs a virtual clock: ticks are returned as fast as they are asked for, with
	// event times one 1/Rate step apart, so a day of ticks takes seconds.
	ModeSimulated ClockMode = "simulated"
	// ModeRealtime paces ticks on the clock: Next waits until a tick's event time has come.
	ModeRealtime ClockMode = "realtime"
)

// Kind says how the last tick was made (Synthetic.LastKind) and what Counts counts.
type Kind string

// The tick kinds. KindGap counts skipped one-minute windows, not ticks.
const (
	KindNormal  Kind = "normal"
	KindDup     Kind = "dup"     // an exact copy of a recent tick: same event id
	KindLate    Kind = "late"    // event time beyond the allowed lateness behind the latest one
	KindOOO     Kind = "ooo"     // event time up to 5 s behind the latest one: out of order, still on time
	KindInvalid Kind = "invalid" // breaks a domain rule: price, symbol, size or a future time
	KindGap     Kind = "gap"
)

// Injection defaults.
const (
	DefaultLateMax = 2 * time.Minute // how far behind a late tick may be
	oooMax         = 5 * time.Second // how far behind an out-of-order tick may be
	maxStepBP      = 20              // the random walk moves at most 0.20 % per tick
	recentTicks    = 64              // duplicates copy one of the last 64 good ticks
)

// Inject sets the fault-injection rates, each a fraction of the ticks returned (0.01 = 1 %).
// Dup+Late+OOO+Invalid must not exceed 1. Gap is the chance that a one-minute window is skipped
// entirely.
type Inject struct {
	Dup, Late, OOO, Invalid, Gap float64
	LateMax                      time.Duration // 0 means DefaultLateMax; more than allowed lateness + 1 s
}

// Config configures a Synthetic source.
type Config struct {
	Seed    uint64
	Symbols []string // each ^[A-Z]{1,6}(\.[A-Z])?$
	Rate    int      // ticks per second of event time, 1 to 1e9
	Start   time.Time
	End     time.Time // zero: unbounded; otherwise Next returns io.EOF from End on
	Mode    ClockMode
	Inject  Inject
	Name    string // the ticks' source; default "synthetic"
}

// Synthetic generates a geometric random walk per symbol from a seeded PRNG: the same Config and
// seed always give the same stream, on every platform (prices are integer cents, never floats).
// It is not safe for concurrent use.
type Synthetic struct {
	cfg      Config
	clock    clock.Clock
	rng      *rand.Rand
	interval time.Duration
	slot     time.Time // the next tick's scheduled event time
	maxTime  time.Time // the latest event time of a good tick so far
	window   time.Time // the one-minute window the last gap decision was for
	cents    map[string]int64
	seq      map[string]int64
	recent   []domain.Tick
	lastKind Kind
	counts   map[Kind]int
}

// NewSynthetic validates cfg and returns a source starting at cfg.Start (the clock's time when
// zero).
func NewSynthetic(cfg Config, c clock.Clock) (*Synthetic, error) {
	if err := validate(&cfg); err != nil {
		return nil, err
	}
	if cfg.Start.IsZero() {
		cfg.Start = c.Now()
	}
	cfg.Start = cfg.Start.UTC()
	if !cfg.End.IsZero() && !cfg.End.After(cfg.Start) {
		return nil, errors.New("synthetic: end must be after start")
	}
	s := &Synthetic{
		cfg: cfg, clock: c, rng: rand.New(rand.NewPCG(cfg.Seed, GeneratorVersion)), //nolint:gosec // G404: a seeded, reproducible stream is the point
		interval: time.Second / time.Duration(cfg.Rate), slot: cfg.Start,
		cents: map[string]int64{}, seq: map[string]int64{}, counts: map[Kind]int{},
	}
	for _, sym := range cfg.Symbols {
		s.cents[sym] = 10_000 + s.rng.Int64N(40_000) // 100.00 to 499.99
	}
	return s, nil
}

func validate(cfg *Config) error {
	var errs []error
	if len(cfg.Symbols) == 0 {
		errs = append(errs, errors.New("no symbols"))
	}
	for _, sym := range cfg.Symbols {
		probe := domain.Tick{EventID: "x", Symbol: sym, Price: decimal.NewFromInt(1), EventTime: time.Unix(0, 0), Source: "x"}
		if domain.Validate(probe, probe.EventTime) != domain.ReasonNone {
			errs = append(errs, fmt.Errorf("bad symbol %q", sym))
		}
	}
	if cfg.Rate < 1 || cfg.Rate > 1e9 {
		errs = append(errs, fmt.Errorf("rate %d outside 1..1e9 per second", cfg.Rate))
	}
	if cfg.Mode != ModeSimulated && cfg.Mode != ModeRealtime {
		errs = append(errs, fmt.Errorf("clock mode %q (want simulated or realtime)", cfg.Mode))
	}
	in := &cfg.Inject
	for name, v := range map[string]float64{"dup": in.Dup, "late": in.Late, "ooo": in.OOO, "invalid": in.Invalid, "gap": in.Gap} {
		if v < 0 || v > 1 {
			errs = append(errs, fmt.Errorf("injection %s=%g outside 0..1", name, v))
		}
	}
	if in.Dup+in.Late+in.OOO+in.Invalid > 1 {
		errs = append(errs, errors.New("dup+late+ooo+invalid exceed 100 %"))
	}
	if in.LateMax == 0 {
		in.LateMax = DefaultLateMax
	}
	if in.Late > 0 && in.LateMax < domain.DefaultAllowedLateness+2*time.Second {
		errs = append(errs, fmt.Errorf("late_max %v must exceed the allowed lateness %v by 2 s", in.LateMax, domain.DefaultAllowedLateness))
	}
	if cfg.Name == "" {
		cfg.Name = "synthetic"
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("synthetic: %w", err)
	}
	return nil
}

// Next returns the next tick. In realtime mode it first waits until the tick's event time.
func (s *Synthetic) Next(ctx context.Context) (domain.Tick, error) {
	if err := ctx.Err(); err != nil {
		return domain.Tick{}, err
	}
	s.skipGaps()
	if !s.cfg.End.IsZero() && !s.slot.Before(s.cfg.End) {
		return domain.Tick{}, io.EOF
	}
	if s.cfg.Mode == ModeRealtime {
		if d := s.slot.Sub(s.clock.Now()); d > 0 {
			if err := s.clock.Sleep(ctx, d); err != nil {
				return domain.Tick{}, err
			}
		}
	}
	t := s.make(s.pickKind())
	s.slot = s.slot.Add(s.interval)
	s.counts[s.lastKind]++
	return t, nil
}

// skipGaps decides, once per one-minute window, whether to skip it entirely.
func (s *Synthetic) skipGaps() {
	for {
		w := s.slot.Truncate(time.Minute)
		if w.Equal(s.window) {
			return
		}
		s.window = w
		if s.cfg.Inject.Gap == 0 || s.rng.Float64() >= s.cfg.Inject.Gap {
			return
		}
		s.counts[KindGap]++
		s.slot = w.Add(time.Minute)
	}
}

// pickKind draws the next tick's kind. Duplicates, late and out-of-order ticks need an earlier
// good tick; without one the tick is normal.
func (s *Synthetic) pickKind() Kind {
	in := s.cfg.Inject
	r := s.rng.Float64()
	var k Kind
	switch {
	case r < in.Dup:
		k = KindDup
	case r < in.Dup+in.Late:
		k = KindLate
	case r < in.Dup+in.Late+in.OOO:
		k = KindOOO
	case r < in.Dup+in.Late+in.OOO+in.Invalid:
		k = KindInvalid
	default:
		k = KindNormal
	}
	if k != KindNormal && k != KindInvalid && len(s.recent) == 0 {
		k = KindNormal
	}
	return k
}

func (s *Synthetic) make(k Kind) domain.Tick {
	s.lastKind = k
	switch k {
	case KindDup:
		return s.recent[s.rng.IntN(len(s.recent))]
	case KindLate:
		lo := domain.DefaultAllowedLateness + time.Second
		return s.remember(s.walk(s.maxTime.Add(-(lo + s.duration(s.cfg.Inject.LateMax-lo)))))
	case KindOOO:
		return s.remember(s.walk(s.maxTime.Add(-(time.Millisecond + s.duration(oooMax-time.Millisecond)))))
	case KindInvalid:
		return s.corrupt(s.walk(s.slot))
	}
	t := s.remember(s.walk(s.slot))
	s.maxTime = t.EventTime
	return t
}

// duration returns a uniform duration in [0, d].
func (s *Synthetic) duration(d time.Duration) time.Duration {
	return time.Duration(s.rng.Int64N(int64(d) + 1))
}

// walk moves a random symbol's price one step and returns its next tick at event time at.
func (s *Synthetic) walk(at time.Time) domain.Tick {
	sym := s.cfg.Symbols[s.rng.IntN(len(s.cfg.Symbols))]
	c := s.cents[sym]
	c += c * (s.rng.Int64N(2*maxStepBP+1) - maxStepBP) / 10_000
	s.cents[sym] = max(c, 1)
	s.seq[sym]++
	t := domain.Tick{
		Symbol: sym, Price: decimal.New(s.cents[sym], -2), Size: decimal.NewFromInt(s.rng.Int64N(1000) + 1),
		EventTime: at, Source: s.cfg.Name, Seq: s.seq[sym],
	}
	t.EventID = EventID(t.Source, t.Symbol, t.Seq, t.EventTime)
	return t
}

// corrupt breaks one domain rule of t.
func (s *Synthetic) corrupt(t domain.Tick) domain.Tick {
	switch s.rng.IntN(4) {
	case 0:
		t.Price = t.Price.Neg()
	case 1:
		t.Symbol = strings.ToLower(t.Symbol)
	case 2:
		t.Size = t.Size.Neg()
	default:
		t.EventTime = t.EventTime.Add(time.Hour)
	}
	t.EventID = EventID(t.Source, t.Symbol, t.Seq, t.EventTime)
	return t
}

// remember keeps t among the recent good ticks duplicates are copied from.
func (s *Synthetic) remember(t domain.Tick) domain.Tick {
	if len(s.recent) < recentTicks {
		s.recent = append(s.recent, t)
	} else {
		s.recent[s.rng.IntN(recentTicks)] = t
	}
	return t
}

// LastKind returns how the tick last returned by Next was made.
func (s *Synthetic) LastKind() Kind { return s.lastKind }

// Counts returns how many ticks of each kind Next has returned, and how many windows it skipped.
func (s *Synthetic) Counts() map[Kind]int {
	out := make(map[Kind]int, len(s.counts))
	for k, v := range s.counts {
		out[k] = v
	}
	return out
}
