package source_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/stream/domain"
	"github.com/daishakpatel/flowforge/internal/stream/source"
)

var update = flag.Bool("update", false, "rewrite the golden stream hash")

var start = time.Date(2026, 11, 2, 14, 30, 0, 0, time.UTC)

func synth(t *testing.T, cfg source.Config) *source.Synthetic {
	t.Helper()
	s, err := source.NewSynthetic(cfg, clock.NewFake(start))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func baseConfig() source.Config {
	return source.Config{Seed: 42, Symbols: []string{"ACME", "BOLT", "CRUX"}, Rate: 100, Start: start, Mode: source.ModeSimulated}
}

func take(t *testing.T, s source.Source, n int) []domain.Tick {
	t.Helper()
	out := make([]domain.Tick, 0, n)
	for range n {
		tk, err := s.Next(t.Context())
		if err != nil {
			t.Fatalf("after %d ticks: %v", len(out), err)
		}
		out = append(out, tk)
	}
	return out
}

// streamHash is the SHA-256 of a canonical line per tick; it pins the generator's output.
func streamHash(ticks []domain.Tick) string {
	h := sha256.New()
	for _, tk := range ticks {
		_, _ = fmt.Fprintf(h, "%s|%s|%s|%s|%d|%s|%d\n", tk.EventID, tk.Symbol, tk.Price, tk.Size, tk.EventTime.UnixNano(), tk.Source, tk.Seq) // hash writes never fail
	}
	return hex.EncodeToString(h.Sum(nil))
}

// TestSynthetic_GoldenStream is task 05.03 §8's golden test: seed 42, 3 symbols, 1 000 ticks. The
// hash changes only with an intentional change of the generator (bump source.GeneratorVersion and
// rerun with -update).
func TestSynthetic_GoldenStream(t *testing.T) {
	t.Parallel()
	got := streamHash(take(t, synth(t, baseConfig()), 1000))
	golden := filepath.Join("testdata", fmt.Sprintf("golden_v%d_seed42.sha256", source.GeneratorVersion))
	if *update {
		if err := os.WriteFile(golden, []byte(got+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden) //nolint:gosec // G304: a fixed testdata path built from a constant
	if err != nil {
		t.Fatalf("%v (run with -update to create it)", err)
	}
	if got != strings.TrimSpace(string(want)) {
		t.Errorf("stream hash %s, golden %s: the generator's output changed", got, want)
	}
}

func TestSynthetic_SameSeedSameStream(t *testing.T) {
	t.Parallel()
	inj := baseConfig()
	inj.Inject = source.Inject{Dup: 0.05, Late: 0.05, OOO: 0.05, Invalid: 0.05, Gap: 0.2}
	a, b := take(t, synth(t, inj), 3000), take(t, synth(t, inj), 3000)
	if streamHash(a) != streamHash(b) {
		t.Error("the same seed and config gave two different streams")
	}
	other := inj
	other.Seed = 43
	if streamHash(take(t, synth(t, other), 3000)) == streamHash(a) {
		t.Error("another seed gave the same stream")
	}
}

func TestSynthetic_NormalTicksAreValidAndOrderedPerSymbol(t *testing.T) {
	t.Parallel()
	ticks := take(t, synth(t, baseConfig()), 2000)
	interval := time.Second / 100
	last := map[string]domain.Tick{}
	for i, tk := range ticks {
		if r := domain.Validate(tk, tk.EventTime); r != domain.ReasonNone {
			t.Fatalf("tick %d invalid without injection: %s %+v", i, r, tk)
		}
		if want := start.Add(time.Duration(i) * interval); !tk.EventTime.Equal(want) {
			t.Fatalf("tick %d at %v, want %v (one tick every 1/rate)", i, tk.EventTime, want)
		}
		if p, ok := last[tk.Symbol]; ok && tk.Seq != p.Seq+1 {
			t.Fatalf("tick %d: %s seq %d after %d", i, tk.Symbol, tk.Seq, p.Seq)
		}
		if tk.EventID != source.EventID(tk.Source, tk.Symbol, tk.Seq, tk.EventTime) {
			t.Fatalf("tick %d: event id is not EventID of its fields", i)
		}
		last[tk.Symbol] = tk
	}
	if len(last) != 3 {
		t.Errorf("symbols seen: %d, want 3", len(last))
	}
}

// TestSynthetic_InjectionRates: over 100 000 ticks each injected kind's share is within ±0.5
// percentage points of its configured rate (task 05.03 §8), and each kind looks like what it claims.
func TestSynthetic_InjectionRates(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Seed = 7
	cfg.Rate = 1000
	cfg.Inject = source.Inject{Dup: 0.01, Late: 0.02, OOO: 0.03, Invalid: 0.01, LateMax: 2 * time.Minute}
	s := synth(t, cfg)
	const n = 100_000
	seen := map[string]bool{}
	var maxTime time.Time
	for i := range n {
		tk, err := s.Next(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		kind := s.LastKind()
		switch kind {
		case source.KindDup:
			if !seen[tk.EventID] {
				t.Fatalf("tick %d: a duplicate with an event id never seen before", i)
			}
		case source.KindLate:
			if behind := maxTime.Sub(tk.EventTime); behind <= domain.DefaultAllowedLateness || behind > 2*time.Minute+time.Second {
				t.Fatalf("tick %d: late tick %v behind the latest event time", i, behind)
			}
		case source.KindOOO:
			if behind := maxTime.Sub(tk.EventTime); behind <= 0 || behind > domain.DefaultAllowedLateness {
				t.Fatalf("tick %d: out-of-order tick %v behind, want within the allowed lateness", i, behind)
			}
		case source.KindInvalid:
			if domain.Validate(tk, maxTime) == domain.ReasonNone {
				t.Fatalf("tick %d: an invalid injection is valid: %+v", i, tk)
			}
		}
		if kind != source.KindDup && seen[tk.EventID] {
			t.Fatalf("tick %d (%s) reuses an event id", i, kind)
		}
		seen[tk.EventID] = true
		if tk.EventTime.After(maxTime) && kind != source.KindInvalid {
			maxTime = tk.EventTime
		}
	}
	counts := s.Counts()
	for kind, want := range map[source.Kind]float64{source.KindDup: 0.01, source.KindLate: 0.02, source.KindOOO: 0.03, source.KindInvalid: 0.01} {
		if got := float64(counts[kind]) / n; math.Abs(got-want) > 0.005 {
			t.Errorf("%s: %.4f of ticks, want %.4f ± 0.005", kind, got, want)
		}
	}
}

func TestSynthetic_GapsSkipWholeWindows(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Rate = 10
	cfg.Inject = source.Inject{Gap: 0.3}
	ticks := take(t, synth(t, cfg), 3000) // 300 s of event time at full rate, more with gaps
	windows := map[time.Time]bool{}
	for _, tk := range ticks {
		w, _ := domain.WindowFor(tk.EventTime, domain.Window1m) // 1m is a valid size
		windows[w.Start] = true
	}
	last := ticks[len(ticks)-1].EventTime
	span := int(last.Sub(start)/time.Minute) + 1
	if missing := span - len(windows); missing == 0 {
		t.Errorf("no 1m window was skipped over %d windows with gap=30%%", span)
	}
}

func TestSynthetic_EndsWithEOF(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.End = start.Add(time.Second) // 100 ticks at 100/s
	s := synth(t, cfg)
	n := 0
	for {
		_, err := s.Next(t.Context())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 100 {
		t.Errorf("%d ticks before End, want 100", n)
	}
}

func TestSynthetic_RealtimeWaitsForTheClock(t *testing.T) {
	t.Parallel()
	fake := clock.NewFake(start)
	cfg := baseConfig()
	cfg.Mode = source.ModeRealtime
	cfg.Rate = 2 // a tick every 500 ms
	s, err := source.NewSynthetic(cfg, fake)
	if err != nil {
		t.Fatal(err)
	}
	if first := take(t, s, 1)[0]; !first.EventTime.Equal(start) {
		t.Fatalf("first tick at %v, want the clock's time %v", first.EventTime, start)
	}
	got := make(chan domain.Tick, 1)
	go func() {
		tk, err := s.Next(context.Background())
		if err == nil {
			got <- tk
		}
		close(got)
	}()
	fake.BlockUntilWaiters(1) // Next sleeps until the second tick is due
	select {
	case <-got:
		t.Fatal("the second tick came before the clock reached its time")
	default:
	}
	fake.Advance(500 * time.Millisecond)
	tk, ok := <-got
	if !ok || !tk.EventTime.Equal(start.Add(500*time.Millisecond)) {
		t.Errorf("second tick %v (%v), want at +500ms", tk.EventTime, ok)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Next on a cancelled context: %v", err)
	}
}

func TestNewSynthetic_RejectsBadConfig(t *testing.T) {
	t.Parallel()
	for name, edit := range map[string]func(*source.Config){
		"no symbols":       func(c *source.Config) { c.Symbols = nil },
		"bad symbol":       func(c *source.Config) { c.Symbols = []string{"acme"} },
		"zero rate":        func(c *source.Config) { c.Rate = 0 },
		"rate above 1e9":   func(c *source.Config) { c.Rate = 2_000_000_000 },
		"bad mode":         func(c *source.Config) { c.Mode = "warp" },
		"injection > 100%": func(c *source.Config) { c.Inject = source.Inject{Dup: 0.6, Late: 0.5} },
		"negative rate":    func(c *source.Config) { c.Inject = source.Inject{OOO: -0.1} },
		"late max too low": func(c *source.Config) { c.Inject = source.Inject{Late: 0.1, LateMax: time.Second} },
		"end before start": func(c *source.Config) { c.End = start.Add(-time.Second) },
	} {
		cfg := baseConfig()
		edit(&cfg)
		if _, err := source.NewSynthetic(cfg, clock.NewFake(start)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestParseInject(t *testing.T) {
	t.Parallel()
	got, err := source.ParseInject("dup=1%, late=2.5%,ooo=3%,invalid=0.5%,gap=1%,late_max=90s")
	if err != nil {
		t.Fatal(err)
	}
	want := source.Inject{Dup: 0.01, Late: 0.025, OOO: 0.03, Invalid: 0.005, Gap: 0.01, LateMax: 90 * time.Second}
	if math.Abs(got.Dup-want.Dup) > 1e-12 || math.Abs(got.Late-want.Late) > 1e-12 || math.Abs(got.OOO-want.OOO) > 1e-12 ||
		math.Abs(got.Invalid-want.Invalid) > 1e-12 || math.Abs(got.Gap-want.Gap) > 1e-12 || got.LateMax != want.LateMax {
		t.Errorf("ParseInject = %+v, want %+v", got, want)
	}
	if got, err := source.ParseInject(""); err != nil || got != (source.Inject{}) {
		t.Errorf("empty: %+v, %v", got, err)
	}
	for _, bad := range []string{"dup", "dup=1", "dup=x%", "warp=1%", "dup=1%,dup=2%", "late_max=soon", "dup=-1%", "dup=NaN%", "dup=Inf%"} {
		if _, err := source.ParseInject(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestEventID(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 11, 2, 14, 30, 0, 250_000_000, time.UTC)
	// 00E §7: sha1(source|symbol|sequence|event_time), event_time as Unix nanoseconds.
	if got := source.EventID("synthetic", "ACME", 42, at); got != "bbec3602714a6509322e5604eb5fd0bb26c3fad8" {
		t.Errorf("EventID = %s", got)
	}
	if source.EventID("synthetic", "ACME", 42, at.In(time.FixedZone("EST", -5*3600))) != source.EventID("synthetic", "ACME", 42, at) {
		t.Error("EventID depends on the time zone")
	}
}
