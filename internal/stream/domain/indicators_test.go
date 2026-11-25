package domain_test

import (
	"math"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/daishakpatel/flowforge/internal/stream/domain"
)

func decs(ss ...string) []decimal.Decimal {
	out := make([]decimal.Decimal, len(ss))
	for i, s := range ss {
		out[i] = dec(s)
	}
	return out
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-12 }

// Worked example 2 of docs/architecture/market-data-math.md.
func TestMovingAverage(t *testing.T) {
	t.Parallel()
	got, ok := domain.MovingAverage(decs("10", "11", "12", "13", "14", "15"), 5)
	if !ok || !got.Equal(dec("13")) {
		t.Errorf("MA(5) = %s, %v; want 13 (mean of the last five)", got, ok)
	}
	got, ok = domain.MovingAverage(decs("1", "2", "2"), 3)
	if !ok || !got.Equal(dec("1.66666667")) {
		t.Errorf("MA(3) = %s, %v; want 1.66666667", got, ok)
	}
	for _, n := range []int{0, -1, 4} {
		if _, ok := domain.MovingAverage(decs("1", "2", "3"), n); ok {
			t.Errorf("MA(%d) of 3 closes: ok", n)
		}
	}
}

// Worked example 3 of docs/architecture/market-data-math.md.
func TestVolatility(t *testing.T) {
	t.Parallel()
	got, ok := domain.Volatility(decs("100", "110", "99", "108.9"), 4)
	if !ok || !near(got, 0.11585728004354241) {
		t.Errorf("Volatility = %v, %v; want 0.11585728004354241 (sample std-dev of ln returns)", got, ok)
	}
	got, ok = domain.Volatility(decs("5", "100", "110", "99", "108.9"), 4) // only the last four count
	if !ok || !near(got, 0.11585728004354241) {
		t.Errorf("Volatility over the last 4 of 5 = %v, %v", got, ok)
	}
	if got, ok := domain.Volatility(decs("7", "7", "7"), 3); !ok || got != 0 {
		t.Errorf("flat closes: %v, %v; want 0", got, ok)
	}
	for name, closes := range map[string][]decimal.Decimal{
		"too few":      decs("1", "2"),
		"zero close":   decs("1", "0", "2"),
		"window above": decs("1", "2", "3"),
	} {
		n := 3
		if name == "window above" {
			n = 4
		}
		if _, ok := domain.Volatility(closes, n); ok {
			t.Errorf("%s: ok", name)
		}
	}
}

func alternating(n int) []decimal.Decimal {
	out := make([]decimal.Decimal, n)
	for i := range out {
		out[i] = dec("100")
		if i%2 == 1 {
			out[i] = dec("102")
		}
	}
	return out
}

func TestDetectAnomaly(t *testing.T) {
	t.Parallel()
	p := domain.DefaultAnomalyParams()
	if p.Window != 20 || p.K != 3 || p.MinSamples != 10 {
		t.Fatalf("defaults = %+v, want n=20 k=3 min=10", p)
	}
	hist := alternating(20) // mean 101, sample std-dev 1.0259783520851542

	z, flagged, ok := domain.DetectAnomaly(hist, dec("110"), p)
	if !ok || !flagged || !near(z, 8.772114910328067) {
		t.Errorf("spike: z %v flagged %v ok %v; want 8.7721…, flagged", z, flagged, ok)
	}
	z, flagged, ok = domain.DetectAnomaly(hist, dec("102"), p)
	if !ok || flagged || !near(z, 0.9746794344808963) {
		t.Errorf("normal close: z %v flagged %v ok %v", z, flagged, ok)
	}
	z, flagged, _ = domain.DetectAnomaly(hist, dec("90"), p)
	if !flagged || z > -3 {
		t.Errorf("downward spike: z %v flagged %v", z, flagged)
	}

	// Only the last Window closes count: a long, wild history beyond them changes nothing.
	long := append(decs("1", "1000", "1", "1000"), hist...)
	if z2, _, _ := domain.DetectAnomaly(long, dec("110"), p); !near(z2, 8.772114910328067) {
		t.Errorf("history beyond the window changed z: %v", z2)
	}

	// Below the minimum number of samples nothing is ever flagged, however extreme.
	if _, flagged, ok := domain.DetectAnomaly(alternating(9), dec("1000000"), p); flagged || ok {
		t.Errorf("9 samples: flagged %v ok %v; want neither", flagged, ok)
	}

	// A flat history has std-dev 0: any different close is flagged with an infinite z.
	flat := decs("100", "100", "100", "100", "100", "100", "100", "100", "100", "100")
	if z, flagged, ok := domain.DetectAnomaly(flat, dec("100.01"), p); !ok || !flagged || !math.IsInf(z, 1) {
		t.Errorf("flat history spike: z %v flagged %v ok %v", z, flagged, ok)
	}
	if z, flagged, ok := domain.DetectAnomaly(flat, dec("100"), p); !ok || flagged || z != 0 {
		t.Errorf("flat history, same close: z %v flagged %v ok %v", z, flagged, ok)
	}
	// A value float64 cannot represent exactly: flatness is decided on the decimals, not on a
	// float mean and std-dev that rounding makes almost-but-not-quite zero.
	flat2 := make([]decimal.Decimal, 20)
	for i := range flat2 {
		flat2[i] = dec("100.1")
	}
	if z, flagged, ok := domain.DetectAnomaly(flat2, dec("100.1"), p); !ok || flagged || z != 0 {
		t.Errorf("flat 100.1 history, same close: z %v flagged %v ok %v; want 0, not flagged", z, flagged, ok)
	}
	if z, flagged, ok := domain.DetectAnomaly(flat2, dec("100.1001"), p); !ok || !flagged || !math.IsInf(z, 1) {
		t.Errorf("flat 100.1 history, tiny rise: z %v flagged %v ok %v; want +Inf, flagged", z, flagged, ok)
	}
}
