package domain_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/daishakpatel/flowforge/internal/stream/domain"
)

func window(t *testing.T, start string, size domain.WindowSize) domain.Window {
	t.Helper()
	w, err := domain.WindowFor(utc(start), size)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func tick(id string, at string, seq int64, price, size string) domain.Tick {
	return domain.Tick{EventID: id, Symbol: "ACME", Price: dec(price), Size: dec(size), EventTime: utc(at), Source: "synthetic", Seq: seq}
}

// Worked example 1 of docs/architecture/market-data-math.md.
func exampleTicks() []domain.Tick {
	return []domain.Tick{
		tick("a", "2026-11-02T14:30:05Z", 1, "100.00", "10"),
		tick("b", "2026-11-02T14:30:20Z", 2, "101.50", "5"),
		tick("c", "2026-11-02T14:30:40Z", 3, "99.75", "20"),
		tick("d", "2026-11-02T14:30:59.999Z", 4, "100.25", "15"),
	}
}

func eqDec(t *testing.T, field string, got decimal.Decimal, want string) {
	t.Helper()
	if !got.Equal(dec(want)) {
		t.Errorf("%s = %s, want %s", field, got, want)
	}
}

func TestAggregate_WorkedExample(t *testing.T) {
	t.Parallel()
	st, err := domain.Aggregate(exampleTicks(), window(t, "2026-11-02T14:30:00Z", domain.Window1m))
	if err != nil {
		t.Fatal(err)
	}
	eqDec(t, "open", st.Open, "100.00")
	eqDec(t, "high", st.High, "101.50")
	eqDec(t, "low", st.Low, "99.75")
	eqDec(t, "close", st.Close, "100.25")
	eqDec(t, "volume", st.Volume, "50")
	eqDec(t, "vwap", st.VWAP, "100.125") // 5006.25 / 50
	if st.Count != 4 || st.Symbol != "ACME" {
		t.Errorf("count %d symbol %q", st.Count, st.Symbol)
	}
	const wantHash = "40b395eaa29be6f59881ada1804095a0b13309429bdaf4e86c1a7e2ed2167ea0" // sha256("1:a1:b1:c1:d")
	if st.InputHash != wantHash {
		t.Errorf("input_hash %s, want %s", st.InputHash, wantHash)
	}
}

func TestAggregate_TiesOrderBySeqThenEventID(t *testing.T) {
	t.Parallel()
	at := "2026-11-02T14:30:10Z"
	ticks := []domain.Tick{
		tick("z", at, 2, "3", "1"),
		tick("y", at, 1, "2", "1"),
		tick("x", at, 1, "1", "1"), // same time and seq as y: x < y by event id, so x opens
		tick("w", at, 3, "4", "1"), // highest seq closes
	}
	st, err := domain.Aggregate(ticks, window(t, "2026-11-02T14:30:00Z", domain.Window1m))
	if err != nil {
		t.Fatal(err)
	}
	eqDec(t, "open", st.Open, "1")
	eqDec(t, "close", st.Close, "4")
}

// TestAggregate_DuplicateKeyDifferentPriceIsDeterministic: an undeduplicated batch may hold one
// event twice with different prices (a producer bug); open and close still do not depend on order.
func TestAggregate_DuplicateKeyDifferentPriceIsDeterministic(t *testing.T) {
	t.Parallel()
	w := window(t, "2026-11-02T14:30:00Z", domain.Window1m)
	a := tick("e", "2026-11-02T14:30:10Z", 1, "10", "1")
	b := tick("e", "2026-11-02T14:30:10Z", 1, "11", "1")
	x, err := domain.Aggregate([]domain.Tick{a, b}, w)
	if err != nil {
		t.Fatal(err)
	}
	y, err := domain.Aggregate([]domain.Tick{b, a}, w)
	if err != nil {
		t.Fatal(err)
	}
	if !sameStats(x, y) {
		t.Errorf("order changed the result: %+v vs %+v", x, y)
	}
}

func TestAggregate_ZeroVolumeVWAPIsClose(t *testing.T) {
	t.Parallel()
	ticks := []domain.Tick{tick("a", "2026-11-02T14:30:01Z", 1, "10", "0"), tick("b", "2026-11-02T14:30:02Z", 2, "12", "0")}
	st, err := domain.Aggregate(ticks, window(t, "2026-11-02T14:30:00Z", domain.Window1m))
	if err != nil {
		t.Fatal(err)
	}
	eqDec(t, "vwap", st.VWAP, "12")
	eqDec(t, "volume", st.Volume, "0")
}

func TestAggregate_VWAPRoundsToScale(t *testing.T) {
	t.Parallel()
	ticks := []domain.Tick{tick("a", "2026-11-02T14:30:01Z", 1, "1", "1"), tick("b", "2026-11-02T14:30:02Z", 2, "2", "2")}
	st, err := domain.Aggregate(ticks, window(t, "2026-11-02T14:30:00Z", domain.Window1m))
	if err != nil {
		t.Fatal(err)
	}
	eqDec(t, "vwap", st.VWAP, "1.66666667") // 5/3, half-up to DerivedScale places
}

func TestAggregate_Rejects(t *testing.T) {
	t.Parallel()
	w := window(t, "2026-11-02T14:30:00Z", domain.Window1m)
	outside := exampleTicks()
	outside[2].EventTime = w.End() // the end is excluded
	other := exampleTicks()
	other[1].Symbol = "OTHR"
	for name, ticks := range map[string][]domain.Tick{"tick outside the window": outside, "mixed symbols": other} {
		if _, err := domain.Aggregate(ticks, w); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	st, err := domain.Aggregate(nil, w)
	if err != nil || st.Count != 0 || st.InputHash != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Errorf("empty window = %+v, %v", st, err)
	}
}

// TestAggregate_OrderIndependent is the property of task 05.02 §8: 1 000 random permutations of
// the same ticks give identical stats and input_hash.
func TestAggregate_OrderIndependent(t *testing.T) {
	t.Parallel()
	const seed = 502
	rng := rand.New(rand.NewPCG(seed, 0)) //nolint:gosec // test permutations, not security
	w := window(t, "2026-11-02T14:30:00Z", domain.Window5m)
	ticks := make([]domain.Tick, 60)
	for i := range ticks {
		at := w.Start.Add(time.Duration(rng.IntN(300)) * time.Second) // repeated times force the tie-breaks
		ticks[i] = domain.Tick{
			EventID: string(rune('A'+i%26)) + string(rune('a'+i/26)), Symbol: "ACME", EventTime: at, Source: "synthetic",
			Seq: int64(rng.IntN(5)), Price: decimal.New(int64(9_000+rng.IntN(2_000)), -2), Size: decimal.NewFromInt(int64(rng.IntN(50))),
		}
	}
	want, err := domain.Aggregate(ticks, w)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		p := append([]domain.Tick(nil), ticks...)
		rng.Shuffle(len(p), func(a, b int) { p[a], p[b] = p[b], p[a] })
		got, err := domain.Aggregate(p, w)
		if err != nil {
			t.Fatal(err)
		}
		if !sameStats(got, want) {
			t.Fatalf("seed %d, permutation %d: %+v differs from %+v", seed, i, got, want)
		}
	}
}

func sameStats(a, b domain.Stats) bool {
	return a.Symbol == b.Symbol && a.Count == b.Count && a.InputHash == b.InputHash &&
		a.Open.Equal(b.Open) && a.High.Equal(b.High) && a.Low.Equal(b.Low) && a.Close.Equal(b.Close) &&
		a.Volume.Equal(b.Volume) && a.VWAP.Equal(b.VWAP)
}

func TestInputHash_IgnoresOrder(t *testing.T) {
	t.Parallel()
	if domain.InputHash([]string{"b", "a", "d", "c"}) != domain.InputHash([]string{"d", "c", "b", "a"}) {
		t.Error("InputHash depends on order")
	}
	if domain.InputHash([]string{"ab", "c"}) == domain.InputHash([]string{"a", "bc"}) {
		t.Error("InputHash: ids are not separated")
	}
	if domain.InputHash([]string{"a\nb"}) == domain.InputHash([]string{"a", "b"}) {
		t.Error("InputHash: an id containing the separator collides with two ids")
	}
}
