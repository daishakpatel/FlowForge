package domain

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
)

// DerivedScale is the number of decimal places of derived prices (VWAP, moving average), rounded
// half away from zero. Raw prices keep their own scale.
const DerivedScale = 8

// Stats is one window's aggregate: a market_aggregates row without the indicators and bookkeeping.
type Stats struct {
	Symbol                 string
	Window                 Window
	Open, High, Low, Close decimal.Decimal
	Volume                 decimal.Decimal // sum of sizes
	VWAP                   decimal.Decimal // Σ(price·size)/Σsize at DerivedScale; the close when Σsize = 0
	Count                  int
	InputHash              string // InputHash of the ticks' event ids
}

// Aggregate computes the stats of the ticks of one symbol in window w. The result depends only on
// the set of ticks, never on their order: open and close are the first and last tick by
// (EventTime, Seq, EventID), with Price and Size as last tie-breaks so even an undeduplicated batch
// is deterministic. Every tick must lie in w and share one symbol; the caller passes the stored set
// (market_ticks is unique on (event_id, event_time), ADR-011). No ticks give a zero Stats with
// Count 0, which is never upserted: market_aggregates requires event_count > 0.
func Aggregate(ticks []Tick, w Window) (Stats, error) {
	st := Stats{Window: w, InputHash: InputHash(nil)}
	if len(ticks) == 0 {
		return st, nil
	}
	sorted := slices.Clone(ticks)
	slices.SortFunc(sorted, func(a, b Tick) int {
		return cmp.Or(a.EventTime.Compare(b.EventTime), cmp.Compare(a.Seq, b.Seq), strings.Compare(a.EventID, b.EventID),
			a.Price.Cmp(b.Price), a.Size.Cmp(b.Size))
	})
	st.Symbol = sorted[0].Symbol
	st.Open, st.Close = sorted[0].Price, sorted[len(sorted)-1].Price
	st.High, st.Low = st.Open, st.Open
	notional := decimal.Zero
	ids := make([]string, len(sorted))
	for i, t := range sorted {
		if t.Symbol != st.Symbol {
			return Stats{}, fmt.Errorf("aggregate %s %v: ticks of %s and %s", w.Size, w.Start, st.Symbol, t.Symbol)
		}
		if !w.Contains(t.EventTime) {
			return Stats{}, fmt.Errorf("aggregate %s %v: tick %s at %v is outside the window", w.Size, w.Start, t.EventID, t.EventTime)
		}
		if t.Price.GreaterThan(st.High) {
			st.High = t.Price
		}
		if t.Price.LessThan(st.Low) {
			st.Low = t.Price
		}
		st.Volume = st.Volume.Add(t.Size)
		notional = notional.Add(t.Price.Mul(t.Size))
		ids[i] = t.EventID
	}
	st.Count = len(sorted)
	st.VWAP = st.Close
	if !st.Volume.IsZero() {
		st.VWAP = notional.DivRound(st.Volume, DerivedScale)
	}
	st.InputHash = InputHash(ids)
	return st, nil
}

// InputHash is the hex SHA-256 of the sorted event ids, each written as <byte length>:<id>, so no
// id can run into the next however it is spelt: equal for the same set of ticks in any order. It is
// stored as market_aggregates.input_hash, so an unchanged window is never rewritten (05.05).
func InputHash(eventIDs []string) string {
	ids := slices.Clone(eventIDs)
	slices.Sort(ids)
	h := sha256.New()
	for _, id := range ids {
		_, _ = h.Write([]byte(strconv.Itoa(len(id)) + ":" + id)) // hash.Hash.Write never fails
	}
	return hex.EncodeToString(h.Sum(nil))
}
