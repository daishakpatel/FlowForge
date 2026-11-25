package domain_test

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/daishakpatel/flowforge/internal/stream/domain"
)

var now = time.Date(2026, 11, 2, 14, 30, 0, 0, time.UTC)

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func goodTick() domain.Tick {
	return domain.Tick{
		EventID: "e1", Symbol: "ACME", Price: dec("101.25"), Size: dec("300"),
		EventTime: now.Add(-time.Second), Source: "synthetic", Seq: 1,
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		edit func(*domain.Tick)
		want domain.Reason
	}{
		{"valid", func(*domain.Tick) {}, domain.ReasonNone},
		{"class suffix symbol", func(tk *domain.Tick) { tk.Symbol = "BRK.B" }, domain.ReasonNone},
		{"zero size is valid", func(tk *domain.Tick) { tk.Size = decimal.Zero }, domain.ReasonNone},
		{"exactly 5 s ahead", func(tk *domain.Tick) { tk.EventTime = now.Add(domain.MaxFutureSkew) }, domain.ReasonNone},
		{"exactly at retention", func(tk *domain.Tick) { tk.EventTime = now.Add(-domain.MaxTickAge) }, domain.ReasonNone},
		{"no event id", func(tk *domain.Tick) { tk.EventID = "" }, domain.ReasonMissingField},
		{"no symbol", func(tk *domain.Tick) { tk.Symbol = "" }, domain.ReasonMissingField},
		{"no source", func(tk *domain.Tick) { tk.Source = "" }, domain.ReasonMissingField},
		{"no event time", func(tk *domain.Tick) { tk.EventTime = time.Time{} }, domain.ReasonMissingField},
		{"lower-case symbol", func(tk *domain.Tick) { tk.Symbol = "acme" }, domain.ReasonBadSymbol},
		{"symbol too long", func(tk *domain.Tick) { tk.Symbol = "ABCDEFG" }, domain.ReasonBadSymbol},
		{"two-letter class", func(tk *domain.Tick) { tk.Symbol = "BRK.BB" }, domain.ReasonBadSymbol},
		{"zero price", func(tk *domain.Tick) { tk.Price = decimal.Zero }, domain.ReasonNonpositivePrice},
		{"negative price", func(tk *domain.Tick) { tk.Price = dec("-0.01") }, domain.ReasonNonpositivePrice},
		{"negative size", func(tk *domain.Tick) { tk.Size = dec("-1") }, domain.ReasonNegativeVolume},
		{"future", func(tk *domain.Tick) { tk.EventTime = now.Add(domain.MaxFutureSkew + time.Millisecond) }, domain.ReasonFutureTimestamp},
		{"too old", func(tk *domain.Tick) { tk.EventTime = now.Add(-domain.MaxTickAge - time.Millisecond) }, domain.ReasonTooOld},
		{"first reason wins", func(tk *domain.Tick) { tk.Symbol = "bad"; tk.Price = decimal.Zero }, domain.ReasonBadSymbol},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tk := goodTick()
			tc.edit(&tk)
			if got := domain.Validate(tk, now); got != tc.want {
				t.Errorf("Validate = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestValidate_TimesInOtherZones(t *testing.T) {
	t.Parallel()
	tk := goodTick()
	tk.EventTime = now.In(time.FixedZone("EST", -5*3600)).Add(-time.Second) // same instant, other zone
	if got := domain.Validate(tk, now); got != domain.ReasonNone {
		t.Errorf("Validate = %q for a valid instant in another zone", got)
	}
}
