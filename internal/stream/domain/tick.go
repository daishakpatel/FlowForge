// Package domain is the pure market-data domain (task 05.02, FR-KAFKA-08..10): the tick model and
// its validation, event-time windows, order-independent OHLCV/VWAP aggregation with an input hash,
// the moving average, rolling volatility and z-score anomaly indicators, and the watermark/lateness
// policy. Formulas and worked examples: docs/architecture/market-data-math.md.
//
// The package is pure (00C §8.1, lint-enforced): no I/O, clock, randomness or FlowForge application
// packages. The current time is always passed in, so the same inputs always give the same outputs,
// in a live run and in a replay alike. Prices and sizes are exact decimals (shopspring/decimal),
// never float64; only the statistics computed from log returns (volatility, z-score) are float64.
package domain

import (
	"regexp"
	"time"

	"github.com/shopspring/decimal"
)

// Tick is one trade, as carried by a market.tick event (schema market_tick.v1, 00E §7) and stored
// in market_ticks.
type Tick struct {
	EventID   string          // globally unique; the dedupe key together with EventTime
	Symbol    string          // ^[A-Z]{1,6}(\.[A-Z])?$
	Price     decimal.Decimal // > 0
	Size      decimal.Decimal // traded quantity, >= 0 (`size`: the window's volume is the sum)
	EventTime time.Time       // when the trade happened; windows use it, never the arrival time
	Source    string          // e.g. synthetic, csv
	Seq       int64           // the source's sequence number; breaks ties between equal EventTimes
}

// Reason says why a tick or event was rejected. It is the market.invalid DLQ reason
// (market_invalid.v1), so its values are a contract: add new ones, never rename.
type Reason string

// The validation reasons, plus ReasonTooLate from the lateness policy (Classify).
const (
	ReasonNone             Reason = ""
	ReasonMissingField     Reason = "missing_field"
	ReasonBadSymbol        Reason = "bad_symbol"
	ReasonNonpositivePrice Reason = "nonpositive_price"
	ReasonNegativeVolume   Reason = "negative_volume"
	ReasonFutureTimestamp  Reason = "future_timestamp"
	ReasonTooOld           Reason = "too_old"
	ReasonTooLate          Reason = "too_late"
)

// Validation bounds.
const (
	// MaxFutureSkew is how far ahead of the receiver's clock an event time may be (00E §7).
	MaxFutureSkew = 5 * time.Second
	// MaxTickAge is the tick retention (migration 0004): an older tick could never be stored.
	MaxTickAge = 14 * 24 * time.Hour
)

var symbolRE = regexp.MustCompile(`^[A-Z]{1,6}(\.[A-Z])?$`)

// Validate returns the first reason t is invalid at time now, in this order: missing_field,
// bad_symbol, nonpositive_price, negative_volume, future_timestamp, too_old; or ReasonNone. A zero
// size is valid (the VWAP of a zero-volume window is its close).
func Validate(t Tick, now time.Time) Reason {
	switch {
	case t.EventID == "" || t.Symbol == "" || t.Source == "" || t.EventTime.IsZero():
		return ReasonMissingField
	case !symbolRE.MatchString(t.Symbol):
		return ReasonBadSymbol
	case !t.Price.IsPositive():
		return ReasonNonpositivePrice
	case t.Size.IsNegative():
		return ReasonNegativeVolume
	case t.EventTime.After(now.Add(MaxFutureSkew)):
		return ReasonFutureTimestamp
	case t.EventTime.Before(now.Add(-MaxTickAge)):
		return ReasonTooOld
	}
	return ReasonNone
}
