package domain

import "time"

// Lateness policy defaults (FR-KAFKA-09, ADR-011).
const (
	// DefaultAllowedLateness is how far the watermark trails the latest event time seen.
	DefaultAllowedLateness = 30 * time.Second
	// DefaultMaxLateness is how far behind the watermark a window may end and still take a late
	// tick (FF_STREAM_MAX_LATENESS, added by the processor in 05.04).
	DefaultMaxLateness = 10 * time.Minute
)

// Lateness is what happens to a tick, judged by its window's end against the watermark.
type Lateness int

// The lateness decisions.
const (
	OnTime        Lateness = iota // its window is still open: aggregate normally
	LateRecompute                 // its window closed, within max lateness: store it and recompute the window
	TooLate                       // beyond max lateness: not stored; DLQ with ReasonTooLate and counted (a decision, not a drop)
)

func (l Lateness) String() string {
	switch l {
	case OnTime:
		return "on_time"
	case LateRecompute:
		return "late_recompute"
	case TooLate:
		return "too_late"
	}
	return "unknown"
}

// Watermark returns maxEventTime - allowedLateness: event-time windows ending at or before it are
// closed.
func Watermark(maxEventTime time.Time, allowedLateness time.Duration) time.Time {
	return maxEventTime.Add(-allowedLateness)
}

// Classify decides a tick's fate from its window's end and the current watermark. A window ending
// after the watermark is open (OnTime); one that ended at most maxLateness before it takes the
// tick and is recomputed (LateRecompute); an older one rejects it (TooLate). It uses event time
// only, never the wall clock, so a replay of the same events classifies them as the live run did.
func Classify(windowEnd, watermark time.Time, maxLateness time.Duration) Lateness {
	switch {
	case windowEnd.After(watermark):
		return OnTime
	case watermark.Sub(windowEnd) <= maxLateness:
		return LateRecompute
	}
	return TooLate
}
