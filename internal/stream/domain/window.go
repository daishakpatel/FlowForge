package domain

import (
	"fmt"
	"time"
)

// WindowSize is a tumbling window length, stored as market_aggregates.window_size.
type WindowSize string

// The window sizes (FR-KAFKA-08).
const (
	Window1m WindowSize = "1m"
	Window5m WindowSize = "5m"
)

// WindowSizes returns every window size, shortest first.
func WindowSizes() []WindowSize { return []WindowSize{Window1m, Window5m} }

// Duration returns the window's length, or 0 for an unknown size.
func (s WindowSize) Duration() time.Duration {
	switch s {
	case Window1m:
		return time.Minute
	case Window5m:
		return 5 * time.Minute
	}
	return 0
}

// ParseWindowSize accepts "1m" and "5m".
func ParseWindowSize(s string) (WindowSize, error) {
	if WindowSize(s).Duration() == 0 {
		return "", fmt.Errorf("unknown window size %q (want 1m or 5m)", s)
	}
	return WindowSize(s), nil
}

// Window is the half-open event-time interval [Start, Start+Size) of one tumbling window.
type Window struct {
	Size  WindowSize
	Start time.Time // UTC, on a multiple of Size since the Unix epoch
}

// WindowFor returns the window of size that contains t. Windows are aligned to UTC epoch
// boundaries, whatever t's zone; times before the epoch round down, not toward zero. Go time has
// no leap seconds, so neither do windows.
func WindowFor(t time.Time, size WindowSize) (Window, error) {
	d := size.Duration()
	if d == 0 {
		return Window{}, fmt.Errorf("unknown window size %q", size)
	}
	// Truncate rounds down relative to the zero time (year 1), which lies a whole number of days
	// before the Unix epoch; 1m and 5m divide a day, so this is epoch alignment.
	return Window{Size: size, Start: t.UTC().Truncate(d)}, nil
}

// End returns the first instant after the window.
func (w Window) End() time.Time { return w.Start.Add(w.Size.Duration()) }

// Contains reports whether t lies in [Start, End).
func (w Window) Contains(t time.Time) bool { return !t.Before(w.Start) && t.Before(w.End()) }
