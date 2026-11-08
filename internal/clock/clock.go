// Package clock is the only way FlowForge code reads time or waits (task 01.04, 00C §8.1,
// ADR-021). Production code receives a Clock (Real in cmd/*/main.go) and tests pass a Fake that
// they advance by hand, so time-dependent logic is deterministic. Lint forbids time.Now, Sleep,
// After, NewTimer and friends outside this package and cmd/**.
package clock

import (
	"context"
	"time"
)

// Clock reads the time and creates timers. Implementations are safe for concurrent use.
type Clock interface {
	Now() time.Time
	// Sleep waits for d or until ctx is done, returning ctx.Err() in the latter case.
	Sleep(ctx context.Context, d time.Duration) error
	NewTimer(d time.Duration) Timer
	NewTicker(d time.Duration) Ticker
	// After is NewTimer(d).C(); the timer can't be stopped, so prefer NewTimer in loops.
	After(d time.Duration) <-chan time.Time
}

// Timer mirrors time.Timer. After Stop or Reset returns, no stale value is received from C
// (Go 1.23 timer semantics).
type Timer interface {
	C() <-chan time.Time
	// Stop prevents the timer from firing; it reports whether the timer was active.
	Stop() bool
	// Reset makes the timer fire d from now; it reports whether the timer was active.
	Reset(d time.Duration) bool
}

// Ticker mirrors time.Ticker: a tick that the reader hasn't taken yet is dropped, not queued.
type Ticker interface {
	C() <-chan time.Time
	Stop()
	Reset(d time.Duration)
}
