package clock

import (
	"context"
	"slices"
	"sync"
	"time"
)

// Fake is a manually advanced Clock for tests. Time moves only through Advance and Set.
//
// Timers and tickers fire in deadline order, and in creation order for equal deadlines. Advance
// fires every due timer synchronously before returning, setting Now to each deadline as it
// fires, so a test sees the same order on every run. Channels have a one-value buffer, like
// time.Timer: a ticker tick nobody has read is dropped. Fake starts no goroutines and is safe
// for concurrent use.
type Fake struct {
	mu      sync.Mutex
	cond    *sync.Cond // broadcast when a waiter is added, for BlockUntilWaiters
	now     time.Time
	seq     uint64 // creation order, the tie-break for equal deadlines
	waiters []*fakeWaiter
}

var _ Clock = (*Fake)(nil)

// NewFake returns a Fake whose Now is start.
func NewFake(start time.Time) *Fake {
	f := &Fake{now: start}
	f.cond = sync.NewCond(&f.mu)
	return f
}

// fakeWaiter is a pending timer or ticker. period is 0 for timers.
type fakeWaiter struct {
	f        *Fake
	ch       chan time.Time
	deadline time.Time
	period   time.Duration
	seq      uint64
}

// Now returns the fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves time forward by d, firing due timers in order. A negative d is ignored.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.advanceTo(f.now.Add(max(d, 0)))
}

// Set moves time to t if t is later than Now, firing due timers in order.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t.After(f.now) {
		f.advanceTo(t)
	}
}

// Waiters returns the number of active timers and tickers, including pending Sleep calls.
func (f *Fake) Waiters() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.waiters)
}

// BlockUntilWaiters blocks until at least n timers, tickers or sleeps are active. Tests call it
// before Advance to know that a goroutine has started waiting.
func (f *Fake) BlockUntilWaiters(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for len(f.waiters) < n {
		f.cond.Wait()
	}
}

// Sleep waits until the fake time has advanced by d, or ctx is done.
func (f *Fake) Sleep(ctx context.Context, d time.Duration) error {
	t := f.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C():
		return nil
	}
}

// NewTimer returns a timer that fires once d after the current fake time. d <= 0 fires at once.
func (f *Fake) NewTimer(d time.Duration) Timer { return f.add(d, 0) }

// NewTicker returns a ticker with period d. It panics if d <= 0, like time.NewTicker.
func (f *Fake) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive interval for Fake.NewTicker")
	}
	return &fakeTicker{f.add(d, d)}
}

// After is NewTimer(d).C().
func (f *Fake) After(d time.Duration) <-chan time.Time { return f.NewTimer(d).C() }

func (f *Fake) add(d, period time.Duration) *fakeWaiter {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	w := &fakeWaiter{f: f, ch: make(chan time.Time, 1), period: period, seq: f.seq}
	f.schedule(w, d)
	return w
}

// schedule (re)arms w to fire d after now; a due timer fires immediately. Caller holds f.mu.
func (f *Fake) schedule(w *fakeWaiter, d time.Duration) {
	w.deadline = f.now.Add(d)
	if d <= 0 && w.period == 0 {
		send(w.ch, f.now)
		return
	}
	f.waiters = append(f.waiters, w)
	f.cond.Broadcast()
}

// advanceTo fires every waiter due by target in (deadline, seq) order. Caller holds f.mu.
func (f *Fake) advanceTo(target time.Time) {
	for {
		i := f.nextDue(target)
		if i < 0 {
			break
		}
		w := f.waiters[i]
		f.now = w.deadline
		send(w.ch, f.now)
		if w.period > 0 {
			w.deadline = w.deadline.Add(w.period)
		} else {
			f.waiters = slices.Delete(f.waiters, i, i+1)
		}
	}
	f.now = target
}

// nextDue returns the index of the earliest waiter due by target, or -1.
func (f *Fake) nextDue(target time.Time) int {
	best := -1
	for i, w := range f.waiters {
		if w.deadline.After(target) {
			continue
		}
		if best < 0 || w.deadline.Before(f.waiters[best].deadline) ||
			(w.deadline.Equal(f.waiters[best].deadline) && w.seq < f.waiters[best].seq) {
			best = i
		}
	}
	return best
}

// remove disarms w and drains a value not yet read; it reports whether w was active.
func (f *Fake) remove(w *fakeWaiter) bool {
	select {
	case <-w.ch:
	default:
	}
	i := slices.Index(f.waiters, w)
	if i < 0 {
		return false
	}
	f.waiters = slices.Delete(f.waiters, i, i+1)
	return true
}

// send delivers t without blocking; a full one-value buffer drops it, like a time.Ticker.
func send(ch chan time.Time, t time.Time) {
	select {
	case ch <- t:
	default:
	}
}

func (w *fakeWaiter) C() <-chan time.Time { return w.ch }

func (w *fakeWaiter) Stop() bool {
	w.f.mu.Lock()
	defer w.f.mu.Unlock()
	return w.f.remove(w)
}

func (w *fakeWaiter) Reset(d time.Duration) bool {
	w.f.mu.Lock()
	defer w.f.mu.Unlock()
	active := w.f.remove(w)
	w.f.schedule(w, d)
	return active
}

type fakeTicker struct{ w *fakeWaiter }

func (t *fakeTicker) C() <-chan time.Time { return t.w.ch }
func (t *fakeTicker) Stop()               { t.w.Stop() }

// Reset changes the period to d, counted from the current fake time. It panics if d <= 0.
func (t *fakeTicker) Reset(d time.Duration) {
	if d <= 0 {
		panic("clock: non-positive interval for Fake ticker Reset")
	}
	t.w.f.mu.Lock()
	defer t.w.f.mu.Unlock()
	t.w.f.remove(t.w)
	t.w.period = d
	t.w.f.schedule(t.w, d)
}
