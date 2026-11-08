package clock

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var t0 = time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)

// fired returns the value waiting on ch, or the zero time if none.
func fired(ch <-chan time.Time) time.Time {
	select {
	case v := <-ch:
		return v
	default:
		return time.Time{}
	}
}

// TestFake_AdvanceFiresTimersInOrder: Advance sets Now to each deadline as it fires, so each
// value received equals its own deadline, not the end of the Advance.
func TestFake_AdvanceFiresTimersInOrder(t *testing.T) {
	t.Parallel()
	f := NewFake(t0)
	c := f.NewTimer(3 * time.Second)
	a := f.NewTimer(time.Second)
	b := f.NewTimer(2 * time.Second)

	f.Advance(5 * time.Second)
	for name, tc := range map[string]struct {
		tm   Timer
		want time.Duration
	}{"a": {a, time.Second}, "b": {b, 2 * time.Second}, "c": {c, 3 * time.Second}} {
		if got := fired(tc.tm.C()); !got.Equal(t0.Add(tc.want)) {
			t.Errorf("%s fired at %v, want %v", name, got, t0.Add(tc.want))
		}
	}
	if !f.Now().Equal(t0.Add(5*time.Second)) || f.Waiters() != 0 {
		t.Errorf("Now = %v, waiters = %d", f.Now(), f.Waiters())
	}
}

func TestFake_EqualDeadlinesFireInCreationOrder(t *testing.T) {
	t.Parallel()
	f := NewFake(t0)
	first := f.NewTimer(time.Second).(*fakeWaiter)
	second := f.NewTimer(time.Second).(*fakeWaiter)
	if i := f.nextDue(t0.Add(time.Second)); f.waiters[i] != first {
		t.Fatal("equal deadlines must fire in creation order")
	}
	f.Advance(time.Second)
	if fired(first.C()).IsZero() || fired(second.C()).IsZero() {
		t.Error("both timers should have fired")
	}
}

func TestFake_StoppedTimerDoesNotFire(t *testing.T) {
	t.Parallel()
	f := NewFake(t0)
	tm := f.NewTimer(time.Second)
	if !tm.Stop() {
		t.Error("Stop on an active timer should report true")
	}
	f.Advance(time.Hour)
	if !fired(tm.C()).IsZero() {
		t.Error("stopped timer fired")
	}
	if tm.Stop() {
		t.Error("second Stop should report false")
	}
}

func TestFake_ResetDiscardsStaleValue(t *testing.T) {
	t.Parallel()
	f := NewFake(t0)
	tm := f.NewTimer(time.Second)
	f.Advance(time.Second) // fires; value sits unread in the buffer
	if tm.Reset(time.Second) {
		t.Error("Reset after firing should report false (not active)")
	}
	if !fired(tm.C()).IsZero() {
		t.Error("stale value delivered after Reset")
	}
	f.Advance(time.Second)
	if got := fired(tm.C()); !got.Equal(t0.Add(2 * time.Second)) {
		t.Errorf("reset timer fired at %v", got)
	}
	tm2 := f.NewTimer(time.Minute)
	if !tm2.Reset(time.Second) {
		t.Error("Reset of an active timer should report true")
	}
}

func TestFake_TickerDeliversNTicks(t *testing.T) {
	t.Parallel()
	f := NewFake(t0)
	tk := f.NewTicker(10 * time.Second)
	const n = 5
	for i := 1; i <= n; i++ {
		f.Advance(10 * time.Second)
		if got := fired(tk.C()); !got.Equal(t0.Add(time.Duration(i) * 10 * time.Second)) {
			t.Fatalf("tick %d at %v", i, got)
		}
	}
	// Ticks nobody reads are dropped, like time.Ticker: 3 periods leave one buffered tick.
	f.Advance(30 * time.Second)
	if fired(tk.C()).IsZero() || !fired(tk.C()).IsZero() {
		t.Error("want exactly one buffered tick after unread periods")
	}
	tk.Reset(time.Second)
	f.Advance(time.Second)
	if fired(tk.C()).IsZero() {
		t.Error("ticker did not tick after Reset")
	}
	tk.Stop()
	f.Advance(time.Hour)
	if !fired(tk.C()).IsZero() || f.Waiters() != 0 {
		t.Error("stopped ticker ticked")
	}
}

func TestFake_TickerPanicsOnNonPositive(t *testing.T) {
	t.Parallel()
	f := NewFake(t0)
	for name, fn := range map[string]func(){
		"NewTicker": func() { f.NewTicker(0) },
		"Reset":     func() { f.NewTicker(time.Second).Reset(-1) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: want panic", name)
				}
			}()
			fn()
		}()
	}
}

func TestFake_SleepAndCancellation(t *testing.T) {
	t.Parallel()
	f := NewFake(t0)
	done := make(chan error, 1)
	go func() { done <- f.Sleep(context.Background(), time.Minute) }()
	f.BlockUntilWaiters(1)
	f.Advance(time.Minute)
	if err := <-done; err != nil {
		t.Errorf("Sleep = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { done <- f.Sleep(ctx, time.Minute) }()
	f.BlockUntilWaiters(1)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled Sleep = %v", err)
	}
	if f.Waiters() != 0 {
		t.Error("cancelled Sleep left its timer armed")
	}
}

func TestFake_ImmediateAfterAndSet(t *testing.T) {
	t.Parallel()
	f := NewFake(t0)
	if got := fired(f.After(0)); !got.Equal(t0) {
		t.Errorf("After(0) = %v, want immediate", got)
	}
	ch := f.After(time.Hour)
	f.Set(t0.Add(-time.Hour)) // backwards: ignored
	f.Advance(-time.Hour)     // negative: ignored
	if !f.Now().Equal(t0) {
		t.Errorf("Now moved backwards to %v", f.Now())
	}
	f.Set(t0.Add(2 * time.Hour))
	if got := fired(ch); !got.Equal(t0.Add(time.Hour)) {
		t.Errorf("After fired at %v", got)
	}
}

func TestFake_ConcurrentUse(t *testing.T) {
	t.Parallel()
	f := NewFake(t0)
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			tm := f.NewTimer(time.Second)
			_ = f.Now()
			tm.Reset(2 * time.Second)
			tm.Stop()
		})
		wg.Go(func() { f.Advance(time.Millisecond) })
	}
	wg.Wait()
}

func TestReal(t *testing.T) {
	t.Parallel()
	var c Clock = Real{}
	before := c.Now()
	if err := c.Sleep(context.Background(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if !c.Now().After(before) {
		t.Error("real time did not advance")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled Sleep = %v", err)
	}
	tm := c.NewTimer(time.Millisecond)
	<-tm.C()
	if tm.Reset(time.Hour) || !tm.Stop() {
		t.Error("real timer Reset/Stop results wrong")
	}
	tk := c.NewTicker(time.Millisecond)
	<-tk.C()
	tk.Reset(time.Millisecond)
	<-tk.C()
	tk.Stop()
	<-c.After(time.Millisecond)
}
