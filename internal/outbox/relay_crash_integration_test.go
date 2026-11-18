//go:build integration && failpoint

package outbox

import (
	"errors"
	"testing"

	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/testutil/failpoint"
)

// TestRelayPG_CrashBetweenPublishAndMark (task 03.07 §8): the relay dies after publishing and
// before marking. A restarted relay publishes the batch again; the duplicates carry the same
// event_ids, so deduping on event_id restores an exactly-once view.
func TestRelayPG_CrashBetweenPublishAndMark(t *testing.T) {
	e := newPGEnv(t, 120, 4)
	t.Cleanup(failpoint.Reset)
	p := &MemoryPublisher{}
	failpoint.Enable(failpoint.OutboxAfterPublish, "error")
	if err := e.relay(p, 500).Once(t.Context(), scheduler.AlwaysLeader{}); !errors.Is(err, failpoint.ErrInjected) {
		t.Fatalf("crash pass: %v; want the injected failure", err)
	}
	if n := len(p.Messages()); n != 120 || len(e.published(t)) != 0 {
		t.Fatalf("after the crash: %d published, %d marked; want 120 and 0", n, len(e.published(t)))
	}
	failpoint.Disable(failpoint.OutboxAfterPublish)

	if err := e.relay(p, 500).Once(t.Context(), scheduler.AlwaysLeader{}); err != nil { // the restarted relay
		t.Fatal(err)
	}
	msgs := p.Messages()
	counts := map[string]int{}
	for _, m := range msgs {
		counts[m.Headers[HeaderEventID]]++
	}
	duplicates := len(msgs) - len(counts)
	if len(counts) != 120 || duplicates != 120 {
		t.Errorf("%d messages, %d distinct event ids, %d duplicates; want 240, 120, 120", len(msgs), len(counts), duplicates)
	}
	for id, n := range counts {
		if n != 2 {
			t.Errorf("event %s published %d times; want exactly twice (once per relay)", id, n)
		}
	}
	checkOrderAndDedupe(t, msgs, 120) // the deduped view: every event once, in order per run
	if got := len(e.published(t)); got != 120 {
		t.Errorf("%d rows marked; want 120", got)
	}
}
