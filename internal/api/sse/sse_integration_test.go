//go:build integration

package sse

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/workerapi"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

type pgStream struct {
	*fixture
	pool *pgxpool.Pool
	tm   *pg.TxManager
	rec  *events.Recorder
	seq  int
}

// newPGStream is the fixture over Postgres, woken only by a real LISTEN ff_events listener: Poll
// is a minute, so an event that arrives within the latency bound came through NOTIFY.
func newPGStream(t *testing.T) *pgStream {
	t.Helper()
	db := testutil.NewFlowforgeDB(t)
	pool, err := pg.NewPool(t.Context(), config.Secret(db.DSN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	waker := workerapi.NewChannelWaker(func(ctx context.Context) (*pgx.Conn, error) {
		return pgx.Connect(ctx, db.DSN)
	}, clock.Real{}, events.NotifyChannel)
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	done := make(chan struct{})
	go func() { defer close(done); _ = waker.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	f := newFixture(t, 10, nil)
	f.h.Hub, f.h.DB, f.h.Events = NewHub(waker, 10), pool, pg.EventRepo{}
	f.start(t)
	return &pgStream{fixture: f, pool: pool, tm: pg.NewTxManager(pool, clock.Real{}, nil),
		rec: events.NewRecorder(clock.Real{}, ids.NewGenerator(clock.Real{}, crand.Reader), "test")}
}

// record commits one task event of run, as a state transition would, and returns its task id.
func (s *pgStream) record(t *testing.T, run string) string {
	t.Helper()
	s.seq++
	task := fmt.Sprintf("task_%04d", s.seq)
	if err := s.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		return s.rec.Record(t.Context(), tx, events.NewTaskEvent(statemachine.EventTaskQueued,
			events.TaskPayload{RunID: run, TaskID: task, TaskKey: "k", Status: model.TaskQueued}))
	}); err != nil {
		t.Fatal(err)
	}
	return task
}

func taskOf(t *testing.T, fr frame) string {
	t.Helper()
	var env struct {
		AggregateID string `json:"aggregate_id"`
		EventType   string `json:"event_type"`
	}
	if err := json.Unmarshal([]byte(fr.data), &env); err != nil {
		t.Fatalf("data %q: %v", fr.data, err)
	}
	if env.EventType != fr.event {
		t.Errorf("event %q but envelope type %q", fr.event, env.EventType)
	}
	return env.AggregateID
}

// TestStreamPG_RunEventsInOrderAndResume: a client receives a run's events in order, with the
// outbox envelope; after reconnecting with Last-Event-ID it gets exactly the newer events, those
// recorded while it was away included.
func TestStreamPG_RunEventsInOrderAndResume(t *testing.T) {
	s := newPGStream(t)
	var want []string
	for range 3 {
		want = append(want, s.record(t, "run_a"))
		s.record(t, "run_b") // filtered out
	}
	c := s.connect(t, "run_id=run_a")
	var last string
	for i := range 6 {
		if i == 3 {
			want = append(want, s.record(t, "run_a"), s.record(t, "run_a"), s.record(t, "run_a"))
		}
		fr := c.next(t)
		if got := taskOf(t, fr); got != want[i] {
			t.Fatalf("frame %d is %s; want %s", i, got, want[i])
		}
		last = fr.id
	}
	c.cancel()
	away := []string{s.record(t, "run_a"), s.record(t, "run_a")}
	c = s.connect(t, "run_id=run_a", "Last-Event-ID", last)
	got := []string{taskOf(t, c.next(t)), taskOf(t, c.next(t))}
	live := s.record(t, "run_a")
	got = append(got, taskOf(t, c.next(t)))
	if want := append(away, live); !slices.Equal(got, want) {
		t.Errorf("after resuming from %s: %v; want %v (no gaps, no duplicates)", last, got, want)
	}
}

// TestStreamPG_NotifyLatency: transition commit → client receipt p95 < 1 s (task 03.07 §8), with
// polling at a minute, so every event arrives through NOTIFY ff_events.
func TestStreamPG_NotifyLatency(t *testing.T) {
	s := newPGStream(t)
	c := s.connect(t, "run_id=run_l")
	testutil.WaitFor(t, func() bool { return s.h.Hub.Open() == 1 }, 5*time.Second)
	// The listener connects asynchronously: wait until a notification gets through.
	warm := s.record(t, "run_l")
	if got := taskOf(t, c.next(t)); got != warm {
		t.Fatalf("warm-up frame %s; want %s", got, warm)
	}
	const n = 50
	lat := make([]time.Duration, n)
	for i := range n {
		start := clock.Real{}.Now()
		task := s.record(t, "run_l")
		fr := c.next(t)
		lat[i] = clock.Real{}.Now().Sub(start)
		if got := taskOf(t, fr); got != task {
			t.Fatalf("frame %d is %s; want %s", i, got, task)
		}
	}
	slices.Sort(lat)
	p95 := lat[n*95/100-1]
	t.Logf("NOTIFY latency over %d events: p50 %s, p95 %s, max %s", n, lat[n/2], p95, lat[n-1])
	if p95 >= time.Second {
		t.Errorf("p95 latency %s; want < 1 s", p95)
	}
}
