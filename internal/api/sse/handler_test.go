package sse

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

func TestMain(m *testing.M) { testutil.LeakCheck(m) }

// memEvents is an in-memory workflow_events table; endless makes Stream invent events forever.
// A hidden id is inserted but not committed yet: invisible to reads, like an in-flight transaction.
type memEvents struct {
	mu        sync.Mutex
	evs       []persistence.EventView
	endless   int // payload size of invented events; 0 = off
	hidden    map[int64]bool
	lastAfter int64 // the afterID of the last Stream call
}

// setHidden makes id invisible (in flight) or visible (committed).
func (s *memEvents) setHidden(id int64, hidden bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hidden == nil {
		s.hidden = map[int64]bool{}
	}
	s.hidden[id] = hidden
}

// age makes every event so far look created d ago.
func (s *memEvents) age(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.evs {
		s.evs[i].CreatedAt = s.evs[i].CreatedAt.Add(-d)
	}
}

func (s *memEvents) add(run string, typ statemachine.EventType) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := int64(len(s.evs) + 1)
	s.evs = append(s.evs, persistence.EventView{
		ID: id, EventID: fmt.Sprintf("evt_%03d", id), RunID: run, Type: typ, Version: 1,
		Payload: json.RawMessage(`{"n":` + strconv.FormatInt(id, 10) + `}`), CorrelationID: run, CreatedAt: clock.Real{}.Now(),
	})
}

func (s *memEvents) Stream(_ context.Context, _ persistence.Querier, f persistence.EventStreamFilter, after, upTo int64, limit int) ([]persistence.EventView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastAfter = after
	if s.endless > 0 {
		out := make([]persistence.EventView, limit)
		for i := range out {
			out[i] = persistence.EventView{ID: after + int64(i) + 1, Type: statemachine.EventTaskQueued, RunID: "run_x",
				Payload: json.RawMessage(`"` + strings.Repeat("x", s.endless) + `"`)}
		}
		return out, nil
	}
	var out []persistence.EventView
	for _, e := range s.evs {
		if e.ID > after && e.ID <= upTo && !s.hidden[e.ID] && (f.RunID == "" || e.RunID == f.RunID) &&
			(f.Types == nil || slices.Contains(f.Types, string(e.Type))) && len(out) < limit {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *memEvents) Marks(_ context.Context, _ persistence.Querier, after int64, limit int) ([]persistence.EventMark, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.endless > 0 {
		out := make([]persistence.EventMark, limit)
		for i := range out {
			out[i] = persistence.EventMark{ID: after + int64(i) + 1, CreatedAt: clock.Real{}.Now()}
		}
		return out, nil
	}
	var out []persistence.EventMark
	for _, e := range s.evs {
		if e.ID > after && !s.hidden[e.ID] && len(out) < limit {
			out = append(out, persistence.EventMark{ID: e.ID, CreatedAt: e.CreatedAt})
		}
	}
	return out, nil
}

func (s *memEvents) LastID(context.Context, persistence.Querier) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.evs)), nil
}

// bell is a Waker that tests ring.
type bell struct {
	mu sync.Mutex
	ch chan struct{}
}

func newBell() *bell { return &bell{ch: make(chan struct{})} }

func (b *bell) Wait() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ch
}

func (b *bell) ring() {
	b.mu.Lock()
	defer b.mu.Unlock()
	close(b.ch)
	b.ch = make(chan struct{})
}

type fixture struct {
	src  *memEvents
	bell *bell
	h    *Handler
	srv  *httptest.Server
}

// newFixture serves Stream with the query from the URL (run_id, types) and Last-Event-ID; a
// refused stream answers 503.
func newFixture(t *testing.T, limit int, change func(*Handler)) *fixture {
	t.Helper()
	f := &fixture{src: &memEvents{}, bell: newBell()}
	f.h = &Handler{Hub: NewHub(f.bell, limit), Events: f.src, Clock: clock.Real{}, Poll: time.Minute, KeepAlive: time.Minute}
	if change != nil {
		change(f.h)
	}
	f.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := Query{RunID: r.URL.Query().Get("run_id")}
		if v := r.URL.Query().Get("types"); v != "" {
			q.Types = strings.Split(v, ",")
		}
		if v := r.Header.Get("Last-Event-ID"); v != "" {
			id, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			q.LastEventID, q.Resume = id, true
		}
		if err := f.h.Stream(w, r, q); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		}
	}))
	return f
}

func (f *fixture) start(t *testing.T) {
	t.Helper()
	f.srv.Start()
	t.Cleanup(f.srv.Close)
}

// frame is one SSE message or comment.
type frame struct {
	id, event, data, comment string
}

type client struct {
	resp   *http.Response
	frames chan frame
	cancel context.CancelFunc
}

// connect opens a stream and parses frames in the background until the body ends.
func (f *fixture) connect(t *testing.T, query string, header ...string) *client {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.srv.URL+"/?"+query, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	c := &client{resp: resp, frames: make(chan frame, 1024), cancel: cancel}
	t.Cleanup(func() { cancel(); _ = resp.Body.Close() })
	go c.read()
	return c
}

func (c *client) read() {
	defer close(c.frames)
	sc := bufio.NewScanner(c.resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var fr frame
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			c.frames <- fr
			fr = frame{}
		case strings.HasPrefix(line, ":"):
			fr.comment = strings.TrimSpace(line[1:])
		default:
			k, v, _ := strings.Cut(line, ": ")
			switch k {
			case "id":
				fr.id = v
			case "event":
				fr.event = v
			case "data":
				fr.data = v
			}
		}
	}
}

// next returns the next event frame, skipping comments, or fails after 5 s.
func (c *client) next(t *testing.T) frame {
	t.Helper()
	timeout := clock.Real{}.After(5 * time.Second) // a real-time bound on a real network read
	for {
		select {
		case fr, ok := <-c.frames:
			if !ok {
				t.Fatal("stream ended")
			}
			if fr.comment == "" {
				return fr
			}
		case <-timeout:
			t.Fatal("no event within 5 s")
		}
	}
}

func (c *client) ids(t *testing.T, n int) []string {
	t.Helper()
	out := make([]string, n)
	for i := range out {
		out[i] = c.next(t).id
	}
	return out
}

func TestStream_RunHistoryInOrderThenLive(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 10, nil)
	f.start(t)
	f.src.add("run_a", statemachine.EventRunCreated)
	f.src.add("run_b", statemachine.EventRunCreated)
	f.src.add("run_a", statemachine.EventRunStarted)
	c := f.connect(t, "run_id=run_a")
	if ct := c.resp.Header.Get("Content-Type"); c.resp.StatusCode != 200 || ct != "text/event-stream" {
		t.Fatalf("status %d, content type %q", c.resp.StatusCode, ct)
	}
	first := c.next(t)
	if first.id != "1" || first.event != "run.created" {
		t.Fatalf("first frame %+v", first)
	}
	var env struct {
		EventID   string          `json:"event_id"`
		EventType string          `json:"event_type"`
		Payload   json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal([]byte(first.data), &env); err != nil || env.EventID != "evt_001" || env.EventType != "run.created" || string(env.Payload) != `{"n":1}` {
		t.Errorf("data %q is not the envelope (%v)", first.data, err)
	}
	if got := c.next(t).id; got != "3" {
		t.Fatalf("second frame id %s; want 3 (run_b filtered out)", got)
	}
	f.src.add("run_a", statemachine.EventTaskReady)
	f.bell.ring()
	if got := c.next(t).id; got != "4" {
		t.Errorf("live frame id %s; want 4", got)
	}
}

func TestStream_ResumeFromLastEventIDHasNoGapsOrDuplicates(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 10, nil)
	f.start(t)
	for range 5 {
		f.src.add("run_a", statemachine.EventTaskQueued)
	}
	c := f.connect(t, "run_id=run_a", "Last-Event-ID", "2")
	if got := c.ids(t, 3); !slices.Equal(got, []string{"3", "4", "5"}) {
		t.Errorf("resumed after 2: %v; want [3 4 5]", got)
	}
}

func TestStream_TypeFilterAndLiveStart(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 10, nil)
	f.start(t)
	f.src.add("run_a", statemachine.EventTaskQueued) // before the stream: not replayed without run_id
	c := f.connect(t, "types=task.failed,run.failed")
	testutil.WaitFor(t, func() bool { return f.h.Hub.Open() == 1 }, 5*time.Second)
	f.src.add("run_a", statemachine.EventTaskQueued)
	f.src.add("run_b", statemachine.EventTaskFailed)
	f.bell.ring()
	if fr := c.next(t); fr.id != "3" || fr.event != "task.failed" {
		t.Errorf("filtered live frame %+v; want id 3 task.failed", fr)
	}
}

// TestStream_ConnectionLimit: with the default limit of 200, the 201st stream is refused and a
// closed stream frees its slot.
func TestStream_ConnectionLimit(t *testing.T) {
	t.Parallel()
	f := newFixture(t, DefaultMaxStreams, nil)
	f.start(t)
	clients := make([]*client, DefaultMaxStreams)
	for i := range clients {
		clients[i] = f.connect(t, "")
		if clients[i].resp.StatusCode != 200 {
			t.Fatalf("stream %d: status %d", i+1, clients[i].resp.StatusCode)
		}
	}
	if c := f.connect(t, ""); c.resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("stream %d: status %d; want 503", DefaultMaxStreams+1, c.resp.StatusCode)
	}
	clients[0].cancel()
	testutil.WaitFor(t, func() bool { return f.h.Hub.Open() < DefaultMaxStreams }, 5*time.Second)
	if c := f.connect(t, ""); c.resp.StatusCode != 200 {
		t.Errorf("after one closed: status %d; want 200", c.resp.StatusCode)
	}
}

func TestStream_KeepAlive(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 10, func(h *Handler) { h.KeepAlive = 30 * time.Millisecond })
	f.start(t)
	c := f.connect(t, "")
	select {
	case fr := <-c.frames:
		if fr.comment != "keep-alive" {
			t.Errorf("first frame %+v; want a keep-alive comment", fr)
		}
	case <-clock.Real{}.After(5 * time.Second):
		t.Fatal("no keep-alive")
	}
}

// TestStream_SlowClientIsDropped: a client that stops reading is cut off once a write blocks
// longer than WriteTimeout, and its slot is freed.
func TestStream_SlowClientIsDropped(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 10, func(h *Handler) { h.WriteTimeout = 100 * time.Millisecond })
	f.start(t)
	f.src.endless = 64 << 10
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", f.srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := io.WriteString(conn, "GET /?run_id=run_x HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	testutil.WaitFor(t, func() bool { return f.h.Hub.Open() == 1 }, 5*time.Second)
	// Never read: the server fills the socket buffers, then a write blocks past the deadline.
	testutil.WaitFor(t, func() bool { return f.h.Hub.Open() == 0 }, 10*time.Second)
}

// TestStream_OutlivesServerTimeouts: a server with short read and write timeouts (as
// flowforge-api has) does not cut the stream.
func TestStream_OutlivesServerTimeouts(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 10, func(h *Handler) { h.KeepAlive = 50 * time.Millisecond })
	f.srv.Config.ReadTimeout, f.srv.Config.WriteTimeout = 200*time.Millisecond, 200*time.Millisecond
	f.start(t)
	c := f.connect(t, "")
	deadline := clock.Real{}.After(time.Second)
	for n := 0; ; n++ {
		select {
		case _, ok := <-c.frames:
			if !ok {
				t.Fatalf("stream ended after %d frames", n)
			}
		case <-deadline:
			return // still streaming past both timeouts
		}
	}
}

func TestHub_Limits(t *testing.T) {
	t.Parallel()
	h := NewHub(newBell(), 2)
	first, second, third := h.acquire(), h.acquire(), h.acquire()
	if !first || !second || third {
		t.Fatalf("hub of 2 admitted %v, %v, %v; want true, true, false", first, second, third)
	}
	h.release()
	if h.Open() != 1 || !h.acquire() {
		t.Error("a released slot must be reusable")
	}
}

// TestStream_StaysBehindAnInFlightLowerID: id 1 is inserted but not committed when id 2 commits.
// The stream must not move past 1, or 1 would be lost once it commits (ids are assigned at insert).
func TestStream_StaysBehindAnInFlightLowerID(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 10, func(h *Handler) { h.Poll = 20 * time.Millisecond })
	f.start(t)
	f.src.add("run_a", statemachine.EventTaskQueued)
	f.src.add("run_a", statemachine.EventTaskQueued)
	f.src.setHidden(1, true)
	c := f.connect(t, "run_id=run_a")
	select {
	case fr := <-c.frames:
		t.Fatalf("frame %+v delivered while id 1 is in flight", fr)
	case <-clock.Real{}.After(300 * time.Millisecond):
	}
	f.src.setHidden(1, false) // the slow transaction commits
	if got := c.ids(t, 2); !slices.Equal(got, []string{"1", "2"}) {
		t.Errorf("after the commit: %v; want [1 2]", got)
	}
}

// TestStream_SkipsARolledBackID: an id that never commits (a rolled-back transaction) holds the
// stream back only for GapTimeout.
func TestStream_SkipsARolledBackID(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 10, func(h *Handler) { h.Poll, h.GapTimeout = 20*time.Millisecond, 100*time.Millisecond })
	f.start(t)
	f.src.add("run_a", statemachine.EventTaskQueued)
	f.src.add("run_a", statemachine.EventTaskQueued)
	f.src.setHidden(1, true) // never committed
	c := f.connect(t, "run_id=run_a")
	if got := c.next(t).id; got != "2" {
		t.Errorf("after the gap timed out: %s; want 2", got)
	}
}

// TestStream_OldGapsAreSettled: a gap before old rows (retention deleted them, or a rollback long
// ago) does not hold the stream back at all.
func TestStream_OldGapsAreSettled(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 10, func(h *Handler) { h.GapTimeout = time.Hour })
	f.start(t)
	for range 3 {
		f.src.add("run_a", statemachine.EventTaskQueued)
	}
	f.src.setHidden(1, true)
	f.src.age(time.Hour)
	c := f.connect(t, "run_id=run_a")
	if got := c.ids(t, 2); !slices.Equal(got, []string{"2", "3"}) {
		t.Errorf("history past an old gap: %v; want [2 3]", got)
	}
}

// TestStream_FilteredStreamAdvancesPastNonMatching: events the filter drops still move the cursor,
// so a stream waiting for a rare type does not rescan them on every wake.
func TestStream_FilteredStreamAdvancesPastNonMatching(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 10, nil)
	f.start(t)
	c := f.connect(t, "types=run.failed")
	testutil.WaitFor(t, func() bool { return f.h.Hub.Open() == 1 }, 5*time.Second)
	for range 50 {
		f.src.add("run_a", statemachine.EventTaskQueued)
	}
	f.src.add("run_a", statemachine.EventRunFailed)
	f.bell.ring()
	if fr := c.next(t); fr.id != "51" {
		t.Fatalf("frame %+v; want id 51", fr)
	}
	f.src.add("run_a", statemachine.EventTaskQueued)
	f.bell.ring()
	testutil.WaitFor(t, func() bool {
		f.src.mu.Lock()
		defer f.src.mu.Unlock()
		return f.src.lastAfter >= 51
	}, 5*time.Second)
}

// TestHub_CloseEndsStreams: shutting down ends open streams, so the server can drain.
func TestHub_CloseEndsStreams(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 10, nil)
	f.start(t)
	c := f.connect(t, "")
	testutil.WaitFor(t, func() bool { return f.h.Hub.Open() == 1 }, 5*time.Second)
	f.h.Hub.Close()
	select {
	case _, ok := <-c.frames:
		if ok {
			t.Fatal("unexpected frame")
		}
	case <-clock.Real{}.After(5 * time.Second):
		t.Fatal("stream still open after Close")
	}
	if f.h.Hub.Open() != 0 {
		t.Errorf("%d streams open after Close", f.h.Hub.Open())
	}
	if c2 := f.connect(t, ""); c2.resp.StatusCode != 503 {
		t.Errorf("a stream after Close: %d; want refused", c2.resp.StatusCode)
	}
}
