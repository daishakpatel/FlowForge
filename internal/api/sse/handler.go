package sse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// Stream defaults (task 03.07 §4).
const (
	DefaultPoll         = 500 * time.Millisecond
	DefaultKeepAlive    = 15 * time.Second
	DefaultWriteTimeout = 10 * time.Second
	DefaultBatch        = 200
	// DefaultGapTimeout is how long a stream waits for a missing event id to commit before it
	// treats the id as rolled back and moves past it.
	DefaultGapTimeout = time.Second
	// settledAge: a gap before a row older than this is final (a rollback long ago, or retention).
	settledAge = 30 * time.Second
)

// Query is one stream's request, already parsed and validated by the REST layer.
type Query struct {
	RunID string    // only this run's events
	Types []string  // only these event types; nil means all
	Since time.Time // only events created at or after this; zero means no bound
	// LastEventID resumes after that workflow_events.id when Resume is set (the Last-Event-ID
	// header).
	LastEventID int64
	Resume      bool
}

// Handler streams events. Each stream wakes on the Hub (NOTIFY ff_events) and also polls every
// Poll, so it keeps up even while the listener reconnects. All fields but Hub, Events and Clock
// have defaults.
//
// Commit horizon: workflow_events ids are assigned at insert, not at commit, so a lower id can
// become visible after a higher one. A stream therefore never moves past a missing id until it
// appears, or until it has been missing for GapTimeout (the transaction rolled back), or the row
// after it is older than 30 s. Otherwise the late event would be skipped for good, also by clients
// resuming with Last-Event-ID.
type Handler struct {
	Hub          *Hub
	DB           persistence.Querier
	Events       persistence.EventStreamRepo
	Clock        clock.Clock
	Poll         time.Duration // DefaultPoll
	KeepAlive    time.Duration // DefaultKeepAlive: a ": keep-alive" comment this often
	WriteTimeout time.Duration // DefaultWriteTimeout: a client whose writes block longer is dropped
	Batch        int           // DefaultBatch events per read
	GapTimeout   time.Duration // DefaultGapTimeout
}

// Stream serves one stream until the client goes away or a write blocks past WriteTimeout.
//
// Where it starts: after LastEventID when resuming; otherwise from the start of the run's
// history when RunID is set, from Since when set, and else from the newest event (live only).
// It emits "id: <workflow_events.id>\nevent: <event_type>\ndata: <envelope JSON>\n\n".
//
// It returns an error only before the response starts: ErrTooManyStreams when the Hub is full,
// or a failed read of the starting position. After that, problems end the stream and are logged.
func (h *Handler) Stream(w http.ResponseWriter, r *http.Request, q Query) error {
	if !h.Hub.acquire() {
		return ErrTooManyStreams
	}
	defer h.Hub.release()
	ctx := r.Context()
	after, err := h.start(ctx, q)
	if err != nil {
		return err
	}
	rc := http.NewResponseController(w)
	// The server's ReadTimeout must not end a long-lived stream; a writer without deadlines
	// (a test recorder) simply has none.
	if err := rc.SetReadDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return fmt.Errorf("clear read deadline: %w", err)
	}
	hdr := w.Header()
	hdr.Set("Content-Type", "text/event-stream")
	hdr.Set("Cache-Control", "no-cache")
	hdr.Set("Connection", "keep-alive")
	hdr.Set("X-Accel-Buffering", "no") // no proxy buffering
	w.WriteHeader(http.StatusOK)
	s := &stream{h: h, w: w, rc: rc, after: after,
		filter: persistence.EventStreamFilter{RunID: q.RunID, Types: q.Types, Since: q.Since}}
	if err := s.flush(); err != nil {
		logging.FromContext(ctx).InfoContext(ctx, "event stream ended before the first byte", logging.Err(err))
		return nil // the response has started; nothing more can be reported to the client
	}
	s.run(ctx)
	return nil
}

func (h *Handler) start(ctx context.Context, q Query) (int64, error) {
	switch {
	case q.Resume:
		return q.LastEventID, nil
	case q.RunID != "" || !q.Since.IsZero():
		return 0, nil
	}
	return h.Events.LastID(ctx, h.DB)
}

// stream is one open stream.
type stream struct {
	h       *Handler
	w       http.ResponseWriter
	rc      *http.ResponseController
	filter  persistence.EventStreamFilter
	after   int64 // every event with a lower or equal id has been sent or filtered out
	buf     bytes.Buffer
	gapAt   int64     // the missing id the stream is waiting for, 0 for none
	gapFrom time.Time // since when
	failing bool      // reads are failing: logged once, and again on recovery
}

func (s *stream) run(ctx context.Context) {
	poll := s.h.Clock.NewTimer(or(s.h.Poll, DefaultPoll))
	defer poll.Stop()
	keep := s.h.Clock.NewTimer(or(s.h.KeepAlive, DefaultKeepAlive))
	defer keep.Stop()
	for {
		wake := s.h.Hub.wake.Wait() // taken before reading, so a notification during the read still wakes us
		if err := s.drain(ctx); err != nil {
			logging.FromContext(ctx).InfoContext(ctx, "event stream dropped", logging.Err(err))
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-s.h.Hub.done: // the server is shutting down
			return
		case <-wake:
		case <-poll.C():
		case <-keep.C():
			s.buf.WriteString(": keep-alive\n\n")
			if err := s.flush(); err != nil {
				return
			}
			keep.Reset(or(s.h.KeepAlive, DefaultKeepAlive))
		}
		poll.Reset(or(s.h.Poll, DefaultPoll))
	}
}

// drain sends every event after s.after up to the commit horizon. A failed read keeps the stream
// open and is retried on the next wake or poll; only a failed write is returned, ending the stream.
func (s *stream) drain(ctx context.Context) error {
	batch := s.h.Batch
	if batch <= 0 {
		batch = DefaultBatch
	}
	for {
		marks, err := s.h.Events.Marks(ctx, s.h.DB, s.after, batch)
		if err != nil {
			s.readFailed(ctx, err)
			return nil
		}
		upTo := s.horizon(marks)
		if upTo == s.after {
			return nil
		}
		evs, err := s.h.Events.Stream(ctx, s.h.DB, s.filter, s.after, upTo, batch)
		if err != nil {
			s.readFailed(ctx, err)
			return nil
		}
		s.readRecovered(ctx)
		for _, e := range evs {
			if err := s.frame(e); err != nil { // cannot happen with JSON from jsonb; never stall on it
				logging.FromContext(ctx).ErrorContext(ctx, "event skipped by the stream", "event_id", e.EventID, logging.Err(err))
			}
		}
		if s.buf.Len() > 0 {
			if err := s.flush(); err != nil {
				return err
			}
		}
		s.after = upTo // past what the filter dropped too, so it is not read again
		if len(marks) < batch || upTo < marks[len(marks)-1].ID {
			return nil // caught up, or waiting at a gap
		}
	}
}

// horizon is the highest id in marks the stream may move to: every id from s.after+1 up to it has
// committed, or is a settled gap.
func (s *stream) horizon(marks []persistence.EventMark) int64 {
	prev := s.after
	for _, m := range marks {
		if m.ID != prev+1 && !s.gapSettled(prev+1, m) {
			return prev
		}
		prev = m.ID
	}
	s.gapAt = 0
	return prev
}

// gapSettled reports whether the ids missing before next (from missing on) can be skipped.
func (s *stream) gapSettled(missing int64, next persistence.EventMark) bool {
	now := s.h.Clock.Now()
	if now.Sub(next.CreatedAt) >= settledAge {
		return true
	}
	if s.gapAt != missing {
		s.gapAt, s.gapFrom = missing, now
		return false
	}
	return now.Sub(s.gapFrom) >= or(s.h.GapTimeout, DefaultGapTimeout)
}

func (s *stream) readFailed(ctx context.Context, err error) {
	if ctx.Err() != nil || s.failing {
		return
	}
	s.failing = true
	logging.FromContext(ctx).WarnContext(ctx, "event stream reads failing; retrying", logging.Err(err))
}

func (s *stream) readRecovered(ctx context.Context) {
	if s.failing {
		s.failing = false
		logging.FromContext(ctx).InfoContext(ctx, "event stream reads recovered")
	}
}

// frame appends one event to the buffer; SSE data must be a single line, so the JSON is compacted.
func (s *stream) frame(e persistence.EventView) error {
	env, err := envelope(e)
	if err != nil {
		return err
	}
	var fr bytes.Buffer // a failed frame must leave nothing half-written in s.buf
	fr.WriteString("id: " + strconv.FormatInt(e.ID, 10) + "\nevent: " + string(e.Type) + "\ndata: ")
	if err := json.Compact(&fr, env); err != nil {
		return fmt.Errorf("compact envelope of event %d: %w", e.ID, err)
	}
	fr.WriteString("\n\n")
	s.buf.Write(fr.Bytes())
	return nil
}

// flush writes the buffer with a write deadline (slow-client protection) and flushes it.
func (s *stream) flush() error {
	if err := s.rc.SetWriteDeadline(s.h.Clock.Now().Add(or(s.h.WriteTimeout, DefaultWriteTimeout))); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return fmt.Errorf("set write deadline: %w", err)
	}
	if _, err := s.w.Write(s.buf.Bytes()); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	s.buf.Reset()
	if err := s.rc.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

// envelope is the event's stored outbox envelope, or one rebuilt from workflow_events once the
// outbox row is gone (published rows are deleted after 3 days): occurred_at is then the row's
// created_at, aggregate_id its task or run id, and producer is empty.
func envelope(e persistence.EventView) ([]byte, error) {
	if len(e.Envelope) > 0 {
		return e.Envelope, nil
	}
	agg := e.TaskID
	if agg == "" {
		agg = e.RunID
	}
	b, err := json.Marshal(events.Envelope{
		EventID: e.EventID, EventType: e.Type, EventVersion: e.Version, OccurredAt: e.CreatedAt.UTC(),
		CorrelationID: e.CorrelationID, AggregateID: agg, Payload: e.Payload,
	})
	if err != nil {
		return nil, fmt.Errorf("rebuild envelope of event %d: %w", e.ID, err)
	}
	return b, nil
}

func or(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}
