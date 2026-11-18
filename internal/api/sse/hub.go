// Package sse serves the Server-Sent Events stream GET /api/v1/events/stream (task 03.07,
// FR-API-06): run events from workflow_events, in id order, resumable with Last-Event-ID. It reads
// Postgres, not Kafka, so the dashboard works with Kafka down (ADR-009). The REST layer parses
// and authorizes the request; this package streams.
package sse

import (
	"errors"
	"sync"
)

// DefaultMaxStreams is FF_SSE_MAX_CONNECTIONS's default.
const DefaultMaxStreams = 200

// ErrTooManyStreams is returned by Handler.Stream when the Hub is full; the API answers 503.
var ErrTooManyStreams = errors.New("too many event streams")

// Waker wakes waiters on every NOTIFY ff_events: Wait returns a channel closed at the next one.
// In production it is a workerapi.Waker on events.NotifyChannel, so one LISTEN connection serves
// every stream.
type Waker interface {
	Wait() <-chan struct{}
}

// Hub fans the shared ff_events listener out to every stream and caps how many are open. Safe
// for concurrent use.
type Hub struct {
	wake Waker
	max  int
	done chan struct{} // closed by Close

	mu     sync.Mutex
	open   int
	closed bool
}

// NewHub returns a Hub that wakes streams from w and admits at most limit at once.
func NewHub(w Waker, limit int) *Hub { return &Hub{wake: w, max: limit, done: make(chan struct{})} }

// Close ends every open stream and refuses new ones. http.Server.Shutdown does not cancel
// requests in flight, so flowforge-api registers Close with RegisterOnShutdown; without it, open
// streams would hold the shutdown until its deadline. Safe to call more than once.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.closed {
		h.closed = true
		close(h.done)
	}
}

// Open returns the number of open streams.
func (h *Hub) Open() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.open
}

func (h *Hub) acquire() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.open >= h.max {
		return false
	}
	h.open++
	return true
}

func (h *Hub) release() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.open--
}
