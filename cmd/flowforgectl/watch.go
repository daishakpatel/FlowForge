package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// terminalRunEvents end a watch. NEEDS_ATTENTION waits for an operator, so it ends one too.
var terminalRunEvents = map[string]bool{"run.succeeded": true, "run.failed": true, "run.cancelled": true, "run.needs_attention": true}

// maxReconnects is how many reconnects in a row may fail to deliver an event.
const maxReconnects = 5

// watchIdle ends a connection that delivered nothing, not even the server's 15 s keep-alive, for
// this long: a half-open TCP connection would otherwise block forever. A variable for tests.
var watchIdle = 45 * time.Second

// runsWatch follows a run on the SSE stream (GET /api/v1/events/stream?run_id=) from its first
// event, printing each one, until the run succeeds, fails, is cancelled or needs attention. A
// dropped or idle stream is resumed with Last-Event-ID, so no event is printed twice or skipped.
func runsWatch(ctx context.Context, e *env, args []string) error {
	pos, err := parse(flag.NewFlagSet("runs watch", flag.ContinueOnError), args, 1, "runs watch <run id>")
	if err != nil {
		return err
	}
	stream := &http.Client{} // no overall timeout: the stream lasts as long as the run
	last := ""
	for attempt := 0; ; attempt++ {
		before := last
		done, err := e.watchOnce(ctx, stream, pos[0], &last)
		if last != before {
			attempt = 0 // that connection delivered events: count failures in a row only
		}
		switch {
		case done:
			return nil
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil && errors.As(err, new(*apiError)):
			return err
		case attempt >= maxReconnects:
			return fmt.Errorf("event stream: %w (gave up after %d reconnects)", err, maxReconnects)
		}
		select { // brief pause before resuming from last
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 200 * time.Millisecond): //nolint:forbidigo // the CLI is a cmd binary
		}
	}
}

// watchOnce reads one connection of the stream; it reports done when a terminal run event arrives.
func (e *env) watchOnce(ctx context.Context, stream *http.Client, runID string, last *string) (bool, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	idle := time.AfterFunc(watchIdle, func() { cancel(errIdle) })
	defer idle.Stop()
	req, err := http.NewRequestWithContext(ctx, "GET", e.c.base+"/api/v1/events/stream?run_id="+esc(runID), nil)
	if err != nil {
		return false, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	if e.c.key != "" {
		req.Header.Set("Authorization", "Bearer "+e.c.key)
	}
	if *last != "" {
		req.Header.Set("Last-Event-ID", *last)
	}
	resp, err := stream.Do(req)
	if err != nil {
		return false, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = resp.Body.Close() }() // read to the end or abandoned with ctx
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		var env struct {
			Error apiError `json:"error"`
		}
		_ = json.Unmarshal(raw, &env)
		env.Error.Status = resp.StatusCode
		if env.Error.Code == "" {
			env.Error.Code, env.Error.Message = http.StatusText(resp.StatusCode), strings.TrimSpace(string(raw))
		}
		return false, &env.Error
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	var id, event, data string
	for sc.Scan() {
		idle.Reset(watchIdle)
		line := sc.Text()
		switch {
		case line == "": // end of one message
			if event != "" {
				*last = id
				if err := e.printEvent(id, event, data); err != nil {
					return false, err
				}
				if terminalRunEvents[event] {
					return true, nil
				}
			}
			id, event, data = "", "", ""
		case strings.HasPrefix(line, "id: "):
			id = line[4:]
		case strings.HasPrefix(line, "event: "):
			event = line[7:]
		case strings.HasPrefix(line, "data: "):
			data = line[6:]
		}
	}
	if errors.Is(context.Cause(ctx), errIdle) {
		return false, errIdle
	}
	if err := sc.Err(); err != nil {
		return false, fmt.Errorf("read stream: %w", err)
	}
	return false, errors.New("stream closed")
}

var errIdle = errors.New("stream idle: no data or keep-alive")

func (e *env) printEvent(id, event, data string) error {
	if e.out.json {
		_, err := fmt.Fprintln(e.stdout, data)
		return err
	}
	var env struct {
		OccurredAt  string `json:"occurred_at"`
		AggregateID string `json:"aggregate_id"`
	}
	_ = json.Unmarshal([]byte(data), &env) // best effort: the line still shows id and type
	_, err := fmt.Fprintf(e.stdout, "%-8s %-28s %-24s %s\n", id, env.OccurredAt, event, env.AggregateID)
	return err
}
