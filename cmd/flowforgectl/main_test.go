package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeAPI records requests and answers from a handler map keyed "METHOD /path".
type fakeAPI struct {
	mu   sync.Mutex
	reqs []recorded
	srv  *httptest.Server
}

type recorded struct {
	method, path, query, body string
	header                    http.Header
}

func newFakeAPI(t *testing.T, routes map[string]http.HandlerFunc) *fakeAPI {
	t.Helper()
	f := &fakeAPI{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.reqs = append(f.reqs, recorded{r.Method, r.URL.Path, r.URL.RawQuery, string(body), r.Header.Clone()})
		f.mu.Unlock()
		if h, ok := routes[r.Method+" "+r.URL.Path]; ok {
			h(w, r)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":{"code":"NOT_FOUND","message":"no route","request_id":"req_1"}}`)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func jsonReply(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

func (f *fakeAPI) run(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	env := map[string]string{"FF_API_URL": f.srv.URL, "FF_API_KEY": "ff_test_key"}
	code := run(t.Context(), args, func(k string) string { return env[k] }, &out, &errb)
	return code, out.String(), errb.String()
}

func TestCLI_APIErrorPrintsEnvelopeAndExits1(t *testing.T) {
	t.Parallel()
	f := newFakeAPI(t, map[string]http.HandlerFunc{
		"POST /api/v1/runs/run_1/pause": jsonReply(409, `{"error":{"code":"INVALID_STATE","message":"run run_1 is CREATED","details":[],"request_id":"req_42"}}`),
	})
	code, _, stderr := f.run(t, "runs", "pause", "run_1")
	if code != 1 || !strings.Contains(stderr, "409 INVALID_STATE") || !strings.Contains(stderr, "req_42") || !strings.Contains(stderr, `"code": "INVALID_STATE"`) {
		t.Errorf("exit %d, stderr %q; want 1 and the error envelope", code, stderr)
	}
	if strings.Contains(stderr, `"Status"`) {
		t.Errorf("the printed envelope has fields the API's lacks: %s", stderr)
	}
	if got := f.reqs[0].header.Get("Authorization"); got != "Bearer ff_test_key" {
		t.Errorf("Authorization %q", got)
	}
}

func TestCLI_UsageErrorsExit2(t *testing.T) {
	t.Parallel()
	f := newFakeAPI(t, nil)
	for _, args := range [][]string{{}, {"runs"}, {"runs", "explode"}, {"runs", "get"}, {"runs", "get", "a", "b"},
		{"-o", "yaml", "workers", "list"}, {"runs", "rerun", "run_1"}, {"workflows", "create"}, {"runs", "get", "--nope", "run_1"}} {
		if code, _, stderr := f.run(t, args...); code != 2 || stderr == "" {
			t.Errorf("%v: exit %d, stderr %q; want 2 and a message", args, code, stderr)
		}
	}
	if len(f.reqs) != 0 {
		t.Errorf("usage errors sent %d requests", len(f.reqs))
	}
}

func TestCLI_NetworkErrorExits1(t *testing.T) {
	t.Parallel()
	var out, errb bytes.Buffer
	code := run(t.Context(), []string{"workers", "list"}, func(k string) string {
		return map[string]string{"FF_API_URL": "http://127.0.0.1:1"}[k]
	}, &out, &errb)
	if code != 1 || !strings.Contains(errb.String(), "GET /api/v1/workers") {
		t.Errorf("exit %d, stderr %q", code, errb.String())
	}
}

func TestCLI_OutputFormats(t *testing.T) {
	t.Parallel()
	list := `{"items":[{"id":"wf_1","name":"diamond","version":2,"created_by":"k","created_at":"2026-11-18T00:00:00Z"}],"next_cursor":null}`
	f := newFakeAPI(t, map[string]http.HandlerFunc{"GET /api/v1/workflows": jsonReply(200, list)})
	code, out, _ := f.run(t, "workflows", "list")
	if code != 0 || !strings.Contains(out, "ID") || !strings.Contains(out, "wf_1") || !strings.Contains(out, "diamond") {
		t.Errorf("table: exit %d, %q", code, out)
	}
	code, out, _ = f.run(t, "workflows", "list", "-o", "json")
	var v map[string]any
	if code != 0 || json.Unmarshal([]byte(out), &v) != nil || v["items"] == nil {
		t.Errorf("json: exit %d, %q", code, out)
	}
}

func TestCLI_RunsStartResolvesNameAndSendsKeyAndInput(t *testing.T) {
	t.Parallel()
	f := newFakeAPI(t, map[string]http.HandlerFunc{
		"GET /api/v1/workflows/by-name/diamond": jsonReply(200, `{"id":"wf_9"}`),
		"POST /api/v1/workflows/wf_9/runs":      jsonReply(201, `{"id":"run_1","status":"CREATED"}`),
	})
	input := filepath.Join(t.TempDir(), "in.json")
	if err := os.WriteFile(input, []byte(`{"symbols":["AAPL"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	code, out, stderr := f.run(t, "runs", "start", "diamond", "--input", input, "--idempotency-key", "k1")
	if code != 0 || !strings.Contains(out, "run_1") {
		t.Fatalf("exit %d, out %q, stderr %q", code, out, stderr)
	}
	post := f.reqs[1]
	if post.header.Get("Idempotency-Key") != "k1" || !strings.Contains(post.body, `"input":{"symbols":["AAPL"]}`) {
		t.Errorf("start request: key %q, body %s", post.header.Get("Idempotency-Key"), post.body)
	}
}

func TestCLI_RequestShapes(t *testing.T) {
	t.Parallel()
	ok := jsonReply(200, `{"id":"x"}`)
	f := newFakeAPI(t, map[string]http.HandlerFunc{
		"POST /api/v1/runs/run_1/rerun":  ok,
		"POST /api/v1/runs/run_1/cancel": ok,
		"PATCH /api/v1/triggers/trg_1":   ok,
		"POST /api/v1/triggers":          ok,
		"DELETE /api/v1/api-keys/key_1":  func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) },
		"GET /api/v1/runs/run_1/tasks":   jsonReply(200, `{"items":[]}`),
		"POST /api/v1/api-keys":          jsonReply(201, `{"id":"key_2","key":"ff_secret"}`),
		"GET /api/v1/workflows/wf_1":     ok,
		"POST /api/v1/runs/run_1/retry":  ok,
		"GET /api/v1/runs/run_1/events":  jsonReply(200, `{"items":[]}`),
	})
	for _, c := range []struct {
		args       []string
		method, in string // in: a substring of the body or query
	}{
		{[]string{"runs", "rerun", "run_1", "--task", "qc", "--task", "pub", "--reason", "fixed"}, "POST", `"task_ids":["qc","pub"]`},
		{[]string{"runs", "cancel", "run_1", "--force"}, "POST", "force=true"},
		{[]string{"triggers", "disable", "trg_1"}, "PATCH", `"enabled":false`},
		{[]string{"triggers", "create", "--name", "n", "--workflow", "w", "--schedule", "* * * * *", "--timezone", "UTC"}, "POST", `"schedule":"* * * * *"`},
		{[]string{"api-keys", "revoke", "key_1"}, "DELETE", ""},
		{[]string{"runs", "tasks", "run_1", "--all"}, "GET", "include_superseded=true"},
		{[]string{"api-keys", "create", "--name", "ci", "--role", "operator"}, "POST", `"role":"operator"`},
		{[]string{"workflows", "get", "wf_1"}, "GET", ""},
		{[]string{"runs", "retry", "run_1"}, "POST", ""},
		{[]string{"runs", "events", "run_1"}, "GET", "limit=200"},
	} {
		n := len(f.reqs)
		if code, _, stderr := f.run(t, c.args...); code != 0 {
			t.Errorf("%v: exit %d, %s", c.args, code, stderr)
			continue
		}
		r := f.reqs[n]
		if r.method != c.method || !strings.Contains(r.body+r.query, c.in) {
			t.Errorf("%v: %s %s?%s %s; want %s with %q", c.args, r.method, r.path, r.query, r.body, c.method, c.in)
		}
	}
}

// TestCLI_RunsWatchResumesAndStopsAtTheEnd: the fake stream drops after the first event; the
// watch reconnects with Last-Event-ID and stops at run.succeeded.
func TestCLI_RunsWatchResumesAndStopsAtTheEnd(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var lastIDs []string
	f := newFakeAPI(t, map[string]http.HandlerFunc{
		"GET /api/v1/events/stream": func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			lastIDs = append(lastIDs, r.Header.Get("Last-Event-ID"))
			first := len(lastIDs) == 1
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			if first {
				_, _ = fmt.Fprint(w, "id: 1\nevent: run.started\ndata: {\"aggregate_id\":\"run_1\"}\n\n")
				return // the connection drops
			}
			_, _ = fmt.Fprint(w, ": keep-alive\n\nid: 2\nevent: task.succeeded\ndata: {\"aggregate_id\":\"task_a\"}\n\n"+
				"id: 3\nevent: run.succeeded\ndata: {\"aggregate_id\":\"run_1\"}\n\nid: 4\nevent: never.read\ndata: {}\n\n")
		},
	})
	code, out, stderr := f.run(t, "runs", "watch", "run_1")
	if code != 0 {
		t.Fatalf("exit %d, stderr %s", code, stderr)
	}
	if !strings.Contains(out, "run.started") || !strings.Contains(out, "task.succeeded") || !strings.Contains(out, "run.succeeded") || strings.Contains(out, "never.read") {
		t.Errorf("watch output %q", out)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(lastIDs) != 2 || lastIDs[0] != "" || lastIDs[1] != "1" {
		t.Errorf("Last-Event-ID per connection %q; want [\"\" \"1\"]", lastIDs)
	}
	if !strings.Contains(f.reqs[0].query, "run_id=run_1") {
		t.Errorf("stream query %q", f.reqs[0].query)
	}
}

func TestCLI_RunsWatchReportsAPIError(t *testing.T) {
	t.Parallel()
	f := newFakeAPI(t, map[string]http.HandlerFunc{
		"GET /api/v1/events/stream": jsonReply(503, `{"error":{"code":"UNAVAILABLE","message":"too many event streams","request_id":"req_7"}}`),
	})
	if code, _, stderr := f.run(t, "runs", "watch", "run_1"); code != 1 || !strings.Contains(stderr, "UNAVAILABLE") {
		t.Errorf("exit %d, stderr %q", code, stderr)
	}
}

// TestCLI_ListsFollowEveryPage: list commands follow next_cursor (cursor= for lists, after_id=
// for log lines) and print every row.
func TestCLI_ListsFollowEveryPage(t *testing.T) {
	t.Parallel()
	paged := func(param string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("limit") != "200" {
				t.Errorf("%s: limit %q, want 200", r.URL.Path, r.URL.Query().Get("limit"))
			}
			if r.URL.Query().Get(param) == "" {
				jsonReply(200, `{"items":[{"id":"first","message":"line 1"}],"next_cursor":"c2"}`)(w, r)
				return
			}
			jsonReply(200, `{"items":[{"id":"second","message":"line 2"}],"next_cursor":null}`)(w, r)
		}
	}
	f := newFakeAPI(t, map[string]http.HandlerFunc{
		"GET /api/v1/workflows":           paged("cursor"),
		"GET /api/v1/attempts/att_1/logs": paged("after_id"),
	})
	for _, args := range [][]string{{"workflows", "list"}, {"runs", "logs", "att_1"}} {
		code, out, stderr := f.run(t, args...)
		page1 := strings.Contains(out, "first") || strings.Contains(out, "line 1")
		page2 := strings.Contains(out, "second") || strings.Contains(out, "line 2")
		if code != 0 || !page1 || !page2 {
			t.Errorf("%v: exit %d, out %q, stderr %q; want both pages", args, code, out, stderr)
		}
	}
}

func TestCLI_RunsStartByIDSkipsTheLookup(t *testing.T) {
	t.Parallel()
	f := newFakeAPI(t, map[string]http.HandlerFunc{"POST /api/v1/workflows/wf_9/runs": jsonReply(201, `{"id":"run_1"}`)})
	if code, _, stderr := f.run(t, "runs", "start", "wf_9"); code != 0 || len(f.reqs) != 1 {
		t.Errorf("exit %d, %d requests, stderr %q; want one POST", code, len(f.reqs), stderr)
	}
}

func TestCLI_RunsWatchEndsOnNeedsAttention(t *testing.T) {
	t.Parallel()
	f := newFakeAPI(t, map[string]http.HandlerFunc{
		"GET /api/v1/events/stream": func(w http.ResponseWriter, r *http.Request) {
			_, _ = fmt.Fprint(w, "id: 1\nevent: run.needs_attention\ndata: {}\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done() // the stream stays open; the watch must stop on its own
		},
	})
	if code, out, stderr := f.run(t, "runs", "watch", "run_1"); code != 0 || !strings.Contains(out, "run.needs_attention") {
		t.Errorf("exit %d, out %q, stderr %q", code, out, stderr)
	}
}

// TestCLI_RunsWatchReconnectsAnIdleStream: a connection that goes silent (a half-open TCP
// connection) is dropped after watchIdle and resumed. Not parallel: it changes watchIdle.
func TestCLI_RunsWatchReconnectsAnIdleStream(t *testing.T) {
	old := watchIdle
	watchIdle = 200 * time.Millisecond
	t.Cleanup(func() { watchIdle = old })
	var mu sync.Mutex
	conns := 0
	f := newFakeAPI(t, map[string]http.HandlerFunc{
		"GET /api/v1/events/stream": func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			conns++
			first := conns == 1
			mu.Unlock()
			if first {
				_, _ = fmt.Fprint(w, "id: 1\nevent: run.started\ndata: {}\n\n")
				w.(http.Flusher).Flush()
				<-r.Context().Done() // silent until the client gives up on it
				return
			}
			_, _ = fmt.Fprint(w, "id: 2\nevent: run.succeeded\ndata: {}\n\n")
		},
	})
	code, out, stderr := f.run(t, "runs", "watch", "run_1")
	if code != 0 || !strings.Contains(out, "run.succeeded") || f.reqs[1].header.Get("Last-Event-ID") != "1" {
		t.Errorf("exit %d, out %q, stderr %q", code, out, stderr)
	}
}

// TestCLI_RunsWatchCountsFailuresInARow: more drops than maxReconnects are fine when each
// connection delivers an event.
func TestCLI_RunsWatchCountsFailuresInARow(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	n := 0
	f := newFakeAPI(t, map[string]http.HandlerFunc{
		"GET /api/v1/events/stream": func(w http.ResponseWriter, _ *http.Request) {
			mu.Lock()
			n++
			i := n
			mu.Unlock()
			event := "task.succeeded"
			if i > 2*maxReconnects {
				event = "run.succeeded"
			}
			_, _ = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: {}\n\n", i, event)
		},
	})
	if code, _, stderr := f.run(t, "runs", "watch", "run_1"); code != 0 {
		t.Errorf("exit %d after %d connections, stderr %q", code, n, stderr)
	}
}
