//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/daishakpatel/flowforge/internal/testutil"
)

// Run, Task and Attempt hold the REST fields the scenarios check.
type Run struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type Task struct {
	ID               string          `json:"id"`
	TaskKey          string          `json:"task_key"`
	Status           string          `json:"status"`
	AttemptCount     int             `json:"attempt_count"`
	Result           json.RawMessage `json:"result"`
	SupersedesTaskID *string         `json:"supersedes_task_id"`
}

type Attempt struct {
	ID            string     `json:"id"`
	AttemptNumber int        `json:"attempt_number"`
	WorkerID      string     `json:"worker_id"`
	Status        string     `json:"status"`
	StartedAt     time.Time  `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at"`
	ErrorCode     *string    `json:"error_code"`
}

var terminalRun = []string{"SUCCEEDED", "FAILED", "CANCELLED", "NEEDS_ATTENTION"}

// fixture reads test/fixtures/workflows/<name>.json.
func fixture(name string) []byte {
	raw, err := os.ReadFile(filepath.Join("..", "fixtures", "workflows", name+".json")) //nolint:gosec // the suite's own fixtures
	if err != nil {
		panic(err) // a missing fixture is a bug in the suite
	}
	return raw
}

// CreateWorkflow stores a definition (raw JSON or a value to encode) and returns its id.
func (h *Harness) CreateWorkflow(def any) string {
	h.t.Helper()
	var wf struct {
		ID string `json:"id"`
	}
	h.MustDo("POST", "/api/v1/workflows", def, &wf, http.StatusCreated)
	return wf.ID
}

// StartRun starts a run of workflow wfID with input (nil: none).
func (h *Harness) StartRun(wfID string, input any) Run {
	h.t.Helper()
	body := map[string]any{}
	if input != nil {
		body["input"] = input
	}
	var r Run
	h.MustDo("POST", "/api/v1/workflows/"+wfID+"/runs", body, &r, http.StatusCreated)
	return r
}

func (h *Harness) Run(id string) Run {
	h.t.Helper()
	var r Run
	h.MustDo("GET", "/api/v1/runs/"+id, nil, &r, http.StatusOK)
	return r
}

// Tasks lists the run's task rows (head rows only, unless all).
func (h *Harness) Tasks(runID string, all bool) []Task {
	h.t.Helper()
	var page struct {
		Items []Task `json:"items"`
	}
	h.MustDo("GET", fmt.Sprintf("/api/v1/runs/%s/tasks?include_superseded=%t&limit=200", runID, all), nil, &page, http.StatusOK)
	return page.Items
}

// Task returns the head row of key.
func (h *Harness) Task(runID, key string) Task {
	h.t.Helper()
	for _, t := range h.Tasks(runID, false) {
		if t.TaskKey == key {
			return t
		}
	}
	h.t.Fatalf("run %s has no task %q", runID, key)
	return Task{}
}

func (h *Harness) Attempts(taskID string) []Attempt {
	h.t.Helper()
	var page struct {
		Items []Attempt `json:"items"`
	}
	h.MustDo("GET", "/api/v1/tasks/"+taskID+"/attempts", nil, &page, http.StatusOK)
	return page.Items
}

// WaitRun waits until the run reaches one of statuses (default: any terminal one) and returns it.
// On timeout it fails with the run's tasks.
func (h *Harness) WaitRun(id string, timeout time.Duration, statuses ...string) Run {
	h.t.Helper()
	if len(statuses) == 0 {
		statuses = terminalRun
	}
	var r Run
	if !testutil.Eventually(func() bool {
		st, _, err := h.Do("GET", "/api/v1/runs/"+id, nil, &r)
		return err == nil && st == http.StatusOK && slices.Contains(statuses, r.Status)
	}, timeout, 100*time.Millisecond) {
		h.t.Fatalf("run %s is %s after %s, want one of %v; tasks: %s", id, r.Status, timeout, statuses, h.describe(id))
	}
	return r
}

// WaitAttempt waits until the head row of key has an attempt satisfying ok and returns it.
func (h *Harness) WaitAttempt(runID, key string, timeout time.Duration, ok func(Attempt) bool) Attempt {
	h.t.Helper()
	var found Attempt
	if !testutil.Eventually(func() bool {
		for _, t := range h.tasksQuiet(runID) {
			if t.TaskKey != key {
				continue
			}
			for _, a := range h.attemptsQuiet(t.ID) {
				if ok(a) {
					found = a
					return true
				}
			}
		}
		return false
	}, timeout, 50*time.Millisecond) {
		h.t.Fatalf("no matching attempt of %s within %s; tasks: %s", key, timeout, h.describe(runID))
	}
	return found
}

// running matches a RUNNING attempt (on worker, when given).
func running(worker string) func(Attempt) bool {
	return func(a Attempt) bool { return a.Status == "RUNNING" && (worker == "" || a.WorkerID == worker) }
}

// tasksQuiet and attemptsQuiet tolerate API errors (e.g. while the API restarts).
func (h *Harness) tasksQuiet(runID string) []Task {
	var page struct {
		Items []Task `json:"items"`
	}
	_, _, _ = h.Do("GET", "/api/v1/runs/"+runID+"/tasks?limit=200", nil, &page)
	return page.Items
}

func (h *Harness) attemptsQuiet(taskID string) []Attempt {
	var page struct {
		Items []Attempt `json:"items"`
	}
	_, _, _ = h.Do("GET", "/api/v1/tasks/"+taskID+"/attempts", nil, &page)
	return page.Items
}

// describe summarizes a run's tasks and attempts (every run's when runID is "") from the
// database, so it works while the API is down.
func (h *Harness) describe(runID string) string {
	rows, err := h.DB.Query(context.Background(), `
		SELECT t.task_key, t.status::text, coalesce(string_agg(a.attempt_number::text || ':' || a.status::text || ':' ||
			coalesce(a.error_code, '') || '@' || coalesce(a.worker_id, '-'), ' ' ORDER BY a.attempt_number), '')
		FROM workflow_tasks t LEFT JOIN task_attempts a ON a.task_id = t.id
		WHERE $1 = '' OR t.run_id = $1 GROUP BY t.id, t.task_key, t.status ORDER BY t.created_at, t.task_key`, runID)
	if err != nil {
		return err.Error()
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var key, status, atts string
		if err := rows.Scan(&key, &status, &atts); err != nil {
			return err.Error()
		}
		fmt.Fprintf(&b, "\n  %s %s [%s]", key, status, atts)
	}
	if err := rows.Err(); err != nil {
		return err.Error()
	}
	return b.String()
}

// mustStatus fails the test, with every task and attempt, unless got is want.
func (h *Harness) mustStatus(what, got, want string) {
	h.t.Helper()
	if got != want {
		h.t.Fatalf("%s is %s, want %s; tasks:%s", what, got, want, h.describe(""))
	}
}

// AssertHealthy checks the DB invariants and that no task has two SUCCEEDED attempts.
func (h *Harness) AssertHealthy() {
	h.t.Helper()
	testutil.AssertInvariants(h.t, h.DB)
	var doubles int
	if err := h.DB.QueryRow(context.Background(), `SELECT count(*) FROM (SELECT task_id FROM task_attempts
		WHERE status = 'SUCCEEDED' GROUP BY task_id HAVING count(*) > 1) d`).Scan(&doubles); err != nil {
		h.t.Fatal(err)
	}
	if doubles != 0 {
		h.t.Errorf("%d tasks have more than one SUCCEEDED attempt", doubles)
	}
}

// metric logs a measurement in a greppable form for docs/testing/phase3-gate.md.
func (h *Harness) metric(name string, d time.Duration) {
	h.t.Logf("METRIC %s=%s", name, d.Round(time.Millisecond))
}
