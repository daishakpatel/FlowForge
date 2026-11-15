package service

import (
	"slices"
	"testing"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// marketData is the task file's example after quality-check failed: its on_failure branch ran,
// the success branch was skipped, and report (always) ran. publish-2 follows the skipped branch.
func marketData() []persistence.TaskView {
	row := func(key string, st model.TaskStatus, deps ...string) persistence.TaskView {
		return persistence.TaskView{ID: "task_" + key, Key: key, Status: st, DependsOn: deps}
	}
	return []persistence.TaskView{
		row("fetch", model.TaskSucceeded),
		row("validate", model.TaskSucceeded, "fetch"),
		row("process", model.TaskSucceeded, "validate"),
		row("quality-check", model.TaskFailed, "process"),
		row("ai-investigate", model.TaskSucceeded, "quality-check"),
		row("publish", model.TaskSkipped, "quality-check"),
		row("report", model.TaskSucceeded, "quality-check"),
		row("publish-2", model.TaskSkipped, "publish"),
		row("archive", model.TaskSkipped, "report"),
	}
}

func planKeys(t *testing.T, rows []persistence.TaskView, keys ...string) []string {
	t.Helper()
	plan, err := planRerun(rows, keys)
	if err != nil {
		t.Fatalf("planRerun(%v): %v", keys, err)
	}
	var out []string
	for _, p := range plan {
		out = append(out, p.Key)
	}
	return out
}

func TestPlanRerun(t *testing.T) {
	t.Parallel()
	// Succeeded descendants (ai-investigate, report) are kept; the walk goes through report to
	// its skipped child archive. Ancestors are untouched.
	want := []string{"quality-check", "publish", "publish-2", "archive"}
	if got := planKeys(t, marketData(), "quality-check"); !slices.Equal(got, want) {
		t.Errorf("rerun quality-check = %v; want %v", got, want)
	}
	if got := planKeys(t, marketData()); !slices.Equal(got, want) { // retry: every FAILED head
		t.Errorf("retry = %v; want %v", got, want)
	}

	rows := marketData()
	rows[3].Status = model.TaskCancelled
	if got := planKeys(t, rows, "quality-check"); !slices.Equal(got, want) {
		t.Errorf("rerun of a CANCELLED task = %v; want %v", got, want)
	}

	for _, tc := range []struct {
		name string
		rows []persistence.TaskView
		keys []string
		code apperrors.Code
	}{
		{"unknown key", marketData(), []string{"nope"}, apperrors.CodeNotFound},
		{"target that succeeded", marketData(), []string{"fetch"}, apperrors.CodeInvalidState},
		{"target that was skipped", marketData(), []string{"publish"}, apperrors.CodeInvalidState},
		{"retry with nothing failed", marketData()[:3], nil, apperrors.CodeInvalidState},
		{"descendant still running", func() []persistence.TaskView {
			r := marketData()
			r[5].Status = model.TaskRunning
			return r
		}(), []string{"quality-check"}, apperrors.CodeInvalidState},
	} {
		if _, err := planRerun(tc.rows, tc.keys); apperrors.CodeOf(err) != tc.code {
			t.Errorf("%s: err = %v; want %s", tc.name, err, tc.code)
		}
	}
}

func TestHeads(t *testing.T) {
	t.Parallel()
	rows := []persistence.TaskView{{ID: "1", Key: "a"}, {ID: "2", Key: "b"}, {ID: "3", Key: "a", SupersedesTaskID: "1"}, {ID: "4", Key: "a", SupersedesTaskID: "3"}}
	var got []string
	for _, h := range heads(rows) {
		got = append(got, h.ID)
	}
	if !slices.Equal(got, []string{"2", "4"}) {
		t.Errorf("heads = %v; want [2 4]", got)
	}
}

func TestValidateRerun(t *testing.T) {
	t.Parallel()
	long := make([]rune, MaxRerunReasonRunes+1)
	for i := range long {
		long[i] = 'é'
	}
	for _, p := range []RerunParams{
		{RunID: "r", Reason: "  "},
		{RunID: "r", Reason: string(long)},
		{RunID: "r", Reason: "x", TaskKeys: make([]string, MaxRerunTasks+1)},
	} {
		if apperrors.CodeOf(validateRerun(p)) != apperrors.CodeInvalidArgument {
			t.Errorf("validateRerun(reason %d runes, %d keys) accepted", len([]rune(p.Reason)), len(p.TaskKeys))
		}
	}
	if err := validateRerun(RerunParams{RunID: "r", Reason: "fix data", TaskKeys: []string{"a"}}); err != nil {
		t.Errorf("valid params: %v", err)
	}
}
