package persistence

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/apperrors"
)

func TestCursor_RoundTripKeepsMicroseconds(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 11, 10, 12, 0, 0, 123456000, time.UTC)
	gotAt, gotID, err := DecodeCursor(EncodeCursor(at, "run_01J"))
	if err != nil || !gotAt.Equal(at) || gotID != "run_01J" {
		t.Errorf("round trip = %v, %q, %v; want %v, run_01J, nil", gotAt, gotID, err, at)
	}
}

func TestCursor_InvalidIsInvalidArgument(t *testing.T) {
	t.Parallel()
	for _, c := range []string{"!!", "bm9waXBl", EncodeCursor(time.Time{}, "")} {
		if _, _, err := DecodeCursor(c); apperrors.CodeOf(err) != apperrors.CodeInvalidArgument {
			t.Errorf("DecodeCursor(%q) = %v, want INVALID_ARGUMENT", c, err)
		}
	}
}

func TestPage_EffectiveLimit(t *testing.T) {
	t.Parallel()
	for in, want := range map[int]int{-1: DefaultPageLimit, 0: DefaultPageLimit, 7: 7, MaxPageLimit + 1: MaxPageLimit} {
		if got := (Page{Limit: in}).EffectiveLimit(); got != want {
			t.Errorf("Limit %d → %d, want %d", in, got, want)
		}
	}
}

// TestQueries_OnlyTransitionsWriteStatus guards ADR-020: the only SQL that assigns the status of
// a run, task or attempt is the conditional transition queries (inserts set the initial status).
// Other tables' status columns, such as worker_observations', are not state machines.
func TestQueries_OnlyTransitionsWriteStatus(t *testing.T) {
	t.Parallel()
	files, err := filepath.Glob("queries/*.sql")
	if err != nil || len(files) == 0 {
		t.Fatalf("no query files: %v", err)
	}
	setStatus := regexp.MustCompile(`(?is)\bUPDATE\s+(?:workflow_runs|workflow_tasks|task_attempts)\b.*?\bSET\b(.*?)\bWHERE\b`)
	assigns := regexp.MustCompile(`(?i)(^|[\s,])status\s*=`)
	allowed := []string{"TransitionRun", "TransitionTask", "FinishAttempt"}
	var found []string
	for _, f := range files {
		b, err := os.ReadFile(f) //nolint:gosec // G304: test reads the repo's own query files
		if err != nil {
			t.Fatal(err)
		}
		for _, block := range strings.Split(string(b), "-- name: ")[1:] {
			name := strings.Fields(block)[0]
			if m := setStatus.FindStringSubmatch(block); m != nil && assigns.MatchString(m[1]) {
				found = append(found, name)
				if !slices.Contains(allowed, name) {
					t.Errorf("%s: query %s sets status outside a Transition helper", f, name)
				}
			}
		}
	}
	if len(found) != len(allowed) {
		t.Errorf("status-writing queries = %v, want exactly %v", found, allowed)
	}
}
