package runtime

import (
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/daishakpatel/flowforge/internal/testutil"
)

// The integration tests run the worker, a gRPC client and server in-process; none of their
// goroutines may outlive the tests (00C §2.10).
func TestMain(m *testing.M) { testutil.LeakCheck(m) }

func TestLogHandler_RingAttrsAndGroups(t *testing.T) {
	t.Parallel()
	sink := newLogSink(3)
	log := slog.New(&logHandler{sink: sink}).With("task", "t1").WithGroup("db")
	for i := range 5 {
		log.Info("query", "n", i)
	}
	log.Error("boom")
	lines, dropped := sink.take()
	if len(lines) != 3 || dropped != 3 {
		t.Fatalf("kept %d lines, dropped %d; want 3 and 3", len(lines), dropped)
	}
	if lines[0].GetMessage() != "query" || lines[2].GetLevel() != "ERROR" {
		t.Errorf("lines %v", lines)
	}
	var fields map[string]any
	if err := json.Unmarshal(lines[0].GetFieldsJson(), &fields); err != nil || fields["task"] != "t1" || fields["db.n"] != float64(3) {
		t.Errorf("fields %s (%v)", lines[0].GetFieldsJson(), err)
	}
	if again, d := sink.take(); len(again) != 0 || d != 0 {
		t.Error("take must clear the sink")
	}
	if levelName(slog.LevelDebug) != "DEBUG" || levelName(slog.LevelWarn) != "WARN" {
		t.Error("level mapping")
	}
}

func withReason(c codes.Code, reason string) error {
	st, err := status.New(c, "x").WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: "flowforge.dev"})
	if err != nil {
		panic(err)
	}
	return st.Err()
}

func TestErrorClassification(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err                         error
		transient, unreg, staleness bool
	}{
		"unavailable":      {status.Error(codes.Unavailable, "down"), true, false, false},
		"deadline":         {status.Error(codes.DeadlineExceeded, "slow"), true, false, false},
		"rate limited":     {withReason(codes.ResourceExhausted, "RATE_LIMITED"), true, false, false},
		"too large":        {withReason(codes.ResourceExhausted, "PAYLOAD_TOO_LARGE"), false, false, false},
		"stale":            {withReason(codes.FailedPrecondition, "STALE_ATTEMPT"), false, false, true},
		"not registered":   {withReason(codes.FailedPrecondition, "INVALID_STATE"), false, true, false},
		"permission":       {withReason(codes.PermissionDenied, "PERMISSION_DENIED"), false, false, false},
		"not a gRPC error": {errors.New("x"), false, false, false},
	} {
		if transient(tc.err) != tc.transient || unregistered(tc.err) != tc.unreg || stale(tc.err) != tc.staleness {
			t.Errorf("%s: transient %v unregistered %v stale %v", name, transient(tc.err), unregistered(tc.err), stale(tc.err))
		}
	}
}

func TestBackoff_BoundedWithJitter(t *testing.T) {
	t.Parallel()
	b := &backoffPolicy{base: 100 * time.Millisecond, maxDelay: time.Second, rng: rand.New(rand.NewPCG(7, 7))} //nolint:gosec // G404: test jitter
	for n, nominal := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second, time.Second} {
		for range 20 {
			if d := b.delay(n); d < nominal/2 || d >= nominal*3/2 {
				t.Fatalf("delay(%d) = %s, want in [%s, %s)", n, d, nominal/2, nominal*3/2)
			}
		}
	}
	if d := b.delay(1000); d >= 1500*time.Millisecond {
		t.Errorf("delay(1000) = %s, not capped", d)
	}
}
