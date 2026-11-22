package client

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func notLeaderErr(t *testing.T, hint string) error {
	t.Helper()
	st, err := status.New(codes.FailedPrecondition, "not the leader").WithDetails(&errdetails.ErrorInfo{
		Reason: reasonNotLeader, Metadata: map[string]string{metaLeaderHint: hint},
	})
	if err != nil {
		t.Fatal(err)
	}
	return st.Err()
}

// TestClassify_HintToADownEndpointBacksOff is the regression test for a failover that spent every
// attempt in ~100 ms: during an election the follower still names the dead leader, so following
// that hint immediately bounced between the two without waiting.
func TestClassify_HintToADownEndpointBacksOff(t *testing.T) {
	c, err := New(Config{Endpoints: []string{"127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	c.byID = map[uint64]int{1: 0, 2: 1, 3: 2} // what Cluster.Status discovery would have learned
	down := map[int]bool{}
	if next, retry, wait := c.classify(context.Background(), 1, notLeaderErr(t, "3"), down); next != 2 || !retry || wait {
		t.Fatalf("fresh hint: next %d retry %v wait %v; want 2, true, false", next, retry, wait)
	}
	if next, retry, wait := c.classify(context.Background(), 2, status.Error(codes.Unavailable, "refused"), down); next != 0 || !retry || !wait {
		t.Fatalf("unavailable: next %d retry %v wait %v; want 0, true, true", next, retry, wait)
	}
	if next, retry, wait := c.classify(context.Background(), 0, notLeaderErr(t, "3"), down); next != 2 || !retry || !wait {
		t.Fatalf("hint to the down endpoint: next %d retry %v wait %v; want 2, true, true", next, retry, wait)
	}
	if _, retry, _ := c.classify(context.Background(), 0, status.Error(codes.Aborted, "conflict"), down); retry {
		t.Fatal("a conflict was classified as retryable")
	}
}

// TestBackoff_EqualJitterWithinCap: the delay before retry n is uniform in [ceiling/2, ceiling]
// with ceiling = min(max, base·2ⁿ⁻¹), so retries spread out, never exceed the cap, and always wait
// at least half of it.
func TestBackoff_EqualJitterWithinCap(t *testing.T) {
	// The defaults outlast an election timeout of 2 s (00E §5.3) over MaxAttempts.
	d := withDefaults(Config{Endpoints: []string{"a:1"}})
	floor := time.Duration(0)
	for attempt := 1; attempt < d.MaxAttempts; attempt++ {
		floor += min(d.MaxBackoff, d.BaseBackoff<<(attempt-1)) / 2
	}
	if floor <= 2*time.Second {
		t.Fatalf("default retries wait at least %s in all, want > 2s", floor)
	}
	c := &Client{cfg: Config{BaseBackoff: 50 * time.Millisecond, MaxBackoff: time.Second}, rng: rand.New(rand.NewPCG(7, 7))} //nolint:gosec // G404: seeded for a reproducible test
	for attempt := 1; attempt <= 10; attempt++ {
		ceiling := min(time.Second, 50*time.Millisecond<<(attempt-1))
		lo, hi := ceiling, time.Duration(0)
		for range 200 {
			d := c.backoff(attempt)
			if d < ceiling/2 || d > ceiling {
				t.Fatalf("attempt %d: backoff %s outside [%s, %s]", attempt, d, ceiling/2, ceiling)
			}
			lo, hi = min(lo, d), max(hi, d)
		}
		if hi-lo < ceiling/4 {
			t.Fatalf("attempt %d: 200 backoffs within [%s, %s], want them spread over [%s, %s]", attempt, lo, hi, ceiling/2, ceiling)
		}
	}
}

// TestBackoff_LargeBaseDoesNotOverflow: BaseBackoff·2ⁿ must not overflow into a negative ceiling
// (rand.Int64N panics on it); the cap applies however many attempts are configured (code review).
func TestBackoff_LargeBaseDoesNotOverflow(t *testing.T) {
	c := &Client{cfg: Config{BaseBackoff: 20 * time.Second, MaxBackoff: time.Minute}, rng: rand.New(rand.NewPCG(1, 1))} //nolint:gosec // G404: seeded for a reproducible test
	for attempt := 1; attempt <= 64; attempt++ {
		if d := c.backoff(attempt); d < 10*time.Second || d > time.Minute {
			t.Fatalf("attempt %d: backoff %s", attempt, d)
		}
	}
}
