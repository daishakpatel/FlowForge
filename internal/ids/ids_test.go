package ids

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/daishakpatel/flowforge/internal/clock"
)

var idFormat = regexp.MustCompile(`^run_[0-9A-HJKMNP-TV-Z]{26}$`)

func TestNew_Format(t *testing.T) {
	t.Parallel()
	id := New(PrefixRun)
	if !idFormat.MatchString(id) {
		t.Fatalf("%q does not match %s", id, idFormat)
	}
	if err := Validate(PrefixRun, id); err != nil {
		t.Errorf("Validate(own id) = %v", err)
	}
}

func TestParse_RejectsInvalid(t *testing.T) {
	t.Parallel()
	good := New(PrefixTask)
	for name, s := range map[string]string{
		"wrong prefix":     strings.Replace(good, "task_", "run_", 1),
		"prefix only":      "task_",
		"no separator":     strings.Replace(good, "_", "", 1),
		"bad character":    good[:len(good)-1] + "U",
		"too short":        good[:len(good)-1],
		"too long":         good + "0",
		"empty":            "",
		"overflowing ulid": "task_8ZZZZZZZZZZZZZZZZZZZZZZZZZ",
	} {
		if err := Validate(PrefixTask, s); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: Validate(%q) = %v, want ErrInvalid", name, s, err)
		}
	}
	id, err := Parse(PrefixTask, good)
	if err != nil || "task_"+id.String() != good {
		t.Errorf("Parse round trip: %v, %v", id, err)
	}
}

// TestNew_ConcurrentUniqueAndIncreasing: 1000 IDs from 100 goroutines are unique, and each
// goroutine sees its own IDs strictly increasing (task 01.04 §8).
func TestNew_ConcurrentUniqueAndIncreasing(t *testing.T) {
	t.Parallel()
	const goroutines, perG = 100, 10
	results := make([][]string, goroutines)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for range perG {
				results[g] = append(results[g], New(PrefixEvent))
			}
		})
	}
	wg.Wait()

	seen := map[string]bool{}
	for g, ids := range results {
		for i, id := range ids {
			if seen[id] {
				t.Fatalf("duplicate id %s", id)
			}
			seen[id] = true
			if i > 0 && id <= ids[i-1] {
				t.Errorf("goroutine %d: %s not after %s", g, id, ids[i-1])
			}
		}
	}
	if len(seen) != goroutines*perG {
		t.Errorf("got %d ids, want %d", len(seen), goroutines*perG)
	}
}

// TestGenerator_MonotonicWithFrozenAndBackwardClock: many IDs in one millisecond, and a clock
// that jumps back, still give strictly increasing IDs.
func TestGenerator_MonotonicWithFrozenAndBackwardClock(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	g := NewGenerator(clock.NewFake(start), rand.Reader)
	prev := g.New(PrefixRun)
	for range 500 {
		id := g.New(PrefixRun)
		if id <= prev {
			t.Fatalf("%s not after %s", id, prev)
		}
		prev = id
	}
	g.clock = clock.NewFake(start.Add(-time.Hour))
	id := g.New(PrefixRun)
	if id <= prev {
		t.Errorf("clock went back and the id went back: %s <= %s", id, prev)
	}
	u, _ := Parse(PrefixRun, id)
	if u.Time() != ulid.Timestamp(start) {
		t.Errorf("timestamp %d, want the last one %d", u.Time(), ulid.Timestamp(start))
	}
}

// TestGenerator_OverflowBorrowsNextMillisecond: entropy that is already at its maximum
// overflows on the next ID in the same millisecond, which moves to the next millisecond.
func TestGenerator_OverflowBorrowsNextMillisecond(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	// The first ULID's 10 entropy bytes are all 0xFF (the maximum); later reads are random.
	maxed := io.MultiReader(bytes.NewReader(bytes.Repeat([]byte{0xFF}, 10)), rand.Reader)
	g := NewGenerator(clock.NewFake(start), maxed)
	a, b := g.New(PrefixRun), g.New(PrefixRun)
	ua, _ := Parse(PrefixRun, a)
	ub, _ := Parse(PrefixRun, b)
	if b <= a || ub.Time() != ua.Time()+1 {
		t.Errorf("a=%s (%d) b=%s (%d): want b in the next millisecond", a, ua.Time(), b, ub.Time())
	}
}

func TestGenerator_PanicsOnEntropyFailure(t *testing.T) {
	t.Parallel()
	g := NewGenerator(clock.Real{}, io.LimitReader(rand.Reader, 0))
	defer func() {
		if recover() == nil {
			t.Error("want panic when entropy fails")
		}
	}()
	g.New(PrefixRun)
}

func TestFake_Deterministic(t *testing.T) {
	t.Parallel()
	var f Fake
	if got := f.New(PrefixRun); got != "run_00000000000000000000000001" {
		t.Errorf("first fake id = %s", got)
	}
	second := f.New(PrefixRun)
	if second != "run_00000000000000000000000002" || Validate(PrefixRun, second) != nil {
		t.Errorf("second fake id = %s", second)
	}
	var g Generator = &Fake{}
	if g.New(PrefixTask) != "task_00000000000000000000000001" {
		t.Error("a fresh Fake must restart its sequence")
	}
}
