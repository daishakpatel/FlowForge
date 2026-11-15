package scheduler

import (
	"slices"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

func TestBudget(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		maxParallel, runActive int
		limits, queueActive    map[string]int
		promotions             int
		queues                 []string // tasks offered in order
		want                   []string // the ones that fit
	}{
		"unlimited":               {0, 5, nil, nil, 100, []string{"a", "a", "b"}, []string{"a", "a", "b"}},
		"run limit counts active": {3, 1, nil, nil, 100, []string{"a", "a", "a"}, []string{"a", "a"}},
		"run already full":        {2, 3, nil, nil, 100, []string{"a"}, nil},
		"queue limit skips only that queue": {
			0, 0, map[string]int{"a": 3}, map[string]int{"a": 2}, 100,
			[]string{"a", "a", "b", "a", "b"}, []string{"a", "b", "b"},
		},
		"promotion cap": {0, 0, nil, nil, 2, []string{"a", "b", "c"}, []string{"a", "b"}},
		"both limits":   {2, 0, map[string]int{"b": 5}, nil, 100, []string{"a", "b", "a"}, []string{"a", "b"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b := newBudget(tc.maxParallel, tc.runActive, tc.limits, tc.queueActive, tc.promotions)
			var got []string
			for _, q := range tc.queues {
				if b.fits(q) {
					b.use(q)
					got = append(got, q)
				}
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("queued %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPromotionOrder(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	task := func(id string, status model.TaskStatus, avail, created time.Duration) *persistence.TaskView {
		return &persistence.TaskView{ID: id, Status: status, AvailableAt: t0.Add(avail), CreatedAt: t0.Add(created)}
	}
	heads := []*persistence.TaskView{
		task("t1", model.TaskReady, 2, 0),
		task("t2", model.TaskQueued, 0, 0),
		task("t3", model.TaskReady, 1, 5),
		task("t4", model.TaskReady, 1, 3),
		task("t6", model.TaskReady, 1, 3),
		task("t5", model.TaskReady, 1, 3),
		task("t7", model.TaskRunning, 0, 0),
	}
	var got []string
	for _, v := range promotionOrder(heads) {
		got = append(got, v.ID)
	}
	if want := []string{"t4", "t5", "t6", "t3", "t1"}; !slices.Equal(got, want) {
		t.Errorf("order %v, want %v (available_at, then created_at, then id)", got, want)
	}
	if n := activeCount(heads); n != 2 {
		t.Errorf("activeCount = %d, want 2 (QUEUED + RUNNING)", n)
	}
}
