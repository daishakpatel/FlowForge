package scheduler

import (
	"cmp"
	"slices"
	"time"

	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// budget is how many more tasks one processRun may queue: the run's max_parallel_tasks minus
// its QUEUED+RUNNING tasks, each limited queue's FF_QUEUE_LIMITS minus that queue's QUEUED+RUNNING
// tasks across all runs, and the per-tick promotion cap that keeps one giant run from starving
// the others. The zero value allows nothing.
type budget struct {
	run        int            // < 0: no per-run limit
	queues     map[string]int // only queues that have a limit
	promotions int
}

// newBudget derives the budget. maxParallel 0 means no per-run limit (model.Concurrency);
// queueActive lacks queues with no active task.
func newBudget(maxParallel, runActive int, limits, queueActive map[string]int, promotions int) budget {
	b := budget{run: -1, queues: make(map[string]int, len(limits)), promotions: promotions}
	if maxParallel > 0 {
		b.run = max(maxParallel-runActive, 0)
	}
	for q, limit := range limits {
		b.queues[q] = max(limit-queueActive[q], 0)
	}
	return b
}

// exhausted reports whether no task of any queue fits any more.
func (b *budget) exhausted() bool { return b.run == 0 || b.promotions <= 0 }

// fits reports whether one more task of queue fits.
func (b *budget) fits(queue string) bool {
	left, limited := b.queues[queue]
	return !b.exhausted() && (!limited || left > 0)
}

// use records that one task of queue was queued. Call it only after fits returned true.
func (b *budget) use(queue string) {
	b.promotions--
	if b.run > 0 {
		b.run--
	}
	if left, limited := b.queues[queue]; limited {
		b.queues[queue] = left - 1
	}
}

// promotionOrder returns the tasks among heads that may be queued now, in the order they are
// queued: READY tasks, and RETRY_WAIT tasks whose available_at has come (03.04), oldest
// available_at first, then creation order (task 03.02 §4).
func promotionOrder(heads []*persistence.TaskView, now time.Time) []*persistence.TaskView {
	var ready []*persistence.TaskView
	for _, t := range heads {
		if t.Status == model.TaskReady || (t.Status == model.TaskRetryWait && !t.AvailableAt.After(now)) {
			ready = append(ready, t)
		}
	}
	slices.SortStableFunc(ready, func(a, b *persistence.TaskView) int {
		return cmp.Or(a.AvailableAt.Compare(b.AvailableAt), a.CreatedAt.Compare(b.CreatedAt), cmp.Compare(a.ID, b.ID))
	})
	return ready
}

// activeCount counts tasks holding a concurrency slot: QUEUED or RUNNING. RETRY_WAIT does not;
// it re-enters through QUEUED (03.04).
func activeCount(heads []*persistence.TaskView) int {
	n := 0
	for _, t := range heads {
		if t.Status == model.TaskQueued || t.Status == model.TaskRunning {
			n++
		}
	}
	return n
}
