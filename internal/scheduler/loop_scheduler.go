package scheduler

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// runsPerTick caps the runs one tick processes (fairness, task 03.02 §5).
const runsPerTick = 200

// Waker wakes the loop early: Wait returns a channel closed at the next notification. In
// production it is LISTEN ff_scheduler_wake on a dedicated connection.
type Waker interface {
	Wait() <-chan struct{}
}

// DecisionLoop is the scheduler's core Loop (the task file's "SchedulerLoop"; renamed to avoid the
// scheduler.SchedulerLoop stutter, 00C §8): every Tick, or when woken, it runs processRun for
// up to runsPerTick CREATED or RUNNING runs, one transaction per run. Successive ticks walk the
// runs round-robin by id, so more than runsPerTick active runs are all served in turn.
type DecisionLoop struct {
	DB      persistence.Querier // reads outside the decision transactions
	Tx      persistence.TxManager
	Cluster persistence.ClusterRepo
	Sched   persistence.SchedulerRepo
	Notify  persistence.Notifier
	Runs    persistence.RunRepo
	Tasks   persistence.TaskRepo
	Defs    persistence.DefinitionRepo
	Inputs  InputResolver              // IdentityResolver until 03.06
	Eval    statemachine.ExprEvaluator // nil until 03.06: run_if=expr tasks stay PENDING
	Clock   clock.Clock
	Tick    time.Duration
	// QueueLimits caps QUEUED+RUNNING tasks per queue across runs (FF_QUEUE_LIMITS); queues not
	// listed are unlimited.
	QueueLimits map[string]int
	Wake        Waker                  // optional
	Metrics     *Metrics               // optional
	Errors      *prometheus.CounterVec // flowforge_loop_errors_total; optional

	pageSize int // runs per tick; 0 means runsPerTick (tests shrink it)

	mu     sync.Mutex
	defs   map[string]*model.Definition
	cursor string // last run id of a full page; "" starts from the first run
}

var _ Loop = (*DecisionLoop)(nil)

// Name implements Loop.
func (l *DecisionLoop) Name() string { return "scheduler" }

// Run implements Loop. It returns nil when ctx ends and an error wrapping
// persistence.ErrStaleEpoch when the stored epoch is not gate.Epoch(): another scheduler has
// taken over, so this one stops deciding. Other failures back off (RunTicking).
func (l *DecisionLoop) Run(ctx context.Context, gate Gate) error {
	return RunTicking(ctx, l.Name(), l.Clock, l.Tick, l.Wake, l.Errors, func(ctx context.Context) error { return l.Once(ctx, gate) })
}

// Once processes one page of runs and refreshes the queue depth. A run that fails is logged,
// counted and retried next iteration; the page or depth query failing (the database is down) is
// returned so Run backs off, as is a stale epoch.
func (l *DecisionLoop) Once(ctx context.Context, gate Gate) error {
	start := l.Clock.Now()
	logger := logging.FromContext(ctx)
	ids, err := l.page(ctx)
	if err != nil {
		return fmt.Errorf("list active runs: %w", err)
	}
	done := gate.Done()
	for _, id := range ids {
		select {
		case <-ctx.Done():
			return nil
		case <-done:
			return nil // no decision after the term ends (Gate contract)
		default:
		}
		res, err := l.runOnce(ctx, id, gate.Epoch())
		switch {
		case errors.Is(err, persistence.ErrStaleEpoch):
			return err
		case err != nil && ctx.Err() == nil:
			logger.ErrorContext(ctx, "process run", "run_id", id, logging.Err(err))
			if l.Errors != nil {
				l.Errors.WithLabelValues(l.Name()).Inc()
			}
		case err == nil:
			l.Metrics.record(res)
		}
	}
	depth, err := l.Sched.QueuedByQueue(ctx, l.DB)
	if err != nil {
		return fmt.Errorf("read queue depth: %w", err)
	}
	l.Metrics.setQueueDepth(depth)
	if l.Metrics != nil {
		l.Metrics.TickDuration.Observe(l.Clock.Now().Sub(start).Seconds())
	}
	return nil
}

// runOnce runs processRun for one run in its own transaction at epoch and returns what it
// committed.
func (l *DecisionLoop) runOnce(ctx context.Context, runID string, epoch int64) (result, error) {
	var res result
	err := l.Tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var err error
		res, err = l.processRun(ctx, tx, runID, epoch) // a retried transaction starts res afresh
		return err
	})
	return res, err
}

// page returns the next runs of the round-robin: up to pageSize ids after the cursor, wrapping
// to the start to fill a short page with runs not already in it. The cursor moves past the last
// id of a full page, or back to the start when every active run fit.
func (l *DecisionLoop) page(ctx context.Context) ([]string, error) {
	size := cmp.Or(l.pageSize, runsPerTick)
	l.mu.Lock()
	after := l.cursor
	l.mu.Unlock()
	ids, err := l.Sched.ActiveRunIDs(ctx, l.DB, after, size)
	if err != nil {
		return nil, err
	}
	if len(ids) < size && after != "" {
		more, err := l.Sched.ActiveRunIDs(ctx, l.DB, "", size-len(ids))
		if err != nil {
			return nil, err
		}
		for _, id := range more {
			if id > after {
				break // already in ids
			}
			ids = append(ids, id)
		}
	}
	next := ""
	if len(ids) == size {
		next = ids[len(ids)-1]
	}
	l.mu.Lock()
	l.cursor = next
	l.mu.Unlock()
	return ids, nil
}

// definition returns the decoded definition, cached by id: definitions are immutable.
// shortcut: the cache is unbounded (one entry per definition ever run); switch to an LRU if the
// number of definitions grows into the tens of thousands.
func (l *DecisionLoop) definition(ctx context.Context, q persistence.Querier, id string) (*model.Definition, error) {
	l.mu.Lock()
	def, ok := l.defs[id]
	l.mu.Unlock()
	if ok {
		return def, nil
	}
	v, err := l.Defs.Get(ctx, q, id)
	if err != nil {
		return nil, err
	}
	if def, err = model.Decode(v.Definition); err != nil {
		return nil, fmt.Errorf("decode definition %s: %w", id, err)
	}
	l.mu.Lock()
	if l.defs == nil {
		l.defs = map[string]*model.Definition{}
	}
	l.defs[id] = def
	l.mu.Unlock()
	return def, nil
}
