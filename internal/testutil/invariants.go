package testutil

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// invariants are the workflow-database invariants every crash and recovery test checks (task
// 03.04): each query returns the offending ids, and must return none.
var invariants = []struct{ name, sql string }{
	{"task RUNNING without a RUNNING attempt", `
		SELECT t.id FROM workflow_tasks t
		WHERE t.status = 'RUNNING' AND NOT EXISTS (
		  SELECT 1 FROM task_attempts a WHERE a.task_id = t.id AND a.status = 'RUNNING')
		LIMIT 10`},
	{"RUNNING attempt of a task that is not RUNNING", `
		SELECT a.id FROM task_attempts a JOIN workflow_tasks t ON t.id = a.task_id
		WHERE a.status = 'RUNNING' AND t.status <> 'RUNNING'
		LIMIT 10`},
	{"task with more than one RUNNING attempt", `
		SELECT task_id FROM task_attempts WHERE status = 'RUNNING'
		GROUP BY task_id HAVING count(*) > 1
		LIMIT 10`},
	{"attempt_count differs from the attempts recorded", `
		SELECT t.id FROM workflow_tasks t
		WHERE t.attempt_count <> (SELECT count(*) FROM task_attempts a WHERE a.task_id = t.id)
		LIMIT 10`},
	{"task status differs from its latest task event", `
		SELECT t.id FROM workflow_tasks t
		LEFT JOIN LATERAL (
		  SELECT e.payload->>'status' AS status FROM workflow_events e
		  WHERE e.task_id = t.id AND e.event_type LIKE 'task.%' AND e.event_type NOT LIKE 'task.attempt.%'
		  ORDER BY e.id DESC LIMIT 1) last ON true
		WHERE (t.status = 'PENDING' AND last.status IS NOT NULL)
		   OR (t.status <> 'PENDING' AND last.status IS DISTINCT FROM t.status)
		LIMIT 10`},
	{"run status differs from its latest run event", `
		SELECT r.id FROM workflow_runs r
		LEFT JOIN LATERAL (
		  SELECT e.payload->>'status' AS status FROM workflow_events e
		  WHERE e.run_id = r.id AND e.task_id IS NULL AND e.event_type LIKE 'run.%'
		  ORDER BY e.id DESC LIMIT 1) last ON true
		WHERE last.status IS DISTINCT FROM r.status
		LIMIT 10`},
}

// AssertInvariants fails t for every workflow-database invariant db breaks (task 03.04):
//
//   - a RUNNING task has exactly one RUNNING attempt, and a RUNNING attempt's task is RUNNING;
//   - attempt_count equals the attempts recorded;
//   - every task's and run's status equals the status in its latest event (a PENDING task has
//     no task event yet).
//
// Events and states are written in one transaction (ADR-009), so any break means a decision
// was half-committed.
func AssertInvariants(t testing.TB, db *pgxpool.Pool) {
	t.Helper()
	ctx := context.WithoutCancel(t.Context())
	for _, inv := range invariants {
		rows, err := db.Query(ctx, inv.sql)
		if err != nil {
			t.Fatalf("invariant %q: %v", inv.name, err)
		}
		var bad []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				t.Fatalf("invariant %q: %v", inv.name, err)
			}
			bad = append(bad, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("invariant %q: %v", inv.name, err)
		}
		if len(bad) > 0 {
			t.Errorf("invariant broken: %s: %s", inv.name, strings.Join(bad, ", "))
		}
	}
}
