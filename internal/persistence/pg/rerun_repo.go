package pg

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
)

var _ persistence.RerunRepo = (*RunRepo)(nil)

// InsertReruns implements persistence.RerunRepo.
func (r *RunRepo) InsertReruns(ctx context.Context, tx pgx.Tx, runID string, supersede []string) ([]string, error) {
	q := sqlcgen.New(tx)
	out := make([]string, 0, len(supersede))
	for _, old := range supersede {
		id, err := q.InsertRerunTask(ctx, sqlcgen.InsertRerunTaskParams{ID: r.ids.New(ids.PrefixTask), SupersedesTaskID: old, RunID: runID})
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return nil, apperrors.NotFound("task " + old + " of run " + runID + " not found")
		case err != nil:
			return nil, conflictOr(err, "task "+old+" was already rerun", "insert rerun of "+old)
		}
		out = append(out, id)
	}
	return out, nil
}
