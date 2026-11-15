package rest

import (
	"context"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/daishakpatel/flowforge/internal/api/service"
	"github.com/daishakpatel/flowforge/internal/apperrors"
)

// rerunRequest is the RerunRequest schema.
type rerunRequest struct {
	TaskIDs []string `json:"task_ids"`
	Reason  string   `json:"reason"`
}

// retryRequest is the RetryRequest schema.
type retryRequest struct {
	Reason string `json:"reason"`
}

// rerunResponse is the RerunResponse schema.
type rerunResponse struct {
	Run     runResponse `json:"run"`
	TaskIDs []string    `json:"task_ids"`
}

// rerunRun is POST /api/v1/runs/{run_id}/rerun (task 03.06).
func (h *handlers) rerunRun(w http.ResponseWriter, r *http.Request) {
	var req rerunRequest
	if err := decodeOptionalJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}
	h.rerun(w, r, func(ctx context.Context, id string) ([]string, error) {
		return h.runs.Rerun(ctx, service.RerunParams{RunID: id, TaskKeys: req.TaskIDs, Reason: req.Reason, Actor: actor(r)})
	})
}

// retryRun is POST /api/v1/runs/{run_id}/retry: a rerun of every FAILED and TIMED_OUT task.
func (h *handlers) retryRun(w http.ResponseWriter, r *http.Request) {
	var req retryRequest
	if err := decodeOptionalJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}
	h.rerun(w, r, func(ctx context.Context, id string) ([]string, error) {
		return h.runs.Retry(ctx, id, req.Reason, actor(r))
	})
}

// rerun answers 200 with the run after the rerun and the created task ids.
func (h *handlers) rerun(w http.ResponseWriter, r *http.Request, do func(ctx context.Context, id string) ([]string, error)) {
	ctx, id := r.Context(), chi.URLParam(r, "run_id")
	created, err := do(ctx, id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	run, err := h.runs.Get(ctx, id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, rerunResponse{Run: toRun(run), TaskIDs: created})
}

// includeSuperseded parses GET …/tasks?include_superseded=.
func includeSuperseded(r *http.Request) (bool, error) {
	v := r.URL.Query().Get("include_superseded")
	if v == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, apperrors.InvalidArgument("include_superseded must be true or false")
	}
	return b, nil
}
