package rest

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/api/service"
	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// runResponse is the Run schema; empty optional fields are omitted.
type runResponse struct {
	ID              string          `json:"id"`
	DefinitionID    string          `json:"definition_id"`
	Status          model.RunStatus `json:"status"`
	Input           json.RawMessage `json:"input"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
	TriggerID       string          `json:"trigger_id,omitempty"`
	ParentRunID     string          `json:"parent_run_id,omitempty"`
	DeadlineAt      *time.Time      `json:"deadline_at,omitempty"`
	CancelRequested bool            `json:"cancel_requested"`
	CreatedBy       string          `json:"created_by"`
	CreatedAt       time.Time       `json:"created_at"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	CompletedAt     *time.Time      `json:"completed_at,omitempty"`
	Version         int64           `json:"version"`
}

func toRun(v persistence.RunView) runResponse {
	return runResponse{
		ID: v.ID, DefinitionID: v.DefinitionID, Status: v.Status, Input: v.Input,
		IdempotencyKey: v.IdempotencyKey, TriggerID: v.TriggerID, ParentRunID: v.ParentRunID,
		DeadlineAt: utc(v.DeadlineAt), CancelRequested: v.CancelRequested, CreatedBy: v.CreatedBy,
		CreatedAt: v.CreatedAt.UTC(), StartedAt: utc(v.StartedAt), CompletedAt: utc(v.CompletedAt),
		Version: v.Version,
	}
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

// actor is created_by for what the caller creates: the principal's key id.
func actor(r *http.Request) string {
	if p, ok := auth.PrincipalFrom(r.Context()); ok {
		return p.KeyID
	}
	return ""
}

// startRunRequest is the StartRunRequest schema.
type startRunRequest struct {
	Input json.RawMessage `json:"input"`
}

func (h *handlers) startRun(w http.ResponseWriter, r *http.Request) {
	var req startRunRequest
	if err := decodeOptionalJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}
	run, replayed, err := h.runs.Start(r.Context(), service.StartRunParams{
		DefinitionID: chi.URLParam(r, "id"), Input: req.Input,
		IdempotencyKey: r.Header.Get("Idempotency-Key"), Actor: actor(r),
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	if replayed {
		w.Header().Set("Idempotent-Replay", "true")
		writeJSON(w, http.StatusOK, toRun(run))
		return
	}
	w.Header().Set("Location", "/api/v1/runs/"+run.ID)
	writeJSON(w, http.StatusCreated, toRun(run))
}

// decodeOptionalJSON strictly decodes a JSON object body into v; an empty body leaves v unset.
func decodeOptionalJSON(r *http.Request, v any) error {
	body, err := io.ReadAll(r.Body)
	if err != nil || len(bytes.TrimSpace(body)) == 0 {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return apperrors.Wrap(apperrors.CodeInvalidArgument, "invalid JSON body: "+err.Error(), err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return apperrors.InvalidArgument("invalid JSON body: unexpected data after the object")
	}
	return nil
}

func (h *handlers) listRuns(w http.ResponseWriter, r *http.Request) {
	page, err := pageParams(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	f, err := runFilter(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	views, next, err := h.runs.List(r.Context(), f, page)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newList(mapList(views, toRun), next))
}

// runFilter reads the 00E §9 run filters: status, workflow (name), since and until (RFC 3339).
func runFilter(r *http.Request) (persistence.RunFilter, error) {
	q := r.URL.Query()
	f := persistence.RunFilter{Status: model.RunStatus(q.Get("status")), Workflow: q.Get("workflow")}
	if f.Status != "" && !slices.Contains(model.RunStatuses(), f.Status) {
		return f, apperrors.InvalidArgument("status must be one of the run statuses")
	}
	for name, dst := range map[string]*time.Time{"since": &f.Since, "until": &f.Until} {
		if s := q.Get(name); s != "" {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return f, apperrors.InvalidArgument(name + " must be an RFC 3339 time")
			}
			*dst = t
		}
	}
	return f, nil
}

func (h *handlers) getRun(w http.ResponseWriter, r *http.Request) {
	v, err := h.runs.Get(r.Context(), chi.URLParam(r, "run_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toRun(v))
}
