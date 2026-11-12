package rest

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// workflowResponse is the Workflow schema.
type workflowResponse struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Version    int             `json:"version"`
	Definition json.RawMessage `json:"definition"`
	CreatedBy  string          `json:"created_by"`
	CreatedAt  time.Time       `json:"created_at"`
}

func toWorkflow(v persistence.DefinitionView) workflowResponse {
	return workflowResponse{
		ID: v.ID, Name: v.Name, Version: v.Version, Definition: v.Definition,
		CreatedBy: v.CreatedBy, CreatedAt: v.CreatedAt.UTC(),
	}
}

func (h *handlers) createWorkflow(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, r, err)
		return
	}
	v, err := h.workflows.Create(r.Context(), body, actor(r))
	if err != nil {
		writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/v1/workflows/"+v.ID)
	writeJSON(w, http.StatusCreated, toWorkflow(v))
}

func (h *handlers) listWorkflows(w http.ResponseWriter, r *http.Request) {
	page, err := pageParams(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	views, next, err := h.workflows.List(r.Context(), page)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newList(mapList(views, toWorkflow), next))
}

func (h *handlers) getWorkflow(w http.ResponseWriter, r *http.Request) {
	v, err := h.workflows.Get(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toWorkflow(v))
}

func (h *handlers) getWorkflowByName(w http.ResponseWriter, r *http.Request) {
	version := 0
	if s := r.URL.Query().Get("version"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			writeError(w, r, apperrors.InvalidArgument("version must be a positive integer"))
			return
		}
		version = n
	}
	v, err := h.workflows.GetByName(r.Context(), chi.URLParam(r, "name"), version)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toWorkflow(v))
}
