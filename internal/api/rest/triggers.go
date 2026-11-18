package rest

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/daishakpatel/flowforge/internal/api/service"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// triggerResponse is the Trigger schema.
type triggerResponse struct {
	ID              string          `json:"id"`
	Name            string          `json:"name"`
	WorkflowName    string          `json:"workflow_name"`
	WorkflowVersion *int            `json:"workflow_version,omitempty"`
	Schedule        string          `json:"schedule"`
	Timezone        string          `json:"timezone"`
	Input           json.RawMessage `json:"input"`
	MissedPolicy    string          `json:"missed_policy"`
	Enabled         bool            `json:"enabled"`
	NextFireAt      *time.Time      `json:"next_fire_at,omitempty"`
	LastFiredAt     *time.Time      `json:"last_fired_at,omitempty"`
	CreatedBy       string          `json:"created_by"`
	CreatedAt       time.Time       `json:"created_at"`
}

func toTrigger(v persistence.TriggerView) triggerResponse {
	return triggerResponse{
		ID: v.ID, Name: v.Name, WorkflowName: v.WorkflowName, WorkflowVersion: v.WorkflowVersion, Schedule: v.Schedule,
		Timezone: v.Timezone, Input: v.Input, MissedPolicy: v.MissedPolicy, Enabled: v.Enabled,
		NextFireAt: utc(v.NextFireAt), LastFiredAt: utc(v.LastFiredAt), CreatedBy: v.CreatedBy, CreatedAt: v.CreatedAt.UTC(),
	}
}

// createTriggerRequest is the CreateTriggerRequest schema.
type createTriggerRequest struct {
	Name            string          `json:"name"`
	WorkflowName    string          `json:"workflow_name"`
	WorkflowVersion *int            `json:"workflow_version"`
	Schedule        string          `json:"schedule"`
	Timezone        string          `json:"timezone"`
	Input           json.RawMessage `json:"input"`
	MissedPolicy    string          `json:"missed_policy"`
	Enabled         *bool           `json:"enabled"`
}

// updateTriggerRequest is the UpdateTriggerRequest schema; omitted fields are unchanged.
type updateTriggerRequest struct {
	Enabled      *bool           `json:"enabled"`
	Schedule     *string         `json:"schedule"`
	Timezone     *string         `json:"timezone"`
	Input        json.RawMessage `json:"input"`
	MissedPolicy *string         `json:"missed_policy"`
}

// createTrigger is POST /api/v1/triggers (admin, task 03.08).
func (h *handlers) createTrigger(w http.ResponseWriter, r *http.Request) {
	var req createTriggerRequest
	if err := decodeOptionalJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}
	v, err := h.triggers.Create(r.Context(), service.CreateTriggerParams{
		Name: req.Name, WorkflowName: req.WorkflowName, WorkflowVersion: req.WorkflowVersion, Schedule: req.Schedule,
		Timezone: req.Timezone, Input: req.Input, MissedPolicy: req.MissedPolicy, Enabled: req.Enabled, Actor: actor(r),
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/v1/triggers/"+v.ID)
	writeJSON(w, http.StatusCreated, toTrigger(v))
}

func (h *handlers) listTriggers(w http.ResponseWriter, r *http.Request) {
	page, err := pageParams(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	list, next, err := h.triggers.List(r.Context(), page)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newList(mapList(list, toTrigger), next))
}

func (h *handlers) getTrigger(w http.ResponseWriter, r *http.Request) {
	v, err := h.triggers.Get(r.Context(), chi.URLParam(r, "trigger_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTrigger(v))
}

// updateTrigger is PATCH /api/v1/triggers/{trigger_id} (admin): enable, disable, reschedule or
// change the input; next_fire_at is recomputed when needed.
func (h *handlers) updateTrigger(w http.ResponseWriter, r *http.Request) {
	var req updateTriggerRequest
	if err := decodeOptionalJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}
	v, err := h.triggers.Update(r.Context(), chi.URLParam(r, "trigger_id"), service.TriggerPatch{
		Enabled: req.Enabled, Schedule: req.Schedule, Timezone: req.Timezone, Input: req.Input, MissedPolicy: req.MissedPolicy,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTrigger(v))
}

func (h *handlers) deleteTrigger(w http.ResponseWriter, r *http.Request) {
	if err := h.triggers.Delete(r.Context(), chi.URLParam(r, "trigger_id")); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
