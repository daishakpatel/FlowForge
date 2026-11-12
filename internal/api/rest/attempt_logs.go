package rest

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
)

// logLineResponse is the LogLine schema.
type logLineResponse struct {
	ID        int64           `json:"id"`
	AttemptID string          `json:"attempt_id"`
	TS        time.Time       `json:"ts"`
	Level     string          `json:"level"`
	Message   string          `json:"message"`
	Fields    json.RawMessage `json:"fields,omitempty"`
}

// listAttemptLogs serves GET /api/v1/attempts/{attempt_id}/logs?after_id=&limit= (task 03.01):
// an attempt's log lines after line id after_id, oldest first. next_cursor is the after_id of the
// next page (null on the last).
func (h *handlers) listAttemptLogs(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), chi.URLParam(r, "attempt_id")
	limit, err := parseLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	var after int64
	if s := r.URL.Query().Get("after_id"); s != "" {
		if after, err = strconv.ParseInt(s, 10, 64); err != nil || after < 0 {
			writeError(w, r, apperrors.InvalidArgument("after_id must be a non-negative integer"))
			return
		}
	}
	if _, err := h.runs.AttemptRepo.Get(ctx, h.db, id); err != nil {
		writeError(w, r, err)
		return
	}
	lines, err := h.logs.List(ctx, h.db, id, after, limit)
	if err != nil {
		writeError(w, r, err)
		return
	}
	items := make([]logLineResponse, len(lines))
	for i, l := range lines {
		items[i] = logLineResponse{ID: l.ID, AttemptID: l.AttemptID, TS: l.TS.UTC(), Level: l.Level, Message: l.Message, Fields: l.Fields}
	}
	next := ""
	if len(lines) == limit {
		next = strconv.FormatInt(lines[len(lines)-1].ID, 10)
	}
	writeJSON(w, http.StatusOK, newList(items, next))
}
