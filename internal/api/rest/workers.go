package rest

import (
	"net/http"
	"time"
)

// workerResponse is the Worker schema. status is ACTIVE, DEREGISTERED, or LOST for an ACTIVE
// worker whose lease (FF_WORKER_LEASE_TTL) ran out.
type workerResponse struct {
	WorkerID    string    `json:"worker_id"`
	Status      string    `json:"status"`
	Capacity    int       `json:"capacity"`
	ActiveTasks int       `json:"active_tasks"`
	TaskTypes   []string  `json:"task_types"`
	Queues      []string  `json:"queues"`
	Version     string    `json:"version,omitempty"`
	FirstSeenAt time.Time `json:"first_seen_at"`
	LastSeenAt  time.Time `json:"last_seen_at"`
}

// listWorkers serves GET /api/v1/workers (task 03.01): every known worker, by id.
func (h *handlers) listWorkers(w http.ResponseWriter, r *http.Request) {
	views, err := h.workers.List(r.Context(), h.db)
	if err != nil {
		writeError(w, r, err)
		return
	}
	liveAfter := h.clock.Now().Add(-h.leaseTTL)
	items := make([]workerResponse, len(views))
	for i, v := range views {
		status := v.Status
		if status == "ACTIVE" && !v.Alive(liveAfter) {
			status = "LOST"
		}
		items[i] = workerResponse{
			WorkerID: v.WorkerID, Status: status, Capacity: v.Capacity, ActiveTasks: v.ActiveTasks,
			TaskTypes: v.TaskTypes, Queues: v.Queues, Version: v.Version,
			FirstSeenAt: v.FirstSeenAt.UTC(), LastSeenAt: v.LastSeenAt.UTC(),
		}
	}
	writeJSON(w, http.StatusOK, newList(items, ""))
}
