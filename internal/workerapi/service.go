package workerapi

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// Registration limits.
const (
	maxCapacity   = 1000
	maxNameLength = 128 // task types and queue names
)

var workerIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}$`)

// Service implements workerv1.WorkerServiceServer. Every field is required. It is safe for
// concurrent use; all state is in Postgres. Callers reach it only through Interceptors, which
// authenticate a worker-role key (auth.PrincipalFrom).
type Service struct {
	workerv1.UnimplementedWorkerServiceServer

	DB       persistence.Querier
	Tx       persistence.TxManager
	Tasks    persistence.TaskRepo
	Attempts persistence.AttemptRepo
	Workers  persistence.WorkerRepo
	Logs     persistence.TaskLogRepo
	Leases   LeaseProvider
	Outcomes OutcomeHandler
	Cancels  CancelSource
	Waker    *Waker // nil: no LISTEN, PollTask re-checks every second only
	Clock    clock.Clock

	LeaseTTL          time.Duration // FF_WORKER_LEASE_TTL
	HeartbeatInterval time.Duration // FF_WORKER_HEARTBEAT
	// StaleRejections counts reports refused for a stale fencing token
	// (flowforge_stale_attempt_rejections_total).
	StaleRejections prometheus.Counter
}

var _ workerv1.WorkerServiceServer = (*Service)(nil)

// Register implements workerv1.WorkerServiceServer.
func (s *Service) Register(ctx context.Context, req *workerv1.RegisterRequest) (*workerv1.RegisterResponse, error) {
	if err := validateRegistration(req); err != nil {
		return nil, err
	}
	err := s.Leases.Register(ctx, persistence.WorkerRegistration{
		WorkerID: req.GetWorkerId(), KeyID: caller(ctx), TaskTypes: req.GetTaskTypes(), Queues: req.GetQueues(),
		Capacity: int(req.GetCapacity()), Version: req.GetVersion(),
	})
	if err != nil {
		return nil, err
	}
	return &workerv1.RegisterResponse{LeaseTtlMs: s.LeaseTTL.Milliseconds(), HeartbeatIntervalMs: s.HeartbeatInterval.Milliseconds()}, nil
}

func validateRegistration(req *workerv1.RegisterRequest) error {
	names := func(field string, list []string) error {
		if len(list) == 0 {
			return apperrors.InvalidArgument(field + " must not be empty")
		}
		for _, n := range list {
			if n == "" || len(n) > maxNameLength {
				return apperrors.InvalidArgument(fmt.Sprintf("%s entries must be 1..%d characters", field, maxNameLength))
			}
		}
		return nil
	}
	switch {
	case !workerIDPattern.MatchString(req.GetWorkerId()):
		return apperrors.InvalidArgument("worker_id must match ^[a-z0-9][a-z0-9-]{2,62}$")
	case req.GetCapacity() < 1 || req.GetCapacity() > maxCapacity:
		return apperrors.InvalidArgument(fmt.Sprintf("capacity must be 1..%d", maxCapacity))
	}
	if err := names("task_types", req.GetTaskTypes()); err != nil {
		return err
	}
	return names("queues", req.GetQueues())
}

// Deregister implements workerv1.WorkerServiceServer.
func (s *Service) Deregister(ctx context.Context, req *workerv1.DeregisterRequest) (*workerv1.DeregisterResponse, error) {
	if _, err := s.registeredWorker(ctx, req.GetWorkerId()); err != nil {
		return nil, err
	}
	if err := s.Leases.Release(ctx, req.GetWorkerId(), caller(ctx)); err != nil {
		return nil, err
	}
	return &workerv1.DeregisterResponse{}, nil
}

// registeredWorker returns the caller's ACTIVE registration of workerID. Another key's worker is
// PERMISSION_DENIED; an unknown or deregistered one is INVALID_STATE (the worker must Register).
func (s *Service) registeredWorker(ctx context.Context, workerID string) (persistence.WorkerView, error) {
	w, err := s.Workers.Get(ctx, s.DB, workerID)
	switch {
	case apperrors.CodeOf(err) == apperrors.CodeNotFound:
		return w, apperrors.InvalidState(fmt.Sprintf("worker %q is not registered: call Register", workerID))
	case err != nil:
		return w, err
	case w.KeyID != caller(ctx):
		return w, apperrors.PermissionDenied(fmt.Sprintf("worker %q belongs to another API key", workerID))
	case w.Status != "ACTIVE":
		return w, apperrors.InvalidState(fmt.Sprintf("worker %q is deregistered: call Register", workerID))
	}
	return w, nil
}

// caller is the API key id the interceptor authenticated.
func caller(ctx context.Context) string {
	p, _ := auth.PrincipalFrom(ctx)
	return p.KeyID
}
