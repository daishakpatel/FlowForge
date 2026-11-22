package coordination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/client"
)

// WorkerPrefix holds one lease-bound key per live worker (00E §5.2).
const WorkerPrefix = "/workers/"

// Registry errors.
var (
	// ErrWorkerConflict: the worker id is live under another API key, or changed concurrently.
	ErrWorkerConflict = errors.New("coordination: worker registered by another key")
	// ErrNotRegistered: the worker holds no live lease.
	ErrNotRegistered = errors.New("coordination: worker not registered")
	// ErrInvalidWorkerID: empty, longer than maxWorkerID bytes, or containing '/'.
	ErrInvalidWorkerID = errors.New("coordination: invalid worker id")
)

// maxWorkerID keeps /workers/{id} within the KV's 512-byte key limit.
const maxWorkerID = 256

func checkWorkerID(id string) error {
	if id == "" || len(id) > maxWorkerID || strings.Contains(id, "/") {
		return fmt.Errorf("%w: %q", ErrInvalidWorkerID, id)
	}
	return nil
}

// WorkerInfo is a /workers/{id} value (00E §5.2). KeyID and LeaseToken extend the 00E layout:
// KeyID enforces "one worker id, one API key", and LeaseToken lets any API instance renew the
// lease from the key alone (Handle), since a worker's heartbeats may reach any instance. A
// fencing token orders lease holders; it is not a secret (04.03).
type WorkerInfo struct {
	ID           string   `json:"-"` // the key's suffix
	KeyID        string   `json:"key_id"`
	Capabilities []string `json:"capabilities"` // task types
	Queues       []string `json:"queues"`
	Capacity     int      `json:"capacity"`
	Version      string   `json:"version"`
	RegisteredMs int64    `json:"registered_ms"`
	LeaseToken   uint64   `json:"lease_token"`
}

// LeaseHandle identifies a worker's current lease.
type LeaseHandle struct {
	WorkerID string
	LeaseID  uint64
	Token    uint64
	Info     WorkerInfo // what to re-register with when the lease is gone
}

// RegistryConfig configures a WorkerRegistry.
type RegistryConfig struct {
	TTL     time.Duration // worker lease TTL (FF_WORKER_LEASE_TTL, 15s)
	Clock   clock.Clock   // registered_ms; default clock.Real
	Metrics *RegistryMetrics
}

// WorkerRegistry keeps worker liveness as Raft leases (task 04.05, ADR-003: Raft is the liveness
// authority); 04.07 wires it behind workerapi.LeaseProvider in the API tier. A live worker has a
// key under WorkerPrefix bound to its lease; the key disappears from reads when the lease expires
// (judged by the Raft leader's clock) and is deleted by the sweeper. Workers never talk to Raft
// themselves. Safe for concurrent use; it keeps no local state.
type WorkerRegistry struct {
	kv  KV
	cfg RegistryConfig
}

// NewWorkerRegistry returns a registry over kv.
func NewWorkerRegistry(kv KV, cfg RegistryConfig) (*WorkerRegistry, error) {
	if kv == nil || cfg.TTL <= 0 {
		return nil, errors.New("coordination: worker registry needs a kv and a positive ttl")
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	return &WorkerRegistry{kv: kv, cfg: cfg}, nil
}

func workerKey(id string) string { return WorkerPrefix + id }

func decode(kv *pb.KeyValue) (WorkerInfo, error) {
	var w WorkerInfo
	if err := json.Unmarshal(kv.GetValue(), &w); err != nil {
		return WorkerInfo{}, fmt.Errorf("decode %s: %w", kv.GetKey(), err)
	}
	w.ID = strings.TrimPrefix(kv.GetKey(), WorkerPrefix)
	return w, nil
}

// current returns the worker's live key and value, or nil when it holds no live lease.
func (r *WorkerRegistry) current(ctx context.Context, id string) (*pb.KeyValue, WorkerInfo, error) {
	kv, err := r.kv.Get(ctx, workerKey(id))
	if errors.Is(err, client.ErrNotFound) {
		return nil, WorkerInfo{}, nil
	}
	if err != nil {
		return nil, WorkerInfo{}, fmt.Errorf("read worker %s: %w", id, err)
	}
	w, err := decode(kv)
	return kv, w, err
}

// Register starts (or, for the same KeyID, restarts) a worker's lease: a new TTL lease and the
// worker's key moved onto it with a CAS, so two concurrent registrations can't both win. A
// worker id live under another KeyID is ErrWorkerConflict. The previous lease, now holding no
// key, is revoked.
func (r *WorkerRegistry) Register(ctx context.Context, info WorkerInfo) (LeaseHandle, error) {
	if err := checkWorkerID(info.ID); err != nil {
		return LeaseHandle{}, err
	}
	cur, prev, err := r.current(ctx, info.ID)
	if err != nil {
		return LeaseHandle{}, err
	}
	if cur != nil && prev.KeyID != info.KeyID {
		return LeaseHandle{}, fmt.Errorf("%w: %s", ErrWorkerConflict, info.ID)
	}
	id, token, err := r.kv.LeaseGrant(ctx, r.cfg.TTL, info.ID)
	if err != nil {
		return LeaseHandle{}, fmt.Errorf("grant lease for worker %s: %w", info.ID, err)
	}
	info.RegisteredMs, info.LeaseToken = r.cfg.Clock.Now().UnixMilli(), token
	value, err := json.Marshal(info)
	if err != nil {
		return LeaseHandle{}, fmt.Errorf("encode worker %s: %w", info.ID, err)
	}
	ok, _, err := r.kv.CAS(ctx, workerKey(info.ID), cur.GetVersion(), value, id)
	if err != nil {
		// The outcome is unknown: the CAS may have applied with its response lost. Revoking the
		// new lease then would delete the live key, so look first.
		if now, _, rerr := r.current(ctx, info.ID); rerr == nil && now.GetLeaseId() == id {
			ok, err = true, nil
		}
	}
	if err != nil || !ok {
		_ = r.kv.LeaseRevoke(context.WithoutCancel(ctx), id) // holds no key; it would expire anyway
		if err != nil {
			return LeaseHandle{}, fmt.Errorf("register worker %s: %w", info.ID, err)
		}
		return LeaseHandle{}, fmt.Errorf("%w: %s changed concurrently", ErrWorkerConflict, info.ID)
	}
	if old := cur.GetLeaseId(); old != 0 {
		_ = r.kv.LeaseRevoke(context.WithoutCancel(ctx), old) // holds no key now; it would expire anyway
	}
	return LeaseHandle{WorkerID: info.ID, LeaseID: id, Token: token, Info: info}, nil
}

// Handle rebuilds a live worker's handle from its key (ErrNotRegistered when it has none).
func (r *WorkerRegistry) Handle(ctx context.Context, workerID string) (LeaseHandle, error) {
	cur, info, err := r.current(ctx, workerID)
	if err != nil {
		return LeaseHandle{}, err
	}
	if cur == nil {
		return LeaseHandle{}, fmt.Errorf("%w: %s", ErrNotRegistered, workerID)
	}
	return LeaseHandle{WorkerID: workerID, LeaseID: cur.GetLeaseId(), Token: info.LeaseToken, Info: info}, nil
}

// Renew extends the lease and returns the handle to keep using. When h's lease is gone:
//   - if the worker's key is live under another lease (another API instance re-registered it),
//     it adopts that lease rather than re-registering, which would revoke it;
//   - if the lease expired (LEASE_EXPIRED), it re-registers with h.Info, so a worker that was
//     briefly unreachable just comes back;
//   - if the lease no longer exists (LEASE_NOT_FOUND: revoked by Deregister, or already swept),
//     it returns ErrNotRegistered and does not re-register: a heartbeat racing a graceful
//     shutdown must not bring the worker back. The caller re-registers explicitly when it
//     knows the worker is still running.
//
// Every other failure is counted (flowforge_worker_lease_renewal_failures_total) and returned
// with h unchanged; the caller decides how to degrade (00_MASTER §6.2).
func (r *WorkerRegistry) Renew(ctx context.Context, h LeaseHandle) (LeaseHandle, error) {
	err := r.kv.LeaseKeepAlive(ctx, h.LeaseID, h.Token)
	if err == nil {
		return h, nil
	}
	if !errors.Is(err, client.ErrLeaseExpired) && !errors.Is(err, client.ErrLeaseNotFound) {
		return r.failed(h, err)
	}
	live, info, lerr := r.current(ctx, h.WorkerID)
	switch {
	case lerr != nil:
		return r.failed(h, lerr)
	case live != nil && info.KeyID != h.Info.KeyID:
		return h, fmt.Errorf("%w: %s", ErrWorkerConflict, h.WorkerID)
	case live != nil:
		return LeaseHandle{WorkerID: h.WorkerID, LeaseID: live.GetLeaseId(), Token: info.LeaseToken, Info: info}, nil
	case errors.Is(err, client.ErrLeaseNotFound):
		return h, fmt.Errorf("%w: %s (lease revoked or swept)", ErrNotRegistered, h.WorkerID)
	}
	again := h.Info
	again.ID = h.WorkerID
	nh, err := r.Register(ctx, again)
	if err != nil {
		return r.failed(h, err)
	}
	return nh, nil
}

// failed counts a renewal the KV did not confirm and returns h unchanged.
func (r *WorkerRegistry) failed(h LeaseHandle, err error) (LeaseHandle, error) {
	r.cfg.Metrics.renewalFailed()
	return h, fmt.Errorf("renew worker %s: %w", h.WorkerID, err)
}

// Deregister ends the lease, deleting the worker's key (graceful shutdown). A lease already gone
// is fine.
func (r *WorkerRegistry) Deregister(ctx context.Context, h LeaseHandle) error {
	if err := r.kv.LeaseRevoke(ctx, h.LeaseID); err != nil && !errors.Is(err, client.ErrLeaseNotFound) {
		return fmt.Errorf("deregister worker %s: %w", h.WorkerID, err)
	}
	return nil
}

// List returns the live workers in id order, page by page. Each page is a linearizable read;
// the listing as a whole is not one snapshot.
func (r *WorkerRegistry) List(ctx context.Context) ([]WorkerInfo, error) {
	var out []WorkerInfo
	for start := ""; ; {
		kvs, next, err := r.kv.List(ctx, WorkerPrefix, start)
		if err != nil {
			return nil, fmt.Errorf("list workers: %w", err)
		}
		for _, kv := range kvs {
			w, err := decode(kv)
			if err != nil {
				return nil, err
			}
			out = append(out, w)
		}
		if next == "" {
			return out, nil
		}
		start = next
	}
}

// Expired returns the workers in known that hold no live lease, sorted and without duplicates.
// The registry can't know which workers ever existed, so the caller passes them (the API keeps
// that set in Postgres, worker_observations).
func (r *WorkerRegistry) Expired(ctx context.Context, known []string) ([]string, error) {
	alive, err := r.List(ctx)
	if err != nil {
		return nil, err
	}
	live := make(map[string]bool, len(alive))
	for _, w := range alive {
		live[w.ID] = true
	}
	var out []string
	for _, id := range known {
		if !live[id] {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}
