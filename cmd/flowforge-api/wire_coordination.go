package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daishakpatel/flowforge/internal/api/rest"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/coordination"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/raftkv/client"
	"github.com/daishakpatel/flowforge/internal/workerapi"
)

// newCoordination returns the worker LeaseProvider and the cluster status sources that
// FF_COORDINATION selects (task 04.07), and a func that releases them. local: leases in Postgres
// (worker_observations). raft: Raft leases through RaftLeaseProvider, which still records every
// heartbeat in Postgres and degrades, never fails, while Raft is down (00_MASTER §6.2).
func newCoordination(cfg config.API, reg *obs.Registry, pool *pgxpool.Pool) (workerapi.LeaseProvider, rest.ClusterInfo, func(), error) {
	pgLeases := &workerapi.PGLeaseProvider{DB: pool, Workers: pg.WorkerRepo{}, Clock: clock.Real{}, TTL: cfg.WorkerLeaseTTL}
	info := rest.ClusterInfo{
		Coordination: cfg.Coordination,
		Fence:        func(ctx context.Context) (int64, string, error) { return pg.ClusterRepo{}.GetEpoch(ctx, pool) },
	}
	if cfg.Coordination != "raft" {
		info.Alive = pgLeases.Alive
		return pgLeases, info, func() {}, nil
	}
	metrics, err := coordination.NewRegistryMetrics(reg)
	if err != nil {
		return nil, info, nil, err
	}
	cl, err := client.New(client.Config{Endpoints: cfg.RaftEndpoints})
	if err != nil {
		return nil, info, nil, fmt.Errorf("raft client: %w", err)
	}
	kv := coordination.NewKV(cl)
	// The provider counts Raft failures; the registry gets no metrics, so none is counted twice.
	registry, err := coordination.NewWorkerRegistry(kv, coordination.RegistryConfig{TTL: cfg.WorkerLeaseTTL})
	if err != nil {
		_ = cl.Close() // construction failed; that error is the one to report
		return nil, info, nil, err
	}
	leases := &workerapi.RaftLeaseProvider{PG: pgLeases, Registry: registry, Metrics: metrics}
	info.Alive = leases.Alive
	info.Leader = func(ctx context.Context) (coordination.LeaderInfo, error) { return coordination.CurrentLeader(ctx, kv) }
	info.Raft = cl.Status
	return leases, info, func() { _ = cl.Close() }, nil // closing on shutdown; nothing to report
}
