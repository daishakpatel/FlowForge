package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/coordination"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/raftkv/client"
	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/workerapi"
)

// leadership is how this scheduler leads and where it reads worker liveness (task 04.07,
// FF_COORDINATION). In local mode it is always leader at epoch 0 and leases come from Postgres.
// In raft mode an Elector campaigns through the Raft KV, PGFence activates each won epoch in
// Postgres before the term starts (ADR-003), and leases come from Raft (RaftLeaseProvider).
type leadership struct {
	gate   scheduler.Gate
	leases workerapi.LeaseProvider
	elect  func(ctx context.Context) error // the Elector's Run; nil in local mode
	close  func()
}

func newLeadership(ctx context.Context, cfg config.Scheduler, reg *obs.Registry, logger *slog.Logger, pool *pgxpool.Pool,
	txm *pg.TxManager, rec *events.Recorder) (*leadership, error) {
	pgLeases := &workerapi.PGLeaseProvider{DB: pool, Workers: pg.WorkerRepo{}, Clock: clock.Real{}, TTL: cfg.WorkerLeaseTTL}
	if cfg.Coordination != "raft" {
		// Local mode decides at epoch 0. After raft mode raised the fence no local decision could
		// commit, so refuse rather than crash-loop on STALE_EPOCH.
		epoch, leader, err := pg.ClusterRepo{}.GetEpoch(ctx, pool)
		if err != nil {
			return nil, fmt.Errorf("read the scheduler epoch: %w", err)
		}
		if epoch != 0 {
			return nil, fmt.Errorf("FF_COORDINATION=local needs scheduler epoch 0, but this database is at epoch %d (leader %q): "+
				"it was last run with FF_COORDINATION=raft. Keep FF_COORDINATION=raft, or stop every scheduler and reset the fence "+
				"with UPDATE cluster_state SET epoch = 0, leader_id = NULL (docs/runbooks/scheduler-failover.md)", epoch, leader)
		}
		logger.Info("coordination: local (single scheduler, always leader at epoch 0)")
		return &leadership{gate: scheduler.AlwaysLeader{}, leases: pgLeases, close: func() {}}, nil
	}
	node := cfg.NodeID
	if node == "" {
		h, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("FF_SCHED_NODE_ID is unset and the hostname is unknown: %w", err)
		}
		node = h
	}
	metrics, err := coordination.NewElectorMetrics(reg)
	if err != nil {
		return nil, err
	}
	cl, err := client.New(client.Config{Endpoints: cfg.RaftEndpoints})
	if err != nil {
		return nil, fmt.Errorf("raft client: %w", err)
	}
	kv := coordination.NewKV(cl)
	el, err := coordination.NewElector(coordination.ElectorConfig{
		NodeID: node, TTL: cfg.LeaderTTL, Renew: cfg.LeaderRenew, Logger: logger, Metrics: metrics,
	}, kv, coordination.PGFence{Tx: txm, Cluster: pg.ClusterRepo{}, Events: rec})
	if err == nil {
		var registry *coordination.WorkerRegistry
		if registry, err = coordination.NewWorkerRegistry(kv, coordination.RegistryConfig{TTL: cfg.WorkerLeaseTTL}); err == nil {
			logger.Info("coordination: raft", "node_id", node, "raft_endpoints", cfg.RaftEndpoints)
			return &leadership{
				gate: el.Gate(), leases: &workerapi.RaftLeaseProvider{PG: pgLeases, Registry: registry},
				elect: el.Run, close: func() { _ = cl.Close() }, // closing on shutdown; nothing to report
			}, nil
		}
	}
	_ = cl.Close() // construction failed; the error below is the one that matters
	return nil, err
}
