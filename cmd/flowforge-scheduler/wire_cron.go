package main

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/api/service"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/cron"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/scheduler"
)

// registerCron adds the cron loop (task 03.08). It fires triggers through service.TriggerService,
// which starts runs with the same idempotent RunService.Start the API uses.
func registerCron(runner *scheduler.Runner, cfg config.Scheduler, reg *obs.Registry, pool *pgxpool.Pool,
	txm *pg.TxManager, gen ids.Generator, rec *events.Recorder, loopErrors *prometheus.CounterVec) error {
	fires, err := cron.NewFires(reg)
	if err != nil {
		return err
	}
	defs, runs, triggers := pg.NewDefinitionRepo(gen), pg.NewRunRepo(gen, rec), pg.NewTriggerRepo(gen)
	starter := &service.TriggerService{
		DB: pool, Triggers: triggers, DefRepo: defs, RunRepo: runs, Clock: clock.Real{},
		Runs: &service.RunService{DB: pool, Tx: txm, DefRepo: defs, RunRepo: runs},
	}
	runner.RegisterLoop(&cron.Loop{
		Tx: txm, Cluster: pg.ClusterRepo{}, Triggers: triggers, Starter: starter, Audit: pg.AuditRepo{}, IDs: gen,
		Clock: clock.Real{}, MissedGrace: cfg.CronMissedGrace, MaxCatchUp: cfg.RetentionIdempotency, Fires: fires, Errors: loopErrors,
	})
	return nil
}
