package cron

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/obs"
)

// The flowforge_cron_fires_total outcomes.
const (
	OutcomeFired    = "fired"    // a run was started
	OutcomeReplayed = "replayed" // the fire's run already existed (a retry after a crash or a second loop)
	OutcomeSkipped  = "skipped"  // missed fires dropped by the missed-fire policy
	OutcomeFailed   = "failed"   // the run could not be started for good (e.g. the workflow is gone)
	OutcomeRetried  = "retried"  // a transient failure: the fire stays due and is retried next tick
)

// NewFires registers flowforge_cron_fires_total{outcome}.
func NewFires(reg *obs.Registry) (*prometheus.CounterVec, error) {
	c, err := reg.Counter(prometheus.CounterOpts{
		Name: "flowforge_cron_fires_total", Help: "Cron trigger fires by outcome: fired, replayed, skipped, failed, retried.",
	}, "outcome")
	if err != nil {
		return nil, fmt.Errorf("register cron fires metric: %w", err)
	}
	return c, nil
}
