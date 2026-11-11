package statemachine

import (
	"math"
	"math/rand/v2"
	"time"

	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// NextRetryDelay returns how long to wait after attempt number attempt (1-based) fails before
// the next attempt is queued (FR-WF-07):
//
//	base  = initial                                     (fixed)
//	base  = min(max_delay, initial · multiplier^(n−1))  (exponential)
//	delay = base · (1 + jitter·u),  u uniform in [−1, 1] from rng
//
// The result is never below 0 nor above max_delay·(1+jitter). With jitter 0, or a nil rng, the
// delay is exactly base. Pass a seeded *rand.Rand for deterministic tests (00C §8.1). attempt
// values below 1 count as 1.
func NextRetryDelay(p model.RetryPolicy, attempt int, rng *rand.Rand) time.Duration {
	maxMS := float64(p.MaxDelayMS)
	base := float64(p.InitialDelayMS)
	if p.Strategy == model.StrategyExponential && attempt > 1 && base > 0 {
		base *= math.Pow(p.Multiplier, float64(attempt-1)) // may overflow to +Inf; capped below
	}
	base = math.Min(base, maxMS)
	u := 0.0
	if rng != nil && p.Jitter > 0 {
		u = rng.Float64()*2 - 1
	}
	ms := math.Max(0, math.Min(base*(1+p.Jitter*u), maxMS*(1+p.Jitter)))
	return time.Duration(ms * float64(time.Millisecond))
}
