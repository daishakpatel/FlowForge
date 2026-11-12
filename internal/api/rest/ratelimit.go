package rest

import (
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
)

// serviceRoleFactor multiplies the rate and burst of worker and ai-readonly keys, which poll.
const serviceRoleFactor = 4

// limiter is a token bucket per API key (FR-SEC-05): each key may make rps requests per second
// sustained and burst at once. Safe for concurrent use; buckets live as long as the process,
// one per key that has called (keys are few, so this stays small).
type limiter struct {
	mu      sync.Mutex
	clock   clock.Clock
	rps     float64
	burst   float64
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newLimiter(c clock.Clock, rps, burst float64) *limiter {
	return &limiter{clock: c, rps: rps, burst: burst, buckets: map[string]*bucket{}}
}

// allow takes a token from keyID's bucket. When none is left it reports how long until one is.
func (l *limiter) allow(keyID, role string) (bool, time.Duration) {
	rate, burst := l.rps, l.burst
	if auth.IsServiceRole(role) {
		rate, burst = rate*serviceRoleFactor, burst*serviceRoleFactor
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()
	b, ok := l.buckets[keyID]
	if !ok {
		b = &bucket{tokens: burst, last: now}
		l.buckets[keyID] = b
	}
	b.tokens = math.Min(burst, b.tokens+now.Sub(b.last).Seconds()*rate)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / rate * float64(time.Second))
}

// rateLimitMiddleware answers 429 RATE_LIMITED with Retry-After once the caller's bucket is
// empty. It runs after authentication, so only /api/v1 routes are limited; the operational
// endpoints are exempt.
func rateLimitMiddleware(l *limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p, _ := auth.PrincipalFrom(r.Context())
			if ok, wait := l.allow(p.KeyID, p.Role); !ok {
				writeError(w, r, apperrors.RateLimited("rate limit exceeded for this API key", wait))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
