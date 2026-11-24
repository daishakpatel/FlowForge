package coordination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/raftkv/client"
)

// The leadership keys (00E §5.2).
const (
	LeaderKey    = "/cluster/scheduler/leader"
	EpochCounter = "/counters/scheduler_epoch"
)

// ErrNoLeader is returned by Current when no scheduler holds the leader key.
var ErrNoLeader = errors.New("coordination: no scheduler leader")

// LeaderInfo is the leader key's value (00E §5.2).
type LeaderInfo struct {
	NodeID  string `json:"node_id"`
	Epoch   int64  `json:"epoch"`
	SinceMs int64  `json:"since_ms"` // the candidate's clock when it campaigned
}

// EpochFence activates a new leader's epoch in Postgres before leadership is announced (ADR-003):
// 04.07 implements it with ClusterRepo.SetEpochIfGreater, after which a deposed leader's
// transactions fail their epoch assertion. An error means the epoch is not active; the Elector
// gives the term up and campaigns again.
type EpochFence interface {
	Activate(ctx context.Context, epoch int64, nodeID string) error
}

// ElectorConfig configures an Elector. Zero values take the defaults noted.
type ElectorConfig struct {
	NodeID string        // this scheduler instance, unique among candidates
	TTL    time.Duration // leader lease TTL (FF_LEADER_TTL, 10s)
	Renew  time.Duration // keep-alive interval (FF_LEADER_RENEW, 3s); must be below TTL − SafetyMargin
	// SafetyMargin is how long before the server could expire the lease this instance stops
	// believing it leads; default TTL/3. It must cover clock-rate drift and GC pauses.
	SafetyMargin time.Duration
	Clock        clock.Clock  // default clock.Real
	Rand         *rand.Rand   // campaign jitter; default seeded from the clock
	Logger       *slog.Logger // default: discard
	Metrics      *ElectorMetrics
}

// Elector campaigns for scheduler leadership and keeps it (task 04.05, 00_MASTER §3.5):
//
//  1. While a leader key exists it only watches (Current every Renew, jittered).
//  2. When none does: grant a TTL lease, take the next epoch from EpochCounter (epochs taken by
//     lost campaigns are skipped: only monotonicity matters), and CAS the leader key from
//     absent to {node, epoch, since} bound to the lease. Losing the CAS ends the attempt.
//  3. Activate the epoch through the EpochFence; only then announce the term on the Gate.
//  4. Keep the lease alive every Renew. LEASE_EXPIRED or LEASE_NOT_FOUND ends the term at once;
//     other failures are retried until the local deadline, where the term ends without waiting
//     for the server (local conservative expiry, see Gate).
//  5. After a term, or a failed attempt, revoke the lease (freeing the key), back off with jitter
//     and start over.
//
// Run is the only goroutine; Gate and Current are safe for concurrent use.
type Elector struct {
	cfg   ElectorConfig
	kv    KV
	fence EpochFence
	gate  *Gate
	rng   *rand.Rand // Run's goroutine only
}

// NewElector returns an elector for cfg over kv.
func NewElector(cfg ElectorConfig, kv KV, fence EpochFence) (*Elector, error) {
	if cfg.SafetyMargin == 0 {
		cfg.SafetyMargin = cfg.TTL / 3
	}
	switch {
	case cfg.NodeID == "":
		return nil, errors.New("coordination: elector needs a node id")
	case kv == nil || fence == nil:
		return nil, errors.New("coordination: elector needs a kv and an epoch fence")
	case cfg.TTL <= 0 || cfg.Renew <= 0 || cfg.SafetyMargin <= 0 || cfg.Renew >= cfg.TTL-cfg.SafetyMargin:
		return nil, fmt.Errorf("coordination: need 0 < renew %s < ttl %s − margin %s", cfg.Renew, cfg.TTL, cfg.SafetyMargin)
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.Rand == nil {
		n := uint64(cfg.Clock.Now().UnixNano())   //nolint:gosec // G115: any bits make a seed
		cfg.Rand = rand.New(rand.NewPCG(n, n>>1)) //nolint:gosec // G404: campaign jitter only
	}
	return &Elector{cfg: cfg, kv: kv, fence: fence, gate: newGate(cfg.Clock), rng: cfg.Rand}, nil
}

// Gate returns the leadership gate the scheduler's loops consult.
func (e *Elector) Gate() *Gate { return e.gate }

// Current returns the leader key's value (a linearizable read), or ErrNoLeader.
func (e *Elector) Current(ctx context.Context) (LeaderInfo, error) { return CurrentLeader(ctx, e.kv) }

// CurrentLeader reads the scheduler leader key through kv (a linearizable read), or returns
// ErrNoLeader; for readers that are not candidates, such as the API's cluster status (04.07).
func CurrentLeader(ctx context.Context, k KV) (LeaderInfo, error) {
	kv, err := k.Get(ctx, LeaderKey)
	if errors.Is(err, client.ErrNotFound) {
		return LeaderInfo{}, ErrNoLeader
	}
	if err != nil {
		return LeaderInfo{}, fmt.Errorf("read leader key: %w", err)
	}
	var info LeaderInfo
	if err := json.Unmarshal(kv.GetValue(), &info); err != nil {
		return LeaderInfo{}, fmt.Errorf("decode leader key: %w", err)
	}
	return info, nil
}

// Run campaigns and leads until ctx ends; it then ends any term and frees the key. It returns
// nil once ctx is done.
func (e *Elector) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		_, err := e.Current(ctx)
		switch {
		case errors.Is(err, ErrNoLeader):
			e.term(ctx)
		case err != nil && ctx.Err() == nil:
			e.cfg.Logger.WarnContext(ctx, "coordination: cannot read the leader key", "err", err)
		}
		if e.sleep(ctx, e.jitter(e.cfg.Renew)) != nil {
			break
		}
	}
	return nil
}

// term makes one campaign and, if it wins, leads until the term ends. It always leaves the
// gate not leading and the lease revoked.
func (e *Elector) term(ctx context.Context) {
	start := e.cfg.Clock.Now()
	id, token, err := e.kv.LeaseGrant(ctx, e.cfg.TTL, e.cfg.NodeID)
	if err != nil {
		e.cfg.Logger.WarnContext(ctx, "coordination: lease grant failed", "err", err)
		return
	}
	defer e.release(ctx, id)
	deadline := start.Add(e.cfg.TTL - e.cfg.SafetyMargin)
	epoch, ok := e.claim(ctx, id, start)
	if !ok {
		return
	}
	if err := e.fence.Activate(ctx, epoch, e.cfg.NodeID); err != nil {
		e.cfg.Logger.WarnContext(ctx, "coordination: epoch fence activation failed; giving the term up", "epoch", epoch, "err", err)
		return
	}
	if !e.cfg.Clock.Now().Before(deadline) {
		return // the campaign outlasted its own lease's local deadline
	}
	e.gate.begin(epoch, deadline)
	e.cfg.Metrics.began(epoch)
	e.cfg.Logger.InfoContext(ctx, "coordination: leading", "epoch", epoch)
	e.hold(ctx, id, token, deadline)
	if e.gate.end() {
		e.cfg.Metrics.ended()
		e.cfg.Logger.InfoContext(ctx, "coordination: leadership ended", "epoch", epoch)
	}
}

// claim takes an epoch and CASes the leader key from absent, bound to lease id.
func (e *Elector) claim(ctx context.Context, id uint64, start time.Time) (int64, bool) {
	n, err := e.kv.IncrCounter(ctx, EpochCounter)
	if err != nil {
		e.cfg.Logger.WarnContext(ctx, "coordination: epoch counter failed", "err", err)
		return 0, false
	}
	epoch := int64(n) //nolint:gosec // G115: a counter incremented once per campaign never nears 2⁶³
	value, err := json.Marshal(LeaderInfo{NodeID: e.cfg.NodeID, Epoch: epoch, SinceMs: start.UnixMilli()})
	if err != nil {
		return 0, false
	}
	won, _, err := e.kv.CAS(ctx, LeaderKey, 0, value, id)
	if err != nil {
		e.cfg.Logger.WarnContext(ctx, "coordination: leader CAS failed", "err", err)
	}
	return epoch, won && err == nil
}

// hold renews the lease until the term ends: ctx done, the lease gone on the server, or the
// local deadline reached without a successful keep-alive.
func (e *Elector) hold(ctx context.Context, id, token uint64, deadline time.Time) {
	next := deadline.Add(-(e.cfg.TTL - e.cfg.SafetyMargin)).Add(e.cfg.Renew) // grant start + Renew
	for {
		now := e.cfg.Clock.Now()
		if !now.Before(deadline) {
			e.cfg.Logger.WarnContext(ctx, "coordination: lease not renewed in time; stepping down before it can expire")
			return
		}
		if now.Before(next) {
			if e.sleep(ctx, min(next.Sub(now), deadline.Sub(now))) != nil {
				return
			}
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, deadline.Sub(now))
		err := e.kv.LeaseKeepAlive(rctx, id, token)
		cancel()
		switch {
		case err == nil:
			deadline = now.Add(e.cfg.TTL - e.cfg.SafetyMargin)
			e.gate.extend(deadline)
			next = now.Add(e.cfg.Renew)
		case errors.Is(err, client.ErrLeaseExpired), errors.Is(err, client.ErrLeaseNotFound):
			e.cfg.Logger.WarnContext(ctx, "coordination: leader lease lost", "err", err)
			return
		case ctx.Err() != nil:
			return
		default:
			e.cfg.Logger.WarnContext(ctx, "coordination: lease renewal failed; retrying", "err", err)
			next = now.Add(e.cfg.Renew / 3)
		}
	}
}

// release revokes the term's lease, which deletes the leader key so the next campaign need not
// wait for the TTL. Best effort: a lease the server already dropped, or an unreachable KV, just
// expires on its own.
func (e *Elector) release(ctx context.Context, id uint64) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.cfg.Renew)
	defer cancel()
	if err := e.kv.LeaseRevoke(rctx, id); err != nil && !errors.Is(err, client.ErrLeaseNotFound) {
		e.cfg.Logger.DebugContext(ctx, "coordination: lease revoke failed; it will expire", "err", err)
	}
}

// jitter spreads d over [d/2, 3d/2) so candidates don't campaign in lockstep.
func (e *Elector) jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(e.rng.Int64N(int64(d)))
}

// sleep waits d on the elector's clock; the only clock waiter while the elector is at rest.
func (e *Elector) sleep(ctx context.Context, d time.Duration) error {
	return e.cfg.Clock.Sleep(ctx, d)
}
