package client

import (
	"context"
	"errors"
	"strconv"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorInfo reasons and metadata keys the server sends (kv.proto, server.Meta*).
const (
	reasonNotLeader    = "NOT_LEADER"
	metaLeaderHint     = "leader_hint"
	metaCurrentVersion = "current_version"
	metaResultCode     = "result_code"
)

// call runs one request with the retry policy and converts its error (toError). leaderOnly marks
// requests only the leader serves: their success updates the leader cache.
func call[R any](ctx context.Context, c *Client, leaderOnly bool, f func(ctx context.Context, endpoint int) (R, error)) (R, error) {
	var res R
	err := c.do(ctx, leaderOnly, func(ctx context.Context, i int) error {
		r, err := f(ctx, i)
		if err == nil {
			res = r
		}
		return err
	})
	return res, err
}

// do runs f against the believed leader and retries the safe cases (package doc): a NOT_LEADER
// hint is followed at once; UNAVAILABLE moves to the next endpoint after a backoff. It gives up
// after MaxAttempts or when ctx (bounded by Config.Timeout) ends.
func (c *Client) do(ctx context.Context, leaderOnly bool, f func(ctx context.Context, endpoint int) error) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.cfg.Timeout)
		defer cancel()
	}
	ep := c.pick()
	down := map[int]bool{} // endpoints that were unavailable during this call
	for attempt := 1; ; attempt++ {
		err := f(ctx, ep)
		if err == nil {
			if leaderOnly {
				c.setLeader(ep)
			}
			return nil
		}
		next, retry, wait := c.classify(ctx, ep, err, down)
		if !retry || attempt >= c.cfg.MaxAttempts || ctx.Err() != nil {
			return toError(err)
		}
		if wait {
			if c.cfg.Clock.Sleep(ctx, c.backoff(attempt)) != nil {
				return toError(err)
			}
		}
		ep = next
	}
}

// classify decides whether err may be retried, where, and whether to back off first. A hint is
// followed at once, unless it names an endpoint already unavailable in this call: until an
// election ends, followers still name the dead leader, and following that hint without waiting
// would spend every attempt bouncing between them.
func (c *Client) classify(ctx context.Context, ep int, err error, down map[int]bool) (next int, retry, wait bool) {
	st := status.Convert(err)
	reason, md := errorInfo(st)
	switch {
	case reason == reasonNotLeader:
		id, perr := strconv.ParseUint(md[metaLeaderHint], 10, 64)
		if to, ok := c.endpointFor(ctx, id); perr == nil && ok && to != ep {
			c.setLeader(to)
			if down[to] {
				// The hinted node may have restarted while its connection waits out gRPC's
				// reconnect backoff, failing every call at once; retry it for real.
				c.conns[to].ResetConnectBackoff()
			}
			return to, true, down[to]
		}
		return c.rotate(ep), true, true
	case st.Code() == codes.Unavailable: // NO_QUORUM, UNAVAILABLE, or the endpoint is down
		down[ep] = true
		return c.rotate(ep), true, true
	}
	return ep, false, false
}

// backoff is the delay before retry attempt+1: "equal jitter", uniform in [ceiling/2, ceiling]
// with ceiling = min(MaxBackoff, BaseBackoff·2^(attempt-1)). The fixed half guarantees the
// attempts span an election: with the defaults, four waits total at least 2.25 s, more than the
// largest randomized election timeout (2 s, 00E §5.3), so an ordinary failover never exhausts
// them.
func (c *Client) backoff(attempt int) time.Duration {
	ceiling := c.cfg.BaseBackoff
	for i := 1; i < attempt && ceiling < c.cfg.MaxBackoff; i++ { // doubling stops at the cap: no overflow
		ceiling *= 2
	}
	ceiling = min(ceiling, c.cfg.MaxBackoff)
	c.rngMu.Lock()
	defer c.rngMu.Unlock()
	return ceiling/2 + time.Duration(c.rng.Int64N(int64(ceiling-ceiling/2)+1))
}

func errorInfo(st *status.Status) (string, map[string]string) {
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info.GetReason(), info.GetMetadata()
		}
	}
	return "", nil
}

// The client's error kinds; match them with errors.Is. A *Error carries the details.
var (
	ErrNotFound          = errors.New("coordination: not found")           // key absent, or bound to an expired lease
	ErrConflict          = errors.New("coordination: version conflict")    // expected_version did not match (Error.CurrentVersion)
	ErrLeaseNotFound     = errors.New("coordination: lease not found")     // never granted, gone, or wrong fencing token
	ErrLeaseExpired      = errors.New("coordination: lease expired")       // the leader's clock says so: re-grant
	ErrInvalidArgument   = errors.New("coordination: invalid argument")    // malformed request or out-of-range value
	ErrResourceExhausted = errors.New("coordination: resource exhausted")  // a KV quota or the entry size limit
	ErrUnavailable       = errors.New("coordination: cluster unavailable") // retries exhausted; a write's outcome is unknown
)

// Error is a failed call. Kind is one of the Err* values (nil for anything else, such as an
// internal error); errors.Is matches it, and Unwrap returns the gRPC status error.
type Error struct {
	Kind           error
	Code           codes.Code // the gRPC code
	Reason         string     // the 00E §4 code, e.g. CONFLICT
	ResultCode     string     // the FSM outcome, e.g. LEASE_EXPIRED, when the FSM decided
	CurrentVersion uint64     // ErrConflict: the key's version (0: absent)
	Message        string
	cause          error
}

func (e *Error) Error() string {
	if e.Kind != nil {
		return e.Kind.Error() + ": " + e.Message
	}
	return "coordination: " + e.Code.String() + ": " + e.Message
}

// Is matches e's Kind.
func (e *Error) Is(target error) bool { return e.Kind != nil && target == e.Kind }

// Unwrap returns the underlying gRPC status error.
func (e *Error) Unwrap() error { return e.cause }

// toError types a gRPC error.
func toError(err error) error {
	st := status.Convert(err)
	reason, md := errorInfo(st)
	e := &Error{Code: st.Code(), Reason: reason, ResultCode: md[metaResultCode], Message: st.Message(), cause: err}
	switch {
	case e.ResultCode == "LEASE_NOT_FOUND":
		e.Kind = ErrLeaseNotFound
	case e.ResultCode == "LEASE_EXPIRED":
		e.Kind = ErrLeaseExpired
	case reason == "CONFLICT":
		e.Kind = ErrConflict
		e.CurrentVersion, _ = strconv.ParseUint(md[metaCurrentVersion], 10, 64) // absent: 0
	case st.Code() == codes.NotFound:
		e.Kind = ErrNotFound
	case st.Code() == codes.InvalidArgument:
		e.Kind = ErrInvalidArgument
	case st.Code() == codes.ResourceExhausted:
		e.Kind = ErrResourceExhausted
	case st.Code() == codes.Unavailable, st.Code() == codes.DeadlineExceeded, st.Code() == codes.Canceled,
		reason == reasonNotLeader:
		e.Kind = ErrUnavailable
	}
	return e
}
