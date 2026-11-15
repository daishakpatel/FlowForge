package runtime

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Dial opens the connection to flowforge-api's WorkerService (FF_API_GRPC_TARGET). It is
// plaintext, as the API serves it (TLS arrives with the deployment hardening tasks), and sends
// token as "authorization: Bearer <token>" on every call. grpc-go reconnects on its own, backing
// off from 100 ms up to 5 s, so a restarted API is found again within seconds.
func Dial(target, token string) (*grpc.ClientConn, error) {
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(bearer(token)),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 5 * time.Second},
			MinConnectTimeout: 5 * time.Second,
		}),
	)
}

// bearer is the worker's API key as per-call credentials. An empty token sends no header, and
// the API answers UNAUTHENTICATED.
type bearer string

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	if b == "" {
		return nil, nil
	}
	return map[string]string{"authorization": "Bearer " + string(b)}, nil
}

// RequireTransportSecurity is false: the API's gRPC port is plaintext until TLS lands.
func (bearer) RequireTransportSecurity() bool { return false }

// reasonOf returns the 00E §4 code the API put in the error's ErrorInfo (e.g. STALE_ATTEMPT),
// or "" for errors without one (transport failures).
func reasonOf(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}

// transient reports whether a call that failed with err may succeed if repeated unchanged: the
// API was unreachable, slow or rate limiting. Every call it is used for is idempotent or fenced
// by the attempt's token, so repeating it is safe (00C §10.1).
func transient(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		return true
	case codes.ResourceExhausted:
		return reasonOf(err) == "RATE_LIMITED"
	default:
		return false
	}
}

// unregistered reports whether err means the API does not know this worker as registered
// (deregistered, or its registration lapsed), so the worker must Register again.
func unregistered(err error) bool {
	return status.Code(err) == codes.FailedPrecondition && reasonOf(err) == "INVALID_STATE"
}

// stale reports whether the API rejected a report or log line for a stale fencing token.
func stale(err error) bool { return reasonOf(err) == "STALE_ATTEMPT" }

// backoffPolicy is bounded exponential backoff with jitter for repeated worker calls:
// base·2^n capped at maxDelay, jittered to [50 %, 150 %). Safe for concurrent use.
type backoffPolicy struct {
	base, maxDelay time.Duration
	mu             sync.Mutex
	rng            *rand.Rand
}

func (b *backoffPolicy) delay(n int) time.Duration {
	d := b.base << min(n, 16)
	if d <= 0 || d > b.maxDelay {
		d = b.maxDelay
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return d/2 + time.Duration(b.rng.Int64N(int64(d)))
}
