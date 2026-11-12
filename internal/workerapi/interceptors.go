package workerapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/obs"
)

// ErrorDomain is the ErrorInfo domain of every FlowForge gRPC error; ErrorInfo.Reason is the
// 00E §4 code, e.g. STALE_ATTEMPT.
const ErrorDomain = "flowforge.dev"

// Interceptors wrap every worker RPC (task 03.01): a request id (returned in the x-request-id
// header and carried in log fields), panic recovery, authentication with auth.VerifyToken
// (only role worker), error mapping to gRPC status codes with an ErrorInfo detail, one log line
// and the gRPC metrics.
type Interceptors struct {
	auth     auth.Authenticator
	ids      ids.Generator
	logger   *slog.Logger
	clock    clock.Clock
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewInterceptors registers flowforge_grpc_requests_total{method,code} and
// flowforge_grpc_request_duration_seconds{method} on reg.
func NewInterceptors(reg *obs.Registry, a auth.Authenticator, g ids.Generator, logger *slog.Logger, c clock.Clock) (*Interceptors, error) {
	requests, err := reg.Counter(prometheus.CounterOpts{
		Name: "flowforge_grpc_requests_total", Help: "Worker gRPC calls by method and status code.",
	}, "method", "code")
	if err != nil {
		return nil, fmt.Errorf("register grpc requests metric: %w", err)
	}
	duration, err := reg.Histogram(prometheus.HistogramOpts{
		Name: "flowforge_grpc_request_duration_seconds", Help: "Worker gRPC call latency by method.", Buckets: prometheus.DefBuckets,
	}, "method")
	if err != nil {
		return nil, fmt.Errorf("register grpc duration metric: %w", err)
	}
	return &Interceptors{auth: a, ids: g, logger: logger, clock: c, requests: requests, duration: duration}, nil
}

// Unary is the grpc.UnaryServerInterceptor.
func (i *Interceptors) Unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	ctx, finish := i.begin(ctx, info.FullMethod)
	defer func() { err = finish(recover(), err) }()
	if ctx, err = i.authenticate(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// Stream is the grpc.StreamServerInterceptor.
func (i *Interceptors) Stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	ctx, finish := i.begin(ss.Context(), info.FullMethod)
	defer func() { err = finish(recover(), err) }()
	if ctx, err = i.authenticate(ctx); err != nil {
		return err
	}
	return handler(srv, &ctxStream{ServerStream: ss, ctx: ctx})
}

// begin starts a call; finish turns a panic or error into a gRPC status, logs and counts it.
func (i *Interceptors) begin(ctx context.Context, method string) (context.Context, func(panicked any, err error) error) {
	start := i.clock.Now()
	id := i.ids.New(ids.PrefixRequest)
	_ = grpc.SetHeader(ctx, metadata.Pairs("x-request-id", id)) // best effort: not every transport carries headers
	ctx = logging.WithFields(logging.NewContext(ctx, i.logger), logging.KeyRequestID, id)
	return ctx, func(panicked any, err error) error {
		if panicked != nil {
			i.logger.ErrorContext(ctx, "grpc handler panic", "method", method, "panic", fmt.Sprint(panicked), "stack", string(debug.Stack()))
			err = apperrors.Internal(fmt.Errorf("panic: %v", panicked))
		}
		err = toStatus(ctx, err)
		code := status.Code(err)
		elapsed := i.clock.Now().Sub(start)
		i.requests.WithLabelValues(method, code.String()).Inc()
		i.duration.WithLabelValues(method).Observe(elapsed.Seconds())
		level := slog.LevelInfo
		if code == codes.Internal || code == codes.Unavailable {
			level = slog.LevelError
		}
		i.logger.Log(ctx, level, "grpc call", "method", method, "code", code.String(), "duration_ms", elapsed.Milliseconds())
		return err
	}
}

// authenticate verifies the bearer key in metadata and requires role worker.
func (i *Interceptors) authenticate(ctx context.Context) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	var token string
	if v := md.Get("authorization"); len(v) == 1 {
		token, _ = strings.CutPrefix(v[0], "Bearer ")
	}
	if token == "" {
		return ctx, apperrors.Unauthenticated("missing API key: send authorization: Bearer ff_…")
	}
	p, err := i.auth.VerifyToken(ctx, token)
	if err != nil {
		return ctx, err
	}
	if p.Role != auth.RoleWorker {
		return ctx, apperrors.PermissionDenied(fmt.Sprintf("role %q may not call the worker API (requires worker)", p.Role))
	}
	return logging.WithFields(auth.WithPrincipal(ctx, p), logging.KeyWorkerID, p.KeyID), nil
}

// toStatus maps an error to a gRPC status: apperrors codes via apperrors.GRPCCode with an
// ErrorInfo{Reason: code, Domain: ErrorDomain} detail, context errors to Canceled or
// DeadlineExceeded. Internal causes are logged, never sent.
func toStatus(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok && !errors.As(err, new(*apperrors.Error)) {
		return err // already a status (e.g. from the gRPC runtime)
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "call cancelled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	}
	e := apperrors.FromError(err)
	if e.Code == apperrors.CodeInternal {
		logging.FromContext(ctx).ErrorContext(ctx, "grpc call failed", logging.Err(err))
	}
	st := status.New(apperrors.GRPCCode(e.Code), e.Message)
	if withInfo, derr := st.WithDetails(&errdetails.ErrorInfo{Reason: string(e.Code), Domain: ErrorDomain}); derr == nil {
		st = withInfo
	}
	return st.Err()
}

// ctxStream replaces a server stream's context with the authenticated one.
type ctxStream struct {
	grpc.ServerStream
	ctx context.Context //nolint:containedctx // grpc.ServerStream exposes its context this way
}

func (s *ctxStream) Context() context.Context { return s.ctx }
