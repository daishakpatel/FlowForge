package rest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/logging"
)

// dbRetryAfter is the Retry-After sent while Postgres is unreachable (master spec §6.2).
const dbRetryAfter = 5 * time.Second

// errorEnvelope is the 00E §4 error body: {"error":{code,message,details,request_id}}.
type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code      apperrors.Code     `json:"code"`
	Message   string             `json:"message"`
	Details   []apperrors.Detail `json:"details"`
	RequestID string             `json:"request_id"`
}

// writeError writes err as the error envelope with the status apperrors.HTTPStatus maps its
// code to. Only the client-safe Message and Details are sent; the cause is logged, once, at
// error level for 5xx (00C §10.1).
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	e := classify(err)
	status := apperrors.HTTPStatus(e.Code)
	ctx := r.Context()
	if status >= http.StatusInternalServerError {
		logging.FromContext(ctx).ErrorContext(ctx, "request failed", "code", e.Code, logging.Err(err))
	} else {
		logging.FromContext(ctx).DebugContext(ctx, "request rejected", "code", e.Code, logging.Err(err))
	}
	if e.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(e.RetryAfter.Seconds()))))
	}
	details := e.Details
	if details == nil {
		details = []apperrors.Detail{}
	}
	writeJSON(w, status, errorEnvelope{Error: errorBody{
		Code: e.Code, Message: e.Message, Details: details, RequestID: requestID(ctx),
	}})
}

// classify turns err into the *apperrors.Error the client sees. Transport and database failures
// that are not the caller's fault become UNAVAILABLE with Retry-After; anything unclassified is
// INTERNAL with a sanitized message.
func classify(err error) *apperrors.Error {
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return apperrors.PayloadTooLarge("request body exceeds 1 MiB")
	}
	if e, ok := errors.AsType[*apperrors.Error](err); ok && e.Code != apperrors.CodeInternal {
		return e
	}
	if _, ok := errors.AsType[*pgconn.ConnectError](err); ok {
		return apperrors.Unavailable("database unavailable", dbRetryAfter, err)
	}
	if connectionLost(err) {
		return apperrors.Unavailable("database unavailable", dbRetryAfter, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return apperrors.Unavailable("request timed out", dbRetryAfter, err)
	}
	return apperrors.FromError(err)
}

// connectionLost reports a database connection that broke under the request: the server dropped
// or refused it (SQLSTATE class 08 except 08P01, a protocol violation that is a client bug, and
// 57P01–57P03 while it shuts down or restarts), or the link failed (unexpected EOF, a network
// error). A pooled connection hits these when Postgres goes away; none is the request's fault.
func connectionLost(err error) bool {
	if pe, ok := errors.AsType[*pgconn.PgError](err); ok {
		c := pe.Code
		return (strings.HasPrefix(c, "08") && c != "08P01") || c == "57P01" || c == "57P02" || c == "57P03"
	}
	_, isNet := errors.AsType[net.Error](err)
	return isNet || errors.Is(err, io.ErrUnexpectedEOF)
}

func requestID(ctx context.Context) string {
	if v, ok := logging.Field(ctx, logging.KeyRequestID); ok {
		return v.String()
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Debug("write response", logging.Err(err)) // the client went away; nothing else to do
	}
}
