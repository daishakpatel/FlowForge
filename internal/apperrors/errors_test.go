package apperrors

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
)

// TestMappings_Match00E is the 00E §4 table, row for row.
func TestMappings_Match00E(t *testing.T) {
	t.Parallel()
	tests := []struct {
		code Code
		http int
		grpc codes.Code
	}{
		{CodeInvalidArgument, http.StatusBadRequest, codes.InvalidArgument},
		{CodeInvalidWorkflow, http.StatusUnprocessableEntity, codes.InvalidArgument},
		{CodeUnauthenticated, http.StatusUnauthorized, codes.Unauthenticated},
		{CodePermissionDenied, http.StatusForbidden, codes.PermissionDenied},
		{CodeNotFound, http.StatusNotFound, codes.NotFound},
		{CodeConflict, http.StatusConflict, codes.AlreadyExists},
		{CodeInvalidState, http.StatusConflict, codes.FailedPrecondition},
		{CodeStaleAttempt, http.StatusConflict, codes.FailedPrecondition},
		{CodePayloadTooLarge, http.StatusRequestEntityTooLarge, codes.ResourceExhausted},
		{CodeRateLimited, http.StatusTooManyRequests, codes.ResourceExhausted},
		{CodeUnavailable, http.StatusServiceUnavailable, codes.Unavailable},
		{CodeInternal, http.StatusInternalServerError, codes.Internal},
		{CodeNotLeader, http.StatusServiceUnavailable, codes.FailedPrecondition}, // HTTP "—" in 00E
		{CodeNoQuorum, http.StatusServiceUnavailable, codes.Unavailable},         // HTTP "—" in 00E
	}
	var seen []Code
	for _, tt := range tests {
		seen = append(seen, tt.code)
		if got := HTTPStatus(tt.code); got != tt.http {
			t.Errorf("HTTPStatus(%s) = %d, want %d", tt.code, got, tt.http)
		}
		if got := GRPCCode(tt.code); got != tt.grpc {
			t.Errorf("GRPCCode(%s) = %v, want %v", tt.code, got, tt.grpc)
		}
	}
	if !slices.Equal(seen, Codes()) {
		t.Errorf("Codes() = %v, want the table order %v", Codes(), seen)
	}
	if HTTPStatus("BOGUS") != http.StatusInternalServerError || GRPCCode("BOGUS") != codes.Internal {
		t.Error("unknown code must map to 500 / Internal")
	}
}

// TestCodes_ExactlyMatch00E parses the 00E §4 table so the code set can't drift.
func TestCodes_ExactlyMatch00E(t *testing.T) {
	t.Parallel()
	b, err := os.ReadFile("../../implementation/00E_CONTRACTS_REFERENCE.md")
	if err != nil {
		t.Fatal(err)
	}
	section := string(b)
	section = section[strings.Index(section, "## 4. Error code catalog"):strings.Index(section, "## 5.")]
	var want []Code
	for _, m := range regexp.MustCompile(`(?m)^\| ([A-Z_]+) \|`).FindAllStringSubmatch(section, -1) {
		if m[1] != "Code" {
			want = append(want, Code(m[1]))
		}
	}
	if !slices.Equal(Codes(), want) {
		t.Errorf("Codes() = %v, 00E §4 = %v", Codes(), want)
	}
}

func TestErrorsAs_ThroughWrapping(t *testing.T) {
	t.Parallel()
	root := errors.New("pq: no rows")
	err := fmt.Errorf("load run: %w", fmt.Errorf("repo: %w", Wrap(CodeNotFound, "run not found", root)))

	e, ok := errors.AsType[*Error](err)
	if !ok || e.Code != CodeNotFound || e.Message != "run not found" {
		t.Fatalf("errors.As = %v, %v", e, ok)
	}
	if !errors.Is(err, &Error{Code: CodeNotFound}) {
		t.Error("errors.Is by code failed")
	}
	if errors.Is(err, &Error{Code: CodeConflict}) {
		t.Error("errors.Is matched a different code")
	}
	if !errors.Is(err, root) {
		t.Error("cause not reachable through Unwrap")
	}
	if CodeOf(err) != CodeNotFound || FromError(err) != e {
		t.Error("CodeOf/FromError did not find the wrapped *Error")
	}
}

func TestFromError_UnknownBecomesSanitizedInternal(t *testing.T) {
	t.Parallel()
	raw := errors.New("dial tcp 10.0.0.7:5432: password authentication failed for user flowforge")
	e := FromError(fmt.Errorf("query: %w", raw))
	if e.Code != CodeInternal || e.Message != "internal error" || len(e.Details) != 0 {
		t.Errorf("got %+v, want INTERNAL / internal error", e)
	}
	if strings.Contains(e.Message, "10.0.0.7") || !errors.Is(e, raw) {
		t.Error("message must be sanitized and the cause kept for logs")
	}
	if FromError(nil) != nil || CodeOf(nil) != "" || Retryable(nil) {
		t.Error("nil must stay nil")
	}
}

func TestConstructors(t *testing.T) {
	t.Parallel()
	d := Detail{Field: "tasks[0].id", Message: "duplicate"}
	tests := []struct {
		err  *Error
		code Code
	}{
		{InvalidArgument("bad", d), CodeInvalidArgument},
		{InvalidWorkflow("bad dag", d), CodeInvalidWorkflow},
		{Unauthenticated("no key"), CodeUnauthenticated},
		{PermissionDenied("viewer"), CodePermissionDenied},
		{NotFound("run"), CodeNotFound},
		{Conflict("dup"), CodeConflict},
		{InvalidState("RUNNING→READY"), CodeInvalidState},
		{StaleAttempt("token 4 < 5"), CodeStaleAttempt},
		{PayloadTooLarge("1 MiB"), CodePayloadTooLarge},
		{RateLimited("slow down", 2*time.Second), CodeRateLimited},
		{Unavailable("postgres down", time.Second, context.DeadlineExceeded), CodeUnavailable},
		{Internal(errors.New("nil map")), CodeInternal},
		{NotLeader("raft-2:7000"), CodeNotLeader},
		{NoQuorum("1/3 reachable"), CodeNoQuorum},
		{New(CodeConflict, "x"), CodeConflict},
	}
	for _, tt := range tests {
		if tt.err.Code != tt.code {
			t.Errorf("%v: code %s, want %s", tt.err, tt.err.Code, tt.code)
		}
		if !strings.HasPrefix(tt.err.Error(), string(tt.code)+": ") {
			t.Errorf("Error() = %q, want %s: prefix", tt.err.Error(), tt.code)
		}
	}
	if got := InvalidWorkflow("bad dag", d).Details; len(got) != 1 || got[0] != d {
		t.Errorf("details = %v", got)
	}
	if RateLimited("x", 2*time.Second).RetryAfter != 2*time.Second {
		t.Error("RetryAfter not set")
	}
	if got := NotLeader("raft-2:7000").Details; len(got) != 1 || got[0].Field != "leader_hint" {
		t.Errorf("leader hint details = %v", got)
	}
	if len(NotLeader("").Details) != 0 {
		t.Error("empty hint must not add a detail")
	}
	if got := (&Error{Code: CodeNotFound}).Error(); got != "NOT_FOUND" {
		t.Errorf("Error() without message = %q", got)
	}
}

func TestRetryable(t *testing.T) {
	t.Parallel()
	for _, c := range Codes() {
		want := c == CodeUnavailable || c == CodeRateLimited || c == CodeNotLeader || c == CodeNoQuorum
		if c.Retryable() != want || Retryable(New(c, "")) != want {
			t.Errorf("%s retryable = %v, want %v", c, c.Retryable(), want)
		}
	}
	if Retryable(errors.New("unknown")) {
		t.Error("unknown errors are INTERNAL and not retryable")
	}
}
