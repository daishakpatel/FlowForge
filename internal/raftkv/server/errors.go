package server

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/node"
)

// Every KV error is a gRPC status with an ErrorInfo{Domain: ErrorDomain, Reason: <00E §4 code>}
// whose metadata may carry these keys (kv.proto documents them for clients).
const (
	ErrorDomain        = "flowforge.dev"
	MetaLeaderHint     = "leader_hint"     // NOT_LEADER: the leader's node id
	MetaCurrentVersion = "current_version" // CONFLICT: the key's version (0: absent)
	MetaResultCode     = "result_code"     // an FSM outcome, e.g. LEASE_EXPIRED (fsm.ResultCode without its prefix)
)

// kvError is a KV error that knows its gRPC status. cause is logged, never sent.
type kvError struct {
	code  apperrors.Code
	msg   string
	meta  map[string]string
	cause error
}

func (e *kvError) Error() string { return e.msg }
func (e *kvError) Unwrap() error { return e.cause }

// GRPCStatus makes the gRPC runtime send e as its status. CONFLICT is ABORTED, the code 00E §4
// allows for version conflicts and kv.proto documents.
func (e *kvError) GRPCStatus() *status.Status {
	c := apperrors.GRPCCode(e.code)
	if e.code == apperrors.CodeConflict {
		c = codes.Aborted
	}
	st := status.New(c, e.msg)
	if d, err := st.WithDetails(&errdetails.ErrorInfo{Reason: string(e.code), Domain: ErrorDomain, Metadata: e.meta}); err == nil {
		return d
	}
	return st
}

func newErr(code apperrors.Code, msg string, meta map[string]string) *kvError {
	return &kvError{code: code, msg: msg, meta: meta}
}

func invalid(msg string) error { return newErr(apperrors.CodeInvalidArgument, msg, nil) }

func internal(cause error) error {
	return &kvError{code: apperrors.CodeInternal, msg: "internal error", cause: cause}
}

func notLeader(lead uint64) error {
	if lead == 0 {
		return newErr(apperrors.CodeNoQuorum, "no raft leader", nil)
	}
	return newErr(apperrors.CodeNotLeader, "not the leader", map[string]string{MetaLeaderHint: strconv.FormatUint(lead, 10)})
}

// resultCode is the FSM code's name without its enum prefix, e.g. LEASE_EXPIRED.
func resultCode(c pb.ResultCode) string { return strings.TrimPrefix(c.String(), "RESULT_CODE_") }

// resultErr maps a failed FSM result (code ≠ OK) to its 00E §4 error (raft-fsm.md's table).
func resultErr(r *pb.CommandResult) error {
	md := map[string]string{MetaResultCode: resultCode(r.GetCode())}
	switch r.GetCode() {
	case pb.ResultCode_RESULT_CODE_VERSION_MISMATCH:
		md[MetaCurrentVersion] = strconv.FormatUint(r.GetKv().GetVersion(), 10)
		return newErr(apperrors.CodeConflict, "version mismatch", md)
	case pb.ResultCode_RESULT_CODE_KEY_NOT_FOUND:
		md[MetaCurrentVersion] = "0"
		return newErr(apperrors.CodeConflict, "key not found at the expected version", md)
	case pb.ResultCode_RESULT_CODE_LEASE_NOT_FOUND:
		return newErr(apperrors.CodeNotFound, "lease not found", md)
	case pb.ResultCode_RESULT_CODE_LEASE_EXPIRED:
		return newErr(apperrors.CodeNotFound, "lease expired", md)
	case pb.ResultCode_RESULT_CODE_TXN_TOO_LARGE:
		return newErr(apperrors.CodeInvalidArgument, "transaction too large", md)
	case pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT:
		return newErr(apperrors.CodeInvalidArgument, "invalid argument", md)
	case pb.ResultCode_RESULT_CODE_QUOTA_EXCEEDED:
		return newErr(apperrors.CodePayloadTooLarge, "quota exceeded", md)
	}
	return internal(errors.New("unexpected fsm result " + r.GetCode().String()))
}

// nodeErr maps a node error. ctx is the caller's: when it has ended, its own error is returned
// (CANCELLED or DEADLINE_EXCEEDED) rather than the server-side timeout it caused.
func nodeErr(ctx context.Context, err error) error {
	if nl, ok := errors.AsType[*node.NotLeaderError](err); ok {
		return notLeader(nl.Leader)
	}
	switch {
	case errors.Is(err, node.ErrNoQuorum):
		return newErr(apperrors.CodeNoQuorum, "no raft quorum", nil)
	case errors.Is(err, node.ErrProposalTooLarge):
		return newErr(apperrors.CodePayloadTooLarge, "request too large for one raft entry", nil)
	case ctx.Err() != nil:
		return status.FromContextError(ctx.Err()).Err()
	case errors.Is(err, node.ErrProposalTimeout), errors.Is(err, node.ErrReadIndexTimeout),
		errors.Is(err, node.ErrProposalDropped), errors.Is(err, node.ErrNodeStopped):
		return &kvError{code: apperrors.CodeUnavailable, msg: "raft unavailable; retry", cause: err}
	}
	return internal(err)
}
