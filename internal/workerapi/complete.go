package workerapi

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// Report limits.
const (
	MaxResultBytes     = 256 << 10
	maxErrorMessageLen = 4 << 10
	maxErrorCodeLen    = 64
	codeResultTooLarge = "RESULT_TOO_LARGE"
)

// CompleteTask implements workerv1.WorkerServiceServer. A result over 256 KiB fails the attempt
// (RESULT_TOO_LARGE, not retryable) and answers PAYLOAD_TOO_LARGE; a stale token answers
// STALE_ATTEMPT.
func (s *Service) CompleteTask(ctx context.Context, req *workerv1.CompleteTaskRequest) (*workerv1.CompleteTaskResponse, error) {
	result := req.GetResultJson()
	if len(result) > MaxResultBytes {
		msg := fmt.Sprintf("result is %d bytes; the limit is %d", len(result), MaxResultBytes)
		if err := s.finish(ctx, req.GetAttemptId(), req.GetFencingToken(), report{kind: AttemptFailed, code: codeResultTooLarge, message: msg}); err != nil {
			return nil, err
		}
		return nil, apperrors.PayloadTooLarge(msg)
	}
	if len(result) > 0 && !json.Valid(result) {
		return nil, apperrors.InvalidArgument("result_json is not valid JSON")
	}
	if err := s.finish(ctx, req.GetAttemptId(), req.GetFencingToken(), report{kind: AttemptSucceeded, result: result}); err != nil {
		return nil, err
	}
	return &workerv1.CompleteTaskResponse{}, nil
}

// FailTask implements workerv1.WorkerServiceServer. The message is redacted and capped.
func (s *Service) FailTask(ctx context.Context, req *workerv1.FailTaskRequest) (*workerv1.FailTaskResponse, error) {
	code := req.GetErrorCode()
	if code == "" || len(code) > maxErrorCodeLen {
		return nil, apperrors.InvalidArgument(fmt.Sprintf("error_code must be 1..%d characters", maxErrorCodeLen))
	}
	msg := logging.Redact(req.GetMessage())
	if len(msg) > maxErrorMessageLen {
		msg = truncate(msg, maxErrorMessageLen)
	}
	r := report{kind: AttemptFailed, code: code, message: msg, retryable: req.GetRetryable()}
	if err := s.finish(ctx, req.GetAttemptId(), req.GetFencingToken(), r); err != nil {
		return nil, err
	}
	return &workerv1.FailTaskResponse{}, nil
}

type report struct {
	kind      AttemptKind
	code      string
	message   string
	retryable bool
	result    json.RawMessage
}

// finish ends a RUNNING attempt and hands the outcome to the OutcomeHandler, in one transaction.
// The fencing check is the conditional UPDATE itself (AttemptRepo.Transition: WHERE id AND
// status = 'RUNNING' AND fencing_token), never a read-then-write in Go (ADR-003).
func (s *Service) finish(ctx context.Context, attemptID string, token int64, r report) error {
	to := model.AttemptStatus(r.kind)
	err := s.Tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		err := s.Attempts.Transition(ctx, tx, persistence.AttemptTransitionParams{
			AttemptID: attemptID, From: model.AttemptRunning, To: to, FencingToken: token,
			Mutate: func(m *persistence.AttemptMutation) {
				m.ErrorCode, m.ErrorMessage, m.Result = r.code, r.message, r.result
				if r.kind != AttemptSucceeded {
					m.Retryable = &r.retryable
				}
			},
		})
		if err != nil {
			return err
		}
		a, err := s.Attempts.Get(ctx, tx, attemptID)
		if err != nil {
			return err
		}
		t, err := s.Tasks.Get(ctx, tx, a.TaskID)
		if err != nil {
			return err
		}
		return s.Outcomes.OnAttemptFinished(ctx, tx, AttemptOutcome{
			Attempt: a, Task: t, Kind: r.kind, ErrorCode: r.code, Message: r.message, Retryable: r.retryable, Result: r.result,
		})
	})
	if apperrors.CodeOf(err) == apperrors.CodeStaleAttempt {
		s.StaleRejections.Inc() // a normal, counted outcome (00C §10.1)
	}
	return err
}
