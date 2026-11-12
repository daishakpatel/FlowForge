package workerapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// Log limits (task 03.01).
const (
	MaxLogLinesPerAttempt = 1000
	MaxLogLineBytes       = 8 << 10
	logBatchSize          = 100
	truncatedSuffix       = "…[truncated]"
	limitMarker           = "log limit reached"
)

var logLevels = []string{"DEBUG", "INFO", "WARN", "ERROR"}

// attemptLog is what ShipLogs tracks per attempt within one stream.
type attemptLog struct {
	taskID, runID string
	stored        int // rows already in task_logs, including any limit marker
}

// ShipLogs implements workerv1.WorkerServiceServer. Each line's attempt must belong to a worker
// registered by the caller's key and carry that attempt's fencing token, or the stream ends with
// STALE_ATTEMPT / PERMISSION_DENIED. Messages and fields are redacted (logging.Redact), lines over
// 8 KiB are truncated, and an attempt keeps at most 1000 lines: the next line becomes one WARN
// "log limit reached" marker and the rest are dropped. Lines are stored in batches of up to 100,
// each batch in one transaction.
func (s *Service) ShipLogs(stream workerv1.WorkerService_ShipLogsServer) error {
	ctx := stream.Context()
	seen := map[string]*attemptLog{}
	var batch []persistence.NewTaskLog
	var accepted, dropped int32
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := s.Tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error { return s.Logs.Insert(ctx, tx, batch) })
		batch = batch[:0]
		return err
	}
	for {
		line, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if err := flush(); err != nil {
				return err
			}
			return stream.SendAndClose(&workerv1.ShipLogsResponse{Accepted: accepted, Dropped: dropped})
		}
		if err != nil {
			return err
		}
		st, err := s.attemptFor(ctx, seen, line)
		if err != nil {
			return err
		}
		switch {
		case st.stored > MaxLogLinesPerAttempt: // marker already written
			dropped++
			continue
		case st.stored == MaxLogLinesPerAttempt:
			batch = append(batch, persistence.NewTaskLog{
				AttemptID: line.GetAttemptId(), TaskID: st.taskID, RunID: st.runID, TS: s.Clock.Now(), Level: "WARN", Message: limitMarker,
			})
			dropped++
		default:
			batch = append(batch, s.logRow(line, st))
			accepted++
		}
		st.stored++
		if len(batch) >= logBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// attemptFor checks the line's attempt once per stream and returns its tracking state.
func (s *Service) attemptFor(ctx context.Context, seen map[string]*attemptLog, line *workerv1.LogLine) (*attemptLog, error) {
	id := line.GetAttemptId()
	a, err := s.Attempts.Get(ctx, s.DB, id)
	if err == nil && a.FencingToken != line.GetFencingToken() {
		err = apperrors.StaleAttempt(fmt.Sprintf("attempt %s has another fencing token", id))
	}
	if apperrors.CodeOf(err) == apperrors.CodeNotFound {
		err = apperrors.StaleAttempt(fmt.Sprintf("attempt %s does not exist", id))
	}
	if err != nil {
		return nil, err
	}
	if st, ok := seen[id]; ok {
		return st, nil
	}
	if _, err := s.registeredWorker(ctx, a.WorkerID); err != nil {
		return nil, apperrors.PermissionDenied(fmt.Sprintf("attempt %s does not belong to a worker of this API key", id))
	}
	t, err := s.Tasks.Get(ctx, s.DB, a.TaskID)
	if err != nil {
		return nil, err
	}
	n, err := s.Logs.Count(ctx, s.DB, id)
	if err != nil {
		return nil, err
	}
	st := &attemptLog{taskID: t.ID, runID: t.RunID, stored: n}
	seen[id] = st
	return st, nil
}

func (s *Service) logRow(line *workerv1.LogLine, st *attemptLog) persistence.NewTaskLog {
	level := strings.ToUpper(line.GetLevel())
	if !slices.Contains(logLevels, level) {
		level = "INFO"
	}
	ts := s.Clock.Now()
	if line.GetTs().IsValid() {
		ts = line.GetTs().AsTime()
	}
	msg := logging.Redact(strings.ToValidUTF8(line.GetMessage(), "�"))
	if len(msg) > MaxLogLineBytes {
		msg = truncate(msg, MaxLogLineBytes)
	}
	var fields json.RawMessage
	if f := line.GetFieldsJson(); len(f) > 0 && len(f) <= MaxLogLineBytes {
		var obj map[string]json.RawMessage
		if red := logging.Redact(string(f)); json.Unmarshal([]byte(red), &obj) == nil {
			fields = json.RawMessage(red) // fields that are not a JSON object are dropped
		}
	}
	return persistence.NewTaskLog{
		AttemptID: line.GetAttemptId(), TaskID: st.taskID, RunID: st.runID, TS: ts, Level: level, Message: msg, Fields: fields,
	}
}

// truncate cuts s to at most limit bytes, on a rune boundary, ending in the truncation marker.
func truncate(s string, limit int) string {
	cut := limit - len(truncatedSuffix)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + truncatedSuffix
}
