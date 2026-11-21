package fsm

import (
	"errors"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// Limits. Every one is enforced as a deterministic result (INVALID_ARGUMENT, TXN_TOO_LARGE,
// QUOTA_EXCEEDED), never as a Go error, so every replica reaches the same outcome.
const (
	MaxKeyBytes      = 512        // a key or counter name
	MaxValueBytes    = 256 << 10  // a value
	MaxKeys          = 100_000    // live keys
	MaxCounters      = 100_000    // counters
	MaxLeases        = 100_000    // live leases
	MaxTxnCompares   = 8          // compares of one Txn
	MaxTxnOps        = 8          // success and failure ops of one Txn, together
	MaxLeaseTTLMs    = 86_400_000 // 24 h; bounds expires_at arithmetic
	MaxIDBytes       = 256        // client_id, request_id, lease owner
	MaxSnapshotBytes = 64 << 20   // an encoded snapshot
	// MaxStateBytes bounds the bytes of keys, values, counter names and lease owners together,
	// and MaxDedupeBytes the encoded results the dedupe table remembers. The per-key limits alone
	// allow far more than a snapshot may hold; these keep every reachable state, plus encoding
	// overhead, under MaxSnapshotBytes, so snapshots never start failing for good. Worst case:
	// 40 MiB of data + 8 MiB of dedupe + about 8.5 MiB of protobuf framing for 100 000 keys,
	// leases and counters and 10 000 dedupe entries ≈ 56.5 MiB.
	MaxStateBytes  = 40 << 20
	MaxDedupeBytes = 8 << 20

	// DefaultDedupeCapacity is how many (client_id, request_id) results are remembered.
	DefaultDedupeCapacity = 10_000
)

var (
	// ErrAlreadyApplied is returned by Apply for an index at or below the applied index whose
	// command has no remembered result. The state is unchanged.
	ErrAlreadyApplied = errors.New("raft entry already applied")
	// ErrSnapshotTooLarge is returned by Snapshot and Restore beyond MaxSnapshotBytes.
	ErrSnapshotTooLarge = errors.New("fsm snapshot exceeds MaxSnapshotBytes")
	// ErrSnapshotVersion is returned by Restore for a snapshot format this build doesn't know.
	ErrSnapshotVersion = errors.New("unknown fsm snapshot version")
	// ErrCorruptSnapshot is returned by Restore for data that doesn't decode to a valid state.
	ErrCorruptSnapshot = errors.New("corrupt fsm snapshot")
)

const (
	codeOK       = pb.ResultCode_RESULT_CODE_OK
	codeInvalid  = pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT
	codeMismatch = pb.ResultCode_RESULT_CODE_VERSION_MISMATCH
	codeNoKey    = pb.ResultCode_RESULT_CODE_KEY_NOT_FOUND
	codeNoLease  = pb.ResultCode_RESULT_CODE_LEASE_NOT_FOUND
	codeExpired  = pb.ResultCode_RESULT_CODE_LEASE_EXPIRED
	codeTooLarge = pb.ResultCode_RESULT_CODE_TXN_TOO_LARGE
	codeQuota    = pb.ResultCode_RESULT_CODE_QUOTA_EXCEEDED
)

func result(c pb.ResultCode) *pb.CommandResult { return &pb.CommandResult{Code: c} }

// checkKey validates a key or counter name.
func checkKey(k string) pb.ResultCode {
	if k == "" || len(k) > MaxKeyBytes {
		return codeInvalid
	}
	return codeOK
}

func checkKV(k string, v []byte) pb.ResultCode {
	if len(v) > MaxValueBytes {
		return codeInvalid
	}
	return checkKey(k)
}
