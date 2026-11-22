// Package coordination is the public name of the Raft KV client (task 04.04): the same types and
// functions as internal/raftkv/client, re-exported so code outside this module can use them. The
// generated proto types the API takes and returns live under internal/, which other modules can't
// import, so the ones the API needs are aliased here too (Txn compares and ops, the responses).
// See that package for the retry and consistency guarantees, and
// docs/architecture/raft-kv-api.md for the API.
//
//	c, err := coordination.New(coordination.Config{Endpoints: []string{"raft-1:7000", "raft-2:7000", "raft-3:7000"}})
//	if err != nil { … }
//	defer c.Close()
//	kv, err := c.Put(ctx, "/cluster/config", cfgJSON)
package coordination

import (
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/client"
)

type (
	// Client is client.Client: the typed KV, lease and counter API.
	Client = client.Client
	// Config is client.Config.
	Config = client.Config
	// Option adjusts one call (client.Option).
	Option = client.Option
	// Error is a failed call (client.Error).
	Error = client.Error
)

// The error kinds; match them with errors.Is.
var (
	ErrNotFound          = client.ErrNotFound
	ErrConflict          = client.ErrConflict
	ErrLeaseNotFound     = client.ErrLeaseNotFound
	ErrLeaseExpired      = client.ErrLeaseExpired
	ErrInvalidArgument   = client.ErrInvalidArgument
	ErrResourceExhausted = client.ErrResourceExhausted
	ErrUnavailable       = client.ErrUnavailable
)

// New returns a client for cfg's endpoints (client.New).
func New(cfg Config) (*Client, error) { return client.New(cfg) }

// WithExpectedVersion makes Put and Delete apply only at version v (client.WithExpectedVersion).
func WithExpectedVersion(v uint64) Option { return client.WithExpectedVersion(v) }

// WithLease binds a Put's key to a lease (client.WithLease).
func WithLease(id uint64) Option { return client.WithLease(id) }

// Serializable makes Get and List read possibly stale local state (client.Serializable).
func Serializable() Option { return client.Serializable() }

// WithLimit caps a List page (client.WithLimit).
func WithLimit(n uint32) Option { return client.WithLimit(n) }

// WithStartKey continues a List from a previous page's next_key (client.WithStartKey).
func WithStartKey(k string) Option { return client.WithStartKey(k) }

// The coordination/v1 proto types the client's methods take and return; kv.proto documents them.
type (
	// KeyValue is coordination/v1 KeyValue.
	KeyValue = pb.KeyValue
	// GetResponse is coordination/v1 GetResponse.
	GetResponse = pb.GetResponse
	// ListResponse is coordination/v1 ListResponse.
	ListResponse = pb.ListResponse
	// CASResponse is coordination/v1 CASResponse.
	CASResponse = pb.CASResponse
	// TxnResponse is coordination/v1 TxnResponse.
	TxnResponse = pb.TxnResponse
	// OpResult is coordination/v1 OpResult.
	OpResult = pb.OpResult
	// LeaseGrantResponse is coordination/v1 LeaseGrantResponse.
	LeaseGrantResponse = pb.LeaseGrantResponse
	// LeaseKeepAliveResponse is coordination/v1 LeaseKeepAliveResponse.
	LeaseKeepAliveResponse = pb.LeaseKeepAliveResponse
	// StatusResponse is coordination/v1 StatusResponse.
	StatusResponse = pb.StatusResponse
	// Member is coordination/v1 Member.
	Member = pb.Member
	// Compare is coordination/v1 Compare.
	Compare = pb.Compare
	// CompareTarget is coordination/v1 Compare_Target.
	CompareTarget = pb.Compare_Target
	// CompareResult is coordination/v1 Compare_Result.
	CompareResult = pb.Compare_Result
	// CompareVersion is coordination/v1 Compare_Version.
	CompareVersion = pb.Compare_Version
	// CompareValue is coordination/v1 Compare_Value.
	CompareValue = pb.Compare_Value
	// CompareLeaseID is coordination/v1 Compare_LeaseId.
	CompareLeaseID = pb.Compare_LeaseId
	// Op is coordination/v1 Op.
	Op = pb.Op
	// OpPut is coordination/v1 Op_Put.
	OpPut = pb.Op_Put
	// OpDelete is coordination/v1 Op_Delete.
	OpDelete = pb.Op_Delete
	// OpGet is coordination/v1 Op_Get.
	OpGet = pb.Op_Get
	// PutOp is coordination/v1 PutOp.
	PutOp = pb.PutOp
	// DeleteOp is coordination/v1 DeleteOp.
	DeleteOp = pb.DeleteOp
	// GetOp is coordination/v1 GetOp.
	GetOp = pb.GetOp
)

// Compare targets and results.
const (
	TargetVersion = pb.Compare_TARGET_VERSION
	TargetValue   = pb.Compare_TARGET_VALUE
	TargetLease   = pb.Compare_TARGET_LEASE
	Equal         = pb.Compare_RESULT_EQUAL
	NotEqual      = pb.Compare_RESULT_NOT_EQUAL
	Greater       = pb.Compare_RESULT_GREATER
	Less          = pb.Compare_RESULT_LESS
)
