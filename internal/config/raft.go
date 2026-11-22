package config

import "time"

// Raft configures a flowforge-raft node (00E §6). The identity and address fields are per node,
// so they have no default. NodeID is the non-zero uint64 the etcd Raft library requires; the format of Peers
// entries is defined by task 04.02.
type Raft struct {
	Common
	NodeID     uint64   `env:"FF_RAFT_NODE_ID" validate:"positive"`
	Peers      []string `env:"FF_RAFT_PEERS" validate:"required"`
	DataDir    string   `env:"FF_RAFT_DATA_DIR" validate:"required"`
	ClientAddr string   `env:"FF_RAFT_CLIENT_ADDR" validate:"required,addr"`
	PeerAddr   string   `env:"FF_RAFT_PEER_ADDR" validate:"required,addr"`
	// ProposalTimeout bounds how long a write waits for its entry to apply, inside the client's
	// own deadline (task 04.04); a timeout is UNAVAILABLE and safe to retry with the same
	// request id.
	ProposalTimeout time.Duration `env:"FF_RAFT_PROPOSAL_TIMEOUT" default:"3s" validate:"positive"`
	HTTPAddr        string        `env:"FF_RAFT_HTTP_ADDR" default:":8083" validate:"addr"` // /metrics, /healthz, /readyz
	// SnapshotEntries and SnapshotTrailing are the snapshot policy (04.06, 00E §5.3): snapshot every
	// SnapshotEntries applied entries (≥ 50) and keep SnapshotTrailing entries before it.
	SnapshotEntries  uint64 `env:"FF_RAFT_SNAPSHOT_ENTRIES" default:"10000" validate:"positive"`
	SnapshotTrailing uint64 `env:"FF_RAFT_SNAPSHOT_TRAILING" default:"1000" validate:"positive"`
	// AdminToken authorises the Admin gRPC service (transfer-leader, snapshot-now, compact); empty
	// disables it. Dev/ops tooling (04.06).
	AdminToken Secret `env:"FF_RAFT_ADMIN_TOKEN" default:""`
}

// Redacted returns every variable with secrets masked, for the startup log.
func (c Raft) Redacted() map[string]string { return redacted(c) }

func (c Raft) crossCheck() []FieldError {
	var errs []FieldError
	if c.SnapshotEntries < 50 {
		errs = append(errs, FieldError{Env: "FF_RAFT_SNAPSHOT_ENTRIES", Msg: "must be at least 50"})
	}
	if c.SnapshotTrailing >= c.SnapshotEntries {
		errs = append(errs, FieldError{Env: "FF_RAFT_SNAPSHOT_TRAILING", Msg: "must be below FF_RAFT_SNAPSHOT_ENTRIES"})
	}
	return errs
}
