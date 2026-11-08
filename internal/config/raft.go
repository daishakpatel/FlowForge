package config

// Raft configures a flowforge-raft node (00E §6). Every field is per node, so none has a
// default. NodeID is the non-zero uint64 the etcd Raft library requires; the format of Peers
// entries is defined by task 04.02.
type Raft struct {
	Common
	NodeID     uint64   `env:"FF_RAFT_NODE_ID" validate:"positive"`
	Peers      []string `env:"FF_RAFT_PEERS" validate:"required"`
	DataDir    string   `env:"FF_RAFT_DATA_DIR" validate:"required"`
	ClientAddr string   `env:"FF_RAFT_CLIENT_ADDR" validate:"required,addr"`
	PeerAddr   string   `env:"FF_RAFT_PEER_ADDR" validate:"required,addr"`
}

// Redacted returns every variable with secrets masked, for the startup log.
func (c Raft) Redacted() map[string]string { return redacted(c) }
