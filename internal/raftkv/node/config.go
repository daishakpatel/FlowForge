package node

import (
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
)

// Peer is one voter of the static cluster: its raft id and the address its peer transport
// listens on (FF_RAFT_PEER_ADDR on that node).
type Peer struct {
	ID   uint64
	Addr string
}

// Config configures a Node. Zero fields take the 00E §5.3 defaults (withDefaults). PreVote and
// CheckQuorum are always on (00E §5.3): they are what keeps a partitioned leader from serving
// writes and a rejoining node from disrupting the cluster, so they are not switchable.
type Config struct {
	ID    uint64 // this node's raft id, non-zero (FF_RAFT_NODE_ID)
	Peers []Peer // every voter including this node (FF_RAFT_PEERS, see ParsePeers)

	ElectionTick  int           // default 10
	HeartbeatTick int           // default 1
	TickInterval  time.Duration // default 100ms

	MaxSizePerMsg   uint64 // default 1 MiB
	MaxInflightMsgs int    // default 256

	// SnapshotEntries is how many entries are applied between snapshots (default 10 000,
	// FF_RAFT_SNAPSHOT_ENTRIES); SnapshotTrailing how many entries before a snapshot the log
	// keeps for slow followers (default 1 000).
	SnapshotEntries  uint64
	SnapshotTrailing uint64

	Logger *slog.Logger // default slog.Default(); the raft library logs at WARN and above only
	Clock  clock.Clock  // default clock.Real; times storage writes and drives the default ticker
	// Ticker drives raft ticks; nil means Clock.NewTicker(TickInterval). Tests pass a
	// *ManualTicker to step time deterministically.
	Ticker clock.Ticker
	// Entropy seeds the per-start proposal-id nonce (default crypto/rand.Reader). It only has
	// to differ between starts of one node; tests pass a seeded reader.
	Entropy io.Reader
	Metrics *Metrics // nil disables metrics
}

// ParsePeers parses FF_RAFT_PEERS entries of the form "<id>=<host:port>" (for example
// "1=raft-1:7001") into peers sorted by id. Ids must be non-zero and unique.
func ParsePeers(entries []string) ([]Peer, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: no peers", ErrInvalidConfig)
	}
	peers := make([]Peer, 0, len(entries))
	for _, e := range entries {
		idStr, addr, ok := strings.Cut(strings.TrimSpace(e), "=")
		if !ok || addr == "" {
			return nil, fmt.Errorf("%w: peer %q is not <id>=<host:port>", ErrInvalidConfig, e)
		}
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("%w: peer %q needs a non-zero numeric id", ErrInvalidConfig, e)
		}
		peers = append(peers, Peer{ID: id, Addr: addr})
	}
	slices.SortFunc(peers, func(a, b Peer) int { return cmpUint(a.ID, b.ID) })
	if err := checkUnique(peers); err != nil {
		return nil, err
	}
	return peers, nil
}

func (c Config) withDefaults() Config {
	if c.ElectionTick == 0 {
		c.ElectionTick = 10
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 1
	}
	if c.TickInterval == 0 {
		c.TickInterval = 100 * time.Millisecond
	}
	if c.MaxSizePerMsg == 0 {
		c.MaxSizePerMsg = 1 << 20
	}
	if c.MaxInflightMsgs == 0 {
		c.MaxInflightMsgs = 256
	}
	if c.SnapshotEntries == 0 {
		c.SnapshotEntries = 10000
	}
	if c.SnapshotTrailing == 0 {
		c.SnapshotTrailing = 1000
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Clock == nil {
		c.Clock = clock.Real{}
	}
	if c.Entropy == nil {
		c.Entropy = rand.Reader
	}
	return c
}

func (c Config) validate() error {
	if c.ID == 0 {
		return fmt.Errorf("%w: node id must be non-zero", ErrInvalidConfig)
	}
	sorted := slices.Clone(c.Peers)
	slices.SortFunc(sorted, func(a, b Peer) int { return cmpUint(a.ID, b.ID) })
	if err := checkUnique(sorted); err != nil {
		return err
	}
	if !slices.ContainsFunc(c.Peers, func(p Peer) bool { return p.ID == c.ID }) {
		return fmt.Errorf("%w: node %d is not in the peer list", ErrInvalidConfig, c.ID)
	}
	if c.TickInterval <= 0 {
		return fmt.Errorf("%w: tick interval %s must be positive", ErrInvalidConfig, c.TickInterval)
	}
	if c.HeartbeatTick >= c.ElectionTick {
		return fmt.Errorf("%w: heartbeat tick %d must be below election tick %d", ErrInvalidConfig, c.HeartbeatTick, c.ElectionTick)
	}
	return nil
}

func (c Config) voterIDs() []uint64 {
	ids := make([]uint64, len(c.Peers))
	for i, p := range c.Peers {
		ids[i] = p.ID
	}
	slices.Sort(ids)
	return ids
}

func checkUnique(sorted []Peer) error {
	for i, p := range sorted {
		if p.ID == 0 {
			return fmt.Errorf("%w: peer ids must be non-zero", ErrInvalidConfig)
		}
		if i > 0 && sorted[i-1].ID == p.ID {
			return fmt.Errorf("%w: duplicate peer id %d", ErrInvalidConfig, p.ID)
		}
	}
	return nil
}

func cmpUint(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// raftLogger adapts slog to the raft library's Logger at WARN and above (task 04.02: keep the
// library quiet by default). Fatal and Panic log and then panic: the library does not expect them
// to return, and the node's goroutine recovers the panic and stops the node.
type raftLogger struct{ l *slog.Logger }

func (r raftLogger) Debug(...any)          {}
func (r raftLogger) Debugf(string, ...any) {}
func (r raftLogger) Info(...any)           {}
func (r raftLogger) Infof(string, ...any)  {}
func (r raftLogger) Warning(v ...any)      { r.l.Warn(fmt.Sprint(v...), "component", "raft") }
func (r raftLogger) Warningf(f string, v ...any) {
	r.l.Warn(fmt.Sprintf(f, v...), "component", "raft")
}
func (r raftLogger) Error(v ...any)            { r.l.Error(fmt.Sprint(v...), "component", "raft") }
func (r raftLogger) Errorf(f string, v ...any) { r.l.Error(fmt.Sprintf(f, v...), "component", "raft") }
func (r raftLogger) Fatal(v ...any)            { r.die(fmt.Sprint(v...)) }
func (r raftLogger) Fatalf(f string, v ...any) { r.die(fmt.Sprintf(f, v...)) }
func (r raftLogger) Panic(v ...any)            { r.die(fmt.Sprint(v...)) }
func (r raftLogger) Panicf(f string, v ...any) { r.die(fmt.Sprintf(f, v...)) }

func (r raftLogger) die(msg string) {
	r.l.Error(msg, "component", "raft")
	panic("raft: " + msg)
}
