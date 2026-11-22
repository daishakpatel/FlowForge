package node

import (
	"fmt"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/obs"
)

// Metrics are the node's 00E §11 Raft metrics. A nil *Metrics records nothing.
type Metrics struct {
	isLeader, term, commit, applied, pending prometheus.Gauge
	unreachable                              *prometheus.GaugeVec
	writeSeconds, applySeconds               prometheus.Observer
	snapshots, leaderChanges                 prometheus.Counter
}

// NewMetrics registers the node metrics on reg. Register them once per process.
func NewMetrics(reg *obs.Registry) (*Metrics, error) {
	m := &Metrics{}
	gauges := []struct {
		dst  *prometheus.Gauge
		name string
		help string
	}{
		{&m.isLeader, "is_leader", "1 while this node is the Raft leader."},
		{&m.term, "term", "Current Raft term."},
		{&m.commit, "commit_index", "Highest log index known committed."},
		{&m.applied, "applied_index", "Highest log index applied to the state machine."},
		{&m.pending, "proposals_pending", "Proposals waiting to be applied on this node."},
	}
	for _, g := range gauges {
		v, err := reg.Gauge(prometheus.GaugeOpts{Namespace: "flowforge", Subsystem: "raft", Name: g.name, Help: g.help})
		if err != nil {
			return nil, fmt.Errorf("register raft metrics: %w", err)
		}
		*g.dst = v.WithLabelValues()
	}
	var err error
	m.unreachable, err = reg.Gauge(prometheus.GaugeOpts{
		Namespace: "flowforge", Subsystem: "raft", Name: "peer_unreachable",
		Help: "1 while the transport reports the peer unreachable; 0 once a message from it arrives.",
	}, "peer")
	if err != nil {
		return nil, fmt.Errorf("register raft metrics: %w", err)
	}
	w, err := reg.Histogram(prometheus.HistogramOpts{
		Namespace: "flowforge", Subsystem: "raft", Name: "write_duration_seconds",
		Help:    "Duration of one durable Ready write (log entries, hard state, snapshot).",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	})
	if err != nil {
		return nil, fmt.Errorf("register raft metrics: %w", err)
	}
	m.writeSeconds = w.WithLabelValues()
	a, err := reg.Histogram(prometheus.HistogramOpts{
		Namespace: "flowforge", Subsystem: "raft", Name: "apply_duration_seconds",
		Help:    "Duration of applying one batch of committed entries to the state machine.",
		Buckets: prometheus.ExponentialBuckets(0.00005, 2, 16),
	})
	if err != nil {
		return nil, fmt.Errorf("register raft metrics: %w", err)
	}
	m.applySeconds = a.WithLabelValues()
	counters := []struct {
		dst  *prometheus.Counter
		name string
		sub  string
		help string
	}{
		{&m.snapshots, "snapshot_total", "raft", "Snapshots this node took (automatic and on demand)."},
		{&m.leaderChanges, "leader_changes_total", "raft", "Times this node saw a new Raft leader."},
	}
	for _, c := range counters {
		v, err := reg.Counter(prometheus.CounterOpts{Namespace: "flowforge", Subsystem: c.sub, Name: c.name, Help: c.help})
		if err != nil {
			return nil, fmt.Errorf("register raft metrics: %w", err)
		}
		*c.dst = v.WithLabelValues()
	}
	return m, nil
}

// initPeers exports peer_unreachable as 0 for every other voter from the start, so the series
// exists before the first failure (dashboards and the metrics catalogue test rely on it).
func (m *Metrics) initPeers(self uint64, peers []Peer) {
	if m == nil {
		return
	}
	for _, p := range peers {
		if p.ID != self {
			m.unreachable.WithLabelValues(strconv.FormatUint(p.ID, 10)).Set(0)
		}
	}
}

func (m *Metrics) applyDone(seconds float64) {
	if m != nil {
		m.applySeconds.Observe(seconds)
	}
}

func (m *Metrics) snapshotted() {
	if m != nil {
		m.snapshots.Inc()
	}
}

func (m *Metrics) leaderChanged() {
	if m != nil {
		m.leaderChanges.Inc()
	}
}

func (m *Metrics) observe(s Status, pending int) {
	if m == nil {
		return
	}
	leader := 0.0
	if s.IsLeader {
		leader = 1
	}
	m.isLeader.Set(leader)
	m.term.Set(float64(s.Term))
	m.commit.Set(float64(s.Commit))
	m.applied.Set(float64(s.Applied))
	m.pending.Set(float64(pending))
}

func (m *Metrics) peerReachable(id uint64, ok bool) {
	if m == nil {
		return
	}
	v := 1.0
	if ok {
		v = 0
	}
	m.unreachable.WithLabelValues(strconv.FormatUint(id, 10)).Set(v)
}

func (m *Metrics) write(seconds float64) {
	if m != nil {
		m.writeSeconds.Observe(seconds)
	}
}
