package node

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func TestParsePeers_Valid(t *testing.T) {
	got, err := ParsePeers([]string{"3=raft-3:7001", "1=raft-1:7001", "2=raft-2:7001"})
	if err != nil {
		t.Fatalf("ParsePeers: %v", err)
	}
	want := []Peer{{ID: 1, Addr: "raft-1:7001"}, {ID: 2, Addr: "raft-2:7001"}, {ID: 3, Addr: "raft-3:7001"}}
	if !slices.Equal(got, want) {
		t.Errorf("peers = %v, want %v", got, want)
	}
}

func TestParsePeers_Invalid(t *testing.T) {
	for name, in := range map[string][]string{
		"empty":        nil,
		"no equals":    {"raft-1:7001"},
		"zero id":      {"0=a:1"},
		"bad id":       {"x=a:1"},
		"no addr":      {"1="},
		"duplicate id": {"1=a:1", "1=b:1"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParsePeers(in); !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("ParsePeers(%q) = %v, want ErrInvalidConfig", in, err)
			}
		})
	}
}

func TestConfig_Defaults(t *testing.T) {
	c := Config{ID: 1, Peers: []Peer{{ID: 1, Addr: "a:1"}}}.withDefaults()
	if c.ElectionTick != 10 || c.HeartbeatTick != 1 || c.TickInterval != 100*time.Millisecond ||
		c.MaxSizePerMsg != 1<<20 || c.MaxInflightMsgs != 256 || c.SnapshotEntries != 10000 ||
		c.SnapshotTrailing != 1000 || c.Logger == nil || c.Clock == nil || c.Entropy == nil {
		t.Errorf("defaults not applied: %+v", c)
	}
}

func TestConfig_Validate(t *testing.T) {
	peers := []Peer{{ID: 1, Addr: "a:1"}, {ID: 2, Addr: "b:1"}, {ID: 3, Addr: "c:1"}}
	for name, c := range map[string]Config{
		"zero id":          {ID: 0, Peers: peers},
		"not a peer":       {ID: 4, Peers: peers},
		"heartbeat ≥ elec": {ID: 1, Peers: peers, ElectionTick: 2, HeartbeatTick: 2},
	} {
		t.Run(name, func(t *testing.T) {
			if err := c.withDefaults().validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("validate() = %v, want ErrInvalidConfig", err)
			}
		})
	}
	if err := (Config{ID: 2, Peers: peers}).withDefaults().validate(); err != nil {
		t.Errorf("valid config: %v", err)
	}
}

func TestEntryCodec_RoundTrip(t *testing.T) {
	id := proposalID{node: 7, boot: 0xdeadbeef, seq: 42}
	data := encodeEntry(id, []byte("payload"))
	gotID, payload, ok := decodeEntry(data)
	if !ok || gotID != id || string(payload) != "payload" {
		t.Errorf("decodeEntry = %v %q %v", gotID, payload, ok)
	}
	for name, bad := range map[string][]byte{"empty": nil, "short": data[:10], "version": append([]byte{9}, data[1:]...)} {
		if _, _, ok := decodeEntry(bad); ok {
			t.Errorf("%s: decodeEntry accepted %x", name, bad)
		}
	}
}
