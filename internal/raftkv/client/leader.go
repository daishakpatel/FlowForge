package client

import (
	"context"
	"time"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// discoverTimeout bounds each Cluster.Status call that maps node ids to endpoints.
const discoverTimeout = 500 * time.Millisecond

// pick returns the believed leader's endpoint, or the next endpoint in turn.
func (c *Client) pick() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader >= 0 {
		return c.leader
	}
	return c.next
}

// setLeader records the endpoint that just served a leader-only request.
func (c *Client) setLeader(ep int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = ep
}

// rotate forgets the leader and returns the endpoint after ep.
func (c *Client) rotate(ep int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = -1
	c.next = (ep + 1) % len(c.conns)
	return c.next
}

// endpointFor maps a NOT_LEADER hint (a node id) to an endpoint. A node's id is learned from its
// Cluster.Status, because nodes know each other only by peer address; unknown ids trigger one
// round of Status calls to every endpoint.
func (c *Client) endpointFor(ctx context.Context, id uint64) (int, bool) {
	if id == 0 {
		return 0, false
	}
	c.mu.Lock()
	ep, ok := c.byID[id]
	c.mu.Unlock()
	if ok {
		return ep, true
	}
	for i, cl := range c.cl {
		sctx, cancel := context.WithTimeout(ctx, discoverTimeout)
		st, err := cl.Status(sctx, &pb.StatusRequest{})
		cancel()
		if err == nil && st.GetNodeId() != 0 {
			c.mu.Lock()
			c.byID[st.GetNodeId()] = i
			c.mu.Unlock()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ep, ok = c.byID[id]
	return ep, ok
}
