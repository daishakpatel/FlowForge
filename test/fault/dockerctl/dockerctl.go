// Package dockerctl drives the Compose Raft cluster's containers (raft-1..3, profile raft) for the
// container-level fault scenarios (task 04.08, ADR-019): kill, start, restart, pause and network
// disconnect, through the docker CLI. Network faults use `docker network disconnect` rather than
// Toxiproxy: cutting a node off its Compose network is a clean partition (peers and clients
// lose it), and latency is covered by the in-process transport.Fault scenarios.
package dockerctl

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// Compose runs docker compose for one project directory and profile.
type Compose struct {
	Dir     string // the directory holding docker-compose.yml
	Profile string // default "raft"
	Network string // the project's network, default "flowforge_default"
}

// New returns a Compose for the raft profile of the project in dir.
func New(dir string) *Compose {
	return &Compose{Dir: dir, Profile: "raft", Network: "flowforge_default"}
}

func (c *Compose) docker(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // G204: fixed verbs; service names from the test
	cmd.Dir = c.Dir
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(errOut.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

func (c *Compose) compose(ctx context.Context, args ...string) error {
	_, err := c.docker(ctx, append([]string{"compose", "--profile", c.Profile}, args...)...)
	return err
}

// Up starts services and waits until they are healthy, until ctx ends. `compose up --wait` fails
// at once on a container still marked unhealthy, which a recently paused or partitioned node is
// until its next successful healthcheck, so Up retries every second.
func (c *Compose) Up(ctx context.Context, services ...string) error {
	for {
		err := c.compose(ctx, append([]string{"up", "-d", "--wait"}, services...)...)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Second):
		}
	}
}

// Kill sends SIGKILL to a service's container.
func (c *Compose) Kill(ctx context.Context, service string) error {
	return c.compose(ctx, "kill", service)
}

// Restart stops and starts services (SIGTERM, then start), keeping their volumes.
func (c *Compose) Restart(ctx context.Context, services ...string) error {
	return c.compose(ctx, append([]string{"restart"}, services...)...)
}

// Pause freezes a service's processes (SIGSTOP, docker pause): it holds its connections but does
// nothing.
func (c *Compose) Pause(ctx context.Context, service string) error {
	return c.compose(ctx, "pause", service)
}

// Unpause resumes a paused service.
func (c *Compose) Unpause(ctx context.Context, service string) error {
	return c.compose(ctx, "unpause", service)
}

// Container returns a service's container id.
func (c *Compose) Container(ctx context.Context, service string) (string, error) {
	id, err := c.docker(ctx, "compose", "--profile", c.Profile, "ps", "-q", service)
	if err == nil && id == "" {
		err = fmt.Errorf("service %s has no container", service)
	}
	return id, err
}

// Disconnect cuts a service's container off the project network: peers and clients lose it, as
// in a network partition.
func (c *Compose) Disconnect(ctx context.Context, service string) error {
	id, err := c.Container(ctx, service)
	if err != nil {
		return err
	}
	_, err = c.docker(ctx, "network", "disconnect", c.Network, id)
	return err
}

// Connect reattaches a disconnected service under its service name, so peers resolve it again.
func (c *Compose) Connect(ctx context.Context, service string) error {
	id, err := c.Container(ctx, service)
	if err != nil {
		return err
	}
	_, err = c.docker(ctx, "network", "connect", "--alias", service, c.Network, id)
	return err
}
