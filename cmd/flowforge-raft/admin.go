package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/daishakpatel/flowforge/internal/config"
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// Operator subcommands (task 04.06), dev/ops tooling that talks to a node's client address:
//
//	flowforge-raft status          --addr host:port          Cluster.Status as JSON
//	flowforge-raft health          --addr host:port          exit 0 only when READY (Compose healthcheck)
//	flowforge-raft dump-state-hash --addr a,b,c              node, applied index and state hash per node
//	flowforge-raft transfer-leader --addr leader --to ID     needs FF_RAFT_ADMIN_TOKEN
//	flowforge-raft snapshot-now    --addr host:port          needs FF_RAFT_ADMIN_TOKEN
//	flowforge-raft compact         --addr host:port          needs FF_RAFT_ADMIN_TOKEN
//
// The token comes from the environment (FF_RAFT_ADMIN_TOKEN), never from a flag, so it doesn't
// show in process listings.

const adminTimeout = 10 * time.Second

type adminConfig struct {
	AdminToken config.Secret `env:"FF_RAFT_ADMIN_TOKEN" default:""`
}

type adminCall struct {
	addrs []string
	to    uint64
	token string
	out   io.Writer
}

var errNotReady = errors.New("node is not READY")

// runAdmin runs one subcommand and returns the process exit code (0 ok, 1 failed or not ready,
// 2 usage).
func runAdmin(ctx context.Context, args []string, opts config.Options, out, errOut io.Writer) int {
	fs := flag.NewFlagSet("flowforge-raft "+args[0], flag.ContinueOnError)
	fs.SetOutput(errOut)
	addr := fs.String("addr", "127.0.0.1:7000", "node client address (comma-separated for dump-state-hash)")
	to := fs.Uint64("to", 0, "transfer-leader: the voter to hand leadership to (0: the most up-to-date)")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	cfg, err := config.Load[adminConfig](opts)
	if err != nil {
		_, _ = fmt.Fprintln(errOut, "flowforge-raft:", err)
		return 2
	}
	ctx, cancel := context.WithTimeout(ctx, adminTimeout)
	defer cancel()
	c := adminCall{addrs: strings.Split(*addr, ","), to: *to, token: cfg.AdminToken.Reveal(), out: out}
	err = dispatch(ctx, args[0], c)
	switch {
	case errors.Is(err, errUnknownSubcommand):
		_, _ = fmt.Fprintf(errOut, "flowforge-raft: unknown subcommand %q (status, health, dump-state-hash, transfer-leader, snapshot-now, compact)\n", args[0])
		return 2
	case err != nil:
		_, _ = fmt.Fprintln(errOut, "flowforge-raft:", err)
		return 1
	}
	return 0
}

var errUnknownSubcommand = errors.New("unknown subcommand")

func dispatch(ctx context.Context, name string, c adminCall) error {
	switch name {
	case "status":
		return withConn(c.addrs[0], func(conn *grpc.ClientConn) error {
			st, err := pb.NewClusterClient(conn).Status(ctx, &pb.StatusRequest{})
			if err != nil {
				return err
			}
			return printJSON(c.out, st)
		})
	case "health":
		return withConn(c.addrs[0], func(conn *grpc.ClientConn) error {
			h, err := pb.NewClusterClient(conn).Health(ctx, &pb.HealthRequest{})
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintln(c.out, h.GetStatus()); err != nil {
				return err
			}
			if !h.GetHealthy() {
				return errNotReady
			}
			return nil
		})
	case "dump-state-hash":
		return dumpStateHash(ctx, c)
	case "transfer-leader":
		return withAdmin(ctx, c, func(ctx context.Context, a pb.AdminClient) (proto.Message, error) {
			return a.TransferLeader(ctx, &pb.TransferLeaderRequest{To: c.to})
		})
	case "snapshot-now":
		return withAdmin(ctx, c, func(ctx context.Context, a pb.AdminClient) (proto.Message, error) {
			return a.SnapshotNow(ctx, &pb.SnapshotNowRequest{})
		})
	case "compact":
		return withAdmin(ctx, c, func(ctx context.Context, a pb.AdminClient) (proto.Message, error) {
			return a.Compact(ctx, &pb.CompactRequest{})
		})
	}
	return errUnknownSubcommand
}

func dumpStateHash(ctx context.Context, c adminCall) error {
	for _, a := range c.addrs {
		err := withConn(a, func(conn *grpc.ClientConn) error {
			st, err := pb.NewClusterClient(conn).Status(ctx, &pb.StatusRequest{})
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(c.out, "node=%d applied=%d state_hash=%x\n", st.GetNodeId(), st.GetStateHashIndex(), st.GetStateHash())
			return err
		})
		if err != nil {
			return fmt.Errorf("%s: %w", a, err)
		}
	}
	return nil
}

func withConn(addr string, f func(*grpc.ClientConn) error) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }() // a one-shot CLI call
	return f(conn)
}

// withAdmin calls an Admin RPC with the bearer token and prints its response.
func withAdmin(ctx context.Context, c adminCall, f func(context.Context, pb.AdminClient) (proto.Message, error)) error {
	if c.token == "" {
		return errors.New("FF_RAFT_ADMIN_TOKEN is not set")
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
	return withConn(c.addrs[0], func(conn *grpc.ClientConn) error {
		res, err := f(ctx, pb.NewAdminClient(conn))
		if err != nil {
			return err
		}
		return printJSON(c.out, res)
	})
}

func printJSON(out io.Writer, m proto.Message) error {
	b, err := protojson.MarshalOptions{Multiline: true, EmitUnpopulated: true}.Marshal(m)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, bytes.NewReader(append(b, '\n')))
	return err
}
