// Command flowforge-migrate applies or reports FlowForge's database migrations (task 02.01).
//
//	flowforge-migrate [--db flowforge|marketdata] up|status
//
// DSNs come from FF_PG_DSN (flowforge) and FF_MD_PG_DSN (marketdata) through internal/config.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/persistence/migrate"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], config.Options{}, os.Stdout, os.Stderr)
	stop()
	if err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, "flowforge-migrate:", err)
		}
		os.Exit(1)
	}
}

// run is main without process exit, so tests can drive it with their own environment.
func run(ctx context.Context, args []string, opts config.Options, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("flowforge-migrate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	db := flags.String("db", string(migrate.Flowforge), "database: flowforge or marketdata")
	flags.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "usage: flowforge-migrate [--db flowforge|marketdata] up|status") // best-effort help text
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		flags.Usage()
		return errors.New("want exactly one command: up or status")
	}

	cfg, err := config.Load[config.Migrate](opts)
	if err != nil {
		return err
	}
	set := migrate.Set(*db)
	var dsn config.Secret
	switch set {
	case migrate.Flowforge:
		dsn = cfg.PGDSN
	case migrate.Marketdata:
		dsn = cfg.MDDSN
	default:
		return fmt.Errorf("--db %q: %w", *db, migrate.ErrUnknownSet)
	}

	switch cmd := flags.Arg(0); cmd {
	case "up":
		v, err := migrate.Up(ctx, dsn.Reveal(), set)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(stdout, "%s: schema at version %d\n", set, v)
		return err
	case "status":
		return printStatus(ctx, stdout, dsn, set)
	default:
		return fmt.Errorf("unknown command %q: want up or status", cmd)
	}
}

func printStatus(ctx context.Context, w io.Writer, dsn config.Secret, set migrate.Set) error {
	list, err := migrate.Status(ctx, dsn.Reveal(), set)
	if err != nil {
		return err
	}
	if len(list) == 0 {
		_, err := fmt.Fprintf(w, "%s: no migrations\n", set)
		return err
	}
	// tabwriter buffers every cell; a write error surfaces from Flush, which is returned.
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "VERSION\tSTATE\tAPPLIED AT\tFILE")
	for _, m := range list {
		state, at := "pending", "-"
		if m.Applied {
			state, at = "applied", m.AppliedAt.UTC().Format(time.RFC3339)
		}
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\n", m.Version, state, at, m.Name)
	}
	return tw.Flush()
}
