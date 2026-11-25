package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/kafka"
)

// ensureTimeout bounds one `topics ensure`, connecting to the brokers included.
const ensureTimeout = time.Minute

// topicsEnsure creates the Kafka topics of a topics file that do not exist and reports how existing
// ones differ from it, without changing them (task 05.01; `make topics`). It talks to the brokers in
// FF_KAFKA_BROKERS, not to the API. Warnings do not fail the command.
func topicsEnsure(ctx context.Context, e *env, args []string) error {
	fs := flag.NewFlagSet("topics ensure", flag.ContinueOnError)
	file := fs.String("f", "deploy/kafka/topics.yaml", "topics file")
	if _, err := parse(fs, args, 0, "topics ensure [-f topics.yaml]"); err != nil {
		return err
	}
	cfg, err := config.Load[config.Kafka](config.Options{LookupEnv: func(k string) (string, bool) {
		v := e.getenv(k)
		return v, v != ""
	}})
	if err != nil {
		return err
	}
	f, err := os.Open(*file) //nolint:gosec // G304: the operator names the topics file on the command line
	if err != nil {
		return fmt.Errorf("open topics file: %w", err)
	}
	want, err := kafka.LoadTopics(f)
	_ = f.Close() // read-only; nothing to flush
	if err != nil {
		return fmt.Errorf("%s: %w", *file, err)
	}
	adm, err := kafka.NewAdmin(cfg)
	if err != nil {
		return err
	}
	defer adm.Close()
	ctx, cancel := context.WithTimeout(ctx, ensureTimeout)
	defer cancel()
	res, err := kafka.EnsureTopics(ctx, adm, want)
	if e.out.json && err == nil {
		return json.NewEncoder(e.stdout).Encode(map[string][]string{
			"created": nonNil(res.Created), "existing": nonNil(res.Existing), "warnings": nonNil(res.Warnings)})
	}
	for _, n := range res.Created { // printed on error too: these topics now exist
		_, _ = fmt.Fprintln(e.stdout, "created", n)
	}
	for _, w := range res.Warnings {
		_, _ = fmt.Fprintln(e.stdout, "warning:", w)
	}
	if err != nil {
		return fmt.Errorf("ensure topics on %v: %w", cfg.Brokers, err)
	}
	if len(res.Created) == 0 && len(res.Warnings) == 0 {
		_, _ = fmt.Fprintf(e.stdout, "no changes: all %d topics exist as declared\n", len(want))
	} else {
		_, _ = fmt.Fprintf(e.stdout, "%d created, %d already existed, %d warnings\n", len(res.Created), len(res.Existing), len(res.Warnings))
	}
	return nil
}

// nonNil makes an empty list encode as [] rather than null.
func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
