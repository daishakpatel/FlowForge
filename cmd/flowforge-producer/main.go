// Command flowforge-producer publishes market-data ticks to Kafka (task 05.03): a seeded synthetic
// stream or a CSV/JSONL file, validated and keyed by symbol. With --count or --duration it stops on
// its own and prints a JSON summary; otherwise it runs until SIGINT/SIGTERM. Flags, determinism and
// injection: docs/architecture/market-data-generator.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/kafka"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/stream/domain"
	"github.com/daishakpatel/flowforge/internal/stream/producer"
	"github.com/daishakpatel/flowforge/internal/stream/source"
	"github.com/daishakpatel/flowforge/internal/version"
	"github.com/daishakpatel/flowforge/pkg/events"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr, clock.Real{})
	stop()
	os.Exit(code)
}

// options are the command-line flags.
type options struct {
	file, symbols, clockMode, start, inject string
	seed                                    uint64
	rate, count, batch                      int
	duration                                time.Duration
	allowInvalid                            bool
}

// usageError is a mistake in the flags: exit code 2.
type usageError struct{ error }

// errHelp ends a run that only printed the flags (-h): exit code 0.
var errHelp = errors.New("help printed")

// summary is what the command prints when it stops.
type summary struct {
	producer.Summary
	Injected map[source.Kind]int `json:"injected,omitempty"`
}

// run executes one command line and returns the exit code: 0 done, 1 failed, 2 bad flags.
func run(ctx context.Context, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer, clk clock.Clock) int {
	err := produce(ctx, args, lookup, stdout, stderr, clk)
	var ue usageError
	switch {
	case err == nil, errors.Is(err, errHelp):
		return 0
	case errors.As(err, &ue):
		_, _ = fmt.Fprintln(stderr, "flowforge-producer:", err)
		return 2
	}
	_, _ = fmt.Fprintln(stderr, "flowforge-producer:", err)
	return 1
}

func produce(ctx context.Context, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer, clk clock.Clock) error {
	cfg, err := config.Load[config.Producer](config.Options{LookupEnv: lookup})
	if err != nil {
		return err
	}
	o, err := parseFlags(args, cfg.Rate)
	if errors.Is(err, flag.ErrHelp) {
		_, _ = fmt.Fprintln(stderr, "usage: flowforge-producer [flags]   (docs/architecture/market-data-generator.md)")
		printFlags(stderr, cfg.Rate)
		return errHelp
	}
	if err != nil {
		return usageError{err}
	}
	src, batch, err := newSource(o, clk)
	if err != nil {
		return usageError{err}
	}
	if c, ok := src.(io.Closer); ok {
		defer func() { _ = c.Close() }() // read-only input
	}
	logger := logging.New(logging.Config{Level: cfg.LogLevel, Format: cfg.LogFormat, Service: "flowforge-producer", Version: version.Version(), Writer: stderr})
	reg := obs.NewRegistry()
	metrics, err := producer.NewMetrics(reg)
	if err != nil {
		return err
	}
	stopServer, err := serve(ctx, cfg.HTTPAddr, reg)
	if err != nil {
		return err
	}
	defer stopServer()
	kp, err := kafka.NewProducer(cfg.Kafka)
	if err != nil {
		return err
	}
	defer kp.Close(context.WithoutCancel(ctx)) // flushes what is buffered
	schemas, err := events.NewRegistry()
	if err != nil {
		return err
	}
	p := producer.New(kp, schemas, clk, metrics, producer.Options{AllowInvalid: o.allowInvalid})
	logger.Info("producing", "symbols", o.symbols, "rate", o.rate, "clock", o.clockMode, "count", o.count, "file", o.file)
	sum, err := producer.Run(ctx, src, p, producer.RunOptions{Count: o.count, Batch: batch})
	out := summary{Summary: sum}
	if s, ok := src.(*source.Synthetic); ok {
		out.Injected = s.Counts()
	}
	if jerr := json.NewEncoder(stdout).Encode(out); jerr != nil {
		return jerr
	}
	if err != nil && !errors.Is(err, context.Canceled) { // a signal is the normal end of an unbounded run
		return err
	}
	return nil
}

// printFlags writes the flag list to w.
func printFlags(w io.Writer, defaultRate int) {
	fs, _ := flagSet(defaultRate)
	fs.SetOutput(w)
	fs.PrintDefaults()
}

func parseFlags(args []string, defaultRate int) (options, error) {
	fs, o := flagSet(defaultRate)
	if err := fs.Parse(args); err != nil {
		return *o, err
	}
	if fs.NArg() > 0 {
		return *o, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if o.count < 0 || o.duration < 0 || o.batch < 0 {
		return *o, errors.New("--count, --duration and --batch must not be negative")
	}
	return *o, nil
}

func flagSet(defaultRate int) (*flag.FlagSet, *options) {
	fs := flag.NewFlagSet("flowforge-producer", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	o := &options{}
	fs.StringVar(&o.file, "file", "", "read ticks from a .csv or .jsonl file instead of generating them")
	fs.StringVar(&o.symbols, "symbols", "ACME,BOLT,CRUX", "synthetic: comma-separated symbols")
	fs.Uint64Var(&o.seed, "seed", 1, "synthetic: PRNG seed; the same seed gives the same stream")
	fs.IntVar(&o.rate, "rate", defaultRate, "synthetic: ticks per second of event time (FF_PRODUCER_RATE)")
	fs.StringVar(&o.clockMode, "clock", string(source.ModeRealtime), "synthetic: realtime (paced, event time = now) or simulated (as fast as possible)")
	fs.StringVar(&o.start, "start", "", "synthetic: first event time, RFC 3339 (default: now; simulated: so the stream ends now)")
	fs.DurationVar(&o.duration, "duration", 0, "stop after this much event time")
	fs.IntVar(&o.count, "count", 0, "stop after this many ticks")
	fs.StringVar(&o.inject, "inject", "", "synthetic: fault injection, e.g. dup=1%,late=2%,ooo=3%,invalid=1%,gap=1%,late_max=2m")
	fs.IntVar(&o.batch, "batch", 0, "ticks per publish (default 500; realtime about 50 ms worth)")
	fs.BoolVar(&o.allowInvalid, "allow-invalid", false, "send invalid ticks to market.ticks instead of market.invalid (tests the processor's DLQ)")
	return fs, o
}

// newSource builds the file or synthetic source and the batch size that suits it.
func newSource(o options, clk clock.Clock) (source.Source, int, error) {
	batch := o.batch
	if o.file != "" {
		if o.duration > 0 {
			return nil, 0, errors.New("--duration bounds generated event time; with --file use --count")
		}
		if batch == 0 {
			batch = producer.DefaultBatch
		}
		fs, err := source.Open(o.file)
		if err != nil {
			return nil, 0, err
		}
		return fs, batch, nil
	}
	in, err := source.ParseInject(o.inject)
	if err != nil {
		return nil, 0, err
	}
	mode := source.ClockMode(o.clockMode)
	if mode == source.ModeSimulated && o.count == 0 && o.duration == 0 {
		return nil, 0, errors.New("--clock simulated needs --count or --duration (its event times would run ahead of the clock)")
	}
	cfg := source.Config{Seed: o.seed, Symbols: splitList(o.symbols), Rate: o.rate, Mode: mode, Inject: in}
	if cfg.Start, err = startTime(o, clk); err != nil {
		return nil, 0, err
	}
	if span := simulatedSpan(o); mode == source.ModeSimulated {
		if span > domain.MaxTickAge {
			return nil, 0, fmt.Errorf("a simulated run of %v reaches past the %v tick retention; every older tick would be rejected as too_old", span, domain.MaxTickAge)
		}
		// End the stream where its span ends (now, by default): gaps would otherwise push the last
		// ticks into the future, where the producer rejects them.
		cfg.End = cfg.Start.Add(span)
	} else if o.duration > 0 {
		cfg.End = cfg.Start.Add(o.duration)
	}
	s, err := source.NewSynthetic(cfg, clk)
	if err != nil {
		return nil, 0, err
	}
	if batch == 0 {
		batch = producer.DefaultBatch
		if mode == source.ModeRealtime {
			batch = max(1, o.rate/20) // publish about every 50 ms
		}
	}
	return s, batch, nil
}

// startTime is --start, or now; a simulated stream starts its span before now so it ends at now,
// and every tick passes the producer's future and retention checks.
func startTime(o options, clk clock.Clock) (time.Time, error) {
	if o.start != "" {
		t, err := time.Parse(time.RFC3339Nano, o.start)
		if err != nil {
			return time.Time{}, fmt.Errorf("--start %q: want RFC 3339", o.start)
		}
		return t, nil
	}
	now := clk.Now()
	if source.ClockMode(o.clockMode) != source.ModeSimulated {
		return now, nil
	}
	return now.Add(-simulatedSpan(o)), nil
}

// simulatedSpan is the event time a simulated run covers: --duration, or --count ticks at --rate.
func simulatedSpan(o options) time.Duration {
	if o.duration > 0 || o.rate <= 0 {
		return o.duration
	}
	return time.Duration(o.count) * (time.Second / time.Duration(o.rate))
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// serve exposes /metrics, /healthz and /readyz on addr until the returned function is called.
func serve(ctx context.Context, addr string, reg *obs.Registry) (func(), error) {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}
	srv := obs.NewServer("", reg)
	go func() { _ = srv.Serve(ln) }() // ends with ErrServerClosed at shutdown
	return func() {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx) // best effort at exit
	}, nil
}
