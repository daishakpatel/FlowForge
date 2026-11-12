package config

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// envOpts returns Options reading from m instead of the process environment.
func envOpts(m map[string]string) Options {
	return Options{LookupEnv: func(k string) (string, bool) { v, ok := m[k]; return v, ok }}
}

// writeFile writes a config file and returns env pointing FF_CONFIG_FILE at it.
func writeFile(t *testing.T, body string, env map[string]string) map[string]string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "flowforge.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if env == nil {
		env = map[string]string{}
	}
	env["FF_CONFIG_FILE"] = path
	return env
}

// validationFields returns the variable names in err, failing unless it is a *ValidationError.
func validationFields(t *testing.T, err error) []string {
	t.Helper()
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("want *ValidationError, got %v", err)
	}
	var names []string
	for _, f := range ve.Fields {
		names = append(names, f.Env)
	}
	return names
}

func TestLoad_Precedence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		file string // empty: no config file
		env  map[string]string
		want time.Duration
	}{
		{name: "default only", want: 500 * time.Millisecond},
		{name: "file overrides default", file: "FF_SCHED_TICK: 2s\n", want: 2 * time.Second},
		{name: "env overrides file", file: "FF_SCHED_TICK: 2s\n", env: map[string]string{"FF_SCHED_TICK": "3s"}, want: 3 * time.Second},
		{name: "env overrides default", env: map[string]string{"FF_SCHED_TICK": "750ms"}, want: 750 * time.Millisecond},
		{name: "empty env counts as unset", file: "FF_SCHED_TICK: 2s\n", env: map[string]string{"FF_SCHED_TICK": ""}, want: 2 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			env := map[string]string{}
			for k, v := range tt.env {
				env[k] = v
			}
			if tt.file != "" {
				env = writeFile(t, tt.file, env)
			}
			cfg, err := Load[Scheduler](envOpts(env))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Tick != tt.want {
				t.Errorf("Tick = %v, want %v", cfg.Tick, tt.want)
			}
		})
	}
}

func TestLoad_DefaultsForEveryService(t *testing.T) {
	t.Parallel()
	api, err := Load[API](envOpts(nil))
	if err != nil {
		t.Fatalf("API: %v", err)
	}
	if api.Env != "dev" || api.LogLevel != "info" || api.LogFormat != "json" || api.OTLPEndpoint != "http://otel-collector:4317" ||
		api.HTTPAddr != ":8080" || api.GRPCAddr != ":9090" || api.WorkerLeaseTTL != 15*time.Second || api.WorkerHeartbeat != 5*time.Second ||
		api.PGDSN.Reveal() != "postgres://flowforge:flowforge@localhost:5432/flowforge?sslmode=disable" ||
		api.MDDSN.Reveal() != "postgres://md:md@localhost:5433/marketdata?sslmode=disable" ||
		!slices.Equal(api.RaftEndpoints, []string{"raft-1:7000", "raft-2:7000", "raft-3:7000"}) {
		t.Errorf("API defaults wrong: %+v", api)
	}

	sch, err := Load[Scheduler](envOpts(nil))
	if err != nil {
		t.Fatalf("Scheduler: %v", err)
	}
	if sch.Tick != 500*time.Millisecond || sch.LeaderTTL != 10*time.Second || sch.LeaderRenew != 3*time.Second ||
		!slices.Equal(sch.KafkaBrokers, []string{"localhost:9092"}) {
		t.Errorf("Scheduler defaults wrong: %+v", sch)
	}

	proc, err := Load[Processor](envOpts(nil))
	if err != nil {
		t.Fatalf("Processor: %v", err)
	}
	if proc.MDDSN.Reveal() == "" || !slices.Equal(proc.KafkaBrokers, []string{"localhost:9092"}) {
		t.Errorf("Processor defaults wrong: %+v", proc)
	}

	w, err := Load[Worker](envOpts(map[string]string{
		"FF_WORKER_ID": "w1", "FF_WORKER_QUEUES": "default, market-data", "FF_WORKER_CAPACITY": "4", "FF_API_GRPC_TARGET": "localhost:9090",
	}))
	if err != nil {
		t.Fatalf("Worker: %v", err)
	}
	if !slices.Equal(w.Queues, []string{"default", "market-data"}) || w.Capacity != 4 || w.AIURL != "http://ai-ops:8000" ||
		w.LeaseTTL != 15*time.Second || w.Heartbeat != 5*time.Second {
		t.Errorf("Worker values wrong: %+v", w)
	}

	r, err := Load[Raft](envOpts(map[string]string{
		"FF_RAFT_NODE_ID": "1", "FF_RAFT_PEERS": "1=raft-1:7001,2=raft-2:7001,3=raft-3:7001", "FF_RAFT_DATA_DIR": "/data",
		"FF_RAFT_CLIENT_ADDR": ":7000", "FF_RAFT_PEER_ADDR": ":7001",
	}))
	if err != nil {
		t.Fatalf("Raft: %v", err)
	}
	if r.NodeID != 1 || len(r.Peers) != 3 {
		t.Errorf("Raft values wrong: %+v", r)
	}
}

func TestLoad_RequiredWithoutDefaults(t *testing.T) {
	t.Parallel()
	_, err := Load[Worker](envOpts(nil))
	got := validationFields(t, err)
	want := []string{"FF_WORKER_ID", "FF_WORKER_QUEUES", "FF_WORKER_CAPACITY", "FF_API_GRPC_TARGET"}
	if !slices.Equal(got, want) {
		t.Errorf("fields = %v, want %v", got, want)
	}

	_, err = Load[Raft](envOpts(nil))
	got = validationFields(t, err)
	want = []string{"FF_RAFT_NODE_ID", "FF_RAFT_PEERS", "FF_RAFT_DATA_DIR", "FF_RAFT_CLIENT_ADDR", "FF_RAFT_PEER_ADDR"}
	if !slices.Equal(got, want) {
		t.Errorf("fields = %v, want %v", got, want)
	}
}

// TestLoad_AggregatesAllErrors checks that one Load reports every invalid variable (task §8).
func TestLoad_AggregatesAllErrors(t *testing.T) {
	t.Parallel()
	_, err := Load[Scheduler](envOpts(map[string]string{
		"FF_SCHED_TICK":               "soon",                        // bad duration
		"FF_PG_DSN":                   "mysql://u:" + leak + "@h/db", // bad DSN, carrying a password
		"FF_LEADER_TTL":               "10s",
		"FF_LEADER_RENEW":             "5s",                     // renew >= ttl/2
		"FF_LOG_LEVEL":                "loud",                   // not in oneof
		"FF_KAFKA_BROKERS":            "localhost:9092,no-port", // bad address in a list
		"FF_ENV":                      "dev",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "otel-collector:4317", // not an absolute URL
	}))
	got := validationFields(t, err)
	want := []string{"FF_SCHED_TICK", "FF_LOG_LEVEL", "OTEL_EXPORTER_OTLP_ENDPOINT", "FF_PG_DSN", "FF_KAFKA_BROKERS", "FF_LEADER_RENEW"}
	if !slices.Equal(got, want) {
		t.Errorf("fields = %v, want %v", got, want)
	}
	if strings.Contains(err.Error(), leak) {
		t.Errorf("error leaks the DSN password: %v", err)
	}

	_, err = Load[Worker](envOpts(map[string]string{
		"FF_WORKER_ID": "w1", "FF_WORKER_QUEUES": "q", "FF_API_GRPC_TARGET": "api:9090",
		"FF_WORKER_CAPACITY": "-2", "FF_WORKER_HEARTBEAT": "10s", "FF_WORKER_LEASE_TTL": "15s",
	}))
	got = validationFields(t, err)
	want = []string{"FF_WORKER_CAPACITY", "FF_WORKER_HEARTBEAT"}
	if !slices.Equal(got, want) {
		t.Errorf("fields = %v, want %v", got, want)
	}
}

func TestLoad_ParseErrors(t *testing.T) {
	t.Parallel()
	_, err := Load[Worker](envOpts(map[string]string{
		"FF_WORKER_ID": "w1", "FF_WORKER_QUEUES": "q", "FF_API_GRPC_TARGET": "api:9090", "FF_WORKER_CAPACITY": "four",
	}))
	if got := validationFields(t, err); !slices.Equal(got, []string{"FF_WORKER_CAPACITY"}) {
		t.Errorf("fields = %v", got)
	}
	_, err = Load[Raft](envOpts(map[string]string{
		"FF_RAFT_NODE_ID": "-1", "FF_RAFT_PEERS": "p", "FF_RAFT_DATA_DIR": "/d", "FF_RAFT_CLIENT_ADDR": ":1", "FF_RAFT_PEER_ADDR": ":2",
	}))
	if got := validationFields(t, err); !slices.Equal(got, []string{"FF_RAFT_NODE_ID"}) {
		t.Errorf("fields = %v", got)
	}
}

func TestLoad_ProdRequiresAdminBootstrapKey(t *testing.T) {
	t.Parallel()
	_, err := Load[API](envOpts(map[string]string{"FF_ENV": "prod"}))
	if got := validationFields(t, err); !slices.Equal(got, []string{"FF_ADMIN_BOOTSTRAP_KEY"}) {
		t.Errorf("fields = %v, want [FF_ADMIN_BOOTSTRAP_KEY]", got)
	}
	if _, err := Load[API](envOpts(map[string]string{"FF_ENV": "prod", "FF_ADMIN_BOOTSTRAP_KEY": "k"})); err != nil {
		t.Errorf("prod with key: %v", err)
	}
	if _, err := Load[API](envOpts(map[string]string{"FF_ENV": "dev"})); err != nil {
		t.Errorf("dev without key: %v", err)
	}
}

func TestLoad_AuthDisabledRejectedInProd(t *testing.T) {
	t.Parallel()
	_, err := Load[API](envOpts(map[string]string{"FF_ENV": "prod", "FF_ADMIN_BOOTSTRAP_KEY": "k", "FF_AUTH_DISABLED": "true"}))
	if got := validationFields(t, err); !slices.Equal(got, []string{"FF_AUTH_DISABLED"}) {
		t.Errorf("fields = %v, want [FF_AUTH_DISABLED]", got)
	}
	cfg, err := Load[API](envOpts(map[string]string{"FF_ENV": "dev", "FF_AUTH_DISABLED": "true"}))
	if err != nil || cfg.AuthDisabled != "true" {
		t.Errorf("dev with auth disabled = %q, %v", cfg.AuthDisabled, err)
	}
	if _, err := Load[API](envOpts(map[string]string{"FF_AUTH_DISABLED": "yes"})); err == nil {
		t.Error("FF_AUTH_DISABLED=yes accepted, want oneof true false")
	}
}

func TestLoad_ConfigFile(t *testing.T) {
	t.Parallel()
	t.Run("scalars of any YAML type and another service's keys", func(t *testing.T) {
		t.Parallel()
		env := writeFile(t, "FF_WORKER_ID: w9\nFF_WORKER_QUEUES: a,b\nFF_WORKER_CAPACITY: 8\nFF_API_GRPC_TARGET: api:9090\nFF_PG_DSN: postgres://x@h/db\n", nil)
		w, err := Load[Worker](envOpts(env))
		if err != nil {
			t.Fatal(err)
		}
		if w.ID != "w9" || w.Capacity != 8 || !slices.Equal(w.Queues, []string{"a", "b"}) {
			t.Errorf("got %+v", w)
		}
	})
	t.Run("unknown keys rejected", func(t *testing.T) {
		t.Parallel()
		env := writeFile(t, "FF_PG_DNS: postgres://x@h/db\nFF_SCHED_TIK: 1s\n", nil)
		_, err := Load[Scheduler](envOpts(env))
		if got := validationFields(t, err); !slices.Equal(got, []string{"FF_PG_DNS", "FF_SCHED_TIK"}) {
			t.Errorf("fields = %v", got)
		}
	})
	t.Run("nested map is a parse error", func(t *testing.T) {
		t.Parallel()
		env := writeFile(t, "FF_PG_DSN:\n  host: h\n", nil)
		_, err := Load[API](envOpts(env))
		var ve *ValidationError
		if err == nil || errors.As(err, &ve) || !strings.Contains(err.Error(), "FF_CONFIG_FILE") {
			t.Errorf("want a plain parse error naming FF_CONFIG_FILE, got %v", err)
		}
	})
	t.Run("missing file", func(t *testing.T) {
		t.Parallel()
		_, err := Load[API](envOpts(map[string]string{"FF_CONFIG_FILE": filepath.Join(t.TempDir(), "nope.yaml")}))
		if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("want os.ErrNotExist, got %v", err)
		}
	})
	t.Run("prefix names the file variable", func(t *testing.T) {
		t.Parallel()
		env := writeFile(t, "FF_SCHED_TICK: 9s\n", nil)
		env["XX_CONFIG_FILE"] = env["FF_CONFIG_FILE"]
		delete(env, "FF_CONFIG_FILE")
		opts := envOpts(env)
		opts.Prefix = "XX"
		cfg, err := Load[Scheduler](opts)
		if err != nil || cfg.Tick != 9*time.Second {
			t.Errorf("Tick = %v, err %v; want 9s", cfg.Tick, err)
		}
	})
}

func TestLoad_ReadsProcessEnvByDefault(t *testing.T) {
	t.Setenv("FF_SCHED_TICK", "4s")
	cfg, err := Load[Scheduler](Options{})
	if err != nil || cfg.Tick != 4*time.Second {
		t.Errorf("Tick = %v, err %v; want 4s", cfg.Tick, err)
	}
}

func TestLoad_RejectsNonStruct(t *testing.T) {
	t.Parallel()
	if _, err := Load[string](Options{}); err == nil {
		t.Error("want an error for a non-struct type")
	}
}

func TestLoad_UnknownRuleAndTypeAreReported(t *testing.T) {
	t.Parallel()
	type bad struct {
		A string  `env:"FF_A" default:"x" validate:"shiny"`
		B float64 `env:"FF_B" default:"1.5"`
	}
	_, err := Load[bad](envOpts(nil))
	if got := validationFields(t, err); !slices.Equal(got, []string{"FF_B", "FF_A"}) {
		t.Errorf("fields = %v", got)
	}
}

func TestValidators(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		fn   func(string) bool
		in   string
		want bool
	}{
		{validDSN, "postgres://u:p@localhost:5432/flowforge?sslmode=disable", true},
		{validDSN, "postgresql://h/db", true},
		{validDSN, "postgres://h", false},
		{validDSN, "host=h dbname=db", false},
		{validDSN, "mysql://h/db", false},
		{validAddr, ":8080", true},
		{validAddr, "raft-1:7000", true},
		{validAddr, "[::1]:0", true},
		{validAddr, "host", false},
		{validAddr, "host:99999", false},
		{validURL, "http://ai-ops:8000", true},
		{validURL, "ai-ops:8000", false},
	} {
		if got := tt.fn(tt.in); got != tt.want {
			t.Errorf("validator(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}
