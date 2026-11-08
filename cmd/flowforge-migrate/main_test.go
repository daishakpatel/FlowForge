package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"testing"

	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/persistence/migrate"
)

// envOpts makes run read env instead of the process environment.
func envOpts(env map[string]string) config.Options {
	return config.Options{LookupEnv: func(k string) (string, bool) { v, ok := env[k]; return v, ok }}
}

// TestRun_RejectsBadInvocations: every case fails before connecting to a database.
func TestRun_RejectsBadInvocations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		env  map[string]string
		is   error // nil: any error will do
	}{
		{name: "no command", args: nil},
		{name: "two commands", args: []string{"up", "status"}},
		{name: "unknown command", args: []string{"down"}},
		{name: "unknown database", args: []string{"--db", "aiops", "up"}, is: migrate.ErrUnknownSet},
		{name: "help", args: []string{"-h"}, is: flag.ErrHelp},
		{name: "invalid DSN", args: []string{"up"}, env: map[string]string{"FF_PG_DSN": "mysql://u:p@h/db"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			err := run(context.Background(), tt.args, envOpts(tt.env), &stdout, &stderr)
			if err == nil || (tt.is != nil && !errors.Is(err, tt.is)) {
				t.Errorf("run(%v) = %v, want error %v", tt.args, err, tt.is)
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout should stay empty on error, got %q", stdout.String())
			}
		})
	}

	var ve *config.ValidationError
	err := run(context.Background(), []string{"up"}, envOpts(map[string]string{"FF_PG_DSN": "mysql://u:p@h/db"}), &bytes.Buffer{}, &bytes.Buffer{})
	if !errors.As(err, &ve) {
		t.Errorf("invalid DSN should surface the config ValidationError, got %v", err)
	}
}
