//go:build e2e

// Package e2e is the workflow-engine end-to-end suite (task 03.09): real flowforge-api,
// flowforge-scheduler and flowforge-worker binaries run as subprocesses against Postgres in a
// container, and the tests kill, pause and restart them. Run with `make test-e2e`; see
// docs/testing/e2e.md.
package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// binDir holds the binaries built once for the whole run (set by TestMain, read-only after).
var binDir string

// binaries are built from cmd/<name>.
var binaries = []string{"flowforge-api", "flowforge-scheduler", "flowforge-worker", "flowforgectl", "flowforge-raft"}

func TestMain(m *testing.M) {
	code, err := buildAndRun(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func buildAndRun(m *testing.M) (int, error) {
	dir, err := os.MkdirTemp("", "flowforge-e2e-bin-")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(dir) }() // best-effort cleanup of the temp build dir
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..")
	for _, b := range binaries {
		cmd := exec.CommandContext(context.Background(), "go", "build", "-race", "-o", filepath.Join(dir, b), "./cmd/"+b) //nolint:gosec // fixed binary names
		cmd.Dir, cmd.Stdout, cmd.Stderr = root, os.Stderr, os.Stderr
		if err := cmd.Run(); err != nil {
			return 0, fmt.Errorf("build %s: %w", b, err)
		}
	}
	binDir = dir
	return m.Run(), nil
}
