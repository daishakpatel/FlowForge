package version

import (
	"os/exec"
	"strings"
	"testing"
)

func TestVersion_Defaults(t *testing.T) {
	if got := Version(); got != "dev" {
		t.Errorf("Version() = %q, want %q", got, "dev")
	}
	if got := Commit(); got != "unknown" {
		t.Errorf("Commit() = %q, want %q", got, "unknown")
	}
}

func TestVersion_LdflagsOverride(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a binary; skipped with -short")
	}
	const pkg = "github.com/daishakpatel/flowforge/internal/version"
	ldflags := "-X " + pkg + ".version=v1.2.3 -X " + pkg + ".commit=abc1234"

	out, err := exec.CommandContext(t.Context(), "go", "run", "-ldflags", ldflags, "./testdata/printversion").CombinedOutput()
	if err != nil {
		t.Fatalf("go run: %v\n%s", err, out)
	}
	if got, want := strings.TrimSpace(string(out)), "v1.2.3 abc1234"; got != want {
		t.Errorf("linked binary printed %q, want %q", got, want)
	}
}
