package testutil

import (
	"errors"
	"strings"
	"testing"
)

func TestWithDatabase(t *testing.T) {
	tests := []struct {
		name    string
		admin   string
		want    string
		wantErr bool
	}{
		{
			name:  "replaces database and keeps query",
			admin: "postgres://postgres:pw@localhost:5432/postgres?sslmode=disable",
			want:  "postgres://postgres:pw@localhost:5432/test_ab?sslmode=disable",
		},
		{
			name:  "adds database when missing",
			admin: "postgresql://u:p@db:6543",
			want:  "postgresql://u:p@db:6543/test_ab",
		},
		{name: "rejects keyword DSN", admin: "host=localhost user=postgres password=pw", wantErr: true},
		{name: "rejects other scheme", admin: "mysql://u:p@db/x", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := withDatabase(tt.admin, "test_ab")
			if tt.wantErr {
				if err == nil {
					t.Fatalf("withDatabase(%q) = %q, want an error", tt.admin, got)
				}
				if strings.Contains(err.Error(), "pw") {
					t.Errorf("error leaks the password: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("withDatabase(%q): %v", tt.admin, err)
			}
			if got != tt.want {
				t.Errorf("withDatabase(%q) = %q, want %q", tt.admin, got, tt.want)
			}
		})
	}
}

func TestServerUnavailable_SkipsByDefault(t *testing.T) {
	t.Setenv(requireDockerEnv, "")
	rec := &recordingTB{}
	pgServer.unavailable(rec, errors.New("docker not running"))
	if !rec.skipped || rec.failed {
		t.Fatalf("skipped=%v failed=%v, want a skip", rec.skipped, rec.failed)
	}
	if !strings.Contains(rec.msg, "FF_TEST_PG_DSN") {
		t.Errorf("skip message %q should name FF_TEST_PG_DSN", rec.msg)
	}
}

func TestServerUnavailable_FailsWhenDockerRequired(t *testing.T) {
	t.Setenv(requireDockerEnv, "1")
	rec := &recordingTB{}
	tsServer.unavailable(rec, errors.New("docker not running"))
	if !rec.failed || rec.skipped {
		t.Fatalf("skipped=%v failed=%v, want a failure", rec.skipped, rec.failed)
	}
}

func TestApplyMigrations_NotYetAvailable(t *testing.T) {
	if err := ApplyMigrations(t, "postgres://x/y", "migrations/flowforge"); !errors.Is(err, ErrMigrationsUnavailable) {
		t.Fatalf("ApplyMigrations error = %v, want ErrMigrationsUnavailable", err)
	}
}

func TestRandomHex_LengthAndAlphabet(t *testing.T) {
	got := randomHex(8)
	if len(got) != 16 || strings.Trim(got, "0123456789abcdef") != "" {
		t.Fatalf("randomHex(8) = %q, want 16 lowercase hex chars", got)
	}
}
