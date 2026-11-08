package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// pgImage matches the `postgres` service in docker-compose.yml; update both together.
const pgImage = "pgvector/pgvector:0.8.7-pg16"

// requireDockerEnv makes a missing Docker fail the test instead of skipping it (CI sets it).
const requireDockerEnv = "FF_TEST_REQUIRE_DOCKER"

// PG is a database created for one test. It is dropped when that test ends.
type PG struct {
	// DSN connects to the test's own database.
	DSN string
	// Database is that database's name, test_<16 hex chars>.
	Database string
	// AdminDSN connects to the server's maintenance database with rights to create and drop
	// databases. Use it only to inspect the server, never to share state between tests.
	AdminDSN string
}

// server is one kind of database server, started at most once per test binary and shared by every
// test in it; each test still gets its own database (newDatabase). Starting a container per test
// would cost several seconds each, so this is the one place package-level state is allowed.
type server struct {
	name     string // for messages
	image    string
	dsnEnv   string // admin DSN override, e.g. FF_TEST_PG_DSN
	user     string
	password string

	mu       sync.Mutex
	adminDSN string // set once the server is reachable
}

var pgServer = &server{name: "postgres", image: pgImage, dsnEnv: "FF_TEST_PG_DSN", user: "postgres", password: "postgres"}

// NewPostgres returns a fresh, empty database on the pgvector Postgres server (the `flowforge` and
// `aiops` server in Compose). It skips or fails the test if no server is available; see the package
// documentation.
func NewPostgres(t testing.TB) *PG {
	t.Helper()
	return newDatabase(t, pgServer)
}

func newDatabase(t testing.TB, s *server) *PG {
	t.Helper()
	admin := s.start(t)
	name := "test_" + randomHex(8)

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if err := execAdmin(ctx, admin, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("testutil: create database %s on %s: %v", name, s.name, err)
	}
	t.Cleanup(func() {
		// t.Context() is already cancelled when cleanups run, so use a fresh one.
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := execAdmin(ctx, admin, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Errorf("testutil: drop database %s on %s: %v", name, s.name, err)
		}
	})

	dsn, err := withDatabase(admin, name)
	if err != nil {
		t.Fatalf("testutil: %v", err)
	}
	return &PG{DSN: dsn, Database: name, AdminDSN: admin}
}

// start returns the server's admin DSN, starting a container on first use unless dsnEnv is set.
func (s *server) start(t testing.TB) string {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.adminDSN != "" {
		return s.adminDSN
	}
	if dsn := os.Getenv(s.dsnEnv); dsn != "" {
		s.adminDSN = dsn
		return dsn
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	c, err := postgres.Run(ctx, s.image,
		postgres.WithUsername(s.user),
		postgres.WithPassword(s.password),
		postgres.WithDatabase("postgres"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		s.unavailable(t, err)
		return ""
	}
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("testutil: %s connection string: %v", s.name, err)
	}
	s.adminDSN = dsn
	return dsn
}

// unavailable skips the test when no container could be started, or fails it when
// FF_TEST_REQUIRE_DOCKER is set, so CI can't pass by skipping everything.
func (s *server) unavailable(t testing.TB, err error) {
	t.Helper()
	if os.Getenv(requireDockerEnv) != "" {
		t.Fatalf("testutil: start %s container: %v (%s is set)", s.name, err, requireDockerEnv)
		return
	}
	t.Skipf("testutil: skipping, cannot start %s container (is Docker running?): %v. Set %s to use an existing server, or %s to fail instead of skip.", s.name, err, s.dsnEnv, requireDockerEnv)
}

func execAdmin(ctx context.Context, adminDSN, sql string) error {
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }() // nothing to do if closing an admin connection fails
	_, err = conn.Exec(ctx, sql)
	return err
}

// withDatabase returns adminDSN pointed at database name. Only URL-form DSNs are supported.
func withDatabase(adminDSN, name string) (string, error) {
	u, err := url.Parse(adminDSN)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return "", fmt.Errorf("admin DSN must be a postgres:// URL, got %q", redact(adminDSN))
	}
	u.Path = "/" + name
	return u.String(), nil
}

// redact hides the password of a URL-form DSN so it can appear in test output.
func redact(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		return "<unparseable DSN>"
	}
	return u.Redacted()
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b) // crypto/rand.Read never returns an error
	return hex.EncodeToString(b)
}
