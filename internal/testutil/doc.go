// Package testutil provides the shared harness for FlowForge tests (task 01.02): real, isolated
// databases for integration tests, polling helpers, and a goroutine-leak check.
//
// # Databases
//
// NewPostgres and NewTimescale give each test its own empty database, created on a server shared
// by the whole test binary and dropped when the test ends, so tests can run in parallel:
//
//	//go:build integration
//
//	func TestRepo_Insert(t *testing.T) {
//		t.Parallel()
//		db := testutil.NewPostgres(t) // db.DSN points at a fresh test_<hex> database
//		pool, err := pgxpool.New(t.Context(), db.DSN)
//		if err != nil {
//			t.Fatal(err)
//		}
//		t.Cleanup(pool.Close)
//		// ...
//	}
//
// The server comes from one of two places:
//   - FF_TEST_PG_DSN / FF_TEST_MD_DSN, when set: an admin DSN in URL form for an existing server,
//     such as the Compose databases from `make dev-up`.
//   - Otherwise a container started with testcontainers-go, using the same pinned images as
//     docker-compose.yml. Ryuk removes it when the test binary exits.
//
// If no container can be started (for example, Docker isn't running), the test is skipped, unless
// FF_TEST_REQUIRE_DOCKER is set, in which case it fails. CI sets it. Integration tests carry the
// `integration` build tag and run with `make test-integration`.
//
// Migrations are not applied yet: ApplyMigrations returns ErrMigrationsUnavailable until task
// 02.01 adds the migration runner.
//
// # Waiting
//
// Use WaitFor instead of time.Sleep when a test waits for something asynchronous:
//
//	testutil.WaitFor(t, func() bool { return worker.Done() }, 2*time.Second)
//
// Eventually is the non-failing form, for callers that want to report their own message.
//
// # Goroutine leaks
//
// Packages that start goroutines check for leaks from TestMain (00C §2.10):
//
//	func TestMain(m *testing.M) { testutil.LeakCheck(m) }
package testutil
