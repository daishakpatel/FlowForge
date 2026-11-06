package testutil

import "testing"

// tsImage matches the `timescaledb` service in docker-compose.yml; update both together.
const tsImage = "timescale/timescaledb:2.31.0-pg16"

var tsServer = &server{name: "timescaledb", image: tsImage, dsnEnv: "FF_TEST_MD_DSN", user: "postgres", password: "postgres"}

// NewTimescale returns a fresh, empty database on the TimescaleDB server (the `marketdata` server in
// Compose). The timescaledb extension is already installed: the image adds it to template1, so every
// new database inherits it. It skips or fails the test if no server is available; see the package
// documentation.
func NewTimescale(t testing.TB) *PG {
	t.Helper()
	return newDatabase(t, tsServer)
}
