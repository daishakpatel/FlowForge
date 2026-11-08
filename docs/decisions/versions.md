# Versions

The one place agents record the versions they choose (`00C` §2.6, §10.3). Add a row whenever a task adds or upgrades a module, tool, image or action; keep older rows when a later task upgrades.

| Component | Version | Date | Task | Notes |
|---|---|---|---|---|
| Go module path | `github.com/daishakpatel/flowforge` | 2026-11-05 | 01.01 | Lowercase form of the GitHub repo `daishakpatel/FlowForge`. |
| Go | 1.27.3 | 2026-11-05 | 01.01 | `go` directive in `go.mod`; CI reads it via `go-version-file`. No separate `toolchain` line: `go mod tidy` drops it when it equals the `go` version. |
| golangci-lint | 2.15.0 | 2026-11-05 | 01.01 | v2 config schema. Pinned in `Makefile` (`GOLANGCI_LINT_VERSION`, warns on mismatch) and in CI. |
| actions/checkout | v7.0.1 (`3d3c42e5aac5ba805825da76410c181273ba90b1`) | 2026-11-05 | 01.01 | Pinned by commit SHA. |
| actions/setup-go | v7.0.0 (`b7ad1dad31e06c5925ef5d2fc7ad053ef454303e`) | 2026-11-05 | 01.01 | Pinned by commit SHA. |
| golangci/golangci-lint-action | v9.3.0 (`ba0d7d2ec06a0ea1cb5fa41b2e4a3ab91d21278a`) | 2026-11-05 | 01.01 | `install-only: true`; CI then runs `make lint` and `make lint-selftest`. |
| actions/upload-artifact | v7.0.2 (`cf430e030ddbb5b0abf93d22962f4752f3646cd9`) | 2026-11-05 | 01.01 | Uploads `coverage.out`. |
| actionlint | 1.7.12 | 2026-11-05 | 01.01 | Local workflow validation only; not run in CI. |
| shellcheck | 0.11.0 | 2026-11-05 | 01.01 | Local script validation only; not run in CI. |
| Image `pgvector/pgvector` | `0.8.7-pg16` | 2026-11-06 | 01.02 | Compose `postgres` and `internal/testutil` (`pgImage`); keep both in step. PostgreSQL 16 + pgvector 0.8.7. |
| Image `timescale/timescaledb` | `2.31.0-pg16` | 2026-11-06 | 01.02 | Compose `timescaledb` and `internal/testutil` (`tsImage`); keep both in step. Telemetry off in Compose. |
| `github.com/testcontainers/testcontainers-go` (+ `modules/postgres`) | v0.45.0 | 2026-11-06 | 01.02 | MIT. Starts throwaway database containers; pulls `testcontainers/ryuk:0.14.0` to clean them up. |
| `github.com/jackc/pgx/v5` | v5.12.0 | 2026-11-06 | 01.02 | MIT. Used by `internal/testutil` for admin connections; 02.03 uses it for the app. |
| `go.uber.org/goleak` | v1.3.0 | 2026-11-06 | 01.02 | MIT. `testutil.LeakCheck`. |
| `github.com/pressly/goose/v3` | v3.29.0 | 2026-11-08 | 02.01 | MIT. Library only (no goose CLI); approved stack. Adds `mfridman/interpolate`, `sethvargo/go-retry`, `golang.org/x/sync`, `go.uber.org/multierr`; its minimums raised `otel` to v1.47.0, `otelhttp` to v0.72.0, `logrus` to v1.10.2, `go-winio` to v0.6.3. `modernc.org/sqlite` appears in `go.sum` only, for goose's own tests. |
| sqlc | v1.31.1 | 2026-11-08 | 02.01 | MIT. Code generator, not a module dependency. `SQLC_VERSION` in the Makefile (`make sqlc` refuses other versions) and the CI `sqlc-drift` job; install with `go install github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1` (needs cgo). |
| `github.com/oklog/ulid/v2` | v2.1.2 | 2026-11-07 | 01.04 | Apache-2.0. `internal/ids`; named by task 01.04. |
| `github.com/prometheus/client_golang` | v1.25.0 | 2026-11-07 | 01.04 | Apache-2.0. `internal/obs`; approved stack (`00C` §10.3). Pulls `client_model` v0.6.3, `common` v0.72.0, `procfs` v0.22.0. |
| `google.golang.org/grpc` | v1.85.0 | 2026-11-07 | 01.04 | Apache-2.0. Only `grpc/codes` so far (`apperrors.GRPCCode`); approved stack. Was indirect at v1.67.0. Its minimum versions raised these indirect modules: `protobuf` v1.36.12, `otel` v1.45.0, `go-logr/logr` v1.4.4, `klauspost/compress` v1.20.1. |
| `go.yaml.in/yaml/v3` | v3.0.5 | 2026-11-07 | 01.03 | MIT + Apache-2.0. Parses the `FF_CONFIG_FILE` YAML layer in `internal/config`; the standard library has no YAML. Maintained successor of `gopkg.in/yaml.v3`; was already in `go.sum` as an indirect dependency of testcontainers, now direct. |
