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
