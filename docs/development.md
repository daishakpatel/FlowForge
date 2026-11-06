# Development

How to set up a machine to build, run and test FlowForge. Exact versions live in [`decisions/versions.md`](decisions/versions.md).

## Prerequisites

| Tool | Needed from | Version | Install (macOS) |
|---|---|---|---|
| Go | 01.01 | as in `go.mod` | `brew install go` |
| golangci-lint | 01.01 | as in `docs/decisions/versions.md` | `brew install golangci-lint` |
| Docker with Compose v2 | 01.02 | recent Docker Desktop or Engine | Docker Desktop |
| sqlc | 02.01 | recorded by 02.01 | `brew install sqlc` |
| buf | 03.01 / 04.01 | recorded by 03.01 | `brew install bufbuild/buf/buf` |
| Python | 06.01 | ≥ 3.12 | `brew install python` |
| Node.js | 07.01 | ≥ 20 | `brew install node` |
| actionlint, shellcheck | optional | any recent | `brew install actionlint shellcheck` |

## Commands

```bash
make help              # list targets
make lint test         # what CI runs first (lint includes integration-tagged files)
make test-race         # unit tests with the race detector
make test-integration  # integration tests against real databases (10-minute timeout)
make lint-selftest     # prove the package-boundary lint rules still fire
make cover-check       # 70 % coverage gate on the gated packages that exist

make dev-up            # start postgres + timescaledb and wait until healthy
make dev-down          # stop them; data is kept in Docker volumes
make dev-reset         # delete the volumes and start fresh (re-runs the init SQL)
make psql              # psql into flowforge as the flowforge role
make psql-md           # psql into marketdata as the md role
make logs              # follow the stack's logs
```

## Local databases

`make dev-up` starts two Postgres-family servers from `docker-compose.yml` (ADR-013). Ports are published on `127.0.0.1` only, and every credential is a dev-only constant.

| Service | Image | Host port | Databases | Roles (user / password) | Extensions |
|---|---|---|---|---|---|
| `postgres` | `pgvector/pgvector:0.8.7-pg16` | `5432` (`FF_DEV_PG_PORT`) | `flowforge`, `aiops` | `flowforge`/`flowforge` owns `flowforge`; `aiops`/`aiops` owns `aiops`; admin `postgres`/`postgres` | `vector` in `aiops` |
| `timescaledb` | `timescale/timescaledb:2.31.0-pg16` | `5433` (`FF_DEV_MD_PORT`) | `marketdata` | `md`/`md` (admin) | `timescaledb` |

The connection strings the services use are in [`.env.example`](../.env.example) and `00E` §6. The `postgres` server's roles and databases come from `deploy/docker/postgres-init/`, which runs only when the volume is first created.

### Compose profiles

| Profile | Services today | Planned additions |
|---|---|---|
| `core` | `postgres` | API, scheduler, worker (09.02) |
| `streaming` | `timescaledb` | Kafka (05.01), market-data processor (05.x) |
| `raft` | none yet | 3-node Raft cluster (04.06) |
| `ai` | `postgres` (hosts `aiops`) | AI service (06.01) |
| `ui` | none yet | dashboard (07.01) |
| `obs` | none yet | Prometheus, Grafana, OTel Collector, Jaeger (08.x) |
| `full` | `postgres`, `timescaledb` | everything |

The `make dev-*` targets use `core` and `streaming`. For another combination, call Compose directly, for example `docker compose --profile full up -d --wait`.

## Integration tests

Integration tests carry the `integration` build tag, so `make test` skips them and `make test-integration` runs them. They get databases from `internal/testutil` (see `go doc ./internal/testutil`):

- **Default:** `testutil` starts throwaway containers from the same pinned images with testcontainers-go, once per test binary. Each test gets its own `test_<hex>` database, dropped when the test ends.
- **Reuse the Compose databases:** set admin DSNs in URL form and the helpers create their per-test databases there instead:

  ```bash
  make dev-up
  FF_TEST_PG_DSN='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable' \
  FF_TEST_MD_DSN='postgres://md:md@localhost:5433/postgres?sslmode=disable' \
  make test-integration
  ```

- **No Docker:** the database tests skip with a message. Set `FF_TEST_REQUIRE_DOCKER=1` to make them fail instead; CI sets it.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `make dev-up` fails with "port is already allocated" | Another container or a local Postgres holds 5432 or 5433. Pick free ports: `FF_DEV_PG_PORT=55432 FF_DEV_MD_PORT=55433 make dev-up` (or set them in `.env`), and use those ports in your DSNs. |
| A database or role from the table is missing | The init SQL runs only on a new volume. Run `make dev-reset` (this deletes the local data). |
| Integration tests are all skipped | Docker isn't running or reachable. Start Docker Desktop, or point `FF_TEST_PG_DSN`/`FF_TEST_MD_DSN` at running servers. |
| `admin DSN must be a postgres:// URL` | `FF_TEST_*_DSN` is in keyword form (`host=… user=…`); use the URL form shown above. |
| `goleak: found unexpected goroutines` | A test left a goroutine running. `testutil.LeakCheck` already ignores the testcontainers reaper; anything else is a real leak to fix. |
