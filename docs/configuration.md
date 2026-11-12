# Configuration

Go services read their configuration through `internal/config` (task 01.03). Each service's `main` calls `config.Load` once and passes the parts each constructor needs; there is no global config. The variable names and defaults come from `00E` §6 ([contracts reference](../implementation/00E_CONTRACTS_REFERENCE.md)); `internal/config/drift_test.go` fails if a variable a service reads is missing from this page, `.env.example` or `00E` §6.

```go
cfg, err := config.Load[config.API](config.Options{Prefix: "FF"})
if err != nil {
	return err // a *config.ValidationError lists every invalid variable
}
logger.Info("config", "values", cfg.Redacted())
```

## Precedence

Later layers win:

1. The default in the struct tag (the table below).
2. The YAML file named by `FF_CONFIG_FILE`, if set.
3. Environment variables. A variable that is set but empty counts as unset, so it can't clear a file value by accident.

The config file is a flat map of the same variable names to scalar values. Lists stay comma-separated, as in the environment:

```yaml
FF_ENV: dev
FF_SCHED_TICK: 250ms
FF_KAFKA_BROKERS: localhost:9092,localhost:9093
```

A key that no FlowForge service reads, such as a typo like `FF_PG_DNS`, is rejected. Keys that belong to another service are allowed, so one file can serve every service. An unreadable or malformed file fails `Load` with a plain error.

## Validation

`Load` checks every variable and reports all problems at once in a `*config.ValidationError` (`Fields []FieldError`, each with `Env` and `Msg`). Messages never include the value, so the error is safe to log even for secrets.

| Rule | Meaning |
|---|---|
| `required` | Must be set (no default exists) |
| `required_in_prod` | Must be set when `FF_ENV=prod` |
| `positive` | Number or duration greater than 0 |
| `dsn` | URL-form Postgres DSN: `postgres://` or `postgresql://`, with a host and database name |
| `addr` | `host:port`; the host may be empty (`:8080`); applies to each item of a list |
| `url` | Absolute URL with a scheme and host |
| `oneof` | One of the listed values |

Cross-field rules: `FF_LEADER_RENEW` must be less than half of `FF_LEADER_TTL`, and `FF_WORKER_HEARTBEAT` less than half of `FF_WORKER_LEASE_TTL`, so a lease survives one missed renewal. Durations use Go syntax (`500ms`, `10s`, `1m30s`).

## Variables

Defaults are dev-only. "—" means there is no default.

| Variable | Default | Rules | Read by (struct) |
|---|---|---|---|
| `FF_ENV` | `dev` | oneof `dev test prod` | all (`Common`) |
| `FF_LOG_LEVEL` | `info` | oneof `debug info warn error` | all (`Common`) |
| `FF_LOG_FORMAT` | `json` | oneof `json text` | all (`Common`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4317` | url | all (`Common`) |
| `FF_CONFIG_FILE` | — | file path | `Load` itself (`<Prefix>_CONFIG_FILE`) |
| `FF_PG_DSN` | `postgres://flowforge:flowforge@localhost:5432/flowforge?sslmode=disable` | dsn, secret | `API`, `Scheduler`, `Migrate` |
| `FF_MD_PG_DSN` | `postgres://md:md@localhost:5433/marketdata?sslmode=disable` | dsn, secret | `Processor`, `API` (analytics), `Migrate` |
| `FF_API_HTTP_ADDR` | `:8080` | addr | `API` |
| `FF_API_GRPC_ADDR` | `:9090` | addr | `API` |
| `FF_ADMIN_BOOTSTRAP_KEY` | — | required_in_prod, secret | `API` |
| `FF_AUTH_DISABLED` | `false` | oneof `true false`; `true` rejected when `FF_ENV=prod` | `API` |
| `FF_API_RATE_LIMIT_RPS` | `50` | positive; per API key, burst 2×, service roles 4× | `API` |
| `FF_RAFT_ENDPOINTS` | `raft-1:7000,raft-2:7000,raft-3:7000` | addr (each) | `API`, `Scheduler` |
| `FF_SCHED_TICK` | `500ms` | positive | `Scheduler` |
| `FF_LEADER_TTL` | `10s` | positive | `Scheduler` |
| `FF_LEADER_RENEW` | `3s` | positive, < TTL/2 | `Scheduler` |
| `FF_KAFKA_BROKERS` | `localhost:9092` | addr (each) | `Scheduler` (outbox relay), `Worker`, `Processor` |
| `FF_WORKER_LEASE_TTL` | `15s` | positive | `API`, `Worker` |
| `FF_WORKER_HEARTBEAT` | `5s` | positive, < lease TTL/2 | `API`, `Worker` |
| `FF_WORKER_ID` | — | required | `Worker` |
| `FF_WORKER_QUEUES` | — | required (list) | `Worker` |
| `FF_WORKER_CAPACITY` | — | positive | `Worker` |
| `FF_API_GRPC_TARGET` | — | required, addr | `Worker` |
| `FF_WORKER_TOKEN` | — | secret | `Worker` |
| `FF_AI_URL` | `http://ai-ops:8000` | url | `Worker` |
| `FF_AI_TOKEN` | — | secret | `Worker` |
| `FF_RAFT_NODE_ID` | — | positive (uint64, as the etcd Raft library requires) | `Raft` |
| `FF_RAFT_PEERS` | — | required (list; entry format from task 04.02) | `Raft` |
| `FF_RAFT_DATA_DIR` | — | required | `Raft` |
| `FF_RAFT_CLIENT_ADDR` | — | required, addr | `Raft` |
| `FF_RAFT_PEER_ADDR` | — | required, addr | `Raft` |

Not read through `internal/config`:

- `FF_DEV_PG_PORT`, `FF_DEV_MD_PORT`: Compose host ports ([development guide](development.md)).
- `FF_TEST_PG_DSN`, `FF_TEST_MD_DSN`, `FF_TEST_REQUIRE_DOCKER`: read by `internal/testutil`, the one package besides `internal/config` and `cmd/**` that the lint rule lets read the environment (`00A` amendment of 01.02).
- `AI_*`: the Python AI service's own settings (task 06.01), with the names in `00E` §6.

## Secrets

Fields of type `config.Secret` (DSNs, keys, tokens) print as `***` in every `fmt` verb, `encoding/json`, `encoding.TextMarshaler` and `slog`. Only `Reveal()` returns the value, so use it only where the value is consumed. Secret fields must be exported: `fmt` prints unexported fields without calling their methods. `Redacted()` on each service struct returns every variable for the startup log, with set secrets shown as `***` and unset ones as an empty string.

Secrets come only from the environment or the config file (`00C` §10.4). Dev defaults such as the DSN passwords above are dev-only; production guards are added in task 09.01.

## Reading the environment elsewhere

The `forbidigo` lint rule rejects `os.Getenv`, `os.LookupEnv` and `os.Environ` everywhere except `internal/config`, `cmd/**` and `internal/testutil`. Add a field to a service struct instead, and add the variable to `00E` §6, `.env.example` and this page in the same change.
