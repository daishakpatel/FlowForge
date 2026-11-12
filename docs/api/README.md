# FlowForge REST API

`flowforge-api` serves the REST API for workflow definitions, runs, tasks, attempts and run events. The contract is [`api/openapi/flowforge.v1.yaml`](../../api/openapi/flowforge.v1.yaml) (OpenAPI 3.1); the running server also serves it at `GET /openapi.json`. This page covers the conventions; the role table and route list are `00E` §9, the error codes `00E` §4.

## Running it

```bash
make dev-up && make migrate-up             # Postgres with the current schema
go run ./cmd/flowforge-api   # first start in dev prints an admin key once; listens on :8080
```

The server refuses to start when the schema is behind its migrations (`run flowforge-migrate up`). On SIGTERM it stops accepting connections and lets in-flight requests finish for up to 30 s.

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Liveness: 200 while the process serves HTTP. |
| `GET /readyz` | Readiness: 200 when Postgres answers and the schema is current, else 503 `UNAVAILABLE` with `Retry-After`. |
| `GET /metrics` | Prometheus metrics, including `flowforge_http_requests_total{route,method,code}` and `flowforge_http_request_duration_seconds`. |
| `GET /openapi.json` | The contract. |

## Authentication

Every `/api/v1` route needs an API key: `Authorization: Bearer ff_<keyid>_<secret>`. Keys have a role (`viewer`, `operator`, `admin`, or a service role) that decides which routes they may call; each key is rate limited, and every mutation and every refusal is audited.

- **First key:** on the first start in dev the server prints a generated admin key once; in prod, set `FF_ADMIN_BOOTSTRAP_KEY`.
- **Without keys:** with `FF_AUTH_DISABLED=true` (dev and test only), every request runs as the dev admin, and `created_by` is `dev`.
- **Never needed:** the operational endpoints above don't take a key.

Key management, roles, rate limits and the audit log are covered in [`authentication.md`](authentication.md).

## Requests and limits

| Limit | Value | Error |
|---|---|---|
| Request body | 1 MiB | `413 PAYLOAD_TOO_LARGE` |
| Workflow definition | 512 KiB | `413 PAYLOAD_TOO_LARGE` |
| Run input | JSON object, 256 KiB | `400 INVALID_ARGUMENT` / `413` |
| `Idempotency-Key` | 128 characters | `400 INVALID_ARGUMENT` |
| Request time | 30 s (the SSE stream, 03.07, is exempt) | `503 UNAVAILABLE` |
| Requests per API key | `FF_API_RATE_LIMIT_RPS` (50/s, burst 100) | `429 RATE_LIMITED` with `Retry-After` |

JSON is `snake_case`; times are RFC 3339 in UTC. Request bodies are decoded strictly: an unknown field is `400 INVALID_ARGUMENT`.

## Errors

Every non-2xx response, including unknown routes, has the `00E` §4 envelope:

```json
{"error": {"code": "INVALID_WORKFLOW", "message": "workflow definition is invalid",
           "details": [{"code": "CYCLE", "task_id": "a", "message": "cycle: a → b → a"}],
           "request_id": "req_01JC…"}}
```

- **`code`** is stable; branch on it, never on `message`.
- **`details`** is always an array. For `INVALID_WORKFLOW` it holds one entry per DAG issue, with the issue code (`docs/architecture/workflow-semantics.md`) and the task id.
- **`request_id`** is also in the `X-Request-ID` header of every response, and in the server's log line for the request. Quote it when reporting a problem.
- **Internal failures are sanitized:** they return `500 INTERNAL` with the message `internal error`. SQL, hostnames and stack traces stay in the logs.
- **Database unavailable:** when Postgres is unreachable, requests are `503 UNAVAILABLE` with `Retry-After`.

## Pagination

List endpoints return `{"items": [...], "next_cursor": "…" | null}`.

- **`limit`:** defaults to 50, and values above 200 are clamped to 200. A value that isn't a positive integer is `400`.
- **`cursor`:** pass the previous page's `next_cursor`. It is opaque, so don't build or parse it.
- **Order:** definitions and runs are newest first, ordered by `(created_at, id)`, so equal timestamps never repeat or skip a row. Run events are oldest first.
- **Unpaged lists:** `GET /runs/{run_id}/tasks` and `GET /tasks/{task_id}/attempts` return everything; their `next_cursor` is always `null`.
- **Run filters:** `GET /api/v1/runs` filters on `status`, `workflow` (the definition name), `since` and `until` (RFC 3339, on `created_at`).

## Idempotent run start

`POST /api/v1/workflows/{id}/runs` accepts an `Idempotency-Key` header:

| Request | Response |
|---|---|
| New key | `201 Created` with `Location: /api/v1/runs/{run_id}` |
| Same key, same definition, equal input (key order and whitespace don't matter) | `200 OK`, the original run, `Idempotent-Replay: true` |
| Same key, different definition or input | `409 CONFLICT` |

Concurrent identical requests are safe: exactly one run is created and every response returns it (`TestStartRun_ConcurrentIdempotentRequestsCreateOneRun`). The 03.07 retention job forgets keys after 24 hours.

Definitions are immutable. Posting an existing `(name, version)` is `409 CONFLICT`; posting without `version` creates the next version of that name.

## Examples

Create the `00E` §3.5 example definition (`internal/workflow/model/testdata/market-data-hourly.json`):

```bash
export FF_KEY=ff_…            # the admin key printed on first start, or a key you created
curl -s -X POST localhost:8080/api/v1/workflows -H "Authorization: Bearer $FF_KEY" \
  -H 'Content-Type: application/json' \
  --data @internal/workflow/model/testdata/market-data-hourly.json
# 201 {"id":"wf_01J…","name":"market-data-hourly","version":1,"definition":{…},"created_by":"<keyid>",…}
```

Start a run of it, idempotently, and read it back:

```bash
alias ffc='curl -s -H "Authorization: Bearer $FF_KEY"'
WF=$(ffc localhost:8080/api/v1/workflows/by-name/market-data-hourly | jq -r .id)
ffc -X POST localhost:8080/api/v1/workflows/$WF/runs \
  -H 'Idempotency-Key: nightly-2026-11-11' \
  --data '{"input":{"symbols":["AAPL","MSFT"]}}'
# 201 the first time; 200 with Idempotent-Replay: true when repeated
ffc 'localhost:8080/api/v1/runs?workflow=market-data-hourly&limit=10'
ffc localhost:8080/api/v1/runs/<run_id>/tasks
ffc localhost:8080/api/v1/runs/<run_id>/events
```

## Contract checks

`make openapi-check` runs:

1. spec validation;
2. route parity: every router route has an OpenAPI operation, and vice versa;
3. contract tests: every response of every operation is validated against the spec, and every documented status code must be produced.

CI runs it in the `integration` job. To add a route, add its operation to the spec and its responses to `TestContract_API`; the check fails otherwise.
