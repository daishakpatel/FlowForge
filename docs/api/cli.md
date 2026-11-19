# flowforgectl

`flowforgectl` is the command-line client of the REST API ([README](README.md)). It sends each command as one or two API calls with your API key and prints the answer.

```bash
go build -o bin/flowforgectl ./cmd/flowforgectl
export FF_API_URL=http://localhost:8080      # the default
export FF_API_KEY=ff_xxxxxxxxxxxx_...         # a key whose role allows the commands you run
bin/flowforgectl workflows list
```

## Usage

```text
flowforgectl [-o table|json] <resource> <verb> [args] [flags]
```

- **Output:** `-o table` (the default) prints chosen columns. `-o json` prints the API's JSON unchanged, indented. `-o` may appear anywhere on the line.
- **Flags:** flags may come before or after the positional arguments.
- **Lists:** list commands follow `next_cursor`, 200 rows a page, and print every row.
- **Exit codes:**

| Code | Meaning | What is printed on stderr |
|---|---|---|
| 0 | Success | — |
| 1 | The API answered with an error, or it couldn't be reached | `API error: <status> <CODE>: <message> (request_id …)`, then the error envelope as JSON |
| 2 | A usage error: unknown command, missing argument, bad flag | The usage line |

## Commands

| Command | API call | Notes |
|---|---|---|
| `workflows create -f def.json` | `POST /workflows` | Stores a new version. An invalid DAG exits 1 with the validation code, e.g. `CYCLE`. |
| `workflows list` | `GET /workflows` | |
| `workflows get <wf_id\|name>` | `GET /workflows/{id}` or `/workflows/by-name/{name}` | A name gives its latest version. |
| `runs start <wf_id\|name> [--input in.json] [--idempotency-key k]` | `POST /workflows/{id}/runs` (a name is first resolved to its latest version) | Repeating the same key returns the first run (`Idempotent-Replay`). |
| `runs get <run_id>` | `GET /runs/{id}` | |
| `runs tasks <run_id> [--all]` | `GET /runs/{id}/tasks` | `--all` includes rows that a rerun superseded. |
| `runs events <run_id>` | `GET /runs/{id}/events` | Every event, oldest first. |
| `runs logs <run_id\|att_id>` | `GET /attempts/{id}/logs` | Every attempt of the run, or just one attempt. |
| `runs watch <run_id>` | `GET /events/stream?run_id=` (SSE) | Prints events from the run's first one until `run.succeeded`, `run.failed`, `run.cancelled` or `run.needs_attention`. A dropped stream, or one silent for 45 s (the server sends a keep-alive every 15 s), resumes with `Last-Event-ID`. It gives up after 5 reconnects in a row that deliver no event. With `-o json`, it prints one event envelope per line. |
| `runs cancel <run_id> [--force]` | `POST /runs/{id}/cancel` | `--force` cancels running attempts at once instead of after `FF_CANCEL_GRACE`. |
| `runs pause <run_id>` / `runs resume <run_id>` | `POST /runs/{id}/pause` / `resume` | |
| `runs rerun <run_id> --reason r [--task key …]` | `POST /runs/{id}/rerun` | `--task` is repeatable. Without it, every failed task is rerun. |
| `runs retry <run_id> [--reason r]` | `POST /runs/{id}/retry` | |
| `workers list` | `GET /workers` | |
| `triggers create --name n --workflow w --schedule '0 * * * *' [--timezone tz] [--version n] [--input in.json] [--missed-policy skip\|catch_up_one] [--disabled]` | `POST /triggers` | |
| `triggers list` | `GET /triggers` | |
| `triggers enable <trg_id>` / `triggers disable <trg_id>` | `PATCH /triggers/{id}` | |
| `api-keys create --name n --role r [--expires-at RFC3339]` | `POST /api-keys` | The secret is printed once. |
| `api-keys list` / `api-keys revoke <key_id>` | `GET /api-keys` / `DELETE /api-keys/{id}` | |

## Example

```console
$ flowforgectl workflows create -f test/fixtures/workflows/cyclic.json
flowforgectl: API error: 422 INVALID_WORKFLOW: workflow definition is invalid (request_id req_01M7RX3AQ6T319W97XR78T02TF)
{
  "error": {
    "code": "INVALID_WORKFLOW",
    "message": "workflow definition is invalid",
    "request_id": "req_01M7RX3AQ6T319W97XR78T02TF",
    "details": [
      {
        "code": "CYCLE",
        "message": "dependency cycle: a → b → a (each task depends on the next)",
        "task_id": "a"
      }
    ]
  }
}
$ echo $?
1
```

[`scripts/demo/engine-demo.sh`](../../scripts/demo/engine-demo.sh) runs a full session against a running deployment; the e2e suite runs it in CI (`TestDemoScript`).
