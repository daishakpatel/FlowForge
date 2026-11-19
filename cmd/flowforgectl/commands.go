package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
)

func commands() map[string]command {
	return map[string]command{
		"workflows create": workflowsCreate, "workflows list": workflowsList, "workflows get": workflowsGet,
		"runs start": runsStart, "runs get": runsGet, "runs tasks": runsTasks, "runs events": runsEvents,
		"runs logs": runsLogs, "runs watch": runsWatch, "runs cancel": runsCancel, "runs pause": runsControl("pause"),
		"runs resume": runsControl("resume"), "runs rerun": runsRerun, "runs retry": runsRetry,
		"workers list":    workersList,
		"triggers create": triggersCreate, "triggers list": triggersList,
		"triggers enable": triggersSetEnabled(true), "triggers disable": triggersSetEnabled(false),
		"api-keys create": apiKeysCreate, "api-keys list": apiKeysList, "api-keys revoke": apiKeysRevoke,
	}
}

var (
	workflowCols = []string{"id", "name", "version", "created_by", "created_at"}
	runCols      = []string{"id", "status", "definition_id", "created_at", "completed_at"}
	taskCols     = []string{"id", "task_key", "status", "attempt_count", "supersedes_task_id"}
	eventCols    = []string{"id", "event_type", "task_id", "created_at"}
	workerCols   = []string{"worker_id", "status", "queues", "capacity", "active_tasks", "last_seen_at"}
	triggerCols  = []string{"id", "name", "workflow_name", "schedule", "timezone", "enabled", "next_fire_at"}
	keyCols      = []string{"id", "name", "role", "created_at", "expires_at", "revoked_at"}
	logCols      = []string{"ts", "level", "attempt_id", "message"}
)

// all fetches every page of the list endpoint path, 200 rows a page, following next_cursor
// through the query parameter param ("cursor", or "after_id" for log lines). It returns
// {"items": [...]}, the shape printer.print expects.
func (e *env) all(ctx context.Context, path, param string) (map[string]any, error) {
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	var items []any
	for next := ""; ; {
		p := path + sep + "limit=200"
		if next != "" {
			p += "&" + param + "=" + url.QueryEscape(next)
		}
		var page struct {
			Items      []any   `json:"items"`
			NextCursor *string `json:"next_cursor"`
		}
		if err := e.c.do(ctx, "GET", p, nil, &page); err != nil {
			return nil, err
		}
		items = append(items, page.Items...)
		if page.NextCursor == nil || *page.NextCursor == "" {
			return map[string]any{"items": items}, nil
		}
		next = *page.NextCursor
	}
}

// list prints every row of the list endpoint path with cols.
func (e *env) list(ctx context.Context, path string, cols []string) error {
	v, err := e.all(ctx, path, "cursor")
	if err != nil {
		return err
	}
	return e.out.print(v, cols...)
}

// get fetches path and prints it with cols.
func (e *env) get(ctx context.Context, path string, cols []string) error {
	var v any
	if err := e.c.do(ctx, "GET", path, nil, &v); err != nil {
		return err
	}
	return e.out.print(v, cols...)
}

// send sends body to path with method and prints the answer with cols (nothing for 204).
func (e *env) send(ctx context.Context, method, path string, body any, cols []string, headers ...string) error {
	var v any
	if err := e.c.do(ctx, method, path, body, &v, headers...); err != nil {
		return err
	}
	if v == nil {
		return nil
	}
	return e.out.print(v, cols...)
}

func esc(s string) string { return url.PathEscape(s) }

func workflowsCreate(ctx context.Context, e *env, args []string) error {
	fs := flag.NewFlagSet("workflows create", flag.ContinueOnError)
	file := fs.String("f", "", "definition JSON file")
	if _, err := parse(fs, args, 0, "workflows create -f file.json"); err != nil {
		return err
	}
	if *file == "" {
		return usagef("usage: workflows create -f file.json")
	}
	raw, err := os.ReadFile(*file)
	if err != nil {
		return fmt.Errorf("read %s: %w", *file, err)
	}
	return e.send(ctx, "POST", "/api/v1/workflows", raw, workflowCols)
}

func workflowsList(ctx context.Context, e *env, args []string) error {
	fs := flag.NewFlagSet("workflows list", flag.ContinueOnError)
	if _, err := parse(fs, args, 0, "workflows list"); err != nil {
		return err
	}
	return e.list(ctx, "/api/v1/workflows", workflowCols)
}

func workflowsGet(ctx context.Context, e *env, args []string) error {
	pos, err := parse(flag.NewFlagSet("workflows get", flag.ContinueOnError), args, 1, "workflows get <id|name>")
	if err != nil {
		return err
	}
	return e.get(ctx, workflowPath(pos[0]), workflowCols)
}

// workflowPath addresses a workflow by id (wf_…) or by name (its latest version).
func workflowPath(ref string) string {
	if strings.HasPrefix(ref, "wf_") {
		return "/api/v1/workflows/" + esc(ref)
	}
	return "/api/v1/workflows/by-name/" + esc(ref)
}

func runsStart(ctx context.Context, e *env, args []string) error {
	fs := flag.NewFlagSet("runs start", flag.ContinueOnError)
	input := fs.String("input", "", "run input JSON file")
	key := fs.String("idempotency-key", "", "Idempotency-Key")
	const u = "runs start <workflow id|name> [--input file] [--idempotency-key k]"
	pos, err := parse(fs, args, 1, u)
	if err != nil {
		return err
	}
	wf := struct {
		ID string `json:"id"`
	}{ID: pos[0]}
	if !strings.HasPrefix(pos[0], "wf_") { // a name: resolve its latest version
		if err := e.c.do(ctx, "GET", workflowPath(pos[0]), nil, &wf); err != nil {
			return err
		}
	}
	body := map[string]json.RawMessage{}
	if *input != "" {
		raw, err := os.ReadFile(*input)
		if err != nil {
			return fmt.Errorf("read %s: %w", *input, err)
		}
		body["input"] = raw
	}
	var headers []string
	if *key != "" {
		headers = []string{"Idempotency-Key", *key}
	}
	return e.send(ctx, "POST", "/api/v1/workflows/"+esc(wf.ID)+"/runs", body, runCols, headers...)
}

// runOne is "runs <verb> <run id>" reading path(id).
func runOne(ctx context.Context, e *env, args []string, verb string, path func(id string) string, cols []string) error {
	pos, err := parse(flag.NewFlagSet("runs "+verb, flag.ContinueOnError), args, 1, "runs "+verb+" <run id>")
	if err != nil {
		return err
	}
	return e.get(ctx, path(pos[0]), cols)
}

func runsGet(ctx context.Context, e *env, args []string) error {
	return runOne(ctx, e, args, "get", func(id string) string { return "/api/v1/runs/" + esc(id) }, runCols)
}

func runsTasks(ctx context.Context, e *env, args []string) error {
	fs := flag.NewFlagSet("runs tasks", flag.ContinueOnError)
	all := fs.Bool("all", false, "include rows superseded by reruns")
	pos, err := parse(fs, args, 1, "runs tasks <run id> [--all]")
	if err != nil {
		return err
	}
	return e.list(ctx, fmt.Sprintf("/api/v1/runs/%s/tasks?include_superseded=%t", esc(pos[0]), *all), taskCols)
}

func runsEvents(ctx context.Context, e *env, args []string) error {
	pos, err := parse(flag.NewFlagSet("runs events", flag.ContinueOnError), args, 1, "runs events <run id>")
	if err != nil {
		return err
	}
	return e.list(ctx, "/api/v1/runs/"+esc(pos[0])+"/events", eventCols)
}

// runsLogs prints the log lines of every attempt of the run's tasks (or of one att_ attempt).
func runsLogs(ctx context.Context, e *env, args []string) error {
	pos, err := parse(flag.NewFlagSet("runs logs", flag.ContinueOnError), args, 1, "runs logs <run id|attempt id>")
	if err != nil {
		return err
	}
	attempts := []string{pos[0]}
	if !strings.HasPrefix(pos[0], "att_") {
		if attempts, err = e.attemptsOf(ctx, pos[0]); err != nil {
			return err
		}
	}
	var lines []any
	for _, a := range attempts {
		page, err := e.all(ctx, "/api/v1/attempts/"+esc(a)+"/logs", "after_id")
		if err != nil {
			return err
		}
		lines = append(lines, page["items"].([]any)...)
	}
	return e.out.print(map[string]any{"items": lines}, logCols...)
}

// ids returns the "id" of each item of a page from all.
func ids(page map[string]any) []string {
	items, _ := page["items"].([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if m, ok := it.(map[string]any); ok {
			if id, ok := m["id"].(string); ok {
				out = append(out, id)
			}
		}
	}
	return out
}

func (e *env) attemptsOf(ctx context.Context, runID string) ([]string, error) {
	tasks, err := e.all(ctx, "/api/v1/runs/"+esc(runID)+"/tasks?include_superseded=true", "cursor")
	if err != nil {
		return nil, err
	}
	var out []string
	for _, t := range ids(tasks) {
		atts, err := e.all(ctx, "/api/v1/tasks/"+esc(t)+"/attempts", "cursor")
		if err != nil {
			return nil, err
		}
		out = append(out, ids(atts)...)
	}
	return out, nil
}

func runsCancel(ctx context.Context, e *env, args []string) error {
	fs := flag.NewFlagSet("runs cancel", flag.ContinueOnError)
	force := fs.Bool("force", false, "force-cancel running attempts now")
	pos, err := parse(fs, args, 1, "runs cancel <run id> [--force]")
	if err != nil {
		return err
	}
	return e.send(ctx, "POST", fmt.Sprintf("/api/v1/runs/%s/cancel?force=%t", esc(pos[0]), *force), nil, runCols)
}

func runsControl(verb string) command {
	return func(ctx context.Context, e *env, args []string) error {
		pos, err := parse(flag.NewFlagSet("runs "+verb, flag.ContinueOnError), args, 1, "runs "+verb+" <run id>")
		if err != nil {
			return err
		}
		return e.send(ctx, "POST", "/api/v1/runs/"+esc(pos[0])+"/"+verb, nil, runCols)
	}
}

// listFlag collects a repeatable string flag.
type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(v string) error { *l = append(*l, v); return nil }

func runsRerun(ctx context.Context, e *env, args []string) error {
	fs := flag.NewFlagSet("runs rerun", flag.ContinueOnError)
	var tasks listFlag
	fs.Var(&tasks, "task", "task key to rerun (repeatable; default: every failed task)")
	reason := fs.String("reason", "", "why (required)")
	const u = "runs rerun <run id> --reason text [--task key ...]"
	pos, err := parse(fs, args, 1, u)
	if err != nil {
		return err
	}
	if *reason == "" {
		return usagef("usage: %s", u)
	}
	body := map[string]any{"reason": *reason}
	if len(tasks) > 0 {
		body["task_ids"] = []string(tasks)
	}
	return e.send(ctx, "POST", "/api/v1/runs/"+esc(pos[0])+"/rerun", body, []string{"run", "task_ids"})
}

func runsRetry(ctx context.Context, e *env, args []string) error {
	fs := flag.NewFlagSet("runs retry", flag.ContinueOnError)
	reason := fs.String("reason", "", "why")
	pos, err := parse(fs, args, 1, "runs retry <run id> [--reason text]")
	if err != nil {
		return err
	}
	var body any
	if *reason != "" {
		body = map[string]any{"reason": *reason}
	}
	return e.send(ctx, "POST", "/api/v1/runs/"+esc(pos[0])+"/retry", body, []string{"run", "task_ids"})
}

func workersList(ctx context.Context, e *env, args []string) error {
	if _, err := parse(flag.NewFlagSet("workers list", flag.ContinueOnError), args, 0, "workers list"); err != nil {
		return err
	}
	return e.list(ctx, "/api/v1/workers", workerCols)
}

func triggersCreate(ctx context.Context, e *env, args []string) error {
	fs := flag.NewFlagSet("triggers create", flag.ContinueOnError)
	name, wf, sched := fs.String("name", "", "trigger name"), fs.String("workflow", "", "workflow name"), fs.String("schedule", "", "5-field cron")
	tz, policy := fs.String("timezone", "", "IANA timezone (default UTC)"), fs.String("missed-policy", "", "skip or catch_up_one")
	version, input, disabled := fs.Int("version", 0, "pin a workflow version"), fs.String("input", "", "run input JSON file"), fs.Bool("disabled", false, "create disabled")
	const u = "triggers create --name n --workflow w --schedule '* * * * *' [--timezone tz] [--version n] [--input file] [--missed-policy p] [--disabled]"
	if _, err := parse(fs, args, 0, u); err != nil {
		return err
	}
	if *name == "" || *wf == "" || *sched == "" {
		return usagef("usage: %s", u)
	}
	body := map[string]any{"name": *name, "workflow_name": *wf, "schedule": *sched, "enabled": !*disabled}
	if *tz != "" {
		body["timezone"] = *tz
	}
	if *policy != "" {
		body["missed_policy"] = *policy
	}
	if *version > 0 {
		body["workflow_version"] = *version
	}
	if *input != "" {
		raw, err := os.ReadFile(*input)
		if err != nil {
			return fmt.Errorf("read %s: %w", *input, err)
		}
		body["input"] = json.RawMessage(raw)
	}
	return e.send(ctx, "POST", "/api/v1/triggers", body, triggerCols)
}

func triggersList(ctx context.Context, e *env, args []string) error {
	if _, err := parse(flag.NewFlagSet("triggers list", flag.ContinueOnError), args, 0, "triggers list"); err != nil {
		return err
	}
	return e.list(ctx, "/api/v1/triggers", triggerCols)
}

func triggersSetEnabled(on bool) command {
	verb := map[bool]string{true: "enable", false: "disable"}[on]
	return func(ctx context.Context, e *env, args []string) error {
		pos, err := parse(flag.NewFlagSet("triggers "+verb, flag.ContinueOnError), args, 1, "triggers "+verb+" <trigger id>")
		if err != nil {
			return err
		}
		return e.send(ctx, "PATCH", "/api/v1/triggers/"+esc(pos[0]), map[string]any{"enabled": on}, triggerCols)
	}
}

func apiKeysCreate(ctx context.Context, e *env, args []string) error {
	fs := flag.NewFlagSet("api-keys create", flag.ContinueOnError)
	name, role, expires := fs.String("name", "", "key name"), fs.String("role", "", "viewer|operator|admin|worker|ai-readonly"), fs.String("expires-at", "", "RFC 3339 expiry")
	const u = "api-keys create --name n --role r [--expires-at time]"
	if _, err := parse(fs, args, 0, u); err != nil {
		return err
	}
	if *name == "" || *role == "" {
		return usagef("usage: %s", u)
	}
	body := map[string]any{"name": *name, "role": *role}
	if *expires != "" {
		body["expires_at"] = *expires
	}
	return e.send(ctx, "POST", "/api/v1/api-keys", body, []string{"id", "name", "role", "key"})
}

func apiKeysList(ctx context.Context, e *env, args []string) error {
	if _, err := parse(flag.NewFlagSet("api-keys list", flag.ContinueOnError), args, 0, "api-keys list"); err != nil {
		return err
	}
	return e.list(ctx, "/api/v1/api-keys", keyCols)
}

func apiKeysRevoke(ctx context.Context, e *env, args []string) error {
	pos, err := parse(flag.NewFlagSet("api-keys revoke", flag.ContinueOnError), args, 1, "api-keys revoke <key id>")
	if err != nil {
		return err
	}
	if err := e.send(ctx, "DELETE", "/api/v1/api-keys/"+esc(pos[0]), nil, nil); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(e.stdout, "revoked", pos[0])
	return nil
}
