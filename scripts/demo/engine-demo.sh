#!/usr/bin/env bash
# engine-demo.sh — show the workflow engine end to end with flowforgectl (task 03.09).
#
# Needs a running API, scheduler and at least one worker on queue "default" (see
# docs/testing/e2e.md, "Running the demo"), and:
#   FF_API_URL  the API, e.g. http://localhost:8080 (the default)
#   FF_API_KEY  an operator or admin key
# It registers the diamond and market-data-skeleton fixtures (a new version each time), shows
# that a cyclic definition is rejected, starts a run of each, follows one live over SSE, and
# prints the tasks.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fixtures="$root/test/fixtures/workflows"
ctl="${FLOWFORGECTL:-$root/bin/flowforgectl}"
if [[ ! -x "$ctl" ]]; then
  echo "building flowforgectl into $root/bin"
  (cd "$root" && go build -o bin/flowforgectl ./cmd/flowforgectl)
fi
: "${FF_API_KEY:?set FF_API_KEY to an operator or admin API key}"

step() { printf '\n== %s\n' "$*"; }

step "register workflows"
"$ctl" workflows create -f "$fixtures/diamond.json"
"$ctl" workflows create -f "$fixtures/market-data-skeleton.json"

step "a cyclic definition is rejected (non-zero exit, CYCLE in the error envelope)"
if "$ctl" workflows create -f "$fixtures/cyclic.json"; then
  echo "unexpected: the cyclic definition was accepted" >&2
  exit 1
fi

step "start a diamond run and watch it over SSE until it ends"
run_id="$("$ctl" -o json runs start diamond --idempotency-key "demo-$(date +%s)" | sed -n 's/^  "id": "\(run_[^"]*\)".*/\1/p')"
echo "run $run_id"
"$ctl" runs watch "$run_id"

step "its tasks"
"$ctl" runs tasks "$run_id"

step "a market-data-skeleton run with input"
input="$(mktemp)"
trap 'rm -f "$input"' EXIT
echo '{"symbols": ["AAPL", "MSFT"]}' >"$input"
md_id="$("$ctl" -o json runs start market-data-skeleton --input "$input" | sed -n 's/^  "id": "\(run_[^"]*\)".*/\1/p')"
"$ctl" runs watch "$md_id" >/dev/null
"$ctl" runs get "$md_id"
"$ctl" runs tasks "$md_id"

step "workers"
"$ctl" workers list
