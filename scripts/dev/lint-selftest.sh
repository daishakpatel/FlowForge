#!/usr/bin/env bash
# Proves the boundary rules in .golangci.yml fire: lints a known-bad fixture in a scratch copy of the
# module and fails unless both depguard and forbidigo report it. Fixtures live under testdata/, which
# normal lint and `go test ./...` skip.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

cp "$root/go.mod" "$root/.golangci.yml" "$work/"
cp -R "$root/scripts/dev/testdata/lint-selftest/." "$work/"

if out="$(cd "$work" && golangci-lint run ./... 2>&1)"; then
  echo "lint-selftest: FAIL lint passed on a fixture that must violate the rules"
  exit 1
fi

status=0
for linter in depguard forbidigo; do
  if grep -q "($linter)" <<<"$out"; then
    echo "lint-selftest: ok   $linter fired"
  else
    echo "lint-selftest: FAIL $linter did not fire"
    status=1
  fi
done
for rule in 'time\.Now' 'time\.NewTimer' 'os\.Getenv'; do
  if grep -q "$rule.*(forbidigo)" <<<"$out"; then
    echo "lint-selftest: ok   forbidigo bans $rule"
  else
    echo "lint-selftest: FAIL forbidigo did not ban $rule"
    status=1
  fi
done
if [[ $status -ne 0 ]]; then
  printf '%s\n' "$out"
fi
exit "$status"
