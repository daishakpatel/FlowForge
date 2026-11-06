#!/usr/bin/env bash
# Coverage gate (NFR-TEST-01, 00C §11): fails if a gated package tree is below COVER_MIN percent.
# Trees with no Go code yet are skipped with a notice, so the gate tightens as tasks add them.
# Usage: cover-check.sh [dir ...]   (defaults to the gated trees; COVER_MIN defaults to 70)
set -euo pipefail

cd "$(dirname "$0")/../.."
min="${COVER_MIN:-70}"
dirs=("$@")
if [[ ${#dirs[@]} -eq 0 ]]; then
  dirs=(internal/dag internal/workflow internal/scheduler internal/raftkv internal/stream)
fi

profile="$(mktemp)"
trap 'rm -f "$profile"' EXIT

status=0
for dir in "${dirs[@]}"; do
  if [[ -z "$(find "$dir" -name '*.go' -not -path '*/testdata/*' 2>/dev/null | head -n 1)" ]]; then
    echo "cover-check: skip $dir (no Go code yet)"
    continue
  fi
  go test -count=1 -coverprofile="$profile" "./$dir/..." >/dev/null
  pct="$(go tool cover -func="$profile" | awk '/^total:/ { sub("%", "", $3); print $3 }')"
  if awk -v p="$pct" -v m="$min" 'BEGIN { exit !(p + 0 < m + 0) }'; then
    echo "cover-check: FAIL $dir ${pct}% < ${min}%"
    status=1
  else
    echo "cover-check: ok   $dir ${pct}% >= ${min}%"
  fi
done
exit "$status"
