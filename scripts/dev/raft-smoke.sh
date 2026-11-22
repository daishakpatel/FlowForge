#!/usr/bin/env bash
# Compose smoke test of the 3-node Raft cluster (task 04.06). Starts the cluster (make raft-up), runs
# the compose-tagged Go test against it (one leader, Put/Get, leader kill and re-election within
# 5 s, transfer-leader, equal state hashes), then checks the kill/restart Make helpers. The cluster
# is left running; make raft-down stops it. Needs Docker.
set -euo pipefail
cd "$(dirname "$0")/../.."

make raft-up
go test -count=1 -tags=compose -v ./test/integration/raft/

# The kill/restart helpers: a killed node comes back healthy.
make raft-kill N=3
make raft-restart N=3
make raft-status >/dev/null
echo "raft-smoke: ok"
