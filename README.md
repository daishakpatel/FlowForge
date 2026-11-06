# FlowForge

A distributed workflow runtime and AI operations platform: a Go control plane with Postgres state and Raft-backed coordination, a Kafka and TimescaleDB market-data pipeline, a Python AI copilot with read-only tools, and a React dashboard. It is being built task by task from the blueprint below.

## Quick start

```bash
git clone https://github.com/daishakpatel/FlowForge.git && cd FlowForge
make lint test          # needs Go and golangci-lint
make dev-up             # local Postgres (flowforge, aiops) and TimescaleDB (marketdata); needs Docker
make test-integration   # integration tests against real databases
```

Toolchain requirements, ports, profiles and troubleshooting are in [`docs/development.md`](docs/development.md); `make help` lists every target. Licensed under MIT.

## Implementation blueprint

Agent-ready build plan: 56 numbered implementation tasks in `implementation/`. To run a task with an agent, see [`prompt.md`](prompt.md).

### Start here
1. Read `implementation/00_MASTER_PROJECT_SPEC.md`, then `00A_ARCHITECTURE_DECISIONS.md`, `00E_CONTRACTS_REFERENCE.md`, `00C_AGENT_OPERATING_RULES.md`.
2. Use `implementation/00B_IMPLEMENTATION_INDEX.md` to navigate (dependency graph, critical path, progress checklist).
3. Hand an AI coding agent `implementation/phase-01-foundation/01.01_*.md` and follow each task's Handoff Instructions.

### Contents
| File | Purpose |
|---|---|
| `original/` | the source specification v1.0 |
| `implementation/00_MASTER_PROJECT_SPEC.md` | audited, updated master spec (tags: existing / recommended addition / optional) |
| `implementation/00A_ARCHITECTURE_DECISIONS.md` | ADRs |
| `implementation/00B_IMPLEMENTATION_INDEX.md` | roadmap, dependency graph, parallel groups, checklists |
| `implementation/00C_AGENT_OPERATING_RULES.md` | rules for implementation agents |
| `implementation/00D_AUDIT_REPORT.md` | audit findings and resolutions |
| `implementation/00E_CONTRACTS_REFERENCE.md` | DDL, state machines, APIs, topics, env vars |
| `implementation/00F_COMPLETION_REPORT_TEMPLATE.md` | per-task completion report |
| `implementation/00G_QUALITY_CONTROL_REPORT.md` | blueprint QC results |
| `implementation/phase-*/` | numbered task files |
