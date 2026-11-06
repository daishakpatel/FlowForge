# Development

How to set up a machine to build and test FlowForge. Exact versions live in [`decisions/versions.md`](decisions/versions.md); later tasks extend this page (01.02 adds the local Compose environment).

## Toolchain

| Tool | Needed from | Version | Install (macOS) |
|---|---|---|---|
| Go | 01.01 | as in `go.mod` | `brew install go` |
| golangci-lint | 01.01 | as in `docs/decisions/versions.md` | `brew install golangci-lint` |
| Docker | 01.02 | recent Docker Desktop or Engine | Docker Desktop |
| sqlc | 02.01 | recorded by 02.01 | `brew install sqlc` |
| buf | 03.01 / 04.01 | recorded by 03.01 | `brew install bufbuild/buf/buf` |
| Python | 06.01 | ≥ 3.12 | `brew install python` |
| Node.js | 07.01 | ≥ 20 | `brew install node` |
| actionlint, shellcheck | optional | any recent | `brew install actionlint shellcheck` |

## Everyday commands

```bash
make help          # list targets
make lint test     # what CI runs first
make test-race     # unit tests with the race detector
make lint-selftest # prove the package-boundary lint rules still fire
make cover-check   # 70 % coverage gate on the gated packages that exist
```
