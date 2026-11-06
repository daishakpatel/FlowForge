SHELL := /bin/bash
.DEFAULT_GOAL := help

MODULE                := github.com/daishakpatel/flowforge
GOLANGCI_LINT_VERSION := 2.15.0
VERSION               ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT                ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LDFLAGS               := -X $(MODULE)/internal/version.version=$(VERSION) -X $(MODULE)/internal/version.commit=$(COMMIT)
COVER_PROFILE         := coverage.out
COMPOSE               := docker compose --profile core --profile streaming

.PHONY: help build test test-race test-integration lint lint-selftest fmt tidy cover cover-check proto sqlc openapi-check clean dev-up dev-down dev-reset psql psql-md logs

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-18s %s\n", $$1, $$2}'

build: ## Compile everything; binaries under cmd/ go to bin/
	go build ./...
	@if compgen -G "cmd/*/main.go" >/dev/null; then mkdir -p bin && go build -ldflags "$(LDFLAGS)" -o bin/ ./cmd/...; else echo "build: no binaries under cmd/ yet"; fi

test: ## Unit tests
	go test -count=1 ./...

test-race: ## Unit tests with the race detector
	go test -race -count=1 ./...

test-integration: ## Integration tests (build tag `integration`; needs Docker or FF_TEST_*_DSN)
	go test -race -count=1 -timeout=10m -tags=integration ./...

lint: ## golangci-lint (formatters + linters, incl. boundary rules)
	@golangci-lint --version | grep -q " $(GOLANGCI_LINT_VERSION) " || echo "lint: warning: expected golangci-lint $(GOLANGCI_LINT_VERSION) (see docs/decisions/versions.md)"
	golangci-lint run --build-tags=integration ./...

lint-selftest: ## Prove the depguard and forbidigo boundary rules fire
	scripts/dev/lint-selftest.sh

fmt: ## Format code (gofmt + goimports)
	golangci-lint fmt ./...

tidy: ## go mod tidy
	go mod tidy

cover: ## Coverage profile and per-function summary
	go test -count=1 -coverprofile=$(COVER_PROFILE) ./...
	go tool cover -func=$(COVER_PROFILE) | tail -n 1

cover-check: ## Fail if a gated package tree is below 70% coverage (NFR-TEST-01)
	scripts/dev/cover-check.sh

proto: ## Generate protobuf code (placeholder until 03.01/04.01)
	@echo "proto: not yet configured (task 03.01 / 04.01)"

sqlc: ## Generate sqlc code (placeholder until 02.01)
	@echo "sqlc: not yet configured (task 02.01)"

openapi-check: ## Check OpenAPI drift (placeholder until 02.04)
	@echo "openapi-check: not yet configured (task 02.04)"

clean: ## Remove build and coverage output
	rm -rf bin $(COVER_PROFILE)

dev-up: ## Start postgres + timescaledb (profiles core, streaming) and wait until healthy
	$(COMPOSE) up -d --wait

dev-down: ## Stop the dev databases (data is kept)
	$(COMPOSE) down

dev-reset: ## Delete the dev databases' volumes and start them fresh (re-runs init SQL)
	$(COMPOSE) down -v
	$(COMPOSE) up -d --wait

psql: ## psql into flowforge as the app role
	$(COMPOSE) exec postgres psql -U flowforge -d flowforge

psql-md: ## psql into marketdata
	$(COMPOSE) exec timescaledb psql -U md -d marketdata

logs: ## Follow the dev stack's logs
	$(COMPOSE) logs -f
