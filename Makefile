# SPDX-License-Identifier: Apache-2.0
# Every target prints a one-line PASS/FAIL summary.

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

GO ?= go
# Release version shown in the acceptance report and the benchmark results.
RELEASE ?= v0.1.0
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildDate=$(BUILD_DATE)
COMPOSE := docker compose -f deploy/docker-compose.yml
IMAGE ?= thnhdatle/foreman:dev
FUZZTIME ?= 30s
ONLY ?=

export CGO_ENABLED ?= 0

run = scripts/step.sh

.PHONY: help bootstrap generate fmt lint test test-integration coverage mutation acceptance bench fuzz \
	examples quickstart-test security build docker up down ci release-check release-dry

help: ## list targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "%-18s %s\n", $$1, $$2}'

bootstrap: ## check tools, download modules, pull images, set hooks path
	@$(run) bootstrap scripts/bootstrap.sh

generate: ## code generation (none needed; kept for the standard target set)
	@$(run) generate $(GO) generate ./...

fmt: ## format all code
	@$(run) fmt bash -c '$(GO) tool gofumpt -w $$(git ls-files "*.go" | grep -v "^testdata/") && golangci-lint fmt ./...'

lint: ## golangci-lint, hadolint, shellcheck, gitleaks, yaml checks
	@$(run) lint scripts/lint.sh

test: ## unit tests with -race and goleak
	@$(run) test env CGO_ENABLED=1 $(GO) test -race -count=1 ./...

test-integration: ## integration tests (Testcontainers), -count=1
	@$(run) test-integration env CGO_ENABLED=1 $(GO) test -race -count=1 -tags integration -timeout 30m ./...

coverage: ## merged unit + integration coverage with thresholds (13.3)
	@$(run) coverage scripts/coverage.sh

mutation: ## mutation testing on the core packages (>= 60%)
	@$(run) mutation scripts/mutation.sh

acceptance: ## acceptance suite, writes acceptance/report.md; CI=1 for CI thresholds; ONLY=FMN-01,FMN-03
	@$(run) acceptance $(GO) run ./acceptance/cmd/runner -version "$(RELEASE)" -only "$(ONLY)"

bench: ## performance criteria + micro benchmarks; writes bench/RESULTS.md
	@$(run) bench env RELEASE=$(RELEASE) scripts/bench.sh

fuzz: ## each fuzz target for FUZZTIME
	@$(run) fuzz scripts/fuzz.sh $(FUZZTIME)

examples: ## run every example and check its expected output
	@$(run) examples scripts/examples.sh

quickstart-test: ## clone to a temp dir and run the README quickstart verbatim
	@$(run) quickstart-test scripts/quickstart-test.sh

security: ## govulncheck, go-licenses, trivy on the local image
	@$(run) security scripts/security.sh

build: ## binaries into ./bin with version ldflags
	@$(run) build $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/ ./cmd/foreman

docker: ## local image for the host architecture
	@$(run) docker docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) -t $(IMAGE) .

up: ## start the compose stack (Postgres 17 + Toxiproxy)
	@$(run) up $(COMPOSE) up -d --wait

down: ## stop the compose stack and remove volumes
	@$(run) down $(COMPOSE) down -v --remove-orphans

ci: ## lint test test-integration coverage examples security
	@$(MAKE) --no-print-directory lint test test-integration coverage examples security
	@echo "PASS ci"

release-check: ## pre-release checks (run before tagging)
	@$(run) release-check scripts/release-check.sh

# Tools fetched by bootstrap (syft for SBOMs) live in the project cache, not on the system.
TOOLS_DIR := $(HOME)/.cache/rayld/foreman/tools

release-dry: ## GoReleaser snapshot build (SBOMs need syft: on PATH or in $(TOOLS_DIR))
	@$(run) release-dry env PATH="$$PATH:$(TOOLS_DIR)" goreleaser release --snapshot --clean --skip=publish
