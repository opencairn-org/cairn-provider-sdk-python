# Task-runner contract (https://fleetlint.org/baseline/#task-runner-contract). `make check` is the definition of green.
SHELL := bash
.SHELLFLAGS := -euo pipefail -c
.ONESHELL:
.DEFAULT_GOAL := check-fast
MAIN_BRANCH ?= main
# Python tools (ruff, mypy, pytest, pip-audit, griffe, ...) are pinned in uv.lock; the coverage floor is in pyproject.toml.
UV          := uv run --locked
PACKAGE     := cairn_provider_sdk
# Licenses allowed for runtime dependencies; substring match.
LICENSES    ?= MIT;BSD;Apache;ISC;PSF;Python Software Foundation;Mozilla Public License 2.0
# Each pin carries a `# renovate:` comment so Renovate bumps it (customManagers in renovate.json5).
# renovate: datasource=github-releases depName=gitleaks/gitleaks
GITLEAKS_VERSION  ?= v8.30.1
# renovate: datasource=go depName=github.com/fleetlint/fleetlint
FLEETLINT_VERSION ?= v0.10.0
# Mutation score floor for the SDK (mutmut; see [tool.mutmut] in pyproject.toml).
MUTATION_MIN ?= 60
# nats-server for the integration tests; go install verifies it against sum.golang.org.
# renovate: datasource=go depName=github.com/nats-io/nats-server/v2
NATS_SERVER_VERSION ?= v2.15.1
# Previous release for the API compatibility check; empty on the first release.
PREV_TAG := $(shell git describe --tags --abbrev=0 --match 'v*' HEAD^ 2>/dev/null || git describe --tags --abbrev=0 --match 'v*' 2>/dev/null || true)

.PHONY: help tools fmt lint test cover audit api-check bench mutate build dist check-fast check release clean proto

help: ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*##' '{printf "  %-12s %s\n", $$1, $$2}'

tools: ## install the locked Python toolchain, fleetlint and nats-server (gitleaks GITLEAKS_VERSION comes from its release page)
	uv sync --locked
	go install github.com/fleetlint/fleetlint/cmd/fleetlint@$(FLEETLINT_VERSION)
	go install github.com/nats-io/nats-server/v2@$(NATS_SERVER_VERSION)

fmt: ## format and autofix in place
	$(UV) ruff format
	$(UV) ruff check --fix

lint: ## format check + ruff + mypy strict + duplication (pylint duplicate-code only), warnings as errors
	$(UV) ruff format --check
	$(UV) ruff check
	$(UV) mypy
	$(UV) pylint $(PACKAGE) tests examples

test: ## all tests (the integration tests need nats-server: `make tools`)
	$(UV) pytest

cover: ## tests with coverage; fails below fail_under in pyproject.toml
	$(UV) pytest --cov --cov-report=term

audit: ## vulnerabilities, licenses, unused/missing deps, lockfile, secrets
	uv lock --check
	uv export --locked --all-groups --no-emit-project --format requirements-txt -q \
	  | $(UV) pip-audit --strict --disable-pip --require-hashes -r /dev/stdin
	$(UV) pip-licenses --partial-match --allow-only="$(LICENSES)" \
	  --packages $$(uv export --locked --no-dev --no-emit-project --no-hashes --no-header --no-annotate -q | sed -E 's/[=;].*//')
	$(UV) deptry .
	gitleaks git --no-banner --redact .

api-check: ## public API against the previous release (griffe); a breaking change needs a new major
	@if [ -z "$(PREV_TAG)" ]; then echo "no previous v* tag: nothing to compare"; exit 0; fi
	$(UV) griffe check $(PACKAGE) --against $(PREV_TAG) --search . --verbose

bench: ## benchmarks (none yet; not part of check)
	@echo "no benchmarks"

mutate: ## mutation testing of the SDK (weekly in CI, not part of check); fails below MUTATION_MIN % killed
	rm -rf mutants
	$(UV) mutmut run
	$(UV) mutmut export-cicd-stats
	$(UV) python -c 'import json; s = json.load(open("mutants/mutmut-cicd-stats.json")); t = s["total"] - s["skipped"]; k = s["killed"] + s["timeout"]; p = 100 * k / t if t else 0; print(f"mutation score {p:.1f}% (minimum $(MUTATION_MIN)%)"); raise SystemExit(p < $(MUTATION_MIN))'

build: ## byte-compile, build the wheel and sdist, and smoke-test the wheel in a clean venv
	$(UV) python -m compileall -q $(PACKAGE) cairn examples
	rm -rf dist
	# Commit time as the archive timestamp so a rebuild of a tag matches.
	SOURCE_DATE_EPOCH=$$(git log -1 --format=%ct 2>/dev/null || date +%s) uv build
	rm -rf .smoke && uv venv -q .smoke && uv pip install -q --python .smoke/bin/python dist/*.whl
	.smoke/bin/python -c "import cairn_provider_sdk, cairn_provider_sdk.testing; from cairn.worker.v1 import worker_service_pb2; print(cairn_provider_sdk.Worker, worker_service_pb2.JobResult.DESCRIPTOR.full_name)"
	rm -rf .smoke

dist: build ## the release artifacts are dist/*.whl and dist/*.tar.gz; the release workflow attaches them

proto: ## regenerate cairn/ from ../cairn-core/api/proto (CORE= to point elsewhere)
	scripts/sync-proto.sh

check-fast: ## before every commit (< 30 s)
	$(UV) ruff format --check
	$(UV) ruff check
	$(UV) pytest -x -q --deselect tests/test_integration.py --deselect examples/minimal/test_acme_worker.py::test_acme_worker_end_to_end

check: lint test cover audit api-check build ## definition of green (< 10 min)
	fleetlint check --fail-on error

release: ## refuse dirty/non-main -> check -> signed tag V (version and CHANGELOG.md section must match); pushing the tag publishes the release
	@test -n "$(V)" || { echo "usage: make release V=vX.Y.Z"; exit 1; }
	@test -z "$$(git status --porcelain)" || { echo "dirty tree"; exit 1; }
	@test "$$(git branch --show-current)" = "$(MAIN_BRANCH)" || { echo "not on $(MAIN_BRANCH)"; exit 1; }
	@grep -q '^version = "$(V:v%=%)"' pyproject.toml || { echo "pyproject.toml version is not $(V:v%=%)"; exit 1; }
	@grep -q '^## \[$(V:v%=%)\]' CHANGELOG.md || { echo "CHANGELOG.md has no '## [$(V:v%=%)]' section"; exit 1; }
	$(MAKE) check
	git tag -s "$(V)" -m "$(V)"
	@echo "tagged $(V) — push with: git push origin $(V)"

clean: ## remove caches, build and coverage output
	rm -rf .pytest_cache .ruff_cache .mypy_cache .coverage mutants dist .smoke
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
