# Native build entrypoints. Supply cache locations explicitly in automation.
PYTHON ?= python3
CARGO ?= cargo
GO ?= go
CARGO_TARGET_DIR ?= $(CURDIR)/target
export CARGO_TARGET_DIR
JOBS ?= 2
OUT ?= $(CURDIR)/out

.PHONY: help generate build test rust-test python-test go-test package data-plane-gateway data-plane-gateway-dev data-plane-gateway-ut
help:
	@echo 'generate | build | test | package; component tests: rust-test python-test go-test'
generate:
	bash build/codegen/go.sh
build: generate
	$(CARGO) build --locked --workspace --all-features -j $(JOBS)
	cd platform/control-plane/sandbox-api && $(GO) build ./...
rust-test:
	$(CARGO) test --locked --workspace --all-features -j $(JOBS) -- --test-threads=$(JOBS)
python-test:
	$(PYTHON) -m pytest -q
	PYTHONPATH=platform/sdk/sandbox/python $(PYTHON) -m pytest -q -c platform/sdk/sandbox/pytest.ini platform/sdk/sandbox/python/tests/unit
go-test: generate
	cd platform/control-plane/sandbox-api && $(GO) test -p $(JOBS) -gcflags=all=-l ./internal/sandbox ./internal/legacy/frontend/sandboxrouter/...
test: rust-test python-test go-test
package:
	bash build.sh -p '$(PYTHON)' -o '$(OUT)/wheels'
	PYTHON='$(PYTHON)' bash platform/sdk/sandbox/python/build.sh '$(OUT)/wheels'
data-plane-gateway:
	$(CARGO) build --locked -p data-plane-gateway --all-features --release -j $(JOBS)
data-plane-gateway-dev:
	$(CARGO) build --locked -p data-plane-gateway --all-features -j $(JOBS)
data-plane-gateway-ut:
	$(CARGO) test --locked -p data-plane-gateway --all-features -j $(JOBS) -- --test-threads=$(JOBS)
