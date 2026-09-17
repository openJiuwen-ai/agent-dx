# Native build entrypoints. Supply cache locations explicitly in automation.
PYTHON ?= python3
CARGO ?= cargo
CARGO_TARGET_DIR ?= $(CURDIR)/target
export CARGO_TARGET_DIR
JOBS ?= 2
OUT ?= $(CURDIR)/out
PYTEST_ARGS ?=

.PHONY: help generate build test rust-test scheduler-bench python-test agent-test sandbox-sdk-test package ci data-plane-gateway data-plane-gateway-dev data-plane-gateway-ut
help:
	@echo 'generate | build | test | package | ci SUITE=<suite>; tests: rust-test scheduler-bench python-test'
generate:
	$(CARGO) check --locked -p adx-protocol -j $(JOBS)
build: generate
	$(CARGO) build --locked --workspace --all-features -j $(JOBS)
rust-test:
	$(CARGO) test --locked --workspace --all-features -j $(JOBS) -- --test-threads=$(JOBS)
scheduler-bench:
	$(CARGO) test --locked --release -p adx-master --test benchmark -j $(JOBS) -- --ignored --nocapture
python-test: agent-test sandbox-sdk-test
agent-test:
	$(PYTHON) -m pytest -q $(PYTEST_ARGS)
sandbox-sdk-test:
	PYTHONPATH=platform/sdk/sandbox/python $(PYTHON) -m pytest -q -c platform/sdk/sandbox/pytest.ini platform/sdk/sandbox/python/tests $(PYTEST_ARGS)
ci:
	$(PYTHON) build/ci/run.py $(SUITE) --jobs $(JOBS)
test: rust-test python-test
package:
	bash build.sh -p '$(PYTHON)' -o '$(OUT)/wheels'
	PYTHON='$(PYTHON)' bash platform/sdk/sandbox/python/build.sh '$(OUT)/wheels'
data-plane-gateway:
	$(CARGO) build --locked -p data-plane-gateway --all-features --release -j $(JOBS)
data-plane-gateway-dev:
	$(CARGO) build --locked -p data-plane-gateway --all-features -j $(JOBS)
data-plane-gateway-ut:
	$(CARGO) test --locked -p data-plane-gateway --all-features -j $(JOBS) -- --test-threads=$(JOBS)

.PHONY: platform-release process-smoke
platform-release:
	JOBS=$(JOBS) PYTHON=$(PYTHON) bash build/release/build.sh
process-smoke:
	$(PYTHON) build/ci/process_smoke.py --package "$(PACKAGE_DIR)" --output "$(EVIDENCE_DIR)"

.PHONY: platform-e2e
platform-e2e:
	$(PYTHON) build/e2e/run.py --bundle "$(BUNDLE_DIR)" --output "$(EVIDENCE_DIR)"

.PHONY: platform-k8s-e2e
platform-k8s-e2e:
	$(PYTHON) build/e2e/kubernetes/run.py --bundle "$(BUNDLE_DIR)/bundle.json" --registry-images "$(BUNDLE_DIR)/registry-images.json" --kubeconfig "$(KUBECONFIG)" --output "$(EVIDENCE_DIR)"
