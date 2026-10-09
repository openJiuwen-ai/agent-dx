# Native build entrypoints. Supply cache locations explicitly in automation.
PYTHON ?= python3
CARGO ?= cargo
ADX_CARGO_CACHE_HELPER := $(CURDIR)/build/cache/cargo_cache.py
ADX_BUILD_CACHE_ROOT ?= $(shell $(PYTHON) $(ADX_CARGO_CACHE_HELPER) --repo $(CURDIR) cache-root)
ADX_CARGO_CACHE_KEY ?= $(shell $(PYTHON) $(ADX_CARGO_CACHE_HELPER) --repo $(CURDIR) cache-key)
ADX_SHARED_CARGO_TARGET := $(ADX_BUILD_CACHE_ROOT)/cargo-target/$(ADX_CARGO_CACHE_KEY)
CARGO_TARGET_DIR ?= $(ADX_SHARED_CARGO_TARGET)
CARGO_INCREMENTAL ?= 0
SCCACHE_DIR ?= $(ADX_BUILD_CACHE_ROOT)/sccache
SCCACHE_CACHE_SIZE ?= 20G
SCCACHE ?= $(shell command -v sccache 2>/dev/null)
export CARGO_TARGET_DIR
export CARGO_INCREMENTAL
export SCCACHE_DIR
export SCCACHE_CACHE_SIZE
ifneq ($(strip $(SCCACHE)),)
RUSTC_WRAPPER ?= $(SCCACHE)
export RUSTC_WRAPPER
endif
JOBS ?= 2
OUT ?= $(CURDIR)/out
PYTEST_ARGS ?=
E2E_PROFILE ?= standalone
K8S_E2E_PROFILE ?= k8s-basic
RUST_POLICY_PACKAGES := -p adx-apiserver -p adx-deployment -p adx-coordinator \
	-p adxlet -p adx-core -p adx-discovery -p adx-observability \
	-p adx-protocol -p adx-scheduling
ifneq ($(origin ADX_WITH_DFS),undefined)
$(error ADX_WITH_DFS was replaced by ADX_WITH_AFS; update the caller)
endif
ifneq ($(origin ADX_DFS_ALL_FEATURES),undefined)
$(error ADX_DFS_ALL_FEATURES was replaced by ADX_AFS_ALL_FEATURES; update the caller)
endif
ADX_WITH_AFS ?= 0
ADX_AFS_ALL_FEATURES ?= 0
AFS_PACKAGES := -p afs -p afs-client -p afs-error -p afs-logging \
	-p afs-metrics -p afs-protocol -p afs-tracing -p afs-transport
AFS_EXCLUDES := --exclude afs --exclude afs-client --exclude afs-error \
	--exclude afs-logging --exclude afs-metrics --exclude afs-protocol \
	--exclude afs-tracing --exclude afs-transport
ifeq ($(ADX_WITH_AFS),1)
else ifeq ($(ADX_WITH_AFS),0)
else
$(error ADX_WITH_AFS must be 0 or 1)
endif
ifeq ($(ADX_AFS_ALL_FEATURES),1)
else ifeq ($(ADX_AFS_ALL_FEATURES),0)
else
$(error ADX_AFS_ALL_FEATURES must be 0 or 1)
endif

.PHONY: help cargo-cache-info cargo-cache-env cargo-cache-isolated-env generate build test rust-check rust-test afs-check afs-build afs-lint afs-all-features-lint afs-test scheduler-bench python-test agent-test sandbox-sdk-test admin-test package ci data-plane-gateway data-plane-gateway-dev data-plane-gateway-ut
help:
	@echo 'generate | build | test | package | ci SUITE=<suite>; set ADX_WITH_AFS=1 for Agent FS (AFS); tests: rust-check rust-test scheduler-bench python-test; cache: cargo-cache-info cargo-cache-env cargo-cache-isolated-env'
cargo-cache-info:
	@$(PYTHON) $(ADX_CARGO_CACHE_HELPER) --repo $(CURDIR) status --target-dir '$(CARGO_TARGET_DIR)'
cargo-cache-env:
	@$(PYTHON) $(ADX_CARGO_CACHE_HELPER) --repo $(CURDIR) env --mode shared
cargo-cache-isolated-env:
	@$(PYTHON) $(ADX_CARGO_CACHE_HELPER) --repo $(CURDIR) env --mode isolated
generate:
	$(CARGO) check --locked -p adx-protocol -j $(JOBS)
build: generate
	$(CARGO) build --locked --workspace $(AFS_EXCLUDES) --all-features -j $(JOBS)
	@if [ "$(ADX_WITH_AFS)" = "1" ]; then \
		$(MAKE) afs-build JOBS=$(JOBS) PYTHON='$(PYTHON)' CARGO='$(CARGO)'; \
	fi
rust-check:
	$(CARGO) fmt --all -- --check
	$(CARGO) clippy --locked --workspace $(AFS_EXCLUDES) --all-features --all-targets -j $(JOBS) -- -D warnings
	$(CARGO) clippy --locked --no-deps --all-features --lib --bins -j $(JOBS) $(RUST_POLICY_PACKAGES) -- -D clippy::unwrap_used
	@if [ "$(ADX_WITH_AFS)" = "1" ]; then \
		$(MAKE) afs-lint JOBS=$(JOBS) PYTHON='$(PYTHON)' CARGO='$(CARGO)'; \
	fi
rust-test:
	$(CARGO) test --locked --workspace $(AFS_EXCLUDES) --all-features -j $(JOBS) -- --test-threads=$(JOBS)
	@if [ "$(ADX_WITH_AFS)" = "1" ]; then \
		$(MAKE) afs-test JOBS=$(JOBS) PYTHON='$(PYTHON)' CARGO='$(CARGO)'; \
	fi
afs-check: afs-build afs-lint afs-test
afs-build:
	$(CARGO) build --locked -p afs --bins -j $(JOBS)
afs-lint:
	$(CARGO) check --locked $(AFS_PACKAGES) --all-targets -j $(JOBS)
	$(CARGO) clippy --locked $(AFS_PACKAGES) --all-targets -j $(JOBS) -- -D warnings
	@if [ "$(ADX_AFS_ALL_FEATURES)" = "1" ]; then \
		$(MAKE) afs-all-features-lint JOBS=$(JOBS) PYTHON='$(PYTHON)' CARGO='$(CARGO)'; \
	else \
		echo 'Skipping AFS all-features/RDMA lint; set ADX_AFS_ALL_FEATURES=1 after libibverbs is prepared.'; \
	fi
afs-all-features-lint:
	@command -v pkg-config >/dev/null 2>&1 && pkg-config --exists libibverbs || { echo 'ADX_AFS_ALL_FEATURES=1 requires libibverbs development files' >&2; exit 2; }
	$(CARGO) clippy --locked $(AFS_PACKAGES) --all-features --all-targets -j $(JOBS) -- -D warnings
afs-test:
	$(CARGO) test --locked $(AFS_PACKAGES) --all-targets -j $(JOBS) -- --test-threads=$(JOBS)
scheduler-bench:
	$(CARGO) test --locked --release -p adx-coordinator --test benchmark -j $(JOBS) -- --ignored --nocapture
python-test: sandbox-sdk-test admin-test
agent-test:
	$(CARGO) test --locked -p adx-agent-core -p adx-agent-store -p adx-agent-api -p adx-activator -p adx-cli --features adx-agent-store/test-memory -j $(JOBS) -- --test-threads=$(JOBS)
	$(CARGO) test --locked -p data-plane-gateway --features agent-api --lib -j $(JOBS) -- --test-threads=$(JOBS)
sandbox-sdk-test:
	PYTHONPATH=platform/sdk/sandbox/python $(PYTHON) -m pytest -q -c platform/sdk/sandbox/pytest.ini platform/sdk/sandbox/python/tests $(PYTEST_ARGS)
admin-test:
	PYTHONPATH=tools/admin/src $(PYTHON) -m unittest discover -s tools/admin/tests -p 'test_*.py'
ci:
	$(PYTHON) build/ci/run.py $(SUITE) --jobs $(JOBS)
test: rust-check rust-test python-test
package:
	PYTHON='$(PYTHON)' bash platform/sdk/sandbox/python/build.sh '$(OUT)/wheels'
	PYTHON='$(PYTHON)' bash tools/admin/build.sh '$(OUT)/wheels'
data-plane-gateway:
	$(CARGO) build --locked -p data-plane-gateway --all-features --release -j $(JOBS)
data-plane-gateway-dev:
	$(CARGO) build --locked -p data-plane-gateway --all-features -j $(JOBS)
data-plane-gateway-ut:
	$(CARGO) test --locked -p data-plane-gateway --all-features -j $(JOBS) -- --test-threads=$(JOBS)

.PHONY: platform-release process-smoke
platform-release:
	@if [ "$(abspath $(CARGO_TARGET_DIR))" = "$(abspath $(ADX_SHARED_CARGO_TARGET))" ]; then \
		echo 'platform-release requires an isolated CARGO_TARGET_DIR; use cargo-cache-isolated-env or set an explicit release cache' >&2; \
		exit 2; \
	fi
	JOBS=$(JOBS) PYTHON=$(PYTHON) ADX_WITH_AFS=$(ADX_WITH_AFS) bash build/release/build.sh
process-smoke:
	$(PYTHON) build/ci/process_smoke.py --package "$(PACKAGE_DIR)" --output "$(EVIDENCE_DIR)"

.PHONY: platform-e2e
platform-e2e:
	$(PYTHON) build/e2e/run.py --bundle "$(BUNDLE_DIR)" --output "$(EVIDENCE_DIR)" --profile "$(E2E_PROFILE)"

.PHONY: platform-k8s-e2e
platform-k8s-e2e:
	$(PYTHON) build/e2e/kubernetes/run.py --bundle "$(BUNDLE_DIR)/bundle.json" --registry-images "$(BUNDLE_DIR)/registry-images.json" --kubeconfig "$(KUBECONFIG)" --output "$(EVIDENCE_DIR)" --profile "$(K8S_E2E_PROFILE)"
