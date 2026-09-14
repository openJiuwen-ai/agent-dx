# Agent DX

Monorepo for Agent Distributed Executor, the Instance execution platform, and shared gateways.

| Directory | Imported code |
|---|---|
| `agent/cli` | Python `adx` CLI |
| `agent/sdk/python` | Agent programming SDK |
| `agent/executor` | In-instance Agent Executor |
| `agent/tests` | Existing Agent unit/integration tests and shared fixtures |
| `gateway` | Rust Edge, Node Proxy, and forwarder |
| `platform/runtime/rrt` | RRT daemon and runtime adapter |
| `platform/sdk/sandbox/python` | Public Sandbox SDK |
| `platform/control-plane/sandbox-api` | Go Sandbox HTTP handlers and their required compatibility dependencies |
| `platform/api/proto/legacy` | Imported wire contracts used by the current implementations |

The Agent product uses the public Sandbox SDK in the target architecture. This first migration preserves existing behavior; the Agent FaaS backend, Gateway etcd/IAM integration, and RRT RuntimeRPC remain to be replaced during the control-plane refactor. Rust Master, Node Manager, and the process supervisor are planned components and have not been scaffolded.

## Build and test

Rust uses the root Cargo workspace. The Sandbox SDK uses distribution `adx-sandbox`, import `adx_sandbox`, CLI `adx-sandbox`, and `ADX_*` environment settings. Agent namespaces are `adx.agentruntime` and `adx.agentexecutor`; Gateway binaries use `adx-`, configuration uses `ADX_`, and internal branded headers use `X-ADX-`. Run matching component versions together.

```sh
cargo test --locked --workspace --all-features -j 2
python -m pytest -q
PYTHONPATH=platform/sdk/sandbox/python python -m pytest -q -c platform/sdk/sandbox/pytest.ini platform/sdk/sandbox/python/tests/unit

# Go 1.24.1+ and protoc with protoc-gen-go / protoc-gen-go-grpc on PATH
bash build/codegen/go.sh
cd platform/control-plane/sandbox-api
go build ./...
go test -p 2 -gcflags=all=-l ./internal/sandbox ./internal/legacy/frontend/sandboxrouter/...
```

`make help` lists the combined entrypoints. Install Python test/build requirements in a virtual environment (`pytest`, `pytest-asyncio`, `setuptools`, `wheel`, `build`, and each package's dependencies). `make package PYTHON=/path/to/venv/bin/python` produces the four Python distributions under `out/wheels`. `BUILD_VERSION` can set release versions explicitly. The Sandbox SDK has its own `VERSION`; it does not derive its version from Agent repository tags.

Set `CARGO_TARGET_DIR`, `GOCACHE`, and `GOMODCACHE` to persistent caches in automation. `build/` contains source scripts; temporary build outputs belong in `out/` or a cache directory. `bash build.sh -C` cleans package artifacts and preserves the source scripts.

The Go API is currently a handler module with `RegisterRoutes`, awaiting the new backend/bootstrap integration. It is not yet an independently runnable replacement control plane. Source import is separate from end-to-end deployment validation.

See [migration status](docs/migration/2026-09-14-import.md), [source pins](docs/migration/sources.json), [architecture](docs/architecture/repository-layout.md), and [Agent usage](agent/README.md).
