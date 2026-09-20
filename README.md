# Agent DX

Monorepo for Agent Distributed Executor, the Instance execution platform, and shared gateways.

| Directory | Responsibility |
|---|---|
| `agent/cli` | Python `adx` CLI |
| `agent/sdk/python` | Agent programming SDK |
| `agent/executor` | In-instance Agent Executor |
| `agent/tests` | Existing Agent unit/integration tests and shared fixtures |
| `gateway` | Rust Edge, Node Proxy, and forwarder |
| `platform/runtime/rrt` | RRT daemon and runtime adapter |
| `platform/sdk/sandbox/python` | Public Sandbox SDK |
| `platform/control-plane/master` | Rust Master, Shard scheduling, Redis and route/snapshot catalogs |
| `platform/control-plane/node-manager` | Instance lifecycle, sandboxd, checkpoints and outage journal |
| `platform/control-plane/control-cli` | Rust adxctl and process supervisor |
| `platform/control-plane/api-server` | Rust public HTTP API, authentication, ownership cache and direct Instance RPC |
| `platform/api/proto` | Instance, snapshot, credentials, routes and node-local protocol definitions |

The Agent product targets the public Sandbox SDK; its current CLI/SDK/Executor still require the legacy Agent backend. The new platform package alone does not run that Agent business flow. The Rust API Server rewrite has passed [Kubernetes acceptance](docs/testing/2026-09-17-rust-api-server-k8s.md); see [migration steps](docs/testing/rust-api-server.md). Rust Master and Node Manager expose authenticated service processes, Redis persistence/discovery, scheduling and node lifecycle recovery; the Rust API Server connects to those services. Edge subscribes to committed routes over gRPC, while Node Proxy requires a complete local binding synchronization before admission. See [implementation status](docs/testing/control-plane-implementation.md) and [route publication](docs/testing/route-publication.md). The Rust process supervisor and unified package are implemented; see [process deployment](docs/testing/process-deployment.md). Local public-SDK acceptance now covers real Firecracker pause/resume, S3 recovery and node lifecycle failures. See the [stage roadmap](docs/testing/control-plane-roadmap.md) for completed gates and remaining work.

![Current component architecture](docs/architecture/current-architecture.svg)

See [current layout](docs/architecture/repository-layout.md) and [public API support](platform/control-plane/api-server/docs/sandbox-lifecycle-api.md). Client options such as network policy, entrypoint inheritance and reload are not all supported by the new backend.

## Build and test

Local component and Socket checks use `python3 build/ci/run.py <suite>`. Buildkite has separate release, image and Kubernetes public-SDK steps. [Buildkite #30](docs/testing/2026-09-18-runtime-environment-k8s.md) passed all eight basic K8s groups, including local-first creation, node failure, restart, resource metrics, logs and traces. Its Kubernetes profile uses the immutable OCI runtime image; standalone deployment retains the local EROFS path. Checkpoint/snapshot/cross-node recovery use local Firecracker acceptance; the K8s FC profile is deferred. See [local checks and end-to-end acceptance](docs/testing/control-plane-ci.md) for prerequisites and implementation milestones.

### Kubernetes E2E target requirements

The current `full` profile requires two distinct schedulable Linux workers. Each
worker must be able to host one ADX Pod with the following combined platform and
Collector resources:

| Target-cluster resource | Enforced minimum | Recommended worker |
|---|---:|---:|
| Worker count | 2 distinct physical workers | 2 workers in separate failure domains |
| CPU per worker | 2.1 requested; up to 4 limited | 4 vCPU |
| Memory per worker | 2 GiB + 128 MiB requested; up to 4 GiB + 256 MiB limited | 8 GiB |
| Ephemeral storage | state/evidence plus a 1 GiB memory-backed image directory | at least 30 GiB free disk |
| Architecture | `linux/amd64` or `linux/arm64`, matching every artifact | `linux/amd64` for the current Buildkite pipeline |

The base Kubernetes profile uses OCI images and does not require EROFS. It does
require privileged Pods, functional cgroup v1 or v2, cross-worker Pod/Service
networking, `br_netfilter`, and
`net.bridge.bridge-nf-call-iptables=1`. The repository does not enforce a numeric
host-kernel or Kubernetes-version floor; use a maintained distribution and a
5.10/5.15-or-newer LTS kernel as the deployment baseline. Capability preflight,
not `uname`, is the current gate.

The target kubeconfig must be able to create/delete the isolated namespace and
manage Pods, Services and Secrets, including exec, copy and diagnostics. The
workers must pull the digest-pinned Node, RRT and Collector images. `full` checks
the actual Pod-to-worker placement after scheduling and fails if both Pods land
on one worker.

Firecracker is a separate conditional profile. It needs one explicitly selected
KVM worker with a 4 CPU / 6 GiB Pod allocation, `/dev/kvm`, KVM API version 12,
privileged host-device access and an architecture-matched runtime kit. Running
the base and Firecracker profiles concurrently is best served by two 4C/8G
workers plus one 8C/16G KVM worker. GPU/NPU, Redis-PV recovery and network-partition
profiles remain planned and are not implied by a green base `full` result.

Detailed resources, kernel checks, RBAC/network requirements and CI-worker
resources are documented in the [Kubernetes E2E README](build/e2e/kubernetes/README.md).

Rust uses the root Cargo workspace. The Sandbox SDK uses distribution `adx-sandbox`, import `adx_sandbox`, CLI `adx-sandbox`, and `ADX_*` environment settings. Agent namespaces are `adx.agentruntime` and `adx.agentexecutor`; Gateway binaries use `adx-`, configuration uses `ADX_`, and internal branded headers use `X-ADX-`. Run matching component versions together.

```sh
cargo test --locked --workspace --all-features -j 2
python -m pytest -q
PYTHONPATH=platform/sdk/sandbox/python python -m pytest -q -c platform/sdk/sandbox/pytest.ini platform/sdk/sandbox/python/tests

cargo test --locked -p adx-api-server -j 2
```

`make help` lists the combined entrypoints. Install Python test/build requirements in a virtual environment (`pytest`, `pytest-asyncio`, `setuptools`, `wheel`, `build`, and each package's dependencies). `make package PYTHON=/path/to/venv/bin/python` produces the four Python distributions under `out/wheels`. `BUILD_VERSION` can set release versions explicitly. The Sandbox SDK has its own `VERSION`; it does not derive its version from Agent repository tags.

Set `CARGO_TARGET_DIR` to a persistent cache in automation. Building the external sandboxd dependency additionally uses Go caches. `build/` contains source scripts; temporary build outputs belong in `out/` or a cache directory. `bash build.sh -C` cleans package artifacts and preserves the source scripts.

The Rust API Server has an `adx-api-server` process entrypoint and configuration under `build/config/examples/`. Component integration evidence is separate from complete platform deployment validation.

See [migration status](docs/migration/2026-09-14-import.md), [source pins](docs/migration/sources.json), [architecture](docs/architecture/repository-layout.md), and [Agent usage](agent/README.md).


## Instance lifecycle and deployment

- [Single-host installation, certificates and CLI](docs/deployment/standalone.md)
- [EROFS/OCI runtime environment and custom-image bootstrap](docs/deployment/runtime-environment.md)
- [CLI and unified process deployment](docs/testing/process-deployment.md)
- [Node lifecycle, resource collection and SQLite outage contract](docs/testing/node-lifecycle.md)
- [Checkpoint storage, S3 and snapshot catalog](docs/testing/snapshot-storage.md)
- [Node Manager / Node Proxy process modes](docs/testing/node-proxy-process-modes.md)
- [Current scheduling baseline recheck](docs/testing/2026-09-16-scheduling-recheck.md)
- [Instance inventory and resource metrics](docs/testing/instance-resource-metrics.md)
- [Component log rotation and compression](docs/testing/log-rotation.md)
- [Live development progress](docs/testing/live-progress.md)

Reusable snapshot creation, cloning into new Instances, catalog queries and deferred artifact deletion are wired through the control plane. The recorded package-v16 Firecracker/MinIO run passed 17 scenarios, including independent clone identities, process memory, writable files and retired-session orphan cleanup after authoritative recovery. Later package-v17 runs exposed an intermittent dual-clone network failure; see the [investigation](docs/testing/2026-09-16-fc-clone-network.md). Local FC cross-node recovery and returning-node cleanup have passed. Remaining current gates are the dual-clone network issue, physical GPU/NPU validation and full-service mixed-load/soak acceptance. Full stage completion is tracked separately from passing component tests or local runtime scenarios.

租户凭证的创建、查询、吊销及缓存契约见 [API Key 管理](docs/testing/api-key-management.md)。

可观测： [实例与资源指标](docs/testing/instance-resource-metrics.md) · [组件日志采集](docs/testing/log-collection.md) · [跨组件Trace](docs/testing/distributed-traces.md) · [日志滚动压缩](docs/testing/log-rotation.md)。
