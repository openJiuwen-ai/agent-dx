<p align="center">
  <img src="assets/logo/agent-dx-lockup-primary.png" alt="Agent DX" width="560">
</p>

<h3 align="center">Distributed execution substrate for openJiuwen Agent Runtime</h3>

<p align="center"><strong>English</strong> | <a href="README.zh.md">中文</a></p>

Agent DX (**Agent Distributed eXecutor**) is a distributed execution substrate for openJiuwen Agent Runtime. It hosts developer-facing capabilities such as agent registration, invocation, and session management. It also provides a public Sandbox API and SDK, distributed scheduling, isolated execution, traffic routing, runtime operations, checkpoint recovery, and deployment tooling, while keeping the execution backend replaceable.

<p align="center">
  <a href="#-quick-start">🚀 Quick start</a> ·
  <a href="docs/architecture/repository-layout.md">📐 Architecture</a> ·
  <a href="platform/sdk/sandbox/python/README.md">📦 Sandbox SDK</a> ·
  <a href="docs/deployment/adxctl.md">⚙️ Deployment</a> ·
  <a href="docs/testing/control-plane-ci.md">✅ Test gates</a>
</p>

## 🎯 When to use Agent DX

| Your goal | Deployment path | What you need |
|---|---|---|
| Add isolated command, file, terminal, and port operations to one agent service | Standalone | One Linux host, separately managed sandboxd, an ADX release package, and Capsule networking |
| Share execution capacity across agent services and machines | Split-host roles | Persistent Redis, one Master deployment, one or more worker deployments, and a reachable Gateway |
| Run a production-style cluster with repeatable acceptance | Kubernetes | Linux workers, persistent Redis, component certificates, sandboxd on worker nodes, and the ADX Kubernetes E2E profile |
| Pause, resume, clone, or recover long-running work | Standalone or distributed | A checkpoint-capable runtime plus local or S3-compatible checkpoint storage |
| Schedule accelerator workloads | Distributed | GPU/NPU inventory from workers and matching device requests in the Sandbox specification |

## 🧩 How Agent DX fits your stack

| Layer | Owns | Agent DX relationship |
|---|---|---|
| Agent framework or application | Prompts, tools, sessions, task logic, and business policy | Calls the public Sandbox SDK or HTTP API; it does not enter platform scheduling or lifecycle state machines |
| Agent DX | Authentication, placement, Capsule state, routing, recovery, and observability | Provides one distributed execution contract across nodes and execution backends |
| sandboxd | Runtime creation, isolation, networking, and checkpoint primitives | Runs as an external service on each worker and is accessed through the Node Manager runtime driver |
| RRT inside the Runtime | Commands, files, terminals, ports, activity, and runtime recovery cooperation | Receives data traffic through Edge and Node Proxy after ownership and generation checks |
| Redis and object storage | Authoritative cluster metadata and optional shared checkpoint bytes | Redis stores control state and discovery; object storage enables cross-node checkpoint access |

## 🔄 How it works

![Agent DX architecture](assets/architecture/agent-dx.svg)

| Step | What happens | Where it lives |
|---|---|---|
| 1 · Access | A caller authenticates and creates or operates a Sandbox through one public contract. | `gateway/api-server/`, `platform/sdk/sandbox/python/` |
| 2 · Place | API Server uses local-first admission or sends the request to Master. Master rotates across Shards; each Shard queues, filters, scores, and reserves a node. | `platform/master/`, `platform/crates/scheduling/` |
| 3 · Run | Node Manager performs final local admission, serializes the Capsule lifecycle, and asks sandboxd to create a Runtime with RRT. | `platform/node-manager/`, `third_party/sandboxd/`, `platform/runtime/rrt/` |
| 4 · Operate and recover | Edge routes data to the owning Node Proxy. Versioned ownership fences old Runtimes; pause, resume, snapshots, restart policy, and reconciliation converge state after failures. | `gateway/`, `platform/node-manager/`, Redis, checkpoint storage |

API Server embeds Edge by default, and Node Manager embeds Node Proxy by default. Both pairs keep the same contracts when configured as separate processes.

## 📦 Installation

ADX release packages target Linux and contain control/data-plane binaries, RRT, the Python Sandbox SDK, and an optional managed Redis binary. sandboxd remains independently managed and is pinned by [`third_party/sandboxd/source.json`](third_party/sandboxd/source.json).

```sh
mkdir adx-release
tar -xzf adx-release.tar.gz -C adx-release
sudo ./adx-release/install.sh
```

The installer verifies the manifest, file digests, and host architecture. It installs the release under `/opt/adx/releases/<commit>`, atomically switches `/opt/adx/current`, preserves `/opt/adx/config`, `/opt/adx/data`, and `/opt/adx/run` across upgrades, and exposes `adxctl` through `/usr/local/bin`.

## 🔧 Quick start

The default `standalone` profile starts managed Redis, Master, Node Manager with embedded Node Proxy, and API Server with embedded Edge on one host. Prepare sandboxd, networking, certificates, and the initial administrator key first.

```sh
sudo adxctl config init --profile standalone
sudoedit /opt/adx/config/deployment.yaml
sudo adxctl validate
sudo adxctl run
```

In another terminal, install the packaged SDK and point it at the public Gateway:

```sh
python3 -m venv /opt/adx-client
/opt/adx-client/bin/python -m pip install /opt/adx/current/sdk/adx_sandbox-*.whl

export ADX_SERVER_ADDRESS=adx.example.com:8443
export ADX_GATEWAY_ADDRESS=adx.example.com:8443
export ADX_TOKEN="$(cat /secure/path/tenant-api-key)"
export ADX_TLS=1
export ADX_GATEWAY_TLS=1
export ADX_SANDBOX_IMAGE=python:3.12-slim
```

Create a Sandbox, execute through RRT, and delete it explicitly:

```python
import os
from adx_sandbox import Sandbox

sandbox = Sandbox(
    image=os.environ["ADX_SANDBOX_IMAGE"],
    cpu=1000,
    memory=2048,
    name="readme-demo",
)
try:
    result = sandbox.commands.run("printf 'hello from ADX\\n'")
    print(result.stdout)
finally:
    sandbox.kill()
```

The image must be supported by the configured sandboxd and [ADX Runtime Environment](docs/deployment/runtime-environment.md). See the [Sandbox SDK guide](platform/sdk/sandbox/python/README.md) for pause/resume, reusable snapshots, placement, mounts, network configuration, data-plane security, and retry semantics.

## 🏗️ Deployment options

| Topology | `adxctl` profile | Processes on this host |
|---|---|---|
| Standalone with managed Redis | `standalone` | Redis, Master, Node Manager + Node Proxy, API Server + Edge |
| Standalone with external Redis | `standalone-external-redis` | Master, Node Manager + Node Proxy, API Server + Edge |
| Control host | `master` | Master; Redis may be managed separately or added to the full YAML |
| Worker host | `node` | Node Manager + Node Proxy by default |
| Ingress host | `edge-api` | API Server + Edge by default |

Every host has its own `/opt/adx/config/deployment.yaml`. Cluster members share the Redis URL, namespace, and mTLS trust. Every worker has a unique `node_id` and reachable control and proxy addresses. Start Redis → Master → workers → API Server. Set `proxy_mode: standalone` or `edge_mode: standalone` only when those components need separate processes.

YAML string values support `${VAR}` and `${VAR:-default}`. Use `adxctl config dump` to inspect the fully resolved profile and host overrides. Complete fields and certificates are documented in the [`adxctl` reference](docs/deployment/adxctl.md), [standalone guide](docs/deployment/standalone.md), and [configuration examples](build/config/examples/README.md).

## 📐 Core abstractions

ADX separates stable logical identity from replaceable execution:

| Abstraction | Meaning and boundary |
|---|---|
| `Sandbox` | Public API and SDK handle presented to applications |
| `Capsule` | Stable internal identity containing tenant, specification, lifecycle, and desired/observed state |
| `Runtime` | One concrete sandboxd execution of a Capsule on one node; restart or recovery may replace it |
| `Assignment` | Authoritative node and device ownership with a `generation` that fences late old execution |
| `Route` / `Binding` | Versioned ownership published to Edge and rechecked locally by Node Proxy before forwarding |
| `Restore Point` / `Snapshot` | A restore point keeps the Capsule ID; a reusable Snapshot creates a new Capsule |
| `Request ID` / `Operation ID` | One logical write identity for retry, deduplication, result lookup, and reconciliation |

The fixed control path is Agent/application → Sandbox SDK/HTTP API → API Server → Master/Shard scheduler or local-first Node Manager → sandboxd. Runtime traffic follows Edge → Node Proxy → RRT and does not enter lifecycle queues. Compatibility fields such as `instanceId`, `instance_id`, and `/api/instances` are translated at the public boundary; internal Rust types, RPCs, persistence keys, metrics, and runtime identity use Capsule terminology.

## ✨ Capabilities

- Capsule creation, query, deletion, pause, resume, reusable snapshots, and snapshot-based cloning.
- Central and local-first creation with CPU, memory, disk, GPU/NPU device, label, affinity, and preference constraints.
- API Key administrator and tenant identities plus configurable internal mTLS.
- Versioned route publication and synchronized local binding checks.
- Node reconciliation, restart policy, idle deletion, checkpoint recovery, local/S3-compatible storage, and reference-aware artifact cleanup.
- Prometheus metrics, OpenTelemetry traces, structured logs, rotation, gzip compression, and external Collector integration.
- Process deployment with managed or external Redis and Kubernetes end-to-end deployment profiles.

## 🛠️ Development and test

The root Cargo workspace contains the platform and Agent components. Python packages build independently. Build outputs belong under `out/` or a configured external cache.

```sh
make help
make rust-check
cargo test --locked --workspace --all-features -j 2
make agent-test
PYTHONPATH=platform/sdk/sandbox/python \
  python -m pytest -q -c platform/sdk/sandbox/pytest.ini \
  platform/sdk/sandbox/python/tests
make package PYTHON=/path/to/venv/bin/python
```

Run component and integration suites with `python3 build/ci/run.py <suite>`. End-to-end gates use installed release artifacts, the public Sandbox SDK, Redis, Gateway, the control plane, sandboxd, and RRT. Environment requirements and gate definitions are in [control-plane CI](docs/testing/control-plane-ci.md) and the [Kubernetes E2E guide](build/e2e/kubernetes/README.md).

Buildkite uses independent `agent-dx`, `agent-dx-python-sdk`, and
`agent-dx-full-test` pipelines. The Full pipeline consumes explicit base-package
and SDK build UUIDs and does not rebuild either candidate. See the
[Buildkite pipeline contract](.buildkite/README.md).

The SDK distribution is `adx-sandbox`, its Python import is `adx_sandbox`, and its CLI is `adx-sandbox`. ADX environment variables use the `ADX_` prefix, and internal branded HTTP headers use `X-ADX-`.

## 📚 Learn more

- [Architecture and repository layout](docs/architecture/repository-layout.md)
- [Agent usage](agent/README.md)
- [Sandbox API](gateway/api-server/docs/sandbox-lifecycle-api.md) and [OpenAPI](platform/api/openapi/sandbox.yaml)
- [Data-plane OpenAPI](platform/api/openapi/data-plane.yaml)
- [Sandbox Python SDK](platform/sdk/sandbox/python/README.md)
- [Deployment configuration](docs/deployment/adxctl.md) and [examples](build/config/examples/README.md)
- [Scheduling](docs/testing/scheduling-performance.md), [node lifecycle](docs/testing/node-lifecycle.md), and [route publication](docs/testing/route-publication.md)
- [Checkpoint and snapshot storage](docs/testing/snapshot-storage.md)
- [Metrics](docs/testing/capsule-resource-metrics.md), [logs](docs/testing/log-collection.md), and [distributed tracing](docs/testing/distributed-traces.md)
- [Rust coding guidelines](docs/development/rust-coding-guidelines.md)
- [Release pipelines and package layout](docs/development/release-pipelines-and-packaging.md)
- [Brand and architecture assets](assets/README.md)

## License

Agent DX is released under the [Apache License 2.0](LICENSE).
