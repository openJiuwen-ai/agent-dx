**English** | [中文](README.zh.md)

# Agent DX

Agent DX is an execution platform for agents and isolated Instances. It provides a public Sandbox API and SDK, distributed scheduling, node-local lifecycle management, shared traffic entrypoints, runtime operations, checkpoint recovery, and deployment tooling in one repository.

![Agent DX architecture](docs/architecture/images/agent-dx.svg)

## Architecture

Agent applications use the public Sandbox SDK to create and operate Instances. Gateway terminates external traffic and separates control requests from data requests. API Server authenticates callers and serves the Sandbox HTTP API. Master owns cluster state, scheduling, node health, route publication, credentials, and snapshot metadata. Node Manager performs final local admission and serializes the lifecycle of every Instance. Node Proxy forwards data traffic to RRT inside the selected Instance.

Redis is the authoritative cluster store and discovery backend. Node Manager uses a local SQLite journal only when cluster-state submission is temporarily unavailable. sandboxd is managed by the deployment environment and provides the execution backend. Node Manager embeds Node Proxy by default and also supports an explicit split-process deployment.

| Directory | Responsibility |
|---|---|
| `agent/` | Agent APIs, sessions, dispatch, and execution orchestration |
| `gateway/` | Edge entrypoint, Node Proxy, routing, and forwarding |
| `platform/control-plane/api-server/` | Sandbox HTTP API, authentication, ownership cache, and Instance RPC clients |
| `platform/control-plane/master/` | Cluster state, scheduling shards, Redis persistence, routes, credentials, and snapshots |
| `platform/control-plane/node-manager/` | Local admission, Instance lifecycle, sandboxd integration, checkpoints, and outage journal |
| `platform/runtime/rrt/` | Instance-local command, file, terminal, activity, and recovery operations |
| `platform/sdk/sandbox/python/` | Public Python Sandbox SDK |
| `platform/api/proto/` | Internal Instance, node, route, credential, and snapshot contracts |
| `platform/deployment/` | `adxctl`, configuration rendering, process supervision, and shutdown cleanup |
| `build/` and `.buildkite/` | Build, packaging, release, and end-to-end validation tooling |

## Capabilities

- Instance creation, query, deletion, pause, resume, snapshots, and snapshot-based cloning.
- Central and local-first creation paths with CPU, memory, disk, GPU/NPU device, label, affinity, and preference constraints.
- API Key authentication with administrator and tenant identities; internal services use configurable mTLS.
- Versioned route publication from Master to Edge and synchronized local bindings between Node Manager and Node Proxy.
- Node reconciliation, restart policies, idle deletion, checkpoint recovery, local and S3-compatible snapshot storage, and reference-aware artifact cleanup.
- Prometheus metrics, OpenTelemetry traces, structured logs, log rotation, gzip compression, and external Collector integration.
- Process deployment with managed or external Redis, plus Kubernetes end-to-end deployment profiles.

## Quick start

Use an ADX release package on a Linux host. The deployment environment must provide sandboxd, certificates, an initial administrator API Key, and Instance networking. The default standalone profile starts Redis, Master, Node Manager with embedded Node Proxy, API Server, and Edge on one host.

```sh
sudo install -d -m 0700 /etc/adx /etc/adx/tls /etc/adx/secrets /var/lib/adx /run/adx
sudo /opt/adx/bin/adxctl config init

# Edit /etc/adx/deployment.yaml, then validate and start it.
sudo /opt/adx/bin/adxctl validate
sudo /opt/adx/bin/adxctl render --output /run/adx/config-review
sudo /opt/adx/bin/adxctl run
```

In another terminal:

```sh
sudo /opt/adx/bin/adxctl status
sudo /opt/adx/bin/adxctl stop
```

Use `standalone-external-redis`, `master`, `node`, or `edge-api` profiles for external Redis and split-host deployments. Each host owns one deployment YAML; hosts join the same cluster through a shared Redis URL and namespace.

See the [standalone guide](docs/deployment/standalone.md), [`adxctl` reference](docs/deployment/adxctl.md), and [runtime environment guide](docs/deployment/runtime-environment.md) for certificates, Redis, sandboxd, networking, SDK setup, and role-specific examples.

## Build and test

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

Run component and integration suites with `python3 build/ci/run.py <suite>`. End-to-end gates use installed release artifacts, the public Sandbox SDK, Redis, Gateway, the control plane, sandboxd, and RRT. Environment requirements and gate definitions are documented in [control-plane CI](docs/testing/control-plane-ci.md) and the [Kubernetes E2E guide](build/e2e/kubernetes/README.md).

The Sandbox SDK distribution is `adx-sandbox`, its Python import is `adx_sandbox`, and its CLI is `adx-sandbox`. ADX environment variables use the `ADX_` prefix, and internal branded HTTP headers use `X-ADX-`.

## Documentation

- [Architecture and repository layout](docs/architecture/repository-layout.md)
- [Agent usage](agent/README.md)
- [Sandbox API](platform/control-plane/api-server/docs/sandbox-lifecycle-api.md)
- [Sandbox Python SDK](platform/sdk/sandbox/python/README.md)
- [Deployment configuration examples](build/config/examples/README.md)
- [Node lifecycle and resource collection](docs/testing/node-lifecycle.md)
- [Checkpoint and snapshot storage](docs/testing/snapshot-storage.md)
- [Scheduling](docs/testing/scheduling-performance.md)
- [Route publication](docs/testing/route-publication.md)
- [Metrics](docs/testing/instance-resource-metrics.md)
- [Logs](docs/testing/log-collection.md)
- [Distributed tracing](docs/testing/distributed-traces.md)
- [Rust coding guidelines](docs/development/rust-coding-guidelines.md)
