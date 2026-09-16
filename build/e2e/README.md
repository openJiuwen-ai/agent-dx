# Public SDK acceptance

Buildkite deploys to **Kubernetes** through [kubernetes/run.py](kubernetes/README.md).
It creates a dedicated namespace with two node Pods and Services, using immutable
images from the build step. Each Pod runs the platform as processes.

## Installed example reproduction

[Installed example acceptance](example/README.md) runs the shipped complete
deployment configuration on a dedicated Linux KVM host, checks public SDK
operations and shutdown cleanup, and preserves configuration hashes.

## Local Docker reproduction

For local reproduction, `prepare.py` turns a verified ADX release package and pinned external backend
into portable runtime images. `run.py` loads and verifies that bundle, starts
Redis, Master/Domain, Sandbox API, Edge, two Node Managers and Node Proxies, and
independently starts real sandboxd on each node. Business tests use the public
SDK installed into the image from the release wheel; no product source checkout
is mounted into the test nodes.

```sh
# Native Linux build stage; reuse an existing clean pinned checkout with --source.
python3 build/e2e/build_backend.py \
  --redis-cli /path/to/redis-cli --jobs 2 --output out/e2e/backend
python3 build/e2e/prepare.py \
  --package out/release/package --backend out/e2e/backend \
  --runtime-base registry.example/adx-e2e-tools@sha256:DIGEST \
  --rrt-base ubuntu@sha256:DIGEST --output out/e2e/bundle

# Deployment stage on a native architecture matching the package.
python3 build/e2e/run.py --bundle out/e2e/bundle --output out/e2e/run-001
```

All output directories must be new. Local runs allow dirty packages and image
tags, recording their actual identities. Buildkite requires the current clean
commit and digest-pinned base images. The local and Kubernetes drivers share business scenarios and gate semantics;
[agent prerequisites and pipeline](../../.buildkite/README.md) describe setup.
`build/images/Dockerfile.e2e-runtime` supplies the tested Ubuntu runtime tools.
Local Docker reproduction requires access to the same bind-mounted paths as the Docker daemon. The Kubernetes deployer uses the supplied kubeconfig and registry references.

## Assertions

- `sdk`: two real instances across two nodes; query, stdout/stderr/exit code,
  binary file round-trip, explicit deletion, Redis terminal state and released
  resources, and empty sandboxd inventories.
- `auth`: invalid key and another tenant cannot read or delete the instance;
  the owner's instance remains running. An administrator creates, lists and
  revokes a tenant key through HTTPS Edge; tenant management requests are denied,
  and revocation takes effect within the configured authentication cache budget.
- `capacity`: fill both nodes' advertised CPU capacity, verify another create
  waits, then release capacity and require that request to become executable.
- `placement`: use the public SDK on two nodes to verify instance affinity OR,
  instance anti-affinity, weighted and ordered node preferences, node ID
  constraints on every OR branch, and reverse instance anti-affinity; verify
  actual assignments, execute a command and check physical cleanup.
- `node-failure`: suspend node2 Node Manager heartbeats while its runtime remains
  independently hosted; require persisted invalidation, resume the same process,
  require backend cleanup before readiness, and prove node1 remains executable.
- `restart`: terminate only Node Manager processes, wait for fresh node sessions
  and completed reconciliation, prove backend IDs are unchanged, then query and
  execute on the original instances.
- `stop`: stop each product supervisor with live instances, require physical
  deletion, verify independently hosted sandboxd still answers, then stop it.

The runner produces `result.json`, `junit.xml`, package/image identity, SDK
results, node catalogs and component logs. Generated keys/certificates use a
private temporary directory outside uploaded evidence. Deployment failures and
cancellation still enter scoped cleanup. Original failures are retained;
cleanup failure or an unexecuted scenario prevents a pass. Images and bundles
are retained as build artifacts/cache; test containers and network are removed.

## Coverage boundary

The basic case uses runc, `idle_timeout=0`, and no writable-layer quota. It does
not validate pause/resume, snapshots, Master outage, cross-node recovery, XPU,
tunnels, mixed-load scheduling or performance. Resource observations read the node's cgroup
limits and filesystem with infrastructure reservations; this fixture is not
the future production sandboxd collector.

sandboxd is locked to PR #56 through `third_party/sandboxd/source.json`. A
single-platform RRT manifest is published within the test network (the pinned
backend's image-index lookup defaults to AMD64). OCI storage is a child of a
tmpfs mount to avoid nested OverlayFS depth and allow sandboxd to reset its
image directory. No host registry port or global Docker configuration is needed.

The SDK-only script remains available for an already provisioned environment:

```sh
python build/e2e/sdk_smoke.py --endpoint 127.0.0.1:8443 \
  --token-file /run/adx-test/api-key --ca /run/adx-test/tls/ca.pem \
  --image registry.example/adx-rrt@sha256:DIGEST --output out/e2e/sdk
```

Running this script alone does not establish the full acceptance result.
