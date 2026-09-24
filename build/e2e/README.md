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
and an independently built SDK candidate into portable runtime images. `run.py` loads and verifies that bundle, starts
Redis, Coordinator/ShardScheduler, API Server with embedded Ingress and two adxlets with embedded Relays, and
independently starts real sandboxd on each node. Business tests use the public
SDK installed into the image from the independent wheel; no product source checkout
is mounted into the test nodes.

```sh
# Native Linux build stage; reuse an existing clean pinned checkout with --source.
python3 build/e2e/build_backend.py \
  --redis-cli /path/to/redis-cli --jobs 2 --output out/e2e/backend
python3 build/e2e/prepare.py \
  --package out/release/package --backend out/e2e/backend \
  --sdk-wheel out/sdk/adx_sandbox-0.1.0-py3-none-any.whl \
  --sdk-candidate out/sdk/sdk-candidate.json \
  --runtime-base registry.example/adx-e2e-tools@sha256:DIGEST \
  --execd-base ubuntu@sha256:DIGEST --output out/e2e/bundle

# Deployment stage on a native architecture matching the package.
python3 build/e2e/run.py --bundle out/e2e/bundle --output out/e2e/run-001
```

The Docker driver defaults to `--cgroupns private`. On a dedicated native Linux
host where sandboxd cannot enable cgroup v2 controllers in a private namespace
(`cgroup.subtree_control: device or resource busy`), use `--cgroupns host`.
Run the driver on the Docker host with permission to create and remove cgroups.
Resource observations still read each container's limits through
`/proc/self/cgroup`; they do not advertise the host's entire capacity. Each run
uses a unique sandboxd cgroup root per node. Cleanup removes those empty groups
after the test containers exit and reports failure if they remain occupied.

Use `--profile l0` for the minimum public API/SDK and authentication closure.
The default `--profile standalone` runs all eleven single-host logical two-node
groups. A group is a deployment and cleanup boundary, not one functional test.
The SDK, data-plane, lifecycle and placement groups emit stable functional
subcases, and JUnit reports those subcases individually. Both profiles write
`required_checks`, per-case results and case-level JUnit; a case that was not
reached is visible as skipped and prevents the JSON result from passing. The
public API inventory and uncovered conditional features are tracked in
[SDK E2E coverage](../../docs/testing/sdk-e2e-coverage.md).
For a focused diagnostic, add `--case stop` (or another case from the selected
profile). The result is labeled `profile: targeted`, records `source_profile`
and `selected_case`, and cannot be cited as a complete profile pass.

All output directories must be new. Local runs allow dirty packages and image
tags, recording their actual identities. Buildkite requires the current clean
commit and digest-pinned base images. The local and Kubernetes drivers share business scenarios and gate semantics;
[agent prerequisites and pipeline](../../.buildkite/README.md) describe setup.
`build/images/Dockerfile.e2e-runtime` supplies the tested Ubuntu runtime tools.
Local Docker reproduction requires access to the same bind-mounted paths as the Docker daemon. The Kubernetes deployer uses the supplied kubeconfig and registry references.

## Assertions

- `sdk`: Linux release packages first verify the configured EROFS or OCI runtime,
  runtime-only override and a plain custom image with the read-only EXECD bootstrap
  mount. The Kubernetes profile uses OCI; local process acceptance uses EROFS. Then
  two real instances across two nodes verify query, stdout/stderr/exit code,
  binary file round-trip, explicit deletion, Redis terminal state and released
  resources, and empty sandboxd inventories.
- `data-plane`: query both schedulable nodes through the installed SDK, create on
  a selected node, reattach by public instance ID (Environment ID), and exercise foreground/background
  commands, both handle and collection stdin/EOF, sync and async waits, stable
  command replay/conflict, typed not-found/timeout results, both kill entry
  points, filesystem text/binary/depth/directory copy, stateful Shell, interactive
  PTY input/EOF/resize/state, default TLS+Token forwarded-port traffic, and a
  per-Environment TLS-only forwarded-port policy, and an SDK reverse-tunnel upstream
  round trip.
- `lifecycle`: close a detached handle, reattach to the same running Environment,
  delete it explicitly, verify ordinary `close()` preserves the remote Environment,
  verify context-manager deletion, then require an idle-timeout Environment to be
  reclaimed without a client-side delete.
- `auth`: invalid key and another tenant cannot read or delete the instance;
  the owner's instance remains running. An administrator creates, lists and
  revokes a tenant key through HTTPS Ingress; tenant management requests are denied,
  and revocation takes effect within the configured authentication cache budget.
- `capacity`: fill both nodes' advertised CPU capacity, verify another create
  waits, then release capacity and require that request to become executable.
- `placement`: use the public SDK on two nodes to verify environment affinity OR,
  instance anti-affinity, weighted and ordered node preferences, node ID
  constraints on every OR branch, and reverse instance anti-affinity. Require
  both nodes to report their live `runc`-only sandboxd runtime inventories;
  requesting `runsc` on node1 must expire in the central scheduling queue
  without an assignment or held resources. Verify actual assignments, execute
  a command and check physical cleanup. A separate heterogeneous-runtime E2E
  requires nodes with different sandboxd inventories.
- `local-first`: restart API Server with `create_mode: "local_first"`, verify
  entry-node rotation, concurrent same-name creation converging to one Environment,
  conflicting specifications rejected, real EXECD commands, and physical cleanup.
  Require Coordinator local-claim logs, then restore the central deployment mode.
- `node-failure`: suspend node2 Adxlet heartbeats while its runtime remains
  independently hosted; require persisted invalidation, resume the same process,
  require backend cleanup before readiness, and prove node1 remains executable.
- `sandboxd-restart`: create live instances, crash each independently hosted
  sandboxd daemon with `SIGKILL` and restart it. Require unchanged backend IDs
  and readable instance files,
  then delete the instances and verify resource release.
- `restart`: terminate only Adxlet processes, wait for fresh node sessions
  and completed reconciliation, prove backend IDs are unchanged, then query and
  execute on the original instances.
- `stop`: independently create a live instance pinned to each node, verify both
  backend inventories are occupied, stop each product supervisor, require
  physical deletion, verify independently hosted sandboxd still answers, then
  stop it. This case has no dependency on a preceding `restart` case.

The runner produces `result.json`, `junit.xml`, package/image identity, SDK
results, node catalogs and component logs. Generated keys/certificates use a
private temporary directory outside uploaded evidence. Deployment failures and
cancellation still enter scoped cleanup. Original failures are retained;
cleanup failure or an unexecuted scenario prevents a pass. Images and bundles
are retained as build artifacts/cache; test containers and network are removed.

## Coverage boundary

The basic suite uses runc and no writable-layer quota. Most cases disable idle
reclamation; the dedicated `lifecycle` case enables a six-second timeout. It does
not validate pause/resume, snapshots, S3 rootfs/mounts, entrypoint inheritance,
failover, runtime network replacement, Coordinator outage, cross-node recovery, XPU,
mixed-load scheduling or performance. Those runtime-specific
contracts are assigned to the Firecracker profile. Resource observations read the node's cgroup
limits and filesystem with infrastructure reservations; this fixture is not
the production sandboxd collector.

sandboxd is locked to PR #56 through `third_party/sandboxd/source.json`. A
single-platform EXECD manifest is published within the test network (the pinned
backend's image-index lookup defaults to AMD64). OCI storage is a child of a
tmpfs mount to avoid nested OverlayFS depth and allow sandboxd to reset its
image directory. No host registry port or global Docker configuration is needed.

The SDK-only script remains available for an already provisioned deployment:

```sh
python build/e2e/sdk_smoke.py --endpoint 127.0.0.1:8443 \
  --token-file /run/adx-test/api-key --ca /run/adx-test/tls/ca.pem \
  --image registry.example/adx-execd@sha256:DIGEST --output out/e2e/sdk
```

Running this script alone does not establish the full acceptance result.

The formal three-machine inventory and completed-result contract is documented
under [multivm](multivm/README.md). The contract verifier does not provision VMs
or turn historical data-plane scripts into a control-plane acceptance result.

The capacity group also scrapes the live Coordinator and both Adxlet metrics endpoints. It checks two running instances and 4000 allocated CPU millis, one queued request while full, and zero instances/reservations after deletion. Per-node CPU, memory and disk allocation gauges must agree across Coordinator and Adxlet. Raw scrapes are retained as `metrics-allocated.json`, `metrics-queued.json` and `metrics-released.json`.

组件日志采集验收复用现有 Ingress/Relay 指标端点，并通过真实 OpenTelemetry Collector 接收结构化组件日志。stop 组包含后端 503、文件滚动与 Collector 重启，控制台输出 `[METRICS PASS]` / `[COLLECTION PASS]`；产物含 `gateway-metrics-node*.json`、`collection-node*.json`、`collected-logs.jsonl` 和 `collector-process.log`。部署及保证边界见 `docs/testing/log-collection.md`。Trace 已纳入采集验收，输出 `[TRACE PASS]` 并保存 `traces-node*.json` 和 `collected-traces.jsonl`；正式结果见 [Buildkite #21](../../docs/testing/2026-09-17-observability-k8s.md)。
