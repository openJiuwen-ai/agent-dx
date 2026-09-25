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
The `lifecycle` group also starts a 120-second background command from a
separate SDK process, lets that client exit, and requires the six-second idle
policy to delete the instance before the command can finish naturally.

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
  PTY input/EOF/resize/state, default TLS+Token forwarded-port traffic,
  authenticated Host-subdomain port forwarding, and a
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
- `node-failure`: pin one running backend to each node, then suspend node2
  Adxlet heartbeats while its runtime remains independently hosted. Require
  persisted invalidation, resume the same process,
  require backend cleanup before readiness, and prove node1 remains executable.
- `sandboxd-restart`: pin one backend to each node, crash each independently
  hosted sandboxd daemon with `SIGKILL` and restart it. Require unchanged
  backend IDs and readable instance files,
  then delete the instances and verify resource release.
- `restart`: pin one backend to each node, terminate only Adxlet processes,
  wait for fresh node sessions
  and completed reconciliation, prove backend IDs are unchanged, then query and
  execute on the original instances.
- `redis-restart` (targeted fault case): pin one backend to each node, confirm
  AOF is enabled, crash supervised Redis with `SIGKILL`, and verify restarted
  Redis retains both persisted assignments and generations. Require unchanged
  backend IDs, readable instance files, executable commands and final release.
  Select it explicitly with `--profile full --case redis-restart`; it does not
  extend the default eleven-group Full gate.
- `redis-pod-restart` (Kubernetes-only targeted fault case): run AOF Redis in
  its own Pod backed by a PVC, keep one backend on each worker, replace the
  Redis Pod and verify the same PVC, assignments, generations and backend IDs.
  Public SDK file and command access must recover before final deletion. This
  uses a unique default dynamic StorageClass or `--redis-storage-class`; see
  `kubernetes/README.md`. The local Docker
  driver does not offer this case.
- `coordinator-restart` (targeted fault case): keep one live backend on each
  node while the supervised Coordinator is killed and restarted. Require a new
  Redis epoch, unchanged committed ownership and backend IDs, both nodes
  routable, and public SDK file/command access before final deletion. Select it
  with `--profile full --case coordinator-restart`.
- `apiserver-restart` and `ingress-restart` (targeted fault cases): restart each
  supervised public-entry role separately while two instances remain live.
  The Ingress case explicitly renders `ingress_mode: standalone`; the default
  fixture keeps Ingress embedded in API Server.
  Require an unchanged Coordinator epoch, persisted ownership and backend IDs,
  restored HTTPS API, public SDK file/command access and final resource release.
- `runtime-affinity` (targeted heterogeneous-runtime case): requires one live
  node to advertise `runsc` through sandboxd and the other live node not to.
  Create with the public SDK without specifying a node, then check the
  persisted assignment, runtime class, command execution and resource release.
  The default two-node fixture configures only `runc`. To build a heterogeneous
  bundle, pass `--runsc-bin /path/to/native/runsc` to `prepare.py`; its ELF
  architecture is checked against the release and its SHA256 is recorded in
  `bundle.json`. In Buildkite, set `ADX_E2E_RUNSC_BIN` and the independently
  checked `ADX_E2E_RUNSC_SHA256`, set an HTTPS `ADX_E2E_RUNSC_URL` and its
  pinned `ADX_E2E_RUNSC_SHA512`, or set an immutable
  `ADX_E2E_RUNSC_IMAGE=registry/repository@sha256:<digest>` and the binary's
  `ADX_E2E_RUNSC_SHA512`. The image path extracts `/runsc` from a regional OCI
  image, verifies the binary, and avoids downloading it from GitHub on each CI
  worker. Set exactly one of the three sources. Select
  `--profile full --case runtime-affinity`; the driver enables `runsc` only
  on node2 and requires live inventory, unpinned SDK creation, physical
  placement, command execution and cleanup. The OCI test image must support
  the selected `runsc` binary. This case remains unverified until it passes
  on an actual heterogeneous deployment.
  For a targeted Buildkite run with newly composed images, set the exact
  `ADX_BASE_PACKAGE_BUILD_ID`, `ADX_SDK_BUILD_ID` and
  `ADX_E2E_ARTIFACT_COMMIT` of the verified product alongside
  `ADX_E2E_TARGET_CASE=runtime-affinity`; the harness commit may differ from
  the product commit, and both identities are recorded.
- `mixed-soak` (targeted stability case): keep one public-SDK Sandbox active on
  each node while a third worker alternates create/command/file/delete between
  nodes for 300 seconds. Two concurrent workers exercise command and binary
  file round trips on the retained Sandboxes. Require at least 40 commands and
  file round trips, five creations and deletions, zero operation errors, and
  physical backend cleanup on both nodes. Read back the authoritative node
  assignment for every created Sandbox and record verified counts per node.
  Record operation counts and p50/p95/p99/max latency in
  `mixed-soak-result.json`. Select it with
  `--profile full --case mixed-soak` or `--profile standalone --case mixed-soak`;
  it is excluded from the fast default gate.
- `idle-active` (targeted activity case): hold a foreground SDK command request
  open for 12 seconds with a six-second idle timeout. Verify that the instance
  and its allocation survive the active request, then close the client and
  require idle reclamation and resource release. Run with
  `--profile full --case idle-active`; it is outside the default basic gate.
- `relay-standalone` (targeted process-layout case): render a separate
  `adx-relay` service on both nodes and confirm each Relay has its own PID and
  a ready health endpoint. Then run the same public SDK command, file, port
  forwarding and reverse-tunnel checks as the embedded Relay data-plane case.
  Select it with `--profile full --case relay-standalone`; it requires a bundle
  containing `adx-relay`.
- `runtime-exit` (targeted sandboxd fault case): delete a real runc backend
  beneath a live SDK Sandbox. Verify that the default Never policy becomes
  Failed without a replacement, then verify a policy with two retries creates
  two distinct runtime identities and reaches Failed after the third loss.
  Check the persisted assignment, retry count, physical backend inventory,
  public SDK command access and final cleanup. Select with
  `--profile full --case runtime-exit`.
- `sandboxd-runtime-loss` (targeted combined daemon/backend fault): create one
  pinned Sandbox, pause adxlet briefly, remove the runtime through sandboxd,
  restart the sandboxd daemon, then resume adxlet. Repeat for the default Never
  policy and a two-attempt restart policy. Require Never to reach Failed with
  resources released, and the restart policy to run a new backend under the
  original assignment. Verify a public SDK command, final deletion and empty
  physical backend inventory. Select with
  `--profile full --case sandboxd-runtime-loss`.
- `coordinator-adxlet-restart` (targeted combined process fault): retain one
  Sandbox per node, then kill Coordinator and node2 adxlet concurrently under
  their supervisor. Require a new Coordinator epoch and adxlet session, unchanged
  Redis ownership and physical backend identities, restored public SDK command
  and file access, and final deletion. Select with
  `--profile full --case coordinator-adxlet-restart`.
- `sqlite-fallback` (targeted Coordinator outage case): enable the adxlet
  degradation journal only for this fixture, create a retained and an idle
  Sandbox, then suspend Coordinator while managed Redis remains available.
  Require the idle backend to be deleted with a durable SQLite pending record
  while Redis still has the old Running result. Resume Coordinator before the
  extended heartbeat deadline; require journal replay, Redis Deleted state,
  an empty pending table and the retained Sandbox serving a new SDK command
  with its original backend. Select with `--profile full --case sqlite-fallback`.
- `create-response-cut` (targeted lost-acknowledgment case): a TLS proxy reads
  the first successful create final event from the real Ingress, verifies the
  Redis assignment and physical sandboxd backend, then closes the downstream
  connection without delivering that event. The installed Sandbox SDK must
  retry with the same request ID and name; the final generation and backend
  must remain unchanged, and the instance must execute and delete normally.
  Select with `--profile full --case create-response-cut`.
- `create-unknown-query` (targeted transient-404 case): a TLS proxy drops the
  first create response before forwarding its write. The installed SDK keeps
  the original request ID and name while an independent public SDK lookup
  receives 404. The proxy then completes the original write and releases the
  retry; require one Redis assignment, one physical backend, a working command
  and full cleanup. Select with `--profile full --case create-unknown-query`.
- `command-response-cut` (targeted unknown command submission): after creating
  a real runc Sandbox, a TLS proxy forwards each `process.start` to Execd and
  discards its successful response. The installed SDK must return
  `CommandSubmissionError` with the stable command and request IDs. A new SDK
  client recovers the result by command ID; a marker written by the command
  must appear exactly once. Redis assignment, physical backend and final
  cleanup are checked. Select with `--profile full --case command-response-cut`.
- `command-watch-unavailable` (targeted command observation outage): a TLS proxy
  rejects only the command Watch handshake while normal HTTP queries remain
  available. The installed SDK starts one real background command and returns
  `CommandUnavailable` after its reconnect budget; a fresh healthy SDK client
  finds and terminates that same command. Redis generation, physical backend
  and final cleanup are checked. Select with
  `--profile full --case command-watch-unavailable`.
- `command-unsupported-feature` (targeted capability negotiation): a TLS proxy
  removes the Watch capability from Execd's real capability response. The
  installed SDK must return `UnsupportedFeature` before sending `process.start`.
  A fresh healthy client then starts the same command ID exactly once, and
  Redis assignment, backend and cleanup are checked. Select with
  `--profile full --case command-unsupported-feature`.
- `sqlite-node-restart` (targeted compound outage): after an idle deletion is
  durably pending in node1's SQLite journal while Coordinator is suspended,
  restart node1's Adxlet before resuming Coordinator. Require a new process
  with the same live backend and pending delete, then replay the journal and
  check the public SDK, Redis and physical cleanup. Select with
  `--profile full --case sqlite-node-restart`.
- `command-registry-capacity` (targeted Execd admission limit): only node1's
  Execd receives a one-record registry limit. Keep one background command
  running, require a second stable command ID to return `ResourceExhausted`,
  then terminate the holder and retry the rejected ID. Its side effect must
  occur exactly once; Redis assignment, backend and physical cleanup are
  checked. Select with `--profile full --case command-registry-capacity`.
- `upload-response-cut` (targeted resumable file transfer): a TLS proxy forwards
  the first binary upload chunk to Execd, waits for its committed offset, then
  discards the response. The installed SDK must query upload status and continue
  with the same upload ID from that offset. A fresh SDK client downloads the
  committed file and checks its SHA256; Redis assignment, physical backend and
  final cleanup are checked. Select with `--profile full --case upload-response-cut`.
- `download-response-cut` (targeted ranged download): an existing file is
  downloaded through a TLS proxy that returns only the first part of a 200
  response, then closes the connection. The installed SDK must preserve its
  `.part` file and issue a Range request for the remaining bytes. The completed
  SHA256, Redis assignment, physical backend and cleanup are checked. Select
  with `--profile full --case download-response-cut`.
- `schedule-deadline` (targeted center-queue timeout): require both workers to
  advertise only runc, request runsc on node1 with a three-second schedule
  timeout, observe the request in the admin queue, and require an outcome-unknown
  same-operation error after the queue deadline. The request must leave the
  queue without an assignment or held resources; both workers must have empty
  physical backend inventories. Select with `--profile full --case schedule-deadline`.
- `resource-stale` (targeted observation-expiry case): pause only node1's
  resource observer so its 10-second capacity sample expires while adxlet,
  Coordinator and existing backends continue. Require node1 to close new
  admission without losing its routable session; a node1-pinned SDK create
  must time out without a new allocation. Resume observation, require node1
  to reopen admission in the same session, create and execute a new instance,
  then delete both. Select with `--profile full --case resource-stale`.
- `network-partition` (targeted two-worker fault): block node2's TCP traffic to
  Coordinator with a container-local firewall rule while keeping sandboxd and
  the Relay running. Require heartbeat invalidation, public Ingress route
  rejection for the failed instance, and continued SDK command and create on
  node1. Verify the firewall counter recorded blocked packets, remove the
  rule, then require node2 to clean its old backend before final deletion.
  Select with `--profile full --case network-partition`.
- `reconcile-crash` (targeted process fault): expire node2 while its sandboxd
  backend remains, then stop that backend's runc init so physical Delete waits
  after sending TERM. Kill Adxlet only after the pending signal proves cleanup
  began and the node remains closed to admission. Resume the init process;
  the replacement Adxlet must use a new session, remove the stale backend and
  reopen admission. Verify node1 still executes and delete both records.
  Select with `--profile full --case reconcile-crash`; this requires Linux
  `/proc` and the pinned runc backend, and is outside the default Full gate.
- `stop`: independently create a live instance pinned to each node, verify both
  backend inventories are occupied and exercise each forwarded port. A separate
  instance on each node is deleted to emit the required lifecycle log and trace. It also
  generates the Collector outage/recovery evidence required by its own metrics
  and log assertions. Then it stops each product supervisor, requires physical
  deletion, verifies independently hosted sandboxd still answers, and stops it.
  This case has no dependency on earlier SDK, data-plane or restart cases.

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
failover, runtime network replacement, prolonged Coordinator outage, cross-node recovery, XPU,
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
