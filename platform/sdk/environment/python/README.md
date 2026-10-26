# Agent DX Environment Python SDK

`adx-environment` is the high-level Python client for the frontend
**environment v1** API. It creates and manages environments, including reusable
Snapshots and a paused-environment lifecycle. It also provides the gateway routes
used for reverse tunnels.

```python
from adx_environment import Environment

with Environment(image="python:3.12-slim", cpu=2000, memory=4096) as environment:
    environment.files.write("/tmp/hello.txt", "hello world")
    print(environment.commands.run("cat /tmp/hello.txt").stdout)
```

`close()` releases local SDK resources and leaves the remote environment alive.
`kill()` deletes a non-detached remote environment as well.
The context manager explicitly calls `kill()` on exit; garbage collection does
not make a remote delete request. If the block raises, its original exception
remains visible even when cleanup also fails.

## Server compatibility

The new Environment backend supports basic lifecycle, on-demand pause/resume, reusable snapshots, grouped placement, idle deletion/pause and restart policy. It requires an image containing the release EXECD at the configured command path; generic `python:3.12-slim` below is only an illustrative image name.

The Environment backend supports S3 rootfs, S3/image/Local mounts, entrypoint inheritance,
creation and runtime network policy, `extra_config`, `failover=True`, independent
resource limits, and per-environment data-plane security. Declared user ports use
Ingress and Relay routing. Public local rootfs paths remain rejected. Local bind
mounts accept `host_path` (alias `hostPath`), with exactly one mount source and
pre-existing node-visible directories. Arbitrary host paths require a trusted
management caller; the API does not enforce per-tenant source-path authorization. `upstream` reverse tunnel uses the published
`/tunnel/{environment}` route; the legacy `/invoke` fallback remains unavailable. See the
[current HTTP contract](https://gitcode.com/openJiuwen/agent-dx/blob/refactor/gateway/apiserver/docs/environment-lifecycle-api.md).

## OOM diagnostics

Command results and background-command info preserve `exit_signal`, such as
SIGKILL=9. EXECD does not observe OOM. A user process can be killed while the
Environment stays Running; a signal alone is not an OOM classification. The current
sandboxd API does not expose a separate user-process OOM reason.

If the execution backend itself is OOM-killed, `environment.get_info().failure`
contains an `EnvironmentFailure(reason="OomKilled", exit_code=..., message=...)`
after the terminal state is committed. A failed direct invocation can raise
`EnvironmentError` with `code="ENVIRONMENT_OOM"`, `retry="never"`, `outcome="terminal"`
and `failure` details. Before publication, the caller may receive
`OUTCOME_UNKNOWN`; query `get_info()` instead of interpreting route errors as
OOM. Successful calls do not perform an additional catalog query.

## Working directory

`Environment(cwd="/workspace/task")` sends the constructor directory to the server.
Execd creates the directory and any missing parents before reporting Ready;
the SDK does not issue a separate filesystem initialization call. An existing
directory and its contents are preserved. Initialization failure makes creation
fail through the normal lifecycle cleanup path.

Commands without an explicit `cwd` use the constructor directory. A per-command
`commands.run(..., cwd="/other")` override must already exist; create it explicitly
with `files.make_dir()` when needed. A failed command does not stop Execd. The
constructor setting does not change Execd's own working directory or an inherited
OCI entrypoint's `WORKDIR`.

## Image startup process

Set `inherit_entrypoint=True` on a fresh image-backed environment to start the
image's effective `Entrypoint` and `Cmd` as its managed workload:

```python
environment = Environment(image="example/worker:latest", inherit_entrypoint=True)
exit_code = environment.wait_entrypoint()
print(exit_code, environment.entrypoint_exit_info)
```

Creation fails when the image has no effective startup command or cannot start
it. A managed process may exit successfully; its terminal status remains
available through bounded polling while the environment and its other APIs remain
usable. Configure workload probes when creation must wait for a service.

## Workload probes

```python
from adx_environment import Environment, ReadinessProbe, HealthcheckProbe, EnvironmentError

try:
    environment = Environment(
        image="example/server:latest",
        inherit_entrypoint=True,
        init_timeout=120,
        readiness_probe=ReadinessProbe.http_get(port=8080, path="/ready"),
        healthcheck_probe=HealthcheckProbe.http_get(
            port=8080, path="/health", failure_threshold=3,
        ),
    )
except EnvironmentError as error:
    print(error.probe_failure)
    raise
```

Both probe types provide `http_get(port=..., path="/")`,
`tcp_socket(port=...)`, and `exec(command=[...])` factories. HTTP and TCP check
environment loopback; the port need not be publicly exposed. HTTP requires a 2xx
response and does not follow redirects. Exec uses argv, the image workload's
user, working directory and effective environment; exit code zero succeeds.
Use an explicit shell argv when shell syntax is needed.

| Setting | ReadinessProbe | HealthcheckProbe |
| --- | --- | --- |
| `initial_delay_seconds` | 0 | 0, measured after readiness |
| `period_seconds` | 2 | 10 |
| `timeout_seconds` | 1 | 2 |
| `failure_threshold` | retries until initialization deadline | 3 consecutive failures |

Execd runs readiness once per startup/resume/restore and only then reports
Running. Adxlet activates routes and publishes Running after that report.
`init_timeout` bounds Execd initialization and readiness after backend preparation.
The total `create_timeout` also covers scheduling, cold image preparation, runtime
startup and publication; by default it is scheduling plus initialization plus a
30-second buffer. Set it explicitly for a cold image that needs more time.
Readiness failure raises `EnvironmentError` with `probe_failure` and is terminal
for that create attempt. Initialization is transient runtime state, not a
separate persisted Environment lifecycle state.

Healthcheck starts after readiness (or immediately after runtime startup when
readiness is absent). A success resets the failure count. At the threshold,
Execd reports a sticky failure; Adxlet retires the execution and applies the
existing restart/failover policy. `environment.get_info().failure` is a
`EnvironmentFailure(reason="ProbeFailed", ...)`; its `probe_failure` dictionary
contains the probe kind, message, consecutive failures and available exit/HTTP
status. Backend OOM and exit failures retain their existing reason values.
Internal probes do not extend the environment's user-activity idle window.
Pause/resume and restore cancel in-flight checks and reset healthcheck counts.
The SDK checks server capability before a probe-dependent create; unsupported
API servers return an error before submission. Use the matching release of
API Server, Adxlet and Execd.

## Lifecycle overview

There are three deliberately different checkpoint paths:

| Path | Public SDK API | Artifact and placement | When to use it |
| --- | --- | --- | --- |
| Reusable Snapshot | `create_snapshot()` then `Environment.create()` | New Environment identity; shared storage prefers the source node with eligible-node fallback, local-only pins it. | Independent clones from a prepared source. |
| Pause / resume | `pause()` then `resume()` | Same Environment ID; Coordinator first tries the owner, then schedules a compatible node after definitive resource rejection or owner loss. | Stop and resume one logical environment. |
| Failure recovery | `failover=True` | Same Environment and node; restores the latest unexpired checkpoint after unexpected backend exit. | Workloads that must recover execution state rather than cold-start. |
| Explicit reload | `reload()` | Same Environment; replaces a Running backend from its latest unexpired checkpoint. | Operator-requested reset to a known recovery point. |

The SDK is a client-side validation, request-ID, attempt, and result-shaping
layer. Adxlet owns lifecycle and checkpoint bytes; Coordinator owns placement, committed state and the snapshot catalog.

## Reusable Snapshots

Create a reusable Snapshot from an open environment:

```python
from adx_environment import Environment

source = Environment(image="python:3.12-slim", name="source")
snapshot = source.create_snapshot(name="python-ready")

# The source stays running. `snapshot` is a SnapshotInfo value.
clone = Environment.create(snapshot, name="worker-1")

clone.kill()
source.kill()
```

The public signature is:

```python
Environment.create_snapshot(*, name=None, timeout_seconds=300) -> SnapshotInfo
```

`name` is optional, but if supplied it must be a nonblank string.
`timeout_seconds` must be an integral, non-boolean value from 1 through 3600.
The result is a frozen `SnapshotInfo(snapshot_id: str, names: tuple[str, ...])`.
The Snapshot has no TTL: delete it explicitly when it is no longer needed.
Snapshots preserve fixed Volume UIDs and protect their definitions through completed
Snapshot deletion, even after the last Binding is deleted. External directory data
is not copied. Clones do not inherit the Snapshot mounts; explicitly requested
Volume mounts use normal admission. Volume inspection exposes `snapshot_ids`.

```python
same_snapshot = Environment.get_snapshot(snapshot.snapshot_id)
snapshots, next_page_token = Environment.list_snapshots(
    name="python-ready", page_size=20,
)
Environment.delete_snapshot(same_snapshot.snapshot_id)
```

The source must have a valid lifecycle identity. In particular, an Environment with
an active reverse tunnel cannot create a reusable Snapshot. A successful
Snapshot leaves the source running and publishes an immutable artifact; the
server records READY metadata only after that artifact is available.
The SDK does not expose the reusable-Snapshot request ID and generates a fresh
identity for each call. Raw HTTP clients that retry an uncertain request must
reuse it only for the same source and name; they must not reuse one request ID
for different catalog content.
The SDK keeps its local tunnel client active while the checkpoint request is
in flight; expected reconnect noise during that scope is logged at debug level
without changing the checkpoint result.

### Create from Snapshot

Use `Environment.create(snapshot_id, **kwargs)` with either a `SnapshotInfo` or a
Snapshot ID string. It performs the normal environment create request with a
`snapshotId`, so the result is a new `Environment` handle that reaches RUNNING in
the usual way.

```python
clone = Environment.create(
    snapshot,
    name="worker-2",
    # Omitted resources inherit the snapshot; explicit values must match it.
)
```

The Coordinator inherits omitted image/runtime/scalar resources and validates explicit values against the source geometry. It does not resize a restored VM. Environment overrides and placement constraints are carried into the new Environment; local-only snapshots require the source node.

For S3, AFS and JuiceFS snapshots, omitting placement constraints automatically adds a soft preference for the snapshot's source node, including selection of its scheduling shard. This can reuse a node-local cache, but does not guarantee a cache hit. If the source is unavailable, lacks resources or fails a hard constraint, another eligible node can restore from shared storage without waiting for the source. Explicit placement policies take precedence over this default. Snapshot-based creates use Coordinator scheduling even when the API Server enables `local_first`; no extra SDK argument is required. Concurrent creates retain independent identities and reservations, while retries of one identity reuse its assignment.

The source briefly pauses during snapshot creation and resumes before success. A reusable snapshot is not consumed by cloning; deletion returns `SNAPSHOT_IN_USE` while references remain. See [storage and cloning](../../../../docs/testing/snapshot-storage.md).

Later dual-clone FC runs exposed a network failure, tracked in the [investigation](https://gitcode.com/openJiuwen/agent-dx/blob/refactor/docs/testing/2026-09-16-fc-clone-network.md); successful earlier batches do not close that issue.

## Pause and resume

Pause one open environment and later resume the same logical environment:

```python
from adx_environment import RetentionPolicy, Environment

environment = Environment(image="python:3.12-slim")

paused = environment.pause(retention=RetentionPolicy.for_seconds(90_000), timeout_seconds=300)
print(paused.snapshot_id, paused.expires_at)

running = environment.resume()
print(running.route_address, running.port_mappings)
```

The public signatures are:

```python
Environment.pause(retention=None, *, timeout_seconds=300) -> PauseResult
Environment.resume() -> ResumeResult
```

`retention=None` inherits `pause_policy.retention`; the default policy retains the
restore point for 90,000 seconds. Use `RetentionPolicy.for_seconds(seconds)` for
a positive integer TTL or `RetentionPolicy.forever()` to retain the point until
explicit deletion or replacement. Finite retention begins after publication.
Permanent pause returns `expires_at=None`; the stored record and RPC use an absent
expiry, rather than a large timestamp. `PauseResult` also contains `environment_id`,
`snapshot_id`, byte `size`, and `state="paused"`.

`timeout_seconds` is keyword-only, integral and non-boolean, from 1 through 3600.
It bounds the sandboxd capture RPC and sets the SDK's per-attempt HTTP wait with
an extra 30-second transport allowance. It is not a total pause deadline: OBS
publication follows capture and uses its own storage request timeout. Losing the
HTTP reply does not cancel an accepted lifecycle operation; its outcome can remain
unknown and the SDK retries the same operation identity.

`resume()` returns a frozen `ResumeResult` with `environment_id`, `state`,
`route_address`, `function_proxy_id`, `node_id`, and `port_mappings`. The SDK
requires the response to identify this environment, report `"running"`, and
include a route address and function-proxy ID. Resume uses Coordinator EnsureRunning, first admitting on the owning node. A shared checkpoint allows another compatible node after definitive NoCapacity or owner invalidation; an uncertain RPC result retains ownership and retries there. The selected node restores the checkpoint, rearms the runtime listener and commits Running. Route-cache convergence after
that result is outside the resume success boundary.

Pausing validates the configured store's artifact and commits Paused after removing the old execution. Local-only files are immediately readable on the owning node; adxlet does not force filesystem writeback, so publication does not guarantee survival of a host crash or power loss. S3 publication waits for upload completion. A SQLite-only result is not cluster success. Public resume uses authoritative ownership and a valid checkpoint. Local-only points remain tied to the original node; shared points preserve the Environment ID across a new ownership generation. Node loss preserves Paused until demand, expiry or deletion.

### Idle pause and traffic resume

```python
from adx_environment import CheckpointStorage, IdleAction, PausePolicy, RetentionPolicy, Environment

environment = Environment(
    image="example/execd:latest",
    idle_timeout=60,
    idle_action=IdleAction.PAUSE,
    pause_policy=PausePolicy(retention=RetentionPolicy.forever(), timeout_seconds=300, storage=CheckpointStorage.SHARED),
)
```

`idle_action` is an `IdleAction` enum and defaults to `IdleAction.DELETE`; `IdleAction.PAUSE` requires a positive idle timeout
and a checkpoint-capable runtime. `CheckpointStorage.CONFIGURED` uses the node backend,
`CheckpointStorage.LOCAL` retains files on that node, and `CheckpointStorage.SHARED` requires complete publication
to its configured object store. Configure an OBS S3-compatible endpoint on every
eligible node, with the same alias/bucket/prefix. The shared setting also limits
creation placement to nodes advertising shared checkpoint storage.

An automatically paused environment also supports `environment.resume()` explicitly.
It uses the same restore intent as traffic-triggered wake-up, so concurrent
manual and automatic requests do not start two runtimes. The checkpoint must
still be valid and unexpired; local-only restore requires the owning node.
The idle policy stays enabled after resume.

The existing idle window observes Execd client operations and Relay streams;
health, metrics and command observation do not keep the environment busy. A detached
background command can be checkpointed. Automatic pause atomically closes new
admission and rechecks activity. New demand before capture cancels pause; demand
once capture starts waits for publication and restores afterward.

Authorized business traffic through Ingress shares the manual resume entry for
both automatic and manual pauses; no auto-resume opt-in is required. Concurrent first requests coalesce into one durable intent.
Ingress waits for the committed Running route before sending application bytes;
passive route subscriptions and health/metrics do not wake a paused environment.
Requests already sent upstream are not replayed. Transient owner RPC failures
retry on the same owner within the resume budget. A temporary wake failure before
forwarding returns HTTP 503. A finite SDK HTTP request sends
`X-ADX-Processing-Timeout-Ms`: authentication, route lookup, wake, route delivery,
connection admission, forwarding and response streaming share one Ingress deadline.
The SDK defaults to 300 seconds for a data request and adds a configurable
communication margin (30 seconds by default) to its HTTP timeout. Without the
header, wake and route delivery share a 300-second default; existing forwarding
and streaming lifetimes remain unchanged. Negative invoke timeouts omit the
header and retain unbounded application HTTP waits.

If readiness is still pending when the deadline expires before application
forwarding, Ingress returns HTTP 503 with `X-ADX-Error-Code: ENV_RESUMING`,
`Retry-After: 1`, and a JSON `error` containing `code: ENV_RESUMING`,
`retry: same_operation`, `outcome: not_started`, `requestForwarded: false`,
`requestId`, and `environmentId`. The durable restore continues independently.
SDK retries retain the request ID and body; **each allowed retry gets the same
configured timeout plus communication margin**. Retry count is configurable via
`ConnectionConfig.request_max_attempts` / `ADX_REQUEST_MAX_ATTEMPTS` (default 3,
including the first data-path attempt). Known-unsent connection failures and absent
direct routes retain their existing single control-plane fallback; `ENV_RESUMING`
never takes that fallback. Retry-After and jitter apply between demand retries.
Exhaustion raises `EnvironmentHTTPError` with the same `ENV_RESUMING` contract; callers
can choose to try again. No sticky Ingress routing is required: replicas share the
Coordinator intent and independently subscribe to the committed route.

HTTP 504 after forwarding carries `PROCESSING_DEADLINE_EXCEEDED`, `outcome: unknown`,
`requestForwarded: true`, and `retry: never`; it is not a safe demand retry. If
response headers have already been delivered, deadline expiry ends the body stream
instead. Existing idempotent invoke transport retries keep the same EXECD operation
ID and never cross into a control-plane fallback after an unknown outcome.
File bytes and seekable file bodies support safe demand retry. Consumed iterators
are not replayed automatically and surface the structured error instead.
Expired or unavailable checkpoints return HTTP 409 `ENV_UNAVAILABLE`, `retry: never`.

A timeout may leave the durable operation pending. If every node rejects admission,
rejected nodes are excluded for a 30-second capacity round before reevaluation.

## Recovery methods

`failover=True` restores from the latest unexpired checkpoint after an
unexpected backend exit on the owning node. If no valid checkpoint exists, the
Environment becomes Failed and is not recreated from the original image.
`Environment.reload() -> bool` explicitly replaces a Running backend from that same
recovery point. It returns `True` only after the replacement reaches Running and
the authoritative result is committed. Automatic restart policy is a separate cold
restart mechanism. Shared-checkpoint node-failure recovery is coordinated by
Coordinator and is not selected by either public call.

## Timeouts, attempts, and errors

For `create_snapshot()` and `pause()`, `timeout_seconds` defaults to the
public `ADX_GET_DEFAULT_TIMEOUT` value of 300 seconds. It is the logical server
checkpoint timeout sent as `timeoutSeconds`; each SDK HTTP attempt uses that
logical value plus a 30-second transport buffer. `resume()` and `reload()`
have no public logical-timeout body field and use the SDK default plus that
buffer for each transport attempt. SDK argument errors are raised
before any request; closed `create_snapshot`, `pause`, and `resume` handles
raise `RuntimeError`.

The SDK generates request IDs; callers of `Environment` cannot set them. Its
attempt rules intentionally differ by operation:

- Reusable Snapshot creation makes one SDK transport attempt only. A connection problem or
  gateway 502/503/504 produces `EnvironmentError` with an **uncertain** outcome,
  because the immutable Snapshot might already have committed. Reconcile it
  explicitly instead of assuming that another attempt is safe.
- Pause, resume, and reload make up to three transport attempts for transient transport
  or gateway failures, all with one internal request ID. If the final result is
  still uncertain, reconcile the instance state instead of changing the request
  under that identity.
- Create from Snapshot uses the normal create policy with up to three attempts
  and one `create-*` identity. When the caller omits `name`, the SDK derives a
  stable name from that identity, so another API Server replica receives the
  same Environment ID. An uncertain result must still be queried or retried with
  the same identity.
- `Environment.delete()` treats HTTP 404 as an idempotent success. HTTP 403 raises
  `PermissionDenied` with the request ID and is not retried; transient transport
  or gateway failures retain one request ID across bounded retries.

Structured server failures raise `EnvironmentHTTPError`, whose `code`, `retry`,
`outcome`, `request_id`, `operation_id`, and `environment_id` fields implement the
[management error contract](https://gitcode.com/openJiuwen/agent-dx/blob/refactor/platform/api/http/error-contract.md). Transport
failures with an uncertain write result surface the same fields on
`EnvironmentError`. Other malformed typed-result
shapes can instead surface `ValueError` or `TypeError` while values are
converted, or `RuntimeError` when resume `portMappings` is not an object.
A confirmed cancellation before assignment returns
`DEADLINE_EXCEEDED / never / not_started`; the SDK makes no additional attempt
or backoff after that response. `error.scheduling_failure` contains placement
rejection counts, requested resources, and worker capacity/available maxima
when the server supplies them. Maxima are per dimension and may describe
different workers; diagnostics describe the current snapshot, not future
capacity after scaling. A deliberate new create call can start a new queue wait.
Unconfirmed runtime/transport results retain the same-operation retry policy.
After retries, a complete service failure remains `EnvironmentHTTPError` with
its status, payload and metadata; it is not labeled as a connection failure.

`reload()` translates `EnvironmentError` into `False`; it does not normalize those
other programming/shape exceptions.

### SDK versus raw HTTP

These are SDK semantics, not a substitute for the [frontend REST contract](https://gitcode.com/openJiuwen/agent-dx/blob/refactor/gateway/apiserver/docs/environment-lifecycle-api.md).
The SDK uses these paths internally:

```text
POST /api/environment/v1/environments/{id}/snapshots
POST /api/environment/v1/environments                 # create from Snapshot: snapshotId
POST /api/environment/v1/environments/{id}/pause
POST /api/environment/v1/environments/{id}/resume
POST /api/environment/v1/environments/{id}/reload
PUT  /api/environment/v1/environments/{id}/network
```

Raw HTTP has intentionally different validation in a few places. A raw
Snapshot request has `name` as its handler field: an omitted or empty name is
accepted, while a supplied whitespace-only name is rejected. Its
`timeoutSeconds` defaults to 300, is validated in the range 1 through 3600,
converted to milliseconds, and forwarded as the checkpoint RPC logical timeout. Raw Pause accepts optional `retention` as `{"kind":"forever"}` or
`{"kind":"ttl","ttlSeconds":3600}`; omission inherits the creation policy.
Its `timeoutSeconds` is validated from 1 through 3600 and defaults to 300. Only Snapshot and Pause currently accept this caller-provided logical
timeout body field. The SDK sends `{}` for resume and reload, and their
handlers define no body fields. Raw HTTP requires a pattern-valid
`X-ADX-Request-ID` header;
the SDK generates that header internally.

Internal signal/POSIX/runtime SDK APIs are not part of this package or the new Environment protocol.

## Other create options

Request one kind of whole device with `type:model:count`:

```python
Environment(xpu="gpu:l20:1")
Environment(xpu="gpu:h100:2")
Environment(xpu="gpu::1")  # any GPU model
Environment(xpu="npu:ascend910b4:1")
```

The SDK accepts one `gpu` or `npu` request with a positive whole-device count.
An empty model leaves the scheduler to select a model. Real device execution
requires a suitable worker and is a separate pending validation gate; see the
[whole-device acceptance](../../../../build/e2e/device/README.md).

Temporary writable storage is specified in MiB:

```python
Environment(storage_mb=153600, storage_limit_mb=153600)
```

`storage_limit_mb=0` uses `storage_mb`, or the cluster default when
`storage_mb` is omitted. A nonzero limit cannot be below `storage_mb`. The
request controls scheduling while the limit is passed separately to sandboxd's
writable-layer enforcement.

## Rootfs and mounts

`rootfs=S3Config(...)` starts from an S3-compatible EROFS root filesystem.
`Mount` supports image-backed read-only bind mounts, S3-backed bind or EROFS
mounts, and Local bind mounts through `host_path`. Local mounts accept `options`
(`ro` by default, or `rw`, plus `nosuid`/`nodev`/`noexec`) and `create_if_missing`
for leaf-directory creation. Optional `create_options=CreateOptions(uid=10001, gid=10001, mode="0700")` requires `create_if_missing=True` and sets ownership/permissions only for newly created directories; existing directories remain unchanged. IDs must be integers from 0 to 4294967294; mode is a four-digit octal string from `0000` to `0777`. Import `CreateOptions` from `adx_environment`. The shared filesystem must already be mounted in
the worker runtime namespace. For example, an SFS/MooseFS user workspace uses
`Mount(target="/home/jiuwen", host_path="/home/agentos/users/user-1",
options=("rw",), create_if_missing=True)`. Credentials are sent to the control plane and sandboxd; callers should
use scoped object-store credentials. EXECD is still supplied by the deployment's
local runtime environment, so a custom rootfs does not need to bake in ADX.

The deployment Runtime Environment is the rootfs baseline. `runtime=` overrides
only its isolation runtime. `rootfs_readonly=True` or `False` overrides only its
read-only setting; leaving it as `None` inherits the deployment default. Passing
`image=` or `rootfs=S3Config(...)` replaces the source atomically while omitted
runtime/read-only fields continue to inherit. Runtime-only and read-only-only
overrides keep the baseline source and do not add the bootstrap mount.

## Network policy models

`NetworkPolicy`, `NetworkRule`, and `PortRange` are accepted at creation.

Deny-all policies retain platform rules for Execd and declared public TCP
ports. Stateful mode uses connection tracking for replies; stateless mode
matches those local service ports in both directions. When runtime control is
configured, a separate rule preserves the node's exact control IPv4/TCP
endpoint. Ordinary traffic remains subject to the user policy; see the
[node network contract](../../../../docs/deployment/runtime-control-stream.md).
`update_network_policy(policy)` atomically replaces the complete runtime policy;
passing `None` clears it. Adxlet reserves the EXECD control port and declared
published ports with the highest rule priority so a user default-deny policy
cannot cut the control route. User priorities must be in `1..UINT32_MAX-1`.
Enforcement is provided by sandboxd's network policy implementation.

Reverse tunnels also retain their server-owned WS listener (default 8765;
`upstream` HTTP port minus one). The HTTP proxy remains guest-loopback only.
Tunnel ports cannot overlap the Execd control or published ports, and caller
`EXECD_TUNNEL_*` environment overrides are rejected. With a traffic policy,
reverse tunnels require stateful mode; stateless combinations are rejected at
creation and replacement. After a successful policy update, an Environment
that owns a tunnel actively retires its current WS transport. The existing
reconnect loop retains the session and resumable requests while opening a fresh
connection. Policy-update completion does not wait for tunnel readiness; retry
application requests during recovery. Updates through another client or raw HTTP
are detected by the existing application heartbeat (20-second interval and
10-second pong timeout by default). Only tunnel ingress is protected, so its
local source port does not authorize new outbound connections.


## Connection configuration

Pass an immutable `ConnectionConfig` to avoid process-global environment
configuration:

```python
from adx_environment import ConnectionConfig, Environment, resources

connection = ConnectionConfig(
    server_address="frontend.example.com:443",
    token="<token>",
    use_tls=True,
    gateway_address="gateway.example.com:443",
    gateway_use_tls=True,
    communication_margin_seconds=30,
    request_max_attempts=3,
)

with Environment(image="python:3.12-slim", connection=connection) as environment:
    print(environment.id)

nodes = resources(connection=connection)
Environment.delete("environment-id", connection=connection)
```

`resources()` reports scheduling capacity and remaining allocatable resources.
Online nodes that are not accepting allocations remain in the result with
status 1, zero CPU/Memory/Disk resources and no GPU/NPU resource entries.
Existing allocations remain held; this view does not report actual utilization
or the rated capacity of admission-disabled nodes.

Without a `ConnectionConfig`, the SDK reads `ADX_SERVER_ADDRESS`, `ADX_TOKEN`,
`ADX_TLS`, `ADX_GATEWAY_ADDRESS`, `ADX_GATEWAY_TLS`,
`ADX_COMMUNICATION_MARGIN_SECONDS`, and `ADX_REQUEST_MAX_ATTEMPTS`.
The communication margin must be finite and between 0 and 3600 seconds;
request_max_attempts must be a positive integer. The gateway address
defaults to the frontend address for reverse-tunnel and user-port routes. PTY
uses the Ingress data-plane route when a gateway address is configured, and falls
back to the server address for combined deployments.

## Build and test

From this directory:

```bash
PYTHON=python3 bash build.sh /tmp/adx-environment-dist
PYTHONPATH=. python3 -m unittest discover -s tests/unit
```

The SDK version is defined once in `pyproject.toml`, matching the AKernel SDK
packaging model. Change `[project].version` when publishing a new SDK release;
the build does not infer or override it from repository tags or environment
variables.

From the monorepo root, the SDK wrapper does the same:

```bash
PYTHON=python3 bash platform/sdk/environment/build.sh /tmp/adx-environment-dist
```

Live K8S/frontend checks also need `ADX_SERVER_ADDRESS`,
`ADX_GATEWAY_ADDRESS`, and a valid token:

```bash
PYTHONPATH=. python3 tests/e2e_execd_direct.py
PYTHONPATH=. python3 examples/reverse_tunnel.py
```

## Runnable examples

Examples are client demonstrations. Basic commands/files are supported; tunnel and user-port examples require matching published routes and are not part of the new basic K8s acceptance:

- `examples/basic_usage.py`
- `examples/command_stdin.py`
- `examples/persistent_shell.py`
- `examples/tunnel_large_response.py`
- `examples/port_forwarding.py`
- `examples/reverse_tunnel.py`
- `examples/named_environment.py`
- `examples/bench_cp.py`
- `examples/recoverable_command.py`

Infra-specific demos should be documented separately instead of being shipped as
runnable SDK examples.

## Architecture

- **Control plane** — environment v1 create, delete, lifecycle, and invoke routes.
- **Direct data plane** — frontend/gateway `/direct/{environment}/...` routes for
  command invoke and binary file I/O.
- **Reverse tunnel** — gateway `/tunnel/{environment}` WebSocket back to a local
  upstream (`adx_environment/tunnel_client.py`).

See [`TODO.md`](TODO.md) for remaining SDK work.

## File listing and concurrent transfers

`files.list()` raises `FileNotFoundError`, `NotADirectoryError`, or `PermissionError`
when Execd returns the corresponding typed filesystem error. The exception includes
both the requested path and the original HTTP error. Other failures remain SDK errors.
Traversal depth is bounded at 64 by Execd.

Direct file uploads open the destination in place and truncate it before writing.
Existing symlinks are followed, including links to a missing target. Overwriting
retains the existing inode, ownership and hard-link identity, with normal OS
write-permission checks and mode semantics. An interrupted upload may leave partial data; concurrent readers may observe empty or partial
content and concurrent writers require application coordination.

Each file download reads metadata and bytes from one opened descriptor. Full
responses use chunked framing without a fixed Content-Length, so an in-place
truncation can return fewer bytes with a valid ending chunk. A transfer interrupted
before that chunk remains an error, allowing the SDK to retry or resume. Reads are
bounded by the initial file size during concurrent appends. Range downloads retain their
fixed-length resume contract and require a stable file; they do not provide a
snapshot of a concurrently modified file. Downloads accept regular files only.

## Native filename/path search

```python
matches = environment.files.search(
    "/workspace", "*.py", exclude_patterns=["test_*", "*/.git/*"],
    depth=64, max_results=1000, max_entries=100000, timeout_ms=10000,
)
for entry in matches:
    print(entry.path)
```

This invokes Execd's `file.search` action in the Environment. Traversal and matching
run in Execd; the SDK receives matching `EntryInfo` values in full-path order and
does not fetch the directory tree to filter locally. Search matches names/paths,
not file contents. Case-sensitive globs support `*`, `?`, `[abc]`, `[a-z]`, and
`[!abc]`; `*` can match separators and leading dots. Braces and backslashes are
literal, unmatched `[` is literal, and invalid character ranges are rejected.
Both the basename and full path are tested. Exclusions filter results, so excluding
a directory's basename alone does not exclude its descendants. Descendant directory
symlinks are never followed. Missing roots and non-directory roots return `[]`;
permission/I/O failures remain errors.

| Parameter | Default | Allowed range |
| --- | --- | --- |
| `depth` | 64 | 1–64 listing levels, including the root listing |
| `max_results` | 1000 | 1–10000 matches |
| `max_entries` | 100000 | 1–1000000 inspected entries |
| `timeout_ms` | 10000 | 1–30000 ms, checked between filesystem operations |

`path` is a nonempty string of at most 4096 UTF-8 bytes. Patterns are at most 1024
UTF-8 bytes, with at most 64 exclusion patterns. A conservative 4 MiB response
budget also bounds result metadata. Exceeding a match/scan/response budget returns
HTTP 429 with `error_code=RESOURCE_EXHAUSTED`; deadline expiry returns HTTP 408
with `error_code=DEADLINE_EXCEEDED`. Invalid parameters/patterns return HTTP 400
with `error_code=INVALID_ARGUMENT`. Errors carry an empty `entries` array and never
masquerade as successful partial results. SDK callers receive `EnvironmentHTTPError`
and can inspect `error.payload["error_code"]`. Filesystem errors retain their
`FILE_NOT_FOUND`, `PERMISSION_DENIED`, `NOT_A_DIRECTORY` or `FILESYSTEM_ERROR` code.

The HTTP contract is `POST /invoke` with
`{"action":"file.search","args":{"path":"/workspace","pattern":"*.py"}}`.
Success contains `entries`, `scanned_entries`, and `error:null`. Use an Execd build
that implements this action together with the matching SDK; unsupported runtimes
fail explicitly.

## Recover a background command

Background commands run without an execution deadline by default. Pass
`timeout` to set one explicitly; foreground commands default to 60 seconds.

Command output is bounded. The API Server supplies defaults of 4 MiB per stdout/stderr
stream. Operators can configure `ADX_EXECD_COMMAND_STDOUT_LIMIT_BYTES` and
`ADX_EXECD_COMMAND_STDERR_LIMIT_BYTES` on the API Server; these override request
Environment variables. `CommandResult.truncated`, `stdout_truncated`, and
`stderr_truncated` indicate retained prefixes. To retrieve larger output reliably,
redirect it to a file and download that file. Increasing the stream limits retains
the independent command-registry memory budget.

The SDK process does not persist command handles. Generate a command ID and
persist the `(environment_id, command_id)` pair in the caller's own database before
submission, then bind fresh handles after a restart:

```python
from adx_environment import Environment

environment = Environment.from_id(saved_environment_id)
command = environment.commands.run(
    "python train.py",
    background=True,
    command_id=saved_command_id,  # generate and persist before submission
)

# In a later SDK process:
environment = Environment.from_id(saved_environment_id)
command = environment.commands.get(saved_command_id)
result = command.wait()
```

`wait()` and `wait_async()` use one hidden multiplexed WebSocket per connection
context as a notification channel. The final result is always read back from
EXECD's authoritative command registry. When the wait deadline expires, they
return `CommandResult(status=RUNNING, error_code="WAIT_TIMEOUT")`; the remote
command remains active and the handle can wait again. A client restart also
does not terminate the remote command. `CommandHandle.kill()` and
`environment.commands.kill()` return `False` when the command is absent or already
finished, and still raise on other execution or transport failures.
Long-running command polling retries connection failures and errors marked
retryable by the server. Terminal errors keep their original error and request ID.
Temporary downstream subscription failures (including a checkpoint freeze)
reconnect the watch for the same command, without starting it again. The SDK
uses a 30-second continuous-outage budget; an Ingress WebSocket handshake alone
does not reset it. Receipt of an actual PENDING/RUNNING or terminal command state
resets the budget. Authorization and permanent subscription errors fail directly.


### Placement

`Environment(..., labels={"app": "worker"}, schedule_affinities=[...])` exposes the
platform's grouped node and same-tenant instance placement rules. Conditions
use the public `scheduleAffinities` shape with `kind`, `affinity`, `labelOps`,
optional `weight` and `preferredPriority`. `node_id` remains a hard requirement
when combined with alternatives. See [placement rules](https://gitcode.com/openJiuwen/agent-dx/blob/refactor/docs/testing/http-node-placement.md)
for the matching, ranking and label contracts. Caller-provided option lists are
copied before the SDK adds a node constraint.


## Tool execution identity

`Environment(image="tools:v1", runtime="conch", user="1001", group="1002")`
selects the POSIX identity used to launch Execd. Commands, native file operations,
and files created on Local/MooseFS mounts use that identity. Supply both fields
as POSIX names or decimal IDs; Conch resolves names on the worker. The shared
filesystem must already grant that UID/GID access; ADX does not recursively chown
mounted data. Omitted fields preserve the runtime deployment's identity.

This override currently requires Conch and `python3` in the guest template's PATH
for its native privilege-drop wrapper. Other runtime backends reject it. It
cannot be combined with `inherit_entrypoint=True` or a snapshot identity override;
image entrypoint `process.user` retains its separate meaning. Use matching API
Server, Coordinator, adxlet, and SDK versions so the typed identity is preserved
across the complete create path. Unknown account resolution fails creation.

## Managed Volumes

```python
from adx_environment import DirectoryVolumeSource, Environment, Mount, VolumeSpec

volume = Environment.ensure_volume(VolumeSpec(
    id="workspace-alice",
    source=DirectoryVolumeSource(storage_domain="sfs", path="workspaces/tenant-a/alice"),
    access_mode="exclusive",
    create_if_missing=True,
))
mount = Mount(target="/workspace", volume_id=volume.spec.id, volume_uid=volume.uid, options=("rw",))
```

Pass the mount to Environment creation as usual. Default access is shared; ensure
returns the existing definition unchanged. `Environment.get_volume(id)`,
`Environment.list_volumes(page_size=100)` and `Environment.delete_volume(id)` use
the same connection configuration. List returns `(items, next_page_token)`.
Deletion requires no references, owners or occupancy and retains directory data.
References cannot mix inline sources or leaf-creation options. AgentBinding
ownership is managed by Activator; SDK-created definitions without an owner use
explicit deletion. See [Volume API](../../../api/http/volumes.md) for topology,
initialization status and the accepted disconnected-writer policy. Real shared
storage/runtime acceptance remains in P7.

Volume IDs accept 1–256 ASCII letters, digits, dots, underscores or hyphens;
`.` and `..` are rejected because URL clients normalize those path segments.
The SDK validates both definitions and lookup/deletion IDs before sending requests.
