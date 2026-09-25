# Agent DX Sandbox Python SDK

`adx-sandbox` is the high-level Python client for the frontend
**sandbox v1** API. It creates and manages sandboxes, including reusable
Snapshots and a paused-sandbox lifecycle. It also provides the gateway routes
used for reverse tunnels.

```python
from adx_sandbox import Sandbox

with Sandbox(image="python:3.12-slim", cpu=2000, memory=4096) as sandbox:
    sandbox.files.write("/tmp/hello.txt", "hello world")
    print(sandbox.commands.run("cat /tmp/hello.txt").stdout)
```

`close()` releases local SDK resources and leaves the remote sandbox alive.
`kill()` deletes a non-detached remote sandbox as well.
The context manager explicitly calls `kill()` on exit; garbage collection does
not make a remote delete request. If the block raises, its original exception
remains visible even when cleanup also fails.

## Server compatibility

The new Environment backend supports basic lifecycle, same-node pause/resume, reusable snapshots, grouped placement, idle deletion and restart policy. It requires an image containing the release EXECD at the configured command path; generic `python:3.12-slim` below is only an illustrative image name.

The Environment backend supports S3 rootfs, S3/image mounts, entrypoint inheritance,
creation and runtime network policy, `extra_config`, `failover=True`, independent
resource limits, and per-sandbox data-plane security. Declared user ports use
Ingress and Relay routing. Public local rootfs paths and host mounts are not a
tenant-facing contract. `upstream` reverse tunnel uses the published
`/tunnel/{sandbox}` route; the legacy `/invoke` fallback remains unavailable. See the
[current HTTP contract](https://gitcode.com/openJiuwen/agent-dx/blob/refactor/gateway/apiserver/docs/sandbox-lifecycle-api.md).

## Image startup process

Set `inherit_entrypoint=True` on a fresh image-backed sandbox to start the
image's effective `Entrypoint` and `Cmd` as its managed workload:

```python
sandbox = Sandbox(image="example/worker:latest", inherit_entrypoint=True)
exit_code = sandbox.wait_entrypoint()
print(exit_code, sandbox.entrypoint_exit_info)
```

Creation fails when the image has no effective startup command or when that
process exits before sandbox initialization completes. After a successful
create, its terminal status remains available through bounded polling while
the sandbox and its other APIs remain usable.

## Lifecycle overview

There are three deliberately different checkpoint paths:

| Path | Public SDK API | Artifact and placement | When to use it |
| --- | --- | --- | --- |
| Reusable Snapshot | `create_snapshot()` then `Sandbox.create()` | New Environment identity; shared storage allows fresh placement, local-only pins the source node. | Independent clones from a prepared source. |
| Pause / resume | `pause()` then `resume()` | Same Environment ID; public resume calls its owning Adxlet. | Stop and resume one logical sandbox. |
| Failure recovery | `failover=True` | Same Environment and node; restores the latest unexpired checkpoint after unexpected backend exit. | Workloads that must recover execution state rather than cold-start. |
| Explicit reload | `reload()` | Same Environment; replaces a Running backend from its latest unexpired checkpoint. | Operator-requested reset to a known recovery point. |

The SDK is a client-side validation, request-ID, attempt, and result-shaping
layer. Adxlet owns lifecycle and checkpoint bytes; Coordinator owns placement, committed state and the snapshot catalog.

## Reusable Snapshots

Create a reusable Snapshot from an open sandbox:

```python
from adx_sandbox import Sandbox

source = Sandbox(image="python:3.12-slim", name="source")
snapshot = source.create_snapshot(name="python-ready")

# The source stays running. `snapshot` is a SnapshotInfo value.
clone = Sandbox.create(snapshot, name="worker-1")

clone.kill()
source.kill()
```

The public signature is:

```python
Sandbox.create_snapshot(*, name=None, timeout_seconds=300) -> SnapshotInfo
```

`name` is optional, but if supplied it must be a nonblank string.
`timeout_seconds` must be an integral, non-boolean value from 1 through 3600.
The result is a frozen `SnapshotInfo(snapshot_id: str, names: tuple[str, ...])`.
The Snapshot has no TTL: delete it explicitly when it is no longer needed.

```python
same_snapshot = Sandbox.get_snapshot(snapshot.snapshot_id)
snapshots, next_page_token = Sandbox.list_snapshots(
    name="python-ready", page_size=20,
)
Sandbox.delete_snapshot(same_snapshot.snapshot_id)
```

The source must have a valid lifecycle identity. In particular, a sandbox with
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

Use `Sandbox.create(snapshot_id, **kwargs)` with either a `SnapshotInfo` or a
Snapshot ID string. It performs the normal sandbox create request with a
`snapshotId`, so the result is a new `Sandbox` handle that reaches RUNNING in
the usual way.

```python
clone = Sandbox.create(
    snapshot,
    name="worker-2",
    # Omitted resources inherit the snapshot; explicit values must match it.
)
```

The new Coordinator inherits omitted image/runtime/scalar resources and validates explicit values against the source geometry. It does not resize a restored VM. Environment overrides and placement constraints are carried into the new Environment; local-only snapshots require the source node. Shared snapshots use normal scheduling. The source briefly pauses during snapshot creation and resumes before success. A reusable snapshot is not consumed by cloning; deletion blocks new references and waits for existing references to be released. See [storage and cloning](https://gitcode.com/openJiuwen/agent-dx/blob/refactor/docs/testing/snapshot-storage.md).

Later dual-clone FC runs exposed a network failure, tracked in the [investigation](https://gitcode.com/openJiuwen/agent-dx/blob/refactor/docs/testing/2026-09-16-fc-clone-network.md); successful earlier batches do not close that issue.

## Pause and resume

Pause one open sandbox and later resume the same logical sandbox:

```python
sandbox = Sandbox(image="python:3.12-slim")

paused = sandbox.pause(ttl_seconds=90_000, timeout_seconds=300)
print(paused.snapshot_id, paused.expires_at)

running = sandbox.resume()
print(running.route_address, running.port_mappings)
```

The public signatures are:

```python
Sandbox.pause(ttl_seconds=90_000, *, timeout_seconds=300) -> PauseResult
Sandbox.resume() -> ResumeResult
```

The SDK accepts only a positive integral, non-boolean `ttl_seconds`; its
default is 90,000 seconds. `timeout_seconds` is keyword-only and must be an
integral, non-boolean value from 1 through 3600. A successful pause returns a
frozen `PauseResult` with `sandbox_id`, `snapshot_id`, byte `size`, `state`,
and `expires_at`. The SDK rejects a response unless it describes this sandbox,
has state `"paused"`, and includes a nonempty snapshot ID, positive size, and
positive expiry.

`resume()` returns a frozen `ResumeResult` with `sandbox_id`, `state`,
`route_address`, `function_proxy_id`, `node_id`, and `port_mappings`. The SDK
requires the response to identify this sandbox, report `"running"`, and
include a route address and function-proxy ID. Resume performs local admission on the owning node, restores the checkpoint, rearms the runtime listener and commits Running. Route-cache convergence after
that result is outside the resume success boundary.

Pausing persists bytes through the configured local or S3 store and commits Paused after removing the old execution. A SQLite-only result is not cluster success. Public resume requires the authoritative Paused identity and a valid checkpoint. Failed-node cross-node recovery with a shared checkpoint is a separate Coordinator coordinator path; it is not selected by the public resume call.

## Recovery methods

`failover=True` restores from the latest unexpired checkpoint after an
unexpected backend exit on the owning node. If no valid checkpoint exists, the
Environment becomes Failed and is not recreated from the original image.
`Sandbox.reload() -> bool` explicitly replaces a Running backend from that same
recovery point. It returns `True` only after the replacement reaches Running and
the result is durably published. Automatic restart policy is a separate cold
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

The SDK generates request IDs; callers of `Sandbox` cannot set them. Its
attempt rules intentionally differ by operation:

- Reusable Snapshot creation makes one SDK transport attempt only. A connection problem or
  gateway 502/503/504 produces `SandboxError` with an **uncertain** outcome,
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
- `Sandbox.delete()` treats HTTP 404 as an idempotent success. HTTP 403 raises
  `PermissionDenied` with the request ID and is not retried; transient transport
  or gateway failures retain one request ID across bounded retries.

Structured server failures raise `SandboxHTTPError`, whose `code`, `retry`,
`outcome`, `request_id`, `operation_id`, and `instance_id` fields implement the
[management error contract](https://gitcode.com/openJiuwen/agent-dx/blob/refactor/platform/api/http/error-contract.md). Transport
failures with an uncertain write result surface the same fields on
`SandboxError`. Other malformed typed-result
shapes can instead surface `ValueError` or `TypeError` while values are
converted, or `RuntimeError` when resume `portMappings` is not an object.
`reload()` translates `SandboxError` into `False`; it does not normalize those
other programming/shape exceptions.

### SDK versus raw HTTP

These are SDK semantics, not a substitute for the [frontend REST contract](https://gitcode.com/openJiuwen/agent-dx/blob/refactor/gateway/apiserver/docs/sandbox-lifecycle-api.md).
The SDK uses these paths internally:

```text
POST /api/sandbox/v1/sandboxes/{id}/snapshots
POST /api/sandbox/v1/sandboxes                 # create from Snapshot: snapshotId
POST /api/sandbox/v1/sandboxes/{id}/pause
POST /api/sandbox/v1/sandboxes/{id}/resume
POST /api/sandbox/v1/sandboxes/{id}/reload
PUT  /api/sandbox/v1/sandboxes/{id}/network
```

Raw HTTP has intentionally different validation in a few places. A raw
Snapshot request has `name` as its handler field: an omitted or empty name is
accepted, while a supplied whitespace-only name is rejected. Its
`timeoutSeconds` defaults to 300, is validated in the range 1 through 3600,
converted to milliseconds, and forwarded as the checkpoint RPC logical timeout. Raw Pause applies the same `timeoutSeconds` contract, while
treating omitted or zero `ttlSeconds` as 90,000 seconds and rejecting negative,
malformed, or non-numeric JSON values; the SDK is deliberately stricter about
TTL. Only Snapshot and Pause currently accept this caller-provided logical
timeout body field. The SDK sends `{}` for resume and reload, and their
handlers define no body fields. Raw HTTP requires a pattern-valid
`X-ADX-Request-ID` header;
the SDK generates that header internally.

Internal signal/POSIX/runtime SDK APIs are not part of this package or the new Environment protocol.

## Other create options

Request one kind of whole device with `type:model:count`:

```python
Sandbox(xpu="gpu:l20:1")
Sandbox(xpu="gpu:h100:2")
Sandbox(xpu="gpu::1")  # any GPU model
Sandbox(xpu="npu:ascend910b4:1")
```

The SDK accepts one `gpu` or `npu` request with a positive whole-device count.
An empty model leaves the scheduler to select a model. Real device execution
requires a suitable worker and is a separate pending validation gate; see the
[whole-device acceptance](../../../../build/e2e/device/README.md).

Temporary writable storage is specified in MiB:

```python
Sandbox(storage_mb=153600, storage_limit_mb=153600)
```

`storage_limit_mb=0` uses `storage_mb`, or the cluster default when
`storage_mb` is omitted. A nonzero limit cannot be below `storage_mb`. The
request controls scheduling while the limit is passed separately to sandboxd's
writable-layer enforcement.

## Rootfs and mounts

`rootfs=S3Config(...)` starts from an S3-compatible EROFS root filesystem.
`Mount` supports image-backed read-only bind mounts and S3-backed bind or EROFS
mounts. Credentials are sent to the control plane and sandboxd; callers should
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
`update_network_policy(policy)` atomically replaces the complete runtime policy;
passing `None` clears it. Adxlet reserves the EXECD control port and declared
published ports with the highest rule priority so a user default-deny policy
cannot cut the control route. User priorities must be in `1..UINT32_MAX-1`.
Enforcement is provided by sandboxd's network policy implementation.

## Connection configuration

Pass an immutable `ConnectionConfig` to avoid process-global environment
configuration:

```python
from adx_sandbox import ConnectionConfig, Sandbox, resources

connection = ConnectionConfig(
    server_address="frontend.example.com:443",
    token="<token>",
    use_tls=True,
    gateway_address="gateway.example.com:443",
    gateway_use_tls=True,
)

with Sandbox(image="python:3.12-slim", connection=connection) as sandbox:
    print(sandbox.id)

nodes = resources(connection=connection)
Sandbox.delete("sandbox-id", connection=connection)
```

Without a `ConnectionConfig`, the SDK reads `ADX_SERVER_ADDRESS`, `ADX_TOKEN`,
`ADX_TLS`, `ADX_GATEWAY_ADDRESS`, and `ADX_GATEWAY_TLS`. The gateway address
defaults to the frontend address for reverse-tunnel and user-port routes. PTY
uses the Ingress data-plane route when a gateway address is configured, and falls
back to the server address for combined deployments.

## Build and test

From this directory:

```bash
PYTHON=python3 bash build.sh /tmp/adx-sandbox-dist
PYTHONPATH=. python3 -m unittest discover -s tests/unit
```

The SDK version is defined once in `pyproject.toml`, matching the AKernel SDK
packaging model. Change `[project].version` when publishing a new SDK release;
the build does not infer or override it from repository tags or environment
variables.

From the monorepo root, the SDK wrapper does the same:

```bash
PYTHON=python3 bash platform/sdk/sandbox/build.sh /tmp/adx-sandbox-dist
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
- `examples/named_sandbox.py`
- `examples/bench_cp.py`
- `examples/recoverable_command.py`

Infra-specific demos should be documented separately instead of being shipped as
runnable SDK examples.

## Architecture

- **Control plane** — sandbox v1 create, delete, lifecycle, and invoke routes.
- **Direct data plane** — frontend/gateway `/direct/{sandbox}/...` routes for
  command invoke and binary file I/O.
- **Reverse tunnel** — gateway `/tunnel/{sandbox}` WebSocket back to a local
  upstream (`adx_sandbox/tunnel_client.py`).

See [`TODO.md`](TODO.md) for remaining SDK work.

## Recover a background command

Background commands run without an execution deadline by default. Pass
`timeout` to set one explicitly; foreground commands default to 60 seconds.

The SDK process does not persist command handles. Generate a command ID and
persist the `(sandbox_id, command_id)` pair in the caller's own database before
submission, then bind fresh handles after a restart:

```python
from adx_sandbox import Sandbox

sandbox = Sandbox.from_id(saved_sandbox_id)
command = sandbox.commands.run(
    "python train.py",
    background=True,
    command_id=saved_command_id,  # generate and persist before submission
)

# In a later SDK process:
sandbox = Sandbox.from_id(saved_sandbox_id)
command = sandbox.commands.get(saved_command_id)
result = command.wait()
```

`wait()` and `wait_async()` use one hidden multiplexed WebSocket per connection
context as a notification channel. The final result is always read back from
EXECD's authoritative command registry. When the wait deadline expires, they
return `CommandResult(status=RUNNING, error_code="WAIT_TIMEOUT")`; the remote
command remains active and the handle can wait again. A client restart also
does not terminate the remote command. `CommandHandle.kill()` and
`sandbox.commands.kill()` return `False` when the command is absent or already
finished, and still raise on other execution or transport failures.
Long-running command polling retries connection failures and errors marked
retryable by the server. Terminal errors keep their original error and request ID.


### Placement

`Sandbox(..., labels={"app": "worker"}, schedule_affinities=[...])` exposes the
platform's grouped node and same-tenant instance placement rules. Conditions
use the public `scheduleAffinities` shape with `kind`, `affinity`, `labelOps`,
optional `weight` and `preferredPriority`. `node_id` remains a hard requirement
when combined with alternatives. See [placement rules](https://gitcode.com/openJiuwen/agent-dx/blob/refactor/docs/testing/http-node-placement.md)
for the matching, ranking and label contracts. Caller-provided option lists are
copied before the SDK adds a node constraint.
