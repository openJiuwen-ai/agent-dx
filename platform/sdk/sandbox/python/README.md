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
| Reusable Snapshot | `create_snapshot()` then `Sandbox.create()` | Immutable, durable artifact. A clone is scheduled on a fresh target and can restore across nodes. | Create many independent sandboxes from a prepared source. |
| Pause / resume | `pause()` then `resume()` | Durable paused-sandbox artifact. Resume is scheduled on a fresh target; it is not tied to the source node. | Temporarily stop one logical sandbox and later continue it. |
| Local recovery | `failover=True` or `reload()` | Latest local recovery candidate on the owning node only. Internal checkpoints and Pause-created artifacts can both carry the candidate flag; it is never a reusable or cross-node artifact. | Recover a running sandbox on its original node. |

The SDK is a client-side validation, request-ID, attempt, and result-shaping
layer. Frontend and FunctionSystem own lifecycle state, scheduling, checkpoint
bytes, and the final READY/PAUSED/RUNNING transition.

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
    cpu=2000,
    memory=8192,
)
```

The backend reuses the source template and immutable artifact only after it
validates Snapshot/template compatibility. CPU and memory resource *presence*
controls inheritance: an omitted or non-positive `cpu` or `memory` copies the
corresponding complete source resource, including its stored limit. A positive
`cpu` or `memory` creates a target resource and replaces that source resource.
Positive `cpu_limit` or `mem_limit` sets a limit on the new target resource;
there is no independent limit-presence inheritance switch, so an omitted value
or default `0` cannot independently request or suppress a template limit.
Options that affect the restored template must remain compatible. The current
server applies the source template's create options during restore but does not
independently reject every source/target reverse-tunnel mismatch. Callers must
therefore create tunnel-enabled clones from a template with the same tunnel
shape; otherwise the returned route may not correspond to a provisioned
tunnel. The clone has a fresh logical identity and fresh scheduler placement
according to its create request. A reusable Snapshot is not consumed. The
backend rejects a non-READY or unavailable Snapshot and a resource-type change.

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
include a route address and function-proxy ID. Resume restores the durable
paused artifact on a scheduler-selected target, rearms the restored runtime's
listener, and then commits the RUNNING result. Route-cache convergence after
that result is outside the resume success boundary.

Pausing creates durable remote bytes and transitions the logical sandbox to
PAUSED; it clears the old runtime/container/endpoint. Resuming requires the
authoritative PAUSED identity and its durable Snapshot. It can run on a
different node from the pre-pause sandbox.

## Failover and reload are local recovery

`failover` is a creation option, not an imperative lifecycle method:

```python
sandbox = Sandbox(image="python:3.12-slim", failover=True)
```

With `failover=True`, FunctionSystem may recover a suitable running sandbox
only from its latest local recovery candidate. Recovery stays on the owning
Proxy/Agent/node and does not use reusable Snapshot metadata. The selector
filters the durable `localRecoveryCandidate` flag rather than a separate
internal-only type, so both internal checkpoints and Pause-created artifacts can
be marked and selected as candidates. A missing candidate is a recovery failure: it never
starts a replacement from scratch or silently discards state. Failure handling
depends on the source state: a current RUNNING sandbox becomes FATAL, while an
EVICTED sandbox continues ghost-cleanup reconciliation.

For an explicit local-recovery request, use:

```python
ok = sandbox.reload()
```

`Sandbox.reload() -> bool` does not replace the `Sandbox` object or any of its
command, filesystem, shell, or PTY facades. It restores the same logical
sandbox using the local recovery path with failover policy disabled, and it
requires an existing latest local recovery candidate under the same flag-based
selection rule. A missing candidate
fails; reload never starts a replacement from scratch or silently discards
state. The HTTP operation returns `{"success": true|false}`. The SDK returns
`False` for a closed sandbox or `SandboxError`. If FunctionSystem has already
stopped the source and recovery then fails, treat `False` as a failed recovery;
current FunctionSystem cannot restore that source automatically.

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
  and one `create-*` identity. An unnamed attempt that reaches another frontend
  can make an extra sandbox, so give important clones a name and reconcile
  uncertain outcomes.

Transport and business failures, malformed Snapshot identity results, and
exhausted attempt budgets raise `SandboxError`. Other malformed typed-result
shapes can instead surface `ValueError` or `TypeError` while values are
converted, or `RuntimeError` when resume `portMappings` is not an object.
`reload()` translates `SandboxError` into `False`; it does not normalize those
other programming/shape exceptions.

### SDK versus raw HTTP

These are SDK semantics, not a substitute for the frontend REST contract.
The SDK uses these paths internally:

```text
POST /api/sandbox/v1/sandboxes/{id}/snapshots
POST /api/sandbox/v1/sandboxes                 # create from Snapshot: snapshotId
POST /api/sandbox/v1/sandboxes/{id}/pause
POST /api/sandbox/v1/sandboxes/{id}/resume
POST /api/sandbox/v1/sandboxes/{id}/reload
```

Raw HTTP has intentionally different validation in a few places. A raw
Snapshot request has `name` as its handler field: an omitted or empty name is
accepted, while a supplied whitespace-only name is rejected. Its
`timeoutSeconds` defaults to 300, is validated in the range 1 through 3600,
converted to milliseconds, and forwarded as the checkpoint/direct-proxy
logical timeout. Raw Pause applies the same `timeoutSeconds` contract, while
treating omitted or zero `ttlSeconds` as 90,000 seconds and rejecting negative,
malformed, or non-numeric JSON values; the SDK is deliberately stricter about
TTL. Only Snapshot and Pause currently accept this caller-provided logical
timeout body field. The SDK sends `{}` for resume and reload, and their
handlers define no body fields. Raw HTTP requires a pattern-valid
`X-ADX-Request-ID` header;
the SDK generates that header internally.

The legacy runtime surface in `api/python/yr` is separate from this SDK:
`snapshot_instance(instance_id, ttl=-1, leave_running=False, function_type="")`,
`snapstart_instance(checkpoint_id)`, and `reload_instance(instance_id)` are
lower-level signal-18/19/25 primitives. They are not `Sandbox` result aliases.

## Other create options

Request whole GPUs with `type:model:count`:

```python
Sandbox(xpu="gpu:l20:1")
Sandbox(xpu="gpu:h100:2")
Sandbox(xpu="gpu::1")  # any GPU model
```

The SDK currently accepts one whole-device `gpu` request with a positive count.
An empty model leaves FunctionSystem to select a model.

Temporary writable storage is specified in MiB:

```python
Sandbox(storage_mb=153600, storage_limit_mb=204800)
```

`storage_limit_mb=0` uses `storage_mb`, or the cluster default when
`storage_mb` is omitted. A nonzero limit cannot be below `storage_mb`.

## Network policies

Creation-time network policies are optional and can be replaced at runtime:

```python
from adx_sandbox import NetworkPolicy, NetworkRule, PortRange, Sandbox

Sandbox()  # unrestricted; no policy is sent
Sandbox(network=NetworkPolicy.block())
Sandbox(
    network=NetworkPolicy.deny_dns("github.com", "*.github.com"),
)
Sandbox(
    network=NetworkPolicy.allowlist(
        [
            NetworkRule(domain="api.github.com", protocol="tcp", port_range=443),
            NetworkRule(cidr="10.0.0.2/32", protocol="tcp", port_range=22773),
            NetworkRule(
                cidr="192.0.2.0/24",
                protocol="tcp",
                port_range=PortRange(8000, 8010),
            ),
        ]
    )
)
```
A running sandbox accepts whole-policy replacement through
`update_network_policy`:

```python
sandbox = Sandbox()
sandbox.update_network_policy(NetworkPolicy.block())
sandbox.update_network_policy(
    NetworkPolicy.deny_dns("github.com", "*.github.com")
)
sandbox.update_network_policy(None)  # clear and restore unrestricted networking
```

The desired policy survives sandboxd restarts, explicit reloads, and same-node
failover. Each call is atomic at the sandbox data plane.

Block mode denies new network flows except the adx control proxy and
published sandbox target ports used by frontend direct file I/O, reverse
tunnels, and explicit user port forwarding. Replies to allowed TCP, UDP, and
related ICMP traffic are admitted from connection state. Commands and the
frontend `/direct` filesystem path remain available; bounded RuntimeRPC chunks
remain a fallback when direct transport fails.

DNS patterns match either one exact name or descendants with a leading
`*.`; the wildcard does not match the apex. Patterns are lowercased and
trailing dots are removed. International names are normalized to ASCII
punycode. DNS-over-HTTPS and direct connections to a known IP are not covered
by a DNS blacklist.

The generic schema v2 API combines IPv4/CIDR and dynamic domain rules with
TCP, UDP, ICMP, peer or sandbox port ranges, priorities, independent ingress
and egress defaults, and stateful or stateless handling. Higher priorities win;
at equal priority, deny wins. Exact domains match only themselves. A leading
`*.` matches descendants but not the apex. Domain authorization follows the
complete CNAME chain, uses a DNS TTL clamped to 1..3600 seconds, and is replaced
only by later IPv4 A or ANY answers rather than parallel AAAA queries.
`NetworkPolicy.allowlist` defaults to ingress allow, egress deny, and stateful
replies. A DNS policy or domain rule forces ordinary TCP and UDP DNS through
sandboxd's managed resolver; allowing another port-53 endpoint does not bypass
it. Construct `TrafficPolicy`, `DNSPolicy`, and `DNSRule` directly for the
low-level model. Domain grants enforce at IPv4 and transport layers, so another
virtual host sharing an authorized address and port is not distinguishable.
With stateless mode, protected published-port rules are ingress-only; callers
must add the matching egress sandbox-source-port rule when replies are needed.
This keeps a published port from becoming a generic egress bypass.

Legacy `block_network` and `dns_blacklist` cannot be combined with each other
or with schema v2 sections. Policy updates replace the complete policy. Packet
ACLs are IPv4 and support IPv4 fragments. The target sandboxd
node drops IPv6 traffic whenever a traffic or DNS policy is active. Arbitrary
non-IP Ethernet protocols are outside the portable ACL contract. The node must
have network ACL support enabled, and existing sandboxes must be drained before
an operator enables it.

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
defaults to the frontend address for reverse-tunnel, user-port, and PTY routes.

## Build and test

From this directory:

```bash
PYTHON=python3 bash build.sh /tmp/adx-sandbox-dist
PYTHONPATH=. python3 -m unittest discover -s tests/unit
```

The parent adx build passes `BUILD_VERSION` so this wheel has the same
version as the control-plane component wheels. Standalone release builds may
set it explicitly, for example `BUILD_VERSION=0.10.0`; an
`ADX_RELEASE_TAG`/`BUILDKITE_TAG` still takes precedence.

From the repository root, the build wrapper does the same:

```bash
PYTHON=python3 bash ../build.sh /tmp/adx-sandbox-dist
```

Live K8S/frontend checks also need `ADX_SERVER_ADDRESS`,
`ADX_GATEWAY_ADDRESS`, and a valid token:

```bash
PYTHONPATH=. python3 tests/e2e_rrt_direct.py
PYTHONPATH=. python3 examples/reverse_tunnel.py
```

## Runnable examples

Only examples expected to run in ordinary SDK/K8S smoke environments are kept:

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
RRT's authoritative command registry. A local wait timeout or client restart
does not terminate the remote command.
