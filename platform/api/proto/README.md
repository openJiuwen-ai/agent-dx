# Protocol boundaries

Internal component protocols are designed for the Capsule architecture. Do not
extend the old POSIX, function, or generic signal services for new functionality.

Public HTTP contracts are separate from these internal gRPC protocols:

- [Sandbox management OpenAPI](../openapi/sandbox.yaml)
- [Capsule data-plane OpenAPI](../openapi/data-plane.yaml)

| Contract | Responsibility | Status |
|---|---|---|
| `capsule.proto` / `adx.control.v1` | Master/Node registration, allocation and node-owned state submission | Direct typed RPC from API Server |
| `capsule_types.proto` | Shared Capsule, Assignment, resources and scheduling types | Shared by control services |
| `snapshot.proto` | Snapshot catalog, references and collection | Master and Node snapshot services |
| `credentials.proto` | API Key validation and administrator operations | AuthService and CredentialService |
| `routes.proto` | Versioned Capsule directory and committed route publication | CapsuleDirectoryService and RouteService |
| `node.proto` / `adx.node.v1` | Versioned Node Proxy bindings and activity | Node-local UDS services |
| RRT HTTP | Commands, files, health, checkpoint cooperation and tunnels | See [runtime contract](../http/runtime-control.md) |
| Management errors | Stable HTTP/gRPC/SDK code, retry and outcome semantics | See [error contract](../http/error-contract.md) |

The Rust API Server converts public HTTP JSON directly into Capsule RPC types.
All owned protobuf definitions are generated through the Cargo protocol crate.
The control files retain the `adx.control.v1` package. This internal rename is a
schema boundary: deployments must upgrade all components together and start
with a fresh Redis/SQLite control state. Public Sandbox HTTP fields such as
`instanceId` and `instance_id` remain compatible and are translated only by API Server.

RRT uses HTTP for operations and runtime cooperation. Its POSIX stream, signal
reporting, generated protobuf modules and protobuf build script have been removed.

The external sandboxd protocol is pinned upstream source under
`third_party/sandboxd`; it is an execution-backend dependency, not an internal
legacy interface owned by ADX.

## Node-local gRPC

`NodeProxyService.UpdateBinding` orders requests by ownership generation and
binding revision. An identical retry is acknowledged without reapplying. An old
request, or a changed payload at the same version, returns FAILED_PRECONDITION.
Retirement leaves a tombstone so a delayed activation cannot revive a binding.
The service retires the previous execution binding before installing a newer
one. Node Manager uses two binding revisions per state revision, reserving the
later one for cleanup when creation fails.

Node Proxy serves this service over the protected `route.sock` UDS; the old
custom length-prefixed protobuf framing is removed. Both embedded and standalone Node Proxy use the same `NodeProxyService` implementation and UDS control contract; process assembly is implemented.
`GetBindingState` returns a proxy process UUID and synchronization epoch.
`BeginBindings` compares that identity, advances the epoch, closes admission and
retires existing streams. `ReplaceBindings` validates the complete snapshot
before reopening admission. Updates carry both session fields, fencing delayed
requests across full sync and proxy restart. Node Manager detects proxy restarts
and replays its in-memory binding catalog; after its own restart it first obtains
the authoritative Master catalog.

`NodeActivityService.ReportSnapshot` sends complete snapshots to Node Manager at
`node-manager.sock`. Each report carries a registered proxy session ID and a
strictly increasing sequence. The receiver rejects foreign sessions and
conflicting duplicates; stale reports cannot overwrite newer observations.
Omitted Capsules are zero only in a fresh snapshot. No snapshot or an expired
snapshot means unknown, not idle. The session must be supplied by node process
registration/assembly; receiving activity never implicitly authorizes a session.

All changes here intentionally require matching component versions in the
unified release. Public Sandbox HTTP paths and response envelopes retain SDK compatibility.

## Scheduling payloads

`capsule_types.proto` defines typed SchedulingPolicy selectors, hard/soft node and
Capsule affinity, topology spread, device requests and physical allocations.
RegisterNode carries labels and healthy device inventory. Assignment carries
node-local GPU/NPU IDs and models; Node Manager checks these against the request
and local inventory before forwarding sandboxd `xpu_allocations`.

Conversions reject unknown enum values, missing constraint submessages, invalid
operands/weights and duplicate physical allocations. Empty scheduling policy
preserves scalar-only behavior. See [rule semantics](../../crates/scheduling/README.md).


### Node process session and reconciliation

RegisterNode includes the process session ID, a monotonic heartbeat sequence and
reconciliation flag. Duplicate reports do not extend liveness. InspectNode reads
the complete authoritative catalog only for the registered reconciling node.
StartAssignedCapsule and CommitCapsule carry the destination/source node process
session to reject delayed requests from a superseded process. Full Assignment and
record revision checks remain mandatory. See [recovery contract](../../../docs/testing/recovery-discovery.md).

## Master directory and route publication

`CapsuleDirectoryService.WatchCapsules` is restricted to the `api-server`
mTLS identity. A new stream starts with the complete retained Capsule
directory; later frames contain revision-linked upserts and deletions. API
Server rejects a revision gap or Master epoch change, clears the local view and
resubscribes for a new full frame. Normal lifecycle lookups use this local
directory. `MasterService.GetCapsule` remains a targeted read-after-write and
uncertain-result recovery operation.

`RouteService.WatchRoutes` is restricted to the `edge` mTLS identity. Each stream
begins with a full `RouteFrame` (`reset=true`); subsequent frames specify the
exact `base_revision`. Epoch identifies the Master storage session. A gap,
conflicting cursor or regressed execution version causes a fresh subscription.
Master publishes only committed, Running Capsules on routable nodes. API key
verification is shared by trusted API Server and Edge callers. Route publication
and credentials do not transfer Capsule lifecycle ownership to Master.
