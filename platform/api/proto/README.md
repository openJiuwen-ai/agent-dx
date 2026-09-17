# Protocol boundaries

Internal component protocols are designed for the Instance architecture. Do not
extend the old POSIX, function, or generic signal services for new functionality.

| Contract | Responsibility | Status |
|---|---|---|
| `control.proto` / `adx.control.v1` | Master/Node registration, allocation and node-owned state submission | mTLS service processes, Redis discovery/state, route publication |
| `node.proto` / `adx.node.v1` | Versioned Node Proxy bindings and node activity snapshots | UDS services, activity reporting and both Node Proxy process modes wired |
| RRT HTTP | Commands, files, health and command observation; HTTP/WebSocket tunnel transport | Existing implementation retained and tested |
| Node Manager/RRT HTTP control | Identity-aware status, checkpoint preparation/abort and restored listener setup | Runtime controller and Node Manager client implemented; see [HTTP contract](../http/runtime-control.md) |
| `legacy/frontend/frontend_proxy_service.proto` | Frontend compatibility entrypoint | The sole retained legacy gRPC service contract |

Frontend compatibility still needs the message types imported by
`frontend_proxy_service.proto`, and `NotifyRequest` decoding used by the HTTP
adapter. These files supply compatibility payloads only: CoreService,
RuntimeService, RuntimeRPC and InvocationRPC service stubs are no longer generated
for Frontend. New internal protocols do not import these definitions.

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
Omitted instances are zero only in a fresh snapshot. No snapshot or an expired
snapshot means unknown, not idle. The session must be supplied by node process
registration/assembly; receiving activity never implicitly authorizes a session.

All changes here intentionally require matching component versions in the
unified release. Only the Frontend compatibility entrypoint carries a legacy
wire-compatibility requirement.

## Scheduling payloads

`control.proto` defines typed SchedulingPolicy selectors, hard/soft node and
Instance affinity, topology spread, device requests and physical allocations.
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
StartAssignedInstance and CommitInstance carry the destination/source node process
session to reject delayed requests from a superseded process. Full Assignment and
record revision checks remain mandatory. See [recovery contract](../../../docs/testing/recovery-discovery.md).

## Master route publication

`RouteService.WatchRoutes` is restricted to the `edge` mTLS identity. Each stream
begins with a full `RouteFrame` (`reset=true`); subsequent frames specify the
exact `base_revision`. Epoch identifies the Master storage session. A gap,
conflicting cursor or regressed execution version causes a fresh subscription.
Master publishes only committed, Running instances on routable nodes. API key
verification is shared by trusted Frontend and Edge callers. Route publication
and credentials do not transfer Instance lifecycle ownership to Master.
