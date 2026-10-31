# Protocol boundaries

Internal component protocols are designed for the Environment architecture. Do not
extend the old POSIX, function, or generic signal services for new functionality.

Public HTTP contracts are separate from these internal gRPC protocols:

- [Coordinator autoscaling HTTP](../../../docs/deployment/coordinator-autoscaling.md)
- [Environment management OpenAPI](../openapi/environment.yaml)
- [Environment data-plane OpenAPI](../openapi/data-plane.yaml)

| Contract | Responsibility | Status |
|---|---|---|
| `environment.proto` / `adx.control.v1` | Coordinator/Node registration, allocation and node-owned state submission | Direct typed RPC from API Server |
| `environment_types.proto` | Shared Environment, Assignment, resources and scheduling types | Shared by control services |
| `snapshot.proto` | Snapshot catalog, references and collection | Coordinator and Node snapshot services |
| `volume.proto` | Tenant Volume definitions, references, node execution grants and initialization | Management: API Server identity; execution: owning node session |
| `credentials.proto` | API Key validation and administrator operations | AuthService and CredentialService |
| `routes.proto` | Versioned Environment directory and committed route publication | EnvironmentDirectoryService and RouteService |
| `node.proto` / `adx.node.v1` | Versioned Relay bindings and activity | Node-local UDS services |
| `runtime_control.proto` / `adx.runtime.v1` | Execd-initiated Ready/state events and node checkpoint commands | Opt-in [bidirectional control stream](runtime-control.md) |
| EXECD HTTP | Commands, files, health and tunnels; control when no stream is configured | See [HTTP runtime contract](../http/runtime-control.md) |
| Management errors | Stable HTTP/gRPC/SDK code, retry and outcome semantics | See [error contract](../http/error-contract.md) |

The shared types include Volume definitions (default `shared` access), fixed UID
references, and execution occupancy identities. User `MountSpec` selects either
an existing inline `StorageSource` or a `VolumeRef`; Rootfs retains its original
`StorageSource`. Coordinator implements internal ensure/get/list/delete RPCs;
ensure returns the existing tenant/ID definition unchanged. Deletion removes only
unreferenced, unowned, unoccupied metadata. Ensure can idempotently associate an
opaque owner; owner release follows confirmed Environment absence and removes only
its associations and unused last-owner definitions. It persists no deleted-owner
marker. Inspection includes initialization state and errors. Placement reserves exclusive occupancy atomically
with the execution assignment. Before start or restore, the owning node requests
an execution grant, verifies the configured mounted backend, and resolves fixed
Volume references into runtime-only paths. Shared first-use initialization has a
durable owner and result; interrupted or failed work requires repair. The original
Environment specification retains its Volume references. `CommitEnvironment`
carries confirmed stopped runtime IDs; releasing compute capacity alone cannot
release exclusive occupancy. Node heartbeat expiry atomically invalidates its
assignments and releases matching occupancy; interrupted initialization requires
repair. Public integration remains tracked in the [Volume implementation plan](../../../docs/development/platform-volume-design.md).

The Rust API Server converts public HTTP JSON directly into Environment RPC types.
All owned protobuf definitions are generated through the Cargo protocol crate.
The control files retain the `adx.control.v1` package. This internal rename is a
schema boundary: deployments must upgrade all components together and start
with a fresh Redis/SQLite control state. Public Environment lifecycle responses use `environmentId`; Python SDK and
AgentBinding identities use `environment_id`. Platform execution settings are
`EnvironmentSpec.execution` / `ExecutionOptions`. The existing `/api/instances`
directory retains its query parameters.

EXECD user operations use HTTP. Runtime cooperation uses an Execd-initiated
bidirectional gRPC stream when `adxlet.runtime_control` is configured; otherwise
it uses the HTTP control interface. Both transports share the same typed operations
and runtime state machine. The removed POSIX and signal services are not used.

The external sandboxd protocol is pinned upstream source under
`third_party/sandboxd`; it is an execution-backend dependency, not an internal
legacy interface owned by ADX.

## Node-local gRPC

`RelayService.UpdateBinding` orders requests by ownership generation and
binding revision. An identical retry is acknowledged without reapplying. An old
request, or a changed payload at the same version, returns FAILED_PRECONDITION.
Retirement leaves a tombstone so a delayed activation cannot revive a binding.
The service retires the previous execution binding before installing a newer
one. Adxlet uses two binding revisions per state revision, reserving the
later one for cleanup when creation fails.

Relay serves this service over the protected `route.sock` UDS; the old
custom length-prefixed protobuf framing is removed. Both embedded and standalone Relay use the same `RelayService` implementation and UDS control contract; process assembly is implemented.
`GetBindingState` returns a proxy process UUID and synchronization epoch.
`BeginBindings` compares that identity, advances the epoch, closes admission and
retires existing streams. `ReplaceBindings` validates the complete snapshot
before reopening admission. Updates carry both session fields, fencing delayed
requests across full sync and proxy restart. Adxlet detects proxy restarts
and replays its in-memory binding catalog; after its own restart it first obtains
the authoritative Coordinator catalog.

`NodeActivityService.ReportSnapshot` sends complete snapshots to Adxlet at
`adxlet.sock`. Each report carries a registered proxy session ID and a
strictly increasing sequence. The receiver rejects foreign sessions and
conflicting duplicates; stale reports cannot overwrite newer observations.
Omitted Environments are zero only in a fresh snapshot. No snapshot or an expired
snapshot means unknown, not idle. The session must be supplied by node process
registration/assembly; receiving activity never implicitly authorizes a session.

All changes here intentionally require matching component versions in the
unified release. Environment lifecycle paths are rooted at `/api/environment/v1/environments`.
The SDK, API Server, Ingress and Activator must use this same contract.

## Scheduling payloads

`environment_types.proto` defines typed SchedulingPolicy selectors, hard/soft node and
Environment affinity, topology spread, device requests and physical allocations.
RegisterNode carries labels, healthy device inventory and a complete `storage_mounts`
map of verified domain/backend/root reports. Missing mounts withdraw eligibility
for dynamically admitted storage domains. Reports use the existing authenticated
node session and heartbeat sequence; Coordinator refreshes queued Volume topology
and resolves execution roots from the same current view. Fixed domains retain
`node_roots` constraints. Storage access mode does not control topology. Assignment carries
node-local GPU/NPU IDs and models; Adxlet checks these against the request
and local inventory before forwarding sandboxd `xpu_allocations`.

Conversions reject unknown enum values, missing constraint submessages, invalid
operands/weights and duplicate physical allocations. Empty scheduling policy
preserves scalar-only behavior. See [rule semantics](../../crates/scheduling/README.md).


### Node process session and reconciliation

RegisterNode includes the process session ID, a monotonic heartbeat sequence and
reconciliation flag. Duplicate reports do not extend liveness. InspectNode reads
the complete authoritative catalog only for the current registered session with
allocation admission closed. Startup uses the reconciliation flag; a validated
live session closes allocation admission for periodic inspection while preserving
published routes. Unchanged local bindings keep their existing synchronization
session and active streams.
StartAssignedEnvironment and CommitEnvironment carry the destination/source node process
session to reject delayed requests from a superseded process. Full Assignment and
record revision checks remain mandatory. See [recovery contract](../../../docs/testing/recovery-discovery.md).

## Coordinator directory and route publication

`EnvironmentDirectoryService.WatchEnvironments` is restricted to the `apiserver`
mTLS identity. A new stream starts with the complete retained Environment
directory; later frames contain revision-linked upserts and deletions. API
Server rejects a revision gap or Coordinator epoch change, clears the local view and
resubscribes for a new full frame. Normal lifecycle lookups use this local
directory. `CoordinatorService.GetEnvironment` remains a targeted read-after-write and
uncertain-result recovery operation.

Coordinator seeds its publication catalog from Redis once at startup. A successful
object CAS updates the catalog and enqueues its typed value or deletion under one
short lock; unchanged replays do not republish old results. Each atomic batch has
one publication cursor. Recovery captures the in-memory snapshot, subscription
and watermark together; it does not scan Redis or wait for other writers to stop.
Both receive and batch-drain overflow reset this cursor. Ambiguous writes are
repaired by bounded, object-locked reads of the affected keys. Publication recovery
does not change node admission. The Coordinator runtime uses four worker threads;
deployments should budget four CPUs for sustained concurrent publication.

Redis requests that time out or are cancelled invalidate their transport lease.
Before replacement serves reads or repairs, it confirms that the old Redis client
has terminated; a delayed write cannot overtake repair on a new connection.
Healthy command-error responses retain the connection. Only safe reads retry at
the transport layer; mutation retries retain their existing identity and CAS.


`CoordinatorService.WatchNodes` publishes every live node with an explicit
`accepting_allocations` bit. API Server keeps the complete view for resource
queries and only rotates accepting nodes as local-first creation entries.
`GetSchedulingQueue` exposes the bounded in-memory ShardScheduler wait view,
while `SetNodeScheduling` persists an administrative pause that blocks new
assignments without changing existing Environment ownership. These internal RPCs
do not carry an end-user identity or restrict the internal component caller;
administrator authorization is enforced once at the public HTTP boundary, and
trusted internal controllers can call Coordinator directly.

`RouteService.WatchRoutes` is restricted to the `ingress` mTLS identity. Each stream
begins with a full `RouteFrame` (`reset=true`); subsequent frames specify the
exact `base_revision`. Epoch identifies the Coordinator storage session. A gap,
conflicting cursor or regressed execution version causes a fresh subscription.
Coordinator publishes only committed, Running Environments on routable nodes. API key
verification is shared by trusted API Server and Ingress callers. Route publication
and credentials do not transfer Environment lifecycle ownership to Coordinator.

## Durable checkpoint cleanup after deletion

A Deleted `CommitEnvironmentRequest` may include `cleanup_artifacts`. Coordinator
unions these with the previously persisted checkpoint and writes a durable cleanup
task atomically with Environment retirement. Nodes hand off physical artifact
removal after publication. `CollectCheckpointCleanup` is a Coordinator-only,
node-session-fenced RPC; its ACK identifies the Environment, generation and
Deleted revision. Coordinator compares the full persisted task before retiring
it. Missing objects are idempotent success; incomplete removal keeps the task.
A matching scoped shared collector may handle a retired source's objects; local
artifacts remain source-only. See the [cleanup contract](../../../docs/testing/failed-environment-gc.md).
## Creation and interruption observations

CreateEnvironmentRequest and ClaimEnvironmentRequest carry request_id for tenant-scoped
identity replay. The first successful publication atomically binds the success receipt;
ordinary central and local reservations remain in memory. EnvironmentResult.newly_created is true
only for the actual initial start operation; replay, reuse, resume and recovery return false.
EnvironmentRecord.observation carries bounded initial-create stage, normal-operation intent
and old-execution interruption evidence through journal/replay and final Running publication.
Coordinator retains its latest interruption/recovery summary in the Redis env record.
All participating components must use matching generated contracts. See the
[metric and persistence contract](../../../docs/testing/sandbox-observability.md).
