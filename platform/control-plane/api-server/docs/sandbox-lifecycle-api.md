# Sandbox lifecycle REST API

This document is the API Server-owned reference for the sandbox lifecycle HTTP
surface. It describes the HTTP contract implemented by the API Server; snapshot
catalog state, checkpoint bytes, and scheduler decisions are owned by the
Rust Master (catalog, placement, Redis) and Node Manager (lifecycle and execution).

The machine-readable contract is [`sandbox.yaml`](../../../api/openapi/sandbox.yaml).
Runtime command and file operations use the separate
[`data-plane.yaml`](../../../api/openapi/data-plane.yaml) contract.

**Support boundary:** the API Server maps the Sandbox schema into the typed
Instance contract. It supports S3 rootfs and mounts, image-backed mounts,
entrypoint inheritance, `extra_config`, independent resource request/limit
values, published ports, per-Instance data-plane security, creation/runtime
network policy, `failover=true`, and reload. Public local rootfs paths and host
mounts are rejected because node paths are deployment-owned. `upstream` reverse
tunnel is carried by the create contract; the legacy `/invoke` compatibility
transport remains unavailable.
Commands/files use Edge → Node Proxy → RRT. See
[placement](../../../../docs/testing/http-node-placement.md) and
[node lifecycle](../../../../docs/testing/node-lifecycle.md).

Rootfs values overlay the deployment Runtime Environment. Omitted fields inherit
the deployment default; `runtime` and `readonly` replace only those fields, while
an explicit image or S3 source atomically replaces the source. Node Manager still
requires the deployment-owned source, bootstrap and environment to match the
local node when no source replacement was requested.

All ordinary responses use the API Server response envelope:

```json
{"code": 200, "message": "", "data": "<base64-encoded JSON>"}
```

`data` is the base64 encoding produced by JSON encoding of the operation
result. The response examples below show the decoded JSON value of `data`.
SSE create responses are an exception and are described below.

Error responses retain the numeric `code` for envelope compatibility and add
a stable `error` object containing `code`, `retry`, `outcome`, `requestId`, and
the available operation/instance identities. Clients must follow those fields
instead of inferring retry safety from HTTP status or message text. See the
[management-plane error contract](../../../api/http/error-contract.md).

## Routes

| Operation | Method and path | Request body | Decoded successful `data` |
| --- | --- | --- | --- |
| Create sandbox | `POST /api/sandbox/v1/sandboxes` | create fields, including optional `snapshotId` and `failover` | `{"sandboxId":"default-name","instanceId":"default-name","status":"running","requestId":"..."}`; `tunnel` is included when requested |
| Create reusable snapshot | `POST /api/sandbox/v1/sandboxes/{sandboxID}/snapshots` | `{"name":"optional","timeoutSeconds":300}` | `{"snapshotId":"...","names":["optional"]}` |
| Get reusable snapshot | `GET /api/sandbox/v1/snapshots/{snapshotID}` | none | tenant-scoped catalog JSON returned by the Rust Master SnapshotService |
| List reusable snapshots | `GET /api/sandbox/v1/snapshots?name=&pageToken=&pageSize=` | none | tenant-scoped catalog JSON returned by the Rust Master SnapshotService |
| Delete reusable snapshot | `DELETE /api/sandbox/v1/snapshots/{snapshotID}` | none | tenant-scoped catalog JSON returned by the Rust Master SnapshotService |
| Pause | `POST /api/sandbox/v1/sandboxes/{sandboxID}/pause` | `{"ttlSeconds":90000,"timeoutSeconds":300}` | `{"sandboxId":"...","snapshotId":"...","size":8192,"state":"paused","expiresAt":...}` |
| Resume | `POST /api/sandbox/v1/sandboxes/{sandboxID}/resume` | none | `{"sandboxId":"...","state":"running","routeAddress":"host:port","functionProxyId":"...","nodeId":"...","portMappings":{}}` |
| Reload latest recovery point | `POST /api/sandbox/v1/sandboxes/{sandboxID}/reload` | none | `{"success":true}` after a new backend execution reaches Running and is published |
| Replace network policy | `PUT /api/sandbox/v1/sandboxes/{sandboxID}/network` | complete network policy, or `{}` to clear it | `{"success":true}` after sandboxd accepts the replacement and the operation is published |

`snapshotId` on the normal create route creates a new sandbox from a reusable
snapshot. The snapshot is reusable; creating from it does not consume it.
The Rust Master resolves the tenant-scoped Ready snapshot, protects it with a restore reference and applies compatible template fields. A local-only artifact pins the clone to the source node; shared storage permits normal scheduling. A deleting snapshot admits only an already-held reference. Explicit image, runtime and scalar resource geometry must match the snapshot; omitted resources inherit. Clones receive independent Instance/backend identities and artifact copies. See [snapshot storage](../../../../docs/testing/snapshot-storage.md).

Create uses the ordinary `X-Request-Id` header. It is optional: when absent,
API Server derives the request ID from the trace ID, echoes it as `X-Request-Id`,
and includes it in the create result. This is distinct from the required
`X-ADX-Request-ID` header used by the lifecycle routes below.

## Create from a reusable snapshot

Use the normal create route with a non-empty `snapshotId`. For example:

```json
{
  "name": "clone",
  "namespace": "default",
  "snapshotId": "snap-ready",
  "failover": false
}
```

`createTimeoutSeconds` and `scheduleTimeoutSeconds` are separate budgets.
`createTimeoutSeconds` bounds the complete public create request and is inherited
by the selected Node Manager and any central fallback. Local-first does not split
that budget into fixed local and central halves. `scheduleTimeoutSeconds` starts
only after a request enters the Master central queue and applies only until an
Assignment is formed. Queue expiry atomically removes an unassigned request and
releases its restore reference; an existing Assignment is never cancelled by the
queue timer. Downstream calls inherit the remaining create deadline and cannot
start a longer one. The default remains 90 seconds for create and 30 seconds for
central scheduling. User commands use the separate RRT HTTP data path.

For snapshot creation, omitted/zero resource values inherit the source. Positive CPU/memory/disk values must equal its resource geometry; restoring with larger limits or resizing is rejected. This applies to the new Rust resolver even though the HTTP handler can encode positive overrides.

Public resume uses the owner cache and calls the owning Node Manager; it performs same-node admission. Cross-node recovery of the same ID is the Master's failed-node recovery flow using a valid shared checkpoint, not a promise made by an ordinary resume request. `failover=true` enables same-node recovery from the latest unexpired checkpoint after an unexpected backend exit; no checkpoint means Failed and no cold start. Reload explicitly replaces a Running backend from that same latest recovery point.

## Reusable snapshots

Creating a reusable snapshot leaves its source sandbox running and requests a
non-expiring snapshot. `name` is optional on raw HTTP; it may be absent or the
empty string, but a supplied whitespace-only name is rejected. The resulting
snapshot belongs to the verified API Key identity. The service replaces incoming tenant headers; a caller cannot claim another tenant via a header or body.

Raw `timeoutSeconds` is honored for reusable snapshots and pause: it defaults
to 300 and must be from 1 through 3600 when supplied. API Server encodes it into the Node checkpoint RPC timeout. The RPC context additionally includes the configured transport timeout. Pause also
accepts `ttlSeconds`; omitted or zero becomes 90000 and only a negative value
is rejected. Snapshot and Pause are currently the only lifecycle bodies with a
caller-provided logical timeout; SDK HTTP transport waits use that logical
value plus a 30-second buffer. Reusable Snapshot uses one SDK transport
attempt, while Pause uses the lifecycle retry policy.

The SDK generates a fresh reusable-Snapshot request ID for each call. Raw HTTP
clients retrying an uncertain result must reuse an ID only for the same source
and name; they must not reuse one identity for different catalog content.

The get/list/delete routes call the Rust Master SnapshotService with verified tenant context. List accepts the optional `name`, `pageToken`, and `pageSize`
query parameters. Delete requires a non-empty `X-ADX-Request-ID`, which is
forwarded to the catalog operation.

## Lifecycle request IDs, errors, and retry

The lifecycle routes require a caller-supplied `X-ADX-Request-ID` whose prefix
matches the operation. The complete accepted forms are:

```text
pause-[A-Za-z0-9][A-Za-z0-9._-]{0,127}
resume-[A-Za-z0-9][A-Za-z0-9._-]{0,127}
reload-[A-Za-z0-9][A-Za-z0-9._-]{0,127}
network-[A-Za-z0-9][A-Za-z0-9._-]{0,127}
snapshot-[A-Za-z0-9][A-Za-z0-9._-]{0,127}
```

More precisely, the character immediately after the prefix must be
alphanumeric and the remaining suffix may use alphanumerics, `.`, `_`, or
`-`. These are header formats, not proof that a caller is the SDK. Raw HTTP
callers may supply a matching value. Pause additionally requires that its
returned `snapshotId` match that header value.

Create replay is separate from lifecycle IDs: it keys a request on tenant plus
`X-Request-Id` and compares the decoded create request. A changed decoded
request under that identity conflicts, as does a named create already in
flight under another identity. JSON object key order does not change the digest; omission, explicit zero and null are distinct request shapes. The cache is an HTTP create boundary, not a guarantee
about an unknown network outcome outside API Server.

For pause, resume, reload, network replacement, and snapshot creation, malformed or missing
lifecycle IDs and invalid local input map to `400`; downstream business
rejection maps to `409`; API Server-to-Node transport failure maps to `503`; and
malformed/invalid authoritative response data maps to `500`. Reload retains
its `{"success":false}` decoded data on an error response. A `503`, a gateway
failure, or a lost connection can be an unknown result: retry only with the
same request ID and then query the authoritative lifecycle or catalog state.
Do not blindly issue a new lifecycle identity for an uncertain operation.

SDK behavior is deliberately not identical to raw HTTP behavior:

- `Sandbox.create_snapshot(name=None, timeout_seconds=300)` rejects blank names
  and validates an integral timeout from 1 through 3600. It sends
  `timeoutSeconds` and makes one HTTP attempt with that configured timeout plus
  a 30-second buffer; raw HTTP forwards that field to the checkpoint RPC timeout. The SDK treats gateway/connection failure
  while creating a reusable snapshot as uncertain and does not silently retry
  it.
- `Sandbox.pause(ttl_seconds=90000, *, timeout_seconds=300)` rejects zero,
  negative, and boolean TTLs and validates its keyword-only timeout from 1
  through 3600. Raw HTTP treats an omitted or zero `ttlSeconds` as `90000`,
  rejects only a negative value, and forwards `timeoutSeconds` as the
  checkpoint RPC timeout. The SDK request timeout is the
  configured timeout plus 30 seconds.
- Pause, resume, and reload use up to three SDK HTTP attempts for retryable
  transport/gateway failures, retaining one `pause-`, `resume-`, or `reload-`
  identity across those attempts. Reusable snapshot creation is the single,
  uncertain-result attempt above. `reload()` returns `false` for a closed
  sandbox or `SandboxError`; callers needing the HTTP status and error message
  should use the REST response.

## Create HTTP versus SSE

`POST /api/sandbox/v1/sandboxes` returns the ordinary JSON envelope unless its
`Accept` header includes `text/event-stream`. Syntactic/bind failures that
occur while API Server reads and prepares the request—such as malformed JSON, an
oversized body, or invalid pre-stream create fields—receive a normal HTTP
error, not an `accepted` SSE event.

Once a valid SSE create has begun, the HTTP status is `200` and the stream
contains an `accepted` event with `{"status":"creating","requestId":"..."}`,
periodic `: heartbeat` comments, and a `final` event. The final event carries
the same create fields as the non-SSE result, with status `running`, `timeout`,
or `failed`; errors add `errorCode` and `message`. Downstream invocation,
create-timeout handling, replay/identity checks, and business failures happen
after `accepted` and are reported by that final event. After `accepted`, the
final event—not a later HTTP status—is the completion boundary.

## Pause, resume, and publication

Pause requires a complete checkpoint, positive size, matching request/snapshot ID and a published Paused record. Object-storage upload must finish before submission. A local SQLite Journaled result means cluster publication is pending and returns an unavailable error, not pause success.

Resume requires a matching Running record, completed RRT readiness and Node Proxy binding, followed by Master/Redis submission. `functionProxyId` is a retained HTTP field populated with the node ID; it does not imply a FunctionProxy process. Current `portMappings` is empty. Master route publication and Edge receiving that update are asynchronous; a successful operation does not guarantee every Edge already has the new route.

Reusable snapshot creation briefly pauses the source, copies its artifact, publishes the catalog and resumes the source. Success requires both the snapshot and resumed source result to be committed. It does not promise uninterrupted source execution. Deleting a referenced snapshot marks it deleting and prevents new references; physical removal follows reference release and backend confirmation.

Reload requires a Running Instance with an unexpired checkpoint. Node Manager
retires and deletes the current backend, restores the same logical Instance as
a fresh execution, completes RRT readiness and local route binding, then
publishes Running. A missing or expired checkpoint fails the operation; reload
never falls back to a cold start.
