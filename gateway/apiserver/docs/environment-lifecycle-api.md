# Environment lifecycle REST API

This document is the API Server-owned reference for the environment lifecycle HTTP
surface. It describes the HTTP contract implemented by the API Server; snapshot
catalog state, checkpoint bytes, and scheduler decisions are owned by the
Rust Coordinator (catalog, placement, Redis) and Adxlet (lifecycle and execution).

The machine-readable contract is [`environment.yaml`](../../../platform/api/openapi/environment.yaml).
Runtime command and file operations use the separate
[`data-plane.yaml`](../../../platform/api/openapi/data-plane.yaml) contract.

**Support boundary:** the API Server maps the Environment schema into the typed
Environment contract. It supports S3 rootfs and mounts, image-backed mounts,
entrypoint inheritance, `extra_config`, independent resource request/limit
values, published ports, per-Environment data-plane security, creation/runtime
network policy, `failover=true`, and reload. Public local rootfs paths remain rejected. Local mounts accept `host_path`
(or `hostPath`) alongside the existing image/S3 source alternatives. `upstream` reverse
tunnel is carried by the create contract; the legacy `/invoke` compatibility
transport remains unavailable.
Commands/files use Ingress → Relay → EXECD. See
[placement](../../../docs/testing/http-node-placement.md) and
[node lifecycle](../../../docs/testing/node-lifecycle.md).

Rootfs values overlay the deployment Runtime Environment. Omitted fields inherit
the deployment default; `runtime` and `readonly` replace only those fields, while
an explicit image or S3 source atomically replaces the source. Adxlet still
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
[management-plane error contract](../../../platform/api/http/error-contract.md).

## Routes

| Operation | Method and path | Request body | Decoded successful `data` |
| --- | --- | --- | --- |
| Probe capability | `GET /api/environment/v1/capabilities` | none | plain JSON `{"capabilities":["workload-probes-v1"]}` (authenticated; no envelope) |
| Create environment | `POST /api/environment/v1/environments` | create fields, including optional `snapshotId` and `failover` | `{"environmentId":"default-name","environmentId":"default-name","status":"running","requestId":"..."}`; `tunnel` is included when requested |
| Create reusable snapshot | `POST /api/environment/v1/environments/{environmentID}/snapshots` | `{"name":"optional","timeoutSeconds":300}` | `{"snapshotId":"...","names":["optional"]}` |
| Get reusable snapshot | `GET /api/environment/v1/snapshots/{snapshotID}` | none | tenant-scoped catalog JSON returned by the Rust Coordinator SnapshotService |
| List reusable snapshots | `GET /api/environment/v1/snapshots?name=&pageToken=&pageSize=` | none | tenant-scoped catalog JSON returned by the Rust Coordinator SnapshotService |
| Delete reusable snapshot | `DELETE /api/environment/v1/snapshots/{snapshotID}` | none | tenant-scoped catalog JSON returned by the Rust Coordinator SnapshotService |
| Pause | `POST /api/environment/v1/environments/{environmentID}/pause` | `{"retention":{"kind":"ttl","ttlSeconds":90000},"timeoutSeconds":300}` | `{"environmentId":"...","snapshotId":"...","size":8192,"state":"paused","expiresAt":...}` |
| Resume | `POST /api/environment/v1/environments/{environmentID}/resume` | none | `{"environmentId":"...","state":"running","routeAddress":"host:port","functionProxyId":"...","nodeId":"...","portMappings":{}}` |
| Reload latest recovery point | `POST /api/environment/v1/environments/{environmentID}/reload` | none | `{"success":true}` after a new backend execution reaches Running and is published |
| Replace network policy | `PUT /api/environment/v1/environments/{environmentID}/network` | complete network policy, or `{}` to clear it | `{"success":true}` after sandboxd accepts the replacement and the operation is published |

`snapshotId` on the normal create route creates a new environment from a reusable
snapshot. The snapshot is reusable; creating from it does not consume it.
The Rust Coordinator resolves the tenant-scoped Ready snapshot, protects it with a restore reference and applies compatible template fields. A local-only artifact pins the clone to the source node; shared storage permits normal scheduling. A deleting snapshot admits only an already-held reference. Explicit image, runtime and scalar resource geometry must match the snapshot; omitted resources inherit. Clones receive independent Environment/backend identities and artifact copies. See [snapshot storage](../../../docs/testing/snapshot-storage.md).

Create uses the ordinary `X-Request-Id` header. It is optional: when absent,
API Server derives the request ID from the trace ID, echoes it as `X-Request-Id`,
and includes it in the create result. This is distinct from the required
`X-ADX-Request-ID` header used by the lifecycle routes below.

## Workload probes on image startup

An image create with `inheritEntrypoint=true` accepts `readinessProbe` and
`healthcheckProbe`. Each contains exactly one action (`httpGet`, `tcpSocket`,
or `exec`) and integral timing values:

```json
{
  "name": "server",
  "rootfs": {"type": "image", "imageurl": "example/server:latest"},
  "inheritEntrypoint": true,
  "initCallTimeoutSeconds": 120,
  "createTimeoutSeconds": 180,
  "scheduleTimeoutSeconds": 30,
  "readinessProbe": {
    "httpGet": {"port": 8080, "path": "/ready"},
    "initialDelaySeconds": 0,
    "periodSeconds": 2,
    "timeoutSeconds": 1
  },
  "healthcheckProbe": {
    "exec": {"command": ["/app/check"]},
    "periodSeconds": 10,
    "timeoutSeconds": 2,
    "failureThreshold": 3
  }
}
```

`tcpSocket` takes `{"port":8080}`. HTTP/TCP target environment loopback; HTTP
requires 2xx and does not follow redirects. Exec is argv (no implicit shell),
with the managed entrypoint's effective user, cwd and environment; exit zero
succeeds. Ports must be 1–65535. Delay defaults to 0 (range 0–86400), period
is 1–86400, and attempt timeout is 1–3600 seconds. Readiness defaults to
period 2 / timeout 1; healthcheck defaults to period 10 / timeout 2 /
`failureThreshold=3`. Readiness does not accept `failureThreshold`.

`initCallTimeoutSeconds` defaults to 30 when omitted or zero; positive values
must be 1–86400. This budget bounds Execd initialization and readiness after
backend preparation, within the remaining end-to-end create deadline. Execd retries readiness until
first success or the deadline. Its transient `Initializing` report stays in
runtime/control-stream memory. Adxlet publishes Running and activates routes
only after readiness succeeds; final failure uses normal Failed publication
and runtime cleanup. Ordinary and SSE errors include `error.probeFailure`,
with `retry=never` and `outcome=terminal`.

Healthcheck is armed after readiness (or runtime startup when readiness is
absent). Success resets consecutive failures. Reaching the threshold reports
final failure through RuntimeStatus; Adxlet processes it through its serialized
controller and existing restart/failover policy. HTTP status polling backs up
control-stream delivery. The committed instance view exposes `failure.reason=ProbeFailed` and
`failure.probe_failure` with `kind`, `message`, `consecutive_failures`, and optional `exit_code` /
`http_status`. Pause/resume/restore cancel checks, recheck readiness and reset
healthcheck counts. Probes do not count as user activity.

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
by the selected Adxlet and any central fallback. Local-first does not split
that budget into fixed local and central halves. `scheduleTimeoutSeconds` starts
only after a request enters the Coordinator central queue and applies only until an
Assignment is formed. Queue expiry atomically removes an unassigned request and
releases its restore reference; an existing Assignment is never cancelled by the
queue timer. Downstream calls inherit the remaining create deadline and cannot
start a longer one. The API Server fixes one absolute create deadline, including
cold image preparation, backend Start, Execd readiness and Running publication.
Coordinator and Adxlet pass the remaining budget through their create RPCs and
sandboxd Start; the ordinary short control-RPC timeout does not cap image preparation.
A same-request retry within that deadline can drive a Pending assignment again,
using the observed node/generation as a fence. The serialized Adxlet controller
replays or waits for the same execution. Transport failure remains an unknown
result; an existing terminal Failed result is not restarted by create replay.
Clock synchronization is required across processes. Deploy API Server, Coordinator
and Adxlet together for this internal protocol change. Cleanup and late-runtime
reconciliation may continue after caller deadline expiry. The default remains
90 seconds for create and 30 seconds for central scheduling. Create responses may
use up to three additional seconds solely to report a deadline failure; backend
execution keeps the original absolute deadline. Confirmed failed creates return
HTTP 504 / `DEADLINE_EXCEEDED`, `retry=never`, `outcome=terminal`, with the phase
in `message` (backend preparation/runtime start, Execd readiness, or route
activation). Query and replay retain the failure message. Transport wait expiry
uses the same timeout code with `outcome=unknown` / `retry=same_operation`;
disconnections and lost replies retain `OUTCOME_UNKNOWN`. User commands use the
separate EXECD HTTP data path.

For snapshot creation, omitted/zero resource values inherit the source. Positive CPU/memory/disk values must equal its resource geometry; restoring with larger limits or resizing is rejected. This applies to the new Rust resolver even though the HTTP handler can encode positive overrides.

Public resume and authenticated Ingress demand call Coordinator EnsureRunning. It persists one intent, prefers original-node admission, and transfers a shared checkpoint only after definitive NoCapacity or owner invalidation. An ambiguous RPC retains the owner. Paused objects stay dormant on node loss. `failover=true` enables same-node recovery from the latest unexpired checkpoint after an unexpected backend exit; no checkpoint means Failed and no cold start. Reload explicitly replaces a Running backend from that same latest recovery point.

## Idle pause and auto resume

Create accepts `idleTimeoutSeconds` (0 disables), `idleAction` (`delete` default,
`pause` requires a positive idle window) and `pauseOptions`.
`pauseOptions.retention` is `{"kind":"ttl","ttlSeconds":90000}` by default, or
`{"kind":"forever"}`. Permanent pause returns `expiresAt=null` and remains retained
until deletion or replacement. The other options are `timeoutSeconds`
(300 default, 1–3600), and `storage` (`configured` default, `local`, `shared`).
Shared requires a configured object backend, e.g. OBS via S3 compatibility;
nodes automatically advertise `ADX_CHECKPOINT_STORAGE`. Anonymous pause/workload
checkpoints follow this policy. Omitted/zero pause request TTL inherits the
Environment policy, otherwise 90000. The retention clock starts after publication.

Only authorized business traffic can demand resume. Concurrent requests share
the same durable operation; Ingress waits for the committed Running route before
sending bytes. Passive subscriptions, health and metrics do not wake an Environment.
Admission rejection across all nodes retains a pending intent with a 30-second
capacity retry round. EnsureRunning waits at most 300 seconds at these callers;
timeout does not prove restore failed or permit another node to run concurrently.

An idle-automatically paused Environment can also be resumed manually with
`POST /api/environment/v1/environments/{environmentId}/resume` or SDK `environment.resume()`.
The same operation-prefixed request ID rules apply. Manual and traffic-triggered
resume converge on one intent even when they race. Either requires an unexpired,
valid checkpoint; local-only restore requires the owning node. After successful
resume the idle policy remains enabled and may pause the Environment again.

## Reusable snapshots

Creating a reusable snapshot leaves its source environment running and requests a
non-expiring snapshot. `name` is optional on raw HTTP; it may be absent or the
empty string, but a supplied whitespace-only name is rejected. The resulting
snapshot belongs to the verified API Key identity. The service replaces incoming tenant headers; a caller cannot claim another tenant via a header or body.

Raw `timeoutSeconds` is honored for reusable snapshots and pause: it defaults
to 300 and must be from 1 through 3600 when supplied. API Server encodes it into the Node checkpoint RPC timeout. The RPC context additionally includes the configured transport timeout. Pause accepts optional `retention`; omission inherits the creation policy.
The capture budget does not bound subsequent OBS publication or the entire pause. Snapshot and Pause are currently the only lifecycle bodies with a
caller-provided logical timeout; SDK HTTP transport waits use that logical
value plus a 30-second buffer. Reusable Snapshot uses one SDK transport
attempt, while Pause uses the lifecycle retry policy.

The SDK generates a fresh reusable-Snapshot request ID for each call. Raw HTTP
clients retrying an uncertain result must reuse an ID only for the same source
and name; they must not reuse one identity for different catalog content.

The get/list/delete routes call the Rust Coordinator SnapshotService with verified tenant context. List accepts the optional `name`, `pageToken`, and `pageSize`
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

- `Environment.create_snapshot(name=None, timeout_seconds=300)` rejects blank names
  and validates an integral timeout from 1 through 3600. It sends
  `timeoutSeconds` and makes one HTTP attempt with that configured timeout plus
  a 30-second buffer; raw HTTP forwards that field to the checkpoint RPC timeout. The SDK treats gateway/connection failure
  while creating a reusable snapshot as uncertain and does not silently retry
  it.
- `Environment.pause(retention=None, *, timeout_seconds=300)` inherits the creation
  policy by default. `RetentionPolicy.for_seconds(seconds)` requires a positive
  integer TTL; `RetentionPolicy.forever()` explicitly disables expiry. The
  keyword-only timeout is from 1 through 3600. SDK request wait per attempt is
  that timeout plus 30 seconds; OBS upload uses its own timeout.
- Pause, resume, and reload use up to three SDK HTTP attempts for retryable
  transport/gateway failures, retaining one `pause-`, `resume-`, or `reload-`
  identity across those attempts. Reusable snapshot creation is the single,
  uncertain-result attempt above. `reload()` returns `false` for a closed
  environment or `EnvironmentError`; callers needing the HTTP status and error message
  should use the REST response.

## Create HTTP versus SSE

`POST /api/environment/v1/environments` returns the ordinary JSON envelope unless its
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

Resume requires a matching Running record, completed EXECD readiness and Relay binding, followed by Coordinator/Redis submission. `functionProxyId` is a retained HTTP field populated with the node ID; it does not imply a FunctionProxy process. Current `portMappings` is empty. Coordinator route publication and Ingress receiving that update are asynchronous; a successful operation does not guarantee every Ingress already has the new route.

Reusable snapshot creation briefly pauses the source, copies its artifact, publishes the catalog and resumes the source. Success requires both the snapshot and resumed source result to be committed. It does not promise uninterrupted source execution. Deleting a referenced snapshot marks it deleting and prevents new references; physical removal follows reference release and backend confirmation.

Reload requires a Running Environment with an unexpired checkpoint. Adxlet
retires and deletes the current backend, restores the same logical Environment as
a fresh execution, completes EXECD readiness and local route binding, then
publishes Running. A missing or expired checkpoint fails the operation; reload
never falls back to a cold start.

Before selecting the expected revision for reload, API Server queries
`Coordinator.GetEnvironment` for the latest committed record. This covers a
workload checkpoint that has acknowledged publication while its directory watch
update is still pending. If that lookup fails, reload fails without starting an
execution from the cached record. Existing assignment, generation and operation
ID checks still apply; other lifecycle operations continue to use the local
directory. This read does not lock the revision against subsequent concurrent
operations, which may still produce a conflict.

## Workload process overrides

Image creation with `inheritEntrypoint: true` accepts an optional `process`:

```json
{
  "image": "registry.example/agentserver:release",
  "inheritEntrypoint": true,
  "process": {
    "commands": [["sh", "-c", "exec jiuwenswarm-agentserver --port 18092"]],
    "user": "agentos",
    "cwd": "/home/agentos"
  }
}
```

Omitted fields inherit the image's startup command, user and working directory.
`commands` replaces the combined OCI Entrypoint/Cmd with 1–32 argv arrays, each
containing 1–256 arguments. No shell is inserted. Multiple commands start
concurrently with the same effective user and cwd; a failed command fails the
workload and terminates its sibling processes. Successful completion waits for
all commands. Startup readiness and health checks use the same effective identity
and working directory.

Like an inherited OCI USER, a non-default `user` requires the deployment-owned sandboxd OCI base spec to grant Execd `CAP_SETUID` and `CAP_SETGID` for child identity changes.

`user` accepts the existing image user-resolution syntax (name or numeric UID,
optionally followed by a group). `cwd` is an absolute canonical path; it must
already be accessible to that user. This is separate from the constructor `cwd`
option below. `process` requires `inheritEntrypoint: true`; the Jiuwenbox backend
does not support these overrides. Environment persistence and internal RPC retain
the explicit fields across lifecycle operations.

## Constructor working directory

The create request accepts an optional `cwd` absolute path without control
characters. Coordinator persists this constructor option and Adxlet passes it to
Execd using the platform-owned `ADX_ENVIRONMENT_CWD` environment value. Execd creates
the directory and missing parents before runtime Ready, including when runtime
identity is refreshed after checkpoint restoration. An existing directory is
accepted. A file conflict, inaccessible or read-only path fails initialization;
the normal failed-creation path releases admission and cleans up the backend.

This option initializes the default directory used by SDK commands. It does not
change Execd's process cwd or the inherited OCI entrypoint's WORKDIR. Per-command
cwd overrides remain strict and never create directories implicitly; spawn errors
use `SPAWN_FAILED` and `error_message`.

## Local mount sources

Each mount requires exactly one nonempty `image_url`, `s3_config`, or `host_path` source. The existing `imageUrl`, `s3Config`, and `hostPath` aliases are accepted. Zero or multiple sources return InvalidArgument (HTTP 400). Local source paths must be absolute without control characters or `.`/`..` components. The public local rootfs restriction is unchanged.

```json
{"image":"registry.example/app:v1","mounts":[{"type":"bind","host_path":"/mnt/sfs/workspaces/w-user","target":"/home/jiuwen","options":["rw"],"create_if_missing":true}]}
```

The request maps to `StorageSource::Local`, then sandboxd HostPath, preserving mount options. The source must exist in the runtime's mount namespace with suitable permissions. This endpoint does not mount SFS, authorize per-tenant host paths, or coordinate multiple sandboxes using the same directory. Source-directory creation requires the explicit flag described below. Expose arbitrary host-path creation only to trusted management callers until deployment-specific path authorization is provided. User sandboxes do not need CAP_SYS_ADMIN for this node-side operation. Component contract tests do not establish runsc/SFS writeback or SQLite recovery guarantees.


`create_if_missing` is an optional boolean on each mount (default `false`). It is
accepted only for Local bind sources; other sources/types return InvalidArgument.
Activator forwards it with the rendered path. Environment storage and RPC retain
it, and adxlet applies it before both sandbox start and restore; sandboxd receives
the resulting ordinary Local mount. No node directory policy is required.

When enabled, only the final source directory is created (mode 0755 subject to the
node umask, owned by the node process). Its parents must already exist. All source
components are opened without following symlinks. Existing directories and their
ownership are preserved; existing regular files are rejected. I/O failures prevent
runtime creation. This flag is permission to recreate a missing directory on a
later request; it is not a data-loss detector or an exclusive-access lock. Deployment
must provision the shared parent, verify SFS before admission, and restrict Local
mount APIs to trusted callers. No data is deleted when the sandbox is deleted.


Local mounts with `create_if_missing: true` may also specify
`"create_options":{"uid":10001,"gid":10001,"mode":"0700"}`. All three fields
are required: uid/gid are integers from 0 through 4294967294, and mode is a
four-digit octal string from `0000` through `0777`. Invalid attributes or
`create_options` without `create_if_missing: true` return InvalidArgument (HTTP 400).
The options pass through Environment storage and RPC to adxlet. Only newly created
directories receive these attributes; existing data, ownership and permissions
remain unchanged. Without these options, the default creation behavior above
is preserved. The node creates the final directory directly and sets ownership
and permissions through its descriptor only when this call created it. Concurrent
mkdir calls that find an existing directory reuse it without changing attributes.
Creation and attribute initialization are not atomic: concurrent first use or a
process interruption can observe incomplete attributes. Permission or filesystem
errors fail this start/restore attempt and leave the directory for operator repair;
existing directories are never automatically repaired, removed or renamed. No
fallback to more permissive access is applied. The shared filesystem must permit
ownership and permission changes, with no renameat2 requirement. A node release
supporting these options is required before templates use them; this behavior has local component coverage, not a runsc/SFS end-to-end claim.
