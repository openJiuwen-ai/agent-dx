# Environment create ownership

Omit the image and rootfs source to use the deployment-owned default rootfs.
On sandboxd, an explicitly selected rootfs skips the built-in profile's
`PATH`. An explicit `env.PATH` sets the request value, subject to a node
override. For an OCI image with no request or node override, sandboxd uses
the image's `PATH` if present and otherwise its runtime template.

An explicit `process.commands` starts a managed workload independently of OCI
entrypoint inheritance, including on a local EROFS rootfs. Its optional user and
working directory default to the Execd identity and `/`; readiness and healthcheck
probes apply to this workload as usual. No image process metadata file is required.
`inheritEntrypoint=true` remains the option for reading an OCI image's process
metadata, with optional `process` overrides.

`POST /api/environment/v1/environments` returns a confirmed-running result as
JSON inside the normal response envelope, or as the final SSE event:

```json
{"environmentId":"tenant-worker","status":"running","requestId":"create-request","newlyCreated":true}
```

`requestId` identifies the request that created the **current Env**. If R1 created
an Env, CREATE with R2 returns the existing Env with `requestId: R1`. If the Env
is deleted and recreated by R3, replaying R1 returns the current Env with
`requestId: R3`. Takeover to another node preserves the Env's creation request ID.
Error objects and the SSE `accepted` event still identify the incoming request.
A Running result with no creation request ID is malformed and returns an internal
data error instead of a successful response with an empty ID.

`newlyCreated` comes from the node's creation result. It is true only when that
logical create confirms a new instance, and false when reusing an existing Env.
A cached response retains its original ownership bit while returning the same
Env creation identity; a replacement Env returns false. Recovery after a lost
response may conservatively return false. The creation request ID alone is not
evidence that this caller owns initialization rollback.

Clients may automatically delete during local initialization rollback only
when `newlyCreated` is exactly boolean true. Reuse, attachment, missing or
malformed ownership must preserve the remote workload and close local clients.
This rule does not restrict explicit deletion or change detached lifetimes.

Successful Request ID receipts are scoped to the tenant and retained in Redis for
600 seconds from their first publication. Replays do not extend that deadline;
Redis and Coordinator restarts preserve it. After expiry, the Request ID alone
no longer identifies an earlier creation. An existing Environment ID still
reuses its current result, even when the receipt points to an older lifetime of
the same name. While the receipt is valid, confirmed absence returns a terminal
result and the caller uses a new request ID to create again. After expiry, the
same request ID may create a new Environment. Receipt expiry does not
delete an Environment or release its resources.

Reusable snapshot restores inherit scheduling reservations and execution limits
independently. An omitted or zero CPU, memory or writable-storage limit inherits
the saved limit; it must not become the smaller scheduling request. Explicit
positive limits remain subject to runtime compatibility and request validation.
Fresh creation retains the existing zero-limit behavior of following its request.

`inheritEntrypoint=true` is a fresh-create option, not a snapshot override.
Combining it with `snapshotId` is rejected with HTTP 400, matching the native
Python SDK. For snapshot restores, omit it or supply `false`: neither disables
the captured image-process metadata. The Coordinator inherits the saved flag
and required rootfs metadata so Execd resumes the original processes without
starting the entrypoint again. Execution-limit overrides above do not permit
changing this captured process geometry.

Internal normalized requests may already carry the inherited flag. A `true`
flag must match a source snapshot with the flag enabled; enabling it for a
snapshot whose flag is `false` is rejected. A `false` protobuf value is treated
as unspecified during inheritance because that field has no presence bit.

Snapshot rootfs and workload probes are captured configuration, not restore
options. Omitted rootfs/readiness/healthcheck fields inherit the source;
explicit internal values must match it (including rootfs `readonly`). A mismatch
returns InvalidArgument. Normalized internal retries with matching values remain
valid. This also preserves rootfs geometry for snapshots without an inherited
entrypoint. Public SDK callers omit these fields when creating from a snapshot.

Restoring does not launch the image entrypoint again. The restored Execd probe
supervisor follows its existing resume protocol: readiness runs for the new
runtime identity, in-flight checks are cancelled and healthcheck failure counts
reset. Adxlet retains the captured probe policy so a subsequent terminal failure
still retires the execution and applies the environment's restart/failover policy.
The native SDK's `wait_entrypoint()` on snapshot or `from_id()` handles discovers
entrypoint state from Execd rather than interpreting omitted create flags as
an explicitly disabled workload. If no managed entrypoint exists, that query
returns an error; it never starts a new process.


## Caller-prepared Local rootfs

Public creation accepts an absolute path on the execution node when it is a
strict descendant of a directory in the API Server's
`allowed_local_rootfs_dirs` list. The check compares path components. An empty
or omitted list disables caller-provided Local rootfs. Requests outside the
allowed directories return `PERMISSION_DENIED` before scheduling.

With `adxctl` profile-based deployment, add the list to the API Server config:

```yaml
service_overrides:
  apiserver:
    config:
      allowed_local_rootfs_dirs:
        - /var/lib/agentos/local-images/objects
```

Then create the environment with a path beneath that directory:

```json
{"name":"local-tools","rootfs":{"type":"local","path":"/var/lib/agentos/local-images/objects/local-IMAGE_ID/rootfs","runtime":"runsc"},"env":{"IMAGE_VALUE":"prepared"}}
```

The caller prepares and retains this directory or file on every eligible node,
or selects a node that provides it. ADX does not import Docker images for this source.
The API Server checks the requested path. Link resolution happens on the
execution node. Use operator-controlled directories and ensure that symbolic
links beneath them resolve within the approved directory tree on every eligible node.
Conch and Supervisor require image bindings. A request that selects either runtime
with an explicit Local rootfs returns `INVALID_ARGUMENT` before scheduling. This
also applies when the runtime comes from the deployment default.

With runsc, an explicit Local rootfs uses a per-sandbox writable overlay by
default. The host source remains read-only and the overlay uses the configured
sandboxd filestore. Set `rootfs.readonly: true` for a read-only sandbox root.
Other supported runtimes require a read-only Local rootfs.
Writable layer quotas (`storageMb` and `storage_limit_mb`) and runsc GPU allocation
are unavailable with an explicit Local source. Writable workspaces use separate mounts.

Local workload entrypoints are supplied through the existing `process` field;
`inheritEntrypoint=true` is rejected for an explicit Local source. Image metadata
must be resolved by the caller into `env` and, when needed, `process`.

### Agent Golden 制品保护

`PUT /api/environment/v1/snapshot-protections/{snapshotId}` 为租户的 Ready Golden 建立 Catalog 持久保护，JSON 请求为 `{"token":"<Activator durable UUID>"}`。以相同凭据重放是幂等的。保护存在期间，普通 snapshot DELETE 和 E2B 删除入口均遵守 Catalog 冲突。

`DELETE /api/environment/v1/snapshot-protections/{snapshotId}` 由 Activator 在封住新 Binding 决议并查清已有引用后调用，使用相同的 JSON 凭据。Platform 在其删除条件满足时以同一个 Catalog CAS 消费保护并关闭新恢复。租户运营应使用 Agent 退役入口，凭据由 Activator 保管。
