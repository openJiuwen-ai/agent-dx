# Volume management and reference mounts

Volume endpoints use the authenticated tenant and the standard base64 JSON
response envelope and [error contract](error-contract.md). Volume IDs contain
1–256 ASCII letters, digits, dots, underscores or hyphens; `.` and `..` are not
valid IDs because URL clients normalize those path segments. The same rule applies
to definitions, mount references, and single-Volume lookup/deletion.
Coordinator owns
metadata and occupancy. Adxlet accesses these through RPC; it has no new Redis
connection, credentials or storage configuration.

| Method | Path | Result |
| --- | --- | --- |
| POST | `/api/environment/v1/volumes` | Ensure a definition; return `{uid,spec,revision}` |
| GET | `/api/environment/v1/volumes/{id}` | Definition, `environment_ids`, `owner_ids`, `snapshot_ids`, `occupancy`, `initialization` |
| GET | `/api/environment/v1/volumes?pageSize=100&pageToken=...` | `{items,nextPageToken}`; page size at most 1000 |
| DELETE | `/api/environment/v1/volumes/{id}` | Remove unreferenced, unowned, unoccupied metadata; retain data |
| DELETE | `/api/environment/v1/volume-owners/{id}` | Release this opaque lifecycle owner's associations after its Environment is absent |

An ensure request contains a tenant-local ID and directory definition:

```json
{
  "id": "workspace-alice",
  "source": {
    "type": "directory",
    "storage_domain": "sfs",
    "path": "workspaces/tenant-a/alice"
  },
  "access_mode": "exclusive",
  "create_if_missing": true,
  "create_options": {"uid": 10001, "gid": 10001, "mode": "0700"}
}
```

`access_mode` defaults to `shared`. A directory's storage domain, accessible nodes,
mount roots and tenant path prefixes come from Coordinator's trusted
`storage_domains` configuration. Registration does not mount storage or touch data.

For elastic shared-storage nodes, enable `dynamic_nodes` on the Coordinator domain.
These are fragments merged into the respective service configurations:

```json
{
  "storage_domains": {
    "sfs-main": {
      "backend_id": "server:/export",
      "dynamic_nodes": true,
      "node_roots": {},
      "tenant_paths": {"tenant-a": "workspaces/tenant-a"}
    }
  }
}
```

Each adxlet is configured with the same domain/backend and its local logical root:

```json
{
  "storage_mounts": {
    "sfs-main": {"backend_id": "server:/export", "root": "/mnt/sfs"}
  }
}
```

Adxlet reports only mounts that pass its existing backend/directory verification,
using the regular registration/heartbeat cycle. A missing or mismatched mount is
omitted. A failed/timed-out observation withdraws the complete view for that cycle;
unrelated work can still use the node. Coordinator admits matching reports only
from current reconciled node sessions, refreshes queued Volume topology on changes,
and uses the same roots for execution grants. Queue deadlines and exclusive
occupancy waits are preserved. Execution still rechecks the actual mount; a mount
lost after a report can fail an execution. These reports do not fence storage or
prove data contents, and roots must refer to the same logical directory.

`dynamic_nodes` defaults to `false`: `node_roots` remains the fixed topology for
node-local data. With `dynamic_nodes: true`, reports replace that static map;
`node_roots` can be empty. This policy is independent of Volume `access_mode`:
both shared and exclusive Volumes on a shared backend support dynamic nodes.
Coordinator configuration is still startup configuration; no reload API is added.
Existing tenant/ID definitions are returned unchanged, including their UID; a
changed source, mode or initialization request does not overwrite them. A new
definition after deletion receives a new UID.

An optional `owner_id` associates the resource with a lifecycle, idempotently.
Activator uses the actual Environment ID of each saved Binding execution reference;
an Alias upgrade associates the new Environment before releasing the old owner. Several owners may share a
Volume. Owner release cancels an unassigned request when its outcome is settled,
removes that Environment's fixed references, and deletes definitions only when
this was the last owner and no other references or occupancy remain. Otherwise
the definition is retained and automatically removed when its remaining uses end. Conflicting
or uncertain cleanup fails for the caller to retry. The operation has no permanent
deleted-owner marker; Activator checks its own Binding after activation and
compensates a late result by deleting the old Environment and releasing its
associations. It does not target the replacement Binding.
Tenant authorization covers queued requests and persisted references before any
request is canceled or association is removed.

Environment creation accepts this reference mount:

```json
{
  "type": "bind",
  "target": "/workspace",
  "options": ["rw"],
  "volume_id": "workspace-alice",
  "volume_uid": "the-uid-returned-by-ensure"
}
```

`volume_uid` is optional for direct callers; Coordinator fixes the resolved UID
before placement. Activator supplies the UID returned by ensure. A mount chooses
exactly one of `volume_id`, `host_path`, `image_url`, or `s3_config`. A VolumeRef
cannot carry `create_if_missing` or `create_options`; those belong to its immutable
definition. An Environment can reference at most one distinct exclusive Volume.
Rootfs and existing inline sources keep their prior contracts.

Exclusive occupancy is reserved atomically with assignment, becomes held before
runtime effects, and is released by confirmed cleanup publication. Releasing
compute capacity alone does not release it. Heartbeat expiry invalidates the old
assignment and releases its matching occupancy in one transaction. This policy
allows the disconnected old runtime to keep accessing the filesystem; it supplies
no storage-side fencing or bound on overlapping access.

A verified adxlet process replacement transfers retained assignments' occupancy
and active execution authorization to the new node session atomically with node
registration. Old-session reports cannot release that occupancy. Interrupted
initialization remains `needs_repair`. Node inspection reports the latest
authorized execution even before its first state publication.

A paused Environment waiting for a busy Volume keeps its durable resume intent
and does not reject its checkpoint's owner node or block unrelated scheduling.
Definitive admission refusals preserve the previous runtime identity for retry;
ambiguous authorization failures retain the attempted identity for cleanup.

Initialization inspection uses `not_requested`, `uninitialized`, `pending`,
`ready`, or `needs_repair` in `initialization.state`, with an error string. Failed
or interrupted first-use preparation is not treated as success merely because a
directory exists. The same initialization owner may retry Pending preparation:
if the directory exists and creation attributes were specified, adxlet verifies
its UID, GID and mode before reporting success. Mismatched attributes fail
initialization without rewriting the directory. Only the caller that creates a
missing directory applies creation attributes; a first-time adoption of an
existing directory preserves its data and attributes. No initialization-repair
operation is exposed in this version.
The deployment must mount the configured backend before adxlet prepares the final
directory; existing data, ownership and permissions remain unchanged.

Publishing a reusable Snapshot fixes its source Volume UIDs and atomically retains
the definitions. `snapshot_ids` reports these protections. Deleting the last
Binding leaves a protected definition available until the final Snapshot or
Environment reference ends. A Snapshot in `Deleting` still retains the definition;
only completion of artifact deletion releases its protection. Snapshot protection
does not hold exclusive occupancy or copy external directory data. Clones keep
the existing behavior: they do not inherit the Snapshot mounts. Explicit clone
mounts use ordinary Volume admission. Standalone definitions still require explicit
deletion after their references end.

The implementation has local Redis, RPC, HTTPS and controlled-runtime evidence,
plus a single-host, two-node real runc/Execd run through the public SDK and both
Activator modes. That run used a dedicated host bind directory, an OCI bootstrap,
explicit unrestricted networking for runc, and a workload with no USER override.
It does not establish shared-storage across hosts, network ACL or USER inheritance,
or real Snapshot/checkpoint recovery acceptance; these boundaries are recorded in
the [implementation plan](../../../docs/development/platform-volume-design.md).

Source snapshot `614895936201fbe8658a26abfe0b8f94e20bb015` was rebuilt and passed 13 scenarios in each
Activator mode, including the ID contract and three Pending initialization
replays (missing directory, matching attributes, mismatched permissions).
The initialization faults were injected as matching persisted Pending state;
they exercise real node RPC and runtime behavior, not actual packet loss.
After Coordinator restart, the acceptance caller retries only explicit
`503 reconciling` deletion responses for at most 30 seconds. Both successful
rounds ended with no backend, Environment or Volume definition remaining.

The subsequent dynamic-node storage admission change has unit/scheduling and real
Redis/mTLS component coverage, including a queued request moving to a late node
on another shard. Its runtime uses a controlled fixture and a dedicated directory
on one host. No new public HTTPS or sandboxd/Execd end-to-end run was performed
for that change; the earlier runtime result applies to its source snapshot.
