# Managed images

A managed publication owns a release receipt and caller holds. The Coordinator
protects its queued, executing, dormant and recoverable consumers. Registry
manifest traversal, deletion and physical garbage collection run in an
operator process.

Before enrolling publications, reserve a fresh repository prefix in the
Registry for this control plane's managed writers. Scope writer and operator
credentials to their assigned namespaces. The Coordinator trusts the owning
tenant to register and settle its Registry writes. It accepts publications from
any producer that uses the reserved scope. For example, configure:

```json
{
  "managed_images": {
    "registry": {"authority": "registry.example.internal", "repository_prefix": "managed"},
    "max_active_per_tenant": 100000,
    "max_pending_per_tenant": 128
  },
  "managed_image_reclamation": false
}
```

The limits shown are defaults. Preparing and publishing records consume both
budgets. A finished deletion or a settled publication with no outputs or leases
frees its active slot. Exceeding a budget rejects new allocations. Existing
request IDs remain replayable. Redis retains compact terminal identities, so
size persistence for cumulative requests as well as active records.

`POST /api/environment/v1/managed-images` begins a publication with
`{"operation":"begin","request_id":"persisted-client-identity","retention":{"mode":"ephemeral","grace_seconds":86400}}`.
Use `{"mode":"retained"}` for permanent retention. The server allocates an
unpredictable publication ID, then atomically records the tenant's request
mapping. Replay returns the original ID even after cancellation or deletion.
Changed initial retention conflicts. Persist the request ID before calling.

`POST /api/environment/v1/managed-images/<id>` accepts these operations:

| Operation | Fields | Contract |
| --- | --- | --- |
| `register_outputs` | `outputs` (name to tagged reference), `attempt_id` | Reserve every repository before writing. Replays require identical outputs and the same writer attempt. |
| `complete` | `digests` (name to sha256 digest), `attempt_id` | The registered writer has finished and supplies the digest for every registered output. Identical receipts are replayable. |
| `cancel_preparation` | none | Settle a preparation before registering outputs. Release the publisher hold and preserve its request identity. |
| `reconcile_abandoned` | `digests`, `writer_fence`, `expected_revision` | An administrator confirms durable writer shutdown or revocation, inspects every registered tag and submits the present subset, including empty. The publisher hold is released and other holds remain. Present outputs follow the normal retention and grace rules. |
| `retain` | `lease_id` | Acquire a durable caller hold. Replaying an active hold succeeds. Released IDs cannot be reused. |
| `release` | `lease_id` | Release just that hold, idempotently. The initial hold is `publisher`. |
| `extend` | `retain_until` (UNIX seconds) | Extend the minimum retention deadline. |
| `pin` | none | Select retained policy. |
| `set_retention` | `retention`, `expected_revision` | Change policy with a current revision and restart grace observation. |
| `observe` | none | An administrator records a bounded sample of protected references and the first authoritative observation of zero references. |
| `claim_deletion` | `manifests` (immutable references) | An administrator records the verified manifest worklist while reclamation is enabled. The claim compares the current admission version and closes admission atomically. |
| `finish_deletion` | none | An administrator acknowledges the persisted worklist is absent. |

Each registered repository must be beneath the configured prefix and contain
the server-issued publication ID as a directory segment. For example,
`registry.example.internal/managed/team/<publication-id>/api:build`. Assign
one distinct repository to each of up to 128 outputs. The producer chooses a
fresh `attempt_id` of up to 256 bytes per writer. It uses that ID to correlate
registration and completion. Its Registry permissions and durable shutdown
evidence are managed by the producer or operator.

Lease identities are bounded to 256 bytes,
with at most 4096 active and released identities together per publication.
Keep one logical hold through application retries. Long-lived templates and
other consumers outside the Environment and Snapshot catalog must own a
durable caller hold. The HTTP/RPC mutation limit is 512 KiB and the manifest
worklist must fit inside that limit with room for the operation envelope.

The collection GET returns bounded active `items` summaries and `next_page_token`.
Use `after=<token>&limit=1..100`. A point GET returns the full receipt with
the most recent observed references or the compact terminal record. GET responses do not claim a fresh
inspection. `observe` reports `references_current=true`, a sample of up to
32 roots and candidate eligibility even when deletion is disabled.
`reclaimable` additionally requires the enable switch. All responses use
the Environment base64 JSON envelope, and `tenant=<id>` requires an
administrator credential.

The API Server translates the SDK's `extraConfig.managedImagePublications`
transport field to structured execution dependencies once. OCI roots and
mounts in the reserved scope carrying a publication ID must match an exact
registered repository before admission. Changing the configured scope
preserves protection for known publication identities. Native image preparation
normalizes the same dependencies before recording its seed specification. The
dependencies follow the Environment and captured Snapshot. An execution that loses its node
preserves image roots by tenant and generation through retirement. Node
reconciliation releases those holds only for the original session or a
verified matching host identity.

Both initial reservation paths, snapshot preparation and Snapshot creation
increase an admission version in the same Redis write as the new root.
Pending central creates also protect the publication. Observation scans the
preparations, Environments, Snapshots and recovery holds in pages outside the
scheduler lock. The first deletion claim fails if a new root arrived during the
scan or if the scan exceeds its budget. Resolve oversized inventories before
enabling reclamation at scale. A retry of an already claimed deletion reuses
the exact persisted worklist. The next
successful observation restarts grace after new admission. Image age and
heartbeat loss do not start grace.

Pre-publication cancellations and verified abandoned publications settle metadata
with audit receipts. Keep unknown publishers protected until their writer is
fenced. A missing tag alone is insufficient. Before claiming deletion the
operator verifies live Registry tag/digest receipts and resolves the manifest
graph. Interrupted deletion resumes the same persisted worklist. Do not use
the reclamation tool during a compatibility rollback or after restoring Redis
from a stale backup. Reconcile writers, running and recoverable consumers,
and publication receipts before resuming destructive operations.

Use a separately authorized maintenance window for physical Registry GC. GC
scans the entire storage domain, so preserving existing data requires a
verified recovery copy or independent physical storage. Cached image and
checkpoint bytes have their own lifetime policies.

The Platform CI gate runs the managed image state machine, isolated Redis
storage and mTLS RPC regressions. Native Nydus/checkpoint and physical GC
qualification also runs in the consumer's owned deployment fixture.
