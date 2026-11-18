# Managed images

A managed publication owns a release receipt and caller holds. The Coordinator
protects its queued, executing, dormant and recoverable consumers. Registry
manifest traversal, deletion and physical garbage collection run in an
operator process.

Before enrolling publications, reserve a fresh repository prefix in the
Registry for this control plane's managed writers. Scope writer and operator
credentials to their assigned namespaces. The configured authority and prefix
must match the managed runtime profiles. For example, set the following
Coordinator configuration in a qualified deployment:

```json
{
  "managed_images": {
    "registry": {"authority": "registry.example.internal", "repository_prefix": "managed"},
    "publisher_profiles": ["managed-build"],
    "max_records_per_tenant": 100000,
    "max_pending_per_tenant": 128
  },
  "managed_image_reclamation": false
}
```

The limits shown are defaults. Pending preparations and publications consume
the pending budget. All durable request identities, including tombstones,
consume the records budget. Exceeding a budget rejects new allocation while
existing request IDs can be queried and replayed. Keep these budgets and Registry
capacity under observation. Only enroll builds in the reserved scope.

`POST /api/environment/v1/managed-images` begins a publication with
`{"operation":"begin","request_id":"persisted-client-identity","retention":{"mode":"ephemeral","grace_seconds":86400}}`.
Use `{"mode":"retained"}` for permanent retention. The server allocates an
unpredictable publication ID, then atomically records the tenant's request
mapping. Replay returns the original ID even after cancellation or deletion.
Changed initial retention conflicts. Persist the request ID before calling.

`POST /api/environment/v1/managed-images/<id>` accepts these operations:

| Operation | Fields | Contract |
| --- | --- | --- |
| `plan` | `outputs` (name to tagged reference), `builder_id` | Register every output before starting publication. The builder must be a running Environment of this tenant and an allowed managed profile, with a durable dependency on this publication. The plan records its execution generation. Administrators can omit `builder_id` for their own controlled writer. |
| `publish` | `digests` (name to sha256 digest) | Commit all planned outputs. The registered builder generation must still own the publication. For an operator plan, an administrator commits the receipt. Identical committed receipts are replayable. |
| `cancel_preparation` | none | Settle a preparation before a plan was registered. The publisher hold is released and the request ID cannot allocate another writer. |
| `reconcile_abandoned` | `digests`, `writer_fence`, `expected_revision` | An administrator confirms durable writer shutdown or revocation, verifies every planned tag and submits the present subset, including empty. The publisher hold is released and other holds remain. Present outputs follow the normal retention and grace rules. |
| `retain` | `lease_id` | Acquire a durable caller hold. Replaying an active hold succeeds. Released IDs cannot be reused. |
| `release` | `lease_id` | Release just that hold, idempotently. The initial hold is `publisher`. |
| `extend` | `retain_until` (UNIX seconds) | Extend the minimum retention deadline. |
| `pin` | none | Select retained policy. |
| `set_retention` | `retention`, `expected_revision` | Change policy with a current revision and restart grace observation. |
| `observe` | none | An administrator records a bounded sample of protected references and the first authoritative observation of zero references. |
| `claim_deletion` | `manifests` (immutable references) | An administrator records the verified manifest worklist while reclamation is enabled. The claim compares the current admission version and closes admission atomically. |
| `finish_deletion` | none | An administrator acknowledges the persisted worklist is absent. |

Each plan uses one session collection under
`<authority>/<prefix>/<collection-id>/publications/<publication-id>/<output>`.
Plans allow at most 128 outputs. Lease identities are bounded to 256 bytes,
with at most 4096 active and released identities together per publication.
Keep one logical hold through application retries. Long-lived templates and
other consumers outside the Environment and Snapshot catalog must own a
durable caller hold. The HTTP/RPC mutation limit is 512 KiB and the manifest
worklist must fit inside that limit with room for the operation envelope.

The collection GET returns bounded `items` summaries and `next_page_token`.
Use `after=<token>&limit=1..100`. A point GET returns the full receipt with
the most recent observed references. GET responses do not claim a fresh
inspection. `observe` reports `references_current=true`, a sample of up to
32 roots and candidate eligibility even when deletion is disabled.
`reclaimable` additionally requires the enable switch. All responses use
the Environment base64 JSON envelope, and `tenant=<id>` requires an
administrator credential.

The API Server translates the SDK's `extraConfig.managedImagePublications`
transport field to structured execution dependencies once. Plain image
references in the reserved layout must match a registered repository before
admission. The structured dependencies are persisted with the
Environment and captured by Snapshots. An execution that loses its node
preserves image roots by tenant and generation through retirement. Node
reconciliation releases those holds only for the original session or a
verified matching host identity.

Both initial reservation paths and Snapshot creation increase an admission
version in the same Redis write as the new root. Pending central creates
also protect the publication. Observation scans persistent roots in bounded
pages outside the scheduler lock. A deletion claim fails if a new root
arrived during the scan or if the scan exceeds its budget. The next
successful observation restarts grace after new admission. Image age and
heartbeat loss do not start grace.

Unplanned cancellations and verified abandoned publications settle metadata
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
