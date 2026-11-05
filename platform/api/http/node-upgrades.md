# Node upgrade management

The Coordinator's optional `autoscaling_listen` HTTP listener also serves
`/v1/node-upgrades`. It has no bearer authentication and must be isolated to
trusted operator Pods. It must never be forwarded by the public API/Ingress.
Node controls use the existing authenticated Coordinator-to-node RPC channel.
The HTTP capability `node_protocol_version:2` and each node inspection's
`protocol_version:2` are required before upgrade work. Earlier private upgrade
builds use a different deletion-context wire layout and are rejected before
control intent is persisted. Durable tasks also carry `protocol_version:2`;
legacy journals remain readable, but their original version must finish or abort
them before rollout. There is no cross-version task continuation.

Capacity exhaustion returns HTTP 429 with `error.code=ResourceExhausted`.
Node RPC unavailability remains HTTP 503; unclassified failures remain HTTP 500.
Target capacity can disappear after the source has paused, so a 429 response
does not prove that migration never started. Query the task and reuse the same
`operation_id` when continuing an accepted operation.

The executor upgrades one native Kubernetes DaemonSet at a time: evacuate its
current workload, obtain strict empty evidence, replace that Pod with UID/RV
preconditions, run a shared C/R canary, clean it, then restore admission.
Upgrade Pause closes new Relay CONNECT admission and drains accepted streams
before checkpointing. Pooled HTTP connections retire after their current response
completes and a bounded lifetime, so continued reuse cannot prevent draining.
The drain, runtime preparation and checkpoint share the Pause budget. A long-lived
stream that cannot finish in time stops the migration rather than being forcibly
cut. Before checkpoint starts, failures retain the source execution and reopen
local admission; unresolved task fencing still requires same-operation continuation
or the normal abort protocol. They do not authorize Pod deletion.

Requests blocked by a planned migration wait without creating competing wake
intent or holding the Coordinator state mutex. The original request deadline
bounds the wait; expiry uses the existing `ENV_RESUMING / not_started` protocol
only for business requests that have not been forwarded. Already forwarded
requests are never automatically replayed. Preparing a source conflicts before
pausing scheduling if an earlier durable resume remains accepted; let that resume
settle, then retry the same upgrade task.

These guarantees require the source adxlet/Relay and Ingress to include the drain
and connection-lifetime changes, as well as the Coordinator wait behavior.
Updating only the target or Coordinator cannot protect streams on an old source.
Unplanned process/network loss and sessions longer than the drain budget remain
separate failure cases; this protocol does not promise transparent stream migration.

| Method/path under `/v1/node-upgrades` | Purpose |
| --- | --- |
| `GET /capabilities` | Protocol support; each original node is also probed. |
| `POST /tasks` | Begin or replay a durable task. |
| `GET /tasks/{task_id}` | Authoritative task, node bindings and operation progress. |
| `GET /nodes/{node_id}/inspect` | Current session and local inventory; errors never mean empty. |
| `POST /tasks/{task_id}/prepare-node` | Stop admission and fence execution on the source. |
| `POST /tasks/{task_id}/migrate` | Pause, transfer ownership by CAS, adopt/restore at the target. |
| `POST /tasks/{task_id}/quiesce` | Revoke grants, settle local work, issue an empty-node permit. |
| `POST /tasks/{task_id}/revoke-delete` | Revoke a pending delete after Kubernetes CAS advances that Pod's RV. |
| `POST /tasks/{task_id}/authorize-canary` | Grant a task-owned test identity and destination. |
| `POST /tasks/{task_id}/revoke-canary` | Durably revoke a canonical probe ID, fence late creation and confirm scoped cleanup. |
| `POST /tasks/{task_id}/verify-node` | Recheck empty evidence after SDK probes and restore original admission. |
| `POST /tasks/{task_id}/complete` | Release verified node gates and complete the task. |
| `POST /tasks/{task_id}/abort` | Freeze, restore an untouched original Pod, or finalize rollback. |

Begin takes `task_id`, positive executor `epoch`, `namespace`, `daemonset_uid`,
`target_image` fixed by SHA-256 digest, and `nodes` containing `node_id`,
`pod_uid`, `node_session_id`. A task ID cannot be reused for another release.
Actions carry the same task/epoch, a stable `operation_id`, and the relevant
Pod/session binding. Migration also carries `environment_id` and an optional
`target_node_id`. Optional `expected_source_generation` and
`expected_source_revision` fence the initial intent before any Prepared record or
Pause call; explicit zero is checked, not treated as absent. They do not replace
the stored identity when continuing an existing operation ID. Canary authorization carries the SDK name/namespace, shared
storage alias, healthy source and destination Pod UID.

`register_source:true` explicitly adds a late source with its current Pod UID and
process session. It is accepted by Active `prepare-node`, or by Aborting
`abort` with `stage:restore-original` for release only. A matching pending peer
can acquire its original source identity once; an existing original identity is
never overwritten. The executor must first prove the Pod belongs to this task's
DaemonSet and still runs the original image. Task isolation is recorded
separately from administrator pause intent; releasing it preserves manual pauses.

Before accepting a migration, the source request Pod UID and process session must
match the task binding and the current live node, including for a canary source.
An unfinished operation rechecks its original source and target sessions before
Pause, ownership transfer or Recover. The ownership CAS also checks both stored
node sessions against that operation, so a registration change cannot silently
redirect its transfer. A historical Completed receipt does not require its old
source process to remain live.

The task uses the existing Redis header CAS, in a separate control field so
ordinary lifecycle commits do not rewrite its journal. Migration retains the
Environment ID, uses a new ownership generation, and requires shared, unexpired
checkpoint data. The internal Pause for an originally Running Environment uses
`Forever` retention so migration waits cannot expire that execution; the user's
configured pause policy is unchanged. A previously Paused Environment remains Paused with its original TTL;
its transfer never calls Recover. Device requests, managed Volume mounts and node-local root/mount
sources are rejected before Pause. Managed Volumes retain their existing exclusive
execution authorization and are not yet qualified for planned migration. Scoped
checkpoint stores must advertise a common exact backend identity; a matching alias
alone does not qualify different S3 buckets. Unscoped shared filesystems require
the cross-node canary. `runtime_rootfs_identity:true` advertises the additional
preflight: node inspection returns `runtime_rootfs_sha256` and `runtime_rootfs_path`
for its immutable deployment default EROFS file. Adxlet hashes its actual bytes
at connection; operators must not replace that file during the process lifetime.
Canary authorization and every unfinished migration compare both nodes before
Pause or Recover. Missing or differing identities fail closed. This flow currently
accepts only the deployment default local rootfs, including a persisted matching
RuntimeProfile; custom image/rootfs overrides are rejected. Matching rootfs bytes
does not establish runtime binary, kernel or CPU compatibility; the backend still
validates those constraints.

`strict_empty` requires Quiescing, no Controllers, holds, authority work, runtime
executions or ledger allocations, and a current authoritative inventory. The
permit binds task, epoch, process session, Pod UID and control revision. Before
DELETE, the executor persists intent and sends the current Pod RV. A later
release cannot bypass an unresolved delete: either that UID is gone or the
executor first CAS-patches it and submits the new RV to `revoke-delete`, together
with `expected_delete_resource_version` containing its original persisted DELETE
RV. The current intent must match that expected RV, and the new RV must differ.
A same-task/Pod/session replay after the intent was cleared succeeds without
changing admission or any newer permit; an old revoke cannot clear a newer DELETE
intent. Both normal and rollback paths save UID/RV intent before asking the
Coordinator to confirm its delete permit, so lost replies remain reconcilable.

New sessions receive isolated control in their registration response before
catalog adoption. The response derives control after the session CAS from the
current task and its persisted pause marker; a task that already ended cannot
return an old isolation instruction. Canary grants are scoped to the task Environment/generation. `canary_cleanup:true`
in capabilities is required by the executor. SDK names must start with
`upgrade-canary-` in namespace `default`; these canonical IDs are reserved and
cannot be created without the current task's unrevoked authorization. History
is retained, including after a constructor loses its response. `revoke-canary`
takes canonical `environment_id` and returns `revoked`, `creation_fenced` and
`cleanup_confirmed`. Revocation is durable before node grants are removed; the
Coordinator clears pending scheduling, fences ownership writers and deletes an
accepted probe through authenticated node RPC. A false or unknown cleanup result
must be retried. Freeze revokes all existing probe IDs; rollback may authorize a
new ID. Finalization checks every historical ID. Per-ID inspection counts that
ID's Controllers, holds, authority work and all backend generations; healthy
peer business remains admitted. Shared checkpoint garbage collection retains
its existing reference protection and is separate from execution cleanup.
A `Published Failed` restore or a lost reply does not complete migration. Query
or explicit resume uses the same operation; sources are not deleted while any
accepted operation remains unresolved.
An operation's `Completed` journal entry means ownership transfer settled or an
explicit Published deletion was confirmed, including through its durable
deletion receipt. It is historical progress, not a guarantee that the user has
kept the Environment running afterward. A successful migrate response still
requires the expected target execution state.

`Cancelled` is an abort-only receipt for a Prepared operation whose ownership
never left its original source generation/session. Freeze removes new Pause
grants while preserving business routes and waits for already admitted work.
Cancellation then requires matching Coordinator and node-local Published
Running state (or the original Paused state), without pending recovery or
restart. Missing, Journaled or unknown evidence and target ownership transfer
remain unresolved. Cancellation is not successful migration: `migrate` rejects
its replay, while abort may restore the original Pod and finish.

Lost control replies can be reconciled with the same task and Pod/session
binding. Replaying `verify-node` on a verified node restores its original
admission setting. Completion, abort and original-Pod restoration retain their
workflow phase while controls are sent, so retries finish pending admission
restoration. A healthy canary peer's temporary grant does not mark it upgraded.
A durable `released` control also survives process restart when an existing Pod
still carries its old bootstrap-isolation environment. Startup lifecycle remains
closed until registration; a subsequent active task's isolation is applied before
catalog adoption, while fresh or unreleased nodes keep bootstrap isolation.
Terminal persistence checks all bound sessions and remaining task pause markers
in the same Header CAS; a concurrent registration keeps the task active.

Abort first freezes new forward work. Rollback actions carry `rollback:true`.
An unchanged original Pod can be reopened only after its original UID/session,
finished operations and revoked delete intent are confirmed. Replacements need
strict empty evidence and a fresh canary. Finalization also requires the
executor's confirmed old template and fresh owner inventory. No-Inspect rollback
is deliberately unavailable: an unreachable replacement remains isolated.

Tests cover Redis fencing, protocol replay, dormant transfer, failed/unknown
restores and node gates. Real multi-host checkpoint compatibility and Kubernetes
Pod replacement remain separate deployment qualification.

`expected_target_control_revision` optionally fences canary authorization before
any node binding or control mutation. A delayed rollback authorization cannot
replace a newer probe grant. Confirmed cleanup is saved in `cleaned_canaries`
before replying; replay remains valid after the old node session is replaced.

This rootfs preflight does not change sandboxd Start/Delete, uncertain-start
cleanup or Quiescing admission. A lost Start result and an empty runtime List do
not prove cleanup completed; existing failure protection remains in effect.
