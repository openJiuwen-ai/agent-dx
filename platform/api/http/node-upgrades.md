# Node upgrade management

The Coordinator's optional `autoscaling_listen` HTTP listener also serves
`/v1/node-upgrades`. It has no bearer authentication and must be isolated to
trusted operator Pods. It must never be forwarded by the public API/Ingress.
Node controls use the existing authenticated Coordinator-to-node RPC channel.
Management capability `version:2`, `node_protocol_version:3` and each node
inspection's `protocol_version:3` are required before upgrade work. Durable tasks
also carry `protocol_version:3`. Earlier upgrade protocols must finish or abort
with their original binaries before rollout; they cannot continue under this
version. Upgrade-specific canary authorization and probe journals are removed.

Capacity exhaustion returns HTTP 429 with `error.code=ResourceExhausted`.
Node RPC unavailability remains HTTP 503; unclassified failures remain HTTP 500.
Target capacity can disappear after the source has paused, so a 429 response
does not prove that migration never started. Query the task and reuse the same
`operation_id` when continuing an accepted operation.

The executor upgrades one native Kubernetes DaemonSet at a time: evacuate its
current workload, obtain strict empty evidence, replace that Pod with UID/RV
preconditions, verify its registered session and recovery capabilities, then
restore admission. Runtime and checkpoint compatibility use the same scheduling
and restore checks as ordinary Resume/Recover.
Upgrade Pause waits for Relay streams and Execd business requests to become
naturally idle while existing Environments continue accepting requests. Relay
then closes admission only if the observed activity version still matches; Execd
preparation independently rechecks and fences its runtime activity version.
Reusable HTTP CONNECTs conservatively remain busy until they close. Pool expiry
can expose idle gaps, but continuous traffic can still prevent migration.
Idle waiting, runtime preparation and checkpoint share the Pause budget. A
long-lived stream or an idle-barrier race stops the migration rather than being
forcibly cut. Before checkpoint starts, failures retain the source execution and reopen
local admission; unresolved task fencing still requires same-operation continuation
or the normal abort protocol. They do not authorize Pod deletion.

Requests blocked by a planned migration wait without creating competing wake
intent or holding the Coordinator state mutex. The original request deadline
bounds the wait; expiry uses the existing `ENV_RESUMING / not_started` protocol
only for business requests that have not been forwarded. Already forwarded
requests are never automatically replayed. Preparing a source conflicts before
pausing scheduling if an earlier durable resume remains accepted; let that resume
settle, then retry the same upgrade task.

These guarantees require the source adxlet/Relay and Ingress to include the idle
barriers and connection-lifetime changes, as well as the Coordinator wait behavior.
Updating only the target or Coordinator cannot protect streams on an old source.
Unplanned process/network loss and sessions longer than the idle-wait budget remain
separate failure cases; this protocol does not promise transparent stream migration.

| Method/path under `/v1/node-upgrades` | Purpose |
| --- | --- |
| `GET /capabilities` | Protocol support; each original node is also probed. |
| `POST /tasks` | Begin or replay a durable task. |
| `GET /tasks/{task_id}` | Persisted task, node bindings and operation progress without waiting for a write operation. |
| `GET /nodes/{node_id}/inspect` | Current session and local inventory; errors never mean empty. |
| `POST /tasks/{task_id}/drain-node` | Stop new placement on the source while retaining current execution and same-owner Resume. |
| `POST /tasks/{task_id}/prepare-node` | Stop admission and fence execution on the source. |
| `POST /tasks/{task_id}/migrate` | Pause, transfer ownership by CAS, adopt/restore at the target. |
| `POST /tasks/{task_id}/quiesce` | Revoke grants, settle local work, issue an empty-node permit. |
| `POST /tasks/{task_id}/revoke-delete` | Revoke a pending delete after Kubernetes CAS advances that Pod's RV. |
| `POST /tasks/{task_id}/verify-node` | Recheck empty evidence, registered recovery capabilities and session, then restore original admission. |
| `POST /tasks/{task_id}/complete` | Release verified node gates and complete the task. |
| `POST /tasks/{task_id}/abort` | Freeze, restore an untouched original Pod, or finalize rollback. |

Begin takes `task_id`, positive executor `epoch`, `namespace`, `daemonset_uid`,
`target_image` fixed by SHA-256 digest, and `nodes` containing `node_id`,
`pod_uid`, `node_session_id`. A task ID cannot be reused for another release.
Task queries do not reconcile operations. After an ambiguous result, retry the
same operation ID to reconcile durable ownership and local progress.
An Environment with an unsettled migration rejects another operation ID.
Actions carry the same task/epoch, a stable `operation_id`, and the relevant
Pod/session binding. Migration also carries `environment_id` and an optional
`target_node_id`. An explicit initial target stays fixed across retries.
Automatic selection rechecks capacity and checkpoint compatibility after Pause
and can update the Prepared target while ownership remains on the source.
For an automatic operation the recorded selection policy takes precedence over
any target hint sent on retry. The ownership CAS fixes the target.
Executors must treat an automatic Prepared target as provisional and read the
durable receipt again after retrying the same operation.
For a `drain-node` source, both automatic and explicit targets must be
compatible, remain `pending` in the task and match the registered session.
A source prepared with `prepare-node` can transfer to an already verified,
schedulable node if its runtime, rootfs and checkpoint storage are compatible.
The Coordinator does not prefer a node simply because it has been upgraded.
Existing Prepared operations without a recorded selection policy keep their
stored target. Optional `expected_source_generation` and
`expected_source_revision` fence the initial intent before any Prepared record or
Pause call; explicit zero is checked, not treated as absent. They do not replace
the stored identity when continuing an existing operation ID.

`register_source:true` explicitly adds a late source with its current Pod UID and
process session. It is accepted by Active `prepare-node` or `drain-node`, or by Aborting
`abort` with `stage:restore-original` for release only. A matching pending peer
can acquire its original source identity once; an existing original identity is
never overwritten. The executor must first prove the Pod belongs to this task's
DaemonSet and still runs the original image. Task isolation is recorded
separately from administrator pause intent; releasing it preserves manual pauses.

The `drain` capability requires Coordinator and adxlet builds supporting the
`draining` gate and revision-guarded Resume. Check `guarded_resume:true` in
management capabilities and in every source node's inspection before starting
the task. The Coordinator also checks each source before pausing its scheduling.
The task pause excludes the source from new placements.
Existing operations and same-session Resume continue unless an administrator
paused the node or the Sandbox has an unsettled migration. Optional migration
selects a compatible, pending node registered with the same upgrade task.
When abort starts, new Resume intents are blocked on draining sources. An
already accepted same-session Resume can settle before evacuation closes the
node gate. If the freeze conflicts with an outstanding Resume, retry the same
abort after it settles.
Quiesce still requires strict emptiness before Pod deletion.

Before accepting a migration, the source request Pod UID and process session must
match the task binding and the current live node.
Accepting a new migration also checks the source owner, revision, execution
state and both registered node sessions in the durable write that records `Prepared`.
Before ownership transfer, the operation checks both sessions and records the
source's authenticated, per-Environment stop inspection for the published Paused
revision. The ownership CAS checks that exact revision and both stored sessions.
After the CAS, retry with the original Pod UID and source session from the task.
The Coordinator verifies the durable source and target generations and the
original target session. It inspects the current source process, including a
replacement, when live. If the source is offline, continuation requires both
the stop evidence recorded by the CAS and durable invalidation or replacement
of the old session. A heartbeat timeout alone grants no recovery authority.
A historical Completed receipt does not require the source to remain live.

The task uses the existing Redis header CAS, in a separate control field so
ordinary lifecycle commits do not rewrite its journal. Migration retains the
Environment ID, uses a new ownership generation, and requires shared, unexpired
checkpoint data. Target selection enters the existing recovery scheduler with
the source excluded and any explicit target constraint; resource reservation and
ownership CAS recheck the same checkpoint requirements. The internal Pause for an originally Running Environment uses
`Forever` retention so migration waits cannot expire that execution; the user's
configured pause policy is unchanged. A previously Paused Environment remains Paused with its original TTL;
its transfer never calls Recover. Device requests, managed Volume mounts and node-local root/mount
sources are rejected before Pause. Managed Volumes retain their existing exclusive
execution authorization and are not yet qualified for planned migration. Scoped
checkpoint stores must advertise a common exact backend identity; a matching alias
alone does not qualify different S3 buckets or filesystem directories. Unscoped
shared filesystems are rejected. Captures record measured runtime binary and
default rootfs fingerprints in `recovery_profile`. Ordinary
cross-node recovery and upgrade migration filter against this profile and the
checkpoint's actual backend scope, then recheck before transferring ownership.
The destination rechecks the profile before restoring. Host kernel release, CPU
model and `/proc/cpuinfo` flags are not exact-match requirements; runsc checks
the CPU features required by the checkpoint during restore. A legacy point can still
resume on its original compatible owner. Ordinary cross-node Resume/Recover may
qualify a legacy point using an exact audited `legacy_checkpoint_proofs` entry
configured on Coordinator. The target still has to match the verified scope and
profile. Planned migration requires evidence on the checkpoint itself. Follow
the [deployment preparation steps](../../../build/config/examples/README.md)
before updating Coordinator.
`runtime_rootfs_identity:true` exposes the deployment default EROFS file's actual
`runtime_rootfs_sha256` and `runtime_rootfs_path` through node inspection. Operators
must not replace runtime inputs during the process lifetime. This flow currently
accepts only the deployment default local rootfs, including a persisted matching
RuntimeProfile; custom image/rootfs overrides are rejected. Matching rootfs bytes
alone is insufficient: the common recovery profile and backend checks also apply.

Whole-node `strict_empty` requires closed admission, no Controllers, holds,
authority work, runtime executions or ledger allocations, and a current
authoritative inventory. Isolated replacement sessions can report this evidence
before binding to the task. Only Quiescing issues a delete permit, which binds
task, epoch, process session, Pod UID and control revision. Before
DELETE, the executor persists intent and sends the current Pod RV. The same
permit and RV can be confirmed again. While an intent is unresolved,
confirmation with another RV and another quiesce for the same Pod are rejected.
Release requires either that UID to be gone or the
executor first CAS-patches it and submits the new RV to `revoke-delete`, together
with `expected_delete_resource_version` containing its original persisted DELETE
RV. The current intent must match that expected RV, and the new RV must differ.
A same-task/Pod/session replay after the intent was cleared succeeds without
changing admission or any newer permit; an old revoke cannot clear a newer DELETE
intent. If the process restarts in the same original Pod or in the same
Quiescing rollback Pod, the caller can revoke the old intent using the current
session once it is isolated for this task. Revocation leaves the binding
unchanged. Both paths save UID/RV intent before confirming the permit so they
can reconcile lost replies.

After a process restart in the same Pod, `quiesce` can bind the new session
once the task's operations on that node have settled. This applies to the
original Pod and to a Quiescing replacement during abort with `rollback:true`.
That rollback path also accepts a strictly empty original Pod still recorded as
`pending`, such as a late old-image Pod registered before its first release.
The caller supplies the same Pod UID and current session. The replacement
process must already be isolated for this task and report strict empty, with no
durable Environment assignments or outstanding delete intent. Quiesce issues a
fresh permit for the new session. Verify the restored Pod during abort before
finalizing with the restored template.

New sessions receive isolated control in their registration response before
catalog adoption. The response derives control after the session CAS from the
current task and its persisted pause marker; a task that already ended cannot
return an old isolation instruction. Source Pause grants bind the exact
Environment generation; no reserved probe names or wildcard generations bypass
normal execution authorization. Per-ID inspection still confirms that a migrated
Environment's old execution retired, its source backend is absent and its new
owner published the expected state. A Paused target must also have no backend for
that Environment. After transfer, Coordinator can wake the affected node
processes to compare against the authoritative catalog before the
next periodic report. Already reconciled sessions keep their other routes during
this live comparison. A wake acknowledgement grants no execution authority.
Completion requires source retirement or fenced durable stop evidence and target
adoption. Periodic comparison remains available if a wake is unavailable.
Shared checkpoint garbage collection retains its existing reference protection
and is separate from execution cleanup.
A `Published Failed` restore or a lost reply does not complete migration. Query
the persisted receipt, then retry the migrate action with the same operation ID;
sources are not deleted while any accepted operation remains unresolved.
An operation's `Completed` journal entry means ownership transfer settled or an
explicit Published deletion was confirmed, including through its durable
deletion receipt. It is historical progress, not a guarantee that the user has
kept the Environment running afterward. A successful migrate response still
requires the expected target execution state.

If capacity runs out after Pause and the original source still owns the
Environment, `Deferring` fences further transfer. Retry that same
operation if source recovery is pending. Once a task-paused Environment has
published Running on the original owner, or an existing user Pause is
confirmed unchanged, the operation records `Deferred`. The migrate response
is HTTP 200 with `outcome:"deferred"`, `completed:false`, and the source
`environment_id`, `node_id` and `generation`. Drain leaves the Environment on
that node to finish naturally. For a `prepare-node` source, send
`defer_on_capacity:true` when the Coordinator advertises `migrate_deferral:true`
and the source supports `guarded_resume`. Once the attempt is `Deferred`, recheck
capacity and use a new operation ID and the current source generation/revision
within the same task. Replaying the old ID returns its historical receipt.
Clients that do not opt in retain the 429 and replay contract. Pod deletion still
requires strict emptiness. If ownership already moved, settle the original transfer.

If the source process restarts before `Deferred`, verify that the original Pod
UID still identifies the current, non-terminating Pod. Retry the same migration
operation with that UID and the freshly inspected session. The Coordinator
requires isolation for the same task, the durable original ownership generation,
and evidence of the task checkpoint or the already published restore before
binding the new session. The source controller must reconcile any surviving
backend before another restore. The migration receipt keeps the original session.
Retry a 503 while the new session reconciles. If Abort freeze
has entered `Aborting`, supply `rollback:true` in that replay, then retry freeze.

If the same original Pod restarts again after `Deferred`, replay the settled
operation with `rebind_source:true`, its original Pod UID and the freshly
inspected session. The Coordinator rebinds the isolated source to the same task
only when no unsettled operation involves that node. It builds grants from
current authoritative ownership and leaves the historical operation and its
receipt intact. Omit the flag to read the historical receipt without rebinding.

`Cancelled` is an abort-only receipt for a Prepared operation whose ownership
never left its original source generation/session. Freeze removes new Pause
grants while preserving business routes and waits for already admitted work.
If the migration already published its Pause on the original source, abort may
restore the Sandbox there under a task-scoped grant for that assignment. It uses
the same rollback operation ID on retry and records `Cancelled` after the source
has published Running. A Sandbox that was already Paused remains Paused. If a
user Pause wins before the task's Pause, forward migration waits for abort; abort
can cancel the operation while preserving the proven user checkpoint.
If the CAS already moved ownership, retry the same migrate action with
`rollback:true` to settle the existing transfer while the task is Aborting.
Cancellation requires matching Coordinator and node-local Published
Running state (or the original Paused state), without pending recovery or
restart. Missing, Journaled or unknown evidence and target ownership transfer
remain unresolved. Cancellation is not successful migration: `migrate` rejects
its replay, while abort may restore the original Pod and finish.

Lost control replies can be reconciled with the same task and Pod/session
binding. Replaying `verify-node` on a verified node restores its original
admission setting. Completion, abort and original-Pod restoration retain their
workflow phase while controls are sent, so retries finish pending admission
restoration.
A durable `released` control also survives process restart when an existing Pod
still carries its old bootstrap-isolation environment. Startup lifecycle remains
closed until registration; a subsequent active task's isolation is applied before
catalog adoption, while fresh or unreleased nodes keep bootstrap isolation.
Terminal persistence checks all bound sessions and remaining task pause markers
in the same Header CAS; a concurrent registration keeps the task active.

Abort first freezes new forward work. Rollback actions carry `rollback:true`.
An original Pod can be reopened after confirming its UID, settled operations
and absence of a delete intent. After a lost release reply and a process restart
in that Pod, replay `restore-original` with the same UID and the current session.
The persisted `RestoredOriginal` phase records the release intent. An original
Pod already recorded as `verified` during abort can use the same replay after
a process restart. The Coordinator verifies that the new session is isolated
for the task and keeps scheduling paused until that session acknowledges release.
Existing Sandbox assignments stay in place.
If explicit Resume or traffic-triggered Resume arrives before adxlet has
recreated the local controller, the same resume intent waits for reconciliation
within the caller's timeout. Replacements need strict empty evidence, current
session and registered recovery capability checks.
Finalization also requires the
executor's confirmed old template and fresh owner inventory. No-Inspect rollback
is deliberately unavailable: an unreachable replacement remains isolated.

Tests cover Redis fencing, protocol replay, dormant transfer, failed/unknown
restores and node gates. Real multi-host checkpoint compatibility and Kubernetes
Pod replacement remain separate deployment qualification.

This rootfs preflight does not change sandboxd Start/Delete, uncertain-start
cleanup or Quiescing admission. A lost Start result and an empty runtime List do
not prove cleanup completed; existing failure protection remains in effect.
