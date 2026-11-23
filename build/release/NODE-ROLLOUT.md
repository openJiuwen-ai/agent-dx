# Rollout Publisher

## Process Plan

`node_rollout.py plan-process` adds read-only planning for standalone adxlet and
relay processes. This command creates no local task,
does not POST maintenance prepare/release, and does not contact Coordinator.
For adxlet, `--origin` freezes the trusted Coordinator management origin for a
apply stage; relay does not accept it.

```sh
python build/release/node_rollout.py plan-process \
  --context production --namespace platform --container supervisor \
  --config /opt/adx/config/deployment.yaml --role adxlet \
  --origin http://coordinator:9090 \
  --commit cccccccccccccccccccccccccccccccccccccccc \
  --sha256 dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd \
  --task-id release-20261110 --epoch 1 --batch-size 2 \
  --node adxlet-abc=registered-adx-node-a \
  --node adxlet-def=registered-adx-node-b > process-plan.json
```

For relay use `--role relay` and omit `--origin`; commit and SHA-256 must identify
that role's executable, not an archive. Config is an absolute POSIX path inside
the selected container with a `.yaml` or `.yml` suffix, checked before external
reads. Rust identity limits of 128 bytes are checked as UTF-8 bytes, not characters.
Context, namespace and container are explicit. Repeat
`--node POD=NODE` for each selected Pod; NODE is the explicit ADX node ID, never
the Kubernetes nodeName. No DaemonSet, OnDelete, image or rootfs policy is imposed.

Set `ADX_ROLLOUT_ADXCTL=/opt/adx/bin/adxctl` for Node images that install
the CLI outside PATH. Use the same setting for planning and applying. The value
is an absolute path inside the target container. The default command is `adxctl`.

The planner resolves local kubectl through `shutil.which` to an absolute path.
It issues Pod GETs and argv-only `kubectl exec ... -- adxctl --config CFG rollout
node-info`, with explicit context/namespace/container and no shell. Both use the
shared bounded JSON reader (8 MiB, 15-second process timer, kill/wait cleanup).
Each selected Pod and NodeInfo is read twice. Pod UID, namespace/name, nodeName,
non-terminating state, containerID, restartCount and running state must agree.
Exactly one role/node-ID component must be standalone, upgradeable, and have a
complete runtime. NodeInfo and runtime Pod UIDs must equal Kubernetes UID, boot
nonces must match, and PID/executable hash/node and relay sessions must remain
unchanged. This is not an atomic snapshot or proof against later changes.

Runtime observation now uses the existing parent-authorized NodeAdmin `Inspect`
in both embedded and standalone Adxlet modes. It returns the actual node/session,
Relay session and route owner plus a live `ready` boolean. Ready requires an ACK
for this session's ordinary (`reconciling=false`) registration with any returned
upgrade control successfully applied, initialized authority, lifecycle readiness,
and no authority reconciliation, drain, stop or process seal. Successor readiness
also requires matching registration/inspection Coordinator epochs and successful
successor publication. Reconciliation or FailedPrecondition clears ACK evidence;
an ordinary Unavailable alone does not invalidate a previously acknowledged session.
Relay readiness additionally requires the exact local full-catalog/session/epoch
confirmation, no buffering/unconfirmed writes, and the actual remote ready owner.
Scheduling pause or Pod isolation is not itself startup failure.

The parent still pins and hashes its owned Child executable, checks actual UDS
identity/Relay binding and rechecks the actor generation. The complete observation,
including UDS connects/RPCs, has one five-second deadline. Embedded Adxlet runtime
may be observed without an independent Relay Child, but `can_upgrade` remains false:
Process Begin/Query and Relay handoff remain standalone-only. ExpectedRuntime
includes the actual `configuration_sha256`; `ready` is not a frozen Submission parameter.
No observation is synchronously awaited before the supervisor actor loop starts.
NodeInfo components expose an independent optional `sources` map keyed by role.
These are authenticated facts of the actual owned Process, not the current
ConfigMap版本声明 or historical startup selection: manifest `version`, verified
whole-package `archive_sha256`, and verified ServiceOverride bytes'
`raw_config_sha256` are independently nullable. Image processes have unknown
sources. Embedded mode retains both roles' configuration contributions on the
single owned Adxlet Process. Binary-only updates preserve actual configuration
provenance but replace binary provenance; local artifacts have no known archive
digest. Sources and runtime share the existing owned-generation/ready proof and
recheck, without another RPC observation or any ExpectedRuntime input expansion.
Unknown sources do not prevent runtime observation or upgrades, but cannot prove
full ready promotion without complete current source facts.
ConfigMap版本声明 supports initial target/ready selection and private configuration
snapshots. Active standalone updates may supply paired `--component-config-path`
and `--component-config-sha256`; omission preserves the current actual configuration.
An explicit descriptor renders the fixed deployment baseline plus that override,
while preserving current parent control wiring. Target acceptance matches both
prepared binary and configuration SHA; recovery restores the original full configuration.

Success emits compact schema-version-1 process JSON, freezing context, namespace,
container, config, role, commit, binary SHA-256, rollout ID, epoch and adxlet-only
origin. Nodes are sorted by ADX node ID. The first node is a separate canary batch;
remaining nodes are split by positive `--batch-size` (default 1). Batches reference
ADX node IDs, not local task IDs. Each node contains only `node_id`, Kubernetes/
container evidence in `kube` (including both observed resourceVersions), and the
exact Rust `submission`. Runtime, local task ID and service ID have one source of
truth: `submission.selection.expected`, `submission.task_id` and
`submission.service_id`. The 64-hex local ID is a canonical SHA-256
of context/namespace/container/config/role/Pod/node/service/rollout/epoch, independent
of input order, batch size or mutable runtime evidence. Adxlet Submission includes
frozen process maintenance identity; relay omits process identity entirely.

ResourceVersion is evidence only, not a future process-resumption incarnation
check. Apply validates plan/Submission consistency and frozen identity before
submitting. After submission, incarnation checks cover Pod/container/boot, not the
old PID, hash, sessions or upgrade eligibility.

## Process Apply

Linux publishing Jobs can advance exactly one explicit batch:

```sh
python build/release/node_rollout.py apply-process \
  --plan process-plan.json --journal process-journal.json --batch-index 0
```

Batch indexes are zero-based. A later invocation must explicitly select the next
batch, and all earlier batches must be successfully completed and, for adxlet,
released. Nodes in the selected batch run sequentially. No automatic batch
progression is performed. Input restore follows the failure contract below;
ready promotion is an independent explicit action.
Without archive flags, executables must already exist in the supervisor's configured local releases.

For role `adxlet`, `plan-process` and every `apply-process` observation before a
Submit or maintenance side effect require `node_info.step_protocol` to be exactly
the integer `2`. The marker identifies an adxctl whose Submit only prepares files
and exposes exit/start/rollback as publisher-driven Steps; a missing, null,
boolean or different value is refused before any Submit due to incompatible local
step semantics (including old all-in-one Submit implementations). Pod and Relay plans do not
require the marker. The normal adxlet publisher chain prepares files, acquires
maintenance, drives Exit and Start, then releases maintenance after current
Task/runtime and Coordinator session proof. The new adxlet registers itself;
the publisher does not preapprove its session. Target failure can drive one
publisher-controlled `rollback` that locally starts the saved original-version attempt. Original-version recovery still fails the batch. An
unproven outcome or a failed original-version recovery retains maintenance and
returns pending; there is no recursive rollback. Local fixtures are not cluster validation.

A completed standalone Relay upgrade may be followed by a separate adxlet
process upgrade against that serving Relay, preserving its existing Environment
bindings. An installed candidate that has not opened admission still rejects
adxlet takeover. Finish the Relay handoff before starting the adxlet upgrade.

If both the target and original-version recovery budgets expire, Query and
same-A Start and rollback requests only observe existing results; they do not restart a
candidate or grant another recovery window. Retain the serving Relay, sandboxes,
Environments and this task's maintenance while diagnosing the failure. Replacing
an adxlet binary alone does not update a fault in the still-running Relay, and
this workflow does not provide an automatic, lossless recovery after both
budgets are exhausted.

Adxlet publishing uses the plan's existing `origin` and HTTP timeout for both
`POST /v1/adxlet-process-upgrades` and
`POST /v1/adxlet-process-upgrades/query`. Writes accept only `action` (`prepare`
or `release`), `owner` and `node_session`; query accepts only `owner`. It returns
HTTP 200 with a flat `status` of `absent`, `active` or `released`. Active includes
`owner`, `origin_session`, `current_session`, `yielded`; released includes `owner`
and `node_session`; absent includes `owner`. Every response is checked against
the frozen owner and relevant saved sessions. An exact historical release receipt
wins over a later task's marker. Otherwise a foreign owner/epoch returns 409 and
is not absence. A 404 from an older Coordinator, invalid response or transport
failure refuses preflight before side effects. No new capability marker is used.

The management listener has no caller authentication. Restrict it to trusted
publishing Jobs and operator sources; never expose it through public API/Ingress.
Owner data is not a credential, and ServiceAccount/RBAC does not replace HTTP
network isolation. This boundary does not provide the former publisher mTLS
caller authentication. Deployment isolation has not been verified for this
revision. The publisher has no dedicated gRPC client, generated rollout stubs,
Supervisor certificate configuration or session-approval phase. The former
`--handoff-config`, `--handoff-stubs` and `--handoff-timeout` options are removed;
process promotion checks receipts through the same HTTP query.

### Remote Archive Preparation

Source implementation adds paired `--archive-url https://HOST/release.tar.gz` and
`--archive-sha256 LOWERCASE_64_HEX` to `plan-process` and `adxctl rollout submit`.
The descriptor exists only in each frozen Submission; commit and optional expected `--sha256`
continue to identify the release and selected role executable. Query and replay
compare the complete Submission, including the archive source. A changed URL or
archive digest under the same task ID conflicts. URLs require anonymous HTTPS
with a host and no fragment; redirects and proxy-environment forwarding are disabled.

With a remote archive, omit `--sha256` to let 本机adxctl准备 derive the selected
fixed executable digest from the schema 1 manifest authenticated by the whole-package
SHA. An explicit expected executable SHA remains a strict check. Without an archive,
the 镜像自带版本 installed-release path still requires `--sha256`. The publisher
script sends instructions and maintains rollout state; it does not download packages.
Before any exit or sealing action, the single worker persists
`task.prepared.binary_sha256` with the existing journal fsync contract. It never
fills the derived value back into Submission, so exact replay remains unchanged.
Target success and retained-candidate acceptance require this prepared fact; missing
or invalid prepared identity is not inferred from plan input or a listening endpoint.

### Read-Only Version Declaration

`plan-process --declaration-candidate candidate.json --configmap NAME` adds GET-only
candidate planning; `--declaration-key` defaults to `target.yaml`. The candidate
must be strict JSON (also valid YAML), with standalone schema 1, complete target
and ready roles and exactly one `upgrade_components` entry. Do not combine this
input with manual role, commit, binary SHA, archive or component-config flags.
Manual planning still requires its original role and target inputs.

The selected target derives the existing Submission's version, remote archive and
component config descriptor, without supplying a binary SHA. The plan freezes one
ConfigMap UID/resourceVersion/key and the exact original and candidate UTF-8 texts,
plus each node's declaration mount path. Only an unambiguous direct ConfigMap
volume/mount is supported; projected, subPath, subPathExpr and shadowed paths are
refused. Both existing runtime observations invoke node-info with this path and
verify the old declaration SHA against the current ConfigMap text, not the candidate.
A final GET requires the same UID and old text; metadata-only resourceVersion
changes are accepted and the fresh version is frozen. Old YAML is not parsed in Python.

`apply-process` uses the same process journal and frozen Submissions for candidate
plans. A single global, fsynced ConfigMap intent records a UID/resourceVersion/value
JSONPatch CAS before sending it through stdin. Only the selected data key changes;
an uncertain ACK is reconciled by GET, not by assuming the command failed. Foreign
UIDs/text and a candidate without this rollout's remote ownership marker are
refused. The first write requires the frozen source RV; metadata changes before
that write require replanning, not a fresh-RV takeover. The shared annotation
`adx.dev/rollout-input-fence` contains the canonical immutable full-plan SHA plus
`:applied` or `:restored`, atomically written with the selected data key under
UID/RV CAS. Confirmation requires matching UID, data and marker. Restored closes
the rollout irreversibly; later foreign markers are never overwritten by old tasks.

Image startup does not require a pre-created declaration key: an authoritative
ConfigMap GET may freeze `original_text: null`. A binaryData key is never treated
as absent. Missing data maps are supported; invalid/empty existing text is refused.
For an existing old declaration, both node observations require its pinned SHA and
successful typed observation, without imposing its desired mode on actual runtime.
For an absent key no mounted old SHA or successful desired observation is invented.
Candidate raw UTF-8 is limited to 4 MiB and normalized official desired JSON to
64 KiB; writes and projection SHA checks use the exact frozen candidate text.

Before the first write, unstarted nodes are freshly checked against original
Pod/container/runtime identities. Only new/preparation stages wait, bounded per
observation, for the frozen direct mount and exact candidate SHA/desired. Existing
submit intents retain Query/exact replay and accepted tasks retain release even
when the declaration later becomes invalid. No server preparation/recovery deadline
is refreshed by projection observation.

Failed batches restore input only after a fresh exact task Query and actual runtime
prove original selection/rollback or never-begin with the full original identity.
Adxlet also requires the current released-session maintenance receipt. Retained
candidates, unknown tasks and missing evidence do not authorize restoration. The
owned candidate key is CAS-restored to old text, or removed when originally absent;
other keys remain intact. An outstanding forward CAS is fenced by metadata CAS when
necessary before recognizing already-original input. Restoration ACK loss is
reconciled on re-entry; the batch remains failed and does not expand.


`adxctl rollout node-info --declaration-path /mounted/target.yaml` observes a
ConfigMap版本声明 only. Without this option it uses the deployment's
`external_declaration`; without either path the response field is null. This
option does not enable startup selection, watch inputs or submit an upgrade.
Startup still requires complete target and ready groups. Optional
`upgrade_components` defaults to an empty list and permits at most one `adxlet`
or `relay`; embedded declarations require an empty list. Both groups validate
absolute component config paths and lowercase 64-hex digests without reading
component config bodies.

The observation shares the startup declaration reader: one pinned FD, a 4 MiB
input limit and a fixed five-second observation budget. It does not perform
本机取包, render configuration or start processes. Success adds
`external_declaration: {path, raw_sha256, desired}` using the official typed
schema, including `artifact: {url, sha256}`. Desired state is not running identity.
Read/validation errors and desired JSON exceeding 64 KiB return only
`{path, error: {code}}` with a static code, never parser input or raw configuration.
Actual runtimes, `external_startup` and top-level `ok` remain independently
observable, including when the declaration changes or becomes invalid.


The dedicated rollout worker uses the existing Tokio runtime for HTTP, with
10-second connect and 30-second stalled-read limits. At most three requests retry
transport failures, 429, 500, 502, 503 or 504. Download, retry/backoff, both archive
passes, every manifest file hash and executable copy share the original monotonic
900-second preparation deadline and supervisor stop flag. Preparation failure
does not enter the Child replacement flow or stop the original process.
For retryable HTTP responses, valid `Retry-After` delta-seconds or HTTP-date
headers override the 1/2-second fallback. Past dates permit zero delay; absent or
invalid headers use the fallback. A valid delay exceeding the remaining task
budget, including an overflowing numeric value, fails preparation rather than
retrying early. Waiting remains cancellable and never refreshes the deadline.
Terminal I/O, cancellation, size and digest errors are not transport retries;
local I/O and exhausted transport failures retain their original error causes.
Planning rejects host forbidden/control characters and malformed percent escapes
before maintenance, while retaining standard DNS, IPv4 and bracketed IPv6 URLs.

Task-private exclusive `archive.partial` and `unpacked.partial` replace no global
installation, current symlink or shared cache. Actual compressed bytes are capped
at 512 MiB, checked against HTTP length and the frozen archive SHA-256 before
extraction. Two gzip/tar passes cap actual decoded bytes at 2 GiB including headers
and padding, require two zero headers, zero-only tar padding, valid gzip CRC/ISIZE,
and exactly one gzip member without compressed trailing data.

The first raw-header pass accepts at most 10,000 headers: ordinary files,
zero-payload directories and GNU longname `L` metadata up to 4096 bytes. The second
standard tar pass resolves GNU longnames, including CI SDK wheel names over 100
characters. This is not general tar metadata support: PAX, GNU longlink, sparse,
links and devices are rejected. A single leading `./` and the real `./` root
directory are supported; duplicate canonical targets, traversal, empty internal
components, backslashes and file/directory conflicts are rejected. Package modes
and scripts are never applied or executed.

Packages retain schema-1 `manifest.json` with `schema_version`, `commit`, `target`
and `files`; builder extras such as profile/with_afs remain accepted. The manifest
limit is 4 MiB. Duplicate JSON file keys fail; all ordinary files except the root
manifest must exactly match the manifest key set and hashes. Host target, commit
and selected role digest retain their existing checks and copy/publication path.
Full-file validation uses an iterative relative-path queue, a single hash read
buffer and constant open directory descriptors. Each queued path is reopened
component by component with NOFOLLOW from the pinned package directory; deep or
wide input does not create recursive worker stack frames or retain child FDs.
Traversal components and file chunks check the same preparation deadline/stop.
Source fixtures cover deep/wide trees and decoded limits with small private caps;
both production archive passes retain the fixed 2 GiB limit.

The plan is immutable canonical-hash input. Plan/journal reads reject symlinks,
nonregular files and JSON larger than 8 MiB. A Linux `fcntl` side lock is held for
the entire invocation. Journal writes use a mode-0600 temporary file, fsync,
atomic replace and directory fsync. Keep the same plan, journal and lock location
for recovery; do not delete the journal to retry an uncertain task. The journal
stores publisher action stages, first submit time, the local Task's attempt and
node session after Exit, the frozen A identity in `rollback_from` during recovery,
and a frozen release session and outcome. It does not duplicate owners, Submissions or complete Query responses.

Before initial operations the frozen runtime and Pod/container identity must
match. Submit intent and first-submit timestamp are durable before argv-only
`adxctl rollout submit`. Adxlet first queries the exact owner through the new
HTTP query endpoint and validates the response as a read-only preflight. Its
Submit only prepares files; a
nonterminal Task with a `prepared` object is required before maintenance.
Preparation failure with no execution and the unchanged complete original runtime
is a failed node without stopping the original or acquiring maintenance. A Task
still preparing returns pending and can be queried by the next invocation.
Relay retains its existing self-driven local execution and never calls Coordinator.
On resumption,
the same local task is queried first. A nonzero CLI exit (including server
not_found), timeout or invalid JSON is unknown, not evidence of non-execution.
Only unchanged Pod/container/boot and the complete frozen original runtime permit
replaying the identical Submission through existing server idempotency. Changed
or unreadable runtime retains pending state without Submit replay. No stderr
string is interpreted as protocol. Only exit zero plus complete JSON is a normal
response, and exit zero never establishes task success; returned Submission must
match the frozen one exactly. Replay never refreshes the first-submit timestamp.

Relay preparing/running tasks are queried every two seconds. The publisher waits at
most 2400 seconds from its durable first-submit intent, performs a final Query,
then preserves pending state. Reentry does not reset this timestamp; after the
limit it may still query and finish proven terminal release, but cannot replay
Submit. This is only a Relay publisher observation budget. Adxlet does not use
the first-submit timestamp as its execution clock: the local Task owns the
900/900/600 execution timers and fixed deadlines, which publisher retries never
refresh. A prepared Task waiting for maintenance does not restart preparation or
expire merely because its original preparation window has elapsed. Neither
timestamp nor timeout proves process exit. Unrecognized outcomes and transport
errors stop the current invocation with pending. Reentry queries first; the
recognized local intent/unknown stages below resume only their existing identity.
Interrupting the publisher is resumable, not a permanent rollout stop.

Once adxlet files are prepared, prepare intent is saved before POSTing the exact
`{action:"prepare", owner, node_session:original_session}` to
`/v1/adxlet-process-upgrades`. The complete prepare acknowledgement or an existing
active query result must match that owner, origin session, current old session
and unyielded marker before recording Exit intent. Before sending Exit, a fresh
HTTP query must still positively confirm the active marker and old session;
the first Exit also requires it to remain unyielded.
An uncertain prepare acknowledgement first queries that marker; only a valid
`status:absent` permits the same prepare body to be replayed. Conflict, errors and
unknown responses retain pending state. Every subsequent mutation has its intent saved first.

Exit is `adxctl rollout step --task-id TASK --step exit --attempt ''
--node-session ''`. The publisher queries the same Task until its existing
`adxlet_execution.stage=exited` proves local reap, checks the original PID and
local attempt/session/deadline, and separately queries Coordinator's yielded
marker. It freezes that local attempt/session, then calls Start with the same
Task/attempt/session after confirming this owner's Yield and local reap. Before
Start, Coordinator's current session may still be the origin; no replacement
session must be reserved. Start owns bootstrap, readiness, actual
binary/configuration checks and Complete. The new adxlet registers with its
saved identity. Coordinator authenticates the Node, checks the unchanged RPC and
Relay addresses, confirmed Yield and initial reconciling/closed-admission report,
then calls existing `GetSession` to verify the actual process. Expired heartbeats
and Coordinator restart do not skip that check. It rechecks owner/current session,
uses the existing replacement CAS, then follows normal registration, Inspect and
recovery. Publisher subsequently checks the actual registered session and fresh
restored Task/runtime before release.
Successor completion and the parent's ready query match the existing
rollout/local-task/epoch owner and current process session. The inherited
predecessor must be sealed and belong to a different session. Begin, sealing and
unfinished predecessor queries still compare the full frozen request. A prior
same-session Pod `released` record is retained; active Pod controls still block
handoff. A fallback process earns its own ready record after recovery, and a
release receipt never substitutes for that recovery. Handshake errors identify
the failing `successor <stage>` and include node/session/task context in the log.
The local adxlet admin address remains `<state_dir>/<service_id>-admin.sock`.
Bootstrap uses a separate fixed `<state_dir>/<service_id>-start.sock`; full task
and attempt identities remain in the journal. The bootstrap carries owner,
selected node session and the fixed startup deadline. No new RPC
or startup ordering is introduced. Standalone Relay upgrade and rollback use
`<state_dir>/s/r-<key>/route.sock` and `s/o-<key>/route.sock`, respectively, where
the 32-character key is derived from the complete task ID. Preparation checks
both Relay paths and the target/original adxlet bootstrap paths against the
100-byte absolute-path limit before any old-process exit operation.
Publisher does not invoke Yield, registration or Complete itself. An uncertain
Exit/Start acknowledgement first queries the same Task and
maintenance identity; proven completed steps are not repeated. `exit_unknown`
replays Exit's original empty attempt/session input, including the local read-only
observation after expiry. The local operation retains any already minted identity
and fixed deadline. A Task merely marked exited with an unexplained live process
remains pending; null runtime alone is never process-exit proof.

Target Start replays the same saved attempt/session A for local `start_intent`
and `start_unknown`, using local idempotency. `target_commit_unknown` only
reconciles that same A through Start's Complete/Receipt path; after expiry the
local replay is Receipt-only. Publisher does not request Rollback for this
unresolved commit stage. A completed Task and current runtime always take the
success path, including a late committed receipt. The publisher does not start
a new target after the local 900-second deadline. A target Start failure
that remains unresolved after same-A replay, or exhaustion of that target window,
can enter the single original-version recovery below.

Rollback is saved as publisher intent with the frozen A identity before
`step --task-id TASK --step rollback --attempt A --node-session A_SESSION`.
The publisher checks the same owner's yielded maintenance marker before first
submitting rollback. Local Rollback first reconciles A's receipt: committed A
finishes successfully without stopping it. An ambiguous known candidate remains
protected. Before the first StopFailed, the worker saves one absolute 600-second
deadline covering sealing A, real reap, original Start, bootstrap and readiness.
`rollback_intent` and `rollback_unknown` preserve A and that deadline.

Only after the actor confirms A sealed and truly reaped or NeverCreated, and the
pinned original executable/configuration are verified, does the worker mint and
save B at `rollback_ready` with `rollback_from={attempt:A, node_session:A_SESSION}`.
It then runs the existing Start path internally using those original files.
External Start accepts only target A; publisher never sends Start(B). An accepted
rollback can start B even if the script disconnects. Coordinator unavailability
also does not prevent local B startup: B can wait for registration while publisher
remains pending. It never releases maintenance automatically.

Replaying rollback always names A, including when B already exists. The local
worker reuses the saved B and deadline, resaving after an ambiguous write and
continuing its Start/Receipt path. A live same-task worker is not duplicated.
`original_start_intent`, `original_start_unknown` and `original_commit_unknown`
refer to that B; after expiry its replay observes Receipt without spawning or
refreshing the budget. A failed original retains maintenance without a recursive
rollback or new identity C. The publisher validates B's exact source and adopts
its identity before terminal classification, even if B is already selected when
the first rollback response is observed. It still verifies current actual runtime
and registered session before release.

Adxlet process publisher journals use schema 2; Relay and Pod journals remain
schema 1, and process plans remain schema 1. A schema-1 adxlet journal is refused
before remote effects; finish in-progress work with its original publisher version
before upgrading. There is no journal migration or old rollback alias. Deploy one
publisher per owner: the nonblocking journal flock only serializes users of that
same journal/lock path, and cannot enforce this convention across different paths.

Original selection requires the current `failed/original_selected` Task,
`rollback_from` naming A, B's candidate PID and session, the original binary and
configuration hashes, and the unchanged independent Relay session. It releases
this owner's maintenance restriction and reports failed. A cancellation after
maintenance but before the first Exit may also release as failed only when the
Task has no execution, its complete actual runtime exactly equals the frozen
original, and Query proves the same owner/current original session is unyielded.
An unknown exit, lost process or changed runtime does not qualify. Release does
not change an administrator's independent pause.

Tracking rejects Pod UID/containerID/restart/nodeName or supervisor boot changes.
Temporary null runtime or false upgrade eligibility does not imply process exit.
Terminal classification requires fresh complete runtime evidence: adxlet checks
candidate PID, attempt, exact node session, unchanged independent relay session
and target/original executable hash;
confirmed pre-begin failure requires the unchanged original runtime. A fixed
release session and outcome are atomically journaled with release intent before
any release POST. The owner is derived from the validated, plan-hash-bound plan;
the persisted session is never refreshed from a later runtime. The first release
requires the current terminal Task/runtime and matching active Coordinator
session, then POSTs `{action:"release", owner, node_session}` to
`/v1/adxlet-process-upgrades`. An uncertain release acknowledgement uses the HTTP
query with that exact owner. On recovery from a durable release intent, query is
checked first: `status:released` with the exact owner and frozen release session
proves this rollout already released, even if a later restart changed its PID or
another task now has maintenance. If no receipt exists, the publisher re-queries
the same Task, checks its exact Submission and terminal outcome, and obtains fresh
complete runtime evidence with the frozen release session before replay. A
foreign marker returns 409; neither that conflict nor `status:absent` proves
release. The release body never changes. Unproven outcomes retain protection.
The former three `/v1/node-upgrades/process-maintenance` routes and publisher
NodeHandoff Query/ReplaceSession RPCs are removed. The owning Node continues
using authenticated Yield/QueryRelease for its local maintenance state.

Relay progress uses `execution.phase` (not adxlet's `adxlet_execution.stage`).
Relay completion checks target hash, candidate relay session and unchanged node
session in the same incarnation. Original-file recovery checks original hash and
the recovered candidate session. Retained-target loss and cancelled/pre-begin
failures are failures, never permission to advance. Relay requires no maintenance
release. A proven adxlet original-file recovery remains a failed batch even after
its maintenance release succeeds. Failure stops later batches, and promotion requires all target
batches to have succeeded.

Output is machine JSON with `status` equal to `completed`, `failed` or `pending`;
only a completely successful selected batch exits zero. Invalid plan/journal or
batch admission uses `PlanRefused` and exits nonzero. Pending preserves journal
intent and protection for the same command to resume.


## Pod Plan

`--config` is required and is an absolute container-internal deployment YAML path.
Planning reads existing NodeInfo through adxctl in embedded or standalone mode,
binding actual node/session, parent boot and running container to Coordinator
inspection. Original Pods may already have process sources or external startup;
replacement provenance constraints do not apply to them.

Optional `--declaration-candidate JSONFILE --configmap NAME --declaration-key target.yaml`
uses shared strict JSON, direct mounts, 4 MiB raw/64 KiB desired limits and the CM
CAS journal. Pod candidates require empty `upgrade_components`; embedded groups
require the same package. A missing old key is frozen as null. Before a new DELETE,
the owned candidate must be CAS-confirmed and observed at the bound projection.
Already ambiguous DELETEs retain their fixed UID/RV reconciliation.
Every actual resend while the old UID remains non-terminating rechecks the owned
candidate and projection; a saved intent is not evidence that DELETE executed.
Declaration desired observations normalize omitted `upgrade_components` to `[]`
before the 64 KiB check, while original candidate bytes and raw SHA stay frozen.

Replacement validation uses default NodeInfo without a path override. Image-only
replacements require null external declaration/startup and explicitly empty actual
sources maps. CM replacements require default initialization at the candidate
path/raw SHA, target startup (not ready fallback), complete matching role sources,
and actual binary/effective config digests matching the initially selected target.
Runtime `status.imageID` is opaque nonempty identity, recorded and rechecked with
container identity and parent boot. No OCI index-to-platform mapping is claimed;
the target digest is compared to PodSpec, not status.imageID.

Abort retains verified services and uses existing strict rollback verify for
unfinished replacements without demanding candidate provenance, allowing valid
ready fallback cleanup. Aborting cleanup does not require cross-node recovery
fingerprints, while session, checkpoint scope, emptiness and permit checks remain.
Template and owned CM restoration/late-write fences
precede finalize; other CM keys are preserved. Stop starts no new deletion and
leaves restoration to explicit abort.

After confirmed abort freeze, owned CM restoration and its `rollout-input-fence`
complete before original-template restoration, restore-original or cleanup release.
Stop only freezes. Cleanup node reports include current component sources and
actual executable/configuration SHA observations, or explicit unknown if NodeInfo
cannot be observed; unknown does not invalidate historical Coordinator receipts.
`mixed_images` describes image differences only. Equal images do not prove equal
component versions or restored original components.

`node_rollout.py plan-pod` uses only Python's standard library. It reads a native
DaemonSet or StatefulSet and its current controller-owned Pods through
`kubectl get`, and reads Coordinator management capabilities and each mapped
node's inspection through HTTP GET. `--daemonset NAME` and `--statefulset NAME`
are mutually exclusive; StatefulSet plans freeze `workload.kind` explicitly while
legacy DaemonSet plans omit it and default to DaemonSet. Planning itself performs
no POST or Kubernetes mutation; `apply-pod` is a separate normal-path publisher
command described below.

Pod inventories use the raw namespaced API endpoint:
`kubectl --context CONTEXT get --raw /api/v1/namespaces/NAMESPACE/pods`, with
namespace path quoting and no `-o json`. High-level `kubectl get pods -o json`
may emit a generic List and is not accepted as typed inventory evidence. The
shared reader requires v1/PodList, nonblank list resourceVersion, no continuation,
and unique-UID v1/Pod items. Omitted item kind/apiVersion are supplied by the typed
PodList; explicit other kinds/versions are refused. Items must have
exact namespace and nonblank name/UID/resourceVersion. Terminating items
remain in inventory for state-specific reconciliation, not disappearance proof.

```sh
python build/release/node_rollout.py plan-pod \
  --context production --origin http://coordinator:9090 \
  --namespace platform --daemonset adxlet --container adxlet --config /opt/adx/config/deployment.yaml \
  --target-image registry.example/adxlet@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --task-id release-20261110 --epoch 1 \
  --node adxlet-abc=registered-adx-node-a \
  --node adxlet-def=registered-adx-node-b > plan.json
```

The origin must be the trusted, isolated Coordinator management HTTP listener,
not the public Gateway. Credentials, non-root paths, proxies and redirects are
not supported. The example address and all workload/node identities must be
replaced with deployment-specific values. Running this command invokes the
operator's configured `kubectl`, resolved to an absolute path. `--context` is
required for plan-pod, used for every Kubernetes read and frozen in the snapshot.
This is not an offline cluster acceptance test.

Repeat `--node POD=NODE` for every current source Pod. `NODE` is its explicitly
confirmed ADX node ID, never an inferred Kubernetes NodeName. Missing, extra,
duplicate Pod mappings and reused ADX node IDs are refused. The operator is
responsible for selecting the ADX node IDs. Each fresh inspection must report
`observed_pod_uid` equal to that source Pod's actual Kubernetes UID. This field
comes from adxlet's startup Downward API environment, not task-bound `pod_uid`.
Coordinator reuses its authenticated node RPC, matching process session and fresh
live-node checks; labels, nodeName and historical authority are not UID evidence.

The publisher does not validate or change the workload's `spec.updateStrategy`.
Deployment operators must prevent controller-driven Pod replacement from racing
the publisher's evacuation and conditional deletion, including while a task is
interrupted. Managing `OnDelete` outside the script is one way to satisfy that
deployment prerequisite. The publisher patches the image template before
per-node evacuation; accepting a plan does not prove that an uncontrolled
`RollingUpdate` is safe. No automatic strategy switch or restoration is provided.

A single ADX worker can follow this Pod replacement workflow only after both
Coordinator and node inspection prove it strictly empty. Running and Paused
Environments must first move to another eligible ADX node; migration excludes
the source even if it has spare capacity. Without a target, the publisher remains
pending in evacuation and does not issue Pod DELETE. Business inactivity alone
does not mean empty. There is no same-node checkpoint/restart/restore fallback in
this workflow. Replacing the only empty worker still leaves a window with no
available worker. The deployment-controller prerequisite above continues to apply.

The selected container in the workload and every source Pod must exist. Pods are selected
by controller owner UID, not labels. DaemonSet requires one source Pod per
Kubernetes host; StatefulSet requires each owned Pod to keep its stable
`<workload>-<ordinal>` name with a canonical decimal ordinal (`workers-0`,
`workers-7`; `workers-00` or `workers-debug` are refused because the controller
never recreates them), allows several logical nodes per host, and may replace a
Pod on a different host while keeping the same stable name.
Terminating objects, missing UID/resourceVersion, incompatible management
or node versions, absent capability/rootfs evidence, unknown inspection results,
HTTP/JSON failures, timeouts and observed identity/object changes fail closed.
Management HTTP errors preserve the Coordinator's structured `error.code` and
`error.message` within the same response size/deadline limits; unstructured bodies are not echoed.
Each response is limited to 8 MiB, Kubernetes requests have a 10-second request
timeout and a 15-second process deadline; HTTP operations have a 15-second budget
with finite socket timeouts. DNS resolution remains subject to OS resolver timing.

Both the workload template and each source Pod's selected container must declare
exactly one `ADX_POD_UID` entry using the following Downward API source (omitting
`apiVersion` also defaults to `v1`). Literal values, duplicates and other field
paths or API versions are rejected:

```yaml
env:
  - name: ADX_POD_UID
    valueFrom:
      fieldRef:
        apiVersion: v1
        fieldPath: metadata.uid
```

Deployment service `env`, including merged profile overrides, cannot override
this reserved key. Managed services inherit it from the parent container.
Adxlet captures it once when opening its upgrade gate, never persists it in
authority and never reloads it per inspection. Missing values mean unknown;
explicit whitespace, non-UTF-8 or values over 128 UTF-8 bytes are startup errors.
The publisher does not read deployment configuration or its environment secrets.

Success writes one snapshot JSON to stdout. `begin` is a payload for the existing
`POST /v1/node-upgrades/tasks`, but this command does not send it. The snapshot
records workload UID/RV, original template image, each Pod's name/UID/RV, owner,
original/target image, Kubernetes node name, explicit ADX node binding, inspected
session and capability evidence. Preserve it as immutable planning input; no
permit or durable task is created. Runtime refusals write an `error.code` of
`PlanRefused` to stderr and exit nonzero; argument syntax errors use argparse's
normal stderr/nonzero behavior. No partial snapshot is emitted on refusal.

The plan stores original and target template SHA-256 fingerprints, not the full
template or environment values. Only the selected container image differs in
these fingerprints. Source Pods are ordered by name; the first is a separate
canary, followed by fixed batches of positive `--batch-size` (default 1).

Objects are read twice and node sessions/rootfs/actual Pod UID evidence are rechecked. This is
not an atomic cross-system snapshot or a guarantee against later changes.
The publisher revalidates identity and uses current UID/RV CAS conditions;
the saved RV is not authorization to delete. HTTP connectivity and even
`strict_empty` in this output are not proof or permission to delete a Pod.
Migration, authoritative empty checks and delete permits remain exclusively in
the existing PR178 node-upgrade APIs. Rootfs evidence alone does not establish
cross-node recovery compatibility.


## Pod Apply

The normal-path Linux publisher advances one explicitly selected batch:

```sh
python build/release/node_rollout.py apply-pod \
  --plan plan.json --journal pod-journal.json --batch-index 0
```

The command validates the immutable plan and binds its canonical hash to a private
atomic journal, holding a nonblocking `fcntl` side lock throughout the invocation.
Reuse the same plan, journal and lock paths on recovery. It never automatically
selects a later batch. Prior batches require both durable local verification and
Coordinator node phase `verified`. Only the final batch can complete the task.
Output is `completed` or `pending`; only selected-batch completion exits zero.
An invocation waits at most 2400 seconds, polling every two seconds; this is an
observation budget, not proof that any timed-out write did not execute.

Begin uses the existing plan `begin` body. Node and task actions use fixed owner,
epoch and deterministic nonempty operation IDs, with intent durable before writes.
Coordinator task GETs reconcile ambiguous results and observe unsettled migration
operations. Source inventory uses authoritative paginated instance queries with
page size 1000; changed epoch/revision/count refuses the read and reentry starts
at page one. Zero generation/revision are valid. Migration remains owned by the
PR178 workflow, including Pause/checkpoint/source retirement; its POST result is
not treated as a task. The exact persisted migration operation must be Completed.
While an Environment is busy, the publisher retries the same migration after
adxlet yields its controller between idle checks. If a network policy update
advanced the revision on the same source assignment, the authorized checkpoint
uses the current state and the publisher keeps the original operation ID and body.
The publisher prioritizes already Paused Environments. If a Running Environment's
prepared migration is waiting for idle, the publisher can transfer a different
Paused Environment first, retaining the original migration intent. A waiting
Resume can then continue after ownership moves to an eligible target.

The workload PATCH tests UID, fresh RV and original image, changing only
the selected image. Full templates stay in memory. A lost PATCH ACK is reconciled
against the target template hash. If only RV changed while UID and the
complete original template hash still match, the publisher atomically saves the
fresh-RV image-only CAS intent before retrying; the obsolete RV cannot authorize
a late write. Foreign template changes remain pending. No full template or
environment secrets enter argv or the journal.

Source quiesce requires exact live UID/session, unknown=false, strict_empty=true,
four zero inventory counts and a definite permit/control revision. Lost ACKs are
reconciled through task GET and fresh inspection. If control was saved as
quiescing with the exact current binding but no permit or delete intent, fresh
actual UID/session, unknown=false and same task/epoch inspection permits replay
of the original fixed-body quiesce, even if Node still reports the older revision
and evacuating phase. This is not evidence of non-execution: server serialization,
new-revision empty grants and raw strict-empty checks still apply. Existing permits
and delete intents are never replaced by this recovery; progression still requires
the exact final Quiescing permit/revision proof. Delete intent freezes permit and
exact Pod RV before the DELETE. Kubernetes transport uses argv-only JSONPatch and
`kubectl delete --raw /api/v1/namespaces/NS/pods/NAME -f -`, with bounded stdin
containing `v1/DeleteOptions` and both UID and resourceVersion preconditions.
It never uses force deletion or UID-only preconditions.

After ambiguous DELETE, only a complete authoritative PodList proving the original
UID absent permits replacement progression. Original UID still present remains
pending or replays the identical UID/RV DELETE; nonterminating changed RV requires
a later revoke workflow, never deletion under a new RV. A terminating original
waits for authoritative disappearance even when its RV changed; it is not exit proof.
Replacement must have the same controller UID, target digest, Downward UID and a
fresh authenticated session, with protocol 3, unknown=false, isolated,
task/epoch-matching inspection and four zero inventory counts before replacement
quiesce and new-permit verify. A DaemonSet replacement must stay on the source
host; a StatefulSet replacement keeps its stable Pod name and may move to another
host, recording that host. Legacy DaemonSet journals that predate the recorded
host resume the source host; StatefulSet always requires its recorded replacement
host and never assumes the source host.
Local verification requires an explicit successful, complete verify-node HTTP
task reply with the exact owner/epoch and verified replacement UID/session.
Task GET phase verified, fresh actual verified control and a scheduling boolean
cannot prove the server completed scheduling cleanup, especially for an originally
paused node. After any lost ACK, replay the same fixed verify-node intent until
that successful reply arrives. First verification still requires strict Quiescing
permit/revision proof; already verified Nodes skip that old-permit check and may
have legitimate new work. The server supports this same-body replay.

Coordinator `evidence()` masks HTTP strict_empty for the not-yet-bound isolated
replacement. This precheck therefore does not require that masked flag; the
server's raw `replacement_binding` inspection still strictly checks emptiness.
After binding, Quiescing evidence must include strict_empty=true and the exact
new permit/revision.

本机 adxctl run 下载并准备版本包；发布脚本只发送指令和维护发布流程。
初始化与主动升级的失败恢复会先确认本机启动的进程退出，再启动相应完整配置。
ConfigMap版本声明的 ready 提升使用上述独立显式入口。
Do not delete journals or switch owners to bypass an
uncertain outcome.

## Stop And Cleanup

`stop-pod --plan plan.json --journal pod-journal.json` durably blocks normal apply
and freezes the same task through abort stage freeze. Only a complete successful
freeze HTTP task reply confirms that grant revocation/cancellation processing
finished; GET Aborting alone proves only that new migrations are fenced. Lost
freeze ACKs replay the exact fixed body before stop can report stopped.
This explicit stop differs from interrupting a normal publisher invocation.
`abort-pod` with the same arguments continues cleanup, never creates migrations
or moves migrated Environments back. Unknown operations retain maintenance;
freeze may replay to let the server cancel Prepared work.
Before any Begin intent, stop/abort is local with no remote task or effects.
An ambiguous Begin intent is closed by replaying the exact Begin before freeze;
a failed GET is never absence proof. Completed and Aborted tasks receive no
freeze and are reported from fresh current workload evidence.

Cleanup fences outstanding DELETEs using UID/fresh-RV annotation JSONPatch before
revoke-delete, and waits rather than restoring terminating originals. Original
restoration requires an explicit compact restore-original success receipt, not
task phase or scheduling boolean inference. Fresh released control must retain
the exact task/epoch/session/revision. Admission is reported, not compared with
the stale task original_paused snapshot; the server respects its latest pause
owner intent. Replacements remain on the target
image; cleanup reuses the normal replacement leg with rollback=true and expects
Aborting verify receipts. Only this Aborting/rollback combination can omit the
cross-node recovery profile: the current session must still be live, routable,
unfenced and known, with a valid checkpoint scope and all operations settled.
Unverified replacements still require strict emptiness and the exact permit;
same-identity verified replays do not force another evacuation. Normal Active
verify keeps its full recovery checks even if rollback=true. Cleanup does not
qualify a profile-less node as a future migration/restore target.
Reuse selects only the cleanup node, never source
prepare/migration or another member of its batch. Exact original or owned target
templates are allowed during this replacement leg. Template restoration changes only the publisher-owned
image with full template hash/UID/RV CAS. An original template with the old
normal PATCH RV is fenced by metadata CAS before finalization. Output is partial
with fresh node outcomes/current images, never a claim of full version rollback.
Finalize and terminal reentry require a complete owned Pod inventory: one
nonterminating Pod per planned host (DaemonSet) or per stable name (StatefulSet),
no extras, exact owner/UID/session and original per-Pod image or verified target
image. Unknown current identity returns
pending rather than labeling saved images current. Originally mixed images remain
mixed; verified replacements are not downgraded.

Partial abort preserves business on target Pods already verified by normal apply:
it checks their current identity and sends same-identity verify for explicit
successful scheduling cleanup, without forcing quiesce, emptiness or evacuation
again. Only not-yet-verified replacements require strict-empty/new-permit/verify.
No Aborting migration is started.

Injected source fixtures include strict metadata JSONPatch and DELETE UID/RV CAS,
freeze/revoke/restore/verify ACK loss, terminal finalize ACK loss, untouched and
ambiguous Begin, terminating fence races, same-name new UID, mixed replacement
reuse, unresolved migration, foreign template, late target PATCH fencing and
extra owned Pod refusals.
The publisher freezes every owned original UID/session and Begin sets all original
bindings; replacement binding does not erase them. There is no reachable
late-original registration branch here, so register_source is unnecessary and no
new inputs are accepted. Unknown or newly substituted original sessions are
refused, never guessed or used to rewrite frozen identity.

## Ready Promotion

After completing all batches and the operator's business observation, invoke
`apply-process --plan PLAN --journal JOURNAL --promote-ready` or
`apply-pod --plan PLAN --journal JOURNAL --promote-ready`. This is a separate
invocation, separate from batch progression. Promotion requires an existing matching, locked journal and a
ConfigMap candidate. Failed, retained, unknown, stopped or aborted work cannot
promote. Process tasks are freshly queried with the exact frozen Submission and
prepared digests; Adxlet also requires its exact historical released receipt.
Pod promotion requires the completed task and all verified nodes/completed moves.

Current whole-owned-group NodeInfo, not historical startup selection, must prove
every candidate version/archive/raw-config source. Standalone checks both owned
roles; embedded checks both contributions on its one Adxlet Child. Runtime binary
and config digests must be valid; the selected process role also matches its
historical prepared content. Ordinary Child restart within the same container and
resident parent is allowed, including new PID/sessions, but Pod/container/parent
scope changes are refused. Peer binary content is not guessed or downloaded.

Promotion changes only `ready.components` to the complete target components in
deterministic official JSON. The frozen plan/candidate are unchanged. A durable
promotion intent precedes UID/RV/value CAS of data plus this plan's `:promoted`
ownership marker, followed by bound projection raw/typed observation. Unknown
writes repeat fresh group and frozen scope proof. If only metadata advances RV
while this plan still owns the exact candidate, a refreshed CAS intent is saved
before retrying. Confirmed own-promoted effects still recheck frozen scope, but
do not require target sources again and are never rewritten. Foreign ownership, including identical text,
is refused.

## Verification Boundary

The recorded 2026-11-14 checks apply to the preceding publisher mTLS design:
162 process/Pod publisher tests, 24 real mTLS client tests, 86 deployment lib
tests, four adxlet process preflight tests, five
Coordinator handoff/pause tests and two Pod regression tests. Four-module
all-targets/all-features compilation, workspace Clippy with `-D warnings`, format,
documentation and diff checks also passed. Ruff/Pylint passed for the six Python
files changed by that revision. The full PR-base Python precheck still reports 388
pre-existing diagnostics and is not recorded as passing. Exact commands and logs
are retained in this workspace at `out/adx-grpc-handoff-20261113/RESULT.md`.

For the 2026-11-16 HTTP and automatic-registration revision, 68 process and 101 Pod
publisher tests, five handoff/storage tests, two new real-RPC maintenance tests
and one existing ordinary-restart RPC regression passed locally. Ruff/Pylint for
the three changed Python files, workspace format checking and all-targets/all-features
Clippy with `-D warnings` also passed. Commands and complete logs are in
`out/publisher-http-20261116/RESULT.md`. Local unit,
component and fault-injection checks do not validate management-network isolation,
a cluster upgrade, cross-Worker recovery, production downloads or a new image.
That local verification run performed no Beijing cluster operation or image build.

A separate deployment-session record on 2026-11-16 confirmed abort cleanup for
`pod-retest-20261116-1700`: the patched Coordinator processed the original
plan/journal to `Aborted / partial`, retained the replacement Node0 UID/session
and target image, left Node1 on its original Pod/image, restored the workload
template and released this task's maintenance. Both nodes were Ready and had no
Environments; no administrator pause was set in that field case. This validates
that empty-replacement cleanup case, not normal upgrade completion, new recovery
profile activation, cross-Worker Environment migration or a full version rollback.
Local evidence and the deployment handoff are indexed in
`out/pod-abort-cleanup-20261116/RESULT.md`.

A subsequent empty-Node0 field run on the same date passed normal Active
`verify-node` with complete measured recovery evidence, a new bootstrap ConfigMap
and a PATH-fixed Node image. The replacement moved from Worker .168 to .210;
Node1 was unchanged. Only batch0 was selected, then the remaining task was
deliberately aborted to `partial`; the full task did not become Completed.
The image reused existing Rust binaries/rootfs. This verifies the empty single-node
upgrade path, not live Environment migration, cross-Worker restore, a full
two-node release or deployment of every latest Node source change. Evidence and
review corrections are in `out/pod-normal-verify-20261116/REVIEW.md`.
