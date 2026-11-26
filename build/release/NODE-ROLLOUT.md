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
规划时每个选中 Pod 和 NodeInfo 各读取一次，记录 Pod UID、namespace/name、nodeName、
非终止状态、containerID、restartCount 和运行状态。
Exactly one role/node-ID component must be standalone, upgradeable, and have a
complete runtime. NodeInfo and runtime Pod UIDs must equal Kubernetes UID, boot
nonces must match. PID、可执行文件摘要以及 node/relay session 冻结在计划中，执行前重新核对。
This is not an atomic snapshot or proof against later changes.

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
NodeInfo 的可选 `sources` 按角色记录实际 Process 的认证版本、整包摘要和原始配置摘要，各项可独立 unknown；来源与 runtime 共用 owned Process 观察及复核。
配置输入由 IaC 或运维准备。主动升级可提供 `--component-config-path`，读取容器内已挂载的
`ServiceOverride` JSON；描述仅包含 `mount_path`，不要求配置文件摘要，省略时保留实际配置。
显式配置以固定部署基线生成私有快照并保留父控制边界，目标验收核对二进制与实际配置摘要，失败恢复使用原完整配置。
同一计划的所有批次使用相同的远端归档和配置输入描述；配置输入与各节点固定部署基线合并，
因此实际配置摘要允许因节点而不同。发布器在任何副作用前拒绝无法无损转换为 CLI 参数的 Submission。

Success emits compact schema-version-1 process JSON, freezing context, namespace,
container, config, role, commit, binary SHA-256, rollout ID, epoch and adxlet-only
origin. Nodes are sorted by ADX node ID. The first node is a separate canary batch;
remaining nodes are split by positive `--batch-size` (default 1). Batches reference
ADX node IDs, not local task IDs. Each node contains only `node_id`, Kubernetes/
container evidence in `kube` (including the observed resourceVersion), and the
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
progression is performed.
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

独立 Relay 先完成交接，再对正在服务的 Relay 执行单独的 adxlet 进程升级，保留已有
Environment 绑定。候选通过普通 `BeginBindings`/`ReplaceBindings` 完成全量同步并开放准入；
首个 snapshot 结果未知时重放冻结内容，精确 ready 后只同步 canonical 最新增量。
交接保持 owner/epoch 校验与旧端真实退休 ACK，父 supervisor 证明旧 Child 已真实 reap 后
才能 Finish；未完成交接时仍拒绝 adxlet takeover。

Relay 升级使用 `adx.node.v1` 的 `BeginDrain` 和 `Reserve`/`Cancel`/`Switch`/`Finish`。
BeginDrain 的成功回执证明业务 TCP 与 health 监听均已真实释放，同 task/session 重放返回
相同释放回执和固定期限；取消调用不撤回已接收排水。Switch 同时携带旧端释放或真实 reap
证据，更换失败候选时还须携带该候选 Child 的真实 reap 证据。

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
caller authentication. Before rollout, verify that the deployed listener is
reachable from the publishing Job and designated operator sources, and that a
workload outside those sources cannot reach it. Verify that no public API or
Ingress route exposes it. Deployment isolation has not been verified for this
revision. The publisher has no dedicated gRPC client, generated rollout stubs,
Supervisor certificate configuration or session-approval phase.

### Remote Archive Preparation

Source implementation adds paired `--archive-url https://HOST/release.tar.gz` and
`--archive-sha256 LOWERCASE_64_HEX` to `plan-process` and `adxctl rollout submit`.
The descriptor exists only in each frozen Submission; commit and optional expected `--sha256`
continue to identify the release and selected role executable. Query and replay
compare the complete Submission, including the archive source. A changed URL or
archive digest under the same task ID conflicts. URLs require anonymous HTTPS
with a host and no fragment; redirects and proxy-environment forwarding are disabled.

远端包可省略 `--sha256`，由本机 adxctl 从整包 SHA 认证的 schema 1 manifest 派生目标摘要；显式提供时严格比对。本地已安装版本仍要求角色 SHA。单个 worker 在退出或封存前将 `task.prepared.binary_sha256` 持久化，原 Submission 保持不变；目标验收核对该准备事实。

File preparation is owned by the local adxctl worker. Downloads, retries,
archive/manifest validation and private executable copies share one fixed
900-second preparation budget; a preparation failure leaves the original process
running. The publisher neither downloads nor unpacks artifacts. Download limits,
Retry-After handling, supported archive entries and validation rules are documented
in [the adxctl manual](../../docs/deployment/adxctl.md).

The plan is immutable canonical-hash input. Plan/journal reads reject symlinks,
nonregular files and JSON larger than 8 MiB. A Linux `fcntl` side lock is held for
the entire invocation. Journal writes use a mode-0600 temporary file, fsync,
atomic replace and directory fsync. Keep the same plan, journal and lock location
for recovery; do not delete the journal to retry an uncertain task.
Journal 保存 publisher 动作阶段、首次 Submit 时间，以及 Exit 后冻结的目标 A 的
`attempt`/`node_session`；不镜像 B 身份、owner、Submission 或完整 Query 响应。
`rollback_intent` 始终重放 Rollback(A)，B 仅从本机 Task 查询取得；
`release_intent` 单独冻结真实结束 session 和 outcome。

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
errors stop the current invocation with pending. Reentry queries first.
下述本机阶段只继续已保存的身份；`error` 表示失败事实，不派生额外 unknown 阶段。
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

Exit 使用 `adxctl rollout step --task-id TASK --step exit --attempt '' --node-session ''`。
Publisher Query 同一 Task，等待 `adxlet_execution.stage=exited` 证明本机真实 reap，
核对旧 PID、新 attempt/session 和固定 deadline，再单独核实同 owner 的 Yield。
Journal 此时冻结目标 A 的 attempt/session，并以同一身份发 Start；启动前不要求
Coordinator 的 current_session 已切为 A，也不预留 replacement session。
本机 `starting` 拥有 Start、bootstrap、ready 和实际 executable/configuration 检查；
核对完成后先持久化 `committing`，再发 Select。

`adxlet_execution` 以公共字段 `old_pid`、`error`、`attempt`、`node_session`、
`deadline_unix_ms` 加扁平 `stage` 描述进度。stage 仅有 `exiting`、`exited`、
`starting`、`committing`、`stopping_for_rollback`、`selected`：
`candidate_pid` 在 starting/stopping_for_rollback 可空，在 committing/selected 必有，
exiting/exited 不携带。Exiting 可尚未生成新身份，或身份已生成但写盘结果未知；
重试始终保存同一身份，不因失败换 attempt/session。`error` 保留失败事实。
`leg={"kind":"target"}` 使用目标文件；原版恢复使用
`leg={"kind":"rollback","from":{"attempt":"A","node_session":"A_SESSION"}}`，
其公共 attempt/session 为 B，from 固定 A。两种 leg 共用同一 Start/bootstrap/ready/Select 路径。
Selected 的结果由 Task.phase 区分：目标为 Completed，原版恢复为 Failed。
Select 已成功而终态写盘失败时保留 Committing，直到同一 Receipt 收口。

The new adxlet registers with its
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
Publisher 不直接调用 Yield、注册或 Select。Exit/Start 回复未知时先查同一 Task 和维护身份；
已证明完成的步骤不重复。Exiting 重放 Exit 的空 attempt/session 输入，保留已生成身份和固定期限，
期限后只做本机只读观察。仅有 exited 标记但仍观察到无法解释的活动进程时保持 pending；
null runtime 不能证明退出。

目标 Start 对 exited/starting 重放保存的 A；同 attempt 的 actor Start 不重复 spawn。
Committing 只继续同 A 的 Select/Receipt，期限后仅查 Receipt，不允许回退或启动竞争候选。
即使较晚收到已 Committed 回执，也按 Completed/selected 和当前实际 runtime 走成功路径。
目标的 900 秒期限不刷新；Starting 的失败事实经同 A 重放仍未解决，或目标窗口耗尽，
才可请求下面的一次原版恢复。

Publisher 在保存 `rollback_intent` 后始终调用
`step --task-id TASK --step rollback --attempt A --node-session A_SESSION`，
首次调用前核实同 owner 已 Yield 的维护 marker。Worker 先查 A 的 Receipt：
已 Committed 则成功收口，不停止它；未知回执不能证明未创建或已退出。
第一次 StopFailed 前持久化固定 600 秒期限及 `stopping_for_rollback`，
由 actor 封闭 A 并证明 NeverCreated 或真实 reap；迟到 Start(A) 不得复活。
只有上述证明成立且准备时固定的 original executable/configuration 重新验证通过，
才保存 B 和带 from=A 的 rollback leg，以 `exited` 进入同一启动路径。
若 B 写盘结果未知，重存同 B，不生成 C。

本机 worker 接收回退后直接启动/继续 B，外部 Start(B) 被拒绝；
脚本断连或 Coordinator 不可达时仍可本机启动 B 等待注册，维护不自动解除。
已有 B 时 journal 仍只保存 A，重放 Rollback(A)，从 Task 校验 leg.from=A 并取得 B；
活动 worker 不重复启动。600 秒覆盖 A stop/reap 与 B Start/bootstrap/ready/Select，
重试或查询不刷新期限。B 的 Committing 期限后仅查 Receipt；失败或超时保留维护，不递归回退。

Adxlet process journal 保持 schema 2，Relay/Pod journal 和 process plan 保持 schema 1，
`step_protocol` 保持 2。本轮首次整套升级直接采用新状态结构，不提供旧阶段映射或兼容路径。
首次升级前需盘点各节点 `state_dir/rollout/` 下的历史任务。确认任务已终态且不再需要重放后，
按明确目录清单归档并移出该目录；旧任务格式会使新 Submit 的任务扫描失败。
活动任务或结果未知时先用原版本收口，保留正在使用的二进制、配置和共享数据。
同 owner 只部署一个 publisher；非阻塞 journal flock 仅约束相同 journal/lock 路径。

原版恢复成功要求 Task 为 Failed/selected、rollback leg.from 精确指向冻结的 A，
并核对 B 的 candidate PID/session、原版 binary/configuration 摘要和未变化的独立 Relay session。
通过后 release 本轮维护，报告升级失败并停止后续批次。首次 Exit 前的取消只有在 Task 无 execution、
完整实际 runtime 等于冻结原版、Query 证明同 owner/current 原 session 尚未 Yield 时，
才可按失败 release。未知退出、进程丢失或 runtime 变化不符合该条件；release 保留管理员独立 pause。

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
checked first. With an exact `status:released` receipt, the publisher also
queries the original supervisor's Task for the matching terminal outcome and
saved identity. A later adxlet PID or subsequent maintenance owner does not
invalidate the historical receipt. If no receipt exists, the publisher also
obtains fresh complete runtime evidence with the frozen release session before
replay. A foreign marker returns 409; neither that conflict nor `status:absent` proves
release. The release body never changes. Unproven outcomes retain protection.
The former three `/v1/node-upgrades/process-maintenance` routes and publisher
NodeHandoff Query/ReplaceSession RPCs are removed. The owning Node continues
using authenticated Yield/QueryRelease for its local maintenance state.

Relay progress uses `execution.phase` (not adxlet's `adxlet_execution.stage`).
Relay completion checks target hash, candidate relay session and unchanged node
session in the same incarnation. Original-file recovery checks original hash and
the recovered candidate session. Retained-target loss and cancelled/pre-begin
failures are failures, never permission to advance. Relay requires no maintenance
release. 已证明的 adxlet 原版恢复在维护解除后仍记为本批失败；失败阻止后续批次。

Output is machine JSON with `status` equal to `completed`, `failed` or `pending`;
only a completely successful selected batch exits zero. Invalid plan/journal or
batch admission uses `PlanRefused` and exits nonzero. Pending preserves journal
intent and protection for the same command to resume.


## Pod Plan

`--config` is required and is an absolute container-internal deployment YAML path.
Planning reads existing NodeInfo through adxctl in embedded or standalone mode,
binding actual node/session, parent boot and running container to Coordinator
inspection.

运行时 `status.imageID` 是非空的运行身份，随容器身份和父进程启动身份记录并复核。
发布器将完整目标镜像引用与 PodSpec 精确比较；不将该引用与 `status.imageID` 比较，
也不推导 OCI index 与平台镜像之间的映射。

目标镜像支持显式版本 tag 和现有完整 `@sha256:` 引用。tag 必须为 1–128 个 ASCII 字符，
以字母、数字或下划线开头，后续只允许字母、数字、下划线、点和连字符；不接受任何大小写形式的
`latest`、省略 tag、空白或控制字符。registry 端口不能替代镜像末段的 tag，
例如 `registry.example:5000/adxlet:release-20261124-1` 有效，
`registry.example:5000/adxlet` 无效。完整 digest 使用 64 个小写十六进制字符。
使用 tag 发布时，每次发布使用唯一新 tag，禁止覆盖已有 tag；tag 字符串校验不证明镜像内容不可变。
任务、计划、journal 和替换 Pod 仍绑定同一个完整镜像引用，发布器不查询 registry 解析 digest。

`node_rollout.py plan-pod` uses only Python's standard library. It reads a native
DaemonSet or StatefulSet and its current controller-owned Pods through
`kubectl get`, and reads Coordinator management capabilities and each mapped
node's inspection through HTTP GET. `--daemonset NAME` and `--statefulset NAME`
are mutually exclusive. Both workload types record `workload.kind` explicitly;
replacement journals require their recorded `replacement_host`. Missing fields
are refused rather than inferred. Planning itself performs
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
  --target-image registry.example:5000/adxlet:release-20261124-1 \
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

规划时每个对象读取一次，并绑定 node session、rootfs 与实际 Pod UID 证据；
执行前重新核对当前对象和运行身份。这不是跨系统原子快照，也不保证之后不发生变化。
The publisher revalidates identity and uses current UID/RV CAS conditions;
the saved RV is not authorization to delete. HTTP connectivity and even
`strict_empty` in this output are not proof or permission to delete a Pod.
Migration, authoritative empty checks and delete permits remain exclusively in
the existing PR178 node-upgrade APIs. Rootfs evidence alone does not establish
cross-node recovery compatibility.


## Pod Apply

Pod task ID 必须是 1–128 个 ASCII 字符，仅允许字母、数字、`.`、`_`、`~`、`-`，且不能完整等于
`.` 或 `..`；epoch 必须是正 u64。规划与计划消费使用相同规则，保证 Coordinator 原始 URL 路径可精确寻址。
消费计划时还会核对 StatefulSet 规范序号名、源宿主机和镜像、原始与目标模板指纹。
首次 Begin 若明确返回 HTTP 400 `InvalidArgument`，journal 记录拒绝；stop/abort 只有在原 task 查询
明确返回 HTTP 404 `NotFound` 后才本地收口，且不重发无效 Begin。查询或传输结果未知时保留 pending；
未知首次结果之后的重放即使收到 400，也不能据此声明远端未执行。

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
UID absent permits replacement progression. If the original Pod is still present
at the same RV, the publisher replays the identical UID/RV DELETE. If the same
nonterminating original Pod has a different RV, the publisher journals and
revokes the old authorization, checks that the node is still strictly empty,
then obtains and confirms a new permit before issuing a DELETE for the new RV.
A terminating original waits for authoritative disappearance even when its RV changes.
替换 Pod 必须匹配原控制器 UID、完整目标镜像引用和 Downward UID，并使用新的认证会话。
其检查结果必须满足 protocol 3、unknown=false、isolated、task/epoch 匹配及四项库存计数为零，
随后才可为替换 Pod 执行 quiesce 并用新 permit 验证。DaemonSet 替换 Pod 必须留在原宿主机；
StatefulSet 替换 Pod 保持稳定名称，可以迁移至其他宿主机并记录该宿主机。
尚未记录替换宿主机的已有 DaemonSet journal 沿用原宿主机；StatefulSet 始终要求已记录的
替换宿主机，不会假定仍在原宿主机。
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

## Verification Boundary

Current local and cluster acceptance requirements are documented in
[process deployment testing](../../docs/testing/process-deployment.md).
Keep each run's exact source snapshot, commands, test counts and failures with its
results. Historical runs do not verify later source changes. Local unit/component
checks do not validate management-network isolation or a cluster upgrade.
