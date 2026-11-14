# DigitalMate 完整实例快照预热池可行性与改造设计

状态：设计建议，未实现。本文只依据指定源码基线分析，不代表现有系统已经支持所述流程。

- agent-dx：`research_prewarm`，`d3955c9e63810c7a90239e8c61df880b3d776049`。
- DigitalMate：`main`，`f02f5d8a715b416d3503f9a2c50868917cd09b7c`。
- 调查方式：`TRACE / working`。追踪首次业务请求、DigitalMate 启动、Snapshot 捕获与恢复、Volume 准入四条静态链路。
- 验证边界：本次没有启动真实 DigitalMate、sandboxd/runsc、SFS 或集群，没有测量恢复时延。文中 `SOURCE` 表示源码直接支持，`PROPOSED` 表示需要新增的实现，`UNKNOWN` 表示必须运行时验证的事项。

## 1. 结论

方案**有条件可行**，但不能把现有 Golden Snapshot 直接恢复成用户实例，再修改 `AgentBinding.parameters` 或环境变量完成领取。

推荐采用**每槽位一份完整实例快照**：

1. Activator 按固定 Template 版本创建池槽位，分配独立 Environment、`pool-<slot>` 工作区和池模型凭据。
2. DigitalMate 完整启动 Bridge、WorkSwarm、数据库和后台组件，达到业务可服务状态。
3. DigitalMate 进入捕获静默状态；Platform 捕获完整运行时，并保证源实例在捕获后不再写外部工作区。
4. 新用户首次请求到达时，池控制器原子领取槽位，将同一个槽位工作区和池凭据转交该用户，从该槽位快照恢复唯一目标 Environment。
5. DigitalMate 通过内部领取接口应用真实模型参数、清除池会话、刷新 WorkSwarm 配置，返回带代次的领取回执。
6. Activator 提交用户 AgentBinding，Gateway 验证目标 Environment 和领取代次后才转发业务流量。

首版只服务**没有既有 Binding、模型凭据和工作区分配的新用户**。老用户、已有工作区和 Binding 删除后的重建继续使用现有冷启动或暂停恢复路径。这样避免把匿名池状态覆盖到已有用户数据上。

方案成立依赖四项改造：

- DigitalMate 提供捕获静默、恢复后领取和状态回执协议。
- Agent 的 Environment capability 支持 Snapshot 捕获、按固定 Snapshot 恢复和读取恢复结果。
- Platform 提供“捕获后源实例退役或保持停止”的语义，防止外部 Volume 在捕获后继续变化。
- Agent Store 与账户库提供原子、幂等的槽位领取及 `(tenant, template, user)` 工作区归属记录。

如果首版不能完成捕获静默和源实例退役，建议先使用运行中完整实例池，不要使用“完整启动后快照再恢复”。

## 2. 为什么不直接复用现有 Golden Snapshot

### 2.1 当前首次请求链路

`SOURCE`：两个 DigitalMate WebSocket 入口都调用 `select_with_initializer`。只有 Binding 不存在时，Gateway 才读取模型参数并计算用户工作区：

- `gateway/src/ingress/digitalmategateway/application_ws.rs:151` 获取 `model_api_base/model_api_key` 和 `workspace`。
- `gateway/src/ingress/digitalmategateway/entrypoint.rs:368` 的旧入口有同样的初始化逻辑。
- `gateway/src/ingress/digitalmategateway/entrypoint.rs:80` 用 `(tenant, template, user_id)` 派生 `w-<hash>`。
- `gateway/src/ingress/agent_service.rs:123` 表明已有 Binding 不再执行初始化闭包。
- `agent/crates/core/src/lib.rs:389` 将参数保存在 AgentBinding；`agent/activator/src/creation.rs:189` 只用这些参数创建缺失的 Environment。

因此，恢复后仅修改 Binding 参数既不会重跑 DigitalMate，也不会改变已打开的工作区文件。

### 2.2 当前 DigitalMate 启动固化内容

`SOURCE`：

- `harness/personal-agent/deploy/entrypoint.py:21` 读取 `DIGITALMATE_WORKSPACE`，创建目录后执行 `engine_cli serve`。
- `engine_cli.py:28` 构造 `WorkSwarmAdapter`；`workswarm_adapter.py:63` 将模型环境复制到 adapter。
- `workswarm_adapter.py:111` 启动 WorkSwarm；`workswarm_adapter.py:134` 要求 `API_BASE/API_KEY/MODEL_NAME` 已解析，并在 `:148` 写入受管 `.env`。
- `server.py:420` 在构造 Bridge 时建立 host 身份、记忆和 EngineService；`engine_hosts.py:17` 将 `host-<uuid>` 持久化到工作区。
- `server.py:693` 启动后端和后台任务；`server.py:3929` 的 `/health/ready` 只检查 WorkSwarm 进程 ready/alive，不证明模型可用、池会话已刷新或用户参数已生效。
- `workswarm_launcher.py:118` 默认开启 `JIUWENSWARM_AGENT_PREWARM=true`；WorkSwarm 在 `agent_warm_pool.py:189` 以配置和环境指纹管理预热会话。

完整启动后的快照会包含 Python 对象、WorkSwarm 子进程、数据库连接、host 身份和后台任务。领取不是一次普通的配置写入。

### 2.3 当前 Snapshot 的边界

`SOURCE`：

- Platform 已有 `Regular/Golden` Snapshot、制品存储、引用和跨节点恢复元数据，见 `platform/crates/core/src/snapshots.rs:18`、`platform/crates/core/src/snapshot_preparation.rs:34`。
- `platform/adxlet/src/controller/snapshots.rs:204` 捕获运行中实例，`:211` 随后通知源实例恢复运行。
- `platform/coordinator/src/rpc/cloning.rs:39` 恢复时继承源进程和运行规格；`gateway/apiserver/src/contract.rs:671` 禁止 `snapshotId` 与 `inheritEntrypoint=true` 同时使用。
- `platform/coordinator/tests/volumes.rs:1005` 明确记录 clone 不自动继承 mounts，调用方必须显式传入相同 VolumeRef。
- `platform/crates/core/src/volumes.rs:11` 的固定规格比较包含执行配置；不能任意更换快照挂载。
- `docs/development/platform-volume-design.md:144` 说明 Snapshot 固定 Volume UID 引用，但不复制或冻结外部卷数据。
- Agent 的 `ExecutionSpec` 和 `CreateEnvironment` 当前没有 `snapshot_id`，`Environment` capability 也没有 Snapshot 方法，见 `agent/crates/core/src/environment.rs:11`、`:114`、`:176`。

现有 Platform 提供快照原语，Agent 层尚不能把它们用于 Template 激活。

### 2.4 必须处理的外部卷一致性

如果内存快照包含已打开的 SQLite 文件、WAL、内存缓存和文件描述符，而 `/data/state` 来自外部 Volume，则内存和外部目录必须来自同一个冻结点。

当前可复用 Snapshot 捕获后让源实例继续运行。若随后删除源实例，捕获后的数据库写入、预热会话写入或日志更新可能留在外部 Volume，却不存在于快照内存中。恢复后可能出现事务视角不一致、旧文件描述符指向变化后的文件、缓存与磁盘不一致。

**PROPOSED**：捕获前先让 DigitalMate 静默，捕获成功后源实例不得再次执行应用写入。只依赖“立即删除源实例”存在竞态，不能作为正确性保证。

## 3. 方案选择

| 方案 | 启动收益 | 工作区与身份 | 主要问题 | 结论 |
| --- | --- | --- | --- | --- |
| A. 单份匿名 Golden Snapshot，领取时换用户 Volume | 高 | 需要换挂载、重新生成身份和数据库状态 | 外部卷不在快照中；已打开文件和 SQLite 状态难以安全迁移 | 首版排除 |
| B. 每槽位完整实例快照，连槽位工作区一起领取 | 高 | 保持同一工作区、host 身份和文件视图 | 需要静默捕获、源退役、领取重配和池事务 | 推荐 |
| C. 每槽位运行中实例，不做 Snapshot | 最高 | 与 B 相同，少一层恢复一致性 | 持续占用 CPU/内存和实例配额 | B 不成立时的首版备选 |
| D. 只预热基础运行时，领取后启动 DigitalMate | 中 | 用户目录和身份在领取后创建 | Python、WorkSwarm 和初始化仍在关键路径 | 低风险演进项 |

B 的“每槽位一份”指库存中的每份快照只恢复一次，不能从同一已初始化快照克隆给多个用户。恢复目标取得该槽位全部状态的唯一所有权。

## 4. 推荐架构与状态

### 4.1 所有权

| 组件 | 负责 | 不负责 |
| --- | --- | --- |
| Agent Activator | Template 固定版本、池配置、槽位和 claim 状态机、创建快照库存、提交 Binding | Platform 内部健康恢复、沙箱状态副本 |
| Agent Store | 槽位、claim、库存 revision 的原子条件写 | 用户登录和 LiteLLM 归属 |
| Platform | Environment 创建、静默捕获协调、制品、恢复、Volume 独占、删除确认 | DigitalMate 参数语义、用户领取 |
| Gateway | 新用户判定、模型凭据供应、调用 claim/commit、领取后鉴权与转发 | 直接修改 Platform 生命周期、私有 Redis 状态 |
| DigitalMate | 捕获静默、恢复后重配、池会话失效、领取回执 | 槽位分配和 Binding 提交 |

这与 `agent/AGENTS.md` 当前“Agent 不保留实例池”的约束不同。实施时需要明确修改该约束：Pool controller 只管理槽位、库存与交付，不复制 Platform 生命周期控制器。

### 4.2 槽位状态

```text
Provisioning -> Quiescing -> Capturing -> ReadySnapshot -> Claiming -> Restoring
      |             |            |             |             |            |
      +-------------+------------+-------------+-------------+------------+
                                    v
                              Quarantined/Retiring

Restoring -> Binding -> Committed
Restoring/Binding 的未知结果 -> Quarantined（同一 claim 重试查询，不换身份重建）
```

槽位至少持久化：

- `pool_id/slot_id/revision`、固定 `(tenant, template, version)`。
- `source_environment_id/source_runtime_id`、`snapshot_id`、制品 digest 和捕获代次。
- `workspace`、Volume UID 集、池模型 owner/key digest；密钥原文使用账户库加密保存。
- `state/deadline/claim_id/target_environment_id/target_generation`。
- DigitalMate `prewarm_epoch`、`claim_token_digest`、`bind_receipt`。

`ReadySnapshot` 不计入 Running 库存。`Claiming` 后的 Environment 只能由同一 `claim_id` 继续查询或恢复，失败重试不得生成另一个目标 Environment ID。

### 4.3 用户归属

新增账户表 `workspace_assignments`，主键包含 `(tenant, template, user_id)`，保存 `workspace/slot_id/claimed_at`。

新用户资格需同时满足：

1. Agent Store 中没有对应 Binding。
2. 账户库中没有模型凭据。
3. 没有工作区分配。

三个条件需要在同一个领取准入流程中复核，防止并发冷建和池领取同时发生。用户首次领取后，即使删除并重建 Binding，也沿用分配记录中的原 `pool-<slot>` 工作区，不走匿名池。

## 5. 端到端流程

### 5.1 制备

1. Activator 读取固定 Template 版本，以 `workspace=pool-<slot>` 和池模型参数渲染 env/mounts/volumes。
2. Activator 先持久化池槽位、Environment ID、工作区及加密池凭据，再调用 Environment 创建接口。远端模型 Key 的失败清理沿用账户供应商规则。
3. DigitalMate 正常启动。池控制器除了 `/health/ready`，还调用内部 `prewarm/status`，确认 WorkSwarm ready、数据库迁移完成、无活动业务会话、预热 epoch 可读取。
4. Activator 通过 Environment capability 请求 `CaptureSnapshot`，携带固定 operation ID 和 `source_final_state=retired`。Platform/Execd 在现有 checkpoint prepare 阶段调用 DigitalMate `prewarm/prepare_capture`：阻止新业务请求，等待在途请求结束，禁止后台任务写工作区，关闭或 checkpoint SQLite 连接，冻结预热会话并返回 `prewarm_epoch`。
5. Execd 校验 prepare 回执中的 `prewarm_epoch`，再允许后端捕获；普通快照继续沿用现有 prepare/resume 语义。
6. Platform 捕获并发布 Snapshot，确认源执行停止且外部 Volume 不再由源写入，然后返回 Snapshot 元数据、源停止确认和已校验的 `prewarm_epoch`。
7. Activator 将槽位置为 `ReadySnapshot`，保存 Snapshot 返回的 `prewarm_epoch`。源 Volume 和 Snapshot 引用继续保护槽位资产，不删除底层目录。

### 5.2 领取

1. Gateway 为业务请求生成稳定 `claim_id`，建议使用 `(tenant, template, user_id, first-admission-generation)` 的哈希；同一请求及跨副本重试复用该 ID。
2. Activator 在一个 Agent Store 条件事务中检查用户仍无 Binding，选择匹配固定版本的 `ReadySnapshot`，将其转为 `Claiming`，记录 claim 和目标 Environment ID。
3. Gateway 将池模型 Key 的 owner 转为用户，保持 Key、额度和 spend 不变。多 worker 模型网关必须保证新 owner 立即可见；不能让其他 worker 继续命中池 owner 的 blocked 缓存。
4. Activator 确保原槽位 Volume UID 挂载到目标 `/data/state`，以该槽位 `snapshot_id` 创建目标 Environment。恢复使用新 Environment/runtime 身份，不修改捕获的进程镜像。
5. 恢复后、开放路由前，Activator 通过受信内部数据面调用 DigitalMate `prewarm/claim`，传入 `claim_id/prewarm_epoch`、模型参数、用户不可读标识和 lease 过期时间。
6. DigitalMate 原子保存领取进度，重写 WorkSwarm 模型配置，调用现有 `agent.reload_config` 能力，按配置指纹废弃池预热会话，再建立用户配置的预热会话。无法热重载的进程级设置需要重启 WorkSwarm supervisor。
7. DigitalMate 验证新模型配置、工作区身份和无池会话残留，返回 `{claim_id, prewarm_epoch, claim_generation, config_digest, host_id}`。响应不返回模型 Key。
8. Activator 提交 AgentBinding，参数中记录用户工作区和凭据版本，不重复保存明文 Key；Pool claim 标记 Committed。
9. Gateway 重新读取 Binding 和 Environment ready 状态，确认 `claim_generation` 与当前 route generation 匹配，再建立 WebSocket。

### 5.3 重试和失败

| 场景 | 处理 |
| --- | --- |
| 相同 `claim_id` 重试 | 返回原槽位、目标 Environment 和当前阶段，不重新领取 |
| 不同用户竞争同一槽位 | 只有一个条件事务成功，另一个选择其他槽位或冷启动 |
| Snapshot 创建响应丢失 | 按相同 Environment/operation ID 查询，不重新捕获 |
| 恢复结果未知 | 保留 Claiming，查询同一目标；不得直接冷建并复用独占 Volume |
| DigitalMate 领取失败或回执不匹配 | 隔离目标和槽位，禁止业务转发；恢复确认已回滚前不得重入池 |
| Binding 提交失败但 DigitalMate 已领取 | 以同一 `claim_id` 完成或补偿；禁止把含用户凭据的槽位退回库存 |
| 池为空或尚未开始领取 | 走原冷启动；冷建前再次检查用户归属，避免与迟到 claim 并发 |
| 领取后任何状态不明 | 不自动换用户工作区冷建；等待原 Environment 删除确认或隔离存储 |

DigitalMate 领取接口必须以 `claim_id` 幂等。第一阶段先持久化接收的领取记录，再执行外部重配；重复请求在参数 digest 相同时继续原阶段或返回原回执，不同 digest 返回冲突。

## 6. agent-dx 改造清单

### 6.1 Agent 核心模型和存储

| 文件 | `PROPOSED` 改造 |
| --- | --- |
| `agent/crates/core/src/lib.rs` | 保持 Template 版本不可变；不为池复制一套 Environment 状态机 |
| 新增 `agent/crates/core/src/pool.rs` | 定义 PoolConfig、PoolSlot、PoolClaim、ClaimReceipt、PoolStatus；校验固定版本、容量、deadline 和状态迁移 |
| `agent/crates/core/src/environment.rs` | `ExecutionSpec/CreateEnvironment` 增加可选 `snapshot_id`；增加 CaptureSnapshot、SnapshotInfo、源停止确认等窄类型和 capability 方法 |
| `agent/crates/store/src/lib.rs` | 注册 pool/slot/claim key 和索引，保持同一 Redis hash slot 内条件事务；不引入跨槽事务假设 |
| 新增 `agent/crates/store/src/pools.rs` | 实现 reserve、publish、atomic claim、commit、quarantine、retire；所有写带 revision 和 `claim_id` |

`snapshot_id` 进入 Agent `ExecutionSpec` 后，需要保持创建重试的规格完全一致。用户模型 Key 不应为了“刷新配置”写回恢复快照规格；运行中重配由 DigitalMate 领取协议完成。

### 6.2 Activator

| 文件 | `PROPOSED` 改造 |
| --- | --- |
| `agent/activator/src/environment_request.rs` | 普通创建保持 `inheritEntrypoint=true`；快照恢复发送 `snapshotId` 且不得发送该标志；保留固定 mounts 和 Volume UID |
| `agent/activator/src/transport.rs`、`local_environment.rs` | HTTP 与同进程 capability 同时实现捕获、查询和恢复，返回相同结构化错误 |
| `agent/activator/src/creation.rs` | 分离冷建与快照恢复分支；已存在 Environment 只观察，不因 Failed/NotFound 模糊结果换身份 |
| 新增 `agent/activator/src/pool.rs` | 有界 reconciler 负责制备、捕获、补池、过期和交付；Platform 仍负责实际健康与恢复 |
| `agent/activator/src/server.rs`、`lib.rs` | 增加内部 Pool API 和领取提交接口，复用 Activator 鉴权、deadline 和错误语义 |
| `agent/api/src/activator.rs`、`local.rs`、`managed.rs` | 暴露窄 Pool client；新增 `select_or_claim` 类编排时保留现有 `select_with_initializer` 冷路径 |

不要让 Gateway 直接访问 Agent Store 或构造 Platform Snapshot RPC。Gateway 只调用 Activator 的产品级领取接口。

### 6.3 Platform

| 文件 | `PROPOSED` 改造 |
| --- | --- |
| `platform/crates/core/src/snapshots.rs` | Snapshot 操作增加捕获代次和源终态请求/结果；保持制品与元数据幂等 |
| `platform/api/proto/snapshot.proto`、相关 control proto | 传输 `prewarm_epoch/source_final_state/source_stopped`；更新生成的协议代码 |
| `platform/adxlet/src/controller/snapshots.rs` | 新增捕获后不执行 `cooperation.resumed` 的模式，完成源执行停止和引用收敛后才对外成功 |
| `platform/runtime/execd/src/runtime/control.rs`、`mod.rs` | 扩展现有 `CheckpointHooks`，在池捕获 prepare 阶段调用 DigitalMate 静默接口并校验 epoch；保留 envd freeze，退役模式不执行应用 resume |
| `platform/coordinator/src/rpc/snapshots.rs`、`cloning.rs` | 校验只有已停止源的可领取快照能进入池恢复；目标显式复用相同 Volume UID，不允许改挂载目标 |
| `gateway/apiserver/src/contract.rs`、`http.rs`、`operations.rs` | API Server 只做类型、校验和 RPC 转发，不加入池业务逻辑 |

源退役模式不能简单删除 Environment 元数据后再发布 Snapshot。需要保留制品归属、Volume UID 引用和停止证明，避免清理任务把恢复所需资产回收。

### 6.4 Gateway 和账户

| 文件 | `PROPOSED` 改造 |
| --- | --- |
| `gateway/src/ingress/digitalmategateway/application_ws.rs` | `/digitalmate/ws` 与 `/orbit/ws` 分别配置是否可领取，不能按路径隐式共用 DigitalMate 池 |
| `gateway/src/ingress/digitalmategateway/entrypoint.rs` | 旧 Jiuwen 入口接入同一领取服务；上传、下载在 Binding 缺失时也需先完成相同准入 |
| 新增 `gateway/src/ingress/digitalmategateway/pool.rs` | 新用户判定、模型供应、claim/commit 编排、回执校验和冷启动回退 |
| `gateway/src/ingress/accounts/mod.rs`、`providers.rs` | 增加池 Key 的持久化供应和 owner 转移；不在预热期发起真实模型请求 |
| `gateway/src/ingress/accounts/schema.sql`、`store.rs` | 增加 tenant 完整的 workspace assignment 和幂等 claim 记录；保留现有账户表所有权 |

模型 Key 的建议顺序：预热时池 owner 持有 blocked Key；领取时转移 owner/alias 并 unblock，不重置 spend。需要先验证 LiteLLM 多 worker 缓存失效；预热期不要远程物化 blocked Key，避免其他 worker 缓存旧状态。

## 7. DigitalMate 改造清单

### 7.1 新增受控生命周期模块

建议在 `harness/personal-agent/services/agent_bridge/src/server_engine/` 新增一个职责单一的 `prewarm_lifecycle.py`，由 Bridge 持有，不让 agent-dx 直接调用 WorkSwarm 私有对象。

定义版本化内部协议：

- `prewarm/status`：返回协议版本、`prewarm_epoch`、host ID、数据库/WorkSwarm/后台任务状态。
- `prewarm/prepare_capture`：进入静默，冻结写入，返回可捕获回执。
- `prewarm/claim`：应用领取参数，执行 WorkSwarm reload 或受控重启，返回配置 digest 和领取代次。
- `prewarm/abort`：只用于尚未接收用户参数的取消；已领取实例由 agent-dx 退役，不返回池。

这些端点只能监听 loopback，并通过 Execd/受信内部数据面访问。业务 `/ws` 不得暴露这些方法。认证 token 由 Platform 在每次执行代次注入，Snapshot 中不得包含可跨 Environment 重放的长期管理 token。

### 7.2 捕获静默

| 现有文件 | `PROPOSED` 改造 |
| --- | --- |
| `server.py` | Bridge dispatch 在静默后拒绝新业务；等待 `_digitalmate_requests`、活动 turn/job 和配置写完成；暂停后台 schedule、mirror、inbox 和事件投递 |
| `engine_service.py`、memory/store 实现 | 提供明确的 flush/checkpoint/close 边界；恢复后如需重开，必须按同一工作区和相同代次执行 |
| `workswarm_adapter.py` | 增加 `prepare_capture/claim`；静默期停止 session 创建、卸载和预热写入，记录 adapter 与 supervisor 状态 |
| `workswarm_launcher.py` | 池模式下延迟用户配置，不把匿名占位参数当成用户配置；提供受支持的 reload 入口 |
| `engine_hosts.py` | host 身份作为槽位资产保持不变；claim 校验身份而非重新生成 |

“完整启动”需要在协议中具体化：至少包括 Bridge、WorkSwarm、数据库迁移和必要的预热会话。是否发起真实模型请求应可配置；默认不发起，避免池凭据计费和外部副作用。

### 7.3 恢复后领取

复用现有 WorkSwarm 热重载，不要默认重启整个 DigitalMate：

- `harness/workswarm/jiuwenswarm/common/schema/message.py` 已有 `AGENT_RELOAD_CONFIG`。
- `harness/workswarm/jiuwenswarm/gateway/app_gateway.py:2002` 附近已有配置保存后的 reload 编排。
- `harness/workswarm/jiuwenswarm/server/runtime/agent_manager.py:2472` 提供 Agent 配置重载，`:2606` 刷新 warm pool。
- `agent_warm_pool.py:330` 按 fingerprint 淘汰旧配置槽位。

DigitalMate 需要把这些已有能力包装成一个**同步完成、可审计的领取操作**。仅写 `.env` 或调用现有 `config.save` 不够：

1. `EngineConfiguration` 的平台模型字段由进程环境覆盖，普通 `config.save` 又排除平台字段，见 `engine_config.py:147`、`:795`、`:838`。
2. Bridge、adapter 和 WorkSwarm 各自持有配置副本。
3. `/health/ready` 不检查新配置 digest，不能作为领取完成信号。
4. 已创建的池会话含有旧模型和 host 上下文，必须失效，不能转给用户。

若热重载不能覆盖 API base/key、MCP 环境或某些子进程，则只重启 WorkSwarm supervisor，Bridge 保持运行并重新建立 adapter。首版允许这一兜底，但必须测量其对首条消息时延的影响。

### 7.4 状态和安全

- 池阶段禁止任何真实用户 Session、上传、下载、连接器登录和定时业务动作。
- 快照不得包含真实用户数据、业务 Bearer Token、AGC/Huawei 登录态或用户连接器凭据。
- 池模型 Key 只存在于受管内存和池工作区的受管 `.env`，文件权限保持 `0600`；日志和回执必须脱敏。
- 领取后清除池预热 Session、内存任务、问题/图片缓存和池生成 ID。只保留明确标记为可共享的运行时缓存。
- 用户提交前再次验证 `claim_generation`，防止旧快照、旧 adapter 或迟到响应接入新 Binding。

## 8. Template 与配置建议

Template 仍表达镜像、资源、probe、mounts 和 volumes。池策略放在 Activator 的 PoolConfig 中，不塞进每个用户 Binding，也不让 DigitalMate 镜像理解 Redis。

建议首版字段：

```json
{
  "pool_id": "digitalmate-new-users",
  "tenant": "jiuwen",
  "template": "digitalmate",
  "version": "18",
  "enabled": false,
  "min_ready_snapshots": 2,
  "max_slots": 4,
  "max_provisioning": 1,
  "snapshot_max_age_seconds": 86400,
  "claim_timeout_seconds": 120,
  "capture_timeout_seconds": 300,
  "restore_storage": "shared"
}
```

这是 `PROPOSED` 配置示例，不是当前可执行配置。`version` 首版必须是具体版本，Alias 解析后再建独立池，不能让不同槽位漂移。`restore_storage=shared` 只表示希望跨节点读取制品，不证明目标集群已经配置或验证共享恢复。

## 9. 验证计划与准入门槛

### 9.1 先做的最小运行时实验

1. **runsc 外部卷一致性**：启动带 SQLite/WAL 的 DigitalMate，静默捕获并退役源实例；恢复原 Volume 后执行 `PRAGMA integrity_check`、WAL 恢复和写入，检查无丢失或重复事务。
2. **捕获后无写入**：在源实例设置捕获后写入探针，证明 `source_final_state=retired` 返回后没有源进程继续修改工作区。
3. **同 Volume 恢复**：验证新 Environment ID 显式复用相同 Volume UID、相同 `/data/state` 目标，独占准入在源停止后才放行。
4. **配置领取**：恢复后用不同 API base/key 调 `prewarm/claim`，验证新 Session 只使用新 Key，旧预热 Session 不可访问，池 Key 无残留请求。
5. **身份与网络**：确认新 Environment/runtime 身份、路由 token 和 Execd 管理 token 已更新，DigitalMate host ID 保持槽位值且只归一个用户。

任何一项失败，都不进入自动补池上线。

### 9.2 功能与故障测试

- 同一 `claim_id` 并发/跨 Activator 副本重试只产生一个目标 Environment。
- 不同用户争抢最后一个槽位不会发生双重交付。
- 在捕获、制品发布、源停止、恢复、DigitalMate 重配、Key 转移、Binding commit 各阶段注入断线和进程重启。
- 快照过期、镜像 digest 变化、Template 版本变化、共享制品缺失和节点不可用时不交付旧库存。
- 池为空回退冷建与迟到 claim 并发时，不会同时占用两个工作区。
- Binding 删除重建沿用已分配工作区，不会再次领取匿名池。
- 两个 WebSocket 入口及文件上传/下载使用同一用户归属规则。

### 9.3 性能与容量

分别记录以下阶段，不能只报 Environment API 总时长：

- 冷启动：调度、rootfs、DigitalMate/WorkSwarm 启动、readiness、首条消息。
- 快照领取：claim、Key 转移、制品缓存/下载、runsc restore、DigitalMate 重配、首条消息。
- 后台成本：捕获时长、制品大小、内存和磁盘占用、对象存储流量、缓存淘汰。

使用相同镜像、模型、工作区后端和节点规格比较 P50/P95/P99。现有 Firecracker 测试数字不能当作 runsc + DigitalMate 的收益承诺。当前没有本方案的真实恢复时延数据。

## 10. 分阶段实施建议

| 阶段 | 交付 | 退出条件 |
| --- | --- | --- |
| P0 可行性 spike | DigitalMate 静默/领取原型、Platform 源退役捕获、单槽位恢复 | 通过 9.1 五项实验 |
| P1 单用户链路 | Agent Snapshot capability、单槽位 Pool、固定版本、手工库存、一个 WS 入口 | 单副本功能与故障注入通过 |
| P2 产品化 | 原子库存、账户归属、两个 WS 入口、冷回退、隔离清理、指标 | 多副本和真实模型/SFS 验收通过 |
| P3 性能优化 | 跨节点共享制品、缓存策略、热重载覆盖、自动容量控制 | P95/P99 和资源成本达到实测目标 |

P0 不应先写完整自动补池。最大的未知是外部工作区与完整进程快照的一致性，先验证这一点能避免在池事务上投入后返工。

## 11. 证据索引

| 主题 | 证据 | 状态 |
| --- | --- | --- |
| 用户初始化只在 Binding 缺失时执行 | `gateway/src/ingress/agent_service.rs:123` | SOURCE |
| 模型参数持久化并用于创建 | `agent/activator/src/creation.rs:189` | SOURCE |
| DigitalMate 启动 WorkSwarm 并写模型配置 | `workswarm_adapter.py:111`、`:134` | SOURCE |
| host 身份持久化 | `engine_hosts.py:17` | SOURCE |
| readiness 不等于用户参数生效 | `server.py:3929` | SOURCE |
| 可复用 Snapshot 捕获后恢复源运行 | `platform/adxlet/src/controller/snapshots.rs:204` | SOURCE |
| Execd 已有 checkpoint prepare/resume hooks | `platform/runtime/execd/src/runtime/control.rs:22`、`platform/adxlet/src/controller/snapshots.rs:164` | SOURCE |
| clone 不自动继承 mounts | `platform/coordinator/tests/volumes.rs:1005` | SOURCE |
| Snapshot 不冻结外部 Volume 数据 | `docs/development/platform-volume-design.md:144` | SOURCE |
| Agent 尚无 Snapshot capability | `agent/crates/core/src/environment.rs:176` | SOURCE |
| WorkSwarm 支持配置 reload 与 warm pool 刷新 | `agent_manager.py:2472`、`:2606` | SOURCE |
| runsc + DigitalMate + SQLite/SFS 快照恢复正确性 | 未执行 9.1 实验 | UNKNOWN |
| 恢复和首条消息的 P95/P99 收益 | 未执行对照测量 | UNKNOWN |

本文引用的 DigitalMate 相对路径均以 `C:/Users/Zz/Code/agent-runtime/DigitalMate` 为根；agent-dx 相对路径以本仓库为根。
