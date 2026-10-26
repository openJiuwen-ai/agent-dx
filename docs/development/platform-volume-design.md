# Platform Volume 能力与迁移设计草案

状态：P1–P6 已完成本地实现及阶段检查，包含公共 HTTP、SDK、Activator ensure／关联／补偿清理、请求内重试和 Snapshot 的固定 UID 引用保护。P7 已完成单宿主机三逻辑节点的真实 runc 端到端验证，包含动态共享存储域节点接入。暂不部署到集群或切换业务模板；跨主机 SFS、网络分区及 Snapshot／checkpoint 真实运行时验收仍待后续，不能将本地证据当成已部署能力。

Platform 增加独立 Volume 定义，与现有内联挂载并存。Volume 保存稳定存储身份、访问模式和初始化规则，默认 shared；显式 exclusive 的卷由 Coordinator 管理具体执行的占用，adxlet 在准入后解析和挂载。需要转为 VolumeRef 的存量目录原地登记，不搬迁数据，不移除现有 image、S3、host_path 等内联挂载入口。

本文按 2026-10-23 讨论确定的边界制定计划：调度参考 Kubernetes 的条件检查、暂不可调度等待和事件重试，不保证严格 FIFO；节点心跳超时沿用现有执行失效流程，在同一事务中释放旧执行的卷占用，接受故障接管期间旧执行可能继续访问。未分配创建请求暂不增加持久化等待队列，由 Activator 在原请求期限内幂等重试。

Agent 的别名选择、主动预初始化和版本升级由独立方案管理，不属于本次 Volume 实现。不使用独享 Volume 的 Environment 仍可并行启动，Platform 不解释模板版本或业务用户。

## 1 范围与设计决定

| 项目 | 决定 |
| --- | --- |
| 挂载入口 | 保留现有内联来源，新增 VolumeRef；同一挂载二选一 |
| 首版存储 | 已有共享文件系统中的受管理目录；节点本地目录必须具有明确节点拓扑 |
| 身份 | 租户内稳定 Volume ID，加不可复用的 Volume UID |
| 访问模式 | 默认 shared，允许并行；exclusive 在控制面至多有一个有效执行占用 |
| 互斥边界 | 以 Volume UID 为边界；业务保证实际目录使用一致，平台不封禁指向同目录的内联 host_path |
| 执行入口 | exclusive 的首次启动、同节点恢复、跨节点恢复、内部重新启动统一准入 |
| 占用权威 | exclusive 占用只由 Coordinator 原子提交；本地创建也必须经 Coordinator RPC，adxlet 不新增 Redis 依赖 |
| 挂起 | 正常生命周期由 adxlet 完成清理后报告释放，保留引用、数据和 Environment |
| 节点失联 | 复用现有心跳超时判定；旧执行分配失效与对应卷占用释放原子提交 |
| 故障一致性 | 允许失联旧执行与新执行并存访问；控制面占用失效不等于旧进程停止 |
| 等待与公平性 | 无严格 FIFO、无指定下一持有者；条件变化后重新调度，不阻塞其他可运行请求 |
| 创建重试 | 未分配等待保持内存态；Activator 在原请求总期限内查询原身份并幂等重试 |
| Agent 卷生命周期 | 首次激活 ensure 并关联 Binding；最后一个 Binding 删除后待 Environment、Snapshot 引用和占用结束才删除卷定义，无独立空闲过期时间 |
| 数据删除 | 删除 Environment 或 Volume 定义均不自动删除目录数据 |
| 接入范围 | 增量新增接口；仅为选择 VolumeRef 的调用方登记目录、适配并受控重建 |
| 首版限制 | 一个 Environment 至多引用一个不同的 exclusive Volume；同卷多个挂载目标合并占用 |

不引入存储动态供应、扩容、用户卷数据快照、目录自动搬迁、运行中内联挂载自动接管或独立持久化创建等待队列。节点失联释放不依赖人工确认、云平台关机或存储隔离，不另设卷释放超时。Rootfs、RuntimeProfile bootstrap 和 checkpoint 存储不因本次变更自动改为用户 Volume。

Kubernetes 的 ReadWriteOnce 限制节点，仍允许同节点多个 Pod 使用；ReadWriteOncePod 限制集群内单 Pod 使用，且要求 CSI 支持。本方案借鉴 RWOP 的调度检查与节点挂载检查，不照搬 PV/PVC 或 CSI 体系，也不宣称共享目录具有存储侧隔离能力。参见[访问模式](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes)和 [RWOP 设计](https://www.kubernetes.dev/resources/keps/2485/#design-details)。

## 2 当前实现与改动位置

当前 [Mount](../../platform/crates/core/src/execution.rs) 同时保存内联或 VolumeRef 来源、目标、选项和内联目录创建属性；[公共请求转换](../../gateway/apiserver/src/contract.rs) 将 host_path 等内联来源转成平台类型；[Agent 模板](../../agent/crates/core/src/mounts.rs) 渲染挂载路径；[节点目录准备](../../platform/adxlet/src/managed_mounts.rs) 在启动和恢复时处理显式目录创建。

现有 [Coordinator 恢复](../../platform/coordinator/src/lib.rs) 只恢复持久化分配，未分配创建等待为内存态；现有持久化 resume 意图继续保留，不与新建等待混同。[Activator](../../agent/activator/src/lib.rs) 已先保存稳定 Binding/Environment 身份，查询不到 Environment 时使用原 ID 创建，在请求总期限内，已按原身份查询并对可重试的断连或未知结果退避重试；不新增后台激活恢复。

现有 [expire_nodes / invalidate_node](../../platform/coordinator/src/rpc.rs) 根据心跳超时失效节点；[持久化失效处理](../../platform/coordinator/src/storage/failure.rs) 已将节点不可用、旧执行结果失效、资源不再持有和准确匹配的 exclusive 卷占用释放原子提交；初始化持有者失联会保留待修复状态。[跨节点恢复](../../platform/coordinator/src/storage/recovery.rs) 仍要求有效且可跨节点读取的恢复点，本方案不新增无恢复点时的冷启动回退。

| 模块 | 目标改动 |
| --- | --- |
| platform/crates/core | Volume 定义、引用、执行占用、解析绑定及校验 |
| platform/api 与 platform/crates/protocol | Volume 操作、引用式挂载、占用凭证和结构化等待原因 |
| gateway/apiserver | HTTP 类型、校验、直接 Platform RPC 调用；不持有 Volume 调度逻辑 |
| platform/coordinator | 资源登记、固定引用、所有占用变更与分配事务、内存等待唤醒、恢复准入及节点失效释放；独占卷元数据的 Redis 访问 |
| platform/adxlet | 通过 Coordinator RPC 申请授权、报告阶段及释放；校验凭证、解析目录、初始化、挂载及失效执行清理；不新增 Redis 客户端、连接配置或凭据 |
| agent | 通过平台能力接口登记或引用 Volume，模板和 Binding 保存稳定引用；Activator 请求内重试 |
| 现有 Environment SDK | 更新 Volume 操作、挂载字段、状态和错误类型，不新增 SDK 产品 |

现有 image、S3、host_path 等内联来源保留原有契约和运行时支持范围，不因新增 Volume 而统一包装成 VolumeSource。用户挂载同时支持内联来源与 VolumeRef，但同一个挂载不能混用。现有 Rootfs 的 StorageSource 保持自身语义。

## 3 Volume 定义

Volume 以鉴权后的 tenant 和 id 定位；uid 由 Platform 生成，身份不包含模板版本、Binding generation 或 EnvID。以下省略服务端生成字段：

```yaml
id: workspace-user-42
source:
  type: directory
  storage_domain: sfs-main
  path: workspaces/tenant-a/user-42
access_mode: exclusive
create_if_missing: false
```

省略 access_mode 时固定解析为 shared；上例工作区因业务需要显式声明 exclusive。shared 同样具有稳定身份、引用、授权和初始化管理，但不限制并行执行，也不进入 exclusive 占用等待。

新建目录的显式初始化规则：

```yaml
create_if_missing: true
create_options:
  uid: 10001
  gid: 10001
  mode: "0700"
```

storage_domain 是受信部署配置中的真实存储域，包含后端身份、租户路径规则和节点接入策略。path 是域内相对路径。默认 dynamic_nodes=false，沿用 node_roots 中明确配置的固定节点与本地根目录，节点本地数据使用该策略。共享存储域可显式设置 dynamic_nodes=true，此时 node_roots 不参与候选计算，允许尚无节点上报时创建卷定义并等待可用节点。

动态域的根目录来自 adxlet 的 storage_mounts 配置。adxlet 使用执行前同一套 /proc/self/mountinfo 与目录检查，按现有报告周期仅上报当前核验通过的域、backend_id 和 root；缺少挂载或身份不符的域不报告，观察失败或超时时撤回该轮完整挂载视图，不关闭无卷任务的节点准入。Coordinator 通过已有 mTLS 节点身份、session 和单调心跳接收报告，只有完成节点对账的当前会话且后端身份匹配受信域时才采用根目录。报告变化时重新计算已缓存 VolumePlacement 中所有域的节点交集并唤醒等待请求，保留原队列期限和 exclusive 占用等待；带卷请求可复用现有候选筛选和队列转移，在另一分片的新节点出现后继续调度。执行授权使用同一份当前映射，节点在初始化和启动前继续核验实际挂载；上报后挂载失效仍可导致本次执行失败，不承诺持续存储健康检测。

挂载报告仅是当前节点能力，不另建持久化拓扑目录，不增加 adxlet 对 Redis 的依赖。节点失效后的已有调度门槛继续阻止新分配；旧 session／旧序号不能恢复已撤回的挂载视图。Coordinator 重启后动态能力由现有节点注册重新提供。共享存储的不同节点根路径可以不同，但部署方必须将其指向同一后端的同一逻辑根；当前挂载源核验不证明目录内容一致，同名本地路径不能作为共享存储证明。

资源登记维护规范化的后端与目录身份索引。同一真实目录不能登记为多个有效 Volume；首版拒绝父子目录重叠，拒绝符号链接、越界和通过不同存储域配置重复登记同一来源。租户目录范围在服务端授权，不能仅因调用方知道 Volume ID 或目录字符串就允许访问。

Volume ID 长度为 1–256 字节，只允许 ASCII 字母、数字、点、下划线和短横线；单独的 `.`、`..` 会被 URL 客户端折叠，因此拒绝。Platform 定义、挂载引用、Agent 渲染结果与 SDK 查询／删除遵循相同规则；目录路径和后端标识沿用各自的校验规则。

来源、访问模式和初始化规则创建后不可变。创建采用 ensure 语义，以 (tenant, Volume ID) 为唯一键：已存在时直接返回已有 Volume 和 UID，不比较请求定义、不覆盖已有定义；不存在时由 Coordinator 原子创建，并发创建的失败方重新读取并返回获胜者。Activator 以返回的已有定义和 UID 为准，模板后来更改路径、访问模式或初始化规则不会修改已存在的卷。首版不支持原地修改来源或 shared/exclusive 转换。节点放置依据 storage_domain 的可访问拓扑，不能把节点本地目录迁移到另一节点后视作原数据。

### 3.1 Activator 自动确保 Volume 存在

Agent 模板声明所需卷的来源、访问模式、初始化规则、稳定 Volume ID 渲染规则和挂载关系。Activator 使用不可变模板与已保存的 Binding 参数渲染这些内容，先通过 Environment 能力接口调用 Platform 确保各卷存在，取得并固定返回的 UID，再以 VolumeRef 创建沙箱。重试和多副本激活复用同一 tenant、Volume ID、Binding generation 和 Environment ID，不随机生成新 Volume ID，也不把模板版本或沙箱身份加入存储身份。

ensure 只保存 Volume 元数据，不创建目录、不移动文件或修改存量权限；create_if_missing 的实际目录准备发生在后续节点执行准入后。Activator 不直接访问 Coordinator Redis，也不保存另一套卷占用锁。业务无需在首次激活前手工创建 Volume。

### 3.2 AgentBinding 管理的生命周期

首次流量触发激活时，Activator ensure Volume 并建立持久化的 Binding 关联；同租户同 ID 的已有卷同样增加该 Binding 的关联，重复调用不重复计数。多个 Binding 可以关联同一个卷，关联身份须区分 Binding 的删除重建，不能使用 Environment 的执行代次作为卷生命周期。

Volume 不设独立空闲过期时间。后续激活、Environment 挂起、重建和节点切换均复用原 Volume。创建超时且明确撤销时清理本次 Environment 的引用，不因此解除 Binding 关联或删除卷；结果未知时先核对和收敛，不以超时推断执行从未发生。

删除 Binding 先收敛其 Environment、未分配创建和引用，再解除 Binding 与各 Volume 的关联。最后一个 Binding 解除关联后，删除无引用的 Volume 定义；若仍有 Snapshot、Environment 引用或占用，记录待清理状态并保留定义，待最后一份引用及占用释放时自动删除。失败保留现有 Binding 删除意图，重试继续完成，不提前宣告删除完成。此流程不恢复后台创建等待队列，也不新增独立的卷空闲回收周期。

激活与 Binding 删除并发时，由 Activator 在平台操作后的权威 Binding 检查中处理：若旧 Binding 已删除、进入删除中或已换代，主动删除旧 Binding 固定身份对应的 Environment 并解除其卷关联，复用正常清理逻辑，不影响新 Binding。Platform 不维护 Binding 删除标记，也不解释其产品状态。此补偿依赖在途激活继续执行到后置检查；Activator 在平台副作用后、检查前崩溃的孤儿清理不因此获得后台恢复保证。

删除定义始终保留底层目录和数据。后续同名重建产生新 UID；若仍配置原目录，则保留并使用已有数据。Binding 生命周期关联和级联清理已接入 P6。独立创建且从未关联 Binding 的卷仍需显式删除。

## 4 内联挂载、VolumeRef 与节点挂载

VolumeRef 挂载声明引用、目标和选项；同一 Environment 可以同时包含内联挂载和 VolumeRef。下例为目标接口示意：

```yaml
mounts:
  - volume_id: workspace-user-42
    target: /home/jiuwen
    options: [rw]
  - type: bind
    image_url: registry.example/models:v2
    target: /models
    options: [ro]
```

Environment 创建受理时，平台解析 Volume ID，固定 Volume UID 和定义版本，并保存引用关系。重试先匹配原始 Environment 请求，复用已经固定的引用；不能重新绑定同名但不同 UID 的 Volume。固定引用须独立于尚未取得的执行分配保存；该最小引用记录用于幂等和删除保护，不作为重启后自动重新入队的创建意图。未分配请求被明确取消或过期、且确认无在途执行副作用后，方可清理其引用；不能因连接断开就清理。解除引用保护不允许同一 Environment ID 静默重新绑定新 UID，幂等凭据或调用方固定引用必须继续约束后续重试。

原始 EnvironmentSpec 与节点解析结果分开保存。节点在通过执行准入、exclusive 取得占用后，将 VolumeRef 解析为当前后端可执行的具体挂载参数，复用现有运行时适配。不能把解析得到的绝对路径原地写回原始请求，否则会破坏规格幂等比较。内联挂载继续沿用现有执行链路。

类型改动位于用户 Mount 层，不把 VolumeRef 加入 Rootfs 共用的通用 StorageSource。挂载目标、只读或读写选项属于使用方；存储来源、目录初始化和访问模式属于 Volume。exclusive 即使被只读挂载，也占用唯一执行名额。

不新增“内联 host_path 不得指向受管目录”的限制，不按 host_path 自动推导 Volume 身份或取得占用。业务负责同一实际目录的引用与访问方式一致。Volume 登记的重复来源检查只约束 Volume 定义之间的冲突，不意味着平台可以拦截所有内联或外部访问。

## 5 引用和执行占用

| 关系 | 建立时机 | 释放时机 |
| --- | --- | --- |
| Binding 关联 Volume | 首次激活 ensure 时；已有卷也建立关联 | Binding 删除并完成其 Environment／待创建请求清理后 |
| Environment 引用 Volume | 创建受理并固定 UID 时 | Environment 删除，或未分配创建明确撤销并收敛副作用后 |
| Snapshot 引用 Volume | 发布 Snapshot 时固定源 Environment 所引用的卷 UID，与 Snapshot 原子持久化 | Snapshot 进入 Deleted，即其存储产物删除完成时；Deleting 仍保护卷定义 |
| exclusive 执行占用 | Coordinator 原子提交占用与执行分配时，下发节点之前 | 正常释放提交，或旧节点执行分配失效的同一事务 |

挂起释放占用但不释放引用。仍被挂起 Environment 引用的 Volume 不得删除。删除 Volume 须同时满足无 Binding 关联、无 Environment 或 Snapshot 引用、无占用、无等待者、无未知的在途执行；默认仅删除定义，保留底层数据。Snapshot 的引用本身不取得 exclusive 占用，也不复制或冻结外部卷数据。克隆保持现有行为，不继承 Snapshot 的 mounts；显式声明的卷挂载仍走普通创建准入。若以后同名重建则生成新 UID，旧引用不得自动指向它。

这里的未知在途执行指仍有有效授权、尚未收敛的控制面操作。节点失效事务已撤销的旧占用不再阻止重新准入；不因无法确认失联旧进程停止而重新增加卷等待。Environment 引用仍独立保留，直到正常删除或明确撤销其创建。

exclusive 占用记录包括 Volume UID、Environment ID、具体执行尝试身份、assignment generation、节点会话和阶段。优先复用现有 runtime ID／执行尝试身份组合，不默认新增独立“卷代次”计数器；必须区分同一 assignment 下的恢复、reload 和自动重启。重放同一执行尝试复用占用；同 EnvID 的另一尝试不自动获得使用权。shared Volume 可同时供多个执行访问，仍维护引用、初始化和存储授权，不设置排他占用。

```text
Free → Reserved → Held → Free
```

Reserved 已表示某次执行被授权，Held 表示节点已经接纳该尝试；从预留到节点准备、挂载、初始化、运行、清理结束前均排他，清理期间仍保持占用。单次 RPC、创建期限或初始化期限超时不证明执行未发生，不能直接释放。正常释放与节点失效释放的条件见第 7 节。

当前实现保持 Held 到清理证明与最终结果提交，随后直接释放。节点失效事务可以将匹配旧占用直接释放为 Free，不要求失联节点先报告阶段变化。

所有释放必须匹配当前完整占用身份，并与当前记录做条件更新。所谓使旧占用“代次失效”，是旧身份不能再操作新占用，不要求额外创建计数器，也不能杀死已经运行的旧进程。

exclusive 占用的创建、阶段变更、执行身份替换、撤销和释放都由 Coordinator 条件更新权威存储。adxlet 的本地记录和挂载检查只用于执行去重、校验及回报重试，不形成独立卷锁，也不直接读写 Redis。目录初始化结果等需要持久化的节点回报同样经过 Coordinator RPC，不因此增加节点侧 Redis 依赖。

沿用已有的、经过节点核验的进程替换流程时，节点会话、保留分配的占用和未释放执行授权在同一事务中转到新会话；旧会话回报不能释放它们。尚未完成的目录初始化转为待修复，不能因进程替换当作初始化成功。节点检查使用最新已授权执行视图，即使该执行尚未发布首条状态，也不能用旧执行结果将其误判为孤儿。

### 5.1 占有与释放时间点

| 时间点／场景 | exclusive 卷处理 |
| --- | --- |
| 创建 Volume、固定 Environment 引用 | 不占用 |
| 等待卷、计算资源或可访问节点 | 不占用，不长期保留计算资源 |
| 卷与完整放置条件满足，提交执行分配 | 同一事务开始占用，成功后才下发节点 |
| 准备目录、挂载、启动、就绪检查、运行或 checkpoint | 持续占用 |
| 挂起、删除、取消或失败清理中 | 持续占用，不能以释放计算容量代替释放卷 |
| 正常生命周期清理完成且不会再回滚启动 | adxlet 报告释放，Coordinator 校验并提交 |
| 已预留但确认从未下发，且下发路径已封闭 | Coordinator 可撤销该次预留 |
| 传输超时、清理结果未知，节点尚未失效 | 保留占用，继续按原尝试查询或处理 |
| 心跳超时触发现有节点执行失效提交 | 同一事务释放旧节点会话对应占用，无需 adxlet 确认 |
| 占用已释放，后续再次执行 | 重新准入并取得新的有效占用身份 |

### 5.2 本地创建与中心调度共用 Coordinator 原子准入

本地创建只表示优先选择入口节点执行，不表示节点可以自行取得 exclusive 卷。扩展现有 [本地创建](../../platform/adxlet/src/rpc/local_create.rs) 到 [Coordinator claim](../../platform/coordinator/src/rpc/claims.rs) 的 RPC 链路：

```text
本地创建：adxlet 临时预留本节点计算资源
        → Coordinator claim RPC
        → Coordinator 原子校验卷空闲并提交执行分配与卷占用
        → 返回该具体执行的授权
        → adxlet 校验后准备目录、挂载并执行

中心调度：Coordinator 选择完整可行的节点
        → Coordinator 原子提交执行分配与卷占用
        → 下发授权与执行请求
        → adxlet 校验后准备目录、挂载并执行
```

上游普通创建默认只保留内存分配，首次成功时才发布持久结果，persist_local_claim 默认关闭。受管卷仍保留持久化授权执行身份，exclusive 卷必须在下发节点前将占用与分配原子提交；默认本地入口即使走 ReserveLocalEnvironment，也由 Coordinator 执行相同的卷 claim 事务。普通无卷创建保持上游默认行为，节点不访问 Redis。成功创建继续复用上游的原子发布与 Request ID 回执，发布时保留卷授权身份；失败、超时或响应丢失不能单凭放弃创建删除尚未释放的卷占用。

两条路径在 Coordinator 内遵守同一存储事务约束：Volume UID、执行尝试、节点会话及分配身份一致，分配与占用全有或全无。本地临时计算预留不构成卷占用；取得有效授权前，不得执行对应卷的目录初始化、挂载或启动。其他节点的本地 claim 与中心调度并发时也只能产生一个有效 exclusive 占用。

卷冲突时，本地创建撤销本次未执行候选的临时计算预留，交由现有中心调度等待路径处理，不长期占住本节点容量。Coordinator 不可用或 claim 响应未知时，不允许本地自行宣告成功、加 Redis／文件锁代替授权或启动新执行；按原身份查询、幂等重试，并沿用请求总期限。Coordinator 已提交而响应丢失的占用仍由其权威记录管理，节点不能据此自行释放。

正常生命周期释放通过 RPC 回报 Coordinator；收到回报只代表提出释放，Coordinator 原子提交后才对其他调度开放卷。新执行身份授权、阶段回报及释放 RPC 均校验调用节点和会话；节点失效自动释放仍由 Coordinator 直接在既有失效事务内完成。

## 6 启动恢复与资源等待

1. 固定稳定 Environment 身份、原始规格与 Volume UID，保存引用关系。
2. 校验存储域、来源、权限、运行时能力和可放置节点。
3. 卷忙或资源不足时进入内存等待，不启动执行，不长期占用计算容量；继续处理其他可运行请求。
4. 本地候选或中心调度的完整放置条件满足时，统一由 Coordinator 在同一权威事务内预留卷并绑定执行分配，返回或下发节点授权。
5. adxlet 校验凭证和节点会话，在准入后准备目录、挂载并启动或恢复。
6. 发布执行结果；节点拒绝或失败按正常释放契约回收预留；节点失效按第 7.2 节释放。

新建等待在当前请求／调度视图中体现为 Pending，不代表一个重启后仍会自动推进的持久化创建任务。恢复等待保留 Paused 并沿用现有持久化 resume 意图，取得准入后才进入实际恢复。运行中的 EnsureRunning 不重复排队。公共 resume、共享 Gateway EnsureRunning、节点本地恢复和内部重新执行均不得绕过 exclusive 卷准入；控制面不可用时，不允许自行启动需要 exclusive 占用的新执行。

同卷繁忙沿用卷等待及释放通知，不计为节点计算容量拒绝，也不使本地 checkpoint 的原节点失去恢复资格。准入明确拒绝且尚未授权的新尝试，回滚后保留先前 runtime ID，以便后续重试；授权结果未知或已授权后失败的尝试保留尝试身份用于准确清理。解除生命周期关联前，先核验内存等待与持久引用的租户归属，越权请求不能先取消其他租户的创建队列。

创建请求总期限、资源排队期限、初始化期限分开。连接中断或响应超时不等于已经持久化的分配被取消，但未分配创建不承诺脱离请求持续后台推进。卷等待使用结构化 WAITING_FOR_VOLUME 原因；执行是否从未开始沿用 [SchedulingFailure 与错误契约](../../platform/api/http/error-contract.md)，不因传输或运行时结果未知伪造 never/not_started。

### 6.1 Coordinator 重启与 Activator 请求内重试

首版不补齐通用的持久化未分配创建等待队列。Coordinator 重启后恢复 Volume 定义、固定引用、占用和已提交执行分配；未分配的新建等待需要重新提交。现有 resume／恢复意图仍由 Platform 按原机制推进。

Agent 路径由 Activator 在当前激活请求期限内，通过 Environment 能力接口执行以下流程，不依赖识别 Coordinator 重启原因：

```text
创建调用连接中断或返回暂时不可用／结果未知
→ 有界退避并加入抖动
→ 按原 tenant、Environment ID 查询
→ 不存在：按原 ID、规格及固定引用幂等重试创建
→ 已存在：复用已提交分配与结果，未就绪则继续在期限内协调
```

查不到记录不能证明旧请求从未执行；重试仍必须经过平台原身份的幂等创建与占用事务。查询暂时失败时不能按“不存在”创建；已确认 Failed、Deleted、权限错误、参数错误或规格冲突按原错误契约返回，不生成新 Binding generation 或 Environment ID。

整个过程沿用原激活请求的绝对总期限，不能每次重试重置完整预算；平台排队和初始化仍受该预算及各阶段期限约束。卷管理明确返回的 401／403、404 或返回身份不匹配不消耗重试预算：鉴权配置或请求拒绝使用 Invalid，缺失使用 NotFound，身份不匹配使用 Conflict；传输中断、超时或成功响应缺少 UID 仍保留 OutcomeUnknown。同进程与 HTTP 适配保持一致。

原激活调用方结束或总期限耗尽后，不新增后台激活控制器；后续请求可继续推动同一 Binding。直接使用 Environment API 的调用方自行承担相同重试职责，未来主动发布执行器也应自行推进其显式发布意图。

因此，短暂 Coordinator 重启可由当前请求的重试吸收，不必等用户再次调用；不承诺 Activator／整个调用链退出后仍持续预启动，也不把“原本尚未启动的创建”与“已有运行中沙箱被迫冷启动”混同。

## 7 正常释放与节点失效释放

调度器只判断卷当前是否可占用，不探测旧进程，也不独立判断旧执行是否停止。正常路径由执行生命周期履行释放契约；失联路径由 Coordinator 依据节点失效事务释放。两个路径都只操作准确匹配的旧占用身份。

### 7.1 正常生命周期释放

挂起全过程继续持有卷：完成 checkpoint、移除旧 runtime、完成可能回滚的步骤，确认最终停止且迟到命令不能重启旧执行后，由 Coordinator 提交挂起结果和占用释放。

当前 [lifecycle](../../platform/adxlet/src/controller/lifecycle.rs) 在移除 runtime 后释放计算容量，但后续发布失败时仍可能恢复旧执行。Volume 占用不能绑定当前 release_capacity 时点；挂起失败回滚继续使用原占用。若占用已经释放，任何再次执行都必须重新准入。

启动失败、删除和取消同样由 adxlet 在清理完成、旧尝试的迟到命令已被阻断且不再自行回滚启动后报告释放。Coordinator 校验并提交，不另设进程停止探测。reload、自动重启等受控替换可持续持有原占用，但新尝试必须获授权并条件更新占用身份，不能未经准入复用旧凭证；释放之后则必须重新参与调度。

### 7.2 节点失联自动释放

复用现有 heartbeat_timeout_seconds、expire_nodes 和 invalidate_node，不新增卷宽限期、独立失联计时或管理释放前置确认。节点失效的权威事务扩展为同时提交：

1. 节点不可用、旧会话不可路由，旧执行分配失效。
2. 释放仍准确匹配该节点旧会话、旧 assignment 和执行尝试的 exclusive 占用，使旧占用身份失效。
3. 更新相关 Environment 结果和变更 revision，供调度缓存、状态视图与等待唤醒消费。

事务成功后才向调度器暴露新状态；事务失败或结果未知时从权威状态核对并幂等重试，不能只释放内存中的卷或只提交执行失效。这样，获得恢复资格的沙箱在其他节点重新调度时，不会被自己的失效卷占用挡住。它仍需满足原有恢复点、节点拓扑、计算资源和当前卷占用条件，不保证优先于其他等待者。

节点失效提交前，同卷接管继续等待，其他可运行请求照常调度；提交后，不再因旧节点失联额外等待 adxlet 释放确认、人工操作或节点／存储隔离。旧节点重新通信时，必须先核对权威分配并清理失效旧执行，再重新开放准入；心跳恢复不能复活已失效的旧占用。

这一选择接受网络分区时旧执行仍能访问 SFS，并与新执行并存读写。平台只保证控制面最多一个有效占用，不保证失联期间实际访问者唯一，也不保证重叠访问时长上限。业务自行承担此故障一致性；不把元数据 fencing 描述为文件系统 fencing。Kubernetes 的[强制删除说明](https://kubernetes.io/docs/tasks/run-application/force-delete-stateful-set-pod/)和[超时强制分离说明](https://kubernetes.io/docs/concepts/cluster-administration/node-shutdown/#forced-storage-detach-on-timeout)也区分控制面推进与旧执行实际停止；本方案不照搬其 CSI detach 超时。

## 8 非 FIFO 调度与事件驱动

采用 Kubernetes 暂不可调度等待、条件变化后重新入队的方式。卷忙只影响相关请求本次尝试，不使整轮调度失败，不持有全局调度锁等待卷释放，也不占住调度工作线程等待节点恢复。沿用现有优先级和放置策略，不新增同卷严格 FIFO、不可插队交接或无饥饿承诺。首版不抢占健康节点上正常运行的持有者，节点失效释放按第 7.2 节处理。

例如，B 先等待卷 V 但缺 GPU，A 后到且 CPU、节点及卷条件齐全，V 空闲后允许先调度 A；B 不因先到而占住 V。释放事件只唤醒相关等待者重新检查完整条件，不直接交给某个指定下一持有者。若 A 是旧持有者，释放后产生的新恢复尝试同样参与竞争，无 B 必先于 A 的保证。

占用、分配、正常释放、取消收敛及节点失效释放纳入 Coordinator 权威持久化事务；不在 Agent 层另放 Redis 锁。资源忙与容量不足分别可观测；仅当卷和完整放置条件同时满足时提交占用，不在没有可行放置方案时长期占卷。

维护 Volume UID 到当前内存等待请求的索引，由释放、取消、节点失效和容量变化唤醒相关请求。权威状态与 revision 一致提交；通知只加速推进，订阅断流补快照，低频校验补漏，不逐 Environment 高频轮询。进程重启后从已恢复的 Platform 意图及调用方重新提交的创建请求建立等待索引，不声称能从存储恢复已丢失的未分配创建等待。可扩展现有 [Coordinator 调度唤醒](../../platform/coordinator/src/lib.rs)。参见 Kubernetes [调度队列](https://github.com/kubernetes/kubernetes/blob/v1.35.0/pkg/scheduler/backend/queue/scheduling_queue.go)和 [RWOP 事件唤醒](https://github.com/kubernetes/kubernetes/blob/v1.35.0/pkg/scheduler/framework/plugins/volumerestrictions/volume_restrictions.go)。

## 9 目录初始化与授权

创建 Volume 只登记，不触碰目录。实际创建在执行准入后进行；exclusive 在占用内初始化，shared 的并发首次初始化需要独立的幂等协调，不能靠排他执行占用保障。验证存储域真实挂载存在且后端匹配，不能退化为同名节点本地目录。路径逐级安全解析，只创建最终目录，父目录由部署准备；已有目录不递归 chown、改权限或清理。

对平台新建目录，初始化意图和成功结果保存在 Coordinator。若 mkdir 后设置属性失败或完成结果未提交，不能仅因目录已存在就认定初始化完成。同一 owner 重放 Pending 时继续准备目录；目录已经存在且声明了 create_options 时，节点核对 uid、gid 和 mode，匹配后才能上报成功，不匹配则报告失败并转为 NeedsRepair，保留数据和属性。只有 mkdir 成功的调用设置创建属性，已有目录的重试只检查、不重写。首次接入的存量目录保持原属性；Begin 应答丢失后重试也执行上述核对。初始化 owner 的互斥不等于同 owner 调用串行；重复调用可能重叠，若核对撞上尚未设置完的属性，会报告失败而非提前放行。完成结果已提交但应答丢失时，沿用 Ready 或 NeedsRepair，不重新初始化。

Volume 管理、引用使用和节点执行分别鉴权，保留现有内联来源的权限和路径合法性校验。平台不新增受管目录的内联 host_path 禁用策略；不使用 VolumeRef 的访问不纳入该卷的占用，业务自行保证访问一致。exclusive 的正常执行准入以 Volume UID 为边界，故障接管采用第 7.2 节的放宽一致性。

## 10 最小管理能力

| 操作 | 约束 |
| --- | --- |
| 创建或确保 Volume | 同租户同 ID 已存在则直接复用已有定义和 UID；新建时原子检查权限及存储身份冲突 |
| 查询和列举 | 返回定义、固定引用、Binding 关联、snapshot_ids、占用和初始化状态，按租户过滤；当前内存等待复用已有调度队列查询，不承诺重启持久化 |
| 删除 Volume | 无 Binding 关联、Environment 或 Snapshot 引用、占用、等待及未知副作用；默认保留数据 |

API Server 只做 HTTP 类型、验证和 Platform RPC 转换，业务实现在 platform。Agent 通过受限的平台能力调用，不访问 Coordinator 元数据。现有 Environment SDK 同步更新以上操作和 Mount 引用类型；SDK 适配不是新建一个 Agent SDK 项目。

## 11 存量登记与切换

保留现有内联接口，VolumeRef 增量接入。只有选择稳定卷管理或 exclusive 的业务需要适配和登记，不要求所有 image、S3、host_path 调用方迁移。项目尚未上线，选择改为 VolumeRef 的存量 Environment 采用受控重建，不自动接管运行中的内联执行。

### 11.1 盘点与登记清单

存量目录接入在实际业务切换时单独安排，本次不提供批量登记工具，未执行真实登记或业务切换。盘点清单仅覆盖选择接入的目录，保存 tenant、源 Binding scope/generation、Environment ID、原始真实路径、规范化存储身份、目标与选项、访问模式、拟登记 Volume ID、登记后的 UID 和迁移阶段。别名关联由独立别名方案管理，不作为 Volume 接入前置条件。

必须使用当前保存的模板和 Binding 参数还原实际路径。不能假定 binding_id 等于存储身份；不同 agent_type 或 Binding 可能共享同一原目录。同一目录映射到同一 Volume，来源不明、目录缺失、身份碰撞或父子重叠时停止处理该项，不能创建替代空目录掩盖问题。

### 11.2 切换顺序

1. 冻结相关创建和发布变更，导出清单并核对真实后端、目录及已有权限。
2. 排空受影响旧执行，禁止再次恢复；确认停止后清理旧 Environment。只挂起而仍允许恢复不满足条件。
3. 幂等登记原目录为 Volume，存量项默认 create_if_missing=false，不移动文件、不新增目录层、不修改 UID/GID/mode。
4. 更新选定模板、Agent 存储引用和部署配置，将这些挂载改为 VolumeRef；需要独占的项显式声明 exclusive，其他项默认 shared。HTTP/gRPC 和现有 SDK 同时支持内联与引用式挂载。
5. 用稳定 Volume UID 重建目标 Environment，完成初始化、业务数据校验及挂起恢复后恢复入口。
6. 核验选定业务的旧执行已无法启动、所需独占访问已统一引用同一 Volume UID，保存清单结果和异常项；其他内联调用继续保留。

登记和调用方适配允许幂等续跑，但注册失败、实例清理未知或数据核验失败不得自动向前推进。切换失败保留数据及登记记录，修复后继续；已选择 exclusive 的业务回滚也应继续引用同一 Volume，由业务保证不混入未协调的内联访问。平台不为此关闭全局内联入口。

存量目录接入只登记元数据和关联，不搬迁文件。已有目录的完整性核验按数据规模执行文件清单、校验值或应用级读取，不能因新 Environment 启动成功就判定数据迁移成功。

## 12 执行计划与验收

按 P1–P7 顺序执行。P1 已完成：142 项聚焦测试及 make rust-check JOBS=2 通过，日志位于 out/volume/logs/p1-*.log。P2 已完成：6 项真实 Redis Volume 测试、1 项 TLS 管理 RPC 测试、51 项存储回归及 make rust-check JOBS=2 通过，日志位于 out/volume/logs/p2-*.log。P3 已完成阶段检查：11 项真实 Redis Volume 测试、16 项调度测试、51 项存储回归、32 项 TLS RPC 测试及 make rust-check JOBS=2 通过；HTTPS local-first 分支实际执行，日志位于 out/volume/logs/p3-final-*.log。P4 已完成阶段检查：13 项真实 Redis Volume、51 项存储、5 项恢复、33 项 TLS RPC（含实际 HTTPS）及节点 journal 6／lifecycle 39／pause_resume 47／reconciliation 35／managed_mounts 6 项通过，make rust-check JOBS=2 通过，日志位于 out/volume/logs/p4-final-*.log。P5 已完成阶段检查：15 项真实 Redis Volume、51 项存储、34 项 TLS RPC（含心跳超时同卷接管及实际 HTTPS）、20 项 on_demand、5 项恢复、5 项普通及 1 项真实 Redis 取消单元测试通过，make rust-check JOBS=2 通过，日志位于 out/volume/logs/p5-*.log。上述为受控 runtime 的组件验证，真实跨节点共享存储与运行时验收仍在 P7。P6 已完成：Agent 普通测试 94 项及 10 项单独测试、Gateway 110 项及 1 项单独真实 Redis 测试、API Server 102 项、SDK 196 项及 105 个子测试、RPC 35 项（实际 HTTPS）、Volume 21 项、Snapshot 4 项、core 8 项、pause_resume 47 项、storage 51 项、recovery 5 项与 make rust-check JOBS=2 通过。Snapshot 覆盖固定 UID、重启、多 Snapshot／Environment 引用、发布与 Binding 删除竞争及最后引用自动清理；日志为 out/volume/logs/p6-final-*、p6-snapshot-*。P7 已完成本机双逻辑节点的真实运行时验证，见 12.5；暂不部署到集群，业务切换与跨主机故障验收未执行。行为修改测试先行，每阶段先增加表达契约的失败用例，再实现并完成聚焦检查。可以分阶段合入，但 exclusive 对外启用必须等待正常释放、节点失效释放和所有新执行入口准入形成闭环，不能仅增加请求字段就开放。

### 12.1 分阶段执行计划

| 阶段 | 主要工作与位置 | 完成条件 |
| --- | --- | --- |
| P1 类型与契约 | platform/crates/core、platform/api、platform/crates/protocol；定义 Volume、默认 shared、内联／VolumeRef 二选一、固定 UID、执行占用身份、Coordinator 授权／阶段／释放回报及结构化等待原因 | 默认值及幂等规范化、双来源拒绝、原有内联来源和 Rootfs 契约回归通过；明确 Coordinator 唯一占用权威及正常与失联释放差异 |
| P2 定义与固定引用 | Coordinator 存储、Volume 管理 RPC；ensure 存在即复用、拓扑、规范化来源索引、不可变定义、删除保护；保存未分配创建的最小固定引用及撤销清理边界 | 同名不同请求定义仍返回已有卷、多副本并发仅产生一个 UID、同名重建 UID 隔离、引用固定与删除竞争、跨租户拒绝均有真实 Redis 组件证据；不新增后台创建等待队列 |
| P3 占用事务与调度 | Coordinator storage、claims、reserve、resume／recovery 及 shard 调度；本地 claim 与中心调度统一由 Coordinator 原子提交卷预留与分配；内存等待索引、事件唤醒、非 FIFO | 本地 claim 与中心调度混合竞争、同／跨 shard 并发只产生一个有效 exclusive 占用；shared 并行；资源不足不先占卷；其他可运行请求不被阻塞；丢失提交回报后重试幂等 |
| P4 节点执行与正常释放 | adxlet controller、managed_mounts、sandboxd／Jiuwenbox 适配；通过 Coordinator RPC 申请执行授权及报告阶段、初始化结果和释放；引用解析、初始化协调及全部执行入口检查 | 节点不新增 Redis 依赖、连接配置或凭据；无授权不挂载／启动；启动、恢复、reload、自动重启、挂起回滚、删除与取消均不绕过准入；不在 release_capacity 时提前释放；初始化中断可识别；旧回报不能释放新占用 |
| P5 失联释放与回归节点 | 扩展 storage/failure.rs 的节点失效事务，同时提交旧分配失效和匹配卷占用释放；更新调度缓存、事件及回归节点清理 | 复用现有心跳超时，无额外卷计时、人工或隔离前置；失败／重启可从存储恢复一致结果；有恢复资格的沙箱不被旧占用阻塞；旧会话和旧命令不能重新取得授权 |
| P6 对外接入与请求内重试 | gateway/apiserver 保持 HTTP 校验及直接 RPC；现有 Environment SDK、Agent 模板／Binding／Environment 适配；Activator 首次激活先渲染并 ensure Volume、建立 Binding 关联、固定 UID，再按原沙箱身份和总期限创建／退避重试；删除 Binding 收敛 Environment 与待创建引用，最后一个关联解除后删除无引用卷；Snapshot 固定 UID 并延长定义生命周期 | 业务无需手工登记；已有定义为准；多 Binding 共享的卷在最后一个 Binding 删除且全部引用释放后清理；Snapshot 发布与卷保护、最终删除与解除保护均原子提交；超时撤销清理 Environment 引用而保留 Binding 所持卷；删除重试幂等且保留目录数据；创建中断后当前激活可重新入队；已提交结果不重复执行；内联保留；无后台激活恢复控制器或独立卷 TTL |
| P7 选定业务接入与真实故障验收 | 本轮完成单宿主机双逻辑节点真实运行时验收；暂不部署，不切换现有 Jiuwen／Orbit 模板 | 独立 Activator 完整 9 组与嵌入式 Activator 2 组通过，见 12.5；业务存量接入与切换、跨主机 SFS／网络分区及 Snapshot／checkpoint 恢复待后续验收 |

依赖关系：P2 依赖 P1；P3 依赖 P2；P4、P5 依赖 P3；P6 的外部启用依赖 P4、P5；P7 依赖完整 P1–P6。阶段内可先做契约和适配，但不以尚未完成的能力更新当前可用性说明。Agent 别名发布按独立设计实施，不作为本计划的隐含前置或附带交付。

### 12.2 必须覆盖的验收场景

| 场景 | 验收结果 |
| --- | --- |
| 省略 access_mode、显式 shared 与显式 exclusive | 前两者规范化等价并行，只有 exclusive 进入排他准入 |
| 内联 image／S3／host_path 与 VolumeRef 混合使用 | 原能力保留；同一挂载混合来源拒绝；不因 host_path 指向受管目录新增禁令 |
| 同名请求重试与 Volume 同名重建 | 定义幂等，固定 UID，旧引用不转向新来源；未分配固定引用可按明确撤销边界清理 |
| 同名 Volume 的来源、访问模式或初始化规则请求变化 | ensure 返回已有定义和同一 UID，不比较定义、不覆盖；Activator 使用返回结果 |
| Activator 多副本并发首次激活及 ensure 响应丢失 | Coordinator 原子创建仅产生一个有效资源；重试复用获胜者 UID，不需要业务预登记 |
| 多个 Binding 关联同一个卷，删除 Binding 或删除重试 | 关联幂等；先收敛该 Binding 的 Environment／待创建引用；最后一个关联解除且无其他使用后才删定义，目录数据保留 |
| 激活超时、明确撤销或后续没有流量 | 明确撤销清理 Environment 引用；Binding 所持卷继续保留，无空闲 TTL；Binding 删除可收敛残留的未分配引用 |
| 健康节点同节点／跨节点并发启动 | 同 Volume UID 的 exclusive 只授权一个执行；shared 保持并行 |
| 多节点本地 claim 与中心调度竞争同卷 | Coordinator 原子提交分配和占用，只授权一个执行；节点本地计算预留不构成卷占用，冲突者不长期占用计算容量 |
| adxlet 无 Redis 网络、配置及凭据 | 本地和中心创建均只依赖 Coordinator RPC 取得卷授权；新增生产路径不引入节点侧 Redis 客户端或直接存储读写 |
| 本地 claim 中 Coordinator 不可用或响应丢失 | 未获得有效授权不初始化／挂载／启动；原身份查询和重试不重复占用；节点不能自行释放可能已提交的占用 |
| 正常释放回报中断或响应丢失 | adxlet 幂等重报，只有 Coordinator 提交释放后卷才可再次分配；重报不影响后来持有者 |
| B 先到但缺 GPU，A 后到且条件满足 | A 可先取得空闲卷；B 等待不阻塞 A 或其他卷；无严格 FIFO 断言 |
| 卷占用期间请求重复、通知丢失 | 同尝试幂等，内存等待去重；补快照／校验后可继续调度 |
| 本地优先创建、同节点恢复、跨节点恢复、reload、自动重启 | 所有 exclusive 新执行有授权，原始规格不被节点绝对路径覆盖 |
| 挂起成功、挂起上传失败回滚、删除、启动失败与取消 | 正常释放遵循生命周期契约；不因计算资源释放或单次超时提前释放 |
| 节点心跳超时与失效事务失败／丢失回报 | 节点旧分配失效与旧卷占用释放全有或全无；未知结果权威核对，无额外释放等待 |
| 心跳恢复与失效提交竞争、旧节点回归、旧释放回报迟到 | 条件更新只影响旧身份；失效后不能靠心跳复活；回归先清理；旧回报不释放新持有者 |
| 旧节点只断控制面但仍可访问 SFS | 按现有节点失效时点允许接管，记录可能实际并存写入；不将测试结果描述为存储侧强隔离或重叠时长保证 |
| Coordinator 在未分配等待期间重启 | 内存等待可丢失；Activator 在原激活预算内用原身份重新提交并继续；无后续调用不承诺后台创建 |
| Coordinator 在分配提交后重启、创建响应丢失 | 恢复已持久化分配与占用，不因断连换 ID 或重复启动；不能伪造 never/not_started |
| 已有持久化 resume 意图 | 继续由 Platform 恢复推进，不因未分配创建的内存队列策略退化 |
| Volume 登记来源重复／重叠、越权引用、节点本地拓扑 | 在定义或准入边界拒绝；不把该检查扩展为禁止所有内联访问 |
| shared 并发首次初始化、初始化中断、SFS 缺失 | 初始化状态可恢复，存量目录不被误改；不将半初始化或本地空目录当作有效存储 |
| 挂起后删除 Volume | Environment 引用继续保护卷定义 |
| 可复用 Snapshot 保存 VolumeRef | 固定 UID 并持久保护定义，最后一个 Binding 删除不影响保留的 Snapshot；Deleting 继续保护，最终 Deleted 解除；多个 Snapshot、显式 Environment 引用及重启后仍一致；克隆不继承 mounts |
| 选定业务存量登记 | 原目录、文件内容和权限保持；选定独占业务统一引用 UID，其他内联调用保留 |

### 12.3 验证门槛与证据

Rust 行为变更完成聚焦测试、cargo fmt --all -- --check 和工作区 clippy 门槛；完整基线使用 make rust-check JOBS=<n>。Coordinator 事务不能只跑默认跳过真实 Redis 的测试，须使用现有 build/ci/run.py storage 与 control-rpc 套件并保留完整日志。Volume 存储回归需在同一隔离 Redis 测试环境中另行执行 cargo test --locked -p adx-coordinator --test volumes -- --ignored --nocapture；本次不修改 CI 入口。Agent、Environment SDK 使用各自 Makefile／原生测试命令，覆盖嵌入式与远程适配路径。长构建和测试按仓库约定委派只执行验证的窄上下文 worker。

文档、API／错误契约、SDK 示例、部署配置和使用说明随对应实现同步更新；架构 Markdown 变更后运行 python3 build/docs/render_architecture.py，完成前运行 python3 build/docs/check.py 和 git diff --check。未完成的阶段继续标为目标方案。

分别记录单元／组件、真实 Redis、真实节点、共享文件系统、运行时与故障注入结果。指标覆盖等待数量与时长、占用／预留时长、释放原因（正常或节点失效）、来源冲突、初始化异常、创建重试及恢复耗时。真实跨节点及故障验证完成前，不宣称生产可用；完成后也只声明本文约定的控制面互斥与故障一致性边界。

卷互斥不自动证明业务数据库在共享文件系统上的兼容性。既有工作区方案涉及的 SQLite WAL 应依据 [SQLite 官方限制](https://www.sqlite.org/wal.html)另行确定存储方案，不能以 Volume 接管验收代替数据库一致性结论。

### 12.4 整体 review 修复回归

本轮修复了四处实现缺口：解除关联先鉴权再取消队列；已核验的 adxlet 会话替换原子转移占用与执行授权；卷忙恢复保留可重试执行身份及原节点资格；节点检查返回最新已授权执行视图。复用现有权威事务、卷等待通知及执行视图，不新增等待队列或释放计时器。

五个最初的失败回归先复现问题，修复后通过。补充组合回归覆盖会话替换与节点失效竞争、保留占用释放、初始化中断、明确拒绝与未知授权结果，以及本地 checkpoint 等待同卷时其他请求继续调度。最终真实 Redis Volume 25 项、按需恢复 21 项、节点 pause_resume 49 项、完整 RPC 37 项（含实际 HTTPS）及 protocol 全包 24 项通过；存储 51 项、恢复 5 项和节点 journal／lifecycle／reconciliation 回归通过。make rust-check JOBS=2（按现有 Makefile 排除 AFS）、文档检查及 git diff --check 均通过。证据为 out/volume/logs/fix-review-red-*、fix-review-green-*、fix-review-regression-*、fix-review-final-*。这些仍为本地组件证据，未部署或执行真实 SFS 故障验收。


2026-10-26 整理 PR 时，基于上游 master 6daa919 整合已有 Snapshot 跨 shard 放置逻辑，复用候选节点筛选加入卷拓扑与卷忙条件，并补两条组合回归。Snapshot 放置 11 项、调度 16 项及 make rust-check JOBS=2 通过；真实 Redis Volume 25 项／on_demand 21 项／storage 51 项／recovery 5 项、control-rpc 37 项（含重新生成 TLS 后的实际 HTTPS）、节点 journal 6／lifecycle 39／pause_resume 49／reconciliation 38／sandboxd_rpc 46 项、protocol 24 项、API 契约 29 项、Agent 98 项／Gateway 110 项、SDK 212 项及 110 个子测试均通过。完整日志为 out/volume/logs/pr-*；未部署，真实 SFS 与跨节点故障验收仍待后续。


PR 创建期间主线推进到 f7aaee0f，已合并观测、生命周期事件和 inspection 缓存改动，并保留卷释放证明与占用事务。新增缓存断言验证了带 Volume 的 Snapshot 发布、重启和最终删除。交叉回归通过：storage 52／Snapshot 5／Volume 25／on_demand 22／recovery 5、节点 lifecycle_events 12／pause_resume 54／reconciliation 39、API lib 54 及契约 29、observability lib 19 项，其余未改路径沿用上一轮证据。control-rpc 完整 40 项及实际 HTTPS 已在默认测试栈下通过，库 2 项和后台追踪 1 项同样通过。此前默认栈溢出定位为 Controller 异步调用链的大栈帧叠加；在命令入口对 dispatch Future 使用 Box::pin，保持同一任务内串行执行和既有追踪语义，将外层调试构建栈帧从 766360 字节降至 203288 字节，无需设置 RUST_MIN_STACK。原失败用例与完整套件均已回归通过。完整合并对照日志位于 out/volume/logs/pr-main2-*；栈诊断和修复验证位于 out/volume/logs/stack-opt-*。

### 12.5 本地真实运行时端到端验证

2026-10-26，基于 07b98646 的干净源码构建调试包及独立安装的 SDK 0.1.0rc3，使用真实 Redis 7.2.5、HTTPS／mTLS、Coordinator、adxlet、仓库锁定版本 sandboxd、runc 1.5.1 和新构建的 Execd。单宿主机两个隔离 Docker 节点绑定同一个专用宿主目录；独立 Activator 的完整 9 组在同一轮通过，嵌入式 Activator 的两组针对性验证也通过，不能将后者称为完整双模式九组验收。

覆盖并发 ensure、已有定义直接复用、旧数据及权限保留、重新创建后的旧 UID 拒绝、准入后目录初始化、shared 并发首次使用及跨逻辑节点读写；中心与本地优先创建均验证 exclusive 等待期间无关请求继续执行、正常释放后接管与文件保留。Coordinator 重启保留占用及执行身份。暂停旧 adxlet 心跳时保留旧真实后端，超时后新节点取得同卷占用；旧节点恢复清理后不能释放新占用。两种 Activator 模式均覆盖并发激活、多个 Binding 共享定义、最后一个 Binding 删除后清理定义并保留数据，以及同卷等待期间 Coordinator 重启后原请求保持 Binding generation／Environment ID／Volume UID 重试完成。

本轮隔离配置显式使用既有 OCI bootstrap、runc unrestricted 网络策略和未声明 USER 的最小 Python 工作负载。锁定 runc 不支持下发的网络 ACL；复用工具镜像的显式 USER 0:0 继承在当前权限下返回 EPERM，两项原失败均保留，未修改产品默认策略、放宽运行时权限或改动产品代码。真实 SFS、跨主机分区、Snapshot／pause／checkpoint 恢复及网络／USER 能力不属于本轮通过范围。

完整结果位于 out/volume/local-e2e/attempt11/result.json（独立模式 9／9）和 attempt12/result.json（嵌入式 2／2），日志位于 out/volume/logs/local-e2e-attempt11.log、local-e2e-attempt12.log。外部验证脚本及汇总位于 out/volume/e2e，复用现有本地 E2E 部署和清理入口；早期前置条件失败与诊断也保留在独立 attempt 目录，未拼接为整轮通过。


### 12.6 补充检视修复验证

Pending 重放改为继续准备并核对已存在目录的 uid／gid／mode，保留首次接入存量目录的属性；明确卷管理拒绝与身份冲突不消耗激活重试预算，缺少 UID 的成功响应仍保留结果未知。Volume ID 在 Platform、Agent 与 SDK 统一为上述 ASCII 规则，并删除未使用的 Releasing 契约；grant 回显校验仅作为响应一致性防御。不新增初始化持久化状态、节点 Redis 依赖或卷锁，不扩展节点 RPC 集成测试。

非法 ID 与错误分类的失败回归先复现，再验证修复。最终 Rust 聚焦回归 63 项通过：core 6、Agent core 2、Activator transport 11／Volume 3、protocol 5、节点目录准备 7、Coordinator 真实 Redis Volume 26、API Server Activator 错误映射 3。覆盖并发 Begin 仅一个首次准备、同 owner Pending 重放、持久化视图重开、成功发布后应答丢失的幂等、属性不符转 NeedsRepair，以及目录数据／属主／属组／权限保持。SDK 完整单元测试 214 项及 123 个子测试通过；make rust-check JOBS=2（既有 AFS 排除范围）、Python 提交前检查、文档渲染／链接检查和 git diff --check 通过。Python 既存长行仅做局部格式整理，未改变其余行为。

证据为 out/volume/logs/review2-green-results.json、review2-rust-check.log、review2-python-precheck-final.log 和 review2-red-*。宿主 Python 3.14 的异步用例挂起、打包 Python 默认 CA 路径缺失及两处新测试夹具错误均保留首次日志；最终 SDK 使用已有 Python 3.11 和系统 CA bundle，未放宽 TLS 校验。这些是该轮本地组件回归；当前代码随后完成的真实运行时验收见第 12.7 节，未部署集群。


### 12.7 本地完整端到端复验（61489593）

2026-10-26，基于干净源码 614895936201fbe8658a26abfe0b8f94e20bb015 重新构建 debug 平台包、新 Execd 与 SDK 0.1.0rc3，并核对节点镜像中的进程文件、已安装 SDK 与 Execd 镜像哈希。独立 Activator 和嵌入式 Activator 在各自一个完整轮次中均通过 13／13，不拼接失败轮次。环境仍为单宿主机两个隔离 Docker 节点、共享专用宿主目录、真实 Redis 7.2.5、公共 HTTPS／mTLS、锁定 sandboxd、runc 1.5.1 与本轮 Execd。

两种模式均覆盖第 12.5 节原九组，再增加 SDK／HTTP ID 边界及三组 Pending 初始化重放：目录未创建时完成初始化并实际启动；已有属性匹配时保留数据并实际启动；权限不匹配时返回 INVALID_ARGUMENT、保持目录属性与数据、进入 NeedsRepair。故障通过冻结 adxlet，在测试 Redis 中以 CAS 注入与已分配执行匹配的 Pending owner 后恢复节点，验证实际授权、Begin／Complete RPC、目录处理和运行时行为；不声称真实模拟 CompleteInitialization 丢包或文件操作并发的精确时序。首次创建验证 1234／2345／0750；重放读写验证使用实际工具进程用户 0:0 并保持 0750，不放宽权限。

完整成功轮次为 out/volume/local-e2e-review2/attempt03-standalone/result.json 与 attempt05-embedded/result.json；两轮 cleanup_errors 为空，最终真实后端、Environment 记录与独立卷元数据哈希均为空。构建、镜像及清理证据分别为同目录 build-provenance.json、image-provenance.json、final-audit.json，汇总为 report.md／report.json。验收脚本及每轮客户端快照仅保留在本地 out/volume/e2e-review2 与对应 attempt，不上传仓库。

前置工具配置和依赖下载失败、故障注入读错哈希、工具用户无权写入属主 1234 目录的断言失败均保留。嵌入模式首轮重启后删除撞到 503 reconciling，且停止夹具报告两处清理错误；最终轮对明确的该 503 作最多 30 秒的调用方删除重试，其余错误仍失败，未修改产品就绪或重试策略。成功的占用、分配、运行时身份和数据断言保持原样；SDK 使用缓存运行依赖并重新安装本轮 wheel，未降低 TLS 校验。

最终宿主审计覆盖五个运行轮次：自有容器、网络、cgroup、五个私有目录和相关 EROFS／loop 资源均无残留；零引用后正常卸载本轮新增 erofs／br_netfilter，恢复原模块与 bridge sysctl 路径基线，未强制卸载或写 sysctl。仍使用 OCI bootstrap、显式 runc unrestricted、未声明 USER 的工作负载；未验证分布式 SFS、跨主机分区、Snapshot／pause／checkpoint 恢复、网络 ACL 或 USER 继承，未部署集群或切换业务模板。


### 12.8 动态共享存储域节点接入

2026-10-26，补齐弹性节点接入：共享域显式开启 dynamic_nodes，adxlet 使用 storage_mounts 配置并通过现有注册／心跳报告实际核验通过的挂载。Coordinator 依据当前已对账节点会话计算可访问节点与执行根目录，挂载报告变化刷新等待请求的域交集；复用候选筛选与队列转移，让带卷请求使用其他分片的新节点。固定域不因报告扩展，卷占用、排队期限和执行身份不因拓扑刷新改变。无新增节点 Redis 依赖、拓扑持久化目录、RPC 服务或配置热更新入口。

失败回归先确认动态域尚不支持：core 在解析 dynamic_nodes 时拒绝未知字段；实现后通过。聚焦验证为 core 7、protocol 6、节点挂载准备 8、调度 17、Snapshot 放置 11；真实 Redis／mTLS 完整 RPC 41 项、真实 Redis Volume 26 项及 on_demand 22 项通过。两条动态 RPC 用例覆盖 a/b 满载、已排队请求、新节点 c 缺失／错误／正确挂载报告、共享域双分片转移、固定域不扩展、挂载撤回及旧序号拒绝、无卷请求继续、exclusive 卷释放前仍等待、数据保留。

完整 RPC 首轮出现读取次数回归的后台路由发布噪声，预期最多 2 次而观测 3 次；隔离该测试的后台发布器后，断言收紧为 claim 恰好一次读取，单项及完整 RPC 重跑均通过。首轮失败日志保留，不放宽生产路径或测试阈值。

证据位于 out/volume/logs/dynamic-storage-red.log、dynamic-storage-focus-*、dynamic-storage-final-* 和 dynamic-storage-retry-*。这些是单机真实 Redis／mTLS 与受控运行时的组件回归，不包含真实分布式 SFS 或本轮新增功能的 sandboxd／Execd 端到端验收。未设置 ADX_TEST_APISERVER，现有 HTTPS 进程分支未执行，不能将其测试名称的通过视作 HTTP 验证。第 12.7 节仅证明其注明的源码快照。未部署集群，build/ 中专项 CI 脚本、接线及配置示例仍不随本次提交。

最终 make rust-check JOBS=2 的格式、工作区全 target／feature Clippy 与生产 unwrap 门槛全部通过，沿用 Makefile 的 AFS 排除范围；文档渲染、163 文档／819 链接／57 个 JSON 示例／5 个 SVG 检查及 git diff --check 通过。最终门槛日志为 out/volume/logs/dynamic-storage-retry-rust-check.log，未为重试增加测试栈配置。


### 12.9 合并上游后的完整端到端复验（cc60cbb0）

2026-10-26，基于上游 989913fa 解决生命周期观测、请求幂等与卷原子事务的冲突。新增真实 Redis 回归同时验证混合 claim 的请求索引、卷占用、创建摘要和删除释放；存储 63、卷 27、RPC 41、adxlet 180、core 卷 7、protocol 卷 6 通过，默认测试栈及 make rust-check JOBS=2 通过。RPC 组件未启用 API Server 进程分支，HTTP 证据来自以下真实运行时验收。

干净源码 cc60cbb04de5fb5e1932dbe71f3ab2df974f1a5a 重新构建九个进程、Execd 和 SDK，并逐文件核对镜像哈希。独立与嵌入式 Activator 使用各自隔离的 Redis、证书、网络、cgroup 和数据目录并行执行，每种模式各自完整通过 20／20（原 13 项与新增动态接入 7 项），两轮 cleanup_errors 为空，不拼接失败轮次。

新增场景在 a/b 满载且请求已排队后实际启动第三个 adxlet：缺失挂载或实际后端错误时卷请求继续等待，无卷执行成功；正常 bind mount 接入正确后端后，原请求保持 Environment ID、排队时间和 Volume UID，在新节点运行并读写保留文件，扩容期间 Coordinator PID、启动时刻与 epoch 不变。固定域不扩展；正常卸载和恢复挂载不终止已有执行，等待请求继续；exclusive 占用不会因报告变化解除，原 owner 释放后才接管；显式重启 Coordinator 后重新报告动态能力，已有执行身份与数据保留，新卷执行成功。

本轮运行时验收为单宿主机、单 Coordinator 调度分片、三 Docker 逻辑节点与专用共享宿主 bind；新节点证书预先加入现有 mTLS 信任配置。跨分片带卷迁移由真实 Redis／mTLS RPC 回归覆盖。双分片首次尝试暴露既有无卷中心请求按轮转进入分片、指定节点位于另一分片时可能排队超时，未扩大本次修改范围。指标读取遵循上游临时 503 策略，验收调用方只对精确的 metrics temporarily unavailable 作最多 10 秒等待并记录重试，未放宽产品校验。

完整成功轮次为 out/volume/local-e2e-dynamic-storage/attempt06-standalone/result.json 与 attempt07-embedded/result.json；汇总为同目录 report.md／report.json，构建和镜像证据位于 rebased/，清理为 final-audit.json。全部七轮自有容器、网络、cgroup、私有目录及相关 loop 无残留；成功轮次三节点实际后端、Environment 记录和卷定义为空，目录数据保留。失败与用户中断轮次全部保留，验收脚本与 build/ 配套仍仅在 out/。沿用 OCI bootstrap、显式 runc unrestricted 和未声明 USER 的工作负载；不包含分布式 SFS、跨主机分区、Snapshot／pause／checkpoint 真实恢复、网络 ACL、USER 继承或真实初始化丢包／文件操作并发时序验收，未部署集群或切换业务模板。


### 12.10 合并内存预留与成功发布改动（上游 bc9b40ee）

2026-10-27，保留上游普通无卷创建的内存预留与首次成功发布流程。受管卷在执行前仍由 Coordinator 持久登记授权身份，exclusive 占用与分配原子提交；默认本地入口不因 persist_local_claim 关闭而绕过卷 claim。成功发布复用 Request ID 回执并保留卷授权身份，删除继续释放引用和占用，成功回执沿用上游保留策略。

卷事务保留卷目录的条件更新，并比较涉及的执行记录、节点会话及节点归属围栏，避免上游对象级 CAS 不再推进全局头版本后发生旧记录覆盖；沿用独立业务计数与存储产物保护。会话交接复用现有 snapshot 校验，不新增持久化状态、节点 Redis 依赖或普通请求的提前 claim。

本轮默认栈下真实 Redis 存储 74、卷 27、mTLS RPC 53、adxlet 181 项通过；Coordinator 默认非忽略回归 44 项及卷相关本地 checkpoint 恢复定向用例通过。新增默认本地创建的 exclusive claim 回归，保留上游与卷功能的全部既有用例。make rust-check JOBS=2、Python 提交前检查、架构 HTML 生成和文档链接检查通过。RPC 未设置 API Server 进程路径，不作为 HTTP 或真实运行时端到端证明；第 12.9 节的运行时证据仍对应 cc60cbb0，本轮未重跑完整端到端，未部署。

扩展 on_demand 套件在 background_resume_round_does_not_starve_later_intents 和 cached_background_sweep_retries_a_live_committed_wake_after_timeout 遇到默认栈溢出；两项均在不含卷改动的上游 bc9b40ee 单独复现。本轮未调大栈，也不声称该完整套件通过。日志保留在 out/volume/logs/conflict-20261027-*。
