# adxlet 生命周期与降级

adxlet 拥有实例串行状态机。Coordinator 提供归属校验、Redis 持久化和路由发布；暂停等节点操作由 API Server 缓存命中后直达节点；手动恢复与 Ingress 流量唤醒统一调用 Coordinator `EnsureRunning`。

## 运行策略

`EnvironmentSpec.lifecycle` 包含 `idle_timeout_seconds`、`idle_action`（默认 delete）、可选 `pause` 和可选 `restart`。HTTP 对应 `idleTimeoutSeconds`、`idleAction`、`pauseOptions`、`restartPolicy`；Python SDK 对应 `idle_timeout`、`IdleAction`、`PausePolicy`、`RestartPolicy`。自动和手动暂停后的鉴权业务流量均可唤醒，无需额外开关。

- 空闲超时为 0 时不回收。启用后，运行实例的 Execd 活跃客户端请求数与 Relay 流计数必须同时为 0，且活动版本在整个空闲窗口内未改变。后台命令仍计入 Execd 状态与活动版本，但客户端退出后不会仅因命令仍运行而阻止回收。观测断流或活动版本改变会重置窗口。adxlet 默认执行删除；`idle_action=pause` 在同一窗口结束后进入自动暂停，要求正数空闲超时和可用的 checkpoint 能力。
- RuntimeDriver 确认执行退出后，节点先退役路由、确认旧执行清理，再标记 Failed。启用重启时，按照指数退避和累计重试上限重新申请本机资源，并分配新的执行 ID，Environment ID 与归属代次保持。
- `restart_attempts` 和 `restart_pending` 随实例结果写入 Redis／降级日志。进程重启不重置次数。显式删除取消重启。清理未确认时保留资源，禁止启动替代执行。
- 可配置 `execd_health_failure_threshold`；省略时不以健康探测失败触发重启。控制流持续不可用达到重连窗口后，仍须先确认旧执行清理。明确健康探针失败由 Execd 主动上报。
- 初次 Start 失败、超时或应答丢失均判定创建 Failed；不会触发运行中退出的自动重启策略。创建进入 Failed 即释放资源预留并提交结果；清理失败或超时不阻止释放。Failed 控制器继续清理残留及迟到执行，不转成 Running。节点重启按权威目录对账。

## 权威记录对账与事件驱动

Coordinator 在启动或异常写入恢复时从 Redis 加载状态。Environment 变更提交后更新内存记录；快照与执行引用的提交同步更新 inspection 缓存。`InspectNode` 从这些缓存构建完整目录，正常读取不访问 Redis。写入结果不确定或被取消时使相关缓存失效，恢复成功后再服务目录。

后台待恢复意图扫描和失联休眠对象的到期扫描也从 Coordinator 已提交缓存选择候选；具体恢复、退役仍通过 Redis CAS 校验和持久化。恢复意图或退役写入被取消、结果不确定时，先重载权威记录，再继续缓存扫描。

ADXlet 启动或节点会话重新接管时，在准入屏障内执行一次运行时清单恢复，接管既有执行并清理无主资源。`report_interval_seconds` 控制心跳、容量上报和权威记录核对；日常核对只比较归属、generation、revision 和已提交记录，使用实例串行队列收敛记录差异，不扫描运行时清单。

同一实例最多一个待完成核对任务。读取目录前记录本机 Controller 身份，期间新接纳的 Controller 不会因缺席旧目录被删除；旧代际不能覆盖新执行。节点会话失效、启动恢复和目录错误仍限制准入。状态提交失败保留待发布结果，由该实例自己的重试截止时间恢复。

已发布本地结果领先于目录时，仅重新核对该 Environment，不采用旧目录、不关闭整个
节点准入，也不阻止该对象的删除等生命周期操作。网络策略等可变 Spec 字段发生变化时，
同一归属下先确认结果已发布并比较 revision，再检查 Spec；旧目录不能将已发布的更新
判为冲突。Journaled 结果必须先补交成功；当前版本的 Spec 或归属冲突仍限制对应
Controller，直到权威核对通过。清理或发布失败也只限制对应 Controller；下一轮重试，
其他对象继续服务。
已撤销归属的清理排在该对象先前接纳的操作之后，后续迟到操作不能重新启动旧执行。

节点必须配置 `runtime_control`，与运行环境内的 Execd 同时升级。Execd 上报 Ready、明确探针失败、checkpoint 请求和活动版本变化。活动变化合并上报，代理通过一条节点级 UDS 订阅发送首次全量和后续增量，只有对应实例被唤醒。断流清除观测，重连发送完整当前状态；没有新鲜的两侧活动信息时不启动空闲计时。


每个 Controller 等待后端退出通知并维护自己的空闲、重启、重试和 checkpoint 到期截止时间。sandboxd 的 Wait 使用长等待，不套普通 RPC 截止时间。Execd 异常断流后给予重连窗口，再按需确认后端；确认已退出时按退出流程清理。配置 `execd_health_failure_threshold` 时，持续断流且后端仍存活也触发清理，重连窗口为该值乘 5 秒；省略时窗口为 5 秒，后端仍存活则继续等待重连。成功重连取消断流确认。暂停、删除的最终结果由 adxlet 在后端完成后提交。

独立的 `metrics_interval_seconds` 默认为 15 秒，只读取运行中实例的 CPU、内存。资源采集和指标写回均在生命周期队列外执行，并发上限 32 只约束后端 Stats 请求；checkpoint 等生命周期操作不占用采样额度。写回在独立内存缓存中原子校验控制器身份、执行代次和运行状态，暂停、退出或控制器替换后的迟到样本被丢弃。样本失败不改变实例状态。日志回收在启动接管完成后从节点已知执行记录取得保留集合；未确认清理的执行继续保留日志，不再为回收日志扫描运行时。异常清理重试采用 1–32 秒退避；已设置的重试截止时间不会因后续被动核对命令而后移。

工作负载主动 checkpoint 请求在瞬时状态提交或 ACK 失败后，由实例自己的退避截止时间继续处理，无需新的状态通知。待办保留操作 ID、执行身份和首次观察到的截止时间；重复通知不延长预算。已捕获的结果通过现有幂等判断继续提交和确认，不重复执行后端捕获；后端结果不确定时仍走清理和失败处理。断流期间保留待办，重连完整状态可确认请求是否已结束。

## 休眠与按需恢复

`pauseOptions.retention` 为 `{"kind":"ttl","ttlSeconds":90000}`（默认）或 `{"kind":"forever"}`。
有限 TTL 从制品发布完成开始计算，与空闲窗口分别计时；永久恢复点的过期时间为空，
保留至显式删除或替换。`timeoutSeconds` 默认 300（1–3600），约束后端捕获 RPC，
不覆盖 OBS 上传等后续步骤的整体耗时。`storage` 为 configured（默认）、local 或 shared；
SDK 使用 `CheckpointStorage` 枚举。shared 要求节点已配置共享对象存储，发布失败不能返回暂停成功。

自动暂停先通过 Relay 的活动版本原子关闭新流准入，再通过 Execd 的活动版本关闭
客户端操作准入并复核需求。已接纳请求不会被当成空闲；捕获前的新需求取消本次暂停，
捕获开始后只排队等待，不取消不确定的 checkpoint，随后由同一恢复入口恢复。
后台命令仍沿用原先的空闲检测语义，可由后端冻结保存。

Ingress 先完成租户和数据面策略校验，再发出 `EnsureRunning`；被动订阅、health/metrics
不产生唤醒需求。Coordinator 在 Redis CAS 中记录恢复意图和固定 operation ID，并按
Environment 串行驱动；多个 Ingress、手动恢复和后台重试共用它。先尝试原节点的本机
资源准入；只有明确 NoCapacity 或旧执行已失效且有共享恢复点，才选择兼容节点。
RPC 超时、应答丢失和发布失败保留当前归属，避免启动第二份执行。全部节点拒绝时，
一个容量轮次排除已经拒绝的节点，30 秒后重试资源，不在节点间持续改写归属。

节点失效将 Paused 标记为归属失效并释放资源，仍保留 Paused；不进入 Running/Failed
对象的主动故障恢复。自动故障恢复过程中的中间 Paused/recovery.pending 并非用户休眠，目标再次失效仍续走原有故障恢复。首次需求再触发休眠对象跨节点恢复。Coordinator/adxlet 重启按 Redis
意图、generation 和节点进程会话对账，清理未提交执行后才能恢复。TTL 到期或显式
删除与恢复提交通过串行节点任务及 Redis 版本校验竞争；失联休眠对象由 Coordinator
退役，迟到结果不能复活它。原节点回来对账时，先排空旧控制器操作并清理执行，再根据
完整全局引用目录回收无人引用的 checkpoint；其他节点或待完成恢复仍在引用的制品必须保留。
仅本机目录不构成删除共享制品的依据。对象删除失败可在下次对账重试。

组件测试 `on_demand.rs` 使用真实 Redis、Coordinator/Node gRPC 和多个真实 Ingress；
执行内存与对象存储为可控夹具。Execd idle admission 测试启动生产二进制，用 FIFO
模拟 checkpoint handoff。它们不构成此次真实 Firecracker/OBS 部署验收。

## 资源与指标

`resource_source` 选择以下之一，也可保留既有 `capacity_file`。两者不能同时配置。

```json
{"kind":"sandboxd","socket":"/run/sandboxd/resource.sock","valid_for_seconds":30}
```

sandboxd 的 `/resource` 通过 Unix Socket 提供 CPU 核数、内存／存储字节数和整卡设备 ID；节点先验证 sandboxd 健康 RPC，再转换为调度器毫核单位。健康检查覆盖 sandboxd 内部资源刷新有效期。

```json
{"kind":"auto","disk_path":"/opt/adx/data","valid_for_seconds":30}
```

自动模式读取当前 Linux 进程的 CPU 可用并行度、cgroup v1/v2 层级限制、cpuset、内存上限与目标磁盘可用空间。它不虚构 GPU/NPU 清单；需要设备清单的部署选择 sandboxd 源或带设备信息的观测文件。

采集只更新容量，不重置已有资源预留。CPU、内存和磁盘容量均允许为 0，
也允许低于已有预留；成功读取的零容量样本立即替换旧容量并刷新有效期。
首次收到零容量样本也可注册节点和维持心跳。节点本地与 Coordinator 使用同一容量口径，
可分配量为 `max(capacity - reserved, 0)`，资源匹配自然拒绝无法满足的新请求。
容量下降不终止已有执行，也不修改已有预留；后续容量增加或业务退出释放预留后，
按新的可分配量恢复调度。Environment 请求的 CPU 和内存仍必须为正数。

首次无有效样本时等待；后续采集失败沿用旧样本至其原有效期结束，失败不续期，
过期后关闭新准入。负数、溢出和格式错误仍属于无效输入。资源观测新鲜和准入标志
分别表示样本有效期与节点是否允许资源匹配；CPU 或内存为零的新鲜样本仍可保持这两个
标志为真，但不满足正 CPU／内存请求。磁盘为零时仍可匹配不请求磁盘的实例。
`pressure` 提供内存和磁盘高／低阈值，任一达到高阈值关闭准入，两者都降到低阈值以下才恢复；压力采集失败也关闭准入。

配置 `metrics_listen` 后提供 `/metrics`。实例 CPU／内存经 RuntimeDriver Stats 定时采样，并携带采样年龄；资源预留和节点准入也作为指标导出。日志由部署环境收集。

## SQLite 降级契约

`degradation_journal` 为可选本地 SQLite 路径。

1. 正常操作完成后，adxlet 经 Coordinator 提交结果。没有待补写日志时不创建本地数据库。
2. 只有 Coordinator 不可用才尝试 SQLite 落盘，WAL 使用 FULL 同步。明确的权限、归属和版本拒绝不会降级。落盘失败不会报告 Journaled。
3. SQLite 保存有序、去重的待补写结果，不保存完整实例目录。暂停后恢复等连续操作必须按序补写，不能只保留最后状态。
4. 本地操作已完成不等于集群可见。内部 RPC 返回 Journaled；公共暂停／恢复 API 返回暂不可用并说明集群发布待完成。Ingress 仍使用缓存路由，由 Relay 复核本机绑定。
5. 当前节点进程在已对账的状态下可继续处理生命周期操作，心跳恢复后补写日志。补写成功后清理旧制品。
6. 节点进程重启后，先等 Coordinator 完整目录。日志记录仅在归属／请求规格匹配且版本更新时补写；已提交结果和退役归属被清除。补写后再次读取目录，再与真实 backend 对账，最后开放生命周期和新准入。
7. Coordinator 不可用时不能以 SQLite 缺记录为由清理或重建运行实例。SQLite 损坏也不能解释为空目录。

本地目录是降级可用性的条件；删除日志意味着丢失其中尚未进入 Redis 的结果。日志不是 Ingress 的发现或路由来源。

## 验证

Rust 用例位于 adxlet 的 `journal.rs`、`resources.rs`、`pause_resume.rs` 与 Coordinator 的 `storage.rs` 测试。真实 Redis 用例覆盖 Redis 进程中断、节点持久化组件重建、有序暂停／恢复补写与重启次数保护。

`build/e2e/firecracker/sdk_node_lifecycle.py` 在显式指定的 Linux 测试部署中通过公共 SDK 验证后端异常退出重启、Coordinator 失联空闲删除、SQLite 日志、节点进程重启等待、恢复补写和资源源断开。脚本中的 Redis／backend 查询是验收证据，生命周期正常操作仍使用公共 API。

后端首次启动使用独立的 `runtime_ready_timeout_seconds`（默认 120 秒）作为单轮就绪探测窗口。连接失败或一轮探测超时后，adxlet 按 `report_interval_seconds` 继续等待外部 sandboxd；等待期间不注册节点、不启动 RPC 服务，也不开放实例准入。配置错误仍立即退出。日常 `rpc_timeout_seconds` 只控制单次 RPC，避免后端资源初始化时间挤占日常故障降级预算。

## 本地验收（2026-09-16）

独立 Lima `adx-fc`，运行目录 `/var/lib/adx-pause-r7`，ARM64 原生 KVM + Firecracker，`package-v6`，源码包含未提交修改。继承同节点 checkpoint 5 项用例全部通过，新增 5 项真实运行时故障用例全部通过：

- 后端执行实例被删除后，按策略自动重启并使用新的执行身份。
- Coordinator 被暂停时，节点空闲删除写入 SQLite，Redis 保留旧状态，未冒充全局提交成功。
- 此时重启 adxlet，等待 Coordinator 对账，保留已有运行实例。
- Coordinator 恢复后，重放删除日志并恢复已有实例路由，SQLite 待同步记录归零。
- 资源采集 socket 暂时不可访问，超过有效期关闭准入；采集恢复后重新开放，已有实例继续运行。

同一轮最终目录中实例全部 Deleted，资源已释放，sandboxd 清单与 checkpoint 目录为空，服务正常停止。
证据位于 `out/ci/pause-resume/fc-r7/evidence/`，包括 `sdk/result.json`、`lifecycle/result.json`、`catalog-final.json`、`inventory-final.txt` 和 `result.json`；构建/部署完整日志位于 `out/ci/stage-2/{linux-2,package-2,fc-r7}.log`。回归日志为 `rust-5.log`（141 项测试，Clippy 通过）及 `clients-4.log`（Go 检查及 138 项 SDK 测试）。这些是本地证据，Kubernetes 验收尚未包含新增生命周期场景。
