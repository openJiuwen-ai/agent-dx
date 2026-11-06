# 节点失效与恢复对账契约

2026-09-16，用户明确：Coordinator 根据既有心跳超时判定节点失效，该节点的旧实例执行随之失效。adxlet 恢复连接或进程重启后，以 Coordinator 权威目录对账并清理失效执行。阶段4已实现心跳超时失效、原节点恢复对账清理与旧结果拒收；共享checkpoint的新代次分配、恢复调用、路由切换与重复请求处理已接通，Lima双独立网络/后端的真实FC跨节点验收已通过；目标adxlet执行中重启与未提交执行清理也已通过；独立主机/K8s门禁另行验收。

## Coordinator 的职责

1. 心跳超时后关闭该节点的调度资格，持久化节点会话及旧执行的失效状态，撤销旧路由。仅设置节点 unavailable 不足以完成此流程。
2. 失效后的旧会话、旧归属代次及旧执行不能通过迟到心跳、结果提交或 SQLite 补写重新变为有效。
3. 用户休眠的 Paused 保留失效归属和恢复点，不主动恢复；手动/流量需求通过 EnsureRunning 触发恢复。自动故障恢复中的中间 Paused/recovery.pending 仍代表恢复运行意图。需要恢复的 Environment 使用新的执行代次重新调度，保持 Environment ID；恢复所需 checkpoint 必须已登记、未过期、可跨节点读取且兼容目标后端。
4. checkpoint 缺失或 local-only 时明确恢复失败，不从原始镜像重新创建。节点不可调度期间不把它的容量重新投入分配。
5. Coordinator 重启后，已有节点先关闭路由和准入，并从恢复完成时开始等待一个心跳超时周期。期限内重新报到可正常对账；期限届满仍未报到则失效旧执行。迟到注册和结果提交同样先检查这段期限，不能绕过定时器。

跨节点选点追加的 checkpoint 存储别名、源节点及已拒绝节点排除条件只用于本次调度。`reserve_recovery` 持久化新 Assignment 后，调度请求与放置快照恢复为权威 Environment spec；新代次、设备和资源预留保持不变。后续独占卷执行授权、claim 和状态提交均比较原 spec，不能因这些临时条件产生身份冲突。该规则同时适用于自动故障恢复与休眠对象的按需恢复。

## 超时前的正常进程重启

同一节点在心跳期限内重启 adxlet 时，Coordinator 通过 mTLS 调用原注册地址的 `NodeService.GetSession`，核对节点身份与新进程会话；原地址/代理地址必须保持一致。复核时不持有调度锁，提交注册时重新检查前任会话和期限。新会话先关闭准入并完成权威对账，保留仍有效的 backend，之后再恢复路由。旧会话的心跳和提交被拒绝。

如果原地址仍由旧会话服务、地址发生变化或不能复核，不能提前接续；若心跳已经超时，则先持久化旧执行失效，即使新的进程已启动也只能对账清理。`GetSession` 只验证服务会话，不改变实例状态，也不替代对账。

## adxlet 的职责

1. 恢复连接时，旧进程会话被判失效也必须触发完整对账，不仅进程重启才对账。
2. 对账完成前关闭生命周期入口与新准入；获取 Coordinator 完整目录后先排空各 Environment 已接纳的串行操作。未提交恢复执行清理后重新接纳权威 Paused 恢复点；已知发布的新执行若与目录不同，拒绝该旧目录并重取，不能当成孤儿删除。随后将实际 backend 执行与 Environment ID、归属节点及执行代次逐一比较。
3. 已失效、已转移或代次落后的执行：退役本机绑定/会话，调用 RuntimeDriver 删除旧执行，清理本机占用。不能复用旧执行重新发布 Running，也不能把自动重启策略用于重新拉起它。
4. SQLite 日志不能覆盖 Coordinator 的失效或转移决定；丢弃退役归属的补写记录。有效归属的日志继续遵守已有降级契约。
5. 清理失败时保持对账未完成和准入关闭，重试清理。受有效快照引用保护的制品不随旧执行无条件删除。

自动恢复仅对本地容量不足，以及后端 restore 调用前 `prepare_execution` 的临时准入错误（`Unavailable`、`VolumeBusy`、`NoCapacity` 或准入超时）返回可重试错误。已确认清理的尝试保留 Paused、checkpoint 与 `recovery.pending`；Coordinator 仍保留该恢复计划的调度预留。临时准入错误沿用已有 Assignment 重试；明确 NoCapacity 可由按需恢复流程记录拒绝后重新选点。执行准入控制 RPC 明确返回 `Unauthenticated` 或 `PermissionDenied` 时，保留已有错误码及原因，确认清理后转为 Failed，不自动重试。`Internal` 保留诊断原因，继续沿用该阶段的临时失败策略；没有明确认证拒绝的 TLS/连接故障仍按不可用处理。checkpoint 读取失败、后端拒绝、开始 restore 后的初始化超时、readiness 或路由激活失败，在确认清理后转为 Failed，即使错误类型为 `Unavailable`。清理不确定时保留实际状态和占用，不从镜像另起执行；Running 结果发布失败只补交结果，不重复调用后端 restore。

执行授权可能先写入 Redis 的 `volume_execution` 和独占卷 Occupancy，再返回暂不可用。adxlet 此时保留本次 runtime id；确认清理后通过 Paused 提交携带 `stopped_runtime_ids`，Coordinator 原子释放对应占用并标记执行已释放，不依赖下一次恢复才释放。下一次准入用这个已清理 runtime 作为 previous，再申请新的 runtime。清理或提交未确认时保留清理凭据和必要占用，由后续同步/对账处理。

失效判定与物理清理是两个步骤：前者由 Coordinator 在心跳超时后完成，后者由恢复后的 adxlet 执行。跨节点恢复不以收到旧节点的停止确认作为前置条件；控制面不应将超时事件记录为已经观察到远端进程退出。

Coordinator 暂时不可用时的 SQLite 降级行为，与 Coordinator 已经作出的节点失效决定分别处理；一旦获取到权威失效记录，对账清理优先，不能以本地日志保留旧执行资格。

## TDD 验收顺序

- 节点超时：旧执行失效与路由撤销持久化；迟到心跳和提交不能复活旧执行。
- adxlet 恢复连接/进程重启：发现原执行仍在，先清理再开放准入；清理失败保持关闭。
- 有效共享 checkpoint：新节点恢复同 Environment ID、新执行代次，发布新路由。
- 无 checkpoint、local-only、过期或不兼容 checkpoint：明确恢复失败。
- 恢复过程被 Coordinator 重启或请求重试打断：不重复创建，不接受旧代结果；原节点之后恢复仍清理旧执行。

当前 `CoordinatorRpc::expire_nodes` 调用 `Session::invalidate_node`，原子更新节点和旧执行结果，并保存 invalidated 标记；迟到注册/提交路径也会先检查超时。adxlet 对账会废弃与权威终态冲突的已有控制器，SQLite 不补写到权威终态之上。`Session::reserve_recovery` 与 `CoordinatorRpc::recover_environments` 已接通共享 checkpoint 的新代次分配/恢复；持久化计划、重复请求和源节点制品保护有组件测试。真实FC双节点六项及计划落盘后Coordinator重启已通过，r5目标adxlet执行中重启及未提交backend清理也已通过，见 [跨节点恢复](2026-09-16-cross-node-recovery.md)。
