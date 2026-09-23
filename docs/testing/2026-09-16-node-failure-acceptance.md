# 节点失效与恢复清理验收

> 历史记录：命令、组件和产物名称对应当时版本；当前命名见 [组件命名](../architecture/naming.md)。

> 当次验收/调查记录：版本、数字及未覆盖范围仅适用于文中批次；当前实现与状态见 [实施总览](control-plane-implementation.md) 和 [阶段路线图](control-plane-roadmap.md)。

2026-09-16，阶段4第一部分按已确认的契约实现：心跳超时判旧执行失效，原 Node Manager 恢复后对账清理。本轮尚不实现跨节点 checkpoint 恢复。

## 实现

- `master/src/storage/failure.rs`：通过一次 Redis Lua 执行原子更新节点可调度/路由状态和该节点未删除实例的失效结果；保存 `invalidated` 标记，保留身份和 checkpoint 元数据。旧执行的逻辑资源占用释放，失效节点仍不能接收分配。
- `master/src/rpc.rs`：定时器、迟到注册及提交都检查心跳期限；更新调度内存并唤醒等待者。Master 重启后的未报到节点也有一个心跳周期的恢复期限。持久化结果不确定时进入权威恢复保护。
- 超时前正常重启：Master 使用新增的 `NodeService.GetSession` 在原地址通过 mTLS 复核新会话；核验通过才接续，对账期间关闭路由。新地址或原地址仍由旧会话服务时拒绝提前接续。已超时的执行仍失效清理。
- `master/src/storage.rs`：失效后拒绝 Running、Paused、资源重新占用及自动重启结果；即使迟到结果 revision 更大也不能复活旧执行。允许恢复节点提交清理终态。既有 create/rejected-retry 路径不能从镜像重新拉起失效实例。
- `node-manager/src/reconciliation.rs`：终态与当前控制器冲突时，先串行废弃旧控制器和 backend，再恢复权威记录。清理失败保持准入关闭。
- `node-manager/src/journal.rs`：权威 Failed/Deleted 终态覆盖本地迟到日志，不能因本地 revision 更大而补写 Running。

## 验证

TDD 红灯确认两处原行为：Master 超时仍保留 Running/资源占用；同进程对账仍复用旧控制器。修复后 Node journal/对账13项、真实Redis重启及mTLS心跳两项通过。随后完整Node测试87项，Master库/真实Redis/mTLS25项，以及Clippy通过；计数存在覆盖重叠，不相加作为独立用例总数。首次广回归因测试夹具缺失 `ADX_TEST_EVIDENCE` 失败，补齐后重跑通过，原日志保留。

新增真实 `node-failure` 用例使用统一 package-v19、ARM64 Linux、两节点 Docker 与外部 sandboxd/runc：

1. 通过安装后的公共 SDK 在两节点创建真实实例。
2. 仅 SIGSTOP node2 的 Node Manager；sandboxd 独立运行。
3. 等待 Master 心跳超时，核对 Redis 旧执行失效、Failed、不占逻辑资源、节点不可调度且退出路由视图。
4. SIGCONT 同一个 Node Manager，等待对账后就绪，检查 node2 backend 清单为空。
5. node1 的原实例仍能通过 SDK 执行命令；显式删除全部测试实例，确认终态和资源回收。

package-v19 的故障组已通过，耗时53.645秒；完整门禁未通过：前五组通过，进程重启组在 backend 身份保持断言失败，停机组未运行，清理无残留。原注册逻辑强制新会话等待旧心跳超时，触发了新增失效清理。原始失败保留在 `local/result.json` 与 `local/043.log`。故障组与既有SDK、认证、容量、放置、进程重启、停机组共用本地/K8s驱动，缺失故障组不能判通过。驱动契约49项通过。

新增两项TDD回归分别复现了“正常重启强制等到超时”与“Master重启后节点永不过期”。修复后真实Redis/mTLS22项、库测试5项及Clippy通过（`restart-red.log`、`grace-red.log`、`grace-green.log`）。Master恢复测试同时覆盖按时报到、定时器过期和先于定时器执行的迟到注册。

package-v20 重新构建后，隔离双节点的七组全部通过：SDK、认证、容量、放置、节点失效、进程重启和停机。失效组确认原节点恢复后 backend 为空；正常重启组确认两个节点的 backend ID 均保持不变，并继续通过公共 SDK 查询和执行。运行 `adx-e2e-ab00f7069a23` 的容器与网络全部清理，`cleanup_errors=[]`。最新驱动回归49项通过。

## 证据与限制

- 包：`out/ci/pause-resume/package-v20/manifest.json`，`aarch64-unknown-linux-gnu / release`；基准 `0dde79ad57583e998389101a763e4d2d825be63e` 加未提交修改，`dirty=true`。
- Master SHA256：`06641e15ce9c5efe6daf95c77fd69aa0f7f58642ccd360884d7177db8e35a659`。
- Node Manager SHA256：`09ae8cab378d5d25baa3e64c597de1076173dd9dced7f70d8df1f84de8c575d8`。
- 节点镜像：`sha256:3845039266614a55911f4e0e74c71da62b34269036613d0f7b4cc2c3a4123de0`。
- sandboxd：`efc201531d7e2e9d69505da151eb66084b61eebf`。
- 日志根：`out/ci/stage-4/failure/`，包含 `red.log`、`green.log`、`regression-2.log`、`node-regression.log`、`driver-final.log`、`build-linux.log`、`package-e2e.log`。
- 最新日志：`restart-red.log`、`restart-green.log`、`grace-red.log`、`grace-green.log`、`driver-r2.log`、`build-linux-r2.log`、`package-e2e-r2.log`。
- 完整结果：`local-r2/result.json`；故障证据：`local-r2/node-failure-result.json`；正常重启：`local-r2/restart-result.json` 与两节点 `backend-before/after` 文件。前轮失败保留在 `local/`。

本轮没有运行正式K8s或FC跨节点恢复。仍需实现共享 checkpoint 的新执行代次分配/恢复、恢复过程重试/重启、归属与路由转移。失效与路由撤销是控制面决定；远端执行在 Node Manager 恢复后实际删除，本轮不声称超时瞬间已物理停止。
