# 共享 checkpoint 跨节点恢复

> 当次验收/调查记录：版本、数字及未覆盖范围仅适用于文中批次；当前实现与状态见 [实施总览](control-plane-implementation.md) 和 [阶段路线图](control-plane-roadmap.md)。

本轮接通心跳失效后的自动恢复：Master 使用已登记、未过期的共享 checkpoint，通过 Domain 调度选择其他节点，保持 Capsule ID 并分配新的执行代次。节点本地恢复点、缺失或过期的恢复点不进入调度，旧执行保留 Failed；不从原始镜像重新创建。

## 实现边界

- `Session::reserve_recovery` 在 Redis 中原子更新归属、代次和恢复计划。计划以目标节点上的 Paused 记录保存，恢复完成前持续占用调度预留。Master 重启后可从该记录重建预留和恢复任务。
- `MasterRpc::recover_instances` 使用现有 Domain Filter/Score，排除失效源节点；每轮最多分发16项、并发4项，循环游标避免靠前的失败任务阻塞后续任务。恢复循环独立于节点心跳处理。
- 内部 `NodeService.RecoverCapsule` 校验目标进程会话和归属，并进入已有的 Capsule 串行控制任务。相同代次使用同一操作 ID，重复请求返回原结果。执行已成功而发布暂时失败时，只补提交结果。
- 恢复点不适用且未留下本机执行时，节点提交终态 Failed，Master 释放预留。恢复操作已经留下执行时，沿用生命周期模块的清理和提交流程。
- 旧节点返回后按权威目录清理旧执行；旧代次结果不能覆盖新归属。节点对账同时获取集群仍引用的共享 checkpoint，防止上传源节点将已经转移的制品误当孤儿删除。
- 新节点恢复成功后才发布新路由。心跳超时是控制面失效判定，不是观察到原物理进程退出的证据；原节点物理清理由恢复后的对账完成。

## 测试覆盖

先编写失败测试，再实现同 ID 新代次恢复与 Redis 原子转移。证据目录为 `out/ci/stage-4/transfer/`。

1. Core：显式源身份允许同 Capsule ID 转移到更高代次，拒绝相同或更低代次。
2. 真实 Redis：未失效时拒绝转移；原子写入新归属；重复预留幂等；Master 存储会话重启后保留计划与调度占用；拒绝旧代次提交。
3. 真实 Redis：缺失、local-only、过期 checkpoint 均不能转移。
4. 真实 Redis 与 mTLS RPC：两个 Domain、源节点失效、目标节点恢复并切换路由、重复恢复不再启动执行、源节点返回清理，以及共享制品引用保护。
5. Node Manager：制品无法使用时提交 Failed、释放本机占用，并且不调用镜像创建；重复请求保持失败结果。
6. Node Manager：恢复已经执行但Master提交失败，重试与再次重放都只提交原结果；backend restore总计一次、镜像start零次。

`final-1.log`：176项通过、0失败，Clippy通过。包含真实Redis/mTLS 28项、Core 7项、Master常规45项、Node Manager 88项、RRT 8项；benchmark未在本轮执行。`publication-retry.log`：新增提交失败重试用例及同文件23项通过，使本轮覆盖增至177项。`build-linux-2.log`：当前运行代码的Linux ARM64 release构建通过。

第4项使用真实对象存储适配器和内存对象存储，RuntimeDriver、就绪与路由执行端为测试实现。这是组件集成证据，不能替代双节点 Firecracker 或 Kubernetes 端到端验收。

## 发布包基础回归

package-v21（当前未提交工作树构建）在本地Docker/ARM64/runc双节点上通过SDK、认证、容量、放置、node-failure、restart、stop七组。运行ID为 `adx-e2e-77744e07b23a`，最终 `cleanup_errors: []`，两个容器及网络均已移除。证据为 `out/ci/stage-4/transfer/local/result.json` 和 `package-e2e.log`。Go API、SDK与Redis复用已验证package-v18中的未变更制品，Rust二进制本轮重新构建。该基础回归证明新恢复接线没有破坏原有路径，不证明真实checkpoint跨节点恢复。

## 真实执行后端验收

package-v21 / Lima双网络命名空间r4已通过六项真实FC验收：同ID跨节点恢复、PID/内存/文件保留、源节点清理、恢复后Master重启、最终清理；另外通过计划持久化后、恢复RPC执行前的Master重启注入。后端使用私有挂载视图，目标不能直接读取源节点运行目录。见 [详细运行记录](firecracker-cross-node.md)。这是单KVM主机两套独立进程节点的证据，独立主机与Kubernetes仍另行验收。

## 本地验收结论与正式门禁

- 阶段4本地验收已完成：r5新增backend Running、Redis尚未提交时杀死目标Node Manager，确认旧backend清理、同代次重新恢复与最终单实例；详见上述运行记录。
- 将该故障恢复场景接入独立 Kubernetes 验收步骤。现有单节点 FC 驱动和基础双节点 runc 用例不覆盖此项。
