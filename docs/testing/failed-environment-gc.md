# Failed Environment 自动回收

Coordinator 自动回收终态 `Failed`，默认保留 600 秒。资源释放与元数据回收分开：创建 Failed 立即释放预留资源，保留期只用于保留诊断结果，不继续占用创建配额。

## 配置与范围

在部署 YAML 中的 Coordinator 服务配置设置：

```yaml
services:
  - id: coordinator
    role: coordinator
    config:
      # 与 listen、redis_url、tls 等其他 Coordinator 参数一起设置
      failed_retention_seconds: 600
```

该值必须大于零；省略时使用 600 秒。Coordinator 每 5 秒检查一次，每轮至多处理 16 条 Failed，按游标轮转，不扫描全部运行实例。保留期到达不等于立即消失：积压、节点清理或存储故障会延迟回收。

失败时间随 Failed 结果持久化到 Redis；相同失败的重试和 Coordinator 重启不重置保留期。旧记录没有失败时间时，从首次巡检开始计时。等待自动重启、正在跨节点恢复，或失效归属仍有有效共享恢复点的记录暂不回收；共享恢复点过期且未恢复后可进入回收候选，但仍有恢复点时必须由在线所属节点清理，不能仅从 Redis 删除记录。

## 清理与隔离契约

- 所属节点健康时，Coordinator 调用 adxlet 删除接口，携带归属 generation、节点会话和期望 Failed revision。adxlet 在该 Environment 的串行控制任务中复核状态；已恢复为 Running、待重启、修订号或会话不匹配时拒绝 GC。只有校验通过后才写入 `observation.intent=policy` 并开始清理；被拒绝的回收请求不修改本地记录。否则，普通 DELETE 发布 `intent=user` 后到达的过期 GC 会造成相同 revision 的本地记录与权威记录内容不同，触发对账冲突并阻断后续删除。
- Coordinator 删除请求必须恰好包含一种 fence：`failed_revision` 进入 Failed 回收，`expected_revision` 约束普通版本删除；运行期限改由 owner 内存 timer 进入既有 Delete，不携带 expiry_revision；API Server 不能携带这些内部 fence。删除回执保存原始 fence 的种类和 revision，不能用迁移后的 Deleted revision 替代；同一归属、会话和原始 fence 的重试返回同一回执，其他 revision 或 fence 种类拒绝。
- 已受理的 Coordinator 删除由独立任务完成；RPC 调用方取消等待，不取消清理和原始 fence 的记录。任务持有节点生命周期读锁，删除完成前不能穿过重新对账的互斥边界。
- 节点先清理后端，再发布 Deleted。提交携带待删除的 checkpoint 制品；Coordinator 从上一份权威结果补取当前 checkpoint，在同一次 Redis CAS 中写入本地/未登记制品的最小pending及删除短回执，再移除 Environment；已登记共享制品由原registry继续承接。公开 DELETE 成功表示后端已停止且剩余对象清理责任已保存，对象删除由后台最终完成。后端清理或提交失败仍保留节点串行控制任务；本地降级 journal 同时保存结果和待清理制品，不能只保存去掉 checkpoint 的 Deleted 结果。
- 节点归属已失效、资源已释放且记录不再持有 checkpoint 时，Coordinator 可以原子退役 Failed。仍有 checkpoint（包括已过期）的记录保留到所属节点返回并清理，避免丢失对象存储引用；这可能延长保留期。此时不声称旧节点的物理执行已经停止；旧节点返回后按权威目录清理旧执行，旧 generation 不能再发布 Running。
- 成功后移除 Redis 主目录、Coordinator 调度目录以及在线 adxlet 控制任务。沿用 600 秒 DeletedReceipt，包含结果摘要与规范化 cleanup payload 摘要；不另留永久 cleanup 回执。同名创建获得新 generation，旧清理确认不能删除新一代任务或恢复旧归属。

## Deleted 后的持久对象清理

`CommitEnvironmentRequest.cleanup_artifacts` 只允许出现在 Deleted 提交中，保存节点仍持有的旧制品清理列表。任务包含完整 Assignment、Deleted revision 和制品列表，以 Environment ID 长度前缀、generation、revision 标识；该任务不进入公开 Sandbox 或 Snapshot 目录。

已登记共享制品由原生 registry、Snapshot 引用、execution holds 和 GC 回收，不重复进入 outbox。outbox 仅保存本地或确实未登记制品；Coordinator 每5秒按游标至多尝试16项，派发前复核持久任务及 Environment checkpoint、执行输入、holds、Ready/Deleting Snapshot 引用。正常 Snapshot 发布在同一 header/源原始记录 CAS 内核对来源，避免 producer 退役后的迟到发布。

未登记共享部分由 Coordinator 的 ArtifactReclaimer 物理删除并校验 namespace scope；缺回收器时保留待办，不依赖源节点在线。local 部分要求原节点在线、已对账且 session 匹配；不跨节点猜测本地路径，源节点永久丢失也不以TTL视为清理完成。

`NodeService.CollectCheckpointCleanup` 仅允许 Coordinator 身份，节点检查就绪、进程 session 和来源，仅清理 source 节点的 local 制品；delegated 或共享对象请求明确拒绝，节点缓存释放不能代替共享对象删除。混合任务先完成本地部分，再由 Coordinator 回收共享部分；中途失败保留整个持久任务，缺失对象按幂等合同重试。全部实际完成后，Coordinator 用 epoch、任务 ID 和完整任务内容 CAS 确认。进程崩溃、RPC 断连或确认丢失不清空任务，任务没有 TTL。

完整确认后 HDEL pending payload，不保存永久 cleanup/artifact fence。S3 制品使用不可重用 UUID，副本生成独立 UUID；owner/session/generation、真实来源和发布CAS约束迟到操作。scope 仅校验实际存储目标，例如 alias 重新指向其它 bucket/endpoint/prefix 时拒绝删错对象，不承担永久历史fence职责。不得由外部工具在同一路径写入另一份制品。

旧 journal 在短回执过期或同名换代后遇到 NotFound/Conflict，必须查询 fresh assignment；只有旧归属明确撤销才ack/drop，当前identity或查询失败保留重试。丢弃obsolete条目不代表原提交成功。节点启动恢复同样以完整权威catalog为准，不以历史完整cleanup回执替代归属。

GC 在 mTLS 模式下仅允许 Coordinator 服务身份调用受控删除；普通 API Server DELETE 的用户权限检查保持不变。network 模式遵循既有内部网络信任边界。

## 验证

组件用例使用真实 Redis 和 mTLS，覆盖在线清理、失败重试、同名重建、离线归属退役、迟到 Running 拒绝、失败年龄跨重试与重启保持。adxlet 生命周期用例覆盖 Running／待重启保护、revision fencing 和后端清理失败后重试。生命周期用例还检查被拒绝的回收请求保持完整记录不变，受理后的回收仍记录 `intent=policy`。`reconciliation` 集成用例通过进程内权威服务和生产 gRPC 客户端，覆盖普通删除清理失败后拒绝旧 revision 的 GC、下一轮对账成功及再次删除成功；该用例使用模拟后端，不代表真实 runtime 或集群验证。

```sh
cargo test --locked -p adxlet --test lifecycle --test reconciliation -j 2
python3 build/ci/run.py storage --jobs 2 --output out/ci/failed-gc-storage
python3 build/ci/run.py control-rpc --jobs 2 --output out/ci/failed-gc-rpc
```

上述是组件测试；实际 runtime 的 standalone 验证结果另见 [PR78 回归记录](pr78-runtime-regressions.md)。
