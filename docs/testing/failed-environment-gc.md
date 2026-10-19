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

- 所属节点健康时，Coordinator 调用 adxlet 删除接口，携带归属 generation、节点会话和期望 Failed revision。adxlet 在该 Environment 的串行控制任务中复核状态；已恢复为 Running、待重启、修订号或会话不匹配时拒绝 GC。
- Coordinator 删除请求必须恰好包含一种 fence：`failed_revision` 进入 Failed 回收，`expiry_revision` 进入期限删除；API Server 不能携带这些内部 fence。删除回执保存原始 fence 的种类和 revision，不能用迁移后的 Deleted revision 替代；同一归属、会话和原始 fence 的重试返回同一回执，其他 revision 或 fence 种类拒绝。
- 已受理的 Coordinator 删除由独立任务完成；RPC 调用方取消等待，不取消清理和原始 fence 的记录。任务持有节点生命周期读锁，删除完成前不能穿过重新对账的互斥边界。
- 节点先清理后端，再发布 Deleted。提交携带待删除的 checkpoint 制品；Coordinator 从上一份权威结果补取当前 checkpoint，在同一次 Redis CAS 中写入持久清理任务及删除回执，再移除 Environment。公开 DELETE 成功表示后端已停止且清理任务已持久化，对象删除由后台最终完成。后端清理或提交失败仍保留节点串行控制任务；本地降级 journal 同时保存结果和待清理制品，不能只保存去掉 checkpoint 的 Deleted 结果。
- 节点归属已失效、资源已释放且记录不再持有 checkpoint 时，Coordinator 可以原子退役 Failed。仍有 checkpoint（包括已过期）的记录保留到所属节点返回并清理，避免丢失对象存储引用；这可能延长保留期。此时不声称旧节点的物理执行已经停止；旧节点返回后按权威目录清理旧执行，旧 generation 不能再发布 Running。
- 成功后移除 Redis 主目录、Coordinator 调度目录以及在线 adxlet 控制任务。保留 600 秒最小名称回执；有 checkpoint 清理时，另保留任务身份及结果摘要，以确认跨重启的旧 journal 重放。同名创建获得新 generation，旧清理确认不能删除新一代任务或恢复旧归属。

## Deleted 后的持久对象清理

`CommitEnvironmentRequest.cleanup_artifacts` 只允许出现在 Deleted 提交中，保存节点仍持有的旧制品清理列表。任务包含完整 Assignment、Deleted revision 和制品列表，以 Environment ID 长度前缀、generation、revision 标识；该任务不进入公开 Sandbox 或 Snapshot 目录。

Coordinator 每5秒按游标至多尝试16项清理。派发前读取持久任务并复核没有 Environment checkpoint 或 Ready/Deleting Snapshot 引用相同物理制品；新快照发布和 Deleted 退役共享原子 header 栅栏。节点必须在线、已对账、session 匹配。local 或无 scope 的制品仅由原节点处理；全部为具有明确 scope 的共享制品时，可由声明相同实际存储 scope 的节点接手。原节点当前 scope 不匹配时，不阻塞兼容节点接手。包含 local 与共享制品的同一任务仍需可处理全部制品的原节点；不会跨节点猜测本地路径。

`NodeService.CollectCheckpointCleanup` 仅允许 Coordinator 身份，节点再次检查就绪、当前进程 session、来源与 scope，按顺序幂等删除。中途失败保留整个任务；已缺失对象视为删除成功，重试可以从头重复。全部完成后，Coordinator 用 epoch、任务 ID 和完整任务内容 CAS 确认。进程崩溃、RPC 断连或确认丢失不清空任务，任务没有 TTL。

清理完成后保留不可变制品身份索引和删除结果摘要，禁止后来的新引用重新使用已退役对象。正常提交按 `(storage, location)` 摘要索引查询 scope，不扫描历史任务；空 scope 按保守别名规则匹配。上述少量控制元数据随历史清理量增长，不等同于保留 checkpoint 内容；当前没有自动压缩策略。S3 制品使用不可重用 UUID，副本生成独立 UUID；不得由外部工具在同一路径写入另一份制品，迟到的物理 DELETE 无法保护被人为复用的对象地址。

GC 在 mTLS 模式下仅允许 Coordinator 服务身份调用受控删除；普通 API Server DELETE 的用户权限检查保持不变。network 模式遵循既有内部网络信任边界。

## 验证

组件用例使用真实 Redis 和 mTLS，覆盖在线清理、失败重试、同名重建、离线归属退役、迟到 Running 拒绝、失败年龄跨重试与重启保持。adxlet 生命周期用例覆盖 Running／待重启保护、revision fencing 和后端清理失败后重试。

```sh
python3 build/ci/run.py storage --jobs 2 --output out/ci/failed-gc-storage
python3 build/ci/run.py control-rpc --jobs 2 --output out/ci/failed-gc-rpc
```

上述是组件测试；实际 runtime 的 standalone 验证结果另见 [PR78 回归记录](pr78-runtime-regressions.md)。

2026-10-16 Linux 原生复验：删除/回执定向测试5项通过（包含在 adxlet 默认179项通过中），真实 Redis 与 mTLS 的 Failed GC 用例1项显式执行通过。取消等待用例先在 builder 副本回退到 inline await 后 RED，再恢复最终实现 GREEN。格式、workspace all-target/all-feature clippy `-D warnings` 与编译通过。真实 KVM 故障回收及各版本运行边界另见 [E2B 生命周期验证记录](2026-10-15-e2b-checkpoint-cooperation.md)，组件测试本身不能替代该运行证据。
