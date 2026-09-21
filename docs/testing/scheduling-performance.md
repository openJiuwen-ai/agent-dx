# 调度优化迁移与验证

本轮以 `feature/distribute_env` 超仓提交 `f3d3d52029be361a895acd775ef939008a37a3dd` 固定的 FunctionSystem 子仓提交 `7cb717dc27778954773c3745ab7fca4c77912377` 为对照，迁移调度热路径优化。Global 只轮转选择 Shard；Shard 与 Master 同进程，负责实际调度；Node Manager 保留本机最终准入。

拓扑分布不作为本轮扩展或性能验收目标。现有节点/实例亲和、反亲和、GPU/NPU 整卡规则参与回归，纯标量快路径不会绕过这些规则。

## 源码映射

| 原分支优化 | ADX 实现与边界 |
|---|---|
| `schedule_snapshot.*` 的不可变单元、增量发布及查询索引 | [snapshot.rs](../../platform/crates/scheduling/src/snapshot.rs)：`im::OrdMap/OrdSet` 共享树节点；节点、实例、租户、标签、反亲和索引只更新受影响路径。旧版本可继续只读。节点 ID 查找不再线性扫描 |
| 查询上下文复用 | [query.rs](../../platform/crates/scheduling/src/query.rs)：每请求准备一次匹配结果，多个候选和 Filter/Score 共用；按租户/精确标签中最小集合筛选，NotIn/DoesNotExist 等表达式仍按原语义复核 |
| `schedule_queue_actor.cpp` 的 256 请求 / 10 ms 有界轮次 | [master/lib.rs](../../platform/master/src/lib.rs)：`schedule_round` 在请求之间检查预算；固定基准快照根，轮内每次预留增量进入后续请求视图。到界返回给调用者处理心跳、更新等事件 |
| `RequestMutationJournal` 与预分配增量对账 | [journal.rs](../../platform/master/src/journal.rs)：按序号直接读取新增区间，合并重复节点；缓存刷新该节点的当前可分配量，避免重复扣减；游标过旧时重建候选 |
| `SchedulingInputSignature` / `SupportsSemanticAggregation` | [shard.rs](../../platform/master/src/shard.rs)：按 CPU、内存、镜像和 runtime 聚合计算。仅框架内置 profile、默认空策略、零磁盘请求且无既有反向硬反亲和时启用。自定义插件即使用内置同名也不启用 |
| `SelectFeasible` 候选复用 | 排序候选集按上述签名复用；资源预留、释放和状态变化后，仅重新评估变化节点并更新排序。Pack/Spread 与未启用复用时逐次结果一致；没有重复使用旧分数 |
| 发布完成后再唤醒等待调度 | Master 先更新快照及变更序列，再合并唤醒事件。包括只有维护状态变化的上报；停滞队列等待新事件，不自行忙循环 |

资源账本不再在每次分配时完整克隆：Shard 先预留标量，再预留卡，卡预留失败撤回本次标量预留。设备忙闲索引随分配/释放更新，不遍历全部实例重建。Node Manager 的最终本机复核不变。

## 请求顺序与失败契约

聚合的是调度计算。每个请求仍独立排队、独立分配 ID 和 generation、独立占用资源。租户和优先级仅在已通过资格检查的内置纯标量 profile 下不进入计算签名，仍完整保留在请求和队列中。租户间轮转、租户内高优先级优先、同优先级 FIFO 不改变。

每轮只取有限数量的请求；暂时无法适配的请求进入本次扫描的 deferred 队列，保留原顺序票据，不反复挡住后面的可执行请求。预算中断后继续未扫描部分，完成扫描后等待资源变化或下一次有效调度机会。

`RoundOutcome` 同时包含 `assignments` 和 `error`。后续请求的插件失败不撤销前面的成功预留，也不丢弃失败请求。调用者必须处理已返回分配，再按错误策略重试。`schedule(domain)` 保留单个结果接口；大量工作应使用 `schedule_round(domain)`。

典型事件循环：取 `take_ready_shard()`，调用 `schedule_round`，派发其已完成分配，再处理错误。只有资源变化、请求到达或预算用尽且还有工作时继续唤醒。插件错误交给调用者退避处理。一个插件调用和一个请求的候选扫描不会被强行抢占，因此 10 ms 是协作预算，不是实时硬截止。

`MutationJournal` 是进程内缓存变更日志，不是 Redis/SQLite 持久化日志。Master 直接拥有内存账本及预留增量，不再搬入旧实现的异步镜像账本/确认协议；恢复持久化状态后才能开放调度仍是服务接线要求。

## 配置

通过 `Master::with_config` 或 `Master::with_framework_config` 传入 `SchedulerConfig`：

| 字段 | 默认值 | 含义 |
|---|---:|---|
| `max_attempts` | 256 | 每轮最多尝试请求数，包括未适配请求 |
| `max_duration` | 10 ms | 请求之间检查的时间预算 |
| `candidate_cache_entries` | 32 | 每 Shard 最多保存的计算签名；超限按进入顺序淘汰，0 关闭复用 |
| `mutation_history` | 65,536 | 有界变更日志条数；溢出后消费者重建 |

请求数、时间和日志容量须大于零。这些调优项当前只通过 Rust 配置接口设置；Master 服务 JSON 暴露 `scheduler_shards` 与 `placement`，未暴露上述 SchedulerConfig 调优字段。

## 自动验证入口

```sh
cargo test --locked -p adx-master -p adx-scheduling -p adx-core -p adx-protocol -p adx-node-manager -j2
cargo clippy --locked -p adx-master -p adx-scheduling -p adx-core -p adx-protocol -p adx-node-manager --all-targets -j2 -- -D warnings
make scheduler-bench JOBS=2
```

新增普通测试包括快照隔离和共享、索引与全量扫描一致性、预算中断后的队列进度、缓存淘汰/签名区分、Spread 重评分、状态发布先于唤醒、日志游标溢出、批内反亲和、跨域反向反亲和、插件错误的部分成功及租户顺序。普通 `rust-test` 自动运行它们。性能基准使用 ignored test 显式运行，不给普通 CI 设置容易抖动的耗时断言。

TDD 红灯日志 `/tmp/adx-scheduler-optimization-red.log`：先确认缺少 `SchedulerConfig` 等实现接口。

## 本地性能证据（ADX 内部对照）

测试场景：128 个节点、4,096 次请求，最多 256 个未释放实例；窗口满后先释放一个再提交。调度调用本身逐个同步执行，因此这不是 256 个并发调度请求。混合场景使用 4 个租户、3 档 CPU 请求，每 16 次请求更新一个节点容量。每个模式预热一次、交替运行开启/关闭缓存的版本，共 7 轮取中位数。每轮检查全部放置结果完全相同，并验证候选评估次数至少减少 75%。

2026-09-14 当次工作树、macOS arm64、Rust release 历史结果（不是当前 HEAD 重测）：

| 场景 | 关闭候选复用 | 开启候选复用 | 比值 | 候选评估次数：关闭 → 开启 |
|---|---:|---:|---:|---:|
| 同类请求 | 38.231 ms | 22.188 ms | 1.72× | 524,288 → 8,063 |
| 混合请求＋节点容量更新 | 40.331 ms | 27.358 ms | 1.47× | 524,288 → 15,921 |

日志 `/tmp/adx-scheduler-optimization-benchmark-v2.log`。对照是同一 ADX 实现关闭候选复用，**不是**旧 C++ 分支的性能比较；双方都使用新快照和索引，因此这些数字也不单独衡量快照增量化的收益。

首次基准发现增量读取仍扫描全部历史，混合场景出现约 18% 回退。已改为按游标直接读取新增区间；之后去掉仅影响排队的租户/优先级签名字段，并用顺序和结果一致性测试约束这项优化。失败和中间证据单独保留在 `/tmp/adx-scheduler-optimization-benchmark.log` 与 `/tmp/adx-scheduler-optimization-benchmark-final.log`。

以上属于本地调度库功能/性能验证。没有启动完整平台或真实 sandboxd，不构成 Buildkite 端到端验收，也不替代生产规模和长稳压测。

最终验证：此前 workspace 全特性 250 项通过；收尾 FIFO 修复后 5 个相关 crate 的 87 项定向测试与严格 Clippy 通过，benchmark 作为独立 release 基准执行。日志 `/tmp/adx-scheduler-optimization-workspace-v2.log`、`/tmp/adx-scheduler-optimization-final-tests.log`、`/tmp/adx-scheduler-optimization-final-check.log`。


## 与 feature/distribute_env 的性能基线对比

已完成同一 Linux ARM64 容器内的历史基准二进制与当前 ADX 工作树复测。1,000 节点、Pack、七轮中位数：开启聚合／候选复用的持续调度与上报确认闭环，该次旧侧 14,219 QPS、ADX 21,180 QPS；生命周期 P99 为 373.46 → 236.24 ms。关闭复用时 13,937 → 14,171 QPS，基本持平。

完整模式对照、重试／更新成本、制品摘要、运行命令及日志见 [Linux 基线比较报告](scheduling-baseline-comparison.md)。旧侧执行文件与分支内历史报告的 SHA256 一致，但没有重新编译当前分支 HEAD。两侧上报应用职责不同，测试属于进程内调度与消息队列闭环，没有覆盖完整平台服务。

为完成冲突重试比较，新增 `Master::retry`：节点明确拒绝执行或确认清理后，释放精确代次的旧分配并重新排队；请求保留失败节点排除列表。重试沿用共享候选排序，仅在选择时跳过当前请求拒绝的节点，避免全量扫描退化，也不影响其他请求使用同一节点。定向测试覆盖过期代次和资源回收。

后续 2026-09-16 的源码复测见 [基线复测](2026-09-16-scheduling-recheck.md) 与 [条件组接线后复测](2026-09-16-placement-groups-recheck.md)；这些历史微基准不代表完整当前服务性能。
