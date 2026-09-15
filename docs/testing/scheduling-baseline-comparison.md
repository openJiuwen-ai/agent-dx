# ADX 与旧调度基线的 Linux 复测

2026-09-14，同一 Linux 容器内顺序执行旧基准和 ADX Release 程序，预热整个矩阵一次，交替执行顺序，正式测量七轮。共 18 组、126 条正式结果；下文取每个指标的七轮中位数。

**持续调度＋资源上报确认闭环：开启复用时 ADX 为 21,180 QPS，旧基准为 14,219 QPS，提升 49%；生命周期 P99 从 373.46 ms 降至 236.24 ms，降低 37%。关闭复用时吞吐基本持平。** 本轮也修复了重试绕过候选缓存的退化，并重新完成全部七轮测量。

这是调度库与进程内消息队列的比较。旧侧使用历史报告对应的基准二进制，未重建当前分支 HEAD；ADX 使用当前未提交工作树。结果不代表完整平台 E2E，也不能直接解释为编程语言的性能差异。

## 版本与运行环境

| 项目 | 固定值 |
|---|---|
| 对照源码的超仓 `feature/distribute_env` | `f3d3d52029be361a895acd775ef939008a37a3dd` |
| 该超仓固定的 FunctionSystem | `7cb717dc27778954773c3745ab7fca4c77912377` |
| 旧报告及测试源码 | 上述子仓的 `docs/domain-scheduler-benchmark-report.md`、`functionsystem/tests/unit/common/schedule_decision/domain_scheduler_current_path_benchmark_test.cpp` |
| 旧执行文件 SHA256 | `8d78ae746a6c05edb3455b87d7634872e36207829d48c5f7f2663ca092f3ac26`，与历史报告记录一致 |
| 旧动态库输入包 | `73d90570-cae6a5f3-ad2095e8-linux-arm64-bazel6-img42d64b1f` |
| ADX 基础提交 | `1e49d86f2123173a8f5358182ca294fab9a9b1e4` ＋当前工作树；源码摘要见 provenance |
| ADX 执行文件 SHA256 | `3d60d33075c68f674a26697cb4e8d6ac37346b522eb3dd577e2653b2fd4e5a82` |
| ADX 编译 | Rust 1.97.1，Linux ARM64，Cargo Release，`--offline --locked -j2` |
| 测量平台 | macOS 主机上的 Docker Linux ARM64 VM，内核 `6.12.76-linuxkit`，glibc 2.34 |
| 容器资源 | 同一容器，CPU affinity `0-3`，内存上限 6 GiB；两程序顺序测量 |
| 运行镜像 | `yr-openeuler22-compile-localbuild:8.5.0-cc`，镜像 SHA256 `42d64b1f1b6c66c5fe51d8a6a37a46d6e838faef89582b623e4b78bf2576eb72` |

旧二进制摘要匹配证明复用了历史执行制品，不证明它由当前分支 HEAD 构建。动态库来自现有缓存输入包，也不声称恢复了历史报告的完整运行环境。因此以本轮旧侧复测值作为分母，不采用原报告的 8,104 QPS 等历史成绩。Docker VM 也不是独占物理测试机。

## 负载与计时边界

共同使用 1,000 个节点、Pack/binpack、同构 CPU/内存请求，每个请求逻辑资源为 CPU 300、内存 128。无设备和亲和约束。静态场景每节点可容纳 1,030 个请求；持续场景每节点 20 个；重试场景每节点 4 个。资源量用于适配计算，不启动实际工作负载。

| 场景 | 驱动方式 | 指标含义 |
|---|---|---|
| `open` | 预热 30 次，再异步提交 1,000 个请求 | 批量完成 QPS；ADX 延迟包含排队等待，旧测试不输出此场景的有效分位数 |
| `closed` | 预热 30 次，1,000 次逐个提交并等分配结果 | 含进程内消息队列的单请求调度延迟；不是纯 Filter/Score 内核时间 |
| `closed500` | 同上，独立线程目标 500 次/秒更新节点 | 调度与周期上报竞争；每次上报等待确认，实际次数记录在 raw JSON |
| `sustained` | 5,000 个在途请求，预热 5,000 次完成，测量 5,000 次完成；每次等待 ADD、DELETE 确认后补交请求，最后排空 | 生命周期从提交计时至两次报告确认；报告延迟为 ADD＋DELETE 合计，不是单条报告 |
| `retry` | 1,000 次初次分配＋拒绝后的重试，无用例内预热 | QPS 统计完整两次调度；P50/P99 只统计重试阶段 |
| `update` | 1,000 次不影响请求适配的节点字段更新，每次等待应用完成 | 更新入口到确认的平均成本；包含消息队列与发布工作 |

旧 `no_aggregate` 关闭聚合，`relaxed` 使用其候选聚合路径、relaxed 参数 32。ADX `cache=0` 关闭候选复用，`cache=32` 最多保存 32 个计算签名。两者的参数含义和扫描策略不同；它们分别是各自实现的关闭/开启模式。

ADX 测试驱动通过 `std::sync::mpsc` 向单线程发送命令，调用真实 `Master::submit/schedule_round/register/retry/release`。旧测试通过原有 Actor/ResourceView 路径运行。**ADD 语义存在实现差异**：旧侧应用实例资源上报并完成镜像账本协调；ADX 已在分配时登记占用，ADD 只确认该分配，DELETE 调用真实释放。闭环结果衡量各自职责下的实现成本，不是同一套资源上报协议的替换实验。

更新用例同样以中性变化触发发布：旧侧修改不参与请求的 noise 资源，ADX 修改 `report_seq` 标签。两者都不改变本轮请求的可行节点，但不能把结果当作相同资源序列化或相同更新算法的成本。

## 正式结果

### 批量调度

| 模式 | 旧 QPS | ADX QPS | ADX / 旧 |
|---|---:|---:|---:|
| 关闭聚合／复用 | 17,545 | 25,280 | 1.44× |
| 开启聚合／复用 | 103,954 | 280,191 | 2.70× |

旧批量测试输出的分位数字段为 0，表示未测，不能用来比较 P99。ADX 开启复用的批量 P99 为 3.275 ms，关闭为 39.236 ms，均包含本轮批量排队时间。

### 单请求调度与周期更新

| 场景／模式 | QPS | P50（µs） | P99（µs） |
|---|---:|---:|---:|
| 无周期更新：旧 no_aggregate | 10,933 | 88.917 | 119.458 |
| 无周期更新：ADX cache=0 | 13,486 | 73.958 | 92.875 |
| 无周期更新：ADX cache=32 | 30,322 | 32.250 | 47.666 |
| 目标 500 更新/秒：旧 no_aggregate | 10,946 | 88.584 | 123.083 |
| 目标 500 更新/秒：ADX cache=0 | 13,586 | 72.959 | 97.208 |
| 目标 500 更新/秒：ADX cache=32 | 29,804 | 32.792 | 48.375 |

旧短队列用例仅提供 no_aggregate 结果，所以 cache=32 行不能描述为“两侧均开启聚合”。这一短测试持续约几十毫秒至百毫秒；ADX 关闭/开启缓存的实际上报次数中位数为 37/17，不替代长期固定速率压力实验。

### 持续调度与资源上报确认闭环

| 模式 | 生命周期 QPS | 生命周期 P50（ms） | 生命周期 P99（ms） | ADD＋DELETE P50（µs） | ADD＋DELETE P99（µs） |
|---|---:|---:|---:|---:|---:|
| 旧 no_aggregate | 13,937 | 357.65 | 364.05 | 53.250 | 151.834 |
| ADX cache=0 | 14,171 | 356.89 | 359.85 | 66.750 | 87.292 |
| 旧 relaxed | 14,219 | 365.50 | 373.46 | 53.500 | 155.459 |
| ADX cache=32 | 21,180 | 234.61 | 236.24 | 41.125 | 67.292 |

关闭复用时 QPS 只高约 1.7%，应视为基本持平；ADX 报告 P50 此时反而更高。开启复用后 QPS 高约 49%、生命周期 P99 低约 37%。七轮开启模式吞吐范围为旧 13,538–14,618、ADX 18,126–21,360 QPS，仍有环境波动。

两侧每轮最终实例目录均为 0；旧侧所有测量内 ADD/DELETE 均成功、无 journal overflow。旧 Actor 的延迟协调预留计数在排空时并非始终为 0，不将“最终实例目录为空”描述为“旧侧所有预留都已经收敛”。ADX 排空后真实分配账本为空。

### 冲突重试与更新成本

| 指标 | 旧 | ADX cache=32 |
|---|---:|---:|
| 初次分配＋冲突重试 QPS | 5,496 | 15,518 |
| 重试 P50 | 87.459 µs | 31.875 µs |
| 重试 P99 | 120.958 µs | 47.000 µs |
| 更新平均耗时 | 38.099 µs | 25.141 µs |
| 更新 P99 | 未输出 | 39.334 µs |

ADX 更新平均耗时按本轮 QPS 的倒数计算；旧侧直接输出总耗时／次数。重试检查所有请求换节点、每个请求只保留一份最终分配，ADX 还检查 generation 递增。

初版 ADX 重试禁用了候选缓存，第一轮完整矩阵出现重试 P99 高于旧侧。现改为共享已排序候选，但在选择时跳过当前请求拒绝过的节点，不从共享缓存删除这些节点。其他请求仍可使用该节点。上述表格全部来自修复后的新二进制与独立最终七轮目录。

## 实现与验证

- [比较程序](../../platform/control-plane/master/examples/compare.rs)：负载、邮箱、报告确认和正确性断言。
- [比较驱动](../../build/ci/compare_schedulers.py)：交替执行、预热、原始日志、指标提取和中位数汇总。
- [Master 重试入口](../../platform/control-plane/master/src/lib.rs) 与 [Domain 排队／候选选择](../../platform/control-plane/master/src/domain.rs)：拒绝后释放精确 assignment 并重新排队。调用方必须确认节点未执行或已清理；不是迁移运行中实例的接口。
- [重试回归测试](../../platform/control-plane/master/tests/retry.rs)：过期 generation、重复拒绝、资源释放、候选缓存复用、请求之间的排除列表隔离。

本轮 `adx-master`、`adx-scheduling`、`adx-core` 定向测试 **51 通过、0 失败、1 个性能用例忽略**；三个 crate 的 `--all-targets -D warnings` Clippy 通过。Linux Release 构建通过，最终比较矩阵全部通过。没有重跑整个 workspace 或完整平台 E2E。

## 复现与证据

证据根目录为仓库内 `out/ci/scheduler-comparison/`，属于本机生成产物。运行容器需要将该目录挂载为 `/evidence`，并提供旧二进制及其动态库；`old.sh` 记录本轮准确入口，`ld-path.txt` 记录依赖搜索路径。构建脚本记录了实际镜像、挂载和工具链参数；复用同名已退出构建容器可用 `docker start -a adx-scheduler-build-20260914`。

```sh
# 在配置好依赖和 /evidence 挂载的同一 Linux 容器中运行两侧。
# --output 必须是新的目录，避免覆盖前次证据。
docker start adx-scheduler-comparison-20260914
python3 build/ci/compare_schedulers.py \
  --container adx-scheduler-comparison-20260914 \
  --output out/ci/scheduler-comparison/reproduction \
  --rounds 7 --warmup 1
```

| 证据 | 路径 |
|---|---|
| 源码、二进制及环境摘要 | [provenance-final.json](../../out/ci/scheduler-comparison/provenance-final.json) |
| 旧二进制解析到的动态库 | [old-linked-libraries.txt](../../out/ci/scheduler-comparison/old-linked-libraries.txt) |
| 正式中位数 | [summary.json](../../out/ci/scheduler-comparison/final-rounds/summary.json) |
| 含预热的原始测量 | [raw.json](../../out/ci/scheduler-comparison/final-rounds/raw.json) |
| 每条准确命令、日志名和日志摘要 | [commands.json](../../out/ci/scheduler-comparison/final-rounds/commands.json) |
| 最终矩阵运行日志 | [final-driver.log](../../out/ci/scheduler-comparison/final-driver.log) |
| 首版重试退化证据 | [首轮 summary.json](../../out/ci/scheduler-comparison/rounds/summary.json) |
| 定向测试 | [adx-regression-final.log](../../out/ci/scheduler-comparison/adx-regression-final.log) |
| 严格 Clippy | [adx-clippy-final.log](../../out/ci/scheduler-comparison/adx-clippy-final.log) |
| 最终 Linux 构建 | [build-adx-retry-fix.log](../../out/ci/scheduler-comparison/build-adx-retry-fix.log) |

## 尚未测到的范围

纯 Filter/Score 放置内核没有旧侧同口径配对测量，不能从 mailbox 延迟中扣算出来。本轮也没有比较 GPU/NPU、亲和混合负载、多 Domain 并行、公平性压力或长时间运行。拓扑分布不纳入本轮目标。

完整平台的 Master RPC、Redis 写入、Node Manager 准入、真实 sandboxd 启动和资源报告仍需在服务链路接通后，通过 Buildkite 创建—执行—删除验收；当前驱动不能替代该流水线。
