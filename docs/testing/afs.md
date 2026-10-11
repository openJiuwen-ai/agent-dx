# Agent FS（AFS） 测试与验收

Agent FS（AFS） 在目标仓中的验收按“目标仓源码、目标仓二进制、目标仓环境证据”登记。DMS/AFS 历史证据保留来源版本、环境和判据，可用于追溯和风险判断，但不自动升级为 Agent DX 当前候选结论。

## 平台与入口矩阵

Rust 构建、Cargo metadata、格式、Clippy、单元测试、FUSE 运行、mount、权限、runc、workspace bind 和性能结论都只在 Linux 上确认。macOS 可用于文档检查和 Python 静态工具核对，不能作为 Rust/文件系统验收通过证据。

| 入口 | 当前可执行平台 | 能形成的结论 |
| --- | --- | --- |
| 根构建／组件包 | Linux x86_64 与 Linux ARM64 | 对应架构的编译、清单和安装合同；ARM 默认不运行全部测试 |
| `acceptance/runner.py --list` 与工具单测 | 任意受支持 Python 环境（平台相关项可 skip） | 清单/调度/拒绝路径，不是文件系统运行通过 |
| `acceptance/runner.py` case 执行 | **专用 Linux ARM64 root 环境** | 仅本轮 lock、case、matrix 与 profile 的结果 |
| `scripts/ownerfs/bind-two-node-localfile.py`、`scripts/dfs/three_node_core.py` | 满足脚本 preflight 的 Linux root/FUSE 环境 | 单 VM 小规模定向功能证据，不等于通用 runner 或 Full |
| Full profile | Linux ARM64 runner 且完整 FROZEN lock；当前通用 ENV verifier 未完成 | 当前仍 BLOCKED，不得以 smoke/定向 driver 替代 |

通用 runner 的 ARM64 guard 是其现有探针、rootfs、LTP 和实验环境合同，不代表
AFS 产品二进制只支持 ARM64，也不代表 x86_64 包或定向 Linux driver 已通过 Full。
在逐项验证 runner 依赖前不得删除该 guard。

开跑前固定并记录：源码 commit、目标二进制 SHA、配置 SHA、测试套件版本、挂载点、容量门禁、数据规模、并发、缓存状态、持久屏障、计时边界、对照系统和适用副本语义。基础自检、工具就绪、功能通过、性能改善和最终达标分别登记。

维护中的验收工具在 [build/e2e/afs/acceptance/](../../build/e2e/afs/acceptance/)。过程日志、原始大证据、VM 镜像和历史归档放在源码树外；仓内只保留紧凑索引、命令、版本、校验和和必要夹具。

测试 runner 执行必须显式传入仓外本轮 `--lock`；`--list` 无需环境。仓内 `acceptance.lock.example.json` 不含个人 VM 状态和候选身份，复制后仍不能通过完整发布门禁。旧固定实验室判定器、原始观测及固定 3FS 编排已完整归档仓外；仓内使用参数化探针与合成负例。通用完整 ENV verifier 未实现，full 明确 BLOCKED；smoke 仍须执行对应真实 driver，工具单测不替代功能或性能验收。有效的 mmap 新鲜度探针保留为 `acceptance/probes/mmap_freshness.py`。

## 功能优先级

阶段二的当前顺序：

1. OwnerFs workspace bind ON 的必要功能闭环和试用交付；
2. OwnerFs 远端读写，以及 bind ON 与远端 FUSE 访问协同；
3. DFS 一写多读核心场景；
4. 普通 OwnerFs 本地 FUSE 访问及性能优化。

大规模、长时间、复杂可靠性、多 Meta、etcd、Redis 和完整故障矩阵继续后置。pjdfstest、固定 LTP filesystem 子集和短 FSx 用于兜底功能完备性；项目自定义 case 只补充 OwnerFs bind、远端访问、DFS 复制、Meta 恢复和权限语义。

## 性能目标

这些目标属于后续产品演进，不是当前候选的既有能力：

| 范围 | 目标 |
| --- | --- |
| OwnerFs workspace bind | 选定核心 case 接近 native ext4，吞吐按既定 `>=0.90x` native ext4 判据验收 |
| 普通 OwnerFs 本地读写 | 吞吐 `>=1.2x` 同条件 MooseFS，独立操作时延 `<=0.8x` 同条件 MooseFS；两项都满足才算达标 |
| 普通 OwnerFs 远端读写 | 吞吐 `>=1.2x` 同条件 MooseFS，独立操作时延 `<=0.8x` 同条件 MooseFS；两项都满足才算达标 |
| DFS | 同接口、三份同步持久副本条件下持平 3FS |
| 删除 | 正确性必须通过，并保留性能对照报告；不新增硬比例 |

时延必须先固定操作范围、计时边界和判定分位数，记录 p50/p95/p99。不得成绩出来后选择有利指标，也不得由吞吐反推时延。不具备同条件对照的结果只能作为摸底。

## 当前候选版本工程验收

每个准备交付的 AFS 候选版本至少核对：

1. 默认 OFF 的构建、测试和包清单验证；
2. 显式 ON 的 AFS crates 构建、严格 all-features Clippy、受影响单测和真实带组件包验证；
3. 从本轮 ON artifact 完成安装、配置、启动、健康检查、正常停止和卸载；
4. 用目标仓实际二进制完成 OwnerFs bind ON＋远端双向访问、权限/清位/errno、中心 local-file 正常重启恢复，以及小规模 DFS 一写多读；
5. 验证 direct-I/O mmap 协商及正常卸载/受管引用排空；测试辅助程序只由验收流程显式构建，不进入普通包。

单个出口未完成时如实登记待验收或阻塞。性能、完整 POSIX 和复杂可靠性继续后置；
工具检查、文档同步和旧候选结果不等于当前候选的功能或性能通过。带日期的历史结果
见 [迁移报告](../migration/2026-10-09-afs-snapshot.md)。

## DFS 外部 S3 闭环

此专项先验证 A 写入／同步 → S3 成功 → Meta 发布 → B 回源读取，随后验证
A 正常退出后仍可读取，以及中心 local-file Meta 正常重启恢复。首次采用
cache OFF、writeback OFF、64KiB／64MiB 有限数据。S3 失联时同步必须报错，
不能发布新的文件长度／版本；恢复连接后才能重新确认提交。实际运行通过、
针对性单测、工具就绪和性能比较分别登记，不以编译证明闭环。

测前记录源码快照／二进制 SHA、配置、Meta 后端、S3 版本和持久设置、
Node 的 guest 文件系统、cache 容量、writeback、数据量与服务退出码。
B 独立读取期间用后端 GET／流量计数确认回源，Node 本地 chunk 目录不应
新增耐久副本。权限、错误传播、摘要校验、授权过期和旧 Node 会话拒绝保持
原要求。既有 Node 复制、精确提交重放和恢复测试按受影响范围回归。

JuiceFS 对照后续使用相同 S3 API／持久配置及 cache/writeback 策略。AFS
Meta 目前小规模运行选 local-file，JuiceFS 使用 PostgreSQL；产品默认 etcd
并不代表本轮实测使用 etcd。该差异必须记录，性能阶段分别观测 Meta 提交／
查询成本；发现主要差距时再分析，不能把整体差异归因于 Rust／Go。
不同 FUSE Range 形状或完整性校验成本的结果只能作摸底，不能冒充同路径比较。
本轮不重跑完整矩阵，不形成新的性能达标结论。第三方测试部署留在仓外。

写节点正常退出后读取已发布版本，与写节点新会话重新取得写权是两个出口。
本轮额外尝试在旧租约仍有效时重启 A 并立即复写原文件，实际返回 EBUSY，
保留该失败；不修改租约来换取通过。小规模闭环在同一 A 会话内完成写入，
再验证 A 退出和 Meta 正常重启后的读取。跨写会话恢复写入单列待验收。

当前新增范围仅为 Linux debug 候选的小规模外部 S3 功能验证，包含空文件、
纯稀疏扩展和截断无新对象的提交；未出新正式包，有界 cache ON 未完成。
正式对照仍需核验双方 S3 请求形状和缓存行为，不能继承本次功能通过为性能通过。
