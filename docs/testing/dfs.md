# DFS/OwnerFs 测试与验收

DFS/OwnerFs 在目标仓中的验收按“目标仓源码、目标仓二进制、目标仓环境证据”登记。DMS/AFS 历史证据保留来源版本、环境和判据，可用于追溯和风险判断，但不自动升级为 Agent DX 当前候选结论。

## Linux 验证原则

Rust 构建、Cargo metadata、格式、Clippy、单元测试、FUSE 运行、mount、权限、runc、workspace bind 和性能结论都只在 Linux 上确认。macOS 可用于文档检查和 Python 静态工具核对，不能作为 Rust/文件系统验收通过证据。

开跑前固定并记录：源码 commit、目标二进制 SHA、配置 SHA、测试套件版本、挂载点、容量门禁、数据规模、并发、缓存状态、持久屏障、计时边界、对照系统和适用副本语义。基础自检、工具就绪、功能通过、性能改善和最终达标分别登记。

维护中的验收工具在 [build/e2e/dfs/acceptance/](../../build/e2e/dfs/acceptance/)。过程日志、原始大证据、VM 镜像和历史归档放在源码树外；仓内只保留紧凑索引、命令、版本、校验和和必要夹具。

## 功能优先级

阶段二的当前顺序：

1. OwnerFs workspace bind ON 的必要功能闭环和试用交付；
2. OwnerFs 远端读写，以及 bind ON 与远端 FUSE 访问协同；
3. DFS 一写多读核心场景；
4. 普通 OwnerFs 本地 FUSE 访问及性能优化。

大规模、长时间、复杂可靠性、多 Meta、etcd、Redis 和完整故障矩阵继续后置。pjdfstest、固定 LTP filesystem 子集和短 FSx 用于兜底功能完备性；项目自定义 case 只补充 OwnerFs bind、远端访问、DFS 复制、Meta 恢复和权限语义。

## 性能目标

这些目标是产品演进目标，不是本次 MR 的合入前置：

| 范围 | 目标 |
| --- | --- |
| OwnerFs workspace bind | 选定核心 case 接近 native ext4，吞吐按既定 `>=0.90x` native ext4 判据验收 |
| 普通 OwnerFs 本地读写 | 吞吐 `>=1.2x` 同条件 MooseFS，独立操作时延 `<=0.8x` 同条件 MooseFS；两项都满足才算达标 |
| 普通 OwnerFs 远端读写 | 吞吐 `>=1.2x` 同条件 MooseFS，独立操作时延 `<=0.8x` 同条件 MooseFS；两项都满足才算达标 |
| DFS | 同接口、三份同步持久副本条件下持平 3FS |
| 删除 | 正确性必须通过，并保留性能对照报告；不新增硬比例 |

时延必须先固定操作范围、计时边界和判定分位数，记录 p50/p95/p99。不得成绩出来后选择有利指标，也不得由吞吐反推时延。不具备同条件对照的结果只能作为摸底。

## 当前迁移 MR 工程验收

本次迁移 MR 的工程验收限定为：

1. 默认 OFF 的构建、测试和包清单验证；
2. 显式 ON 的 DFS crates 构建、`cargo check`、Clippy、带组件包清单和部署配置渲染验证；
3. 测试辅助程序仍只由验收流程显式构建，不进入普通试用包；
4. 未完成的 DFS 单测、安装运行、健康检查、正常停止、卸载和小规模 OwnerFs/DFS 运行回归在 MR 中标记为待验收。

未完成项必须保持待验收，不得用工具检查、文档同步或旧候选结果冒充目标仓功能/性能通过。
