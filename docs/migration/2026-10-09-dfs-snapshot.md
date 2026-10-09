# DFS/OwnerFs 快照迁移报告

日期：2026-10-09。目标分支基线：`openJiuwen/agent-dx refactor` `11dc43e270ea7d02b5bbb9d1b1777e279060b129`。

## 来源

- 来源仓库：`lelezi257/dms`。
- 合入 main：`1bf02097483539363a158163c682f9b805c15a86`。
- 已验证 PR head：`808ad739c07b0c6e04d8075376faa72737f4dd44`。
- 来源与受测完整 Git tree：`4de2bd77a15cb05e7c7b2c858eba5b8493aefd82`。
- 依赖边界：使用官方固定 `fuser = { version = "=0.18.0" }`，不携带 `third_party/fuser` 或私有补丁。

原仓历史证据和本地归档保留在 Agent Runtime 工作区的 `rust-distributed-memory-store/local-archive/`，不进入 Agent DX 源码树。

## 路径映射

| 来源 | 目标 | 处理 |
| --- | --- | --- |
| `src/` | `dfs/src/` | 产品源码快照 |
| `client/` | `dfs/client/` | DFS 客户端 crate |
| `common/*` | `dfs/common/*` | 共享错误、日志、指标、协议、追踪和传输 crate |
| `error-codes.toml` | `dfs/error-codes.toml` | 保留相对 include 输入 |
| `examples/*.toml` | `dfs/examples/` 与 `build/config/examples/dfs/` | crate 示例和发布包配置示例 |
| Cargo 绑定测试 | `dfs/tests/` | 随 crate 保留 |
| 验收工具 | `build/e2e/dfs/` | 保留维护中的 runner、driver、probe、小规模入口和约 2.8MiB 小型回归 fixture；旧部署器、历史过程证据和大日志留在本地归档 |
| 许可证 | `docs/migration/licenses/dfs-source/` | 保留来源 LICENSE/NOTICE |

未导入：`.github/`、`.codex/`、`.omx/`、历史 `development/`、过程性测试工具快照、原始日志、压缩包、旧 release 包、VM 镜像、过程计划和历史 checkpoint。

## 当前 MR 边界

默认 ADX 工程路径保持不带 DFS。DFS 相关变更会在默认 pipeline 中触发 Buildkite DFS gate；该 gate 使用 `ADX_WITH_DFS=1` 执行检查，但不把文件系统 artifact 注入默认发布包。只有显式带组件的 release package 才包含 `afs-meta`、`afs-node`、DFS 示例配置和 `with_dfs: true` 清单。普通 release package 本轮仍不包含文件系统二进制、配置或运行依赖。

目标仓本轮二进制已有下列限定运行证据；原 DMS/AFS 历史结论仍绑定原版本。本次 MR 的 ON 安装交付出口尚未完成，MR 建立不表示迁移 Goal 已完成。

## 当前验证

Rust 受测输入为目标提交 `3c6e3b47f25317662640ec7b47039f2c1f6c735b`，516 个 Rust/Cargo/build 输入逐文件核验匹配。随后测试辅助提交 `ff1d7cb` 和本轮 DFS 驱动调整不改变这些编译输入。环境为现有 `afs-build` ARM64 Linux VM、guest ext4；完整原始日志、配置、失败记录和私有测试 TLS 材料保留在源码树外的 `rust-distributed-memory-store/local-archive/migration-20261009/linux/`。

| 项目 | 状态 | 本轮范围 |
| --- | --- | --- |
| 来源与依赖 | 通过 | 源 main/受测 head 完整 tree 相同；官方精确 fuser 0.18.0，无 vendor/私有补丁 |
| Rust 工程 | 通过 | Linux fmt；严格 workspace/all-targets/all-features Clippy；AFS 库 615 通过、22 ignored；七个 helper crate 测试；OFF `make build` 和 ON `make dfs-build` |
| 部署适配 | 通过 | config 30、process 9 项测试，受测文件 hash 与本轮产品输入一致；含非 ready 健康状态和非零退出保留 Meta |
| 工具回归 | 通过（限定范围） | 迁移工具 Linux 44 项，36 通过、8 范围 skip；hash 绑定 fixture guards 17 项；新增 DFS 身份配置回归通过。不等于功能验收 |
| OwnerFs bind ON＋远端 | 功能通过（限定范围） | 实际 Home 底层 ext4 bind 与远端 FUSE；双向 64KiB/close-to-open、权限/setid、errno、目录持久屏障、local-file 有序全停重启及删除可见；70 检查、7 actual wait0、无 owned 挂载/进程残留 |
| FUSE mmap 与正常排空 | 功能通过（限定范围） | 已编译 AFS libtest 中 covered-root lifecycle 和 file/mmap reference drain 两个真实 kernel case；普通 unmount/FUSE join 成功，无残留 |
| DFS 一写多读 | 功能通过（限定范围） | 同 VM 三个独立 Node/mTLS TCP；A 写、B/C 并发读，三轮 64KiB/fsync/close-to-open、删除可见；55 检查、4 actual wait0；只要求一份持久副本，不是三同步副本或跨主机证明 |
| 统一 ON 包及安装 | 准备中／待验收 | 本地 builder 缺 ADX 原有 Redis 7.2.5、Rust musl target、EROFS 1.8.10；Python pip/setuptools 已具备。未证明 ADX 官方编译镜像缺少 AFS 依赖；用户已同意限定工具准备，随后执行真实出包、安装和生命周期 |
| GitCode 交付 | 进行中 | Issue #10、MR !32 已创建；必要 ON 安装出口未完成，暂不合并 |

本轮未声明性能、跨主机、完整 POSIX、三同步副本、崩溃恢复或分布式锁通过。库测试的 22 个 ignored 中仅上述两个 kernel case 另行实际执行，不把其余 ignored 计为通过。

### 二进制及证据身份

| 产物 | SHA-256 |
| --- | --- |
| `afs-meta` | `e958df19a2ed4b2540935311a883f518fc44c835c0031ac9fb8d0d7e69f58b91` |
| `afs-node` | `f3389209869913f3a45fdf8c0f433f35e2ef0c53731ae5cf1f5095e9b5e7eddf` |
| `adxctl` | `f896b9326451cbc293f827d1ad772f91f8820da37f4517917506d363e649508b` |
| kernel case test ELF | `fad9b76c629f0792846967d95492eec10fbd8a0bd973331ca0ca0d0f8c60b929` |

仓外证据索引（相对于上述 `linux/`）：

- `rust-verify-3c6e3b4-20261009T2226/linux-verify-logs.after-retry.tar`：`fe44e17acb8f5441d2fb2e7dea151d38713f9721bfba480377a58e76cfda5312`。
- `runtime/run-20261009T143133Z/owner-workroot.tar.gz`：`9a205ed320ba1387bf652b960c673ce4537396c89943def9a6b8b686fa7643ec`。
- `runtime/run-20261009T143133Z/dfs-rerun-20261009T143351Z/dfs-workroot.tar.gz`：`66cf64178116e360fba4db2011aad36cb0388fe0444a8bc72871de94c30e7fed`。
- `kernel-runtime-20261009T2231-kernel-tests/kernel-runtime-20261009T2231-kernel-tests.tar`：`4e4e12174d620182408df3184543b182d1432f79fe5b87e110c6b33fea8967e4`。

首次 DFS 驱动在配置准备时因未绑定变量失败，未启动服务；原失败归档保留。最小修正并以身份配置测试先重现失败再通过后，只补跑 DFS。首次 ON 构建被两个 root-owned 可再生 `.d` 文件权限阻塞；归档内容、stat 和 hash 后按缓存维护授权仅 unlink 这两个文件，一次重试通过。均不隐去原错误，也不重跑无变化的通过项。

缓存维护保留必要 ELF 和源证据，四份重复可再生 target 释放 23,750,498,150 逻辑字节；唯一 target 构建后 VM 可用 34,861,178,880 字节。未删除测试数据或重建环境。

## 剩余必要出口

- M3：准备本地 builder 的固定发布依赖后，用统一入口产生真实 ON artifact，完成安装、配置、启动、健康检查、正常停止和卸载。
- M5：把最终交付结果追加到同一个 MR，核对远端分支和实际文件树；不自动合并。

M1/M2 与本轮 M4 限定运行已收口；上述必要交付未完成前 Goal 保持进行中。性能、完整 POSIX、复杂可靠性和锁专题仍按 [产品计划](../development/dfs-plan.md) 后置。
