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

本迁移报告不声明目标仓二进制已经通过 DMS/AFS 历史运行验收。目标仓后续必须用本仓源码和二进制重新记录必要 Linux 运行证据。

## 当前验证

| 项目 | 状态 | 证据边界 |
| --- | --- | --- |
| 来源快照身份 | 已锁定 | 来源 main `1bf02097483539363a158163c682f9b805c15a86`、受测 PR head `808ad739c07b0c6e04d8075376faa72737f4dd44` 与完整 Git tree `4de2bd77a15cb05e7c7b2c858eba5b8493aefd82` |
| 第三方 FUSE 依赖 | 已解除私有补丁 | 目标树不导入 `third_party/fuser`；`dfs/Cargo.toml` 固定官方 `fuser = "=0.18.0"` |
| 默认 OFF 工程边界 | 已验证 | 默认 workspace、release package 和 deployment 配置拒绝 DFS artifact；Linux Python 契约测试 41 项通过 |
| 显式 ON 编译入口 | 部分通过 | Linux `ADX_WITH_DFS=1 make dfs-check` 已通过 `dfs-build`、`cargo check` 和 `clippy -D warnings`；`dfs-test` 在编译 `afs` lib test 时被资源终止，未登记为通过 |
| 文档一致性 | 已验证 | `python3 build/docs/check.py` 报告 138 份文档、632 个本地链接、27 个 JSON 示例、2 个 SVG，错误 0；`git diff --check` 通过 |

Linux VM 中可再生 Cargo target 缓存已做身份记录并清理到源码树外。保留的历史证据、命令、版本、校验和和二进制身份归档在 Agent Runtime 本地 `rust-distributed-memory-store/local-archive/migration-20261009/linux/`，不进入目标仓。

## 待完成

- M2：在空间稳定的 Linux target 上补跑 `dfs-test`，登记通过或具体失败。
- M3：验证显式 ON 包生成并记录 `with_dfs: true`，并完成显式 ON 安装、启动、健康检查、正常停止和卸载闭环。
- M4：完成目标仓 Linux 小规模运行回归并登记二进制身份。
- M5：按 GitCode 规范提交、推送并创建目标为 `refactor` 的 MR。
