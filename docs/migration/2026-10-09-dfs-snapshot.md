# Agent FS（AFS）快照迁移报告

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
| `examples/*.toml` | `dfs/examples/` 与 `build/config/examples/afs/` | crate 示例和发布包配置示例 |
| Cargo 绑定测试 | `dfs/tests/` | 随 crate 保留 |
| 验收工具 | `build/e2e/dfs/` | 保留维护中的 runner、driver、probe、小规模入口和约 2.8MiB 小型回归 fixture；旧部署器、历史过程证据和大日志留在本地归档 |
| 许可证 | `docs/migration/licenses/dfs-source/` | 保留来源 LICENSE/NOTICE |

未导入：`.github/`、`.codex/`、`.omx/`、历史 `development/`、过程性测试工具快照、原始过程日志（维护回归所需的固定小型 fixture 除外）、压缩包、旧 release 包、VM 镜像、过程计划和历史 checkpoint。

## 当前 MR 边界

默认 ADX 工程路径保持不带 AFS。AFS 相关变更会在默认 pipeline 中触发 Buildkite AFS gate；该 gate 使用 `ADX_WITH_AFS=1` 执行检查，但不把文件系统 artifact 注入默认发布包。只有显式带组件的 release package 才包含 `afs-meta`、`afs-node`、AFS 示例配置和 `with_afs: true` 清单。普通 release package 本轮仍不包含文件系统二进制、配置或运行依赖。

目标仓本轮二进制已有下列限定运行证据；原 DMS/AFS 历史结论仍绑定原版本。本次 MR 的 ON 安装交付出口尚未完成，MR 建立不表示迁移 Goal 已完成。

### AFS 命名与 CLA 对齐

产品统一为 Agent FS（AFS），OwnerFs 和 DistributedFs（DFS）是其中的路径；DMS 仅作来源追溯。当前工程入口统一为 `ADX_WITH_AFS=0|1`、`ADX_AFS_ALL_FEATURES=0|1`、Make `afs-*`、CI suite/component `afs`、包参数 `--with-afs`、清单与部署字段 `with_afs`；发布示例改为 `etc/examples/afs/`。旧环境变量明确报出替换提示，旧 CLI/YAML/manifest 拒绝。内部 DFS 类型和已对齐的源码/测试目录不作全仓重命名。

新分支初始提交 `f633bc7b99bf4548993b5bc6718a2a64d1539276` 的完整树 `3c21c1e5ef2399b9f892acad72290ae81bc3cfe3` 与旧候选 `83640fa` 一致。author、committer 和唯一 signoff 均使用已签署 CLA 的邮箱；[MR !33](https://gitcode.com/openJiuwen/agent-dx/merge_requests/33) 远端 CLA 已通过，替代并关闭 !32。旧分支及证据保留，不重写历史或 force push。命名整改受测输入为仓外 `afs-naming-candidate-v2.patch`（SHA-256 `23599c7db5d883d9b52eaec119a9405a723e8c5c83d167e1bfe8b04deba7afde`），Linux 临时 tree `0d88162d1ed45ea5ab54be7345852dc845d66bc6` 与提交前代码精确匹配；随后仅更新本报告。Linux fmt、部署 config33/process10、Python unittest59 及严格 workspace/all-targets/all-features Clippy 通过。Host 工程 unittest49（48通过、1范围skip）及文档检查通过；旧变量/参数/字段拒绝及包与汇总清单 ON/OFF 模式矛盾的针对性回归包含其中。一次不必要的 pytest 调用因缺模块失败，未安装或修改环境，既有 unittest 入口完成相应测试。下方原命令、版本和结果仍保留原身份。

## 当前验证

Rust 受测输入为目标提交 `3c6e3b47f25317662640ec7b47039f2c1f6c735b`，516 个 Rust/Cargo/build 输入逐文件核验匹配。后续测试辅助及 DFS 驱动调整不改变这些编译输入。迁移分支随后整合 ADX `refactor` 的 `4d828315175313f545638ca9b4613b60ec9fd68c` 公共恢复修复；AFS 产品及依赖输入未改，部署层按下表重新回归，旧二进制运行记录仍绑定原身份。环境为现有 `afs-build` ARM64 Linux VM、guest ext4；完整原始日志、配置、失败记录和私有测试 TLS 材料保留在源码树外的 `rust-distributed-memory-store/local-archive/migration-20261009/linux/`。

| 项目 | 状态 | 本轮范围 |
| --- | --- | --- |
| 来源与依赖 | 通过 | 源 main/受测 head 完整 tree 相同；官方精确 fuser 0.18.0，无 vendor/私有补丁 |
| Rust 工程 | 通过 | Linux fmt；严格 workspace/all-targets/all-features Clippy；AFS 库 615 通过、22 ignored；七个 helper crate 测试；OFF `make build` 和 ON `make dfs-build` |
| 部署适配 | 通过 | 整合 `refactor 4d82831` 后 Linux config 31、process 10 项通过；保留公共持续重试，AFS 失败不自动重启掩盖错误，非 ready 健康状态及非零退出保留 Meta 均通过；新增示例 schema 回归先失败后通过 |
| 测试辅助程序 | 通过（限定范围） | `4b40209` 的两个 support examples 显式构建成功；workspace probe idle TERM/wait0、identity 的 nosuid/nodev 拒绝与 dev/ino 身份核对通过，正常卸载无残留。产品 bin 仅 afs-meta/afs-node；真实包排除探针仍随 M3 验证 |
| 工具回归 | 通过（限定范围） | 迁移工具 Linux 44 项，36 通过、8 范围 skip；hash 绑定 fixture guards 17 项；新增 DFS 身份配置回归通过。不等于功能验收 |
| OwnerFs bind ON＋远端 | 功能通过（限定范围） | 实际 Home 底层 ext4 bind 与远端 FUSE；双向 64KiB/close-to-open、权限/setid、errno、目录持久屏障、local-file 有序全停重启及删除可见；70 检查、7 actual wait0、无 owned 挂载/进程残留 |
| FUSE mmap 与正常排空 | 功能通过（限定范围） | 已编译 AFS libtest 中 covered-root lifecycle 和 file/mmap reference drain 两个真实 kernel case；普通 unmount/FUSE join 成功，无残留 |
| DFS 一写多读 | 功能通过（限定范围） | 同 VM 三个独立 Node/mTLS TCP；A 写、B/C 并发读，三轮 64KiB/fsync/close-to-open、删除可见；55 检查、4 actual wait0；只要求一份持久副本，不是三同步副本或跨主机证明 |
| 统一 ON 包及安装 | 进行中／待验收 | Redis 7.2.5、EROFS 1.8.10 与 Python 打包工具已就绪；原 musl 下载超时记录保留，经用户授权由主机同官方源下载校验、传入现有 VM 后 target 已就绪。旧836候选首次因输出目录已存在而组装失败，保留原记录；随后 fresh output 出包成功仅绑定旧候选。新AFS入口最终 artifact、安装和生命周期尚未通过，公共镜像保持原样 |
| GitCode 交付 | 进行中 | Issue #10、MR !33（CLA yes）替代已关闭的 !32；新命名的 Linux 验证已通过，必要 ON 出包及安装出口未完成，暂不合并 |

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
- `test-helper-20261009T225045-test-helper/test-helper-evidence.tar`：`fb9e29818863a3f3e7ac03053fce10c73c3a0a06f2171c6e5c748afae2c8c115`。
- `release-deps-20261009T223725-release-deps/release-deps-evidence.tar`：`d3d98e30b8d02e26c00d5119f07acfef544a85d79050b074ac18e785af92d9a3`。
- `naming-v2-20261009T2334/naming-v2-evidence.tar`：`5f23bc1f7dc97131ed1aac1be826b853ea8aaca4d9f0f7ee33707810c02970cc`。
- `release-20261009T231226-release/release-evidence.tar`：`6306756955de5baa456288ce81d5ea4aab5db4cdb3890be41afed05671e39978`，保留旧836出包原失败与随后成功，不等于新 AFS 入口安装通过。

首次 DFS 驱动在配置准备时因未绑定变量失败，未启动服务；原失败归档保留。最小修正并以身份配置测试先重现失败再通过后，只补跑 DFS。首次 ON 构建被两个 root-owned 可再生 `.d` 文件权限阻塞；归档内容、stat 和 hash 后按缓存维护授权仅 unlink 这两个文件，一次重试通过。均不隐去原错误，也不重跑无变化的通过项。

缓存维护保留必要 ELF 和源证据，四份重复可再生 target 释放 23,750,498,150 逻辑字节；唯一 target 构建后 VM 可用 34,861,178,880 字节。未删除测试数据或重建环境。

### ADX 公共恢复修复的整合

在原迁移候选 `4b40209` 上正常合并 `refactor 4d82831`，保留提交历史。删除 AFS 新增示例和测试里的旧 `restart_limit` 字段，沿用当前公共配置 schema；部署文档同时说明 ADX 持续重试和 AFS 失败状态保留的差异。AFS 异常退出及排空失败不能自动重启后清除错误，因此只在 AFS 角色上保留失败关闭条件，其他角色使用 ADX 新重试行为。

新增示例 schema 测试先以 unknown-field 失败；既有 AFS 异常退出测试也在直接合并结果上失败。最小适配后，Linux fmt、严格 workspace/all-targets/all-features Clippy 及部署 31+10 项通过。原失败及整合输入保留仓外，不改写原运行结论，不重跑未受影响的 AFS 完整功能矩阵。 整合后的 `adxctl` 重新构建通过，SHA-256 为 `5b4773d0828a411da340dada40d30a4837d4e45201ce26d854efba17a1721009`；此前安装前的功能运行仍使用上表原二进制。

仓外索引：`refactor-integration-4d82831/refactor-red-evidence.tar` 为 `c6cfeb5c2070414a520354a82285cd29c94e3058e0d6c400a449e0bcbb9dbd32`，`refactor-green-evidence.tar` 为 `5e4cad901e7df63516b03c1ea5b1dff78a0736d8b23f5da9000d969cd7cd9e98`。

## 剩余必要出口

- M3：准备本地 builder 的固定发布依赖后，用统一入口产生真实 ON artifact，完成安装、配置、启动、健康检查、正常停止和卸载。
- M5：把最终交付结果追加到同一个 MR，核对远端分支和实际文件树；不自动合并。

M1/M2 与本轮 M4 限定运行已收口；上述必要交付未完成前 Goal 保持进行中。性能、完整 POSIX、复杂可靠性和锁专题仍按 [产品计划](../development/dfs-plan.md) 后置。
