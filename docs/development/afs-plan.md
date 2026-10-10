# Agent FS（AFS） 迁移计划

更新日期：2026-10-10。

本页是 Agent DX 仓内当前 Agent FS（AFS） 迁移入口。它承接 DMS/AFS 快照的正式能力说明，但不导入原仓过程资产、历史证据、checkpoint、`.codex`、`.omx` 或旧 GitHub Actions。

## 产品命名与开关

统一产品名称为 Agent FS（AFS），包含 OwnerFs 和 DistributedFs（DFS）。构建开关为 `ADX_WITH_AFS=0|1`（默认 `0`），扩展 lint 开关为 `ADX_AFS_ALL_FEATURES=0|1`；Make 入口为 `afs-*`，CI suite/component 为 `afs`，包参数为 `--with-afs`，清单与部署字段为 `with_afs`。发布配置示例位于 `etc/examples/afs/`。

本次迁移尚未合入，旧开关和参数不保留别名：使用 `ADX_WITH_DFS` 或 `ADX_DFS_ALL_FEATURES` 会报出替换提示，旧 `--with-dfs` 和部署 `with_dfs` 会被拒绝，旧 manifest 不能按新清单验证器误判为 OFF。历史包和测试必须由原版本工具验证；修改旧包清单不能将其升级为新版本。

整体组件目录为 `afs/`，通用测试位于 `build/e2e/afs/`。只有与 OwnerFs 平级的 DistributedFs 模式使用 DFS 命名：例如 `afs/src/node/vfs/ownerfs/` 与 `afs/src/node/vfs/dfs/`，以及 `build/e2e/afs/scripts/ownerfs/` 与 `build/e2e/afs/scripts/dfs/`。内部 DFS 类型、feature、配置和专项 case ID 保留原语义。DMS 仅用于来源追溯。

## 当前目标

把 DMS/AFS 已合入 GitHub main 的源码快照纳入 Agent DX `refactor` 工程体系，形成一个可审查 MR。迁移后的文件系统组件必须满足：

- 源码位于根级 `afs/`，包含 OwnerFs 和 DistributedFs 两条路径。
- 默认 ADX 构建、测试、发布包和部署不包含文件系统二进制或运行依赖。
- `source-gate` 是公共源码质量入口；显式设置 `ADX_WITH_AFS=1` 后，它在同一 fmt/Clippy/tooling gate 内纳入 AFS lint，不改变默认发布包。
- 显式设置 `ADX_WITH_AFS=1` 后，Buildkite 运行 `build-afs` 和 `build-afs-arm64` 可选组件步并在组装、包清单和发布校验中传递 `--with-afs`。
- 默认 `afs-check` 覆盖 OwnerFs 和 DFS 默认 feature；RDMA/all-features lint 需要显式设置 `ADX_AFS_ALL_FEATURES=1`，并以 `libibverbs` 开发文件作为前置。
- 测试辅助程序只由验收流程显式构建，不作为普通产品二进制发布。

显式 ON 在 x86_64 与 ARM64 使用相同开关和包清单合同。ARM64 沿用公共原生 Linux builder、独立缓存、凭据适配和本地组件组装；无外部 backend 的清单与 AFS ON/OFF 分别校验。正式 ON #143 已完成两架构组件测试、编译和组包；其 ARM 安装 smoke 仅检查安装及已安装命令的 help。同一 ARM 最终包另在 Linux VM 完成已安装 `adxctl` 的 bind ON＋远端读写、权限／错误、local-file 有序恢复及正常卸载核心运行。该证据限定同 VM 两 Node，不代替跨 VM 或 ARM Full。具体版本和运行结果见[迁移报告](../migration/2026-10-09-afs-snapshot.md)。

公共 Full #66 已使用 ON #137 的 x86 交付件通过原有11组业务用例。ARM Full 尚不能直接复用该入口：当前交接脚本固定选择 x86 产物及 target，并要求外部 backend；#143 ARM 清单的 backend 为 null。还需在公共流程补架构选择，核验原生 ARM 后端、固定 runtime／Collector 镜像和至少两个 ARM Kubernetes worker。不得用 #143 的 x86 镜像 bundle 代替 ARM Full；这些缺口不阻止独立验证 ARM 包的 bind ON 与远端 FUSE 核心场景。

最新正式 ON [#147](https://buildkite.com/agent-dx/agent-dx/builds/147) 绑定 `a5fef9c`，整轮 passed：双架构公共组件及 AFS 测试／编译、统一组包、安装 smoke、发布索引，以及 x86 验收镜像和 Kubernetes L0 全部通过；两份最终包均 with_afs=true 且不含测试探针。OBS／PyPI 外发关闭，ARM Full 与新包真实文件系统运行未在此轮新增。原 #144／#146 checkpoint 超时及其他失败保持版本和证据；原期限下本轮通过，没有证明间歇性超时根因已消除。fixture 原子发布的确定性回归和现有 checkpoint 测试的失败诊断已纳入，产品语义、期限、持久屏障及第三方源码未变。#143 的真实 bind ON／远端访问与 Full66 继续保留原身份，详见[迁移报告](../migration/2026-10-09-afs-snapshot.md)。

## 支持范围

当前 MR 只证明工程集成和限定运行入口，不把原 DMS/AFS 的历史验收结论自动升级为 Agent DX 目标仓结论。

| 范围 | 当前处理 |
| --- | --- |
| OwnerFs workspace bind mount | 保留实现和配置，默认 OFF；显式配置后用于当前实际试用场景 |
| OwnerFs 远端 FUSE 访问 | 保留代码路径，目标仓二进制须有本轮限定 Linux 运行证据 |
| DistributedFs 一写多读 | 保留核心代码与验收驱动，目标仓回归按受影响范围补测 |
| 中心 Meta `local-file` | 作为当前可重启恢复基线 |
| `memory` Meta | 仅用于一次性演示，重启不保留状态 |
| etcd / Redis Meta | 代码保留，迁移 MR 不以其验收为前置 |
| 跨节点 `fcntl/flock` 和阻塞锁取消 | 后置专题，不作为本次 MR 前置能力 |

必要正确性边界仍保留：读写新鲜度、close-to-open、权限和 suid/sgid 清除、错误传播、持久化屏障、direct-I/O mmap 协商、正常卸载和受管引用排空。

## 架构摘要

AFS 把用户可见的文件语义和文件字节搬运分开。应用通过 FUSE 挂载访问 OwnerFs 或 DFS；`afs-node` 靠近工作负载，负责挂载、缓存、复制和节点间传输；`afs-meta` 负责命名空间、租约、文件版本、布局、放置和副本目录。

- Meta 是命名空间、inode、写租约、文件版本、布局、放置和副本目录的权威。Meta 不代理稳定数据流；底层后端接受状态后才发布提交结果。
- OwnerFs 面向小规模 Agent workspace。Home 节点保存底层真实目录；远端节点通过受控 peer 访问 Home，仍需逐次校验授权、句柄、权限和错误传播。
- DFS 把已提交数据表示为不可变 chunk。普通写入先进入 dirty 状态，sync、同步写标志、close-time flush 或后台策略触发提交，形成新的文件版本。
- 复制发生在文件布局之下。只有收到可验证持久 receipt 后，Meta 才提交新版本和副本目录；验证缓存不能自动算作持久副本。
- 读取先固定一致视图再选择本地副本、远端副本或缓存来源；跨挂载可见性按 close-to-open 处理。

OwnerFs 和 DFS 共用进程、FUSE 和传输基础设施，但挂载、后端状态机、缓存策略和验收结论相互独立。

## 工程入口

复用 ADX 既有公共构建、镜像、发布与部署体系，只增加组件开关和必要适配；本地 builder 缺少既有工具时按现有镜像定义补齐，不把环境准备或通用构建整改混入文件系统迁移。

默认入口保持 ADX 原行为，不携带 Agent FS（AFS） 二进制、配置示例或 FUSE 专属依赖：

```sh
make rust-check
make rust-test
make platform-release
```

显式文件系统入口：

```sh
ADX_WITH_AFS=1 make afs-check
ADX_WITH_AFS=1 make platform-release
python3 build/ci/run.py afs
```

普通 ON 产品构建只编译 `afs-meta` 和 `afs-node`，不编译 `tests/support` 下的 examples/验收探针。需要覆盖 RDMA/all-features 时显式运行 `ADX_AFS_ALL_FEATURES=1 make afs-lint`；环境缺少 `libibverbs` 开发文件时该项失败关闭，不把默认 feature 检查冒充为 RDMA 验收。

默认包不得包含 `bin/afs-meta`、`bin/afs-node` 或 `etc/examples/afs/`。带 AFS 包的 `manifest.json` 必须包含 `with_afs: true`，并记录上述文件的摘要。部署层只负责把已有 AFS TOML 配置交给 `afs-meta`/`afs-node`，以及启动、健康查询和正常停止，不改写 AFS 自有配置 schema。随包 AFS 示例必须显式使用 `local-file` Meta 和 `/opt/adx` 下的持久数据/运行时路径，避免目标仓试用误走 etcd 或 `/tmp` 状态。部署 YAML 必须显式设置 `with_afs: true` 才允许 `afs-meta` 或 `afs-node` 角色；默认 profile 和默认包继续拒绝文件系统角色。`status` 只在 AFS HTTP `/health` 返回 JSON `status=ready` 时标记就绪，HTTP 200 但状态为 `starting/degraded` 仍不是 ready。

本机 VM 配置、租约、历史运行 lock 和旧阶段专用驱动作为仓外本地资产维护；仓内保留核心验收、标准套件、参数化探针与合成负例；旧固定实验室原始观测、紧耦合判定器及固定 3FS 编排完整归档仓外。通用完整 ENV verifier 尚未实现，full 明确 BLOCKED。通用 runner 执行必须显式指定本轮 `--lock`，列出 case 不需环境。后置专题工具和原版本记录不作为当前交付结论。

## 文档与配置

- 源码局部规则：[afs/AGENTS.md](../../afs/AGENTS.md)。
- 架构边界：[docs/architecture/afs.md](../architecture/afs.md)。
- 部署与配置：[docs/deployment/afs.md](../deployment/afs.md)。
- 测试与验收：[docs/testing/afs.md](../testing/afs.md)。
- 示例配置：[build/config/examples/afs/meta.toml](../../build/config/examples/afs/meta.toml)、[build/config/examples/afs/node.toml](../../build/config/examples/afs/node.toml) 和 [build/config/examples/afs/deployment-ownerfs-local.yaml](../../build/config/examples/afs/deployment-ownerfs-local.yaml)。
- 维护中的验收驱动：[build/e2e/afs/acceptance/README.md](../../build/e2e/afs/acceptance/README.md)。
- 迁移来源和许可证：[迁移来源索引](../migration/sources.json) 与 [AFS 快照报告](../migration/2026-10-09-afs-snapshot.md)。

## 本次验收出口

本次 MR 必须同时完成工程集成和限定实际运行，不能将创建 MR 视为完成：

1. 默认 OFF 构建、测试和包边界。
2. 显式 ON 构建、严格 all-features Clippy、受影响单测及真实带组件包。
3. 从本轮 ON artifact 安装、配置、启动、健康检查、正常停止和卸载。
4. 目标仓二进制的 OwnerFs bind ON＋远端双向访问、权限/清位/errno、local-file 有序重启恢复、direct-I/O mmap 和正常卸载/排空，以及小规模 DFS 一写多读。
5. 验收辅助程序只按需构建，不进入普通包；版本、二进制、配置和证据可追溯。

未完成的必要出口继续登记为待验收或阻塞。当前进度统一见 [迁移报告](../migration/2026-10-09-afs-snapshot.md)，不自动继承原仓运行结论。

性能目标、完整 POSIX、复杂可靠性、多 Meta、etcd、Redis 和大规模长时间测试继续后置。性能目标包括：OwnerFs workspace bind 核心 case `>=0.90x` native ext4；普通 OwnerFs 本地和远端读写吞吐 `>=1.2x` 同条件 MooseFS 且独立操作时延 `<=0.8x` 同条件 MooseFS；DFS 在同接口、三份同步持久副本条件下持平 3FS；删除要求正确性和性能对照报告，不新增硬比例。历史 DMS/AFS 证据保留原版本、原环境和原判据；新目标仓候选不自动继承。
