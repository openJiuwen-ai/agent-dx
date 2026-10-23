# Agent FS（AFS） 演进计划

更新日期：2026-10-16。

本页记录 Agent FS（AFS） 尚未完成的能力、验证缺口与演进顺序。当前职责、
部署和测试合同分别由架构、部署与测试文档定义；来源与历史结果保留在
`docs/migration/`，不在本页充当当前能力说明。

## 产品命名与开关

统一产品名称为 Agent FS（AFS），包含 OwnerFs 和 DistributedFs（DFS）。构建开关为 `ADX_WITH_AFS=0|1`（默认 `0`），扩展 lint 开关为 `ADX_AFS_ALL_FEATURES=0|1`；Make 入口为 `afs-*`，CI suite/component 为 `afs`，包参数为 `--with-afs`，清单与部署字段为 `with_afs`。发布配置示例位于 `etc/examples/afs/`。

旧开关和参数不保留别名：使用 `ADX_WITH_DFS` 或 `ADX_DFS_ALL_FEATURES`
会报出替换提示，旧 `--with-dfs` 和部署 `with_dfs` 会被拒绝，旧 manifest
不能按新清单验证器误判为 OFF。历史包和测试必须由原版本工具验证；修改旧包
清单不能将其升级为新版本。

整体组件目录为 `afs/`，通用测试位于 `build/e2e/afs/`。只有与 OwnerFs 平级的 DistributedFs 模式使用 DFS 命名：例如 `afs/src/node/vfs/ownerfs/` 与 `afs/src/node/vfs/dfs/`，以及 `build/e2e/afs/scripts/ownerfs/` 与 `build/e2e/afs/scripts/dfs/`。内部 DFS 类型、feature、配置和专项 case ID 保留原语义。DMS 仅用于来源追溯。

## 当前演进目标

文件系统子系统必须持续满足以下工程合同：

- 源码位于根级 `afs/`，包含 OwnerFs 和 DistributedFs 两条路径。
- 默认 ADX 构建、测试、发布包和部署不包含文件系统二进制或运行依赖。
- `source-gate` 是公共源码质量入口；显式设置 `ADX_WITH_AFS=1` 后，它在同一 fmt/Clippy/tooling gate 内纳入 AFS lint，不改变默认发布包。
- 显式设置 `ADX_WITH_AFS=1` 后，Buildkite 运行 `build-afs` 和 `build-afs-arm64` 可选组件步并在组装、包清单和发布校验中传递 `--with-afs`。
- 默认 `afs-check` 覆盖 OwnerFs 和 DFS 默认 feature；RDMA/all-features lint 需要显式设置 `ADX_AFS_ALL_FEATURES=1`，并以 `libibverbs` 开发文件作为前置。
- 测试辅助程序只由验收流程显式构建，不作为普通产品二进制发布。

显式 ON 在 x86_64 与 ARM64 使用相同开关和包清单合同。当前通用 acceptance
runner 的 case 执行仍限定专用 Linux ARM64 root 环境；这来自探针、rootfs、LTP
与实验环境约束，不是产品架构限制。两种架构的包验证与定向 Linux FUSE 驱动
应分别登记，不能互相替代。

## 支持范围

当前支持范围以本仓候选的工程检查和限定运行证据为准；原 DMS/AFS 的历史验收
结论不能自动升级为 Agent DX 当前结论。

| 范围 | 当前处理 |
| --- | --- |
| OwnerFs workspace bind mount | 保留实现和配置，默认 OFF；显式配置后用于当前实际试用场景 |
| OwnerFs 远端 FUSE 访问 | 保留代码路径，目标仓二进制须有本轮限定 Linux 运行证据 |
| DistributedFs 一写多读 | 原生Node数据路径保留；单副本、不配置外部存储的一写一读已形成限定Linux闭环。原生新写默认CRC及写入代次GC保护，多副本写、跨节点读与正常修复有1GiB限定Linux验收；有界视图/授权保持，显式BLAKE3可选磁盘Peer缓存与正常local-file恢复按下方边界验证；多读者规模及性能另验 |
| DFS 外部 S3 发布／回源 | 实验接入：外部配置固定单本地副本；4MiB预上传／四路PUT、64MiB有序发布与预算内跨窗口生产、不可变载荷真实计费、异步cache／抗扫描准入、close-to-open视图及最长5秒授权复用、同对象Ready流水线与完整BLAKE3已实现。成功fsync等所有已接纳序号的对象与Meta持久确认，普通write可先返回已接纳；功能按下方交付边界及测试合同验证；性能、崩溃恢复、外部缓存P2P及引用感知GC后置 |
| 中心 Meta `local-file` | 作为当前可重启恢复基线 |
| `memory` Meta | 仅用于一次性演示，重启不保留状态 |
| etcd / Redis Meta | 代码保留，当前候选不宣称已验收 |
| 跨节点 `fcntl/flock` 和阻塞锁取消 | 后置专题，不属于当前支持范围 |
| 按目标 Node ID pin 服务端证书 | 后置安全专题；当前客户端验证 CA/server_name，服务端绑定调用方叶证书与 Node ID |

必要正确性边界仍保留：读写新鲜度、close-to-open、权限和 suid/sgid 清除、错误传播、持久化屏障、direct-I/O mmap 协商、正常卸载和受管引用排空。


## DFS 原生 Peer 阶段边界

原生新写默认独立ID＋分段CRC；一次写入代次保护替代逐chunk登记，最终发布只核对相关引用。多副本CRC、精确范围远端读及正常修复的当前验收合同见[CRC定向检查](../testing/afs.md#原生crc与写入代次gc定向检查)。下面的完整根、Bao及可选Peer磁盘缓存专指显式BLAKE3对象；CRC不进入这些可选缓存。

不配置外部存储时，DFS通过Node数据副本提供内容；单副本场景的B端真实读取A端数据，
中心Meta不代理正文。成功fsync保留本地耐久副本与Meta持久发布屏障；单副本不承诺失去A后仍可读。

- 每次新open取得当前已发布视图，已打开句柄采用最长5秒有界读上下文；Node/session、epoch、范围授权及最终交付仍独立检查。已知失效立即停止，未知失效不超过原截止期限，Meta断链或重启不续期已有授权。
- A端复用当前已完整校验的不可变对象并共享正文帧，最后引用释放才退账；私有结果在完整校验与最终授权之前不公开。
- `[dfs_peer_cache]`缺省OFF，开启时只保存可丢弃磁盘副本，容量可配，不成为耐久副本或新权限来源；与外部后端组合暂不支持。初次获取核对完整Meta根，稳定命中完整验证当前请求覆盖的1MiB段。
- 同对象填充在原预算内复用两个256KiB载荷块，按块接收、完整根／段校验、私有写盘及交付；缓存I/O失败继续同一次RPC读取，错误与正常排空不省略。不新增正文RAM驻留缓存、下一对象预取或线程池。

限定验证包括64KiB覆盖／重开、64MiB内容与实际P2P路由、权限、cache损坏回源、
local-file Meta正常重启和正常退出。Linux工程检查、功能、工作量减少、性能改善及最终交付
分别登记；首次读持平、稳定E2E提速、复读性能不退化和完整POSIX／复杂可靠性仍需独立验收。
固定块减少内部交接不代表复制量下降或性能目标完成，原始比较数据不进入产品仓。

当前buffer管理已限定实现原生DFS本地写正文的独占builder、消费冻结、有效范围共享与有空闲基线的复用池；
Linux Node使用官方jemalloc。同handle FIFO与同inode互斥串行保持，不承诺跨handle全inode到达顺序FIFO。
真实FUSE顺序写／小覆盖、最后引用退账及GNU Linux ARM峰值回落已有有限运行证据，历史版本与当前回归分别登记。
读／解码缓冲未扩展为全局统一池，正文内容缓存和授权不变，S3预算独立保持。

原生Peer双块路径已将完整根校验移至既有producer，consumer保留完整段校验及私有落盘；
真实EOF和成功join后才确认可信根／Completion／输出绑定。默认关闭的有界粗诊断用于核对阶段与重叠，
不替代正式性能轮。64MiB有限前后对照完成，首次读持平、复读与更大规模最终性能仍未关闭。
上述实现及定向回归不等于完整POSIX、崩溃／HA、RDMA硬件或统一安装包验收。
配置与针对性运行要求见[部署](../deployment/afs.md#原生写-buffer-与-node-allocator)和
[测试](../testing/afs.md#原生写-buffer-验证)；磁盘缓存原合同保持。

## DFS 外部存储交付边界

本阶段对接管理员已部署的 S3 服务，包括 SeaweedFS 的 S3 endpoint。
产品仓只提供连接配置、AFS 适配代码及必要回归，不管理外部服务、数据库或测试集群。

| 范围 | 支持边界 |
| --- | --- |
| 文件系统策略 | 仅新文件系统启用；固定本地单副本，外部对象回执承担数据持久屏障；禁止与 Node 复制混用 |
| 写入与发布 | 普通 write 可先返回已接纳；有界预上传、跨窗口生产及有序发布；成功 fsync 等待所需 PUT 回执和 Meta 持久确认 |
| 独立读取 | 不依赖写 Node 在线；open 获取已发布视图，完整 GET／BLAKE3、范围权限及最终授权检查 |
| 内容与授权复用 | 内容 cache 可关闭且不算耐久副本；授权最多复用5秒，已知失效停止，未知失效受原截止约束 |
| 资源生命周期 | 载荷、发布描述及缓存分别限额，最后持有者释放才退账；停机关闭入口并排空／join |
| 恢复 | local-file Meta 的正常停止／重启后恢复已发布引用；不承诺未同步写入崩溃恢复或新会话立即接管旧写租约 |
| 后置专题 | 引用感知对象GC、外部缓存P2P协同、复杂故障／大规模长时间验收，以及独立性能评估 |

各候选仍须按[测试与验收](../testing/afs.md#dfs-外部-s3-闭环)登记实际Linux验证，
不自动继承历史构建或运行结论。本次接入不承诺性能优势，实验数据和诊断失败另行跟踪。
连接配置见[部署文档](../deployment/afs.md#dfs-外部-s3-连接)。

## 架构摘要

AFS 把用户可见的文件语义和文件字节搬运分开。应用通过 FUSE 挂载访问 OwnerFs 或 DFS；`afs-node` 靠近工作负载，负责挂载、缓存、复制和节点间传输；`afs-meta` 负责命名空间、租约、文件版本、布局、放置和副本目录。

- Meta 是命名空间、inode、写租约、文件版本、布局、放置和副本目录的权威。Meta 不代理稳定数据流；底层后端接受状态后才发布提交结果。
- OwnerFs 面向小规模 Agent workspace。Home 节点保存底层真实目录；远端节点通过受控 peer 访问 Home，仍需逐次校验授权、句柄、权限和错误传播。
- DFS 把已提交数据表示为不可变 chunk。普通写入先进入 dirty 状态，sync、同步写标志、close-time flush 或后台策略触发提交，形成新的文件版本。
- 复制发生在文件布局之下。只有收到可验证持久 receipt 后，Meta 才提交新版本和副本目录；验证缓存不能自动算作持久副本。
- 读取先固定一致视图再选择本地副本、远端副本或缓存来源；跨挂载可见性按 close-to-open 处理。

OwnerFs 和 DFS 共用进程、FUSE 和传输基础设施，但挂载、后端状态机、缓存策略和验收结论相互独立。

## 工程入口

AFS 复用 ADX 既有公共构建、镜像、发布与部署体系，只增加显式组件开关和必要适配。

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

本机 VM 配置、租约、运行 lock 和大体积证据作为仓外本地资产维护；仓内保留核心
验收、标准套件、参数化探针与合成负例。通用完整 ENV verifier 尚未实现，full
明确 BLOCKED。通用 runner 执行必须显式指定本轮 `--lock`，列出 case 不需环境。

## 文档与配置

- 源码局部规则：[afs/AGENTS.md](../../afs/AGENTS.md)。
- 架构边界：[docs/architecture/afs.md](../architecture/afs.md)。
- 部署与配置：[docs/deployment/afs.md](../deployment/afs.md)。
- 测试与验收：[docs/testing/afs.md](../testing/afs.md)。
- 示例配置：[build/config/examples/afs/meta.toml](../../build/config/examples/afs/meta.toml)、[build/config/examples/afs/node.toml](../../build/config/examples/afs/node.toml) 和 [build/config/examples/afs/deployment-ownerfs-local.yaml](../../build/config/examples/afs/deployment-ownerfs-local.yaml)。
- 维护中的验收驱动：[build/e2e/afs/acceptance/README.md](../../build/e2e/afs/acceptance/README.md)。
- 迁移来源和许可证：[迁移来源索引](../migration/sources.json) 与 [AFS 快照报告](../migration/2026-10-09-afs-snapshot.md)。

## 候选版本验收出口

每个准备交付的 AFS 候选版本必须同时完成工程检查和适用的限定实际运行：

1. 默认 OFF 构建、测试和包边界。
2. 显式 ON 构建、严格 all-features Clippy、受影响单测及真实带组件包。
3. 从本轮 ON artifact 安装、配置、启动、健康检查、正常停止和卸载。
4. 目标仓二进制的 OwnerFs bind ON＋远端双向访问、权限/清位/errno、local-file 有序重启恢复、direct-I/O mmap 和正常卸载/排空，以及小规模 DFS 一写多读。
5. 验收辅助程序只按需构建，不进入普通包；版本、二进制、配置和证据可追溯。

未完成的必要出口继续登记为待验收或阻塞，不能因代码合入而自动放行。

性能目标、完整 POSIX、复杂可靠性、多 Meta、etcd、Redis 和大规模长时间测试继续后置。性能目标包括：OwnerFs workspace bind 核心 case `>=0.90x` native ext4；普通 OwnerFs 本地和远端读写吞吐 `>=1.2x` 同条件 MooseFS 且独立操作时延 `<=0.8x` 同条件 MooseFS；DFS 在同接口、三份同步持久副本条件下持平 3FS；删除要求正确性和性能对照报告，不新增硬比例。历史 DMS/AFS 证据保留原版本、原环境和原判据；新目标仓候选不自动继承。

## 历史与来源

来源路径、许可证和带日期的旧构建／运行证据统一保存在
[迁移来源索引](../migration/sources.json) 与
[AFS 快照报告](../migration/2026-10-09-afs-snapshot.md)。这些记录用于追溯，
不改变本页的当前支持范围，也不自动证明新候选通过。
