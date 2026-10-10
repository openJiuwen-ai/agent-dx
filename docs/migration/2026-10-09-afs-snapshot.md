# Agent FS（AFS）快照迁移报告

日期：2026-10-09。目标分支基线：`openJiuwen/agent-dx refactor` `11dc43e270ea7d02b5bbb9d1b1777e279060b129`。

## 来源

- 来源仓库：`lelezi257/dms`。
- 合入 main：`1bf02097483539363a158163c682f9b805c15a86`。
- 已验证 PR head：`808ad739c07b0c6e04d8075376faa72737f4dd44`。
- 来源与受测完整 Git tree：`4de2bd77a15cb05e7c7b2c858eba5b8493aefd82`。
- 依赖边界：使用官方固定 `fuser = { version = "=0.18.0" }`，不携带 `third_party/fuser` 或私有补丁。

原仓历史证据和过程归档只以本文中的 `EV-*` 逻辑证据 ID 引用；物理位置保留在源码树外的归档映射中，不进入 Agent DX 源码树。

## 证据 ID

本文只保留稳定逻辑证据 ID、版本、tree、命令范围、结果和 SHA-256。`EV-*` 到物理目录或文件的映射不随 MR 提交，由仓外 `evidence-location-map.json` 保存；移出源码树的失败记录、历史输入和原始日志均可按该映射恢复。

## 路径映射

| 来源 | 目标 | 处理 |
| --- | --- | --- |
| `src/` | `afs/src/` | 产品源码快照 |
| `client/` | `afs/client/` | DFS 客户端 crate |
| `common/*` | `afs/common/*` | 共享错误、日志、指标、协议、追踪和传输 crate |
| `error-codes.toml` | `afs/error-codes.toml` | 保留相对 include 输入 |
| `examples/*.toml` | `afs/examples/` 与 `build/config/examples/afs/` | crate 示例和发布包配置示例 |
| Cargo 绑定测试 | `afs/tests/` | 随 crate 保留 |
| 验收工具 | `build/e2e/afs/` | 保留维护中的 runner、driver、probe、小规模入口和当前回归输入；旧部署器、历史过程证据和大日志留在源码树外证据归档 |
| 许可证 | `docs/migration/licenses/afs-source/` | 保留来源 LICENSE/NOTICE |

未导入：`.github/`、`.codex/`、`.omx/`、历史 `development/`、过程性测试工具快照、原始过程日志（维护回归所需的固定小型 fixture 除外）、压缩包、旧 release 包、VM 镜像、过程计划和历史 checkpoint。

## 当前 MR 边界

默认 ADX 工程路径保持不带 AFS。AFS 相关变更会在默认 pipeline 中触发 Buildkite AFS gate；该 gate 使用 `ADX_WITH_AFS=1` 执行检查，但不把文件系统 artifact 注入默认发布包。只有显式带组件的 release package 才包含 `afs-meta`、`afs-node`、AFS 示例配置和 `with_afs: true` 清单。普通 release package 本轮仍不包含文件系统二进制、配置或运行依赖。

目标仓本轮二进制已有下列限定运行证据；原 DMS/AFS 历史结论仍绑定原版本。本次 MR 的统一 ON 包及安装交付出口已完成；本报告限定到可审查的迁移成果，不表示性能、完整 POSIX 或复杂可靠性达标。

### AFS 命名与 CLA 对齐

产品统一为 Agent FS（AFS），OwnerFs 和 DistributedFs（DFS）是其中的路径；DMS 仅作来源追溯。当前工程入口统一为 `ADX_WITH_AFS=0|1`、`ADX_AFS_ALL_FEATURES=0|1`、Make `afs-*`、CI suite/component `afs`、包参数 `--with-afs`、清单与部署字段 `with_afs`；发布示例改为 `etc/examples/afs/`。旧环境变量明确报出替换提示，旧 CLI/YAML/manifest 拒绝。内部 DFS 类型保持模式语义；整体组件目录已按下一节补正为 AFS。

新分支初始提交 `f633bc7b99bf4548993b5bc6718a2a64d1539276` 的完整树 `3c21c1e5ef2399b9f892acad72290ae81bc3cfe3` 与旧候选 `83640fa` 一致。author、committer 和唯一 signoff 均使用已签署 CLA 的邮箱；[MR !33](https://gitcode.com/openJiuwen/agent-dx/merge_requests/33) 远端 CLA 已通过，替代并关闭 !32。旧分支及证据保留，不重写历史或 force push。命名整改受测输入为仓外 `afs-naming-candidate-v2.patch`（SHA-256 `23599c7db5d883d9b52eaec119a9405a723e8c5c83d167e1bfe8b04deba7afde`），Linux 临时 tree `0d88162d1ed45ea5ab54be7345852dc845d66bc6` 与提交前代码精确匹配；随后仅更新本报告。Linux fmt、部署 config33/process10、Python unittest59 及严格 workspace/all-targets/all-features Clippy 通过。Host 工程 unittest49（48通过、1范围skip）及文档检查通过；旧变量/参数/字段拒绝及包与汇总清单 ON/OFF 模式矛盾的针对性回归包含其中。一次不必要的 pytest 调用因缺模块失败，未安装或修改环境，既有 unittest 入口完成相应测试。下方原命令、版本和结果仍保留原身份。

## 组件目录命名补正（2026-10-10）

AFS 为整体组件，OwnerFs 与 DFS 仅在模式层平级。当前目录统一为 `afs/`、`build/e2e/afs/`，整体架构／部署／测试／计划文档使用 `afs.md`／`afs-plan.md`，来源许可证位于 `licenses/afs-source/`。此前 fd1b377 及其运行包的目录为 `dfs/`，上次工程开关统一未覆盖组件目录；本节补正该遗漏。Cargo workspace、CI 变更触发、脚本源码定位、发布许可证读取和生效链接同步迁移。

`afs/src/node/vfs/ownerfs/` 与 `afs/src/node/vfs/dfs/`、`build/e2e/afs/scripts/ownerfs/` 与 `build/e2e/afs/scripts/dfs/` 保留平级模式名称。模式 feature、协议/配置、数据目录和专项 case ID 不机械改名；原版本的运行/失败结论保留。旧文档链接仍可在原 Git 提交追溯，逐文件旧新映射见 `EV-AFS-LAYOUT`。

本次路径补正的受测 tree `8b61b99bd4b4df4de956742fa9d4f0a3ca811f4f`：Linux locked metadata（8个AFS crate均在afs/、19个默认成员不含AFS）、fmt、`ADX_WITH_AFS=1 make afs-build JOBS=1`、严格 workspace/all-targets/all-features Clippy通过。Linux工程60、runner27、OwnerFs脚本4、DFS脚本1、network fixture20、verbs fixture17单测通过；host工程50（49通过/1范围skip）及文档140/链接638通过。108个Rust文件与185个历史回归输入字节一致；只更新工具定位和活动文档，未改运行逻辑。Linux初次Python调用未带Cargo PATH、fixture直接模块调用缺import path的失败原样保留，以现有环境正确入口有限复验通过，未安装依赖。

`EV-AFS-LAYOUT` 中的 `afs-layout-evidence.tar` SHA-256 `4761bed42dcdb235c7621901f9af38eda973e2c41c6d6f6546d657fac771ee3c`；同一证据 ID 保存旧新路径映射、patch及字节核验。随后仅更新本报告。下方 bfd876 包／运行证据继续绑定原版本；新路径候选未重复完整出包／安装／功能矩阵，不自动继承运行结论。

## 当前验证

Rust 受测输入为目标提交 `3c6e3b47f25317662640ec7b47039f2c1f6c735b`，516 个 Rust/Cargo/build 输入逐文件核验匹配。后续测试辅助及 DFS 驱动调整不改变这些编译输入。迁移分支随后整合 ADX `refactor` 的 `4d828315175313f545638ca9b4613b60ec9fd68c` 公共恢复修复；AFS 产品及依赖输入未改，部署层按下表重新回归，旧二进制运行记录仍绑定原身份。环境为 ARM64 Linux builder 与 ext4 数据盘；完整原始日志、配置、失败记录和私有测试 TLS 材料保留在 `EV-LINUX-20261009`。

| 项目 | 状态 | 本轮范围 |
| --- | --- | --- |
| 来源与依赖 | 通过 | 源 main/受测 head 完整 tree 相同；官方精确 fuser 0.18.0，无 vendor/私有补丁 |
| Rust 工程 | 通过 | Linux fmt；严格 workspace/all-targets/all-features Clippy；AFS 库 615 通过、22 ignored；七个 helper crate 测试；OFF `make build` 和 ON `make dfs-build` |
| 部署适配 | 通过 | 整合 `refactor 4d82831` 后 Linux config 31、process 10 项通过；保留公共持续重试，AFS 失败不自动重启掩盖错误，非 ready 健康状态及非零退出保留 Meta 均通过；新增示例 schema 回归先失败后通过 |
| 测试辅助程序 | 通过（限定范围） | `4b40209` 的两个 support examples 显式构建成功；workspace probe idle TERM/wait0、identity 的 nosuid/nodev 拒绝与 dev/ino 身份核对通过，正常卸载无残留。产品 bin 仅 afs-meta/afs-node；真实包排除探针仍随 M3 验证 |
| 工具回归 | 通过（限定范围） | 迁移工具 Linux 44 项，36 通过、8 范围 skip；hash 绑定 fixture guards 17 项；新增 DFS 身份配置回归通过。不等于功能验收 |
| OwnerFs bind ON＋远端 | 功能通过（限定范围） | 实际 Home 底层 ext4 bind 与远端 FUSE；双向 64KiB/close-to-open、权限/setid、errno、目录持久屏障、local-file 有序全停重启及删除可见；70 检查、7 actual wait0、无 owned 挂载/进程残留 |
| FUSE mmap 与正常排空 | 功能通过（限定范围） | 已编译 AFS libtest 中 covered-root lifecycle 和 file/mmap reference drain 两个真实 kernel case；普通 unmount/FUSE join 成功，无残留 |
| DFS 一写多读 | 功能通过（限定范围） | 同一 Linux 环境内三个独立 Node/mTLS TCP；A 写、B/C 并发读，三轮 64KiB/fsync/close-to-open、删除可见；55 检查、4 actual wait0；只要求一份持久副本，不是三同步副本或跨主机证明 |
| 统一 ON 包及安装 | 通过（限定范围） | Redis 7.2.5、EROFS 1.8.10 与 Python 打包工具已就绪；原 musl 下载超时记录保留，经用户授权由同官方源下载校验、传入 Linux builder 后 target 已就绪。旧836候选首次因输出目录已存在而组装失败，保留原记录；随后 fresh output 出包成功仅绑定旧候选。新AFS入口bfd876统一release已成功，with_afs=true、产品bin/AFS示例存在且probe_count=0；installed adxctl 的 bind ON／远端核心及有序恢复通过，公共镜像保持原样 |
| GitCode 交付 | 进行中 | Issue #10、MR !33（CLA yes）替代已关闭的 !32；新命名的 Linux 验证已通过，新AFS入口统一出包及安装通过；远端最终报告核验后交付，仍不自动合并 |

本轮未声明性能、跨主机、完整 POSIX、三同步副本、崩溃恢复或分布式锁通过。库测试的 22 个 ignored 中仅上述两个 kernel case 另行实际执行，不把其余 ignored 计为通过。

### 二进制及证据身份

| 产物 | SHA-256 |
| --- | --- |
| `afs-meta` | `e958df19a2ed4b2540935311a883f518fc44c835c0031ac9fb8d0d7e69f58b91` |
| `afs-node` | `f3389209869913f3a45fdf8c0f433f35e2ef0c53731ae5cf1f5095e9b5e7eddf` |
| `adxctl` | `f896b9326451cbc293f827d1ad772f91f8820da37f4517917506d363e649508b` |
| kernel case test ELF | `fad9b76c629f0792846967d95492eec10fbd8a0bd973331ca0ca0d0f8c60b929` |

证据索引（逻辑 ID 和 SHA-256）：

- `EV-RUST-VERIFY-3C6E3B4`：`fe44e17acb8f5441d2fb2e7dea151d38713f9721bfba480377a58e76cfda5312`。
- `EV-OWNER-RUNTIME`：`9a205ed320ba1387bf652b960c673ce4537396c89943def9a6b8b686fa7643ec`。
- `EV-DFS-RUNTIME`：`66cf64178116e360fba4db2011aad36cb0388fe0444a8bc72871de94c30e7fed`。
- `EV-KERNEL-RUNTIME`：`4e4e12174d620182408df3184543b182d1432f79fe5b87e110c6b33fea8967e4`。
- `EV-TEST-HELPER`：`fb9e29818863a3f3e7ac03053fce10c73c3a0a06f2171c6e5c748afae2c8c115`。
- `EV-RELEASE-DEPS`：`d3d98e30b8d02e26c00d5119f07acfef544a85d79050b074ac18e785af92d9a3`。
- `EV-NAMING-V2`：`5f23bc1f7dc97131ed1aac1be826b853ea8aaca4d9f0f7ee33707810c02970cc`。
- `EV-RELEASE-OLD-836`：`6306756955de5baa456288ce81d5ea4aab5db4cdb3890be41afed05671e39978`，保留旧836出包原失败与随后成功，不等于新 AFS 入口安装通过。

首次 DFS 驱动在配置准备时因未绑定变量失败，未启动服务；原失败归档保留。最小修正并以身份配置测试先重现失败再通过后，只补跑 DFS。首次 ON 构建被两个 root-owned 可再生 `.d` 文件权限阻塞；归档内容、stat 和 hash 后按缓存维护授权仅 unlink 这两个文件，一次重试通过。均不隐去原错误，也不重跑无变化的通过项。

缓存维护保留必要 ELF 和源证据，四份重复可再生 target 释放 23,750,498,150 逻辑字节；唯一 target 构建后 Linux builder 可用 34,861,178,880 字节。未删除测试数据或重建环境。

### ADX 公共恢复修复的整合

在原迁移候选 `4b40209` 上正常合并 `refactor 4d82831`，保留提交历史。删除 AFS 新增示例和测试里的旧 `restart_limit` 字段，沿用当前公共配置 schema；部署文档同时说明 ADX 持续重试和 AFS 失败状态保留的差异。AFS 异常退出及排空失败不能自动重启后清除错误，因此只在 AFS 角色上保留失败关闭条件，其他角色使用 ADX 新重试行为。

新增示例 schema 测试先以 unknown-field 失败；既有 AFS 异常退出测试也在直接合并结果上失败。最小适配后，Linux fmt、严格 workspace/all-targets/all-features Clippy 及部署 31+10 项通过。原失败及整合输入保留仓外，不改写原运行结论，不重跑未受影响的 AFS 完整功能矩阵。 整合后的 `adxctl` 重新构建通过，SHA-256 为 `5b4773d0828a411da340dada40d30a4837d4e45201ce26d854efba17a1721009`；此前安装前的功能运行仍使用上表原二进制。

`EV-REFACTOR-INTEGRATION` 中 red 证据 SHA-256 为 `c6cfeb5c2070414a520354a82285cd29c94e3058e0d6c400a449e0bcbb9dbd32`，green 证据 SHA-256 为 `5e4cad901e7df63516b03c1ea5b1dff78a0736d8b23f5da9000d969cd7cd9e98`。

## 新 AFS 入口统一发布包

来源 `bfd876bc92cdd98fde63dc3457ae8f4c5e385fea`／tree `1cdc26aa6c6fd274d7adf966ec72116ec70c99a2`，Linux clean checkout。`ADX_WITH_AFS=1 make platform-release JOBS=1` rc0，输出目录此前不存在；实际包目录 201,622,404 字节，规范 tar 流 SHA-256 `af1229eb54c23dacf28d93875bbc75332af3c5c8c68b02582f2a028194c2b677`。manifest SHA-256 `b31c697f4861c31cebb13b01d2c9ec1ead4c69bb0e430b81f3f54261d1686834`，`with_afs: true`，包含 `afs-meta`、`afs-node` 和 `etc/examples/afs/`；两个测试 examples 不入包（probe_count=0）。该包后续实际安装与限定运行已通过，见下节。

| 新包产物 | SHA-256 |
| --- | --- |
| `bin/adxctl` | `81c5d236f037c3c85a0e9e66a6140052dd47a2f90fef1bf862cba7cd35a7769c` |
| `bin/afs-meta` | `9d5d5f8400307b7a1270954db2f338f1b36ceed7a1c0db36a7755ede2c27cf23` |
| `bin/afs-node` | `66b88561da894bf34c56a7baf922c210025e7ac53b8b4bd9b86d30d3f4be1b9f` |

`EV-RELEASE-BFD876` 中 `release-evidence.tar` SHA-256 `00a5db39c631325f9af5376ccde89b4f5433122be4e4db9a359a9d280695690f`，含命令、准入、清单、全部文件 hash 和工作树身份。旧836出包记录保留原版本，不作新包验收证据。

## 实际安装与运行

从上述 bfd876 ON 包调用原 `install.sh`，安装到全新 ext4 数据路径，未修改默认安装根或旧数据。安装后的 `adxctl`／`afs-meta`／`afs-node` SHA 与包内相同。实际 `adxctl validate/render/run/status/stop`，bind OFF provision → bind ON 双 Node → 全停 → 中心 local-file 重启读回 → 删除可见 → 正常停止均通过，82 个检查。两个节点在同一 Linux 环境，以 mTLS TCP 通信；不是跨主机证明。

Home workspace 绑定源为真实 `state/ownerfs/root-776f726b7370616365-e1`，目标是 FUSE 根下一级 `workspace`；目标 fstype=ext4，与源 dev/ino `[64769,1850507]` 一致，远端根为 `afs-ownerfs` FUSE。覆盖双向约64KiB/fsync/close-to-open、权限拒绝、ENOENT、非特权写清除 setid 和 root CAP_FSETID 保留语义。有序重启后 bind／远端读回与删除可见通过。

三个 supervisor `run` 实际 wait0；这不是每个子进程的独立 wait0 记录。独立收尾核对 11 个观察到的 supervisor/服务 PID 均已退出，无本轮挂载残留；仅删除本轮新安装目录／链接，保留数据及证据。

`EV-INSTALL-RUN` 索引：

- `pass-installed-ownerfs.tgz`：`f6b44e50dec6dcf743b85602f102dd047c0d23d8d1b9be143f681fb726d85a87`。
- driver：`ce886a3bb31ca0a1f425cb416fcdef5de3ab72d25a5451bff87d689aebd42311`。
- `fail-preflight-wrong-source-path.tgz`：`d4d2c12709144493a99c0c0f4e13d1ac82eb9b0be5b5c2df3dcf2765defd01c2`，调用参数 source 路径拼写错误，零服务启动。
- `fail-render-driver-assertion.tgz`：`de475bf9ea6db2b39e4020eb5fa0dbed869ec47b552812e93cd1f22e3b40da2f`，误要求 AFS-only render 必须生成 JSON；实际按设计直接传已有 TOML，修正观察器后执行正式运行。原失败在服务启动前，均保留，不修改环境或产品。
- `root-postcheck.json`：完整 observed PID 列表与无残留核对；TLS 私钥只留在该逻辑证据 ID 对应的仓外归档。

## 剩余必要出口

- M3：已关闭；bfd876 统一 ON 出包及该包实际安装、配置、启动、ready、正常停止／卸载通过。
- M5：把最终交付结果追加到同一个 MR，核对远端分支和实际文件树；不自动合并。

M1/M2/M3 与本轮 M4 限定运行已收口；M5 仅剩本报告提交后的远端身份核验。性能、完整 POSIX、复杂可靠性和锁专题仍按 [产品计划](../development/afs-plan.md) 后置。


## 2026-10-10：同步并行发布流程并解决 MR 冲突

在命名补正候选 `ea54b66` 上正常合并目标 `refactor 683b61fe7f30395a85dbb0f12f2592afb83ca89e`，不重写已有提交。目标新增五个提交，七处内容冲突集中在公共 CI／发布脚本及其测试。保留最新双架构并行、ARM 原生 Linux builder、凭据隔离、输出权限、可选外部 backend 和正常失败留证流程，在相同扩展点接入 AFS 默认 OFF／显式 ON。ARM64 增加可选 `build-afs-arm64`，与 x86_64 使用同一个 `ADX_WITH_AFS`、包清单和发布校验合同；模式层 OwnerFs／DFS 及 AFS 产品源码未改。

Linux 精确受测 tree `8d267b893991d8bf08142622fc7e127515f80b9c`，随后仅补本文与 Buildkite README。Python 10 个模块68项、部署 process10项、API create_replay4项、fmt及严格 workspace/all-targets/all-features Clippy 均通过；host58项中57通过、1范围skip，文档及shell语法检查通过。ARM开关／组件接线回归先失败后通过，补验无backend清单与AFS ON/OFF、真实CLI校验、容器适配参数传递及失败证据留存。

本轮未执行远端 Buildkite 双架构完整发布、OBS上传或新候选完整安装／文件系统运行矩阵；历史包和运行结果继续绑定原版本。`EV-REFACTOR-CONFLICTS` 中 `refactor-conflicts-linux-evidence.tar` SHA-256 为 `0fa68f1963f3253242ac9b4419077b7ba3d796f153553cc3e421e63f27c3a525`，保存精确patch/tree、命令、准入、完整输出和退出码。阶段一历史8/8不重开，未引入私有第三方修改。

## 2026-10-10：限定整理 AFS E2E 工具

以 `a0d03508c8e87e61acfce501c3d684ad476a5ded` 为归档锚点，原327文件／4,738,211字节完整归档并逐项校验后，移出15个固定实验室配置、租约、旧PREPARING锁及失效实验入口（83,553字节）。保留维护中的标准测试、OwnerFs bind ON与远端／local-file恢复、DFS一写多读、性能测量工具和锁／RDMA负例；原mmap探针字节不变，改为 `build/e2e/afs/acceptance/probes/mmap_freshness.py`。runner执行必须显式传入本轮 `--lock`；`--list`不需锁，示例PREPARING锁不能产生正式PASS。基线准备不再默认引用实验室源码或固定环境路径，缺少必需输入时在复制或创建输出前拒绝。

此前185个冻结回归输入在 `bfef054` 中映射为144个SHA-256内容对象，并去掉41份重复内容／192,157字节；manifest保留原路径、大小、SHA和来源版本，测试临时恢复并验证全部输入，原网络／RDMA负例不删。manifest SHA-256为 `158b716513d028a498e015c9f9a895569f406c60886e9790172d157a184de7bd`。本轮继续将旧固定实验室输入移出当前树，改为形状、hash 和拒绝语义检查；原始输入与失败记录保留在 `EV-MR-PORTABILITY`，不作为当前环境默认值。当前测试树仍保留维护中的Python工具及测试文件，实际文件数以本轮最终 tree 为准。

Linux aarch64／Python3.12.3精确受测tree `7e05e7128dd1abbf4e667cfc0513e4559863d8c4`：12个模块141项测试全部通过，无skip；覆盖恢复校验／损坏拒绝、网络与RDMA负例、缺锁拒绝、环境准入、挂载隔离、rootfs与bind／DFS核心工具。CLI入口、shell语法、三个基线必需输入缺失拒绝、文档140份／639链接及diff检查通过。最后仅追加本节并复核文档。Rust、Cargo、产品构建入口及第三方无变化，本轮未重跑编译、出包或文件系统运行，不将工具通过计为新产品功能／性能通过。G1历史8/8及已有运行证据保持原版本。

`EV-E2E-CLEANUP` 保存完整原件 `afs-e2e-before.tar`、`before.json`、`retired-map.json` 并支持恢复，旧提交仍保留。Linux原始日志 `afs-e2e-cleanup-evidence.tar` SHA-256为 `f4cb14d4a8bb597c991b4f9e02e88032d86ec9fbbd1e350dc3e035e66b5e0980`，含候选身份、准入、命令、输出和退出码；原失败及过程资产不入目标仓。

## MR 全范围可移植性整理

以 `bfef0544106d50db86962f6120e43a2797009fee` 为锚点核对整个 MR。个人物理路径、VM／网段及固定实验轮次退出当前树；文档改用 `EV-*` 索引，位置映射保存在仓外。旧环境判定器、冻结输入和固定3FS实验原件共157文件／2,591,244字节归档核验；其中156文件移出、环境入口替换并新增针对性测试，E2E树275→120文件，净减2,579,003字节，保留91个Python工具／测试。185原输入已从144内容对象实际恢复并核对大小与SHA，历史版本、失败及全部原校验和保留。

当前ENV入口只校验输入路径、hash和格式，完整资格始终BLOCKED；旧判定器原本也不能授予full。维护中的测量工具必须显式传入本轮候选身份，结果标为caller supplied identity，不能代替二进制观测。MooseFS预期源须为非空`mfs#…`且严格匹配实际挂载；拒绝误标OwnerFs、空源及无效源。宿主路径拒绝、挂载核验、数据与持久屏障检查保留。

Linux精确受测tree `b9ae4c47ca64142839ea5c0649c7138a90187117`：20模块284项，281通过／3项因非root跳过；sudo补跑DFS manyread模块25项全部通过。最终工具tree `a927f2b49c3f41f01dde8f5378dd89e84b135de8`：新增Moose负例先失败后通过，受影响Owner写／DFS模块33项全部通过，覆盖上述3个root-only项。CLI、缺候选身份／缺输入拒绝、shell语法及文档检查通过；最后只追加本文并复核文档。未修改Rust／Cargo／公共构建发布入口／第三方，未新增产品运行、完整POSIX或性能结论；G1历史8/8保持关闭。

`EV-MR-PORTABILITY` 保存原件、路径／SHA映射、恢复核对和原失败。Linux原始日志 `afs-mr-portability-linux-evidence-v2.tar` SHA-256 `c30a439b5eab976e6cabdec5f5c78e75ba1b36320d6c416387d9a3f43a933b14`，含候选tree、准入、命令及退出码；通用full verifier与3FS资格仍按现有计划后置。

## 公共 Buildkite 验证边界

固定提交 `e110e2101904e86494fb266fe77a414831e28c10` 的 [AFS OFF #130](https://buildkite.com/agent-dx/agent-dx/builds/130) 与 [AFS ON #131](https://buildkite.com/agent-dx/agent-dx/builds/131) 均失败。两轮 x86 Platform、Gateway、Execd、Source、adxadmin、Sandbox SDK 通过；AFS gate 失败，组包未执行。ON AFS 库测试609通过、6失败、22忽略：四项因公共镜像缺少 `strace`，两项因测试将 root 上下文误作普通用户。OFF 按条件跳过 AFS 组件构建，仍执行相关变更的 AFS gate。

整改仅涉及公共镜像的必需追踪工具和测试身份；保留生产权限、同步错误传播及持久屏障行为，不删除失败检查。镜像通过既有维护流程构建、推送、回拉验证后才更新固定摘要，新提交另行验收。ARM 在准备 `ADX_SWR_PULL_CONFIG` 时访问被拒，未进入编译，按已接受的范围暂缓；不修改公共凭据策略。上述轮次关闭 OBS／PyPI 最终发布，使用 Buildkite 传递产物，失败记录保持原版本结论。该 CI 缺口不重开阶段一历史8/8，也不改变既有包的限定运行结论。

公共镜像维护 [#132](https://buildkite.com/agent-dx/agent-dx/builds/132) 基于 `21d03e2edf461c55152f9c288d04ca0793f1b12a` 完成构建、推送及按摘要回拉验证，包含工具检查和真实子进程追踪。发布摘要为 `sha256:7cd63f1d779963576ba3ddbf258a01af9fad289fb4f0e848a34dfb75d5139ade`，Dockerfile SHA-256 为 `030508ebf208d31690de2b7100c3278a8e36e7afddd6a51e0f2b0cedf4789559`；result.json 由该构建产物提供。x86 公共配置和流水线引用统一切换到此摘要，ARM引用不变。两项权限测试在 Linux 普通用户与 root 下通过，fmt、严格 workspace/all-targets/all-features Clippy、6项镜像合同和文档检查通过；新固定提交的完整 x86 OFF／ON结果仍待验收，镜像验证不等于文件系统功能或性能通过。
