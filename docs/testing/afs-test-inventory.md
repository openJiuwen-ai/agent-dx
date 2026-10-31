# AFS 静态用例盘点附件

下文为2026-10-19静态盘点快照。2026-10-31第二/三步已实现并验证，当前入口、namespace拓扑和结果以[统一规范](test-strategy.md#step-2)及[runner说明](../../afs/tests/README.md)为准；不把旧静态计数当当前执行数量。

当前树已删除或改名的源码链接固定到该历史基线；这些条目不表示当前仍支持对应旧后端。

日期：2026-10-19。静态源码基线：`1e9ae5f235d412b06fa4195bdbe26860bd7cd840`。
扫描前工作树干净；本次没有运行产品/工具测试、连接集群或触发 Buildkite。
**现有公共设计、调用链与AFS归属统一维护在 [ADX 测试体系现状总表](test-strategy.md)。**
本附件保留源码事实、旧case逐项对照和文件索引，不另维护分层/环境/CI规范，避免与总表漂移。

标记：**事实**来自该基线的源码/配置；**推断**为按依赖和断言作出的覆盖判断；
**建议**为后续补充和接入顺序；没有本轮运行证据的项目一律**待验证**。
表中的“已有覆盖”指源码已有对应断言，不表示当前候选已经在 CI 运行通过。

## 1. 核心结论与统计口径

- **事实**：静态扫描发现 AFS Rust 83 文件、1,634 个测试定义，其中 73 个有显式 `ignore`；
  `build/e2e/afs/**/test_*.py` 有 43 文件、508 个 `test_` 函数定义。
  Rust 采用紧邻单行 `#[test]`/`#[tokio::test]` 属性的词法扫描；Python 使用 AST，均未导入或执行。
  未展开宏、feature/cfg、参数化和 Cargo targets；support/example 定义可能进入不同 target。
  **这些数不是唯一可运行 case 数、UT 数、通过数或覆盖率，不能直接相加成产品验收数量。**
- **事实**：[旧清单](../../build/e2e/afs/acceptance/cases.json) 有 70 条：69 active、1 reserved；
  7 READY、62 TODO、1 NOT_APPLICABLE。69 active 的 smoke/full.required 均为 true。
  READY 是注册状态，TODO 不代表仓内没有相关断言；清单没有准确反映当前全部资产。
- **事实**：[visibility driver](../../build/e2e/afs/acceptance/drivers/visibility.py) 已实现 FUN-02/03/04
  的绑定和判定入口，但对应清单仍 TODO；OwnerFs/DFS 的独立驱动也覆盖其中部分行为。
- **事实**：[deploy driver](../../build/e2e/afs/acceptance/drivers/deploy.py) 虽包含部署 smoke 切片，
  但 `verify_manifest_for_offline`/`run_deployment_slice` 仍要求 `afs-processctl`、`dep02-smoke.sh`
  和旧包内容。它不能直接充当当前 ADX 统一包的部署验收；不要为复活旧工具而改产品交付。
- **事实**：真实 DFS 跨句柄 KEEP/新版本可见性/非 root 新 OPEN 拒绝已在
  [fuse_contract.rs](../../afs/tests/fuse_contract.rs) 的
  `real_dfs_keep_cache_contract::dfs_real_nodes_cross_handle_keep_cache_and_permission` 入仓。
  它显式 ignored，依赖外部真实 A/B 挂载和 metrics；不能继续登记为“未入仓”。
- **事实**：[AFS Full 环境边界](../../build/e2e/afs/acceptance/environment.py) 始终返回 BLOCKED；
  [旧 runner](../../build/e2e/afs/acceptance/runner.py) 当前还有 Linux ARM64 guard。
  这是该工具的现有限制，不能提升为 ADX 统一测试必须依赖 ARM64/四 VM/全部对照系统的原则。
- **推断/建议**：优先缺口是把已有断言和真实场景接入 ADX 共用用例集合、制品/环境及结果模型，
  同时补公共 SDK 到真实 AFS 存储的闭环；不是先照抄旧清单再建 AFS 专属流水线。

## 2. 最终总表入口与附件边界

开发自检、主线回归、固定版本验收三个时机下的拦截问题、用例位置、执行人、环境准备、
Buildkite现有调用链、AFS已接/缺失，以及已有规划和待补项，
已合并到 [唯一现状总表](test-strategy.md)。公共体系以本次讨论前已检入实现为准，未新增共享设计。此前分散的分层、运行方式、CI和优先级总览不再在此重复。

本附件下表仅回答“旧ID关联哪些已有源码资产、还缺什么”，以及对应的建议选择。
旧ID/静态定义都不是实际通过计数；用例目录迁移和CI执行遵循最终总表，不沿用旧manifest的
69项全部smoke.required作为公共冒烟集合。三类AFS组件集成的目标统一 `afs/tests/`，
旧 `build/e2e/afs/` 组件位置仅是迁移前事实；本轮未移动任何文件。
标准兼容/故障/资源/性能/长稳按被测边界归层：仅测AFS的场景也收进AFS集成套件，不一律当作公共产品E2E；
本地与Buildkite使用同一套用例，目录位置不限制CI调用。

## 3. 旧 70 项清单逐项对照

下表保留旧 ID 追溯。状态列来自 manifest；“关联资产”说明仓内相关实现，不等于已经满足该 ID
全部条件。尤其不能把 TODO 解释为零覆盖，或把 READY 解释为本轮 PASS。
归属与选择列是按 ADX 共用规范提出的建议；未修改原 JSON、driver 或 profile。

| 旧 ID / 名称 | manifest状态 | 责任层（建议） | 关联已有资产/覆盖（事实；无本轮PASS） | 缺口与建议 | 优先级 | 建议集合 |
| --- | --- | --- | --- | --- | --- | --- |
| ENV-01 `environment-gate` | TODO | ADX共用环境 | 旧 inventory/网络/TLS/verbs 探针 | 完整验证器未实现；改为按本次能力准入，不把所有对照系统硬绑冒烟 | P0 | 所有集合前置 |
| STD-01 `pjdfstest` | READY | ADX兼容验收/AFS适配 | S1：固定 pjdfstest driver/绑定 | 缺当前挂载、套件与公共调度的执行证明 | P1 | 冒烟短代表；full适用集 |
| STD-02 `LTP-filesystem` | READY | ADX兼容验收/AFS适配 | S1：固定 LTP driver/适用性 | 补真实目标绑定与完整计数；不得事后排除产品失败 | P1 | full/兼容专项 |
| STD-03 `FSx` | READY | ADX兼容验收/AFS适配 | S1：FSx driver/seed/回放 | 完整时长和seed需实跑，工具绿不证明数据完整性 | P1 | full有界集/专项 |
| STD-04 `differential-random` | READY | ADX兼容验收/AFS适配 | S1：随机差分/缩减/参考模型 | 绑定独立参考与真实目标；保留最小失败序列 | P1 | full有界集/专项 |
| STD-05 `suite-accounting` | READY | ADX共用结果/套件适配 | S1：accounting driver与负例 | 接入共同required实例/排除/清理汇总，不单列一个绿项代替套件 | P0 | 代码门禁及所有集合汇总 |
| FUN-01 `basic-io` | TODO | AFS集成→ADX用户E2E | U1/U2/E1/E2：小文件读写/内容校验 | 公共用户链路未绑定；边界/大规模按支持合同扩展 | P0 | 冒烟核心；full扩展 |
| FUN-02 `same-mount-visible` | TODO | AFS集成→ADX用户E2E | U1/V1：同挂载可见性driver存在，manifest未注册 | 核对模式合同再接入；accepted dirty可见性不能与持久发布混用 | P1 | 集成门禁；full |
| FUN-03 `close-to-open` | TODO | AFS集成→ADX用户E2E | V1/E1/E2/E3/F1：close-to-open断言 | 复用现有driver，补公共case/实际后端证明 | P0 | 冒烟核心；full扩展 |
| FUN-04 `sync-to-reopen` | TODO | AFS集成→ADX用户E2E | U1/V1/E2：fsync/fdatasync发布/重开 | 补公共链路与声明后端；区分正常恢复和崩溃 | P0 | 冒烟代表；full完整屏障 |
| FUN-05 `dup-close` | TODO | AFS内核集成→ADX用户E2E | U1/F1：句柄/旧FD/跨句柄合同 | 显式dup/fork最终close实例和实际业务覆盖尚需核对补齐 | P1 | full；最低层集成 |
| FUN-06 `sync-flags` | TODO | AFS集成→ADX可靠性 | U1：同步写/有序提交回归 | O_SYNC/O_DSYNC真实返回后崩溃矩阵不由普通fsync通过替代 | P0 | 集成门禁；full故障扩展 |
| FUN-07 `sparse-resize` | TODO | AFS集成→ADX用户E2E | U1/M1：稀疏/截断/布局；F1长度变化 | 补真实系统调用边界、远偏移和资源不物化合同 | P1 | full；最低层集成 |
| FUN-08 `namespace` | TODO | AFS集成→ADX用户E2E | U1/M1/E3：目录/rename/unlink/link/EXDEV | 公共运行时内真实namespace原子性/errno矩阵未绑定 | P0 | 冒烟rename/delete；full完整 |
| FUN-09 `permissions` | TODO | AFS集成→ADX权限E2E | U1/M1/E1/F1：权限/setid及非root新OPEN拒绝 | 补公共租户与runtime身份，按适用性扩展xattr/时间戳 | P0 | 冒烟拒绝/允许；full权限矩阵 |
| FUN-10 `locking-mmap` | TODO | AFS内核集成/ADX专项 | U1/F1：锁状态机与真实基本mmap | native/FUSE/跨节点锁支持仍受限；不得用ext4或局部UT宣布支持 | P2 | 声明能力的集成/full专项 |
| FUN-11 `concurrent-file` | TODO | AFS集成→ADX并发E2E | U1/F1：同文件并发、旧READ竞争 | 补真实多进程/用户并发和合法结果oracle | P1 | full并发 |
| FUN-12 `concurrent-inodes` | TODO | AFS集成→ADX并发E2E | U1：多inode与共享bucket不互阻 | 真实并发/公平性及有界资源需固定工作负载 | P1 | full/资源扩展 |
| FUN-13 `mount-isolation` | READY | AFS集成→ADX隔离E2E | S2：真实mount隔离driver READY | 需真实相同inode号等证据；补公共Environment/租户负例 | P0 | 冒烟代表；full隔离矩阵 |
| DIST-01 `owner-local-remote` | TODO | AFS子系统集成→ADX分布式E2E | E1/E3：OwnerFs本地bind与远端互访 | 加入公共workspace/volume和实际节点证据 | P0 | ON冒烟代表；full跨节点 |
| DIST-02 `dfs-remote-owner` | TODO | AFS子系统集成→ADX分布式E2E | E2/E4/F1：DFS一写多读/远端源 | 补公共用户入口与真实跨主机选路，不把单VM称跨主机 | P0 | ON冒烟代表；full跨节点 |
| DIST-03 `replicas` | TODO | AFS集成/ADX复制专项 | U1/M1/E4：放置/持久回执和R3工具 | 独立验证真实副本数、持久屏障和丢副本；R1不代R3 | P1 | 支持副本策略的full/专项 |
| DIST-04 `multi-source` | TODO | AFS集成→ADX分布式E2E | U1/E4：source选择/多节点cohort | 多source同时生效和失败切换要有真实路径证据 | P1 | full/分布式专项 |
| DIST-05 `source-retry` | TODO | AFS集成→ADX故障E2E | U1：source失败/重试/授权错误 | 补实际源退出/坏响应及有限回退，不放宽完整校验 | P1 | 集成门禁；full故障 |
| DIST-06 `backend-parity` | TODO | AFS适配→ADX能力矩阵 | M1：local-file及部分etcd/Redis合同 | 候选每个声明后端应有真实运行，旧矩阵不自动成为全支持 | P1 | full按支持后端 |
| DIST-07 `rpc-contract` | TODO | AFS接口集成→ADXE2E | M1/P1：真实RPC/mTLS/错误/幂等 | 已有合同复用；补实际业务链的身份与deadline证明 | P0 | 代码/集成门禁；full真实链路 |
| DIST-08 `async-repair` | TODO | AFS集成/ADX可靠性专项 | U1：复制/修复相关模块断言 | 异步修复的产品支持和实际补副本资格待核对，不自动承诺 | P2 | 声明支持后的专项 |
| PERF-01 `owner-local-read` | TODO | ADX共用性能专项/AFS适配 | T2：native closeout C基准/probe；不是本ID完整driver | OwnerFs本地读：固定候选、路径语义和对照后接入；局部benchmark不代表完整目标 | P2 | 性能专项；不进常规冒烟 |
| PERF-02 `owner-local-write` | TODO | ADX共用性能专项/AFS适配 | T2：native closeout C基准/probe；不是本ID完整driver | OwnerFs本地写：固定同步/计时、路径语义和对照；不能借普通ext4结果证明所有OwnerFs模式 | P2 | 性能专项；不进常规冒烟 |
| PERF-03 `owner-remote-read` | TODO | ADX共用性能专项/AFS适配 | T2：owner_remote_small.py小规模读测量 | 补公共候选/参考/运行绑定及完整判据；单批测量不代表稳定达标 | P2 | 性能专项；不进常规冒烟 |
| PERF-04 `owner-remote-write` | TODO | ADX共用性能专项/AFS适配 | T2：owner_remote_write_small.py小规模写测量 | 固定提交/计时和完整性判据；工具输出不自动满足吞吐与时延双目标 | P2 | 性能专项；不进常规冒烟 |
| PERF-05 `dfs-read` | TODO | ADX共用性能专项/AFS适配 | T1/E4：dfs_compare双口径与DFS读计数工具 | 固定候选、缓存、参考语义和稳定性；JuiceFS外存与3FS同步副本结果不可互换 | P2 | 性能专项；不进常规冒烟 |
| PERF-06 `dfs-write` | TODO | ADX共用性能专项/AFS适配 | T1/E4：dfs_compare发布/E2E与写计数工具 | 固定持久屏障、复制/外存语义和计时边界；阶段和不代替连续E2E | P2 | 性能专项；不进常规冒烟 |
| PERF-07 `metadata` | TODO | ADX共用性能专项/AFS适配 | S3/T2：callback计数、native基准和OwnerFs索引性能模块 | 局部计数/索引微基准不等于用户元数据吞吐；完整公共workload/判据未绑定 | P2 | 性能专项；不进常规冒烟 |
| PERF-08 `rdma-profile` | TODO | ADX共用性能专项/AFS适配 | P1：RDMA功能/lifecycle例；未见本ID注册的性能driver | 真实设备性能workload与对照待建设；不以编译或功能通过替代 | P2 | RDMA性能专项 |
| REL-01 `dirty-crash` | TODO | AFS集成→ADX故障专项 | U1：dirty/未提交失败边界 | 进程或物理崩溃点与预期可恢复集合未统一成真实case | P1 | full故障/崩溃专项 |
| REL-02 `barrier-restart` | TODO | AFS集成→ADX恢复E2E | M1/E1：local-file重开与正常全停恢复 | 公共制品正常重启闭环；强杀/断电须另测 | P0 | full正常恢复；崩溃专项 |
| REL-03 `finalize-crash` | TODO | AFS集成→ADX故障专项 | U1：finalize/发布状态机与失败 | 补真实发布关键点崩溃与数据/Meta一致性oracle | P1 | full故障/崩溃专项 |
| REL-04 `lost-meta-ack` | TODO | AFS集成→ADX故障专项 | U1/M1：未知ACK精确重放/重开 | 组件错误注入已有；全链真实丢回复与不误发布需绑定 | P0 | 集成门禁；full真实cut |
| REL-05 `meta-restart` | TODO | AFS集成→ADX恢复E2E | M1/E3：Meta正常重启及部分真实后端合同 | 扩声明持久后端真实失败/恢复；不得称HA | P1 | full恢复/故障 |
| REL-06 `stale-owner` | TODO | AFS集成→ADX故障E2E | U1/M1：旧lease/session/epoch拒绝 | 真实partition/restart/迟到ACK要保留权威与数据证据 | P0 | 集成门禁；full故障 |
| REL-07 `replica-loss` | TODO | AFS集成/ADX复制专项 | U1/M1/E4：复制合同和R3测量辅助 | 真实隔离/丢失/替换/修复不由内容测量代替 | P2 | 声明副本策略的故障专项 |
| REL-08 `capacity-io` | TODO | AFS集成→ADX资源故障E2E | U1/M1：容量/错误传播局部回归 | 真实ENOSPC/EIO/只读与后台失败，需独立安全测试资源 | P0 | 代码/集成门禁；full故障 |
| REL-09 `corrupt-copy` | TODO | AFS集成→ADX完整性E2E | U1：完整摘要、短长响应/坏cache/坏正文拒绝 | 真实物理损坏注入与回源/失败合同统一，不能返回部分输出 | P0 | 集成门禁；full损坏专项 |
| REL-10 `network` | TODO | AFS集成→ADX网络故障E2E | U1/P1：期限/取消/失败回退；网络探针 | 探针只证明连通；真实断网/恢复/超时任务回收需工作负载 | P1 | full/网络专项 |
| REL-11 `handle-cleanup` | TODO | AFS集成→ADX生命周期E2E | U1/F1/E1/E2：join/busy/wait0/卸载 | 公共删除/停止验证实际FD、进程、mount、任务残留 | P0 | 冒烟清理必选；full异常扩展 |
| REL-12 `owner-home-loss` | TODO | AFS集成/ADX恢复专项 | U1/E3：Home重启与失败返回 | Home数据丢失/不可用的产品边界待固定；不假定自动恢复 | P1 | full/故障专项 |
| REL-13 `namespace-durable` | TODO | AFS集成→ADX耐久性E2E | M1/E1/E2：namespace与父目录sync | 目录rename/unlink等真实崩溃耐久性未由正常重启证明 | P1 | full/崩溃专项 |
| REL-14 `soak` | TODO | ADX共用长稳专项 | U1/T1：资源生命周期断言及观测工具 | 固定时长、负载、峰值/回落、失败率和清理判据后实跑 | P2 | 长稳专项，不进提交冒烟 |
| REL-15 `duplicate-active-meta` | NOT_APPLICABLE | 不在当前支持验收范围 | manifest明确reserved/inactive | Meta HA/多活设计未纳入；保留排除而非执行PASS | P2 | 不适用；未来能力另评审 |
| RDMA-01 `verbs-preflight` | TODO | ADX设备准入/AFS适配 | P1/S3：verbs和probe辅助 | 真实设备/权限/连通资格与当前候选待验证 | P2 | RDMA专项前置 |
| RDMA-02 `file-io` | TODO | AFS设备集成→ADX专项 | P1：真实RDMA读写roundtrip ignored | 不以缺设备拒绝/编译替代真实I/O | P2 | RDMA专项 |
| RDMA-03 `lifetime` | TODO | AFS设备集成→ADX专项 | P1：posted取消/endpoint排空/poison ignored | 实际verbs取消/期限/生命周期显式执行 | P2 | RDMA专项 |
| RDMA-04 `fallback` | TODO | AFS组件集成→ADX专项 | P1：Auto缺设备回退、显式RDMA拒绝 | 补运行中故障语义；未知结果不可任意重放 | P2 | 普通拒绝代码门禁；RDMA专项 |
| RDMA-05 `integrity` | TODO | AFS设备集成→ADX专项 | U1/P1：完整校验及大载荷合同 | 真实设备完整性/坏数据/边界须有独立oracle | P2 | RDMA专项 |
| OPS-01 `health` | READY | AFS组件集成→ADX就绪 | U2/S2：health合同和真实driver READY | 公共部署使用业务ready，不只HTTP200/进程存活 | P0 | 冒烟必选；full故障状态 |
| OPS-02 `metrics` | TODO | ADX可观测E2E/AFS适配 | U1/T1：metrics及工作量采样 | 关键模式/source/回调可观测；完整资源账本另验 | P1 | 冒烟最小路径证明；full指标 |
| OPS-03 `trace` | TODO | ADX可观测E2E/AFS适配 | 域内tracing/公共ADX trace资产 | AFS业务实际关联与丢失/扰动边界待验证 | P1 | full可观测；诊断专项 |
| OPS-04 `diagnostics` | TODO | ADX共用诊断/报告 | T1：错误、identity、日志和归档断言 | 统一每个失败保存输入/日志/身份，避免只剩退出码 | P1 | 所有集合证据要求 |
| OPS-05 `backpressure` | TODO | AFS集成→ADX资源专项 | U1：内存/连接/task/buffer准入回收断言 | 真实并发分母、峰值、降级与idle回落仍需实际负载 | P1 | full有界资源；压力专项 |
| OPS-06 `reproducible-runner` | TODO | ADX共用调度/结果 | T1：runner/过滤/超时/结果等已实现 | 旧条目仍称runner未实现；统一注册和required覆盖，非另造AFS框架 | P0 | 代码门禁及所有集合 |
| OPS-07 `workspace-location-affinity` | TODO | AFS接口集成→ADX放置E2E | E3/U2：root location/REST/本地远端路径 | 若纳入产品能力，补公共调度实际归属和拒绝语义 | P1 | 集成门禁；full放置扩展 |
| DEP-01 `clean-install` | TODO | ADX共用交付/E2E | D1：包校验/安装合同；旧deploy有切片 | 旧driver依赖旧包；复用ADX包证明真实服务和业务 | P0 | 公共ON包冒烟；full干净安装 |
| DEP-02 `cluster-start` | TODO | ADX共用部署/E2E | E1/E2/E3：AFS专用启动脚本 | 旧deploy明确不实现集群；应复用ADX部署/fixture，禁止另建部署器 | P0 | 冒烟所需拓扑；full跨机 |
| DEP-03 `idempotent` | TODO | ADX共用部署集成/E2E | D1：配置/幂等工具；旧deploy部分切片 | 当前包重复安装/启动与冲突拒绝需统一验证 | P1 | full交付；关键拒绝代码门禁 |
| DEP-04 `lifecycle` | TODO | ADX共用生命周期/E2E | D1：supervisor替身合同；E1/E2真实wait0 | 用当前包证明真实AFS停止/失败传播/重启，不把替身当真实进程 | P0 | 冒烟启动/停止；full恢复 |
| DEP-05 `failure-cleanup` | TODO | ADX共用交付负例 | D1：包内容/模式/清单拒绝；旧driver部分切片 | 当前制品的端口/权限/缺FUSE/坏digest与失败回收 | P0 | 工具门禁；有界负例/完整full |
| DEP-06 `offline-package` | TODO | ADX共用交付/E2E | D1：当前安装包工具；旧driver仅检查本地tarball | 断网安装并未由本地文件证明；依赖清单与外部服务合同另定 | P1 | 声明离线能力的full |
| DEP-07 `uninstall` | TODO | ADX共用清理/E2E | E1/E2/D1：正常停止/卸载和保留数据合同 | 旧uninstall不能直接用于ADX；公共程序清理与持久数据保留明确分开 | P0 | 冒烟物理清理；full卸载边界 |
| DEP-08 `restart-package` | TODO | ADX共用交付/E2E | M1/E1：同数据正常恢复；旧条目无driver | 当前制品同版本重部署/格式不兼容拒绝需按支持合同验证 | P1 | full交付恢复 |

资产索引（按测试名/入口核查，非运行结果）：

- **U1**：[DFS VFS](../../afs/src/node/vfs/dfs.rs)、[OwnerFs VFS](../../afs/src/node/vfs/ownerfs.rs)、[DFS read](../../afs/src/node/dfs_read.rs)、[Chunk](../../afs/src/node/chunk.rs)、[ExternalChunk](../../afs/src/node/external_chunk.rs)、[Peer cache](../../afs/src/node/peer_cache.rs)、[replication](../../afs/src/node/replication.rs) 等模块测试。
- **U2**：[配置](../../afs/tests/config_contract.rs)、[REST](../../afs/tests/rest_contract.rs)、[Node health](../../afs/tests/node_health_contract.rs)、[Meta capability](../../afs/tests/meta_health_capability.rs)、[错误](../../afs/tests/error_contract.rs)。
- **M1**：[Meta 合同](../../afs/tests/meta_contract.rs)、[local-file](https://gitcode.com/openJiuwen/agent-dx/blob/1e9ae5f235d412b06fa4195bdbe26860bd7cd840/afs/src/meta/store/local_file.rs)、[Redis](https://gitcode.com/openJiuwen/agent-dx/blob/1e9ae5f235d412b06fa4195bdbe26860bd7cd840/afs/tests/redis_contract.rs)、[local storage](../../afs/tests/storage_localfs.rs)。
- **P1**：[OwnerFs Peer](../../afs/tests/ownerfs_peer_contract.rs)、[RDMA lifecycle](../../afs/tests/rdma_lifecycle.rs)、[RDMA probe](../../afs/common/transport/tests/rdma_probe.rs)。
- **F1**：[FUSE 合同](../../afs/tests/fuse_contract.rs)；`dfs_real_nodes_cross_handle_keep_cache_and_permission` 是外部真实 DFS fixture，其他测试逐条看 backend，不混用结论。
- **E1**：[OwnerFs bind/远端/恢复驱动](../../build/e2e/afs/scripts/ownerfs/bind-two-node-localfile.py)。
- **E2**：[DFS 三 Node 核心驱动](../../build/e2e/afs/scripts/dfs/three_node_core.py)。
- **E3**：[OwnerFs 跨主机驱动](../../build/e2e/afs/ownerfs_acceptance.py)。
- **E4**：[DFS manyread](../../build/e2e/afs/acceptance/dfs_manyread_small.py)、[cohort](../../build/e2e/afs/acceptance/dfs_multinode_small.py)、[R3 helper](../../build/e2e/afs/acceptance/dfs_r3_small.py)。R3 helper 的历史固定默认值不能作为当前候选身份。
- **V1**：[可见性 driver](../../build/e2e/afs/acceptance/drivers/visibility.py) 的 `CASE_IDS` 和具体分派覆盖 FUN-02/03/04；与 manifest TODO 是不同事实。
- **S1**：[标准套件绑定](../../build/e2e/afs/acceptance/drivers/suite_binding.py)、[pjdfstest](../../build/e2e/afs/acceptance/drivers/standard.py)、[LTP](../../build/e2e/afs/acceptance/drivers/ltp.py)、[FSx](../../build/e2e/afs/acceptance/drivers/fsx.py)、[随机差分](../../build/e2e/afs/acceptance/drivers/random_fs.py)、[accounting](../../build/e2e/afs/acceptance/drivers/accounting.py)。
- **S2**：[mount isolation](../../build/e2e/afs/acceptance/drivers/mount_isolation.py)、[health](../../build/e2e/afs/acceptance/drivers/health.py)。
- **S3**：[参数化网络/verbs/内核探针](../../build/e2e/afs/acceptance/probes/README.md)。
- **D1**：[部署 config](../../platform/deployment/tests/config.rs)、[supervisor process](../../platform/deployment/tests/process.rs)、[包工具](../../build/release/tests/test_package.py)、[component manifest](../../build/release/tests/test_components.py)、[CI 合同](../../build/e2e/tests/test_pipeline_contracts.py)。
- **T1**：[runner 自测](../../build/e2e/afs/acceptance/test_runner.py)、[identity 自测](../../build/e2e/afs/acceptance/test_target_identity.py)、[性能工具](../../build/e2e/afs/perf/README.md) 与各 driver/probe 紧邻的 `test_*.py`。
- **T2**：[OwnerFs 远端读](../../build/e2e/afs/acceptance/owner_remote_small.py)、[远端写](../../build/e2e/afs/acceptance/owner_remote_write_small.py)、[native C 基准](../../build/e2e/afs/acceptance/probes/ownerfs_native_closeout_benchmark.c)、[native I/O helper](../../build/e2e/afs/acceptance/probes/ownerfs_native_closeout_io.c)；它们各有适用路径，不能合称完整 OwnerFs 性能验收。

## 4. 静态测试文件索引

以下完整列出本轮扫描到的文件与定义数，仅用于资产发现和后续选择器核对。
`src/` 并不自动等于 UT，`tests/` 并不自动等于 E2E；`ignore` 还可能受 feature/cfg 限制。
Rust 属性词法统计未展开宏/target；Python AST 统计不证明 unittest discovery 实际发现或执行。

| Rust 文件 | 静态定义 | 显式 ignore | 初步责任分类（推断，须逐例看依赖） |
| --- | ---: | ---: | --- |
| [afs/common/error/src/lib.rs](../../afs/common/error/src/lib.rs) | 4 | 0 | 模块测试/私有组件集成混合 |
| [afs/common/logging/src/fallback.rs](../../afs/common/logging/src/fallback.rs) | 1 | 0 | 模块测试/私有组件集成混合 |
| [afs/common/logging/src/file_writer.rs](../../afs/common/logging/src/file_writer.rs) | 2 | 0 | 模块测试/私有组件集成混合 |
| [afs/common/logging/src/init.rs](../../afs/common/logging/src/init.rs) | 2 | 0 | 模块测试/私有组件集成混合 |
| [afs/common/logging/src/retention.rs](../../afs/common/logging/src/retention.rs) | 2 | 0 | 模块测试/私有组件集成混合 |
| [afs/common/logging/tests/process_contract.rs](../../afs/common/logging/tests/process_contract.rs) | 3 | 0 | 组件集成/接口合同 |
| [afs/common/metrics/src/lib.rs](../../afs/common/metrics/src/lib.rs) | 3 | 0 | 模块测试/私有组件集成混合 |
| [afs/common/protocol/tests/owner_read_payload.rs](../../afs/common/protocol/tests/owner_read_payload.rs) | 2 | 0 | 组件集成/接口合同 |
| [afs/common/protocol/tests/owner_write_payload.rs](../../afs/common/protocol/tests/owner_write_payload.rs) | 1 | 0 | 组件集成/接口合同 |
| [afs/common/tracing/src/context.rs](../../afs/common/tracing/src/context.rs) | 1 | 0 | 模块测试/私有组件集成混合 |
| [afs/common/tracing/src/grpc.rs](../../afs/common/tracing/src/grpc.rs) | 1 | 0 | 模块测试/私有组件集成混合 |
| [afs/common/tracing/src/server.rs](../../afs/common/tracing/src/server.rs) | 3 | 0 | 模块测试/私有组件集成混合 |
| [afs/common/transport/src/grpc/error_status.rs](../../afs/common/transport/src/grpc/error_status.rs) | 5 | 0 | 模块测试/私有组件集成混合 |
| [afs/common/transport/src/grpc/security.rs](../../afs/common/transport/src/grpc/security.rs) | 1 | 0 | 模块测试/私有组件集成混合 |
| [afs/common/transport/src/rdma.rs](../../afs/common/transport/src/rdma.rs) | 3 | 0 | 模块测试/私有组件集成混合 |
| [afs/common/transport/src/shm.rs](../../afs/common/transport/src/shm.rs) | 8 | 0 | 模块测试/私有组件集成混合 |
| [afs/common/transport/tests/rdma_probe.rs](../../afs/common/transport/tests/rdma_probe.rs) | 5 | 5 | 组件集成/接口合同 |
| [afs/common/transport/tests/tcp_config.rs](../../afs/common/transport/tests/tcp_config.rs) | 1 | 0 | 组件集成/接口合同 |
| [afs/src/bin/afs-node.rs](../../afs/src/bin/afs-node.rs) | 3 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/config.rs](../../afs/src/config.rs) | 3 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/dfs.rs](../../afs/src/dfs.rs) | 3 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/meta/dfs.rs](../../afs/src/meta/dfs.rs) | 37 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/meta/rpc.rs](../../afs/src/meta/rpc.rs) | 9 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/meta/store/etcd.rs](https://gitcode.com/openJiuwen/agent-dx/blob/1e9ae5f235d412b06fa4195bdbe26860bd7cd840/afs/src/meta/store/etcd.rs) | 1 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/meta/store/local_file.rs](https://gitcode.com/openJiuwen/agent-dx/blob/1e9ae5f235d412b06fa4195bdbe26860bd7cd840/afs/src/meta/store/local_file.rs) | 14 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/meta/store/redis.rs](https://gitcode.com/openJiuwen/agent-dx/blob/1e9ae5f235d412b06fa4195bdbe26860bd7cd840/afs/src/meta/store/redis.rs) | 2 | 1 | 模块测试/私有组件集成混合 |
| [afs/src/meta/store.rs](../../afs/src/meta/store.rs) | 24 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/meta.rs](../../afs/src/meta.rs) | 5 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/api/local.rs](../../afs/src/node/api/local.rs) | 8 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/chunk/native_live_tests.rs](../../afs/src/node/chunk/native_live_tests.rs) | 20 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/chunk/outboard.rs](../../afs/src/node/chunk/outboard.rs) | 33 | 1 | 模块测试/私有组件集成混合 |
| [afs/src/node/chunk.rs](../../afs/src/node/chunk.rs) | 114 | 3 | 模块测试/私有组件集成混合 |
| [afs/src/node/dfs_fetch.rs](../../afs/src/node/dfs_fetch.rs) | 2 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/dfs_read.rs](../../afs/src/node/dfs_read.rs) | 125 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/external_chunk.rs](../../afs/src/node/external_chunk.rs) | 93 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/fuse/state.rs](../../afs/src/node/fuse/state.rs) | 3 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/fuse.rs](../../afs/src/node/fuse.rs) | 46 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/native_workspace.rs](../../afs/src/node/native_workspace.rs) | 17 | 8 | 模块测试/私有组件集成混合 |
| [afs/src/node/peer_cache.rs](../../afs/src/node/peer_cache.rs) | 54 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/read_work.rs](../../afs/src/node/read_work.rs) | 20 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/replication.rs](../../afs/src/node/replication.rs) | 30 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/rpc/control.rs](../../afs/src/node/rpc/control.rs) | 14 | 1 | 模块测试/私有组件集成混合 |
| [afs/src/node/rpc/data.rs](../../afs/src/node/rpc/data.rs) | 48 | 5 | 模块测试/私有组件集成混合 |
| [afs/src/node/rpc/meta.rs](../../afs/src/node/rpc/meta.rs) | 37 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/rpc/peer.rs](../../afs/src/node/rpc/peer.rs) | 49 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/storage/localfs.rs](../../afs/src/node/storage/localfs.rs) | 1 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/storage.rs](../../afs/src/node/storage.rs) | 3 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/dfs/pending_trace.rs](../../afs/src/node/vfs/dfs/pending_trace.rs) | 3 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/dfs.rs](../../afs/src/node/vfs/dfs.rs) | 263 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/locks.rs](../../afs/src/node/vfs/locks.rs) | 31 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/ownerfs/bind_mount.rs](../../afs/src/node/vfs/ownerfs/bind_mount.rs) | 11 | 5 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/ownerfs/catalog.rs](../../afs/src/node/vfs/ownerfs/catalog.rs) | 1 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/ownerfs/identity_index_perf.rs](../../afs/src/node/vfs/ownerfs/identity_index_perf.rs) | 8 | 1 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/ownerfs/mount_manager.rs](../../afs/src/node/vfs/ownerfs/mount_manager.rs) | 13 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/ownerfs/native_home.rs](../../afs/src/node/vfs/ownerfs/native_home.rs) | 9 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/ownerfs/native_home_fuse_fixture.rs](../../afs/src/node/vfs/ownerfs/native_home_fuse_fixture.rs) | 2 | 2 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/ownerfs/native_home_tests.rs](../../afs/src/node/vfs/ownerfs/native_home_tests.rs) | 20 | 5 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/ownerfs/official_fuser_tests.rs](../../afs/src/node/vfs/ownerfs/official_fuser_tests.rs) | 1 | 1 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/ownerfs/root.rs](../../afs/src/node/vfs/ownerfs/root.rs) | 25 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/ownerfs/structural_index_invariants.rs](../../afs/src/node/vfs/ownerfs/structural_index_invariants.rs) | 7 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/ownerfs/structural_index_perf.rs](../../afs/src/node/vfs/ownerfs/structural_index_perf.rs) | 14 | 1 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/ownerfs/subtree_index_invariants.rs](../../afs/src/node/vfs/ownerfs/subtree_index_invariants.rs) | 4 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/ownerfs/subtree_index_perf.rs](../../afs/src/node/vfs/ownerfs/subtree_index_perf.rs) | 7 | 1 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs/ownerfs.rs](../../afs/src/node/vfs/ownerfs.rs) | 91 | 1 | 模块测试/私有组件集成混合 |
| [afs/src/node/vfs.rs](../../afs/src/node/vfs.rs) | 3 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/node.rs](../../afs/src/node.rs) | 22 | 0 | 模块测试/私有组件集成混合 |
| [afs/src/runtime.rs](../../afs/src/runtime.rs) | 5 | 0 | 模块测试/私有组件集成混合 |
| [afs/tests/config_contract.rs](../../afs/tests/config_contract.rs) | 50 | 0 | 组件集成/接口合同 |
| [afs/tests/dfs_create_contention.rs](../../afs/tests/dfs_create_contention.rs) | 6 | 0 | 组件集成/接口合同 |
| [afs/tests/error_contract.rs](../../afs/tests/error_contract.rs) | 4 | 0 | 组件集成/接口合同 |
| [afs/tests/fuse_contract.rs](../../afs/tests/fuse_contract.rs) | 22 | 17 | 组件集成/接口合同 |
| [afs/tests/local_sdk.rs](../../afs/tests/local_sdk.rs) | 9 | 0 | 组件集成/接口合同 |
| [afs/tests/meta_contract.rs](../../afs/tests/meta_contract.rs) | 61 | 2 | 组件集成/接口合同 |
| [afs/tests/meta_health_capability.rs](../../afs/tests/meta_health_capability.rs) | 6 | 0 | 组件集成/接口合同 |
| [afs/tests/node_health_contract.rs](../../afs/tests/node_health_contract.rs) | 21 | 0 | 组件集成/接口合同 |
| [afs/tests/ownerfs_peer_contract.rs](../../afs/tests/ownerfs_peer_contract.rs) | 14 | 3 | 组件集成/接口合同 |
| [afs/tests/rdma_lifecycle.rs](../../afs/tests/rdma_lifecycle.rs) | 7 | 6 | 组件集成/接口合同 |
| [afs/tests/redis_contract.rs](https://gitcode.com/openJiuwen/agent-dx/blob/1e9ae5f235d412b06fa4195bdbe26860bd7cd840/afs/tests/redis_contract.rs) | 4 | 4 | 组件集成/接口合同 |
| [afs/tests/rest_contract.rs](../../afs/tests/rest_contract.rs) | 7 | 0 | 组件集成/接口合同 |
| [afs/tests/storage_localfs.rs](../../afs/tests/storage_localfs.rs) | 8 | 0 | 组件集成/接口合同 |
| [afs/tests/support/perf_denominators.rs](../../afs/tests/support/perf_denominators.rs) | 4 | 0 | test support/example 内定义；不当独立业务case |
| [afs/tests/support/root_command_meta.rs](../../afs/tests/support/root_command_meta.rs) | 4 | 0 | test support/example 内定义；不当独立业务case |
| [afs/tests/vfs_contract.rs](../../afs/tests/vfs_contract.rs) | 1 | 0 | 组件集成/接口合同 |

| Python 工具自测文件 | 静态 test_ 定义 |
| --- | ---: |
| [build/e2e/afs/acceptance/probes/test_env_network.py](../../build/e2e/afs/acceptance/probes/test_env_network.py) | 13 |
| [build/e2e/afs/acceptance/probes/test_env_verbs.py](../../build/e2e/afs/acceptance/probes/test_env_verbs.py) | 14 |
| [build/e2e/afs/acceptance/probes/test_fuse_callback_counts.py](../../build/e2e/afs/acceptance/probes/test_fuse_callback_counts.py) | 7 |
| [build/e2e/afs/acceptance/probes/test_native_control_capacity.py](../../build/e2e/afs/acceptance/probes/test_native_control_capacity.py) | 9 |
| [build/e2e/afs/acceptance/probes/test_native_lock_kernel.py](../../build/e2e/afs/acceptance/probes/test_native_lock_kernel.py) | 8 |
| [build/e2e/afs/acceptance/probes/test_native_orderly_recovery.py](../../build/e2e/afs/acceptance/probes/test_native_orderly_recovery.py) | 11 |
| [build/e2e/afs/acceptance/probes/test_native_source_rejection.py](../../build/e2e/afs/acceptance/probes/test_native_source_rejection.py) | 8 |
| [build/e2e/afs/acceptance/probes/test_owner_io_samples.py](../../build/e2e/afs/acceptance/probes/test_owner_io_samples.py) | 5 |
| [build/e2e/afs/acceptance/probes/test_physical_cache.py](../../build/e2e/afs/acceptance/probes/test_physical_cache.py) | 5 |
| [build/e2e/afs/acceptance/probes/test_workspace_host.py](../../build/e2e/afs/acceptance/probes/test_workspace_host.py) | 5 |
| [build/e2e/afs/acceptance/probes/test_workspace_remote.py](../../build/e2e/afs/acceptance/probes/test_workspace_remote.py) | 6 |
| [build/e2e/afs/acceptance/test_accounting_driver.py](../../build/e2e/afs/acceptance/test_accounting_driver.py) | 34 |
| [build/e2e/afs/acceptance/test_cleanup_reporting.py](../../build/e2e/afs/acceptance/test_cleanup_reporting.py) | 2 |
| [build/e2e/afs/acceptance/test_deploy_driver.py](../../build/e2e/afs/acceptance/test_deploy_driver.py) | 5 |
| [build/e2e/afs/acceptance/test_dfs_cohort_relay.py](../../build/e2e/afs/acceptance/test_dfs_cohort_relay.py) | 8 |
| [build/e2e/afs/acceptance/test_dfs_manyread_small.py](../../build/e2e/afs/acceptance/test_dfs_manyread_small.py) | 25 |
| [build/e2e/afs/acceptance/test_dfs_multinode_small.py](../../build/e2e/afs/acceptance/test_dfs_multinode_small.py) | 10 |
| [build/e2e/afs/acceptance/test_dfs_r3_write_small.py](../../build/e2e/afs/acceptance/test_dfs_r3_write_small.py) | 7 |
| [build/e2e/afs/acceptance/test_dfs_unique_io.py](../../build/e2e/afs/acceptance/test_dfs_unique_io.py) | 9 |
| [build/e2e/afs/acceptance/test_environment_qualification.py](../../build/e2e/afs/acceptance/test_environment_qualification.py) | 7 |
| [build/e2e/afs/acceptance/test_fsx_driver.py](../../build/e2e/afs/acceptance/test_fsx_driver.py) | 13 |
| [build/e2e/afs/acceptance/test_health_driver.py](../../build/e2e/afs/acceptance/test_health_driver.py) | 12 |
| [build/e2e/afs/acceptance/test_inventory.py](../../build/e2e/afs/acceptance/test_inventory.py) | 10 |
| [build/e2e/afs/acceptance/test_locks_cross.py](../../build/e2e/afs/acceptance/test_locks_cross.py) | 10 |
| [build/e2e/afs/acceptance/test_locks_guards.py](../../build/e2e/afs/acceptance/test_locks_guards.py) | 6 |
| [build/e2e/afs/acceptance/test_locks_renewal_fault.py](../../build/e2e/afs/acceptance/test_locks_renewal_fault.py) | 4 |
| [build/e2e/afs/acceptance/test_ltp_driver.py](../../build/e2e/afs/acceptance/test_ltp_driver.py) | 26 |
| [build/e2e/afs/acceptance/test_mount_isolation.py](../../build/e2e/afs/acceptance/test_mount_isolation.py) | 13 |
| [build/e2e/afs/acceptance/test_owner_remote_small.py](../../build/e2e/afs/acceptance/test_owner_remote_small.py) | 15 |
| [build/e2e/afs/acceptance/test_owner_remote_write_small.py](../../build/e2e/afs/acceptance/test_owner_remote_write_small.py) | 8 |
| [build/e2e/afs/acceptance/test_owner_restart.py](../../build/e2e/afs/acceptance/test_owner_restart.py) | 4 |
| [build/e2e/afs/acceptance/test_prepare_workspace_rootfs.py](../../build/e2e/afs/acceptance/test_prepare_workspace_rootfs.py) | 4 |
| [build/e2e/afs/acceptance/test_random_fs_driver.py](../../build/e2e/afs/acceptance/test_random_fs_driver.py) | 11 |
| [build/e2e/afs/acceptance/test_runner.py](../../build/e2e/afs/acceptance/test_runner.py) | 30 |
| [build/e2e/afs/acceptance/test_standard_driver.py](../../build/e2e/afs/acceptance/test_standard_driver.py) | 31 |
| [build/e2e/afs/acceptance/test_suite_binding.py](../../build/e2e/afs/acceptance/test_suite_binding.py) | 24 |
| [build/e2e/afs/acceptance/test_target_identity.py](../../build/e2e/afs/acceptance/test_target_identity.py) | 29 |
| [build/e2e/afs/acceptance/test_visibility_driver.py](../../build/e2e/afs/acceptance/test_visibility_driver.py) | 9 |
| [build/e2e/afs/acceptance/test_vm_capacity_manifest.py](../../build/e2e/afs/acceptance/test_vm_capacity_manifest.py) | 3 |
| [build/e2e/afs/perf/test_dfs_compare.py](../../build/e2e/afs/perf/test_dfs_compare.py) | 40 |
| [build/e2e/afs/scripts/dfs/test_three_node_core.py](../../build/e2e/afs/scripts/dfs/test_three_node_core.py) | 1 |
| [build/e2e/afs/scripts/ownerfs/test_bind_two_node_localfile.py](../../build/e2e/afs/scripts/ownerfs/test_bind_two_node_localfile.py) | 4 |
| [build/e2e/afs/scripts/ownerfs/test_native_workspace_control.py](../../build/e2e/afs/scripts/ownerfs/test_native_workspace_control.py) | 3 |

## 5. 后续使用与本轮边界

1. 后续补用例先依据唯一最终总表登记合同和 required 集，不按旧 69 项 smoke 标记直接接线。
2. 按共用规范给每条候选用例登记合同、owner、层级、fixture、产物身份、集合与状态；已有断言先复用。
3. 先闭合 P0 组织/交付/公共用户链路，再扩 P1 支持矩阵，重型 P2 按专项验收。
4. 后续实现仍在 ADX 统一代码/发布/部署/CI 体系中；AFS 只拥有其 UT 和组件集成的断言与适配。
   三类组件集成的目标位置统一为 `afs/tests/`，外置旧组件脚本后续迁入；本轮未移动。
   新增脚本名、调用链、test kit与publish依赖按最终总表及“表中调用链的固定解释”执行。

本轮只生成静态盘点和规范草案，没有补测试、改产品/Makefile/driver/manifest/pipeline、
运行测试、构建包、启停 VM/服务、触发 CI、提交或推送。所有当前运行结果保持待验证。
