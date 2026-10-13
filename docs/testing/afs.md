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

外部后端的基本流程为 A 写入 → 同步确认 S3 对象和 Meta 持久提交 → rename 发布并
同步父目录 → B 独立打开读取 → 重复读取。先使用64KiB／64MiB数据验证必要功能，
再按任务范围扩大；内容、功能、测量完成及性能达标分别登记。
SeaweedFS、PostgreSQL、JuiceFS 的部署、凭据和环境编排留在产品仓外。

### 发布、回源与恢复

| 边界 | 必要覆盖 |
| --- | --- |
| 持久发布 | PUT 失败不发布新 head／长度；确认成功后另一节点读取完整内容；外部模式不新增 Node 耐久副本 |
| 窗口与重放 | 真实4MiB满块跨 write 预上传，所有命令共享四路上限且完成一路立即补位；乱序回执不串窗口，部分失败保留身份与成功回执，Meta 未知 ACK 精确重放并阻挡后续 mutation |
| 同步与修改 | Full／DataOnly、同步写、可写 flush、dirty 读、覆盖／稀疏写／截断和正常退出维持原语义；成功 fsync 等待所需对象回执和 Meta 持久确认 |
| 读正确性 | 完整 SHA 一致、完整长度／摘要拒绝短长响应及内容损坏、错误不能发布部分输出；cache 命中不能替代权限、当前版本和期限检查 |
| 已发布数据恢复 | A 正常退出后 B 清空客户端内容缓存并从 S3 回源；local-file Meta 正常全停重启后重新打开读取 |
| 写会话恢复边界 | 新会话立即复写与退出后读取分开验收；旧写租约有效时可能返回 EBUSY，不能通过修改租约或只测读取宣布恢复写权通过 |

正常重启恢复不等于物理崩溃恢复、HA 或复杂可靠性。cache OFF 与 ON 分别绑定候选，
历史 debug／release 或不同后端的功能通过不自动继承。当前支持与剩余缺口见
[演进计划](../development/afs-plan.md#支持范围)。

### 缓存、授权与资源生命周期

| 边界 | 必要覆盖 |
| --- | --- |
| 缓存安全 | 队列／pending／磁盘额度、私有文件检查、损坏回源、填充失败、store/generation 隔离、重开与正常排空；资源不足跳过填充，S3／Meta 错误仍传播 |
| 不可变载荷 | GET 验证后、Pending、后台填充按完整 chunk 身份共享同一分配；错误长度／摘要／身份不能准入或修改输出；写侧私有 StagedChunk 经 transport／retry 交接不重复整块复制或校验 |
| 字节预算 | 分配前准入，dirty／stage／transport／retry 的租约持有到最后 owner；cache 按实际 Vec 容量去重计费，pending／worker／单驻留槽及读者仍持有的旧 pin 均不得提前退账 |
| 驻留语义 | Node 共享最多4MiB已验证对象，预算不足回退；每 callback 仍核验 head／授权。RAM 命中允许延后发现磁盘副本变化，新磁盘加载必须再检查权限和完整摘要 |
| 当前授权融合 | 同一 Meta read view 核验 head 和全部授权、解析真正缺失或 TTL 过期的 source；仍存在但无效的 grant 必须拒绝，不能重签洗白；HeadChanged 先于旧 copy 可用性判断 |
| 拒绝与回退 | 错误 inode／namespace／layout、MAC、epoch、过期及越界拒绝；仅真正 Unimplemented 走兼容路径，权限拒绝／timeout／其他错误不回退、不 GET、不修改输出 |
| 原路径隔离 | 没有候选 layout、空洞／EOF、dirty 写状态、P2P 维持原新鲜读取路径；外部 S3 实验不改变 OwnerFs bind 语义 |
| 校验线程 | 官方串行／并行完整摘要一致，请求范围外损坏仍拒绝；GET／Ready 共用私有两线程池、单对象 gate；停止后拒绝新校验、实际 worker panic 传播并显式 join，失败不被退出成功掩盖 |

入口满时应真实等待；timeout、ticket 放弃及 Drop 不留下独立客户端任务。HTTP
连接复用不能替代并发／字节准入。载荷预算、cache 内容额度和进程 RSS 是不同指标，
不能用事后 RSS 或四路并发证明字节上限。旧 generation、索引及前台临时读取缓冲
须另计环境预算，不能把 cache 额度当成整个目录或文件系统配额。

Linux 受影响回归可从现有模块入口执行；按改动选择过滤项，不为文档变化重跑：

```sh
cargo test --locked -p afs --lib --no-default-features --features dfs node::external_chunk::tests::
cargo test --locked -p afs --lib --no-default-features --features dfs node::dfs_read::tests::
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
```

Node 复制、Meta 条件事务、精确重放及 local-file 恢复的已有回归按受影响范围复用；
上面的两个模块命令不代表这些其他模块、完整 POSIX 或 Full 已通过。

### JuiceFS 同后端性能对照

在同一组 Linux 节点分时运行，测量时不得并行编译或启动另一套客户端。固定双方
版本、Meta 后端、S3服务及持久设置、应用数据／操作、并发、缓存、计时边界和资源。
AFS Meta 与 JuiceFS 的 PostgreSQL 按黑盒比较，实际使用 local-file／etcd 必须记录，
不得用程序默认值代替运行配置。后端差异明显时先隔离定位，不把整体差异归因语言。

有限顺序对照配置采用 JuiceFS 1.4.1，磁盘缓存256MiB、buffer-size160MiB、单缓存
目录，writeback／prefetch／max-readahead 关闭，cache-partial-only=false、
cache-large-write=false、四路上传；AFS磁盘缓存256MiB、pending36MiB、单对象
resident4MiB、写预算160MiB。上述预算不涵盖所有运行时／VFS／IO缓冲，不宣称
两者内存架构完全等价。

先固定应用4MiB读写，核对实际 FUSE callback、GET／PUT次数和字节，再比较相同
发布、首读、重复读阶段。cache ON 不保证 JuiceFS 一律发完整 GET；请求形状不同
只能摸底。AFS完整对象BLAKE3与JuiceFS范围CRC的强度、已打开句柄授权／新鲜度
策略差异应保留，不能靠关闭检查制造对等。

记录独立 syscall 的 p50／p95／p99，测前固定p95为判定分位数；完整read包含的
应用 SHA、open／EOF等成本另行说明。发布包含写入、file fsync、close、rename和
parent fsync，双方可能把上传等待放在不同操作中，不能只比较短fsync或write。
ODIRECT不代表后端冷盘，资源采集窗口、CPU精度和RSS／HWM范围需披露。
小样本尾分位和单轮改善不能证明稳定优势；反序／随机顺序重复仍不自动等于统计稳定。
64MiB同流程通过不能替代512MiB、三同步副本／3FS、P2P或复杂故障验收。

### 限定白盒诊断

DFS 外部后端路径复用已有 OpenTelemetry tracing。普通运行保持 tracing 默认关闭；
诊断可显式配置 `trace_enabled=true`、OTLP gRPC `trace_endpoint` 和
`trace_sample_ratio=1.0`。采集前固定输出预算，检查导出失败／丢弃计数，正常停止
AFS 并排空导出后再停止 collector。全采样结果用于归因，不能直接替代关闭 tracing
时的正式性能成绩。

Linux 上可按需构建测试辅助接收器：

```bash
cargo build --locked --release -p afs --example afs-trace-receiver --no-default-features --features dfs
target/release/examples/afs-trace-receiver --listen 127.0.0.1:4317 --output spans.jsonl --max-output-bytes 67108864
```

它位于 `afs/tests/support/trace_receiver.rs`，使用新建文件和有界 JSONL 输出，支持正常
SIGTERM 排空；不是默认产品 bin，也不属于普通试用包。部署与分时对照编排仍在仓外
测试方案中，产品不依赖此工具。

观测范围包括 FUSE 排队和请求大小、dirty 预算触发的准备／上传／发布、S3 worker
排队、读端授权／缓存分支／整块装载／摘要校验／分段复制，以及 Meta 合并提交的
fork／clone／sort／编码字节／etcd CAS。跨线程和 RPC 保留 trace 关联，合并提交通过
links 关联多个请求；不能把多请求共有批次、嵌套 span 或并行 PUT 时间重复相加。
处理字节是逻辑读取／哈希／复制量，不代表物理磁盘 I/O；不同 VM 的时间戳不能直接
相减来计算单向网络时延。
