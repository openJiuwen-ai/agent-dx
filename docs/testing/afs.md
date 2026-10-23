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

## DFS 双口径测量与定向回归

维护入口为[双口径驱动](../../build/e2e/afs/perf/README.md)，由仓外fixture提供固定候选、
挂载、身份及已有后端；该驱动不部署SeaweedFS、PostgreSQL或VM。

- `contract`：双方`O_DIRECT`、内核预读及对象预取关闭，比较自身读引擎。
- `product`：双方普通buffered读取，记录各自真实缓存／预读配置。零FUSE READ的页缓存
  收益只属于该系列，不能用于解释历史O_DIRECT成绩。
- 每方每系列三次、分时固定顺序，ready后静置30秒，复读固定等待1秒。完整E2E由A单调
  时钟覆盖写入至B首次完整SHA／EOF ACK，不用阶段相加或扣S3常数。所有原值和max/min保留，
  `max/min > 1.10`标为波动，不追加有利样本；功能、测量完成、稳定性和性能达标分开登记。

```sh
python3 build/e2e/afs/perf/dfs_compare.py --config /absolute/private/config.json --validate-only
python3 build/e2e/afs/perf/dfs_compare.py --config /absolute/private/config.json --series both
python3 -m unittest discover -s build/e2e/afs/perf -p 'test_*.py' -v
```

Linux定向回归至少核对：同版本重开不新增正文READ；同长、变长、变短及空版本失效；
LOOKUP旧长度与OPEN新版本的竞争；旧READ延迟回复不污染新OPEN；授权拒绝与独立期限；
并发刷新、关闭、未知提交与顺序屏障；source/reader淘汰不续TTL及在途Arc生命周期。
真实FUSE测试在具备root、`/dev/fuse`和正常卸载条件的Linux上显式执行：

```sh
cargo test --locked -p afs --all-features --test fuse_contract dfs_buffered_ -- --ignored --test-threads=1
cargo test --locked -p afs --all-features --lib node::vfs::dfs::tests::p1_ -- --test-threads=1
cargo test --locked -p afs --all-features --lib node::dfs_read::tests
```

单VM测试后端通过不等于三VM产品验收。元数据表锁计数、register/snapshot/release压力
及共享指针断言分别说明其范围，不能冒充全链仅两把锁、跨VM文件I/O或已达性能门槛。
新版本不自动继承旧候选性能；BLAKE3、权限、fsync和close-to-open合同继续独立验证。

### 已知验证欠账与测量边界

真实 DFS 跨句柄合同已放入 `afs/tests/fuse_contract.rs`：
`real_dfs_keep_cache_contract::dfs_real_nodes_cross_handle_keep_cache_and_permission`。
外部流程先准备同一 Linux 主机上的 local-file Meta 和两个真实 DFS Node、mTLS、R1，
B 预读为 0；再在 A 的专属子目录设置非 root 调用者权限，传入 B 的对应子目录。
不要修改受保护的 DFS 根目录来准备权限。测试本身不部署服务，不在 libtest 内降权。

整个测试进程以非 root 身份运行，并提供三个必需参数；缺失参数、非真实 DFS 挂载、
metrics 缺失或异常均明确失败。`AFS_DFS_FUSE_WRITER_MOUNT` 和
`AFS_DFS_FUSE_READER_MOUNT` 是挂载下的绝对目录，必须属于两个不同的真实 DFS mount；
`AFS_DFS_FUSE_READER_METRICS` 是 B 的 HTTP `IP:port`。测试所在网络须直连该 metrics
地址；使用 loopback fixture 时，子进程的 `NO_PROXY`／`no_proxy` 需覆盖两个 Node 的地址。
外部流程把已构建的测试 ELF 整体以非 root 身份执行：

```sh
"$FUSE_CONTRACT_TEST_ELF" real_dfs_keep_cache_contract::dfs_real_nodes_cross_handle_keep_cache_and_permission --ignored --exact --nocapture
```

合同核对 H1 部分读 v1、A 同 inode 覆盖并 fsync 发布 v2、H2 重开读 v2、H1 未缓存尾页、
H2 全量校验后 H3 全读且正文 READ 增量 0，以及 chmod 000 拒绝新的非 root OPEN 后恢复。
完整内容、BLAKE3、长度、EOF 和 READ 增量分别保留。READ 零增量仅支持 KEEP 行为，
没有直接采集 FOPEN flag，也不承诺旧句柄固定快照或立即撤销已授权句柄。
单主机两真实 Node 的功能通过不等于跨 VM 性能或完整 POSIX 通过。

V1挂载显式配置`fuse_max_readahead_bytes=0`以隔离可见性，属于功能诊断，不是product
默认预读性能样本。16MiB校验阶段约4097次READ不能据此判断“1MiB预读或max_pages失效”。
下一轮product测量前仍须记录应用read／pread大小、实际FUSE回调大小与次数、内核
read_ahead_kb、协商max_pages及挂载参数；不从配置或一次功能测试推断真实请求粒度。

生产默认32worker已有有限合同回归，但真实Node中FUSE／Peer／私有结果同时活跃峰值
仍未知。32应用线程RSS观察不等于32个后端请求同时运行，也不证明整个Node内存上限
或峰值后回收。全局 proof total／resident gauges 与 events 必须一起采集；cache 局部
metadata 不能代替全账。有限资源诊断可以报告采样最大值和容量全扫事件增量，不能把
32 个应用调用当作真实范围请求分母，也不能把采样最大值当作精确在途峰值。缺少范围
请求分母时将降级率记为未知，不用 0 个事件伪造完整比例。区分配置估算、局部单测和
运行观测，不将历史通过默认扩展到未覆盖的并发峰值、RSS 或回收结论。

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
| S3签名策略 | 省略配置默认signed；显式unsigned仍有SigV4 Authorization，普通PUT路径／Content-Length／完整正文不变；HTTP错误不能生成成功receipt；完整BLAKE3和fsync屏障保持 |
| 持久发布 | PUT 失败不发布新 head／长度；确认成功后另一节点读取完整内容；外部模式不新增 Node 耐久副本 |
| 窗口与重放 | 真实4MiB满块跨 write 预上传，所有命令共享四路上限且完成一路立即补位；乱序回执不串窗口，部分失败保留身份与成功回执，Meta 未知 ACK 精确重放并阻挡后续 mutation |
| 同步与修改 | Full／DataOnly、同步写、可写 flush、覆盖／稀疏写／截断和正常退出保持；成功 fsync 等待入口之前已接纳的整个 inode 序号（包括其它 FD）的所有窗口对象回执和 Meta 持久确认。外部本机 dirty read 先完成同一有序发布屏障，再经已发布读取；发布失败报错且不交付旧或部分输出。无 dirty 不增加 Meta 写事务，非外部 DFS 保留原 overlay |
| 跨窗口与预算 | 旧 PUT 进行时普通顺序 write 可接纳下一窗口；旧 ACK 不丢 active 内容／killpriv；readonly fsync 可同步同inode本地writer，readonly flush不代提交；未知 ACK 冻结重放；flush／drain／O_SYNC 不只提交首窗。成功 PUT 可退休正文，但最后 partial read／HTTP／retry pin 释放才退账，提交描述跨未知 Meta ACK 保留；Uploaded 仍计逻辑64MiB窗口而不是空洞。pending 分配前准入、publisher 转换拒绝不吸收未接纳正文、全部释放后归零 |
| 读正确性 | 完整 SHA 一致、完整长度／摘要拒绝短长响应及内容损坏、错误不能发布部分输出；cache 命中不能替代权限、当前版本和期限检查 |
| 已发布数据恢复 | A 正常退出后 B 清空客户端内容缓存并从 S3 回源；local-file Meta 正常全停重启后重新打开读取 |
| 满窗准入 | 正文已退休且前窗尚 Preparing 时，满 active 窗口不得以1字节继续接纳；推进原提交链后再冻结。连续33个4MiB块应保持完整分块，每次 publication delta 不超过64MiB；无进展或失败保留错误与已接纳序号 |
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
| 驻留语义 | Node 共享最多4MiB已验证对象，预算不足回退；每次 open 在线观察最新 head；每 callback 仍检查有界授权、身份、范围和期限。RAM 命中允许延后发现磁盘副本变化，新磁盘加载必须再检查权限和完整摘要 |
| 有界版本授权 | 同一 Meta read view 核验版本归属、live session、对象／范围，按需建立最长5秒上下文；单调截止从 RPC 发出计时，命中／心跳不续期，超过期限不可交付输出 |
| 期限与生命周期 | 有效范围复用、过期在线核验、断链有效／到期、Meta 持久重启后新准入；已知失效与输出串行化，停止后不得通过心跳复活；128项／1MiB保守预留含在途引用 |
| 拒绝与回退 | 错误 inode／namespace／layout、MAC、epoch、过期及越界拒绝；新版本接口的权限拒绝／timeout／其他错误不回退或同次重签；保留旧接口原拒绝语义、不 GET、不修改输出 |
| 原路径隔离 | 空洞／dirty 写状态、P2P 保留原路径；一致版本视图的 EOF 不要求逐读 head RPC；外部 S3 实验不改变 OwnerFs bind 语义 |
| 校验线程 | 官方串行／并行完整摘要一致，请求范围外损坏仍拒绝；GET／Ready 共用私有两线程池、单对象 gate；停止后拒绝新校验、实际 worker panic 传播并显式 join，失败不被退出成功掩盖 |
| Ready对象内流水线 | 固定1MiB段边界、空对象／短尾完整BLAKE3一致，保留原allocation；受控验证三处相邻段read／hash重叠，并核对真实trace；中间／尾段损坏、短读／超长、后段读取和worker错误不生成已验证载荷，报错前等待已接纳工作；raw分配前预留、与pending／pin共用原预算并安全转移；恢复不递归取锁，预算不足不删文件或绕道GET；退出等待已接纳任务 |
| 独占Ready复用／owned结果 | 只回收唯一载荷，reader／fill pin阻止回收；原capacity跨raw／Verified交接连续计费，短尾、较大对象回退和失败退账；损坏／短读／超长不能交付；owned结果覆盖多range／空洞／短尾与预算拒绝，最终过期／停止／失效检查与借用路径共用，借用输出失败不变；FUSE直接使用owned回复，其他后端默认适配保留行为 |

入口满时应真实等待；timeout、ticket 放弃及 Drop 不留下独立客户端任务。HTTP
连接复用不能替代并发／字节准入。载荷预算、cache 内容额度和进程 RSS 是不同指标，
不能用事后 RSS 或四路并发证明字节上限。旧 generation、索引及前台临时读取缓冲
须另计环境预算，不能把 cache 额度当成整个目录或文件系统配额。

Linux 受影响回归可从现有模块入口执行；按改动选择过滤项，不为文档变化重跑：

```sh
cargo test --locked -p afs --lib --no-default-features --features dfs node::external_chunk::tests::
cargo test --locked -p afs --lib --no-default-features --features dfs node::dfs_read::tests::
cargo test --locked -p afs --lib --no-default-features --features dfs external_version
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
```

Node 复制、Meta 条件事务、精确重放及 local-file 恢复的已有回归按受影响范围复用；
上面的两个模块命令不代表这些其他模块、完整 POSIX 或 Full 已通过。

### 性能与实验边界

外部存储对接的功能验收不包含性能达标承诺。性能测试与产品功能分别登记；
实验部署、比较环境、原始数据和临时探针保存在产品源码树外。

### 限定白盒诊断

DFS 外部后端路径复用已有 OpenTelemetry tracing。普通运行保持 tracing 默认关闭；
诊断可显式配置 `trace_enabled=true`、OTLP gRPC `trace_endpoint` 和
`trace_sample_ratio=1.0`。采集前固定输出预算，检查导出失败／丢弃计数，正常停止
AFS 并排空导出后再停止 collector。全采样结果用于归因，不能直接替代关闭 tracing
时的正式性能成绩。

使用管理员已有的兼容 OTLP gRPC collector；产品不提供或部署实验接收器。

观测范围包括 FUSE 排队和请求大小、dirty 预算触发的准备／上传／发布、S3 worker
排队、读端授权／缓存分支／整块装载／摘要校验／分段复制，以及 Meta 合并提交的
历史快照候选的fork／clone／sort／编码字节／etcd CAS；当前canonical候选记录操作日志append，不混用旧阶段字段。跨线程和 RPC 保留 trace 关联，合并提交通过
links 关联多个请求；不能把多请求共有批次、嵌套 span 或并行 PUT 时间重复相加。
处理字节是逻辑读取／哈希／复制量，不代表物理磁盘 I/O；不同 VM 的时间戳不能直接
相减来计算单向网络时延。

需要细分载荷交接时，使用 `node.fuse.read/write.execute` 与对应的
`allocate/input_copy/reply`，区分请求执行、分配／复制和内核回复。
写侧 `node.dfs.chunk.stage` 下分别记录 `vec_to_arc` 与完整 `hash`；
Ready 装载分别记录私有文件检查、预算／分配、verifier gate 和同对象分段读／校验。
`pipeline_scope` 包含并行读／hash及scope等待，不能当作纯等待或CPU时间。
span引用可能被异步后代持有，尤其写请求的外层关联span不等于callback返回时延；
按短寿命执行／回复边界和应用syscall计时核对，不直接累加父子或重叠区间。


## DFS 外部缓存扫描与回源观测

缓存策略的必要回归包括：任务／pending拒绝时旧Ready文件保持、可选resident
释放与caller pin继续计费、两候选取较旧者、唯一／无候选及Pending排除、删除
失败不扣容量且可重试。空body分片后仍须等待真实EOF，短读、超长、流错误和
BLAKE3错误不得产生成功载荷；并发观测快照不能倒退累计计数。以上受影响检查
在Linux运行，完整POSIX、复杂故障或正式性能须单独验收。

正常性能轮通过`/metrics`采集以下固定标签指标，无需开启全量OTLP：

| 指标 | 口径 |
| --- | --- |
| `afs_dfs_s3_get_stage_seconds{phase}` | queue、headers_sdk_aggregate、body_first_poll_wait、body_poll_wait_total、body_poll_wait_max、body_copy、body_final_eof_wait；first／max是body等待的子集，不能再次相加 |
| `afs_dfs_external_verify_seconds{source}` | get包含gate＋完整hash；ready包含gate＋加载＋完整hash，均为wall时间 |
| `afs_dfs_external_verify_bytes_total{source}` | 成功完整校验字节，source为get／ready；失败不计为成功校验 |
| `afs_dfs_s3_request_reply_seconds{operation}` | 调用者等待worker回复，包含嵌套工作，不与GET阶段再次相加 |
| `afs_dfs_s3_cache_admit_seconds{result}` | attempted是尝试准入而非保证接受，另有no_cache |
| `afs_dfs_s3_cache_activity_total{event}` | admissions、evictions、task_rejects、pending_budget_rejects、capacity_rejects；capacity包括零长度或单对象超过配置限额，pending为聚合内存不足 |
| `afs_dfs_s3_get_body_bytes_total{result}` | 成功GET完整收流字节，不是应用全部读取字节 |
| `afs_dfs_s3_slow_gets_total{result}` | 所有达到50ms慢GET阈值的成功请求数；日志每60秒最多16条，因此条数可能更少 |
| `afs_dfs_s3_get_failures_total{stage,result}` | headers／body／metadata／verify错误及read timeout；计数只证明失败阶段，不提供完整失败耗时 |

原生Peer读另外提供`afs_dfs_read_scan_bytes_total`、`afs_dfs_read_scans_total`和
`afs_dfs_read_copy_bytes_total{stage}`。scan记录实际完整根扫描循环的逻辑文件读取／BLAKE3
输入，含失败前工作，不等于物理磁盘IO；覆盖组工作另按真实执行统计，不能把没有scan
计数当作没有本地IO或hash。copy按自有交接入口记录，包括scan_to_range、frame_to_attempt、
attempt_to_staging、staging_to_output、outboard_range_delivery、cache_to_output及
peer_outboard_metadata；proof小元数据复制与正文复制分别解释，不含codec／TLS／内核复制，
也不是内存峰值。不得换算成E2E收益或与嵌套累计耗时直接相加。
本机和A端范围请求可使用可信标准根绑定的32KiB覆盖组；legacy、缺／坏sidecar或资源不足
保留完整根扫描及原范围checksum。B私有结果仍在成功前不公开，复杂布局保持原回退。
完整BLAKE3、授权和错误传播保持。原生DFS新open仍取得fresh inode；已打开句柄
复用已提交读视图最多5秒，截止从fresh请求前开始并受原Node/session期限约束，
命中和心跳不延长本次视图。到期重新取得head，本机发布、truncate、getattr等已有
刷新点使旧视图失效；不承诺旧句柄每次read立即看到其他客户端修改，也不是永久快照。
read开始和最终交付均检查session/epoch、停止及原截止，borrowed输出在成功前不修改。
普通Meta断链不会伪造撤销，但新open、head刷新、source miss或授权刷新需要Meta时
失败直接报错。read/grant拒绝清除受影响上下文，Node注册拒绝、epoch变化或停止才
停止共享授权。源缓存默认max5秒、grant安全余量1秒，命中不续期；A授权缓存最多5秒，
原grant及Node／读取上下文截止不延长。

A端Peer服务仅复用当前被请求的
已验证不可变对象，不预读下一个对象；B的可选纯磁盘缓存单独验收。对象首次完整BLAKE3通过后
才可进入有界LRU；命中仍核对catalog、Durable、device/epoch、record revision及当前物理文件的
dev/ino/size/mtime/ctime，变化则失效重验。命中交付的是已验证的内存字节，不能声称每次
命中都重新检测磁盘后来发生的静默损坏；缓存不是读授权，逐range授权与范围checksum保留。

A端每个LocalChunkStore默认共用16MiB Peer正文额度、2个LRU条目，每个驻留对象最多4MiB；
这是已批准的原8MiB额度调整。填充、驻留、淘汰后仍被发送流持有的对象、范围回退正文和
RDMA packed正文，均按实际capacity计费至最后一个载荷所有者释放；native gRPC额度不足
按入口固定的`dfs_peer_serve_wait_ms`有界等待，默认20ms，0禁等；RDMA packed仍立即准入。单飞表最多8个active key，完整对象可共享一次填充；partial／无完整Ready结果不承诺
只扫描一次。原8个read slots、每流4个64KiB待发送帧及扫描scratch仍受原上限约束，
需单独计入进程资源预算；16MiB不是总RSS，也不是每个请求额外获得16MiB。大于4MiB对象
保留范围读取，协议最大请求不代表当前额度一定能准入。fsync与持久提交路径不变。

R-6定向回归应通过屏障证明：不同对象能同时填充、同完整对象8请求只填充一次；旧Ready
follower不移除新代健康resident；packed准入跨越关闭时，shutdown不得在真实lease释放前
假成功；帧／退役对象及零字节可增长lease的最后引用必须保留计费／排空责任。已知停止或
等待后过期不得发送Header，原根／I/O失败不能被并发停止掩盖。首次关闭起10秒期限不续期，
超时报告错误且不detach真实工作。这些局部证明不替代多VM、实际RDMA硬件或正式性能。

`afs_dfs_peer_read_payload_bytes`和`afs_dfs_peer_read_payload_peak_bytes`分别报告当前
计费载荷及历史峰值；`afs_dfs_peer_read_payload_reserved_bytes_total`／
`afs_dfs_peer_read_payload_released_bytes_total`记录准入和最终释放容量。
`afs_dfs_peer_read_object_cache_total{result="hit|miss"}`仅记录固定标签的槽查询结果。
`afs_dfs_peer_serve_resources{resource}`补充residents、flights、readers、payload_limit_bytes、
fixed_control_bytes、control_bytes、control_peak_bytes、control_limit_bytes固定标签。
控制元数据包络64KiB计入内联表、ID／record capacity、有界终态错误与Arc包装；控制owner
末引用才退账，shutdown后实际动态占用须为0。类型／capacity账不是实测allocator或RSS。
这些指标不授予权限、不执行准入；LRU驻留并非泄漏，取消／排空及最终引用退账须用针对性
回归分别确认，不能仅看RSS推断。

原生gRPC正文帧使用Bytes持有原PeerReadPayload：保留Data frame后取消流、淘汰
缓存槽，必须继续计费直到最后一个frame／对象引用释放；范围fallback遵守同一规则。
授权与帧生产共用一个blocking任务，授权拒绝仍在返回stream之前发生；取消发生在
不可中断授权期间，permit必须保持到任务真实退出，已观察取消后不再读取正文。
回归应同时覆盖slice地址共享、最后引用退账、预算耗尽、损坏无Header、wire兼容、
遗漏Completion及错误checksum拒绝。帧仍为64KiB、队列仍为4，完整校验不省略。

`afs_dfs_peer_read_events_total{event}`按固定标签记录blocking_authorize、
blocking_produce、blocking_combined任务提交，以及data_frame、body_vec_allocation、
shared_frame正文事件。旧独立阶段标签在合并路径上为0；任务提交不代表已经完成，
帧事件不代表消费方成功交付。与copy／scan／RPC计数同窗口核对，不从计数换算耗时，
也不能以正文帧共享宣称codec、TLS或内核复制消失。

`afs_dfs_read_rpc_calls_total{method}`记录客户端get_inode、get_file_version、
get_chunk_sources、validate_read_grants和peer_read_ranges调用尝试，包含失败。
按Node和操作窗口取增量；get_inode还包含open/getattr等调用，不能把其总数当作
read-head次数，也不能把客户端尝试当作服务端成功次数。指标不含所有DFS控制RPC。

64MiB验证保留全缓存重复读；数据规模超过既定磁盘缓存时，第二遍称repeat，
不能继续称全暖缓存。双方预读关闭，固定接口、数据、并发、缓存、持久屏障及
p95判据，分时运行；实际GET次数／字节、整体时延、调用p50／p95／p99和阶段
指标分别记录。不通过挑种子、增加缓存或单边trace扣减来宣称性能收益。

缓存淘汰回归须覆盖同秒／跨秒两候选选择、命中刷新、首次Ready及重启索引初始化、
128对象／64容量扫描、预算与pin、删除失败回滚。新策略只使用1秒单调访问桶，
不改变完整校验、最终授权或硬准入额度。独立ON诊断沿用lookup／admit／fill spans；
新增桶与选择原因仅记录每cache前1024次成功淘汰，复用原计数并输出截断数，
不分配事件缓冲。fill的written_bytes只统计成功完整写入，outcome区分失败，
不能把写入成功等同持久保证或将接纳当Ready完成。诊断不替代OFF完整E2E对照。

## 原生 Peer 磁盘读缓存验收

本节是当前WI-6实现的受影响检查清单，不表示正式验收、性能达标或历史结论自动继承。
仍须绑定候选源码／二进制及Linux运行证据。开启`dfs_peer_cache`后分别检查miss回填→重开
命中、覆盖写后fresh open、truncate及partial grant不扩大范围、授权到期／session拒绝、
损坏／短文件安全回源、淘汰／恢复、unsafe目录拒绝和正常停止。B的durable目录应为空，
Meta仍只有原A持久副本，缓存不能提高副本数或掩盖真实Meta／Peer错误。

算法／格式回归必须覆盖官方标准BLAKE3根与32KiB Bao覆盖组，不能以独立段摘要或CRC
替代。encoded proof为64B格式／长度／根header及parent CV，4MiB对象最大8192B；未知
格式、超限、截断、Meta根／长度／对象重放或正文与CV不一致拒绝。检查4KiB小读、未对齐、
跨组、尾组、空／短对象、组内损坏、文件身份变化与未请求组损坏延后检测。实际read尝试、
读取字节、hash及组到私有结果的复制均需记录，不把组数当系统调用数，不再要求旧的
1MiB对齐直读零复制。连续对齐范围核对一次逻辑file read、官方validator scratch复制及
标准根；合法短读／中断仍按实际尝试计数，非对齐／多range保持安全路径。hash／身份／
最终授权失败不得公开私有输出。

B缓存v2固定4096B header＋正文＋proof trailer；正文offset固定，trailer最多8192B，
无优化证明时可保守完整根扫描。旧v1可丢弃缓存退休重建；持久副本旧／缺／坏sidecar不能
当缓存删除，必须保留完整根验证。原生规范ChunkId的根／长度锚与Meta布局及catalog一致，
legacy ID保持全扫；核对无新增Meta RPC。原生写ACK只等待原正文／目录／catalog屏障，
可选sidecar安装在后台。用阻塞住的installer证明ACK仍能成功，不用“还没来得及安装”
推断解耦；另核对ACK后崩溃的正文／catalog恢复、按需修复及完整BLAKE3。
只有测试明确等待installer idle后，才能断言sidecar存在或覆盖组扫描量；安装前完整核根
回退仍合法，不能把这个成本从首次读性能中扣掉。核对queue=1/32/64、queued＋active上限、
原ledger内control／proof和最后引用退账；健康proof不重写／rename／file sync。
注入安装／目录sync错误、控制准入和线程启动失败，ACK与必需屏障仍保持；停止拒新、
绝对截止、真实join及watchdog失败需分别证明。optional失败不降低正文／catalog barrier、
receipt或fsync持久性。
读侧另测健康sidecar的多对象Peer／Pinned读取：write、rename、file sync、directory sync
均为0；热Peer正文LRU命中不访问／修复sidecar，逐出后完整扫描才按需修复；
多个独立reader面对同对象缺失／损坏，只作有界共享修复，并证明修复期间Peer
正文slot已释放。不得把写批次必要file sync一起删掉。读侧容量错误保留健康proof，
安全全扫不触发proof重建或持久化；缺坏修复仍不得改写正文／catalog。
S3不生成本次原生写proof，原S3窗口、载荷／descriptor预算和有序发布回归保持独立。

同一Node原1MiB proof ledger共用于A store、native写pool及B cache；检查实际proof容量、
Arc／包装、构建／encode／decode临时分配、4096项证明指针索引（当前64位构建32KiB），
以及在途、缓存替换、eviction、shutdown和最后clone释放。预算0／被其它持有者占满必须
保留完整根安全读取，不能暗增池、先释放仍在途的分配或制造可选缓存满的新读失败。
未使用的CV不能标成已验证证明；格式／身份结构错误即使预算不足仍拒绝。
补自然多对象驻留压力：A多个pinned reader与B缓存proof共用总额一半的resident子限，
不能只手工占满账本模拟结果。验证新writer仍有机会生成proof、工作租约有余量，resident
引用退役至最后owner才退账；并发超过总额时允许安全fallback，不能要求32路全部快路径。
范围工作租约须贯穿proof加载到验证，不重复预留或跨账本使用；容量不足、损坏与普通
I/O错误分开验证。总账／resident用量、预留失败、原生写无proof及范围全扫退化分列，
cache局部metadata指标不能替代全账。当前用量／限额是
`afs_dfs_outboard_metadata_bytes`、`afs_dfs_outboard_metadata_limit_bytes`及
`afs_dfs_outboard_resident_bytes`、`afs_dfs_outboard_resident_limit_bytes`；固定事件
`afs_dfs_outboard_events_total{event}`区分`reserve_total_failed`、`reserve_resident_failed`、
`write_without_proof`、`range_fullscan_fallback`与`range_fullscan_unretained`。最后一项包括
B退休proof后失去完整文件身份的安全全扫，不能误判为容量错误触发修复。
proof ledger不等于RSS，原codec／TLS／网络envelope及磁盘配额分别核算。
resident接近512KiB时，每张76KiB工作票据使剩余额度约支持6路同时验证（不计其他临时
占用）；这只是量级估算，不是固定并发门槛。更多重叠请求可能走整块扫描，小范围读
因此产生正文I/O／hash放大；后续性能采集须按相同请求窗口记录
`afs_dfs_outboard_events_total{event="range_fullscan_fallback"}`增量及范围读尝试数，
说明计数边界后再计算退化比例，不能从32个worker配置推断实际退化率。

版本cache验证entries和保留bytes双上限、碎片化大layout、extent／index／ID字符串的
capacity、同key替换及LRU；不能只按len估算。超大版本经真实加载可读但不进入缓存；
淘汰后的活跃Arc正确性保持，不将cache账本下降当成所有内存释放。现有超大版本用例
证明可用性而非低成本；未缓存版本的重复Meta全布局RPC和索引重建仍是已知限制。
entries和bytes组合不足容器保守开销时可能完全无法接纳条目，配置关系见部署说明，
不能以参数各自合法推断缓存已启用或有效命中。


协议回归检查request tag7 `include_blake3_outboard`默认false、header tag8只在显式请求
且完整对象时附带；普通无cache完整读、合法partial、旧Peer缺字段继续原安全路径。
`DfsReadRangesHeader.blake3_outboard`与`DfsPutReplicaFrame.data`生成Rust字段为Bytes，
需检查仓内调用和所有权；wire tag兼容不等于Rust源码API完全兼容。
partial不扩大grant或正文：A按可信根验证本地覆盖组，B保留原range checksum，不冒称B完成
独立整对象根证明。A header的Bytes共享原proof owner；B先检查边界／绑定、同账本预留后
只复制小proof元数据并退役opaque解码Bytes；codec产生的wire copy仍属原transport预算。

双块gRPC在Header首正文前显式固定核根模式，不能只凭Some(builder)判定资格。完整
peer tree且预算可用时，consumer唯一正文根计算同时匹配Meta锚定整对象根及Completion；
缺proof／预算不足／旧Peer保持producer完整BLAKE3安全路径。模式不在中途切换，producer
始终核对顺序、长度、deadline、Completion及真实EOF。typed有账Builder随原Header移交，
consumer按实际块做outboard更新、私有落盘和提取；两个256KiB块和单项交接队列保持。
Completion只登记待确认状态；真实EOF／成功join、长度／摘要及私有sink／输出绑定全部
通过后，consumer finish核对实际根／CV并最终授权，才能Ready和公开输出。
旧Peer没有proof但本地取得有账Tree时，consumer独占正文核根；Tree不可用则producer
独占完整根，consumer不再保留root-only构建器。producer hash为0不是所有混合版本的承诺。
两种模式都检查真实工作量及回收；缓存写失败仍完成完整核根，不因producer hash为0
冒称未校验，也不把标准outboard计算当免费。
针对性检查超过两块的真实复用、32KiB组／256KiB块边界、尾块、Header／Completion／额外帧、
late Status、跨FD／对象／私有输出重放、坏Meta根、满队列取消、总deadline及真实join退账。
consumer错误或无效join不得留下证明；保留current-thread、过大窗口及小批预算回退。
缓存创建、配额或落盘失败仍用同次完整校验RPC交付合法区间，不增加正文RAM缓存或预取。

`afs_dfs_read_hash_bytes_total{stage}`按实际执行区分root_in_receive、root_fallback、
outboard_body_hash、outboard_parent_hash、cache_segment_hash及cache_full_hash等路径。
parent计实际Bao父合并的CV输入，不冒称BLAKE3内部全部compression；`cache_segment_hash`
是兼容指标名，本候选表示覆盖组正文hash，不再代表1MiB独立摘要。删除的旧段索引扫描不
继续登记为免费工作或沿用旧字节值；若fallback保留两遍实际扫描则两遍都计。
`peer_outboard_metadata`记录B的proof元数据复制，`outboard_range_delivery`和`cache_to_output`
记录各自消费路径的实际用户态复制，含官方validator scratch；这些兼容名不只表示最终
输出交接，文件到私有结果的内核搬运另列，不宣称直读为0。失败前已做的工作也记录，
不代表验证成功。
`afs_dfs_peer_disk_cache_metadata_bytes`／`metadata_peak_bytes`观察cache持有的真实证明
元数据，不能代替Node共享ledger总量或RSS；同Node其他proof持有者须独立核对。
`afs_dfs_peer_disk_cache_data_read_calls_total`记录正文实际read尝试，含失败／中断，不含
header和尾部EOF探测。`stage_nanoseconds_total`／`stage_observations_total`按operation
聚合准备、正文读取、校验及结束检查，legacy `segment_verify`在本候选表示outboard校验。
低频计时／计数也有成本，不从完整E2E扣减，不把两读合计伪拆为每次读。

R-5并发回归用屏障证明阻塞fill不阻塞独立hit、四个同对象请求只填充一次、每请求最终
授权独立、leader个体失效不撤销合法follower。共享flight失败不能替代另一请求的授权、
期限或source结论；合法follower经既有有界范围读独立回源并完整校验，不重新加入仍被
引用的终态flight。另测leader异常drop／请求级超时／另一source损坏后的独立读取，以及
follower自身停止时不得重试或交付；同时覆盖pin退休／同key替换、外来文件、
删除失败、flight上限及停止唤醒。Ready等待后的复查和取得fill许可后的复查遇到可选缓存
I/O错误时均应安全回源，不能吞掉真正Peer／授权错误。shutdown指标归零须同时检查
实际ledger used/resident及活动持有者，不能只将gauge写成0。

`afs_dfs_peer_disk_cache_resources{resource}`使用固定标签：operations、flights、fill_body_bytes、
indexed_entries、retired_entries、retired_disk_bytes、control_bytes。fill_body_bytes是显式准入的
逻辑4MiB额度，不是实测RSS或已分配正文；retired_disk_bytes在实际删除前保持收费。
proof与资源gauge在同一短表锁内发布，操作hash／read／copy统计使用本操作账本累加，
不能用并发全局前后差推算单请求工作。锁持有分位数与真实分配观测须单列，不能以
无锁依赖屏障或逻辑包络代替；当前局部回归不构成多VM、FUSE性能或完整资源验收。

填充链`afs_dfs_peer_disk_cache_fill_stage_nanoseconds_total{stage}`及对应observations
记录lock_wait、lookup、prepare、receive_verify、store、deliver和total。lock_wait还覆盖
首次root检查；lookup不含此前Meta sources RPC。receive_verify含完整Peer调用、解码、
复制、双lane校验、私有写盘、Completion和真实EOF，不是纯网络；流式store只含最后Ready
提交。父子阶段及重叠不能相加；prepare的多段观察次数不是fill次数。
`fill_stage_thread_cpu_nanoseconds_total`及对应observations只含同步consumer线程，
不含远端、producer及h2／TLS后台线程；wall减threadCPU不能命名成网络／磁盘等待。

并发回填以确定性屏障验证两个不同对象可同时进入独立pending、同对象只填充一次、
legacy独占、超限及取消不泄漏。实际RPC回归检查活动流固定复用同一Channel、两个块
总capacity为512KiB、控制额度为256KiB；连接LRU淘汰不能另开活动会话。最后块仍被持有
时reservation不能归零；最后释放后旧会话即使还有测试引用也不能保留载荷分配。
原`stream_reserved_bytes`／`stream_reserved_peak_bytes`按共享会话观察一次4MiB流式额度，
不按并发请求重复增加；`stream_private_peak_bytes`为单操作私有输出加包络的峰值观察，
不是Node并发输出总量或RSS上限。`stream_frame_bytes`／`stream_frame_peak_bytes`
只观察两个块的真实capacity，归还池仍占账，最后owner销毁才释放。控制余量与transport
分别核算，指标不执行准入。旧完整Vec的fill_bytes与流式reservation互斥，不合并为同次峰值。
`peer_frame_to_block`是帧到块复制，`fill_to_output`是请求区间提取；没有完整Vec不等于零复制。
`afs_dfs_peer_read_events_total`的stream_frame包含控制帧，stream_block_alloc记录实际分配，
stream_block_handoff／stream_block_write记录块交接／sink调用，不等于物理write或成功落盘。
旧64KiB Data分支中，64MiB完整4MiB对象的预期1024帧、256次块交接仍须以实际
回调与尾块核对。R-4健康proof分支采用32KiB已验证帧，预期2048帧，不能混用两条路径。

功能检查后再固定数据、并发、cache容量、双方预读0、持久屏障及repeat间隔做分时性能
对照。诊断轮与正式轮分开；若两读间未采样，只报告合计工作。首次回源、稳定磁盘命中、
超cache repeat分别登记，不借旧版本结论宣称收益。64MiB本地单时钟可由A在create前起，
覆盖发布和B首次完整读校验ACK；若总时间含固定1秒repeat等待必须注明，阶段和仅作辅助。
测试专用控制适配器留在本地测试方案，不进产品包。

## 源元数据批量与有界缓存验证

R-1只前瞻源元数据，双方关闭正文预读的contract配置仍有效。必要回归应覆盖：
当前mandatory优先、总128项、同版本后续16对象一次拿源但只有当前正文ops、0禁用、
512B以上advisory跳过、缺源时精确错误码限定一次mandatory重验、真正权限／会话错误
不被可选降级吞掉。请求前mono／wall锚点、短grant不驻留、慢回复和external延迟安装
不重赠期限，均以受控时钟／屏障验证，不能只测默认数值。

已知失效的交错必须分别证明：其他版本／peer的健康缓存不被误清；旧在途回复不恢复
已清的源；新远端epoch回复不能混入本批此前已命中的旧源，须丢弃所有旧命中并对全部
当前mandatory至多重新授权一次。Node与device两种epoch、持续变化的Unavailable和
最终授权／私有输出失败原子性均保留。没有实时watcher，不把未知变化宣传成即时获知。

`afs_dfs_source_cache_resources{resource="entries|bytes|peak_bytes|limit_bytes"}`使用固定标签，
分别记录实际驻留条目、保守容量估算、高水位和配置字节限。capacity包含两份key、
所有嵌套String／Vec及索引包络；空表须实际释放后才报0，Drop／失效及native/external
同账分别核对。4MiB不涵盖当前RPC reply、读取clone或进程RSS。正式中期检查点逐A/B、
逐before／after_first／after_repeat检查四个series存在及实际值；漏采是证据不完整，
不补0。B首读`get_chunk_sources≤1`、1秒复读0和A授权防回退分别核对；单测计数不代替
真实FUSE计数，次数减少不等于控制关键路径或完整E2E已经改善。

## 原生对象调度与磁盘预读回归

`dfs_readahead_max_bytes`默认0，范围0..33554432；0不创建后台任务，contract固定0。
product如需验证ON，测前显式设为8388608并由实际ELF的`--print-config`确认；A/B有效
值、配置和二进制SHA都要留证。原生只读句柄按固定版本上的顺序访问增长1／2／4窗口，
距上次结尾至多1MiB，随机跳转清零；对象数按4MiB保守折算并封顶8。页缓存、内核
read-ahead、源元数据lookahead与正文预读分别记录，不能用“配置请求ON”冒充真实后台工作。

`dfs_fetch::ObjectFetcher`的前台分片及后台任务复用唯一FillFlight对象表，不另建正文
对象缓存。必要的Linux回归包括：

- 同对象前后台单飞、不同对象实际sink重叠；跨对象请求仍保持失败不公开部分结果，所有
  分片必须join并最终核权后才复制输出，回退前释放旧scratch，不多持一整份正文。
- 默认0的后台任务／Peer调用严格为0；ON按顺序窗口实际填入可丢弃磁盘缓存，缺失、
  partial、过期或失效grant不查询Meta、不取得flight、不读正文；随机和版本变化使旧scope失效。
- 后台首次poll遇cache、Channel、stream状态锁或任务／proof／磁盘容量不足立即放弃，
  不等待、不驱逐健康条目争空间；前台等待者优先。前台排队使用原读截止和原授权，不能续时。
- 正文只用共享两块256KiB，最多两个流复用同一Channel和connection≤2MiB窗口，
  每流窗口≤2MiB、解码消息≤128KiB；私有输出沿原单批预算，后台空输出绑定不申请假4MiB。
- 每帧、consumer和磁盘commit前重查来源generation、句柄scope、已知拒绝及Node停止；
  receipt已完成后再失效也不得安装Ready。最后核权与rename沿用既有非原子边界，不承诺
  新的全局撤销协议；缓存后续交付仍独立授权。
- 静默网络、块／控制等待和慢消费者期间取消能唤醒并真实join；先drop receiver、取消／
  结束producer，再join实际任务，不能detach或提前退账。取消不能中断已进入内核的磁盘I/O。
- 固定状态、handle容器、任务上下文和字符串实际capacity从原proof总账／resident子限收费，
  额度不足安全降级，最后持有者释放前不退账。S3 pending36MiB不得挪用。

针对性入口按受影响范围选择，不把命令列出等同于已经通过：

```sh
cargo test --locked -p afs --lib --all-features r7_
cargo test --locked -p afs --lib --all-features node::dfs_fetch::tests::
cargo test --locked -p afs --test config_contract --all-features
```

正常测量采集`afs_dfs_fetch_events_total{event}`的scheduled、skip_disabled、skip_budget、
skip_grant、skip_busy、joined、cancelled、failed，单位是事件次数；
`afs_dfs_fetch_resources{resource}`的jobs／job_limit是数量，metadata_bytes／
metadata_peak_bytes是原账本内的当前／高水位字节，不是新增额度或RSS。实际构造前
job_limit可能为0。任务数不等于正文并发，指标本身不执行准入，也不证明持续峰值。

新候选可在[双口径驱动](../../build/e2e/afs/perf/README.md)上显式选择
`fetch_metric_profile="r7"`并保留`metric_profile="p2"`，逐B端before／after_first／
after_repeat核对全部固定标签；漏采、非有限值或缺快照为证据不完整，不补0。旧P1M
不强求新字段。先形成Linux回归，再以单独候选身份做有限FUSE及分时contract／product
测试；局部并发证明、默认OFF或ON工具接线不替代正式读侧验收与性能目标。

## 原生写 buffer 验证

以下是受影响定向检查入口；通过数与候选身份以各轮实际 Linux 记录为准，不替代完整 POSIX、
可靠性、正式安装包或性能验收。

```sh
cargo test --locked -p afs --lib write_buffer_tests
cargo test --locked -p afs --lib native_builder_
cargo test --locked -p afs --lib p2_w1_ -- --test-threads=1
cargo test --locked -p afs --lib dispatch_tests
cargo test --locked -p afs --test config_contract
cargo test --locked -p afs --bin afs-node allocator::tests
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
```

`p2_w1_`比较原生区间索引与旧 extent 日志的确定性写入／截断／冻结／恢复序列，核对内容、
write_seq、Chunk 划分及实际输入复制次数；另测16MiB中十万次对齐覆盖的活跃区间上界。
该结构上界不外推任意字节边界，也不是全部分配或RSS实测；S3／legacy沿用原日志，
其预算与恢复须由受影响的`deferred_publication_`回归单独核对。

真实 FUSE 使用本地 owner、NodeReplicated R1、无 S3：六次应用1MiB写入合计6MiB，记录内核实际
回调大小和中途提交轨迹；满4MiB块先冻结预上传，2MiB尾块在原生发布入口冻结，
无中途发布时同一版本包含4MiB＋2MiB有效正文。复制入口合计6MiB、冻结地址保持，
远端重新打开核对 SHA／长度／EOF。随后偏移8192覆盖4KiB，验证小 Chunk、新布局和旧 Chunk 不变；
另验证未冻结时同位置原地覆盖。若后台中途发布，功能结果与“同批两正文”结构验收必须分开登记。

`afs_dfs_write_buffer_*` 记录实际 live/idle/peak capacity、分配、复用、释放和输入 copy；
`input_copy_bytes_total`／`input_copies_total` 来自实际 slice-copy/extend 入口，不能当端到端总复制。
诊断轮设 `log_level="debug"`，固定 `fuse_write_admission` 和 `dfs.write_buffer.*` 事件关联真实回调、
buffer offset、世代、冻结前后指针与最后引用；不记录正文。hash、网络、SDK、读快照和 profiler 抽样
分别记账，诊断时延不作正常性能收益。常规回收轮保持日志低等级、trace OFF。

`afs_node_allocator_*` 将 jemalloc 的 allocated/active/resident/retained、后台开关、decay 和观测有效位
分别公开；它们不等于 AFS live/idle 或 Linux RSS。峰值测试测前固定池基线、实际触页 live 峰值、
RSS允许偏差和观测窗口，完成原提交入口、释放所有引用后核对 live下降、idle≤基线、超额释放和RSS回落；
重复负载不得逐轮累积。慢副本、读取引用和未知提交必须继续持有正文，不能为回落提前退账。

必须补充不对齐／跨块／稀疏／截断扩展／并发覆盖、合法短写与同步标志、prepare失败恢复和
未知 Meta ACK 精确重试、停止排空，以及受影响 OwnerFs／RN／RDMA／S3 回归。完整BLAKE3、授权、
成功 fsync 和 close-to-open 不因这些优化放松；RDMA仍按现有独立注册 endpoint 缓冲合同验证。

## Peer HTTP/2接收帧验证

`dfs_peer_receive_cap_reduces_actual_http2_data_frames`用原有64KiB业务流式协议分别经
显式16KiB控制Channel和生产PeerConnectionPool交付同一128KiB正文，先核正文、原解码器、
EOF和单次RPC，再比较透明代理按HTTP/2头读取的非空DATA帧。代理只用于机制断言，不以其
耗时或CPU作为产品收益；流控、消息、正文、控制和并发预算必须保持。

## Peer 完整对象唯一核根回归

Header固定consumer Tree或producer/plain checksum，之后不切换。分别验证新Peer证明、
legacy无证明但有预算、root-only／零预算三条路径：实际正文根hash输入恰为对象长度，
正常consumer的producer工作为0；缓存覆盖组和parent CV另外计量，不漏计也不重复算根。
完整流必须覆盖Header、所有Data、Completion、真实EOF和join；迟到错误、Meta根／wire根
不符、篡改、输出绑定和授权失败仍拒绝。同步完整结果入口与双缓冲入口都需覆盖。
`root_fallback`事件是次数，同名hash stage仍为字节，不能通过改计数掩盖实际重复执行。

## 未对齐覆盖组读取回归

保留原对齐一次正文读和32KiB官方scratch防回退；非对齐1MiB及两个非连续512KiB范围，
用真实positioned读次数和复制量证明不再逐组编码／解码。完整覆盖组hash量不能少计；
测试首尾未请求padding、中部篡改、短尾、越界与预算末引用。源／输出别名、超过128个
范围及无完整中部的小读取仍保持旧codec及写入顺序，不宣称所有范围均走直读。
所有结果都在私有输出内验证；根、文件身份及最终授权未通过不得交付。
validator补读边界时，verify_ns包含该I/O，read_ns另记录实际读取，二者不能排他相加。
局部读／复制次数不等于完整E2E性能达标。

## 原生读粗粒度诊断

原生读粗诊断由默认OFF的`dfs_read_diagnostics_enabled`显式开启，使用固定有界对象聚合。双块诊断按实际核根者记录producer完整根或consumer的outboard校验，并记录写入／提取；legacy `SegmentBlake3`／root_segment字段在本候选表示后者，成功join且两条lane都确有工作时才计算真实区间交集；单一核根者下没有另一条root工作，不能伪造重叠或有效零值。空、时钟失败或固定区间容量溢出同样不得记成有效零重叠。它不保存逐帧事件或正文，不增加载荷额度。ON时延不作OFF成绩，聚合hash／交集不从完整E2E扣减；同步线程CPU降低也不等于全Node CPU降低。

同一schema4响应还可包含`object_capacity`／`object_overflow`／`object_records`：固定120个
24B槽，记录A端每对象读取／核验及B端实际索引命中的读取／核验区间，miss不占槽。
它与原256条请求记录及两个既有工作区合计受同一64KiB静态上限约束，溢出不覆盖旧槽。
记录的56位ChunkId摘要只用于有界诊断关联，有碰撞局限，不是对象身份／授权依据；
active、成功及错误退出分开，缺失端点或旧版没有这些字段均为未知，不能当0。
A区间不含此前的Meta授权，B区间不含此前的源解析；使用各自单调时钟核对父子边界，
不能跨节点相减或把并行区间之和当作E2E关键路径。扫描区间也不等于可以全部消除的时延。

仅在导出已开启的诊断快照时采集`sampled_monotonic_ns`和`process_cpu_ns`（可为空）；
后者为本Node进程累计user＋sys CPU，不是线程或组件独占CPU。前后差值只对应采样窗口；
A窗口若包含写入发布须明确注明，不能改称仅首次读CPU。正式性能轮次保持诊断OFF，
诊断单次数据不用于证明稳定提速或跨版本时延变化的因果关系。


通过现有Node管理HTTP的 `GET /v1/diagnostics/dfs-read-timing` 在测量窗口外读取固定快照。
先检查schema、容量、overflow、clock_failures及每条success/completed；该进程前256条之后不轮转，
因此长时测量记录不完整时只能登记缺口。正文收齐只表示最后预期Data帧已解码，不能当作真实EOF、
校验完成或授权交付。Meta、FUSE及对象父阶段按实际嵌套与重叠核对，不能直接相加；
consumer线程CPU不含producer或远端CPU。快照不保留正文／路径／授权信息，也不是业务授权来源。
开启配置及监听范围见[部署说明](../deployment/afs.md#原生读粗粒度诊断)。

schema 4 在原有 256 条请求记录内补充 Peer 服务、持久化批次和写侧 Meta 调用，
并用固定的 24 个粗阶段聚合记录 slot／cache 锁等待、帧交接、正文／目录／catalog
屏障、proof 安装及 Meta 客户端调用。诊断元数据仍受原 64KiB 上限约束；正式性能
运行保持关闭，独立短诊断使用前后快照核对，记录容量不足时明确登记不完整。
`first_data_ns` 在 A 表示首帧交入本地发送队列，在 B 表示首个非空正文帧解码完成；
它们都不代表线上发送时间。粗阶段的 `wall_ns` 是可能重叠的累计尝试耗时，
首尾时间只是包络；不能把包络当连续忙碌时间，也不能与其请求父阶段重复相加。
Meta 区间包含请求准备、排队／传输和结果转换，不是纯 Meta 服务端或网络耗时。
各 Node 的单调时钟分别分析；同一轴上的父子区间取并集／交集，不能跨 Node 相减。

## 共用 Node 的受影响验证

默认构建、OwnerFs-only 和 DFS-only 的条件编译需要分别检查；all-features 通过不能代替
OwnerFs-only 严格 lint。Node allocator 是进程级配置，OwnerFs 库／Peer 测试不能单独证明
真实 `afs-node` 在 jemalloc 下的挂载、排空或 RSS 行为。受影响时补同一候选的有限读写、
fsync、重开、卸载与正常退出，并记录 allocator 生效和 RSS；不要求 RSS 精确等于空闲池基线，
也不把有限运行称为 OwnerFs 性能达标。

```sh
python3 -m unittest discover -s build/release/tests -v
cargo clippy --locked -p afs --no-default-features --features ownerfs --all-targets -- -D warnings
cargo clippy --locked -p afs --no-default-features --features dfs --all-targets -- -D warnings
```

共用 FUSE 读写路径有常规 tracing spans；`dfs_read_diagnostics_enabled=false` 只关闭额外的
有界诊断账本，不表示关闭所有 span／时钟／指标。未测量时不宣称这些观测没有性能成本。

Meta 快照增长与版本回滚限制见[部署说明](../deployment/afs.md#meta-快照升级与回滚边界)。

## Peer 覆盖组发送回归

健康sidecar整对象请求使用两块32KiB最终帧交替读取及标准根验证。针对性入口：

```sh
cargo test --locked -p afs --lib p2_r4_
cargo test --locked -p afs --lib dfs_stream_whole_digest_reuse_preserves_wire_and_partial_hash_work
cargo test --locked -p afs --lib peer_read_event_metrics_are_fixed_and_shared
```

覆盖损坏首组零正文、损坏后组只有已验证前缀且无Completion、慢消费者两帧真实背压、
Bytes末引用与预算退账、stop／调用者失效退出、catalog／路径置换、可选proof损坏退休及
缺proof／容量不足的Header前回退。正常完整流不先做整对象扫描，B整对象核根保持。
固定事件`stream_verified_group`、`stream_group_corrupt`、`stream_proof_retired`只累计次数；
`data_frame`按实际帧计，64MiB全部走该路径时为2048个32KiB帧，不能复用旧1024帧假设。
`peer_read_verify`粗聚合为实际组读／验证，未含全部回压／身份核查，不能冒充整个Peer父项；
新路径不产生旧整对象私有读取compact记录，缺失不解释为耗时零。正常性能关闭额外诊断，
独立诊断的组工作与发送父项会交叠，完整E2E按同条件实测。

## 原生 catalog 尾事务恢复回归

`LocalCatalog::recover`只处理物理EOF处无换行且JSON解析为不完整的尾事务：保留此前
有效前缀，截断并同步后可重新追加和恢复。完整JSON无末尾换行仍原样接受，后续追加
补分隔符；非法UTF-8、完整语法／字段、checksum、revision以及带换行的不完整记录
都应拒绝启动。恢复不删除正文孤儿，截断或sync失败明确报错。

```sh
cargo test --locked -p afs --lib --all-features catalog_tail_recovery_tests
```

覆盖事务头、记录中部、记录间及checksum尾截断；部分多字节UTF-8与真正非法字节分别
处理，并核对完整无换行→append→restart。复用原ACK／persist／精确重放回归，证明
正文／目录／catalog屏障和未知Meta提交阻挡后续修改未降低。尾恢复只是恢复前置，
不能替代组提交、停止排空或性能验收。

### 原生持久化组提交回归

```sh
cargo test --locked -p afs --lib --all-features p2_w3_ -- --test-threads=1
cargo test --locked -p afs --lib --all-features p2_w0_
cargo test --locked -p afs --lib --all-features immutable_chunk_retry_
```

受控同批须让16个单对象请求在取批前都完成正文持久化，再证明共享一次目录／WAL及父目录
屏障、同一提交revision，且屏障前无ACK；非受控并发只报告真实批数，不要求恰好一组。
同ID安装／重放／quarantine与旧reader、新inode替换交错必须安全；阻塞磁盘I/O时其他
catalog元数据操作仍能进展。首组ACK不依赖后续请求全部耗尽。

队列检查queued加active，1MiB控制账在真实引用释放前不退；超过额度的原大批保持整批
独占fallback，不永等不可能满足的许可。失败注入分别核验WAL／同步／父目录屏障前无
成功ACK、失败锁存后健康重放也报错且不新增WAL字节、重开后按原尾恢复合同处理。
还须覆盖修复rename后目录失败、同批后续正文失败和正常drop／重开，证明健康重放
不能跳过目录屏障。完整且checksum有效的WAL（含无末换行）重开时也必须执行文件
同步，失败拒绝open；完整损坏仍先拒绝，不走同步成功快路。停止必须允许最后dirty提交，再关闭准入并唤醒／排空等待者。计数和软件回归不替代实际
Node生命周期、RSS、完整E2E或性能门槛；未运行的场景须单独保留待验收。

## Native Peer 准入等待回归

`cargo test --locked -p afs --lib --all-features p2_t1_`覆盖8个真实RPC槽已占满时
首次poll等待、释放后真实流完成、0禁等、固定期限／spurious通知、排队后grant到期、
stop及最后Bytes切片释放。预算check/wait之间的交错用屏障验证，不能只凭睡眠猜测；
配置合同另核对默认20ms、CLI/TOML优先级和0..50ms范围。R-4、native Peer和RDMA
packed软件回归保留；单元RPC不等于FUSE／多VM性能或真实verbs验收。
同一入口的RPC槽、flight和control/payload共享原deadline，重试／Header前回退不重赠
20ms；已开始的I/O、同flight结果和R-4双帧背压受原授权／stop／传输合同约束。
大窗口和多连接仍后置；当前两个流共享一个Channel及原≤2MiB connection窗口，
不能把小窗口等待通过宣传成16MiB connection／4MiB stream或多连接验收。

## 原生写入准入、预上传与有序发布回归

原生路径与 S3 原预算分别验证；不能用参数存在、基元单测或编译成功代替整组运行通过。
保留原16MiB提交阈值tests-only RED，后续核对普通write不触发该旧入口、满4MiB只预上传，
成功fsync覆盖先前接纳的所有序号。一次提交只等已冻结的 `through_seq`，持续生产不使
旧提交无限等待；未知结果用原OperationId／精确请求重放，write／resize／setattr不能越过。

分别覆盖本机Required且实际持久ACK验证后的 Uploaded 回读，以及无本机副本时正文
保留至发布；按根完整校验，失败不交付部分输出。慢上传、读快照、冻结请求和最后共享
slice期间不得提前退live，实际capacity、空闲池、元数据actual／reserved和RSS分列。
另用已有大base布局加一次小覆盖，核对不会把原可提交文件变成4MiB元数据门槛错误；
记录既有完整layout/domain并存份数不增加。新增描述符与工作区受限，不冒称全部布局／RPC受限。
冷池／热池、FIFO取消／并发顺序、预算等待不持inode锁、描述符压力、无副本live压力、
定时busy补做、stop唤醒与真实任务join都须有针对性证据。连续超过预算的写入须证明
能推进，不能只测试不满预算的64MiB；S3有界窗口、OwnerFs和RDMA受影响范围另登记。

正式功能核对需记录真实FUSE callback大小、6MiB的4＋2MiB几何、正文输入复制、
冻结前后地址／capacity／代际、提交后4KiB覆盖、提交前原地覆盖，以及远端重开SHA／
长度／EOF。正式性能与诊断分开，保留固定计时和缓存条件；未运行的资源与性能项继续待验收。


### 原生上传错误归属与终态回归

区分产品runtime与no-runtime同步推进：后者因预算等待而推进其他inode上传时，
业务失败仍必须交还真实owner，不能污染当前write／fsync；结构故障不能伪装成功。
两个inode共享同一pipeline，以确定性失败验证owner保留原OperationId／不可变正文，
健康inode不继承外来错误；任务取消或失败不能提前退仍被持有的正文额度。

receipt安装的实际容量拒绝属于已记录的业务失败，不是外层结构错误。
结构错误回归必须命中真正提前返回的分支，并核对已取走任务后的joining、任务表、
等待者唤醒和owner失败，不能用容量拒绝通过替代该证明。锁中毒时不能强行恢复为健康，
后续同步／drain须明确失败而非无限等待，原请求与正文保留，未成功发布前不得生成成功回执。

handle错误cursor与对象尝试错误分别验证；恢复后仍可能消费历史异步错误，不能规定
任意固定重试次数必定成功。正常drain首个inode提交失败可中止后续inode发布，必须返回
失败；该best-effort边界不降低成功fsync持久性，也不等于真实持FD dirty停止已验收。

## Meta 操作日志的有限组件回归

阶段功能范围、改动导航、优化前后工程摸底与未完成项见[Meta P3阶段交付报告](afs-meta-p3.md)。

Linux 上复用 workspace Cargo 入口：

```sh
cargo test --locked -p afs --lib p3_initialization
cargo test --locked -p afs --lib p3_recovery_
cargo test --locked -p afs --lib p3_lifecycle
cargo test --locked -p afs --lib grant_lifecycle
cargo test --locked -p afs --lib p3_configured
cargo test --locked -p afs --lib p3_production_recovery
cargo test --locked -p afs --lib p3_checkpoint
cargo test --locked -p afs --lib p3_segments
cargo test --locked -p afs --lib p3_maintenance
cargo test --locked -p afs --lib p3_operational
cargo test --locked -p afs --lib p3_history
cargo test --locked -p afs --lib p3_retention
cargo test --locked -p afs --lib p3_views
cargo test --locked -p afs --lib p3_lock_owner
cargo test --locked -p afs --lib p3_watch_
cargo test --locked -p afs --test meta_log_contract
cargo test --locked -p afs --test config_contract
```

`p3_initialization` 覆盖初始化中断后的身份保持，以及未知／非空残留拒绝；
`p3_recovery_` 覆盖缺 CURRENT、新非空 WAL 未列入选择集及精确空预备段。
重复六次 image rename 后 SIGKILL 另验证孤儿镜像不会耗尽后续安装额度，
外来／损坏镜像不被误删。`p3_lifecycle` 验证已完成命令／ACK 的有界成对回收、
引用退休和恢复重建，保留 pending／failed／活动屏障及旧 session 拒绝记录。
这些新增例不构成完整权威实体回收或物理掉电认证。

`meta.rs` 共享启动工厂测试检查实际后端选择、原会话／原结果重开、易失 memory、
坏完整记录及旧格式拒绝。占用端口的反例确保恢复失败先于 bind；身份策略仍先于后端访问。
`meta_log_contract` 通过领域 API 验证 create／rename／unlink、固定读视图、watch 次序、
重开与原结果幂等重放，确认不新增长期重试记录。配置测试覆盖默认、CLI 优先和非法上限。
测试数据应在 guest 原生文件系统的显式 TMPDIR 中，不把容器默认 overlay 当成 ext4 证据。

上述入口分别提供组件及下述进程故障证据，不等于物理掉电、FUSE A 写 B 读、完整 P3 或性能验收。
`p3_checkpoint` 另外检查自包含状态编码、显式安装后真实 LocalLog／Store 重开、
后续记录不重复应用、原结果／完成时间／epoch 保留、损坏快照安全回退、身份与
CURRENT 校验拒绝、临时文件计账及调用方取消后的单飞所有权。
`p3_checkpoint_process_crash_matrix` 在实际测试子进程中覆盖半写image、image已安装、
CURRENT切换后、退休段删除中，以及新空段选择前／后六个SIGKILL位置；核对原ACK／结果、
完整状态和后续追加。它是同内核进程故障验证，不能替代物理掉电或真实产品FUSE运行。
`p3_segments` 检查小阈值轮转、三个checkpoint后
旧段文件实际删除、最新快照损坏时从上一份补齐全部提交，以及文件数容量拒绝前
文件内容不变、缺少必需段则拒绝恢复。此处未专项测量物理空间峰值／回收延迟。
`p3_maintenance` 检查无手动调用的后台安装、写前等待不追加／不发布、超时后仍健康，
与已实际写入却返回StorageFull的未知结果不重试；另检查memory移除已覆盖记录和
CURRENT临时文件采用独立清单上限。它不替代真实磁盘满／完整资源窗口及进程故障验证。
`p3_operational` 进一步检查真实LocalLog四槽压力下24笔确认可继续、观察期限后仍保持
原快照任务／捕获所有权、捕获额度超限在install前拒绝且不毒化权威。四槽用例的文件数／
字节是提交后的采样，进程故障用例记录文件分配块字节；均不等于大场景磁盘峰值或RSS。
P3-4修改成组后已执行完整默认AFS库门禁；后续P3-5已补管理历史回收组件回归，
完整产品及资源验收仍待完成。旧格式和失败证据保持原身份，不据组件通过提升完整验收结论。

`p3_history` 的选择器回归分别检查原完成时间／条数／字节、受保护请求和每次工作上限，
时间回退时事件仍按完整revision前缀处理。状态／实际LocalLog回归检查确定清理的精确
代际、业务revision不变而日志位置推进、WAL重开和checkpoint＋后缀不复活旧结果，
并核对后续事件序号、Compacted重扫及错误关联清理不部分发布业务前缀。
`p3_retention` 检查 owned String／Vec capacity 的完整计账及独立结构预留、计数／字节
拒绝、恢复重建索引、业务关联清理持久后可见、空闲及持续重复请求期间的定时清理，
以及取消调用者仍由写者持有额度、未知ACK后停止后续管理记录。
排队重试回归通过持久 ACK 门控使历史清理与待组批重试确定性交错，核对原结果不被
再次执行；另检查同身份引用计数、末引用释放、退休冲突和身份字节上限。新事件计账只访问本次
新增尾部，不遍历旧历史前缀；原完成时间不因查询、重试或重启而延长。
`p3_retention_local_runtime` 在真实LocalLog上运行Store actor，检查空闲到期的管理记录
持久后checkpoint／重开不复活结果，以及降额启动时先持久清理、再次放宽后仍保持原水位。
这两项是同进程正常释放重开，不是产品CLI、跨进程故障或物理掉电证明。
`p3_views` 核对主状态owned容量的增量计账／恢复、同State共享、跨代际数量／字节
拒绝、空闲期限回收、执行中引用不提前退账、已知权威失效，以及并发快速创建／丢弃时
登记元数据仍有界。普通写者不被旧视图准入拒绝阻塞；期限后的读不得交付成功。
`p3_lock_owner` 用真实duplicated fd验证最后创建进程持有者解锁，不依赖额外fd关闭，
并模拟外来进程归属，验证全部I/O在调度／改盘前拒绝、外来Drop不解开父进程的锁。
它证明的是描述符生命周期合同，不单独证明某次历史锁失败必然由fork造成。
`p3_watch_` 覆盖复制前的条数／owned字节拒绝、请求limit=0仍有限、超限revision整体回退
及不伪造Compacted。响应持有回归检查独立条数／字节准入、固定登记表、取消／EOF退账、
空闲队列主动释放、旧代际不误释新槽位，以及已过期构建者退出前不提前退账。
真实RPC用例有意保留完全未poll的流31秒，之后必须报错且不交付旧命令；并非测试卡住。
Root命令接口契约及受影响真实gRPC丢ACK修复用例补充核对接线与错误语义。
这些是有限组件证据，不覆盖公共RPC／socket缓冲的整体资源、完整产品运行及资源验收，
不等于24h长稳或新的性能结论；新记录／image不继承旧版本运行结论。

P3-6有限配置回归包括默认local-file、退役选项拒绝、TOML／CLI覆盖和非法范围。
共享启动工厂的有限运行测试另核对实际历史条数／空闲过期、恢复后不复活、
短周期checkpoint与WAL分段；序列化配置正确不替代运行接线证据。
旧快照StoreBackend、etcd／Redis适配和专属测试已按clean-break移除；保留现有领域、健康故障和丢ACK重放断言并改用canonical日志。旧源码与历史结论仍按原版本保留。配置／组件通过不代表真实产品或P3-6全部完成。

### Meta指标与旧工具接线回归

```sh
python3 -m unittest discover -s build/e2e/afs/acceptance -p test_ownerfs_meta_storage.py -v
cargo test --locked -p afs --test config_contract p3_cli_retired_backend_errors_explain_safe_migration
cargo test --locked -p afs --lib p3_metrics_
cargo test --locked -p afs --lib p3_operational_
```

纯Python只证明local-file参数／路径在准入、初启与重启一致以及旧选项拒绝；
真实进程CLI检查迁移指引、exit2与help/version，均不等于真实OwnerFs挂载恢复。
指标回归核对成功ACK前后计数、失败不发布、history条数／逻辑字节／删除量、独立Registry
以及Registry不pin权威状态。原checkpoint取消、observer超时和capture拒绝用例同时核对
在途及成败：observer超时但owned任务仍在执行时，不得提前记任务失败或退在途。
最新有限软件门禁与此前运行分别记录身份；不据这些断言宣布RSS、性能或24h长稳通过。
指标名称和重启口径见[部署说明](../deployment/afs.md#meta操作日志指标)。

## Meta canonical 向量容量回归

`p3_root_deactivation_accepts_accumulated_grants_below_byte_budget` 通过实际Store事务
积累65537份grant，再调用Root停用入口，核对一笔revision移除活动Root，
完整帧小于4KiB、旧grant仍可留待GC，以及MemoryLog的framed恢复／原结果。
旧版本曾用一笔事务删除全部grant；该历史结论不能当作当前回收机制。该夹具不是65537次真实授权RPC，
也不替代同规模LocalLog、真实FUSE或物理掉电验证。

`p3_record_` 四项回归核对原四种向量的合计槽／结构字节额度不增加、
有长度提示的分配前拒绝、无提示postcard的有界渐进分配、写者与恢复端的几何容量一致，
以及原12字段序列与schema不变。该共享额度不包括嵌套载荷／整个进程RSS；
完整编码仍独立受单帧字节上限约束。

`p3_a27_typed_byte_limits_reject_atomically_and_reopen_controls`使用真实LocalLog，
分别构造512项layout、64份grant和128份copy。测前固定4KiB拒绝／64KiB接纳对照，
拒绝后核对原公开Arc／revision／所有目录文件字节不变；同一输入在接纳对照提交、
幂等确认、释放旧句柄并重开后全字段一致。这是typed Store夹具，不是完整
DfsService的receipt／placement验证。
`p3_a27_ready_group_keeps_failed_requests_out_of_durable_authority`核对6个ready请求中，
apply_txn合同错误、canonical深度错误、字节超限和false condition均不产生成功结果，
正常首尾两笔只用一次物理追加，失败权威不泄漏，重开状态及原结果一致。
这些回归不证明任意规模合法操作都能在固定结构额度内接纳；大事务的支持范围
仍须与资源合同一起核对，不能以测试通过替代该交付判断。

```sh
cargo test --locked --release -p afs --lib --all-features meta::store::tests::p3_root_deactivation_accepts_accumulated_grants_below_byte_budget -- --exact
cargo test --locked --release -p afs --lib --all-features meta::store::log::tests::p3_record_
```


## OwnerFs epoch 与有界 grant 清理回归

```sh
cargo test --locked --release -p afs --lib --all-features grant_lifecycle -- --nocapture
```

针对性用例覆盖：abort/reopen 后 epoch 不复用、并发不同 Root 预留、epoch 耗尽的原子拒绝，
历史 grant 不放大停用记录，Home 恢复后精确选择新 session/generation，以及有界扫描/删除、
同 key 重新 acquire 的两种写者顺序、过期当前 Home 的保留、过期/已替代 holder session 回收，
以及降低单记录额度时小批继续收敛、准备失败不修改公开权威。
真实 LocalLog 用例核对后台清理、checkpoint、释放旧写者后重开及同名 Root 重建拒绝旧凭证。
这些用例不替代真实 FUSE/bind 运行、物理掉电、性能或完整 P3 验收；此前候选保持原证据身份。

## Meta 事务资源边界与有限交付回归

业务条件合法和资源可接纳分别判断。当前保持单帧64MiB、批次128MiB、
canonical共享262144槽及由四类结构宽度计算的原向量容量预算；并不提供无界原子事务。
Node普通本地写路径另有既存1024个未发布descriptor及4MiB元数据预算，
外部存储路径继续使用其独立窗口／载荷和descriptor预算。
这些不是本次回归新增的业务上限，也不能被当作整个进程RSS限制。

一次DFS发布新增K个receipt、每份C_i个copy时，主要权威变更为
`3 + Σ(2 + C_i + repair_i)`；继承及新layout的extent仍放在一个Layout值中，
不能把它们错误地计成同数量的顶层canonical条目。字段／标识长度仍计入实际编码字节；
Root停用只写Root删除和撤权命令，旧grant的物理回收另走有界管理事务。
namespace、租约和单对象管理操作不因全库对象数量增长而自动变成批量删除。

Linux回归入口：

```sh
cargo test --locked --release -p afs --test meta_contract --all-features p3_supported_commit_shapes_survive_local_log_reopen -- --exact --nocapture
cargo test --locked --release -p afs --test meta_log_contract --all-features
```

前者经过实际RPC适配器、领域receipt／placement核验、canonical和真实LocalLog：
核对1024个receipt的单副本／三副本，以及一个layout包含65537个extent，
每次只发布一笔版本、原请求重试不追加、重开恢复inode/layout和原结果。
ACK来自可信Node测试夹具，不声称真实TCP、正文持久化或物理三副本运行。
字节超限整笔拒绝／组内失败使用前述`p3_a27_`回归；不能把接纳测试当作超限测试。

有限交付须明确当前已验证配置和工作流。任意更大复制拓扑、长期累积的placement／inode字段、
直接构造的超大Store事务仍须遵守帧、结构及历史预算；未证明的组合不能宣称全部支持。
Full FSx和FUN/REL完整harness的未执行状态保持，已有标准、组件和故障证据只能按
版本及改动影响复用；该有限出口不等于完整P3、完整POSIX或正式性能通过。


## 原生CRC与写入代次GC定向检查

原生DFS Node默认启用CRC和写入代次保护，保留capability本地处理及省略逐chunk登记。`generation_`检查覆盖库存保护、无关GC变化、确切代次退休、检查后退休竞态、回复丢失重放、迟到准入与墓碑清理，以及重试保留旧收据时所有出生代次仍须有效。`META_DFS_GC_CHANGED`在CRC路径指相关代次缺失/退休；不再把无关全局GC变化当作失败。`meta::dfs::gc_fence_tests::`同时保留显式BLAKE3对照路径的原条件事务检查。

`crc32c_`检查覆盖描述及Meta wire、CRC向量/末段、独立ID、原ID复制/修复、catalog重开、跨段读取、损坏/缺表/错表/截断拒绝、局部覆盖、截短/扩容、部分成功后失败重试，以及远端Header/Completion身份和范围。all-features还核对RDMA CRC Completion与Meta绑定；这不是RDMA硬件运行。`meta_capability_`保持原inode映射及错误顺序，`pr88_fix_`保留组合开关和尾块重试检查。Rust检查只在Linux执行。

```sh
cargo test --release -p afs --lib generation_ -- --test-threads=1
cargo test --release -p afs --lib crc32c_ --all-features -- --test-threads=1
cargo test --release -p afs --lib meta::dfs::gc_fence_tests:: -- --test-threads=1
cargo test --release -p afs --lib pr88_fix_ -- --test-threads=1
cargo test --release -p afs --lib meta_capability_ -- --test-threads=1
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

### 1GiB多副本、跨节点读与正常修复

`build/e2e/afs/scripts/dfs/crc32c_general.py`复用现有OwnerFs TLS/生命周期和三节点DFS工具。在独立Linux网络及挂载namespace内启动Meta与A/B/C；写入1GiB单文件（1024次串行1MiB pwrite），同fd真实fsync，desired=3、sync-required=2、min-distinct-nodes=2、local-copy=required。正常GC始终启用，修复worker补齐第三份；不手工复制或停GC。随后启动空数据目录的D，完整读回1GiB及非对齐/EOF范围，核对数据面流量、内容SHA和三份物理正文/catalog。正常退出必须真实wait0并无遗留挂载。

```sh
sudo unshare --mount --net --propagation private sh -c '
  ip link set lo up
  exec python3 build/e2e/afs/scripts/dfs/crc32c_general.py \
    --repo "$PWD" --binary-dir "$PWD/out/bin" \
    --work-root /path/to/fresh-crc-general-run --require-defaults
'
```

需预先构建Linux产品二进制，work-root必须全新，具备`/dev/fuse`、OpenSSL、iproute2和至少5GiB可用空间；运行前排除端口及已有服务冲突。证据目录包含配置、二进制身份、公共指标、实际字节、修复完成日志、物理副本内容及结果。背景提交周期在此固定60000ms以避免定时发布切分单次验收。该驱动是限定功能/持久化E2E，不是吞吐前后对照，也不证明多VM、物理故障域、断电或HA。性能收益须另有相同完整业务完成点的比较，不能引用历史单副本倍率当作本轮结果。


### PR88审阅修复：组合开关和尾块重试

`pr88_fix_`定向检查同时开启Meta最终验号和CRC新写：先写入并FullSync，再单独截短、扩容并FullSync，检查长度、已有内容、扩容补零和每代固定写入保护凭据。纯长度变更在准备提交时取得凭据，并在准备失败后的重试中保留它。

尾块检查将两个小块真实持久化，模拟第二份收据丢失，再次FullSync；要求第一块不重复提交、第二块沿用原ID、最终只有两个本地对象且完整内容正确，重试不重新获取GC凭据。尾块的不可变上传状态在等待I/O前保存在冻结范围中；仍按原顺序等待，不增加后台可见范围或并行补投。这些是定向正确性检查，本轮未重跑1GiB性能实验。


### PR93长度变更准备失败后的代次身份

`pr93_fix_`在Linux复用真实Meta gRPC服务及Store事务夹具（Meta使用MemoryLog），分别覆盖截短和扩容。CRC原正文真实落盘并发布后，新代次登记成功，注入一次准备失败；重试必须使用原保护携带的operation，而不是另造代次。检查旧head在失败时保持不变、原代次引用无逐chunk登记、重试通过Meta真实验号并原子退休原引用，以及最终长度、保留内容与扩容补零。它是组件正确性检查，不是FUSE/断电或性能验收。

```sh
cargo test --release -p afs --lib pr93_fix_ -- --test-threads=1
```
