# Agent FS（AFS）

Agent FS（AFS）是 Agent DX 的可选文件系统子系统，为工作负载提供
OwnerFs 与 DistributedFs（DFS）两种文件访问模式。`afs-meta` 管理命名空间、
租约、文件版本、布局与副本目录；`afs-node` 管理 FUSE 挂载、OwnerFs Home
访问、DFS 数据与节点间传输。文件系统权威状态与 Platform Environment 生命周期
保持独立，只复用 ADX 的构建、发布与进程管理入口。

AFS 默认不进入普通 ADX 构建和发布包；通过 `ADX_WITH_AFS=1` 显式启用。
当前部署必须使用 mTLS，并把每张受信节点证书精确绑定到一个 Node ID。
DFS Peer 客户端允许接收 128 KiB HTTP/2 DATA 帧；业务消息、流控窗口、正文预算、
并发和校验合同保持不变，对端仍可发送更小的帧。

![AFS 架构总览](../assets/architecture/afs.svg)

详细路径见[AFS 架构与时序图](../docs/architecture/afs.md)。

## 组件范围

| 目录 | 内容 |
| --- | --- |
| `src/` | `afs-meta`、`afs-node`、OwnerFs、DFS、Meta 后端、FUSE 适配和运行时入口 |
| `client/` | DFS 客户端 crate |
| `common/` | 错误、日志、指标、协议、追踪和传输等共享 crate |
| `examples/` | crate 级示例配置；发布包示例另见 `build/config/examples/afs/` |
| `tests/` | Cargo 绑定的单元、契约和小型集成测试 |

来源、路径映射和许可证记录保存在 `docs/migration/`；它们不定义当前运行合同。
FUSE 依赖使用根工作区声明的官方固定 `fuser =0.18.0`。

## 模式选择与运行边界

- `fs = "ownerfs"`：小规模 Agent workspace，以 Home 节点真实目录为权威，
  远端 Node 经授权 peer 访问。
- `fs = "dfs"`：已提交数据以不可变 chunk/FileVersion 表示，适合一写多读。
- `fs = "all"`：同一进程启用两种模式，但挂载、状态机和验收结论仍相互独立。

本工作树 local-file 的操作日志格式、旧目录拒绝及尚未完成的回收限制见
[Meta 部署合同](../docs/deployment/afs.md#meta-操作日志与数据目录)。

历史结果／watch 有界不表示全部权威实体有界：旧 NodeSession 拒绝记录、未完成及失败
命令仍保留；安全回收范围和限制见上述部署合同。

Meta 默认使用唯一持久后端 `local-file`。`memory` 仅适合一次性演示；etcd／Redis适配已移除，
多 Meta/HA、完整 POSIX、跨节点文件锁、复杂故障矩阵和性能达标不在当前承诺内。
Operator 负责创建 namespace/root grant、配置挂载和回收引用；Environment 创建尚不会
自动 provision/mount/reclaim AFS。

## 构建入口

默认入口保持 Agent DX 原行为：

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

包流水线显式设置 `ADX_WITH_AFS=1` 时才运行 AFS 组件步骤；共享 source gate
同样读取该开关以纳入 AFS lint。仓内脚本不根据文件变化自行推断或打开该开关。
默认包始终排除 `afs-meta`、`afs-node` 和 AFS 示例配置。

SHM 的 memfd、sealing 与原子 CLOEXEC FD 传递实现及对应运行测试只在 Linux 编译。macOS 上返回 `ShmError::Unsupported`，仅检查该不支持契约；这不代表 AFS/FUSE 在 macOS 可运行。

## 当前能力边界

- OwnerFs workspace bind mount 默认 OFF；当前实际试用场景需要显式配置 ON。
- OwnerFs 远端 FUSE 访问和 DFS 一写多读代码路径保留，目标仓二进制需要按受影响范围补 Linux 运行证据。
- 中心 Meta `local-file` 是当前可重启恢复基线；`memory` 只适合一次性演示。
- etcd、Redis、多 Meta、高可用、完整 POSIX、复杂可靠性和大规模长时间测试后置。
- 跨节点 `fcntl/flock`、阻塞锁取消、bind/native 与远端 FUSE 锁域协同后置，不能把本机 ext4 锁或单挂载内核回退宣传为分布式锁。

必要正确性边界仍需保留：读写新鲜度、close-to-open、权限和 suid/sgid 清除、错误传播、持久化屏障、direct-I/O mmap 能力协商、正常卸载和受管引用排空。

原生DFS新open取得fresh状态，已打开读视图最多复用5秒、命中不续期；Node/session
授权与版本视图分别检查，已知失效停止交付。A当前已验证对象单槽最多4MiB，与在途
正文共用8MiB容量；B可显式启用可丢弃的磁盘读缓存，不新增常驻正文RAM缓存。具体期限、断链、失效与资源范围见
[读取上下文边界](../docs/architecture/afs.md#dfs-原生节点读取上下文)及
[验收与指标](../docs/testing/afs.md)。这不改变成功fsync的持久要求。

## 文档入口

- 演进计划、支持范围和后续验收：[docs/development/afs-plan.md](../docs/development/afs-plan.md)
- 架构边界：[docs/architecture/afs.md](../docs/architecture/afs.md)
- 部署与配置：[docs/deployment/afs.md](../docs/deployment/afs.md)
- 测试与验收：[docs/testing/afs.md](../docs/testing/afs.md)
- 快照来源和路径映射：[docs/migration/2026-10-09-afs-snapshot.md](../docs/migration/2026-10-09-afs-snapshot.md)
- 来源索引和校验身份：[docs/migration/sources.json](../docs/migration/sources.json)
- 验收工具入口：[build/e2e/afs/acceptance/README.md](../build/e2e/afs/acceptance/README.md)
- 发布配置示例：[build/config/examples/afs/](../build/config/examples/afs/)

历史来源不自动构成当前候选版本的运行结论。所有功能、性能和交付状态必须以
当前 Agent DX 源码、二进制和对应 Linux 证据登记。

## 原生DFS CRC写入与GC保护

原生DFS Node默认采用独立ID＋32KiB分段CRC32C及写入代次GC保护，保留capability本地化，省略逐chunk Meta登记。多副本写、跨节点范围读和正常副本修复保持原复制/授权/持久化合同；FullSync在相关代次有效时原子发布，无关GC变化不导致失败。CRC用于意外损坏检测，不提供Bao密码学证明或内容去重。S3仍走原路径，本次不提供旧版本兼容/迁移。

限定Linux验收覆盖1GiB、两份同步副本、正常修复至三份，以及空节点远端完整读回；不等于物理故障域、RDMA硬件或HA验证。配置和复现命令见[部署说明](../docs/deployment/afs.md#原生dfs分段crc32c与写入代次保护)与[验收合同](../docs/testing/afs.md#1gib多副本跨节点读与正常修复)。下述可选BLAKE3磁盘缓存不用于CRC对象。

## 原生 DFS 可选磁盘读缓存

未配置外部存储时，Node 可通过 `[dfs_peer_cache]` 指定专用目录和 `capacity_bytes`；
省略即 OFF。容量由部署选择，256MiB只是一种配置示例。缓存按不可变 chunk 身份隔离，
近似 LRU 淘汰；不登记持久副本，不替代 A 的权威数据，不允许据此删除唯一持久副本。
B 命中仍取得有效 Meta grant；首次获取／身份改变完整验证可信Meta根，稳定命中完整校验本次覆盖的1MiB分段后交付授权范围。
只有 grant 覆盖整个当前对象时才整块回填，不预读下一对象；partial／旧协议缺少范围提示
沿用范围读取。符合runtime／窗口／原预算条件的gRPC回填复用两个256KiB私有载荷块，
按块交接同对象接收、完整校验和私有文件写入；wire正文帧仍为64KiB，块归还后才能复用。
实际EOF及任务排空后，最终授权通过才提交Ready与交付。缓存I/O失败继续同次RPC验证读取，
不增加持久副本或正文RAM缓存；其他适配器保留原缓冲路径。磁盘或填充失败不伪造fsync成功，权限拒绝与远端读取错误仍传播。
配置、资源和验收出口见[部署说明](../docs/deployment/afs.md#原生-peer-可丢弃磁盘读缓存)。

## DFS 外部 S3 后端（实验接入）

在 Meta 和所有 DFS Node 的 TOML 中显式配置相同的
`[dfs_external_backend]`，即可选择独立的外部对象持久化路径。该策略只允许
新文件系统初始化；已有 Node 复制文件系统不能通过改配置原地切换。store_id
和 generation 持久写入 Meta，重启不能改变。未配置时沿用原 Node 复制路径。

外部模式固定单副本配置，禁用 Node 复制及复制修复。普通 write 进入 dirty
状态，fsync/fdatasync 等提交屏障先等待所有普通 S3 PUT 完成，再通过原 Meta
事务发布 FileVersion、布局和对象引用。上传失败不能发布新版本；Meta 确认
未知时保留原请求做幂等恢复，不删除可能已上传的对象。本地 chunk/catalog
不作为该模式的耐久副本，数据可靠性取决于管理员提供的 S3 持久性合同；
AFS Meta 的持久性仍需独立保证。

S3 适配层复用一个自有 worker、runtime 和客户端，所有请求共享最多四路
活动 PUT，完成一路即可补位。入口队列容量为1，队满等待形成回压；worker
最多接收32个命令。外部模式顺序写的完整4MiB块可在后续 write 期间上传，
以64MiB为一次发布单元；旧窗口冻结后，可在原预算余量内接纳下一窗口，
普通 write 成功表示数据已接纳，不表示当前窗口已发布。fsync、fdatasync、
可写 flush 和同步写按入口前已接纳的 inode 序号排空所有相关窗口。独立只读
客户端只见 Meta 已提交版本。外部模式本机 dirty read 先按 inode 完成读入口前
已接纳序号的有序发布，再从已发布版本读取；发布失败返回错误，不回退旧内容。
该读屏障可提前发布不足64MiB的尾部，仅 open／getattr 不触发发布。
中间版本的可见时点可滞后于普通 write，成功同步与 close-to-open 不降级。
只有所需成功回执
齐全且 Meta 持久确认后才能报告同步成功；未知确认精确重试，PUT 失败
保留窗口身份和已确认回执。非阻塞收取成功 PUT 回执并核对身份后，可回收
已上传正文；最后一个 HTTP／重试／读 pin 退出后才释放正文预算，提交描述仍
保留到 Meta 确认。逻辑64MiB窗口不随正文回收缩小，Uploaded 状态不是空洞。
当前窗口已满而前窗尚在提交时，先推进前窗并冻结当前窗，再接纳新数据；
物理正文有余量也不能越过逻辑窗口边界。
非外部 DFS 仍使用 frozen／active dirty overlay。服务端可能留下未发布对象，不能直接删除共享
内容对象。正常退出关闭入口、排空并 join worker。

每个不超过 4MiB 的不可变 chunk 对应一个原始字节对象，key 为
`afs/v1/<store_id>/<generation>/chunks/<摘要前两位>/<ChunkId>`。
读 Node 先取得 Meta 的独立外部读授权，再完整 GET 并验证长度和 BLAKE3，
最后返回授权范围；不依赖写 Node 在线，不回退到 Peer。普通路径不增加
HEAD、条件 PUT/GET 或版本 ID。内容 cache 默认 OFF，writeback OFF；
Node 可配置有界异步磁盘 cache，pending 与磁盘内容分别限额，完整校验后
填充。S3／磁盘读取完成一次完整校验后，以只读、绑定完整 chunk 身份的缓冲区
交给读引擎，不重复计算同一缓冲区摘要。开启 cache 时，Node 共享一个最多4MiB的
已验证对象槽，相邻读取按完整身份复用不可变内容；新磁盘加载仍检查文件安全性并
完整校验。Ready 重载在同一对象分配内按至多四个1MiB段连续读／校验，
当前段hash与下一段read重叠，复用同一个Hasher并在每轮结束等待已接纳工作；
全部长度、EOF及完整BLAKE3比较成功后才交付，不预取下一对象。
驻留、Ready未验证分配、pending／worker及被读者持有的已接纳对象计入原
pending额度，共享分配只计一次；Ready分配前取得预留，验证后原子转移计费。
切换Ready对象时可回收没有其他持有者且容量足够的旧resident Vec；原capacity
持续计费，旧Verified身份退出，新对象完整读取／EOF及BLAKE3成功才可交付。
只读committed external路径将通过最终授权检查的私有owned结果直接交给FUSE；
DFS外部批量读取的借用接口仍保证失败不改输出，dirty／remote／非外部后端保留原路径。
Ready预算不足返回容量错误，保留缓存文件，不绕到未计费的回源；其他填充或
驻留准入不足仍放弃对应缓存。此额度不是全部前台读缓冲
或进程RSS上限。RAM命中不探测磁盘副本后来的变化，下次加载仍检测损坏并回源。
外部只读 open 在线获取最新版本，已打开句柄在一致视图上读取；重新 open 看见
已发布更新，旧 FD 不保证立即跟随远端修改，也不是永久快照。对象合法范围通过
`AuthorizeExternalVersionRead` 按需准入，最多复用5秒且受 grant／session 限制；
每次 read 仍检查身份、范围、期限和已知失效，完整校验不变。缓存命中和心跳
不续期；Meta 断链只允许尚有效的上下文，新 open／新对象／到期续期须在线成功。
已知失效停止受影响输出，Node 重启丢弃上下文；拒绝和超时直接传播，不重新
签发洗白。授权元数据最多128项／1MiB，含被在途读者持有的保守预留，不增加
原载荷预算。缓存不能替代 S3 持久化回执，不将上传成功等同于
本地 cache 完成。小块写默认填充，全块写需显式 cache_large_write=true；
队满或运行中写盘失败跳过填充，损坏缓存回源。缓存空间不足时，从两个可淘汰
Ready候选中比较1秒单调访问时间桶，淘汰较旧桶；同桶保留第一个随机候选，
不按精确访问顺序打破平局。实际命中刷新桶，首次Ready及恢复索引按当前进程时钟
初始化；时间桶不持久化。Pending／临时文件不作为受害者，不增加缓存水位或预算。
先检查任务和真实pending分配准入，再淘汰磁盘文件；调用者仍持有的载荷继续计费。
删除失败保留容量计费和后续淘汰资格，不能把缓存拒绝当成后端持久失败。
外部后端运行指标区分GET排队、SDK响应头合计、正文等待／复制／EOF和完整校验；
普通性能轮可采集，不需要全量TRACE。慢GET摘要受固定阈值和速率上限限制，
不记录凭据或完整S3对象路径；SDK内部连接／重试细分仍不可由这些指标推断。
配置及验证范围见下述链接。
S3 容量查询暂不支持，statfs 明确返回不支持，不能用 Node 空间冒充后端容量。
删除不立即删除 S3 对象；复杂对象回收后置。

SeaweedFS 等 S3 服务是外部运行依赖，其部署脚本、数据库和测试凭据不进入
产品仓。连接方式见[部署配置](../docs/deployment/afs.md#dfs-外部-s3-连接)，
验证边界见[验收说明](../docs/testing/afs.md#dfs-外部-s3-闭环)。

## 原生 DFS 写 buffer

本地 inode owner 的普通 FUSE 写在原有顺序门禁内直接填最终可写 buffer，原提交入口消费冻结并共享
只读正文，不新增满4MiB提交入口。`dfs_write_buffer_idle_bytes` 默认128MiB，仅限制空闲复用容量，
不限制 live 正文；最后引用释放才归还，停止清空。Linux Node 默认使用官方 jemalloc，Meta 和其他
ADX 进程不强制替换。S3 原窗口和预算独立保持，不新增正文 RAM 内容缓存。
具体一致性、未知提交、一次复制范围、ENOMEM/OOM和验证边界见
[架构](../docs/architecture/afs.md#dfs-原生写正文所有权)、[配置](../docs/deployment/afs.md#原生写-buffer-与-node-allocator)
与[测试](../docs/testing/afs.md#原生写-buffer-验证)。减少复制不自动等于性能达标。
