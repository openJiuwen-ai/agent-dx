# AFS DFS 的支持边界与已知限制

本文是 DFS（`fs = "dfs"`）对外承诺的范围：支持矩阵、已知限制和本轮重构带来的已知变化。
POSIX 与 Linux 文件操作的逐项行为见 [POSIX 支持边界](afs-posix-boundary.md)；配置与运维见
[部署说明](../deployment/afs.md)；语义与可靠性机制见 [架构边界](afs.md)。
每一条都指到代码或测试；"外推"表示由较小规模的实测按已知复杂度推算，没有在该规模下实测。

## 支持矩阵

承诺范围沿用重构整体评审定的支持矩阵（Meta 仍是单进程，不做高可用、多 Meta 或无状态 Meta）。
"验证"列写的是当前代码上实际跑过的证据，不比它更强。

### 存储模式 × Meta 后端

| 存储模式 \ Meta 后端 | `local-file` | `redis` | `etcd` | `memory` |
|---|---|---|---|---|
| 本地盘 1 副本 | 支持 | 支持 | 支持 | 仅测试 |
| 本地盘 2 副本 | 支持 | 支持（未跑 e2e） | 支持（未跑 e2e） | 仅测试 |
| S3 加本地缓存（外部后端） | 支持 | 支持 | 支持 | 仅测试 |

| 组合 | 验证 |
|---|---|
| `local-file` × 1 副本 | 全部单机 e2e 套件（`afs/tests/run.py`：`local-basic`、`s0-semantics`、`local-write-special`、`local-read-special`、`local-extended`、`local-resources`、`posix-boundary`、`local-performance`）、`fuse_contract` 真实挂载用例、性能矩阵 |
| `local-file` × 2 副本 | `local-write-special`（`copies=2`）、多机套件（`cluster-*`）、降级写与修复故障场景（`local-fault`：`degraded_write_*`、`repair_*`、`scrub_restores_bad_replica` 等）、性能矩阵 |
| `local-file` × S3 | 故障场景（`local-fault` 的驱动 `afs/tests/faults/harness.sh` 用 S3 模式的 MinIO：`s3_reclaim`、`s3_deleter_lost`、`s3_deleter_stuck` 等）、本地与云端性能矩阵；e2e 套件不覆盖 S3 |
| `redis`／`etcd` × 1 副本 | `s0-semantics` 与 `local-basic` 在两个后端上各跑一次；与 `local-file` 同一组日志合同测试（`afs/src/meta/store/log_contract_tests.rs`，以 `--ignored` 连真实服务跑）；故障场景 `meta_backend_redis`、`meta_backend_etcd`（本地盘 1 副本、单 Node：Meta `kill -9` 后从后端恢复；误启动的第二个 Meta 打开同一后端与前缀后，旧 Meta 的写被 fencing 拒绝、其 Node 自行退出；后端 `kill -9` 前至少一条已确认写，重启后逐条校验路径与 SHA-256 并验证新写入）；性能只有与 Meta 同机、回环连接的数字（[部署说明](../deployment/afs.md#meta-操作日志的-redisetcd-后端)） |
| `redis`／`etcd` × S3 | 无专门用例；Meta 日志后端与存储模式之间没有耦合（同一状态机与提交路径），按"支持"列出 |
| `redis`／`etcd` × 2 副本 | 无专门用例；Meta 日志后端与存储模式之间没有耦合（同一状态机与提交路径），按"支持"列出 |
| `memory` | 只用于可丢弃状态的测试；重启不保留，Node 默认不对它报告就绪（`allow_volatile_meta`） |

持久性按 Meta 后端分别声明：`local-file` 到本机盘 fsync；`redis` 只在单实例
`appendonly yes`＋`appendfsync always` 时（Meta 启动时检查）；`etcd` 取决于其 Raft 多数派；`memory` 不承诺。

### 接入面

| 接入 | DFS | OwnerFs |
|---|---|---|
| FUSE | 支持 | 支持 |
| SDK（`afs/crates/client`：路径式与 fd 式，含 usrbio 共享内存环） | 支持 | 不适用 |
| RDMA（Node 之间副本与对端读，`feature = "rdma"`） | 只保证可编译，**不在支持矩阵内**，未验证 | 同左 |

SDK 路径式 `close` 与 FUSE `close` 同语义（先 DataOnly 同步提交再释放），fd 式走 FUSE 的 flush＋release。

## 已知限制

按影响排序，前三项可能丢数据或使服务不可用。

| # | 限制 | 影响 | 规避与去向 | 依据 |
|---|---|---|---|---|
| 1 | **`local-file` Meta 日志目录必须在本机独占盘上**（R63）。本地后端以文件锁（flock）做写者互斥、term 做兜底；把目录放在 NFS 或多台机器共用的盘上、文件锁失效时，旧写者可能覆盖新写者已确认的写，重启后这些写丢失 | **丢失已确认的写** | 每个 Meta 用本机独占盘；需要共享存储时改用 `redis`／`etcd` 后端（term 与 tip 在服务端同一原子单元里比较）；彻底修要改恢复路径，另立项 | `afs/src/meta/store/local_log.rs` 写入合同；S4c 实测 |
| 2 | **Meta WAL 增量追加记录需要新版 Meta**（R86）。可证明为连续追加的提交只写新增 extent，其他提交仍写全量 layout。新版可恢复旧全量记录和混写日志 | 旧版 Meta 无法恢复含新增量记录的 WAL | 首次启用前升级所有可能接管同一存储的 Meta；回滚仅使用旧版匹配的完整恢复点 | `afs/src/meta/store/log.rs`、`afs/src/meta/store/state/replay.rs` |
| 3 | **持续存储故障下的 DFS 写入恢复**（R84）：写入准入及 fsync 发布前等待默认限 30 秒。已接收的正文仍用于重试。后台按 inode 退避，故障解除或 Meta 提供合规副本后可继续提交 | `strict` 缺少同步副本时 fsync 可能返回对应 errno；已发出的 Meta 发布仍遵循现有确认与精确重试语义 | 调整 `dfs_write_timeout_ms` 并监控 `afs_dfs_native_writeback_errors_total` | local-fault LF27、LF28 |
| 4 | **大批删除的回收时长**（R73）：GC 规划仍随实体总数增长；1.6 万文件约 390–440 chunk/秒，100 万文件**外推**约 40 分钟到 3 小时回收完 | 删除后空间回收慢 | 观察 `afs_meta_dfs_gc_pending_chunks` 与 `afs_meta_dfs_gc_generation_age_seconds`；按需优化 | S3a 报告 |
| 5 | **S3 模式删除节点失联或卡住时回收延后**（S4-9）：一代只等一个删除者。失联：会话过期（约 30 秒）后剔除，剩余对象由列举对账找回，3000 个对象实测 40 秒；卡住（仍注册但 120 秒未完成）：转交下一个存活 Node，实测 141 秒。时延约为会话期限＋对象数 /（1000 × `dfs_external_gc_list_pages_per_pass`）× 5 秒，大规模按此**外推** | 回收延后，不丢数据 | 对象很多时调大 `dfs_external_gc_list_pages_per_pass` | `afs/src/meta/dfs/gc/deleter.rs`；故障场景 `s3_deleter_lost`、`s3_deleter_stuck` |
| 6 | **文件锁只在本机有效**（决策 11）：`flock`、`fcntl`、OFD 锁由各 Node 内核本地处理，跨 Node 没有互斥 | 跨机协调的程序不能依赖文件锁 | 不在本轮 | [POSIX 边界](afs-posix-boundary.md) 第 4 项 |
| 7 | **`statfs`／`df` 显示本 Node 的 chunk 存储盘**，不是集群容量或配额 | 容量判断需另看 | — | POSIX 边界第 26 项 |
| 8 | **`FUSE_HANDLE_KILLPRIV_V2` 未开**：依赖的 fuser 0.18 拿不到内核清位标记，开了会造成 suid 不清除的安全回归；代价是每次写多一次内核往返 | 小写性能 | 改 fuser 另立项 | POSIX 边界末节 |
| 9 | **1970 年以前的时间戳不支持**：`utimensat` 设 1970 年前的 atime／mtime 返回 `EINVAL` | — | — | POSIX 边界第 22 项 |
| 10 | **Redis／etcd 连接无 TLS 与认证**（Redis URL 可带口令） | 只能部署在受信网络 | — | `afs/src/meta/store/redis_log.rs`、`etcd_log.rs` |
| 11 | **Meta 被 fence 或日志写失败后失败关闭，需人工重启**：第二个 Meta 打开同一份日志后，旧 Meta 的下一次写返回 `META_LOG_FENCED`；结果未知的写（连接断、超时、追加出错）同样让 Meta 失败关闭。失败关闭后 Meta 进程不退出：当时在途的请求得到 `META_LOG_FENCED`（应用看到 `EBUSY`），之后的请求得到 `IO_UNAVAILABLE`（应用看到 `EHOSTUNREACH`），`/health` 返回 503。它服务的 Node 心跳从第一次失败起约 15 秒（距最后一次成功心跳 25 秒）后自行退出 | 需要重启 Meta 与 Node | 见[部署说明的运维要点](../deployment/afs.md#运维要点) | `afs/src/meta/store/engine.rs`（`ensure_available`）；`afs/src/node/registration.rs`（`HEARTBEAT_FAIL_CLOSED_AFTER`）；故障场景 `meta_backend_*` |
| 12 | **会话过期的 Node 需重启**才能重新加入：会话期限 30 秒，过期会话不能续期（`expired node session cannot be renewed`），session_id 在进程启动时生成，Node 随即退出（日志 `node.shutdown_failed`） | 网络长时间中断后需重启 Node | 进程守护自动拉起 | `afs/src/meta/store/state/apply.rs`；`afs/src/node/registration.rs` |
| 13 | **长时间运行时元数据操作的 Meta CPU 缓慢上升**（Q09）：同一负载 12 轮，每轮 create 的 Meta CPU 由 1.31 秒升到 2.14 秒（前 10 轮），未见收敛；绝对值仍远低于重构前（约 13.5 秒起） | 长跑后元数据吞吐下降约 30% | 观察项，随 R86 的堆剖析一起查 | S5a 报告 §6.3 |
| 14 | **owner 崩溃时未提交的写丢失，代理方不重连**（Q06）：见下文"owner 崩溃" | 未 fsync 的写丢失；新写者至多等 30 秒 | 需要持久的写 fsync 或 close 并检查返回值 | 见下文 |
| 15 | **大量打开且有脏数据的文件让 create 偶尔等到下一个 5 秒提交节拍**（约 1/20，最长 4.9 秒；重构前即如此）（R91） | 尾延迟 | 观察项，随 R79（命名空间变更是全挂载屏障）一起处理 | S5a 报告 §6.4 |
| 16 | **Meta 读接口按单一信任域授权**（M0）：受信 Node 证书即可读任意 namespace 的元数据，不绑定会话或 namespace | 多租户前需补 | — | [架构边界](afs.md#meta-与数据路径) |
| 17 | **Meta 故障时，已打开的读句柄可能停在打开时的版本**（R94）：本机写者全部关闭时，要为本机最新版本建 GC 引用后才推进其他已打开的读句柄；经写状态读过的版本此时已有引用，推进不再需要 Meta。若该读句柄从未经写状态读到新版本、这次建引用失败（Meta 不可用），且在补建成功前该版本已被其他 Node 的提交取代并回收，句柄会一直读打开时的版本。经写状态的读若建不了基线引用：没有本机未提交数据时改读句柄自己的版本，有则返回错误 | 只在 Meta 调用失败的窗口内出现；数据不丢，已持久的提交照常成功 | 日志 `dfs.handle_view_reference_failed`、`dfs.write_state_read_reference_failed`（WARN）可查；重新打开文件即读到最新版本 | `afs/src/node/vfs/dfs/handles.rs`（`OpenedView`、`base_read_reference`、`publish_deferred_views`）；`view_references` 单测；故障场景 `local_commit_reader_under_gc`、`lapsed_lease_reader_under_gc` |
| 18 | **RDMA 未验证**（决策 8） | — | 只保证可编译 | — |
| 19 | **他节点的命名空间与属性变化最多晚一个内核 TTL 被看到**（性能专项 A）：DFS 挂载的内核目录项与属性缓存默认 1 秒（`dfs_kernel_cache_ttl_ms`）。在这 1 秒内，本节点按路径的 `stat`／`ls -l`／`access` 可能看到他节点已改名、删除或替换的旧名字与旧属性；**他节点收紧权限（chmod／chown）在本节点最多晚 1 秒生效**；**按路径的 `truncate(2)`、`chmod`、`chown`、`utimes`、`setxattr` 可能作用到已被他节点替换掉的旧 inode**（应用以为改了新文件，实际没改；`open(O_TRUNC)` 不受影响，见下句）。`open` 与 `opendir` 有名字校验：经由一个最近交给内核的名字打开时，Node 向 Meta 确认这个名字仍指向该 inode，否则让内核重走一次路径，所以删除后同名重建、rename 覆盖、rename 后旧名都不会在 TTL 内打开到旧文件；**硬链接文件在本节点最近经由两个以上名字访问过时退回只确认 inode 存在**，经由被替换的那个名字仍可能打开旧 inode。负查找不缓存。本节点经 FUSE 的变化立即可见 | 跨节点陈旧最多 1 秒 | 需要更强一致时设 `dfs_kernel_cache_ttl_ms = 0`（恢复每次路径解析都问 Node）；setattr 时的名字校验列为后续项 | `afs/src/node/fuse/policy.rs`（`DfsFusePolicy`）；`afs/src/node/vfs/dfs/recent_names.rs`；指标 `afs_dfs_open_name_check_total` |

### owner 崩溃（Q06）

文件的写租约在别的节点（owner）时，本节点的写经 `DfsOwnerFiles` 交给 owner 执行。代理方的远端写会话绑定 owner 的
节点、会话与租约代际，**没有重连或迁移**（`afs/src/node/vfs/dfs/remote_client.rs`）：owner 进程崩溃时，它已接受但尚未提交的写
随之丢失，代理方之后的 write、fsync、close 返回错误；已经 fsync 或 close 成功的数据已提交到 Meta，不受影响。新的写者要等
原租约过期（至多 30 秒，`DFS_WRITE_LEASE_SECONDS`）才能取得租约（`afs/src/meta/dfs/write_lease.rs`）。这与本地写者进程崩溃时
未同步数据丢失的语义相同：需要持久的写必须 fsync 或 close 并检查返回值。该结论来自读代码，仓库里没有专门覆盖这条路径的用例。

## 已知变化（相对重构前）

本轮重构中经评审接受、对使用者可见的变化。升级与兼容类变化（协议、存储格式）见
[部署说明](../deployment/afs.md#协议与格式不兼容时的停机替换)。

| 变化 | 说明 | 依据 |
|---|---|---|
| **远端读默认校验正文** | `dfs_read_crc_mode` 默认 `receiver`（原 `off`）：坏副本不交给应用，换副本读，全部坏时 `EIO`。本地远端顺序读约慢 6%～9%；显式设 `off` 可关闭，但可能交付静默损坏 | `afs/src/config.rs`；S4a |
| **预设长度文件写完马上读变慢**（F1／R87） | 文件内部的顺序小写合并成最大 4 MiB 的 chunk（Meta 实体数与 GC 量大幅下降，写吞吐提升）；S5a 实测（当时本地读首次打开一个 chunk 要整块 CRC 校验）：读 4 MiB chunk 的首次读校验比 1 MiB chunk 每 MiB 多花约 30%–40% Node CPU。页缓存未命中、刚写完马上读时：云端 native FUSE −32.5%、r2 FUSE −35%、r2 SDK −12%、S3 FUSE −43%，本地 S3 SDK 约 −45%；同时写入 +13%～+244%；冷读（磁盘是瓶颈）无差别。本地读已改为只校验请求覆盖的 32 KiB 段（性能专项 F-a，本地 VM 4 MiB chunk 冷读 4 KiB −90%、1 MiB −71%、整块 −41%；云端未重测）；S3 回源仍整对象 GET 并整对象校验 | `afs/src/node/vfs/dfs/dirty_extents.rs`、`afs/src/node/chunk/store.rs`（`open_for_range_reads`）；S4b、S5a、性能专项 F-a |
| **S3 模式小文件写约慢 9%**（R90） | S3 对象可回收（R05）后，每写一个 S3 小文件多一次 GC 引用 RPC 与一条 Meta WAL 记录；同期 S3 小文件读 +21% | S5a |
| **readdir 分页** | 目录按名字游标分页，每页至多 1024 项，百万项可列出；每次列目录的 Meta RPC 次数随目录大小增加 | `afs/src/meta/dfs/` `READ_DIR_PAGE_MAX`；S4a |
| close 后他人立即看到新 size 与 mtime（Q03） | 重构前即如此，S0 特征测试 `dfs.semantics.close_mtime` 证实（原担心 mtime 滞后到下次后台提交，不成立）。size 由 close 的同步提交保证；mtime 由最后一个写者 release 时立即触发的元数据提交带上，见[架构边界的语义一节](afs.md#fsyncclose-与-sdk决策-512) | S0 |
