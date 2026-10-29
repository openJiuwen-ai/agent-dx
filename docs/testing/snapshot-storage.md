# Checkpoint 存储与快照生命周期

同事部署和复现请先阅读[JuiceFS / AFS 部署与双节点验证手册](shared-filesystem-runbook.md)，其中包含环境、制品、完整命令、用例和结果判定。本页保留实现合同与历史验证记录。

`adx-checkpoint-storage` 提供节点文件访问接口 `CheckpointAccess` 和 Coordinator
共享回收接口 `ArtifactReclaimer`。adxlet 处理捕获、恢复和本机缓存；Coordinator
管理 Environment/Snapshot 的保留关系并回收共享文件。默认 ADX 构建不依赖 AFS/FUSE。

## 存储模式

| 模式 | 捕获 | 恢复 | 物理回收 |
| --- | --- | --- | --- |
| local only | 写节点私有目录，完成可读性校验 | 直接读取本地目录 | 原节点；不支持跨节点恢复 |
| S3 + 本地缓存 | 写本地 staging，文件上传完成后发布 manifest | 命中缓存直接用；否则下载并校验后使用 | Coordinator 删除 S3；节点独立淘汰缓存 |
| AFS / JuiceFS（distributed_fs） | 直接写挂载目录中的独立捕获目录，逐文件同步后发布 manifest | 直接使用同一共享路径，不预下载整个制品 | Coordinator 删除共享 manifest 和目录 |
| distributed_fs + local_cache | 本地捕获，再同步复制到共享卷，最后持久发布 manifest | 本地命中，或下载校验后在节点私有目录中恢复 | Coordinator 删除共享制品；节点只淘汰未使用缓存 |

每个共享后端使用稳定 `alias`。同一 alias 的所有节点与 Coordinator 必须配置同一
存储 namespace。调度通过 `ADX_CHECKPOINT_STORAGE=<alias>` 选择兼容节点。
S3 和共享文件系统支持跨节点恢复；本地恢复仍受源节点约束。

### adxlet 配置

`checkpoint_dir` 是本地目录简写；与 `checkpoint_storage` 互斥。
以下片段分别放入节点配置：

```json
{"checkpoint_storage":{"kind":"local","root":"/opt/adx/data/checkpoints"}}
```

```json
{"checkpoint_storage":{"kind":"s3","alias":"obs","bucket":"adx-checkpoints","region":"cn-north-4","endpoint":"https://obs.cn-north-4.myhuaweicloud.com","allow_http":false,"prefix":"production/checkpoints","root":"/opt/adx/data/checkpoints","cache_max_entries":500,"cache_budget_bytes":53687091200}}
```

```json
{"checkpoint_storage":{"kind":"distributed_fs","alias":"afs-checkpoints","mount_root":"/mnt/afs-dfs","prefix":"adx/checkpoints"}}
```

S3 通过标准 AWS 环境变量/身份机制取得凭据。记录只保存 alias、制品 ID、大小，
不保存端点或密钥。缓存默认 500 条、50 GiB，活跃恢复/执行文件受到 pin 保护。
上传完成并提交权威结果后，原 staging 直接进入缓存，不复制或重新下载。

`distributed_fs` 是统一的目录访问接口。adxlet 和 Coordinator 不区分底层文件系统：
前者调用捕获／恢复接口，后者调用回收接口；两者配置相同 alias、prefix 和共享 namespace。
`checkpoint-storage` 适配层识别实际挂载的 AFS DFS 或 JuiceFS，不需要 `filesystem` 选择字段。
Linux 上的挂载必须是 `afs-dfs` FUSE，或 `fuse.juicefs` / `JuiceFS:<volume>`。
启动时固定挂载身份，每次访问检查挂载 ID、设备、来源和子目录；消失、被覆盖或重挂载时
返回错误，不落到本地目录。重挂载后应在确认目标 namespace 正确后重启使用该挂载的 ADX 进程。
Coordinator 也需要能访问同一 namespace；各进程可以有独立挂载路径。
`store-id` 核验 prefix 与 alias 一致。

JuiceFS 接入同样只配置共享路径，例如：

```json
{"checkpoint_storage":{"kind":"distributed_fs","alias":"shared-checkpoints","mount_root":"/mnt/juicefs","prefix":"adx/checkpoints"}}
```

JuiceFS client、PostgreSQL 元数据、Ceph RGW 对象存储及凭据由部署层提供，
sandboxd 和 ADX 业务组件不调用这些后端的 SDK。JuiceFS client 必须在 sandboxd 可见的
挂载 namespace 中运行。部署应关闭 JuiceFS writeback，持久化元数据数据库须启用同步提交；
`seal()` 在文件 fsync 完成后才发布 manifest，再由 Coordinator 提交权威恢复点。
快照目录、每个执行节点的私有 runtime 目录和业务 shares 目录分别管理。
匿名恢复点跟随 Environment，仍在运行的输入由 Coordinator 的持久引用保护；
底层 COW 块、打开文件与物理对象的回收由文件系统负责。

### 共享文件系统与节点本地 reflink

gVisor 原生 filestore FICLONE 要求捕获目录与运行中的 filestore 位于同一支持
reflink 的文件系统。节点 filestore 位于本地 XFS、checkpoint 持久层位于 JuiceFS
时，应在节点的 `distributed_fs` 配置中增加 `local_cache`：

```json
{"checkpoint_storage":{"kind":"distributed_fs","alias":"shared-checkpoints","mount_root":"/mnt/juicefs","prefix":"adx/checkpoints-cached","local_cache":{"root":"/opt/adx/data/checkpoints","budget_bytes":53687091200,"max_entries":500}}}
```

`root` 必须是独立的节点本地绝对目录，由部署层验证它与 sandboxd filestore
位于同一个 reflink 文件系统；不能位于共享挂载内部或覆盖共享挂载。
`budget_bytes` 和 `max_entries` 分别默认 50 GiB 和 500，必须为正数，
仅约束未被执行使用的缓存。捕获完成后，ADX 保留本地原文件作为恢复缓存，
将稀疏文件、目录、权限和 SHA-256 manifest 发布到共享卷。
数据及目录完成同步后，manifest 才成为发布标识；源节点丢失后，其他节点
先将完整文件校验并落到自己的本地目录，再交给 sandboxd。
本地 reflink 不省去跨节点数据传输，也不实现应用内存脏页增量快照。

首次启用缓存模式必须选择新的、空的 prefix。`cached-layout` 固定该 namespace
的文件格式，防止与直接访问模式混写；省略 `local_cache` 的原有直接访问模式不变。
所有共享该 alias 的执行节点应使用相同模式。Coordinator 继续使用不带
`local_cache` 的 `distributed_fs` 回收配置，删除 `artifacts/<id>` 整个目录；
节点不删除共享制品。后台恢复的本地文件受执行引用保护，不能手动清理缓存目录。
挂载丢失或 namespace 被替换时停止共享访问，失败发布保留本地恢复文件。

目录 fixture 单元测试覆盖源盘丢失、缓存引用、稀疏传输、损坏输入、失败发布和
namespace 替换；它们不是原生 gVisor、真实 JuiceFS 或 Kubernetes 跨节点验收。

外部 sandboxd 必须能看到传入的绝对路径。运行时的可写文件应放到私有执行目录，
不能修改已发布的 checkpoint。若希望避免跨文件系统复制，sandboxd 的运行目录也应配置
在同一共享文件系统中；ADX 目前不通过单次 restore RPC 改写 sandboxd 的全局存储根。
此配置下需选择该文件系统支持的磁盘 I/O 模式；本仓存储验证使用 sandboxd
默认 `writable_io_engine = "Sync"`、`writable_cache_type = "Writeback"`；
设置 `ADX_STORAGE_IO_ENGINE=Async` 可选择异步磁盘基线，所有对照后端必须采用相同配置。
当前 AFS 不提供 `AsyncDirect` 所需的 direct I/O 对齐信息，不能直接沿用该运行时默认值。
AFS DFS 的 `copy_file_range` 通过独立 inode / FileVersion 共享不可变 chunk 的 extent，
支持分段大文件、稀疏空洞和目标范围覆盖，不预读或重新上传源文件数据。
Meta 校验固定源版本、同 namespace、源/目标范围及完整布局；副本后续写入不会改变源快照。
源与私有运行文件需要处于同一 AFS 文件系统，且目标写归属在本客户端；
同 inode 或远端目标写归属返回不支持，调用方可能走普通复制回退。

锁定 sandboxd `efc2015` / Firecracker `v1.16.1-akernel.3` 的 SharedFile 路径
使用上述标准文件操作创建私有 `memory.live`，无需修改外部运行时协议。
验收同时检查 COW 提交日志、系统调用和延迟触页的远端数据增量；
`copy_file_range` 返回逻辑字节数或 guest RSS 增长本身都不代表实际数据已按需读取。

### Coordinator 配置

本地模式使用默认空列表。共享模式必须配置对应的回收后端，配置中没有节点缓存目录：

```json
{"checkpoint_reclaimers":[{"kind":"s3","alias":"obs","bucket":"adx-checkpoints","region":"cn-north-4","endpoint":"https://obs.cn-north-4.myhuaweicloud.com","allow_http":false,"prefix":"production/checkpoints"}]}
```

```json
{"checkpoint_reclaimers":[{"kind":"distributed_fs","alias":"afs-checkpoints","mount_root":"/mnt/afs-dfs","prefix":"adx/checkpoints"}]}
```

Coordinator 的 S3 身份需要 prefix 下的 List/Delete；共享文件系统身份需要目录访问与删除权限。
缺少回收器时删除保持待完成并报告错误，不转交给源节点删除共享数据。

## 文件接口与流程

节点侧：

1. `begin_capture()` 分配运行时写入目录；`capture_references()` 给出捕获预留身份。
2. adxlet 在调用运行时前持久化文件预留。`size_bytes=0` 仅用于未完成捕获的保护，不能恢复。
3. runtime 写入完成后，`seal()` 完成该后端的发布合同并返回完整 artifact。
   S3 上传并发布 manifest；共享文件系统同步文件并发布 manifest；local 校验文件可读性。
4. adxlet 提交恢复点/Snapshot 的权威元数据，随后调用 `committed()` 完成本地缓存登记。
5. 恢复时 `prepare_restore()` 返回 `RestoreAccess`；S3 取得缓存 pin，共享文件系统/local 返回原目录。
   adxlet 在执行仍依赖文件时保留访问对象及持久化输入记录，确认停止后释放。
6. 节点 `release()` 只处理本地文件/缓存，共享文件系统不在此删除共享文件。

外部 sandboxd 可以独立开启 runsc 后台内存恢复，Start RPC 成功返回不再证明所有输入页
已经加载。adxlet 因此将 runsc 的恢复输入和 Firecracker 的运行中输入一样保留至执行停止；
同步 runsc 恢复也采用此保守期限，因为当前协议没有后台加载全部完成的通知。
S3 的本地缓存 pin 和 Coordinator 的持久化执行引用共同保护输入；adxlet 重新对账时
重建访问保护。代价是恢复输入的缓存占用时间更长，活跃输入可以暂时超过缓存预算。

运行中创建新 checkpoint 时，runsc 不把新捕获输出作为后续执行输入；已有后台恢复
输入继续受到保护。Firecracker 仍保留可能作为增量基线的新捕获文件。
可复用 Snapshot 发布前仍先提交新制品的临时输入引用，发布成功后 runsc 释放该临时引用。
后台恢复需要配套的外部 sandboxd 开关与未压缩 split pages；它不改变本页的捕获和持久化发布合同。

`seal()` 完成不代表 Environment/Snapshot 元数据已经提交；对外成功需要两者完成。
local-only 沿用异步写回语义，不强制 fsync，也不承诺主机断电/丢失后的文件耐久性。
共享文件系统 restore 检查 manifest、路径、文件类型和长度，不扫描完整文件内容；数据读取由对应文件系统执行。

## 匿名快照跟随 Environment

`EnvironmentRecord.checkpoint` 是该 Environment 的匿名恢复点。
显式有限 TTL 或永久保留沿用 pause 的 retention；永久保留持续到替换或删除 Environment。
到期、替换或删除会移除对应保留关系。旧文件若仍被运行时映射，需等该执行停止后才能回收。

`pauseOptions.storage` 的 typed 枚举值为 `configured`（默认）、`local`、`shared`。
`configured` 使用配置后端，`shared` 要求共享后端；S3 节点可用 `local` 保留本地恢复点。
共享文件系统节点显式 `local` 会拒绝，需选择本地存储节点。该选项不会把共享目录当本地磁盘。

## 可复用 Snapshot

创建 Snapshot 直接捕获独立制品，不需要先创建匿名恢复点或复制已有恢复点。
从 Snapshot 创建 Environment 时借用该不可变制品，并创建独立运行身份和可写运行状态；
不自动复制第二份快照。`snapshot_source` 与匿名 `checkpoint` 分开记录。
目标暂停生成自己的匿名恢复点，且停止原执行后，才释放旧 Snapshot 的使用关系。

从 S3、AFS、JuiceFS Snapshot 创建时，调用方未指定放置策略则自动弱亲和
`source_node_id`。Coordinator 在选择分片时也比较该偏好；原节点不可用、资源不足或
不满足硬约束时，立即选择其他符合约束的节点。排队期间资源被其他请求占用时，
可转移到其他分片，保留 Environment ID、排队期限及已拒绝节点列表。
全部节点均不满足时才等待容量变化。显式放置策略覆盖自动偏好；local-only 仍强制来源节点。
来源节点只是缓存命中的线索，缓存淘汰后仍从共享存储恢复。
API Server 即使配置 `local_first`，带 `snapshotId` 的创建也直接进入 Coordinator，
避免任意入口节点绕过偏好。SDK 调用保持 `Environment.create(snapshot_id, ...)`。

Snapshot 使用中删除立即返回 HTTP 409：

```json
{"code":409,"message":"snapshot is in use","error":{"code":"SNAPSHOT_IN_USE","retry":"never","outcome":"not_started","requestId":"request-id","sandbox_ids":["sandbox-a","sandbox-b"],"template_ids":["node/base"]}}
```

保持 Snapshot Ready，不排队等待 Sandbox 退出。调用方处理完列表中的使用者后重新删除。
列表包含已分配恢复、仍映射文件的源/目标执行，以及失联节点未确认清理的旧执行。
SDK 的 `EnvironmentHTTPError.sandbox_ids` / `template_ids` 提供这些列表。

无使用者时，Redis CAS 将 Snapshot 置为 Deleting，关闭新引用。后台执行物理删除并提交 Deleted；
存储失败或应答丢失会幂等重试。共享删除不要求源 adxlet 在线。

## 共享回收与故障

Coordinator 在 Redis 持久化共享制品清单。每轮检查 Environment 恢复点、执行输入、
Snapshot 和旧执行保护，删除期间用制品级内存互斥阻止当前 Coordinator 新增引用。
先删除存储内容，再移除清单项，两步完成才算回收成功；不写入持久化的制品删除标记。
失败或应答丢失时保留清单，由调用方重试；存储内容或清单项已不存在均按幂等成功处理。
Coordinator 重启后重新检查引用并重试删除。物理内容已删除、清单尚未清理的重启窗口内，
恢复请求可能因为文件不存在失败。这一流程保证重试后最终删除，不保证跨重启的删除／恢复原子性。
内存互斥以单个活动 Coordinator 为边界，不提供多活动 Coordinator 间的互斥。
现有 Snapshot 目录的 Deleting/Deleted 状态仍用于其 API 生命周期，不另建制品删除状态机。

节点失联只让执行归属失效，不证明进程已停止。旧执行文件保护在 Redis 中保留；
节点重新完成停止旧执行及对账，并用当前会话报告可路由后，才释放该保护。
无法恢复的节点会保守保留文件，需要运维确认隔离/清理后恢复该节点的对账，不能只按心跳 TTL 删除。
Paused 保持休眠，不因为节点失效批量唤醒。

Coordinator 重启后，adxlet 排空已接纳的恢复操作期间仍上报心跳，标记正在对账且停止接纳新任务。
关闭准入不能阻塞心跳，否则共享存储下载较慢时会把健康恢复节点误判失效。

节点已不运行共享存储删除扫描。无权威记录的历史对象不自动推断归属；
S3 未完成 multipart upload 由部署配置过期中止规则。
停止写入且已无保留关系的捕获预留（`size_bytes=0`）也由 Coordinator 回收；
这种记录仍禁止用于恢复，回收不要求先形成完整 manifest。

## 验证边界

定向测试包含存储目录直读、缺失挂载、manifest/符号链接拒绝、缓存 pin、
匿名恢复点生命周期、真实 Redis 的重启/引用竞争/旧执行保护、HTTP/SDK 使用中删除契约。
目录 fixture 仅验证文件访问适配逻辑。真实 AFS/FUSE、双 Ingress、Firecracker 与
S3/OBS 的单独验证由 `build/e2e/firecracker/storage.py` 执行，日志和逐项证据保存在
工作树 `out/afs-fc/`；原有组件测试日志保留在 `out/snapshot-storage/`。
测试拓扑为同一专用 KVM 主机的两个隔离执行节点，不代表两台物理机器故障域。
AFS COW 与同版本 S3 对比的本轮日志另保存在工作树 `out/afs-cow/`；故障用例记录
Coordinator 重启后的目录进度与排空心跳，并检查目标节点未被误判失效、旧 VMM 退出早于替代 VMM 启动。

## 分段 profile 与性能对比

在已准备好的 Firecracker / AFS fixture 中，为 Meta 进程及其启动父进程设置
`AFS_META_PROFILE=1` 可记录 canonical log 批次的 append＋sync 和状态发布耗时，`bytes` 是该批次的 framed 字节数。
这些计时不包含排队、准备或完整检查点；checkpoint 与历史保留另通过 Meta 的固定指标观察。先保留整份脱敏 `export/`
（包括两个 Ingress 的服务日志），再汇总：

```bash
python3 build/e2e/firecracker/profile_storage.py /path/to/export/evidence --output /path/to/stages.json
```

汇总按 Environment、Runtime 和首个成功请求的 ID 关联恢复阶段，包括 adxlet 准备、
sandboxd open / prepare / instantiate / VMM ready / load / agent、应用就绪探测、
路由激活和 Ingress 等待。COW 排队与执行计时取同一请求组前后指标增量，属于
sandboxd 阶段内部开销，不能与父阶段相加；RPC 与首 HTTP 的外层差值单独保留。
汇总保留提交数量和编码字节区间；local-file 的日常编码是本批次增量，不能把它的编码字节
当成完整权威状态大小，与旧版完整快照的大小区间直接比较。前后对比应同时核对
`state_counts`（实体、幂等结果和事件数量）、请求操作及负载；后台完整检查点另计，
避免把后台续租数量变化或编码格式改变误当成相同工作量。

Meta 增量持久化回归由 `cargo test -p afs --all-features` 覆盖：小提交不重写历史快照、
后台检查点保留并发提交、跨检查点重开不丢幂等结果／watch 事件、提交成功后应答丢失、
不完整尾帧截断、完整帧校验和损坏、版本缺口／重复、检查点安装中断后恢复及失败后的权威关闭。
另验证后台等待到期保留原 pending 请求，正常关闭期间的迟到心跳不重新开启读授权。
这些进程／文件测试不代表物理断电或跨物理主机的存储故障验证。

正式后端对比关闭 Meta profile 和 syscall trace；`compare_storage.py` 会拒绝带此类
诊断的轮次，并核对发布包、外部运行时、guest 镜像和工作负载身份。每个后端保留
三次冷恢复、三次热恢复及生命周期验证，使用相同拓扑和顺序。独立的带 trace 轮次
验证内存文件 COW、首次响应前未整份读取、延迟触页后远端读取增加，不计入性能样本。
业务 HTTP 失败时，fixture 在删除 Environment 前保留请求身份、Ingress 副本、状态码、
响应正文及一次 guest 日志／网络状态采集；guest 采集失败也保留原错误响应。
只有明确的 `503 ENV_RESUMING` 使用等待重试，502 等执行结果不确定的失败直接终止验收。

OBS 的恢复中重启用例在目标 fixture 的网络 namespace 内，临时阻断 Execd readiness
端口 50090。确认真实目标 VMM 已恢复且控制面仍有 pending recovery 后，才重启
Coordinator；观察目标节点至少两次成功的排空心跳后解除阻断。异常退出也移除规则，
且最终验证目标未失效、旧 VMM 先退出后再启动替代 VMM。该阻断只用于性能样本之后的
故障用例，上传、下载、VMM 和业务请求仍使用真实实现。

## 原生 AFS 回收验收

设置 `ADX_STORAGE_GC=1` 后，真实 AFS/Firecracker 驱动把专用 Meta 的结果窗口设为 30 秒、
Watch 窗口设为 16 个 revision，保留生产的 60 秒引用保护期。
驱动先验证 COW/open-unlink 数据在回收期间仍可读取，替换一个 AFS Node session 后再检查
剩余读者及最终物理清理。之后执行双 Ingress 流量唤醒和 Coordinator／执行节点故障场景，
再对同一个 Environment 做两批各三次暂停恢复，每次 12 个并发请求且只允许一次物理恢复。

`gc-observations.json` 记录每阶段的 Meta 实体、幂等结果、Watch 水位，以及各 Node 的
实际 chunk 数量、字节和 catalog 大小。两批之间等待保护期结束，比较稳定存量；删除
Environment 后必须回到初始存量附近，同时 inode、目录项、版本、布局、引用、写租约和 chunk
实体不能超过初始数量。初始存量只允许 CheckpointStore 的三字节 `store-id` 及其元数据，
不允许上轮执行文件或写租约。只看到目录删除或 SDK 成功不能判定物理回收通过。
执行拓扑仍是一台 KVM 主机上的两个隔离执行节点，不代表两个物理故障域。

Meta 的 GC 引用更新在同一个不可变视图内按 key 检查当前 Node session、引用、inode、
version、chunk 与 GC 屏障，避免每次文件访问都复制整个命名空间。引用提交校验相关 key
的 mod revision／缺失状态和当前 session，避免无关提交导致准入饥饿。真正的 GC
可达性扫描仍使用完整视图及 Store revision：读者先提交会使旧扫描失效；GC 先提交
会使相关 inode／chunk／屏障的条件失效。COW、已打开文件和失效 session 的保护期保留。

## AFS unlink 并发回归

`afs/tests/dfs_commit_contention.rs` 在 unlink 读取与提交之间执行真实 DfsService
元数据写回／同父目录的另一个 unlink，检查冲突后重新校验和提交。回归同时覆盖
权限变化、名字被替换、目录新增子项、持续竞争返回 EAGAIN，以及提交成功但应答
丢失后的幂等重放。测试不把预设返回值代替真实的 Meta 比较提交。

真实 AFS/Firecracker 验证可开启 `AFS_META_NAMESPACE_TRACE=1` 关联失败条件和
相邻写入事务；关联必须使用 inode 及版本／head，不能只凭相近时间认定竞争双方。
删除验收还要检查私有运行文件、匿名恢复点、Meta 实体和物理数据块均已收敛。


## JuiceFS 与 AFS 的同包验证

以下为验证驱动的范围和验收条件；是否通过以对应运行的 `result.json` 与清理结果为准。

真实双节点驱动接收部署层提供的 JuiceFS 配置文件 `ADX_FC_JUICEFS_CONFIG`。
驱动启动三个独立 JuiceFS client，分别供两个执行节点和 Coordinator 使用；
ADX 仍只取得普通 `distributed_fs` 路径配置。配置文件必须为仅所有者可读的普通文件，
PostgreSQL 密码从单独文件读取，不能写入 URL；对象存储凭据保留在 JuiceFS 部署层。
该驱动不替用户格式化已有卷或启动生产 PostgreSQL/Ceph。

基线使用 JuiceFS 1.4.1、PostgreSQL 17、Ceph RGW，`--cache-size=0 --cache-dir=memory`，
保持默认 I/O 调优及关闭 writeback。AFS 对照使用同一 ADX 发布包、sandboxd/Firecracker、
guest 镜像、资源规格、工作负载与存储所在设备。计时关闭 strace 和 Meta profile，
诊断单独执行。冷目标仍可能受文件系统与服务端缓存影响；记录各层读字节和配置，
不能把所有冷目标样本当作全链路 cache miss。

比较必须保留捕获发布、恢复准备、COW、VMM load、agent／路由、首个 HTTP 和延迟触页的边界。
每次流量唤醒使用 12 个请求、两个 Ingress，并以真实 VMM 记录确认只恢复一次。
节点故障、Coordinator 重启和删除保护仍走同一生命周期用例。
Coordinator 完成共享文件删除不代表 JuiceFS 底层对象已即时回收；
对象垃圾回收和 trash 策略须单独观察，不套用 AFS Meta 的回收统计。

JuiceFS 的 open-unlink 保证限于同一挂载（见其
[POSIX 兼容性说明](https://juicefs.com/docs/community/posix_compatibility/)）。
Coordinator 必须在运行／恢复引用释放前保留文件目录项；跨客户端打开的 FD
不能替代该引用保护。通用生命周期已经按这个规则延迟共享文件删除，
不在 adxlet 或 Coordinator 中增加 JuiceFS 类型分支。
文件系统探针独立验证 `copy_file_range` 无对象数据下载、克隆写入不修改源文件，
以及删除源文件后仍有目录项的 COW 副本内容完整。

共享目录的 Firecracker 验证使用外部文件系统写者、两个节点中的只读消费者，
校验只读约束、恢复后共享数据保持最新值、私有状态恢复以及删除 Environment 后共享目录保留。
本轮固定的 sandboxd 构建拒绝 Firecracker RW host-path；SDK 的 `rw` 表达能力
不能当作该运行时已经支持可写业务共享目录的证据。

### 原生盘基线的方法与诊断

2026-10-20 的基线使用专用 KVM 主机上的两个隔离执行节点、两个 Ingress。
经授权清空使用的 200 GiB 数据盘划分为 80 GiB 原生 XFS（reflink 开启）与剩余空间的
原生 Ceph BlueStore OSD；AFS 数据、运行目录及 PostgreSQL 在 XFS 分区。
两种后端共用一块物理盘，因此这是存储栈对比，不是独立硬件的对照试验。
历史文档的原始 workload 源码未取得，本轮按其规格重建确定性 512 MiB 内存负载，
修改其中 64 MiB 后再做增量捕获；不能声明与原程序逐字节相同。

每轮先直接调用 sandboxd，分别验证 Full 与 Incremental 的跨节点恢复，再走公共 SDK
的双 Ingress 冷、热流量组。每一步均核对完整 512 MiB SHA-256 与进程状态连续性；
首次 HTTP 不计算 checksum，完整触页耗时另计。
SDK 冷、热组分别创建独立 Environment，各使用其首个恢复点；它们不构成长时间多轮
增量捕获的增长曲线。直接 sandboxd 探针才在同一执行状态上连续验证 Full 与 Incremental。
每轮重新启动文件系统客户端，同轮的后续步骤复用这些客户端，服务端及 Linux 缓存保留。
JuiceFS 关闭数据块缓存，
保留默认 32 MiB readahead／300 MiB buffer；AFS 使用产品默认参数。
Async I/O、VM 规格、工作负载、发布包与外部运行时身份均须一致。
其中 Async 控制虚拟磁盘 I/O；`memory.live` 仍采用 SharedFile，内存访问经过所挂载的文件系统。

AFS 的独立 CPU profile 暴露出 GC 引用更新复制整个 Meta 命名空间的问题：
两个 15 秒样本中，`update_gc_reference` 的 inclusive CPU 占比约 68%／74%，
其中完整 `snapshot` 约 40%／49%。仅改为点读后，真实并发访问又暴露了
全局 Store revision CAS 的饥饿，64 次重试耗尽导致内存捕获 EBUSY。
因此修复同时覆盖固定视图点读与相关 key 条件提交；实际 GC 扫描仍保留全局 revision
校验。并发测试覆盖无关提交、迟到读者、引用释放、session 替换及 chunk 回收屏障。

后续条件诊断将剩余竞争定位到同一个 publication 引用：一轮记录 38,389 次引用 revision
冲突，其他已记录条件合计 70 次。多个上传任务为同一代内存写入并发追加保护块，
相关 key CAS 仍可能耗尽重试。Meta 因此使用固定 64 个异步锁分片，同一引用先排队再读取
权威视图；引用、inode、chunk、session 和 GC 的持久化条件全部保留。
256 路并发追加不同块的回归先在旧实现复现失败，修复后验证全部成功且 256 个块完整保留；
Meta 定向回归为 164 通过、1 忽略。真实性能轮还须检查后台引用准入重试耗尽记录为零。

JuiceFS 的独立对象请求诊断发现重复 GET、读取消重试与 live memory 写入后的版本变化；
一个 4 MiB 内存对象出现 51 次读取。VMM 的按需映射与底层对象读放大须分别观察。
代理记录的是写入 socket 的字节，取消请求时与客户端实际消费字节不同；诊断轮开启
额外日志与请求代理，其耗时不计入正式对比。agent 的健康握手、网络恢复与释放阶段
分别计时，关联窗口中的并发 GET 只能说明相关性，不能直接认定为唯一原因。
对应版本的 [JuiceFS reader 实现](https://github.com/juicedata/juicefs/blob/0b90c7d/pkg/vfs/reader.go)
中，BUSY 读被 invalidate 时进入 REFRESH，未使用的请求在 drop 时可被取消；
这与观察到的重复读取、取消现象相容，但尚未通过单变量对照确认各自贡献。

压测服务还须能容纳恢复后的连接突发。早期轮次恢复及路由发布已在 1.25 秒内完成，
12 个请求中 8 个成功、4 个在 Node Proxy 的 3 秒 TCP connect 期限内失败。
独立复现暂停 accept 进程时，Python 默认监听队列仅接纳 5／12 个连接；
工作负载改用 128 的队列后接纳 12／12，网关超时保持不变。
旧轮次保留为失败证据，不与新工作负载身份的性能样本合并。

#### 本轮分段结果与未通过项

统一 package-05、同一工作负载身份的原生盘批次中，JuiceFS 三轮全部通过，AFS 两轮
通过、第三轮在 SDK 热唤醒返回 HTTP 504。以下仅统计已通过完整校验的对应阶段，
保留失败整轮，不能当作两后端均通过的正式性能验收。数值为秒，中位数（最小–最大）；
AFS 热唤醒只有两个完成样本，另一次失败单列。

| 分段 | AFS | JuiceFS |
| --- | --- | --- |
| 直接 Full 捕获发布，n=3 | 3.94（3.86–3.95） | 3.88（3.85–3.88） |
| 直接 Full 恢复 RPC，n=3 | 1.19（1.17–1.19） | 5.08（3.34–6.14） |
| 直接 Full 首 HTTP，n=3 | 1.37（1.35–1.37） | 21.96（20.61–25.33） |
| 直接 Incremental 捕获发布，n=3 | 38.47（35.60–39.69） | 2.85（2.68–8.93） |
| 直接 Incremental 首 HTTP，n=3 | 3.95（3.31–12.28） | 28.70（28.47–32.26） |
| SDK 冷唤醒首 HTTP，n=3 | 1.01（0.99–1.68） | 7.21（5.20–7.70） |
| SDK 冷唤醒后完整触页，n=3 | 34.96（34.23–42.12） | 6.90（6.65–7.17） |
| SDK 热唤醒首 HTTP | 8.24／12.38；第三轮失败 | 10.50（8.86–11.41），n=3 |

直接路径的 workload 由 sandboxd 初始进程启动，SDK 路径由 Execd 启动；两者的 guest
初始化与页面访问历史不同，不能用它们的耗时相减计算 ADX 开销。SDK 首 HTTP 取
12 个请求耗时的最小值，读计数和 RSS 则在全部 12 个请求返回后、checksum 前读取，
不能视为恰在首响应时刻的采样。

JuiceFS 三轮 SDK 冷／热组恢复准备中位数约 4.82／2.64 ms，VMM load 约 59／54 ms，
agent 约 2,540／5,801 ms，其中健康握手约 2,198／4,651 ms；Ingress 路由等待约 10.6／10.5 ms。
首批请求后 memory.live 映射 RSS 约 41／42 MiB，完整触页后约 557 MiB，证明没有在恢复前
把 1 GiB guest 内存全部触入。但客户端 GET 数据量中位数已达约 1,179／1,568 MiB，
完整触页后累计约 2,380／2,805 MiB。按需映射有效不代表对象读取无放大。

AFS 两个通过的热样本首响应分别为 8.24／12.38 秒，相关恢复／路由发布约
8.07／4.34 秒，慢耗时分布在 agent／finalize 和发布后的请求响应。失败热样本恢复和
路由约 0.8 秒完成，第二个 Ingress 的 6 个请求在 Node Proxy TCP connect 的 3 秒期限
内失败，第一个 Ingress 的 6 个请求约 33 秒后成功。该轮热唤醒未完成完整内存校验；
不通过忽略 HTTP 504、延长网关期限或只取成功请求来判定通过。

这六轮后台 GC 引用准入重试耗尽记录均为零，fixture 清理错误均为空；这证明本次引用
竞争修复通过该检查，并不代表运行中写回延迟已消失，也不代表已执行完整故障／GC 收敛套件。

额外 AFS TCP／vCPU 诊断轮不计入上述正式批次。该轮通过完整校验、冷／热各 12 请求及
单次恢复，首响应为 6.62／1.05 秒，未复现 504。vCPU 200 ms 间隔采样中，冷唤醒窗口
30 个样本有 24 个停在 `fuse_wait_on_page_writeback`；完整触页窗口 255 个样本中有
175 个停在该处。代表性栈为 `record_steal_time → do_wp_page → fuse_page_mkwrite →
fuse_wait_on_page_writeback`：KVM 写 guest 共享内存页时等待 FUSE 页写回，运行态写入
也会阻塞 vCPU。该轮两组各 12 条业务 TCP 握手均完成，SYN 到 SYN-ACK 最大延迟
分别约 0.53／0.37 ms。因此已确认写回阻塞，但尚无失败轮的包级证据将之前的 504
唯一归因于它；网关 connect 期限、AFS 写回尾延迟及其竞争窗口仍是未解决项。

#### 同负载原生 XFS 对照

在同一专用 KVM 主机、原生 XFS 运行盘、同一 package-05、sandboxd／guest 镜像及
`deterministic-512m-v1` 负载上，直接 sandboxd 对照三轮全部通过。磁盘使用 Async I/O，
每轮均完成 Full／Incremental 恢复、真实 HTTP、512 MiB 全量 SHA、PID／nonce 连续性，
最终两个执行节点 inventory 均为零。中位数如下，单位为秒：

| 直接 sandboxd 分段 | XFS Full | XFS Incremental |
| --- | ---: | ---: |
| 捕获发布 | 0.372 | 0.247 |
| 恢复 RPC | 0.919 | 0.278 |
| 首 HTTP | 1.227 | 0.330 |
| 首响应后完整工作集校验 | 4.414 | 4.334 |

这三轮标记 `native_control_only`，没有运行 ADX SDK 或流量唤醒；两个执行节点在同一
物理主机看到 XFS 目录，不代表 XFS 能跨机器共享。Full 首 HTTP 的三个原始值为
1.227／2.246／1.150 秒，保留第二轮波动。只比较相同的直接 sandboxd 路径；SDK 服务
的初始进程与页面历史不同，不能将 XFS 直接路径作为 SDK 的可相减基线。

独立 JuiceFS 单变量诊断将 max-readahead 从默认 32 MiB 降至 0。该轮通过校验，SDK
冷／热首 HTTP 为 6.128／4.479 秒，全部并发请求后累计 GET 为 501／397 MiB；但随后
完整触页升至 15.414／14.640 秒，之前三轮默认预读中位数为 6.898／8.101 秒。
因此不能只凭 GET 下降判定整体优化；该轮作为诊断记录，不并入正式性能批次。
固定版本在 cache-size=0 时已自动禁用块 prefetch，后续对照只调整有效的 readahead
参数。实现依据为 [JuiceFS cached store](https://github.com/juicedata/juicefs/blob/0b90c7d/pkg/chunk/cached_store.go)。

CPU／等待诊断使用另行构建并配对的 guest agent 与 sandboxd，采样结束后恢复原运行时，
不将诊断耗时并入正式性能结果。健康检查 handler 本体的 guest CPU 记账约
0.09–0.14 ms；AFS SDK 冷／热健康阶段为 552／195 ms，JuiceFS 4 MiB 预读为
2,165／4,141 ms。对应宿主 vCPU 运行估计为 59／50 ms 和 109／145 ms，其余包含
缺页、脏页限速、guest 休眠及传输等等待，不能全部归为某一种存储操作。
其中采样分别覆盖约 463／194 ms 和 2,159／4,126 ms；未覆盖区间不推算为零 CPU。

guest `getrusage` 在 SharedFile 恢复期不能直接代表物理 CPU。实测 JuiceFS 一个
277 ms release 阶段记了约 275 ms guest process CPU，同窗宿主 vCPU 实际运行估计
仅约 8 ms；因此主要结合宿主 `schedstat` 与等待栈判断计算／等待。
宿主 vCPU 时间包含整个 guest 和 KVM 开销，也不能独占归给 agent。

AFS 页写回排队修复后的诊断轮全部通过，但增量捕获仍为 33.18 秒：3,383 次普通 write
的后端执行累计 31.25 秒，其中 16 次落在 1–5 秒桶。冷／热完整触页约
29.01／20.31 秒，vCPU 同窗采样覆盖约 29.01／20.32 秒，FUSE 等待估计约
22.70／15.66 秒。这确认了长尾仍存在，不能把接收线程的排队修复表述为全部解决。
据此，普通 native write 准入也交给有界 worker，同句柄顺序和原持久化屏障保持不变。
阻塞上传回归先在旧实现得到 Borrowed/Queued 断言失败。owned FUSE 输入另设每 mount
32 MiB 字节预算，复制前取得额度，执行或丢弃后释放；额度耗尽时仍背压。关闭会唤醒
等待者、返回 EIO，未准入的任务只丢弃正文，不执行后端工作。两项并发／关闭回归在
无预算等待时先失败，补齐等待后与 52 项 FUSE、15 项 native 写入测试一起通过。
同 fh FIFO、关闭排空与持久化屏障保持不变，不能提前应答未完成的 fsync 来缩短统计值。

JuiceFS 的独立几何对照使用同一 package-07、原运行时、4 MiB readahead，比较
4 MiB 与 1 MiB 块的新测试卷。单轮冷／热唤醒后累计 GET 分别减少约 60%／58%，
完整触页后累计 GET 减少约 39%／39%；该诊断对照不混入后续三轮正式样本。
1 MiB 块通过创建独立卷验证，原 4 MiB 卷及配置保留，不表示已进行存量卷迁移。
ADX 的生产配置仍使用通用 `distributed_fs`，块和预读设置归 JuiceFS 挂载配置管理。
后端比较工具拒绝把不同块大小、预读或缓存参数的同类后端混合统计。


#### package-08 优化后的三后端复测

`final-v8-batch-01` 九轮串行全部通过，无重试；AFS、JuiceFS、XFS 各三个独立试验。
三者的 package-08、sandboxd／Firecracker／kernel／initrd、guest 镜像、HTTP probe 和
工作负载身份均已核对。源代码固定在 `feat/snapshot-storage-modes` 的
`fab350f1c21b2788ae291cc31127d32ef64c6931` 加本次工作树修改，Rust 与驱动文件分别由
`writeback-08-manifest.json`、`drivers-14-manifest.json` 固定。
JuiceFS 使用 1 MiB 块、4 MiB readahead，cache-size=0、memory cache-dir、writeback 关闭；
PostgreSQL 同步提交与原生 Ceph OSD 保持不变。每轮启动新客户端，未清空服务端缓存。

以下为中位数（最小–最大），单位秒。XFS 是直接 sandboxd 控制组，不运行 SDK。
首响应后的完整校验均覆盖 512 MiB 确定性工作集，不能解释为对运行中整个 1 GiB guest
RAM 逐字节比较。直接路径与 SDK 路径不相减计算平台开销。

| 分段，n=3，秒 | 原生 XFS | AFS | JuiceFS |
| --- | ---: | ---: | ---: |
| 直接 Full 捕获发布 | 0.370（0.360–0.400） | 3.866（3.808–4.013） | 3.887（3.867–3.889） |
| 直接 Full 恢复 RPC | 1.048（0.850–1.111） | 1.150（1.139–3.902） | 3.997（3.826–4.291） |
| 直接 Full 首 HTTP | 1.461（1.210–1.477） | 1.336（1.313–4.118） | 8.049（7.915–8.866） |
| 直接 Full 完整工作集校验 | 4.438（4.153–4.484） | 13.372（5.007–14.277） | 22.782（19.496–22.951） |
| 直接 Incremental 捕获发布 | 0.263（0.260–0.291） | 26.978（25.599–29.730） | 2.624（2.500–3.388） |
| 直接 Incremental 恢复 RPC | 0.271（0.249–0.287） | 1.531（1.285–5.778） | 12.205（11.687–12.951） |
| 直接 Incremental 首 HTTP | 0.367（0.339–0.386） | 2.355（2.088–6.735） | 26.561（25.322–28.537） |
| 直接 Incremental 完整工作集校验 | 4.484（4.375–4.830） | 41.083（37.652–53.790） | 27.596（26.487–32.475） |
| SDK 冷组捕获发布 | — | 11.978（10.803–12.012） | 3.962（3.959–3.968） |
| SDK 冷首 HTTP | — | 1.079（0.824–1.131） | 6.793（6.678–7.212） |
| SDK 冷完整触页 | — | 29.647（9.047–34.165） | 6.870（6.465–7.250） |
| SDK 热组捕获发布 | — | 8.812（8.702–9.423） | 3.985（3.977–3.994） |
| SDK 热首 HTTP | — | 6.351（4.167–7.758） | 5.865（5.795–6.623） |
| SDK 热完整触页 | — | 17.645（11.639–20.333） | 7.428（6.979–8.155） |

AFS／JuiceFS 冷、热每组每轮各 12 个真实 HTTP 请求跨两个 Ingress，物理恢复均为一次，
全部 nonce、进程连续性、SHA、两个执行节点最终零 inventory、fixture 清理均通过。
AFS GC 引用准入重试耗尽记录与所有 secret findings 均为零。
这组运行使用 benchmark 范围，未重跑完整节点故障／Coordinator 重启／GC 收敛套件。
两个执行节点位于同一专用 KVM 主机，不代表两个物理故障域或存储 HA。

SDK 阶段的中位数如下，单位毫秒。guest agent 指 sandboxd 的 guest agent 握手阶段，
Execd readiness 是 adxlet 随后的运行时就绪确认，两者分别观测；父子阶段和不同阶段的
中位数不能相加还原端到端耗时。

| SDK 分段 | AFS 冷 | AFS 热 | JuiceFS 冷 | JuiceFS 热 |
| --- | ---: | ---: | ---: | ---: |
| adxlet 恢复准备 | 24.05 | 20.30 | 4.69 | 2.79 |
| sandboxd VMM load | 86 | 84 | 51 | 61 |
| sandboxd guest agent | 545 | 335 | 2,756 | 2,791 |
| Execd readiness | 82.89 | 3,568.42 | 1,047.06 | 897.55 |
| Ingress 等待路由 | 10.47 | 10.47 | 10.46 | 10.79 |

直接 Incremental 也须独立看待：JuiceFS VMM load 中位数 1,922 ms、guest agent
10,145 ms；不能把所有场景的 load 都概括为几十毫秒。对应 XFS load／agent 为
129／85 ms，AFS 为 182／1,138 ms。COW 和恢复准备不解释全部恢复后访问成本。

JuiceFS SDK 冷／热组全部唤醒请求返回后，累计 GET 中位数为 625／650 MiB，完整触页后
累计为 1,577／1,679 MiB。与旧 package-05、4 MiB 块／32 MiB readahead 批次相比，
前一窗口分别下降约 47%／59%，后一窗口下降约 34%／40%；冷首 HTTP 从 7.21 秒变为
6.79 秒，热首 HTTP 从 10.50 秒变为 5.86 秒。旧、新包分别统计，不混合为同组样本，
该前后对比不单独证明某一项修改的因果贡献。各类文件和重复访问都进入客户端 GET
计数，不能直接把总量除以 guest 工作集称为纯内存读取放大倍数。
两后端唤醒响应后的 VMM memory.live RSS 约 40–42 MiB，完整触页后约 556–557 MiB，
仍未在恢复前把 1 GiB 内存全部触入。

AFS Incremental 捕获发布中位数由旧批次的 38.47 秒降至 26.98 秒，约下降 30%，
但 SDK 捕获发布仍约 8.81–11.98 秒，冷完整触页中位数仍为 29.65 秒。
直接 Full 完整校验中位数为 13.37 秒，旧批次为 9.46 秒，因此不能表述为全面加速。
三轮热组 peer 正文读取均为零，仍需 11.64–20.33 秒完整触页；其 Execd readiness
为 3.29–5.67 秒。现有证据同时指向本地 FUSE 写回／发布和运行时就绪等待，尚未把
全部 readiness 时间归因于单一存储操作或控制流重连。
首轮增量阶段普通 write 后端执行累计 26.82 秒，复制／准入／排队累计约 0.025 秒；
页写回排队累计约 116.61 秒，包含并发调用，不能当作 29.73 秒暂停的独立可加分量。
AFS 接收线程和有界输入修复已通过正确性验证，后端写回长尾仍是待继续优化的主要问题。

原始证据归档 `final-v8-batch-01-public.tar.gz` 的远端／本地 SHA256 均为
`87d0828269b6df52a8047d0fa41f9cd4a15fe8fa158284ac609355921b1344f1`。
完整样本与独立范围统计保留在 `final-v8-summary.json`、`final-v8-comparison.json` 和
`filesystem-package08-stages.json`；未将 CPU／Meta 诊断轮或单轮调参样本纳入正式九轮。
