# Agent FS（AFS）

Agent FS（AFS）是 Agent DX 的可选文件系统子系统，为工作负载提供
OwnerFs 与 DistributedFs（DFS）两种文件访问模式。`afs-meta` 管理命名空间、
租约、文件版本、布局与副本目录；`afs-node` 管理 FUSE 挂载、OwnerFs Home
访问、DFS 数据与节点间传输。文件系统权威状态与 Platform Environment 生命周期
保持独立，只复用 ADX 的构建、发布与进程管理入口。

AFS 默认不进入普通 ADX 构建和发布包；通过 `ADX_WITH_AFS=1` 显式启用。
当前部署必须使用 mTLS，并把每张受信节点证书精确绑定到一个 Node ID。

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

Meta 推荐使用 `local-file` 持久后端。`memory` 仅适合一次性演示；etcd、Redis、
多 Meta/HA、完整 POSIX、跨节点文件锁、复杂故障矩阵和性能达标不在当前承诺内。
Operator 负责创建 namespace/root grant、配置挂载和回收引用；Sandbox 创建尚不会
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

## 当前能力边界

- OwnerFs workspace bind mount 默认 OFF；当前实际试用场景需要显式配置 ON。
- OwnerFs 远端 FUSE 访问和 DFS 一写多读代码路径保留，目标仓二进制需要按受影响范围补 Linux 运行证据。
- 中心 Meta `local-file` 是当前可重启恢复基线；`memory` 只适合一次性演示。
- etcd、Redis、多 Meta、高可用、完整 POSIX、复杂可靠性和大规模长时间测试后置。
- 跨节点 `fcntl/flock`、阻塞锁取消、bind/native 与远端 FUSE 锁域协同后置，不能把本机 ext4 锁或单挂载内核回退宣传为分布式锁。

必要正确性边界仍需保留：读写新鲜度、close-to-open、权限和 suid/sgid 清除、错误传播、持久化屏障、direct-I/O mmap 能力协商、正常卸载和受管引用排空。

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
每 inode 最多64MiB收口发布；fsync、fdatasync、可写 flush 和同步写强制
收口。独立只读客户端仍只见 Meta 已提交版本，中间可见时机受最多64MiB的发布窗口约束；同挂载和 owner 代理的 dirty 读保持。只有所需成功回执
齐全且 Meta 持久确认后才能报告同步成功；未知确认精确重试，PUT 失败
保留窗口身份和已确认回执。服务端可能留下未发布对象，不能直接删除共享
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
完整校验。驻留、pending／worker及被读者持有的已接纳对象按实际分配计入原
pending 额度，共享分配只计一次；预算不足则放弃驻留。此额度不是全部前台读缓冲
或进程RSS上限。RAM命中不探测磁盘副本后来的变化，下次加载仍检测损坏并回源。
每次读取仍刷新head并通过Meta授权；已有不可变layout的外部只读路径通过
`AuthorizeCurrentExternalRead` 在同一Meta视图中合并两项检查。head变化或旧服务
明确不支持该RPC时重新获取head走兼容路径，拒绝、超时及其它错误直接传播；
缓存不能替代授权或S3持久化回执，不将上传成功等同于
本地 cache 完成。小块写默认填充，全块写需显式 cache_large_write=true；
队满或运行中写盘失败跳过填充，损坏缓存回源。配置及验证范围见下述链接。
S3 容量查询暂不支持，statfs 明确返回不支持，不能用 Node 空间冒充后端容量。
删除不立即删除 S3 对象；复杂对象回收后置。

SeaweedFS 等 S3 服务是外部运行依赖，其部署脚本、数据库和测试凭据不进入
产品仓。连接方式见[部署配置](../docs/deployment/afs.md#dfs-外部-s3-连接)，
验证边界见[验收说明](../docs/testing/afs.md#dfs-外部-s3-闭环)。
