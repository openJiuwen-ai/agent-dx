# Agent FS（AFS） 架构边界

Agent FS（AFS） 是 Agent DX 的可选文件系统子系统。它位于根级 `afs/`，
只在显式设置 `ADX_WITH_AFS=1` 时进入文件系统专用构建、测试、带组件发布包
和部署入口；默认 Agent DX 构建、包和部署不包含 `afs-meta`、`afs-node`
或 FUSE 专属系统依赖。

## 架构总览

![AFS 组件与控制、数据路径](../../assets/architecture/afs.svg)

OwnerFs 与 DFS 是平级模式。虚线箭头表示控制与持久元数据，实线表示数据访问；
`afs-meta` 不转发文件正文。DFS 节点复制与外部 S3 是替代路径，不能把图中两条
路径理解为外部模式同时保留 Node 耐久副本。图示描述当前工作树的实现边界，
不代表所有模式已完成运行或性能验收。

## 组件职责

AFS 把用户可见的文件语义和文件字节搬运分开：

| 组件 | 职责 |
| --- | --- |
| `afs-meta` | 命名空间、inode、写租约、文件版本、布局、放置、副本目录和幂等提交结果 |
| `afs-node` | FUSE 会话、OwnerFs Home 访问、DFS dirty 状态、本地 chunk 存储、复制执行、远端读、验证缓存和卸载排空 |
| `afs/common/*` | AFS 域内共享的错误、日志、指标、协议、追踪和传输 crate |
| `afs/client` | DFS 客户端 crate；不作为 Agent DX 平台内部 RPC 适配层 |

`afs/common/*` 是 AFS 域内共享层，不纳入 Agent DX 根级产品基础设施。
Agent DX 平台内部 gRPC 仍按 Environment、Runtime、Snapshot、Route 和 Node
责任拆分；AFS 不把 POSIX/Frontend 适配器接入 Platform 内部 RPC。

## Meta 与数据路径

Meta 是可恢复状态的权威，不代理稳定数据流。Meta 校验已提交状态并准备候选状态，只有持久后端接受后才发布提交结果。读请求先固定一致视图，再选择本地副本、远端副本或缓存来源，避免一次复合读混用多个 revision。

OwnerFs 面向小规模 Agent workspace。Home 节点把 workspace 保存为普通本地目录；远端节点通过受控 peer 回到 Home 访问文件。远端传输会话本身不授予文件访问权限，每次打开、读写、flush、sync 和 close 都必须校验授权、句柄、权限、root grant、session、Home session 和 fence。

DFS 把已提交数据表示为不可变 chunk。普通写入先进入 inode 的 dirty 状态；sync、同步写标志、close-time flush 或后台策略触发提交，形成新的 `FileVersion`。复制发生在文件布局之下，Meta 只在收到可验证持久 receipt 后提交新版本和副本目录。验证缓存不能自动算作持久副本。

所有启用 OwnerFs 或 DFS 权威能力的 Meta 都要求 mTLS 和
`trusted_node_certs`。CA 只证明证书链可信；Meta 还把握手得到的叶证书 DER 精确
映射到一个 Node ID，并校验 RPC 中声明的 caller／holder／owner 身份。DFS-only
部署与 OwnerFs 部署执行同一身份绑定，不能用另一张同 CA 证书冒充目标 Node。
Node 的 Meta、peer control 和 data 诊断请求复用生产 TLS 配置，诊断不建立明文旁路。
这里的 Node ID 绑定发生在服务端对调用方的校验中；客户端仍按 CA 和
`tls_server_name` 验证服务端，不承诺按目标 Node ID 对服务端证书逐一 pinning。

## OwnerFs 远端与 bind 数据路径

![OwnerFs 远端访问与本地 workspace bind](../../assets/architecture/afs-ownerfs.svg)

远端 FUSE 经 OwnerFiles 到 Home 访问普通文件，Home 逐操作核验授权和身份。
本地 workspace bind 的源是同一个 Home 底层真实目录，目标是 OwnerFs FUSE
根下对应的一级目录；该子树的访问由 Linux VFS 直接进入底层文件系统。
挂载授权与身份核验属于生命周期，不表示每次 bind 文件操作都回到 Node。
其他未被 bind 覆盖的目录仍走 FUSE；容器不是此能力的前提。

图为逻辑时序，root/Home 查询按需发生，不承诺每次读写的固定 RPC 数。
bind 与远端访问共享底层文件，新打开的可见性及权限仍需按支持范围验证；
图示不承诺已有 FD/mmap 即时撤权或跨节点锁一致性。

## DFS 外部对象持久化边界

![DFS 外部 S3 写入发布与独立读取](../../assets/architecture/afs-dfs-s3.svg)

图示为当前外部 S3 实验路径：PUT 成功与 Meta 持久 ACK 是两个独立屏障。
顺序 write 可与预上传重叠，独立读者只见已提交 head；fsync 强制收口，
成功返回仍要求所有必要对象已获成功回执且 Meta 已持久确认。缓存命中
可省略 GET，但不能省略适用的版本、授权、对象身份和期限检查。
此预上传与 64MiB 发布窗口不作用于 OwnerFs。

可选外部后端配置把新文件系统固定为单副本策略：本地暂存不是耐久副本，
不运行 Node 复制／修复。Node 用官方 S3 客户端执行普通 PUT，成功后返回
绑定 store/generation/key、完整长度和摘要的有类型凭据。Meta 在原版本提交
事务中发布外部对象引用和 inode head；旧 Node 复制凭据与外部凭据不能混用。

S3 桥接复用一个自有线程、runtime 和客户端，容量1的入口队列满时等待；
worker 最多持有32个命令。所有命令共享四个上传许可，单个 PUT 完成即可
调度下一块，不再等待固定四块全部完成。顺序写每满4MiB即预上传，每 inode
最多64MiB封口；最终发布仍通过原 Meta 条件事务串行执行。窗口首 PUT 前
固定 operation_id，成功回执仅归属本窗口；部分失败保留窗口和成功部分，
Meta 未知确认保留精确请求并阻挡后续 mutation。fsync／可写 flush／同步写
仍等待全部必需回执及 Meta 持久确认。覆盖和截断先安全排空已有窗口，再走
同步修改路径。写侧通过字段私有的 `StagedChunk` 传递已封口身份，HTTP PUT
使用不可变 payload 视图，避免封口后再做一次全量 hash 或 `to_vec`；dirty、
stage、transport、retry 和最终 owner 交接共享有类型预算租约。首次封口仍要
完整计算 chunk 身份，原有预算、fsync、错误传播和退出排空语义不降低；这里
不宣称全链路零拷贝。独立只读客户端只见已提交 head；同挂载／owner 代理仍读
dirty 视图。未同步崩溃只恢复最后已提交版本，不新增 WAL 承诺。服务端已接受的
不可变对象不能取消撤销或盲删，正常退出关闭入口、排空并 join worker。

B 使用外部读授权 RPC，校验 reader epoch、MAC、版本／布局、对象身份、
范围和期限后 GET；每块完整校验，在最终返回前再次检查期限。该路径不伪装
Peer 接收者，也不依赖原 writer session。内容 cache 默认 OFF；可选 Node
本地有界异步 cache，以 store/generation/chunk 身份隔离。完整校验后的内容
先进入有界 pending，再由单后台线程写入临时文件并 rename；正常退出排空。
读仍先获 Meta 授权，损坏缓存丢弃后回源。S3／磁盘读取只通过字段私有、构造时
校验完整长度和 BLAKE3 的只读缓冲区交给读引擎；引擎核对完整 chunk 身份后
复制授权范围，保留最终期限检查。开启cache时，Node共享一个最多4MiB的已验证
对象槽，按完整chunk身份复用相邻请求；每次读取仍刷新head并重新授权。驻留、
pending／worker以及槽位替换后仍被读者持有的已接纳分配，共用原pending额度，
按Vec容量去重计费，最后引用释放后才可复用额度。预算不足不保留新槽位，回退
原读取路径。新磁盘加载仍核对私有文件并完整校验；RAM命中不探测磁盘副本之后
的损坏／权限变化，检测延后至下一次磁盘加载。不是每个handle各保留一个对象。
S3 GET 与 Ready 磁盘缓存重载共享每个 store 的私有两线程 BLAKE3 池，使用固定
官方 BLAKE3 的 rayon 接口计算同一个完整摘要；入口最多允许一个对象校验，
不增加对象队列、预读、驻留或载荷额度。调用者等待校验完成，不能将此解释为
异步读流水线。校验异常关闭入口并保留失败；正常退出先排空已有工作，再显式
关闭并 join 校验线程，失败传回 Node 退出路径。此机制的功能与性能结论须绑定
实际 Linux 候选，不能由线程数或 hash span 缩短推导 E2E 改善。
外部只读路径已有不可变版本／layout时，将其作为候选，使用
`AuthorizeCurrentExternalRead` 在一个固定的Meta read view内同时检查当前inode head
与全部原有授权，并可在同一视图里解析确实缺失或 TTL 已过期的 external source。
仍存在但 grant 已失效的 cache 条目必须按原 grant 校验并拒绝，不能降级为 missing
再重签。head不匹配时不返回旧layout的授权，且在解析旧 head 的 external copy
可用性之前返回 HeadChanged；Node重新获取head并重建读取。旧Meta明确返回未实现时
走原有逐读head＋授权路径，权限拒绝、超时和其它错误不能触发兼容回退，也不能发起
GET 或发布部分输出。冷启动没有layout、稀疏空洞／EOF、dirty写状态和P2P读取保留
原路径。这只合并通信，不缓存新鲜度或授权；输出仍在完整验证及最终期限检查后一次发布。

缓存额度不充当进程 RSS 或文件系统总配额。小块写允许缓存与 PUT 重叠，但只有 PUT 成功才返回持久凭据。
对象存储部署与服务端持久性属于外部依赖，Meta 状态持久化
仍由 AFS 配置的 Meta 后端承担。详见[配置](../deployment/afs.md#dfs-外部-s3-连接)。

## OwnerFs workspace bind mount

OwnerFs workspace bind mount 是当前实际试用场景的优先能力，默认 OFF。开启后，它把 Home 上 workspace 的底层真实目录 bind 到 OwnerFs FUSE 根目录下对应的一级目录，例如 `/mnt/afs/ownerfs/agent1`。把 FUSE 目录自身 bind 到别处不满足此设计。

核心实现只负责挂载、身份核验和卸载；runc、容器启动、执行、停止和探针属于适配层。ON 场景仍需保留授权、Home/root/epoch 核验、数据新鲜度、close-to-open、权限、错误传播、正常卸载和受管引用排空。当前不承诺已有 native FD、mmap、描述符转移或二级 clone 的即时撤权。

## 后置能力

当前不承诺以下能力：完整 POSIX、复杂可靠性、多 Meta、高可用、etcd/Redis
后端验收、大规模长时间运行、RDMA 性能专项、跨节点 `fcntl/flock`、阻塞锁
等待取消、bind/native 与远端 FUSE 锁域协同。bind 本机 ext4 锁和单挂载
内核回退不能宣传为分布式锁。

支持范围、验证边界和性能目标见 [Agent FS（AFS） 演进计划](../development/afs-plan.md)。
