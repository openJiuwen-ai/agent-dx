# DFS/OwnerFs 架构边界

DFS/OwnerFs 是从 DMS/AFS 快照迁入的可选文件系统组件。它位于根级 `dfs/`，只在显式设置 `ADX_WITH_DFS=1` 时进入文件系统专用构建、测试、带组件发布包和部署入口；默认 Agent DX 构建、包和部署不包含 `afs-meta`、`afs-node` 或 FUSE 专属系统依赖。

## 组件职责

AFS 把用户可见的文件语义和文件字节搬运分开：

| 组件 | 职责 |
| --- | --- |
| `afs-meta` | 命名空间、inode、写租约、文件版本、布局、放置、副本目录和幂等提交结果 |
| `afs-node` | FUSE 会话、OwnerFs Home 访问、DFS dirty 状态、本地 chunk 存储、复制执行、远端读、验证缓存和卸载排空 |
| `dfs/common/*` | AFS 域内共享的错误、日志、指标、协议、追踪和传输 crate |
| `dfs/client` | DFS 客户端 crate；不作为 Agent DX 平台内部 RPC 适配层 |

`dfs/common/*` 是 AFS 域内绑定，不纳入 Agent DX 根级产品基础设施。Agent DX 平台内部 gRPC 仍按 Environment、Runtime、Snapshot、Route 和 Node 责任拆分；不得为了迁移 DFS 重新引入旧 POSIX/Frontend 适配器到 Platform 内部 RPC。

## Meta 与数据路径

Meta 是可恢复状态的权威，不代理稳定数据流。Meta 校验已提交状态并准备候选状态，只有持久后端接受后才发布提交结果。读请求先固定一致视图，再选择本地副本、远端副本或缓存来源，避免一次复合读混用多个 revision。

OwnerFs 面向小规模 Agent workspace。Home 节点把 workspace 保存为普通本地目录；远端节点通过受控 peer 回到 Home 访问文件。远端传输会话本身不授予文件访问权限，每次打开、读写、flush、sync 和 close 都必须校验授权、句柄、权限、root grant、session、Home session 和 fence。

DFS 把已提交数据表示为不可变 chunk。普通写入先进入 inode 的 dirty 状态；sync、同步写标志、close-time flush 或后台策略触发提交，形成新的 `FileVersion`。复制发生在文件布局之下，Meta 只在收到可验证持久 receipt 后提交新版本和副本目录。验证缓存不能自动算作持久副本。

## OwnerFs workspace bind mount

OwnerFs workspace bind mount 是当前实际试用场景的优先能力，默认 OFF。开启后，它把 Home 上 workspace 的底层真实目录 bind 到 OwnerFs FUSE 根目录下对应的一级目录，例如 `/mnt/afs/ownerfs/agent1`。把 FUSE 目录自身 bind 到别处不满足此设计。

核心实现只负责挂载、身份核验和卸载；runc、容器启动、执行、停止和探针属于适配层。ON 场景仍需保留授权、Home/root/epoch 核验、数据新鲜度、close-to-open、权限、错误传播、正常卸载和受管引用排空。当前不承诺已有 native FD、mmap、描述符转移或二级 clone 的即时撤权。

## 后置能力

本次 MR 不以前置完成以下能力：完整 POSIX、复杂可靠性、多 Meta、高可用、etcd/Redis 后端验收、大规模长时间运行、RDMA 性能专项、跨节点 `fcntl/flock`、阻塞锁等待取消、bind/native 与远端 FUSE 锁域协同。bind 本机 ext4 锁和单挂载内核回退不能宣传为分布式锁。

支持范围、验证边界和性能目标见 [DFS/OwnerFs 迁移计划](../development/dfs-plan.md)。
