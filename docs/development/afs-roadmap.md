# AFS Roadmap

- 状态：Draft for discussion
- 范围：AFS（OwnerFs + DFS）
- 最后更新：2026-10-27

## 1. 定位

AFS 是 Agent DX 的可选文件系统子系统，提供两种模式：

- **OwnerFs**：小规模 Agent workspace，以 Home 节点真实目录为权威，远端 Node 经授权 peer 访问。
- **DFS**：一写多读分布式文件，已提交数据表示为不可变 chunk + FileVersion。

AFS 的目标：Agent 场景下的共享文件存储。DFS 吞吐优先；OwnerFs 以本地/远端工作区可用为先。

## 2. 支持范围（AFS-wide）

| 范围 | 当前处理 |
|---|---|
| OwnerFs workspace bind mount | 保留实现和配置，默认 OFF；显式配置后用于当前试用场景 |
| OwnerFs 远端 FUSE | 保留代码路径；目标仓二进制需 Linux 运行证据 |
| DFS 一写多读 | 原生 Node 路径；1GiB 多副本/CRC 有限验证；外部 S3 实验 |
| 中心 Meta `local-file` | 当前可重启恢复基线 |
| `memory` Meta | 仅用于演示，重启不保留 |
| etcd / Redis Meta | 作为 `MetaLog` 后端可用（只存操作日志与 checkpoint，Meta 仍单进程，不做 HA）；持久性与性能边界见[部署说明](../deployment/afs.md#meta-操作日志的-redisetcd-后端) |
| 跨节点 `fcntl/flock` | 明确不做 |
| 完整 POSIX / 多 Meta / HA / 大规模长稳 | 未完成，见 [dfs-plan.md](dfs-plan.md) |

必要正确性边界：读写新鲜度、close-to-open、权限与 suid/sgid 清除、错误传播、持久化屏障、direct-I/O mmap 协商、正常卸载与排空。

## 3. OwnerFs（待补）

现状：

- workspace bind mount 默认 OFF；远端 FUSE 路径保留。
- 支持范围与验证边界见 [architecture](../architecture/afs.md)、[deployment](../deployment/afs.md)、[testing](../testing/afs.md)。
- **Roadmap：本节留白**；后续补 OwnerFs 的主题、阶段和验收。

## 4. DFS

### 4.1 主题 × 阶段

6 个主题按能力域划分；每个交付项按主要归属列一次。

| 主题 | Now | Next | Later |
|---|---|---|---|
| **性能与扩展** | 大文件写/读；usrbio 零拷贝；读扇出/预取；元数据性能；基准套件；客户端条带；缓存层级；小文件/目录规模；SDK 通道 | RDMA/TCP；多客户端/尾延迟/QoS；集群规模 | GPU Direct Storage/KVCache |
| **可靠性与完整性** | 持久屏障；WAL/崩溃恢复；Meta 持久化/重启；复制/多副本；跨主机 R=3；完整性；fencing/租约；元数据备份/恢复；长稳 | Meta HA；scrub；故障演练；备份/恢复/DR | 跨集群复制/容灾 |
| **运维与可观测** | 指标；健康检查；管理 API/状态；fsck/gc/status | 日志/追踪；仪表盘/告警；容量/用量报表；升级/回滚；诊断；滚动升级；监控集成 | 多集群管理 |
| **接口与兼容** | POSIX 基础语义；权限；close-to-open；mmap/O_DIRECT；FUSE | xattr；本地锁；fallocate/punch hole；copy_file_range/readdirplus/lseek；K8s CSI | NFS/SMB/WebDAV/CSI 通道；S3 网关 |
| **数据管理与安全** | FileVersion；原生 GC；S3 引用感知 GC；mTLS；认证/授权 | COW/克隆；快照；版本保留；回收站；配额；目录统计；导入/导出/同步；POSIX ACL；多租户；租户配额；审计 | 冷热分层；压缩/去重；静态加密/密钥管理；迁移/均衡；合规 |
| **生态与部署** | checkpoint 集成；单机/多机；存算分离；本地盘；边缘/Agent；控制面/数据面分离 | 数据集工具；客户端语言/SDK；云/混合；S3 后端；K8s 部署 | Hadoop/Spark/Flink；容器运行时；HPC；跨平台客户端；异构/NUMA；多集群联邦 |

跨节点文件锁（flock/fcntl）明确不做。

### 4.2 细节

当前状态、91 行明细、非目标与验收见 [dfs-plan.md](dfs-plan.md)。

## 5. 候选版本验收（AFS-wide）

每个准备交付的 AFS 候选版本必须同时完成工程检查和适用的限定实际运行：

1. 默认 OFF 构建、测试和包边界。
2. 显式 ON 构建、严格 all-features Clippy、受影响单测及真实带组件包。
3. 从本轮 ON artifact 安装、配置、启动、健康检查、正常停止和卸载。
4. OwnerFs bind ON + 远端双向访问、权限/清位/errno、local-file 有序重启恢复、direct-I/O mmap 和正常卸载/排空，以及小规模 DFS 一写多读。
5. 验收辅助程序只按需构建，不进入普通包；版本、二进制、配置和证据可追溯。

未完成的必要出口继续登记为待验收或阻塞，不能因代码合入而自动放行。

## 6. 文档与配置

- 架构边界：[docs/architecture/afs.md](../architecture/afs.md)。
- 部署与配置：[docs/deployment/afs.md](../deployment/afs.md)。
- 测试与验收：[docs/testing/afs.md](../testing/afs.md)。
- DFS 细节：[docs/development/dfs-plan.md](dfs-plan.md)。
- 示例配置：[build/config/examples/afs/](../../build/config/examples/afs/)。
- 迁移来源与许可证：[迁移来源索引](../migration/sources.json) 与 [AFS 快照报告](../migration/2026-10-09-afs-snapshot.md)。
