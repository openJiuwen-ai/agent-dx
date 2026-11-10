# DFS Plan

- 状态：**历史（2026-10-27 规划草案）**。本文保留当时的主题与阶段划分，不再维护；其中多项已落地或已调整，
  当前能力、支持矩阵与已知限制以 [支持边界与已知限制](../architecture/afs-support-boundary.md) 为准，
  与当时规划不同之处见文末"2026-11-09 核对"。
- 范围：AFS DistributedFs（DFS），不含 OwnerFs
- 最后更新：2026-10-27
- 主题矩阵见 [afs-roadmap.md](afs-roadmap.md)

## 1. 定位

AFS DFS 面向 Agent 场景的“一写多读”分布式文件。已提交数据表示为不可变 chunk + FileVersion；`afs-meta` 管理命名空间、写租约、文件版本、布局与副本目录；`afs-node` 负责 FUSE、chunk 存储、复制和节点间传输。

目标负载：模型权重分发、checkpoint 写、数据集冷读、Agent 日常读写。优先级是**吞吐优先**：大文件大块读写第一，POSIX 语义补到 Agent 日常可用。

## 2. 当前状态

已具备（限定 Linux 验证）：

- 不可变 chunk + FileVersion、Meta/数据分离、单写者租约、close-to-open、POSIX 权限/umask/suid-sgid/errno。
- mTLS + 叶证书绑定 Node ID、BLAKE3/CRC32C 端到端校验、持久屏障。
- FUSE 访问；usrbio SDK（合入中）；RDMA transport（feature-gated）；Peer/S3 读缓存。
- 本地盘 / S3 外部后端（实验）、Prometheus 指标、`/health` `/metrics` `/v1/store`。

尚未完成或未验证：

- 跨主机三副本、Meta HA、完整崩溃恢复长稳、scrub。
- 完整 POSIX、POSIX ACL、xattr（部分实现）、配额、快照、回收站。
- fsck/gc/status/upgrade 工具链、K8s CSI、多租户/审计。
- 现有 DFS 性能工作仍有欠账；不能按“已达标”表述。

## 3. 关键交付明细

### 性能与扩展

| 交付项 | 阶段 | 说明 |
|---|---|---|
| 大文件写吞吐 | Now | 1GiB 级、4MiB/1MiB 请求，单流与多流 |
| 大文件读吞吐 | Now | 首读/复读，1MiB 请求，读扇出 |
| usrbio 零拷贝 | Now | 共享内存环、POSIX fd 接入 |
| 元数据性能 | Now | create/stat/readdir；20k ops/s、p99<2ms 目标 |
| 读扇出/副本分摊/预取 | Now | 请求内扇出、副本分摊、下一对象预取 |
| 客户端条带 | Now | 大文件并行条带 |
| 缓存层级 | Now | 页缓存/Peer cache/S3 cache/元数据缓存 |
| 基准套件 | Now | fio/自研；与 3FS/JuiceFS 同条件对照 |
| 小文件/目录规模 | Now | 百万～千万级 |
| usrbio SDK 通道 | Now | Rust SDK、POSIX fd 接入 |
| RDMA/TCP 传输 | Next | feature-gated；按硬件条件启用 |
| 多客户端/尾延迟/QoS | Next | 多客户端聚合、尾延迟、公平性 |
| 集群规模 | Next | 节点/容量/文件数规模 |
| GPU Direct Storage/KVCache | Later | GPU 直读、推理缓存适配 |

### 可靠性与完整性

| 交付项 | 阶段 | 说明 |
|---|---|---|
| 持久屏障 | Now | fsync/fdatasync/O_SYNC |
| WAL/崩溃恢复 | Now | 组提交、checkpoint、恢复链 |
| Meta 持久化/重启 | Now | local-file 重启恢复 |
| 复制/多副本 | Now | 链式/多副本/quorum |
| 跨主机 R=3 | Now | 三副本跨主机、真实故障域 |
| 完整性 | Now | BLAKE3/CRC32C 端到端校验 |
| fencing/租约 | Now | 写租约、epoch、失效与撤权 |
| 元数据备份/恢复 | Now | Meta 备份/恢复最小闭环 |
| 长稳 | Now | 24h+ soak |
| Meta HA | Next | 多 Meta/选主/failover |
| scrub | Next | 后台全量校验、修复 |
| 故障演练 | Next | 慢节点/坏盘/网络分区 |
| 备份/恢复/DR | Next | 备份、恢复、容灾 |
| 跨集群复制/容灾 | Later | 跨集群 DR |

### 运维与可观测

| 交付项 | 阶段 | 说明 |
|---|---|---|
| 指标 | Now | Prometheus `/metrics` |
| 健康检查 | Now | `/health` ready |
| 管理 API/状态 | Now | `/v1/store`、status/info |
| fsck/gc/status | Now | 基础工具链 |
| 日志/追踪 | Next | 结构化日志、Trace |
| 仪表盘/告警 | Next | Grafana/Alert |
| 容量/用量报表 | Next | 目录/租户用量 |
| 升级/回滚 | Next | 滚动升级、回滚 runbook |
| 诊断 | Next | profile/trace/慢查询 |
| 滚动升级 | Next | 不停机升级/回滚 |
| 监控集成 | Next | Prometheus/Grafana |
| 多集群管理 | Later | 多集群 |

### 接口与兼容

| 交付项 | 阶段 | 说明 |
|---|---|---|
| POSIX 基础语义 | Now | open/read/write/truncate/rename/link/symlink |
| 权限/umask/suid-sgid/errno | Now | 权限语义与错误传播 |
| close-to-open | Now | 新 open 取 fresh head |
| mmap/O_DIRECT | Now | direct-I/O mmap、O_DIRECT |
| FUSE 通道 | Now | Linux FUSE |
| xattr | Next | get/set/list/remove |
| 本地锁 | Next | flock/fcntl 本地冲突域 |
| fallocate/punch hole | Next | 预分配/打洞 |
| copy_file_range/readdirplus/lseek | Next | 扩展 POSIX 操作 |
| K8s CSI | Next | CSI driver |
| NFS/SMB/WebDAV/CSI 通道 | Later | 协议网关/CSI 访问 |
| S3 网关 | Later | S3-compatible gateway |
| 跨节点文件锁 | 不做 | 明确不做，见 §5 |

### 数据管理与安全

#### 数据管理

| 交付项 | 阶段 | 说明 |
|---|---|---|
| FileVersion | Now | 不可变 chunk + 版本 |
| 原生 GC | Now | 版本/孤儿/垃圾块 |
| S3 引用感知 GC | Now | 外部对象回收 |
| COW/克隆 | Next | 文件/目录/范围克隆 |
| 快照 | Next | 文件系统级快照 |
| 版本保留 | Next | TTL/条数/引用 |
| 回收站 | Next | 误删恢复 |
| 配额 | Next | 文件系统/目录/用户组/嵌套 |
| 目录用量统计 | Next | du/配额依据 |
| 导入/导出/同步 | Next | sync/rsync/DistCp |
| 冷热分层 | Later | storage class |
| 压缩/去重 | Later | LZ4/Zstd、去重 |
| 迁移/均衡 | Later | 数据迁移/均衡 |

#### 安全与多租户

| 交付项 | 阶段 | 说明 |
|---|---|---|
| mTLS | Now | 传输加密、证书绑定 |
| 认证/授权 | Now | Node/session/epoch/range |
| POSIX ACL | Next | ACL 语义 |
| 多租户 | Next | 命名空间/隔离 |
| 租户配额 | Next | 租户级配额 |
| 审计 | Next | 审计日志 |
| 静态加密/密钥管理 | Later | at-rest encryption/key mgmt |
| 合规 | Later | FIPS/等保 |

### 生态与部署

#### 生态与集成

| 交付项 | 阶段 | 说明 |
|---|---|---|
| checkpoint 集成 | Now | adx-checkpoint-storage |
| 数据集工具 | Next | WebDataset/HF/MosaicML |
| 客户端语言/SDK | Next | Python/C/C++/Go/Java |
| Hadoop/Spark/Flink | Later | Hadoop SDK |
| 容器运行时 | Later | Docker/Podman |
| HPC | Later | Slurm/MPI-IO |
| 跨平台客户端 | Later | Windows/macOS |

#### 部署形态与架构

| 交付项 | 阶段 | 说明 |
|---|---|---|
| 单机/多机/集群 | Now | 单机/同机多节点/跨主机 |
| 存算分离/一体 | Now | Meta/Node 分离；Agent+Node 同机 |
| 本地盘后端 | Now | 本地 chunk 存储 |
| 边缘/Agent | Now | Agent+Node 同机 |
| 控制面/数据面分离 | Now | Meta 与数据面职责分离 |
| 云/本地/混合 | Next | 云/本地/混合部署 |
| S3 后端 | Next | S3-compatible backend |
| K8s 部署 | Next | operator/Helm |
| 异构硬件/NUMA | Later | 异构硬件 |
| 多集群联邦 | Later | multi-cluster federation |

## 4. 阶段依赖

- 正确性/耐久 → 生产可用；跨主机 R=3 依赖复制与 Meta 持久化闭环。
- usrbio 零拷贝 → 大文件写吞吐；读扇出/预取 → 大文件读吞吐。
- 快照依赖 FileVersion；配额依赖 Meta 与目录统计；Meta HA 依赖 Meta 持久化闭环。

## 5. 非目标

- OwnerFs 新功能与性能。
- 跨节点 `fcntl/flock`。
- S3 外部后端的性能专项（功能保留）。
- 完整 POSIX 全量兼容。
- 第一阶段不追求通用大数据/HPC 生态全覆盖。

## 6. 验收与测量

- 功能与边界：[docs/testing/afs.md](../testing/afs.md)、[docs/architecture/afs.md](../architecture/afs.md)。
- 性能：沿用现有 DFS 性能测量合同；不以单次样本或不同口径的数字宣称达标。
- 结论分级：编译通过 ≠ 功能通过 ≠ 测量完成 ≠ 性能改善 ≠ 最终达标。
- 证据：目标仓源码、二进制、配置、环境、用例和结果一起登记。

## 2026-11-09 核对（相对上文规划，以代码 `refactor-afs-dfs` 为准）

- **已落地**：etcd／Redis 作为 Meta 操作日志后端（Meta 仍单进程）；副本节点故障时的降级写与异步补齐；后台巡检读盘（scrub）；
  S3 引用感知回收；readdir 分页（百万项）；usrbio 已在树内（含 POSIX fd 导入与客户端崩溃回收）；
  `user.*` xattr、本机文件锁、`copy_file_range`（元数据 COW 范围克隆）、`lseek` 数据／空洞（内核回退）；
  24 个本机故障场景入库（`afs/tests/suites/dfs/test_faults.py`，进程被杀、擦盘、满盘、对象存储挂起、Redis／etcd 被杀等，非真实网络分区）。
- **仍未做**：跨主机三副本的真实故障域验收、Meta HA／多 Meta、24 小时长稳、元数据备份／恢复、fsck 工具链、
  fallocate／punch hole、readdirplus、POSIX ACL、配额、快照、回收站、K8s CSI、客户端条带、多租户与审计。
- 外部 S3 后端已在支持矩阵内，"S3 后端 Next"与"S3 外部后端（实验）"两处说法均已过期。
