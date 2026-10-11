# Agent FS（AFS）

Agent FS（AFS）是 Agent DX 的可选文件系统子系统，为工作负载提供
OwnerFs 与 DistributedFs（DFS）两种文件访问模式。`afs-meta` 管理命名空间、
租约、文件版本、布局与副本目录；`afs-node` 管理 FUSE 挂载、OwnerFs Home
访问、DFS 数据与节点间传输。文件系统权威状态与 Platform Environment 生命周期
保持独立，只复用 ADX 的构建、发布与进程管理入口。

AFS 默认不进入普通 ADX 构建和发布包；通过 `ADX_WITH_AFS=1` 显式启用。
当前部署必须使用 mTLS，并把每张受信节点证书精确绑定到一个 Node ID。

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
