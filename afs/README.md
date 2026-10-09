# Agent FS（AFS）

`afs/` 是从 DMS/AFS 仓库迁入的可选文件系统源码快照，包含 OwnerFs 和 DistributedFs 两条路径。它只在显式开启 `ADX_WITH_AFS=1` 时参与 Agent DX 的文件系统构建、测试和带组件发布包；默认 Agent DX 构建、测试、发布包和部署不包含这些运行二进制或 FUSE 专属系统依赖。

## 组件范围

| 目录 | 内容 |
| --- | --- |
| `src/` | `afs-meta`、`afs-node`、OwnerFs、DFS、Meta 后端、FUSE 适配和运行时入口 |
| `client/` | DFS 客户端 crate |
| `common/` | 错误、日志、指标、协议、追踪和传输等共享 crate |
| `examples/` | crate 级示例配置；发布包示例另见 `build/config/examples/afs/` |
| `tests/` | Cargo 绑定的单元、契约和小型集成测试 |

迁入内容不包含源仓过程资产、历史证据、checkpoint、旧 release 包、VM 数据、`.codex`、`.omx`、GitHub Actions 或 `third_party/fuser`。FUSE 依赖使用根工作区声明的官方固定 `fuser =0.18.0`。

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

AFS 相关文件变化会触发专用 CI gate，并在 gate 内用 `ADX_WITH_AFS=1` 检查文件系统源码。该 gate 不会把 `afs-meta`、`afs-node` 或 AFS 示例配置注入默认发布包。

## 当前能力边界

- OwnerFs workspace bind mount 默认 OFF；当前实际试用场景需要显式配置 ON。
- OwnerFs 远端 FUSE 访问和 DFS 一写多读代码路径保留，目标仓二进制需要按受影响范围补 Linux 运行证据。
- 中心 Meta `local-file` 是当前可重启恢复基线；`memory` 只适合一次性演示。
- etcd、Redis、多 Meta、高可用、完整 POSIX、复杂可靠性和大规模长时间测试后置。
- 跨节点 `fcntl/flock`、阻塞锁取消、bind/native 与远端 FUSE 锁域协同后置，不能把本机 ext4 锁或单挂载内核回退宣传为分布式锁。

必要正确性边界仍需保留：读写新鲜度、close-to-open、权限和 suid/sgid 清除、错误传播、持久化屏障、direct-I/O mmap 能力协商、正常卸载和受管引用排空。

## 文档入口

- 迁移计划、支持范围和后续验收：[docs/development/afs-plan.md](../docs/development/afs-plan.md)
- 架构边界：[docs/architecture/afs.md](../docs/architecture/afs.md)
- 部署与配置：[docs/deployment/afs.md](../docs/deployment/afs.md)
- 测试与验收：[docs/testing/afs.md](../docs/testing/afs.md)
- 快照来源和路径映射：[docs/migration/2026-10-09-afs-snapshot.md](../docs/migration/2026-10-09-afs-snapshot.md)
- 来源索引和校验身份：[docs/migration/sources.json](../docs/migration/sources.json)
- 验收工具入口：[build/e2e/afs/acceptance/README.md](../build/e2e/afs/acceptance/README.md)
- 发布配置示例：[build/config/examples/afs/](../build/config/examples/afs/)

目标仓不继承 DMS/AFS 历史运行验收结论。所有 Agent DX 候选版本的功能、性能和交付状态必须以目标仓源码、目标仓二进制和对应 Linux 证据登记。
