# DFS and OwnerFs

本目录是从 DMS/AFS 仓库白名单迁入的源码快照，包含 OwnerFs 和 DistributedFs 两条路径。

- 遵守根 `AGENTS.md`、根 `rustfmt.toml`、根 workspace lint 和目标仓构建入口。
- 文件系统进程二进制必须继续通过 `ADX_WITH_DFS=1` 显式启用；默认 Agent DX 构建、发布包和部署不得引入 DFS runtime artifact 或 FUSE 专属系统依赖。
- 不导入源仓过程资产，例如 `.github/`、`.codex/`、`.omx/`、历史证据、checkpoint、大日志或旧 release 包。
- 不 vendor 或私有 patch 第三方 FUSE 源码；当前依赖是 `Cargo.toml` 声明的官方固定 `fuser` 版本。
- Rust 构建、Cargo metadata、格式、Clippy、单元测试、FUSE runtime 验证和性能结论都只在 Linux 上确认。macOS 只用于文档检查和 Python 静态工具核对，不能作为 Rust/文件系统验收通过证据。
- `tests/support` 程序是测试辅助。验收流程可以显式构建它们，但它们不是产品二进制，不得进入默认发布包。
- `dfs/common/*` 是 AFS 域内共享 crate，不是 Agent DX 根级产品基础设施；不要把旧 POSIX 或 Frontend 适配器接回 Platform 内部 RPC。
