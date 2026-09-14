# Agent DX

统一维护 Agent 产品、Instance 执行平台和共享 Gateway。源码已按目录规划迁入，Sandbox SDK 使用 adx 命名；其余导入组件的二进制和协议行为保留。

- `agent/`：CLI、Agent SDK、Executor 和现有测试。
- `gateway/`：Edge、Node Proxy 与公共转发。
- `platform/runtime/rrt/`：RRT 与现有运行时适配。
- `platform/sdk/sandbox/python/`：Sandbox 客户端 SDK。
- `platform/control-plane/sandbox-api/`：Sandbox HTTP handler、快照接口及其最小包级依赖；旧依赖集中于 `internal/legacy/`。
- `platform/api/proto/legacy/`：本次导入仍使用的协议。

本阶段是源码与构建组织迁移。Agent 到 Sandbox SDK 的后端替换、新 Rust 管控面、Redis 路由与新 RRT 控制链按已定方案后续实施。Go API 当前提供可构建的 handler 模块，服务启动与后端接入尚未重构。

构建、测试和打包命令见 [README](README.md)。来源版本、迁移边界与验证结果见 [迁移记录](docs/migration/2026-09-14-import.md)。
