[English](README.md) | **中文**

# Agent DX

统一仓库包含上层 Agent 产品、Instance 执行平台，以及共享接入与转发组件 Gateway。新 Rust 控制面、Rust API Server、RRT HTTP 协作与统一进程部署已实现。

![当前组件架构](docs/architecture/current-architecture.svg)

| 目录 | 职责 |
|---|---|
| `agent/` | Agent CLI、SDK、Executor；当前仍依赖旧 Agent/FaaS 后端，接入 Sandbox SDK 是目标，尚未完成业务迁移 |
| `platform/control-plane/` | Rust Master、Node Manager、adxctl，Rust API Server |
| `platform/crates/` | Instance 模型、协议、发现、Filter/Score 与可观测公共库 |
| `platform/runtime/rrt/` | 实例内命令、文件、终端与恢复协作 |
| `platform/sdk/sandbox/` | 公开 Python SDK：分发名 `adx-sandbox`，导入名 `adx_sandbox` |
| `gateway/` | Edge 路由/认证/转发，Node Proxy 本机绑定与数据转发 |
| `build/` / `.buildkite/` | 代码生成、构建、统一发布包、进程与 K8s 验收 |

Master 内 Global 轮转选择 Shard，Shard 执行实际调度；Node Manager 做本机准入并拥有每个 Instance 的串行生命周期。Redis 保存集群状态，SQLite 用于节点提交故障时的降级日志。Master 向 Edge 发布全量/增量路由，Node Proxy 复核本机绑定。Node Proxy 支持与 Node Manager 共进程或分进程。

## 使用与开发

- [单机安装、证书、CLI 与 SDK 示例](docs/deployment/standalone.md)
- [配置示例](build/config/examples/README.md) · [进程托管与停止清理](docs/testing/process-deployment.md)
- [Sandbox API 支持范围](platform/control-plane/api-server/docs/sandbox-lifecycle-api.md) · [Python SDK](platform/sdk/sandbox/python/README.md)
- [构建与测试入口](docs/testing/control-plane-ci.md) · [Buildkite K8s 流水线](.buildkite/README.md)
- [当前实现](docs/testing/control-plane-implementation.md) · [目录与职责](docs/architecture/repository-layout.md) · [来源版本](docs/migration/sources.json)
- [Metrics](docs/testing/instance-resource-metrics.md) · [日志采集](docs/testing/log-collection.md) · [Trace](docs/testing/distributed-traces.md) · [日志滚动压缩](docs/testing/log-rotation.md)

根目录 `make help` 查看构建入口；Rust 使用 Cargo workspace，Python 包独立构建。`python3 build/ci/run.py <suite>` 运行组件检查，`make package` 生成四个 Python 包；统一进程发布包由 `build/release/package.py` 汇总。运行环境独立托管 sandboxd。

## 验收状态

[Buildkite #30](docs/testing/2026-09-18-runtime-environment-k8s.md) 使用 OCI 运行环境通过八组基础 K8s 用例及清理，包含本地优先创建、资源指标、日志和 Trace；两个 Pod 在同一宿主，不能作为跨宿主故障隔离证据。独立进程部署继续支持本地 EROFS。暂停、快照、S3 和跨节点恢复已有本地 Firecracker 验收，正式 K8s FC 暂缓。

本期仍需完成 FC 双克隆网络问题、GPU/NPU 实卡验收和真实服务长稳。模板预热、证书热重载及统一实时 Trace 队列丢弃指标已后置。详见 [阶段路线图](docs/testing/control-plane-roadmap.md) 和 [独立事项清单](docs/testing/control-plane-remaining.json)。
