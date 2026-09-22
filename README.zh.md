<p align="center">
  <img src="assets/logo/agent-dx-lockup-primary.png" alt="Agent DX" width="560">
</p>

<h3 align="center">openJiuwen Agent Runtime 的分布式执行底座</h3>

<p align="center"><a href="README.md">English</a> | <strong>中文</strong></p>

Agent DX（**Agent Distributed eXecutor**）是 openJiuwen Agent Runtime 的一种分布式执行底座，Agent 层提供 Template 与 Environment 管理，由无状态 Activator 按需启动用户 Harness，支持 HTTP、WebSocket 和 SSH 访问。同时提供公开 Sandbox API 与 SDK、分布式调度、隔离执行、流量路由、运行时操作、Checkpoint 恢复与部署工具，并保持执行后端可替换。

<p align="center">
  <a href="#-快速开始">🚀 快速开始</a> ·
  <a href="docs/architecture/repository-layout.md">📐 架构</a> ·
  <a href="platform/sdk/sandbox/python/README.md">📦 Sandbox SDK</a> ·
  <a href="docs/deployment/adxctl.md">⚙️ 部署</a> ·
  <a href="docs/deployment/adxadmin.md">🔐 管理</a> ·
  <a href="docs/testing/control-plane-ci.md">✅ 测试门禁</a>
</p>

## 🎯 何时使用 Agent DX

| 目标 | 部署方式 | 所需条件 |
|---|---|---|
| 为单个 Agent 服务增加隔离命令、文件、终端和端口操作 | 单机部署 | 一台 Linux 主机、独立托管的 sandboxd、ADX 发布包和 Capsule 网络 |
| 让多个 Agent 服务共享跨机器执行资源 | 按角色多机部署 | 持久化 Redis、一个 Master 部署、一个或多个 Worker 部署和可访问的 Gateway |
| 以可重复验收方式运行生产形态集群 | Kubernetes | Linux Worker、持久化 Redis、组件证书、Worker 上的 sandboxd 和 ADX Kubernetes E2E 配置 |
| 暂停、恢复、克隆或接管长时间任务 | 单机或分布式 | 支持 Checkpoint 的运行时，以及本地或 S3 兼容 Checkpoint 存储 |
| 调度加速卡工作负载 | 分布式 | Worker 上报 GPU/NPU 清单，Sandbox 规格申请匹配设备 |

## 🧩 Agent DX 在技术栈中的位置

| 层 | 负责内容 | 与 Agent DX 的关系 |
|---|---|---|
| Agent 框架或业务应用 | Prompt、工具、会话、任务逻辑和业务策略 | 调用公开 Sandbox SDK 或 HTTP API，不进入平台调度或生命周期状态机 |
| Agent DX | 认证、放置、Capsule 状态、路由、恢复和可观测 | 在多个节点和执行后端之上提供统一分布式执行契约 |
| sandboxd | 运行时创建、隔离、网络和 Checkpoint 原语 | 作为每个 Worker 上的外部服务，由 Node Manager 的运行时驱动调用 |
| Runtime 内的 RRT | 命令、文件、终端、端口、活动统计和恢复协作 | Edge 与 Node Proxy 完成归属和 generation 校验后转发数据请求 |
| Redis 与对象存储 | 权威集群元数据和可选共享 Checkpoint 制品 | Redis 保存控制状态与服务发现；对象存储支持跨节点访问 Checkpoint |

## 🔄 工作流程

![Agent DX 架构](assets/architecture/agent-dx.svg)

| 步骤 | 发生的事情 | 所在模块 |
|---|---|---|
| 1 · 接入 | 调用方认证后，通过统一公开契约创建或操作 Sandbox。 | `gateway/api-server/`、`platform/sdk/sandbox/python/` |
| 2 · 放置 | API Server 使用本地优先准入或提交 Master。Master 在 Shard 间轮转，Shard 负责排队、过滤、评分和节点预留。 | `platform/master/`、`platform/crates/scheduling/` |
| 3 · 运行 | Node Manager 完成本机最终准入、串行管理 Capsule 生命周期，并调用 sandboxd 创建带 RRT 的 Runtime。 | `platform/node-manager/`、`third_party/sandboxd/`、`platform/runtime/rrt/` |
| 4 · 操作与恢复 | Edge 把数据请求路由到归属 Node Proxy。版本化归属隔离旧 Runtime；暂停、恢复、快照、重启策略和对账在故障后收敛状态。 | `gateway/`、`platform/node-manager/`、Redis、Checkpoint 存储 |

API Server 默认内嵌 Edge，Node Manager 默认内嵌 Node Proxy；显式拆分进程时仍复用同一套契约。

## 📦 安装

ADX 发布包面向 Linux，包含控制面与数据面二进制、RRT、Python Sandbox SDK 和可选的托管 Redis 二进制。sandboxd 由部署环境独立托管，版本固定在 [`third_party/sandboxd/source.json`](third_party/sandboxd/source.json)。

```sh
mkdir adx-release
tar -xzf adx-release.tar.gz -C adx-release
sudo ./adx-release/install.sh
```

安装器校验清单、文件摘要和主机架构，将版本写入 `/opt/adx/releases/<commit>` 并原子切换 `/opt/adx/current`。升级会保留 `/opt/adx/config`、`/opt/adx/data` 和 `/opt/adx/run`，同时通过 `/usr/local/bin` 提供 `adxctl` 命令。

## 🔧 快速开始

默认 `standalone` profile 在一台主机启动托管 Redis、Master、内嵌 Node Proxy 的 Node Manager，以及内嵌 Edge 的 API Server。开始前先准备 sandboxd、网络、证书和初始管理员密钥。

```sh
sudo adxctl config init --profile standalone
sudoedit /opt/adx/config/deployment.yaml
sudo adxctl validate
sudo adxctl run
```

在管理员工作站安装平台无关的 wheel，然后通过公开 HTTPS API 创建首个租户 Key：

```sh
pipx install ./adxadmin-0.1.0-py3-none-any.whl
export ADX_ENDPOINT=https://adx.example.com:8443
export ADX_CA_FILE=$HOME/.config/adx/public-ca.pem
export ADX_ADMIN_TOKEN_FILE=$HOME/.config/adx/admin.key
adxadmin key create --tenant example
```

查询、分页、吊销、JSON 输出和错误语义见 [`adxadmin` 指南](docs/deployment/adxadmin.md)。

在另一终端安装发布包中的 SDK，并连接公开 Gateway：

```sh
python3 -m venv /opt/adx-client
/opt/adx-client/bin/python -m pip install /opt/adx/current/sdk/adx_sandbox-*.whl

export ADX_SERVER_ADDRESS=adx.example.com:8443
export ADX_GATEWAY_ADDRESS=adx.example.com:8443
export ADX_TOKEN="$(cat /secure/path/tenant-api-key)"
export ADX_TLS=1
export ADX_GATEWAY_TLS=1
export ADX_SANDBOX_IMAGE=python:3.12-slim
```

创建 Sandbox，通过 RRT 执行命令，然后显式删除：

```python
import os
from adx_sandbox import Sandbox

sandbox = Sandbox(
    image=os.environ["ADX_SANDBOX_IMAGE"],
    cpu=1000,
    memory=2048,
    name="readme-demo",
)
try:
    result = sandbox.commands.run("printf 'hello from ADX\\n'")
    print(result.stdout)
finally:
    sandbox.kill()
```

镜像必须由当前 sandboxd 和 [ADX Runtime Environment](docs/deployment/runtime-environment.md) 支持。暂停恢复、可复用快照、放置约束、挂载、网络、数据面安全和重试语义见 [Sandbox SDK 指南](platform/sdk/sandbox/python/README.md)。

## 🏗️ 部署组合

| 拓扑 | `adxctl` profile | 本机进程 |
|---|---|---|
| 单机托管 Redis | `standalone` | Redis、Master、Node Manager + Node Proxy、API Server + Edge |
| 单机外置 Redis | `standalone-external-redis` | Master、Node Manager + Node Proxy、API Server + Edge |
| 控制节点 | `master` | Master；Redis 可独立部署，也可加入完整 YAML |
| Worker 节点 | `node` | 默认 Node Manager + Node Proxy |
| 接入节点 | `edge-api` | 默认 API Server + Edge |

每台主机使用独立的 `/opt/adx/config/deployment.yaml`。集群成员共享 Redis URL、namespace 和 mTLS 信任；每个 Worker 使用唯一 `node_id` 和可访问的控制、代理地址。启动顺序为 Redis → Master → Worker → API Server。仅在确实需要分进程时设置 `proxy_mode: standalone` 或 `edge_mode: standalone`。

YAML 字符串支持 `${VAR}` 与 `${VAR:-default}`。使用 `adxctl config dump` 查看合并后的 profile 和主机覆盖。完整字段与证书说明见 [`adxctl` 参考](docs/deployment/adxctl.md)、[单机部署指南](docs/deployment/standalone.md)和[配置示例](build/config/examples/README.md)。

## 📐 核心抽象

ADX 将稳定逻辑身份与可替换物理执行分开：

| 抽象 | 含义与边界 |
|---|---|
| `Environment` | Agent 执行上下文，与稳定逻辑 Sandbox 1:1 绑定，由无状态 Activator 管理 |
| `Sandbox` | 面向应用的公开 API 与 SDK 句柄 |
| `Capsule` | 稳定内部身份，包含租户、规格、生命周期和期望／实际状态 |
| `Runtime` | Capsule 在一个节点上的一次 sandboxd 执行；重启或恢复可替换 Runtime |
| `Assignment` | 带 `generation` 的权威节点和设备归属，用于隔离迟到的旧执行 |
| `Route` / `Binding` | 发布到 Edge 的版本化归属，以及 Node Proxy 转发前的本机复核 |
| `Restore Point` / `Snapshot` | 恢复点保留 Capsule ID；可复用 Snapshot 创建新的 Capsule |
| `Request ID` / `Operation ID` | 用于重试、去重、结果查询和对账的一次逻辑写身份 |

固定控制链路为 Agent／应用 → Sandbox SDK／HTTP API → API Server → Master／Shard 调度器或本地优先 Node Manager → sandboxd。运行时数据请求走 Edge → Node Proxy → RRT，不进入生命周期队列。`instanceId`、`instance_id` 和 `/api/instances` 等兼容字段只在公开边界转换；内部 Rust 类型、RPC、持久化键、指标和运行身份统一使用 Capsule 命名。

## ✨ 能力

- Capsule 创建、查询、删除、暂停、恢复、可复用快照和基于快照的克隆。
- 中心调度与本地优先创建，支持 CPU、内存、磁盘、GPU/NPU 设备、标签、亲和约束和偏好评分。
- API Key 管理员／租户身份和可配置内部 mTLS。
- 版本化路由发布与同步本机绑定检查。
- 节点对账、重启策略、空闲删除、Checkpoint 恢复、本地／S3 兼容存储和引用感知制品清理。
- Prometheus Metrics、OpenTelemetry Trace、结构化日志、滚动、gzip 压缩和外部 Collector 对接。
- 托管或外置 Redis 的进程部署，以及 Kubernetes 端到端部署配置。

## 🛠️ 开发与测试

根 Cargo workspace 包含平台和 Agent 组件；Python 包独立构建。构建产物放在 `out/` 或显式配置的外部缓存。

```sh
make help
make rust-check
cargo test --locked --workspace --all-features -j 2
make agent-test
PYTHONPATH=platform/sdk/sandbox/python \
  python -m pytest -q -c platform/sdk/sandbox/pytest.ini \
  platform/sdk/sandbox/python/tests
make package PYTHON=/path/to/venv/bin/python
```

组件与集成测试使用 `python3 build/ci/run.py <suite>`。端到端门禁使用已安装发布包、公开 Sandbox SDK、Redis、Gateway、控制面、sandboxd 和 RRT。环境要求与门禁定义见[控制面 CI](docs/testing/control-plane-ci.md)和 [Kubernetes E2E 指南](build/e2e/kubernetes/README.md)。

Buildkite 使用相互独立的 `agent-dx`、`agent-dx-python-sdk` 和
`agent-dx-full-test` 三条流水线。Full 流水线只消费显式指定的基础包与 SDK build
UUID，不重新构建任一候选；契约见 [Buildkite 流水线说明](.buildkite/README.md)。

SDK 发布名为 `adx-sandbox`，Python 导入名为 `adx_sandbox`，CLI 为 `adx-sandbox`。ADX 环境变量使用 `ADX_` 前缀，内部品牌 HTTP 头使用 `X-ADX-`。

## 📚 深入了解

- [架构与仓库目录](docs/architecture/repository-layout.md)
- [Agent 使用](agent/README.md)
- [Sandbox API](gateway/api-server/docs/sandbox-lifecycle-api.md)与 [OpenAPI](platform/api/openapi/sandbox.yaml)
- [数据面 OpenAPI](platform/api/openapi/data-plane.yaml)
- [Sandbox Python SDK](platform/sdk/sandbox/python/README.md)
- [部署配置](docs/deployment/adxctl.md)与[配置示例](build/config/examples/README.md)
- [远程集群管理](docs/deployment/adxadmin.md)与 [API Key 管理](docs/testing/api-key-management.md)
- [调度](docs/testing/scheduling-performance.md)、[节点生命周期](docs/testing/node-lifecycle.md)与[路由发布](docs/testing/route-publication.md)
- [Checkpoint 与快照存储](docs/testing/snapshot-storage.md)
- [Metrics](docs/testing/capsule-resource-metrics.md)、[日志](docs/testing/log-collection.md)与[分布式 Trace](docs/testing/distributed-traces.md)
- [Rust 编程规范](docs/development/rust-coding-guidelines.md)
- [发布流水线与软件包布局](docs/development/release-pipelines-and-packaging.md)
- [品牌与架构资源](assets/README.md)

## 许可证

Agent DX 使用 [Apache License 2.0](LICENSE)。
