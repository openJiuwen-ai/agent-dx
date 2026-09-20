[English](README.md) | **中文**

# Agent DX

Agent DX 是面向 Agent 与隔离 Instance 的执行平台。仓库统一提供 Sandbox API 与 SDK、分布式调度、节点本地生命周期管理、共享流量入口、运行时操作、Checkpoint 恢复和部署工具。

![Agent DX 架构](docs/architecture/images/agent-dx.svg)

## 架构

Agent 应用通过公开 Sandbox SDK 创建和操作 Instance。Gateway 承接外部流量，并分离控制请求与数据请求。API Server 认证调用方并提供 Sandbox HTTP API。Master 管理集群状态、调度、节点健康、路由发布、凭证和快照元数据。Node Manager 完成本机最终准入，并串行管理每个 Instance 的生命周期。Node Proxy 将数据请求转发到目标 Instance 内的 RRT。

Redis 是集群状态和服务发现的权威后端。集群状态暂时无法提交时，Node Manager 使用本地 SQLite 日志记录待同步结果。sandboxd 由部署环境管理，提供执行后端。Node Manager 默认内嵌 Node Proxy，也支持显式分进程部署。

| 目录 | 职责 |
|---|---|
| `agent/` | Agent API、会话、任务分发和执行编排 |
| `gateway/` | Edge 接入、Node Proxy、路由和转发 |
| `platform/control-plane/api-server/` | Sandbox HTTP API、认证、归属缓存和 Instance RPC 客户端 |
| `platform/control-plane/master/` | 集群状态、调度 Shard、Redis 持久化、路由、凭证和快照 |
| `platform/control-plane/node-manager/` | 本机准入、Instance 生命周期、sandboxd、Checkpoint 和降级日志 |
| `platform/runtime/rrt/` | Instance 内命令、文件、终端、活动统计和恢复操作 |
| `platform/sdk/sandbox/python/` | 公开 Python Sandbox SDK |
| `platform/api/proto/` | Instance、节点、路由、凭证和快照内部协议 |
| `platform/deployment/` | `adxctl`、配置生成、进程托管和停机清理 |
| `build/` 与 `.buildkite/` | 构建、打包、发布和端到端验证工具 |

## 能力

- Instance 创建、查询、删除、暂停、恢复、快照和基于快照的克隆。
- 中心调度与本地优先创建，支持 CPU、内存、磁盘、GPU/NPU 整卡、标签、亲和约束和偏好评分。
- API Key 管理员与租户认证；内部服务支持可配置 mTLS。
- Master 向 Edge 发布带版本的路由，Node Manager 与 Node Proxy 同步本机绑定。
- 节点对账、实例重启策略、空闲删除、Checkpoint 恢复、本地与 S3 兼容快照存储，以及引用感知的制品清理。
- Prometheus 指标、OpenTelemetry Trace、结构化日志、日志滚动、gzip 压缩和外部 Collector 对接。
- 支持托管 Redis 或外置 Redis 的进程部署，以及 Kubernetes 端到端部署方案。

## 快速开始

在 Linux 主机上安装 ADX 发布包。部署环境需要准备 sandboxd、证书、初始管理员 API Key 和 Instance 网络。默认 standalone profile 在一台主机启动 Redis、Master、内嵌 Node Proxy 的 Node Manager、API Server 和 Edge。

```sh
sudo install -d -m 0700 /etc/adx /etc/adx/tls /etc/adx/secrets /var/lib/adx /run/adx
sudo /opt/adx/bin/adxctl config init

# 编辑 /etc/adx/deployment.yaml，然后校验并启动。
sudo /opt/adx/bin/adxctl validate
sudo /opt/adx/bin/adxctl render --output /run/adx/config-review
sudo /opt/adx/bin/adxctl run
```

在另一个终端执行：

```sh
sudo /opt/adx/bin/adxctl status
sudo /opt/adx/bin/adxctl stop
```

外置 Redis 和分主机部署分别使用 `standalone-external-redis`、`master`、`node` 或 `edge-api` profile。每台主机维护一份部署 YAML，各主机通过相同的 Redis URL 和 namespace 加入同一集群。

证书、Redis、sandboxd、网络、SDK 和各角色配置参见[单机部署](docs/deployment/standalone.md)、[`adxctl` 指南](docs/deployment/adxctl.md)和[运行环境](docs/deployment/runtime-environment.md)。

## 构建与测试

根目录 Cargo workspace 包含平台和 Agent 组件，Python 包独立构建。构建产物写入 `out/` 或配置的外部缓存。

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

使用 `python3 build/ci/run.py <suite>` 运行组件与集成测试。端到端门禁使用安装后的发布制品、公开 Sandbox SDK、Redis、Gateway、控制面、sandboxd 和 RRT。环境要求和门禁定义参见[控制面 CI](docs/testing/control-plane-ci.md)与 [Kubernetes E2E](build/e2e/kubernetes/README.md)。

Sandbox SDK 分发名为 `adx-sandbox`，Python 导入名为 `adx_sandbox`，命令行为 `adx-sandbox`。ADX 环境变量使用 `ADX_` 前缀，内部品牌 HTTP Header 使用 `X-ADX-`。

## 文档

- [架构与目录规划](docs/architecture/repository-layout.md)
- [Agent 使用](agent/README.md)
- [Sandbox API](platform/control-plane/api-server/docs/sandbox-lifecycle-api.md)
- [Sandbox Python SDK](platform/sdk/sandbox/python/README.md)
- [部署配置示例](build/config/examples/README.md)
- [节点生命周期与资源采集](docs/testing/node-lifecycle.md)
- [Checkpoint 与快照存储](docs/testing/snapshot-storage.md)
- [调度](docs/testing/scheduling-performance.md)
- [路由发布](docs/testing/route-publication.md)
- [指标](docs/testing/instance-resource-metrics.md)
- [日志](docs/testing/log-collection.md)
- [分布式 Trace](docs/testing/distributed-traces.md)
- [Rust 编码规范](docs/development/rust-coding-guidelines.md)
