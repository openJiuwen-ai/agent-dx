[English](README.md) | **中文**

# Agent DX

Agent DX 是面向 Agent 与隔离 Instance 的执行平台。仓库统一提供 Sandbox API 与 SDK、分布式调度、节点本地生命周期管理、共享流量入口、运行时操作、Checkpoint 恢复和部署工具。

![Agent DX 架构](docs/architecture/images/agent-dx.svg)

## 架构

Agent 应用通过公开 Sandbox SDK 创建和操作 Instance。Gateway 承接外部流量，并分离控制请求与数据请求。API Server 认证调用方并提供 Sandbox HTTP API。Master 管理集群状态、调度、节点健康、路由发布、凭证和快照元数据。Node Manager 完成本机最终准入，并串行管理每个 Instance 的生命周期。Node Proxy 将数据请求转发到目标 Instance 内的 RRT。

Redis 是集群状态和服务发现的权威后端。集群状态暂时无法提交时，Node Manager 使用本地 SQLite 日志记录待同步结果。sandboxd 由部署环境管理，提供执行后端。Node Manager 默认内嵌 Node Proxy，也支持显式分进程部署。

## 核心抽象

ADX 内部统一使用 **Instance** 表达受管执行单元；**Sandbox** 只作为对外 API 和 SDK 的产品接口。Agent Distributed Executor 位于平台之上，通过 Sandbox SDK 使用 Instance 能力，不参与平台内部调度和生命周期状态机。

| 抽象 | 所属层 | 含义与边界 |
|---|---|---|
| `Agent` / `Session` | Agent 层 | 面向 Agent 的任务、会话、亲和和执行编排；只通过公开 Sandbox SDK 使用平台 |
| `Sandbox` | 公开接口层 | 用户持有的 API/SDK 句柄；一次创建映射到一个 Instance，不作为内部调度对象 |
| `Instance` | 管控面 | 稳定的内部身份，包含租户、规格和期望／实际状态；创建、暂停、恢复和删除都围绕它收敛 |
| `Assignment` | 调度层 | Instance 的权威归属，记录节点、设备和 `generation`；新一代归属会隔离迟到的旧执行 |
| `Shard` | Master 调度层 | Master 内的调度分区。Global 只轮转选择 Shard，Shard 负责排队、Filter／Score 和节点选择 |
| `Node` | 节点层 | 一次 Node Manager 注册会话及其容量、设备和健康状态；Node Manager 执行最终本机准入 |
| `Runtime Environment` | 执行层 | RRT、启动命令和本地 EROFS／OCI 运行环境进入 Instance 的方式；sandboxd 是当前执行后端 |
| `Route` / `Binding` | 数据面 | Master 发布版本化 Instance 归属，Edge 缓存路由，Node Proxy 在转发前复核本机绑定 |
| `Restore Point` / `Snapshot` | 恢复层 | 暂停恢复点保留原 Instance ID；可复用 Snapshot 用于创建新的 Instance，制品可落本地或对象存储 |
| `Request ID` / `Operation ID` | 可靠性契约 | 标识一次逻辑写操作，用于重试、去重、结果查询和故障对账；超时不自动等价于失败 |

调用分层固定为：Agent／业务应用 → Sandbox SDK／HTTP API → API Server → Master／ShardScheduler 或本地优先 Node Manager → sandboxd；运行时数据请求走 Edge → Node Proxy → Instance 内 RRT，不进入控制面生命周期队列。

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

## 部署

ADX 使用同一发布包按配置启动不同角色。部署环境需要准备 Linux 主机、独立运行的 sandboxd、组件证书、初始管理员 API Key、Instance 网络，以及与主机架构一致的 `/opt/adx` 发布包。`adxctl` 读取一份仅描述**当前主机**的 YAML，默认路径是 `/etc/adx/deployment.yaml`；它负责校验、渲染和托管进程，不用于创建 Instance。

### 单机部署：由 ADX 托管 Redis

默认 `standalone` profile 在一台主机启动 Redis、Master、内嵌 Node Proxy 的 Node Manager、API Server 和 Edge。sandboxd 仍由部署环境独立托管。

```sh
sudo install -d -m 0700 /etc/adx /etc/adx/tls /etc/adx/secrets /var/lib/adx /run/adx
sudo /opt/adx/bin/adxctl config init --profile standalone

# 编辑证书、初始密钥、sandboxd socket、Instance CIDR 和磁盘路径。
sudo /opt/adx/bin/adxctl validate
sudo /opt/adx/bin/adxctl render --output /run/adx/config-review
sudo /opt/adx/bin/adxctl run
```

`run` 在前台运行 supervisor，生产环境应由 systemd 或 Pod 托管。另一个终端可查询和停止整个本机部署：

```sh
sudo /opt/adx/bin/adxctl status
sudo /opt/adx/bin/adxctl stop
```

### 单机部署：使用外置 Redis

```sh
sudo /opt/adx/bin/adxctl config init --profile standalone-external-redis
sudoedit /etc/adx/deployment.yaml   # 设置实际 redis_url 和 namespace
sudo /opt/adx/bin/adxctl validate
sudo /opt/adx/bin/adxctl run
```

外置 Redis 不由 `adxctl status`、重启预算或 `stop` 管理。所有组件必须使用同一个持久化 Redis 和 namespace。

### 多主机按角色部署

控制节点、每个 Worker 和接入节点分别生成自己的配置，不能把多个节点写进同一份 YAML：

```sh
# 控制节点：Master；如需本机托管 Redis，在完整 YAML 中增加 redis 角色。
sudo /opt/adx/bin/adxctl config init --profile master

# 每个 Worker：Node Manager，默认同进程运行 Node Proxy。
sudo /opt/adx/bin/adxctl config init --profile node

# 接入节点：API Server + Edge。
sudo /opt/adx/bin/adxctl config init --profile edge-api
```

每台主机都要修改自己的 `/etc/adx/deployment.yaml`，使用相同的 `redis_url`、`namespace` 和匹配的 mTLS 信任关系；每个 Worker 配置唯一 `node_id` 以及其他节点可访问的控制面和 Proxy 地址。推荐按 Redis → Master → Workers → API Server／Edge 的顺序启动。Node Proxy 只有在显式设置 `proxy_mode: standalone` 时才作为独立进程部署。

YAML 字符串字段支持 `${VAR}` 和 `${VAR:-default}`。可用 `adxctl config dump` 检查环境变量和 profile 合并后的完整配置。Kubernetes 中仍运行这些进程，由 Pod 管理 `adxctl run`、证书、Redis 连接和 sandboxd 依赖。

完整字段、证书、Redis、网络和运行环境见 [`adxctl` 指南](docs/deployment/adxctl.md)、[单机部署](docs/deployment/standalone.md)、[配置示例](build/config/examples/README.md)和[运行环境](docs/deployment/runtime-environment.md)。

## 使用

发布包在 `sdk/` 中包含 `adx-sandbox` wheel。部署就绪后，安装 SDK，并将外部入口和 API Key 传给客户端：

```sh
python3 -m venv /opt/adx-client
/opt/adx-client/bin/python -m pip install /opt/adx/sdk/adx_sandbox-*.whl

export ADX_SERVER_ADDRESS=adx.example.com:8443
export ADX_GATEWAY_ADDRESS=adx.example.com:8443
export ADX_TOKEN="$(cat /secure/path/tenant-api-key)"
export ADX_TLS=1
export ADX_GATEWAY_TLS=1
export ADX_SANDBOX_IMAGE=python:3.12-slim
```

创建 Instance、通过 RRT 执行命令，并显式删除：

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

镜像必须能由当前 sandboxd 和 ADX Runtime Environment 启动。需要避免进程级环境变量时，应用可以显式构造 `ConnectionConfig`。暂停／恢复、可复用快照、资源约束和错误重试语义见 [Sandbox Python SDK](platform/sdk/sandbox/python/README.md)；直接调用 HTTP 的路径与请求格式见 [Sandbox API](platform/control-plane/api-server/docs/sandbox-lifecycle-api.md)。Agent 应用从 [Agent 使用指南](agent/README.md)进入，其底层仍使用同一 Sandbox SDK。

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
- [Sandbox OpenAPI](platform/api/openapi/sandbox.yaml)
- [数据面 OpenAPI](platform/api/openapi/data-plane.yaml)
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
