# 组件命名与 Environment 抽象

当前发布包默认共进程部署：`adx-apiserver` 内嵌 Ingress，`adxlet` 内嵌 Relay；同时提供 `adx-ingress` 和 `adx-relay`，供显式分进程部署。调试用 `adx-data-plane-forward` 不随包发布。

本文对应 `community/refactor` 基础上的命名调整。组件代码、启动命令、部署角色、内部 RPC、存储字段和测试使用下表中的名称。

## 进程与目录

| 组件 | 可执行文件 / Cargo package | 源码目录 | 部署角色 |
|---|---|---|---|
| Coordinator | `adx-coordinator` | `platform/coordinator/` | `coordinator` |
| 节点管理 | `adxlet` | `platform/adxlet/` | `adxlet` |
| API Server | `adx-apiserver` | `gateway/apiserver/` | `apiserver` |
| Ingress | `adx-ingress` | `gateway/src/ingress/` | `ingress` |
| Relay | `adx-relay` | `gateway/src/node/` | `relay` |
| Execd | `adx-execd` | `platform/runtime/execd/` | 在 Runtime 内启动 |

Coordinator 保存集群状态，内嵌 Global 轮转与 ShardScheduler。adxlet 管理节点准入、Environment 生命周期、运行时适配和本机对账。Ingress 负责公开接入与跨节点转发；Relay 校验本机绑定并转发到 Runtime。Execd 在 Runtime 内提供命令、文件、终端和 checkpoint 协作。

默认部署启动 `adx-apiserver`（内嵌 Ingress）和 `adxlet`（内嵌 Relay）。拆分时设置 `ingress_mode: standalone` 与 `proxy_mode: standalone`，由独立 `adx-ingress` 和 `adx-relay` 进程承载相应能力。合进程仍允许 API 与数据面使用各自的监听端口。进程装配不改变模块职责和归属检查。

`sandboxd` 仍由部署环境独立托管，名称与其外部协议不变。`adxctl` 仍是部署 CLI，`adxadmin` 仍是管理员客户端。

## 平台 Environment 与 Runtime

`adx_core::EnvironmentSpec` 描述稳定逻辑对象的租户、资源、放置约束和执行规格。`EnvironmentRecord` 保存生命周期、归属及恢复记录，`EnvironmentState` 表达平台生命周期。它们取代原来的 Capsule 类型。

一个 Environment 在重启、暂停恢复和跨节点接管期间保持 ID。`RuntimeIdentity { environment_id, runtime_id, ownership_generation }` 描述该对象在某个节点的一次实际执行。替换 Runtime 不等于创建新的 Environment；归属 generation 用来拒绝旧执行的状态提交与流量。

内部协议统一使用 Environment，存储字段使用 `environment_id`；Redis 控制账本条目使用 `environment:`，发现记录使用 `coordinator:v1` 后缀。本次配置和存储按新格式初始化。部署使用独立的 Redis namespace 和节点数据目录，不直接复用旧格式数据库；初始化不会自动删除旧运行实例，切换前由部署者完成其停机清理。

## RuntimeProfile 是部署配置

`adx_core::runtime_profile::RuntimeProfile` 保存默认 rootfs、bootstrap、Execd 入口和环境变量。统一部署配置使用 `runtime_profile`，内部执行规格使用 `EnvironmentSpec.runtime_profile`。它描述如何准备 Runtime，不代表一个已创建的 Environment。

运行环境的本地 EROFS 与 OCI 来源、用户 rootfs 覆盖规则见 [内置运行环境](../deployment/runtime-environment.md)。

## Agent 与公开 Sandbox 边界

Agent 层已有的 `adx_agent_core::Environment` 表示产品执行上下文，绑定稳定的逻辑 Sandbox；由 Activator 管理其 Template、元数据与激活状态。平台 `adx_core::EnvironmentRecord` 表示调度和执行对象。两层通过 Sandbox 能力接口连接，分别维护自己的状态机和存储。

公开 Python 包仍为 `adx-sandbox` / `adx_sandbox`，用户仍使用 `Sandbox`。公开 HTTP 路径 `/api/instances`、字段 `instanceId` 和 SDK 的 `instance_id` 保持现有接口定义，由 API Server 在边界映射到平台 `environment_id`。内部组件重命名不把 Agent 产品接口并入平台 API。

## 配置与构建

独立主机使用各自的 `deployment.yaml`。内置 profile 为 `standalone`、`standalone-external-redis`、`coordinator`、`node`、`ingress-api`。`adxctl` 将它们展开为上表中的组件配置，详见 [部署入口](../deployment/standalone.md)。

Execd 仍独立出包；发布清单、构建步骤、进程启动参数、日志和测试驱动使用 `execd` 名称。历史验收报告保留当时的命令和产物名称，不能作为本次重命名的端到端验收证据。
