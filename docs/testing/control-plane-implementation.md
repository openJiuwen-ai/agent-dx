# 管控面当前实现

核对日期：2026-09-21。内部以 Capsule 为核心，公开接口保持 Sandbox HTTP/SDK 形态。本文描述当前源码；当次测试数据保留在带日期的验收报告中。

## 模块与职责

| 模块 | 当前实现 | 边界 |
|---|---|---|
| `platform/crates/core` | Capsule 状态、资源/设备账本、归属代次、checkpoint 与调度类型 | 纯模型，不访问 Redis、sandboxd |
| `platform/crates/protocol` / `platform/api/proto` | Capsule gRPC、身份检查、类型转换；Node Proxy 绑定/活动协议 | 协议按职责拆分；RRT 使用 HTTP |
| `crates/error` | 稳定错误码、重试指令与操作结果语义 | HTTP/gRPC 适配留在各自边界，公共 crate 不绑定协议框架 |
| `crates/process` | 服务进程统一的类型化 `--config`、安全 JSON 配置读取、退出信号与 FD limit | 不承载协议或组件业务配置 |
| `crates/transport` | mTLS/HTTP TLS、请求上下文和剩余 deadline | 不承载路由、调度或生命周期规则 |
| `platform/crates/scheduling` | 静态 Filter/Score、Pack/Spread、整卡、节点/实例亲和与反亲和、增量快照与索引 | 拓扑规则存在于内部类型和库；不是公开 HTTP 已验收能力 |
| `platform/master` | Global 轮转、同进程 Shard 队列与选点、自动分片、Redis、认证、路由/快照目录、节点失效与跨节点恢复协调 | 单 Master；无 scaler、抢占或租户配额；未分配队列仅在内存 |
| `platform/node-manager` | 每 Capsule 串行任务、准入、暂停/恢复/删除、空闲删除、可配置重启、资源采集、对账 | 普通生命周期由节点决定；SQLite 是提交故障降级日志 |
| 同上 `sandboxd.rs` / `runtime_control.rs` | RuntimeDriver、Start/Stats/checkpoint/restore、RRT HTTP 准备与身份校验 | sandboxd 自行生成物理 ID；平台 Capsule ID 与后端 ID 分开 |
| 同上 `checkpoint.rs` / `checkpoint/` | 本地/S3 存储抽象、下载缓存、引用保护、过期和孤儿制品回收 | local-only 制品只在源节点可用；模板预热后置 |
| 同上 `routes.rs` / `proxy.rs` | 本机绑定、全量同步、代理重启重放、活动接收；默认嵌入 NodeProxyService | 省略 `proxy_mode` 即 `embedded`；显式 `standalone` 保留分进程；两种模式使用同一 UDS 控制契约 |
| `gateway/api-server` | Rust HTTPS 服务、API Key 缓存、版本化实例目录订阅、生命周期和快照适配、管理员密钥接口；默认托管 Edge 服务 | [支持范围](api-server.md)；Agent 路由需要另配业务服务 |
| `gateway` | 可复用 Edge 服务、Redis 发现与 gRPC 路由订阅、认证、转发；Node Proxy 绑定复核与数据转发 | 数据请求不进入生命周期队列；Edge 路由仅内存缓存；显式分进程复用同一实现 |
| `platform/runtime/rrt` | 命令、文件、Shell/PTY、命令观察、HTTP 运行时协作 | 恢复时更新身份/认证、退役旧连接并重建监听 |
| `platform/deployment` | Rust `adxctl`、统一 YAML 配置、supervisor、清理后停止 | API Server 默认内嵌 Edge，Node Manager 默认内嵌 Node Proxy；显式分进程仍使用同一发布包；sandboxd 由部署环境托管 |
| `crates/observability` 与各组件埋点 | Trace 上下文/采样/OTLP、Metrics、结构化日志；组件和 supervisor 的滚动压缩实现 | Collector 是外部采集组件；实时队列丢弃指标后置 |

## 生命周期与提交

创建：Master 先持久化 Assignment，再由 Node Manager 本机准入、启动 sandboxd、确认 RRT 就绪和本机绑定，最后提交 Running。Master 向 API Server 首次全量、后续增量发布保留的实例目录，包括用于幂等生命周期结果的终态记录；普通已有实例操作命中本地目录后直达节点，无需预先向 Master 登记每次操作意图。

可配置 `create_mode: "local_first"`：API Server 订阅可用节点并轮转入口，Node Manager 用同一 Admission 暂留标量资源和整卡，再由 Master 验证硬约束、CAS 确认归属并同步内存账本后启动。本地不满足时由入口节点保留同 ID 回退 ShardScheduler；API Server 不在连接失败或结果未知时二次发起中心创建。`createTimeoutSeconds` 约束完整创建，`scheduleTimeoutSeconds` 仅从请求进入 Master 中心队列后计时，本地准入不预切或消耗中心排队预算。Pack/Spread、软评分和队列公平性仅适用于中心路径；默认仍为 `central`。并发、暂留释放和结果未知契约见 [本地优先与原子归属](atomic-capsule-claim.md)。

暂停：节点完成 checkpoint、确认旧执行删除、保存制品，再经 Master 提交 Paused 和恢复点。对象存储成功要求上传完成。恢复：公开 resume 直达所属节点重新准入和恢复，完成 RRT/绑定后提交 Running；共享 checkpoint 的故障跨节点恢复由 Master 协调新归属。删除先退役本机绑定、确认后端删除，再释放资源和提交结果。

`StateSink` 返回 Published 表示 Master 已提交 Redis；Journaled 表示结果只进入 SQLite 降级日志。后者不发布集群路由，API Server 不返回集群生命周期成功。Master 恢复后去重补写；Node Manager 重启而 Master 不可用时只观察，等待权威对账。详见 [节点生命周期](node-lifecycle.md)、[暂停恢复](capsule-checkpoint.md) 和 [存储](snapshot-storage.md)。

节点心跳超时后，Master 持久化失效、撤销路由并协调有效共享 checkpoint 的恢复。local-only、缺失或过期 checkpoint 直接失败。原节点返回先清理失效执行，再开放准入。心跳失效是控制面判定，不是物理进程已停止的证据；当前没有跨宿主网络分区的强隔离验收，见 [失效契约](node-failure-takeover.md)。

## 公开能力与 Agent 边界

新平台已接通创建、查询、删除、同节点暂停/恢复、reload、可复用快照目录和克隆、空闲删除、重启策略、`failover=true`、HTTP/SDK 放置约束、S3 rootfs／mount、镜像入口继承、创建及运行期网络策略、独立执行 limit、extra_config、每实例数据面安全策略、鉴权端口转发、`upstream` reverse tunnel 及命令/文件数据链路。公开请求中的本机 rootfs 和 host mount 会被拒绝；节点本地 RRT 运行环境仍由部署配置拥有。旧 `/invoke` 兼容传输不属于新数据链路。

`agent/` 已实现 Rust Template/Environment 管理和无状态 Activator；Gateway Edge 装配 Agent API，inline create/get/kill 直接适配 Sandbox。Environment 首次访问按稳定身份激活，用户 Harness 通过 HTTP/WS/SSH 访问。具体部署及已验证范围见 [Agent 使用说明](../../agent/README.md)；平台基础 E2E 与 Agent 端到端验收分开记录。 API Server 原有 `agent_address` 兼容转发入口继续保留，与 Edge 内的 Agent API 分开。

## 验收与剩余范围

- 本地优先创建链路已通过真实 Redis/mTLS/HTTPS 与新制品本地双节点8组验收，见 [本地报告](2026-09-17-local-first-e2e.md)；提交 `363e44f` 的 [Buildkite #30](2026-09-18-runtime-environment-k8s.md) 已用正式发布包完成同一八组 K8s 验收。
- 最新正式基础 K8s 为 Buildkite #30：OCI default/runtime-only/custom 三种运行环境、资源 Metrics、日志滚动/采集和跨组件 Trace 均通过，清理无残留。两个 Pod 位于同一宿主。历史 [Buildkite #24](2026-09-17-rust-api-server-k8s.md) 保留为 Rust API Server 七组迁移记录。
- 本地 FC：暂停/恢复、S3、SQLite 降级、快照与跨节点恢复分别有真实验收，见 [路线图](control-plane-roadmap.md)。后续双克隆运行暴露网络问题，不能只引用较早成功批次宣称已解决。
- 本期未完成：FC 双克隆网络、GPU/NPU 实卡验收、真实服务混合负载与长稳。
- 后置：x86 双克隆对照、K8s FC、模板预热、证书热重载、统一实时 Trace 队列丢弃指标。

[未完成事项](control-plane-remaining.json) 是进度页清单；[CI 入口](control-plane-ci.md) 区分组件、真实依赖协作与完整 E2E。各报告中的 `out/`、`/tmp/` 是当时的本地产物位置，干净克隆不包含这些文件。历史性能对照的版本、运行条件和测量范围见 [基线报告](scheduling-baseline-comparison.md)，不沿用其数字作为当前服务吞吐承诺。

当前核对范围、修订及检查结果见 [2026-09-18 文档核对](2026-09-18-documentation-audit.md)；前一轮全仓基线清单保留在 [2026-09-17 核对](2026-09-17-documentation-audit.md)。

Rust API Server 与 Shard 改名已通过本地回归及独立K8s验收；升级与验证过程见 [迁移记录](rust-api-server.md)。
