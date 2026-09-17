# 管控面当前实现

核对日期：2026-09-17。内部以 Instance 为核心，公开接口保持 Sandbox HTTP/SDK 形态。本文描述当前源码；当次测试数据保留在带日期的验收报告中。

## 模块与职责

| 模块 | 当前实现 | 边界 |
|---|---|---|
| `platform/crates/core` | Instance 状态、资源/设备账本、归属代次、checkpoint 与调度类型 | 纯模型，不访问 Redis、sandboxd |
| `platform/crates/protocol` / `platform/api/proto` | Instance gRPC、身份检查、类型转换；Node Proxy 绑定/活动协议 | 协议按职责拆分；RRT 使用 HTTP |
| `platform/crates/scheduling` | 静态 Filter/Score、Pack/Spread、整卡、节点/实例亲和与反亲和、增量快照与索引 | 拓扑规则存在于内部类型和库；不是公开 HTTP 已验收能力 |
| `platform/control-plane/master` | Global 轮转、同进程 Shard 队列与选点、自动分域、Redis、认证、路由/快照目录、节点失效与跨节点恢复协调 | 单 Master；无 scaler、抢占或租户配额；未分配队列仅在内存 |
| `platform/control-plane/node-manager` | 每 Instance 串行任务、准入、暂停/恢复/删除、空闲删除、可配置重启、资源采集、对账 | 普通生命周期由节点决定；SQLite 是提交故障降级日志 |
| 同上 `sandboxd.rs` / `runtime_control.rs` | RuntimeBackend、Start/Stats/checkpoint/restore、RRT HTTP 准备与身份校验 | sandboxd 自行生成物理 ID；平台 Instance ID 与后端 ID 分开 |
| 同上 `checkpoint.rs` / `checkpoint/` | 本地/S3 存储抽象、下载缓存、引用保护、过期和孤儿制品回收 | local-only 制品只在源节点可用；模板预热后置 |
| 同上 `routes.rs` / `proxy.rs` | 本机绑定、全量同步、代理重启重放、活动接收；嵌入 NodeProxyService | `proxy_mode=embedded/standalone`；两种模式都使用同一 UDS 控制契约 |
| `platform/control-plane/api-server` | Rust HTTPS 服务、API Key 缓存、归属缓存直达节点、生命周期和快照适配、管理员密钥接口 | [支持范围](api-server.md)；Agent 路由需要另配业务服务 |
| `gateway` | Edge Redis 发现与 gRPC 路由订阅、认证、转发；Node Proxy 绑定复核与数据转发 | 数据请求不进入生命周期队列；Edge 路由仅内存缓存 |
| `platform/runtime/rrt` | 命令、文件、Shell/PTY、命令观察、HTTP 运行时协作 | 恢复时更新身份/认证、退役旧连接并重建监听 |
| `platform/control-plane/control-cli` | Rust `adxctl`、统一 JSON 配置、supervisor、清理后停止 | 同一发布包含控制面/数据面；sandboxd 由部署环境托管 |
| `platform/crates/observability` 与各组件埋点 | Trace 上下文/采样/OTLP、Metrics、结构化日志；supervisor 滚动压缩 | Collector 是外部采集组件；实时队列丢弃指标后置 |

## 生命周期与提交

创建：Master 先持久化 Assignment，再由 Node Manager 本机准入、启动 sandboxd、确认 RRT 就绪和本机绑定，最后提交 Running。普通已有实例操作从 API Server 归属缓存直达节点，无需预先向 Master 登记每次操作意图。

暂停：节点完成 checkpoint、确认旧执行删除、保存制品，再经 Master 提交 Paused 和恢复点。对象存储成功要求上传完成。恢复：公开 resume 直达所属节点重新准入和恢复，完成 RRT/绑定后提交 Running；共享 checkpoint 的故障跨节点恢复由 Master 协调新归属。删除先退役本机绑定、确认后端删除，再释放资源和提交结果。

`StateSink` 返回 Published 表示 Master 已提交 Redis；Journaled 表示结果只进入 SQLite 降级日志。后者不发布集群路由，API Server 不返回集群生命周期成功。Master 恢复后去重补写；Node Manager 重启而 Master 不可用时只观察，等待权威对账。详见 [节点生命周期](node-lifecycle.md)、[暂停恢复](instance-checkpoint.md) 和 [存储](snapshot-storage.md)。

节点心跳超时后，Master 持久化失效、撤销路由并协调有效共享 checkpoint 的恢复。local-only、缺失或过期 checkpoint 直接失败。原节点返回先清理失效执行，再开放准入。心跳失效是控制面判定，不是物理进程已停止的证据；当前没有跨宿主网络分区的强隔离验收，见 [失效契约](node-failure-takeover.md)。

## 公开能力与 Agent 边界

新平台已接通创建、查询、删除、同节点暂停/恢复、可复用快照目录和克隆、空闲删除、重启策略、HTTP/SDK 放置约束及命令/文件数据链路。客户端存在的方法不自动代表服务器支持：`reload()`、创建 `failover=true`、网络策略、挂载、入口继承、独立资源上限、公开用户端口及每实例数据面安全策略尚未接入新后端。

`agent/` 已迁入，目标是通过 Sandbox SDK 使用平台；当前 CLI/SDK/Executor 仍有旧 FaaS/外部运行时依赖。九条 `/api/agent` 路由只是兼容转发入口，未配置 Agent 服务时不可用。当前基础平台 E2E 不证明 Agent 业务闭环。

## 验收与剩余范围

- 最新正式基础 K8s：[Buildkite #21](2026-09-17-observability-k8s.md)，测试提交 `b3145d6d43d04c06dbc85a91928d441814111d87`，七组及清理通过，包含资源 Metrics、日志滚动/采集和跨组件 Trace。两个 Pod 位于同一宿主。
- 本地 FC：暂停/恢复、S3、SQLite 降级、快照与跨节点恢复分别有真实验收，见 [路线图](control-plane-roadmap.md)。后续双克隆运行暴露网络问题，不能只引用较早成功批次宣称已解决。
- 本期未完成：FC 双克隆网络、GPU/NPU 实卡验收、真实服务混合负载与长稳。
- 后置：x86 双克隆对照、K8s FC、模板预热、证书热重载、统一实时 Trace 队列丢弃指标。

[未完成事项](control-plane-remaining.json) 是进度页清单；[CI 入口](control-plane-ci.md) 区分组件、真实依赖协作与完整 E2E。各报告中的 `out/`、`/tmp/` 是当时的本地产物位置，干净克隆不包含这些文件。历史性能对照的版本、运行条件和测量范围见 [基线报告](scheduling-baseline-comparison.md)，不沿用其数字作为当前服务吞吐承诺。

本次逐文件核对范围、修订及检查结果见 [2026-09-17 文档核对](2026-09-17-documentation-audit.md)。

Rust API Server 与 Shard 改名的验证单独记录在 [重写状态](rust-api-server.md)，历史 E2E 结果不代表本次重写验收。
