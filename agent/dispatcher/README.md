# Dispatcher

Dispatcher 是独立 Rust 进程，负责受管 Session 实例选址及创建、删除协调。它调用统一 Gateway 的 Sandbox API，不直接访问 Master、Node Manager、平台存储或 RRT。inline Agent 操作不经过 Dispatcher。

## 启动

在根 workspace 执行 `cargo build --locked -p adx-dispatcher`，配置：

- `ADX_DISPATCHER_CONFIG`：JSON 配置路径，本地验证可参考 `examples/local.json`。
- `ADX_DISPATCHER_SERVICE_TOKEN`：Gateway 调用 Dispatcher 的服务凭证，至少 32 字节。
- `ADX_SANDBOX_SERVICE_TOKEN`：Dispatcher 调用 Gateway Sandbox 的服务凭证，至少 32 字节。

服务凭证与用户 API Key 分离。Gateway 完成用户认证后才能确定 tenant 并发起内部请求；服务 token 不得暴露给用户服务。部署时可通过环境变量注入，示例文件不保存密钥。

HTTP 监听要求显式配置 `allow_plaintext_transport: true`，用于本地验证或 TLS 终止代理后的私有链路。本期 Dispatcher 不内置 TLS 终止。Gateway HTTPS 校验证书，`gateway_ca_file` 可指定私有 CA，客户端禁止重定向。部署应保护私有 HTTP 链路，在入口终止 TLS。

进程以唯一 boot ID 注册到 `node_id` 下，成员 TTL 为 20 秒，每 5 秒续租。其他存活 boot 不能抢占注册；注册冲突会结束进程，Redis 失败会撤销 ready 状态。成员租约仅用于发现，不代表 Session 所有权。复用同一 node ID 的新进程需要等待旧注册到期或正常注销。正常停止发送 SIGTERM，让进程注销自己的 boot；强制退出后等待 TTL，不删除其他 boot 的注册来强占节点。

## 内部接口

POST 接口使用 `Authorization: Bearer <ADX_DISPATCHER_SERVICE_TOKEN>` 和 JSON，请求上限 64 KiB。传输保护期限为 `create_timeout_seconds + 10` 秒，为超时状态提交和回包留出余量。Scope 为 `{tenant, template, version, session_id}`。

| 路由 | 请求 | 响应 |
|---|---|---|
| `/internal/adx/v1/resolve` | `{scope, affinity_key, bypass_cache?}` | `{instance_id, sandbox_id, tenant, scope, session_generation}` |
| `/internal/adx/v1/release` | `{scope}` | 202，删除意图已记录，后端删除可能尚未完成 |
| `/internal/adx/v1/release-instance` | `{scope, instance_id}` | 202，仅删除该 Scope 下指定的受管实例 |
| `/health/live` | GET，无凭证 | 204 |
| `/health/ready` | GET，无凭证 | 注册有效时为 204，否则为 503 |

Resolve 要求 Session 已存在且活跃，不重放应用请求。Resolve 和创建共用 `create_timeout_seconds`（默认 60 秒）。新实例继承发起 Resolve 的绝对截止时间，包含请求已消耗的等待时间；`create_deadline_ms` 随 Creating 记录只保存一次，重试和进程替换不能重置。到期后 ADX 原子记录删除意图，Sandbox 确认 Deleted 后才移除 Session 成员。删除未知或失败时保留待恢复状态；超时请求不自动补池，清理完成后的后续请求可重新冷启动。部署节点须同步时钟。

创建 Session 只生成空池，不支持预留实例或池大小配置。并发冷启动通过原子空池检查复用首次预留。Release 在子实例确认删除且绑定清理完成后移除 Session，公开 ID 可用新 generation 重建。启动恢复读取当前状态，不使用持久操作队列。

错误格式为 `{kind,message}`：invalid=400，not_found=404，conflict=409，not_ready/unavailable/outcome_unknown=503，unsupported=501。`outcome_unknown` 要求调用方先查询原 Session/Instance 再重试。格式错误、超大请求及未授权请求等框架错误仅返回 HTTP 错误状态，不保证产品错误封装。

Sandbox 接口为 `POST /api/sandbox/v2/instances` 和 `GET/DELETE /api/sandbox/v2/instances/{id}?tenant=...`。创建也携带 tenant 查询参数和 `CreateSandbox` body。这是通用 Gateway Sandbox 边界，不另设 Dispatcher 回调接口。数据类型位于 `adx-agent-core::sandbox`。GET 不存在时返回 404；DELETE 只有后端确认且满足迟到创建隔离语义时才能报告 Deleted，404 本身不是删除确认。

## 缓存、选址与恢复

一致性 hash 根据完整 Session scope 确定首选节点，每节点包含 128 个 SHA-256 虚拟点。相同节点的 boot 替换不改变 hash 位置，任意存活 Dispatcher 均可处理有效 Session，不需要 owner、分片移交或内存交接。

每个进程和 Session 使用与 boot 相关的随机起点进行轮转选址。有界 Session、实例池和 affinity 缓存支持热请求不读 Redis；池按需每 30 秒刷新，affinity 无短 TTL。`bypass_cache=true` 必须重读权威状态，失败时不回退缓存。冷启动预留、首次绑定、逻辑实例确认结束后的重绑使用 Redis CAS。同一逻辑 ID 下用户进程重启保留 affinity，不提供显式 affinity 释放或状态变更订阅。

恢复扫描只协调 Creating、待删除实例和 Deleting Session，inline 没有 ADX 记录。Ready/Failed 实例不做周期 Sandbox 查询，创建后的健康由 Substrate 负责。启动失败保持 Failed，直至显式删除；迟到启动结果不能使 Ready 回退。未知结果沿用原 ID 和 Creating/Deleting 状态继续协调。缓存旁路只重读 Agent 状态，不探测 Substrate，不因连接失败切换亲和。本期不承诺检测外部直接删除的 Substrate 实例。创建/删除正确性仍依赖后端隔离契约，模拟后端测试不能证明真实平台能力。

Dispatcher 只维护自身成员注册，不订阅或扫描其他成员。路由环由 Gateway 发现模块维护，使用缓存、定期刷新和失败后刷新。公共请求/响应类型及 hash ring 位于 `adx-agent-core`，引入客户端不会内嵌 Dispatcher 进程。本期不实现自动弹性、选主或并发度采集。

创建观测按 500ms、1s、2s、4s、最多 5s 退避，请求等待方刷新状态也退避。同一进程内请求与后台任务共享有界的每实例查询节奏；不同副本仍可能独立查询，替换或淘汰可重置本地退避，但不能延长持久化创建期限。先检查期限，再检查退避门限，正在执行的查询受剩余创建时间限制。删除重试及后台扫描保持既定频率。Sandbox 暂无生命周期 watch，轮询作为当前适配方式；没有删除隔离保证的迟到创建不能视为已取消，ADX 保留删除意图直至确认。

Session 缓存锁仅覆盖内存快照、affinity 和轮转游标。Redis 读取/CAS、Sandbox 调用和退避等待均在锁外。每缓存槽的异步门限只合并普通刷新，显式 bypass 仍执行权威读取。缓存 epoch 拒绝 bypass 或失效后的迟到结果；生命周期协调保留每实例工作锁，并发冷启动继续依赖 Repository CAS，不增加分布式锁。

Gateway 的发现与最多一次重选共用绝对 HTTP 截止时间，两次调用传递相同的内部认证 Header `X-ADX-Deadline-Ms`，第二次仅使用剩余预算。Dispatcher 按调用方期限收紧 Resolve/创建期限；过期期限在写状态前拒绝，已有 Creating 记录仍沿用保存的期限。默认创建期限 60 秒时，Gateway Dispatcher `timeout_seconds` 为 75 秒，需大于 `create_timeout_seconds + 10`。受管 Agent 入口再留 10 秒回包余量，默认共 85 秒；inline 沿用其原保护期限。

## 验证

`make agent-test` 运行组件测试。真实 Redis 必须使用一次性实例：

```sh
ADX_AGENT_TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
  cargo test --locked -p adx-agent-store -p adx-dispatcher \
  --features adx-agent-store/test-memory -- --ignored --test-threads=1
```

重连测试会主动终止普通 Redis 客户端连接，不能连接共享服务。独立进程测试使用模拟 Gateway Sandbox HTTP 服务和真实 Dispatcher 可执行文件，检查状态收敛及进程硬替换，日志写入 `out/agent-v2-p3/process/`；不覆盖真实 Platform、RRT 或应用 HTTP/WS/SSH 转发。

服务认证、origin 校验和限制定义在 Agent core。Template/affinity 缓存满时按 FIFO 淘汰一项，Session 缓存只淘汰空闲槽，不丢弃活跃的本地协调。内部 Target 响应允许额外顶层字段，写请求与持久记录继续严格解码。共享实现见 [传输校验](../crates/core/src/transport.rs) 和 [限制常量](../crates/core/src/limits.rs)。Session 缓存默认上限 10,000 项，每 Session 的 affinity 缓存默认上限 256 项；HTTP 连接超时最多 3 秒。
