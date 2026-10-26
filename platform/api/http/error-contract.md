# ADX 管理面错误契约

本文定义 API Server、内部 gRPC 和 Environment SDK 之间稳定的错误语义。适用范围是 Environment 创建、查询、暂停、恢复、快照、删除、实例目录查询及管理员接口。Ingress → Relay → EXECD 的应用流量错误属于数据面协议，不使用本文的自动重试规则。

## HTTP 响应

成功响应继续使用 `code`、`message`、`data` 包装。失败响应保留数值型 HTTP `code`，并增加结构化 `error`：

```json
{
  "code": 503,
  "message": "reply lost",
  "data": null,
  "error": {
    "code": "OUTCOME_UNKNOWN",
    "retry": "same_operation",
    "outcome": "unknown",
    "requestId": "create-4f2...",
    "operationId": "pause-90a...",
    "environmentId": "environment-a"
  }
}
```

`requestId` 始终存在并同时通过 `X-Request-Id` 返回。`operationId` 只在暂停、恢复、快照等显式生命周期操作中出现；`environmentId` 仅在请求已经具有稳定实例身份时出现。调用方不能从错误文本推断是否重试。

SSE 创建的 `final` 事件保留 `errorCode` 数值字段，并携带同一个 `error` 对象。HTTP 连接中断且没有收到 `final` 时，SDK 按 `OUTCOME_UNKNOWN` 处理，并用原 Request ID 查询或重试。

## 重试和结果语义

`retry` 只有以下取值：

| 值 | 调用方行为 |
|---|---|
| `never` | 不自动重试；修改请求、凭据或由用户处理 |
| `same_operation` | 只能使用相同 Request ID、Operation ID 和 Environment ID 查询或重试，不能换 ID 重建 |
| `after_backoff` | 在整体 deadline 内退避后重试；有稳定操作身份时仍沿用原身份 |

`outcome` 只有以下取值：

| 值 | 含义 |
|---|---|
| `not_started` | 服务端确认目标操作没有越过执行边界 |
| `unknown` | 请求可能已经执行，调用方必须查询或以相同身份重试 |
| `terminal` | 已得到终态，例如资源不存在或持久化数据损坏 |

`retry=after_backoff` 不表示可以忽略整体 deadline；`outcome=unknown` 也不能降级成普通 404。

## 稳定错误码

| 稳定码 | gRPC | HTTP | retry | outcome | 说明 |
|---|---|---:|---|---|---|
| `INVALID_ARGUMENT` | `InvalidArgument`、`OutOfRange` | 400、413 | `never` | `not_started` | 请求格式、范围或不支持的字段组合无效 |
| `UNAUTHENTICATED` | `Unauthenticated` | 401 | `never` | `not_started` | 凭据缺失、无效或过期 |
| `PERMISSION_DENIED` | `PermissionDenied` | 403 | `never` | `not_started` | 已认证身份没有权限；实例接口可以为租户隔离映射成 404 |
| `NOT_FOUND` | `NotFound` | 404 | `never` | `terminal` | 权威状态确认资源不存在；删除接口可以将其视为幂等成功 |
| `CONFLICT` | `AlreadyExists`、`FailedPrecondition`、`Aborted` | 409 | `never` | `not_started` | 身份、规格、代次、状态或操作参数冲突 |
| `RESOURCE_EXHAUSTED` | `ResourceExhausted` | 429 | `after_backoff` / `never` | `not_started` / `terminal` | 准入、队列或资源暂时不足可退避；后端明确返回的创建限流失败为 terminal/never |
| `UNSUPPORTED` | `Unimplemented` | 501 | `never` | `not_started` | 当前版本不提供该能力 |
| `UNAVAILABLE` | `Unavailable` | 503 | `after_backoff` | `not_started` | 请求尚未进入执行边界，下游暂不可用 |
| `DEADLINE_EXCEEDED` | `DeadlineExceeded` | 504 | `never` / `same_operation` | `terminal` / `not_started` / `unknown` | 确认撤销未分配调度请求时为 not_started/never；节点确认创建失败时为 terminal/never；未确认超时保留原身份查询或重试 |
| `OUTCOME_UNKNOWN` | `Cancelled`、`Unknown`、`Unavailable`、`Internal` | 500、503 | `same_operation` | `unknown` | 写请求已越过执行边界，但最终应答未确认 |
| `DATA_LOSS` | `DataLoss` | 500 | `never` | `terminal` | 权威记录不完整、身份不一致或持久化数据损坏 |
| `INTERNAL` | 其他未分类状态 | 500 | `after_backoff` | `not_started` | 服务端在进入执行边界前失败；必须记录日志并补充明确分类 |

同一个 gRPC 状态可能映射成不同稳定码。例如 `Unavailable` 在写请求尚未提交时是 `UNAVAILABLE`，在 Adxlet 已经接受创建或生命周期操作后是 `OUTCOME_UNKNOWN`。API Server 根据请求是否越过执行边界作出映射；SDK 不重新猜测。

内部 RPC 的连接拒绝、连接重置等传输不可用返回 HTTP 503。Tonic 在握手或重连期间将部分连接重置报告为 `Unknown / transport error` 或带有传输来源的 `Cancelled`；API Server 仅在错误来源链包含 `tonic::transport::Error` 时归一为 `Unavailable`，不会把应用返回的 `Unknown`、`Cancelled` 或 `Internal` 一律改成 503。RPC 超时归一为 504、`DEADLINE_EXCEEDED`；只有等待超时而没有确认终态时，仍使用 `outcome=unknown`、`retry=same_operation`。写请求的结果可能未知，创建重试沿用原 Request ID 和 Environment ID；尚未成功确认的执行可以重新分配 generation，并由对账回收残留。已确认成功和后续生命周期继续校验归属代次；不能因连接失败覆盖已有成功。

### 创建超时阶段

创建超时使用 HTTP 504 和 `DEADLINE_EXCEEDED`。节点确认该创建已失败时，返回
`retry=never`、`outcome=terminal`；`message` 指明 `backend preparation/runtime start`、
`Execd readiness` 或 `route activation`，不增加公开 `stage` 字段。后端准备和运行时启动
属于同一次 sandboxd Start。后端明确返回的内部超时保留 `image pull (rootfs)`、
`image pull (volume <target>)` 或 `runtime start (<runtime>)` 及原始原因，即使创建总期限尚未到。
只有外层计时器先到且未收到后端阶段结果时，才使用合并的准备/启动阶段文案。
相同 Request ID 重放或查询后重放保留该失败原因，不重新启动原执行。
工作负载配置了 readiness probe 时，保留下文既有 `ProbeFailure` 契约。

API Server、Coordinator 和入口节点为创建错误回传预留最多 3 秒；节点失败状态发布
最多使用其中 1 秒。请求携带的绝对创建截止时间保持不变，后端启动、就绪等待和
路由激活均仍受原截止时间限制。该窗口不延长业务执行预算，也不等待物理清理。
失败控制器继续按既有对账路径清理资源。若此窗口仍未收到节点结果，错误文案说明
等待调度/节点初始化结果超时，`outcome=unknown`，不猜测某个底层步骤已完成。
断连、应答丢失等无法确认的结果继续使用 `OUTCOME_UNKNOWN`。
节点已判定超时，但失败记录未确认写入权威存储或本地持久日志时，同样返回
`DEADLINE_EXCEEDED` 和原阶段文案，附带发布未确认说明；此时使用
`outcome=unknown`、`retry=same_operation`，由后续对账补写，不能宣称已确认终态。

### 调度队列到期

调度队列继续等待至 `scheduleTimeoutSeconds`。Coordinator 只有在同一状态锁内确认
没有 assignment 或进行中的 local claim、成功撤销 pending 请求并完成相关引用释放后，才通过内部
`adx-scheduling-cancelled: true` 与类型化 JSON details 返回调度失败。API Server
对此使用 HTTP 504、`DEADLINE_EXCEEDED / never / not_started`；SSE 使用 HTTP 200，
在 `final` 事件中携带 `errorCode=504` 和相同的结构化 `error`。
`never` 表示本次队列等待已经结束，SDK 不自动重开等待预算；调用者可在资源或
放置条件变化后发起新的逻辑创建。已分配、状态存储不明、普通 RPC 超时和断连
继续遵循原有 `same_operation / unknown` 契约，不凭超时推断已取消或已清理。

`error.schedulingFailure` 提供当前锁定调度快照的诊断：

- `snapshotRevision`、`nodeCount`、`eligibleNodes`：快照版本、已知 worker 数、当前通过全部硬过滤器的 worker 数。
- `requested`、`maxCapacity`、`maxAvailable`：请求资源、逐维最大 worker 总容量、逐维最大可用资源。资源字段为 `cpu_millis`、`memory_bytes`、`disk_bytes`；可用量已扣除预留，最大值可能来自不同 worker，不能拼成一台可用机器。
- `rejections`：各失败条件的节点计数，可重叠。标量不足使用 `insufficient_cpu`、`insufficient_memory`、`insufficient_disk`；其他键为失败过滤器名的下划线形式，如 `node_available`、`runtime_fit`、`device_fit`、`node_affinity`、`placement_groups`、`environment_affinity`、`topology_spread`。Volume 检查增加 `waiting_for_volume`（卷被占用）和 `volume_topology`（节点不可访问相关存储域）；同卷等待不阻塞其他请求，没有严格 FIFO。没有 worker 时为 `no_workers=1`。

诊断不包含节点 ID、标签、其他租户的实例或内部地址。它描述到期时的集群状态，
不证明未来扩容后仍无法调度。`eligibleNodes>0` 时可能只是队列预算先到，不能
伪造容量不足。过滤器自身执行失败继续报告原错误，不转成资源不足。
已确认撤销的响应进入普通完成缓存；同一 Request ID 的重放在缓存保留期间返回
原失败，不重新排队，也不生成运行时失败实例记录。

### 创建重放缓存

生命周期操作的重试身份表不以 `cache_entries` 为准入限制，不会因为保留的操作记录多而返回 429；实际节点资源或准入拒绝仍按原错误契约处理。

API Server 的已完成创建响应使用 600 秒 TTL 与 `cache_entries` 容量淘汰；缓存满只淘汰旧完成项，不产生 `RESOURCE_EXHAUSTED`。正在执行和结果未知的请求保留同一租户、Request ID、Environment ID 与规格，不受完成缓存容量限制。结果未知的重试在保留窗口内查询权威归属，或以原身份进入原子创建链路；不会仅凭传输超时将 Environment 标为 Failed 或释放节点预留。未知上下文的 `create_unknown_retention_seconds` 默认 600 秒，从首次未知结果计时，不因未决重试续期；到期后周期 GC 直接回收，不查询权威状态，执行中或仍有共享身份等待的请求不回收。GC 不改变 Environment 或资源状态。完成响应过期、淘汰、未知请求 GC 或进程重启后，成功请求仍由 Coordinator 的持久回执去重：同租户的 Request ID 或 Environment ID 任一个已有成功，就复用该结果和规格。已删除结果不会复活；回执不会指向同名后续的新执行。

### sandboxd Start 失败

sandboxd 应用明确返回的 Start 超时使用 gRPC `DeadlineExceeded`，并携带内部 trailer
`sandboxd-start-failure: true`。adxlet 保留其阶段和原因，将节点失败结果按既有机制发布；
发布确认后 HTTP/JSON 与 SSE final 均为 `DEADLINE_EXCEEDED / never / terminal`。
非超时 Start 失败保留后端 gRPC 分类和原始 message，不统一转换成 `Unavailable`。
镜像无效请求、认证失败、权限拒绝、不存在和限流分别映射为 `InvalidArgument`、
`Unauthenticated`、`PermissionDenied`、`NotFound` 和 `ResourceExhausted`，公开错误
继续遵循上表既有重试语义。后端限流属于该创建已失败的结果，返回
`RESOURCE_EXHAUSTED / never / terminal`；只有节点准入/容量不足继续使用
`after_backoff / not_started`，Coordinator 不将后端限流当作准入失败重试。
只对真实网络超时和明确的超时状态执行超时分类。
该 trailer 表示后端返回了应用失败，不表示物理资源已清理。没有该标记的 RPC 超时
继续按外层期限和结果确认状态处理，不能仅凭传输错误确认后端结果。

adxlet 未获得有效 Start 成功结果（失败、超时、应答丢失或无效载荷）时，将 Environment 创建判定为 `Failed`，不会重复启动同一执行，也不会把迟到后端接纳为成功实例。创建进入 Failed 即释放本机标量资源和 GPU/NPU 预留，并在清理之前提交 `resources_held=false`；查询、删除失败或清理超时不阻止资源释放。Failed 控制器独立重试清理迟到执行及补写未发布结果。节点重启按权威目录对账。

这与公开创建 HTTP 应答丢失的 `OUTCOME_UNKNOWN` 分开：客户端不知道服务端结果时，仍须查询或以相同身份重试。查得 Failed 后不能据此再次启动原执行；相同 Environment 的 create 会报状态冲突。checkpoint 的未知结果保护不受该 Start 契约影响。

## 工作负载 probe 失败

Readiness 的初始化预算耗尽，或恢复后的 readiness 失败，使用带
`ProbeFailure` JSON details 的 gRPC `FailedPrecondition`。API Server 将其映射为
HTTP 409、`code=UNAVAILABLE`、`retry=never`、`outcome=terminal`，并在
`error.probeFailure` 中保留 `kind`、`message`、`consecutive_failures` 和可选的
`exit_code` / `http_status`。SSE final 使用同一结构。该分类优先于普通
`FailedPrecondition → CONFLICT`；调用方不将已确认的 probe 失败当作结果未知重试。

Healthcheck 达到连续失败阈值后，由 adxlet 清理并按既有策略处理执行；查询的
`failure.reason=ProbeFailed` 和 `failure.probe_failure` 保存已提交的原因，
复用现有 OOM/退出失败模型。自动重启成功后该字段清空。

## SDK 契约

Python SDK 对结构化错误抛出 `EnvironmentHTTPError`，并公开 `status_code`、`code`、`retry`、`outcome`、`request_id`、`operation_id`、`environment_id` 和可选的 `probe_failure`、`scheduling_failure`。自动重试只能发生在 `same_operation` 或 `after_backoff`，且复用原身份；`never` 立即返回调用方。创建重试耗尽若最后收到完整服务失败，仍抛出 `EnvironmentHTTPError` 并保留状态码、载荷与诊断；连接/流中断才使用传输失败文案。旧服务没有 `error` 对象时，SDK 保留原有 HTTP 状态兼容路径，但无法给出稳定码。

创建请求未显式提供名称时，SDK 使用 `create-*` Request ID 中的 UUID 生成稳定名称。跨 API Server 重试因此仍得到相同 Environment ID；查询暂时返回 404 不能触发换名称或创建第二个实例。

实现位置：

- 稳定错误码、重试与结果语义：`crates/error/src/lib.rs`
- API Server 的 gRPC/HTTP 分类与序列化：`gateway/apiserver/src/errors.rs`、`src/http.rs`
- Python SDK 解析：`platform/sdk/environment/python/adx_environment/_transport.py`
- 系统可靠性验收：`docs/testing/system-reliability-gates.md`

## OOM 原因与 SDK

OOM 分为命令级与 Environment 级，不由 HTTP 502/503 或 exit_code=137 推断。

- **用户进程**：Execd 只保留命令退出结果，不观测或判断 OOM。SDK `CommandResult.exit_signal` / `CommandInfo.exit_signal` 表示命令实际终止信号（如 SIGKILL=9），没有普通退出码时继续保留原来的 `exit_code=-1`。用户进程退出而 Execd/后端仍 Running 时，只是命令失败；普通 SIGKILL 不能证明 OOM。当前 sandboxd 接口没有单独的用户进程 OOM 事件，不提供 `PROCESS_OOM` 之类推断码。
- **整个运行环境**：Adxlet 观察到后端 Exited 后、删除前调用 sandboxd `Wait`，仅将后端提供的 OOM message 转换为 `failure.reason=OomKilled`，同时保留后端 `exit_code`。原因与同一个 Environment 记录一起持久化、发布到订阅目录。`GET /api/instances?instance_id=...` 和 SDK `get_info().failure` 可读取；成功启动/恢复的新执行清除旧原因。
- SDK direct invoke 重试耗尽后，在原 deadline 剩余时间内进行一次最长 2 秒的认证目录查询。仅当目录已经提交 `status=failed` 且 `failure.reason=OomKilled` 时返回 `EnvironmentError(code=ENVIRONMENT_OOM, retry=never, outcome=terminal)`，`failure` 属性保留详情。这是 SDK 对已确认 Environment 终态的分类，不是新的管理面 gRPC 状态，也不改变数据网关的通用 502/503 响应。
- 原因尚未提交、API 暂不可用、普通退出或缺少 OOM 证据时保留 `OUTCOME_UNKNOWN` 等原分类；失败查询不会刷新 deadline。早于状态提交的首个调用仍可能收到 OUTCOME_UNKNOWN，之后用 `get_info()` 确認原因。不会在普通成功调用中增加目录查询。
- 普通退出记录 `failure.reason=Exited`；后端已消失且没有可读取退出记录时为 `Missing`，不猜测 OOM。SDK 未知结果查询的权限与普通实例查询相同，不能跨租户读取原因。

### Confirmed creation failures

An internal create status may include bounded adx-create-stage/adx-create-reason
provenance. adx-create-terminal=true is emitted only when the producer has confirmed
a Published terminal Failed result. It maps to retry=never/outcome=terminal; plain
transport Unavailable or DeadlineExceeded retains outcome=unknown. API diagnostics
export bounded labels rather than raw error messages. See the
[Sandbox metric contract](../../../docs/testing/sandbox-observability.md).

### Volume 管理错误

Volume 管理沿用上述错误结构。ensure 与删除应答未知时用相同 tenant/Volume ID 或 owner ID 重试；不能从超时推断定义或关联未提交。删除有引用、关联或占用的卷返回 CONFLICT，数据目录始终保留。接口见 [Volume API](volumes.md)。
