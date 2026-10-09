# ADX 管理面错误契约

本文定义 API Server、内部 gRPC 和 Sandbox SDK 之间稳定的错误语义。适用范围是 Sandbox 创建、查询、暂停、恢复、快照、删除、实例目录查询及管理员接口。Ingress → Relay → EXECD 的应用流量错误属于数据面协议，不使用本文的自动重试规则。

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
    "instanceId": "sandbox-a"
  }
}
```

`requestId` 始终存在并同时通过 `X-Request-Id` 返回。`operationId` 只在暂停、恢复、快照等显式生命周期操作中出现；`instanceId` 仅在请求已经具有稳定实例身份时出现。调用方不能从错误文本推断是否重试。

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
| `RESOURCE_EXHAUSTED` | `ResourceExhausted` | 429 | `after_backoff` | `not_started` | 准入、队列或资源暂时不足 |
| `UNSUPPORTED` | `Unimplemented` | 501 | `never` | `not_started` | 当前版本不提供该能力 |
| `UNAVAILABLE` | `Unavailable` | 503 | `after_backoff` | `not_started` | 请求尚未进入执行边界，下游暂不可用 |
| `DEADLINE_EXCEEDED` | `DeadlineExceeded` | 504 | `same_operation` | `not_started` | 服务端确认操作尚未执行，例如中心队列在 Assignment 前过期 |
| `OUTCOME_UNKNOWN` | `Cancelled`、`Unknown`、`DeadlineExceeded`、`Unavailable`、`Internal` | 500、503、504 | `same_operation` | `unknown` | 写请求已越过执行边界，但最终应答未确认 |
| `DATA_LOSS` | `DataLoss` | 500 | `never` | `terminal` | 权威记录不完整、身份不一致或持久化数据损坏 |
| `INTERNAL` | 其他未分类状态 | 500 | `after_backoff` | `not_started` | 服务端在进入执行边界前失败；必须记录日志并补充明确分类 |

同一个 gRPC 状态可能映射成不同稳定码。例如 `Unavailable` 在写请求尚未提交时是 `UNAVAILABLE`，在 Adxlet 已经接受创建或生命周期操作后是 `OUTCOME_UNKNOWN`。API Server 根据请求是否越过执行边界作出映射；SDK 不重新猜测。

内部 RPC 的连接拒绝、连接重置等传输不可用返回 HTTP 503。Tonic 在握手或重连期间将部分连接重置报告为 `Unknown / transport error` 或带有传输来源的 `Cancelled`；API Server 仅在错误来源链包含 `tonic::transport::Error` 时归一为 `Unavailable`，不会把应用返回的 `Unknown`、`Cancelled` 或 `Internal` 一律改成 503。RPC 超时仍返回 504。写请求的结果可能未知，重试必须沿用原 Request ID、Operation ID、Environment ID 和归属代次；连接失败不能作为释放归属或换 ID 重建的依据。

### 创建重放缓存

生命周期操作的重试身份表不以 `cache_entries` 为准入限制，不会因为保留的操作记录多而返回 429；实际节点资源或准入拒绝仍按原错误契约处理。

API Server 的已完成创建响应使用 600 秒 TTL 与 `cache_entries` 容量淘汰；缓存满只淘汰旧完成项，不产生 `RESOURCE_EXHAUSTED`。正在执行和结果未知的请求保留同一租户、Request ID、Environment ID 与规格，不受完成缓存容量限制。结果未知的重试在保留窗口内查询权威归属，或以原身份进入原子创建链路；不会仅凭传输超时将 Environment 标为 Failed 或释放节点预留。未知上下文的 `create_unknown_retention_seconds` 默认 600 秒，从首次未知结果计时，不因未决重试续期；到期后周期 GC 直接回收，不查询权威状态，执行中或仍有共享身份等待的请求不回收。GC 不改变 Environment 或资源状态。完成响应过期、淘汰、未知请求 GC 或进程重启后，重试按当前 Environment 状态处理，不再保证历史请求去重或参数绑定。

### sandboxd Start 失败

adxlet 未获得有效 Start 成功结果（失败、超时、应答丢失或无效载荷）时，将 Environment 创建判定为 `Failed`，不会重复启动同一执行，也不会把迟到后端接纳为成功实例。创建进入 Failed 即释放本机标量资源和 GPU/NPU 预留，并在清理之前提交 `resources_held=false`；查询、删除失败或清理超时不阻止资源释放。Failed 控制器独立重试清理迟到执行及补写未发布结果。节点重启按权威目录对账。

这与公开创建 HTTP 应答丢失的 `OUTCOME_UNKNOWN` 分开：客户端不知道服务端结果时，仍须查询或以相同身份重试。查得 Failed 后不能据此再次启动原执行；相同 Environment 的 create 会报状态冲突。checkpoint 的未知结果保护不受该 Start 契约影响。

## SDK 契约

Python SDK 对结构化错误抛出 `SandboxHTTPError`，并公开 `status_code`、`code`、`retry`、`outcome`、`request_id`、`operation_id` 和 `instance_id`。自动重试只能发生在 `same_operation` 或 `after_backoff`，且复用原身份；`never` 立即返回调用方。旧服务没有 `error` 对象时，SDK 保留原有 HTTP 状态兼容路径，但无法给出稳定码。

创建请求未显式提供名称时，SDK 使用 `create-*` Request ID 中的 UUID 生成稳定名称。跨 API Server 重试因此仍得到相同 Environment ID；查询暂时返回 404 不能触发换名称或创建第二个实例。

实现位置：

- 稳定错误码、重试与结果语义：`crates/error/src/lib.rs`
- API Server 的 gRPC/HTTP 分类与序列化：`gateway/apiserver/src/errors.rs`、`src/http.rs`
- Python SDK 解析：`platform/sdk/sandbox/python/adx_sandbox/_transport.py`
- 系统可靠性验收：`docs/testing/system-reliability-gates.md`
