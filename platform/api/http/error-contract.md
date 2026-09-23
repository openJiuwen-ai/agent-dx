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

## SDK 契约

Python SDK 对结构化错误抛出 `SandboxHTTPError`，并公开 `status_code`、`code`、`retry`、`outcome`、`request_id`、`operation_id` 和 `instance_id`。自动重试只能发生在 `same_operation` 或 `after_backoff`，且复用原身份；`never` 立即返回调用方。旧服务没有 `error` 对象时，SDK 保留原有 HTTP 状态兼容路径，但无法给出稳定码。

创建请求未显式提供名称时，SDK 使用 `create-*` Request ID 中的 UUID 生成稳定名称。跨 API Server 重试因此仍得到相同 Environment ID；查询暂时返回 404 不能触发换名称或创建第二个实例。

实现位置：

- 稳定错误码、重试与结果语义：`crates/error/src/lib.rs`
- API Server 的 gRPC/HTTP 分类与序列化：`gateway/apiserver/src/errors.rs`、`src/http.rs`
- Python SDK 解析：`platform/sdk/sandbox/python/adx_sandbox/_transport.py`
- 系统可靠性验收：`docs/testing/system-reliability-gates.md`
