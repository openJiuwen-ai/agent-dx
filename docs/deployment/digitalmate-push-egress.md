# DigitalMateGateway 华为 Push Kit

沙箱通过 DigitalMateGateway 主业务 HTTPS 入口调用 `POST /api/digitalmate/push/v1/send`。网关在入口验证工作负载和当前 Binding，扣减发送配额后再复查一次 Binding，随后签发 PS256 JWT，向固定华为端点发送单设备通知。服务账号私钥和 JWT 仅由网关持有。

## 启用 Push

1. 配置与 `digitalmate.template` 匹配的现有 `privacy` 身份服务。Push 复用它签发的工作负载凭据。发送请求使用与邮件接口相同的业务入口。需要限制调用网络时，在该入口对发送路径设置访问策略。
2. 把华为服务账号 JSON 只读挂载到网关进程。必填字段为 `project_id`、`key_id`、`sub_account` 和 PKCS#8 PEM `private_key`。每个网关实例在内存中复用该账号签发的 JWT（有效期 3600 秒）。剩余有效期不超过 60 秒时，下一次发送会刷新 JWT，并发请求共用刷新结果。更新凭据后滚动重启网关，新实例加载新文件并建立自己的缓存。
3. 在 Push 使用的 PostgreSQL 数据库预先执行[配额建表脚本](../../gateway/src/ingress/digitalmategateway/composio/budget.sql)。即使单独启用 Push，也需要版本为 1 的 `digitalmate_composio_budget` schema。为 Push 配置独立、跨副本稳定的 `organization` 和一致的组织、用户分钟额度。schema 缺失或同一组织的配额配置冲突会使网关启动失败。
4. 在 `ADX_JIUWEN_CONFIG` 中加入可选的 `push` 段。网关进程需要能读取服务账号文件和数据库连接环境变量，并通过 HTTPS 访问华为。以下是示例路径和额度。鉴权服务和业务应用的配套字段见[完整 Unix 网关示例](../../gateway/examples/digitalmategateway.json)。

```json
{
  "push": {
    "service_account_file": "/run/digitalmate-push/service-account.json",
    "notification_category": "WORK",
    "organization": "digitalmate-push",
    "database": { "database_url_env": "ADX_ACCOUNT_DATABASE_URL", "database_max_connections": 4 },
    "limits": { "organization_per_minute": 600, "user_per_minute": 60 }
  }
}
```

## 发送接口

使用 `Authorization: Bearer <dm-privacy-v1 工作负载凭据>` 和唯一的 `Content-Type: application/json`。最大请求体为 16 KiB，包含 `event_id`、`title`、`body`、`profile_id`、一个 `device_token`、字符串映射 `click_target` 和 1–3600 秒的 `ttl_seconds`。查询参数和 `Origin` 会被拒绝。出站项目来自网关配置。

DigitalMate 管理设备注册、撤销和设备选择。网关不核验 `device_token` 归属，因此持有效工作负载凭据的调用方可以向其提供的 Token 发送通知，受配额约束。`click_target.workspace` 和 `principal` 表示 DigitalMate 客户端会话中的导航范围（例如 Bridge 的 `local` 工作空间）。客户端在打开通知时与当前登录范围比对。网关的鉴权和配额只使用已验证工作负载的身份。

| HTTP | result | 调用方处理 |
| --- | --- | --- |
| 200 | `accepted` | 华为已受理，设备展示由系统决定。 |
| 200 | `rejected` | 华为明确拒绝该消息，例如目标 Token 无效，或华为返回 HTTP 400 与业务码 `80100003`（消息结构错误）。结束本次投递。 |
| 4xx / 503 | `not_sent` | 网关尚未出站或华为明确拒绝网关鉴权。仅 `retryable: true` 时允许重试，429 遵守 `Retry-After`。华为 401/403 会产生网关告警，应核对服务账号、项目权限及 JWT。 |
| 409 | `unknown` | 已开始出站，无法确认投递结果。保留原事件供核对，禁止自动重发。 |

调用方应省略 `x-adx-processing-timeout-ms`。Push 会在鉴权和出站前拒绝调用方指定的处理时限，返回 HTTP 400、`not_sent` 和 `retryable: false`。接口总时限为 20 秒，覆盖读取请求、鉴权、配额和华为响应。出站前超时返回可重试的 `not_sent`。出站后的超时、上游 408/429/5xx、不完整或超大的响应均返回 `unknown`。调用方自身超时、断连或收到缺少明确结果的响应，也按结果未知处理。
