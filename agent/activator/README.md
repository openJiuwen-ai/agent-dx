# Activator

Activator 为独立部署的无状态产品控制进程 `adx-activator`。多个副本连接相同 ADX Redis namespace，通过条件写提交唯一 Environment 身份，再调用 Gateway 的通用 Sandbox HTTP 接口。无成员注册、选主、所有权或恢复扫描。

Gateway 通过 `agent/api` 的 ActivatorClient 发起 HTTP 请求。公开认证与业务流量转发留在 Gateway；Activator 管理 Template/Environment 状态。配置见 [Agent 部署说明](../README.md#部署)。

独立进程启动读取 `ADX_ACTIVATOR_CONFIG` 指向的 JSON，示例见 [local.json](examples/local.json)。服务凭据为 `ADX_ACTIVATOR_SERVICE_TOKEN` 与 `ADX_SANDBOX_SERVICE_TOKEN`，均不写入示例配置。当前进程监听 HTTP，要求显式允许内网明文；生产须置于 TLS 服务代理后并限制访问。

内部端点统一为 POST `/internal/adx/v1/`：

| 后缀 | 请求 | 行为 |
| --- | --- | --- |
| `templates/publish` | tenant、template | 发布不可变版本 |
| `templates/get` | tenant、name、version | 查询模板 |
| `environments/get` | scope | 查询产品元数据 |
| `environments/list` | tenant、template、version、page_size?、page_token? | 有界分页查询产品元数据，不访问 Sandbox |
| `environments/activate` | scope、expected_generation? | 首次访问幂等提交 Environment，再查询或创建 Sandbox，返回可用目标 |
| `environments/delete` | scope | 平台确认删除后条件清理元数据 |

仅可信服务可传入租户 scope；公网租户认证在 Gateway 完成。端点不持有用户流量，不限制 HTTP/WS/SSH 并发。`x-adx-deadline-ms` 传递入口绝对期限，创建 Sandbox 时继续作为内部请求的 `deadline_unix_ms` 传递；各服务只能按自身配置缩短它，不能重新开始计时。服务主机需要同步时钟。写入响应超时不表示拒绝，调用方复用原身份查询/重试。

列表默认每页 50 条，最多 100 条，返回 `environments` 和 `next_page_token`。分页 token 绑定租户及模板版本，非法参数或跨范围 token 返回 Invalid；模板不存在返回 NotFound。列表包含 Deleting 元数据，不表示平台运行状态，并发更新期间不提供快照。删除完成后下一次访问可重建同名 Environment；删除中返回 Conflict。

同一次用户请求在 Gateway 内部重试时，携带首次选定的 `expected_generation`。元数据已删除或属于新 generation 时返回 Conflict，不重新创建或转到另一次生命周期；该字段不属于用户的调用参数。

`/health/live` 和 `/health/ready` 表示进程启动完成；启动时验证 Redis schema，后续存储故障由请求明确返回，不进行后台全表扫描或健康轮询。
