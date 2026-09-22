# Activator

`adx-activator` 为无状态产品控制进程。多个副本连接相同 ADX Redis namespace，通过条件写提交唯一 Environment 身份，再调用通用 Sandbox 接口。无成员注册、选主、所有权或恢复扫描；部署系统提供稳定地址或 Gateway 配置地址列表。

启动读取 `ADX_ACTIVATOR_CONFIG` 指向的 JSON，示例见 [local.json](examples/local.json)。服务凭据为 `ADX_ACTIVATOR_SERVICE_TOKEN` 与 `ADX_SANDBOX_SERVICE_TOKEN`，均不写入示例配置。当前进程监听 HTTP，要求显式允许内网明文；生产须置于 TLS 服务代理后并限制访问。

内部端点统一为 POST `/internal/adx/v1/`：

| 后缀 | 请求 | 行为 |
| --- | --- | --- |
| `templates/publish` | tenant、template | 发布不可变版本 |
| `templates/get` | tenant、name、version | 查询模板 |
| `environments/create` | scope | 提交稳定 generation 与 sandbox_id |
| `environments/get` | scope | 查询产品元数据 |
| `environments/activate` | scope | 查询或幂等创建 Sandbox，返回可用目标 |
| `environments/delete` | scope | 平台确认删除后条件清理元数据 |

仅可信服务可传入租户 scope；公网租户认证在 Gateway 完成。端点不持有用户流量，不限制 HTTP/WS/SSH 并发。`x-adx-deadline-ms` 传递剩余请求预算；写入响应超时不表示拒绝，调用方复用原身份查询/重试。

`/health/live` 和 `/health/ready` 表示进程启动完成；启动时验证 Redis schema，后续存储故障由请求明确返回，不进行后台全表扫描或健康轮询。
