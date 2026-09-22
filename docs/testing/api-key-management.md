# API Key 管理

Master 在 Redis 中保存密钥 SHA256 摘要、租户及可选到期时间。部署配置通过受保护文件注入初始管理员密钥；管理员通过 API Server 管理租户密钥。Edge 的控制路由须包含 `prefix:/api/admin/v1/keys`，默认路由及统一部署示例已配置。用户传入 `Authorization: Bearer <key>` 或既有 `X-Auth` / `X-Auth-Token`。

| HTTP 接口 | 行为 |
| --- | --- |
| `POST /api/admin/v1/keys` | 请求 `{"tenantId":"team-a","expiresAtUnixSeconds":0}`，生成租户密钥；0或省略表示不过期。201响应包含 `key` 元数据和一次性 `apiKey` 明文。 |
| `GET /api/admin/v1/keys` | 返回 `items` 和 `nextPageToken`；可按 `tenantId` 筛选，`pageSize` 默认100、最大1000，后续传入 `pageToken`。不返回密钥明文。 |
| `DELETE /api/admin/v1/keys/{id}` | 吊销指定租户密钥，重复调用返回204。`id` 为创建或列表返回的摘要标识。 |

所有管理接口要求管理员身份，且响应设为 `Cache-Control: no-store`。租户不能创建、查询或吊销密钥；请求中的 `administrator` 等未知字段被拒绝。内部 `CredentialService` 仅接受 mTLS 验证的 API Server，并再次检查管理员上下文。Edge 只能调用密钥验证服务。

面向管理员的 [`adxadmin`](../deployment/adxadmin.md) 已封装上述三个 HTTP 接口。它从
`0600` Key 文件读取管理员凭证，只连接公开 HTTPS 入口，创建密钥时默认写入新的
标准输出，也可通过 `--output-file` 写入新的 `0600` 文件；创建请求不自动重试。
`adxctl` 继续只负责本机部署与进程监督。

## 持久化和失联

- 明文使用两个随机 UUID v4 拼接并添加 `adx_` 前缀，仅存在于创建响应中，Redis 不保存明文。创建调用不自动重试；如果响应丢失，可按租户查询并吊销不再需要的记录，再创建新密钥。
- 吊销原子写入永久吊销标记并删除可认证记录。Master 或 Redis 重启后，重复加载旧的部署凭证不会复活已吊销的租户密钥。
- API 管理范围为租户密钥。初始管理员凭证的轮换仍由部署配置和运维负责；不能通过租户密钥接口删除管理员凭证。
- API Server 与 Edge 的现有认证缓存继续生效，吊销传播上限受各自配置的缓存 TTL 约束。到期时间会进一步缩短缓存有效期。Master 不可用时不能创建或吊销密钥。
- 管理写入受当前 Master epoch 约束；旧进程不能继续修改新会话中的密钥。

## 验证

`out/ci/stage-6/auth/green-2.log`：真实 Redis AOF 崩溃重启、吊销后启动配置重放、旧 Master 写入隔离，以及真实 mTLS 的组件/管理员权限测试通过。`clippy.log`：Master全部目标Clippy通过。

`go-green.log`：所有Go包测试、vet和API构建通过。`http.log` / `http/frontend-http.log`：真实 Go HTTPS → Rust mTLS → Redis 的创建、查询、吊销、重复吊销、缓存过期和非法到期时间用例通过。`process.log`另验证实际Master二进制在重启前后均注册密钥服务。上述HTTP测试的实例执行后端使用fixture，不代表完整运行时或Kubernetes端到端验收。

2026-09-16：package-v18 已补齐通过 HTTPS Edge 的管理员创建/查询/吊销、租户拒绝与缓存期限内失效验收，见 [真实双节点记录](2026-09-16-deployment-acceptance.md)。
