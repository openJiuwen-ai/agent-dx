# Console 业务查询

DigitalMate 租户提供浏览器接口：

- `GET /api/console/v1/tenants/{id}/users`
- `GET /api/console/v1/tenants/{id}/users/{user_id}`
- `POST /api/console/v1/tenants/{id}/users/{user_id}/key/reveal`

Console 先校验自身会话及目标租户权限。standard 租户返回 `403 feature_not_enabled`。
业务查询使用 `business.accounts_api` 指定的 HTTPS 管理入口和独立文件中的租户 Key：

```json
{
  "accounts_api": {
    "base_url": "https://gateway.example.com/api/accounts/v1/admin",
    "timeout_ms": 30000,
    "token_file": "/run/secrets/accounts-tenant-key"
  },
  "bindings": [
    {"kind": "digitalmate", "template": "digitalmate", "ref": {"kind": "alias", "name": "production"}},
    {"kind": "orbit", "template": "orbit", "ref": {"kind": "alias", "name": "stable"}},
    {"kind": "jiuwen", "agent_type": "assistant", "template": "jiuwen", "ref": {"kind": "alias", "name": "stable"}}
  ]
}
```

Gateway 必须显式配置该认证租户的 Accounts 管理 grant。用户归属来自服务端 grant，
请求不能覆盖；任意有效租户 Key 不自动获得管理权。见 [Accounts 管理 API](../../docs/api/accounts-admin.md)。
Console 不持有数据库 DSN、LiteLLM master key 或解密材料；旧 business 配置字段会被拒绝。

列表支持 `user_id/provider/status/display_name/litellm_user_id/key_id/key_alias`，
筛选为 AND，`page_size` 为 1–200（默认 50），游标绑定范围、筛选和 actor。
重复参数、未知字段和非法编码被拒绝。基础筛选在同一只读快照中 count/page；
模型反向筛选需要完整有限扫描，故障时明确失败，不返回部分 total。

业务用户列表的首次加载及筛选失败均在该区域内显示单条错误；重新查询会清除旧提示，
请求期间禁用查询按钮，成功后清除错误并更新列表。

详情保留无模型凭据用户；未知、故障、无凭据是不同状态。管理查询不会创建用户、Key
或凭据。额度使用十进制字符串，负余额保留，缺失额度不解释为零。
Key 明文只在显式 reveal 响应中出现，需传 `key_id` 与 `purpose=view|copy`；
版本或归属变化返回 409，不可恢复返回 404。响应禁止缓存，服务端记录操作审计。

Console 仅额外聚合配置的 AgentBinding 关联，不执行生命周期操作。每个种类必须显式配置；
Jiuwen 的 `agent_type` 必须与 Gateway 的真实映射一致，不能根据模板名猜测。
按租户、模板和派生的 `binding_id` 查询 Binding catalog 并遍历分页，逐条验证完整身份。
关联行的 `refs: [{kind,name}]` 列出该身份实际存在 Binding 的全部版本与别名，去重后按种类、名称排序；
配置的 `ref` 只用于映射校验，不代表实际已有的引用。
完整查询有匹配为 exists，完整查询为空为 not_created，失败、分页游标重复或身份不符为 uncertain；
后续分页失败保留之前已验证的引用，404 等查询失败不解释为不存在。
尚未创建的行仅展示模板、状态和预期 Binding ID，不展示版本或别名；
其他行展示已验证的全部引用及对应 Binding 详情链接，无法确认时明确提示查询状态。
只有全部确认不存在时 `has_binding=false`，不确定时为 null，并标记 Agent 降级。

顶层可选 `upstream_ca_file` 在启动时读取 PEM，向公共根补充私有 CA，
HTTP 和终端 WS 共用信任并验证主机名。文件缺失、为空或损坏会阻止启动。
URL 中的主机名必须与证书 SAN 匹配，容器必须能到达该地址。

本地 Console、TLS 和临时 PostgreSQL 测试已有验证记录；新管理接口尚未发布到
bj-test-0，不能据此宣称现有 48443 或部署浏览器链路已接通。
