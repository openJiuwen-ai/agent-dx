# Gateway 后台摄取协调

摄取模块在现有 DigitalMateGateway 进程中运行，位于用户沙箱外。它负责验签、
持久判重、容量与租约、合批、请求已有用户实例就绪及交付，不做邮件语义分类、
报告生成或通知。用户授权和后台意图仍由 M44 owner 管理。

## 接收、持久化与交付

固定入口 `POST /api/digitalmate/ingestion/v1/composio/webhook` 由已有
`DigitalMateGatewayApi::matches/handle` 统一分发。原始请求最多 1 MiB；
Composio 签名/时间戳校验后，精确匹配当前注册的 trigger/account/config/user。
只有 PostgreSQL 提交后返回200 `durable`。未验签401、归属不符403、同 ID
不同摘要409；容量/数据库/提交未知返回503与有限 Retry-After。

`Inbox` 是订阅投影、事件、批次及回执的唯一持久写入者。队列有全局与每订阅
容量，精确重复不重复占额；按工作区锁序列化，持久批次固定 ID、digest 和成员。
worker 领取有界租约并续租，失败或结果未知重试原批次，不重新生成业务身份。
终态清理由模块内维护任务执行。

身份 owner 校验真实既有 grant，邮件订阅/批次只保存随机授权引用 authority_ref，
不存储 Binding generation、Environment 或端口。邮件交付传逻辑 Scope 给普通
AgentV2Access::select_with_initializer / connect_service_when_ready，缺 Binding
即失败，不另建生命周期/连接模式。原始 deadline 限定等待，物理发送前由身份
owner 重验原授权，不能改绑新工作区；已发送 HTTP 不重放。wire/ACK 版本2及
显式旧投影升级规则见下方交付契约，不能直接重写旧批次摘要或丢弃未知结果。

交付前先 GET 同批回执，缺失才 POST；只提交匹配 batch ID/digest/实际字节数
的 durable ACK。接收端持久受理不代表分类、最终 Signal 或报告完成。
详细字段、bootstrap 和失效边界见 [交付契约](ingestion-delivery-contract.md)。

## 配置与密钥

Unix 的 `entrypoint_runtime.rs` 根据可信 `ADX_JIUWEN_CONFIG` 显式启用模块：

```json
{
  "ingestion": {
    "database_url_env": "ADX_INGESTION_DATABASE_URL",
    "database_max_connections": 8,
    "database_allow_plaintext": false,
    "workers": 2,
    "lease_seconds": 30,
    "delivery_timeout_seconds": 60,
    "max_pending_events": 10000,
    "max_pending_bytes": 67108864,
    "per_subscription_events": 1000,
    "debounce_milliseconds": 1000,
    "dedupe_retention_seconds": 604800
  }
}
```

同一配置还需已有 tenant/jiuwen/digitalmate/privacy 配置，且 privacy 与
digitalmate 指向同一模板。数据库默认验证 TLS；自有 CA 用绝对路径
`database_ca_file`。明文仅可由可信操作者显式开启，不作 TLS 失败回退。
账号、预算、摄取各有有界连接池/超时和 schema；schema owner 预先应用
[账号 SQL](../../gateway/src/ingress/accounts/schema.sql)、
[预算 SQL](../../gateway/src/ingress/digitalmategateway/composio/budget.sql)、
[摄取 SQL](../../gateway/src/ingress/digitalmategateway/ingestion/schema.sql)，
运行角色仅 DML，启动不执行 DDL。

项目 Key 与 Webhook secret 独立存入 `adx_accounts.service_secrets`，沿用
现有根密钥和 AEAD，服务级独立 AAD、CAS revision 与 tombstone。用户配置
API 无法读取它们，也不下发到模板、沙箱或端侧。

两个私有输入文件分别为 `{"project_api_key":"..."}` 和
`{"webhook_secrets":["..."]}`。Linux 上必须是绝对路径普通文件，权限不宽于
0600，拒绝链接。项目导入兼容的可选测试元数据不配置 auth config；provider
配置在 [Composio 配置](composio-gmail-trusted.md) 中显式指定。

```sh
adx-ingress import-composio-key /run/private/composio.local.json new
adx-ingress import-composio-webhook-secret /run/private/webhook.local.json new
adx-ingress
```

命令由 DigitalMate `server_config::run_cli` 处理，公共服务 main 仅转发。
沿用 `ADX_JIUWEN_CONFIG`、`ADX_ACCOUNT_CONFIG`、`ADX_CREDENTIAL_KEY_FILE`；
不启动监听、不访问外部 Composio、不创建新根，只输出 revision。轮换用当前
revision 替代 `new`；Webhook 最多保留两把签名 secret，不复用项目 Key。

## 生命周期与验证

`background_service` 返回模块拥有的后台任务。`IngressService` 只提供统一
shutdown 并 spawn；worker/维护任务通过 JoinSet 完成信号监督，无每秒健康轮询。
任一任务意外退出会报告失败；正常停止通知并等待任务退出，Drop 取消遗留任务。
缺失必需密文/schema/身份 owner 时启用模块必须启动失败，不能降级匿名/内存队列。

普通回归：`cargo test -p data-plane-gateway --features agent-api,mock-e2e --lib`。
外部数据库用例要求显式的 `ADX_INGESTION_TEST_DATABASE_URL`，数据库名以
`dm_ingestion_test_` 开头，按用例新建数据库并串行执行。启动用例在同一进程
调用真实 bind/serve；HTTPS 用例使用仓库内客户端。Relay 的重启回执用例使用仓库内标准库 HTTP/SQLite 替身；
不依赖其他仓库源码，也不证明真实 DigitalMate 接收器。跨仓端到端属于部署联验。
真实 OAuth、邮箱数据、ADX 集群/SFS 恢复及最终 Signal 回调各自单独验收。
