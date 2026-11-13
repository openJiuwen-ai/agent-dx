# Gateway 后台摄取协调

摄取协调和 Composio 可信适配是 ADX #268 的两个职责模块，均属于既有 DigitalMateGateway 进程，设计运行于用户沙箱外。#179 已合入的配置中心作为前提，DigitalMate #1192 承载另一仓库的消费者；不新增服务或第二套账户注册表。本页描述候选源码，不代表已部署。

当前开发已接入官方 master `d86a2ee9b9dfbf08d0655197ae20e7bb3e1f41a3`（含 #179 镜像）。阶段 B 在既有身份和配置 owner 内扩展分离用途及服务级密文，沿用原信任根和授权记录；不复制 signer 或另建授权系统。新增契约仍须独立设计评审后合入。

## 已实现的持久接收和调度

`composio/webhook.rs` 使用可信部署提供的原始 UTF-8 secret，对未经改写的请求字节执行 HMAC-SHA256 验证。最多两个 secret 支持轮换；必要 header 禁止重复，时间窗口前后 300 秒，原始正文最多 1 MiB。只接受 V3 trigger message envelope，其他事件不会被成功回执后丢弃。

固定入口为 `POST /api/digitalmate/ingestion/v1/composio/webhook`。`WebhookHttp` 通过实际 Ingress dispatcher 挂载，只有 PostgreSQL 事务提交后才返回 200。未验签为 401，订阅不匹配为 403，同 ID 不同 digest 为 409；容量不足、数据库故障和提交无法确认返回 503 与 `Retry-After: 5`，错误不含邮件正文。可信 `ADX_JIUWEN_CONFIG` 显式提供 `ingestion` 才启用；缺少密文、数据库或身份依赖时拒绝启动，不静默降级。

`ingestion/schema.sql` 定义专属 PostgreSQL schema，`Inbox` 是唯一写入者，不读取 account/privacy/公共连接表或其他 owner 的私有池。调用方显式提供池、待处理总数/字节数、单订阅容量和 0–30 秒聚合窗口。事件须匹配当前订阅的 tenant/user/workspace、connection、账户、auth config、trigger 和全部代次。供应商签名中的 user_id 不能自行选择工作区。

唯一键为 `(subscription_id, generation, event_id)`。精确重复保留原回执，同键不同摘要拒绝；不同事件 ID 即使邮件文字相同也不合并。容量锁与事件插入处于同一事务。沙箱持久受理或订阅撤销后清除原始正文，仅保留摘要和状态。终态摘要/回执按显式配置的 1–30 天判重保留期有界清理，每轮最多 1000 个事件和批次，默认启动轮询每 60 秒。待处理和在途输入永不因年龄过期；精确判重保证限于配置保留期。测试采用7天。

每个工作区最多一个在途批次。通过 `FOR UPDATE SKIP LOCKED`、到期租约和上次尝试时间领取；每批最多 50 个事件、1 MiB 原始字节。完整成员集合及 digest 原子落盘，重试恢复同一批次。epoch 拦截旧 worker，心跳续租、退避及有界 1–16 个 worker 由 `IngestionRuntime` 管理。退出后租约恢复工作，临时数据库故障不永久停止 worker。这是工作区公平领取，尚非 Composio 项目级配额实现。

ACK 只提交被领取的准确成员，不推进 sequence 高水位，A/B 处理期间到达的 C 不会被旧回执删除。旧订阅代次不能覆盖新代次；撤销围栏本订阅租约并清除待处理正文，不取消同工作区其他连接的批次。

## 沙箱就绪和交付

在请求就绪前检查当前用途；就绪完成后，GET 和 POST 各自重新取得短期用途凭证。
不复用就绪等待前的凭证，也不复用 GET 的凭证来发 POST。

`AgentV2Access.select_existing` 消费已发布 `ManagedService.retry_resolve` 的 expected-generation 契约。缺失、删除或重建后的 Binding 不会被创建或换选；只接受模板声明的 HTTP port。Environment 就绪仍由 ADX Activator/Platform owner 处理，摄取不实现生命周期。端口暂未监听时等待，发送 HTTP 字节前再次读真实 Binding，核对 generation、phase 和 environment ID。

`RelayDelivery` 经现有 Ingress/Relay stream 访问固定沙箱路径：

- `GET /api/digitalmate/ingestion/v1/batches/{batch_id}`：先查丢失的持久回执。
- `POST /api/digitalmate/ingestion/v1/batches`：状态不存在时交付固定成员集合。

不接受任意 URL，不直连节点绕过 Relay。连接、响应大小和总时间有界。超时/传输失败保留 unknown 并重试原批次；仅匹配 version/batch ID/digest/accepted 的回执才能移出 Gateway 队列。`DeliveryAuthorization` 必须由真实用途授权 owner 提供，无匿名或通用 bearer 默认实现。

#1192 的 `BatchInbox` 使用持久工作区内独立 SQLite，WAL/FULL、事务提交后回 accepted；GET 可在独立进程重启后恢复回执。它复核 manifest、原始事件摘要和供应商元数据，并在提交前后读取真实连接代次与 `read_history` 能力。无能力的已登录身份不能受理。容量上限拒绝新输入，精确重试仍返回原回执。accepted 仅表示持久交接，不表示邮件分类、Signal 或报告完成。

## 真实接口与未完成接线

此前跨仓库验证固定过 #1176 的公开候选（包括 `f7b930ec`）；这不是新主线或新权限端口的验收。#1192 薄适配直接调用实际 ScanRegistrations 注册/完成方法及 `ConnectionRegistry.ensure_ready/access_snapshot`，不复制私表。完整 Gateway Authority 所需物理外呼认领/回执与 Scan 当前投影仍需 owner 对齐；现有三种薄消费不能宣称已覆盖整个 Authority。快照与 `read_history` 均不授予后台订阅意图。

#179 阶段 A 的配置 bearer 保持原用途。#268 阶段 B 在同一身份 owner 中增加分离的 connector/delivery 前缀及实际 issuer/introspection，不将配置 bearer 当作后台工具授权，不分发项目 Key 到用户工作区。delivery 不能解密整份用户配置。

既有身份 HTTPS 的 `POST /v1/ingestion/bootstrap` 只接受 delivery bearer 和精确 `{}`。它复核当前持久 batch、启用订阅、既有 grant 和完整 Binding，返回 `purpose: "ingestion.connector-bootstrap"`、实际 `target`、`connector_token`、`expires_at` 四个字段。target 仅含 tenant/user/workspace、template/ref、Binding ID/generation、Environment 与配置端口，必须匹配 batch 的 scope/generation/port。派生 connector 凭据复用当前 grant 和 signer，带签名 bootstrap 截止，不晚于当前 delivery 到期；禁止续期，不新增 grant，不返回 privacy bearer、用户配置或模型/项目密钥。缺失既有 owner runtime 仍返回503 `AUTH_UNAVAILABLE`。这只提供连接身份；真实启用意图、连接代次/修订与 `read_history` 仍须接收/读取端逐次核验，不代表任务或模型执行许可。

冷启交换必须发生在 delivery 被 accepted 并完成之前，只有仍为 ready 的持久 batch 可被核验。receipt 完成后不能用旧 delivery 再次交换；进程丢失或凭据过期后的长期后台读取恢复，不能仅凭本接口宣称已实现。

现有 `/v1/connector/identity` 及成功的 `/v1/connector/renew` 响应公开已验签凭据对应的 `renewable`、`recovery_expires_at` 和 `bootstrap_until`，并沿用真实 grant/current Binding 核验。普通 connector 返回 `renewable: true`、`bootstrap_until: null`；恢复截止是该凭据签名的原续期窗口，受原服务端续期/撤销策略控制，不代表永久许可。bootstrap connector 返回 `renewable: false`，两个截止均等于 access 到期，续期仍拒绝。消费者只能依据真实 HTTPS authority 响应核验当前 scope/代次及这些字段；本地文件或自行解 token 不能产生授权事实。

以下生产接线仍未实现，PR 保持 Draft：

- 用户明确启用/撤销的持久订阅 owner，以及现有 Composio 适配器内受限 trigger 操作和未知结果恢复。`Inbox.register` 只接受已经过真实 owner 授权的投影。
- #1192 真实 `server.py` 的冷 receiver mount/admission、用途限定 bootstrap 的消费者及实际连接源/Session 权限；不能放行所有 internal 路由规避。
- Gmail 授权/读取 HTTP 路由采用真实 Authority 和已实现的共享预算；独立组件通过不代表调用方已装配。
- 真实 trigger/OAuth、暂停实例唤醒、实际 Gmail 与集群验收。

Gateway 的独立池、真实用途 producer、回调入口和 worker 已接入 `IngressService.bind/serve`。
原配置没有 `ingestion` 时保持关闭；worker 异常退出使服务失败并开始排空，正常停机
停止 worker，不创建强引用环。正式进程测试覆盖缺密文拒绝、实际密文导入、验签
401/持久接收 200 与停止；供应商和用户订阅输入仍为测试材料。

协调不新建 SDK/client，不持项目 Key；后台补读和 trigger 管理必须通过原有可信适配器受限端口。项目 Key 留在可信适配器，provider Token 留在 Composio。

## 候选启动配置与服务密文

以下字段放在已有 `ADX_JIUWEN_CONFIG` 中；`privacy.template` 必须匹配
`digitalmate.template`。沿用既有 `ADX_ACCOUNT_CONFIG` 和
`ADX_CREDENTIAL_KEY_FILE`，不重新生成根密钥。生产者侧代码就绪不表示 #1192
冷接收端、实际订阅权限或外部 Composio 配置已经完成。

```json
{
  "tenant": "demo",
  "jiuwen": {"agent_type": "assistant", "template": "jiuwen", "version": "1"},
  "digitalmate": {"template": "digitalmate", "template_ref": "1", "backend_path": "/digitalmate/ws", "service_port": 8080},
  "privacy": {
    "listen": "0.0.0.0:9443", "template": "digitalmate",
    "tls_certificate": "/run/digitalmate-privacy/server.crt",
    "tls_private_key": "/run/digitalmate-privacy/server.key"
  },
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

数据库 URL 从可信进程环境中指定名称读取，不从工作区配置或请求读取。专用池
使用证书验证 TLS、5 秒连接/获取/回收上限与数据库语句/锁超时；自有 CA 可设置
绝对路径 `database_ca_file`。`database_allow_plaintext` 默认 false；显式 true
仅用于操作者明确选择的开发环境，不在 TLS 失败后回退。账号与摄取 schema
分别由 schema owner 预先应用：
[账号/配置](../../gateway/src/ingress/accounts/schema.sql)、
[摄取](../../gateway/src/ingress/digitalmategateway/ingestion/schema.sql)。运行角色仅需相应 DML，启动不执行 DDL。

服务级配置在 `adx_accounts.service_secrets` 中加密，使用既有 AEAD/root 和独立
service AAD，用户配置 API 无法访问。以下是两种独立、仓库外的私有导入文件格式：

```json
{"project_api_key":"replace-with-project-key","gmail_auth_config_id":"","test_account_email":""}
```

```json
{"webhook_secrets":["replace-with-webhook-signing-secret"]}
```

项目 Key 文件中的后两个字段是联验元数据，此命令只导入 Key；auth config 仍须在
尚未完成的 Gmail 路由装配中显式接入，此文件不会启用读取路由。验签 secret 最多两个，用于明确轮换窗口；不能
把项目 Key 当作验签 secret。Linux 文件必须是绝对路径、普通文件且权限不宽于
0600，拒绝符号链接。导入沿用上述三个现有配置环境变量，在同一可执行程序中
运行，不启动 listener、不创建新根、不访问外部 Composio：

```sh
adx-ingress import-composio-key /run/private/composio.local.json new
adx-ingress import-composio-webhook-secret /run/private/composio-webhook.local.json new
adx-ingress
```

导入仅输出新 revision；已有值或 tombstone 的轮换需用当前 revision 替换 `new`。
保留根密钥和配置库备份，不可通过换根文件来“修复”解密失败。Key 仅供 Gateway
读取，不写入模板、用户沙箱或端侧。缺少有效签名密文、必需 schema 或对应隐私
服务时，启用摄取的 Gateway 必须拒绝启动。原配置没有 `ingestion` 时不挂载入口
或启动 worker。

## 验证范围

`ingestion::persistence_tests` 要求测试数据库名以 `dm_ingestion_test_` 开头才会重建专属 schema，覆盖重启、并发领取、精确回执、C 与旧 A/B、撤销、容量、真实租约到期与 COMMIT 拒绝回滚。

`ingestion::relay_tests` 使用真实 public LocalControl/Activator/ManagedService、Ingress HTTP、Relay、PostgreSQL 和独立 Python HTTP/SQLite 进程。显式 `mock-e2e` 特性才可使用仓库既有 loopback 测试策略。Platform 执行和用途身份是受控外部端口，不能称作真实集群沙箱或 #179 身份联验。接收端 kill 后重启，重试先查询既有回执，不产生第二次持久输入。

```sh
ADX_INGESTION_TEST_DATABASE_URL=postgresql://.../dm_ingestion_test_... \
ADX_INGESTION_TEST_PYTHON=/path/to/task/venv/bin/python \
ADX_INGESTION_TEST_RECEIVER_SCRIPT=/path/to/DigitalMate/tests/server/ingestion_receiver_process.py \
cargo test -p data-plane-gateway --features agent-api,mock-e2e --lib ingestion:: -- --ignored --test-threads=1
```

Windows 全 Gateway 的 Unix 依赖限制仍存在，本批改用 WSL 完整真实 Gateway target 验证，不把直接源文件模块 runner 当成完整 target 证据。部署、真实账户与业务 pipeline 验收未执行。其余说明见 [Composio Gmail 指南](composio-gmail-trusted.md)。签名依据为 [Composio receiving events](https://docs.composio.dev/docs/setting-up-triggers/subscribing-to-events)。

生产者精确端口、字段和冷启动顺序见 [待实现的后台用途契约](ingestion-delivery-contract.md)。

### 固定版本验证回执（2026-11-12）

代码冻结点 ADX `3f3c46e19d5b28ac459cccb28e1ae92ed9f5b76c`、DigitalMate `638deccf46b6b0de18ece5f799c34eab042ac1c4`：

- 真实 Gateway lib target 摄取普通用例 16 passed，显式 PostgreSQL/跨仓库进程用例 2 passed；前后 Rust、schema、Python 产品/fixture SHA 一致。
- `cargo clippy -p data-plane-gateway --all-targets --all-features -- -D warnings` 和 `cargo clippy --workspace --all-targets --all-features -- -D warnings` 通过，后者 10m27s。不是此前独立 module runner 的结果。
- `git -c core.autocrlf=false archive` 导出的同一提交，995 个 Rust/Cargo/rustfmt 文件与 Git blob 逐个一致，`cargo fmt --all -- --check` 通过。Windows 工作树/默认 archive 的 CRLF 诊断原样保留；没有修改全局配置或其他源文件。
- 原 26 条 Composio 受控 HTTP 回归首次在该真实 Gateway binary 运行出现 24 passed/2 failed。保留原失败后，同 binary 做三轮配对：继承本机 proxy 环境时分别 23/26、22/26、21/26；仅测试子进程令 loopback 绕过代理时连续三轮 26/26。未改源码、生产 Client、目标限制或断言，不能把环境纠正描述为产品逻辑修复，也不把受控 upstream 称为真实 Composio 账户。
- 受控本机 HTTP 测试应仅在该命令进程设置 `NO_PROXY=127.0.0.1,localhost`（并对齐小写 `no_proxy`），避免用户的全局代理改写 loopback 流量；不修改部署代理策略。
- UTF-8 文档检查 195 篇/1364 链接/68 JSON 示例、diff 检查通过。GitHub 原生 CI、正式用途/订阅/启动装配和集群部署仍为独立门槛，未因本回执成为已完成项。

此证据段为文档增量，运行时输入与上述代码冻结提交一致。完整命令、前后哈希、初始失败和六轮对照日志保存在本任务的私有验证归档，不包含真实凭证/邮件。
