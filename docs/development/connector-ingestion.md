# Gateway 后台摄取协调

摄取协调和 Composio 可信适配是 ADX #268 的两个职责模块，均属于既有 DigitalMateGateway 进程，部署在用户沙箱外。#179 配置中心仍为独立 PR，DigitalMate #1192 承载另一仓库的沙箱消费者；不新增服务或第二套账户注册表。

代码基于官方 master `f2e2c0c735f23ac16fe86a5140f1eab03602ed6b`。没有复制或修改 #179 凭证私有实现，也不修改协调会话维护的总方案。

## 已实现的持久接收和调度

`composio/webhook.rs` 使用可信部署提供的原始 UTF-8 secret，对未经改写的请求字节执行 HMAC-SHA256 验证。最多两个 secret 支持轮换；必要 header 禁止重复，时间窗口前后 300 秒，原始正文最多 1 MiB。只接受 V3 trigger message envelope，其他事件不会被成功回执后丢弃。

固定入口为 `POST /api/digitalmate/ingestion/v1/composio/webhook`。`WebhookHttp` 通过实际 Ingress dispatcher 挂载，只有 PostgreSQL 事务提交后才返回 200。未验签为 401，订阅不匹配为 403，同 ID 不同 digest 为 409；容量不足、数据库故障和提交无法确认返回 503 与 `Retry-After: 5`，错误不含邮件正文。入口需要显式装配，现有启动配置不会自动开启。

`ingestion/schema.sql` 定义专属 PostgreSQL schema，`Inbox` 是唯一写入者，不读取 account/privacy/公共连接表或其他 owner 的私有池。调用方显式提供池、待处理总数/字节数、单订阅容量和 0–30 秒聚合窗口。事件须匹配当前订阅的 tenant/user/workspace、connection、账户、auth config、trigger 和全部代次。供应商签名中的 user_id 不能自行选择工作区。

唯一键为 `(subscription_id, generation, event_id)`。精确重复保留原回执，同键不同摘要拒绝；不同事件 ID 即使邮件文字相同也不合并。容量锁与事件插入处于同一事务。沙箱持久受理或订阅撤销后清除原始正文，仅保留摘要和状态。终态摘要/回执按显式配置的 1–30 天判重保留期有界清理，每轮最多 1000 个事件和批次，默认启动轮询每 60 秒。待处理和在途输入永不因年龄过期；精确判重保证限于配置保留期。测试采用7天。

每个工作区最多一个在途批次。通过 `FOR UPDATE SKIP LOCKED`、到期租约和上次尝试时间领取；每批最多 50 个事件、1 MiB 原始字节。完整成员集合及 digest 原子落盘，重试恢复同一批次。epoch 拦截旧 worker，心跳续租、退避及有界 1–16 个 worker 由 `IngestionRuntime` 管理。退出后租约恢复工作，临时数据库故障不永久停止 worker。这是工作区公平领取，尚非 Composio 项目级配额实现。

ACK 只提交被领取的准确成员，不推进 sequence 高水位，A/B 处理期间到达的 C 不会被旧回执删除。旧订阅代次不能覆盖新代次；撤销围栏本订阅租约并清除待处理正文，不取消同工作区其他连接的批次。

## 沙箱就绪和交付

`AgentV2Access.select_existing` 消费已发布 `ManagedService.retry_resolve` 的 expected-generation 契约。缺失、删除或重建后的 Binding 不会被创建或换选；只接受模板声明的 HTTP port。Environment 就绪仍由 ADX Activator/Platform owner 处理，摄取不实现生命周期。端口暂未监听时等待，发送 HTTP 字节前再次读真实 Binding，核对 generation、phase 和 environment ID。

`RelayDelivery` 经现有 Ingress/Relay stream 访问固定沙箱路径：

- `GET /api/digitalmate/ingestion/v1/batches/{batch_id}`：先查丢失的持久回执。
- `POST /api/digitalmate/ingestion/v1/batches`：状态不存在时交付固定成员集合。

不接受任意 URL，不直连节点绕过 Relay。连接、响应大小和总时间有界。超时/传输失败保留 unknown 并重试原批次；仅匹配 version/batch ID/digest/accepted 的回执才能移出 Gateway 队列。`DeliveryAuthorization` 必须由真实用途授权 owner 提供，无匿名或通用 bearer 默认实现。

#1192 的 `BatchInbox` 使用持久工作区内独立 SQLite，WAL/FULL、事务提交后回 accepted；GET 可在独立进程重启后恢复回执。它复核 manifest、原始事件摘要和供应商元数据，并在提交前后读取真实连接代次与 `read_history` 能力。无能力的已登录身份不能受理。容量上限拒绝新输入，精确重试仍返回原回执。accepted 仅表示持久交接，不表示邮件分类、Signal 或报告完成。

## 真实接口与未完成接线

公共底座 #1176 最新固定候选为 `4816f0ccdcc6a1c441e0a92503ef25c99fb79c4e`，包含 eb9e94c0 公共出口与客户端时钟边界；Registry/Scan/Store/server 运行时代码与已验证79a20197相同。#1192 薄适配直接调用 ScanRegistrations 的注册/完成方法及 `ConnectionRegistry.ensure_ready/access_snapshot`，不复制私表。快照不是后台订阅授权，`read_history` 不代替明确启用意图和 trigger 能力。

#179 已发布 `b307dabae73f357463ea1bf1d98f6dacc171399f`，现有身份用途是配置中心，尚无 `ingestion.delivery` 用途、订阅投影或任意项目 Key 分发端口。本模块不将配置 bearer 当作后台工具授权。

以下生产接线仍未实现，PR 保持 Draft：

- 用户明确启用/撤销的持久订阅 owner，以及现有 Composio 适配器内受限 trigger 操作和未知结果恢复。`Inbox.register` 只接受已经过真实 owner 授权的投影。
- 与 #179 对齐的后台用途身份和首次后台启动时的受限配置 bootstrap。现有沙箱在 WS trusted admission 前不启动 privacy，不能放行所有 internal 路由规避。
- Gateway 进程启动配置中构造独立池、授权、入口与 worker；#1192 `server.py` 的 receiver mount/admission。公开 builder 不代表已经装配。
- Composio 共享预算、真实 trigger/OAuth/暂停沙箱及集群验收。

协调不新建 SDK/client，不持项目 Key；后台补读和 trigger 管理必须通过原有可信适配器受限端口。项目 Key 留在可信适配器，provider Token 留在 Composio。

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
