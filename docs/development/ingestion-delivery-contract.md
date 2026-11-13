# 后台交付用途与冷启动候选契约

本文固定 #268/#1192 端口。#268 已在既有身份 owner 中准备生产者与进程装配代码，
#1192 的冷启动及公共底座消费仍待补齐。它不是已上线的部署说明。#179 阶段 A 仅实现配置中心用途，
不能复用其 privacy bearer 作为后台批次授权，也不能复制私有 signer 另建 grant 系统。

## 阶段 B 的开发分工（2026-11-12，提议待独立评审）

#179 owner 已明确其已合入的阶段 A 保持配置 get/put/renew 与握手身份的既有范围。
用户要求的新增用途、服务级 Composio 配置和 cold receiver 在 #268/#1192 原 PR
范围补齐，不修改香港阶段 A 固定组合。本批实现准备不等于设计已接受或已部署。

- Gateway 配置中心继续使用既有根密钥和 AEAD owner。服务级密文采用独立
  `digitalmate_service_secret` AAD 与 `adx_accounts.service_secrets` owner 表，
  固定 Composio project/webhook 名称、CAS revision 和删除 tombstone。
  不通过用户 privacy API 暴露，不在模板或沙箱内放 project Key。
- 新 `connector.tool`、`ingestion.delivery` 用途在既有身份框架内扩展；工具身份
  不授予连接读权限，批次身份固定目标、batch ID/digest/实际 wire size 和短 TTL。
  必须验证真实当前账号、Binding、连接及明确订阅意图，不能根据 DTO 自行签发。
- 冷启动能力由 Runtime/配置 owner 单独提议与发布，只能提供用途限定、无秘密的
  verifier 配置/Binding 投影及版本/有效期。delivery 身份不能读取整份
  `DIGITALMATE_CONFIG`，包括用户模型密钥；readonly 不能替代保密性约束。
- Scan 保持唯一授权业务 flow owner，Registry 保持唯一连接写入者。Gateway
  物理外呼效果日志只能在公开 Scan 当前投影约束下防止重复外呼，不能成为第二业务主表。
- 正式装配仍在同一 `IngressService` / `Bridge.create_app`，沿用已有 Relay 和
  会话入口。端侧授权入口、状态与回跳按用户要求等飞书 #1142 合入后复用，不重复实现。
  飞书当前 ArkWeb WebView 不直接承载 Google OAuth；Google 登录使用现有系统浏览器
  通道或系统授权会话，遵从 [Google OAuth policy](https://developers.google.com/identity/protocols/oauth2/policies)。

精确 issuer/introspection 与 Scan/Registry 新端口尚待协调 owner 发布及独立评审。
原有 privacy 凭证在上述提议接受前后都不能被解释为邮件读取或交付许可。

### 已接入既有进程的身份生产者源码（候选，未部署）

既有 `TokenSigner` 增加固定 `dm-connector-v1` / `dm-ingestion-v1` 前缀；
原 `dm-privacy-v1` 的格式和范围保留。受信业务握手同一次既有身份 admission
产生两种分离用途的凭证，不新增 grant 表。`connector.tool` 证明当前工作负载身份，
不授予连接读取或会话执行权限；调用方仍必须查询实际 Registry/Session owner。

`DeliveryIdentity` 实现实际 `DeliveryAuthorization`。签发与校验都查询 Inbox
当前持久 ready batch、enabled subscription、代次、完整 binding、digest 和真实
wire size，并查询既有有效账号 admission 与当前 Binding。短期签名携带既有
grant ID、tenant、batch ID/digest/size/到期；完整接收目标从该 batch 的持久内容
和当前 owner 记录获得，不能由请求 DTO 构造。TTL 为 60 秒，签发时钟上界容忍
沿用身份框架的 60 秒，下界不放宽。终态或撤销后的批次不再通过这一校验。

配置中心同一 HTTPS listener 的新增候选路由全部要求 POST、JSON、精确 `{}`：

| 路由 | 凭证与结果 |
| --- | --- |
| `/v1/connector/identity` | connector 凭证；查询当前账号与 Binding，返回身份投影和到期，不返回工具权限。 |
| `/v1/connector/renew` | connector 凭证；重验当前目标并仅续期 connector 凭证。 |
| `/v1/ingestion/admit` | delivery 凭证；返回接收目标、batch ID/digest/size/到期。 |
| `/v1/ingestion/bootstrap` | 当前验证 delivery 后明确返回503 `AUTH_UNAVAILABLE`（既有身份 owner 的错误码）。不解密用户配置、不返回 value/token；等待 Runtime/配置 owner 的无秘密窄合同。 |

delivery 路由必须由真实 Inbox 显式装配，没有匿名或退回 privacy 凭证的默认路径。
批次交付凭证不作为后续 pipeline 的会话执行、背景读取或发布权限；这些权限必须
由相应真实 owner 单独核验。订阅写入入口与明确用户意图、公共底座 Session/Registry
适配与冷 receiver 装配仍在开发。Gateway 正式入口现通过 `IngressService.bind/serve`
装配受限 issuer、Webhook 和 Relay worker，随进程监督与停止。上述源码的 PG/HTTP 边界回归使用受控
Binding 和订阅输入，不是 ADX 部署或真实 Gmail 证明。

## 生产者与消费者端口

Gateway 端 `ingestion/relay_delivery.rs`：

```rust
#[async_trait]
pub trait DeliveryAuthorization: Send + Sync {
    async fn headers(&self, batch: &DeliveryBatch)
        -> Result<http::HeaderMap, DeliveryFailure>;
}
```

`DeliveryBatch` 是 `ingestion/delivery.rs` 的 v1 严格结构，包含固定 batch ID/digest、
SubscriptionBinding 与完整事件集合。SubscriptionBinding 包含 tenant/user/workspace、
connection ID/authorization generation、subscription ID/generation、完整模板 Scope、
Binding generation、service port、Composio account/auth config/user 与 trigger。
这些值本身不是授权证明。实际 issuer 必须读取当前业务 owner、明确订阅意图和真实
Binding，而不能从 `enabled: true`、客户端 epoch 或历史快照产生权限。

沙箱端 `composio/ingestion.py`：

```python
RequestVerifier = Callable[[web.Request], Awaitable[DeliveryAdmission]]

@dataclass(frozen=True)
class DeliveryAdmission:
    identity: ReceiverIdentity
    batch_id: str
    digest: str
    wire_bytes: int
    expires_at: int
```

ReceiverIdentity 的字段为 tenant、user_id、workspace、binding_id、binding_generation、
template、template_ref、service_port，purpose 必须为 `ingestion.delivery`。
只允许实际身份 owner 在完成验签和当前授权检查后构造 DeliveryAdmission，禁止从批次
JSON 或调用方自报 header 直接构造。集成 fixture 的 `x-test-*` header 只是受控测试端口，
生产代码没有该信任路径。

POST 的 wire_bytes 是 Gateway 实际 `serde_json::to_vec(batch)` 的字节数，范围 1–2 MiB；
接收端与实际原始请求长度精确比较。批次原始事件总量仍最多 1 MiB。
GET 绑定同一 batch ID/digest，不可用一批的凭证查询另一批。验签 owner 确定 TTL 的
签发/时钟容忍上限；接收端在读 body 前和回 ACK 前再次拒绝已到期证明。测试采用 60 秒，
不代表已发布的生产 TTL。错误只返回受限原因码，不记录 bearer、邮件和上游错误正文。

## 冷启动顺序

1. worker 从真实 Inbox 领取固定批次，issuer 检查当前账户、明确订阅授权和目标 Scope。
   ADX 仅对已有 Binding 的指定 generation 请求就绪，不创建新 Binding。
2. issuer 生成限定该 generation、port、batch ID/digest/size/TTL 的交付证明。
   共用既有身份 owner 和信任根；不下发 root、Composio 项目 Key 或 provider Token。
3. Gateway 经现有 Ingress/Relay 连接固定接收路由。GET/POST 都需要该证明。
4. `server.py` 的 admission 仅允许这两个精确路由进入实际 RequestVerifier。
   verifier 先验真实用途及当前目标。Runtime 的无秘密 bootstrap 消费尚未发布，
   不能把用户配置或 privacy bearer 交给 delivery consumer，也不开放其他 internal 路由。
5. Runtime 正式受限装配完成后由真实 Registry.ensure_ready/access_snapshot 提供当前连接事实，
   接收端核对 connection authorization generation、账户及 read_history；订阅授权仍由
   订阅 owner/issuer 单独证明。SQLite 提交后重查当前连接和实际用途 owner，再回 accepted。
   提交后撤销返回403时，持久去重回执仍存在，不能理解成回滚；重试、状态查询、
   重启都必须重新验用途。staging 回执不授予执行或报告发布权限。
6. Gateway 只提交准确 ACK 对应的成员。回执丢失时先 GET 同一 batch，再决定是否 POST。

生产者还必须明确当前 Binding/订阅/连接撤销与在途请求的界限。取消无法撤回已经发生的
远端只读 I/O；过期/旧代次不得启动新业务处理或交付迟到内容。受理仅代表持久 staging，
后续 pipeline 在执行前仍需向真实 owner 重验。

## 当前限制

#268 已有 Gateway HTTP 入口、真实 PostgreSQL 队列和 Relay 客户端；#1192 已有独立
SQLite receiver 与公共 Registry/Scan 适配。实际用途 producer、冷启动 verifier/装配、
明确订阅的启停和 Composio trigger 操作尚未实现。Gateway 侧用途生产者及正式服务
装配已实现并通过隔离 PG/HTTP 和实际 bind/serve 回归；消费者的受限冷启动和真实
连接/会话授权仍须补齐，尚不可据此宣称生产端到端可用。
生产者固定后应使用同版本真实身份/持久存储重新联验，再由指定唯一部署 owner 集成香港版本。
