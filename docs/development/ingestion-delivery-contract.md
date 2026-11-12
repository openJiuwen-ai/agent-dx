# 后台交付用途与冷启动接线契约（待生产者实现）

本文固定 #268/#1192 消费者的实际端口，供 #179 身份与装配 owner 实现生产者。
它不是已上线的 issuer、验签协议或部署说明。当前 #179 `b307daba` 仅实现配置中心用途，
不能复用其 privacy bearer 作为后台批次授权，也不能复制私有 signer 另建 grant 系统。

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
   verifier 先验真实用途及当前目标，再通过 #179 既有正式方法执行受限配置 bootstrap。
   未验用途前不能读配置、启动后台 producer、读取 Registry 或开放其他 internal 路由。
   privacy bearer 即使作为 bootstrap 能力传递，也只传给配置 owner，不能充当交付证明。
5. bootstrap 完成后由真实 Registry.ensure_ready/access_snapshot 提供当前连接事实，
   接收端核对 connection authorization generation、账户及 read_history；订阅授权仍由
   订阅 owner/issuer 单独证明。SQLite 提交后回 accepted，再查当前连接和证明有效期。
6. Gateway 只提交准确 ACK 对应的成员。回执丢失时先 GET 同一 batch，再决定是否 POST。

生产者还必须明确当前 Binding/订阅/连接撤销与在途请求的界限。取消无法撤回已经发生的
远端只读 I/O；过期/旧代次不得启动新业务处理或交付迟到内容。受理仅代表持久 staging，
后续 pipeline 在执行前仍需向真实 owner 重验。

## 当前限制

#268 已有 Gateway HTTP 入口、真实 PostgreSQL 队列和 Relay 客户端；#1192 已有独立
SQLite receiver 与公共 Registry/Scan 适配。实际用途 producer、冷启动 verifier/装配、
明确订阅的启停和 Composio trigger 操作尚未实现。公开注入点不代表可在生产配置中启用。
生产者固定后应使用同版本真实身份/持久存储重新联验，再由指定唯一部署 owner 集成香港版本。
