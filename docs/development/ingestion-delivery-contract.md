# 后台交付身份与冷启动契约

本契约描述 Gateway 已实现的身份生产者、固定目标交付及消费者要求，不代表
真实邮箱或任意新用户冷启已验收。配置中心、Composio 与摄取位于同一 Gateway；
Scan/Registry、接收及业务处理在用户实例。配置 bearer 不能替代工具或交付授权。

## 身份 owner 与私有 HTTPS

既有 `TokenSigner` 分离 `dm-privacy-v1`、`dm-connector-v1`、`dm-ingestion-v1`
用途，复用原 grant/current Binding，未新建授权主表。DigitalMate WS 的
`admit_workload` 同次入场下发 privacy 和 connector 身份；后者只证明工作负载，
不证明连接/任务/邮件权限。

以下路由在既有身份 HTTPS listener 上，只接受 POST、JSON、精确 `{}`：

| 路由 | 凭证与结果 |
| --- | --- |
| `/v1/connector/identity` | connector；重验账号/grant/Binding，返回身份和期限/续期属性 |
| `/v1/connector/renew` | 普通 connector；重验当前目标并续期同一用途 |
| `/v1/ingestion/admit` | delivery；返回当前目标、batch ID/digest/size/到期 |
| `/v1/ingestion/bootstrap` | delivery；复核实际批次/订阅/grant/Binding 后返回短期 connector bootstrap |

bootstrap 成功精确返回 `purpose: ingestion.connector-bootstrap`、`target`、
`connector_token`、`expires_at` 四项。target 含 tenant/user/workspace、
template/ref、Binding ID/generation、Environment、service_port；必须与持久
批次及实际 owner 相符。截止不晚于当前 delivery；不能续期、不创建 grant、
不返回 privacy bearer、用户配置或模型/项目 Key。未装配必需 owner 才返回
503 `AUTH_UNAVAILABLE`，不是所有 bootstrap 都返回503。

普通 connector 的已验证响应为 `renewable: true`、`bootstrap_until: null`，
`recovery_expires_at` 是原签名恢复期限。bootstrap 为 `renewable: false`，
恢复/签名截止均为其 access 截止。消费者以真实 HTTPS 结果核验；本地文件、
自行解码 Token 或 DTO 字段不能产生授权事实。

## 批次与接收合同

Gateway `DeliveryAuthorization::headers(batch)` 必须查询实际持久 ready batch、
启用订阅、代次、digest、真实 wire size、有效账号 grant 及当前完整 Binding。
不能根据 `enabled: true` 或调用方 epoch 签发。delivery TTL 为60秒，签名
时钟容忍沿用原 owner 的60秒上界；撤销、终态或旧代次不通过校验。

`DeliveryBatch` v1 包含固定 ID/digest、完整 SubscriptionBinding 与事件集。
SubscriptionBinding 带 tenant/user/workspace、模板 Scope、Binding generation、
端口、connection/subscription ID及代次、account/config/project user/trigger。
值本身不是授权。POST wire size 是实际序列化字节数（最多2 MiB），原始事件
总量最多1 MiB；接收端精确比较，不相信自报 header。

消费者的 `DeliveryAdmission` 含真实 `ReceiverIdentity`、batch_id、digest、
wire_bytes、expires_at。身份投影包含 tenant/user/workspace、Binding ID及代次、
template/ref/service_port，purpose 必须为 `ingestion.delivery`。只允许身份
owner 经验签和当前检查后构造；生产路径不信任 fixture 的 `x-test-*`。

## 就绪、提交、重试

1. worker 领取持久批次并检查当前用途；仅选择已有 Binding 的原 generation。
2. DigitalMate 内部 adapter 通过正式 ManagedService 请求就绪、共享 Ingress/Relay
   建连；调用方显式复核目标。原始 deadline 限定所有预写等待，不重放已发送请求。
3. 就绪后为每次物理 GET/POST 重新获取短期用途证明。GET 限定同一 batch/digest；
   无已持久回执才 POST，回执丢失重查原批次。
4. 接收端仅为精确批次路由执行 verifier，在读 body 前和回 ACK 前检查用途/期限；
   还需真实 Registry/intent 的连接代次、账户和 `read_history` 校验。
5. 接收持久提交后重查权限，返回准确 durable ACK。提交后撤销可能返回403，但
   不表示已提交数据被回滚；重试及状态查询必须再次授权和判重。
6. Gateway 仅确认匹配 ACK 的批次成员。此受理只代表 staging，后续处理、读取、
   Signal 发布仍由各自 owner 重验，不由 delivery 身份授权。

取消不能撤回已发生的远端读取，但旧代次不得启动新处理或释放迟到结果。临时
bootstrap 不能代替长期 normal 身份、用户后台意图或首次可信运行配置投影。
任意新用户在启动前的可信 target 配置及消费者挂载由部署方单独验证，不得从
历史用户复制身份或把配置中心整份解密交给 delivery consumer。
