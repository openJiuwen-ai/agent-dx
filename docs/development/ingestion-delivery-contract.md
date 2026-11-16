# 后台交付身份与 Scope 访问契约

配置中心、Composio 和摄取运行于既有 Gateway；Registry、接收和处理在用户实例。
本契约描述候选源码，不代表新 wire 版本已在香港验收。

## 身份与业务引用

复用 #179 的 PrivacyIdentity、TokenSigner 和现有 privacy_grants。邮件不新增授权表。
authority_ref 是现有 grant 的随机 ID；不是 Binding generation 的改名、哈希或
序列化包装，也不是凭证。同一授权续期复用 ID；撤销或工作区重建后旧引用失效。
generation 只由身份 owner 在 grant 与 current Binding 之间校验，邮件业务不存取它。

私有 HTTPS 路由均接受 POST、JSON 和精确 {}：

| 路由 | 契约 |
| --- | --- |
| /v1/connector/identity | 校验同一 privacy 工作负载身份或受限 bootstrap，返回逻辑身份、grant_ref、期限和续期属性 |
| /v1/identity/renew | 仅续期普通 privacy 工作负载凭证；精确返回 token/expires_at，bootstrap 不可调用 |
| /v1/ingestion/admit | 校验持久批次、订阅及现有 grant，返回限定批次的 admission |
| /v1/ingestion/bootstrap | 将有效 delivery 交换为受其截止约束的短期 connector |

grant_ref 来自已验签 claims.id，不接受请求 JSON 指定。普通身份 renewable=true、
bootstrap_until=null；正常登录只下发一张 dm-privacy-v1，配置与连接器共用既有续期。
旧普通 dm-connector-v1 与 /v1/connector/renew 已删除，配对切换须按部署文档停写。
bootstrap 不创建授权、
不能取得 privacy bearer、用户配置、模型 Key 或项目 Key。

bootstrap target 仍是身份 owner 的 logical7：tenant/user/workspace、template/ref、
Binding ID/generation。只有私有身份客户端消费它，不再将 generation 投影给邮件
receiver、批次或 Signal scope。receiver 端口仅为可信启动装配，不是邮件身份。

## wire v2 与持久状态

DeliveryBatch.version=2，canonical manifest 域为 digitalmate.ingestion.batch.v2。
SubscriptionBinding 包含逻辑 Scope、用户/工作区、authority_ref、连接/订阅 ID
及其业务 revision、provider account/config/user/trigger；不含 Binding generation、
Environment ID 或 service port。连接授权和订阅意图的 generation 仍是业务围栏。

POST 不超过 2 MiB，原始事件总量不超过 1 MiB。固定成员、batch ID/digest 覆盖
实际字节；重试不另建批次。DurableAck.version=2，必须匹配原 ID/digest 和 accepted。
DeliveryAdmission 返回 Scope6、purpose=ingestion.delivery、authority_ref、batch
ID/digest、wire_bytes、期限；接收端同时匹配身份、引用和批次，不信 JSON 自报权限。

安全粗筛使用 PostgreSQL 物理 schema2；SQLite 接收/handoff 物理 schema 仍为2，
两者均不能与 wire 版本混淆。批次及 ACK 的 wire v2 字段不包含筛选策略。
旧 schema1 停写并确认所有工作 settled 后，由 schema owner 显式应用
[筛选升级 SQL](../../gateway/src/ingress/digitalmategateway/ingestion/upgrade_screening_v2.sql)，
步骤见[筛选契约](ingestion-subscription-state.md#structural-screening)。启动不执行迁移。
新业务只读写 v2 projection，没有 v1 新写入兼容别名。

## 普通公共访问

邮件 owner 和 Relay delivery 只传 Scope、配置的 service selector 给既有
AgentV2Access::select_with_initializer，再调用 connect_service_when_ready。
Binding 缺失时 initializer 返回错误；邮件不创建工作区或执行实例。

已有 Binding 沿用公共 initializer 的普通 resolve 路径和缓存行为；不存在时才调用
initializer。连接等待仅重试尚未监听的端口，不增加 Binding 轮询或成功后的额外复核。
路由或打开连接失败沿用共享层的原 deadline 和一次预写重试，保留既有 WS 换代重选
语义；没有 ExistingServiceAdmission、select_existing 或邮件专属连接模式。此选择
不代替身份 owner 对原 authority_ref、连接和用途的校验，不承诺两次公共调用间的
Binding 元数据读取与 resolve 是原子操作。

OwnerAuthorization 只是身份 owner 的原操作上下文。连接完成后的 refresh 保持
最初 actor、授权、操作和原始 body 摘要，只刷新允许范围内的短期证明，不按
Scope 重新挑新授权。重建后旧操作失败。M44 业务不比较 Binding generation 或
前后 admission。GET/POST 每次物理发送前获取新证明，已发送写请求不由 transport 重放。

接收 accepted 只代表 durable staging。读取、claim、处理、发布和 ACK 仍经实际
Registry/intent/身份 owner 校验。匹配报告 HTTP202 后不因下游报告失败重投 Signal。

## 显式切换与恢复

先停止所有旧 writer，并确认 Gateway、receiver、Signal 旧任务全部终态。ready、
租约、未知结果、未处理输入存在时拒绝迁移；先用旧栈恢复，不重写旧 batch 摘要、
清库、停车或重放以绕过检查。

[operator工具](../../build/ops/ingestion_scope_v2.py) 仅通过可信 HTTPS 的 normal
connector identity 获取现有 grant 引用，验证真实 logical7 和旧订阅一致，持有
Inbox 全表事务锁，再验身份，只更新当前订阅 JSON。默认锁定校验，提交需 --apply。
不读账户私表、不发新 grant、不改业务 revision；历史事件、ID/digest、现存回执
及其已有字节原样保留，不能复原旧服务原本已清理的 payload。

切换前专用公网代理先对 webhook 与订阅写入口返回临时503及 Retry-After；禁用
ingestion 的程序按未配置返回404，不能把它当成维护期可靠接收。

切换顺序：新版 Gateway 先禁用 ingestion 运行装配但保留 private identity；停旧
进程及订阅同步 writer；复查 settled；运行 operator；确认消费者只读预检和 v2
配置；最后启用摄取。仅关闭 webhook 入口不足以停止后台或订阅写入。消费者
初始化拒绝 legacy 未终态/unknown，终态历史只保留，不解析成新任务。

新 Gateway/消费者和私有配置成对部署。回滚先停写、检查是否已产生 v2 工作；
不能只换旧镜像覆盖新 projection 或已发生状态。OAuth、正常授权、连接记录和
source/Signal/report ID 均保留。真实邮箱、实例冷启及 SFS 故障恢复单独验收。
