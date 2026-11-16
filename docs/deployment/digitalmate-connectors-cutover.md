# DigitalMate 连接器配套部署与数据库切换

本文说明配置中心、Composio 可信代理及后台摄取的部署边界。执行切换须先明确目标
环境和获准停写范围；文档和 PR 更新本身不执行部署。邮件解释、Signal 与报告仍在
用户沙箱，Gateway 只提供可信代理、持久摄取和既有 ADX 就绪交付。

## 身份与配置中心

正常沙箱调用复用原有 `dm-privacy-v1` 工作负载凭证。DigitalMate WS 经现有
`PrivacyIdentity::admit` 在 `Authorization` 下发一张凭证，不再签发第二张普通
connector 凭证或注入 `x-digitalmate-connector-identity`。客户端提交的内部身份头
仍先剥离。同一凭证用于私密配置与可信连接器的调用身份，续期只走既有
`/v1/identity/renew`（精确返回 token/expires_at）。`/v1/connector/identity` 仅投影
并校验同一 grant 的作用域与期限，不签发第二种身份。连接授权、正文读取和后台
意图仍须由公共连接底座/M44 校验；身份有效不等于允许读邮件。

后台批次仍使用 `dm-ingestion-v1`，交换出的 `dm-connector-v1` 仅为受交付截止约束
的短期 bootstrap，不能续期、读取配置秘密或进入正常恢复载体。旧的可续期普通
`dm-connector-v1` 不再接受，`/v1/connector/renew` 删除，无兼容别名。

Composio 项目 Key 和 Webhook 签名 secret 是另两种服务端秘密，保存在配置中心的
`service_secrets` 密文表，不能下发为沙箱身份。供应商用户 OAuth Token 由 Composio
托管。[配置中心部署](digitalmate-privacy.md)与[可信代理契约](../development/composio-gmail-trusted.md)
说明各自的身份、续期、权限和配置字段。

## 数据库版本与操作所有者

| 状态域 | 新库输入 | 已有库处理 |
| --- | --- | --- |
| 账号及配置中心 | [accounts/schema.sql](../../gateway/src/ingress/accounts/schema.sql) | 账号 `schema_version` 保持1；本 PR 仅增 `service_secrets`。先检查实际表形状；`CREATE TABLE IF NOT EXISTS` 不修复不兼容旧表，不改根密钥或既有用户 grant |
| Composio 共享预算 | [composio/budget.sql](../../gateway/src/ingress/digitalmategateway/composio/budget.sql) | 独立预算 schema 版本1；已有相同表不重建，不与摄取版本混用 |
| Gateway 摄取 | [ingestion/schema.sql](../../gateway/src/ingress/digitalmategateway/ingestion/schema.sql) | 新库直接 schema2；已有 schema1 停写且 settled 后由 schema owner 执行 [upgrade_screening_v2.sql](../../gateway/src/ingress/digitalmategateway/ingestion/upgrade_screening_v2.sql)，不能用新建库 SQL 代替升级 |
| 订阅投影与 batch/ACK | wire v2 | [ingestion_scope_v2.py](../../build/ops/ingestion_scope_v2.py) 仅切换已 settled 的旧订阅 JSON，保留物理 schema1/2，不是上行的 schema 升级 SQL |
| 沙箱接收及交接 | SQLite schema2 | 遵从配套消费端的预检/切换契约，不将 PostgreSQL SQL 用于 SQLite，不重写旧批次摘要或丢弃未知结果 |

保留旧库切换工具的范围是已有专用联调部署及其需要保留的授权/终态记录，不是为
尚未上线的系统承诺长期多版本兼容。已有联调 schema1 的部署记录不证明某次执行时
在线库仍为1；操作者必须重新检查目标库。新环境不执行旧投影或 schema1 升级。

版本核对使用 schema-owner 连接，只查询目标版本表：

```sql
SELECT version FROM adx_accounts.schema_version;
SELECT version FROM digitalmate_composio_budget.version;
SELECT version FROM digitalmate_ingestion.version;
```

DDL 由 schema owner 提前执行；Gateway 的账号/预算/摄取连接仅持所需 DML 权限，
启动不建表、不升级、不在 schema 缺失时降级为内存。备份数据库和现有根密钥，
不要输出 DSN、密钥或邮件。旧库表形状/版本不符则停止，不自动重建。

## 已有部署的切换顺序

1. 记录实际 Gateway、消费端、模板/Alias 与精确实例版本、三个数据库版本、项目
   secret revision 和旧批次状态。备份与回滚范围须覆盖原授权、待处理及终态记录。
2. 准备兼容 `screening_contract:1` 的公共底座/M44 owner 与本次单凭证消费端，
   在下述停写窗口配对更新 Gateway 和实际用户实例，不假定新旧身份协议互通。
   仅更新 Alias 不代表既有实例已经更新。不从模板参数或旧用户记录编造身份。
3. 将专用 Webhook/订阅写入口置为临时503并带 Retry-After，停止全部旧摄取调度、
   订阅同步与相关 writer，核对没有在途/未知或未终态工作。仅关闭公网 Webhook 不够。
   若有未完成工作，先在旧组合内恢复处理，不能清库、重写摘要或假报终态绕过检查。
4. schema owner 检查并补齐账号 `service_secrets` 和预算表；新 Gateway 可先启用
   privacy/Composio、禁用 ingestion，供原有身份接口与 operator 使用。保留已有根密钥、
   项目 Key 和 Webhook secret；首次导入用 `new`，轮换先读 revision 后 CAS，不盲目覆盖。
5. 对需要保留的旧订阅，按[交付契约的显式切换步骤](../development/ingestion-delivery-contract.md#显式切换与恢复)
   先执行 operator 锁定 dry-run，再在同等停写条件下使用 `--apply`。它通过现有受信
   HTTPS 身份获取原 grant 引用；不能发新 grant、续期旧操作或改绑工作区。
6. 仅当物理摄取库仍为1、旧 wire 切换已完成且全部工作 settled 时，应用 schema-owner
   升级 SQL。已有2不重复执行该 SQL。SQL 在事务锁内再次拒绝非 settled/未知状态，
   保留终态身份，增加默认空筛选策略；运行进程不承担升级。
7. 核对消费端/receiver 预检与 wire-v2 配置；旧普通 connector 载体不能恢复时，
   通过正常登录 WS 取得 privacy 工作负载凭证，完成共同续期和精确身份校验后才
   原子替换私有载体。保留原 grant、邮箱 OAuth 与连接/订阅事实，不重建授权。
   两端就绪后启用新版摄取，再恢复入口及同步 writer。缺失 policy/revision 协商
   时应拒绝，不能静默丢掉用户规则。本次身份收敛不改变 SQL 或数据库版本。
8. 在获准环境用实际测试账户验正常握手、连接授权、订阅、真实邮件、关闭/休眠后
   既有 ADX 唤醒、原持久卷、receiver 持久 ACK；最终下游匹配 HTTP202 另验。
   进程就绪、原生 CI 或受控测试均不代替这条验收。

## 部署需要改什么

- Gateway：同一 DigitalMateGateway 进程装配 `privacy`、`composio`、`ingestion`，
  匹配同一 DigitalMate 模板；配置数据库 TLS/CA、有界连接池/预算、认证配置及加密服务
  secret。完整字段及私有导入命令见[摄取配置](../development/connector-ingestion.md#配置与密钥)。
- 入口：仅开放既有业务登录/WS、固定 Composio 路由与精确签名 Webhook；私有身份和
  加解密接口保留受限访问、可信 TLS 与窄网络规则，不因新增凭证公开到互联网。
- 用户实例：发布兼容 owner/receiver 的配套版本，保留正常登录握手、可信身份续期和
  实际持久卷；项目 Key、根密钥与 Gateway TLS 私钥仍不进入模板、用户卷或镜像。
- ADX Worker：沿用部署者已有的受信启动配置与普通业务用户降权。邮件不要求修改
  Execd、启动器 UID/GID/capabilities 或平台生命周期；已有正确配置不因本功能重做。

回滚也要停写并核对是否已经产生 v2 工作，不能直接换旧镜像读取新投影或覆盖新状态。
禁止通过切回旧数据库快照丢失已提交副作用；未知结果先按原批次/操作身份查询。
