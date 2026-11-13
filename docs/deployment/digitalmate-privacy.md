# DigitalMate Gateway 隐私配置中心部署

状态：可选功能的实现候选。本文描述代码和部署约束，不代表已完成真实 ADX 快照、克隆隔离或香港环境验收。

2026-11-13 北京四 `adx-jiuwen-test` 已完成 2 CPU / 2 GiB 的有界端到端验证：无真实模型 Key 时 DigitalMate 就绪，Gateway 注册模型配置，DigitalMate 经配置中心 HTTPS 获取后原生重载，真实模型返回 `pong`；容器及四个应用进程 PID 不变，重连保留历史且配置 revision 不变。使用独立合成业务会话，不能代表 Huawei/AGC 登录或故障注入验收。

本次 Gateway 镜像为 `swr.cn-north-4.myhuaweicloud.com/openjiuwen-online-test/adx-jiuwen-control:model-config-20261113-76e60175f566-labels1`，digest `sha256:7858924b15204f450f1ed71f490b2aced6c87d3437f4944dd32eb7478ea8159b`。配套 DigitalMate 镜像 `digitalmate-bot:model-config-20261113-7230811d6de5`，digest `sha256:8877891a822b785f00a4b64d956aa80eef5998fcda8f8fbf2ee90aeb5d9986b0`，使用独立模板 `digitalmate-model-config-test:3`。配套仓 `docs/verification/model-configuration-beijing-2cpu-2026-11-13.md` 记录完整边界和数字。

性能验收尚未通过：首次真实模型配置激活的 Agent prepare 为 3350.4 ms；追加五次占位配置重载的 Agent prepare 为 3368.5–3645.7 ms，完整重载 p50 3747.9 ms，均未达到 1 秒目标。没有足够证据将瓶颈归因于 CPU 限流、runsc 或 SFS；这不是实际 snapshot 加速效果的证明。

## 职责与边界

配置中心运行在 DigitalMateGateway 进程内，在账号数据库保存经过认证加密的第三方凭证。
未配置 `privacy` 时，保留现有业务入口。沙箱激活复用 `AgentV2Access`；不修改 Activator、
Platform、API Server、共享连接池或生命周期逻辑。

根密钥复用 `ADX_CREDENTIAL_KEY_FILE`。AES-256-GCM 的 AAD 绑定租户、工作区、用户、配置名和密钥标识，
与模型凭证的 AAD 分离；业务身份签名通过独立用途标签派生 HMAC 密钥。不要求额外部署一份加密根密钥。
Gateway 进程仍能接触根密钥和明文，这属于应用层加密，不提供 KMS 进程隔离。

## Gateway 配置与数据库

在 `ADX_JIUWEN_CONFIG` 的既有应用配置旁增加：

```json
{
  "privacy": {
    "listen": "0.0.0.0:18444",
    "template": "orbit",
    "tls_certificate": "/run/digitalmate-privacy/server.crt",
    "tls_private_key": "/run/digitalmate-privacy/server.key",
    "legacy_keyring_file": "/run/digitalmate-privacy/legacy-keyring.json"
  }
}
```

`template` 必须匹配已配置的 DigitalMate 业务模板。地址和路径为示例，服务证书必须匹配沙箱实际访问的主机名。
新环境不需要 `legacy_keyring_file`；已有密文迁移时，该文件必须是旧加解密服务的原始 keyring，不能重新生成代替。
旧 keyring 应以受保护的普通文件提供，权限不宽于 `0600`；不直接使用投射为符号链接的 Secret 路径。

启动前由数据库 schema 所有者应用 [账号库 schema](../../gateway/src/ingress/accounts/schema.sql)，
运行账号仅授予必要的 DML 权限。新增配置、事务回执和业务授权表。未发布的共享目录原型曾使用不同的
`privacy_grants` 结构，不能假定 `CREATE TABLE IF NOT EXISTS` 会迁移不兼容表结构。
部署前先备份数据库和现有根密钥；替换根密钥需要协调密文重加密与业务重新授权，不能直接换文件。

## 身份下发、续期与撤销

1. 标准激活完成后，Gateway 从已登录的业务主体推导 scope 和工作区，剥离客户端提交的身份头，
   在 WebSocket 握手中注入敏感的 `Authorization: Bearer dm-privacy-v1...`。
2. DigitalMate 先启动可接收握手的入口，通过配置中心 HTTPS 校验业务凭证，然后加载私密配置、启动后台任务。
   校验完成前，其余业务 HTTP 路由不接收业务。凭证只保存在进程内存中，不写入模板参数、工作区文件或工具环境。
3. 访问凭证标称有效期为五分钟。运行中的沙箱每分钟续期，临近过期时访问前也会续期，均不依赖手机登录。
   续期重新核对有效账号、Binding、工作区和代次。上次成功续期后最多三十天内可恢复，超时需要新登录握手。
4. 读取配置时校验 HMAC、数据库授权和账号状态，不调用 ADX RPC。账号禁用或业务授权撤销可拒绝后续检查；
   Binding 删除或代次变化在续期时被发现，未撤销凭证仍受短期访问有效期限制。该限制不能解释为物理执行证明。
5. 验签允许签发副本最多快六十秒；数据库期限上界和 Python 客户端使用相同容差。
   过期下界不额外放宽，不能用容差继续接受已过期访问。实际时间边界仍受部署时钟同步影响。
6. 同代次重连复用授权记录。签发新代次时，在既有用户/工作区事务锁内重新确认当前代次，
   再清理同一 scope 的旧代次授权，避免记录积满配额，并防止延迟的旧请求误删新授权。
   其他用户、工作区和 scope 的授权不受这项清理影响。网络故障不会自动延长权限或撤销合法授权。

入口到沙箱的可信传输链必须加密或具备等效隔离。通用连接器也支持无 TLS 的部署配置，
仅设置 Gateway 模式不能证明整条链路已加密。不要将可信沙箱套接字直接暴露给不可信调用者。

## 模板、回收与快照

DigitalMate 沙箱使用 [DigitalMate 仓库的正式部署模板](https://github.com/openJiuwen-ai/DigitalMate/blob/main/harness/personal-agent/deploy/digitalmate-template.prod.json)，
本仓库不重复维护完整业务模板。接入本功能时，在业务模板中配置 `DIGITALMATE_SECRET_STORE=privacy`、
`DIGITALMATE_CLIENT_AUTH=gateway`，以及配置中心的 HTTPS 地址、公开 CA、租户、工作区和用途；
具体变量见 [DigitalMate 接入说明](https://github.com/openJiuwen-ai/DigitalMate/blob/feat/connection-foundation/docs/guides/development/gateway-privacy-configuration.md)。
该接入说明属于配套候选分支，不能据此声称已在主干发布。沿用既有工作区卷，不增加身份目录、`chown` 权限或扫盘进程。
已有部署保留模板名、工作区路径及 `orbit-config` 用途；这些名称仅为已有部署和密文上下文兼容。

公开 CA 以只读文件提供，根密钥和 TLS 私钥只能由可信 Gateway 持有。
CA 文件可设为 `0444`，其父目录必须允许运行用户遍历（例如 `0755`）；
部署前须使用实际运行用户验证 TLS 加载，不能只核对证书文件本身的权限。

未启用下述模型配置名的原 privacy 模式，需要先接受身份握手再达到业务就绪。此时使用 TCP 就绪探针确认入口能够接受连接。
正式模板默认的 `/health/ready` HTTP 就绪探针需在该业务配置中明确调整，否则可能形成“等待身份才能就绪、就绪后才下发身份”的循环。
这是业务启动配套，不修改 ADX 就绪机制；不应因此修改普通模板或平台生命周期策略。

配置中心不覆盖空闲策略，不保活、延长或自动唤醒沙箱。暂停期间后台任务与续期一同暂停；
恢复后，过期身份先重新校验再访问。需要持续后台运行的模板，应按原业务要求单独选择生命周期策略，不能全局禁用回收。

公共预热快照必须来自未加载任何用户凭证、用户配置或专属工作区的种子；仅“尚未完成本次业务握手”不是充分条件。
本功能不实现 ADX 公共快照的参数或卷重绑定。已授权的内存快照可能包含业务凭证和第三方明文，不能发布为公共镜像。
保留同一有效 Binding 身份和凭证的副本，无法仅靠本业务协议与原实例区分；撤销不等于擦除已捕获的明文或吊销供应商 Token。

## 写入、迁移与回滚

配置版本和删除墓碑拒绝旧刷新结果覆盖新值或恢复已删除密钥。现有连接元数据保存在本地 Store，
密文由配置中心保存，因此保留这条实际跨库边界所需的暂存、回执和恢复；不增加第二个 Store 写入方。
恢复结果不明确时拒绝猜测。它不能使外部 OAuth 交换具备事务性：若供应商已轮换 Token，但响应尚未到达 DigitalMate 就丢失，
仍可能需要用户重新授权。

旧工作区设置 `DIGITALMATE_PRIVACY_MIGRATE_LEGACY=1` 明确导入旧密文。
导入验证原租户、工作区和 `orbit-config` 上下文，回读验证后保留加密的 `.legacy-backup`，
拒绝覆盖更新值或删除墓碑。管理入口仍是 DigitalMate 现有设置，不新增并行的公开 CRUD API。

每个工作区最多六十四个配置名、四千零九十六条事务记录。未决事务不回收，完成回执保留九十天。
每个所有者最多六十四条有效业务授权；同代次复用，旧代次按相同 scope 清理，过期和已撤销记录在后续认证签发时清理。

回滚需停止工作区写入，在已授权的可信 `EngineConfiguration.prepare_legacy_rollback()` 上下文导出当前值，
得到私有 `.legacy-rollback.json`，核对后再原子安装并切回旧服务。新 CLI 进程不能继承内存凭证，当前不提供无人值守回滚 CLI。
迁移前备份中的 OAuth Token 可能已失效，不能代替当前值。旧密文或回滚仍依赖的旧密钥必须保留。

HTTPS 接口限制请求体、连接数和处理时间，响应带 `no-store`。
`digitalmate_privacy` 结构化日志只记录有界操作名、状态和耗时，不包含凭证、用户、工作区或配置值。
现有日志平台可聚合这些字段；未增加新指标服务。

## 验证范围

本地验证使用真实 Rust HTTPS、隔离 PostgreSQL 和实际 Python 客户端/后端；Binding 响应和飞书供应商由明确的测试替身提供。
覆盖初始化、手机断开后续期、旧状态恢复、连续重建超过授权配额、旧请求乱序和时钟偏差。
这些结果不等同于真实 ADX 运行时 Snapshot/Restore、克隆隔离、手机或香港环境验收；环境部署结果另行记录。

## 可选模型配置注册

在 `privacy` 中设置 `model_config_name`（例如 `DIGITALMATE_MODEL_CONFIG`）启用模型配置模式。
名称遵守既有配置名规则，不能使用连接配置保留名 `DIGITALMATE_CONFIG`。配套 DigitalMate 必须使用
同名 `DIGITALMATE_MODEL_CONFIG_NAME`、privacy 存储和 Gateway 认证；新旧部署配置须同时匹配。

Gateway 对目标模板的激活初始化只提供工作区参数，不再提供 `model_api_key/model_api_base`。
运行时就绪后，Gateway 先调用配置中心读取接口；仅在没有注册值时沿用账号模型服务取得并确认模型凭据，再调用注册接口写入
`API_KEY/API_BASE` 字符串字典；注册成功且业务会话仍有效后才转发身份握手。
`ConfigurationRegistry` 仅提供注册和读取方法，接收 scope、名称和值；不向调用方提供数据库或加密器。重连复用已注册值，不重新申请模型凭据。
配置中心继续使用账号库；未来远程注册适配器须验证 Gateway 写入身份，并保持同样契约。

注册首次创建，相同内容重试返回原 revision；不同内容、删除墓碑或未决事务拒绝覆盖。
沙箱 bearer 可以通过既有 `/v1/secret/get` 读取，不能通过 stage/commit/abort 修改此名称。
连接凭据的原有两阶段提交、取消及故障恢复不变。未新增模型专用 HTTP 接口。

配套 DigitalMate 在握手前完成无真实 Key 预热，`/health/ready` 可作为此模式的运行时就绪探针；
业务仍等待身份、配置和新 Agent 就绪。此前描述的握手前 readiness 依赖仍适用于未设置模型配置名的
原 privacy 模式。正式模板的模型参数占位符必须移除，不能只切换 Gateway 开关而沿用旧模板。
DigitalMate 仓库提供 `harness/personal-agent/deploy/digitalmate-template.model-config.example.json`，
部署时填写实际 HTTPS 地址、租户及公开 CA，并保持目标模板名与 Gateway 配置一致。

此能力不实现 Key 轮换或实际快照恢复；启动失败、注册冲突和配置中心不可用保持明确失败，不回退为模板注入秘密。
