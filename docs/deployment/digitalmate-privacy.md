# DigitalMate Gateway 隐私配置中心部署

状态：可选功能的实现候选。本文描述代码和部署约束，不代表已完成真实 ADX 快照、克隆隔离或香港环境验收。

2026-11-16 北京四 `adx-jiuwen-test` 对 Gateway 提交 `549baf350343` 与 DigitalMate 提交 `6ba134a388c0` 的实现完成 2 CPU / 2 GiB 验证：无真实 Key 时 WorkSwarm 就绪，登录调用真实注册、配置加密落库，DigitalMate 读取后原生重载并由真实模型返回 `pong`，进程 PID 不变。仅外部 Huawei/AGC 身份验证使用回环测试夹具；账号会话、模型服务和配置中心均为实际实现。首次登录（含模型凭据准备和注册）104.12 ms，首次 WebSocket 激活握手 1396.36 ms；该次没有单独测量 Agent prepare，也没有验证 Snapshot/Restore。注册冲突不返回 Token、撤销新会话、故障解除后的恢复均通过。

以上集群记录对应尚未恢复模板 Key 注入的历史候选，不能替代本文后续“注册与注入并行”调整的集群验收。本次并行调整已通过本地 Gateway 288 项测试、普通连接回归及独立注入/重连用例、真实 PostgreSQL 隐私回归；一次性回填脚本通过 8 项隔离数据库测试及 Rust→Python→Rust 密文互通测试。测试后已恢复原 Ingress/Activator 镜像与配置，删除测试账号、凭据、Binding、Runtime 和夹具；Node 未重启。

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
   正常连接器调用复用这同一张 privacy 工作负载凭证和同一条 `privacy_grants`，
   不额外签发普通 connector 凭证或注入专用头。连接器身份投影不授予邮箱读取，
   仍需真实连接和 M44 的操作/意图证明。它不是 Composio 项目 Key、供应商
   OAuth Token 或新的手机登录会话。后台批次交换的短期 bootstrap 独立受限，
   不能读取配置或续期为正常 privacy 身份。
2. DigitalMate 先启动可接收握手的入口，通过配置中心 HTTPS 校验业务凭证，然后加载私密配置、启动后台任务。
   校验完成前，其余业务 HTTP 路由不接收业务。配置客户端在进程内保存凭证；
   显式启用连接器恢复时，同一正常身份可经实际续期和作用域校验后存入 operator
   指定的 workspace 外私有载体。该载体受保护、原子写入，不是第二个身份库；
   不写入模板参数、可编辑工作区或工具环境，也不保存项目 Key。
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

Composio 与后台摄取配套的账号新增表、摄取 schema 升级、旧订阅投影切换及发布顺序，
见 DigitalMate 仓库的[连接器配套部署与数据库切换](https://github.com/openJiuwen-ai/DigitalMate/blob/main/docs/guides/operations/connectors-cutover-and-database.md)。账号 schema 版本与
摄取 schema、batch wire 版本独立，本次复用工作负载凭证不新增身份数据库或 SQL 升级。

DigitalMate 沙箱使用 [DigitalMate 仓库的正式部署模板](https://github.com/openJiuwen-ai/DigitalMate/blob/main/harness/personal-agent/deploy/digitalmate-template.prod.json)，
本仓库不重复维护完整业务模板。接入本功能时，在业务模板中配置 `DIGITALMATE_SECRET_STORE=privacy`、
`DIGITALMATE_CLIENT_AUTH=gateway`，以及配置中心的 HTTPS 地址、公开 CA、租户、工作区和用途；
具体变量见 [DigitalMate 接入说明](https://github.com/openJiuwen-ai/DigitalMate/blob/main/docs/guides/development/gateway-privacy-configuration.md)。
无 Key 预热与模型配置读取仍需配套 DigitalMate PR #1287，不能仅凭原隐私配置接入说明认定模型配置能力已在主干发布。沿用既有工作区卷，不增加身份目录、`chown` 权限或扫盘进程。
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

在 `privacy` 中设置 `model_config_name`（例如 `DIGITALMATE_MODEL_CONFIG`）指定登录时注册模型配置的名称；此字段不控制模板参数注入。
名称必须为 1–128 个 ASCII 字母、数字、下划线、连字符或点。模型配置与连接配置应使用不同名称，避免混用值的结构；配置中心不按具体名称区分读写权限。
Gateway 在 `DigitalMateGatewayConfig::validate()` 阶段校验此字段，早于隐私服务读取密钥/TLS 文件或启动监听器；未设置此字段仍表示关闭模型配置注册。
切换到配置中心读取的 DigitalMate 模板使用同名 `DIGITALMATE_MODEL_CONFIG_NAME`、privacy 存储和 Gateway 认证。迁移期间旧模板继续使用注入的 Key，不要求同时切换读取方式。

Gateway 在新建 Binding 的初始化中继续提供 `workspace` 和 `model_api_key/model_api_base`，与登录注册同时生效。已有 Binding 的普通重连跳过该初始化，不重新查询模型服务或修改实例启动参数。
Huawei 和 AGC 登录在身份认证成功后、返回登录响应前，由 Gateway 沿用账号模型服务取得并确认当前模型凭据，
调用注册接口写入 `API_KEY/API_BASE` 字符串字典。租户、模板及工作区由 Gateway 配置和已认证用户推导，不依赖沙箱启动。
注册成功且业务会话仍有效后才返回登录 Token；每次重新登录都会注册当前值。
认证与注册共用登录阶段的 45 秒截止时间；注册失败或超时不返回成功/Token，并在最多 5 秒内尝试撤销刚创建的账号登录会话。
注册本身不创建会话；撤销对象是认证步骤已写入账号库、尚未返回给客户端的业务 Token。
超时不能证明配置没有落库，不删除配置或回滚 revision；再次登录依赖幂等注册收敛。
`ConfigurationRegistry` 仅提供注册和读取方法，接收 scope、名称和值；不向调用方提供数据库或加密器。
账号服务复用已持久化的模型凭据并确认其上游可用性；WebSocket 建连/重连与 `/api/v1/auth/session` 检查均不触发注册。
WebSocket 继续检查会话、激活工作区并下发隐私身份，不在这条路径补注册。存量用户不以重新登录作为模板升级的前置条件：先在旧模板仍可使用注入 Key 时完成存量回填，再切换读取配置中心的新模板。
配置中心继续使用账号库；未来远程注册适配器须验证 Gateway 写入身份，并保持同样契约。

注册首次创建，相同内容重试返回原 revision；不同内容按读取到的 revision 更新并返回新 revision。
并发写入导致 revision 变化时，只有当前值与本次注册相同才视为幂等成功，否则返回冲突，不自动重试覆盖。
删除墓碑和未决事务仍返回冲突，避免绕过原有取消及故障恢复协议。
注册接口没有凭据来源的版本号，因此不能判断两个 Gateway 提交的不同值哪个更新；部署期间应统一账号凭据来源和 API_BASE，
不能依赖该接口防止旧配置实例在后续请求中重新注册旧值。
持有目标 scope 有效隐私 bearer 的调用方，可以通过既有 `/v1/secret/get` 读取，也可以通过 stage/commit/abort 更新或删除模型配置，与普通隐私配置使用相同权限和事务规则。注册方法不维护配置名白名单；可信调用适配器负责验证写入身份与 scope。
连接凭据的原有两阶段提交、取消及故障恢复不变。未新增模型专用 HTTP 接口。

配套 DigitalMate 在握手前完成无真实 Key 预热，`/health/ready` 可作为此模式的运行时就绪探针；
业务仍等待身份、配置和新 Agent 就绪。此前描述的握手前 readiness 依赖仍适用于未设置模型配置名的
原 privacy 模式。切换为配置中心读取的新模板移除模型参数占位符；过渡期旧模板仍保留占位符，Gateway 同时提供注入参数以支持它。新模板本身不引用这些参数并不意味着 Gateway 已停止提供它们。
DigitalMate 仓库提供 `harness/personal-agent/deploy/digitalmate-template.model-config.example.json`，
部署时填写实际 HTTPS 地址、租户及公开 CA，并保持目标模板名与 Gateway 配置一致。

Gateway 可以重新注册已经取得的新 Key 或 API_BASE；此能力不负责定时生成、轮换或吊销 Key，也不实现实际快照恢复。
配置中心写入新值不等于运行中的 DigitalMate 已完成重载：配套 DigitalMate 当前仅在首次身份初始化时读取模型配置，
已经初始化的进程需要重新启动才能通过该路径加载新值；本次没有新增后台监听或重连热更新协议。
已切换为配置中心读取的 DigitalMate，在配置缺失或读取失败时仍明确失败，不自动回退读取注入 Key；因此必须在切模板前核验存量覆盖。

## 存量迁移顺序

1. 升级 Ingress 并启用模型配置注册，保留旧模板和启动参数注入。新登录继续注册当前账号凭据。
2. 在受保护的运维环境中回填旧用户模型配置。一次性脚本读取账号库已有模型凭据，在内存中解密后，使用配置中心的 AES-256-GCM 格式和目标 scope 重新加密，写入 `adx_accounts.user_secrets`；不能直接复制账号凭据密文。该运维方式不需要创建业务会话、隐私访问 Token 或沙箱，也不新增产品接口。
3. 回填须使用现有 workspace/secret 事务锁和数量约束；只补缺失记录，相同值保留原 revision，已有不同值、删除墓碑、未决事务、无有效模型 Key 或解密失败均列为阻塞项。完成覆盖与回读核验后才进入模板切换；这不是无条件 SQL 覆盖。
4. 创建并切换配置中心读取的新模板。workspace 由租户、模板名和用户推导，模板名改变时必须回填到新 scope；版本或 alias 的变化不改变 workspace。
5. 全部旧模板退出使用后，再单独移除 Gateway 参数注入。这一步不包含在当前实现中。

一次性回填脚本不属于产品源码或配置中心 API，不随本 PR 提交。常规业务模块继续使用注册/获取方法，不依赖配置中心数据库结构。
