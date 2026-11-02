# 工作区凭证加解密部署

`adx-credential-crypto` 是用户运行环境外的内部 HTTPS 服务，负责凭证加解密；用户应用继续管理供应商授权、Token 刷新和工作区内的密文。主密钥不进入用户镜像。此功能默认不启用，不会自动启动服务、生成密钥或修改已有模板。

运行入口由镜像继承的 `serve` 命令提供；初始化、轮换 CLI 仅用于可信运维，不是另一种常驻部署方式。

协议、授权边界和错误语义见[模块说明](../../agent/credential-crypto/README.md)。本页仅使用通用路径和示例域名；实际部署配置、证书、身份和密钥由部署者提供，不应提交到仓库。

## 组件与部署前提

- Activator 继续作为 AgentBinding 的权威入口，复用现有 Redis 与 namespace。服务授权记录只存身份摘要、Binding 引用、generation、用途和到期时间，不存用户 Token 或加密密钥。
- Crypto 通过 ADX 模板和固定 Binding 托管为独立 Environment。发布包中的二进制用于构建服务镜像及可信运维，不安装为主机常驻服务。
- Crypto 使用独立的 verify-only 服务凭据访问 Activator。签发和撤销仍使用原私有控制凭据；两者必须不同，均不得交给用户环境。
- 自动下发依赖 Unix，以及 Activator 和 Worker 明确挂载的同一身份共享卷。共享卷必须支持实际部署所需的 advisory lock、原子替换、fsync 和 ownership 操作。没有共享卷时使用可信流程手动签发、分发身份，不假定 Activator 能访问远端节点的任意路径。

## 1 准备服务材料

使用标准构建入口取得服务镜像输入及运维二进制，或从源码构建：

```sh
cargo build --locked --release -p adx-activator -p adx-credential-crypto --bins
```

参考 [crypto 配置](../../build/config/examples/credential-crypto.json)，准备私有配置文件、服务端证书和私钥、验证 Activator 的 CA 以及 verify-only 凭据文件。Unix 下私有目录使用 0700、文件使用 0600；Crypto 当前也要求其证书和 CA 文件满足私有文件检查。Windows 下需配置 owner-only ACL。

仅首次初始化独立 keyring：

```sh
adx-credential-crypto init-keyring --output /mnt/private/adx-credential-crypto/connection-keyring.json
```

已存在的 keyring 必须保留并受控备份。升级或重启时不重新初始化；缺失原密钥不能通过生成新密钥恢复已有密文。示例程序不会创建真实平台或供应商密钥。

## 2 在 Activator 显式启用

参考 [Activator 配置示例](../../agent/activator/examples/credential-grants.json)，将 `credential_grants_token_env` 设置为实际 verify-only 环境变量名。示例中的 `192.0.2.0/24` 为文档占位网段，必须替换为部署环境真实的 sandbox CIDR；平台地址、Redis、namespace 和租户环境变量也应沿用实际环境配置。

Activator 私有监听由部署侧 TLS 代理保护，或仅通过显式授权的同机 loopback 通信访问。服务授权管理路径不得暴露给用户运行环境；仅独立 Crypto 服务持有 verify-only 凭据；Gateway 复用已有 Activator 客户端及私有控制身份进行校验。嵌入式部署需显式装配同一 `service_grants::router`，现有嵌入式 Gateway 不会被此功能自动修改。

可选 `credential_delivery` 配置开启自动身份下发：

1. 配置目标 `template`、身份共享卷 `root`、Gateway 入口 CA 文件，以及用户容器实际 UID/GID。
2. 在可信 Activator 挂载完整身份卷；在 Worker 仅向对应用户只读挂载 `root/users/${workspace}`。示例统一使用 `/mnt/shared/credential-identities`。
3. 预创建共享卷根目录和业务工作区父目录。元数据和锁目录只供可信服务访问，不能挂入用户容器。
4. 配置 `ttl_seconds` 和 `renew_before_seconds`。示例为 30 天身份、到期前 7 天续期；续期窗口必须小于总有效期。检查间隔取续期窗口的一半与 300 秒中的较小值；1 秒窗口对应 500 毫秒间隔，短有效期也会提前检查。调度延迟或依赖故障仍可能造成过期，不保证可用性。

Activator 在 Binding 提交后、Environment 创建前下发身份。独立进程在启动时检查，随后按上述间隔重复检查已下发身份。发布新身份文件前，先持久化新身份摘要和待撤销的旧摘要；文件可用后再幂等撤销旧授权。撤销失败或结果未知时保留待办，后续请求或进程重启后的检查继续完成；故障期间旧身份可能仍然有效，直到撤销完成、原有效期结束或 Binding 失效。元数据损坏或不可读取时返回错误，不覆盖记录丢弃待办。续期任务只维护服务身份，不创建或恢复 Environment；其生命周期跟随独立 Activator 进程。未启用该选项的实例不运行此续期任务。

## 3 启动 Crypto 并配置用户应用

### ADX 模板托管

此模式使用一个独立服务模板和固定 Binding。生命周期仍经 Activator → Environment API → Platform；Gateway 复用 Relay 连接当前实例，不固定实例 IP，不新增生命周期控制器。主密钥仅挂入 Crypto 服务实例；模板托管本身不提高密钥保护等级，有宿主机管理权限的人仍属于信任边界。

1. 使用 [Dockerfile.credential-crypto](../../build/images/Dockerfile.credential-crypto) 构建镜像，build context **仅包含**对应 Linux/CPU 架构的 `adx-credential-crypto` 二进制；`BASE` 指定兼容该二进制的已批准、固定 digest 的 Linux 基础镜像。镜像以 UID/GID `10002:10002` 继承 serve 入口。不要把任何配置、密钥或用户目录放入 build context。
2. 可信部署流程在所有允许调度该服务的 Worker 上准备 `/mnt/private/adx-credential-crypto`，放入 [服务配置](../../build/config/examples/credential-crypto.json) 并命名为 `server.json`，以及 keyring、后端 TLS 证书/私钥、Activator CA 及 verify-only 身份。目录 0700、文件 0400/0600，由 UID 10002 可读；各节点使用同一受控 keyring。这是独立的受保护持久目录，不能放在用户身份共享卷内。模板没有新增节点选择能力，部署侧需保证实际放置节点上的材料一致。
3. 通过已有 Platform 网络配置允许 Crypto 到 Activator 的私有 HTTPS verify 地址，以及 DigitalMate 到内部 Gateway 的 HTTPS 入口。不要通过开放沙箱互访来接通服务。租户管理凭据只供可信运维使用；能发布任意 host mount 模板或执行实例命令的主体属于密钥信任边界。
4. 把 [服务模板](../../agent/activator/examples/credential-crypto-template.json) 的 image 替换为实际不可变引用，用受控管理身份发布，记录响应的真实数字版本。模板只读挂载上述私有目录，不自动创建密钥；`idle_timeout_seconds=0` 关闭空闲回收，TCP readiness 经 Platform 执行。`service.protocol=http` 用于服务选择，得到原始服务流后仍执行 TLS。

```sh
adx --token-file /run/secrets/operator-adx-token template publish -f credential-crypto-template.json
```

5. 为内部 Gateway 构建启用 `agent-api` 的版本，在**新部署配置**设置 `ADX_CREDENTIAL_CRYPTO_CONFIG` 指向 [Gateway 配置](../../build/config/examples/credential-crypto-gateway.json)，同时保留已有 `ADX_AGENT_CONFIG`。`scope` 固定服务租户、模板、发布版本及 Binding ID（示例 `shared`）；端口须由模板声明。`server_name` 匹配后端证书 SAN，`ca_file` 信任**后端 Crypto CA**。Gateway 校验复用 `ADX_AGENT_CONFIG` 中已有的 ActivatorClient/LocalControl，不另外配置授权 URL 或 token 文件，也不持有 KEK。Gateway 保留既有 Agent 控制权限，属于可信控制面；此模式不隔离 Gateway/Activator 管理员。
6. DigitalMate 的 `DIGITALMATE_CRYPTO_URL` 设置为内部 Gateway 的 HTTPS **origin**，例如 `https://crypto-gateway.internal.example:8443`，不带路径。用户的 `ca.pem` 和 Activator delivery 的 `ca_file` 应信任 **Gateway 入口 CA**，它与后端 CA 分开配置。保留 `identity`、`/run/orbit-crypto`、工作区路径及已有 `orbit-config` 密文上下文；仅在新实例或受控切换时应用新模板版本。

首次有效工作区请求创建或复用固定 Binding 并激活配置中心。运维也可使用原管理鉴权，提前 `POST /api/agent/v2/templates/credential-crypto/versions/<实际版本>/bindings/shared/resolve`，请求体为 `{"protocol":"http","port":18443}`。结果未知时查询同一 Binding，不换 ID 重建；模板发布与查询本身不创建实例。

入口只承接 TLS `POST /v1/seal`、`POST /v1/open`。Gateway 先逐次向 Activator 验证用户身份与 context，通过后才激活；Crypto 再次验证。仅向后端发送工作区 Bearer 和必需 HTTP 头，客户端路由头、Cookie 等不转发。普通 `/agent/http` 使用租户管理凭据且会移除它，不能直接交给用户应用调用 Crypto。管理、keyring 操作、rewrap 和 `/healthz` 不经此入口暴露；其他路径保持原路由行为。

Gateway 请求与响应各限 512 KiB、入口并发 128、总期限默认 60 秒（可配 1–300），覆盖读取、授权、激活、TLS 和完整响应；后端另有自己的连接期限。HTTP 交换复用既有后端连接池及其端点限制（默认 64），Crypto 使用一次性连接，避免复用不同的 TLS 身份。授权调用保留 3 秒上限及 16 KiB 响应上限。禁用 query、Upgrade、重复 Authorization；不重放结果未知的业务请求。授权故障返回 `503 AUTHORITY_UNAVAILABLE`，服务选择/TLS/交换或总期限失败返回 `503 CRYPTO_UNAVAILABLE`，错误不回显凭据或上游正文。部署仍需内部网络访问控制，共享 Gateway 上的域名本身不构成隔离边界。

私有文件检查拒绝符号链接。若部署源是投影 Secret 等链接文件，需由可信初始化流程复制为上述私有目录中的普通文件，并设置正确 ownership/权限后再只读挂载；不能把原投影链接直接交给 Crypto。

升级可使用明确配置的 Alias 或切换固定版本；固定版本变化会选择另一个 scope，旧 Binding/实例不会因入口切换自动删除，应在回滚窗口结束后由运维通过原管理接口清理。保留原 keyring 与上下文，不能通过重新生成密钥升级；不承诺旧连接零中断。

此模式需要相应版本与配置的 Gateway、服务授权能力和新 Crypto 镜像；旧 Gateway 不会仅因发布模板而自动识别。未设置 `ADX_CREDENTIAL_CRYPTO_CONFIG` 时不注册这两条入口。修改不会自动更新运行进程、发布模板或迁移现有 Pod。

### DigitalMate 用户模板

参考 [DigitalMate 模板示例](../../agent/activator/examples/orbit-credential-crypto-template.json)。DigitalMate bot-agent 持有配置中心客户端并委托 WorkSwarm 执行任务；使用 `DIGITALMATE_SECRET_STORE=remote` 和 `DIGITALMATE_CRYPTO_*` 配置，自动下发也检查这套变量。将 Crypto URL、CA、租户和工作区上下文设置为实际值，并保留 `purpose=orbit-config` 与已有密文的上下文一致；该值参与密文认证，不能作为产品名称直接替换。既有模板名 `orbit`、工作区和 `/run/orbit-crypto` 挂载路径保持不变。`${workspace}` 来自已有 Binding 参数。模型相关配置沿用应用原有机制，不由 Crypto 管理。

用户只读身份目录包含 `identity` 和 `ca.pem`，不包含 keyring、TLS 私钥、管理凭据或 verify-only 凭据。示例中的身份路径与 Activator 的共享卷 root 对齐；业务工作区挂载则沿用部署已有存储，并满足应用自身的锁和数据库要求。

托管实例使用 Activator 自动下发和续期；可信人工恢复和紧急撤销使用现有私有 `service-grants/issue`、`service-grants/revoke` API，不由 Crypto CLI 再实现一套签发流程。issue 请求的 scope/generation 必须来自权威 Binding；返回的 credential 只允许写入受保护的身份目录，不记录到终端或日志。网络超时可能已经签发，不能盲目重复调用。

## 4 验证与升级

使用两个一次性合成工作区和假凭证检查 seal/open 往返、跨工作区拒绝、篡改拒绝、撤销、过期、Binding 删除或 generation 换代后的拒绝。`/healthz` 只代表 Crypto 进程存活，不能代替真实鉴权和解密检查。

配置自动下发时，还需验证首次激活前文件就绪、运行 UID 可读、挂载只读、并发交付与续期、重启后原密文可解。模板托管还需确认固定 Binding、调度节点的私有目录、后端 TLS SAN/CA、Gateway 入口 CA 和网络例外。不要读取或打印真实 Token。Gateway/Relay/TLS/Crypto 本机组件测试不能代替实际 OCI 镜像、共享卷和多节点验收。

升级保留原 keyring、CA、密文上下文和业务工作区，记录旧二进制与模板版本。通过服务镜像、模板版本及固定 Binding 更新配置中心，用户实例按受控流程切换；没有启用此功能的模板不需要改动。回滚时保留服务材料和密文，不能退回明文模式来读取加密配置。密钥轮转、旧 key 保留及离线 rewrap 的操作见模块说明；KMS 后端尚未实现。

本功能不包含供应商 OAuth、后台同步、生产容量保证或灾难恢复验收。Redis 是授权元数据的权威存储，需要现有持久化和访问控制；丢失或不可用时拒绝加解密，不回退为无鉴权模式。
