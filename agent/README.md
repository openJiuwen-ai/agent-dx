# Agent-DX v2

Agent 层使用 Rust，产品 API 嵌入统一 Gateway。无状态 Activator 默认与 Edge 共进程，也可独立多副本部署。

| 目录 | 职责 |
| --- | --- |
| `crates/core` | 产品类型、协议、Sandbox 能力边界及通用校验 |
| `crates/store` | Template/Environment 元数据、Redis 原子条件写；内存实现仅供测试 |
| `activator` | 产品管理与稳定身份激活，调用 Sandbox 接口，无后台健康/恢复扫描 |
| `api` | inline 规格适配、Activator 本地/HTTP 调用适配、Environment 身份选择、服务选择 |
| `cli` | Rust 用户命令行 `adx`；Template 发布/查询、Environment 分页查询/删除、SSH 交互终端 |

Environment 对应一个稳定逻辑 Sandbox ID。指定 Environment 的首次访问会幂等创建元数据，再激活 Sandbox；多副本复用同一身份。无效模板或未声明的 service 不创建 Environment。Sandbox 状态、健康、暂停/恢复与运行载体 0–1 由 Platform 保证。产品删除确认后，新访问可以重建同名 Environment，generation 隔离旧生命周期。

公开 API、认证、路由和连接实现位于 Gateway。HTTP/WS/SSH 通过普通方法调用 Agent 的 `ManagedService`，共用 Environment 身份选择和 service 唯一匹配规则；身份选择本身不创建元数据或 Sandbox。HTTP 响应头、SSH 终端提示与协议转发仍由 Gateway 处理。

Gateway 对同一次请求的内部重试固定首次选定的 generation。删除或同名重建后，旧请求的重试返回冲突，不触发重建或选择新的生命周期。

用户 Harness 由 RRT 启动，自行定义业务接口。HTTP/WS/SSH 保持透明转发，不限制业务并发。inline create/get/kill 是独立的旧协议兼容入口，直接适配 Sandbox，独立于 v2 管理接口、Environment、Activator 和 ADX Redis。

## 部署

```sh
cargo build --locked -p data-plane-gateway -p adx-api-server --features data-plane-gateway/agent-api --bins
```

Gateway 保留独立 Edge 与 API Server 内嵌 Edge 两种部署形态，共用进程装配代码。构建时为承载 Edge 的二进制启用 `data-plane-gateway/agent-api`。两套公开接口可以分别启用，路由、配置与认证分开：

- `ADX_AGENT_CONFIG` 指向 v2 配置文件，选择 Activator 的部署模式。受管管理和 HTTP/WS 数据入口使用 Platform API Key。
- `ADX_INLINE_CONFIG` 指向 inline 兼容配置文件；装配现有 create/get/kill 管理接口，使用独立 JWT/IAM 认证，并要求 `ADX_SANDBOX_CONFIG` 提供 Sandbox 能力。

默认内嵌模式的完整配置如下，`mode` 和 `timeout_seconds` 可省略：

```json
{
  "mode": "embedded",
  "timeout_seconds": 60,
  "embedded": {
    "redis_url": "redis://127.0.0.1:6379/0",
    "namespace": "agent"
  }
}
```

内嵌模式要求 `ADX_SANDBOX_CONFIG`，直接复用其中装配的 `PlatformSandbox`。Agent API → LocalControl → Activator → Sandbox 均为进程内方法调用；不启动 Activator HTTP 监听器，不使用 Activator 服务令牌，也不回调本机 Sandbox HTTP 接口。现有 Sandbox API 的配置及凭据要求保持不变。Redis 启动连接/schema 校验失败则 Edge 启动失败；请求和连接随 Edge 生命周期释放。多个 Gateway 连接同一 ADX Redis namespace，复用现有原子条件写和稳定 Sandbox ID，不新增选主或发现机制。

独立模式需另外构建并启动 `adx-activator`：

```sh
cargo build --locked -p adx-activator --bin adx-activator
```

Gateway 的独立模式配置如下，组件间令牌从环境变量读取；此模式不要求 Gateway 本地装配 Sandbox API，由独立 Activator 的 Sandbox 地址决定能力提供方：

```json
{
  "mode": "remote",
  "timeout_seconds": 60,
  "remote": {
    "urls": ["https://activator.internal"],
    "token_env": "ADX_ACTIVATOR_SERVICE_TOKEN",
    "ca_path": "/etc/adx/ca.pem",
    "allow_plaintext": false
  }
}
```

`embedded` 与 `remote` 只允许配置选中模式的一组参数。原仅包含 `activator` 地址对象的配置需改为上述显式 `remote` 配置。两种模式由入口使用 `timeout_seconds`（默认 60 秒、必须为正）确定一次绝对 deadline。管理面在鉴权后统一处理 trace、请求读取、业务调用和 JSON 响应；HTTP/WS 目标激活的模板查询、激活和内部目标重试共用同一期限。LocalControl 只做方法调用，不重新启动计时；remote 将剩余预算传给 Activator，地址重选也不重置期限。纯读超时返回 Unavailable，可能写入的操作超时返回 OutcomeUnknown，重试使用原身份；已到期的内部重试直接拒绝。网络连接及 Sandbox 后端调用的独立上限保留。激活期限不覆盖建立后的 HTTP 响应流、WS 或 SSH 会话；SSH 模板校验、身份提示和后端握手共用 SSH 连接期限。

Gateway 不缓存 Template 或 Target，查询和获取目标均调用 Activator。内嵌模式由同进程 Activator 访问 ADX Redis；独立模式 Gateway 不连接 ADX Redis。业务层始终经 Control/Sandbox 接口，Platform 自身的 Redis 发现不受影响。Activator 部署见 [进程说明](activator/README.md)，状态保证见 [存储说明](crates/store/README.md)。

inline 完整配置示例：

```json
{
  "iam_address": "http://iam.internal:31100",
  "backend_timeout_seconds": 30,
  "max_inflight": 64,
  "inline_profiles": [
    {
      "sandbox_type": "docker",
      "request_image": "app:1",
      "image": "app:1",
      "isolation_runtime": "runc",
      "working_dir": "/"
    }
  ]
}
```

IAM 地址对应部署中的可信 IAM 服务；当前复用的 IAM 客户端使用内部 HTTP，访问 `GET /iam-server/v1/token/auth` 并传递 `X-Auth`。配置 IAM 地址不会部署 IAM。profile 必须与 Sandbox 的预装能力一致，示例中的镜像需替换为实际预装镜像。

inline 管理路径为 `POST /api/agent`、`GET/DELETE /api/agent/{id}`。按原 Frontend 管理协议依次从 `X-Auth`、query `token`、`iam_token` Cookie 取凭据，要求 JWT 的 `developer` 角色和有效租户，再由 IAM 验证原始 token；不缓存验证结果。调用 Sandbox 的租户取自已验证的 JWT `sub`，不信任客户端的 `tenant_id`。管理入口继续要求 TLS；认证失败返回 HTTP 401 和字符串 `error`。两套认证不互相回退，Platform API Key 不能代替 inline JWT。

inline 的完整详情/列表与 exec/files 仍待补齐。HTTP/WS 已接入下述统一入口；既有共享 Sandbox-ID 转发入口继续遵循自身认证。CLI 只使用 v2 接口。

受管接口使用 `/api/agent/v2/templates/{name}/versions/{version}/environments/{id}`，PUT/GET/DELETE 对应创建、查询和删除。`POST .../{id}/resolve` 接受 `{protocol, port?}`。HTTP/WS 通过下面的统一入口触发激活；共享转发使用真实 Sandbox ID。

`GET /api/agent/v2/templates/{name}/versions/{version}/environments` 返回 `environments` 和 `next_page_token`。可传 `page_size`（默认 50，范围 1–100）和 `page_token`；token 绑定认证租户、模板与版本。列表仅返回产品元数据，包含删除中的记录，不查询平台健康状态；并发创建/删除期间不保证分页快照。模板不存在时返回 404。

Rust CLI 的当前管理能力与配置见 [CLI 使用说明](cli/README.md)。HTTP CLI 仍在开发计划中，可以用普通 HTTP/WS 客户端访问统一入口；SSH CLI 使用系统 OpenSSH。

## 统一 HTTP/WS 访问

HTTP 使用 `/agent/http[/业务路径]`，WS 使用 `/agent/ws`；受管 WS 可追加业务路径。HTTP 入口拒绝 WebSocket Upgrade，WS 入口要求 GET 和 WebSocket Upgrade；inline 与受管目标均执行该校验。目标二选一，同时传入或重复参数返回 400：

| 参数 | 路径与认证 |
| --- | --- |
| `instance=<create返回的ID>` | inline 直接访问；原 JWT/IAM 认证；不调用 Activator |
| `target=urn:adx:instance:<id>` | 与 instance 参数相同的 inline 分支 |
| `target=urn:adx:template:<name>:<version>` | v2 API Key 认证；服务端生成 Environment ID 并激活 |
| `target=urn:adx:environment:<name>:<version>:<id>` | v2 API Key 认证；创建或复用指定 Environment |

URN 各段按 UTF-8 百分号编码；作为 query 参数时还需要经过 URL query 编码。租户来自认证，不放在 URN 内。inline 的 `port` 默认 18092；受管目标的端口必须匹配 service，只有一个对应协议端口时可省略。

```sh
curl --get 'https://gateway.example/agent/http/chat' \
  -H "Authorization: Bearer $ADX_TOKEN" \
  --data-urlencode 'target=urn:adx:template:assistant:1'
```

受管 HTTP/SSE、WS 101 和选定身份后的失败响应都带 `X-ADX-Environment-ID` 与 `X-ADX-Environment-URN`；Gateway 覆盖后端同名 Header。ID 通知不代表创建成功。独立的不带 env 请求生成新 ID，同一请求的内部重试复用 ID 和 generation；后续访问应使用返回的 Environment URN。

inline HTTP/WS 按 `X-Auth` → query `token` → `iam_token` Cookie → 首个非空 `Sec-WebSocket-Protocol` 值取 JWT，并通过 IAM 验证；与管理接口不同，数据访问不要求 developer 角色。普通租户只能访问自身实例；原协议 system 租户 `0` 可访问其他租户实例。认证失败不切换到 API Key 验证。入口要求 TLS。

inline HTTP 去掉公开 path 前缀，消费 `instance/target/port/tenant_id/token` 路由 query，保留业务 query 多值、Host、X-Auth 和其他业务 Header，移除 Authorization；缺少 X-Forwarded-Proto 时设置为 https。inline WS 的后端握手 path 保持原 `/serverless/v1/ws`，原 query、Cookie 与子协议 Header 继续转发；仅外部 path 改为 `/agent/ws`。受管 HTTP/WS 去掉公开前缀与目标参数，将业务路径交给 Harness，并移除平台凭据 Header。

响应体按流转发，WS 升级后使用现有双向字节通道，不增加业务 envelope 或 WS 消息。inline 路由/连接失败返回 502、非可连接状态返回 409、租户无权访问返回 403。Node Proxy 连接和路由仍由原 Gateway 数据面管理。

## SSH 交互终端

设置 `ADX_SSH_CONFIG` 后，独立 Edge 或 API Server 内嵌 Edge 会启动独立 SSH 监听端口。原实例使用路由用户名 `yr:instance:<id>[:port=<port>][:trace=<id>]`，受管目标使用 `adx:target:<百分号编码的完整URN>[:port=<port>][:trace=<id>]`。两种目标共用 listener，分别检查配置中的公钥授权表；路由用户名不是租户或后端 Linux 用户。

SSH 首版只提供一个连接一个交互式终端，要求 PTY 和 shell 请求，支持输入输出、窗口调整、退出码与连接关闭。Environment 提示在鉴权完成、交互 shell 建立后通过 stdout 显示，然后激活并连接后端；它通知选定的身份，不表示 Sandbox 已就绪。省略 env 时当前连接只生成一个 ID；携带返回的 Environment URN 可再次访问。仅认证或打开 channel 不创建资源。inline 直接定位实例，默认端口 22，不调用 Activator，不生成 Environment。

后端复用共享 L4 connector → Node Proxy → Sandbox sshd；没有新建数据通道或本地目标缓存。使用未修改的 russh 0.63.3，不发送认证 Banner。首版不提供 SSH 远程 exec、SFTP 或端口转发。已有 Sandbox-ID SSH/CONNECT 转发入口保持原有行为。

配置字段如下，路径相对进程工作目录；公钥使用实际 OpenSSH 公钥文件内容：

| 字段 | 含义 |
| --- | --- |
| `bind` | 必填监听地址，例如 `0.0.0.0:2222` |
| `host_key` | Gateway SSH 主机私钥文件，必填 |
| `backend_key` / `backend_user` | Gateway 连接后端 sshd 的私钥文件和登录用户，必填 |
| `backend_host_keys` | 可信后端主机公钥字符串数组，至少一个；不接受未知主机密钥 |
| `inline_authorized_keys` | inline 客户端授权数组，每项 `{public_key, tenant_id}`，默认空 |
| `agent_authorized_keys` | 受管客户端授权数组，每项 `{public_key, tenant_id}`，默认空；非空时需要 `ADX_AGENT_CONFIG` |
| `auth_timeout_seconds` | SSH 身份认证期限，默认 15 秒 |
| `connect_timeout_seconds` | 模板校验、激活与后端握手共享期限，默认 60 秒 |
| `max_connections` | SSH 客户端连接上限，默认 256 |

两张客户端授权表独立匹配，不互相回退，至少配置一张。普通租户只能访问自己实例；inline 的 system 租户 `0` 可访问其他租户实例，受管分支仍严格匹配租户。SSH 使用公钥认证，不读取 JWT 或 Platform API Key。网络来源限制复用现有 Edge client ACL。

部署时预置 Gateway 主机私钥，把它的公钥通过可信渠道加入客户端 known_hosts；将 backend_key 对应公钥加入镜像中 backend_user 的 authorized_keys，把镜像 sshd 的主机公钥加入 backend_host_keys。后端主机密钥轮换时先加入新公钥，再更新镜像，最后移除旧公钥。私钥文件应限制读取权限，不放进 Template 或 Redis。镜像需预装 sshd 并声明对应 service，不要求修改 Platform。

Template 的 service 声明 HTTP/WS/SSH 协议及端口，例如 `[{"protocol":"http","port":8080},{"protocol":"ws","port":8080},{"protocol":"ssh","port":22}]`。受管 SSH 无端口参数时必须恰好匹配一个 SSH service。用户直接调用 Harness 自定义接口，审计轨迹能力暂缓。

预装镜像、RRT 与匹配的启动 profile 仍可用于验证。当前 Sandbox 适配不能证明从未观察到的创建已被取消；这类删除返回结果未知并保留产品记录。ADX 不通过删除元数据掩盖平台副作用。pause/resume 完全由 Platform 负责，不属于 ADX 适配范围。首版按 Running 视为服务已就绪；平台当前只保证资源和 runtime IP，RRT/业务端口就绪保证记为 Platform 能力缺口，ADX 不补探测。

这一删除限制也适用于通过管理 PUT 仅建立元数据、尚未被流量激活的 Environment：删除会保留 Deleting 记录，无法依靠重复删除确认完成。正常使用应由访问流量创建 Environment。冷启动还可能遇到 Running 与共享路由发布之间的短暂窗口；HTTP/WS 或 SSH 已返回 Environment 身份后，调用方应使用该身份重试，不能把省略 env 的新请求当作原请求重试。

## 验证

2026-09-22 在隔离容器中完成真实 Platform 端到端复测：两个 Gateway Edge 各自内嵌 Activator，共用真实 Redis、Master、Node Manager、sandboxd/runc；用户镜像预装 RRT、HTTP/WS 服务和 OpenSSH。16 组业务检查及清理检查通过，结束后后端实例清空。inline 使用独立的签名校验测试 IAM；本次不是生产 IAM、多机或 Kubernetes 验收。

- Rust CLI：Template 发布/读取，Environment 三页查询、跨 Gateway get/delete，连接已有 Environment 的 SSH 终端，自动生成 Environment 的 stdout 提示及同 ID 重连；验证真实终端输出和后端退出码。
- 受管访问：HTTP/WS 流量触发创建，响应头返回 ID/URN，跨 Gateway 并发访问保持身份，WS 回显，租户隔离和认证不回退。
- inline：一次 POST 创建、另一 Gateway 查询，通过 instance/URN 访问 HTTP、保留原后端握手路径的 WS、原生 OpenSSH 终端、跨 Gateway 删除和重复删除；验证 query/Cookie/subprotocol 凭据、业务 Host/query、IAM 签名/角色/租户拒绝及 HTTP/WS 协议匹配。

通过范围包含调用方使用原 Environment ID 重试，不表示冷启动首请求必定成功。本轮 HTTP/WS 首请求曾返回 503，SSH 曾在通知 ID 后遇到路由尚未发布；原 ID 重试后通过。inline 创建期间还观察到业务 HTTP 返回 200 而 RRT 控制接口短暂超时，随后原实例进入 Running；此前同类 RRT 控制接口超时也曾导致 Platform 报告 `capsule start timed out`，本次没有修复该平台问题。首轮额外构造的未激活元数据删除仍受上述平台删除确认限制；不将其计为通过项。未修改 Platform 源码，未在 ADX 增加健康轮询或恢复控制器。

上述端到端二进制构建自本次业务规则下沉前的提交 `3408166`，构建来源、两轮结果和脱敏日志保存在本地 `out/cli-inline-real-e2e-20260922/`；验证脚本和报告不提交。HTTP CLI、inline 列表及 exec/files 尚未实现，不属于本次通过范围。

组件与静态检查另行验证：最后管理入口修复批次中 Agent API/Activator 21 项、Gateway 全功能 114 项、API Server 30 项及配置边界补测通过；CLI 6 项、原生 OpenSSH 组件用例、真实 Redis 索引/分页与双内嵌副本用例在对应批次通过。这些批次的 workspace 格式和全 targets/features 严格 Clippy 已通过。

2026-09-23 业务规则下沉后：Agent API/Activator 23 项、Gateway 全功能 114 项及显式原生 OpenSSH 用例通过，workspace 格式和严格 Clippy 通过。两组新增业务测试覆盖身份生成/复用和 service 唯一选择；Gateway 保留协议集成验证，移除重复的 UUID 格式断言。原生 SSH 使用真实连接与模拟 Platform，本轮未重跑完整平台端到端或 Redis 集成；日志位于本地 `out/agent-boundary-20260923/`。

运行 `make agent-test`。真实 Redis 测试将 `ADX_AGENT_TEST_REDIS_URL` 指向一次性数据库并显式使用 `--ignored`，测试会留下独立命名空间记录。组件测试不能替代真实部署验证。
