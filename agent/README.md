# Agent-DX v2

Agent 层使用 Rust，产品 API 嵌入统一 Gateway。Activator 可独立部署，也可与 API Server 和 Ingress 共进程；多个副本使用相同产品状态命名空间。

| 目录 | 职责 |
| --- | --- |
| `crates/core` | 产品类型、协议、Sandbox 能力边界及通用校验 |
| `crates/store` | Template/Environment 元数据、Redis 原子条件写；内存实现仅供测试 |
| `activator` | 产品管理与稳定身份激活，调用 Sandbox 接口，无后台健康/恢复扫描 |
| `api` | inline 规格适配、Activator HTTP 客户端或本地模块调用、Environment 身份选择、服务选择 |
| `cli` | Rust 用户命令行 `adx`；Template 发布/查询、Environment 分页查询/删除、HTTP 流式调用、SSH 交互终端 |

Environment 对应一个稳定逻辑 Sandbox ID。指定 Environment 的首次访问会幂等创建元数据，再激活 Sandbox；多副本复用同一身份。无效模板或未声明的 service 不创建 Environment。Sandbox 状态、健康、暂停/恢复与运行载体 0–1 由 Platform 保证。产品删除确认后，新访问可以重建同名 Environment，generation 隔离旧生命周期。

公开 API、认证、路由和连接实现位于 Gateway。HTTP/WS/SSH 通过普通方法调用 Agent 的 `ManagedService`，共用 Environment 身份选择和 service 唯一匹配规则；身份选择本身不创建元数据或 Sandbox。HTTP 响应头、SSH 终端提示与协议转发仍由 Gateway 处理。

Gateway 对同一次 HTTP/WS 请求的内部重试固定首次选定的 generation，并强制绕过 Activator 的成功激活缓存。删除或同名重建后，旧请求的重试返回冲突，不触发重建或选择新的生命周期。

用户 Harness 由 Execd 启动，自行定义业务接口。HTTP/WS/SSH 保持透明转发，不限制业务并发。inline create/get/list/kill 和 exec/files 是独立的旧协议适配入口，直接适配 Sandbox，独立于 v2 管理接口、Environment、Activator 和 ADX Redis。

## 部署

```sh
cargo build --locked -p data-plane-gateway -p adx-apiserver --features data-plane-gateway/agent-api --bins
```

Gateway 保留独立 Ingress 与 API Server 内嵌 Ingress 两种部署形态，共用进程装配代码。构建时为承载 Ingress 的二进制启用 `data-plane-gateway/agent-api`。两套公开接口可以分别启用，路由、配置与认证分开：

- `ADX_AGENT_CONFIG` 指向 v2 配置文件，选择独立 Activator 地址或嵌入式 Activator 的 Redis 命名空间。受管管理和 HTTP/WS 数据入口使用 Platform API Key。
- `ADX_INLINE_CONFIG` 指向 inline 兼容配置文件；装配 create/get/list/kill 和 exec/files 管理接口，使用独立 JWT/IAM 认证，并要求 `ADX_SANDBOX_CONFIG` 提供 Sandbox 能力。

选择独立模式时，另行构建并启动无状态 `adx-activator` 进程；多个副本连接相同 ADX Redis namespace，并通过 Sandbox HTTP 接口访问平台。嵌入式模式由 API Server 将同一 Sandbox 业务服务以 Rust 模块接口交给 Activator，受管 Agent 请求无需内部 HTTP/RPC 回环。

```sh
cargo build --locked -p adx-activator --bin adx-activator
```

独立模式 Gateway 配置如下。组件间令牌从环境变量读取；受管 Agent 入口本身不要求本机装配 Sandbox API，能力提供方由 Activator 的 `sandbox_url` 指定。启用 inline 时仍需本地 Sandbox 配置。

```json
{
  "timeout_seconds": 60,
  "activator": {
    "urls": ["https://activator.internal"],
    "token_env": "ADX_ACTIVATOR_SERVICE_TOKEN",
    "ca_path": "/etc/adx/ca.pem",
    "allow_plaintext": false
  }
}
```

API Server 内嵌 Ingress 时，也可将 `ADX_AGENT_CONFIG` 配成嵌入式模式；`ADX_SANDBOX_CONFIG` 提供预装 profile，`ADX_SANDBOX_EXECD_TOKEN` 供创建规格映射，Activator 产品状态仍存于指定 Redis namespace：

```json
{
  "timeout_seconds": 60,
  "embedded": {
    "redis_url": "redis://127.0.0.1:6379",
    "namespace": "adx-agent"
  }
}
```

`activator` 与 `embedded` 只能配置一个。独立 Ingress 不持有 API Server 的业务服务，应使用独立 Activator 模式。

`timeout_seconds` 默认 60 秒且必须为正，由入口确定一次绝对 deadline。管理面在鉴权后统一处理 trace、请求读取、业务调用和 JSON 响应；HTTP/WS 目标激活的模板查询、激活和内部目标重试共用同一期限。客户端将剩余预算传给 Activator，地址重选不重置期限。纯读超时返回 Unavailable，可能写入的操作超时返回 OutcomeUnknown，重试使用原身份；已到期的内部重试直接拒绝。网络连接及 Sandbox 后端配置上限只能缩短剩余预算。创建的内部 `deadline_unix_ms` 只随请求传输，不进入执行规格或 Redis；它从入口经 Activator/Sandbox 传递到 Platform RPC；发现 Coordinator、读取请求和上游查询已消耗的时间不会重新补回。内部 Sandbox HTTP 入口默认上限同为 60 秒；inline 仍受 `backend_timeout_seconds` 上限约束，Platform RPC 受 `rpc_timeout_seconds` 上限约束。激活期限不覆盖建立后的 HTTP 响应流、WS 或 SSH 会话；SSH 模板校验、身份提示和后端握手共用 SSH 连接期限。

Gateway 与 Activator 都缓存不可变模板，按 tenant/name/version 隔离，各最多 1024 条；同键在途读取合并，缺失和失败不缓存。Gateway 不缓存 Env/Target，每次解析都调用 Activator。Activator 的 Env 成功绑定采用 LRU，默认容量 200000，滑动 TTL 为 18000 秒（5 小时）；热命中直接返回并续期，不读 Redis、不调用 Sandbox API。TTL 只在请求访问时检查，没有 Env 后台刷新或过期扫描；容量淘汰、闲置过期、重启和 bypass 会引起回源。缓存是派生状态，实际连通性由 Gateway/Relay 校验；删除中的并发请求允许成功或失败。Activator 部署与缓存内存测量见 [进程说明](activator/README.md)，权威状态保证见 [存储说明](crates/store/README.md)。

独立模式经 ActivatorClient 调用远程进程；嵌入式模式经 LocalControl 调用同进程 Activator，复用上述缓存及 bypass 语义。嵌入式 Activator 使用默认 200000 条、5 小时配置，当前没有独立的缓存配置入口。

### Activator 发现与 Env 亲和路由

静态 `activator.urls` 中的每个地址必须直达一个实例，不能是随机负载均衡到多个实例的地址。动态部署可以用 `activator.discovery` 替代 `urls`，两者互斥：

```json
{
  "timeout_seconds": 60,
  "activator": {
    "discovery": {
      "redis_url": "redis://redis.internal:6379/0",
      "namespace": "adx-production",
      "refresh_seconds": 5
    },
    "token_env": "ADX_ACTIVATOR_SERVICE_TOKEN",
    "ca_path": "/etc/adx/ca.pem",
    "allow_plaintext": false
  }
}
```

Activator 在相同 Redis namespace 注册实例 ID 和可直达地址。Gateway 启动后立即异步读取成员列表，默认随后约每 5 秒刷新（±20% 抖动），`refresh_seconds` 范围 1–300。请求仅使用本地快照，不同步查 Redis。刷新失败保留上次快照；成功读取空列表则停止选路；首次尚无列表时受管请求返回 Unavailable。Gateway 仅使用 Store 的注册发现客户端，不读取或修改 Template/Environment 元数据。

完整 Env scope（tenant/template/version/environment_id）按固定 SHA-256 Rendezvous Hash 排序实例。所有 Gateway 使用相同成员集合时选择相同实例，成员顺序不影响结果；静态模式用规范化地址作实例 ID。resolve、bypass、单 Env 查询和删除使用相同选择规则，generation 不参与选路。成员变更只重新分配受影响的 Env；短暂列表差异允许重复缓存，身份隔离仍由权威元数据和 generation 保证。只有连接失败且请求未发送时才尝试排名第二的实例，共用原 deadline，最多两个地址；响应超时或结果未知不自动重放。无单个 Env 的模板管理/列表请求仍轮询实例。

注册发现与 Env 亲和用于独立 Activator 模式；嵌入式模式直接调用本进程 Activator，不注册成员、不做跨进程选路，也不保证相同 Env 总是落到同一个进程；多个 API Server 可能各自缓存同一 Env。注册发现不改变 Platform 的发现机制，也不增加 Env 生命周期控制器。独立模式全热解析为 **1 次 Gateway→Activator、0 次 Agent Redis、0 次 Sandbox API**；后台成员刷新与实例续租单独计费，不属于单次解析访问。

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

inline 生命周期路径为 `POST/GET /api/agent`、`GET/DELETE /api/agent/{id}`。按原 Frontend 管理协议依次从 `X-Auth`、query `token`、`iam_token` Cookie 取凭据，要求 JWT 的 `developer` 角色和有效租户，再由 IAM 验证原始 token；不缓存验证结果。调用 Sandbox 的租户取自已验证的 JWT `sub`，不信任客户端的 `tenant_id`。管理入口继续要求 TLS；认证失败返回 HTTP 401 和字符串 `error`。两套认证不互相回退，Platform API Key 不能代替 inline JWT。

inline 另提供 `POST /api/agent/{id}/exec`、`POST .../files/upload`、`GET .../files/download`、`GET .../files/list`、`POST .../files/mkdir`，经过相同管理鉴权，通过已有 Relay 连接调用 Execd。上传接收 multipart，path 必须在 file 之前；下载按字节流返回，支持单 Range。文件权限、mkdir 非递归等当前平台尚不支持的语义明确拒绝，详细范围与生命周期兼容差异见 [平台缺口清单](PLATFORM_GAPS.md)。HTTP/WS 已接入下述统一入口；CLI 只使用 v2 接口。

create 的真实 Sandbox ID 采用原 Frontend namespace/name UUIDv5 规则，重试和跨 Gateway 调用保持同一 ID。列表读取 Platform 目录首帧快照后关闭订阅，不新增 ADX 索引或常驻 watch；按认证租户筛选 Running 的 ADX 实例。详情由平台记录及匹配的预装 profile 组成，不返回内部 Execd 凭据；无法还原的旧字段不伪造。

受管接口使用 `/api/agent/v2/templates/{name}/versions/{version}/environments/{id}`，GET/DELETE 对应查询和删除；Environment 由访问流量触发创建。`POST .../{id}/resolve` 接受 `{protocol, port?, bypasscache?}`；`bypasscache` 为布尔值，默认 false。true 强制当前 Activator 读取 Redis Environment 并查询 Sandbox，跳过 Env 成功绑定，不绕过不可变模板缓存、租户/service 校验或 generation 隔离。平台刷新失败、不就绪或请求被取消后，本次绑定不作为成功缓存保留；较早的在途响应不能覆盖更新的刷新结果。其他 Activator 的本地缓存不接收失效广播；普通热调用可能继续返回旧绑定，目标失效通过 Gateway 发送前 bypass 重试修正。重试固定原 generation，同名重建不会使原请求切换到新环境。

例如强制刷新 HTTP 目标：

```json
{"protocol": "http", "port": 8080, "bypasscache": true}
```

该参数仅属于 resolve 的 JSON 请求和内部激活请求，不作为 `/agent/http`、`/agent/ws` 的路由 query 参数。HTTP/WS 在业务请求尚未发送时的既有一次目标重试自动携带 bypasscache=true；不会重放结果未知的业务请求。HTTP/WS 通过下面的统一入口触发激活；共享转发使用真实 Sandbox ID。

`GET /api/agent/v2/templates/{name}/versions/{version}/environments` 返回 `environments` 和 `next_page_token`。可传 `page_size`（默认 50，范围 1–100）和 `page_token`；token 绑定认证租户、模板与版本。列表仅返回产品元数据，包含删除中的记录，不查询平台健康状态；并发创建/删除期间不保证分页快照。模板不存在时返回 404。

Rust CLI 的当前管理能力与配置见 [CLI 使用说明](cli/README.md)。HTTP CLI 支持方法、Header、文件/stdin 请求体、逐块响应和 stdout Environment 通知；SSH CLI 使用系统 OpenSSH。

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

响应体按流转发，WS 升级后使用现有双向字节通道，不增加业务 envelope 或 WS 消息。inline 路由/连接失败返回 502、非可连接状态返回 409、租户无权访问返回 403。Relay 连接和路由仍由原 Gateway 数据面管理。

## SSH 交互终端

设置 `ADX_SSH_CONFIG` 后，独立 Ingress 或 API Server 内嵌 Ingress 会启动独立 SSH 监听端口。原实例使用路由用户名 `yr:instance:<id>[:port=<port>][:trace=<id>]`，受管目标使用 `adx:target:<百分号编码的完整URN>[:port=<port>][:trace=<id>]`。两种目标共用 listener，分别检查配置中的公钥授权表；路由用户名不是租户或后端 Linux 用户。

SSH 首版只提供一个连接一个交互式终端，要求 PTY 和 shell 请求，支持输入输出、窗口调整、退出码与连接关闭。Environment 提示在鉴权完成、交互 shell 建立后通过 stdout 显示，然后激活并连接后端；它通知选定的身份，不表示 Sandbox 已就绪。省略 env 时当前连接只生成一个 ID；携带返回的 Environment URN 可再次访问。仅认证或打开 channel 不创建资源。inline 直接定位实例，默认端口 22，不调用 Activator，不生成 Environment。

后端复用共享 L4 connector → Relay → Sandbox sshd；没有新建数据通道或本地目标缓存。使用未修改的 russh 0.63.3，不发送认证 Banner。首版不提供 SSH 远程 exec、SFTP 或端口转发。已有 Sandbox-ID SSH/CONNECT 转发入口保持原有行为。

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

两张客户端授权表独立匹配，不互相回退，至少配置一张。普通租户只能访问自己实例；inline 的 system 租户 `0` 可访问其他租户实例，受管分支仍严格匹配租户。SSH 使用公钥认证，不读取 JWT 或 Platform API Key。网络来源限制复用现有 Ingress client ACL。

部署时预置 Gateway 主机私钥，把它的公钥通过可信渠道加入客户端 known_hosts；将 backend_key 对应公钥加入镜像中 backend_user 的 authorized_keys，把镜像 sshd 的主机公钥加入 backend_host_keys。后端主机密钥轮换时先加入新公钥，再更新镜像，最后移除旧公钥。私钥文件应限制读取权限，不放进 Template 或 Redis。镜像需预装 sshd 并声明对应 service，不要求修改 Platform。

Template 的 service 声明 HTTP/WS/SSH 协议及端口，例如 `[{"protocol":"http","port":8080},{"protocol":"ws","port":8080},{"protocol":"ssh","port":22}]`。受管 SSH 无端口参数时必须恰好匹配一个 SSH service。用户直接调用 Harness 自定义接口，审计轨迹能力暂缓。

预装镜像、Execd 与匹配的启动 profile 仍可用于验证。当前 Sandbox 适配不能证明从未观察到的创建已被取消；这类删除返回结果未知并保留产品记录。ADX 不通过删除元数据掩盖平台副作用。pause/resume 完全由 Platform 负责，不属于 ADX 适配范围。首版按 Running 视为服务已就绪；当前 adxlet 创建路径会检查 Execd 控制状态并激活节点路由，但用户业务端口及 Gateway 路由传播仍不具有自定义健康检查保证；这些限制记为 Platform 能力缺口，ADX 不补探测。

流量激活时会先保存稳定身份，再提交平台创建。创建失败、响应丢失或进程中断可能留下尚未激活成功的 Environment；该记录供原身份重试，并非独立的创建功能。如果平台从未观察到该实例，删除仍可能保留 Deleting 记录，无法依靠重复删除确认完成。冷启动还可能遇到 Running 与共享路由发布之间的短暂窗口；HTTP/WS 或 SSH 已返回 Environment 身份后，调用方应使用该身份重试，不能把省略 env 的新请求当作原请求重试。

## 验证

缓存改动接入上游 `9d75a6c` 后，`make agent-test JOBS=2` 为 163 项通过、11 项默认忽略，API Server lib 测试 18 项通过；两者合计 181 项通过。新增 LocalControl 回归先确认 bypass 被忽略会失败，再验证透传后热命中、强制回源失败失效及恢复通过；这 3 项 local 测试已包含在 Agent 总数中。`make rust-check JOBS=2` 的格式、全 workspace 严格 Clippy 和既定 unwrap 检查全部通过。独立模式保留 Env 亲和，内嵌模式仅本地调用，没有 Env 稳定选路。日志在本地 `out/env-cache-pr-validation/`；集成后未重新执行真实 Platform 端到端或性能压测，以下数据保留原始构建边界。

2026-09-24，缓存与 Env 亲和路由改动通过 `make agent-test JOBS=2`（160 项通过、11 项默认忽略）及 `make rust-check JOBS=2`（全 workspace 格式、all-targets/all-features 严格 Clippy 和 unwrap 策略检查）。另行使用一次性 Redis 执行 9 项去重专项用例，覆盖 Store 条件写/重连、注册租约过期与 incarnation 隔离、Gateway 发现变化/失败保留快照，以及心跳续租/退出注销，全部通过。全热零 Redis/零 Sandbox API、Gateway 单次 Activator RPC、滑动 TTL、LRU、bypass 与 generation 隔离由组件测试验证。该阶段完整日志保存在本地 `out/env-affinity-validation/`。默认容量随后调整为 200000，5 小时滑动 TTL 不变；8 项缓存聚焦用例通过，真实 LRU 的 20 万条样例 RSS 增量约 167.1 MiB，测量口径见 Activator 文档。

同日在接入上游 `9d75a6c` 前，基于 `4d822f6` 加缓存改动的 release 构建完成真实 Platform 端到端验证：一轮 16/16 项通过；重复一轮 12/16 项通过，4 项受同一个 Sandbox 冷启动终态 Failed 影响。两轮缓存专项均 5/5 通过：热 resolve 的 Sandbox/Redis 元数据访问为零，bypass 每次 1 次 Sandbox GET 和 2 次 Env 读取，同名重建固定 generation，以及 Activator 故障切换/租约过期/重新加入均符合契约。现场再次观察到 Harness HTTP 200 而 Execd 控制端点超时，冷启动可靠性不能按全通过报告，边界见 [平台缺口](PLATFORM_GAPS.md)。证据在本地 `out/env-cache-baseline-20260924/`；这是单节点真实容器验证，不是多机或 Kubernetes 集群验收。文档检查仍有两处既有 `agentostesting/810` 报告的 JSON 示例错误，新配置示例及文档链接通过检查。

同一 release 构建的稳态负载基线使用单节点 4 CPU/6 GiB、独立负载端 2 CPU/2 GiB、8 个真实 Env、两个 Activator。10 个阶段均零错误：32 并发热 resolve 约 31283 次/秒、p95 1.34 ms；同并发 bypass 约 4434 次/秒、p95 23.03 ms。128 并发热 resolve 两次约 30252/30315 次/秒，512 并发约 28292 次/秒、p95 41.09 ms。1024 条 WebSocket 全部建立，10 Hz/连接的 echo 负载约 9891 消息/秒、p95 18.85 ms。热路径的 Sandbox API 调用均为零。32 并发 HTTP 短连接约 1410 次/秒，但负载端已接近 2 核上限；所有数字是此配置基线，不是集群极限，未覆盖 20 万活跃 Env 或长期淘汰压力。原始数据及完整口径在本地 `out/env-cache-baseline-20260924/report.md` 和 `real4/performance.json`。容量调整后的 `make rust-check JOBS=2` 也已通过。

同日新 Env 创建补测（镜像已缓存，并发 2）为 4/8 最终成功、4/8 Platform 启动超时；首响应 7 次 503、1 次 502，成功就绪耗时 2.922–118.884 秒，8 个 Env 最终删除均成功。补测已把夹具的 sandboxd 实例上限从 8 调为 32，排除了此前被 8 个预热 Env 占满的配置限制；本轮仍有真实启动超时，不能按冷启动可靠性通过报告。补测前 8 个预热候选中另有 3 个启动失败，全部失败证据保留。测试容器/网络已清理，补测清理错误为零。

合并上游 `e295f89` 后，Activator、Agent API、CLI、Agent core、Gateway 与 API Server 的聚焦测试共 199 项通过、2 项默认忽略；另行执行原生 OpenSSH 用例并通过，验证终端 ID/URN 输出和退出码。CLI 参数测试覆盖 OpenSSH 的固定 `ControlMaster` 选项。全 workspace 格式与全 targets/features 严格 Clippy 通过，提交文档检查通过。这轮为组件与原生客户端 socket 验证，未重跑完整 Platform 端到端或真实 Redis 专项；完整日志在本地 `out/pr27-rebase-20260923/`。

2026-09-23，合并上游组件命名调整前的 `3141e27` 完成以下验证：Agent API、Activator、CLI、Agent core 与 Gateway 聚焦测试 167 项通过、2 项忽略；workspace 格式及全 targets/features 严格 Clippy 通过。双独立 Activator 与真实 Redis 的集成覆盖包含在下述端到端验证中。拟提交文档及补丁检查通过；工作区文档检查另报告两处用户自验报告中的非标准 JSON 示例，这些报告不在提交范围内。当时的生产依赖检查确认 Agent API 仅通过 HTTP 客户端调用 Activator；当前新增 Store 的注册发现客户端依赖，元数据访问仍在 Activator。

上述提交的真实容器验证使用两个 Gateway Edge（现名 Ingress）、两个独立 Activator 进程和真实 Redis/Platform/RRT，两个 Activator 的 readiness 均为 204。19 项端到端检查全部通过，覆盖模板与 Environment 管理、自动 ID 回传、跨 Gateway 身份复用、HTTP/WS/SSH、Rust HTTP/SSH CLI、租户隔离，以及 inline create/get/list/kill、exec、mkdir、上传提交、下载 Range 和文件列表。

该轮对部分冷启动响应的 NotReady/OutcomeUnknown，在测试用例中保留原 Environment/Sandbox ID 做有界退避查询后继续；未增加 ADX 等待，也未通过更换身份重建掩盖失败。现场确认当时的 RRT（现名 Execd）入口监视线程持锁休眠，会阻塞启动完成和控制 HTTP；根因、Platform 中央创建期限传递缺口及修复边界见 [平台缺口](PLATFORM_GAPS.md)。全用例通过不等于冷启动首请求可靠性已验收。

验证容器和网络已清理，清理错误为零。完整日志、构建来源和匹配 Build ID 的 RRT 现场符号证据保存在本地 `out/adx-e2e-reliability-20260923/`；脚本和报告不提交。Platform 源码未修改，预装 Execd、动态挂载和自定义健康检查仍按平台缺口暂缓。

运行 `make agent-test`。真实 Redis 测试将 `ADX_AGENT_TEST_REDIS_URL` 指向一次性数据库并显式使用 `--ignored`，测试会留下独立命名空间记录。组件测试不能替代真实部署验证。
