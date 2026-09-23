# Agent-DX v2

Agent 层使用 Rust，产品 API 嵌入统一 Gateway。无状态 Activator 独立部署，可运行多个副本。

| 目录 | 职责 |
| --- | --- |
| `crates/core` | 产品类型、协议、Sandbox 能力边界及通用校验 |
| `crates/store` | Template/Environment 元数据、Redis 原子条件写；内存实现仅供测试 |
| `activator` | 产品管理与稳定身份激活，调用 Sandbox 接口，无后台健康/恢复扫描 |
| `api` | inline 规格适配、Activator HTTP 客户端、Environment 身份选择、服务选择 |
| `cli` | Rust 用户命令行 `adx`；Template 发布/查询、Environment 分页查询/删除、HTTP 流式调用、SSH 交互终端 |

Environment 对应一个稳定逻辑 Sandbox ID。指定 Environment 的首次访问会幂等创建元数据，再激活 Sandbox；多副本复用同一身份。无效模板或未声明的 service 不创建 Environment。Sandbox 状态、健康、暂停/恢复与运行载体 0–1 由 Platform 保证。产品删除确认后，新访问可以重建同名 Environment，generation 隔离旧生命周期。

公开 API、认证、路由和连接实现位于 Gateway。HTTP/WS/SSH 通过普通方法调用 Agent 的 `ManagedService`，共用 Environment 身份选择和 service 唯一匹配规则；身份选择本身不创建元数据或 Sandbox。HTTP 响应头、SSH 终端提示与协议转发仍由 Gateway 处理。

Gateway 对同一次请求的内部重试固定首次选定的 generation。删除或同名重建后，旧请求的重试返回冲突，不触发重建或选择新的生命周期。

用户 Harness 由 Execd 启动，自行定义业务接口。HTTP/WS/SSH 保持透明转发，不限制业务并发。inline create/get/list/kill 和 exec/files 是独立的旧协议适配入口，直接适配 Sandbox，独立于 v2 管理接口、Environment、Activator 和 ADX Redis。

## 部署

```sh
cargo build --locked -p data-plane-gateway -p adx-apiserver --features data-plane-gateway/agent-api --bins
```

Gateway 保留独立 Ingress 与 API Server 内嵌 Ingress 两种部署形态，共用进程装配代码。构建时为承载 Ingress 的二进制启用 `data-plane-gateway/agent-api`。两套公开接口可以分别启用，路由、配置与认证分开：

- `ADX_AGENT_CONFIG` 指向 v2 配置文件，配置独立 Activator 的访问地址。受管管理和 HTTP/WS 数据入口使用 Platform API Key。
- `ADX_INLINE_CONFIG` 指向 inline 兼容配置文件；装配 create/get/list/kill 和 exec/files 管理接口，使用独立 JWT/IAM 认证，并要求 `ADX_SANDBOX_CONFIG` 提供 Sandbox 能力。

另行构建并启动无状态 `adx-activator` 进程；多个副本连接相同 ADX Redis namespace，并通过 Sandbox HTTP 接口访问平台。

```sh
cargo build --locked -p adx-activator --bin adx-activator
```

Gateway 配置如下。组件间令牌从环境变量读取；受管 Agent 入口本身不要求本机装配 Sandbox API，能力提供方由 Activator 的 `sandbox_url` 指定。启用 inline 时仍需本地 Sandbox 配置。

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

`timeout_seconds` 默认 60 秒且必须为正，由入口确定一次绝对 deadline。管理面在鉴权后统一处理 trace、请求读取、业务调用和 JSON 响应；HTTP/WS 目标激活的模板查询、激活和内部目标重试共用同一期限。客户端将剩余预算传给 Activator，地址重选不重置期限。纯读超时返回 Unavailable，可能写入的操作超时返回 OutcomeUnknown，重试使用原身份；已到期的内部重试直接拒绝。网络连接及 Sandbox 后端配置上限只能缩短剩余预算。创建的内部 `deadline_unix_ms` 只随请求传输，不进入执行规格或 Redis；它从入口经 Activator/Sandbox 传递到 Platform RPC；发现 Coordinator、读取请求和上游查询已消耗的时间不会重新补回。内部 Sandbox HTTP 入口默认上限同为 60 秒；inline 仍受 `backend_timeout_seconds` 上限约束，Platform RPC 受 `rpc_timeout_seconds` 上限约束。激活期限不覆盖建立后的 HTTP 响应流、WS 或 SSH 会话；SSH 模板校验、身份提示和后端握手共用 SSH 连接期限。

Gateway 不缓存 Template 或 Target，查询和获取目标均调用 Activator。Gateway 通过 ActivatorClient 调用独立 Activator，不连接 ADX 状态 Redis。Activator 持有产品状态并调用 Sandbox 接口；Platform 自身的 Redis 发现不受影响。Activator 部署见 [进程说明](activator/README.md)，状态保证见 [存储说明](crates/store/README.md)。

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

受管接口使用 `/api/agent/v2/templates/{name}/versions/{version}/environments/{id}`，GET/DELETE 对应查询和删除；Environment 由访问流量触发创建。`POST .../{id}/resolve` 接受 `{protocol, port?}`。HTTP/WS 通过下面的统一入口触发激活；共享转发使用真实 Sandbox ID。

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

合并上游 `e295f89` 后，Activator、Agent API、CLI、Agent core、Gateway 与 API Server 的聚焦测试共 199 项通过、2 项默认忽略；另行执行原生 OpenSSH 用例并通过，验证终端 ID/URN 输出和退出码。CLI 参数测试覆盖 OpenSSH 的固定 `ControlMaster` 选项。全 workspace 格式与全 targets/features 严格 Clippy 通过，提交文档检查通过。这轮为组件与原生客户端 socket 验证，未重跑完整 Platform 端到端或真实 Redis 专项；完整日志在本地 `out/pr27-rebase-20260923/`。

2026-09-23，合并上游组件命名调整前的 `3141e27` 完成以下验证：Agent API、Activator、CLI、Agent core 与 Gateway 聚焦测试 167 项通过、2 项忽略；workspace 格式及全 targets/features 严格 Clippy 通过。双独立 Activator 与真实 Redis 的集成覆盖包含在下述端到端验证中。拟提交文档及补丁检查通过；工作区文档检查另报告两处用户自验报告中的非标准 JSON 示例，这些报告不在提交范围内。生产依赖检查确认 Agent API 仅通过 HTTP 客户端调用 Activator，Activator/Store 只作为该 crate 的测试依赖。

上述提交的真实容器验证使用两个 Gateway Edge（现名 Ingress）、两个独立 Activator 进程和真实 Redis/Platform/RRT，两个 Activator 的 readiness 均为 204。19 项端到端检查全部通过，覆盖模板与 Environment 管理、自动 ID 回传、跨 Gateway 身份复用、HTTP/WS/SSH、Rust HTTP/SSH CLI、租户隔离，以及 inline create/get/list/kill、exec、mkdir、上传提交、下载 Range 和文件列表。

该轮对部分冷启动响应的 NotReady/OutcomeUnknown，在测试用例中保留原 Environment/Sandbox ID 做有界退避查询后继续；未增加 ADX 等待，也未通过更换身份重建掩盖失败。现场确认当时的 RRT（现名 Execd）入口监视线程持锁休眠，会阻塞启动完成和控制 HTTP；根因、Platform 中央创建期限传递缺口及修复边界见 [平台缺口](PLATFORM_GAPS.md)。全用例通过不等于冷启动首请求可靠性已验收。

验证容器和网络已清理，清理错误为零。完整日志、构建来源和匹配 Build ID 的 RRT 现场符号证据保存在本地 `out/adx-e2e-reliability-20260923/`；脚本和报告不提交。Platform 源码未修改，预装 Execd、动态挂载和自定义健康检查仍按平台缺口暂缓。

运行 `make agent-test`。真实 Redis 测试将 `ADX_AGENT_TEST_REDIS_URL` 指向一次性数据库并显式使用 `--ignored`，测试会留下独立命名空间记录。组件测试不能替代真实部署验证。
