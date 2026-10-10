# Agent-DX v2

Agent 层使用 Rust，产品 API 嵌入统一 Gateway。Activator 可独立部署，也可与 API Server 和 Ingress 共进程；多个副本使用相同产品状态命名空间。

| 目录 | 职责 |
| --- | --- |
| `crates/core` | 产品类型、协议、Sandbox 能力边界及通用校验 |
| `crates/store` | Template/AgentBinding 元数据、Redis 原子条件写；内存实现仅供测试 |
| `activator` | 产品管理与稳定身份激活，调用 Sandbox 接口，无后台健康/恢复扫描 |
| `api` | Activator HTTP 客户端或本地模块调用、AgentBinding 身份选择、服务选择 |
| `cli` | Rust 用户命令行 `adx`；Template 发布/查询、AgentBinding 分页查询/删除、HTTP 流式调用、SSH 交互终端 |

AgentBinding 对应一个稳定逻辑 Sandbox ID。指定 AgentBinding 的首次访问会幂等创建元数据，再激活 Sandbox；多副本复用同一身份。无效模板或未声明的 service 不创建 AgentBinding。Sandbox 状态、健康、暂停/恢复与运行载体 0–1 由 Platform 保证。产品删除确认后，新访问可以重建同名 AgentBinding，generation 隔离旧生命周期。

公开 API、认证、路由和连接实现位于 Gateway。HTTP/WS/SSH 通过普通方法调用 Agent 的 `ManagedService`，共用 AgentBinding 身份选择和 service 唯一匹配规则；身份选择本身不创建元数据或 Sandbox。HTTP 响应头、SSH 终端提示与协议转发仍由 Gateway 处理。

Gateway 对同一次 HTTP/WS 请求的内部重试固定首次选定的 generation，并强制绕过 Activator 的成功激活缓存。删除或同名重建后，旧请求的重试返回冲突，不触发重建或选择新的生命周期。

用户 Harness 由 Execd 启动，自行定义业务接口。HTTP/WS/SSH 保持透明转发，不限制业务并发。Ingress 的 Inline 管理、exec/files 和旧 JWT/IAM 适配已移除；Sandbox 生命周期由 API Server 管理。

## 镜像启动与 Sandbox 配置

Agent 模板使用业务镜像的 ENTRYPOINT/CMD、WORKDIR 和 USER，不接受 `entrypoint` 或 `working_dir` 覆盖。创建规格设置 `inherit_entrypoint=true`；Platform 注入部署侧 runtime 并生成进程配置，Execd 据此启动业务进程。业务镜像不需要预装 Execd 或额外进程 JSON。模板工作目录覆盖和持久挂载暂不支持。

```json
{"name":"assistant","version":"1","image":"registry.example/assistant:v1","isolation_runtime":"runc","env":{"APP_MODE":"production"},"resources":{"cpu_millis":1000,"memory_mib":1024},"service":[{"protocol":"ws","port":8080}]}
```

独立 Activator 的 `sandbox_url` 必须指向 API Server 管理监听地址。创建调用 `POST /api/sandbox/v1/sandboxes`，查询调用 `GET /api/instances?instance_id=...`，删除调用 `DELETE /api/sandbox/v1/sandboxes/{id}`。稳定 ID `adx-<generation>` 映射为 namespace=`adx`、name=`<generation>`。每个租户配置自己的 Platform API Key 环境变量，详见 [Activator 配置](activator/README.md)。创建时的 runtime 注入由 API Server 部署配置负责。

嵌入式 Activator 使用 Agent 侧 `LocalSandbox`，与独立 `HttpSandbox` 共用 `sandbox_request` 将 Agent 执行规格转成 Sandbox 请求。API Server 的 `SandboxService.create_request()` 统一校验请求、转换 EnvironmentSpec 并注入自身配置的 runtime profile。文件访问由 Ingress 通用 `sandbox_files` 能力处理，独立模式复用 API Server 实例查询，嵌入式复用 SandboxService；Jiuwen 不读取 Execd 凭据。旧 PlatformSandbox、EnvironmentRequestMapper 和 runtime 查询已删除，Ingress 不再提供 `/api/sandbox/v2/instances`。部署需配置 `ADX_SANDBOX_FILES_CONFIG`，见 [Gateway 文件访问配置](../gateway/README.md#common-sandbox-file-access)。本地组件测试已覆盖新创建规格的准入；独立部署已验证真实新建 Sandbox 和跨副本上传下载。跨进程配置一致性仍需由部署者保证。

Agent 模板无需逐镜像注册预装 profile。Sandbox Running 仍不等于业务端口通过自定义就绪检查。

## 部署

```sh
cargo build --locked -p data-plane-gateway -p adx-apiserver --features data-plane-gateway/agent-api --bins
```

Gateway 保留独立 Ingress 与 API Server 内嵌 Ingress 两种部署形态，共用进程装配代码。构建时为承载 Ingress 的二进制启用 `data-plane-gateway/agent-api`。Agent 入口按以下方式启用：

- `ADX_AGENT_CONFIG` 指向 v2 配置文件，选择独立 Activator 地址或嵌入式 Activator 的 Redis 命名空间。受管管理和 HTTP/WS 数据入口使用 Platform API Key。

选择独立模式时，另行构建并启动无状态 `adx-activator` 进程；多个副本连接相同 ADX Redis namespace，并通过 API Server 既有 HTTP 接口访问平台。嵌入式模式由 API Server 将同一 Sandbox 业务服务以 Rust 模块接口交给 Activator，受管 Agent 请求无需内部 HTTP/RPC 回环。

```sh
cargo build --locked -p adx-activator --bin adx-activator
```

独立模式 Gateway 配置如下。组件间令牌从环境变量读取；受管 Agent 入口不要求本机装配 Sandbox 后端，Activator 的 `sandbox_url` 指向 API Server。

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

API Server 内嵌 Ingress 时，也可将 `ADX_AGENT_CONFIG` 配成嵌入式模式；创建使用 API Server 自身的 `runtime_profile`，Activator 产品状态仍存于指定 Redis namespace：

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

`timeout_seconds` 默认 60 秒且必须为正，由入口确定一次绝对 deadline。管理面在鉴权后统一处理 trace、请求读取、业务调用和 JSON 响应；HTTP/WS 目标激活的模板查询、激活和内部目标重试共用同一期限。客户端将剩余预算传给 Activator，地址重选不重置期限。纯读超时返回 Unavailable，可能写入的操作超时返回 OutcomeUnknown，重试使用原身份；已到期的内部重试直接拒绝。网络连接及 Sandbox 后端配置上限只能缩短剩余预算。创建的 `deadline_unix_ms` 不进入执行规格或 Redis。独立 Activator 用剩余期限限制 HTTP 等待，但现有 API Server 没有该绝对期限字段；创建请求中的 `createTimeoutSeconds` 固定取 Activator 配置，保持相同身份重试的请求体与操作 ID 稳定。API Server 可能在调用方超时后继续创建，超时返回 OutcomeUnknown，不自动重放或换 ID。嵌入式 `LocalSandbox` 同样按剩余期限限制调用等待，固定创建预算为 60 秒；提交后超时返回 OutcomeUnknown。两种模式使用相同的创建请求和确定性操作 ID。激活期限不覆盖建立后的 HTTP 响应流、WS 或 SSH 会话；SSH 模板校验、身份提示和后端握手共用 SSH 连接期限。

Gateway 与 Activator 都缓存不可变模板，按 tenant/name/version 隔离，各最多 1024 条；同键在途读取合并，缺失和失败不缓存。Gateway 不缓存 AgentBinding/Target，每次解析都调用 Activator。Activator 的 AgentBinding 成功绑定采用 LRU，默认容量 200000，滑动 TTL 为 18000 秒（5 小时）；热命中直接返回并续期，不读 Redis、不调用 Sandbox API。TTL 只在请求访问时检查，没有 AgentBinding 后台刷新或过期扫描；容量淘汰、闲置过期、重启和 bypass 会引起回源。缓存是派生状态，实际连通性由 Gateway/Relay 校验；删除中的并发请求允许成功或失败。Activator 部署与缓存内存测量见 [进程说明](activator/README.md)，权威状态保证见 [存储说明](crates/store/README.md)。

独立模式经 ActivatorClient 调用远程进程；嵌入式模式经 LocalControl 调用同进程 Activator，复用上述缓存及 bypass 语义。嵌入式 Activator 使用默认 200000 条、5 小时配置，当前没有独立的缓存配置入口。

### Activator 发现与 AgentBinding 亲和路由

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

Activator 在相同 Redis namespace 注册实例 ID 和可直达地址。Gateway 启动后立即异步读取成员列表，默认随后约每 5 秒刷新（±20% 抖动），`refresh_seconds` 范围 1–300。请求仅使用本地快照，不同步查 Redis。刷新失败保留上次快照；成功读取空列表则停止选路；首次尚无列表时受管请求返回 Unavailable。Gateway 仅使用 Store 的注册发现客户端，不读取或修改 Template/AgentBinding 元数据。

完整 AgentBinding scope（tenant/template/version/binding_id）按固定 SHA-256 Rendezvous Hash 排序实例。所有 Gateway 使用相同成员集合时选择相同实例，成员顺序不影响结果；静态模式用规范化地址作实例 ID。resolve、bypass、单 AgentBinding 查询和删除使用相同选择规则，generation 不参与选路。成员变更只重新分配受影响的 AgentBinding；短暂列表差异允许重复缓存，身份隔离仍由权威元数据和 generation 保证。只有连接失败且请求未发送时才尝试排名第二的实例，共用原 deadline，最多两个地址；响应超时或结果未知不自动重放。无单个 AgentBinding 的模板管理/列表请求仍轮询实例。

注册发现与 AgentBinding 亲和用于独立 Activator 模式；嵌入式模式直接调用本进程 Activator，不注册成员、不做跨进程选路，也不保证相同 AgentBinding 总是落到同一个进程；多个 API Server 可能各自缓存同一 AgentBinding。注册发现不改变 Platform 的发现机制，也不增加 AgentBinding 生命周期控制器。独立模式全热解析为 **1 次 Gateway→Activator、0 次 Agent Redis、0 次 Sandbox API**；后台成员刷新与实例续租单独计费，不属于单次解析访问。

受管接口使用 `/api/agent/v2/templates/{name}/versions/{version}/bindings/{binding_id}`，GET/DELETE 对应查询和删除；AgentBinding 由访问流量触发创建。`POST .../{id}/resolve` 接受 `{protocol, port?, bypasscache?}`；`bypasscache` 为布尔值，默认 false。true 强制当前 Activator 读取 Redis AgentBinding 并查询 Sandbox，跳过 AgentBinding 成功绑定，不绕过不可变模板缓存、租户/service 校验或 generation 隔离。平台刷新失败、不就绪或请求被取消后，本次绑定不作为成功缓存保留；较早的在途响应不能覆盖更新的刷新结果。其他 Activator 的本地缓存不接收失效广播；普通热调用可能继续返回旧绑定，目标失效通过 Gateway 发送前 bypass 重试修正。重试固定原 generation，同名重建不会使原请求切换到新环境。

例如强制刷新 HTTP 目标：

```json
{"protocol": "http", "port": 8080, "bypasscache": true}
```

该参数仅属于 resolve 的 JSON 请求和内部激活请求，不作为 `/agent/http`、`/agent/ws` 的路由 query 参数。HTTP/WS 在业务请求尚未发送时的既有一次目标重试自动携带 bypasscache=true；不会重放结果未知的业务请求。HTTP/WS 通过下面的统一入口触发激活；共享转发使用真实 Sandbox ID。

`GET /api/agent/v2/templates/{name}/versions/{version}/bindings` 返回 `bindings` 和 `next_page_token`。可传 `page_size`（默认 50，范围 1–100）和 `page_token`；token 绑定认证租户、模板与版本。列表仅返回产品元数据，包含删除中的记录，不查询平台健康状态；并发创建/删除期间不保证分页快照。模板不存在时返回 404。

Rust CLI 的当前管理能力与配置见 [CLI 使用说明](cli/README.md)。HTTP CLI 支持方法、Header、文件/stdin 请求体、逐块响应和 stdout AgentBinding 通知；SSH CLI 使用系统 OpenSSH。

## 统一 HTTP/WS 访问

HTTP 使用 `/agent/http[/业务路径]`，WS 使用 `/agent/ws`；受管 WS 可追加业务路径。HTTP 入口拒绝 WebSocket Upgrade，WS 入口要求 GET 和 WebSocket Upgrade。通过 target 指定目标，重复路由参数返回 400：

| 参数 | 路径与认证 |
| --- | --- |
| `target=urn:adx:template:<name>:<version>` | v2 API Key 认证；服务端生成 AgentBinding ID 并激活 |
| `target=urn:adx:binding:<name>:<version>:<binding_id>` | v2 API Key 认证；创建或复用指定 AgentBinding |

URN 各段按 UTF-8 百分号编码；作为 query 参数时还需要经过 URL query 编码。租户来自认证，不放在 URN 内。受管目标的端口必须匹配 service，只有一个对应协议端口时可省略。

```sh
curl --get 'https://gateway.example/agent/http/chat' \
  -H "Authorization: Bearer $ADX_TOKEN" \
  --data-urlencode 'target=urn:adx:template:assistant:1'
```

受管 HTTP/SSE、WS 101 和选定身份后的失败响应都带 `X-ADX-AgentBinding-ID` 与 `X-ADX-AgentBinding-URN`；Gateway 覆盖后端同名 Header。ID 通知不代表创建成功。独立的不带 env 请求生成新 ID，同一请求的内部重试复用 ID 和 generation；后续访问应使用返回的 AgentBinding URN。

HTTP/WS 入口使用 Platform API Key，要求 TLS。移除公开路径前缀及目标参数后，将业务路径交给 Harness，并移除平台凭据 Header。响应体按流转发，WS 升级后使用现有双向字节通道，不增加业务 envelope。Relay 连接和路由由共享 Gateway 数据面管理。直接按 Sandbox ID 的数据访问使用原有 `/direct`、CONNECT 等入口。

## SSH 交互终端

设置 `ADX_SSH_CONFIG` 后，独立 Ingress 或 API Server 内嵌 Ingress 会启动独立 SSH 监听端口。原实例使用路由用户名 `yr:instance:<id>[:port=<port>][:trace=<id>]`，受管目标使用 `adx:target:<百分号编码的完整URN>[:port=<port>][:trace=<id>]`。两种目标共用 listener，分别检查配置中的公钥授权表；路由用户名不是租户或后端 Linux 用户。

SSH 首版只提供一个连接一个交互式终端，要求 PTY 和 shell 请求，支持输入输出、窗口调整、退出码与连接关闭。AgentBinding 提示在鉴权完成、交互 shell 建立后通过 stdout 显示，然后激活并连接后端；它通知选定的身份，不表示 Sandbox 已就绪。省略 env 时当前连接只生成一个 ID；携带返回的 AgentBinding URN 可再次访问。仅认证或打开 channel 不创建资源。inline 直接定位实例，默认端口 22，不调用 Activator，不生成 AgentBinding。

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

流量激活时会先保存稳定身份，再提交平台创建。创建失败、响应丢失或进程中断可能留下尚未激活成功的 AgentBinding；该记录供原身份重试，并非独立的创建功能。如果平台从未观察到该实例，删除仍可能保留 Deleting 记录，无法依靠重复删除确认完成。冷启动还可能遇到 Running 与共享路由发布之间的短暂窗口；HTTP/WS 或 SSH 已返回 AgentBinding 身份后，调用方应使用该身份重试，不能把省略 env 的新请求当作原请求重试。

## 验证

以下为相应历史版本证据，包含当时尚存在的 Inline 和旧 Sandbox HTTP 端点；不代表本次 API Server 适配已完成真实平台验收。

缓存改动接入上游 `9d75a6c` 后，`make agent-test JOBS=2` 为 163 项通过、11 项默认忽略，API Server lib 测试 18 项通过；两者合计 181 项通过。新增 LocalControl 回归先确认 bypass 被忽略会失败，再验证透传后热命中、强制回源失败失效及恢复通过；这 3 项 local 测试已包含在 Agent 总数中。`make rust-check JOBS=2` 的格式、全 workspace 严格 Clippy 和既定 unwrap 检查全部通过。独立模式保留 AgentBinding 亲和，内嵌模式仅本地调用，没有 AgentBinding 稳定选路。日志在本地 `out/env-cache-pr-validation/`；集成后未重新执行真实 Platform 端到端或性能压测，以下数据保留原始构建边界。

2026-09-24，缓存与 AgentBinding 亲和路由改动通过 `make agent-test JOBS=2`（160 项通过、11 项默认忽略）及 `make rust-check JOBS=2`（全 workspace 格式、all-targets/all-features 严格 Clippy 和 unwrap 策略检查）。另行使用一次性 Redis 执行 9 项去重专项用例，覆盖 Store 条件写/重连、注册租约过期与 incarnation 隔离、Gateway 发现变化/失败保留快照，以及心跳续租/退出注销，全部通过。全热零 Redis/零 Sandbox API、Gateway 单次 Activator RPC、滑动 TTL、LRU、bypass 与 generation 隔离由组件测试验证。该阶段完整日志保存在本地 `out/env-affinity-validation/`。默认容量随后调整为 200000，5 小时滑动 TTL 不变；8 项缓存聚焦用例通过，真实 LRU 的 20 万条样例 RSS 增量约 167.1 MiB，测量口径见 Activator 文档。

同日在接入上游 `9d75a6c` 前，基于 `4d822f6` 加缓存改动的 release 构建完成真实 Platform 端到端验证：一轮 16/16 项通过；重复一轮 12/16 项通过，4 项受同一个 Sandbox 冷启动终态 Failed 影响。两轮缓存专项均 5/5 通过：热 resolve 的 Sandbox/Redis 元数据访问为零，bypass 每次 1 次 Sandbox GET 和 2 次 AgentBinding 读取，同名重建固定 generation，以及 Activator 故障切换/租约过期/重新加入均符合契约。现场再次观察到 Harness HTTP 200 而 Execd 控制端点超时，冷启动可靠性不能按全通过报告，边界见 [平台缺口](PLATFORM_GAPS.md)。证据在本地 `out/env-cache-baseline-20260924/`；这是单节点真实容器验证，不是多机或 Kubernetes 集群验收。文档检查仍有两处既有 `agentostesting/810` 报告的 JSON 示例错误，新配置示例及文档链接通过检查。

同一 release 构建的稳态负载基线使用单节点 4 CPU/6 GiB、独立负载端 2 CPU/2 GiB、8 个真实 AgentBinding、两个 Activator。10 个阶段均零错误：32 并发热 resolve 约 31283 次/秒、p95 1.34 ms；同并发 bypass 约 4434 次/秒、p95 23.03 ms。128 并发热 resolve 两次约 30252/30315 次/秒，512 并发约 28292 次/秒、p95 41.09 ms。1024 条 WebSocket 全部建立，10 Hz/连接的 echo 负载约 9891 消息/秒、p95 18.85 ms。热路径的 Sandbox API 调用均为零。32 并发 HTTP 短连接约 1410 次/秒，但负载端已接近 2 核上限；所有数字是此配置基线，不是集群极限，未覆盖 20 万活跃 AgentBinding 或长期淘汰压力。原始数据及完整口径在本地 `out/env-cache-baseline-20260924/report.md` 和 `real4/performance.json`。容量调整后的 `make rust-check JOBS=2` 也已通过。

同日新 AgentBinding 创建补测（镜像已缓存，并发 2）为 4/8 最终成功、4/8 Platform 启动超时；首响应 7 次 503、1 次 502，成功就绪耗时 2.922–118.884 秒，8 个 AgentBinding 最终删除均成功。补测已把夹具的 sandboxd 实例上限从 8 调为 32，排除了此前被 8 个预热 AgentBinding 占满的配置限制；本轮仍有真实启动超时，不能按冷启动可靠性通过报告。补测前 8 个预热候选中另有 3 个启动失败，全部失败证据保留。测试容器/网络已清理，补测清理错误为零。

合并上游 `e295f89` 后，Activator、Agent API、CLI、Agent core、Gateway 与 API Server 的聚焦测试共 199 项通过、2 项默认忽略；另行执行原生 OpenSSH 用例并通过，验证终端 ID/URN 输出和退出码。CLI 参数测试覆盖 OpenSSH 的固定 `ControlMaster` 选项。全 workspace 格式与全 targets/features 严格 Clippy 通过，提交文档检查通过。这轮为组件与原生客户端 socket 验证，未重跑完整 Platform 端到端或真实 Redis 专项；完整日志在本地 `out/pr27-rebase-20260923/`。

2026-09-23，合并上游组件命名调整前的 `3141e27` 完成以下验证：Agent API、Activator、CLI、Agent core 与 Gateway 聚焦测试 167 项通过、2 项忽略；workspace 格式及全 targets/features 严格 Clippy 通过。双独立 Activator 与真实 Redis 的集成覆盖包含在下述端到端验证中。拟提交文档及补丁检查通过；工作区文档检查另报告两处用户自验报告中的非标准 JSON 示例，这些报告不在提交范围内。当时的生产依赖检查确认 Agent API 仅通过 HTTP 客户端调用 Activator；当前新增 Store 的注册发现客户端依赖，元数据访问仍在 Activator。

上述提交的真实容器验证使用两个 Gateway Edge（现名 Ingress）、两个独立 Activator 进程和真实 Redis/Platform/RRT，两个 Activator 的 readiness 均为 204。19 项端到端检查全部通过，覆盖模板与 AgentBinding 管理、自动 ID 回传、跨 Gateway 身份复用、HTTP/WS/SSH、Rust HTTP/SSH CLI、租户隔离，以及 inline create/get/list/kill、exec、mkdir、上传提交、下载 Range 和文件列表。

该轮对部分冷启动响应的 NotReady/OutcomeUnknown，在测试用例中保留原 AgentBinding/Sandbox ID 做有界退避查询后继续；未增加 ADX 等待，也未通过更换身份重建掩盖失败。现场确认当时的 RRT（现名 Execd）入口监视线程持锁休眠，会阻塞启动完成和控制 HTTP；根因、Platform 中央创建期限传递缺口及修复边界见 [平台缺口](PLATFORM_GAPS.md)。全用例通过不等于冷启动首请求可靠性已验收。

验证容器和网络已清理，清理错误为零。完整日志、构建来源和匹配 Build ID 的 RRT 现场符号证据保存在本地 `out/adx-e2e-reliability-20260923/`；脚本和报告不提交。Platform 源码未修改，预装 Execd、动态挂载和自定义健康检查仍按平台缺口暂缓。

运行 `make agent-test`。真实 Redis 测试将 `ADX_AGENT_TEST_REDIS_URL` 指向一次性数据库并显式使用 `--ignored`，测试会留下独立命名空间记录。组件测试不能替代真实部署验证。
