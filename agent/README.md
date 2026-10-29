# Agent-DX v2

Agent 层使用 Rust，产品 API 嵌入统一 Gateway。Activator 可独立部署，也可与 API Server 和 Ingress 共进程；多个副本使用相同产品状态命名空间。

面向开发与交付的讲解见 [Agent 统一能力与 Jiuwen 交付场景串讲](../docs/development/agent-jiuwen-walkthrough.zh.md)，通过单机、多机和场景部署视图，先讲通用模型、激活与访问，再讲 digital-mate App 场景的账号、模型和共享工作区部署。

| 目录 | 职责 |
| --- | --- |
| `crates/core` | 产品类型、协议、Environment 能力边界及通用校验 |
| `crates/store` | Template/AliasConfig/AgentBinding 元数据、Redis 原子条件写；内存实现仅供测试 |
| `activator` | 产品管理、稳定逻辑身份激活及别名目标态收敛；不做 Environment 健康恢复 |
| `api` | Activator HTTP 客户端或本地模块调用、AgentBinding 身份选择、解析结果校验 |
| `cli` | Rust 用户命令行 `adx`；Template 发布/查询、AgentBinding 分页查询/删除、HTTP 流式调用、SSH 交互终端 |

AgentBinding 对应一个长期存在的逻辑实例；current 保存当前实际版本与 Environment ID，别名升级时可更换 Env 而保留 Binding generation。指定 AgentBinding 的首次访问会幂等创建元数据，再激活 Environment；多副本复用同一身份。无效模板或未声明的 service 不创建 AgentBinding。Environment 状态、健康、暂停/恢复与运行载体 0–1 由 Platform 保证。产品删除确认后，新访问可以重建同名 AgentBinding，generation 隔离旧生命周期。

公开 API、认证、路由和连接实现位于 Gateway。HTTP/WS/SSH 通过普通方法调用 Agent 的 `ManagedService` 选择逻辑 AgentBinding 身份，再由 Activator 统一选择实际版本并校验 service 协议和端口的唯一匹配；逻辑身份选择本身不创建元数据或 Environment。HTTP 响应头、SSH 终端提示与协议转发仍由 Gateway 处理。

Gateway 对同一次请求的目标失效重试强制绕过 Activator 的成功激活缓存。HTTP/SSH 固定首次选定的 generation。WS 在发送任何后端握手或业务字节前，若权威元数据确认 Binding 已删除或换代，可在原 deadline 内重新解析同一逻辑 Binding 一次；缺失时使用本次初始化参数创建，已存在时忽略参数。Deleting、鉴权失败、draining 及同 generation 的其他冲突不触发重选。已开始发送后不切换生命周期，不重放业务请求。

用户 Harness 由 Execd 启动，自行定义业务接口。HTTP/WS/SSH 保持透明转发，不限制业务并发。Ingress 的 Inline 管理、exec/files 和旧 JWT/IAM 适配已移除；Environment 生命周期由 API Server 管理。

## 镜像启动与 Environment 配置

Agent 模板使用业务镜像的 ENTRYPOINT/CMD、WORKDIR 和 USER，不接受 `entrypoint` 或 `working_dir` 覆盖。创建规格设置 `inherit_entrypoint=true`；Platform 注入部署侧 runtime 并生成进程配置，Execd 据此启动业务进程。业务镜像不需要预装 Execd 或额外进程 JSON。模板工作目录覆盖暂不支持；可通过 mounts 声明 Local bind 挂载。

```json
{"name":"assistant","image":"registry.example/assistant:v1","isolation_runtime":"runc","env":{"APP_MODE":"production"},"resources":{"cpu_millis":1000,"memory_mib":1024},"service":[{"protocol":"ws","port":8080}]}
```

模板 `resources.cpu_millis` 与 `memory_mib` 是调度预留。可选的 `cpu_limit_millis` 与 `memory_limit_mib` 是运行上限，省略时各自等于对应预留；上限必须大于等于预留。例如 `{"cpu_millis":500,"memory_mib":1024,"cpu_limit_millis":2000,"memory_limit_mib":4096}` 按 0.5 CPU / 1 GiB 调度，允许运行时使用至 2 CPU / 4 GiB。Activator 将上限映射为 Environment API 的 `cpu_limit`、`mem_limit`，独立与嵌入式模式共用此转换。上限不计入调度预留，也不保证所有 Environment 同时获得上限资源；已有 Environment 不会因模板内容变化被原地调整或重建。

独立 Activator 的 `environment_url` 必须指向 API Server 管理监听地址。创建调用 `POST /api/environment/v1/environments`，查询调用 `GET /api/instances?instance_id=...`，删除调用 `DELETE /api/environment/v1/environments/{id}`。稳定 ID `adx-<generation>` 映射为 namespace=`adx`、name=`<generation>`。每个租户配置自己的 Platform API Key 环境变量，详见 [Activator 配置](activator/README.md)。创建时的 runtime 注入由 API Server 部署配置负责。

嵌入式 Activator 使用 Agent 侧 `LocalEnvironment`，与独立 `HttpEnvironment` 共用 `environment_request` 将 Agent 执行规格转成 Environment 请求。API Server 的 `EnvironmentService.create_request()` 统一校验请求、转换 EnvironmentSpec 并注入自身配置的 runtime profile。文件访问由 Ingress 通用 `environment_files` 能力处理，独立模式复用 API Server 实例查询，嵌入式复用 EnvironmentService；Jiuwen 不读取 Execd 凭据。旧 PlatformEnvironment、EnvironmentRequestMapper 和 runtime 查询已删除，Ingress 不再提供 `/api/environment/v2/instances`。文件入口需要 `ADX_ENVIRONMENT_FILES_CONFIG`，见 [Gateway 文件访问配置](../gateway/README.md#common-environment-file-access)；跨进程配置必须保持一致。

Agent 模板无需逐镜像注册预装 profile。Environment Running 仍不等于业务端口通过自定义就绪检查。

## 自动暂停与按需恢复

模板可声明 `lifecycle`，由 Activator 的 HTTP/嵌入式适配器透传至公共 Sandbox 创建接口：

```json
{"lifecycle":{"idle_timeout_seconds":600,"idle_action":"pause","pause":{"retention":{"kind":"forever"},"timeout_seconds":300,"storage":"shared"}}}
```

省略策略时空闲超时为 0，不自动回收；`idle_action` 默认 delete，pause 要求正数空闲窗口。`pause` 可选，默认保留 90000 秒、捕获超时 300 秒、使用节点配置的存储。显式保留策略可选 `{"kind":"ttl","ttl_seconds":86400}` 或 `{"kind":"forever"}`；`storage` 可选 configured/local/shared，shared 要求可用的共享对象存储。保留期从 checkpoint 发布完成开始，与空闲计时分开；forever 保留至删除或替换。模板策略变更需要新版本，只影响新建 Sandbox。

Platform 检测空闲、保存 checkpoint 并释放执行资源。持续 HTTP/WS/SSH 业务流仍视为活跃；Jiuwen 打开的 WebSocket 会阻止空闲暂停，断开后才开始计时。不能把网关连接存活检查的 `idle_timeout_seconds` 当成模板空闲回收策略。后台进程本身不一定阻止休眠，具体语义见 [节点生命周期](../docs/testing/node-lifecycle.md)。

Paused/Pausing/Resuming 保留原 AgentBinding generation 和 Sandbox ID；Activator 冷缓存及 bypass 查询可返回该身份，不创建第二个 Sandbox，也不调用 resume。鉴权后的业务 HTTP/WS/SSH 或文件访问经共享 Gateway 请求 Platform EnsureRunning，等待已提交 Running 路由后转发。并发首请求的恢复去重由 Platform 负责；管理查询、未授权访问和被动健康探测不唤醒。`Target` 表示可路由身份，不保证业务进程已 Ready；Failed/Deleted 和未就绪的创建仍返回原有错误。恢复超时沿用原身份重试，不能据此重建实例。

## 受管卷

模板可声明 `volumes`，并以 `mounts[].volume_id` 挂载；内联 host_path 继续保留。例如：

```json
{"volumes":[{"id":"workspace-${workspace}","source":{"type":"directory","storage_domain":"sfs","path":"workspaces/tenant-a/${workspace}"},"access_mode":"exclusive","create_if_missing":true,"create_options":{"uid":10001,"gid":10001,"mode":"0700"}}],"mounts":[{"type":"bind","volume_id":"workspace-${workspace}","target":"/workspace","options":["rw"]}]}
```

首次激活先保存 Binding，再用不可变模板和已保存参数 ensure 卷并关联 Binding，固定返回的 UID 后创建 Environment。已有定义为准，不覆盖其路径或模式；默认 shared，exclusive 显式声明。多个 Binding 可以关联同卷。激活超时保留关联；删除 Binding 先收敛 Environment 与引用，最后一个关联解除且没有其他使用后删除卷定义，底层数据保留。

创建连接失效后，Activator 在原总期限内查询同一个 Environment ID，并仅在无 Environment 时幂等重试创建；退避带抖动。已有 Creating 仍返回 NotReady，生命周期继续由 Platform 处理。Binding 在平台操作后已删除或换代时，Activator 补偿清理旧身份，不删除新 Binding 的 Environment。不新增后台激活恢复或 Platform Binding 删除标记；进程在副作用后、后置检查前崩溃不获得孤儿清理保证。

部署配置、公共接口和验证范围见 [Volume API](../platform/api/http/volumes.md)；真实存储及运行时验收仍按 [P7](../docs/development/platform-volume-design.md) 执行。

[Orbit 模板](activator/examples/orbit-sfs-template.json)不包含 version 字段，发布后由服务端分配数字版本；该模板使用受管共享卷 `orbit-workspace-${workspace}`，在 storage domain `sfs` 中声明 `workspaces/${workspace}`，挂载到 `/data/state`。将域根配置为 `/mnt/sfs` 时复用原目录。存量固定版本 Binding 转到 Alias、存储域配置和首次卷交接须在停写窗口内按经审查的运维清单执行，不提供自动转换接口。

## 模板挂载与 Binding 参数

模板支持 `mounts: [{"type":"bind","host_path":"/mnt/sfs/workspaces/${workspace}","target":"/home/jiuwen","options":["rw"],"create_if_missing":true}]`。Agent 支持 Local bind 和 VolumeRef；Environment API 仍支持 image/S3 等内联来源。最多 32 个挂载，目标路径不得重复；options 支持 ro、rw、nosuid、nodev、noexec，拒绝重复项及 ro/rw 同时出现。

首次访问通用 `/agent/ws` 或 `/agent/http` 时，可通过 `X-ADX-Parameters: workspace=w-user&model_api_base=https%3A%2F%2Fmodel.example%2Fv1` 提供 URL 表单参数。共享解析/激活流程仅在 Binding 不存在时解析和校验，原子保存参数后激活。已有 Binding 完全忽略新参数，包括缺失、不同值和格式错误；并发创建以成功落库的 Binding 为准。删除 Binding 后再次访问重新初始化。没有独立 prepare 阶段或接口。

Template 的 `mounts[].host_path`、`mounts[].volume_id`、Volume 定义和 `env` 值支持 `${name}` 渲染。挂载参数按相对子目录校验，环境变量参数允许普通字符串；不执行 Shell 展开或递归展开。参数仅能替换模板声明的占位符。参数当前明文保存并出现在 Binding 元数据中；模型 Key 的后续加解密由业务 Gateway/AgentServer 管理，ADX 不提供模型专用私有启动配置。参数 Header 由 Ingress 消费，不转发到业务服务。

```yaml
env:
  API_BASE: "${model_api_base}"
  API_KEY: "${model_api_key}"
mounts:
  - type: bind
    host_path: "/mnt/users/${workspace}"
    target: /home/agentos
```

参数名为 1–64 个 ASCII 字母、数字或下划线，最多 64 项，每个值最多 8192 字节且不能包含 NUL；编码输入最多 65536 字节，并受 HTTP Header 总大小限制。用于 host_path 的值必须是非空相对路径，拒绝绝对路径、空分量、`.`/`..`、反斜杠、控制字符及再次展开。挂载模板必须保留固定绝对根目录，渲染后路径最长 4096 字节；沙箱目标为无占位符的绝对路径。路径语法检查不代表节点符号链接授权或共享文件系统挂载完整性检查。

挂载可声明 `create_if_missing`（默认 false）。为 true 时，adxlet 在创建或恢复沙箱前幂等创建缺失的源目录；父目录必须预先存在，拒绝符号链接路径，已有目录不清空或改变权限。该字段由模板经 Activator 原样传到 Platform，不需要节点目录配置。

需要为非 root 应用创建目录时，可同时声明 `create_options: {"uid":10001,"gid":10001,"mode":"0700"}`。三个字段均必填，UID/GID 为 0–4294967294 的整数，mode 为 `0000`–`0777` 的四位八进制字符串。仅允许与 `create_if_missing: true` 一起使用；只对新目录设置所有权和权限，已有目录保持原状，不递归 chown。未声明时保持原有 0755（受节点 umask 影响）及节点进程所有权。节点直接创建最终源目录，再通过目录描述符设置所有权和权限；设置失败则本次启动失败并保留目录，需修正残留权限。该过程不承诺原子初始化，并发首次使用可能看到尚未设置完成的权限。需要包含该能力的 adxlet；单独更新 Ingress 或模板不能使旧节点支持它。

Activator 在准备和激活时渲染 mounts；HTTP 与嵌入式 Sandbox 适配器发送相同的具体挂载，Environment 创建请求不传 parameters 或业务 user_id；Volume 管理的 owner_id 使用对应 Env 引用的实际 Environment ID，不代表业务用户。Jiuwen 根据认证身份的 `[tenant, template, user_id]` JSON 数组 SHA-256 生成 `w-<完整十六进制摘要>`，版本、Binding 和 Sandbox ID 不参与目录标识。

[Jiuwen 模板例子](activator/examples/jiuwen-sfs-template.json)将应用数据和 HOME 收敛到 `/home/jiuwen`，仍采用内联挂载，尚未切换到受管 Volume；SFS 挂载由部署准备，目录创建由上述挂载声明控制。使用 SQLite/WAL 的工作区必须保证旧 Sandbox 完全停止访问后才启用后继，不能同时运行新旧版本。详见[工作区设计](../docs/development/user-workspace-volumes-design.md)和[Volume 方案](../docs/development/platform-volume-design.md)。

## 进程配置

```sh
cargo build --locked -p data-plane-gateway -p adx-apiserver --features data-plane-gateway/agent-api --bins
```

Gateway 保留独立 Ingress 与 API Server 内嵌 Ingress 两种进程形态，共用装配代码。构建时为承载 Ingress 的二进制启用 `data-plane-gateway/agent-api`。Agent 入口按以下方式启用：

- `ADX_AGENT_CONFIG` 指向 v2 配置文件，选择独立 Activator 地址或嵌入式 Activator 的 Redis 命名空间。受管管理和 HTTP/WS 数据入口使用 Platform API Key。

选择独立模式时，另行构建并启动无状态 `adx-activator` 进程；多个副本连接相同 ADX Redis namespace，并通过 API Server 既有 HTTP 接口访问平台。嵌入式模式由 API Server 将同一 Environment 业务服务以 Rust 模块接口交给 Activator，受管 Agent 请求无需内部 HTTP/RPC 回环。

```sh
cargo build --locked -p adx-activator --bin adx-activator
```

独立模式 Gateway 配置如下。组件间令牌从环境变量读取；受管 Agent 入口不要求本机装配 Environment 后端，Activator 的 `environment_url` 指向 API Server。

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
    "namespace": "adx-agent",
    "sandbox_network": {
      "sandbox_cidrs": ["10.0.0.0/20"],
      "internal_cidrs": [],
      "public_egress": true,
      "egress_allow": []
    }
  }
}
```

新建模板 Sandbox 统一附加配置的 ACL：独立模式使用 `sandbox_network`，嵌入式模式使用 `embedded.sandbox_network`。Sandbox 地址池拒绝互访，公网与业务外连按配置放行；DNS 和模型服务等内部地址需要显式例外，见[网络隔离配置](activator/README.md#模板沙箱网络隔离)。已有 Sandbox 不修改。

`activator` 与 `embedded` 只能配置一个。独立 Ingress 不持有 API Server 的业务服务，应使用独立 Activator 模式。

`timeout_seconds` 默认 60 秒且必须为正，由入口确定一次绝对 deadline。管理面在鉴权后统一处理 trace、请求读取、业务调用和 JSON 响应；HTTP/WS 目标激活的模板查询、激活和内部目标重试共用同一期限。客户端将剩余预算传给 Activator，地址重选不重置期限。纯读超时返回 Unavailable，可能写入的操作超时返回 OutcomeUnknown，重试使用原身份；已到期的内部重试直接拒绝。网络连接及 Environment 后端配置上限只能缩短剩余预算。每次调用的 `deadline_unix_ms` 不进入执行规格或 Redis；后台收敛的下一次核对使用新的有界等待预算，沿用同一已持久化 EnvID 和固定创建规格。独立 Activator 用剩余期限限制 HTTP 等待，但现有 API Server 没有该绝对期限字段；创建请求中的 `createTimeoutSeconds` 固定取 Activator 配置，保持相同身份重试的请求体与操作 ID 稳定。API Server 可能在调用方超时后继续创建，超时返回 OutcomeUnknown，不自动重放或换 ID。嵌入式 `LocalEnvironment` 同样按剩余期限限制调用等待，固定创建预算为 60 秒；提交后超时返回 OutcomeUnknown。两种模式使用相同的创建请求和确定性操作 ID。激活期限不覆盖建立后的 HTTP 响应流、WS 或 SSH 会话；SSH 模板校验、身份提示和后端握手共用 SSH 连接期限。

Gateway 与 Activator 都缓存不可变模板，按 tenant/name/version 隔离，各最多 1024 条；同键在途读取合并，缺失和失败不缓存。Gateway 不缓存 AgentBinding/Target，每次解析都调用 Activator。Activator 的 AgentBinding 成功绑定采用 LRU，默认容量 200000，滑动 TTL 为 18000 秒（5 小时）；热命中直接返回并续期，不读 Redis、不调用 Environment API。TTL 只在请求访问时检查；别名收敛另由元数据事件及启动/重连/成员变化触发，不做周期全量 Binding 扫描；容量淘汰、闲置过期、重启和 bypass 会引起回源。缓存是派生状态，实际连通性由 Gateway/Relay 校验；删除中的并发请求允许成功或失败。Activator 进程与缓存内存测量见[进程说明](activator/README.md)，权威状态保证见[存储说明](crates/store/README.md)。

独立模式经 ActivatorClient 调用远程进程；嵌入式模式经 LocalControl 调用同进程 Activator，复用上述缓存及 bypass 语义。嵌入式 Activator 使用默认 200000 条、5 小时配置，当前没有独立的缓存配置入口。

### Activator 发现与 AgentBinding 亲和路由

静态 `activator.urls` 中的每个地址必须直达一个实例，不能是随机负载均衡到多个实例的地址。动态发现可以用 `activator.discovery` 替代 `urls`，两者互斥：

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

完整逻辑 AgentBinding scope（tenant/template/template_ref/binding_id）按固定 SHA-256 Rendezvous Hash 排序实例。所有 Gateway 使用相同成员集合时选择相同实例，成员顺序不影响结果；静态模式用规范化地址作实例 ID。resolve、bypass、单 AgentBinding 查询和删除使用相同选择规则，generation、当前实际版本和 EnvID 不参与选路。成员变更只重新分配受影响的 AgentBinding；短暂列表差异允许重复缓存，身份隔离仍由权威元数据和 generation 保证。只有连接失败且请求未发送时才尝试排名第二的实例，共用原 deadline，最多两个地址；响应超时或结果未知不自动重放。无单个 AgentBinding 的模板管理/列表请求仍轮询实例。

独立模式的请求亲和使用注册发现；嵌入式请求直接调用本进程 Activator，不保证请求落到同一进程。两种模式配置 `registration` 后均自动启动注册续租、变更监听与后台收敛，无需单独配置 `reconciliation`；省略 registration 时只按请求处理。嵌入式 registration 位于 `embedded` 内，后台按注册成员分工；多个 API Server 仍可能各自缓存同一 Binding。嵌入式注册不会新增内部 HTTP 接口，其成员目录不能用于远程 ActivatorClient 的请求发现。配置组合见 [Activator 收敛配置](activator/README.md#别名收敛配置)。成员分工不构成排他锁，仍由 CAS 与 Platform 幂等保护副作用。注册发现不改变 Platform 的发现机制。普通 HTTP/WS/SSH 的 scope 解析、别名选版及服务协议/端口校验统一由 Activator 完成，Gateway 不读取 Alias 权重或自行选版；独立模式的数字版本和 Alias 全热 resolve 均为 **1 次 Gateway→Activator、0 次 Agent Redis、0 次 Environment API**；后台成员刷新与实例续租单独计费，不属于单次解析访问。

Alias 的公开管理入口为 `GET/PUT /api/agent/v2/templates/{name}/aliases/{alias}`。PUT 请求为 `{seed, versions, expected_revision?}`，返回 `{alias:{config,revision}}`；tenant 由 Gateway 鉴权确定，与模板发布使用同一租户 API Key。两种 Activator 模式均支持，示例见 [Alias 配置](activator/README.md#别名收敛配置)。

SSH 使用同一 Activator resolve 结果校验服务、连接目标 Env；文件入口从 Activator 的只读 Binding/模板联合结果读取实际版本配置与 EnvID，不重新根据 Alias 选择模板，也不因查询元数据创建 Env。业务会话鉴权、下载令牌与文件范围检查仍由 Ingress 负责。

受管接口使用 `/api/agent/v2/templates/{name}/versions/{version}/bindings/{binding_id}`，GET/DELETE 对应查询和删除；AgentBinding 由访问流量触发创建。`POST .../{id}/resolve` 接受 `{protocol, port?, bypasscache?}`；`bypasscache` 为布尔值，默认 false。true 强制当前 Activator 读取 Redis AgentBinding 并查询 Environment，跳过 AgentBinding 成功绑定，不绕过不可变模板缓存、租户/service 校验或 generation 隔离。平台刷新失败、不就绪或请求被取消后，本次绑定不作为成功缓存保留；较早的在途响应不能覆盖更新的刷新结果。启用别名收敛时，各 Activator 订阅 Binding 更新并失效已有缓存；传播是异步的，目标失效仍通过 Gateway 发送前 bypass 重试修正。未启用监听时保留请求触发刷新语义。HTTP/SSH 重试固定原 generation；WS 仅在尚未发送后端握手或业务字节时允许一次删除/换代刷新，复用原始 deadline 和初始化参数。

例如强制刷新 HTTP 目标：

```json
{"protocol": "http", "port": 8080, "bypasscache": true}
```

该参数仅属于 resolve 的 JSON 请求和内部激活请求，不作为 `/agent/http`、`/agent/ws` 的路由 query 参数。HTTP/WS 在业务请求尚未发送时的既有一次目标重试自动携带 bypasscache=true；不会重放结果未知的业务请求。HTTP/WS 通过下面的统一入口触发激活；共享转发使用真实 Environment ID。

`GET /api/agent/v2/templates/{name}/versions/{version}/bindings` 返回 `bindings` 和 `next_page_token`。可传 `page_size`（默认 50，范围 1–100）和 `page_token`；token 绑定认证租户、模板与版本。列表仅返回产品元数据，包含删除中的记录，不查询平台健康状态；并发创建/删除期间不保证分页快照。模板不存在时返回 404。

Rust CLI 的当前管理能力与配置见 [CLI 使用说明](cli/README.md)。HTTP CLI 支持方法、Header、文件/stdin 请求体、逐块响应和 stdout AgentBinding 通知；SSH CLI 使用系统 OpenSSH。

## 别名目标态与灰度升级

发布接口接收不含 `version` 的 TemplateDraft，原子分配同租户/模板下递增的数字版本并返回完整 TemplateVersion。版本为规范的正 u64 十进制字符串；`0`、前导零和溢出均拒绝。Alias 名不能为纯数字。通用 template/binding URN 均使用同一 reference 段；不存在自动 latest、版本优先回退查询或另一个 Alias URN 类型。已有字符串版本需通过人工运维转为数字快照和同名 Alias，不自动迁移存储。

AliasConfig 保存 `tenant/template/alias/seed/versions`，versions 为 `{version, weight}` 列表。所有版本必须已发布，版本不能重复；weight 为非负 u64 整数，总和须在 1–u64::MAX 内，不要求为 100 或 10000。按 `weight / sum(weights)` 选择稳定的 Binding 分布，不逐请求抽样，也不保证实际请求量精确等于权重。seed 在同一 Alias 生命周期内不可修改，revision 不参与选择；列表重排与同比缩放不会改变分配。

Scope 使用 `template_ref: {"version":"1"}` 或 `template_ref: {"alias":"production"}`。AgentBinding 保留 generation、parameters、跨版本 Volume ID/UID，并保存 current 及可选 secondary；Env 引用只有真实 version、environment_id，secondary 另带 `preparing` / `retiring` 意图。固定版本 Binding 始终使用 current。没有 AliasBinding、target_version、发布任务或第三个 Env 槽位。

升级为 `current=E1 → current=E1, secondary=E2/preparing → current=E2, secondary=E1/retiring → current=E2`。先保存候选 ID，再调用 Platform；Ready Running 或已初始化 Paused 才原子交换引用。切换前 CAS 核对最新 Alias revision 与 Binding 引用。配置连续变化时优先使用最新目标，但先清理不再需要的 secondary；已退役 Env 不再次提升，回退会准备新的 Env。每次平台调用使用保存的实际 ID，不从 BindingID 或 generation 反推。

切换后直接调用既有 Environment delete；Activator 不感知存量连接。删除未知时保留引用，确认删除及卷关联释放后清空。**Platform 优雅退出尚缺失，本次不实现；当前实际删除会清理运行时，不能保证旧连接不中断。** 新 Env 先建立卷 owner，旧 Env 删除确认后释放旧 owner；独享卷仍可能等待旧执行释放，无强制抢占或零空窗承诺。

启用方式、Redis 通知配置及受控 curl 配置入口见 [Activator 别名收敛](activator/README.md#别名收敛配置)，完整方案见 [Agent 别名目标态与灰度升级方案](../docs/development/agent-alias-rollout-design.md)。管理页不在首期范围。公开 GET/DELETE/resolve 和分页路径也接受 `/api/agent/v2/templates/{name}/aliases/{alias}/bindings[/<binding_id>]`；读取 Alias 使用 `GET /api/agent/v2/templates/{name}/aliases/{alias}`，创建或更新使用同路径 PUT。公开入口的租户始终来自认证，两种 Activator 模式均支持通过 Ingress 写入配置。

## 统一 HTTP/WS 访问

HTTP 使用 `/agent/http[/业务路径]`，WS 使用 `/agent/ws`；受管 WS 可追加业务路径。HTTP 入口拒绝 WebSocket Upgrade，WS 入口要求 GET 和 WebSocket Upgrade。通过 target 指定目标，重复路由参数返回 400：

| 参数 | 路径与认证 |
| --- | --- |
| `target=urn:adx:template:<name>:<reference>` | v2 API Key 认证；服务端生成 AgentBinding ID 并激活 |
| `target=urn:adx:binding:<name>:<reference>:<binding_id>` | v2 API Key 认证；创建或复用指定 AgentBinding，数字 reference 为版本，非数字为 Alias |

URN 各段按 UTF-8 百分号编码；作为 query 参数时还需要经过 URL query 编码。租户来自认证，不放在 URN 内。受管目标的端口必须匹配 service，只有一个对应协议端口时可省略。

```sh
curl --get 'https://gateway.example/agent/http/chat' \
  -H "Authorization: Bearer $ADX_TOKEN" \
  --data-urlencode 'target=urn:adx:template:assistant:1'
```

受管 HTTP/SSE、WS 101 和选定身份后的失败响应都带 `X-ADX-AgentBinding-ID` 与 `X-ADX-AgentBinding-URN`；Gateway 覆盖后端同名 Header。ID 通知不代表创建成功。独立的不带 env 请求生成新 ID，同一请求的内部重试复用 ID 和 generation；后续访问应使用返回的 AgentBinding URN。

HTTP/WS 入口使用 Platform API Key，要求 TLS。移除公开路径前缀及目标参数后，将业务路径交给 Harness，并移除平台凭据 Header。响应体按流转发，WS 升级后使用现有双向字节通道，不增加业务 envelope。Relay 连接和路由由共享 Gateway 数据面管理。直接按 Environment ID 的数据访问使用原有 `/direct`、CONNECT 等入口。

## SSH 交互终端

设置 `ADX_SSH_CONFIG` 后，独立 Ingress 或 API Server 内嵌 Ingress 会启动独立 SSH 监听端口。原实例使用路由用户名 `yr:instance:<id>[:port=<port>][:trace=<id>]`，受管目标使用 `adx:target:<百分号编码的完整URN>[:port=<port>][:trace=<id>]`。两种目标共用 listener，分别检查配置中的公钥授权表；路由用户名不是租户或后端 Linux 用户。

SSH 首版只提供一个连接一个交互式终端，要求 PTY 和 shell 请求，支持输入输出、窗口调整、退出码与连接关闭。受管 SSH 在鉴权和 PTY/shell 请求完成后调用 Activator resolve；服务校验失败直接拒绝，其他结果在连接后端前通过 stdout 提示选定的 AgentBinding 身份，激活失败也可能提示原身份供重试。提示不表示 Environment 已就绪或后端 SSH 握手成功。省略 env 时当前连接只生成一个 ID；携带返回的 AgentBinding URN 可再次访问。仅认证或打开 channel 不创建资源。inline 直接定位实例，默认端口 22，不调用 Activator，不生成 AgentBinding。

后端复用共享 L4 connector → Relay → Environment sshd；没有新建数据通道或本地目标缓存。使用未修改的 russh 0.63.3，不发送认证 Banner。首版不提供 SSH 远程 exec、SFTP 或端口转发。已有 Environment-ID SSH/CONNECT 转发入口保持原有行为。

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

预装镜像、Execd 与匹配的启动 profile 仍可用于验证。删除 Environment 明确返回 NotFound 时，Activator 条件清理对应 generation 的 Binding 及其 parameters；删除成功返回 Deleted 同样清理。超时、断连、权限错误或其他未知结果保留记录供重试。元数据清理不证明迟到的平台创建已被取消。pause/resume 完全由 Platform 负责，不属于 ADX 适配范围。模板可配置 `init_timeout_seconds` 和一次性 `readiness_probe`；Activator 通过 Environment 能力接口透传，Platform 在成功后才发布 Running 和绑定路由。Jiuwen 当前使用 TCP 18092 探针，只保证 AgentServer 前端监听，不保证内部 Runtime 已完成 warming；模板也可配置周期 `healthcheck`（同样的 action/timing 字段，加正整数 `failure_threshold`，默认 3），由 Platform 执行并处理失败。

流量激活时会先保存稳定身份，再提交平台创建。创建失败、响应丢失或进程中断可能留下尚未激活成功的 AgentBinding；该记录供原身份重试，并非独立的创建功能。如果删除明确返回不存在，则清理绑定；如果删除结果未知，则保留 Deleting 记录并沿用原身份重试。冷启动还可能遇到 Running 与共享路由发布之间的短暂窗口；HTTP/WS 或 SSH 已返回 AgentBinding 身份后，调用方应使用该身份重试，不能通过换用新身份重试来绕过该绑定的生命周期。

## 验证

运行 `make agent-test`。真实 Redis 测试将 `ADX_AGENT_TEST_REDIS_URL` 指向一次性数据库并显式使用 `--ignored`，测试会留下独立命名空间记录。组件测试只证明相应代码与 fixture 覆盖的契约。


创建排队确认取消后的重试由Activator管理：Binding保存当前创建尝试序号及5→10→20→40→60秒封顶的退避时间，候选Env ID保持不变；每轮先查询候选Env；候选不存在时，在第一次创建及后续重试前统一检查旧Env已Paused/Deleted或不存在，不区分模板卷模式。旧Env尚未暂停只延后检查，仅确定分配前取消才推进Request ID。结果未知和已有执行的重启恢复仍保持原平台语义，见[创建排队取消与退避](activator/README.md#创建排队取消与退避)。
