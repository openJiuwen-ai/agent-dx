# Activator

Activator 是无状态产品控制模块，可作为独立进程 `adx-activator`，也可嵌入 API Server 所在进程。多个副本连接相同 ADX Redis namespace，通过条件写提交唯一 AgentBinding 身份。独立模式调用 API Server 的现有 Environment HTTP 管理接口；嵌入式模式直接调用 API Server 的 Environment 应用服务。独立模式可通过 Redis 注册成员租约；启用别名目标态收敛后按 Rendezvous Hash 分工，无选主或逐 Binding 排他工作锁，不承担 Environment 健康恢复。

Gateway 在独立模式通过 ActivatorClient 发起 HTTP 请求，在嵌入式模式通过 LocalControl 直接调用 Activator。公开认证与业务流量转发留在 Gateway；Activator 管理 Template/AgentBinding 状态。配置见[Agent 进程配置](../README.md#进程配置)。

Agent 的 `CreateEnvironment`/`ExecutionSpec` 在本 crate 的 `environment_request` 转成通用 Environment 创建请求；`HttpEnvironment` 与 `LocalEnvironment` 共用映射、操作 ID 和响应解释。API Server 只转换自己的 Environment 请求到 Platform EnvironmentSpec，并负责 runtime 注入。嵌入式适配通过 `EnvironmentApplication` 调用同一 `EnvironmentService.create_request()`，不依赖 Ingress mapper，也不在 Agent 层构造 Platform 类型。嵌入式配置固定创建预算为 60 秒，调用等待仍受请求剩余期限约束；提交后超时保留原 ID 并返回 OutcomeUnknown。

独立进程启动读取 `ADX_ACTIVATOR_CONFIG` 指向的 JSON，示例见 [local.json](examples/local.json)。入口服务凭据为 `ADX_ACTIVATOR_SERVICE_TOKEN`。`environment_url` 指向 API Server（示例端口 8081 须与其 `listen` 配置一致），`environment_api_key_envs` 将租户映射到保存该租户 Platform API Key 的环境变量名，例如 `{"tenant":"ADX_PLATFORM_TENANT_API_KEY"}`。这里的 Platform API Key 用于 Environment 管理，与最终用户的模型 API Key 无关。凭据内容不写入 JSON；未配置租户的查询返回 NotFound，创建/删除在提交请求前返回 Unavailable。每项 key 必须属于对应租户，不能使用跨租户管理员 key；现有查询响应不含租户字段，Activator 无法独立验证配置归属。HTTP 明文必须显式允许，否则客户端需要 TLS 和受信任 CA。

内部端点统一为 POST `/internal/adx/v1/`：

| 后缀 | 请求 | 行为 |
| --- | --- | --- |
| `templates/publish` | tenant、不含 version 的 template | 原子分配数字版本，返回完整 TemplateVersion |
| `templates/get` | tenant、name、version | 查询模板 |
| `aliases/get` | tenant、template、alias | 查询最新配置和 revision |
| `aliases/put` | config、expected_revision? | 校验已发布版本并 CAS 保存完整分布 |
| `bindings/get` | scope | 查询产品元数据 |
| `bindings/view` | scope | Activator 解析版本或别名，返回同一次读取的 `{binding, template}`；尚无 Binding 时 binding 为 null，不创建或激活 Env |
| `bindings/resolve` | activation（scope、expected_generation?、bypasscache?、parameters?）、service（protocol、port?） | Activator 选择实际版本并在创建前校验服务，返回 `{target, port}`；热命中不读取 Redis/Platform |
| `bindings/list` | tenant、template、template_ref、page_size?、page_token? | 有界分页查询产品元数据，不访问 Environment |
| `bindings/activate` | scope、expected_generation?、bypasscache?、parameters? | 热命中续期返回 AgentBinding 绑定；未命中或显式 bypass 时读取 Redis 并查询/创建 Environment |
| `bindings/delete` | scope | 平台确认删除或明确返回 NotFound 后条件清理元数据 |

仅可信服务可传入租户 scope；公网租户认证在 Gateway 完成。端点不持有用户流量，不限制 HTTP/WS/SSH 并发。`x-adx-deadline-ms` 传递入口绝对期限，创建 Environment 时以剩余期限限制 API Server HTTP 等待。API Server 当前不接受此绝对期限：`createTimeoutSeconds` 使用固定配置，使相同创建操作的重试请求体保持一致；后端可能继续运行到自己的期限。参与调用的主机需要同步时钟。写入响应超时不表示拒绝，调用方复用原身份查询或重试。

列表默认每页 50 条，最多 100 条，返回 `bindings` 和 `next_page_token`。分页 token 绑定租户、模板及 template_ref，非法参数或跨范围 token 返回 Invalid；模板不存在返回 NotFound。列表包含 Deleting 元数据，不表示平台运行状态，并发更新期间不提供快照。删除完成后的独立回源请求可重建同名 AgentBinding；回源发现 Deleting 返回 Conflict。其他副本的热缓存允许短暂陈旧，删除中的并发请求可能成功或失败。

同一次 HTTP/WS 用户请求在 Gateway 内部重试时，携带首次选定的 `expected_generation` 和 `bypasscache: true`。元数据已删除或属于新 generation 时返回 Conflict，不重新创建或转到另一次生命周期；`expected_generation` 不属于用户的调用参数。

`/health/live` 表示进程存活；未启用别名收敛时 ready 表示启动完成，后续存储故障由回源明确返回。启用收敛时 ready 还要求订阅恢复和初始扫描完成；断线期间 alias 缓存不作为最新状态。不会周期全表扫描或进行 Environment 健康轮询；成员续租与列表刷新独立运行。

发布文件使用 TemplateDraft，不含 version。每次成功发布生成新数字版本（即使内容相同），响应返回完整 TemplateVersion；发布超时可能已经分配编号，不能自动重放，应通过受控管理通路核对序列及模板记录。序列按 tenant/template 隔离，并与快照原子提交；旧数据必须先经运维转换，服务不会自动编号旧字符串版本。

## 激活缓存

模板缓存位于 Activator 持有的 AgentState，按 tenant/name/version 隔离，容量 1024，采用 FIFO。模板成功读合并在途请求；模板缺失或存储错误不作为结果缓存。Gateway 也保留同样的不可变模板缓存。

AgentBinding 缓存按完整 scope 保存已确认的 Target（含 generation、environment_id、service），采用 LRU 和滑动 TTL。`binding_cache.capacity` 默认 200000，设为 0 禁用；`binding_cache.idle_seconds` 默认 18000（5 小时），必须为正。每次命中更新最近访问顺序和时间；持续访问可以无限续期。闲置过期只在下一次请求访问时检查，不触发后台工作。容量满时淘汰最久未访问的条目，不删除权威 AgentBinding 或 Environment。

`bindings/resolve` 和原始激活的全热请求均使用同一绑定缓存；resolve 在 Activator 校验协议/端口并返回确定的端口，Redis 和 Environment API 均为零次。未命中或 bypass 先取消旧成功标记，再读取 Redis；平台返回身份一致的 Running/ready 或 Pausing/Paused/Resuming，且元数据复核通过后才填充。失败或取消不恢复旧成功标记，较早在途结果不能覆盖后发刷新。删除入口主动失效本副本缓存；启用 registration 时通过 Redis Binding 通知失效其他副本缓存，传播期间允许短暂陈旧；未启用时保留按请求刷新。TTL 管理闲置数据，不保证实时状态一致，删除过程中请求允许成功或失败。

`bypasscache` 默认为 false；true 跳过 AgentBinding 成功绑定，不绕过不可变模板、认证或身份校验，不强制重建 Environment。已有 Running Environment 通过 GET 复用，平台确认不存在时才按原身份 CREATE。HTTP/WS 既有的一次发送前目标重试强制 bypass，并固定 generation；同名重建返回 Conflict，不切换生命周期。SSH 没有新增自动重试。Platform/Environment API 不变。

### 内存容量

默认容量为 200000 条。缓存包含 Binding parameters，其内存占用随参数数量和长度增加。当前字段布局新增后尚未重新测量 RSS，不使用旧结构体的测量值估算本版容量。

更长标识、多 service、请求在途状态和内存分配器都会改变占用。容量按需调整，无按字节硬限额。可运行：

```sh
cargo test --locked -p adx-activator --lib binding_cache_memory_profile -- --ignored --nocapture --test-threads=1
```

## 实例注册

`registration` 为可选配置；配置后同时启用下述别名后台收敛，要求 Redis 通知能力就绪；缺省使用静态地址并按请求处理。示例：[discovery.json](examples/discovery.json)。`instance_id` 在活跃副本之间必须唯一，建议重启后保持稳定；`advertise_url` 必须是 Gateway 可直达的单实例地址，不能使用 `0.0.0.0` 或多实例随机负载均衡地址。地址及认证/TLS 校验沿用 ActivatorClient 的传输策略。

进程绑定监听端口后先注册，再开始服务；注册失败会拒绝启动。默认租约 15 秒，心跳 5 秒；`lease_seconds` 范围 6–300，`heartbeat_seconds` 至少 1 且不超过租约的三分之一。Redis 服务端时间决定租约是否到期，与 AgentBinding 的 5 小时滑动 TTL 独立。正常退出时先停心跳并注销，然后结束 HTTP 服务；异常退出靠租约过期被发现端移除，Gateway 默认最多还需一个刷新周期才看到变更。

每次进程启动生成新的 incarnation，同一个活跃 ID 的其他 incarnation 无法续租或注销它。异常退出后立即用相同 ID 启动可能在旧租约到期前返回 Conflict，调用方应在租约到期后重试。后续续租失败会产生 tracing 警告；不会修改 AgentBinding 元数据或发起 Environment 恢复。成员 ID 同时用于请求亲和与后台 Binding 分工，任何 Activator 仍可通过 Redis 恢复 AgentBinding 身份。

## Environment HTTP 适配契约

- 创建：`POST /api/environment/v1/environments`，将 `adx-<generation>` 拆为 namespace/name，保留镜像、隔离 runtime、资源、业务 env、模板 service 端口、渲染后的 mounts、模板 lifecycle、`init_timeout_seconds`、`readiness_probe` 和 `inheritEntrypoint=true`，并附加配置的 `sandbox_network` 策略。模板 readiness 支持 HTTP、TCP 或 exec 单动作，Activator 映射为公共 Environment 的 `readinessProbe`；runtime profile 由 API Server 注入。显式启动覆盖与保留环境变量在提交前拒绝。
- 查询：`GET /api/instances?instance_id=<id>`，检查单条记录的 ID，Running 才标记 ready。没有返回的 runtime ID 不合成；404 表示当前不可见，不能确认已删除。
- 删除：`DELETE /api/environment/v1/environments/<id>`，API Server 确认成功返回 Deleted；明确 HTTP 404 返回 NotFound，Activator 对这两种结果均按 generation 条件清理 Binding 及其 parameters。断连、超时或不完整响应保留 OutcomeUnknown，不清理绑定。
- 删除使用由动作、tenant 和 Environment ID 确定的 `X-Request-Id`；创建还包含持久化的尝试序号，仅在确定分配前取消后推进；同一组 Activator 副本须使用一致的创建规格、`sandbox_network` 与 `request_timeout_seconds`，未完成操作期间改变请求体会被 API Server 的幂等校验拒绝。禁止重定向及自动重放。HTTP 400/409 映射 Invalid/Conflict，查询 403 映射 NotFound；创建/删除 403 和所有 401 表示上游鉴权不可用，不触发删除收尾。读响应丢失为 Unavailable，写响应丢失为 OutcomeUnknown。

旧配置中的 `ADX_ENVIRONMENT_SERVICE_TOKEN` 已移除；Activator 使用 API Server 管理地址与 `environment_api_key_envs`。Ingress 不再提供 `/api/environment/v2/instances`。

## 模板沙箱网络隔离

独立 Activator 的配置必须提供 `sandbox_network`；嵌入式模式在 `ADX_AGENT_CONFIG` 的 `embedded.sandbox_network` 使用同一结构。缺省情况下，所有经模板创建的新 Environment 统一使用该策略。部署者可通过 `runtime_policies` 为指定 runtime 作出显式选择，模板和用户启动环境变量不能覆盖它。直接调用 Environment API 的其他场景不受此配置影响；已有 Environment 不更新。

```json
{
  "sandbox_cidrs": ["10.0.0.0/20"],
  "internal_cidrs": ["192.168.0.0/16"],
  "public_egress": true,
  "egress_allow": [
    {"cidr": "192.168.1.10/32", "protocol": "tcp", "ports": {"first": 4000, "last": 4000}},
    {"cidr": "192.168.1.53/32", "protocol": "udp", "ports": {"first": 53, "last": 53}},
    {"cidr": "192.168.1.53/32", "protocol": "tcp", "ports": {"first": 53, "last": 53}}
  ]
}
```

示例地址必须替换为实际 Environment 地址池与服务地址。`sandbox_cidrs` 非空并覆盖全部 Environment 地址池；`internal_cidrs` 列出需要默认拒绝的内部地址段。`egress_allow` 支持 TCP/UDP 端口范围，或不带 ports 的 ICMP；与任一 Environment 地址池重叠的例外直接拒绝。CIDR、规则数量与端口范围在启动及创建边界校验，非法配置返回 Invalid，不会退化为无策略创建。

映射为 schemaVersion 2 的 stateful ACL，入站/出站默认 deny。用户规则按优先级依次为：Environment 地址池双向 deny（400）、显式业务外连 allow（300）、内网及常见保留地址 deny（200）、公网 allow（100）。公网规则允许 IPv4 与 IPv6 全球单播范围，公共出口仍受宿主网络策略限制。DNS 使用 UDP/TCP 53 的精确地址例外；模型服务使用实际地址及端口。服务地址改变时须更新配置，当前不自动解析域名生成规则。

模板 service 端口会作为 Environment `ports` 提交，Platform 保留 Execd 及这些业务端口的管理/转发访问。Platform 的保留规则优先级高于上述规则，因此隔离依靠所有 Environment 的出站限制，不能仅依赖目的端入站；不能据此声称共享 Execd token 或所有恶意流量绕过风险已解决。不要给用户开放网络策略更新的管理凭据。

本地测试覆盖规则优先级、公网与内网区分、模型/DNS 端口例外、错误配置、HTTP/同进程映射及 API Server 规格转换。这些测试不覆盖外部网络设施或业务服务的连通性。

`runtime_policies` 是可选的 runtime 名称到策略的映射：`deployment`（也是未配置 runtime 的缺省行为）使用上述严格 ACL；`unrestricted` 不向 Environment 请求写入 ADX 网络过滤策略，原生运行时部署策略仍然生效。例如，部署者显式设置 `"runtime_policies":{"supervisor":"unrestricted"}`，可运行使用原生网络策略的 supervisor Agent，而其他 runtime 保持严格 ACL。此选项会放弃该 runtime 的 ADX 沙箱间/内网过滤；它不保证与上述隔离语义等价。仅在部署者明确接受此网络边界时启用，且不得把管理凭据交给最终用户。ADX 不会根据 backend 拒绝自动改用 unrestricted。配置仍整体校验；修改策略不会更新已有 Environment，未完成创建也须遵循同一身份/规格的幂等约束。

当前 sandboxd ACL 后端仅支持 IPv4。`sandbox_network` 的地址池、内部地址段和服务例外均须使用 IPv4 CIDR；启动校验拒绝 IPv6 配置，公网放行也仅生成 IPv4 规则。

## 休眠实例的解析

模板 lifecycle 的空闲窗口、pause retention/timeout/storage 映射到 `idleTimeoutSeconds`、`idleAction`、`pauseOptions`；默认策略不额外写入请求字段。生命周期状态只作为 Platform 观察值，不持久化到 AgentBinding。暂停相关状态返回稳定 Target，ready 仍为 false；真正的鉴权业务流由 Gateway EnsureRunning 唤醒。该行为对热缓存、冷缓存及 bypass 一致，不增加 Agent 自己的恢复控制器。配置与边界见 [自动暂停与按需恢复](../README.md#自动暂停与按需恢复)。

激活请求的 parameters 为原始 URL 表单字节，由共享激活流程仅在 Binding 缺失时解析。已有 Binding 完全忽略输入；并发创建复用成功落库的参数。Template.env 与 mounts.host_path 使用持久化参数渲染。参数当前明文存储，无独立 prepare 或模型专用 launch 配置。

Volume 定义、固定 UID、Binding 关联及迟到激活补偿见 [Agent 受管卷](../README.md#受管卷)。创建连接断开在原请求期限内查询原身份并退避重试；不增加持久化创建等待队列。

Volume management retries preserve the saved Binding and original deadline.
Explicit credential/request refusals, missing resources and returned identity
conflicts fail immediately; transport ambiguity or a missing UID in an ensure
success remains an unknown outcome. Volume IDs use 1–256 ASCII letters, digits,
dots, underscores or hyphens, excluding the URL-normalized `.` and `..` segments.

## 别名收敛配置

独立和嵌入式 Activator 均由 `registration` 自动启用后台协调，没有单独的 `reconciliation` 配置字段或启停开关。独立模式示例见 [discovery.json](examples/discovery.json)，嵌入式模式在 `embedded` 内提供同样的 registration：

```json
{
  "registration": {
    "instance_id": "activator-1",
    "advertise_url": "http://activator-1:8091",
    "lease_seconds": 15,
    "heartbeat_seconds": 5
  }
}
```

配置 registration 后自动完成成员注册、租约续租、变更监听、启动恢复扫描和成员变化接管。后台默认并发 8、单次工作期限 60 秒、成员刷新间隔 5 秒；这些是内部默认值，不是部署配置项。省略 registration 时不注册成员、不启动后台协调，保留按请求处理及请求触发缓存刷新。

参与后台收敛的所有副本使用相同 Redis URL/namespace 和各自稳定且唯一的实例 ID。独立模式的请求侧可通过同一 Redis 注册目录发现可调用的 Activator。嵌入式 `advertise_url` 是注册身份地址，不新增内部 HTTP 监听或改变本地调用模式；远程 ActivatorClient 的发现目录不能包含没有内部 HTTP 接口的嵌入式成员。

启用 registration 时，Redis 由部署者配置 `notify-keyspace-events 'K$g'`；Activator 仅检查，不自动修改。需要 CONFIG GET、PSUBSCRIBE 和普通元数据访问权限。启动先确认订阅，再扫描持久化 Binding；断线后先订阅再补读，恢复期间 readiness 返回 503 且 alias 缓存不作为最新状态。成员列表默认每 5 秒刷新，变化后重新分配 owner 并扫描，包括缩容和租约过期。没有周期全量 Binding 扫描；已开始工作的对象按结果短暂退避后继续核对。owner 只用于减少重复调用，CAS 与 Platform 幂等仍为正确性边界。

Scope 请求示例：`{"scope":{"tenant":"tenant","template":"app","template_ref":{"alias":"production"},"binding_id":"workspace"}}`。版本引用改用 `"template_ref":{"version":"1"}`。内部 Scope 保留类型，外部 URN 只通过 reference 是否纯数字解析版本或 Alias。返回的 AgentBinding current/secondary 包含真实版本及 EnvID；Binding generation 在升级期间不变。

Alias 配置统一通过 Ingress 的公开 Agent 管理 API 写入，独立和嵌入式模式使用同一路径与租户 API Key，身份规则与模板发布一致。Ingress 从凭据确定 tenant，从 URL 确定 template/alias；请求体不接受 tenant 覆盖。内部 aliases/put 仅供 Gateway→独立 Activator 服务间 RPC，运维不需要直连该端点或 Redis。

先发布 app 的版本 1、2，再保存完整目标态：

```sh
curl --fail-with-body -X PUT "$ADX_SERVER_ADDRESS/api/agent/v2/templates/app/aliases/production" \
  -H "Authorization: Bearer $ADX_TOKEN" \
  -H 'Content-Type: application/json' \
  --data-binary '{"seed":"stable","versions":[{"version":"1","weight":9},{"version":"2","weight":1}],"expected_revision":null}'
```

响应为 `{"alias":{"config":{...},"revision":"..."}}`。更新前 GET 同一路径，将 `alias.revision` 填入 expected_revision，并提交最新完整分布；seed 保持不变。409 表示并发修改，应重读后编辑；超时可能已写入，先查询同一 Alias，不能盲目重试。首次配置和改权都只保存目标态，由后台主动准备，不等待流量触发。

Alias 更新仅分页读取该 tenant/template/alias 的现有 Binding 索引，再按 owner 分工。启动、订阅恢复和成员变化仍进行全量恢复扫描；单个 Binding 通知只入队该对象并失效缓存。

目标模板缺失时不创建新的 secondary。已有 Preparing 候选缺失模板时记录带 scope、版本与 EnvID 的错误，先 CAS 标记 Retiring，再清理候选 Env/卷关联，确认完成后 CAS 清除 secondary；current 保持不变。清理失败或结果未知保留退役引用，后台重试；不会把模板 NotFound 当作完成，也不会因重试不断生成新的候选身份。

用户删除在持久化 Deleting 后，即使前台返回失败或结果未知，owner 仍会接续删除。暂时故障、冲突与未知结果默认约 5 秒后重试；尚需下一步时约 1 秒继续。Invalid/Unsupported 记录阻塞警告后停止主动重试，等待相关变更或恢复扫描；未成功保存 Deleting 的失败不代表删除意图已提交。可通过 Binding GET/list 查看 Deleting/current/secondary，并结合 `Binding convergence waiting or blocked` 的 scope/error 日志排查。未启用 registration 时仍需请求方重试。

current 切换后调用既有 delete，不新增退役协议、连接计数或排空等待。Platform 优雅退出属于外部能力缺口，本次不实现；当前删除会直接清理运行时。删除确认/NotFound 后释放旧 Env 的卷 owner，再 CAS 清除 secondary；未知结果保留引用。Binding 整体删除覆盖两个引用。

真实 Redis 测试（一次性数据库；重连用例会关闭该数据库的普通或 Pub/Sub 连接）：

```sh
ADX_AGENT_TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
  cargo test --locked -p adx-activator --test reconciliation -- --ignored --test-threads=1
```

这些测试使用模拟 Platform，不证明真实运行时、共享文件系统、连接排空或集群端到端行为。


### 创建排队取消与退避

HTTP 与进程内 Environment 适配器保留 Platform 的 `adx_core::scheduling::SchedulingFailure`，映射为 `EnvironmentError::SchedulingCancelled`。只有结构化的确定排队取消才允许发起下一次创建尝试；HTTP 必须同时为504且包含 `DEADLINE_EXCEEDED / never / not_started / schedulingFailure`，进程内调用使用协议的取消标记。普通超时、响应丢失、未找到 Env、损坏诊断和业务启动失败均不构成推进尝试的依据。Platform 的排队超时及 API Server 的600秒完成缓存保持原行为。

Activator 将每个仍被 Binding 引用的 Env 的当前尝试序号、下次检查时间和退避秒数保存在 `creation_attempts`。收到确定取消后按 Binding generation、活动引用和原尝试元数据做 revision CAS，仅推进一次序号；同一个 Env ID、版本、规格及 Volume UID 不变。序号0保持原 Request ID，后续 Request ID由tenant、Env ID和序号派生；多副本及进程重启复用同一尝试身份。结果未知继续查询或重放原身份，不能通过新序号续开未知操作的预算。

确认取消后，退避为5、10、20、40、60秒并在60秒封顶。后台将对象放回待处理队列，等待期间不占worker；元数据通知、成员变化或进程重启可能提前唤醒检查，但持久化的下次创建时间仍生效。每轮先查询候选 Env；已有 Pending/Starting、重启恢复、Paused或Failed 执行均直接观察，不再 CREATE。候选不存在且达到检查时间后，统一查询旧current Env，不区分无卷、共享卷或独占卷模板：旧Env仍在运行、启动、暂停中、恢复或失败时只延后检查，不发创建请求、不推进尝试序号；旧Env已Paused/Deleted或查询不存在时才提交候选创建。这项升级策略在候选第一次创建前即生效，也适用于后续重试；Binding首次激活current不检查旧Env。Running仅表示旧Env尚未暂停，不证明有活跃连接；该检查不主动pause旧Env，也不构成查询与创建之间的原子排空保证。仅查询不存在不授权改变Request ID，结果未知仍保留原尝试身份。

Activator不读取卷occupancy；卷的实际占用、释放与准入始终由Platform判定。重试前仍按既有流程ensure并固定Volume UID，不新增卷管理接口。

Alias目标变化会先退役原候选；清理完成时同时移除该候选的创建尝试元数据，不改Binding generation。此机制不会改变业务请求对current的解析、主动暂停旧Env或保留休眠状态的既有灰度行为。
