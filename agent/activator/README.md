# Activator

Activator 是无状态产品控制模块，可作为独立进程 `adx-activator`，也可嵌入 API Server 所在进程。多个副本连接相同 ADX Redis namespace，通过条件写提交唯一 AgentBinding 身份。独立模式调用 API Server 的现有 Environment HTTP 管理接口；嵌入式模式直接调用 API Server 的 Environment 应用服务。独立模式可通过 Redis 注册成员租约；无选主、生命周期所有权或恢复扫描。

Gateway 在独立模式通过 ActivatorClient 发起 HTTP 请求，在嵌入式模式通过 LocalControl 直接调用 Activator。公开认证与业务流量转发留在 Gateway；Activator 管理 Template/AgentBinding 状态。配置见[Agent 进程配置](../README.md#进程配置)。

Agent 的 `CreateEnvironment`/`ExecutionSpec` 在本 crate 的 `environment_request` 转成通用 Environment 创建请求；`HttpEnvironment` 与 `LocalEnvironment` 共用映射、操作 ID 和响应解释。API Server 只转换自己的 Environment 请求到 Platform EnvironmentSpec，并负责 runtime 注入。嵌入式适配通过 `EnvironmentApplication` 调用同一 `EnvironmentService.create_request()`，不依赖 Ingress mapper，也不在 Agent 层构造 Platform 类型。嵌入式配置固定创建预算为 60 秒，调用等待仍受请求剩余期限约束；提交后超时保留原 ID 并返回 OutcomeUnknown。

独立进程启动读取 `ADX_ACTIVATOR_CONFIG` 指向的 JSON，示例见 [local.json](examples/local.json)。入口服务凭据为 `ADX_ACTIVATOR_SERVICE_TOKEN`。`environment_url` 指向 API Server（示例端口 8081 须与其 `listen` 配置一致），`environment_api_key_envs` 将租户映射到保存该租户 Platform API Key 的环境变量名，例如 `{"tenant":"ADX_PLATFORM_TENANT_API_KEY"}`。这里的 Platform API Key 用于 Environment 管理，与最终用户的模型 API Key 无关。凭据内容不写入 JSON；未配置租户的查询返回 NotFound，创建/删除在提交请求前返回 Unavailable。每项 key 必须属于对应租户，不能使用跨租户管理员 key；现有查询响应不含租户字段，Activator 无法独立验证配置归属。HTTP 明文必须显式允许，否则客户端需要 TLS 和受信任 CA。

内部端点统一为 POST `/internal/adx/v1/`：

| 后缀 | 请求 | 行为 |
| --- | --- | --- |
| `templates/publish` | tenant、template | 发布不可变版本 |
| `templates/get` | tenant、name、version | 查询模板 |
| `bindings/get` | scope | 查询产品元数据 |
| `bindings/list` | tenant、template、version、page_size?、page_token? | 有界分页查询产品元数据，不访问 Environment |
| `bindings/activate` | scope、expected_generation?、bypasscache?、parameters? | 热命中续期返回 AgentBinding 绑定；未命中或显式 bypass 时读取 Redis 并查询/创建 Environment |
| `bindings/delete` | scope | 平台确认删除或明确返回 NotFound 后条件清理元数据 |

仅可信服务可传入租户 scope；公网租户认证在 Gateway 完成。端点不持有用户流量，不限制 HTTP/WS/SSH 并发。`x-adx-deadline-ms` 传递入口绝对期限，创建 Environment 时以剩余期限限制 API Server HTTP 等待。API Server 当前不接受此绝对期限：`createTimeoutSeconds` 使用固定配置，使相同创建操作的重试请求体保持一致；后端可能继续运行到自己的期限。参与调用的主机需要同步时钟。写入响应超时不表示拒绝，调用方复用原身份查询或重试。

列表默认每页 50 条，最多 100 条，返回 `bindings` 和 `next_page_token`。分页 token 绑定租户及模板版本，非法参数或跨范围 token 返回 Invalid；模板不存在返回 NotFound。列表包含 Deleting 元数据，不表示平台运行状态，并发更新期间不提供快照。删除完成后的独立回源请求可重建同名 AgentBinding；回源发现 Deleting 返回 Conflict。其他副本的热缓存允许短暂陈旧，删除中的并发请求可能成功或失败。

同一次 HTTP/WS 用户请求在 Gateway 内部重试时，携带首次选定的 `expected_generation` 和 `bypasscache: true`。元数据已删除或属于新 generation 时返回 Conflict，不重新创建或转到另一次生命周期；`expected_generation` 不属于用户的调用参数。

`/health/live` 和 `/health/ready` 表示进程启动完成；启动时验证 Redis schema，后续存储故障由回源请求明确返回；热缓存可继续服务。不进行 AgentBinding 后台全表扫描或健康轮询；启用注册后仅周期续租实例成员资格。

## 激活缓存

模板缓存位于 Activator 持有的 AgentState，按 tenant/name/version 隔离，容量 1024，采用 FIFO。模板成功读合并在途请求；模板缺失或存储错误不作为结果缓存。Gateway 也保留同样的不可变模板缓存。

AgentBinding 缓存按完整 scope 保存已确认的 Target（含 generation、environment_id、service），采用 LRU 和滑动 TTL。`binding_cache.capacity` 默认 200000，设为 0 禁用；`binding_cache.idle_seconds` 默认 18000（5 小时），必须为正。每次命中更新最近访问顺序和时间；持续访问可以无限续期。闲置过期只在下一次请求访问时检查，不触发后台工作。容量满时淘汰最久未访问的条目，不删除权威 AgentBinding 或 Environment。

全热请求直接返回绑定，Redis 和 Environment API 均为零次。未命中或 bypass 先取消旧成功标记，再读取 Redis；平台返回身份一致的 Running/ready 或 Pausing/Paused/Resuming，且元数据复核通过后才填充。失败或取消不恢复旧成功标记，较早在途结果不能覆盖后发刷新。删除入口主动失效本副本缓存；无跨副本失效广播，其他副本可暂时使用旧绑定。TTL 管理闲置数据，不保证实时状态一致，删除过程中请求允许成功或失败。

`bypasscache` 默认为 false；true 跳过 AgentBinding 成功绑定，不绕过不可变模板、认证或身份校验，不强制重建 Environment。已有 Running Environment 通过 GET 复用，平台确认不存在时才按原身份 CREATE。HTTP/WS 既有的一次发送前目标重试强制 bypass，并固定 generation；同名重建返回 Conflict，不切换生命周期。SSH 没有新增自动重试。Platform/Environment API 不变。

### 内存容量

默认容量为 200000 条。缓存包含 Binding parameters，其内存占用随参数数量和长度增加。当前字段布局新增后尚未重新测量 RSS，不使用旧结构体的测量值估算本版容量。

更长标识、多 service、请求在途状态和内存分配器都会改变占用。容量按需调整，无按字节硬限额。可运行：

```sh
cargo test --locked -p adx-activator --lib binding_cache_memory_profile -- --ignored --nocapture --test-threads=1
```

## 实例注册

`registration` 为可选配置；缺省使用静态地址。示例：[discovery.json](examples/discovery.json)。`instance_id` 在活跃副本之间必须唯一，建议重启后保持稳定；`advertise_url` 必须是 Gateway 可直达的单实例地址，不能使用 `0.0.0.0` 或多实例随机负载均衡地址。地址及认证/TLS 校验沿用 ActivatorClient 的传输策略。

进程绑定监听端口后先注册，再开始服务；注册失败会拒绝启动。默认租约 15 秒，心跳 5 秒；`lease_seconds` 范围 6–300，`heartbeat_seconds` 至少 1 且不超过租约的三分之一。Redis 服务端时间决定租约是否到期，与 AgentBinding 的 5 小时滑动 TTL 独立。正常退出时先停心跳并注销，然后结束 HTTP 服务；异常退出靠租约过期被发现端移除，Gateway 默认最多还需一个刷新周期才看到变更。

每次进程启动生成新的 incarnation，同一个活跃 ID 的其他 incarnation 无法续租或注销它。异常退出后立即用相同 ID 启动可能在旧租约到期前返回 Conflict，调用方应在租约到期后重试。后续续租失败会产生 tracing 警告；不会修改 AgentBinding 元数据或发起 Environment 恢复。注册地址只决定缓存亲和，任何 Activator 仍可通过 Redis 恢复 AgentBinding 身份。

## Environment HTTP 适配契约

- 创建：`POST /api/environment/v1/environments`，将 `adx-<generation>` 拆为 namespace/name，保留镜像、隔离 runtime、资源、业务 env、模板 service 端口、渲染后的 mounts、模板 lifecycle、`init_timeout_seconds`、`readiness_probe` 和 `inheritEntrypoint=true`，并附加配置的 `sandbox_network` 策略。模板 readiness 支持 HTTP、TCP 或 exec 单动作，Activator 映射为公共 Environment 的 `readinessProbe`；runtime profile 由 API Server 注入。显式启动覆盖与保留环境变量在提交前拒绝。
- 查询：`GET /api/instances?instance_id=<id>`，检查单条记录的 ID，Running 才标记 ready。没有返回的 runtime ID 不合成；404 表示当前不可见，不能确认已删除。
- 删除：`DELETE /api/environment/v1/environments/<id>`，API Server 确认成功返回 Deleted；明确 HTTP 404 返回 NotFound，Activator 对这两种结果均按 generation 条件清理 Binding 及其 parameters。断连、超时或不完整响应保留 OutcomeUnknown，不清理绑定。
- 创建/删除使用由动作、tenant 和 Environment ID 确定的 `X-Request-Id`；同一组 Activator 副本须使用一致的创建规格、`sandbox_network` 与 `request_timeout_seconds`，未完成操作期间改变请求体会被 API Server 的幂等校验拒绝。禁止重定向及自动重放。HTTP 400/409 映射 Invalid/Conflict，查询 403 映射 NotFound；创建/删除 403 和所有 401 表示上游鉴权不可用，不触发删除收尾。读响应丢失为 Unavailable，写响应丢失为 OutcomeUnknown。

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
