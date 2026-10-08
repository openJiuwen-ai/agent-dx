# Activator

Activator 是无状态产品控制模块，可作为独立进程 `adx-activator`，也可嵌入 API Server 所在进程。多个副本连接相同 ADX Redis namespace，通过条件写提交唯一 Environment 身份。独立模式调用 Gateway 的通用 Sandbox HTTP 接口；嵌入式模式直接调用 API Server 的 Sandbox 应用服务。独立模式可通过 Redis 注册成员租约；无选主、生命周期所有权或恢复扫描。

Gateway 在独立模式通过 ActivatorClient 发起 HTTP 请求，在嵌入式模式通过 LocalControl 直接调用 Activator。公开认证与业务流量转发留在 Gateway；Activator 管理 Template/Environment 状态。配置见 [Agent 部署说明](../README.md#部署)。

独立进程启动读取 `ADX_ACTIVATOR_CONFIG` 指向的 JSON，示例见 [local.json](examples/local.json)。服务凭据为 `ADX_ACTIVATOR_SERVICE_TOKEN` 与 `ADX_SANDBOX_SERVICE_TOKEN`，均不写入示例配置。当前进程监听 HTTP，要求显式允许内网明文；生产须置于 TLS 服务代理后并限制访问。

内部端点统一为 POST `/internal/adx/v1/`：

| 后缀 | 请求 | 行为 |
| --- | --- | --- |
| `templates/publish` | tenant、template | 发布不可变版本 |
| `templates/get` | tenant、name、version | 查询模板 |
| `environments/get` | scope | 查询产品元数据 |
| `environments/list` | tenant、template、version、page_size?、page_token? | 有界分页查询产品元数据，不访问 Sandbox |
| `environments/activate` | scope、expected_generation?、bypasscache? | 热命中续期返回 Env 绑定；未命中或显式 bypass 时读取 Redis 并查询/创建 Sandbox |
| `environments/delete` | scope | 平台确认删除后条件清理元数据 |

仅可信服务可传入租户 scope；公网租户认证在 Gateway 完成。端点不持有用户流量，不限制 HTTP/WS/SSH 并发。`x-adx-deadline-ms` 传递入口绝对期限，创建 Sandbox 时继续作为内部请求的 `deadline_unix_ms` 传递；各服务只能按自身配置缩短它，不能重新开始计时。服务主机需要同步时钟。写入响应超时不表示拒绝，调用方复用原身份查询/重试。

列表默认每页 50 条，最多 100 条，返回 `environments` 和 `next_page_token`。分页 token 绑定租户及模板版本，非法参数或跨范围 token 返回 Invalid；模板不存在返回 NotFound。列表包含 Deleting 元数据，不表示平台运行状态，并发更新期间不提供快照。删除完成后的独立回源请求可重建同名 Environment；回源发现 Deleting 返回 Conflict。其他副本的热缓存允许短暂陈旧，删除中的并发请求可能成功或失败。

同一次 HTTP/WS 用户请求在 Gateway 内部重试时，携带首次选定的 `expected_generation` 和 `bypasscache: true`。元数据已删除或属于新 generation 时返回 Conflict，不重新创建或转到另一次生命周期；`expected_generation` 不属于用户的调用参数。

`/health/live` 和 `/health/ready` 表示进程启动完成；启动时验证 Redis schema，后续存储故障由回源请求明确返回；热缓存可继续服务。不进行 Env 后台全表扫描或健康轮询；启用注册后仅周期续租实例成员资格。

## 激活缓存

模板缓存位于 Activator 持有的 AgentState，按 tenant/name/version 隔离，容量 1024，采用 FIFO。模板成功读合并在途请求；模板缺失或存储错误不作为结果缓存。Gateway 也保留同样的不可变模板缓存。

Env 缓存按完整 scope 保存已确认的 Target（含 generation、sandbox_id、service），采用 LRU 和滑动 TTL。`env_cache.capacity` 默认 200000，设为 0 禁用；`env_cache.idle_seconds` 默认 18000（5 小时），必须为正。每次命中更新最近访问顺序和时间；持续访问可以无限续期。闲置过期只在下一次请求访问时检查，不触发后台工作。容量满时淘汰最久未访问的条目，不删除权威 Env 或 Sandbox。

全热请求直接返回绑定，Redis 和 Sandbox API 均为零次。未命中或 bypass 先取消旧成功标记，再读取 Redis；平台返回身份一致的 Running/ready 且元数据复核通过后才填充。失败或取消不恢复旧成功标记，较早在途结果不能覆盖后发刷新。删除入口主动失效本副本缓存；无跨副本失效广播，其他副本可暂时使用旧绑定。TTL 管理闲置数据，不保证实时状态一致，删除过程中请求允许成功或失败。

`bypasscache` 默认为 false；true 跳过 Env 成功绑定，不绕过不可变模板、认证或身份校验，不强制重建 Sandbox。已有 Running Sandbox 通过 GET 复用，平台确认不存在时才按原身份 CREATE。HTTP/WS 既有的一次发送前目标重试强制 bypass，并固定 generation；同名重建返回 Conflict，不切换生命周期。SSH 没有新增自动重试。Platform/Sandbox API 不变。

### 内存容量

默认容量为 200000 条。Linux 64 位本地 RSS 测量中，200000 条真实 LRU 缓存增量约 167.1 MiB，约 875 字节/条。样例 tenant/template/version/env 长度分别为 36/32/8/36 字节，generation 为 36、sandbox_id 为 40，每条一个 HTTP service；结构体 Scope 为 96 字节、Binding 为 200 字节，RSS 还包含字符串分配、LRU 和分配器开销。

这是缓存样例测量，不是进程总 RSS 上限；更长标识、多 service、请求在途状态和内存分配器都会改变占用。容量按需调整，无按字节硬限额。复现：

```sh
cargo test --locked -p adx-activator --lib env_cache_memory_profile -- --ignored --nocapture --test-threads=1
```

## 实例注册

`registration` 为可选配置；缺省保留静态地址部署。示例：[discovery.json](examples/discovery.json)。`instance_id` 在活跃副本之间必须唯一，建议重启后保持稳定；`advertise_url` 必须是 Gateway 可直达的单实例地址，不能使用 `0.0.0.0` 或多实例随机负载均衡地址。地址及认证/TLS 校验沿用 ActivatorClient 的传输策略。

进程绑定监听端口后先注册，再开始服务；注册失败会拒绝启动。默认租约 15 秒，心跳 5 秒；`lease_seconds` 范围 6–300，`heartbeat_seconds` 至少 1 且不超过租约的三分之一。Redis 服务端时间决定租约是否到期，与 Env 的 5 小时滑动 TTL 独立。正常退出时先停心跳并注销，然后结束 HTTP 服务；异常退出靠租约过期被发现端移除，Gateway 默认最多还需一个刷新周期才看到变更。

每次进程启动生成新的 incarnation，同一个活跃 ID 的其他 incarnation 无法续租或注销它。异常退出后立即用相同 ID 启动可能在旧租约到期前返回 Conflict，部署应重试启动。后续续租失败会产生 tracing 警告；不会修改 Env 元数据或发起 Sandbox 恢复。注册地址只决定缓存亲和，任何 Activator 仍可通过 Redis 恢复 Env 身份。
