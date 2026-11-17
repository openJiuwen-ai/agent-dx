# Sandbox 创建与运行异常指标合同

本合同覆盖 API Server 创建调用、Coordinator 已持久化的新建结果、运行期中断和恢复。
公共 API 的 Sandbox 对应内部 Environment。指标通过现有私有 `metrics_listen` 导出，
health-checker 负责阈值、最小样本、持续触发/恢复、缺数处理和通知。

## 创建调用与新建结果

用户入口的创建成功率统一按 API Server 的完整 `create` 调用计量：
调度失败、启动失败和确认业务超时均是 `failure`；传输超时、响应丢失等无法确认结果的
情况是 `unknown`；调用 future 被丢弃是 `cancelled`。鉴权和参数校验前的拒绝单列。
REST、SSE、embedded 共享创建服务，SSE accepted 不提前计成功。
重试和缓存重放各算一次调用，已有 Running 复用返回成功也算调用成功。

**创建调用成功率 = success / (success + failure)**，其中 failure 包括确认超时。
必须同时查看 unknown、cancelled、拒绝、当前排队/启动数和等待年龄。
这个比值表示已获得确定结果的调用，不表示去重逻辑请求的全链路成功率。

Coordinator 另外记录已持久化的初次创建结果，首次权威 Running 为 success；
持久化生命周期中的终态 Failed 为 failure，其中结构化 DeadlineExceeded 为 timeout。
结果只裁定一次，重复提交、已有 Running 复用、resume、restart、recovery 均不补计。
默认中央和本地创建在成功前只保留内存预留，失败预留不会为统计而写入 Redis；
这些失败、调度失败及确认超时通过 API 的调用结果和阶段分类观测。
因此持久化创建计数不能作为默认创建路径的完整成功率分母；成功率使用上面的 API 调用口径。

### 成功新建调用耗时 P95

API Server 从本次 create 应用服务调用开始计时，截至确认 Published Running、路由目录已覆盖
同一 assignment generation 和已发布 revision。包含本次调用内的排队、runtime 准备、配置的
readiness、路由激活、状态发布及目录确认；不包含确认后的内部收尾等待，也不承诺用户应用的任意业务健康。
确认时仅暂存耗时，完整 create 服务调用最终成功后，才与 success 结果在同一指标锁内提交样本。
若在收尾阶段取消，只计 cancelled，不增加成功耗时样本。

只有实际发起初次新建且成功返回的调用进入新直方图。Node 的 `newly_created` 响应标志区分
实际新建和 Running 重放；API 另外排除 pending 重试、缓存重放、重复请求等待和已有 Running 复用。
unknown、失败和取消不进入成功耗时样本；后续确认成功不补记最早起点，也不回填这份直方图。
客户端断连不必然取消服务调用；SSE 后台任务若继续完成，仍按其最终服务结果统计。
该指标正式名称是**成功新建调用耗时**，与跨重试、跨重启的逻辑创建首次受理耗时不同。
没有成功样本时分位数无足够数据，不能显示为 0 秒或据此判定健康。

## 指标与上报位置

所有聚合指标均不含 sandbox_id、request_id、event_id、incident_id 标签。
`cluster`、`adx_env` 是采集侧注入的隔离标签。部署侧须提供真实值。

| 指标 | 类型/单位 | 标签和边界 | 上报位置 |
| --- | --- | --- | --- |
| `adx_environment_create_requests_total` | counter/调用 | 原有 source=http、embedded；HTTP 在鉴权前 | API Server |
| `adx_environment_create_responses_total` | counter/调用 | 原有 outcome=success、failure、unknown、cancelled；含重试与重放 | API Server create 完成/取消 |
| `adx_environment_create_in_flight` | gauge/调用 | 原有；只表示活跃调用 | API Server |
| `adx_environment_create_duration_seconds` | histogram/秒 | 原有混合结果调用耗时 | API Server |
| `adx_environment_create_success_call_duration_seconds` | histogram/秒 | 新增，成功实际新建调用；无业务 ID 标签 | API Server 完整 create 调用成功时 |
| `adx_environment_create_errors_total` | counter/调用 | 新增 outcome=failure、unknown、cancelled；stage、reason 有限分类 | API Server 调用结束 |
| `adx_environment_create_rejections_total` | counter/请求 | 新增 reason 有限分类；不属于 create 调用结果分母 | API Server HTTP/embedded 入口 |
| `adx_environment_owned_creation_results_total` | counter/env | 新增 outcome=success、failure、timeout；已持久化的初次结果 | Coordinator 与 env 结果同 Redis CAS |
| `adx_environment_creating` | gauge/env | 新增 state=queued、starting；中央队列或已分配未裁定 | Coordinator 当前目录 |
| `adx_environment_create_oldest_wait_seconds` | gauge/秒 | 新增 state=queued、starting；任一起点未知时该状态年龄样本缺失 | Coordinator 当前目录 |
| `adx_environment_create_wait_age_unknown` | gauge/env | 新增 state=queued、starting；起点缺失/无效的数量 | Coordinator 当前目录 |
| `adx_environment_interruptions_total` | counter/执行中断 | 新增 reason、runtime_class；同一次执行不重复计数 | Coordinator 接收证据/判定节点失联 |
| `adx_worker_incidents_total` | counter/节点事故 | 新增 reason=node_unreachable；同一次失联周期只计一次，恢复可路由后再次失联另计 | Coordinator 节点所有权失效 |
| `adx_environment_recovery_results_total` | counter/恢复裁定 | 新增 result=recovered、unrecoverable、closed_by_user | Coordinator 与 env 结果同 Redis CAS |
| `adx_environment_unrecovered` | gauge/env | 新增 state=waiting、recovering、unrecoverable；当前保留摘要 | Coordinator 当前目录 |
| `adx_environment_operation_unknown` | gauge/env | 新增；操作意图已发布、节点随后失联的当前对象 | Coordinator 当前目录 |
| `adx_environment_lifecycle_operations_total` | counter/操作调用 | 新增 operation=reload、delete、pause；intent=user、idle、pressure、policy；outcome=success、failure、unknown、cancelled | adxlet 操作结束 |
| `adx_node_pressure_pause_skipped_total` | counter/次评估 | 新增 reason=budget_exhausted、in_flight_exhausted、window_too_young；启动即导出零基线 | adxlet 压力暂停评估被拒或豁免 |
| `adx_node_pressure_level` | gauge/等级 | 新增 resource=memory、disk；0=Normal、1=High；仅压力采样已配置的节点导出 | adxlet 当前板值 |
| `adx_node_pressure_percent` | gauge/百分比 | 新增 resource=memory、disk；0-100 | adxlet 当前板值 |
| `adx_node_pressure_last_transition_unix_seconds` | gauge/Unix 秒 | 新增 resource=memory、disk；从未观测时为 0 | adxlet 当前板值 |
| `adx_node_pressure_observed_at_unix_seconds` | gauge/Unix 秒 | 新增；从未成功采样时为 0，该组 gauge 仅在配置压力采样的节点存在 | adxlet 当前板值 |
| `adx_node_pressure_consecutive_failures` | gauge/轮数 | 新增；连续采样失败轮数，成功采样归零；等级冻结仍可见 | adxlet 当前板值 |
| `adx_observability_snapshot_age_seconds` | gauge/秒 | 新增；距成功读取持久化累计数的本地时间 | Coordinator，约每秒刷新 |
| `adx_observability_history_known` | gauge/布尔 | 新增；是否具有可信统计覆盖起点，尚未确认起点时为 0 | Coordinator |
| `adx_observability_started_timestamp_seconds` | gauge/Unix 秒 | 新增；可信统计覆盖起点，未知时不导出 | Coordinator |

所有直方图共享边界：0.01、0.05、0.1、0.5、1、2、5、10、30、60、300、3600、+Inf 秒。
API errors/rejections and adxlet lifecycle-operation producers export bounded
zero-valued label tuples from startup, allowing `increase()` to observe their
first event after a prior scrape. Lifecycle baselines include reload/user,
pause/user-idle-or-pressure, and delete/user-idle-or-policy; raw error details
never become labels. Events before the first scrape remain outside this
guarantee. The pressure-pause skip counter follows the same discipline with
one baseline per reason.

API/adxlet counter 为进程内计数，重启归零；取消计数不等于用户主动取消，也不等于平台失败。
Coordinator 业务累计数与权威状态原子写入 Redis，重连、普通重启及回放不归零。
源数据建立前的历史不补计。人工清库或重建 namespace 后计数重新开始；普通 Coordinator epoch
变化只隔离旧写入。旧 exporter 被新 epoch 隔离后 `/metrics` 返回 503。
Redis 读取失败时保留旧累计快照并增加 snapshot_age，不能将陈旧值视为恢复。
旧 Header 的覆盖起点缺失或为 0 时，Coordinator 在当前 epoch 完整读取并校验统计后，
通过 Header CAS 持久化当前时间 T0；仅从 T0 向后声明可信覆盖，不补齐此前历史，也不清空已有计数。
已有非零起点保持不变。初始化暂时失败沿用统计刷新重试，不将未知历史宣称为已知，也不阻断业务启动。

### 失败分类

`stage`：scheduling、runtime_prepare、runtime_start、readiness、route_activation、state_publish、unknown。
`reason`：capacity、constraint、dependency、timeout、probe_failed、runtime_error、conflict、transport、
caller_cancelled、unknown。结构化 scheduling timeout、业务 deadline 和 probe failure 优先；
只有有限枚举允许进入标签，任意错误文本不能成为标签。缺少可靠证据使用 unknown。
拒绝 reason：invalid_argument、unauthenticated、permission_denied、conflict、dependency_unavailable、unknown。

adxlet 在准备、启动、readiness、路由及发布阶段更新 `observation.create_stage`；RPC 携带
`adx-create-stage`、`adx-create-reason`。只有已确认发布的终态 Failed 才设置 `adx-create-terminal`。
普通 Unavailable/DeadlineExceeded 不因为错误码相同就升级为确定创建失败。

运行中断 reason：oom、runtime_exit、probe_failed、execution_missing、node_unreachable、
control_unhealthy、unknown。保留原退出码，不按 137 等单一退出码推断 OOM；用户命令非零退出
不属于 runtime 退出。runtime_class 只有 runsc、firecracker、runc、other。
现有 sandboxd adapter 的 OOM 分类依赖 backend 明确的 OOM 消息；没有这份证据时保留一般退出分类。

## Redis 字段与原子性

Coordinator 控制 hash 的 `environment:<id>` 是现有 `StoredEnvironment`，新增：

| 字段 | 内容/保留规则 |
| --- | --- |
| `creation.request_id` | 初次创建 request ID，最多 256 bytes；不放入 Prometheus 标签 |
| `creation.started_at_unix_millis` | Coordinator 首次预留接纳时间；默认随成功结果持久化，用于 starting 等待年龄 |
| `creation.result` | unsettled、success、failure、timeout；只裁定一次 |
| `creation.reason` / `settled_at_unix_millis` | 确认失败的有限摘要及裁定时间；API 阶段诊断另见调用指标 |
| `last_interruption` | 最近一次中断及恢复摘要，恢复成功后保留，下次中断替换，env 删除后一起删除 |
| `result.observation` | adxlet 随状态 journal/RPC 携带的中断证据、正常操作意图、创建阶段 |

`last_interruption` 包括 event_id、detected_at_unix_millis、assignment_generation、runtime_id、
worker_id、reason、evidence_source、exit_code、detail（UTF-8 最多 4096 bytes）、incident_id、
occurred_at_unix_millis、recovery_state、recovery_kind、recovery_worker_id、recovery_runtime_id、
resolved_at_unix_millis。无法知道准确发生时间时 occurred_at 为 null，检测时间始终保留。
event_id 按旧执行的 generation/runtime 固定，与 environment_id 一起关联；节点 incident_id
按 worker/session/跨重启唯一序号固定。原始 failure 可在恢复时清除，中断证据与最近摘要保留。

控制 header 记录统计覆盖起点。累计数保存在同 namespace 的 `:business` hash。
活动 Environment 的结果、最近摘要及其对象计数在同一个 epoch-fenced Lua 事务内更新，
普通对象提交只比较该对象版本。Environment 退役和节点失联提交将相关计数原子归并到
按对象身份散列的 64 个固定 `retired:<bucket>` 字段，并删除已归并的对象计数字段；
归并只额外比较目标桶的旧值，不改变全局控制 header。所有 u64 加法在 Rust 中检查，
Lua 仅比较和写入序列化字节。删除回放、同名重建及 Redis/Coordinator 重启均保持累计口径。
Coordinator 启动时在新 epoch 下归并已有孤立计数字段；崩溃后的重试不会重复归并。
无法解码的字段或暂未收敛的桶保留原值并告警，不阻塞控制状态恢复；有效字段继续归并。
损坏计数修复前，统计刷新报告失败并保留旧快照，不将不完整累计数发布为有效统计。
只有确认新 epoch 已隔离本次会话时才中止启动归并。
RPC 服务首次读取业务统计失败时以未初始化快照继续启动，后台刷新循环在数据修复后恢复统计；
控制状态读取和 epoch 隔离错误仍中止启动。
历史计数有效且归并完成后，指标刷新读取当前对象计数和至多 64 个退役累计桶，读取规模随当前对象数量变化，
不随历史退役对象总数持续增长，也不扫描全部控制目录。
`StoredNode.last_incident_id` 用于当前失联周期的事件回放去重，重新确认可路由后清除；
仅重连但仍在 reconciliation 的心跳不结束该失联周期。
生命周期提交返回确认的完整 StoredEnvironment，同步内存中的中断和恢复摘要；
删除及删除回放只返回结果，不恢复已删除记录。
日志在提交成功后输出 `environment_interrupted`、`environment_recovery_resolved`，用于关联排障；
日志不作为完整历史影响账本。

同 tenant/request ID 的成功回执位于 `:create-receipt:<身份摘要>` 独立键，和初次成功结果原子发布。
Redis 从首次发布起保留 600 秒，读取和重试不续期，重启保留到期时间；不维护永久请求索引。
Environment ID 或有效期内的 Request ID 任一命中成功身份，复用已有结果；默认预留不持久化请求绑定。
未命名请求由 API 根据 tenant/request ID 生成稳定名称。可选持久化 Claim 保留候选归属，
最终仍由成功发布确定有效结果。成功 Request ID 回执在 600 秒窗口内跨删除保留；
窗口内删除后新建须使用新请求身份。回执过期不删除仍存活的 Environment。

从无 TTL 的回执版本升级时，停止创建写入并等待最后一次成功发布满 600 秒，
再替换全部 Coordinator，使用 Redis `UNLINK` 清理旧的 `:create-receipts` 和 `:create-requests` 两个 hash。
这些旧 hash 不会由新写入自动过期；不要删除控制主表或在新旧 Coordinator 混跑时清理。

### 快速恢复与正常操作竞态

adxlet 在清理旧 runtime 或启动替代执行前，将旧执行身份和异常证据写入 `observation.interruption`。
该字段经现有 EnvironmentRecord 协议和持久化提交链路传递；快速恢复可直接发布新的 Running，
Coordinator 仍根据旧执行证据计一次中断、一次恢复。发布失败时保留证据，按原有 journal/提交
重试链路重放；重复 Running/Failed 提交不会重复计量。
若首次 Running 发布在写入 journal 前被取消，随后退出仍能提交终态并裁定创建结果。
reload 的替代执行遇到相同发布缺口时，也允许其自身证据随合法终态提交；不因 Coordinator
仍保留旧执行 Running 而拒绝结果，也不将未发布替代执行的退出计为旧执行中断。
首次权威 Running 前的退出证据保留在 result.observation，不计运行期中断；证据重放不会补计。

reload/delete/pause 在停止执行前，先向 Coordinator 发布当前执行的 `observation.intent`。
不能确认意图发布时不能继续主动停止；只有尚未进入清理的中止才清除意图并再次发布。
清理已开始而结果不确定时保留意图；后续确认执行缺失或收到普通退出事件，按正常停止
归并，不据此产生运行中断。普通退出码无法区分主动停止与独立故障；明确的 OOM、
探针失败等独立故障证据仍按异常处理。没有正常停止意图的退出继续计为运行中断。
节点失联时，已有正常操作意图的对象进入 operation_unknown 视图，不据此判为非预期中断或健康。
独立 backend/probe/reconcile 确认的真实异常仍携带事件证据，并清除正常意图。

恢复状态区分 waiting、recovering、recovered、unrecoverable、closed_by_user。
首次新执行 Running 确认为 recovered；显式用户删除为 closed_by_user；已终止且无可执行重启预算、
无有效可恢复 checkpoint 时为 unrecoverable。失联节点的本地 checkpoint 不作为跨节点恢复依据。
仍有有效恢复路径而未完成时保留 waiting/recovering，不把 Failed 存量直接累计为永久损失。

旧 env 缺 creation 字段时，已有 Running/Failed 不补计初次结果；旧未完成 reservation 仍计入
starting，但其等待年龄未知。旧 env 缺 last_interruption 不制造异常；新的权威运行期异常可正常记录。

## 当前影响范围查询

Coordinator 同一私有监听提供：

```sh
curl --fail 'http://127.0.0.1:19090/observability/environments?limit=100'
curl --fail 'http://127.0.0.1:19090/observability/environments?include_resolved=true&reason=oom'
curl --fail 'http://127.0.0.1:19090/observability/environments?worker_id=worker-a&recovery_state=unrecoverable'
curl --fail 'http://127.0.0.1:19090/observability/environments?operation_unknown=true'
```

默认只列最近摘要为 waiting/recovering/unrecoverable 的当前对象。支持 reason、worker_id
（受影响的原 worker）、recovery_state、incident_id、since/until（检测时间 Unix 毫秒）、
include_resolved、cursor、limit（默认 100，最大 1000）。operation_unknown 模式仅支持 worker_id
和分页，用于定位正常操作期间失联的对象。过滤值非法返回 400；目录不可用返回 503。

返回 snapshot_at_unix_millis、coordinator_epoch、coverage、pagination、total_environments、
total_workers、next_cursor、items。总数在分页前按当前过滤结果去重；items 含 env ID、当前 worker、
当前状态、最近摘要和操作意图。按 env ID 游标分页是 live_current_snapshot，跨页期间状态可变，
不承诺历史一致性快照。接口位于私有监控网，包含业务定位信息，部署方限制访问。

事件累计和当前影响范围必须分开展示：同 env 两次中断是 2 次事件，当前查询最多 1 个 env；
一个 worker 故障影响 20 个运行 env 是 1 次 worker 事故、20 次执行中断和当前最多 20 个关联 env。
**total_workers 是本次查询中原受影响 worker 去重数，不是 worker_incidents_total。**

首期 coverage=current_env_latest_interruption：env 删除或下一次异常替换摘要后，早期关联对象
不再完整。因此 since/until 只过滤当前保留摘要，不能回答任意历史窗口的完整 env/worker 去重数。
这部分历史能力尚未交付。operation_unknown 模式使用独立 coverage=current_env_operation_unknown。

## PromQL 与采集

[私有采集示例](../../build/observability/sandbox-prometheus.json) 抓取单个有效 Coordinator、
全部 API Server 和 adxlet，15 秒间隔并注入 cluster/adx_env。各组件配置现有 `metrics_listen`，
分别如 127.0.0.1:19090、19091、19093；仅修改需要启用的监听和 Prometheus scrape target/标签。
ADX 维护 producer 和示例，部署/监控侧配置地址及网络访问，health-checker 消费正式合同。

创建调用成功率（failure 包括确认超时；unknown/cancelled 另外查询）：

```promql
sum by (cluster, adx_env) (increase(adx_environment_create_responses_total{outcome="success"}[5m]))
/
sum by (cluster, adx_env) (increase(adx_environment_create_responses_total{outcome=~"success|failure"}[5m]))
```

成功新建调用 P95，先合并 bucket 再算分位数：

```promql
histogram_quantile(0.95,
  sum by (cluster, adx_env, le) (
    rate(adx_environment_create_success_call_duration_seconds_bucket[5m])
  )
)
```

```promql
sum by (cluster, adx_env, outcome) (increase(adx_environment_create_responses_total{outcome=~"unknown|cancelled"}[5m]))
sum by (cluster, adx_env, stage, reason) (increase(adx_environment_create_errors_total[5m]))
sum by (cluster, adx_env, reason) (increase(adx_environment_interruptions_total[5m]))
adx_environment_create_oldest_wait_seconds > 60
```

最后一行仅示范查询，60 秒不是 ADX 内置告警阈值。看板同时显示未完成、未知年龄、unknown、
采集 up、snapshot_age 及覆盖信息。无样本、分母为 0、缺失或过期不能补 0 作为健康。
本实现不添加告警或通知规则。只抓一个有效 Coordinator，避免同一 Redis 累计数被重复相加。

## 本地验证

组件与真实双节点端到端结果见 [本地验证记录](sandbox-observability-validation.md)，包含未通过项与验证边界。

仓库中的两项 ignored 测试使用真实 native Redis 和 Prometheus，绑定随机 loopback 端口，
调用生产 metrics HTTP handler，执行实际 scrape/PromQL 并保存原始样本、查询与影响范围 JSON。
API/Coordinator 的下游 runtime 在组件测试中使用测试实现；这不是实际 sandboxd/runtime 部署验证。

```sh
export ADX_TEST_REDIS_SERVER=/absolute/path/to/redis-server
export ADX_TEST_PROMETHEUS=/absolute/path/to/prometheus
export ADX_TEST_EVIDENCE="$PWD/out/observability/local-001"
cargo test --locked -p adx-coordinator --test observability -- --ignored --nocapture
cargo test --locked -p adx-apiserver --test create_replay \
  prometheus_collects_new_creation_latency_without_replay_samples -- --ignored --nocapture
```

```sh
promtool check config build/observability/sandbox-prometheus.json
promtool check metrics < "$ADX_TEST_EVIDENCE/coordinator/scrape.txt"
promtool check metrics < "$ADX_TEST_EVIDENCE/apiserver/scrape.txt"
```

当前文本出口沿用现有格式，`promtool check metrics` 会报告缺少 HELP 文本的 lint 问题；
采集验收以实际 Prometheus target、指标值和 PromQL 结果为证据，不能将该 lint 检查记为通过。

验收同时包含原子 request 绑定、重复提交、跨 Coordinator 重启、直接恢复 Running 携带旧证据、
OOM 后恢复、节点影响范围、正常操作先发布意图、存量缺字段及统计起点保留。
测试/生产集群部署不属于本次本地验证授权。

Redis 操作预算同时包含重连与命令响应；驱动内部的默认连接／响应超时不再提前截断该预算。
定向存储用例用 `CLIENT PAUSE` 验证超过 500ms 的响应在较长预算内成功、较短预算仍及时失败，
并确认超时后替换连接可以继续访问状态。

## 数据面流量

Ingress 的 `data_plane_ingress_frontend_bytes_total{direction="received|sent"}`
是公网连接在 TLS 解密后的接收/发送累计字节，包含 HTTP 头、帧和升级后的流。
Relay 的 `data_plane_relay_transfer_bytes_total{direction="to_runtime|from_runtime"}`
是到运行时/从运行时返回的负载累计字节，包含仍在传输的连接。两者位于不同跳，
不可相加为业务总流量；都不包含 TLS 记录、IP 或链路层开销。

所有方向启动时导出 0，进程重启清零。区间流量先对各生产者应用
`increase(counter[window])`，再跨实例汇总，以处理重启清零；采样外推可能产生小数，
首轮抓取之前的流量无法恢复。HTTP 响应类别 `1xx` 单列 101 WebSocket 升级；
SSE 的 HTTP 200 不代表 Environment 最终创建成功。

Ingress 仅在现有 I/O 返回时执行每连接 relaxed 原子计数，不复制负载、不发送额外 RPC。
Relay 复用现有每流计数，转发热路径不新增原子操作。连接注册/关闭与抓取汇总使用
登记表锁；数据块转发不获取该锁。抓取成本与活动连接数成正比。
