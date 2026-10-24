# Environment 可观测数据与容量

## 创建吞吐

API Server 的私有 `metrics_listen` 暴露 `GET /metrics`，示例地址是
`127.0.0.1:19092`。不配置时不开监听；监控端点需限制在私网，独立于用户认证入口。

| 指标 | 口径 |
| --- | --- |
| `adx_environment_create_requests_total{source="http|embedded"}` | 创建入口请求数。HTTP 在身份认证和参数校验之前计数，覆盖同步、SSE、兼容创建接口及其重试；embedded 在应用服务入口计数。转发层不重复计数。 |
| `adx_environment_create_responses_total{outcome="success|failure|unknown|cancelled"}` | 已进入创建应用服务的调用结果，包含重放响应。结构化调度取消和确认的业务失败为 failure；无法确认执行结果的传输错误为 unknown；调用 future 被丢弃为 cancelled。unknown/cancelled 不代表创建失败。参数校验前的拒绝只有请求计数。 |
| `adx_environment_create_in_flight` | 正在等待创建结果的应用服务调用，包含等待重放结果的调用。SSE 响应头返回不会提前减去后台创建。 |
| `adx_environment_create_duration_seconds` | 应用服务调用开始至最终结果或取消的耗时 Histogram，覆盖排队、就绪及提交确认，包含重放调用；与 `environment.create` Span 同口径，JSON、SSE、embedded 共用。排除进入服务前的认证、参数转换及 SDK 传输/重试；固定 12 个有限桶。 |
| `adx_environment_created_total` | Coordinator 首次接受新 Environment 的持久化 Running 结果。已有结果的重复提交、resume、restart 和 recovery 不计入。 |

这些是进程内监控计数器，进程重启归零，不是持久化审计账本。确认提交与进程崩溃
之间存在监控计数丢失窗口；历史数据通过 Prometheus Counter reset 规则处理。

`build/observability/rules.json` 每 15 秒分别对副本计数器计算一分钟速率，再按
`adx_env` 汇总。Schedule 显示请求 QPS、首次就绪 QPS、两者在所选时间范围内的
峰值、调用结果、进行中数量和耗时 P95/P99。峰值为一分钟平均速率的历史最大值，
不是单秒突发峰值。多环境选择时先求同一时刻总速率，再取峰值。

## Running 与 Paused

长期状态计数来自 `adx_coordinator_environments{state="Running|Paused"}`。
Schedule 显示两种状态的当前数量、趋势和节点分布。当前数量读取已提交状态；
Recovering、Invalidated 和其他过渡状态不算作 Running 或 Paused。

可选 Environment 明细页读取 Coordinator 的 `/metrics/environments`：
`adx_environment_info{environment_id,node_id,state}=1`。它独立于运行时资源采样，
Paused 或尚无资源样本的实例仍可列出。资源采样仅覆盖执行中的 runtime；暂停停止
屏障确认后移除旧样本，不把暂停前 CPU/内存展示为当前值。

## 指标分层与保留

| 入口 | 内容 | 默认采集配置 |
| --- | --- | --- |
| Coordinator、adxlet `/metrics` | 状态数量、调度账本、节点资源使用汇总、进程与 Trace 导出指标 | `prometheus.json`，15 秒 |
| API Server `/metrics` | 创建请求和调用结果指标 | `prometheus.json`，15 秒 |
| Collector `18888/metrics` | 接收、拒收、发送及队列指标 | `prometheus.json`，15 秒 |
| Coordinator `/metrics/environments` | 当前 Environment 状态清单 | 可选 `prometheus-environments.json`，30 秒 |
| adxlet `/metrics/environments` | 逐 Environment/runtime 的 CPU、内存和采样年龄 | 可选 `prometheus-environments.json`，30 秒 |

长周期库不抓取两个明细入口。需要逐实例排障时才启动明细采集器，并在 Grafana
配置 UID 为 `prometheus-details` 的数据源。明细开关是是否启用该采集器，不需要
改变生命周期巡检频率或重启业务服务。

从 `build/observability` 目录启动两个独立 Prometheus 实例，使用各自的数据目录：

```sh
prometheus --config.file=prometheus.json \
  --storage.tsdb.path=/var/lib/prometheus/adx-summary \
  --storage.tsdb.retention.time=30d --web.listen-address=127.0.0.1:9090
```

```sh
prometheus --config.file=prometheus-environments.json \
  --storage.tsdb.path=/var/lib/prometheus/adx-environments \
  --storage.tsdb.retention.time=24h --web.listen-address=127.0.0.1:9091
```

地址是进程部署示例，部署时替换所有目标并补充稳定的 `adx_env` 和节点标签。
仅修改 `scrape_interval` 不会改变后端 retention；两个保留期通过独立 TSDB 启动参数
生效，现有 Prometheus 不支持在同一 TSDB 中用抓取 job 单独设置保留期。
长期库和明细库不要再次合并远程写入同一个不分保留策略的存储。

adxlet 汇总资源指标按有界 `runtime_class=runsc|firecracker|runc|other` 分组：

- `adx_node_environment_cpu_usage_seconds_total`：把相邻同 runtime CPU 累计值的增量
  加入节点生命周期累计值，删除实例不减少累计值；runtime 或底层计数器重置后从
  新累计值继续记账。用 `rate` 求采样到的 CPU 核数，不对活跃实例累计值直接求和。
- `adx_node_environment_memory_usage_bytes` / `memory_limit_bytes`：只汇总 45 秒内样本。
- `adx_node_environment_samples` / `stale_samples`：新鲜和过期资源样本数。

汇总资源指标每个节点最多 4 个运行时类别 × 5 条时间序列，数量随节点规模增长。
逐实例资源明细为每个执行代次 4 条时间序列，状态清单另计；它们的历史基数随实例
创建和状态变化累积，因此放在独立短保留库。

CPU/内存属于已采样 runtime 的资源使用，不等于分配账本。未曾采样就结束的短任务
不会贡献完整 CPU 时间；CPU 变化可能在下一次成功采样才被观测。结合新鲜样本数
判断覆盖度。资源采样失败、暂停或缺少样本不能解释为实例实际用量为零。
汇总内存为零而新鲜样本数也为零时，表示无当前观测。

逐实例 `adx_environment_cpu_usage_seconds_total`、`memory_usage_bytes`、
`memory_limit_bytes` 和 `stats_age_seconds` 的标签保持 `environment_id,runtime_id`。
Environment 明细页使用采样年龄过滤当前 CPU/内存，Paused 列表来自独立清单，
不会依赖旧的资源样本枚举。

## 巡检 Trace

正常权限核对只比较 Coordinator 缓存与节点记录；一致的 Running、Paused 和已释放终态记录不创建 `environment.queue` / `environment.execute`。记录差异、恢复和清理保留 `environment.authority_reconcile`，失败结果标记错误。
生命周期由主动控制流、后端退出通知和实例截止时间推进。退出保留 `environment.unexpected_exit`，自动暂停保留 `environment.auto_pause`，状态提交和运行时启动继续记录原有 Span。CPU、内存采集独立运行，不查询生命周期或触发状态变更。
Execd 的无入站 Trace 上下文健康检查、指标抓取和 HTTP 状态轮询同样不创建 Span；
带上下文的诊断请求、业务请求和控制写操作保留 HTTP Trace。

## 大盘查询与采集健康

Schedule 自动刷新统计数据。`ADX Environment Logs` 独立页默认最近 15 分钟、关闭
自动刷新，日志每次最多返回 500 行；runtime 正则支持 `.*environment-id.*`，正文
关键词按字面匹配，可直接输入反斜杠、引号和反引号。关键词先进行 URL 编码，
在 Loki 查询模板中还原并匹配原文；runtime 过滤先执行，临时匹配标签在返回前移除。
Environment、runtime 正则使用 PromQL/LogQL 原始字符串，
保留 `env-\d+` 等正则及双引号的含义；匹配反引号时使用 RE2 的 `\x60` 写法。
该页同时提供有返回上限的 HTTP Trace 明细；日志与 Trace 内容查询不会随 Schedule
总览自动执行。

总览只查询日志速率、控制日志告警/错误量、Collector 队列比例和拒收/入队失败速率。
两项日志统计使用 instant 查询，分别固定回看 1 分钟和 5 分钟，避免随总览时间范围
扩大而反复扫描整段日志历史。
运行时 ID、请求 ID、文件路径和 Environment ID 不应提升为 Loki 索引标签；保留在
日志正文或结构化元数据。Collector 的批次数量上限不等于磁盘字节上限，应监控实际
队列磁盘、日志目录空间及采集延迟，并按实测设置后端容量。

## 验证入口

- 预先拉取 `grafana/loki:3.4.2` 后运行 `python3 -m unittest discover -s build/observability/tests -v`：使用隔离 Loki 验证大盘关键词的特殊字符、空值、大小写、原文保留和 runtime/环境隔离，容器由测试自动清理。
- `cargo test -p adxlet --lib metrics::tests`：汇总累计值、runtime 重置、实例回收、过期样本和分层入口。
- `cargo test -p adxlet --test background_traces`：多次普通巡检/过期扫描不新增 Span，实际失败保留生命周期 Span，失败后旧资源样本移除。
- `cargo test -p adxlet --test authority_traces`：真实 gRPC 周期核对下，Running/Paused 正常巡检不新增 Span；异常清理、失败和恢复保留 Span，恢复后的正常巡检重新静默。
- `python3 -m unittest discover -s build/e2e/tests -p test_fc_metrics.py`：Firecracker 生命周期验收分别读取 `/metrics` 和 `/metrics/environments`，要求节点账本与指定实例的资源样本均可用。
- `cargo test -p adx-apiserver --test create_replay`：响应前进行中计数、完成后响应计数、幂等重放不产生第二次创建。
- `cargo test -p adx-execd --lib passive_polls_do_not_export_but_requests_keep_context`：真实 HTTP 轮询不导出 Span，诊断上下文、控制写操作和业务请求继续导出。
- Coordinator `control-rpc` 套件：真实 Redis/mTLS 下创建、重复提交、暂停/恢复及计数。
- 在 `build/observability` 执行 `promtool check config prometheus.json`、
  `promtool check config prometheus-environments.json` 和 `promtool test rules rules.test.yaml`。

## 节点采集与生命周期

`metrics_interval_seconds` 默认为 15 秒，控制节点对运行中实例的资源采样；Prometheus 抓取周期独立配置。采样不查询生命周期状态，也不触发暂停、重启或故障处理。样本直接写入独立指标缓存，不等待生命周期队列确认；写入时原子检查控制器身份、runtime 和执行代次，避免旧样本覆盖当前执行。生命周期依赖 Execd 主动控制流、代理活动增量、后端退出通知和实例截止时间；正常权威记录对账从 Coordinator 缓存读取，不重复扫描 Redis 或后端执行。
