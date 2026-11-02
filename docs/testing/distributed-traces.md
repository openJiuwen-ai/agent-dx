# 跨组件 Trace

ADX 使用 OpenTelemetry SDK 和 W3C `traceparent` / `tracestate`。采样与导出默认关闭；开启后通过 OTLP/HTTP 向部署环境的 Collector 发送。日志采集与现有 Prometheus 指标继续独立工作。

## 请求与任务边界

- Ingress 接收 HTTP 上下文、创建 `HTTP 方法 + 路由模板` Span，并将子上下文传给 Environment API、Execd HTTP 或 Relay CONNECT。
- Rust API Server 创建以 HTTP 方法和接口模板命名的服务端 Span，gRPC 客户端拦截器在发送时注入当前上下文，保证远端服务端 Span 是当前客户端 RPC Span 的子节点；节点地址缓存命中后的直达操作也经过同一拦截器。
- Coordinator 在创建、查询、提交和快照 RPC 入口接收上下文；独立创建/提交任务显式携带子 Span。周期节点注册/心跳及内部调度轮次不创建 Span。中心排队耗时仍包含在该请求的 `coordinator.create` Span 中，不额外导出服务多个请求的调度轮次。
- adxlet 对用户生命周期命令在入队时保存 `environment.queue` Span，在出队时结束它；随后使用保留的上下文创建 `environment.execute`。queue 仅计入入队背压和串行等待，execute 计入实际处理，两者时间不重叠。执行 Span 的父节点仍为对应的已结束队列 Span。创建链路在执行 Span 内继续划分 `environment.runtime.start`、`environment.runtime.ready`、`environment.route.activate` 和 `environment.state.commit`，分别覆盖执行后端启动、Execd 就绪、本机路由绑定和持久化提交。调用方断开后，已接受操作继续保持原上下文；状态提交继续向 Coordinator 透传。
- 周期 Tick 和恢复点有效期扫描不生成 queue/execute Span。巡检确认退出时创建 `environment.unexpected_exit`，触发自动暂停时创建 `environment.auto_pause`，实际状态提交、恢复和启动保留生命周期 Span。
- Relay 的 `relay.connect` Span 只覆盖建连段（策略/路由校验、TCP 拨号、CONNECT 成功响应），随后显式结束；后续字节泵不在该 Span 内。Execd 的 HTTP Span 延续 Ingress 注入的上下文，是同一请求下与建连 Span 并列的子 Span。
- Execd 对没有入站 Trace 上下文的 `GET /healthz`、`GET /metrics` 和 `GET /control/v1/status` 不创建 Span；带上下文的诊断请求和其他操作继续跟踪。Execd 在解析 HTTP 头后创建以 HTTP 方法和接口模板命名的 Span，Environment ID 作为属性；`/invoke` 另外记录有界的 `rpc.method` 操作名和 `http.response.status_code`，5xx 标记 Error。此 Span 覆盖 HTTP 操作处理；异步提交命令之后的用户进程运行时间尚不属于该 HTTP Span。
- 文件 API、数字人 WebSocket 后端握手、adxlet→Execd 控制请求与 adxlet→Relay 路由控制都会注入当前 W3C 上下文；CONNECT 之后的原始字节流不再解析或注入。

创建总耗时统一使用 API Server 的 `environment.create` Span，与
`adx_environment_create_duration_seconds` 的应用服务边界一致：参数转换后进入创建服务，
直到确认 Running、本机路由绑定、持久化提交及创建结果目录确认；失败或取消则计到该调用结束。
JSON、SSE 和内嵌创建共用此埋点；重放也记录本次调用耗时，不冒充第一次创建耗时。
`environment.create.outcome` 区分 success、failure、unknown、cancelled，失败结果标记 Error；
unknown/cancelled 不表示已确认实例创建失败。`environment_create_completed` 日志使用同一服务边界。
创建终结时同时写入 `result`、`error.code`、`error.message`、`instance_id`、`user.id`（当前取租户）
与 `newly_created`；阶段 Span（`apiserver.create`、`coordinator.create_environment`、`node.create_environment`、
`environment.execute`、`environment.runtime.*`、`environment.state.commit`）写入 `environment.id`，
失败分支写入 `result=fail` 与有界错误信息。`environment.runtime.start` 内部另有 `sandbox.create` Span，
记录 `sandbox.backend`、`environment.id` 与结果。
SDK 连接、传输、重试退避、身份认证和参数转换不计入此服务耗时；完整 SDK 调用仍需客户端计时。
SSE 保留请求上下文，在最终结果交给流之前完成创建 Span，客户端读取耗时另算。

HTTP Span 包含 `http.request.method`、`http.route`，Ingress/API Server 返回后记录 `http.response.status_code`。HTTP Span 当前覆盖生成响应的处理阶段；创建等流式响应可能先返回，而后台任务继续运行。Span 时长不等于 SDK 等待完整操作的总耗时，完整链路与后台执行阶段需打开 Trace 查看。实例/快照/密钥 ID 使用 `{id}` 占位，不记录查询参数、用户端口路径或文件路径。未知接口统一使用 `/unmatched`。

创建、执行、删除是独立请求，各有自己的 Trace，通过 Environment ID 关联。SDK 目前没有新增自动事务级根 Span。

sandboxd 是独立运行时服务。本版本在 adxlet→sandboxd 的 gRPC 边界停止 Trace 传播；sandboxd 原有的资源指标和日志采集不依赖该传播，也不将 sandboxd span 计入本版本的端到端 Trace 验收。

## 入口相关性 ID 与 Span 内容边界

HTTP 入口同时接受 W3C `traceparent`/`tracestate`。没有 `traceparent` 时，入口按 `X-Trace-Id`（其次 `X-Request-Id`）确定性地派生 32 位十六进制 Trace ID；两个头都不存在时仍生成新的根 Trace。处于记录态的请求在响应中回写 `X-Trace-Id`，便于用同一编号检索日志与 Trace；关闭态不添加该响应头。

Span 字符串属性超过 1024 字节时保留首尾并插入省略号；请求体、文件内容、URL 和凭据不得写入属性。失败 Span 可记录 `exception` 事件（`exception.type` / `exception.message`），并使用有界错误描述作为状态消息。

请求作用域内（`Trace::run` 或 `scope` 仍在执行）的组件日志自动附加 `trace_id`/`span_id`：JSON 日志写入同名字段，文本日志追加在行尾。作用域外的日志（例如异步关闭回调）需要显式传递编号。WebSocket 握手或 JSON 隧道头需要携带上下文时使用 `adx_observability::trace::propagation_headers()`；CONNECT 建立后的字节流是 L4 透传，不解析也不注入，只有 CONNECT 头与握手帧可以携带 `traceparent`。

## 配置与部署

部署级 `tracing` 策略统一控制各组件（OTLP/HTTP，默认关闭）：

```yaml
tracing:
  endpoints: ["127.0.0.1:4318"]   # 空列表 = 关闭（默认）；非空 = 开启（100%）；多项取第一项
```

`adxctl` 渲染时为每个服务（Redis 与 AFS 除外）写入：

```json
{
  "ADX_TRACE_ENABLED": "true",
  "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://127.0.0.1:4318/v1/traces"
}
```

adxlet 同时把这些变量写入 `execd_env`，并补 `ADX_NODE_ID` 与 `ADX_ENV`，使 Runtime 内 Execd 使用同一 Collector 与资源标识。endpoint 接受 `host:port` 或 `http://host:port[/v1/traces]`；仅支持 OTLP/HTTP，不接受 gRPC 或 `https://`。`endpoints` 是唯一开关：非空即开启且按 100% 采样，空列表即关闭。非法配置在加载阶段失败。

如需比例采样，可在具体服务的 `env` 中显式设置 `ADX_TRACE_SAMPLE_RATIO`（0–1，默认 1）；部署级 `tracing` 不写入该变量，因此服务级配置可覆盖。

每个 Span 的资源属性包含 `service.name`、`service.instance.id`、`node.id`（存在 `ADX_NODE_ID` 时）与 `adx_env`（存在 `ADX_ENV` 时）；嵌入 Ingress 的 Span 另带 `adx.component=ingress`。`build/observability/collector.json` 的 traces 流水线通过 `resource/adx` 处理器补齐 `adx_env` 与 `adx.node.id`。

`ADX_TRACE_SAMPLE_RATIO` 在 0–1 之间。开启时使用父采样决定，根请求按比例采样；要完全关闭导出，应设置 `ADX_TRACE_ENABLED=false`。不合法的布尔值或比例使启动失败。

`build/observability/collector.json` 增加 OTLP HTTP 接收及 Trace 导出流水线，默认监听 `127.0.0.1:4318`。节点服务可访问本机 Collector；Execd 使用 adxlet 配置的 `execd_env` 设置上述变量，地址须为实例网络可达的节点/Collector 地址，不能把实例自己的 localhost 当作宿主 Collector。按部署的私网边界调整监听地址和访问规则。

### AFS 边界

仓库级 tracing 开关不覆盖 AFS。AFS 是独立可选组件（默认 `ADX_WITH_AFS=0`，发布包与 supervisor 不含 AFS），使用自有 TOML 的 `trace_enabled` / `trace_endpoint` / `trace_sample_ratio`，默认关闭；开启后经 OTLP/gRPC（默认 4317）导出，而本仓库 Collector 默认只接收 OTLP/HTTP 4318。启用 AFS 时需单独配置其 TOML 与 Collector 接收端；部署级 `tracing` 不会改写 AFS 配置文件。

本地及基础 K8s 验收使用 Collector sidecar/独立进程的 14317 接口，发送到测试接收端；这些测试地址不是产品默认值。

## 在 Grafana Explore 查询接口

`adx-schedule` 顶部的 **Explore 接口查询** 打开原生 Tempo Explore，带入 `adx_env`，
使用最近 6 小时、50 条 Trace、每条最多 3 个匹配 Span 的查询。入口固定
`Table Format = Spans`，选择 `name`、`resource.service.name`、
`span.http.response.status_code`、`span.rpc.method`、`span.environment.create.outcome`；排除 `/unmatched`、健康和指标请求。
创建行显示 `environment.create` 总耗时，成功创建的 HTTP 响应头行不混入该列表；HTTP 4xx/5xx 行仍保留，认证或参数校验失败、尚未进入创建服务的请求也可查询。打开完整 Trace 仍可查看 HTTP 阶段。
Name 显示 HTTP 方法和接口模板，rpc.method 区分 Execd 的 `/invoke` 操作；点击 Span ID
查看整条链路。Span 表不生成 Traces 表的 nested JSON 列。

这是查询入口的预设，不修改 Grafana 全局默认表格式；手工进入 Explore 时需在
Search Options 选择 Spans。原生 Explore 仍保留 Trace Service / Trace Name 根 Span 列，
没有看板的列隐藏配置。若客户端注入 traceparent 却没有导出父 Span，根信息会缺失；
Name 和 service.name 仍来自实际匹配 Span。不要为改善展示伪造根 Span 或丢弃远端父上下文。

## 故障、开销与字段

业务线程只向标准 SDK 的有界批处理队列提交 Span。单次 Rust 导出超时为2秒；退出等待上限3秒。队列满时 SDK 可丢弃 Trace，不承诺 Trace 持久化或无限重试。Collector 独立配置磁盘发送队列，只有 Collector 已接收的内容才进入该队列。

Coordinator 和 adxlet 的 `/metrics` 增加 `adx_trace_exported_spans_total`、`adx_trace_export_failed_spans_total`，统计 SDK 导出成功/最终失败的 Span 数；不把它们解释为队列满丢弃数。Rust SDK 会记录开始丢弃及退出时的总丢弃诊断；当前没有统一暴露各语言的实时队列满丢弃计数。Collector 的接收/拒绝/排队/导出指标另外采集。

API 请求日志包含 Trace ID 和 Span ID；adxlet 操作完成日志包含 `traceparent`。业务字段只记录操作、方法、路由模板、状态、Environment ID等；Trace不添加API Key、请求正文、命令内容、文件内容或用户代码。传播器仅处理W3C Trace字段，不传播 baggage。

## 验收

真实 Redis 合约测试验证连续心跳和资源不足的调度轮次不导出 Span，而创建请求保留调用方 Trace ID、排队期限与超时清理；用例纳入 `control-rpc` / `api-control` 组件套件。组件测试覆盖跨任务/队列的并发隔离、调用方取消后已接受操作继续关联、采样关闭、导出端不可用时提交不阻塞、失败计数与退出时间。真实部署验收检查完整创建链路的Trace ID，以及每个 `environment.execute` 的父Span确为同Trace内的 `environment.queue`，同时核对EXECD收到远端上下文。

本地与Rust API Server重写后的 [Buildkite #24 正式验收](2026-09-17-rust-apiserver-k8s.md) 已通过；统一实时队列满丢弃指标后置，详见 [事项清单](control-plane-remaining.json)。节点恢复后产生新的后台Trace，以Environment/代次/状态关联，不承诺跨进程重启续接已结束的Span。

### 后台 Trace 裁剪验证（2026-10-08）

`background_traces` 使用隔离的真实 Redis 和内存 Span exporter，验证首次注册、
连续心跳及资源不足的中心调度轮次不产生心跳/轮次 Span；创建请求的 Trace ID、
父 Span、中心排队期限和超时后的队列清理保持有效。用例已加入 `control-rpc`
和 `api-control` 的 CI 驱动。另行通过原有 mTLS 节点心跳过期、对账及旧会话隔离
RPC 用例；CI harness、全工作区 Clippy、格式和文档检查通过。

日志位于执行工作区 `out/otel-readability/background-*.log`。本次验证为源码组件
回归；cn-north-4 的日志正文/HTTP Trace 验收镜像尚未包含此次后台埋点裁剪，
需要后续发布镜像后才停止产生这两类旧 Span。历史 Trace 保留原记录。
