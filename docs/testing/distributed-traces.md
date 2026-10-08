# 跨组件 Trace

ADX 使用 OpenTelemetry SDK 和 W3C `traceparent` / `tracestate`。采样与导出默认关闭；开启后通过 OTLP/HTTP 向部署环境的 Collector 发送。日志采集与现有 Prometheus 指标继续独立工作。

## 请求与任务边界

- Ingress 接收 HTTP 上下文、创建 `HTTP 方法 + 路由模板` Span，并将子上下文传给 Sandbox API、Execd HTTP 或 Relay CONNECT。
- Rust API Server 创建以 HTTP 方法和接口模板命名的服务端 Span，gRPC 客户端拦截器携带上下文；节点地址缓存命中后的直达操作也经过同一拦截器。
- Coordinator 在创建、查询、提交和快照 RPC 入口接收上下文；独立创建/提交任务显式携带子 Span。周期节点注册/心跳及内部调度轮次不创建 Span。中心排队耗时仍包含在该请求的 `coordinator.create` Span 中，不额外导出服务多个请求的调度轮次。
- adxlet 在每个 Environment 命令封装中保存 `environment.queue` Span，串行执行时创建 `environment.execute`。创建链路在执行 Span 内继续划分 `environment.runtime.start`、`environment.runtime.ready`、`environment.route.activate` 和 `environment.state.commit`，分别覆盖执行后端启动、Execd 就绪、本机路由绑定和持久化提交。调用方断开后，已接受操作继续保持原上下文；状态提交继续向 Coordinator 透传。
- Relay 为 CONNECT 生命周期记录 Span。它转发的是字节流，Execd 的 HTTP Span 延续 Ingress 注入的上下文；两者可能是同一请求的并列子 Span。
- Execd 在解析 HTTP 头后创建以 HTTP 方法和接口模板命名的 Span，Environment ID 作为属性；`/invoke` 另外记录有界的 `rpc.method` 操作名。此 Span 覆盖 HTTP 操作处理；异步提交命令之后的用户进程运行时间尚不属于该 HTTP Span。

HTTP Span 包含 `http.request.method`、`http.route`，Ingress/API Server 返回后记录 `http.response.status_code`。HTTP Span 当前覆盖生成响应的处理阶段；创建等流式响应可能先返回，而后台任务继续运行。Span 时长不等于 SDK 等待完整操作的总耗时，完整链路与后台执行阶段需打开 Trace 查看。实例/快照/密钥 ID 使用 `{id}` 占位，不记录查询参数、用户端口路径或文件路径。未知接口统一使用 `/unmatched`。

创建、执行、删除是独立请求，各有自己的 Trace，通过 Environment ID 关联。SDK 目前没有新增自动事务级根 Span。

sandboxd 是独立运行时服务。本版本在 adxlet→sandboxd 的 gRPC 边界停止 Trace 传播；sandboxd 原有的资源指标和日志采集不依赖该传播，也不将 sandboxd span 计入本版本的端到端 Trace 验收。

## 配置与部署

通过统一部署配置中对应服务的 `env` 设置：

```json
{
  "ADX_TRACE_ENABLED": "true",
  "ADX_TRACE_SAMPLE_RATIO": "0.1",
  "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://127.0.0.1:4318/v1/traces",
  "OTEL_BSP_MAX_QUEUE_SIZE": "2048",
  "OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "512",
  "OTEL_BSP_SCHEDULE_DELAY": "1000"
}
```

`ADX_TRACE_SAMPLE_RATIO` 在 0–1 之间。开启时使用父采样决定，根请求按比例采样；要完全关闭导出，应设置 `ADX_TRACE_ENABLED=false`。不合法的布尔值或比例使启动失败。

`build/observability/collector.json` 增加 OTLP HTTP 接收及 Trace 导出流水线，默认监听 `127.0.0.1:4318`。节点服务可访问本机 Collector；Execd 使用 adxlet 配置的 `execd_env` 设置上述变量，地址须为实例网络可达的节点/Collector 地址，不能把实例自己的 localhost 当作宿主 Collector。按部署的私网边界调整监听地址和访问规则。

本地及基础 K8s 验收使用 Collector sidecar/独立进程的 14317 接口，发送到测试接收端；这些测试地址不是产品默认值。

## 在 Grafana Explore 查询接口

`adx-schedule` 顶部的 **Explore 接口查询** 打开原生 Tempo Explore，带入 `adx_env`，
使用最近 6 小时、50 条 Trace、每条最多 3 个匹配 Span 的查询。入口固定
`Table Format = Spans`，选择 `name`、`resource.service.name`、
`span.http.response.status_code`、`span.rpc.method`；排除 `/unmatched`、健康和指标请求。
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
