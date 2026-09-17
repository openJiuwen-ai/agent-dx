# 跨组件 Trace

ADX 使用 OpenTelemetry SDK 和 W3C `traceparent` / `tracestate`。采样与导出默认关闭；开启后通过 OTLP/HTTP 向部署环境的 Collector 发送。日志采集与现有 Prometheus 指标继续独立工作。

## 请求与任务边界

- Edge 接收 HTTP 上下文、创建 `edge.http` Span，并将子上下文传给 Sandbox API、RRT HTTP 或 Node Proxy CONNECT。
- Rust API Server 创建服务端 Span，gRPC 客户端拦截器携带上下文；节点地址缓存命中后的直达操作也经过同一拦截器。
- Master 在创建、查询、提交和快照 RPC 入口接收上下文；独立创建/提交任务显式携带子 Span。`shard.schedule_round` 记录实际调度轮次，其父上下文是驱动该轮次的任务；一轮可能服务多个等待请求，不把整个轮次耗时分别归给每个请求。
- Node Manager 在每实例命令封装中保存 `instance.queue` Span，串行执行时创建 `instance.execute`。前者包含等待与执行时间，后者只覆盖实际执行；调用方断开后，已接受操作继续保持原上下文。状态提交继续向 Master 透传。
- Node Proxy 为 CONNECT 生命周期记录 Span。它转发的是字节流，RRT 的 HTTP Span 延续 Edge 注入的上下文；两者可能是同一请求的并列子 Span。
- RRT 在解析 HTTP 头后创建 `rrt.http`，Instance ID 作为属性。此 Span 覆盖 HTTP 操作处理；异步提交命令之后的用户进程运行时间尚不属于该 HTTP Span。

创建、执行、删除是独立请求，各有自己的 Trace，通过 Instance ID 关联。SDK 目前没有新增自动事务级根 Span。

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

`build/observability/collector.json` 增加 OTLP HTTP 接收及 Trace 导出流水线，默认监听 `127.0.0.1:4318`。节点服务可访问本机 Collector；RRT 使用 Node Manager 配置的 `rrt_env` 设置上述变量，地址须为实例网络可达的节点/Collector 地址，不能把实例自己的 localhost 当作宿主 Collector。按部署的私网边界调整监听地址和访问规则。

本地及基础 K8s 验收使用 Collector sidecar/独立进程的 14317 接口，发送到测试接收端；这些测试地址不是产品默认值。

## 故障、开销与字段

业务线程只向标准 SDK 的有界批处理队列提交 Span。单次 Rust 导出超时为2秒；退出等待上限3秒。队列满时 SDK 可丢弃 Trace，不承诺 Trace 持久化或无限重试。Collector 独立配置磁盘发送队列，只有 Collector 已接收的内容才进入该队列。

Master 和 Node Manager 的 `/metrics` 增加 `adx_trace_exported_spans_total`、`adx_trace_export_failed_spans_total`，统计 SDK 导出成功/最终失败的 Span 数；不把它们解释为队列满丢弃数。Rust SDK 会记录开始丢弃及退出时的总丢弃诊断；当前没有统一暴露各语言的实时队列满丢弃计数。Collector 的接收/拒绝/排队/导出指标另外采集。

API 请求日志包含 Trace ID 和 Span ID；Node Manager 操作完成日志包含 `traceparent`。业务字段只记录操作、方法、路由模板、状态、Instance ID等；Trace不添加API Key、请求正文、命令内容、文件内容或用户代码。传播器仅处理W3C Trace字段，不传播 baggage。

## 验收

组件测试覆盖跨任务/队列的并发隔离、调用方取消后已接受操作继续关联、采样关闭、导出端不可用时提交不阻塞、失败计数与退出时间。真实部署验收检查完整创建链路的Trace ID，以及每个 `instance.execute` 的父Span确为同Trace内的 `instance.queue`，同时核对RRT收到远端上下文。

本地与Rust API Server重写后的 [Buildkite #24 正式验收](2026-09-17-rust-api-server-k8s.md) 已通过；统一实时队列满丢弃指标后置，详见 [事项清单](control-plane-remaining.json)。节点恢复后产生新的后台Trace，以Instance/代次/状态关联，不承诺跨进程重启续接已结束的Span。
