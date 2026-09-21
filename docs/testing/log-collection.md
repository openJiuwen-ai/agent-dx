# 组件日志采集

组件输出由 Supervisor 写入文件；部署环境运行 OpenTelemetry Collector Contrib，通过 filelog 读取并用 OTLP/HTTP 发送给日志后端。Collector 是独立部署服务，配置样例在 `build/observability/collector.json`，镜像版本和摘要在 `build/observability/source.json`。目前接入托管组件的文件日志；adxctl 自身诊断由启动它的终端、systemd 或 Pod 日志通道采集。实例内用户 stdout/stderr 继续使用 RRT/sandboxd 的通道。

## 组件与字段

在部署配置的每个服务 `env` 中设置 `ADX_LOG_FORMAT=json`。Master、Node Manager、Edge、Node Proxy、转发进程和 API Server 支持此开关，默认保持 text。默认共进程部署中，Edge 与 API Server 共享 `api-server` 服务日志，Node Proxy 与 Node Manager 共享 `node-manager` 服务日志；显式分进程时才分别产生 `edge` 或 `node-proxy` 服务日志。Rust 日志支持 `RUST_LOG` 级别过滤。Gateway 使用 JSON 时保持 `ADX_DATA_PLANE_LOG_DIR` 未配置，由 Supervisor 接管 stdout；现有 access/audit 开关仍有效。

Rust 输出包括时间、级别、target 和 fields；Rust API Server 输出时间、级别、event 和 HTTP 路由模板、方法、状态、耗时。Collector 将日志解析为结构化 body，并附加 `service.name`（Supervisor 服务 ID）、`adx.node.id` 和文件路径。Redis 等纯文本仍可采集，保留原文。

Node Manager 的 `capsule_operation_completed` 事件记录 capsule_id、generation、revision 和操作返回时的状态。API 请求日志只记录路由模板，不记录 URL 查询参数、Authorization、正文和用户文件。开启Trace后，API请求日志包含Trace ID与Span ID，Node Manager操作完成日志记录traceparent；接线与采样配置见[跨组件Trace](distributed-traces.md)。

## 进程部署

1. 配置 Supervisor 文件日志，并为 Collector 提供同一服务账号可读的日志目录。默认文件权限为 0600，不应放宽为全局可读。
2. 设置以下环境变量，创建状态目录并授权 Collector 写入：

| 配置 | 含义 |
| --- | --- |
| `ADX_LOG_DIR` | 部署 `state_dir/logs` 的绝对路径 |
| `ADX_NODE_ID` | 本节点稳定标识 |
| `ADX_COLLECTOR_STATE` | Collector 文件读取位置和发送队列的持久化目录 |
| `ADX_OTLP_ENDPOINT` | 实际日志后端的 OTLP/HTTP 基地址，Exporter 追加 `/v1/logs` |

3. 由部署环境启动 Collector：`otelcol-contrib --config=/opt/adx/config/collector.json`。样例以 OTLP JSON 编码发送；生产后端需要支持该编码。按后端要求补充 TLS CA、客户端证书或认证扩展，凭证从受保护配置读取。

建议 Supervisor 的配置起点：

```yaml
logging:
  enabled: true
  line_records: true
  max_record_bytes: 65536
  max_file_bytes: 104857600
  rotate_seconds: 86400
  compress: true
  compress_after_seconds: 300
  max_files: 10
  max_age_seconds: 604800
  max_total_bytes: 1073741824
```

`line_records` 在完整换行记录之间滚动，避免 JSON 被文件边界截断。单条记录可以超过 `max_file_bytes`，最大受 `max_record_bytes` 限制。超长记录丢弃至下一个换行，记录日志健康错误和丢弃字节数，之后继续接收正常记录；退出时残余短行补换行。

`compress_after_seconds` 为关闭文件保留采集窗口。在窗口内既不压缩也不按保留策略删除，所以文件数量与归档总字节预算是窗口过后的约束，短时磁盘占用可能超过配置值。按实际日志速率为此预留空间；这不是 Collector 的读取确认协议。

Collector 只读活动文件与未压缩归档，排除 `.gz`/`.tmp`，避免把同一日志当成两份输入。采集端停机超过未压缩窗口可能造成未读取日志无法实时补传；压缩归档可另行离线导入，但需自行处理重复。读取位置和有界发送队列持久化可覆盖正常重启与短期后端故障，不承诺任意崩溃/磁盘丢失下的 exactly-once。

Collector 自身指标使用独立的 `127.0.0.1:18888/metrics`，避开 Sandbox API 的 8888 端口。Collector 的内存限制、发送队列和重试可独立配置。Collector 不在实例生命周期调用链内；后端不可用时实例操作继续。队列耗尽会产生背压，进而可能超过文件保留窗口，必须监控 Collector 自身的发送失败、排队及内存指标。

已验证的 Collector 0.161.0 自身指标包括 `otelcol_exporter_queue_size`、`otelcol_exporter_queue_capacity`、`otelcol_exporter_sent_log_records`、`otelcol_receiver_accepted_log_records`、`otelcol_receiver_refused_log_records`。队列单位为批次；重试中的请求不等同于最终丢弃。

## Pod 部署

保持进程部署的组件配置，在同一 Pod 中增加 Collector sidecar，共享日志目录和 Collector 状态卷。平台仍由 `adxctl` 托管，Collector 由部署环境托管。生产中持久化状态卷按需要使用 PVC；`emptyDir` 只能承受容器重启，不能承诺 Pod 重建后保留采集位置。

仓库 E2E 的 Collector sidecar 是测试夹具：镜像内提供锁定的 Collector 和本地 OTLP 接收端，记录采集证据；不等同于产品自带中心日志后端。其独立启动、故障注入和重启不通过 ADX Supervisor。

## Metrics 与验证

Master/Node Manager 的实例及资源指标继续通过 `/metrics` 抓取。Edge 与 Node Proxy 已有 `/metrics`，包括请求/连接/流量/错误/路由等；无需重复实现。可使用 `build/observability/prometheus.json` 中的四类目标配置，替换地址后由部署环境抓取。不同来源的资源视图不要直接相加。

本轮验收包括：完整行滚动、超长行后恢复、延迟压缩和保留、HTTP 请求敏感字段省略；将后端置为 503 时完成真实双节点创建/执行/删除，随后抓取 Gateway 指标并接收组件日志；注入 OTLP 后端 503、滚动文件、重启 Collector，以 40 条唯一记录验证该受控场景的读取位置和持久化队列恢复。最终结果以验收报告为准。
