# ADX Grafana 看板

这三张看板是 ADX 可观测能力的一部分，覆盖调度与运行状态、数据面请求和服务进程开销。
看板 JSON 与指标实现、采集配置一起在本仓维护。修改指标名称、标签或单位时，应同步更新查询和说明。

## 看板与口径

| JSON | 内容与口径 |
| --- | --- |
| [adx-schedule.json](dashboards/adx-schedule.json) | CPU/内存/磁盘调度容量、预留、可用及分配率，Running Environment 分布，队列、节点健康、日志和 Trace 导出。 |
| [adx-data-plane.json](dashboards/adx-data-plane.json) | Ingress 请求速率、状态分类、4xx/5xx 比例、响应头耗时 P50/P95/P99、超过 100 ms 比例、活跃请求及连接池；Relay 活跃流、连接成功/失败、拒绝与路由错误；Ingress 逻辑流及物理连接。 |
| [adx-process-resources.json](dashboards/adx-process-resources.json) | 进程 CPU 核数、RSS、文件描述符、线程，以及按 Pod/组件/PID 的运行时长和虚拟内存明细。 |

数据面速率和延迟查询使用 5 分钟窗口；延迟表示响应头返回耗时，不是命令完整执行或流式响应结束耗时。
Trace 面板显示导出累计 span 数量，具体调用链通过 Tempo 查询。
Schedule 聚焦调度账本、队列、准入和实例分布。节点实际使用量和沙箱实际使用量
由部署环境的节点资源与沙箱详情看板承载，避免在调度看板重复展示。

## 导入与采集前提

1. 在 Grafana 配置 UID 为 `prometheus` 的 Prometheus 数据源；Schedule 日志面板还需要 UID 为 `loki` 的 Loki 数据源。若使用其他 UID，导入前修改 JSON 中的对应数据源引用。
2. 抓取 Coordinator、Adxlet、Ingress、Relay 的 `/metrics`，并接入部署环境的节点/沙箱资源指标与组件日志。ADX 端点与环境标签示例见 [prometheus.json](../prometheus.json)，采集职责见 [组件日志采集](../../../docs/testing/log-collection.md)。
3. 为指标及日志注入稳定的 `adx_env` 标签。示例静态抓取配置使用 `adx_env=adx`，部署多个环境时应分别配置实际值；日志采集配置也需提供同名标签，以便 Schedule 按环境查询。按 Pod/节点/组件筛选还需要 `k8s_namespace_name`、`k8s_node_name`、`k8s_pod_name`、`component_name`，部署采集配置负责补充；进程部署可以使用环境/组件筛选。
4. 在 Grafana 的 **Dashboards → New → Import** 中逐个上传 JSON，并选择实际的 `adx_env`。看板 UID 分别为 `adx-schedule`、`adx-data-plane`、`adx-process-resources`；导入同 UID 会更新已有看板，按 Grafana 提示确认。
5. 看板导航使用 Grafana 默认路径 `/d/<uid>`。部署 Grafana 到子路径时，在 JSON 的 `links[].url` 中加上实际路径前缀。

进程查询按文件描述符样本的新鲜度过滤，窗口为 45 秒，适配当前 15 秒采集间隔；改变采集周期时需同步调整。
空闲时请求速率为 0，延迟分位数可能没有数据。进程总量仅覆盖实际暴露并被抓取的进程，不能据此断言所有组件已被采集。

磁盘展示调度账本，不是文件系统实际使用率。调度容量来自所选资源源，
预留量直接读取 `reserved`；节点维护或压力保护会使 `available` 为零，
不能用容量减可用量表示已分配。CPU/内存/磁盘均采用同一口径。
