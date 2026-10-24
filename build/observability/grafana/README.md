# ADX Grafana 看板

这些看板是 ADX 可观测能力的一部分，覆盖调度与运行状态、数据面请求和服务进程开销。
看板 JSON 与指标实现、采集配置一起在本仓维护。修改指标名称、标签或单位时，应同步更新查询和说明。

## 看板与口径

| JSON | 内容与口径 |
| --- | --- |
| [adx-schedule.json](dashboards/adx-schedule.json) | CPU/内存/磁盘调度容量、已分配及分配率，Running/Paused 分布，创建当前/峰值 QPS、耗时与结果、节点汇总资源、队列、节点健康和采集健康。 |
| [adx-data-plane.json](dashboards/adx-data-plane.json) | Ingress 请求速率、状态分类、4xx/5xx 比例、响应头耗时 P50/P95/P99、超过 100 ms 比例、活跃请求及连接池；Relay 活跃流、连接成功/失败、拒绝与路由错误；Ingress 逻辑流及物理连接。 |
| [adx-process-resources.json](dashboards/adx-process-resources.json) | 进程 CPU 核数、RSS、文件描述符、线程，以及按 Pod/组件/PID 的运行时长和虚拟内存明细。 |
| [adx-environments.json](dashboards/adx-environments.json) | 当前 Running/Paused 实例清单及新鲜 CPU/内存明细；使用独立短保留数据源 `prometheus-details`。 |
| [adx-environment-logs.json](dashboards/adx-environment-logs.json) | 主进程 stdout/stderr 与 HTTP Trace 明细，默认最近 15 分钟、手动刷新，日志最多 500 行、Trace 最多 50 条。 |

创建 QPS 使用一分钟平均速率；峰值是在所选时间范围内先汇总各副本/环境的同一时刻速率，再取最大值。
请求速率包含重试；首次创建就绪速率排除重放、resume、restart 和 recovery。SSE 接受响应不代表创建完成。
采集规则及进程内计数器边界见 [可观测数据与容量](../../../docs/testing/observability-capacity.md)。
数据面速率和延迟查询使用 5 分钟窗口；延迟表示响应头返回耗时，不是命令完整执行或流式响应结束耗时。
Schedule 每 30 秒刷新汇总统计；两项日志统计分别固定回看最近 1 分钟和 5 分钟，使用 instant 查询。日志和 HTTP Trace 明细在独立页按需查询。

日志页只查询 `component_name="adx-runtime"` 的主进程 stdout/stderr，保留原文。
Runtime ID 使用正则过滤，`.*` 选择全部，`.*environment-id.*` 选择该实例的执行代次；
正文关键词为区分大小写的字面匹配，可直接输入反斜杠、引号和反引号，空值匹配全部。
Runtime ID 使用原始字符串保留正则语义；正文关键词通过 Grafana `percentencode`
传入 Loki，由 `urldecode` 还原后用 `contains` 匹配原始日志。runtime 过滤先执行，
匹配用的临时标签在返回前移除。Runtime ID 保持 Loki 结构化元数据，
不增加逐实例索引；SDK command 返回的 stdout/stderr 不会自动归档为主进程日志。
Environment 明细清单独立于资源样本，因此 Paused 和无资源采样的实例仍能展示。

关键词查询回归使用隔离 Loki 容器，覆盖特殊字符、字面匹配、原文保留及 runtime/环境隔离：

```sh
docker pull grafana/loki:3.4.2
python3 -m unittest discover -s build/observability/tests -v
```

测试使用 Grafana 11.5.2 的标量变量格式规则和仓库内大盘查询，Loki 负责真实解析和匹配。
修改插值方式时还应在 Grafana 页面验证输入框到数据源的完整链路。

## 导入与采集前提

1. 在 Grafana 配置 UID 为 `prometheus` 的 Prometheus 数据源；Schedule 日志统计和独立日志页需要 UID 为 `loki` 的 Loki 数据源，Trace 明细需要 `tempo`。Environment 明细页需要 `prometheus-details`。若使用其他 UID，导入前修改 JSON 中的对应数据源引用。
2. 抓取 Coordinator、Adxlet、API Server、Ingress、Relay 的 `/metrics` 和 Collector `18888/metrics`，并接入部署环境的节点/运行环境资源指标与组件日志。ADX 端点与环境标签示例见 [prometheus.json](../prometheus.json)，采集职责见 [组件日志采集](../../../docs/testing/log-collection.md)。
3. 为指标及日志注入稳定的 `adx_env` 标签。示例静态抓取配置使用 `adx_env=adx`，部署多个环境时应分别配置实际值；日志采集配置也需提供同名标签，以便 Schedule 按环境查询。按 Pod/节点/组件筛选还需要 `k8s_namespace_name`、`k8s_node_name`、`k8s_pod_name`、`component_name`，部署采集配置负责补充；进程部署可以使用环境/组件筛选。
4. 在 Grafana 的 **Dashboards → New → Import** 中逐个上传 JSON，并选择实际的 `adx_env`。看板 UID 分别为 `adx-schedule`、`adx-data-plane`、`adx-process-resources`、`adx-environments`、`adx-environment-logs`；导入同 UID 会更新已有看板，按 Grafana 提示确认。
5. 看板导航使用 Grafana 默认路径 `/d/<uid>`。部署 Grafana 到子路径时，在 JSON 的 `links[].url` 中加上实际路径前缀。

进程查询按文件描述符样本的新鲜度过滤，窗口为 45 秒，适配当前 15 秒采集间隔；改变采集周期时需同步调整。
空闲时请求速率为 0，延迟分位数可能没有数据。进程总量仅覆盖实际暴露并被抓取的进程，不能据此断言所有组件已被采集。

磁盘展示调度账本，不是文件系统实际使用率。调度容量仅汇总可调度节点，
暂停准入节点贡献为 0；底层 capacity 指标仍保留额定容量，供恢复调度使用。
已分配直接读取 `reserved` 账本，包含启动中与运行中实例；节点暂停准入不会
释放已有分配。可调度量受维护、压力和采样状态限制，不能用调度容量减可用量
推算已分配。集群分配率面板的分子与分母均只统计可调度节点。CPU/内存/磁盘口径一致。

节点状态表按环境与节点关联当前瞬时值，显示健康、准入、资源采样状态及 CPU/内存/磁盘调度容量和分配率；
九条查询通过 `node_key` 使用 `joinByField` 的 `outer` 模式合并，保持每个环境与节点一行。
Grafana 11.5.2 下 `outerTabular` 会产生重复节点行和大量空单元格。若部署环境通过 Helm
管理看板，升级前需将这份 JSON 同步到 chart 的 dashboard 配置中。
CPU 以核显示，内存与磁盘按字节自动换算；暂停准入时容量显示 0。缺少观测显示 `Unknown`。Coordinator 指标携带 `node_id`；adxlet 的采样指标需要
部署抓取配置注入相同的 `node_id`，或在 Kubernetes 中提供与节点 ID 一致的
`k8s_node_name`。资源采样样本超过 45 秒后不作为当前状态；修改默认 15 秒
采集间隔时需要同步调整这个窗口。Running/Paused 趋势按状态区分图例。容量趋势图仅显示调度容量与已分配两类曲线。

ADX Schedule 的标题、列名、图例、状态值和筛选说明统一使用英文。节点容量表列为
`Node (Env / ID)`、`Reachability`、`Admission`、`Resource Sample`、
`CPU Capacity (cores)`、`CPU Allocation`、`Memory Capacity`、
`Memory Allocation`、`Disk Capacity`、`Disk Allocation`。

节点表的 CPU、内存、磁盘分配率列使用横向条形及百分比显示，按可达节点的
`reserved / capacity` 计算，包含启动中和运行中的实例。分母使用原始容量，
暂停准入后仍能显示已有分配比例；容量列的调度容量为 0。原始容量为 0 或
缺少观测时显示 `Unknown`。条形固定以 100% 为满格，超过 100% 的数值仍显示；
低于 70% 为绿色，70%–90% 为黄色，达到 90% 为红色。分配率不是实际使用率。
