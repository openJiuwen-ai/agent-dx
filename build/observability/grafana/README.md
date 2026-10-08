# ADX Grafana 看板

这三张看板是 ADX 可观测能力的一部分，覆盖调度与运行状态、数据面请求和服务进程开销。
看板 JSON 与指标实现、采集配置一起在本仓维护。修改指标名称、标签或单位时，应同步更新查询和说明。

## 看板与口径

| JSON | 内容与口径 |
| --- | --- |
| [adx-schedule.json](dashboards/adx-schedule.json) | CPU/内存/磁盘调度容量、已分配及分配率，Running Environment 分布，队列、节点健康、沙箱日志搜索和 Trace 导出。 |
| [adx-data-plane.json](dashboards/adx-data-plane.json) | Ingress 请求速率、状态分类、4xx/5xx 比例、响应头耗时 P50/P95/P99、超过 100 ms 比例、活跃请求及连接池；Relay 活跃流、连接成功/失败、拒绝与路由错误；Ingress 逻辑流及物理连接。 |
| [adx-process-resources.json](dashboards/adx-process-resources.json) | 进程 CPU 核数、RSS、文件描述符、线程，以及按 Pod/组件/PID 的运行时长和虚拟内存明细。 |

数据面速率和延迟查询使用 5 分钟窗口；延迟表示响应头返回耗时，不是命令完整执行或流式响应结束耗时。
Trace 面板显示导出累计 span 数量，具体调用链通过 Tempo 查询。
Schedule 聚焦调度账本、队列、准入和实例分布。节点实际使用量和沙箱实际使用量
由部署环境的节点资源与沙箱详情看板承载，避免在调度看板重复展示。
沙箱日志搜索面板仅查询 `component_name="adx-runtime"` 的主进程 stdout/stderr，
保留原文。支持环境、沙箱运行记录多选及原文关键词搜索；关键词区分大小写，留空不限制。
查询使用 `${log_search:doublequote}` 为关键词添加字符串引号并转义双引号；
空关键词生成 `|= ""`，仍返回日志。
运行记录候选来自所选时间范围内的 `adx_environment_stats_age_seconds` 的
`runtime_id` 标签，可输入 Sandbox ID 搜索对应运行记录。同一 Sandbox 的不同
运行代次是不同记录；缺少资源采样的运行记录不出现在候选中，可选择 All 查看
全部已采集输出。运行 ID 在 Loki 中仍为结构化元数据，不新增逐实例索引。
SDK command 返回的 stdout/stderr 不会自动归档为主进程日志。
环境和运行记录的 All 使用 `.+`，匹配非空标签；运行记录和关键词仅影响日志
面板，不改变调度指标。控制面日志仍被采集，可在 Loki Explore 单独查询。

## 导入与采集前提

1. 在 Grafana 配置 UID 为 `prometheus` 的 Prometheus 数据源；Schedule 日志面板还需要 UID 为 `loki` 的 Loki 数据源。若使用其他 UID，导入前修改 JSON 中的对应数据源引用。
2. 抓取 Coordinator、Adxlet、Ingress、Relay 的 `/metrics`，并接入部署环境的节点/沙箱资源指标与组件日志。ADX 端点与环境标签示例见 [prometheus.json](../prometheus.json)，采集职责见 [组件日志采集](../../../docs/testing/log-collection.md)。
3. 为指标及日志注入稳定的 `adx_env` 标签。示例静态抓取配置使用 `adx_env=adx`，部署多个环境时应分别配置实际值；日志采集配置也需提供同名标签，以便 Schedule 按环境查询。按 Pod/节点/组件筛选还需要 `k8s_namespace_name`、`k8s_node_name`、`k8s_pod_name`、`component_name`，部署采集配置负责补充；进程部署可以使用环境/组件筛选。
4. 在 Grafana 的 **Dashboards → New → Import** 中逐个上传 JSON，并选择实际的 `adx_env`。看板 UID 分别为 `adx-schedule`、`adx-data-plane`、`adx-process-resources`；导入同 UID 会更新已有看板，按 Grafana 提示确认。
5. 看板导航使用 Grafana 默认路径 `/d/<uid>`。部署 Grafana 到子路径时，在 JSON 的 `links[].url` 中加上实际路径前缀。

进程查询按文件描述符样本的新鲜度过滤，窗口为 45 秒，适配当前 15 秒采集间隔；改变采集周期时需同步调整。
空闲时请求速率为 0，延迟分位数可能没有数据。进程总量仅覆盖实际暴露并被抓取的进程，不能据此断言所有组件已被采集。

磁盘展示调度账本，不是文件系统实际使用率。调度容量仅汇总可调度节点，
暂停准入节点贡献为 0；底层 capacity 指标仍保留额定容量，供恢复调度使用。
已分配直接读取 `reserved` 账本，包含启动中与运行中实例；节点暂停准入不会
释放已有分配。可调度量受维护、压力和采样状态限制，不能用调度容量减可用量
推算已分配。集群分配率面板的分子与分母均只统计可调度节点。CPU/内存/磁盘口径一致。

节点状态表按环境与节点关联当前瞬时值，显示健康、准入、资源采样状态及 CPU/内存/磁盘调度容量和分配率；
CPU 以核显示，内存与磁盘按字节自动换算；暂停准入时容量显示 0。缺少观测显示 `Unknown`。Coordinator 指标携带 `node_id`；adxlet 的采样指标需要
部署抓取配置注入相同的 `node_id`，或在 Kubernetes 中提供与节点 ID 一致的
`k8s_node_name`。资源采样样本超过 45 秒后不作为当前状态；修改默认 15 秒
采集间隔时需要同步调整这个窗口。集群运行中沙箱趋势隐藏图例。容量趋势图仅显示调度容量与已分配两类曲线。

ADX Schedule 的标题、列名、图例、状态值和筛选说明统一使用英文。节点容量表列为
`Node (Env / ID)`、`Reachability`、`Admission`、`Resource Sample`、
`CPU Capacity (cores)`、`CPU Allocation`、`Memory Capacity`、
`Memory Allocation`、`Disk Capacity`、`Disk Allocation`。

节点表的 CPU、内存、磁盘分配率列使用横向条形及百分比显示，按可达节点的
`reserved / capacity` 计算，包含启动中和运行中的实例。分母使用原始容量，
暂停准入后仍能显示已有分配比例；容量列的调度容量为 0。原始容量为 0 或
缺少观测时显示 `Unknown`。条形固定以 100% 为满格，超过 100% 的数值仍显示；
低于 70% 为绿色，70%–90% 为黄色，达到 90% 为红色。分配率不是实际使用率。
