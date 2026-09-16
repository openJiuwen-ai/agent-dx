# 实例数量与资源分配 Metrics

阶段8首批能力，已通过[本地与Buildkite #17验收](2026-09-16-metrics-acceptance.md)。Master与Node Manager可通过配置 `metrics_listen` 启用HTTP `GET /metrics`，未配置时不开端口。示例配置使用Master `127.0.0.1:19090`、Node Manager `127.0.0.1:19091`；`adxctl`按服务配置下发。其他路径/方法返回404。端点用于监控采集，没有用户API Key认证；跨机采集时应绑定部署的私有监控接口并由部署环境限制访问。

## 指标口径

| 指标 | 标签 | 含义 |
| --- | --- | --- |
| `adx_master_instances` | domain_id, node_id, state | 已分配归属的实例当前状态数量，包含无执行结果的Reserved、失效的Invalidated和跨节点Recovering；Deleted不计入 |
| `adx_master_queued_requests` | domain_id | 尚未分配的内存队列长度；单列，不与实例计数相加解释为运行数量 |
| `adx_master_deleted_records` | 无 | 当前目录保留的Deleted记录数，Gauge，不是永久累计事件计数 |
| `adx_master_node_schedulable` | domain_id, node_id | 该节点当前是否可调度；Master重启待报到或心跳过期时为0 |
| `adx_master_node_reachable` | domain_id, node_id | 当前会话是否已报到且未超出心跳期限，不等同于可调度 |
| `adx_master_node_heartbeat_age_seconds` | domain_id, node_id | 最近接受的心跳年龄；未报到时不提供时间值 |
| `adx_node_accepting_allocations` | 无 | Node Manager本机准入开关 |
| `adx_node_resource_observation_fresh` / `adx_node_device_observation_fresh` | 无 | 本机容量/设备观测是否仍在有效期内 |

资源Gauge有两个前缀：Master为 `adx_master_node`，附带domain_id和node_id；Node Manager为 `adx_node`，节点身份由采集target标签关联。后缀：

- `_{capacity,reserved,available,overcommitted}_{cpu_millis,memory_bytes,disk_bytes}`。
- `_devices{kind="gpu|npu",model="…",state="capacity|reserved|available|overcommitted"}`，单位整卡。

capacity是可分配上限（设备仅计健康库存）；reserved来自实际账本，包含待启动预留。available为当前可调度余量，不可调度时为0，设备采集过期也为0。overcommitted显示超过当前容量的占用；设备消失或变为不健康不抹掉原分配，模型标签来自原分配记录。没有设备也没有保留分配的型号不输出设备时间序列。

CPU使用量等既有 `adx_instance_*` 指标保持，由RuntimeBackend采样。使用量与reserved不同，不应互相替代；高基数实例明细配置和完整采样失败诊断后续补齐。

Master导出当前已应用的目录与调度账本，不在抓取时访问Redis。状态锁繁忙或需要权威恢复时返回503，让采集器标记该次抓取不可用，避免输出假零。跨组件采样并非原子快照：Master反映最近提交，节点故障降级期间两端允许出现差异。

## 采集示例

外部Prometheus兼容采集器的静态配置示例（与进程同主机时）：

```yaml
scrape_configs:
  - job_name: adx-master
    static_configs:
      - targets: ['127.0.0.1:19090']
  - job_name: adx-node
    static_configs:
      - targets: ['127.0.0.1:19091']
        labels:
          node_id: node-1
```

Pod部署需为采集方暴露私有metrics端口，并配置相应服务发现。现阶段未引入产品内置Collector或强制监控后端。

按域汇总示例：`sum by (domain_id, state) (adx_master_instances)`；按集群汇总：`sum(adx_master_node_reserved_cpu_millis)`。Master与Node的资源视图分别使用，不叠加求和，避免同一分配计数两次。

## 验收

针对容量下降、释放、维护状态、缺失/不健康设备、Master恢复后预留恢复与队列数量增加回归。真实Redis/mTLS生命周期测试额外抓取HTTP端点，核对Running、排队、删除后的实例和资源；心跳故障测试核对Invalidated和不可用容量。结果及部署验收边界另行记录，组件/Socket测试不能代替正式K8s验证。
