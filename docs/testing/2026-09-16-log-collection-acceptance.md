# 组件结构化日志与采集验收

功能提交 `447e135efa9ddb6b776d3b1888d5b18103090d29`，分支 `ci/control-plane-k8s-20260916`。配置及采集保证边界见[组件日志采集](log-collection.md)。

## 实现与组件检查

Master、Node Manager、Gateway 和 Sandbox API 可输出 JSON。Node Manager 的操作完成日志保留 Instance ID、generation、revision、状态；API 请求日志记录路由模板、方法、状态及耗时，省略查询参数、凭证和正文。Collector 附加节点/服务归属并通过 OTLP/HTTP 发送；Redis 原文日志同样可收集。

Supervisor 增加完整行滚动、超长记录边界与延迟压缩窗口。首先验证旧实现拒绝新契约，再增加跨文件 JSON 完整性、跨读取缓冲、超长行后恢复、保留窗口及压缩清理测试。macOS Rust 19项通过，Linux额外覆盖 `/dev/full`；Clippy、三类服务bins检查、Go入口测试/构建及Python驱动55项通过。

Collector Contrib 固定0.161.0与镜像摘要，真实二进制配置验证通过；CI所用同摘要amd64区域镜像拉取验证通过。Collector自身指标端口使用18888，Sandbox API使用8888。

Linux ARM64全部Rust bins与修改后的Go API重新构建，SDK/Redis复用已验证发布包。`out/ci/stage-8/log-collection/package/manifest.json` 标记本地工作树dirty=true；`source-files.json`记录迁入CI提交的文件哈希。正式CI从干净提交重新构建。

## 本地真实双节点

run_id=`adx-e2e-c6ac8608ed63`，使用外部sandboxd/runc。最终证据目录为`out/ci/stage-8/log-collection/local-3/`。公共SDK七组及清理全部通过，`cleanup_errors=[]`、`missing_checks=[]`。

| 用例组 | 结果 | 秒 |
| --- | --- | ---: |
| sdk | PASS | 25.068 |
| auth | PASS | 20.822 |
| capacity | PASS | 33.624 |
| placement | PASS | 95.816 |
| node-failure | PASS | 55.65 |
| restart | PASS | 4.665 |
| stop | PASS | 32.944 |

两节点均在Collector后端持续返回503时完成公开SDK创建、查询、命令、二进制文件和删除。每节点分别保存独立采集批次；随后滚动文件、让后端返回503、重启Collector并恢复后端，40条带唯一序号的记录各收到一次。此受控503发生在接收端存储前，不代表任意ACK丢失时也不存在重复。

节点1收到api/edge/master/node1/proxy/redis日志，节点2收到node2/proxy日志；两侧均检查到Running与Deleted操作完成事件，生成的测试凭证未出现在发送载荷中。Edge和两侧Node Proxy原有指标端点抓取通过；满载、排队和释放三个时点的资源账本检查继续通过。两节点分别98/22个gzip归档可读，8个服务未报告日志I/O错误或丢弃字节。

## 正式基础K8s

[Buildkite #19](https://buildkite.com/agent-dx/agent-dx/builds/19)的发布包构建通过。首轮镜像步骤在40分钟限时后退出：发布包下载约31分钟，随后Collector镜像仍在拉取。K8s用例未执行；已仅重试失败的镜像步骤一次，仍使用同一提交，等待最终核验。独立Collector sidecar、每节点采集证据和Gateway指标进入SDK/stop组及构建汇总。只有实际Pod部署、七组用例、采集证据与清理全部通过后才判定正式验收成功。

## 证据与边界

- `red.log`、`green-2.log`、`go-check.log`、`python-checks-final.log`：契约失败与组件检查。
- `collector-validate-5.log`、`collector-mirror.log`：真实Collector配置和锁定镜像。
- `acceptance-3.log`、`local-3/result.json`、`local-3/case-results.json`：业务及清理结果。
- `local-3/collection-node*.json`、`gateway-metrics-node*.json`：两节点汇总结论。
- `local-3/telemetry-node*/collected-logs.jsonl`、`collector-process.log`、`collector-metrics.txt`：独立原始采集与自身指标。

本轮交付组件日志采集。跨组件Trace上下文、采样和导出仍单列待办；实例内用户日志未改为Supervisor采集。未覆盖采集端长时间离线超过保留窗口、磁盘断电、真实日志后端长期容量或FC新验收。
