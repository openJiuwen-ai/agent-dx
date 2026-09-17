# Trace 本地验收

> 当次验收/调查记录：版本、数字及未覆盖范围仅适用于文中批次；当前实现与状态见 [实施总览](control-plane-implementation.md) 和 [阶段路线图](control-plane-roadmap.md)。

本轮接入 OpenTelemetry、W3C HTTP/gRPC 上下文、Master 独立任务、Node Manager 每实例队列、Gateway 和 RRT HTTP。配置及语义边界见[跨组件Trace](distributed-traces.md)。

## 组件与故障检查

- RED：Rust 新上下文契约和 Go HTTP→RPC 契约分别因新接口未实现而失败，保留 `red.log`、`go-red.log`。
- Rust 上下文测试：不同请求经过通道和独立任务保持父上下文；取消结束对应 Span。
- Node Manager 18项生命周期测试通过，含真实 InstanceHandle 队列：创建调用方取消后已接受操作继续；随后删除沿用自己的请求上下文。
- 导出故障测试通过：关闭采样时不沿用上游的采样位；不可达导出端不阻塞 Span 提交，失败计数增加，退出有时间上限。
- Go 全套测试和服务构建通过；Gateway 67项、RRT HTTP 3项、Python 驱动58项通过；Clippy零警告。
- Linux ARM64全部 workspace bins重新构建，Go入口重新构建；公共SDK/Redis复用已验证发布包。

## 双节点真实 SDK

run_id=`adx-e2e-592f53cf6dfd`，外部sandboxd/runc，统一发布包。SDK、auth、capacity、placement、node-failure、restart、stop七组通过，`cleanup_errors=[]`、`missing_checks=[]`。

| 检查 | node1 | node2 |
| --- | ---: | ---: |
| 收到的唯一 Span | 2329 | 918 |
| 已验证 queue→execute 父子关系 | 669 | 418 |
| 含 Edge/API/Master/Node/状态提交的完整创建 Trace | 9 | 在 node1 聚合检查 |
| RRT 接收到远端请求上下文 | PASS | PASS |
| 日志恢复唯一探针 | 40/40 | 40/40 |

node1采集到 `adx-master`、`adx-node-manager`、`adx-rrt`、`adx-sandbox-api`、`edge-frontend`、`node-proxy`。验证父子关系使用 Trace ID 与 Span ID 联合键，拒绝“父Span ID相同但属于另一条Trace”的伪关联。采集后端返回503期间公共SDK仍可创建、执行、读写文件和删除；恢复后的日志与Trace均已收到，测试凭证未出现在采集载荷中。原资源账本和Gateway指标、日志滚动压缩检查继续通过。

本地包标记dirty=true；证据在 `out/ci/stage-8/traces/`：`acceptance-1.log`、`local/result.json`、`local/traces-node*.json`、`local/telemetry-node*/collected-traces.jsonl`、`local/collection-node*.json`、`package/manifest.json`。最终迁入提交的文件哈希保存于`source-files.json`。

## 正式 CI 状态与边界

[Buildkite #19](https://buildkite.com/agent-dx/agent-dx/builds/19)验收的是前一日志采集提交447e135，编译通过；镜像步骤及唯一一次重试均因Collector下载达到40分钟限时，K8s未运行。第二次在同一层`3c661e367453`等待约29分钟。它不是本轮Trace代码的正式验收结果。

本轮Trace已进入基础K8s采集检查和构建汇总。2026-09-17，[Buildkite #20](https://buildkite.com/agent-dx/agent-dx/builds/20)已完成Collector同步至SWR及原摘要回拉；[Buildkite #21](https://buildkite.com/agent-dx/agent-dx/builds/21)已对b3145d6完成正式K8s验收，七组用例及日志/Trace核验通过，见[正式验收记录](2026-09-17-observability-k8s.md)。

尚未覆盖真实GPU/NPU、FC本轮Trace验证、长时间导出端中断、统一的各语言实时队列丢弃计数、SDK事务级根Span、用户进程内部Span。HTTP/gRPC单次请求、生命周期队列和节点操作的关联已验证；不把上述边界描述成完整业务全链路覆盖。
