# 文档与当前实现核对（2026-09-17）

## 核对基线与范围

源码基线为 `d36cfaf0f27309dabb16d42b359488bb322f98f9`。本次修改文档和文档检查工具，不修改产品行为。核对时，开发工作树 `instance-pause-resume` 与审计工作树的受版本管理产品源码、协议、构建脚本及配置内容一致；开发工作树原有未提交修改保留。

范围为仓库全部 Markdown、HTML、SVG 文档，加迁移清单、路线图及进度页状态。依赖清单、许可证、第三方声明与来源锁定文件属于构建输入或历史溯源，按原始身份保留，不做产品命名替换。忽略目录中的构建产物、工作区外的早期讨论页不属于仓库文档覆盖范围。

当前能力以源码及实际服务接线为准；验收结论以对应批次记录为准。历史报告保留版本、测试数量和失败信息，增加批次边界与当前入口，不能用旧报告中的“尚未实现”说明当前状态，也不能把组件测试等同于完整 E2E。

## 主要修订与源码依据

| 主题 | 修订后的契约 | 核对入口 |
|---|---|---|
| 分层与目录 | Global 轮转、同进程 Domain 调度、Node Manager 生命周期；共享 Gateway；Agent 的 Sandbox SDK 适配仍有缺口 | [当前目录与架构](../architecture/repository-layout.md)、[Agent 入口](../../agent/README.md) |
| SQLite | 正常提交走 Master/Redis；JournalSink 在提交故障时写本地降级日志，Journaled 不等于集群发布成功 | [JournalSink](../../platform/control-plane/node-manager/src/journal.rs)、[节点契约](node-lifecycle.md) |
| HTTP/SDK | 暂停/恢复、快照及放置已接通；客户端保留的方法与字段不自动代表后端支持 | `../../platform/control-plane/sandbox-api/controlbackend/create.go`（当次基线源码）、`../../platform/control-plane/sandbox-api/controlbackend/backend.go`（当次基线源码） |
| 恢复与克隆 | 公开 resume 直达原归属节点；故障跨节点恢复由 Master 协调；快照克隆显式指定的资源规格须匹配模板 | `../../platform/control-plane/sandbox-api/controlbackend/checkpoint.go`（当次基线源码）、[克隆规则](../../platform/control-plane/master/src/rpc/cloning.rs) |
| 失败与路由 | 使用当前 Rust Edge 状态与错误映射；删除旧 Sandbox Router 的 FATAL/OOM/410 和 etcd 监听说明 | [Edge 请求处理](../../gateway/src/edge/server.rs)、[失败语义](../../platform/control-plane/api-server/docs/sandbox-runtime-failure.md) |
| 调度 | Filter/Score 含分组亲和规则；拓扑仍存在内部库中，未作为公开 HTTP 验收能力；部分调优参数只有 Rust 配置接口 | [调度库](../../platform/crates/scheduling/README.md)、[服务配置](../../platform/control-plane/master/src/bin/adx-master.rs) |
| RRT | HTTP 控制协作已接入完整暂停恢复；恢复身份支持同归属代次推进执行版本；Status 含 activity_revision | [HTTP 契约](../../platform/api/http/runtime-control.md)、[身份类型](../../platform/crates/core/src/runtime.rs) |
| 日志与指标 | Edge/Node Proxy 已有日志、Metrics、Trace；文件压缩可配置；实例终态数量是 Gauge；实例用量标签当前无单独开关 | [Gateway 日志](../../gateway/src/common/logging.rs)、[可观测说明](observability-logging-plan.md) |
| 部署与工具 | 资源源支持 auto/sandboxd；证书启动加载；supervisor 清理实例后停止；修正无效 Gateway 命令及 Agent 构建目录 | [配置示例](../../build/config/examples/README.md)、[进程部署](process-deployment.md) |
| CI 与进度 | 当前正式基础 K8s 记录更新至 #21；FC 保持本地验收；阶段 8 已完成约定范围 | [#21 验收](2026-09-17-observability-k8s.md)、[剩余事项](control-plane-remaining.json) |

README 的架构图和目录 HTML 已更新。HTML 从同目录 Markdown 生成，后续修改只维护 Markdown 再生成，减少两份说明漂移。

## 仍需明确的实现边界

- Agent CLI/SDK/Executor 仍有旧 FaaS/外部运行时依赖；基础平台 E2E 不证明 Agent 业务闭环。
- 新后端不支持 `reload()`、创建 `failover=true`、挂载、入口继承、网络策略、独立资源上限、公开用户端口及每实例数据面安全策略；SDK 保留兼容字段不构成服务端承诺。
- FC 双克隆网络、GPU/NPU 实卡、完整服务混合负载与长稳仍待验收；K8s FC、x86 克隆对照、模板预热、证书热重载和统一实时 Trace 队列丢弃指标后置。
- 心跳失效会撤销归属/路由并触发恢复协调，返回节点先清理旧执行；这不构成跨宿主网络分区中旧进程已停止的物理证明。

## 检查方法与结果

```bash
python3 build/docs/check.py --output out/docs-audit/check.json
python3 build/docs/render_architecture.py --check
git diff --check
```

文档检查覆盖本页在内的 80 份 Markdown/HTML/SVG，检查本地链接和 Markdown 锚点、20 个 JSON 示例、SVG XML 结构及 HTML 生成一致性；均通过。外部网址可达性、JSON 的运行时配置语义不由该脚本保证，配置语义另对照解析器和示例检查。

另外检查 20 个 Makefile/CLI/驱动帮助或测试计划入口，不执行产品测试。首次 `frontend-control --list` 因缺少 `ADX_TEST_SANDBOX_API` 被正确拒绝；指定已有 API 可执行文件后通过，文档已补充该前置条件。日志与命令清单保存在本地 `out/docs-audit/commands.log`、`commands.json`、`frontend-control-list.log`，不随 Git 分发。

本轮未重新编译、部署或触发 Buildkite；引用的是既有 #21（测试源码 `b3145d6d43d04c06dbc85a91928d441814111d87`）验收记录。历史测试原始产物未全部重跑或重新获取。HTML/SVG 已做结构检查；浏览器预览因本地 URL 策略被拒绝，未完成目视验收。

## 逐文件覆盖清单

下表是本轮仓库文档清单。当前说明对照源码、配置或脚本；历史记录保留批次事实并校正与当前状态的关系；页面检查结构与数据源。静态检查工具的完整路径清单通过 `--output` 输出。

| 文档 | 核对类别 |
|---|---|
| [.buildkite/README.md](../../.buildkite/README.md) | 当前说明／源码与入口 |
| [AGENTS.md](../../AGENTS.md) | 当前说明／源码与入口 |
| [README.md](../../README.md) | 当前说明／源码与入口 |
| [README.zh.md](../../README.zh.md) | 当前说明／源码与入口 |
| [agent/AGENTS.md](../../agent/AGENTS.md) | 当前说明／源码与入口 |
| [agent/README.md](../../agent/README.md) | 当前说明／源码与入口 |
| `agent/README.zh.md`（当次基线文档，旧 Agent 目录现已清理） | 当前说明／源码与入口 |
| `agent/cli/README.md`（当次基线文档，旧 Agent 目录现已清理） | 当前说明／源码与入口 |
| `agent/executor/README.md`（当次基线文档，旧 Agent 目录现已清理） | 当前说明／源码与入口 |
| `agent/executor/README.zh.md`（当次基线文档，旧 Agent 目录现已清理） | 当前说明／源码与入口 |
| `agent/sdk/python/README.md`（当次基线文档，旧 Agent 目录现已清理） | 当前说明／源码与入口 |
| [build/config/examples/README.md](../../build/config/examples/README.md) | 当前说明／源码与入口 |
| [build/dev/progress.html](../../build/dev/progress.html) | 页面／结构与内容同步 |
| [build/e2e/README.md](../../build/e2e/README.md) | 当前说明／源码与入口 |
| [build/e2e/example/README.md](../../build/e2e/example/README.md) | 当前说明／源码与入口 |
| [build/e2e/firecracker/README.md](../../build/e2e/firecracker/README.md) | 当前说明／源码与入口 |
| [build/e2e/kubernetes/README.md](../../build/e2e/kubernetes/README.md) | 当前说明／源码与入口 |
| [docs/architecture/current-architecture.svg](../architecture/current-architecture.svg) | 页面／结构与内容同步 |
| [docs/architecture/repository-layout.html](../architecture/repository-layout.html) | 页面／结构与内容同步 |
| [docs/architecture/repository-layout.md](../architecture/repository-layout.md) | 当前说明／源码与入口 |
| [docs/deployment/standalone.md](../deployment/standalone.md) | 当前说明／源码与入口 |
| [docs/migration/2026-09-14-import.md](../migration/2026-09-14-import.md) | 批次记录／历史边界 |
| [docs/migration/adjustments.md](../migration/adjustments.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-15-checkpoint-acceptance.md](2026-09-15-checkpoint-acceptance.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-16-buildkite-16.md](2026-09-16-buildkite-16.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-16-buildkite-k8s.md](2026-09-16-buildkite-k8s.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-16-cross-node-recovery.md](2026-09-16-cross-node-recovery.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-16-deployment-acceptance.md](2026-09-16-deployment-acceptance.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-16-fc-clone-network.md](2026-09-16-fc-clone-network.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-16-installed-example.md](2026-09-16-installed-example.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-16-log-collection-acceptance.md](2026-09-16-log-collection-acceptance.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-16-logging-acceptance.md](2026-09-16-logging-acceptance.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-16-metrics-acceptance.md](2026-09-16-metrics-acceptance.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-16-node-failure-acceptance.md](2026-09-16-node-failure-acceptance.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-16-placement-e2e.md](2026-09-16-placement-e2e.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-16-placement-groups-recheck.md](2026-09-16-placement-groups-recheck.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-16-scheduling-fairness.md](2026-09-16-scheduling-fairness.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-16-scheduling-recheck.md](2026-09-16-scheduling-recheck.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-16-trace-acceptance.md](2026-09-16-trace-acceptance.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-17-collector-swr.md](2026-09-17-collector-swr.md) | 批次记录／历史边界 |
| [docs/testing/2026-09-17-documentation-audit.md](2026-09-17-documentation-audit.md) | 本次核对方法与覆盖清单 |
| [docs/testing/2026-09-17-observability-k8s.md](2026-09-17-observability-k8s.md) | 批次记录／历史边界 |
| [docs/testing/api-key-management.md](api-key-management.md) | 当前说明／源码与入口 |
| [docs/testing/control-plane-ci.md](control-plane-ci.md) | 当前说明／源码与入口 |
| [docs/testing/control-plane-implementation.md](control-plane-implementation.md) | 当前说明／源码与入口 |
| [docs/testing/control-plane-roadmap.md](control-plane-roadmap.md) | 当前说明／源码与入口 |
| [docs/testing/control-rpc.md](control-rpc.md) | 当前说明／源码与入口 |
| [docs/testing/distributed-traces.md](distributed-traces.md) | 当前说明／源码与入口 |
| [docs/testing/firecracker-cross-node.md](firecracker-cross-node.md) | 当前说明／源码与入口 |
| [docs/testing/api-server.md](api-server.md) | 当前说明／源码与入口 |
| [docs/testing/http-node-placement.md](http-node-placement.md) | 当前说明／源码与入口 |
| [docs/testing/instance-checkpoint.md](instance-checkpoint.md) | 当前说明／源码与入口 |
| [docs/testing/instance-resource-metrics.md](instance-resource-metrics.md) | 当前说明／源码与入口 |
| [docs/testing/live-progress.md](live-progress.md) | 当前说明／源码与入口 |
| [docs/testing/local-e2e.md](local-e2e.md) | 当前说明／源码与入口 |
| [docs/testing/log-collection.md](log-collection.md) | 当前说明／源码与入口 |
| [docs/testing/log-rotation.md](log-rotation.md) | 当前说明／源码与入口 |
| [docs/testing/master-storage.md](master-storage.md) | 当前说明／源码与入口 |
| [docs/testing/node-failure-takeover.md](node-failure-takeover.md) | 当前说明／源码与入口 |
| [docs/testing/node-lifecycle.md](node-lifecycle.md) | 当前说明／源码与入口 |
| [docs/testing/node-proxy-process-modes.md](node-proxy-process-modes.md) | 当前说明／源码与入口 |
| [docs/testing/observability-logging-plan.md](observability-logging-plan.md) | 当前说明／源码与入口 |
| [docs/testing/process-deployment.md](process-deployment.md) | 当前说明／源码与入口 |
| [docs/testing/recovery-discovery.md](recovery-discovery.md) | 当前说明／源码与入口 |
| [docs/testing/route-publication.md](route-publication.md) | 当前说明／源码与入口 |
| [docs/testing/scheduling-baseline-comparison.md](scheduling-baseline-comparison.md) | 批次记录／历史边界 |
| [docs/testing/scheduling-performance.md](scheduling-performance.md) | 当前说明／源码与入口 |
| [docs/testing/snapshot-storage.md](snapshot-storage.md) | 当前说明／源码与入口 |
| [gateway/README.md](../../gateway/README.md) | 当前说明／源码与入口 |
| [platform/api/http/runtime-control.md](../../platform/api/http/runtime-control.md) | 当前说明／源码与入口 |
| [platform/api/proto/README.md](../../platform/api/proto/README.md) | 当前说明／源码与入口 |
| `../../platform/control-plane/sandbox-api/README.md`（当次基线源码） | 当前说明／源码与入口 |
| [platform/control-plane/api-server/docs/sandbox-lifecycle-api.md](../../platform/control-plane/api-server/docs/sandbox-lifecycle-api.md) | 当前说明／源码与入口 |
| [platform/control-plane/api-server/docs/sandbox-runtime-failure.md](../../platform/control-plane/api-server/docs/sandbox-runtime-failure.md) | 当前说明／源码与入口 |
| [platform/crates/scheduling/README.md](../../platform/crates/scheduling/README.md) | 当前说明／源码与入口 |
| [platform/runtime/rrt/README.md](../../platform/runtime/rrt/README.md) | 当前说明／源码与入口 |
| [platform/sdk/sandbox/README.md](../../platform/sdk/sandbox/README.md) | 当前说明／源码与入口 |
| [platform/sdk/sandbox/python/README.md](../../platform/sdk/sandbox/python/README.md) | 当前说明／源码与入口 |
| [platform/sdk/sandbox/python/TODO.md](../../platform/sdk/sandbox/python/TODO.md) | 当前说明／源码与入口 |
| [third_party/sandboxd/README.md](../../third_party/sandboxd/README.md) | 当前说明／源码与入口 |

此外核对 `docs/testing/control-plane-remaining.json` 与本地 `out/dev/progress/state.json` 的阶段状态；`docs/migration/frontend-package-selection.txt` 标明历史导入清单。
