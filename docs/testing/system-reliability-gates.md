# 系统错误、超时与故障可靠性门禁

本文定义跨 API Server、Coordinator、adxlet、Relay、Execd 和 sandboxd 的系统级验收契约。组件 UT 只能证明局部规则；标记为 E2E 的项目必须经公共 Sandbox SDK 发起，并同时核对客户端结果、Redis 权威状态、路由和 sandboxd 物理实例。

## 错误契约

统一错误响应需要稳定携带 `code`、`message`、`retry`、`outcome`、`requestId`、`operationId` 和 `instanceId`。`retry` 只允许 `never`、`same_operation`、`after_backoff`；`outcome` 只允许 `not_started`、`unknown`、`committed`、`terminal`。HTTP、内部 gRPC 和 SDK 必须保持同一含义，不能把 `unknown` 降级成普通 404，也不能换 Environment ID 重建。

| 门禁 | 场景 | 通过条件 | 当前状态 |
|---|---|---|---|
| ERR-01 | 稳定错误映射 | Invalid、Auth、Permission、NotFound、Conflict、NoCapacity、Unavailable、Deadline、OutcomeUnknown、DataLoss、Internal 的 HTTP/gRPC/SDK 映射一致 | API Server 已逐类校验 HTTP 状态、稳定码、retry、outcome 和操作身份；SDK 结构化解析已有契约测试；完整进程 E2E 待补 |
| ERR-02 | 可重试分类 | 稳定业务冲突不重试；临时不可用按退避；结果未知只以同一 request/operation/instance 身份重试 | SDK 已覆盖 terminal final 不重试、structured unknown final 同身份重试和重试耗尽；真实断流 E2E 待补 |
| ERR-03 | 创建应答丢失 | Node 已启动但响应被切断；重试收敛到同一 backend 和 generation | SDK 已覆盖 accepted 后连接报错及正常 EOF 无 final，两类情况均复用 Request ID 和稳定 Environment 名称；[Full #42](https://buildkite.com/agent-dx/agent-dx-full-test/builds/42) 用真实 TLS 代理切断首个 Running 应答，验证 Redis generation 和 sandboxd backend 不变及公开 SDK 重试成功 |
| ERR-04 | 查询空结果 | 结果未知后的 404 不触发换 ID 或第二次 Start | SDK 稳定 Environment ID、同身份重试与原子 claim 已有测试；待真实代理断流后查询 E2E |

## Deadline 契约

`createTimeoutSeconds` 是从 API Server 接受请求到返回最终结果的整体预算。`scheduleTimeoutSeconds` 只在中心 ShardScheduler 尚未形成 Assignment 时生效。Local-first 本地准入不分配固定的一半预算，也不消耗中心排队预算；本地明确不满足后，由入口 adxlet 用同一 Environment ID 转交 Coordinator，中心排队计时从此开始。一旦形成 Assignment，启动 sandboxd、Execd ready、路由绑定和 Redis 提交不再受 schedule timeout 约束，但仍受整体 create deadline 及各阶段更短的内部 deadline 约束。

下游只能继承上游剩余预算或使用更短的阶段预算，不能重新获得更长 deadline。任何超时都不等于操作未发生；返回结果未知时，调用方使用同一身份查询或重试。

公共 `crates/transport` 提供请求／操作身份与剩余 deadline 原语；API Server、Coordinator 和 adxlet 仍在各自边界决定具体阶段预算和 gRPC/HTTP 状态。

| 门禁 | 场景 | 通过条件 | 当前状态 |
|---|---|---|---|
| TIME-01 | Local-first 本地命中 | 不出现 45s 本地加 45s 中心的预算切分；不调用中心队列 | 已有定向测试，待 E2E 时序证据 |
| TIME-02 | Local-first 本地不满足 | Node 明确 fallback 后才进入中心队列，中心单独使用 schedule timeout | 已实现，待 E2E 时序证据 |
| TIME-03 | 中心队列耗尽 | 尚未形成 Assignment 时原子移出内存队列并释放快照引用，返回 DeadlineExceeded 并提示同 ID 重试；已形成 Assignment 时不取消 | 已实现并有真实 Redis/mTLS 测试，待公共 SDK E2E 时序证据 |
| TIME-04 | 分阶段 deadline | 调度、启动、Execd ready、持久化分别耗尽；下游 deadline 不增长 | 待 E2E |
| TIME-05 | 上游超时后的结果 | 最终结果可由同一 request/instance 查询，物理 backend 唯一 | 部分组件覆盖，待 E2E |

## sandboxd 故障

| 门禁 | 场景 | 通过条件 | 当前状态 |
|---|---|---|---|
| SD-01 | sandboxd 晚于 adxlet 启动 | adxlet 持续等待；不注册、不准入；sandboxd 就绪后才开始对账和服务 | 已有 UDS 定向测试 |
| SD-02 | daemon 重启且 runtime 保留 | 已有实例不误判退出；资源观测过期后关闭新准入，连接恢复后对账并重新开放 | pinned gRPC/UDS 契约测试覆盖按标签找回 backend 且不重复 Start；[Full #17](https://buildkite.com/agent-dx/agent-dx-full-test/builds/17) 对两节点 daemon 注入 `SIGKILL`，验证原 backend ID 保留及 SDK 恢复；[Full #40](https://buildkite.com/agent-dx/agent-dx-full-test/builds/40) 验证资源采集进程暂停时观测过期、关闭新准入和恢复。两种故障同时发生仍待独立注入 |
| SD-03 | daemon 重启且 runtime 丢失，Never | 实例进入 Failed、撤路由、释放资源，不自动冷启动 | [Full #52](https://buildkite.com/agent-dx/agent-dx-full-test/builds/52) 冻结 adxlet 后经 sandboxd 删除 runtime，并强制重启 daemon；旧 backend 未恢复，原归属保持，Redis Failed 且资源释放，最终物理清理通过；入口撤路由仍需单独端到端断言 |
| SD-04 | daemon 重启且 runtime 丢失，自动重启 | 遵守重试上限与退避，新 backend 使用同一 Environment ID 和有效 generation | [Full #52](https://buildkite.com/agent-dx/agent-dx-full-test/builds/52) 验证 daemon PID 变化、旧 backend 丢失、同归属新 backend、Running/restart_attempts=1 和 SDK 命令；[Full #39](https://buildkite.com/agent-dx/agent-dx-full-test/builds/39) 已验证后续连续退出时的重试上限与退避，最终资源清理通过 |

## adxlet 与节点故障

| 门禁 | 场景 | 通过条件 | 当前状态 |
|---|---|---|---|
| NM-01 | 心跳期限内进程重启 | 新 session 完成权威对账并保留有效 backend；对账完成前不准入 | 已有本地 E2E |
| NM-02 | 超过心跳期限后进程返回 | 旧实例已经失效；返回节点清理旧执行与绑定后才重新准入 | 已有本地 E2E |
| NM-03 | 对账期间再次崩溃 | 再次启动继续从 Redis 权威状态收敛，不复活旧 generation | 已有中断物理清理后新 adxlet 重读 inventory、重复幂等删除并保持准入关闭的组件测试；[Full #43](https://buildkite.com/agent-dx/agent-dx-full-test/builds/43) 在双物理 worker 真实注入失联、runc init 暂挂、对账中 Adxlet 崩溃，随后验证换 session、清理旧 backend 再准入 |
| NODE-01 | 节点故障且无 checkpoint | Failed、撤路由，不从镜像冷启动 | 已有组件覆盖；[Full #31](https://buildkite.com/agent-dx/agent-dx-full-test/builds/31) 在双物理 worker 网络隔离期间验证失效和公开入口拒绝。完整三 VM 门禁仍待执行 |
| NODE-02 | 共享 checkpoint | 跨节点恢复同一 Environment ID，generation 递增，旧执行不能复活 | 已有本地 FC 覆盖，待多 VM 门禁 |
| NODE-03 | local-only checkpoint | 明确恢复失败，不在其他节点创建空白实例 | 已有组件覆盖，待多 VM 门禁 |
| NODE-04 | 原节点迟到返回 | 清理旧执行后才开放准入，旧提交和旧路由全部拒绝 | 已有本地进程失联 E2E；[Full #31](https://buildkite.com/agent-dx/agent-dx-full-test/builds/31) 验证恢复节点的旧 backend 清理和准入顺序。旧 session 的直接写入拒绝仍由组件及三 VM 专项用例分别覆盖 |

上述未完成项进入 Standalone、Multi-VM 和 Full Deployment 的故障扩展组。正式门禁不能用 mock、单元测试或跳过用例替代真实进程、网络、Redis 与执行后端证据。
