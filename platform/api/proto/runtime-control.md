# Execd 主动控制流契约

启用 adxlet 的 `runtime_control` 配置后，Execd 完成监听、镜像入口初始化和控制状态安装，主动连接本节点 adxlet 的 `RuntimeControlService.OpenControl`。服务定义为 [`runtime_control.proto`](runtime_control.proto)，共享消息为 [`ControlOperation`](../../crates/protocol/src/runtime_stream.rs) 和 [`RuntimeStatus`](../../crates/core/src/runtime.rs)。用户命令、文件、PTY、端口与 reverse tunnel 继续使用原来的数据面接口。

## 建连与身份

每条连接的首帧必须是 Hello，包含完整 RuntimeStatus 和执行凭证。adxlet 在 sandboxd Start/restore 前登记预期身份，故 Ready 可以早于 Start 的返回；重启对账仅为权威目录允许的执行重新登记。未登记执行不能因主动注册而获得归属或资源。

凭证是节点主密钥对 `{environment_id, runtime_id, ownership_generation}` 的 HMAC-SHA256。主密钥持久化在节点 `key_file`，不会注入实例；每个执行只获得自己的派生凭证。普通用户规格不能覆盖控制地址或凭证。主密钥丢失后已有执行不能使用原凭证重连，因此不得作为临时配置目录清理。

Hello 验证成功后的 Running 状态就是 Ready 通知。adxlet 等待通知并确认 sandboxd 执行仍在运行，随后应用 Relay 绑定和提交创建结果。创建就绪直接等待状态通知；正常生命周期订阅最新流状态，断流使观测失效并进入重连流程。同一身份的新连接替代旧连接；旧连接的迟到状态、结果和断线清理不能修改新连接。删除/失效清理及暂停确认后端停止后撤销预期身份，迟到重连被拒绝。恢复使用新 Runtime 身份登记，旧执行槽位不会跨恢复累积。

此端点独立于节点组件 RPC 的 mTLS 监听，当前使用节点私有网络内的 HTTP/2 和执行凭证，不读取组件证书。它不应暴露在公网；凭证不提供传输加密。部署需允许 Runtime 到本节点端点的网络访问。

## 双向消息

- Execd → adxlet：完整控制状态变化，包括 Initializing、Ready、probe 最终 failure、checkpoint 请求/绝对截止时间、Prepared、Resumed、Restored、Failed；重连始终发送完整快照。
- adxlet → Execd：Status、Quiesce、Prepare、Abort、Finish。消息复用现有控制状态机，携带原有 identity、operation_id、expected_revision。
- Execd → adxlet：按 request_id 返回控制结果或稳定错误类别 INVALID / CONFLICT / UNAVAILABLE。

Execd 将活动变化合并后主动上报，合并窗口为 250 ms；普通 Status 查询仅用于明确操作所需的即时确认。消息上限 64KiB，每连接最多 16 个待应答请求与 16 个待发送消息；调用取消、超时、断线均释放待应答条目。连接断开本身不证明 runtime 退出，不授权冷启动或改变归属；断流后等待重连，超出窗口再按需确认后端并应用健康策略。

每个活跃 Runtime 维持一条到所属 adxlet 的 TCP/HTTP2 控制连接，两端各占一个连接 FD；不会向 Coordinator 增加逐 Runtime 的控制连接。该成本需要与数据面的 tunnel/长连接分开计量。

## 工作负载 probes

Readiness 由 Execd 执行，Waiting 阶段报告 `initializing`，只在首次成功后报告
`running`。Adxlet 的创建流程直接等待执行槽位上的状态 watch，readiness
通知不经过正在执行 Create 的串行命令队列。最终失败携带共享 `ProbeFailure`
类型，通过 watch 唤醒创建并走原启动失败清理/发布路径。

Healthcheck 达到阈值后报告 `failed` 和结构化 `failure`；状态保留至执行退休，
重连携带完整快照。该报告更新当前执行的状态 watch，直接唤醒 Environment 控制器；控制器确认执行身份、清理并应用 restart/failover 策略。Execd 不自行重启，Initializing 不作为 Redis 生命周期状态。

## 实例内 checkpoint

```text
用户 POST Execd Unix Socket /checkpoint
    → Execd 保存 operation_id、绝对 deadline，通知 adxlet
    → adxlet 唤醒该 Environment 的串行控制器
    → Prepare → sandboxd Checkpoint(leave_running=true)
    → 等待 Resumed 通知 → 保存制品 → 提交恢复点元数据
    → Finish → Execd 唤醒 Unix Socket 调用方
```

后端继续运行、Prepared、Resumed 或消息发送成功均不等于 checkpoint 成功。成功 ACK 仍要求制品和恢复点元数据提交完成。断线后待请求和控制操作保存在 Execd 控制状态中，重连快照让 adxlet 对账；使用同一 operation_id 重试，不因连接改变再次生成 checkpoint。已失效执行按既有对账规则清理。

状态通过每个执行自己的 watch 交给 Controller；该 watch 保留最新状态和未完成的请求，不依赖共享提示队列或周期扫描。checkpoint 在 Controller 串行队列执行，gRPC 收包任务只更新状态。

Unix API 的空 body / `{}` 默认 600 秒，显式 timeoutSeconds 为 1–3600 秒。原绝对 deadline 继续约束 prepare、sandboxd、handoff、制品和元数据提交；Resumed 等待不另加一个更短的 RPC 截止时间。连接和普通控制请求仍有独立超时，单条长连接不设置整个会话的 RPC deadline。

## 配置与升级

参考 [主动控制流部署说明](../../../docs/deployment/runtime-control-stream.md)。adxlet 要求配置 `runtime_control`，普通生命周期观测依赖主动控制流。[HTTP 控制契约](../http/runtime-control.md) 用于显式操作和诊断。开启需要同时更新 adxlet 和镜像/rootfs 中的 Execd；只更新节点二进制不能使旧 Execd 主动注册。无需修改 sandboxd、SDK 或用户 checkpoint 请求格式。

测试及证据层级见 [控制流验证](../../../docs/testing/runtime-control-stream.md)。

## 活动与恢复

请求和命令开始、结束时推进 `activity_revision`，Execd 合并变化后主动发送完整状态，不要求生命周期 revision 同步变化。节点直接订阅当前执行的状态槽位，读取最新状态后处理；通知不会因共享唤醒队列满而丢失。旧会话和旧执行代际仍被拒绝。连接断开清除观测，重连 Hello 恢复完整状态，包括尚未完成的 checkpoint 请求。

代理活动通过 `RelayService.WatchActivity` 订阅：首次返回 `reset=true` 的完整视图，后续只包含变化条目及 `removed=true` 的退役条目。每条信息包含 Environment ID、runtime ID、活动 revision 和当前计数；短请求即使在合并窗口内结束，也会留下版本变化。接收方按执行分发，不广播唤醒所有实例。

自动删除前，节点先按代理活动版本关闭入站，再发送 Quiesce 校验 Execd 活动版本和当前计数并关闭运行时请求准入。版本已变或仍有请求时拒绝回收；确认应答丢失时通过幂等 ReopenIdle 恢复运行时准入，确认成功后才重开代理准入，断流后由实例重试截止时间继续恢复。节点重启接管运行中实例时，也在发布路由前确认运行时准入已恢复。自动暂停继续使用 Prepare 的空闲屏障。
