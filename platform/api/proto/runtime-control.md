# Execd 主动控制流契约

启用 adxlet 的 `runtime_control` 配置后，Execd 完成监听、镜像入口初始化和控制状态安装，主动连接本节点 adxlet 的 `RuntimeControlService.OpenControl`。服务定义为 [`runtime_control.proto`](runtime_control.proto)，共享消息为 [`ControlOperation`](../../crates/protocol/src/runtime_stream.rs) 和 [`RuntimeStatus`](../../crates/core/src/runtime.rs)。用户命令、文件、PTY、端口与 reverse tunnel 继续使用原来的数据面接口。

## 建连与身份

每条连接的首帧必须是 Hello，包含完整 RuntimeStatus 和执行凭证。adxlet 在 sandboxd Start/restore 前登记预期身份，故 Ready 可以早于 Start 的返回；重启对账仅为权威目录允许的执行重新登记。未登记执行不能因主动注册而获得归属或资源。

凭证是节点主密钥对 `{environment_id, runtime_id, ownership_generation}` 的 HMAC-SHA256。主密钥持久化在节点 `key_file`，不会注入实例；每个执行只获得自己的派生凭证。普通用户规格不能覆盖控制地址或凭证。主密钥丢失后已有执行不能使用原凭证重连，因此不得作为临时配置目录清理。

Hello 验证成功后的 Running 状态就是 Ready 通知。adxlet 等待通知并确认 sandboxd 执行仍在运行，随后应用 Relay 绑定和提交创建结果。配置开启时不进行 10ms HTTP 就绪轮询。同一身份的新连接替代旧连接；旧连接的迟到状态、结果和断线清理不能修改新连接。删除/失效清理及暂停确认后端停止后撤销预期身份，迟到重连被拒绝。恢复使用新 Runtime 身份登记，旧执行槽位不会跨恢复累积。

此端点独立于节点组件 RPC 的 mTLS 监听，当前使用节点私有网络内的 HTTP/2 和执行凭证，不读取组件证书。它不应暴露在公网；凭证不提供传输加密。部署需允许 Runtime 到本节点端点的网络访问。

## 双向消息

- Execd → adxlet：完整控制状态变化，包括 Ready、checkpoint 请求/绝对截止时间、Prepared、Resumed、Restored、Failed；重连始终发送完整快照。
- adxlet → Execd：Status、Prepare、Abort、Finish。消息复用现有控制状态机，携带原有 identity、operation_id、expected_revision。
- Execd → adxlet：按 request_id 返回控制结果或稳定错误类别 INVALID / CONFLICT / UNAVAILABLE。

状态采集需要最新活动计数时，adxlet 可在流上发送 Status；不为每次用户命令额外推送活动事件。消息上限 64KiB，每连接最多 16 个待应答请求与 16 个待发送消息；调用取消、超时、断线均释放待应答条目。连接断开本身不证明 runtime 退出，不授权冷启动或改变归属；仍由既有后端对账及可选健康策略决定生命周期。

每个活跃 Runtime 维持一条到所属 adxlet 的 TCP/HTTP2 控制连接，两端各占一个连接 FD；不会向 Coordinator 增加逐 Runtime 的控制连接。该成本需要与数据面的 tunnel/长连接分开计量。

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

通知通过有界唤醒队列交给现有控制器，不在 gRPC 收包任务中执行 checkpoint。队列饱和时状态仍保存在当前执行槽中，周期监控读取缓存补偿唤醒；无需重新轮询 HTTP。节点最多并行处理 32 个通知任务，同一 Environment 仍串行。

Unix API 的空 body / `{}` 默认 600 秒，显式 timeoutSeconds 为 1–3600 秒。原绝对 deadline 继续约束 prepare、sandboxd、handoff、制品和元数据提交；Resumed 等待不另加一个更短的 RPC 截止时间。连接和普通控制请求仍有独立超时，单条长连接不设置整个会话的 RPC deadline。

## 配置与升级

参考 [主动控制流部署说明](../../../docs/deployment/runtime-control-stream.md)。未配置 `runtime_control` 时，节点继续使用 [HTTP 控制契约](../http/runtime-control.md)。开启需要同时更新 adxlet 和镜像/rootfs 中的 Execd；只更新节点二进制不能使旧 Execd 主动注册。无需修改 sandboxd、SDK 或用户 checkpoint 请求格式。

测试及证据层级见 [控制流验证](../../../docs/testing/runtime-control-stream.md)。
