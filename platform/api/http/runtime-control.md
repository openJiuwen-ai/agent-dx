# Adxlet ↔ EXECD HTTP 契约

EXECD 使用同一个 HTTP 监听端口提供用户操作与运行时协作。Adxlet 决定 Environment 生命周期并调用 sandboxd，EXECD 负责实例内的准备、观测和恢复后初始化。共享 JSON 类型定义在 `platform/crates/core/src/runtime.rs`；服务实现为 `runtime/control.rs`，调用端为 Adxlet `runtime_control.rs`。

## 身份与就绪

sandboxd Start 环境包含 `ADX_ENVIRONMENT_ID`、`ADX_RUNTIME_ID`、`ADX_OWNERSHIP_GENERATION`；Adxlet 注入的值覆盖普通启动配置中的同名值。三个字段必须同时存在且有效。没有身份配置的独立 HTTP 运行模式只提供数据操作，控制接口返回 503。

`GET /control/v1/status` 返回：

```json
{
  "identity": {"environment_id": "c", "runtime_id": "i-1", "ownership_generation": 1},
  "revision": 1,
  "phase": "running",
  "checkpoint": null,
  "requested_checkpoint": null,
  "active_requests": 0,
  "active_commands": 0,
  "activity_revision": 0
}
```

Adxlet 核对完整身份和 `running`，并在请求前后确认 sandboxd 实例仍在运行。`/healthz` 仅作浅层存活检查。运行时状态与操作版本仅在当前执行内有效；它们不替代 Coordinator 持久化的归属代次。

配置 `EXECD_HTTP_TOKEN` 时控制请求携带 `X-Auth`。这属于节点到实例的调用凭据，和用户 API Key、控制面组件间 mTLS 分开。控制 JSON 正文最多 64 KiB，需要 Content-Length，拒绝 chunked。

状态查询、health/metrics、命令观察连接不作为忙碌操作。状态返回 HTTP/隧道活动连接与活动命令计数；Adxlet 应结合 Relay 的观测判断空闲。采集失败表示未知，不能视作零。

## Checkpoint 顺序

1. Adxlet 读取运行时身份和 revision。
2. `POST /control/v1/checkpoint/prepare`，正文为 `identity`、`operation_id`、`expected_revision`。
3. EXECD 进入 Preparing，先成功打开后端 handoff 文件，随后返回 Prepared。接受后的任务独立于 HTTP 连接，断线不会取消它。Adxlet 必须同时验证返回的身份、operation_id、运行阶段和 checkpoint 阶段都是预期值，再允许调用 sandboxd Checkpoint。
4. sandboxd 负责实际 checkpoint。EXECD 根据 handoff 的 `resume`、`restore` 或 `error` 处理后续运行。Adxlet 负责等待后端结果、制品上传、元数据持久化以及路由更新。

Prepared 仅证明实例内的 handoff 准备完成，不代表 checkpoint 制品已生成或暂停成功，也不承诺应用事务静止。Preparing/Prepared/Restoring/Failed 期间拒绝新的数据操作和隧道连接；已有工作由后端冻结语义约束。

同一个 operation_id 和完全相同的准备参数可查询/重试当前操作；参数不同或身份过期返回 409。新操作要求当前 revision 和 Running 阶段。只保留当前操作：更旧的重试因版本不匹配被拒绝，不能越过后续操作重新执行。

若后端明确证明 checkpoint 未开始，可调用 `POST /control/v1/checkpoint/abort-unstarted`，正文仍为 `identity`、`operation_id`、`expected_revision`（采用 Prepared 响应版本）。EXECD 返回 Running/Aborted，保留已打开的 handoff reader，下一次准备复用它。后端超时或结果未知不能调用这个接口宣告恢复。

准备失败时返回的状态包含 checkpoint Failed；Adxlet 客户端不将它当作 Prepared。异常 handoff/恢复初始化失败进入 Failed，状态可查询，数据操作不可用。

## 恢复

`restore` handoff 后，EXECD 重新读取完整执行身份。同实例恢复允许归属代次增加，或在同归属代次下推进 execution runtime ID；跨 Environment 克隆另校验 checkpoint 源身份与受控的 `ADX_RESTORE_ORIGIN`。恢复端不能改变已继承监听 socket 的端口。身份校验通过后刷新子进程环境、可选 HTTP token，关闭继承的 HTTP/tunnel 会话，重新注册监听器，再进入 Running/Restored。源端 `resume` 保留原身份。

节点需使用目标执行记录查询状态，只有身份和阶段匹配后才能绑定目标路由。EXECD 不向 Coordinator 注册，不负责迁移归属，也不直接发布 Ingress 路由。

完整暂停/恢复、sandboxd checkpoint、本地/S3 和 Redis 提交已接入产品流程，并有 [本地 FC 验收](../../../docs/testing/control-plane-roadmap.md)。`control_http.rs` 的真实客户端/进程与 FIFO 通知测试仅证明协议行为。正式 K8s FC 后置，不能由基础 K8s 通过推导。

## Workload-local checkpoint

When Adxlet has checkpoint storage configured it supplies
`ADX_EXECD_CONTROL_SOCKET_PATH=/run/adx`. The operator may override the directory
through `execd_env`; AKernel uses `/run/akernel`. EXECD binds `execd.sock` there. The
socket belongs to the Environment filesystem and is not mounted from the host.

```sh
curl --fail-with-body --unix-socket /run/adx/execd.sock \
  -X POST http://localhost/checkpoint
```

This creates an anonymous **local recovery point**, retaining the same running
runtime, allocation and route. It does not create a reusable snapshot catalog
entry. The point replaces the previous point, follows Environment cleanup, and is
consumed by reload or same-node failover. It cannot recover a lost node.

EXECD serializes local requests and exposes the pending operation ID in
`GET /control/v1/status` as `requested_checkpoint`. Adxlet reads it during
its lifecycle monitoring cycle, checks execution identity, prepares the handoff,
and calls sandboxd Checkpoint with `leave_running=true` (capture timeout 300s).
The existing HTTP cooperation channel carries this exchange; it requires no
outbound control-plane credential in the guest.

After backend success and the matching `resumed` handoff, Adxlet retains
the artifact locally and commits the Running record. Only then does it send
`POST /control/v1/checkpoint/finish` with `identity`, `operation_id`, and nullable
`error`. EXECD requires the matching identity, pending ID and successful handoff
before returning `200 {"status":"completed"}` to the Unix caller. A repeated
identical finish is idempotent. Concurrent local requests return 409; unsupported
runtime or failed work returns 503 with an error body. Other paths/methods return
404/405. This operation is exposed only on the Unix listener.

A lost commit or finish response is retried without invoking Checkpoint again.
A Journaled result leaves the caller pending until cluster publication succeeds.
A client disconnect does not cancel accepted work. A restored runtime discards
its inherited source request, refreshes identity and rearms the Unix listener.
Backend result uncertainty is never translated into success or a second capture;
Adxlet retires the uncertain execution. External pause/snapshot requests
cannot prepare a different operation while a local request is pending.

On Adxlet restart, a prepared workload request without a matching committed
recovery point is retired during reconciliation before routes become ready. A
committed point survives and its completion acknowledgement may be retried.
