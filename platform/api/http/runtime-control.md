# Node Manager ↔ RRT HTTP 契约

RRT 使用同一个 HTTP 监听端口提供用户操作与运行时协作。Node Manager 决定 Instance 生命周期并调用 sandboxd，RRT 负责实例内的准备、观测和恢复后初始化。共享 JSON 类型定义在 `platform/crates/core/src/runtime.rs`；服务实现为 `runtime/control.rs`，调用端为 Node Manager `runtime_control.rs`。

## 身份与就绪

sandboxd Start 环境包含 `ADX_INSTANCE_ID`、`ADX_RUNTIME_ID`、`ADX_OWNERSHIP_GENERATION`；Node Manager 注入的值覆盖普通启动配置中的同名值。三个字段必须同时存在且有效。没有身份配置的独立 HTTP 运行模式只提供数据操作，控制接口返回 503。

`GET /control/v1/status` 返回：

```json
{
  "identity": {"instance_id": "i", "runtime_id": "i-1", "ownership_generation": 1},
  "revision": 1,
  "phase": "running",
  "checkpoint": null,
  "active_requests": 0,
  "active_commands": 0
}
```

Node Manager 核对完整身份和 `running`，并在请求前后确认 sandboxd 实例仍在运行。`/healthz` 仅作浅层存活检查。运行时状态与操作版本仅在当前执行内有效；它们不替代 Master 持久化的归属代次。

配置 `RRT_HTTP_TOKEN` 时控制请求携带 `X-Auth`。这属于节点到实例的调用凭据，和用户 API Key、控制面组件间 mTLS 分开。控制 JSON 正文最多 64 KiB，需要 Content-Length，拒绝 chunked。

状态查询、health/metrics、命令观察连接不作为忙碌操作。状态返回 HTTP/隧道活动连接与活动命令计数；Node Manager 应结合 Node Proxy 的观测判断空闲。采集失败表示未知，不能视作零。

## Checkpoint 顺序

1. Node Manager 读取运行时身份和 revision。
2. `POST /control/v1/checkpoint/prepare`，正文为 `identity`、`operation_id`、`expected_revision`。
3. RRT 进入 Preparing，先成功打开后端 handoff 文件，随后返回 Prepared。接受后的任务独立于 HTTP 连接，断线不会取消它。Node Manager 必须同时验证返回的身份、operation_id、运行阶段和 checkpoint 阶段都是预期值，再允许调用 sandboxd Checkpoint。
4. sandboxd 负责实际 checkpoint。RRT 根据 handoff 的 `resume`、`restore` 或 `error` 处理后续运行。Node Manager 负责等待后端结果、制品上传、元数据持久化以及路由更新。

Prepared 仅证明实例内的 handoff 准备完成，不代表 checkpoint 制品已生成或暂停成功，也不承诺应用事务静止。Preparing/Prepared/Restoring/Failed 期间拒绝新的数据操作和隧道连接；已有工作由后端冻结语义约束。

同一个 operation_id 和完全相同的准备参数可查询/重试当前操作；参数不同或身份过期返回 409。新操作要求当前 revision 和 Running 阶段。只保留当前操作：更旧的重试因版本不匹配被拒绝，不能越过后续操作重新执行。

若后端明确证明 checkpoint 未开始，可调用 `POST /control/v1/checkpoint/abort-unstarted`，正文仍为 `identity`、`operation_id`、`expected_revision`（采用 Prepared 响应版本）。RRT 返回 Running/Aborted，保留已打开的 handoff reader，下一次准备复用它。后端超时或结果未知不能调用这个接口宣告恢复。

准备失败时返回的状态包含 checkpoint Failed；Node Manager 客户端不将它当作 Prepared。异常 handoff/恢复初始化失败进入 Failed，状态可查询，数据操作不可用。

## 恢复

`restore` handoff 后，RRT 重新读取完整执行身份。身份变化要求 ownership_generation 增大。恢复端不能改变已继承监听 socket 的端口。身份校验通过后刷新子进程环境、可选 HTTP token，关闭继承的 HTTP/tunnel 会话，重新注册监听器，再进入 Running/Restored。源端 `resume` 保留原身份。

节点需使用目标执行记录查询状态，只有身份和阶段匹配后才能绑定目标路由。RRT 不向 Master 注册，不负责迁移归属，也不直接发布 Edge 路由。

当前实现没有把 Node Manager 的完整暂停/恢复状态机、sandboxd Checkpoint、对象存储和 Redis 提交串成产品流程。`control_http.rs` 验证真实客户端/进程及 FIFO 通知；完整 checkpoint E2E 必须在已固定的 sandboxd PR #56 环境执行。
