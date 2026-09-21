# Agent-DX v2

Agent 层使用 Rust，产品 API 嵌入统一 Gateway，无状态 Activator 可多副本部署。

| 目录 | 职责 |
| --- | --- |
| `crates/core` | 产品类型、协议、Sandbox 能力边界及通用校验 |
| `crates/store` | Template/Environment 元数据、Redis 原子条件写；内存实现仅供测试 |
| `activator` | 产品管理与稳定身份激活，调用 Sandbox 接口，无后台健康/恢复扫描 |
| `api` | inline 适配、Activator 客户端、服务选择 |

Environment 对应一个稳定逻辑 Sandbox ID。元数据先持久化，再由首次访问触发幂等创建；多副本复用同一身份。Sandbox 状态、健康、暂停/恢复与运行载体 0–1 由 Platform 保证。产品删除确认后可以重建同名 Environment，generation 隔离旧生命周期。

用户 Harness 由 RRT 启动，自行定义业务接口。HTTP/WS/SSH 保持透明转发，不限制业务并发。inline create/get/kill 直接适配 Sandbox，独立于 Environment、Activator 和 ADX Redis。

## 部署

```sh
cargo build --locked -p data-plane-gateway -p adx-api-server --features data-plane-gateway/agent-api --bins
cargo build --locked -p adx-activator --bin adx-activator
```

Gateway 保留独立 Edge 与 API Server 内嵌 Edge 两种部署形态；Agent 装配共用同一入口。构建时为承载 Edge 的二进制启用 `data-plane-gateway/agent-api`，保留既有 TLS、认证、共享路由与 `ADX_SANDBOX_CONFIG`。`ADX_AGENT_CONFIG` 使用 `inline_only` 或 `both`；后者增加以下配置（令牌从环境变量读取）：

```json
{
  "activator": {
    "urls": ["https://activator.internal"],
    "token_env": "ADX_ACTIVATOR_SERVICE_TOKEN",
    "timeout_seconds": 60,
    "ca_path": "/etc/adx/ca.pem",
    "allow_plaintext": false
  }
}
```

这是 Agent 配置片段，完整配置还包括 inline_profiles、backend_timeout_seconds 与 max_inflight。Gateway 不缓存 Template 或 Target，查询和获取目标均调用 Activator。Gateway 不配置 ADX Redis；Platform 自身的 Redis 发现不受影响。Activator 部署见 [进程说明](activator/README.md)，状态保证见 [存储说明](crates/store/README.md)。

受管接口使用 `/api/agent/v2/templates/{name}/versions/{version}/environments/{id}`，PUT/GET/DELETE 对应创建、查询和删除。`POST .../{id}/resolve` 接受 `{protocol, port?}`；HTTP/WS 数据入口为 `/agent/v2/{name}/{version}/{environment}/{protocol}/{port}/...`。共享转发使用返回的真实 Sandbox ID。

Template 的 service 声明 HTTP/WS/SSH 协议及端口，例如 `[{"protocol":"http","port":8080},{"protocol":"ws","port":8080},{"protocol":"ssh","port":22}]`。用户直接调用 Harness 自己定义的接口；SSH 先 resolve 获取真实 Sandbox ID，再使用共享 SSH/CONNECT 入口。审计轨迹能力暂缓，当前只提供基础运行与访问链路。

预装镜像、RRT 与匹配的启动 profile 仍可用于验证。当前 Sandbox 适配不能证明从未观察到的创建已被取消；这类删除返回结果未知并保留产品记录。ADX 不通过删除元数据掩盖平台副作用。pause/resume 完全由 Platform 负责，不属于 ADX 适配范围。首版按 Running 视为服务已就绪；平台当前只保证资源和 runtime IP，RRT/业务端口就绪保证记为 Platform 能力缺口，ADX 不补探测。

## 验证

运行 `make agent-test`。真实 Redis 测试将 `ADX_AGENT_TEST_REDIS_URL` 指向一次性数据库并显式使用 `--ignored`，测试会留下独立命名空间记录。组件测试不等同于真实 Platform 运行与故障恢复验收；本轮不修改 Platform 源码。

容器内真实 Platform 验证已覆盖双 Gateway、双 Activator 的 HTTP/WS/SSE/SSH 转发、稳定身份、进程替换与跨 Gateway 删除。创建链路仍存在平台限制：部分 RRT 启动时业务 HTTP 已响应，但控制接口超时，Platform 报告 `capsule start timed out`，创建返回结果未知或最终失败。该问题也影响 inline 创建和同名 Environment 重建；当前不能视为完整端到端验收通过，调用方应回查原身份，不因超时换新 ID 重复创建。
