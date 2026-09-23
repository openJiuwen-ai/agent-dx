# API Server 与 Ingress 进程模式

API Server 和 Ingress 保持独立模块、监听与 TLS 身份。`gateway::ingress::IngressService` 统一负责 Ingress 的监听、Coordinator 路由订阅、认证缓存、连接池和排空；`adx-apiserver` 与 `adx-ingress` 只是两种托管方式。

## 共进程（默认）

同一部署包含 `apiserver` 和 `ingress` 两个逻辑角色，API Server 未设置 `ingress_mode` 或设置为 `embedded` 时，`adxctl render` 只生成一个 `adx-apiserver` 进程。它把 Ingress 控制配置作为 `ingress_control` 写入 API Server 私有配置，并把 Ingress 的 `ADX_DATA_PLANE_*` 环境合并给该进程。

API Server 仍监听回环管理端口，Ingress 仍监听对外 TLS、可选明文及健康端口。两者共享 Tokio runtime 和进程重启预算；Ingress 监听绑定失败会阻止 API Server 启动，运行中的内嵌 Ingress 退出会使 API Server 退出并交给 supervisor 整体重启。Ingress 使用自己的证书连接 Coordinator 和 Relay，API Server 继续使用 API Server 证书执行管理 RPC。

## 分进程（显式选择）

在 `apiserver.config` 中设置：

```yaml
ingress_mode: standalone
```

`adxctl render` 随后生成 `adx-apiserver` 和 `adx-ingress` 两个进程。Ingress 的控制配置及环境仍来自 `role: ingress`，适用于需要独立故障域、资源限制或日志进程边界的部署。控制请求继续通过配置的回环地址转发给 API Server，两种模式使用相同路由缓存和数据转发实现。

默认共进程的 supervisor 日志归入 API Server 服务；Ingress 的 `adx_access`、`adx_audit` 和普通 tracing 事件仍写入该进程 stdout，由统一日志滚动模块采集。显式分进程时，Ingress 拥有单独的 supervisor 日志。

## 验证边界

部署测试验证默认只渲染一个进程、Ingress 环境和控制配置完整注入，并验证 `standalone` 明确恢复两个进程。API Server 配置测试拒绝缺少 `ingress_control` 的内嵌模式及携带该字段的分进程模式。完整验收还应从公开 SDK 经 Ingress 创建 Environment、访问 Execd 并删除，且核对 supervisor 状态中不存在独立 Ingress 进程。

2026-09-21 本地验证覆盖：部署配置 24 项、API Server 30 项、Gateway 107 项、Coordinator/adxlet 169 项，以及相关 crate 的严格 Clippy。另用真实 `adx-apiserver` 进程加载内嵌配置，确认同一 PID 同时提供 API 回环监听和 Ingress 健康监听，并经 SIGTERM 完成排空退出。该进程验收故意不启动 Coordinator、sandboxd 或 Execd，因此只证明装配、监听与关闭契约；不能代替公开 SDK 创建—访问—删除 E2E。
