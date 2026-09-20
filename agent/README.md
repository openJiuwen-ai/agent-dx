# Agent-DX v2

Agent 层使用 Rust 实现，API 嵌入统一 Gateway，Dispatcher 独立部署。

| 目录 | 职责 |
|---|---|
| `crates/core` | 与平台无关的产品类型、校验和接口契约 |
| `crates/store` | 当前状态存储、Redis 条件事务；内存实现仅供显式启用的测试使用 |
| `dispatcher` | 受管实例选址、内部 HTTP 服务和创建/删除协调，见 [Dispatcher 说明](dispatcher/README.md) |
| `api` | 内嵌 inline/managed API、Dispatcher 发现和调用、受管服务选择 |

Session 是完整上下文索引，并独占实例池。可选 `affinity_key` 将请求绑定到同一逻辑 Instance；用户进程重启不使亲和失效，Session 删除或逻辑实例确认结束后失效。Session 清理完成后允许复用公开 ID，内部 generation 隔离旧请求。Resolve 的 `bypass_cache` 跳过缓存读取权威状态。

inline create/get/kill 直接适配 Sandbox，不使用 ADX Redis、Dispatcher 或后台恢复任务。共享 HTTP/WS/SSH 转发使用真实 Sandbox ID。旧 Python CLI、SDK、Executor 及其测试已删除；SDK、CLI 和 EventLog 后续补充，执行能力由 Platform RRT 提供。

## 部署

复用既有 [Platform 独立进程部署](../docs/deployment/standalone.md)，不要求 Kubernetes。构建统一 Gateway 和独立 Dispatcher：

```sh
cargo build --locked -p data-plane-gateway --features agent-api --bins
cargo build --locked -p adx-dispatcher --bin adx-dispatcher
```

Gateway 保留既有 TLS、租户认证和转发配置，新增 `ADX_AGENT_CONFIG` 与 `ADX_SANDBOX_CONFIG`。`inline_only` 模式不初始化 ADX Redis 或 Dispatcher；`both` 模式启用受管接口。Platform 自身的发现和存储依赖仍保留。

Gateway 与 Dispatcher 共用独立的 Agent Redis namespace，与 Platform namespace 隔离。Redis 保存 Template、Session、Instance、AffinityBinding 和 Dispatcher 成员当前状态。Redis 不可用时已有暖缓存可能继续命中，但状态写入和强制刷新失败，不切换到独立内存存储。事务与可靠性约束见 [存储说明](crates/store/README.md)。

每个 Dispatcher 使用独立 `node_id` 和 `ADX_DISPATCHER_CONFIG`，启动、替换及内部传输配置见 [Dispatcher 说明](dispatcher/README.md)。Gateway 副本须共享 profiles、Agent namespace 和服务凭证。增加 Gateway/Dispatcher 副本不会自动增加用户实例；本期不启用自动弹性。

当前以预装镜像提供 RRT、解释器、用户脚本及服务依赖，RRT 拉起显式入口。镜像启动配置必须匹配 Sandbox profile；动态注入、迟到创建取消及 RRT 启动锁竞争仍是平台限制。首次 Ready 后由 Substrate 保证健康；直接从 Substrate 删除实例不会自动同步 ADX 状态，应通过 ADX 实例/Session 接口释放。

## 验证

运行 `make agent-test` 验证 Agent 和 Gateway 组件。真实 Redis 测试需将 `ADX_AGENT_TEST_REDIS_URL` 指向一次性数据库，并显式使用 `--ignored`；各测试使用独立命名空间，详见 [存储保证](crates/store/README.md)。组件测试与真实平台端到端验收分开统计，Platform 源码不在本次修改范围。
