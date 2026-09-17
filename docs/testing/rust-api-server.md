# Rust API Server 与调度 Shard 迁移

2026-09-17。基于 `153f839` 实施，独立工作树 `api-server-rust`，分支 `refactor/api-server-rust`。本次重写已通过 [Buildkite #24](2026-09-17-rust-api-server-k8s.md) 正式基础K8s验收。

## 契约

- Frontend → API Server，生产二进制 `adx-api-server`，部署角色与受信组件身份 `api-server`。
- Domain → Scheduling Shard，代码 `ShardScheduler`，内部字段 `shard_id`，Master 配置 `scheduler_shards`。Global 轮转 → Shard Filter/Score → Node Manager 准入的分层保持。
- 调度指标标签改为 `shard_id`，消费者应同步更新。TLS/DNS domain 与拓扑故障域的名称保持其原本含义。
- 读取旧 Redis/SQLite 记录时允许旧调度字段别名；新写入使用 Shard 字段。升级需统一更新组件配置和证书角色，不运行混合版本组件。
- 公共 Sandbox HTTP 路径、响应包装、SSE、请求 ID、租户身份、归属缓存和操作去重保持；内部直接调用 Instance RPC。
- Agent 路由仍为可选业务服务转发，不将旧 FaaS 执行链带入 API Server。

## 部署迁移

统一更新发布包和部署配置：HTTP 服务角色改为 `api-server`，启动 `adx-api-server`；使用 [API Server 配置](../../build/config/examples/api-server.json) 和 [统一部署示例](../../build/config/examples/deployment.json)。内部 mTLS 受信身份映射同步改为 `api-server`，加载部署环境提供的对应证书，重启组件生效。

Master 使用 `scheduler_shards` 配置逻辑分片数量，节点归属仍由 Master 自动分配。指标查询使用 `shard_id` 标签。旧持久化字段的读取兼容用于数据迁移，不代表旧组件和新组件可以混用。公开 Sandbox URL 与 Python SDK 调用方式保持；客户端不需要感知内部 Shard。

## 测试驱动记录

首批4项契约测试先对空实现失败，再通过直接 Instance 转换实现：验证租户来源、CPU/内存/磁盘单位、快照省略规格继承、显式拒绝不支持选项、实例亲和 OR 分组。Rust API Server、Master 和 CLI 已编译。新增超时校验、缓存过期和 SSE/重放测试。

真实 Redis/mTLS/HTTPS 验证首轮发现测试配置未迁移与 HTTP 异步状态机栈溢出；保留失败日志并修复。修复后的完整服务、SDK、发布包与基础Kubernetes七组验收已通过。

## 复验入口

```bash
cargo test --locked -p adx-api-server -j 2
ADX_TEST_API_SERVER=/absolute/path/adx-api-server \
ADX_TEST_REDIS_SERVER=/absolute/path/redis-server \
python3 build/ci/run.py api-control --jobs 2 --output out/ci/api-control-new
```

本地组件验证和正式 Kubernetes 验收分别记录；本次正式通过的代码提交为 `385b698043f0b62fac943a43da3de17bf59f6441`。

本轮已通过工作区 370 项测试（29 项环境或性能用例默认忽略）、58 项 E2E 驱动测试、7 项 CI 驱动测试，以及 11 项实际执行的 Redis/mTLS RPC 测试和生产 API 二进制 HTTPS 契约。暂停/恢复重试、快照 CRUD、租户隔离、克隆和 9 条 Agent 流式转发路由均已通过；严格 Clippy 通过；另实际执行 14 项 Redis 存储回归，全部通过。提交 `5dea82e` 的 [Buildkite #22](https://buildkite.com/agent-dx/agent-dx/builds/22) 已通过 Rust release 编译、SDK 打包和统一包校验；外部 runc 下载连续超时后主动取消，未执行 K8s。后续使用既有 `ADX_BACKEND_ARTIFACT_BUILD`（#21 的构建 UUID） 复用同一 sandboxd 提交与架构的外部后端制品；流水线逐文件校验摘要，ADX 产品仍从本次提交构建。

证据目录为 `out/ci/api-server/`：`contract-red.log` / `contract-green.log`、`workspace-final.log`、`clippy-final.log`、`storage-final/result.json`、`rpc-6/result.json`、`rpc-6/api-http.log`、`e2e-driver.log`、`ci-driver.log`。RPC 夹具使用真实服务与 Redis/mTLS，RuntimeBackend 和就绪/路由检查为受控实现，因此不替代真实 sandboxd/RRT 的 K8s 验收。

#23 的产品构建通过；后端复用参数误用了页面编号21，artifact CLI要求构建UUID，因此下载被拒绝且未执行K8s。触发参数随后修正为#21的UUID，不改变后端版本或产品代码。

额外兼容回归：含空格、加号与百分号的Instance名称，创建后经转义路径删除的用例先失败；补齐单次路径解码后，11项RPC和全部HTTPS用例再次通过（`escaped-red/`、`escaped-green/`），严格Clippy通过（`clippy-path.log`）。
