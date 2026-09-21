# Agent 产品状态存储

`AgentState` 保存不可变 Template 和 Environment 元数据；`Repository` 提供原子条件持久化。生产使用 Redis，`test-memory` 仅供显式启用的测试，不作为故障降级存储。

Environment scope 包含 tenant、template、version、environment_id。首次条件写提交 generation 和稳定 sandbox_id，并发创建返回胜出的同一记录。产品 Active/Deleting 不代表 Sandbox 运行状态。删除在平台确认后按 revision 条件清理，旧 generation 的清理不能影响同名新环境。

每条记录使用 UUID revision。事务先检查全部条件再修改；不持久化 Sandbox 运行状态、操作流水或扫描索引。

schema marker 为 `environment-v1`，初始化 `SET NX` 后读取确认，不扫描命名空间，不覆盖冲突 marker。使用新的部署 namespace，不提供旧业务模型在线迁移。

`OutcomeUnknown` 可能已经提交。调用方读取原身份，不能换一个 Sandbox ID 绕过未知结果。Redis 断线后允许重新连接，不能自动重放结果未知的写入。

## Redis 配置

- 独立 `adx:v2:<namespace>:` 命名空间，不访问 Platform 私有键。
- 单一权威写入端点，不承诺 Redis Cluster 跨 slot 事务。
- 持久化和故障切换配置决定确认写入的可靠性，Lua 原子性不等于异步复制零丢失。
- 元数据使用 GET、SET、EVAL、DEL。本 crate 不修改 Redis 服务配置。
- 单事务最多 128 个条件、CAS 最多 32 次，具体上限见 core/limits.rs。

```sh
make agent-test
ADX_AGENT_TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
  cargo test --locked -p adx-agent-store --tests -- --ignored --nocapture --test-threads=1
```

真实测试仅使用一次性数据库；连接恢复用例会关闭该数据库的普通连接。测试使用独立 namespace，但会留下记录。
