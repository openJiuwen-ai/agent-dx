# Agent 当前状态存储

`AgentState` 定义产品事务，`Repository` 提供原子条件持久化。生产实现使用 Redis；`test-memory` 是显式启用的测试功能，不作为故障降级存储。

每条记录使用不透明的 UUID revision。事务对每次写入和条件删除做版本检查。绑定事务原子检查当前 Session generation、候选 Instance 和绑定 revision；重新绑定还必须确认原 Instance 已 Deleted。冷启动原子检查实例池为空，同时写入 Session 成员和 Creating Instance，并发请求复用已有池。实例结束后保留终态记录，并从对应 generation 的 Session 中移除成员。Session 删除会条件清理关联绑定及空的 Deleting Session，允许复用公开 ID，同时拒绝旧 worker 的迟到写入。

当前命名空间 schema marker 为 `session-affinity`，初始化通过 `SET NX` 后 `GET` 确认；并发初始化可以收敛，不覆盖冲突 marker。项目没有已发布的旧状态 schema，启动时不扫描命名空间或执行旧数据迁移。Instance 保存 `create_deadline_ms` 和 `scope/session_generation`，持久化记录继续使用严格解码。

`OutcomeUnknown` 不代表明确拒绝。调用方应查询原记录，不能换一个 Instance ID 或假设写入失败。相同 ID 的预留重试返回已有预留，直接使用事务接口的调用方可比较预期 revision。业务编排在重试及进程替换期间必须保持这些身份不变。

## Redis 配置

- 使用一个权威写入端点，当前不承诺 Redis Cluster 跨 slot 事务。
- 使用独立的 `adx:v2:<namespace>:` 命名空间，校验 namespace，不访问 Platform 私有键。
- 按确认写入的可靠性要求配置持久化和故障转移。Lua 原子性不代表异步副本切换不会丢失数据。
- ACL 允许对应命名空间的 `GET`、`SET`、`MGET`、`SCAN`、`EVAL`、`PEXPIRE` 和 `DEL`。本 crate 不修改 Redis 服务端配置。
- 扫描调用方须容忍空页、重复项及状态变化，仅在游标为零时结束；不持久化扫描索引或操作队列。
- Dispatcher 成员参数由进程配置控制。注册值包含 boot UUID，续租和注销比较完整注册值；Gateway 定期刷新有 TTL 的成员信息，并在路由失败后刷新。成员发现不使用 Pub/Sub 或 PUBLISH。

## 验证

```sh
make agent-test
ADX_AGENT_TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
  cargo test --locked -p adx-agent-store --tests -- --ignored --nocapture
```

真实 Redis 测试应使用一次性数据库。测试使用独立 namespace，但会留下记录。生产连接恢复与准入预算已接入；Dispatcher 仅维护自身成员租约，Gateway 缓存发现结果并主动刷新。此 crate 本身不是 Dispatcher 进程。

## 存储限制

限制统一定义在 [Agent core](../core/src/limits.rs)：单事务最多 128 个条件，CAS 最多尝试 32 次；Redis 客户端默认并发 64，配置上限 4,096。SCAN 默认 COUNT 为 100、上限 1,000，只是扫描提示，不保证返回条数。
