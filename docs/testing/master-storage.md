# Master Redis 持久化与恢复

2026-09-14。本阶段实现 Master 存储库、调度内核恢复入口和真实 Redis 集成测试。生命周期状态决策仍在 Node Manager；Master/Node RPC 服务、Node Manager 的 StateSink 接线及 Edge 路由订阅尚未完成。

## 模块与边界

| 模块 | 职责 |
|---|---|
| `master/src/storage.rs`：`RedisStore` | Redis 连接、超时、断线后重新连接、原子条件提交 |
| 同上：`Session` | Master 启动代次、节点自动分域、分配持久化、节点结果核验与提交 |
| 同上：`StoredSnapshot` | 一致读取持久化目录，生成已提交的 Running 路由视图 |
| `Master::restore/restore_with_config` | 恢复节点归属、代次下限、标量和设备占用；保留调度配置入口 |
| `ResourceLedger::restore` / `DeviceLedger::restore` | 恢复既有占用，允许当前容量不足或设备缺失；拒绝重复卡归属 |
| `build/ci/run.py storage` | 启动独立 Redis 进程、执行集成用例、保存版本/二进制摘要和诊断日志 |

普通调度算法仍只处理内存状态，不在每次 Filter/Score 中访问 Redis。异步服务接线时，由服务层按下面的提交顺序协调持久化和派发。

## Redis 数据布局

每个部署使用独立命名空间，当前单实例 Redis 中的一个 Hash：`adx:{namespace}:control:v1`。

| 字段 | 内容 |
|---|---|
| `header` | schema、域数量、Master epoch、generation 下限、全局发布 revision、节点分域轮转位置 |
| `node:<node_id>` | 节点资源和设备、固定 domain_id、Node Manager 地址、Node Proxy 地址 |
| `instance:<instance_id>` | 原始 InstanceSpec、精确 Assignment、最后一次已提交的节点结果 |

节点更新和实例提交使用 HMGET 读取头部与目标字段，再通过比较旧头部的 Lua 脚本提交。只有首次节点注册需要统计各域节点数；全量 HGETALL 用于启动恢复和完整目录读取，不用于每次实例结果提交。

脚本不解析 JSON、不把 u64 转为 Lua 浮点数。验证和计数器递增在 Rust 中进行，脚本只比较字节，最终用一条 HSET 同时写入记录和新头部。Redis 保证脚本执行期间不被其他命令插入；使用单条写命令也避免脚本先写部分数据、随后报错的路径。[Redis 脚本语义](https://redis.io/docs/latest/develop/programmability/eval-intro/)

实例和发布版本不存在两份独立提交的状态：路由视图从同一次全量读取中的节点与已提交 Running 结果生成。revision 是集群目录发布游标，允许包含不改变路由的更新；不是 Node Manager 的实例 revision。当前没有持久化增量事件流，gRPC 全量／增量发布将在服务层实现。

这个布局面向已选的单 Master、单 Redis。全局头部 CAS 会串行化持久化写入，有限重试 32 次后返回暂不可用；此阶段没有测量其高并发吞吐。数据规模增大时，需要单独测量启动全量读取与提交争用，不能沿用调度内核基准作为存储性能证据。

## 写入与恢复契约

1. **Master 启动**：`RedisStore::begin(domain_count)` 增加持久化 epoch，返回 Session；加载目录并通过 `Master::restore` 重建调度状态。缺少头部、未知 schema、域数量不兼容、非法归属或重复卡占用会阻止恢复。不会把损坏的数据目录重新初始化为空集群。
2. **节点注册**：首次按节点数量最少的域分配，并列时轮转；已有节点保留 domain_id。节点地址与资源更新也通过条件提交。恢复到内存后的所有节点暂时关闭新调度，重新注册后才参与选点。
3. **首次分配**：调度内核产生 Assignment；`Session::reserve` 持久化后才能向节点派发。请求仍在等待资源时不写 Redis，重启后由客户端重试。不能把仅有的内存分配当作可执行的持久化授权。
4. **节点结果**：Node Manager 完成本机操作后提交结果，Master 核验 InstanceSpec、完整 Assignment、实例 revision 和占用一致性，再调用 `Session::commit`。普通生命周期操作没有新增一轮 Master 操作意图登记。Running 才进入路由视图；Failed/Deleted 移除路由。
5. **释放资源**：Failed 不等于资源可用；`resources_held=true` 时恢复占用。服务层必须先确认节点清理、提交终态，再释放调度账本；不能仅以 State=Failed 释放。
6. **节点明确拒绝分配**：确认未执行或已清理后，可以通过 `replace_rejected` 条件替换精确旧分配，新的 generation 必须更高。Running、Deleted 或仍占用资源的 Failed 不能走这个入口。旧分配之后的迟到结果会被拒绝；这不是运行中实例的跨节点接管协议。

`reserve/commit/replace_rejected` 支持相同内容的重复调用。同一实例 revision 携带不同内容、旧 revision、旧 assignment 或已删除实例的新运行结果均返回冲突。重试不能把删除状态倒退为 Running。Deleted 及已释放的 Failed 记录暂时保留为终态记录，恢复后的调度内核不允许把同一 ID 当成新创建；终态记录回收尚未实现。

每次 Master 真正启动才调用 begin；Redis 连接恢复不增加 epoch。旧 Session 的写入和全量读取会被拒绝。这个机制用于排除旧 Master 写者，不提供主备选举，也不证明失联节点上的旧执行已经停止。

## 故障与持久性

| 情况 | 行为 |
|---|---|
| Redis 不可用／超时 | 返回 Unavailable，清除失效连接；后续调用重新连接，不在传输层盲目重放写入 |
| 写入已应用但响应丢失 | 调用方以相同实例身份和结果重试；通过读取现值与条件提交判定结果 |
| Master 重启 | 保留原 Assignment/generation 和资源占用，新分配从持久化 generation 下限继续 |
| 资源容量缩小／卡暂时消失 | 保留原占用；可分配资源不足时继续拒绝新请求，不擦除历史占用 |
| 未分配的内存等待队列 | 不恢复，由客户端重试 |
| 本地降级日志 | 本阶段未实现 SQLite；不得把返回 Unavailable 描述成已经 Journaled |

客户端提交成功的持久性遵从 Redis 部署的 AOF 配置。库不执行 CONFIG SET，也不擅自改变已选的可配置刷盘策略。集成测试使用 `appendonly yes`、`appendfsync always` 验证进程 SIGKILL 后恢复；这不证明 everysec/no 配置下断电零丢失，也不覆盖 Redis 数据盘丢失或恢复旧备份后的代次回退。[Redis 持久化说明](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)

## 本地验证入口

```sh
export ADX_TEST_REDIS_SERVER=/absolute/path/to/redis-server
export CARGO_TARGET_DIR=/your/cache/cargo-target
python3 build/ci/run.py storage --jobs 2
cargo test --locked -p adx-master -p adx-core -p adx-scheduling -j2
cargo clippy --locked -p adx-master -p adx-core -p adx-scheduling --all-targets -j2 -- -D warnings
```

测试依赖一个真实 redis-server 可执行文件；没有可执行文件时失败，不回退为模拟服务或跳过用例。每个集成用例创建独立临时目录和 Unix Socket，不连接部署中的 Redis，不使用 FLUSHALL。子进程由测试退出清理；运行器超时会终止本轮进程组。

实际测试环境：macOS ARM64，官方 Redis 7.2.5 源码独立编译，Rust 1.97.1。本次 Redis 是测试依赖，没有据此确定统一产品发布包的 Redis 版本。源码包与二进制摘要见 [provenance.json](../../out/ci/master-storage/provenance.json)。

覆盖：

- 两域节点归属保持、重启后占用和 generation 恢复、等待队列不恢复、删除 ID 不复活。
- 完全相同的并发提交幂等；租户/Assignment/revision 不匹配时不改变记录或路由。
- 大于 2^53 的 generation 不丢精度；计数器到达上限时停止分配，不回绕。
- AOF 写入后 SIGKILL Redis、重新启动，复用原 Session 重连并读取相同结果。
- 缺失 header 和未知 schema 不触发空库初始化；原节点数据保持。
- 拒绝后的分配替换、旧结果拒绝、运行中结果不可被该接口接管。
- 容量缩小、设备暂时缺失与恢复后的占用保持，以及重复物理卡归属导致恢复失败。

证据保存在 `out/ci/master-storage/`：`red.log` 是接口未实现时的红灯；`regression-final.log`、`clippy-final.log`、`integration-final/result.json` 和各测试 Redis 日志是最终验证入口。完整平台服务和 Buildkite 创建—执行—删除 E2E 仍是下一条接线任务。

最终结果：55 项普通回归通过（另有 6 项显式用例未在普通测试中执行），5 项真实 Redis 集成单独执行且全部通过、无忽略；严格 Clippy、定向格式检查和 7 项 CI harness 测试通过。
