# Master 发现、节点心跳与重启对账

2026-09-15。实现范围是创建／删除状态子集的服务发现、节点存活管理和本机控制器恢复。Instance 状态机仍属于 Node Manager。

## 模块与调用链

| 模块 | 职责 |
|---|---|
| `platform/crates/discovery` | Rust Redis 客户端，读取并校验 Master 地址与 epoch |
| `master/storage.rs` | 发布带 TTL 的 Master 地址；持久化节点进程会话、心跳序号、路由可见性 |
| `master/rpc.rs` | 心跳时效、注册／对账握手、完整节点目录、过期节点关闭调度、拒绝旧进程提交 |
| `node-manager/reconciliation.rs` | 校验权威目录与运行时清单，恢复资源账本／控制器，清理确认未归属的运行时 |
| `node-manager/controller.rs` | 串行恢复运行实例的本机绑定；清理缺失或未提交的启动并提交失败结果 |
| `sandbox-api/internal/discovery` | Go Redis 发现与连接级 gRPC resolver；Master／认证客户端共用连接 |
| 三个服务进程入口 | 配置加载、续期／心跳循环、连接替换、重启对账装配 |

```text
Master 启动 → Redis 建立新 epoch → 恢复资源占用、节点置为不可达
            → 监听 RPC → 发布并续期 Master 地址

Node 启动 → Redis 发现 Master → 注册 session_id（对账中、关闭新分配）
          → InspectNode 获取本节点完整目录
          → RuntimeBackend.inventory 获取实际运行时
          → 恢复控制器／占用／本机绑定，提交清理结果
          → 后续心跳开放准入
```

配置示例在 `build/config/examples/`。Master 的 `advertised_address` 为带明确端口的 HTTPS 地址，`discovery_ttl_seconds` 默认 15 秒。Node 与 API Server 的 `discovery` 指定 Redis URL 和 namespace；也保留显式 `master_address` 配置，两者互斥。Node 按 `report_interval_seconds` 查询发现并上报；API Server 按 `discovery.poll_seconds` 刷新。mTLS 仍使用部署环境提供的 CA／证书和组件身份映射。

Redis 地址记录位于 `adx:{namespace}:master:v1`，包含 schema、epoch、address。读取时同时读取 `control:v1` 的 header，精确比较 u64 epoch；旧 Master 即使还留下未过期地址，也不再是有效发现结果。发布使用当前 Session 的 CAS，旧 epoch 不能覆盖新进程地址。这不提供选主或主备切换。

发现或实例目录流暂时失败不清空 API Server 最近完成同步的 Instance 目录。已有节点操作仍可按缓存的 generation 直达 Node 并由节点复核；新 Master 地址出现后 resolver 重连，首帧全量替换旧目录。revision 断档或非法 epoch 增量会清空目录并等待新的全量帧。认证缓存继续受其 TTL 和密钥到期时间限制。

## 心跳契约

节点每次启动生成 `session_id`，每次报告递增 `heartbeat_sequence`。相同序号且相同请求可重试，但不会延长存活期限；倒退序号或同序号不同内容被拒绝。正常进程重启可在心跳期限内接续：Master 通过 mTLS 向原注册地址调用 `NodeService.GetSession`，确认该地址已由新会话服务，才接受新会话并要求对账；旧会话立即被拒收。地址改变或复核不通过时不能提前接续。

Master 使用单调时钟判断心跳是否过期，`heartbeat_timeout_seconds` 默认 30 秒。过期后在同一 Redis 操作中关闭节点调度/路由，将其未删除的旧执行标记为失效 Failed 并释放逻辑资源占用；保留执行身份与 checkpoint 元数据供对账及后续恢复使用。节点仍不可调度，容量不会因此重新投入分配。维护开关／容量不足与节点是否可路由分别存储，因此停止新调度不等同于下线已有实例。本阶段最初验证的是 Master 路由视图；后续的 [路由发布阶段](route-publication.md) 已接入 Edge 全量／增量订阅。

节点回来后重新进入对账握手；Master 重启后同样要求重新对账。恢复的节点从 Master 加载目录完成时起有一个心跳超时周期的报到期限，期间路由关闭；逾期未报到的旧执行失效，迟到注册和结果提交也先检查期限。较长的对账期间继续发送关闭准入的心跳。CommitInstance 校验节点证书、当前进程 session、完整 Assignment、Spec 和 revision。Master 派发 Create 也携带目标 Node session，节点拒绝发送给旧进程身份的迟到请求。

失效标记在 Redis/Master 重启后继续有效。迟到的 Running、Paused 或自动重启结果不能恢复旧执行资格。原 Node Manager 恢复连接后先清理旧控制器与实际运行时，再开放准入。有效共享 checkpoint 的跨节点恢复和归属转移已实现，并通过本地 FC 验收，详见 [节点失效契约](node-failure-takeover.md)。

## 本机恢复规则

没有 Master 完整目录或无法读取实际运行时清单时，保持生命周期入口关闭；查询错误不能转换成空目录。启动时不依赖 SQLite 中的完整实例目录，SQLite 只在提交不可用时作为降级日志，不能覆盖权威失效记录。若 Master 的预留／注册持久化结果不确定而进入恢复保护，仍需重启 Master，从 Redis 重建调度内存；地址重连不清除这类一致性保护。

| 权威记录与实际状态 | 处理 |
|---|---|
| 已提交 Running，运行时身份匹配且运行中 | 从持久化记录恢复 IP／资源和整卡占用，复核 RRT，就绪后重新绑定；不执行 Start |
| 已提交 Running，但实际运行时缺失／停止 | 退役绑定、确认清理、释放占用，递增 revision 提交 Failed |
| 只有已提交分配，没有完成状态 | 清理可能残留的启动，提交 Failed；不从镜像重建 |
| Failed 但仍持有资源 | 重试清理；确认删除后释放资源并提交结果 |
| Deleted 或已释放资源的 Failed | 保持终态，清理该执行身份可能存在的残留 |
| 完整目录确认不存在的 managed runtime／本地控制器 | 串行排空操作，退役绑定并删除运行时，释放本机占用；关闭旧控制器、拒绝旧代次重建，不补造集群记录 |
| 目录格式／身份／设备分配冲突 | 整体拒绝，在任何清理前报错 |

恢复的资源占用允许超过新的容量上限；已经消失／不健康的整卡仍保留占用。删除或路由退役失败不会提前释放资源。对账失败保持入口关闭，可重复执行；已经建立的串行控制器保留未完成提交的结果。

运行时清单只包含带 ADX 管理标签且可验证执行身份的实例。不会按不完整身份猜测其他运行时的归属。RRT 和本机绑定恢复失败时保留实例与资源，等待依赖恢复。

## 验证入口与范围

```sh
cargo test --locked -p adx-node-manager -p adx-master -p adx-discovery -p adx-protocol -j2
cargo clippy --locked -p adx-node-manager -p adx-master -p adx-discovery -p adx-protocol --all-targets -j2 -- -D warnings
ADX_TEST_REDIS_SERVER=/path/to/redis-server python3 build/ci/run.py storage --jobs 2
ADX_TEST_REDIS_SERVER=/path/to/redis-server python3 build/ci/run.py control-rpc --jobs 2
ADX_TEST_REDIS_SERVER=/path/to/redis-server ADX_TEST_API_SERVER=/path/to/adx-api-server python3 build/ci/run.py api-control --jobs 2
```

本地红灯、回归、真实 Redis、mTLS RPC 和 Go 进程验证证据保存于 `out/ci/recovery-discovery/`。红灯首先确认 RuntimeObservation／inventory／reconcile 接口缺失。真实 Redis 检查发现过期与 epoch 切换，RPC 检查心跳重复／过期／重新对账／旧 session 拒绝，Node 测试检查不重新启动、资源恢复、未提交实例清理以及失败时保留占用。

这些是组件与服务协作验证：运行时、RRT 就绪与本机路由部分使用测试后端。后续 [路由发布阶段](route-publication.md) 已接入 Edge 订阅与 Node Proxy 完整启动同步。真实 sandboxd、统一 supervisor 和两节点 SDK 链路已通过 [Buildkite #21](2026-09-17-observability-k8s.md)；本段所列早期组件结果仍不等同于该 E2E。


## 本轮结果

验证对象为 `refactor/monorepo-layout` 分支基准 `1e49d86f2123173a8f5358182ca294fab9a9b1e4` 上的未提交工作树。Rust／Redis／服务 RPC 在 macOS ARM64 执行；Go 在 Linux ARM64 工具链容器内测试，并构建本机 ARM64 服务程序。

| 检查 | 结果 | `out/ci/recovery-discovery/` 下的证据 |
|---|---|---|
| 四个变更 Rust 包及测试目标 | 92 通过，11 个专用入口测试默认忽略 | `rust-v5.log` |
| 严格 Clippy | 通过 | `clippy-v5.log` |
| Go 全包测试、vet、服务构建 | 206 项测试通过；vet／构建通过 | `go-v5.log` |
| 真实 Redis 存储与发现 | 7 通过，0 忽略 | `storage-v5/result.json` |
| 真实 mTLS RPC、节点恢复及 Master 进程重启 | 3 通过，0 忽略 | `rpc-v5/result.json` |
| Go HTTP 进程使用 Redis 发现，与 Rust RPC／Redis 协作 | 同一 RPC 套件的 3 个用例再次通过，0 忽略 | `frontend-v5/result.json`、`frontend-http.log` |

默认忽略项不计为通过；Redis／RPC 已由上面的专用入口实际执行，性能基准本阶段未重跑。`red.log` 与中间告警日志保留；`source-manifest.json` 记录验证源码及服务二进制 SHA256。
