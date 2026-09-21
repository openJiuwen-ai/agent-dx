# Master / Node RPC 与 StateSink

2026-09-15。本阶段把 Master 调度、Redis 存储和 Node Manager 生命周期模块接到真实 gRPC 服务，实现 `MasterStateSink`。测试使用真实 TLS Socket 和 Redis；运行时、就绪探测、本机路由使用可控测试实现，尚未启动真实 sandboxd 或完整平台。

## 模块划分

| 模块 | 当前实现 |
|---|---|
| `platform/master/src/rpc.rs` | 注册、创建、查询、结果提交；协调内存调度与 Redis，持久化分配后才派发 |
| `platform/node-manager/src/rpc.rs` | NodeService 适配、API Server 直达删除、MasterStateSink 客户端 |
| `platform/crates/protocol/src/auth.rs` | 从 TLS 对端证书识别 Master、API Server、具体 Node；校验可信 API Server 的租户上下文 |
| `platform/crates/protocol/src/lib.rs` | CapsuleRecord 编解码校验、错误映射；保留公共协议边界 |
| `platform/api/proto/capsule.proto` | 增加可信 CallerContext、节点代理地址、查询结果中的 Node Manager 地址 |
| `build/ci/run.py control-rpc` | 生成独立测试证书、运行真实 Redis／TLS 协作测试、保存证据 |

服务进程、部署配置、启动时证书加载和 supervisor 已装配；证书热重载后置。用户身份由 Master API Key 模块管理。下文测试结果保留早期组件验证边界，完整系统结果见 [Buildkite #21](2026-09-17-observability-k8s.md)。

## 调用顺序

```text
可信 API Server → Master.CreateCapsule
  → Global 选择 Shard，Shard 排队／Filter／Score／预留
  → Redis 保存精确 Assignment
  → Master → Node.CreateCapsule
       → Node Manager 本机准入、sandboxd、就绪、本机绑定
       → StateSink → Master.CommitCapsule → Redis
  ← Running + Published

可信 API Server → Master.GetCapsule
  ← 最后提交的实例结果 + Node Manager 地址
可信 API Server → 对应 Node.DeleteCapsule
  → 本机退役路由、确认运行时删除、释放本机资源
  → StateSink → Master.CommitCapsule → Redis
  → Master 释放调度预留并唤醒等待请求
```

Master 不运行实例生命周期状态机。NodeService 调用现有 CapsuleHandle 串行任务；相同分配的重复创建共用同一个控制器。Master 的创建请求在内存队列等待资源，实际分配前不写 Redis。

API Server 超时或断开不会取消已接收的创建任务；资源释放后任务继续执行。相同 ID 和相同 Spec 的重试复用原分配；相同 ID 但不同 Spec 返回冲突。进程重启仍不恢复未分配等待队列，客户端需按原 ID 重试。

Master 不持锁等待 Node RPC，因此节点回调 CommitCapsule 不会与创建请求互相等待。Redis 分配提交失败或节点注册提交结果不明确时，新调度进入需要恢复的状态，不能把未确认持久化的内存分配继续下发；服务应重新读取权威目录并恢复后再开放调度。已有节点的结果提交和查询仍走存储校验。

Node 的删除入口只接受本机已经管理的精确 Assignment；不会因为一次删除请求缺少本机记录而创建新控制器。Node Manager 自身重启后的权威对账和控制器恢复已接通，见 [发现与恢复契约](recovery-discovery.md)。

## StateSink 的成功条件

`MasterStateSink` 使用带节点客户端证书的 Channel 调用 CommitCapsule，校验响应包含完全相同的 CapsuleRecord，才返回 Published。超时／断线返回 Unavailable，版本或身份冲突返回 Conflict；不把未提交结果当作成功。

删除已在本机完成、但 Redis 提交失败时，Node Manager 保留 Deleted 结果。后续同一删除调用只重新提交结果，不再次删除运行时。底层 MasterStateSink 仅返回 Published 或错误；产品节点外层 SQLite JournalSink 在符合降级条件时持久化结果并返回 Journaled。Journaled 不等于集群发布成功，详见 [节点降级契约](node-lifecycle.md)。

Master 收到 Published 的创建结果后再读取已提交记录核对，避免把未落盘的节点响应当作集群成功。路由发布仍只从已提交 Running 记录生成；Edge 已通过全量/增量流订阅，重连重新同步。

## 组件身份与租户边界

| RPC | 允许的组件身份 | 额外检查 |
|---|---|---|
| Master.RegisterNode | Node(node_id) | 证书身份必须与注册 ID 一致 |
| Master.CreateCapsule | API Server | CallerContext 与 Spec 租户匹配，或已认证管理员 |
| Master.GetCapsule | API Server 或归属 Node | 租户权限／节点归属 |
| Master.CommitCapsule | 归属 Node | 完整 Assignment、Spec、实例 revision 由存储层核验 |
| Node.CreateCapsule | Master | 本机 node_id、Spec、Assignment、设备分配 |
| Node.DeleteCapsule | API Server | 租户权限、本机控制器、完整 Assignment |

服务使用部署提供的 CA 做双向 TLS，`Peers` 再按客户端叶证书的 SHA256 映射组件身份。普通请求头不能声明自己是 Master 或 Node；即使证书由同一 CA 签发，未配置的组件证书也被拒绝。默认空 Peers 拒绝所有组件。

CallerContext 的管理员位仅从受信 API Server 接受，不是公开 HTTP 请求可以自行设置的权限。用户侧 API Key 校验、短时认证缓存和 Go HTTP 后端装配已实现，见 [API Server 阶段](api-server.md)。当前 RPC 要求 mTLS；如后续启用明确配置的非 TLS 部署模式，需要在启动层定义身份边界，不能仅关闭 TLS 后信任客户端自报角色。

## 验证

```sh
export ADX_TEST_REDIS_SERVER=/absolute/path/to/redis-server
export CARGO_TARGET_DIR=/your/cache/cargo-target
python3 build/ci/run.py control-rpc --jobs 2
cargo test --locked -p adx-master -p adx-node-manager -p adx-protocol -p adx-core -p adx-scheduling -j2
```

`control-rpc` 每轮生成有效期两天的独立 CA 和测试证书，启动独立 Redis 目录／Socket，然后使用正式服务实现运行协作测试。缺少 Redis 或 openssl 时失败，不切换模拟网络，也不以忽略用例作为通过。

[RPC 协作测试](../../platform/master/tests/rpc.rs) 覆盖：

- 运行时 Start 入口直接查询真实 Redis，确认 Assignment 已提交；并发相同创建只启动一次。
- 真实 mTLS 的 Node / API Server / Master 角色隔离、未知证书拒绝、跨租户读写拒绝、过期 Assignment 拒绝。
- Node Manager 查询地址返回，API Server 直达节点删除；重复删除不重复调用后端。
- 容量耗尽后的创建请求等待，API Server 超时后操作继续，释放资源后成功执行。
- 删除结果提交时 Redis 故障，重启后重试只补交结果；最终路由视图和运行时集合为空。

协议规则测试另验证 CapsuleRecord 往返、无效状态／身份拒绝，以及请求头不能冒充 TLS 身份。生成的 Go 协议需要通过现有 Sandbox API 测试与 vet，保证本轮协议变更不破坏保留的兼容层。

本轮结果（2026-09-15）：

| 验证 | 结果 | 日志／证据，相对 `out/ci/control-rpc/` |
|---|---|---|
| 五个相关 Rust crate 回归 | 94 通过，0 失败，7 个需专门环境或性能入口的用例忽略 | `regression.log` |
| 最后调整后的 Node Manager／protocol 回归 | 39 通过，0 失败，0 忽略 | `adapter-final.log` |
| 严格 Clippy，含所有测试目标 | 通过 | `clippy-final-v2.log` |
| 真实 Redis＋mTLS RPC | 1 通过，0 失败，0 忽略；三个执行阶段均退出 0 | `integration-final-v2/result.json` |
| Go 协议重新生成、完整测试和 vet | 107 个顶层测试及 93 个子测试通过；vet 通过 | `go.log` |
| CI harness | 7 通过 | `harness.log` |

Rust／RPC 在 macOS ARM64 执行，测试 Redis 7.2.5 使用 AOF always；二进制摘要在 RPC `result.json`。Go 在 `golang:1.25.5-bookworm` Linux ARM64 容器中验证。本轮是基于 `1e49d86f2123173a8f5358182ca294fab9a9b1e4` 的未提交工作树；`source-manifest.json` 保存相关源码摘要，不把基准提交本身标为包含这些改动。

`red.log` 保留协议转换缺失时的红灯，早期 Clippy 失败日志也保留。最终 RPC 目录包含 Redis 日志和临时测试证书。七个默认忽略的测试不能计入 94 项通过数；本轮通过专门入口实际运行其中的 RPC 用例，先前阶段的五项 Redis 存储用例结果见 [存储验证](master-storage.md)。

## 当前接线与限制

Go HTTP、服务配置、Redis 发现、心跳与重启对账、Edge 路由订阅、Node Proxy 启动同步、SQLite 降级及本地 FC 暂停/恢复、跨节点恢复均已接线。统一发布与基本 K8s 已验收；证书热重载和 K8s FC 后置。

`replace_rejected` 的持久化接口与节点故障后的 checkpoint 恢复流程职责不同。RPC 不把任意 Node 超时当作“尚未执行”来调用旧分配替换；故障恢复按持久化失效、有效 checkpoint、新归属和返回节点对账处理，见 [恢复契约](node-failure-takeover.md)。
