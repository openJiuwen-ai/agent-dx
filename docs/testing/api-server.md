# Rust API Server 与服务启动

2026-09-18 核对：HTTP 兼容层已接入 Instance RPC，提供 Master、Node Manager 和 Rust API Server 服务入口。生命周期状态机仍在 Node Manager。下文契约按当前代码描述，末尾测试表保留 2026-09-15 当次证据。

## 请求链路

```text
SDK → Rust API Server
  认证：短时摘要缓存 → 缺失时 Master.VerifyApiKey → Redis 摘要记录
  目录：Master 首次全量 → revision 增量 upsert/delete → 本地实例目录
  创建：本地兼容字段转换 → Master.CreateInstance → Shard → Node Manager
  删除：本地实例目录 → Node Manager.DeleteInstance
                         ↑ 仅结果不明／读后写时 Master.GetInstance
Node Manager → sandboxd / RRT HTTP / Node Proxy UDS
Node Manager → MasterStateSink → Redis 条件提交
```

API Server 通过 `InstanceDirectoryService.WatchInstances` 维护完整的内存实例目录，条目包含 InstanceRecord、节点地址和 Node Proxy 地址。每次连接先接收全量 reset，随后按 Master epoch 与 revision 接收增量 upsert/delete；revision 断档或非法 epoch 增量会清空目录并重新全量同步。普通传输断开期间保留最近完整目录并后台重连，节点继续检查租户与完整 generation。`GetInstance` 只用于创建后的读后写收敛，以及结果不明时针对原 Assignment 的恢复查询。

实现与本地验收证据见 [实例目录订阅验收](2026-09-18-instance-directory.md)。

目录未完成首次同步时，API Server 不接受依赖实例归属的请求。已同步目录中的缺失项是确定的 NotFound，不触发逐项 Redis 查询。实例目录与 Edge 路由缓存分开：前者还包含 Redis 保留的 Deleted 等终态记录，以维持重复生命周期请求的幂等结果；公开查询仍将 Deleted 映射为 NotFound。后者只发布可路由的 Running 实例。

正在等待确认的删除固定其目标，不因刷新或缓存淘汰更换目标；待确认操作达到上限时拒绝新的删除。已完成操作由节点的状态机与持久化结果处理重复调用。这些待确认记录仅在 API Server 内存中，不提供 API Server 重启后找回未确认操作的承诺。

## HTTP 兼容与支持范围

沿用既有 Sandbox URL、请求／响应封装和 SSE 行为。认证接受 SDK 的 `X-Auth`、`X-Auth-Token` 与 Bearer；调用者不能通过请求体或 `X-Tenant-Id` 自报租户。当前适配镜像、隔离 runtime、CPU、内存、磁盘、GPU/NPU 整卡、环境变量和 Instance ID。CPU 使用 millicores、HTTP 内存 MiB 转成内部字节；环境随 Spec 持久化和下发，执行身份及节点部署参数由 sandboxd 适配器最后覆盖。

公开 JSON 直接转换为 `adx.control.v1` 的 Instance 类型；协议按 Instance、快照、凭证和路由职责拆分。已接通暂停／恢复、reload、可复用快照、空闲回收、重启策略、failover、HTTP 亲和策略、S3 rootfs／mount、镜像入口继承、创建及运行期网络策略、独立 request/limit、extra_config、每实例数据面安全模式、鉴权端口转发和 `upstream` reverse tunnel。公开 API 拒绝本机 rootfs 与 host mount，防止客户端把节点路径作为租户契约；它们只属于部署拥有的本地运行环境。旧 `/invoke` 兼容传输仍未接入；用户命令走 RRT HTTP 数据接口。Agent 路由保留，配置 Agent 服务地址时转发给上层 Agent 服务；未配置时返回暂不可用。

## 认证

部署将初始 API Key 放在受保护的文件中，Master 初始化读取并将 SHA256 摘要及租户／管理员身份保存到 Redis。重复启动不更新已有密钥身份。到期时间为 Unix 秒，0 表示不设置到期时间；Master 校验和 API Server 缓存都受该到期时间约束。缓存不保存明文密钥，未知／无效凭证不进入正向缓存。

组件间必须使用部署提供的 mTLS。Master 按证书指纹识别 API Server 与具体 Node；身份上下文仅从受信 API Server 接收。管理员密钥创建／查询／吊销和 Edge 认证已接线，见 [API Key 管理](api-key-management.md)。证书更新通过重启组件生效，热重载后置。

## 服务装配

Master、Node Manager 和 API Server 均使用 `--config /path/to/config.json`，未知配置项拒绝。配置示例见 `build/config/examples/`，文件路径按进程工作目录解释。API Server 默认内嵌 Edge；API 监听使用 HTTPS，或以 `loopback_http=true` 限制在字面回环地址供 Edge 转发。内部 gRPC 客户端校验配置的 CA 与服务端名称。显式 `edge_mode=standalone` 才启动独立 `adx-edge-frontend`。

Master 在绑定监听地址后开启一次 Redis Session，恢复调度目录，装配 MasterService 与 AuthService。

Node Manager 装配 Sandboxd、RrtReadiness、UdsRoutes 及带 SQLite 降级的 StateSink。资源源支持 `resource_source.kind=auto` 自动探测、`kind=sandboxd` external collector，以及兼容的 `capacity_file`；配置互斥，按有效期关闭新准入。实例 Stats 与节点容量分别采集，见 [节点生命周期](node-lifecycle.md)。

Node 启动后先关闭生命周期入口，以本次进程身份注册并获取 Master 完整目录，与 sandboxd 实际实例对账，恢复资源占用和本机绑定后再开放准入。Master 暂不可用时不清理或重建。详细契约见 [发现与重启对账](recovery-discovery.md)。

入口支持 Redis 注册发现，API Server 的 Master／认证和目录客户端共用动态 resolver。Node Proxy 全量同步、统一配置生成与 supervisor 已装配。`adxctl stop` 和 supervisor 的 SIGTERM/SIGINT 会执行本机实例清理；单独 Node Manager 故障重启走权威对账。

## 验证入口

```sh
# 先在运行测试的主机平台构建 Rust 服务（不是测试模拟服务）
cargo build --locked -p adx-api-server -j 2
export ADX_TEST_API_SERVER="${CARGO_TARGET_DIR:-$PWD/target}/debug/adx-api-server"
export ADX_TEST_REDIS_SERVER=/absolute/path/to/redis-server
python3 build/ci/run.py api-control --jobs 2
```

测试启动真实 Rust HTTPS 进程，经过生产 Rust RPC、mTLS、Redis 和 Node Manager 状态机。运行时、就绪检查和本机路由仍由 RPC 测试夹具提供可控实现，因此不算 sandboxd／RRT／Edge 完整平台 E2E。此前的单独 `control-rpc` 入口保持可运行；`api-control` 要求实际 API 二进制。

Rust 重写已通过 [Buildkite #24](2026-09-17-rust-api-server-k8s.md) 独立K8s验收，详见 [迁移记录](rust-api-server.md)。下面保留旧版服务的历史验证数据。

## 2026-09-15 组件验证记录

后续完整平台验收见 [Buildkite #21](2026-09-17-observability-k8s.md)。以下数字与未提交标记只属于当时批次。

| 验证 | 结果 | 证据，相对 `out/ci/frontend-control/` |
|---|---|---|
| Go 完整模块测试、vet 与 API 可执行文件构建 | 111 个顶层测试及 93 个子测试通过，vet 通过 | `go-complete.log` |
| 五个 Rust crate 回归 | 94 通过，0 失败，9 个显式环境／性能用例默认忽略 | `rust-final.log` |
| 严格 Clippy，全部目标 | 通过 | `clippy-final.log` |
| 真实 Go HTTPS→Rust RPC→Redis；独立 Master 进程启动、重启 | 2 通过，0 失败，0 忽略 | `integration-complete/result.json` |
| 真实 Redis 存储集成，含初始凭证幂等与旧 Session 拒绝 | 6 通过，0 失败，0 忽略 | `storage-final/result.json` |
| CI harness | 7 通过 | `harness.log` |

Go 测试／构建使用 Linux ARM64 Go 1.25.5 容器，交叉编译 macOS ARM64 API 二进制；RPC、独立 Master 进程与 Redis 7.2.5 在 macOS ARM64 运行。Node Manager 入口通过编译和静态检查，未用真实 sandboxd 启动完整节点。`source-manifest.json` 保存源码和可执行文件 SHA256；最终 RPC 结果另包含 API 与 Redis 二进制摘要。基准提交为 `1e49d86f2123173a8f5358182ca294fab9a9b1e4`，验证对象是其上的未提交工作树。

`integration-complete/frontend-http.log`、`sandbox-api.log`、`master-process-0.log` 和 `master-process-1.log` 分别保存跨语言断言、API 进程以及 Master 首次启动／重启证据。普通 Rust 回归中的忽略用例不算通过；对应真实入口分别实际执行上述 2 项 RPC 和 6 项 Redis 测试。


## 创建入口模式

API Server 的 `create_mode` 默认 `central`；配置为 `local_first` 后，订阅 Master 的可用节点目录并轮转选择 Node Manager。
本地资源和硬约束满足时通过原子 claim 创建；否则保持同 Instance ID 进入 ShardScheduler。
本地成功不经过 Pack/Spread、软偏好评分或中心队列。相同规格的同 ID 创建收敛，规格/租户变化返回冲突。
入口超时使用同身份向 Master 重试；不会把一次缺失查询当作重新生成 ID 的许可。
目录有效期、mTLS、暂留资源和验收范围见 [创建契约](atomic-instance-claim.md)。
