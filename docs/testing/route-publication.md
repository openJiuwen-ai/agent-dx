# Master 路由发布与 Node Proxy 同步

2026-09-15。接通 Master → Edge 的路由发布与 Node Manager → Node Proxy 的完整绑定同步。Capsule 生命周期仍由 Node Manager 管理。

## 模块和实际调用链

| 模块 | 实现职责 |
|---|---|
| `master/src/routes.rs` | 从已提交的 Redis 状态构造路由，共享一个发布视图，提供 mTLS 全量／增量流 |
| `gateway/src/edge/master_routes.rs` | Redis 发现 Master，订阅并校验路由版本，原子更新内存缓存；API Key 短时校验缓存 |
| `gateway/src/edge/resolver.rs` | 新入口只读本地缓存；缺失时暂不可用，命中不查询 Master／Redis |
| `gateway/src/node/route_control.rs` | 本机完整绑定同步、准入开关、控制会话及实例版本校验、旧连接退役 |
| `node-manager/src/routes.rs` | UDS 同步客户端，本机绑定目录，代理进程重启后的全量重放 |
| `node-manager/src/reconciliation.rs` | Master 权威目录对账与完整本机绑定同步的顺序协调 |

```text
Node Manager → sandboxd 启动／RRT 就绪 → Node Proxy 绑定确认
             → Master 提交 Running 到 Redis
             → Master RouteService 发布
             → Edge 更新内存缓存 → Node Proxy 复核绑定 → 实例 TCP 端点

Node Proxy 重启 → 关闭准入 → Node Manager 检测新 proxy_session_id
               → BeginBindings → ReplaceBindings 完整校验 → 开放准入
```

只有已提交的 Running 记录、有效运行时地址和可路由节点才能进入发布视图。节点维护开关仅影响调度；节点不可达或实例退役则使路由退出视图。未写入集群存储的节点结果不会凭本地缓存发布给 Edge。

## 路由同步契约

- 每条订阅流首先发送完整快照，此后增量携带 `epoch / base_revision / revision`。订阅和取得全量使用同一把发布视图锁，避免全量与增量之间漏事件。
- Edge 精确校验增量基线；版本缺口、倒退、错误执行身份或同版本不同快照都拒绝整帧，重新订阅全量。坏帧不修改已有缓存。
- Master 共享 64 帧广播缓冲，每个订阅另有 8 帧发送队列。慢订阅丢失增量后返回错误，重连获取全量。
- 已同步 Edge 断连时继续使用内存缓存；缺失路由返回暂不可用。Edge 重启没有磁盘缓存，等待全量后就绪。Node Proxy 始终复核本机绑定，已退役实例不能因 Edge 缓存滞后重新接入。
- Edge 定期重查 Redis 的 Master 地址和 epoch，发现变化后重新连接。Redis 发现失败不主动清空已有缓存。
- 当前 Master 每 200 ms 检查存储版本，变化时读取状态快照并计算差异。Edge 应用增量时重建缓存视图；本阶段没有宣称大规模发布性能达标。

## 本机同步与重启

代理进程 UUID 标识一次 Node Proxy 启动；`sync_epoch` 标识一次完整同步。`BeginBindings` 比较旧身份后推进 epoch，关闭准入，并退役已有流。`ReplaceBindings` 先校验全部条目，成功后才开放入口；相同快照重试可确认完成。

增量绑定必须携带当前代理 UUID 和同步 epoch，再按实例归属 generation／绑定 revision 校验。退役保留版本记录；旧进程或旧同步的迟到激活无法复活实例。完整同步遗漏的旧绑定也保留退役记录。

Node Manager 持续运行而仅代理重启时，可直接重放本机内存目录。Node Manager 自身重启则须先取得 Master 权威目录，不能把不完整内存当作全量。完整对账会中断本机已有代理连接；同步失败时保持关闭，等待重试。这一流程不新增节点磁盘依赖。

共进程和分进程均已装配同一 `NodeProxyService`，两种模式都走 UDS 与同一绑定校验；见 [进程模式](node-proxy-process-modes.md)。本页末尾的早期测试是分进程批次。

## 认证与配置

`RouteService` 只允许 `edge` 组件证书；API Key 校验允许受信 API Server 和 Edge。Master 配置需要加入 Edge DER 身份。Edge 新入口不调用独立 IAM，也不读 etcd 路由。

认证缓存只以密钥摘要索引，限制条目数和有效期；有效期取缓存 TTL 与密钥到期时间的较小值。Master 不可用时只接受仍有效的缓存身份，新密钥无法校验。主动吊销最多受到缓存 TTL 的延迟影响。数据请求仍校验租户归属，端口转发也要求凭证。

| 配置 | 用途 |
|---|---|
| `build/config/examples/master.json` | Master 的 `edge` 证书身份 |
| `build/config/examples/edge-control.json` | Redis 地址／namespace、控制 RPC mTLS、发现周期、认证缓存预算 |
| `ADX_EDGE_CONTROL_CONFIG` | 指向 Edge 控制连接配置文件 |
| `ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR=/opt/adx/run/node` | Node Proxy 本机控制 socket 目录，须限制目录访问权限 |
| Node Manager `proxy_socket=/opt/adx/run/node/route.sock` | 与上述目录一致 |

Edge 对外 TLS、Edge → Node Proxy 的网络／mTLS 配置继续单独设置。默认部署由 Node Manager 内嵌 Proxy；显式 `proxy_mode=standalone` 才启动两个受管进程。旧的可选 etcd 库和历史测试脚本还未整体删除，历史脚本须适配新的启动契约后才能复用。

## 验证范围

`out/ci/route-publication/` 保存红灯、编译、单测和服务协作测试。新 RPC 用例使用真实 Redis、双向 TLS、gRPC 路由流、Node Proxy H2 和实际 TCP 回显服务，覆盖：

1. 错误组件身份不能订阅；错误／缺失 API Key 不能接入。
2. 全量与同连接增量驱动 Edge 缓存，真实数据流往返。
3. Master 端点停止后仍使用已同步路由和有效认证缓存。
4. 本机退役先于 Edge 删除路由时，代理拒绝新连接。
5. Redis 发布新 Master 地址／epoch 后，Edge 重新获取全量，旧 Master 无法继续刷新。

本机测试另覆盖代理重启重放、完整同步之前禁止准入、坏快照原子拒绝，以及旧同步请求拒绝。Go 服务生成协议、单测、vet 和构建独立执行。

这些测试使用 TCP 回显服务和测试运行时，未启动真实 sandboxd／RRT，也未构成公开 Sandbox SDK 创建—执行—删除的 Buildkite 验收。后续已完成统一进程装配、Node Activity、SQLite 降级、暂停/快照生命周期；基本 K8s 结果见 [Buildkite #21](2026-09-17-observability-k8s.md)，FC 结果见 [路线图](control-plane-roadmap.md)。

## 本轮结果

验证对象为 `refactor/monorepo-layout` 基准 `1e49d86f2123173a8f5358182ca294fab9a9b1e4` 上的未提交工作树。Rust／Redis／RPC 在 macOS ARM64 执行，Go 在 Linux ARM64 工具链容器测试并构建本机服务。

| 检查 | 结果 | `out/ci/route-publication/` 证据 |
|---|---|---|
| Gateway、Master、Node Manager、Protocol | 174 通过、0 失败、12 默认忽略 | `rust-v6.log` |
| 严格 Clippy，全部相关测试目标 | 通过 | `clippy-final.log` |
| 最后一处等价 lint 修改后的 Gateway 协议矩阵 | 3 通过、0 忽略 | `gateway-final.log` |
| Go 全包测试、vet、服务构建 | 206 通过，vet／构建通过 | `go-v3.log` |
| 真实 Redis／mTLS／路由与 H2/TCP | 4 通过、0 忽略 | `rpc-v6/result.json` |
| 启用 Go HTTP 服务的同一 RPC 套件 | 4 通过、0 忽略 | `frontend-v6/result.json` |
| 真实 Redis 存储／发现恢复 | 7 通过、0 忽略 | `storage-v6/result.json` |

默认忽略项不算通过；专用 Redis／RPC 用例由上面独立入口实际运行，性能基准未执行。API Server 表项复用了同一 RPC 套件，不能与 RPC 计数当成八个独立场景。源码、被执行的测试程序和服务制品 SHA256 记录于 `source-manifest.json`。中间失败日志保留，最终通过证据以上表为准。
