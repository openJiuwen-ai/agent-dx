# Node Manager 与 Node Proxy 进程模式

`gateway::node::NodeProxyService` 统一持有数据监听、健康监听、绑定控制服务和连接任务。`adx-node-proxy` 直接托管该服务；Node Manager 通过 `proxy_mode` 选择是否在本进程托管它。

## 共进程（默认）

Node Manager 省略 `proxy_mode` 或设置 `"proxy_mode": "embedded"` 时，在本进程托管 Node Proxy。统一部署配置只保留 `node-manager` 服务，Node Proxy 的 `ADX_DATA_PLANE_*` 环境配置放入该服务的 `env`。

```yaml
services:
  - id: node-manager
    role: node-manager
    config:
      proxy_socket: /opt/adx/run/node/route.sock
    env:
      ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR: /opt/adx/run/node
      ADX_DATA_PLANE_NODE_PROXY_BIND: 0.0.0.0:8443
      ADX_DATA_PLANE_NODE_PROXY_HEALTH_BIND: 127.0.0.1:18443
      ADX_DATA_PLANE_ALLOWED_EDGE_CIDRS: 10.0.0.0/8
      ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS: 10.0.0.0/8
```

## 分进程（显式选择）

Node Manager 必须设置 `"proxy_mode": "standalone"`，部署配置同时包含 `node-manager` 和 `node-proxy` 两个服务。Proxy 的环境配置放在 `node-proxy.env`；Node Manager 的 `proxy_socket` 指向相应目录下的 `route.sock`。独立二进制仍复用同一个 `NodeProxyService`，用于需要独立故障域或资源隔离的部署。

这是组合方式的配置片段。部署仍需补齐 Master 发现、证书、sandboxd、RRT、资源源和实际网络范围。需要 mTLS 的环境继续配置 Node Proxy 服务端证书、私钥和 Edge 客户端 CA。

CLI 校验共进程的 `proxy_socket` 与控制目录一致，并拒绝同一部署中的多个服务拥有同一个 Proxy 控制 socket。

两种模式都使用同一套受保护 UDS 绑定协议和完整同步逻辑；当前共进程模式仍走 UDS，没有另外实现一套进程内状态副本。数据请求直接由 Proxy 服务处理，不进入 Capsule 生命周期队列。共进程模式共享进程与 Tokio 执行器；需要独立资源隔离时使用分进程部署。

## 生命周期边界

- 监听绑定完成后，Proxy 的实例绑定准入仍保持关闭。Node Manager 完成权威目录对账和全量绑定同步，才能开放已有实例流量。
- Node Manager 共进程重启会同时重启 Proxy。既有 sandboxd 实例由部署环境托管，不随该进程退出；新 Proxy 在重新同步前拒绝旧缓存流量。
- Proxy 内部控制或健康服务异常退出会结束其服务，由 Node Manager 报错退出并交给 supervisor 重启，避免控制服务已停而数据监听继续接收请求。
- 正常退出停止新流量、按配置等待活跃流排空，再关闭连接及控制服务。CLI 的 `stop` 仍先通过 Node Manager 清理本机实例，之后停止服务。

## 验证

`gateway/tests/node_service.rs` 使用真实监听和 UDS，验证初始绑定未就绪、服务停止后 TCP 监听和已建立控制连接都关闭。`out/ci/stage-6/composition-tests.log` 中 Node Manager、Gateway、CLI 共157项测试通过，Clippy 通过。CLI 的 socket 归属校验随后通过8项测试与 Clippy。

`out/ci/pause-resume/fc-r12/evidence/` 记录 package-v10 的真实共进程 Firecracker/S3 验收：公共 SDK 10项全部通过，计数从93恢复到215、PID保持16，覆盖 Node Manager 重启、Master 失联时本地记录与补写、资源采集失效和最终清理。`deployment-final.json` 从实际运行配置导出并只保留非敏感字段，确认进程角色只有 redis/master/node1/api/edge，node1 为 embedded；早期 `deployment-node1.json` 是配置变更前的副本，不作为最终部署证据。

这证明两种进程模式均已接通；密钥管理与完整部署示例已另行验收，见 [部署报告](2026-09-16-installed-example.md)；配置/证书更新通过重启组件应用，热重载后置。

2026-09-16更新：最新package-v11在Lima r14再次通过10项共进程FC/S3验收，并覆盖SDK指定节点；证据位于 `out/ci/pause-resume/fc-r14/evidence/`。

2026-09-20更新：`embedded` 已改为 Node Manager 和统一部署的默认值。当前源码重新打包后，本地Docker双节点八组公开SDK验收全部通过；`deployment-node1.json` 和 `deployment-node2.json` 均显示每节点只有一个 `node-manager`、没有独立 `node-proxy`。两节点 Proxy `/metrics` 均报告 ready=1，CONNECT错误为0；结果、JUnit和清理检查均通过，证据位于 `out/ci/local-e2e-embedded-default-20260920/`。
