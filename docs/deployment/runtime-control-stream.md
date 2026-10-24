# 部署 Execd 主动控制流

在节点的 adxlet 服务配置中增加：

```yaml
runtime_control:
  listen: 10.231.16.1:19003
  advertised_address: http://10.231.16.1:19003
  key_file: /opt/adx/data/runtime-control-key
```

上述地址是示例，应替换为**运行环境内可以访问的本节点 IPv4 地址**。当前固定 sandboxd 的网络策略使用 IPv4 CIDR，控制地址不接受域名或 IPv6，配置校验会直接拒绝。`127.0.0.1` 在独立网络命名空间的运行环境内指向运行环境本身，不能用作普通 Runtime 的回连地址。Kubernetes 可使用节点 Pod IP 或实际可达的宿主地址；网络策略、路由和防火墙需要允许 Runtime 到该节点的 TCP 19003。端口可改，但 advertised_address 必须与实际监听和转发一致。

`key_file` 必须为绝对路径。首次启动由 adxlet 创建父目录及随机主密钥，文件权限 0600，落盘后才使用；后续启动读取同一密钥。也可以部署时提供 32–4096 字节 UTF-8 密钥文件，权限 0600/0400。目录须可写，文件须随节点状态保留。它与管理员 API Key、Execd HTTP token、组件 mTLS 证书是不同凭证。

adxlet 自动注入 `ADX_RUNTIME_CONTROL_ADDRESS` 和按执行身份派生的 `ADX_RUNTIME_CONTROL_TOKEN`；主密钥不会进入实例环境。用户不需要在 SDK 的 env 中配置这些字段，普通请求中的同名值不会生效。恢复时按目标节点/执行身份刷新地址和凭证，Execd 重连到目标节点。

使用 profile 配置时，可按 [`deployment-node-control-stream.yaml`](../../build/config/examples/deployment-node-control-stream.yaml) 写本节点配置：

```sh
export ADX_NODE_ID=node-a
export ADX_NODE_ADDRESS=192.168.10.179
export ADX_RUNTIME_CONTROL_ADDRESS=http://192.168.10.179:19003
adxctl validate --config /opt/adx/config/deployment.yaml
```

配置文件需要先保存到指定路径；`validate` 命令只校验，不部署服务。进程启动方式仍见 [adxctl](adxctl.md)。

`runtime_control` 是 adxlet 的必需配置，示例部署和 E2E 配置均启用。升级时须同时把支持主动控制流的 Execd 装入运行时 rootfs 或 bootstrap。节点缺少配置会在启动时返回明确错误。`metrics_interval_seconds` 默认为 15 秒，独立于心跳与数据对账的 `report_interval_seconds`。

就绪、checkpoint 协作和请求应答走专用双向 gRPC 流；HTTP 控制接口仍可用于独立诊断。网络限制下不能主动回连时应先修复地址与网络配置，启用流模式不会在失败后自动退回 HTTP 轮询。

Execd 在阻塞线程池打开 checkpoint handoff 文件，通过本次 prepare 新建的私有 Unix socket 通知单线程 I/O 调度器；该 socket 在发布 Prepared 前已消费并关闭，不带入快照。后续跨快照的 handoff 读取继续使用阻塞任务的结果通路。客户端断开不会取消已经接纳的准备操作；只有成功打开 handoff 后才发布 Prepared，读取到恢复结果后仍须校验执行身份并重新开放监听器。

正常权威核对从 Coordinator 缓存取得目录，只比较记录归属和版本。运行时清单接管在节点启动或会话恢复时执行。活动、退出、健康失败和 checkpoint 请求由事件及实例截止时间推进，资源采样不推进生命周期。

用户配置 deny-by-default 网络策略时，adxlet 保留到此节点 IPv4／控制 TCP 端口的系统规则，优先级为平台保留的最大值。创建和运行期策略更新均使用同一转换；stateless 策略也允许该端点的应答。其他目标、其他端口和用户 DNS 策略保持原限制。该例外不能由 SDK env 改写，仍需执行身份凭证认证。

SDK 所需的 Execd HTTP 端口（50090）及用户声明的公开 TCP 端口也保留系统规则。Execd HTTP 端口在 stateful 和 stateless 模式均按运行环境本地端口匹配双向流量，避免运行期策略更新切换连接状态的 generation 后丢弃既有控制连接的回包。用户声明的公开端口在 stateful 模式放行入站并由连接跟踪处理应答；stateless 模式匹配双向流量。系统规则不移除用户的全网段拒绝规则。全封禁模式仍允许这些平台必需通路，因此它的含义是限制普通网络流量，而非关闭所有管理访问。

当 adxlet 与 sandboxd 共享网络命名空间时，优先监听 sandboxd 网桥的网关 IPv4，例如上述 `10.231.16.1:19003`；所属运行环境在同一子网内直接回连。节点先等待 sandboxd 就绪，再绑定该地址，随后开放准入。如果两个进程在不同网络命名空间，必须改用 adxlet 自身拥有、且运行环境可路由到的地址，不能绑定另一个命名空间的网桥。一个 backend 网络的多个运行环境共用此控制端点，身份仍按执行分别校验。


## 反向隧道与网络策略替换

adxlet 根据类型化执行选项 `execution.tunnel_http_port` 推导 WS 监听端口
（默认 HTTP 8766／WS 8765），该选项随执行记录持久化并传递。API 保留
`EXECD_TUNNEL_*` 环境变量；节点清除用户、profile 和节点环境映射中的原始值，
再根据执行选项生成监听变量。HTTP 监听器仅绑定运行环境内的 loopback，
不增加公开访问的 ACL 例外。

stateful 策略保留 WS 端口最高优先级的 TCP 入站规则，不增加按源端口放行的
出站规则。创建和运行期策略替换均拒绝反向隧道与 stateless 流量策略的组合，
因为后端无法将其返回路径例外限制为已建立连接。

策略替换会重置后端连接跟踪，可能中断已有 WS 连接。策略更新确认成功后，
持有隧道的 SDK 客户端在自己的事件循环上主动退役旧传输连接。现有重连循环
保留同一个 session 和可恢复请求状态，并建立新的入站连接，不等待失效路径上
的关闭握手。策略更新完成表示安装已完成，隧道就绪可能稍后恢复；恢复期间，
应用请求可能需要重试。

其他客户端或原始 HTTP 发起的更新仍由现有应用心跳兜底发现，默认心跳间隔为
20 秒、pong 超时为 10 秒。standalone 回归要求持有隧道的客户端在 10 秒内恢复，
短于默认心跳间隔。
