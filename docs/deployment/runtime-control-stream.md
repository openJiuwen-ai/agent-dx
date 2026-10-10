# 部署 Execd 主动控制流

在节点的 adxlet 服务配置中增加：

```yaml
runtime_control:
  listen: 10.231.16.1:19003
  advertised_address: http://10.231.16.1:19003
  key_file: /opt/adx/data/runtime-control-key
```

上述地址是示例，应替换为**沙箱内可以访问的本节点 IPv4 地址**。当前固定 sandboxd 的网络策略使用 IPv4 CIDR，控制地址不接受域名或 IPv6，配置校验会直接拒绝。`127.0.0.1` 在独立网络命名空间的沙箱内指向沙箱本身，不能用作普通 Runtime 的回连地址。Kubernetes 可使用节点 Pod IP 或实际可达的宿主地址；网络策略、路由和防火墙需要允许 Runtime 到该节点的 TCP 19003。端口可改，但 advertised_address 必须与实际监听和转发一致。

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

该开关当前是显式启用。现有 profile 默认未配置 runtime_control，保持 HTTP 控制方式，避免更新 adxlet 后仍使用旧 Execd 的镜像导致就绪失败。启用前必须把新 Execd 装入内置运行时 rootfs 或用户镜像的 bootstrap。对外用户 API、SDK、命令、端口和 tunnel 接口不变；sandboxd 不增加配置字段或补丁。

就绪、checkpoint 协作和请求应答走专用双向 gRPC 流；HTTP 控制接口仍可用于独立诊断。网络限制下不能主动回连时应先修复地址与网络配置，启用流模式不会在失败后自动退回 HTTP 轮询。

用户配置 deny-by-default 网络策略时，adxlet 保留到此节点 IPv4／控制 TCP 端口的系统规则，优先级为平台保留的最大值。创建和运行期策略更新均使用同一转换；stateless 策略也允许该端点的应答。其他目标、其他端口和用户 DNS 策略保持原限制。该例外不能由 SDK env 改写，仍需执行身份凭证认证。

SDK 所需的 Execd HTTP 端口（50090）及用户声明的公开 TCP 端口也保留系统规则。Execd HTTP 端口在 stateful 和 stateless 模式均按沙箱本地端口匹配双向流量，避免运行期策略更新切换连接状态的 generation 后丢弃既有控制连接的回包。用户声明的公开端口在 stateful 模式放行入站并由连接跟踪处理应答；stateless 模式匹配双向流量。系统规则不移除用户的全网段拒绝规则。全封禁模式仍允许这些平台必需通路，因此它的含义是限制普通网络流量，而非关闭所有管理访问。

当 adxlet 与 sandboxd 共享网络命名空间时，优先监听 sandboxd 网桥的网关 IPv4，例如上述 `10.231.16.1:19003`；所属沙箱在同一子网内直接回连。节点先等待 sandboxd 就绪，再绑定该地址，随后开放准入。如果两个进程在不同网络命名空间，必须改用 adxlet 自身拥有、且沙箱可路由到的地址，不能绑定另一个命名空间的网桥。一个 backend 网络的多个沙箱共用此控制端点，身份仍按执行分别校验。
