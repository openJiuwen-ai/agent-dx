# 单机进程部署

本文使用统一发布包在一台 Linux 主机上部署 Master、Node Manager（默认内嵌 Node Proxy）、API Server 和 Edge。入口是 HTTPS Edge；API Server 的 HTTP 监听只绑定本机回环地址，内部 RPC 与 Edge→Node Proxy 使用 mTLS。`adxctl` 的命令语义、按角色拆分部署、内置／外置 Redis 配置见独立的 [`adxctl` 部署指南](adxctl.md)。

## 准备与安装

- 使用与 Linux 主机架构一致的 ADX release 包；部署前执行 `python3 build/release/package.py verify /path/to/package` 校验完整清单。将整个包安装到 `/opt/adx`，不要混用不同包的二进制或 SDK。
- 按 `third_party/sandboxd/source.json` 准备外部 sandboxd。默认示例连接 `/run/sandboxd/sandboxd.sock`。sandboxd 的运行时、网络、镜像访问与主机权限由部署环境准备。
- 配置随包部署的本地 `runtime/adx-runtime-rootfs.img`：默认用作实例根文件系统，自定义镜像时只读挂载内置环境，用户镜像不需要预装 RRT。配置及启动语义见 [本地运行环境](runtime-environment.md)。
- `adxctl config init` 默认生成由 ADX 托管本机 Redis 的单机配置；外置 Redis 使用 `--profile standalone-external-redis`。两种方式都应启用 AOF 和持久化磁盘；具体命令见 [`adxctl` Redis 部署](adxctl.md#单机-standalone-部署)。

```sh
sudo install -d -m 0700 /etc/adx /etc/adx/tls /etc/adx/secrets /var/lib/adx /run/adx
sudo /opt/adx/bin/adxctl config init
```

检查配置中的包目录、状态目录、监听地址、Redis 地址和 namespace。示例采用单机回环地址发布内部服务；分节点部署时必须改为对端能访问的地址，并调整防火墙与 CIDR。对外 Edge 默认监听 8443，仅允许本机客户端；远程客户端需要配置 `ADX_DATA_PLANE_EDGE_FRONTEND_ALLOWED_CLIENT_CIDRS`，证书 SAN 也需包含实际入口域名或 IP。

示例使用 `resource_source.kind=auto` 从实际主机或 cgroup 限制探测 CPU、内存和磁盘上限。`disk_path` 必须存在。需要 external collector 时，替换为 `kind=sandboxd` 与对应的资源 HTTP UDS；该 UDS 与 sandboxd 生命周期 gRPC socket 不同。不要同时配置 `capacity_file` 与 `resource_source`。资源观测过期后节点关闭新准入，详细字段见 [节点生命周期](../testing/node-lifecycle.md)。

将 Node Proxy 的 `ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS` 改为 sandboxd 实际实例网络；示例 `10.88.0.0/16` 是部署输入，不是后端网络探测结果。

## 证书与初始密钥

证书由部署环境签发。内部证书需要合适的 serverAuth/clientAuth 用途，示例的内部服务名为 `adx.internal`，必须被服务端证书 SAN 覆盖。每个组件使用独立的证书和私钥。

| 文件 | 使用方 |
| --- | --- |
| `tls/ca.pem` | 内部 RPC 及 Edge→Node Proxy 的 CA |
| `tls/master.pem`、`master.key`、`master.der` | Master 证书、私钥和供对端识别的叶证书 DER |
| `tls/node-1.pem`、`node-1.key`、`node-1.der` | Node Manager 和本机 Node Proxy |
| `tls/api-server.pem`、`api-server.key`、`api-server.der` | API Server 内部 RPC 身份 |
| `tls/edge.pem`、`edge.key`、`edge.der` | Edge 内部 RPC 与代理客户端身份 |
| `tls/edge-public.pem`、`edge-public.key` | Edge 对外 HTTPS；客户端信任其签发 CA |
| `tls/public-ca.pem` | SDK 及管理客户端信任的对外 HTTPS CA，可与内部 CA 不同 |
| `secrets/admin-key` | 初始管理员 API Key 明文文件，仅由 Master 初始化读取 |

以上文件相对 `/etc/adx`。私钥和 API Key 权限设为 0600，目录设为 0700，并允许对应服务用户读取。部署环境可用 `openssl x509 -in component.pem -outform DER -out component.der` 从叶证书生成 DER；`peers` 映射检查叶证书字节，因此只更新 PEM 不足以完成内部身份轮换。`node:node-1` 必须与 Node Manager 的 `node_id` 一致。

用密码学随机数生成初始管理员 API Key 并写入受保护文件。配置只引用路径。Master 保存摘要；用管理员接口创建租户密钥，见 [API Key 管理](../testing/api-key-management.md)。正常业务客户端使用租户密钥。

服务启动并就绪后，可通过以下管理请求生成后续 SDK 示例使用的租户密钥。此脚本只写受保护文件，不输出明文密钥：

```python
import json, os, ssl, urllib.request
from pathlib import Path

request = urllib.request.Request(
    'https://localhost:8443/api/admin/v1/keys',
    data=json.dumps({'tenantId': 'example'}).encode(), method='POST',
    headers={'Content-Type': 'application/json',
             'Authorization': 'Bearer ' + Path('/etc/adx/secrets/admin-key').read_text().strip()},
)
context = ssl.create_default_context(cafile='/etc/adx/tls/public-ca.pem')
with urllib.request.urlopen(request, context=context, timeout=30) as response:
    credential = json.load(response)
fd = os.open('/etc/adx/secrets/tenant-key', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, 'w') as output:
    output.write(credential['apiKey'])
```

## 校验、启动和业务就绪

```sh
/opt/adx/bin/adxctl validate
/opt/adx/bin/adxctl render --output /run/adx/config-review
/opt/adx/bin/adxctl run
# 另一个终端
/opt/adx/bin/adxctl status
```

CLI 为 API Server 注入共享 Redis 地址和 namespace，发现轮询间隔 `config.discovery.poll_seconds` 默认 5 秒，可配置为 1–86400 秒。Node Manager 的发现配置采用自身协议，由 CLI 分别生成。

`validate` 校验部署结构和 CLI 约束；TLS 文件内容、组件字段、sandboxd 和网络连通性由组件启动与真实请求检查。sandboxd 可以晚于 supervisor 启动：Node Manager 会保持存活并重复等待后端就绪，在此期间不会向 Master 注册或接收新实例；配置错误仍会使进程退出。`render` 的输出目录必须尚不存在；生成目录 0700、文件 0600，可能包含 Redis 连接凭证，不应上传为公开日志。`run`/`start` 都在前台运行 supervisor，systemd 或 Pod 可直接托管该进程。

状态输出中的 PID 不能代替业务就绪。通过公共 SDK 创建一个小实例、执行命令后显式删除，检查完整调用链：

```sh
python3 -m venv /opt/adx-client
/opt/adx-client/bin/python -m pip install /opt/adx/sdk/*.whl
# 使用 /opt/adx-client/bin/python 运行下面的SDK示例
```

```python
import os
from pathlib import Path
from adx_sandbox import ConnectionConfig, Sandbox

os.environ['SSL_CERT_FILE'] = '/etc/adx/tls/public-ca.pem'  # 对外证书的签发 CA
connection = ConnectionConfig(
    server_address='localhost:8443',
    token=Path('/etc/adx/secrets/tenant-key').read_text().strip(),
    use_tls=True, verify_tls=True,
)
sandbox = Sandbox(image='YOUR_RRT_IMAGE', runtime='runc', cpu=250, memory=256,
                  idle_timeout=0, connection=connection, create_timeout=120)
try:
    result = sandbox.commands.run('printf ready')
    assert result.exit_code == 0 and result.stdout == 'ready'
finally:
    try:
        sandbox.kill()
    finally:
        sandbox.close()
```

先安装包内 `sdk/` 下的 wheel。暂停/恢复需使用支持 checkpoint 的后端并准备制品目录或对象存储，不能用 runc 的基础创建验收替代，见 [快照与存储](../testing/snapshot-storage.md)。组件日志位于部署的 `state_dir/logs/`；就绪失败优先检查证书、Redis、资源源、sandboxd、RRT 与 Node Proxy 绑定。

SDK 中的 runtime 及资源数值应与已准备的 sandboxd 后端匹配。例如 Firecracker 验收使用 `runtime='firecracker', cpu=1000, memory=512`；部署配置的角色、身份和内部调用链无需因此更换。

## 更新与停止

配置和证书在进程启动时读取。更新证书时，同步更新引用它的 `peers` DER 文件和必要的 CA，再按部署维护流程重启相关组件。本期没有 `adxctl restart` 子命令，也不提供证书热重载或无中断轮换保证。

`adxctl stop` 会先删除本机已管理实例，提交清理结果，然后退出服务；停止 supervisor 的 SIGTERM/SIGINT 也遵守该契约。它不适合作为保留现有实例的证书更新命令。需要保留实例时，由部署环境重启选定组件，待 Node Manager 完成权威对账和路由同步后恢复使用。Master 不可用时，Node Manager 重启只能观察实际实例，需等待 Master 对账后恢复生命周期操作。

Kubernetes 同样在 Pod 内运行进程，但部署与验收应使用独立的 [K8s 驱动](../../build/e2e/kubernetes/README.md)；其 namespace、镜像身份、用例结果与清理记录单独保存。
