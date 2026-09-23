# `adxctl` 部署指南

当前发布包只提供默认共进程部署：`adx-apiserver` 内嵌 Ingress，`adxlet` 内嵌 Relay。独立 `adx-ingress`、`adx-relay` 和 `adx-data-plane-forward` 不随包发布；分进程能力保留在源码中，使用时需自行构建对应二进制。

`adxctl` 是 ADX 统一发布包的本机进程部署工具。它读取一份 YAML 部署文件，生成本机各组件的最终配置，并以前台 supervisor 方式启动这些组件。它不创建 Environment，也不调用 Sandbox API；集群 API Key 等远程管理操作使用独立的 [`adxadmin`](adxadmin.md)。

release 安装器默认创建 `/usr/local/bin/adxctl -> /opt/adx/current/bin/adxctl`。因此日常命令直接使用 `adxctl`；切换 `/opt/adx/current` 后，命令自动指向新版本。安装器不会把其他内部服务二进制加入系统 `PATH`。

一份部署 YAML 只描述**当前主机**。单机部署可以在一份文件中包含所有角色；多机部署时，每台主机使用自己的文件，各文件通过相同的 `redis_url` 和 `namespace` 加入同一集群。`adxctl` 默认读取 `/opt/adx/config/deployment.yaml`，也可用全局 `-c/--config` 或 `ADX_DEPLOYMENT_CONFIG` 选择其他文件。它只接受 `.yaml` 或 `.yml`，不接受 JSON 部署文件。

## 组件名

| 部署角色 | 二进制 | 部署位置 | 作用 |
| --- | --- | --- | --- |
| `coordinator` | `adx-coordinator` | 控制节点 | 持久化状态、全局轮转、Shard 调度、节点心跳与路由发布 |
| `adxlet` | `adxlet` | 每个工作节点 | 本机资源准入、Environment 生命周期、sandboxd 与恢复；默认同时嵌入 Relay |
| `relay` | `adx-relay` | 显式选择分进程时 | 数据面绑定和到 Environment Execd 的转发 |
| `apiserver` | `adx-apiserver` | 接入节点，默认内嵌 Ingress | 用户 API Key、Sandbox HTTP API、归属缓存和生命周期转发 |
| `ingress` | 内嵌时无独立进程；分进程时为 `adx-ingress` | 接入节点 | 对外 TLS、控制请求转发以及到 Relay 的数据连接 |
| `redis` | `redis-server` | 可选，仅一个主机 | 由 `adxctl` 托管的 Redis 7.2.5 和 AOF |

当前没有 `adx-frontend` 二进制。原控制面 Frontend 已重写并命名为 `adx-apiserver`。API Server 默认在同一进程中托管 Ingress，两个模块仍保持独立监听，默认使用各自的 TLS 身份；显式设置 `ingress_mode: standalone` 时才启动 `adx-ingress`。API Server 只监听回环地址，由 Ingress 对外提供 HTTPS。

sandboxd 不属于上述角色，始终由部署环境独立启动。Execd 位于发布包 `runtime/`，进入 Environment 环境运行，也不是宿主机服务。

## 命令

```sh
# 首次生成默认单机配置：全角色＋adxctl 托管的本地 Redis
sudo adxctl config init

# 查看模板但不写文件；profile 还支持 standalone-external-redis/coordinator/node/ingress-api
adxctl config template --profile node

# 正式部署可生成只引用内置 profile 的精简文件
sudo adxctl config init --profile node --compact
export ADX_NODE_ID=worker-a

# 展示环境变量与覆盖项合并后的完整有效配置
sudo adxctl config dump

# 编辑生成的 /opt/adx/config/deployment.yaml 后检查，不启动进程
sudo adxctl validate

# 生成最终组件配置，供上线前审查；输出目录必须不存在
sudo adxctl render \
  --output /opt/adx/run/config-review

# 前台启动 supervisor；start 是 run 的别名
sudo adxctl run

# 以下命令在另一个终端执行，读取同一份默认配置
sudo adxctl status
sudo adxctl stop

# 非默认位置可在所有子命令前后传入，也可设置 ADX_DEPLOYMENT_CONFIG
adxctl -c /srv/adx/node.yaml validate
```

`config init` 默认选择 `standalone` profile，即单机全角色并托管本地 Redis。它创建父目录并以 `0600` 写入完整模板；增加 `--compact` 时只写入 `schema_version` 和 `profile`，运行时再合并内置默认值。目标已存在时会失败，只有显式增加 `--force` 才覆盖。其他 profile 如下：

| profile | 生成的本机角色 |
| --- | --- |
| `standalone` | Redis、Coordinator、adxlet（内嵌 Relay）、API Server（内嵌 Ingress） |
| `standalone-external-redis` | 单机全角色，不托管 Redis；API Server 仍默认内嵌 Ingress |
| `coordinator` | Coordinator |
| `node` | adxlet（内嵌 Relay） |
| `ingress-api` | API Server（默认内嵌 Ingress；可显式拆分） |

`validate` 负责部署结构、路径、角色、Redis 和 socket 约束。组件证书内容、端口占用、sandboxd、Redis 连通性及服务自身字段由 `run` 和真实请求继续验证。

`render` 将 `redis_url`、`namespace`、Coordinator 发现配置、Node 管理 socket 和运行环境写入各组件最终配置。生成目录权限为 `0700`，文件权限为 `0600`。它可能包含 Redis 凭证，只用于本机审查。

`run` 不转入后台。systemd、Pod 或其他进程管理器应直接托管它。supervisor 在 `state_dir` 创建锁和 `supervisor.sock`，同一目录只能运行一个部署。日志写入 `state_dir/logs/<service-id>.log`。

`status` 返回子进程 PID、角色、重启次数、失败标记和日志状态。PID 存在只代表进程存活，不代表集群已经可以创建 Environment。

`stop` 先要求本机 adxlet 停止新准入并删除其管理的 Environment，清理成功后才按反向顺序退出组件。清理失败时命令失败，并保留 Coordinator、Redis、代理等依赖以便重试。

## 部署文件的公共字段

```yaml
schema_version: 1
package_dir: /opt/adx/current
state_dir: /opt/adx/run/control
redis_url: redis://127.0.0.1:6379/
namespace: adx
restart_limit: 3
restart_delay_ms: 1000
stop_timeout_seconds: 90
services: []
```

| 字段 | 含义 |
| --- | --- |
| `package_dir` | 当前主机安装的完整发布包；组件从其 `bin/` 目录启动 |
| `state_dir` | supervisor 锁、控制 socket、生成配置和日志目录；每个本机部署必须唯一 |
| `redis_url` | 集群状态和 Coordinator 发现使用的 Redis；所有主机必须指向同一个后端 |
| `namespace` | Redis 中的 ADX 集群隔离名；同一集群必须一致，不同集群必须不同 |
| `services` | 当前主机需要启动的角色，不是整个集群的角色清单 |
| `restart_limit` | 单次 supervisor 生命周期内，每个异常退出进程的最大重启次数 |
| `stop_timeout_seconds` | 单个 Drain 或进程停止阶段的超时 |
| `environment` | adxlet 和 API Server 共用的本地 EROFS 或 OCI 运行环境定义 |
| `logging` | supervisor 接管组件输出时的滚动、压缩和保留策略 |

每个 `services[]` 元素由 `id`、`role`、`config` 和可选 `env` 组成。`config` 是组件配置，`env` 用于 Ingress 和 Relay 等环境变量入口。不要手工重复公共 `redis_url` 和 `namespace`；`adxctl render` 会按角色注入。

### profile 默认值与受控覆盖

完整 YAML 继续受支持。需要减少重复配置时，文件可以引用一个内置 profile，并只写当前主机的差异：

```yaml
schema_version: 1
profile: node

state_dir: "${ADX_STATE_DIR:-/opt/adx/run/node}"
redis_url: "${ADX_REDIS_URL}"
namespace: "${ADX_NAMESPACE:-adx}"

logging:
  max_files: 9

service_overrides:
  adxlet:
    config:
      node_id: "${ADX_NODE_ID}"
      advertised_address: "${ADX_NODE_ADDRESS}"
      tls:
        certificate: "${ADX_NODE_CERT:-/opt/adx/config/tls/node-1.pem}"
    env:
      ADX_DATA_PLANE_RELAY_BIND: "${ADX_PROXY_BIND:-0.0.0.0:19002}"
```

合并规则固定如下：

- 顶层标量覆盖 profile 默认值，未出现的字段保留默认值。
- `logging` 和 `environment` 对象递归合并。
- `service_overrides` 按本机角色名定位；默认 Node profile 只有 `adxlet`，Proxy 的环境项也覆盖在该角色下。不能改变角色或增加隐藏进程。
- 服务 `config` 对象递归合并，`env` 按变量名覆盖。
- 数组整体替换，不按下标合并。
- profile 文件不能同时声明完整 `services`，避免两套来源产生歧义。
- 合并后仍执行与完整部署文件相同的类型检查和部署校验。

每台 Worker 使用自己的部署文件；`service_overrides.adxlet` 表示覆盖本机唯一的 adxlet，不是引用名为 `adxlet` 的集群节点。集群身份由该文件中的 `config.node_id` 单独指定，并且必须在集群中唯一。本机 supervisor 的服务 ID 固定使用角色名，不参与节点归属。精简 `node` profile 会生成 `node_id: "${ADX_NODE_ID}"`；未设置该变量或没有在 YAML 中改成明确值时，配置加载直接失败。

`adxctl config dump` 将当前环境变量和覆盖项解析成完整 YAML，便于上线前审查，也可以再次作为完整配置读取。输出可能包含带凭证的 Redis URL，不要写入公开日志。

### 使用环境变量设置字符串字段

部署环境不同但结构相同时，可在任意 YAML 字符串值中引用环境变量：

```yaml
package_dir: "${ADX_PACKAGE_DIR:-/opt/adx/current}"
state_dir: "${ADX_STATE_DIR:-/opt/adx/run/control}"
redis_url: "${ADX_REDIS_URL}"
namespace: "${ADX_NAMESPACE:-adx}"
services:
  - id: adxlet
    role: adxlet
    config:
      node_id: "${ADX_NODE_ID}"
      advertised_address: "${ADX_NODE_ADDRESS:-127.0.0.1:50052}"
```

- `${VAR}` 要求变量存在且非空，否则配置加载失败。
- `${VAR:-default}` 在变量未设置或为空时使用默认值。
- `$$` 生成字面量 `$`。
- 展开在 YAML 解析完成后执行，只处理值中的字符串，不处理字段名，也不会把变量内容解析成新的 YAML 对象或数组。
- 数字和布尔字段继续要求原生 YAML 类型。例如 `restart_limit: 3`；不要写成 `restart_limit: "${LIMIT}"`。需要按环境调整此类字段时，为不同部署文件保留明确数值。

`validate`、`render`、`run`、`status` 和 `stop` 每次读取配置时都会使用当前进程环境。运行中的 supervisor 已经生成并启动的组件不会因环境变量变化而自动更新；修改后需要按部署维护流程重新加载对应进程。

## 单机 standalone 部署

单机部署在一台 Linux 主机上运行 Coordinator、adxlet（内嵌 Relay）和 API Server（内嵌 Ingress）。主机还必须先准备 sandboxd、证书、初始管理员密钥以及 Environment 网络。

### 使用 `adxctl` 托管 Redis

发布包提供完整示例 [deployment-standalone-managed-redis.yaml](../../build/config/examples/deployment-standalone-managed-redis.yaml)。其中包含 `redis` 角色，Redis 只监听 `127.0.0.1:6379`，数据写入 `/opt/adx/data/redis`，使用 AOF `everysec`。

```sh
sudo adxctl config init

# 修改证书路径、监听地址、Environment CIDR、sandboxd socket 和磁盘路径后执行
sudo adxctl validate
sudo adxctl run
```

supervisor 按 Redis → Coordinator → adxlet（含内嵌 Proxy）→ API Server（含内嵌 Ingress）的顺序拉起进程。启动顺序不替代业务就绪检查；应等待节点完成 Coordinator 对账和 Relay 全量绑定同步，再使用 SDK 创建实例。显式分进程时，独立 Relay 会在 adxlet 前启动，独立 Ingress 会在 API Server 后启动。

### 使用外置 Redis

`standalone-external-redis` profile 对应发布包中的 [deployment.yaml](../../build/config/examples/deployment.yaml)。它是单机全角色、外置 Redis 配置，不包含 `redis` 角色，示例地址为 `redis://127.0.0.1:6379/`。

1. 执行 `sudo adxctl config init --profile standalone-external-redis`。
2. 先启动外置 Redis，并配置持久化与访问控制。
3. 将部署文件顶层 `redis_url` 改成实际地址。
4. 保持所有组件使用同一个顶层 `namespace`。
5. 执行相同的 `validate` 和 `run` 命令。

外置 Redis 不受 `adxctl status`、重启预算或 `stop` 管理。ADX 停止后 Redis 应继续运行并保留状态。

## 多机按角色部署

推荐至少分为一个控制节点、一个或多个工作节点、一个接入节点。三类主机均安装相同版本的完整 `/opt/adx` 包，但各自的 `services` 不同。

### 1. Coordinator 控制节点

使用 `coordinator` profile，其中只包含 `coordinator` 角色：

```sh
sudo adxctl config init --profile coordinator
sudo vi /opt/adx/config/deployment.yaml
# 修改 redis_url、advertised_address、TLS peers 和 bootstrap key 路径
sudo adxctl validate
sudo adxctl run
```

`listen` 是 Coordinator 本机监听地址；`advertised_address` 必须是 adxlet、API Server 和 Ingress 可访问的地址（默认 mTLS 使用 `https://`，network 模式使用 `http://`）。Coordinator 会将该地址带 TTL 写入共享 Redis。mTLS 模式的 `tls.peers` 必须登记实际 API Server、Ingress 和所有 `node:<node_id>` 的叶证书 DER。

若 Redis 也由控制节点托管，可在该文件的 `services` 开头增加 `redis` 角色。多机访问时 Redis 不能只绑定回环地址；非回环监听必须配置绝对路径 `password_file`，并将带 URL 编码凭证的同一个 `redis_url` 配置到所有主机。

### 2. Worker 节点

每个 Worker 使用 `node` profile，默认只启动 `adxlet` 进程，并在其中嵌入 Relay：

```sh
sudo adxctl config init --profile node
sudo vi /opt/adx/config/deployment.yaml
# 设置唯一 node_id、可达的 advertised_address/proxy_address、Redis、证书和网络 CIDR
# 确认外部 sandboxd 已创建 /run/sandboxd/sandboxd.sock
sudo adxctl validate
sudo adxctl run
```

每个节点的 `node_id` 必须唯一，并与 Coordinator `tls.peers` 中的 `node:<node_id>` 对应。`advertised_address` 是控制 RPC 地址，`proxy_address` 是 Ingress 连接的数据面地址。内嵌 Relay 的 `ADX_DATA_PLANE_*` 配置位于 `adxlet.env`；其中 `ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS` 必须覆盖 sandboxd 实际分配的 Environment 网段。

需要独立故障域或单独限制资源时，可以使用完整部署 YAML：为 adxlet 显式设置 `proxy_mode: standalone`，把 Proxy 环境项移到独立的 `role: relay` 服务。省略 `proxy_mode` 与设置 `embedded` 等价，不能在默认内嵌模式下再启动独立 Relay。

sandboxd 仍由节点部署环境单独管理。`adxctl stop` 只停止 ADX 进程，完成 Environment 清理后不会停止 sandboxd。

### 3. API Server 与 Ingress 接入节点

使用 `ingress-api` profile。部署文件保留 `apiserver` 和 `ingress` 两个逻辑角色，默认只启动一个 `adx-apiserver` 进程：

```sh
sudo adxctl config init --profile ingress-api
sudo vi /opt/adx/config/deployment.yaml
# 设置共享 Redis、Ingress 公网证书、允许的客户端 CIDR 和对外监听地址
sudo adxctl validate
sudo adxctl run
```

API Frontend 对应 `role: "apiserver"` 和 `adx-apiserver`。示例将 API 监听绑定到 `127.0.0.1:8888` 并启用 `loopback_http`。`role: "ingress"` 提供 Ingress 的控制配置和 `ADX_DATA_PLANE_*` 环境；默认由 `adxctl render` 合并进 API Server 进程。Ingress 对外监听 `0.0.0.0:8443`，将 Sandbox 控制请求转发到同进程的 API 回环监听，并按 Coordinator 路由把实例数据请求转发到目标 Relay。

需要独立故障域、日志或资源限制时，在 `apiserver.config` 中设置 `ingress_mode: standalone`。此时渲染结果包含 `adx-apiserver` 与 `adx-ingress` 两个进程，但继续使用相同的 Ingress 服务实现、端口和证书配置。默认模式下不要手工启动 `adx-ingress`，否则会争用 Ingress 监听端口。

当前受支持的部署要求 Ingress 与 API Server 同机：Ingress 到 API Server 的控制请求使用回环 HTTP，API Server 的 `loopback_http` 也拒绝非回环监听。`deployment-ingress-api.yaml` 因此把两者放在同一份本机清单中。若后续需要分开部署，必须先为 Ingress 的 API Server 上游连接补齐 HTTPS 和服务端身份校验，不能直接将当前回环地址替换为远端 IP。

### 推荐启动顺序

1. Redis。
2. Coordinator，确认 `adxctl status` 中进程存活并已发布发现地址。
3. 各 Worker，确认 adxlet 完成对账并上报资源。
4. API Server（默认同时启动 Ingress；显式分进程时再启动独立 Ingress）。
5. 使用公开 SDK 完成创建、执行命令、删除的业务就绪检查。

组件发现允许稍后收敛，但上述顺序能提供更清晰的首次部署日志。所有主机必须使用同一构建的发布包、同一 `redis_url`、同一 `namespace` 和一致的内部通信模式；mTLS 模式还需要相互匹配的证书身份。

## 状态、故障与配置更新

典型状态输出如下：

```json
{
  "ok": true,
  "services": [
    {"id": "coordinator", "role": "coordinator", "pid": 1234, "restarts": 0, "failed": false}
  ]
}
```

- `status` 连接失败：supervisor 未运行，或命令所读部署文件的 `state_dir` 与运行实例不同。
- `deployment already supervised`：同一 `state_dir` 已有 supervisor。
- `package executable missing or not executable`：`package_dir/bin` 不完整、架构错误或权限错误。
- `stop` 失败：至少一个 adxlet Drain 或组件停止未完成；依赖会保留，检查 `state_dir/logs/` 后使用相同命令重试。
- `render` 失败且目录已存在：换一个新的输出目录；工具不会覆盖已有审查证据。

配置和证书在组件启动时读取。`adxctl` 当前没有 `restart` 或热重载子命令。完整 `stop` 会删除本机 Environment，因此不应用它进行需要保留实例的普通证书轮换；此类维护由部署环境按组件重启，并等待 adxlet 对账和路由重新同步。

完整的单机依赖、证书和 SDK 业务就绪步骤见[单机进程部署](standalone.md)，运行环境见[本地 EROFS 与 OCI](runtime-environment.md)。

### 内部通信使用 network 模式

Profile 部署可显式设置 `internal_security: network`：

```yaml
schema_version: 1
profile: standalone
internal_security: network
```

这会将 Coordinator、adxlet、API Server 和 Ingress 的内部 gRPC，以及
Ingress → Relay 数据链路改为明文；组件角色和节点 ID 由请求声明，依赖部署网络隔离，
不再通过证书认证。节点会话、实例归属、租户权限和 API Key 校验仍然执行。
对外 Ingress HTTPS 证书和监听保持不变。省略该配置仍使用 mTLS，连接失败不会自动降级。

使用完整 `services` 配置时，Coordinator、adxlet、Ingress 的 `config.tls` 设置为
`{mode: network}`，API Server 设置 `config.internal_security: network`；Coordinator
公布 `http://` 地址。Ingress 和 Relay 设置
`ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE=network`，并移除内部
`ADX_DATA_PLANE_INGRESS_NODE_TLS_*`、`ADX_DATA_PLANE_RELAY_TLS_*`
及 `ADX_DATA_PLANE_RELAY_MTLS_CLIENT_CA` 环境项；对外 HTTPS 配置保留。
同一条内部链路的两端必须使用一致模式。

### 管理员 Key 的生成、读取与轮换

Key 由部署者生成并写入受保护文件，Coordinator 继续通过
`bootstrap_credentials[].key_file` 读取。配置和渲染结果只包含文件路径。
例如首次部署时生成密钥（已有文件应复用，不要在每次启动时重新生成）：

```sh
install -d -m 0700 /opt/adx/config/secrets
(umask 077; set -C; openssl rand -hex 32 > /opt/adx/config/secrets/admin-key)
```

Coordinator 启动时将配置中的管理员 Key 视为完整目标集合，并在 Redis 中原子更新：
新增 Key 生效、移出的管理员 Key 记录永久撤销标记，租户 Key 保留。
至少配置一个未过期的有效管理员 Key。重复 Key、已撤销 Key、无效文件或
与租户 Key 冲突的配置会导致启动失败，不能通过恢复旧文件重新启用撤销的 Key。
这适用于升级前已登记的管理员 Key，不需要另行迁移目录。

轮换时先准备新的权限为 0600 的密钥文件，原子替换原 `key_file`，再重启
Coordinator；仅修改文件不会触发热重载。部署者从新文件读取 Key 更新客户端，
不要把内容写入服务日志。需要分阶段切换时，先同时配置两个 Key，重启后
更新客户端，再移除旧 Key 并再次重启。保留 Redis 数据。

API Server 和 Ingress 的既有认证缓存可能继续接受旧 Key 直到缓存到期；默认
示例为 10 秒，在途认证另受 RPC 超时限制。轮换不撤销已建立的长连接。
初始管理员 Key 不由租户 Key 删除接口管理，而由上述部署配置集合管理。
