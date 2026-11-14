# ADX 部署 Chart

从 `agent-dx-iac/charts/adx` 整理的独立组件 Helm Chart。部署范围为 ADX
Coordinator、API Server、Activator、Ingress、adxlet + Relay + sandboxd，及可选 Redis。
支持的运行方式是 Linux Kubernetes / CCE、runsc、OBS checkpoint 和 SFS NFSv3。
不包含 DigitalMateGateway、Jiuwen/Orbit 业务入口、账户/OAuth、PostgreSQL 或 LiteLLM。
通用 Agent API 与 Environment 文件入口仍由 Ingress 提供；业务 Template 通过 Agent API 单独发布。

来源为 `agent-dx-iac` 提交 `7385315aacc81d2189e7d8d222953a3b1cd2c232`
及 2026-11-05 工作区的共享模板和启动脚本。本次验证是离线渲染、启动配置测试和打包，
没有执行集群部署或端到端验收。

## 组件与资源归属

每次安装必须显式选择 `deployment_component`，每个组件使用独立 release：

| 参数 | 建议 release | 主要资源 |
| --- | --- | --- |
| `coordinator` | `adx-coordinator` | Deployment、内部 RPC Service、公开 Node 证书投影 |
| `apiserver` | `adx-apiserver` | Deployment、HTTPS Service（8888） |
| `activator` | `adx-activator` | Deployment、HTTP Service（8091） |
| `ingress` | `adx-ingress` | Deployment、共享 HTTPS Service（8443） |
| `adx-node` | `adx-adx-node` | StatefulSet、无头 Service、SFS PV/PVC、Node 出站策略 |
| `budget` | `adx-budget` | 可选 ResourceQuota |
| `dependencies` | `adx-dependencies` | 显式启用的 Redis Deployment、PVC、Service |

资源名保持固定；同一 namespace 只能部署一套组件，不能通过不同 release 名复用同一个组件。
Chart 不创建 Namespace、Secret、TLS 公网入口或 Agent Template；`control` 合集不作为安装选项。
独立发布控制组件不会修改 Node 的 StatefulSet、镜像或数据卷。

## 参数与镜像准备

本地渲染和打包依赖 Helm 3；组件容器中的启动脚本需要 Python 3.10+。
Kubernetes 版本要求为 1.27+，用于 StatefulSet 起始 ordinal 和 PVC 保留策略。
集群须支持 NetworkPolicy；实际限制效果取决于 CNI。

```bash
helm lint charts/adx --strict --namespace adx-example
helm template adx-coordinator charts/adx --namespace adx-example \
  --set deployment_component=coordinator
helm package charts/adx --destination out/charts
```

命令从 agent-dx 根目录执行，不访问集群。解压 Chart 后，也可在 Chart 目录中将
`charts/adx` 替换为 `.` 使用同样的 Helm 命令。

复制 [values.yaml](values.yaml) 到自己的部署配置目录。
默认值仅用于渲染，镜像和 EROFS 摘要全为零，存储类、OBS 桶、SFS、网络地址均需替换。
不提供预设的可拉取镜像。配置项的主要约束如下：

- `namespace` 必须与 Helm 的 `--namespace` 一致；每个环境独立设置
  `tenant`、`platform_namespace`、`agent_namespace` 和 Redis 状态库。
- `images` 五个组件都固定为 `repository@sha256:<digest>`；不要使用浮动标签。
  控制镜像必须包含 Python 3、`/opt/adx/bin/adxctl` 及对应组件二进制；
  Ingress 必须启用 `agent-api` 构建 feature，才能提供通用 Agent/Environment 入口。
  Node 镜像额外包含 `/opt/adx/backend/{sandboxd,runsc,distill_fs}`、mount/unshare/findmnt、
  运行后端需要的网络/镜像工具，以及 `runtime_payload_path` 指定的 EROFS。
  使用当前源码构建的完整发布包组装镜像；Chart 不下载二进制或构建镜像。
- `runtime_payload_path` 必须是 Node 镜像中实际存在的 `/opt/adx/runtime/<sha256>.erofs`，
  所有组件引用同一路径。载荷内的 Execd 与 Node 版本需配套；已有 Environment 的旧路径不能直接丢弃。
- `dependencies.redis_host` 为 `host:port`，平台使用 Redis DB 0，Agent 使用 DB 1。
  Redis 凭据只在容器启动时从 Secret 读取。
- `node_identities.capacity` 为 1–100，预置 `adx-node-0` 至 `adx-node-<capacity-1>` 身份。
  `node.start_ordinal` 必须小于 capacity；`node.runtime_control_port` 必填，示例为 17002。
- `node.data_storage: pvc` 为示例默认。`emptyDir` 仅适合接受 Pod 重建丢失节点缓存/运行状态的场景。
  `node.sfs` 配置实际 NFSv3 共享，启动会核验来源和 `rw,hard,proto=tcp,lookupcache=pos`。
  `storage_domains` 与 `node.storage_mounts` 使用相同 backend/root，租户路径使用独立 namespace 前缀。
- `node.checkpoint_storage` 当前只支持 HTTPS OBS 桶域名、虚拟主机寻址；缓存根目录必须在 `/work/`。
  可选 `checkpoint_reclaimers` 配置 Coordinator 对相同共享前缀的回收，不会自动从 Node 配置推断。
- 修改 `node.dns_address`、sandbox CIDR、`sandbox_network.internal_cidrs/egress_allow` 以匹配实际网络。
  Node 出站策略默认只额外允许本 namespace 内 Redis、Coordinator 和 API Server；
  外部 Redis、DNS、私网 OBS 或其他必要服务必须显式补充允许地址/端口。
  不要照搬示例 IP；应核对 sandbox 地址范围与基础设施 CIDR 无冲突。

Node 需要 privileged 容器，在自己的 mount/cgroup namespace 内运行；不使用 host PID/network，
不修改宿主 sysctl。宿主需提前具备 cgroup、overlay/EROFS、loop device、NFS 客户端及 bridge netfilter 支持，
`bridge-nf-call-iptables` 必须已开启。命名空间准入策略必须允许该 Node 工作负载。

## Secret 合同

Secret 必须在安装前由运维准备，Chart 不生成密钥，也不把凭据写入 values 或 ConfigMap。
生成的含凭据运行配置只在容器 `/work` 中以 0600 权限保存。

| values 引用 | Secret 内容 |
| --- | --- |
| `secrets.runtime` | `ca.pem`；`coordinator.pem/key/der`、`apiserver.pem/key/der`、`ingress.pem/key/der`；`admin-key`、`tenant-key`、`redis-key`、`execd-token`、`service-token` |
| `secrets.registry` | `kubernetes.io/dockerconfigjson` 类型，键 `.dockerconfigjson`；同时供镜像拉取和 sandboxd 使用 |
| `secrets.obs` | `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY` |
| `node_identities.private_secret` | 每个 ordinal 的 `adx-node-N.pem`、`adx-node-N.key` |
| `node_identities.public_secret` | 每个 ordinal 的 `adx-node-N.der` |
| `managed_dependencies.redis.secret` | 仅启用受管 Redis 时需要 `redis.acl` |

各角色仅投影其所需键；Coordinator 只收到 Node 公钥，Node 通过 `subPathExpr: $(POD_NAME)`
仅挂载自己的私钥目录。组件证书使用相同 CA；内部 RPC 验证名为 `adx.internal`，
API Server 证书还需匹配 `apiserver` DNS，公开 Ingress 证书需覆盖实际访问域名。
Secret 更新需按角色重启/发布使启动读取生效；更新 Node 证书前先安排实例排空或暂停。

可选 Redis 使用单副本持久 AOF。`redis.acl` 必须配置与 `redis-key` 相同的密码，
并允许平台/Agent 所需命令和键空间。默认按 UID/GID 10001 运行；自选镜像必须兼容该非 root 身份。
`use_image_entrypoint: true` 使用镜像入口，否则调用 `/opt/adx/bin/redis-server`。
已有 Redis 保持 `managed_dependencies.enabled: false`，应用 release 不接管它。

## 安装与升级

以下命令使用准备好的独立配置；所有集群操作显式给出 kubeconfig、context、namespace。
先建立 namespace/Secret、准备 Redis/SFS/OBS，再发布组件。

```bash
export ADX_KUBECONFIG=/secure/path/kubeconfig
export ADX_CONTEXT=your-context
export ADX_NAMESPACE=your-namespace
export ADX_VALUES=/secure/path/values.yaml

helm lint charts/adx --strict --namespace "$ADX_NAMESPACE" -f "$ADX_VALUES"
helm template adx-adx-node charts/adx --namespace "$ADX_NAMESPACE" -f "$ADX_VALUES" \
  --set deployment_component=adx-node > /tmp/adx-node-review.yaml

# 只在需要由本 Chart 新建 Redis 时执行；先在 values 中配置并启用这两个开关。
# helm upgrade --install adx-dependencies charts/adx \
#   --kubeconfig "$ADX_KUBECONFIG" --kube-context "$ADX_CONTEXT" --namespace "$ADX_NAMESPACE" \
#   -f "$ADX_VALUES" --set deployment_component=dependencies --wait --timeout 10m

for component in coordinator apiserver activator ingress; do
  helm upgrade --install "adx-$component" charts/adx \
    --kubeconfig "$ADX_KUBECONFIG" --kube-context "$ADX_CONTEXT" --namespace "$ADX_NAMESPACE" \
    -f "$ADX_VALUES" --set "deployment_component=$component" --wait --timeout 10m
done

helm upgrade --install adx-adx-node charts/adx \
  --kubeconfig "$ADX_KUBECONFIG" --kube-context "$ADX_CONTEXT" --namespace "$ADX_NAMESPACE" \
  -f "$ADX_VALUES" --set deployment_component=adx-node --wait --timeout 10m
```

首次 Node 安装由 Kubernetes 默认创建 1 个副本，Chart 不声明 `replicas`。
扩容必须满足 `start_ordinal + replicas <= capacity`，扩容前准备完整身份池；使用显式
`kubectl --kubeconfig ... --context ... --namespace ... scale statefulset/adx-node --replicas=N`。
Ingress Service 只提供集群内 HTTPS；外部 TLS 入口、证书签发和域名由部署环境管理。
API Server 保留独立 8888 HTTPS 入口。

日常升级只运行目标组件的那条 `helm upgrade`。Node 升级会影响其沙箱，先排空或暂停实例，
再核对镜像/载荷/存储一致性。Helm `--wait` 只表示 Kubernetes 就绪，不等同于 Environment 创建、
命令执行、Agent 激活和 pause/resume 通过；这些需安装后单独验收。

StatefulSet PVC 在缩容/删除时保留；SFS PV/PVC 和受管 Redis PVC 具有 `helm.sh/resource-policy: keep`。
卸载 release 不清理这些数据，重装时也不要强行接管其他 release 的已有资源。
这套 Chart 不提供 IaC 仓库中的环境初始化、业务模板发布或现网升级 CLI。
