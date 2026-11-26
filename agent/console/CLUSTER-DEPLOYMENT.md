# Console 集群部署操作手册（adx-wld）

本手册使用 **agent-dx 仓内的源码、镜像构建入口和 Helm Chart**，不依赖外部 IaC 仓库里的 Console 文件。目标是在已有 `adx-wld` namespace 中部署独立 Console，并只读复用该环境的共享存储。本文是部署操作流程，不表示已执行集群 Console 发布。

## 1. 部署结构与已确认前提

```text
浏览器 https://localhost:28443
  -> kubectl port-forward
  -> Service adx-console:8443
  -> HTTPS Nginx sidecar
  -> 同 Pod Console HTTP 127.0.0.1:8088
       -> Gateway HTTPS（Agent 管理和终端 WS）
       -> Platform HTTPS（Environment 和 Volume）
       -> 只读 PVC（文件列表、预览、下载）
```

- namespace 是 `adx-wld`，租户 ID 是 `wld`，两者不能混用。
- 已确认 Service：`ingress:8443`、`apiserver:8888`；都使用 HTTPS。
- 已确认 PVC：`sfs-workspaces`，RWX，Bound，PV 为 `adx-wld-sfs-workspaces`。
- PV 的 NFS source 为 `9deb8fc7-c941-11f1-b607-fa1652d6f62f.sfsturbo.internal:/`，集群内解析到 `192.168.0.2`，现有 Node 以 NFS v3 挂载。
- 当前 WSL 无法解析该内部域名，访问上述 IP 的 111/2049 TCP 超时。因此本文让 Pod 挂载 PVC，不要求 WSL 挂载 NFS。
- 当前会话注册表是单进程内存，Chart 固定单副本、Recreate 更新；更新或回滚会中断终端并要求重新登录。
- Accounts 管理用户接口曾实测 404，必须发布接口和 grant 后另验业务用户页。模板、终端、只读文件不等于 Accounts 业务查询通过。

## 2. 仓内文件清单

| 文件 | 用途 |
|---|---|
| `build/images/Dockerfile.console` | 前端测试/构建及 Rust release 镜像构建 |
| `agent/console/config.example.json` | 应用配置模板 |
| `agent/console/deploy/Chart.yaml` | Console Helm Chart |
| `agent/console/deploy/values.yaml` | 镜像、资源、Secret 引用、PVC 参数 |
| `agent/console/deploy/templates/resources.yaml` | Deployment、Service、HTTPS 代理配置 |
| `agent/console/terminal_helper.py` | 目标工作负载内的终端 helper，不在 Console 容器内安装 |
| `agent/console/TERMINAL.md` | 终端 profile、身份和 helper 合同 |
| `agent/console/BUSINESS.md` | Accounts 管理接口和业务身份映射 |
| `agent/console/CLUSTER-DEPLOYMENT.md` | 本手册的仓内版本 |

环境配置和凭据在执行时生成到 `$HOME/adx-console-cluster`，不要把带摘要/凭据的实际配置提交到仓库。仓内 Chart 不创建 PostgreSQL、Redis、Gateway、Platform、PVC 或工作负载运行时；这些是已有环境前提。

## 3. 工具与统一操作变量

在 Linux/WSL Bash 中执行，需要 Docker、Helm、kubectl、OpenSSL、Python 3。仓库不在下列路径时修改第一行。镜像构建需要 Node/npm/Cargo registry 网络；不要把本机挂载的 debug 二进制当作发布镜像。

```bash
cd /mnt/c/code/agent-dx
export KUBECONFIG=/home/wld/beijing/openjiuwen-adx-test-kubeconfig.yaml
export CONTEXT=externalTLSVerify
export NAMESPACE=adx-wld
export RELEASE=adx-console
export WORK="$HOME/adx-console-cluster"
mkdir -p "$WORK/config" "$WORK/credentials" "$WORK/tls"
chmod 700 "$WORK" "$WORK/config" "$WORK/credentials" "$WORK/tls"
# 后文 k 函数始终带明确 context/namespace；新终端需重新定义。
k() { kubectl --kubeconfig "$KUBECONFIG" --context "$CONTEXT" -n "$NAMESPACE" "$@"; }
k get svc ingress apiserver
k get pvc sfs-workspaces
```

`WORK` 是新部署专用工作目录，不会自动读取 `/tmp/opencode/console-login-validation`，也不会继承旧 operator token。

## 4. 构建和发布镜像

```bash
export IMAGE_REPOSITORY=swr.cn-north-4.myhuaweicloud.com/openjiuwen-online-test/adx-agent-console
export IMAGE_TAG="console-$(date -u +%Y%m%dT%H%M%SZ)-$(git rev-parse --short HEAD)"
docker build -f build/images/Dockerfile.console -t "$IMAGE_REPOSITORY:$IMAGE_TAG" .
# 使用已有受控登录方式登录 SWR，勿将 registry 密码写进文档或仓库。
docker push "$IMAGE_REPOSITORY:$IMAGE_TAG"
docker image inspect "$IMAGE_REPOSITORY:$IMAGE_TAG" --format '{{json .RepoDigests}}'
```

构建上下文包含当前工作区；先核对 `git status`，明确本次包含哪些修改。使用新唯一 tag，不覆盖历史 tag；记录实际 digest。集群节点若不能拉取默认 `nginx:stable-alpine`，先镜像同步到允许的 registry，并修改下面 `proxyImage`；生产可使用批准的 digest 引用。

若 SWR 私有仓库需要拉取凭据，使用环境已有、权限正确的 imagePullSecret，或由管理员创建。该 Secret 必须位于 `adx-wld`。不在 Chart 中内置 registry 凭据。

## 5. 先确认上游证书名称，不关闭 TLS 校验

优先使用完整 Service DNS，但证书 SAN 必须包含所用名称：

```text
ingress.adx-wld.svc.cluster.local:8443
apiserver.adx-wld.svc.cluster.local:8888
```

先复制本环境受控凭据（源文件已存在于当前机器；其他机器自行准备）：

```bash
cp "$HOME/adx-wld/ca.pem" "$WORK/credentials/ca.pem"
cp "$HOME/adx-wld/tenant-key" "$WORK/credentials/tenant-key"
chmod 600 "$WORK/credentials/ca.pem" "$WORK/credentials/tenant-key"
```

通过已有本机 49443 Gateway 转发查看证书，另开终端临时转发 Platform（端口未占用时）：

```bash
k port-forward service/apiserver 58888:8888 --address=127.0.0.1
```

在另一个已设置 WORK 的终端检查服务端证书 SAN：

```bash
openssl s_client -connect 127.0.0.1:49443 -servername ingress.adx-wld.svc.cluster.local \
  -CAfile "$WORK/credentials/ca.pem" -showcerts </dev/null 2>/dev/null \
  | openssl x509 -noout -subject -ext subjectAltName
openssl s_client -connect 127.0.0.1:58888 -servername apiserver.adx-wld.svc.cluster.local \
  -CAfile "$WORK/credentials/ca.pem" -showcerts </dev/null 2>/dev/null \
  | openssl x509 -noout -subject -ext subjectAltName
```

如果 49443 未转发，先在独立终端执行 `k port-forward service/ingress 49443:8443 --address=127.0.0.1`。上面的命令用于查看 SAN，不等价于所选 DNS 名验证成功；发布后仍要验证 Console 请求。

选择方式：

1. SAN 包含 Service DNS：直接使用该 DNS。
2. SAN 只含其他内部名称：确认该名称是否已有集群 DNS，使用该名称和 Service 端口；没有 DNS 时可临时在 values 的 `hostAliases` 将**证书覆盖的名称**指向对应 Service ClusterIP。用 `k get svc ingress apiserver -o wide` 查询实时 IP，禁止照抄旧 IP。
3. 两个服务需要相同主机名但不同 Service IP：`hostAliases` 无法按端口区分，需修正证书/集群 DNS 或增加部署代理；不要配置相互冲突的 hostAliases。

证书签发/轮换属于已有 Gateway/Platform 环境操作，不在 Console Chart 中替换。禁止使用 `-k` 或关闭校验来绕过上游 SAN 问题。

## 6. 生成应用配置和独立登录凭据

先填写上一步确认的实际 URL。以下是 Service DNS 示例，**未确认 SAN 前不可直接作为验收配置**：

```bash
export GATEWAY_URL=https://ingress.adx-wld.svc.cluster.local:8443
export PLATFORM_URL=https://apiserver.adx-wld.svc.cluster.local:8888
python3 - <<'PY'
from pathlib import Path
import getpass
import hashlib
import json
import os
import secrets

root = Path(os.environ['WORK'])
path = root / 'config/config.json'
if path.exists():
    raise SystemExit('配置已存在；请编辑现有文件，不要重新初始化凭据')
config = json.loads(Path('agent/console/config.example.json').read_text())
digest = lambda value: hashlib.sha256(value.encode()).hexdigest()
tokens = {}
for label in ('operator', 'wld'):
    value = getpass.getpass(f'设置 {label} Console 登录 token: ')
    if not value or value != getpass.getpass('再次输入: '):
        raise SystemExit('token 不能为空且两次必须一致')
    tokens[label] = value
config.update(listen='127.0.0.1:8088', public_origin='https://localhost:28443',
              upstream_ca_file='/run/secrets/ca.pem', shared_files=None,
              terminal_profiles_file=None)
config['agent_api']['base_url'] = os.environ['GATEWAY_URL'].rstrip('/') + '/api/agent/v2'
config['apiserver']['base_url'] = os.environ['PLATFORM_URL'].rstrip('/')
config['ingress']['base_url'] = os.environ['GATEWAY_URL'].rstrip('/')
config['platform_admins'] = [{'admin_id': 'operator', 'display_name': 'Operator',
                             'token_sha256': digest(tokens['operator'])}]
config['tenants'] = [{
    'tenant_id': 'wld', 'display_name': '北京 adx-wld', 'type': 'digitalmate',
    'admin_token_sha256': digest(tokens['wld']), 'agent': {'enabled': True},
    'upstream_token_file': '/run/secrets/tenant-key',
    'business': {
        'accounts_api': {'base_url': os.environ['GATEWAY_URL'].rstrip('/') + '/api/accounts/v1/admin',
                         'timeout_ms': 30000, 'token_file': '/run/secrets/tenant-key'},
        'bindings': [
            {'kind': 'digitalmate', 'template': 'digitalmate',
             'ref': {'kind': 'alias', 'name': 'production'}},
            {'kind': 'orbit', 'template': 'orbit',
             'ref': {'kind': 'alias', 'name': 'stable'}}]}}]
with path.open('x') as file:
    path.chmod(0o600)
    json.dump(config, file, ensure_ascii=False, indent=2)
session = root / 'credentials/console-session'
with session.open('xb') as file:
    session.chmod(0o600)
    file.write(secrets.token_bytes(32))
print('已生成配置和 session secret。请在密码管理器保存刚才输入的两个登录 token。')
PY
```

这里 Console 租户登录 token 与上游 tenant-key 独立；浏览器填前者，Console 请求上游使用后者。若需沿用原登录凭据，受控复制原 `platform_admins` 和匹配租户的 `admin_token_sha256`，而不是根据摘要猜原文。不要直接上传整个本机旧配置，它可能含 localhost、旧转发端口或宿主机特有名称。

### 应用配置项含义与修改规则

| 配置项 | 含义 / 如何改 |
|---|---|
| `listen` | Console HTTP 监听；本 Chart 同 Pod 代理，设 `127.0.0.1:8088` |
| `public_origin` | 浏览器 HTTPS Origin，必须与访问地址一致，无业务路径；本机转发用 `https://localhost:28443`，换域名/端口需同步修改 |
| `upstream_ca_file` | 容器内上游 PEM CA 路径，示例 `/run/secrets/ca.pem`；非浏览器证书 |
| `agent_api.base_url` | Gateway Agent 管理前缀，末尾 `/api/agent/v2` |
| `agent_api.timeout_ms` | Agent 管理 HTTP 超时，默认 10000 毫秒，可按实际延迟调大 |
| `apiserver.base_url` / `timeout_ms` | Platform HTTPS 根地址和请求超时，URL 不加 `/api/agent/v2` |
| `ingress.base_url` | Gateway HTTPS 根地址，终端 WS 用它，不能填写业务 Harness WS 路径 |
| `ingress.execd_port` | 声明的 Execd 端口配置，默认 49983；核对当前 Gateway direct 路由及工作负载 Execd 实际端口，不沿用临时宿主机端口 |
| `ingress.connect_timeout_ms` | 终端上游连接超时，毫秒 |
| `cookie_secret_file` | session 密钥文件，至少 32 字节；本手册 `/run/secrets/console-session` |
| `session_ttl_seconds` | session 有效期，默认 28800 秒；重启仍会使内存会话失效 |
| `terminal_idle_timeout_seconds` | 终端空闲超时，默认 600 秒 |
| `terminal_lost_timeout_seconds` | 终端失联超时，默认 30 秒 |
| `terminal_profiles_file` | 终端 profile JSON 文件；未准备时为 null，详见第 9 节 |
| `file_preview_bytes_limit` | 文本预览最大字节数，默认 262144，不是上传限制 |
| `shared_files` | 未启用为 null；启用后配置映射路径和目录条目上限，详见第 8 节 |
| `audit_target` | 可选审计 target 配置；未有部署约定时保留源码默认，不将它当日志服务器地址 |
| `platform_admins[].admin_id` / `display_name` | 平台登录 ID / 展示名称；示例 operator |
| `platform_admins[].token_sha256` | 原始平台 token 的 SHA-256 十六进制摘要，不是原文 |
| `tenants[].tenant_id` / `display_name` | 上游真实租户 ID / 展示名称；本环境 wld |
| `tenants[].type` | `standard` 无 business；`digitalmate` 必须配置 business，不能只改 type |
| `tenants[].admin_token_sha256` | 租户 Console 登录 token 摘要 |
| `tenants[].agent.enabled` | 当前配置校验要求 true |
| `tenants[].upstream_token_file` | 此租户访问 Gateway/Platform 的 Bearer 文件，容器内绝对路径 |
| `business.accounts_api` | HTTPS 管理根 URL、timeout_ms、token_file；需 Gateway 实际发布并有 grant |
| `business.bindings[].kind` | `digitalmate`、`orbit` 或 `jiuwen`，同租户不得重复 kind |
| `business.bindings[].template` | 实际已发布模板名，不是 Environment ID |
| `business.bindings[].ref` | `kind` 为 alias/version（单数），`name` 为实际别名或版本号字符串；用于映射校验，用户关联展示按 binding_id 查询到的全部实际引用 |
| `business.bindings[].agent_type` | Jiuwen 规则必填的真实 agent_type，其他类型不需虚构 |

用户详情仅展示 `business.bindings` 中配置的业务种类；模板目录中存在 Jiuwen 不会自动补齐业务身份映射。
本环境 Gateway 的 Jiuwen 映射为 template `jiuwen`、agent_type `assistant`、alias `stable`，
需在 Console 同步配置。尚未创建的关联不展示版本/别名；已有 Binding 展示全部实际引用，
查询失败显示无法确认并保留此前已验证的引用。完整配置示例见 [业务查询](BUSINESS.md)。

JSON 使用严格字段解析，不添加自定义字段，不使用注释。配置变更后必须重启 Pod 加载。

## 7. 浏览器 TLS、Kubernetes Secrets 与 Helm 首次发布

本机端口转发访问时可生成 localhost 自签名测试证书；生产域名使用组织签发、SAN 匹配该域名的证书。两类证书不要和上游 CA 混淆。

```bash
openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 7 \
  -keyout "$WORK/tls/tls.key" -out "$WORK/tls/tls.crt" \
  -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1'
chmod 600 "$WORK/tls/tls.key"
k create secret generic adx-console-config \
  --from-file="$WORK/config" --dry-run=client -o yaml | k apply -f -
k create secret generic adx-console-credentials \
  --from-file="$WORK/credentials" --dry-run=client -o yaml | k apply -f -
k create secret tls adx-console-browser-tls \
  --cert="$WORK/tls/tls.crt" --key="$WORK/tls/tls.key" \
  --dry-run=client -o yaml | k apply -f -
```

在 `$WORK/values.yaml` 保存环境 values（修改 repository/tag 为第 4 节实际值）：

```yaml
image:
  repository: swr.cn-north-4.myhuaweicloud.com/openjiuwen-online-test/adx-agent-console
  tag: REPLACE_WITH_UNIQUE_TAG
  pullPolicy: IfNotPresent
imagePullSecrets: []
proxyImage: nginx:stable-alpine
configSecret: adx-console-config
credentialsSecret: adx-console-credentials
tlsSecret: adx-console-browser-tls
sharedStorage:
  enabled: false
  claimName: sfs-workspaces
  mountPath: /srv/adx-shared
hostAliases: []
```

| Helm 参数 | 含义 / 修改规则 |
|---|---|
| `image.repository` / `tag` | 完整 registry 仓库和唯一版本，不留占位值 |
| `image.pullPolicy` | 默认 IfNotPresent；唯一 tag 避免节点复用旧镜像 |
| `imagePullSecrets` | 例如 `[{name: swr-pull}]`，引用已有同 namespace Secret |
| `proxyImage` | Nginx 镜像，集群需能拉取；可换为受控 registry/digest |
| `configSecret` | 包含 config.json 及可选映射/profile 文件的 Secret 名 |
| `credentialsSecret` | 包含 tenant-key、ca.pem、console-session 的 Secret 名 |
| `tlsSecret` | 包含 tls.crt/tls.key 的 Secret 名 |
| `resources` / `proxyResources` | 两个容器 requests/limits；默认 Console 100m/256Mi 请求、1 CPU/1Gi 上限，代理 20m/32Mi 请求、200m/128Mi 上限；按实际监控调节 |
| `sharedStorage.enabled` | 是否将 PVC 只读挂入 Console；先 false 验登录，再按第 8 节启用 |
| `sharedStorage.claimName` | 已有同 namespace PVC 名，本环境 sfs-workspaces |
| `sharedStorage.mountPath` | Console 内挂载点，必须与映射 read_only_root 完全一致 |
| `hostAliases` | 经 SAN 确认的名称到实际 Service IP 映射；优先使用正常集群 DNS |

```bash
helm lint agent/console/deploy -f "$WORK/values.yaml"
helm template "$RELEASE" agent/console/deploy -n "$NAMESPACE" \
  -f "$WORK/values.yaml" > "$WORK/rendered.yaml"
k apply --dry-run=server -f "$WORK/rendered.yaml"
helm upgrade --install "$RELEASE" agent/console/deploy \
  --kubeconfig "$KUBECONFIG" --kube-context "$CONTEXT" -n "$NAMESPACE" \
  -f "$WORK/values.yaml" --wait --timeout 5m
k get pods -l app.kubernetes.io/name=adx-console
k logs deployment/adx-console -c console
k logs deployment/adx-console -c https
```

首次先保持 shared_files=null、sharedStorage.enabled=false，验证登录/模板，随后启用文件能力。此状态文件面板提示不支持是预期的未配置状态，不能当作最终文件验收通过。Console readiness 经 HTTPS 代理检查 /console/，代理另有 TCP 探针；它们不证明上游权限或业务功能。

## 8. 启用只读文件：先查 Volume，再配映射

2026-11-20 的 adx-wld 实施证据、实际身份/映射和验证边界见
[集群验证记录](CLUSTER-VALIDATION-20261120.md)。工作区若为 UID 10001 的 0700，
默认 UID 65532 无读取权限；通过 values `securityContext.runAsUser/runAsGroup=10001`
使用经核实的非 root 身份，保留 `fsGroup=65532` 读取 Secrets，不修改共享数据权限。
此设置仅适用于已核实使用该 UID 的工作区。复部署脚本通过 `VALUES_FILE` 传入该覆盖文件。
Console 镜像需包含 Platform Volume API base64 data 信封解码修复。

### 8.1 核对真实版本与 Volume

通过已登录 Console 的 Network 或管理 API 查询目标 Binding 和其 current/secondary 实际版本。以页面示例为线索：`wld / digitalmate / versions / 3 / terminal-v3-20261118`，但不能把页面 ref 当作 current 版本的唯一证据。

需要确认：模板渲染后挂载有 `volume_id`；Binding 的 `volumes` 中记录同 ID 的 UID；Platform `GET /api/environment/v1/volumes/{id}` 返回的 `uid`、`spec.tenant_id`、`spec.source` 对应它。通过受控 API 客户端发 Bearer，不在截图或日志中输出 Key/Binding 参数敏感值。

如果没有 volume_id 或 UID，先修正仓内实际模板发布源并发布新不可变版本，按 Agent 正常流程创建/切换 Binding；不要手工伪造 UID 或覆盖历史版本。此时 Console 配置不是唯一修复项。

### 8.2 启用 PVC，观察真实 mount source

先将 values 的 sharedStorage.enabled 改为 true，保持应用 shared_files=null，按第 7 节 helm upgrade 发布。然后读取 Console 的挂载信息：

```bash
k exec deployment/adx-console -c console -- id
k exec deployment/adx-console -c console -- cat /proc/self/mountinfo
```

找到 mount point 为 `/srv/adx-shared` 的行：分隔符 ` - ` 右边第二个字段是 source，左侧 mount options 必须含 `ro`。不要只根据 PV 的 server/path 猜；CSI/NFS 实际呈现可能不同。进程应为非 root（默认 65532）。此步骤只观察，不执行 chmod/chown 修复共享数据权限；若 UID 65532 无读取/遍历权限，由存储管理员按实际租户目录调整 ACL/权限。

### 8.3 新增映射并开启应用能力

创建 `$WORK/config/storage-mappings.json`，下面仅为结构示例，替换全部尖括号字段：

```json
{
  "revision": "adx-wld-files-v1",
  "domains": [
    {
      "storage_domain": "<Volume spec.source 中的真实 storage_domain>",
      "backend_id": "<部署定义的稳定后端标识>",
      "read_only_root": "/srv/adx-shared",
      "tenant_paths": {"wld": "<wld 在该存储根内的相对路径前缀>"},
      "mount_source": "<mountinfo 中真实 source>"
    }
  ]
}
```

- `revision`：非空映射修订标识，修改映射时更新，使旧文件上下文失效。
- `storage_domain`：必须精确匹配 Volume 的目录 source；不得重复配置同一个域。
- `backend_id`：非空的部署后端身份标识；不等于自动查询出来的 PVC UID。
- `read_only_root`：绝对路径且为实际独立只读挂载点。
- `tenant_paths.wld`：相对路径，不能以 `/` 开头，不含空段、`.`、`..`；Volume path 必须位于此前缀下。
- `mount_source`：与容器 mountinfo 精确一致；不是随便填写的宿主机目录。

Volume path 是相对于该存储域根目录的路径；若 PVC 挂了整个 SFS 根，则应以该根解释，不能把 volume path 再拼接一次或遗漏 `adx-wld/workspaces` 层级。实际值必须由 Volume 和存储域配置共同确认。

编辑 `$WORK/config/config.json`：

```json
{
  "shared_files": {
    "mappings_file": "/etc/adx-console/storage-mappings.json",
    "directory_entries_limit": 1000
  }
}
```

以上是配置片段，不是完整 JSON。`directory_entries_limit` 是单目录条目上限，避免无界列目录。更新 config Secret 并重启（第 11 节）。文件读取还依赖 Platform Volume 查询网络和授权；只读挂载存在不等于查询必定成功。

## 9. 终端配置：Console profile + 工作负载 helper

Console 镜像不替目标 Environment 安装 helper。目标运行时镜像/rootfs 必须含仓内 `agent/console/terminal_helper.py`，按 `TERMINAL.md` 配套打包和发布。若环境已经安装并通过验证，不要为迁移 Console 重建 Node 或替换工作负载。

在 `$WORK/config/terminal-profiles.json` 保存数组，每个实际支持的不可变版本配置一条，例如：

```json
[
  {
    "tenant": "wld",
    "template": "digitalmate",
    "version": "3",
    "revision": "digitalmate-3-profile-1",
    "backend": "pty_adapter",
    "uid": 1000,
    "gid": 1000,
    "supplementary_groups": [],
    "home": "/home/app",
    "shell": "/bin/sh",
    "adapter": "/opt/adx/terminal-helper",
    "cwd_directory": "/home/app"
  }
]
```

上述身份/路径都是示例，必须以目标 Environment 实际用户、目录权限、helper 安装位置修改；不要为了符合示例创建错误用户。`cwd_directory` 可替换为实际 `cwd_mount_target`，两者的用法见 TERMINAL.md。`tenant/template/version` 必须匹配当前或 secondary slot 实际版本，`revision` 用于 profile 修订，`backend` 仅使用当前支持的 `pty_adapter`。

配置 `terminal_profiles_file=/etc/adx-console/terminal-profiles.json`。确认 Gateway 固定 Environment direct PTY 路由和实际 Execd 端口配套；不能把宿主机的 50090 等临时转发端口当作集群运行时端口。更新 Secret 和重启后，单独验证真实终端握手、输入、resize、关闭和退出。

## 10. 浏览器访问与验收清单

本机原 Docker HTTPS 代理若占用 28443，先停止其代理容器，或为集群另选端口并同步 public_origin 与浏览器证书。不要无确认地停止其他服务。

```bash
ss -ltnp 'sport = :28443'
# 端口可用后持续运行：
k port-forward service/adx-console 28443:8443 --address=127.0.0.1
```

打开 `https://localhost:28443/console/#/login`，信任本次测试证书。平台登录填 operator 和第 6 节设置的 token；租户登录填 wld 和其独立 Console token。

| 验收项 | 通过条件 |
|---|---|
| HTML | `/console/` 返回 200，不被 Nginx 静态目录覆盖 |
| 登录/session | 正确 token 200，错误 token 401，错误 Origin 403，退出后 session 401 |
| 模板/Binding | 目录、版本、Alias、指定 Binding 正确返回，不以 session 200 代替 |
| Platform | 查询实际 Environment 和 Volume 成功，区分网络/TLS与 401/403 授权错误 |
| 文件 | mount 列表、目录、预览、下载成功；mountinfo 为 ro，越界和符号链接拒绝；上下文变更后旧请求拒绝 |
| 终端 | 对真实 Running Environment 完成 PTY 连接、输入、resize、关闭；profile/helper 匹配 |
| Accounts | 用户查询成功且有管理 grant；404 要发布路由，403 要核对 grant，不反复替换 Key |

可通过 `curl --cacert "$WORK/tls/tls.crt" https://localhost:28443/console/ -o /dev/null -w '%{http_code}\n'` 检查页面。浏览器返回 `return=` 是前端导航参数，不解决存储或终端部署问题。

## 11. 配置更新、token 重置、回滚与卸载

编辑 config/mappings/profiles 后上传同名 config Secret；凭据更新时上传 credentials Secret，随后重启：

```bash
k create secret generic adx-console-config \
  --from-file="$WORK/config" --dry-run=client -o yaml | k apply -f -
# 仅当 credentials 内容实际更新时执行：
k create secret generic adx-console-credentials \
  --from-file="$WORK/credentials" --dry-run=client -o yaml | k apply -f -
k rollout restart deployment/adx-console
k rollout status deployment/adx-console --timeout=180s
```

Secret 不应存放无关文件或旧配置备份，否则 --from-file 会一并上传。Helm values 修改后重新 helm upgrade；proxy ConfigMap 的 subPath 挂载及应用配置都需要 Pod 重建，helm upgrade 后若未触发 Pod 变更同样执行 rollout restart。

重置 operator：只更新 `$WORK/config/config.json` 中其 token_sha256，计算方法和隐藏输入流程见 `LOGIN-VALIDATION.md` 第 3.1 节，将脚本 path 改为本手册的实际文件路径，再上传 config Secret 和 rollout restart；不要照抄本机 Docker restart 命令。若原始 token 遗失，摘要不可还原。

```bash
helm history "$RELEASE" --kubeconfig "$KUBECONFIG" --kube-context "$CONTEXT" -n "$NAMESPACE"
# 根据 history 选择实际修订号：
helm rollback "$RELEASE" REPLACE_WITH_REVISION \
  --kubeconfig "$KUBECONFIG" --kube-context "$CONTEXT" -n "$NAMESPACE" --wait
```

外部 Secret 不随 Helm 回滚，必须同步恢复与旧镜像匹配的受控配置，并重启。卸载只移除本 Chart 的 Console 资源：

```bash
helm uninstall "$RELEASE" --kubeconfig "$KUBECONFIG" --kube-context "$CONTEXT" -n "$NAMESPACE"
```

独立创建的三个 Secret 和本地 WORK 保留，确认不再使用后由管理员单独清理；不要删除整个 namespace、已有 PVC 或共享数据。

## 12. 部署、卸载与重新部署速查

以下命令在仓库根目录执行，先完成第 3 节的变量和 `k` 函数设置。
首次部署按第 4–7 节准备镜像、配置、凭据、证书和 `$WORK/values.yaml`；
共享文件与终端分别按第 8、9 节配置。新机器不能只复制一个 config.json 就完成部署。

### 12.1 配置在哪里，如何生效

```text
部署机 $WORK/config/config.json（通常 /root/adx-console-cluster/config/config.json）
  -> namespace adx-wld 中的 Secret adx-console-config，键 config.json
  -> Pod 内 /etc/adx-console/config.json
  -> Console 启动时读取
```

本地绝对路径不是集群节点路径；无需把本地目录复制到每个节点。
只修改本地文件不会更新运行中的服务；必须上传 Secret 并重启。
`config` 目录还可能包含 `storage-mappings.json` 和 `terminal-profiles.json`，更新时保留这些文件。
凭据放在独立的 `adx-console-credentials`，浏览器证书放在 `adx-console-browser-tls`。
实际配置、凭据、私钥及包含它们的备份不得提交仓库。

### 12.2 首次部署或保留配置后的重新安装

确认第 7 节的三个 Secret 已准备好，values 中的镜像已推送且与本次配置匹配，然后执行：

```bash
helm upgrade --install "$RELEASE" agent/console/deploy \
  --kubeconfig "$KUBECONFIG" --kube-context "$CONTEXT" -n "$NAMESPACE" \
  -f "$WORK/values.yaml" --wait --timeout 5m
k rollout status deployment/adx-console --timeout=180s
k get deployment adx-console
k get service adx-console
```

安装后按第 10 节开启转发、登录并分别验证实际使用的功能。
Deployment Ready 或静态页面 HTTP 200 不等于登录、Accounts、文件和终端均已通过。

### 12.3 修改浏览器地址（localhost、内网 IP 或公网域名）

例如将浏览器入口改为 `https://172.31.9.216:28443/console/#/login`：

```bash
export PUBLIC_ORIGIN=https://172.31.9.216:28443
python3 - <<'PY'
import json
import os
from pathlib import Path

path = Path(os.environ['WORK']) / 'config/config.json'
config = json.loads(path.read_text())
config['public_origin'] = os.environ['PUBLIC_ORIGIN']
path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + '\n')
PY
k create secret generic adx-console-config \
  --from-file="$WORK/config" --dry-run=client -o yaml | k apply -f -
k rollout restart deployment/adx-console
k rollout status deployment/adx-console --timeout=180s
# 只输出非敏感的生效地址；当前镜像包含 python3。
k exec deployment/adx-console -c console -- python3 -c \
  'import json; print(json.load(open("/etc/adx-console/config.json"))["public_origin"])'
```

`public_origin` 只包含协议、主机和端口，不包含 `/console/`、路径或 URL fragment。
当前只支持一个 Origin：切换到 IP 后，localhost 登录和写操作不再通过来源校验。
修改 TLS 访问地址时，也要为新主机名或 IP 配置对应 SAN 的浏览器证书，按第 7 节更新 TLS Secret 后重启。

内网其他机器访问时，在部署机持续运行：

```bash
k port-forward service/adx-console 28443:8443 --address=0.0.0.0
```

仅本机使用时改为 `--address=127.0.0.1`，同时将 Origin 配置为实际使用的 localhost 地址。
`0.0.0.0` 是监听地址，不能作为浏览器入口。`172.31.9.216` 仅是本次环境示例，
新机器需使用实际可路由的部署机地址。访问设备只需要浏览器，不需要 kubeconfig；
但必须有内网/VPN 路由，且防火墙、安全组允许 TCP 28443。
私网 IP 不会因为修改监听地址而成为公网 IP。公网使用需另行提供公网负载均衡、
Ingress 或 NAT，并同步 Origin 与证书；本 Chart 本身不配置这些公网资源。

Pod 重建、网络中断或转发挂起后，停止对应旧转发进程并重新执行上述命令。
用 `ss -ltnp 'sport = :28443'` 确认占用者，不要停止无关进程。
转发进程存在不代表连接可用；从访问设备执行以下检查（证书需匹配入口）：

```bash
curl --noproxy '*' --cacert "$WORK/tls/tls.crt" \
  --connect-timeout 5 --max-time 15 \
  "$PUBLIC_ORIGIN/console/" -o /dev/null -w 'HTTP %{http_code}\n'
```

访问设备需持有公开证书/CA 文件，不应复制服务端私钥或管理凭据目录。
请求超时先检查路由、防火墙和转发；证书错误检查 SAN 与信任；
“访问地址与服务配置不一致”检查实际请求 Origin 与已加载的 `public_origin`。
端口转发是临时接入方式，长期多人访问应使用部署环境提供的稳定 HTTPS 入口。

### 12.4 更新镜像或配置，原地重新部署

配置修改按第 11 节上传 Secret。镜像更新先按第 4 节构建、推送唯一 tag，
再更新 `$WORK/values.yaml` 中的镜像，执行第 12.2 节的 `helm upgrade --install`。
不要只执行 `kubectl set image` 而不更新 values，否则下次 Helm 发布可能恢复旧镜像。

若本次仅改变外部 Secret 或代理配置且 Helm 未重建 Pod，执行：

```bash
k rollout restart deployment/adx-console
k rollout status deployment/adx-console --timeout=180s
```

重新启动端口转发并重新登录。当前内存会话和终端连接不跨重启保留。
镜像回滚用第 11 节的 `helm rollback`；外部 Secret 需要独立恢复，Helm 不会替你回滚。

### 12.5 卸载 Console

先停止本机对应的 Console 端口转发，再执行：

```bash
helm uninstall "$RELEASE" \
  --kubeconfig "$KUBECONFIG" --kube-context "$CONTEXT" -n "$NAMESPACE" --wait
k get deployment adx-console
k get service adx-console
```

最后两条命令预期返回 NotFound。Chart 管理的代理 ConfigMap 同时移除；
独立创建的三个 Secret、本地 `$WORK`、已有 PVC 及其中的数据保留。
卸载 Console 不会删除 Template、AgentBinding 或 Environment，也不会卸载 Gateway/Platform。

**需要重装时保留 Secret 和 `$WORK`，直接执行第 12.2 节。**
若明确不再保留本 Console 的配置和登录材料，才执行：

```bash
k delete secret adx-console-config adx-console-credentials adx-console-browser-tls
```

删除前确认这些 Secret 没有被其他部署引用；不要删除 namespace、共享 PVC 或上游 tenant-key。
删除 Secret 后重装必须重新准备第 6、7 节材料；若重新生成登录 token，原登录 token 不再有效。
本地 `$WORK` 的备份和删除由部署者按凭据保管要求处理，不与集群卸载隐式绑定。

## 13. 常见故障及验证边界

| 故障 | 检查重点 |
|---|---|
| ImagePullBackOff | 唯一 tag 是否已推送、registry 网络、同 namespace imagePullSecrets |
| Pending | PVC 是否同 namespace/Bound、节点资源、挂载失败事件 |
| Console 启动失败 | 严格 JSON 字段、Secret 文件名、65532 可读、session secret 长度、CA/profile 合法 |
| HTTPS 代理启动失败 | TLS Secret、Nginx 日志、证书私钥读权限、临时目录 |
| 401 登录失败 | Console token 原文与摘要对应；不是 tenant-key、不是摘要字符串 |
| 403 Origin | public_origin 与浏览器 scheme/host/port 完全一致 |
| 上游 TLS 失败 | CA、SAN、DNS/hostAliases、真实 Service 端口；不关闭校验 |
| shared_mount_unsupported | shared_files 未配置、挂载无 volume_id 或映射不含 storage_domain；刷新不能修复缺配置 |
| shared_storage_unavailable | ro 实际挂载、source、权限、映射文件和非 root 身份 |
| volume_identity_conflict | Binding 与 Platform Volume 的 id/uid/tenant 不一致，不能伪造映射绕过 |
| 文件查询 401/403 | Platform 授权需独立确认，不把 Gateway Key 权限当所有平台权限 |
| 终端 profile/helper 错误 | 真实版本 profile、工作负载内 helper、身份与 cwd、Gateway direct 端口 |

本次新增 Chart 已执行 Helm lint 和本地模板渲染；未执行 Console 镜像重新构建/推送、Chart 集群安装、真实只读挂载文件链路或集群浏览器端到端验证。实际操作应记录镜像 digest、配置 revision、集群 dry-run 和各验收结果后再标记上线。
