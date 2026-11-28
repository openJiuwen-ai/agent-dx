# 管理台前端登录验证步骤

适用于当前 `agent/console` 实现。下面命令在 Linux/WSL 的 Bash 中执行，仓库目录为 `/mnt/c/code/agent-dx`。使用 Docker、Python 3、OpenSSL 和 curl；浏览器运行于同一台机器。

## 1. 验证范围与入口

- 浏览器地址：`https://localhost:28443/console/#/login`。
- 当前登录使用配置中的管理员 ID/租户 ID 和管理 token，不是 accounts 业务用户账号，也不是 LiteLLM Key。
- 登录、会话查询及租户目录不要求 PostgreSQL、Redis、Gateway 或 LiteLLM 可用。
- 登录后查看真实模板需要接通 Agent 管理接口；占位上游地址只能验证登录及租户目录。
- session cookie 设置了 `Secure; HttpOnly; SameSite=Strict`，应通过 HTTPS 代理访问，不要用 `http://localhost:8088` 验证浏览器登录。

## 2. 构建当前镜像

在仓库根目录执行：

```bash
docker build -f build/images/Dockerfile.console -t adx-agent-console:local .
```

构建会执行 npm 安装、前端测试和 Vite 构建，再用 v6 构建 Rust release。需要能下载 Node 基础镜像、npm 和 Cargo 依赖。先前构建的同名镜像不一定包含最新工作区代码。

## 3. 生成验证配置与登录凭据

实际 namespace 为 `adx-wld`，租户 ID 为 `wld`，类型为 `digitalmate`，不能从 namespace 拼接租户 ID。租户 Key 从 `~/adx-wld/tenant-key` 读取；首次初始化将同一 Key 用作租户管理员登录 token 和上游 Bearer，平台管理员 token 独立随机生成。配置只保存登录 token 的 SHA-256 摘要，上游调用从挂载的文件读取原文。不要使用个人 LiteLLM Key 代替租户 Key。

digitalmate 类型要求配置 Accounts 管理 API 与 Binding 映射，不能只修改 `type`。由管理员先在 `~/adx-wld/` 准备：

- `tenant-key`：本环境租户 Key；`ca.pem`：本环境 CA。

Gateway 必须实际发布 Accounts 管理 API，并为该租户配置显式管理 grant；有效租户 Key 本身不代表拥有管理权。Console 只访问 HTTPS API，不需要数据库 DSN、LiteLLM master key 或解密材料。生成配置不代表现有 49443 已发布 Accounts 新接口。

以下脚本必须在仓库根目录执行，因为它通过相对路径读取 `agent/console/config.example.json`；不能在 `~/adx-wld` 凭据目录中直接运行。下面先切换到本机仓库；其他机器请替换为实际仓库路径。

脚本支持首次初始化和单租户验证目录更新。更新保留平台管理员摘要、原单租户登录摘要及 session secret，同时将租户 ID 改为 `wld`；缺少原有 session secret 时停止，多租户目录不支持此迁移。首次平台 token 仅显示一次，更新不会生成新登录凭据。执行前将 Accounts URL 改为实际已发布的入口。只调整现有目录的网络地址时直接使用第 7.3 节，避免重新初始化占位上游配置。

```bash
cd /mnt/c/code/agent-dx
python3 - <<'PY'
from pathlib import Path
import hashlib
import json
import secrets

source = Path.home() / 'adx-wld'
required = ('tenant-key', 'ca.pem')
for name in required:
    if not (source / name).is_file() or not (source / name).read_bytes().strip():
        raise SystemExit(f'Required credential file missing or empty: {source / name}')
tenant_token = (source / 'tenant-key').read_text().strip()
root = Path('/tmp/opencode/console-login-validation')
config_path = root / 'config/config.json'
previous = json.loads(config_path.read_text()) if config_path.exists() else None
if previous is not None and len(previous['tenants']) != 1:
    raise SystemExit('Only single-tenant validation configs are supported')
session_path = root / 'secrets/console-session'
if previous is not None and (not session_path.is_file() or len(session_path.read_bytes()) < 32):
    raise SystemExit('Existing session secret missing or invalid')
root.mkdir(parents=True, exist_ok=True)
root.chmod(0o755)
for name in ('config', 'secrets', 'tls'):
    directory = root / name
    directory.mkdir(exist_ok=True)
    directory.chmod(0o755)

config = json.loads(Path('agent/console/config.example.json').read_text())
config['public_origin'] = 'https://localhost:28443'
config['upstream_ca_file'] = '/run/secrets/ca.pem'
platform_token = secrets.token_urlsafe(32) if previous is None else None
digest = lambda token: hashlib.sha256(token.encode()).hexdigest()
config['platform_admins'] = previous['platform_admins'] if previous is not None else [{
    'admin_id': 'operator',
    'display_name': 'Local Operator',
    'token_sha256': digest(platform_token),
}]
config['tenants'] = [{
    'tenant_id': 'wld',
    'display_name': '北京 adx-wld',
    'type': 'digitalmate',
    'admin_token_sha256': digest(tenant_token),
    'agent': {'enabled': True},
    'upstream_token_file': '/run/secrets/adx-wld-upstream',
    'business': {
        'accounts_api': {
            'base_url': 'https://localhost:49443/api/accounts/v1/admin',
            'timeout_ms': 30000,
            'token_file': '/run/secrets/adx-wld-upstream',
        },
        'bindings': [
            {'kind': 'digitalmate', 'template': 'digitalmate',
             'ref': {'kind': 'alias', 'name': 'production'}},
            {'kind': 'orbit', 'template': 'orbit',
             'ref': {'kind': 'alias', 'name': 'stable'}},
        ],
    },
}]
if previous is not None:
    config['tenants'][0]['admin_token_sha256'] = previous['tenants'][0]['admin_token_sha256']
def write_file(path, data):
    if path.exists():
        path.chmod(0o600)
    path.write_bytes(data)
    path.chmod(0o444)
write_file(config_path, json.dumps(config, indent=2).encode())
if previous is None:
    write_file(session_path, secrets.token_bytes(32))
write_file(root / 'secrets/adx-wld-upstream', tenant_token.encode())
write_file(root / 'secrets/ca.pem', (source / 'ca.pem').read_bytes())
for path in (root / 'secrets').iterdir():
    path.chmod(0o444)
(root / 'config/config.json').chmod(0o444)
print('平台管理员 ID: operator')
if platform_token is not None:
    print('平台管理员 token:', platform_token)
else:
    print('保留原有管理员摘要及 session secret；使用原登录凭据')
print('租户管理员 ID: wld')
print('租户管理员 token:', '使用原 Console token（保留旧摘要）' if previous is not None
      else '使用 ~/adx-wld/tenant-key 内容（不是文件路径）')
print('验证目录:', root)
PY
```

配置中存的是 token 的 **SHA-256 十六进制摘要**；登录输入的是原始 token，不能输入摘要。平台管理员登录字段为 `admin_id`，租户管理员为 `tenant_id`。

`business.accounts_api.base_url` 必须指向已发布的管理路由；`localhost` 指 Console 所在网络命名空间，bridge 网络容器不能直接访问宿主机 loopback 转发。模板管理使用第 7 节的独立转发容器与 `adx.internal` 网络别名；Accounts 和 Platform 仍需单独核对入口、网络及授权。倍率、模型 namespace、数据库及解密材料只在 Gateway Accounts 配置。Jiuwen 关联另需真实 `agent_type`，不能随意填写。

上述 secret 权限适用于临时、本机验证，目的是让容器 UID 65532 可读。实际部署应使用受控 Secret 挂载和专用 UID/GID 权限，不直接复用这些临时文件。

### 3.1 重置平台管理员 operator 的登录 token

平台管理员 ID 为 `operator`。首次初始化只输出一次原始 token，配置仅保存 SHA-256 摘要；若原 token 已遗失，无法从摘要还原，可按以下步骤重置。

在创建验证目录的同一用户下执行；当前本机目录由 root 创建，应使用 root。脚本通过隐藏输入读取新 token，不将其写入命令历史，仅更新 `operator` 的登录摘要。建议使用密码管理器生成并保存高强度随机 token。

```bash
python3 - <<'PY'
from pathlib import Path
import getpass
import hashlib
import json
import stat

path = Path('/tmp/opencode/console-login-validation/config/config.json')
config = json.loads(path.read_text())
admins = [a for a in config['platform_admins'] if a['admin_id'] == 'operator']
if len(admins) != 1:
    raise SystemExit('必须存在且仅存在一个 operator 管理员')

token = getpass.getpass('输入新的 operator token: ')
confirm = getpass.getpass('再次输入: ')
if not token or token != confirm:
    raise SystemExit('token 不能为空，且两次输入必须一致')

admins[0]['token_sha256'] = hashlib.sha256(token.encode()).hexdigest()
mode = stat.S_IMODE(path.stat().st_mode)
path.chmod(mode | stat.S_IWUSR)
try:
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2))
finally:
    path.chmod(mode)

print('operator 登录摘要已更新；请重启 Console')
PY
```

若 Console 已运行，重启以加载新摘要并检查启动日志；若尚未启动，继续第 4、5 节即可：

```bash
docker restart adx-console-login
docker logs --tail 20 adx-console-login
```

重启会清除当前内存登录会话，所有已登录用户需要重新登录。打开 `https://localhost:28443/console/#/login`，选择“平台管理员”，管理员 ID 填 `operator`，管理 token 填刚才输入的新 token **原文**。

本步骤不修改租户登录凭据、上游 tenant-key 或 session secret。

## 4. 配置 HTTPS 反向代理

生成有效期 7 天、带 localhost SAN 的本机测试证书：

```bash
openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 7 \
  -keyout /tmp/opencode/console-login-validation/tls/localhost.key \
  -out /tmp/opencode/console-login-validation/tls/localhost.crt \
  -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1'
```

生成 Nginx 配置：

```bash
python3 - <<'PY'
from pathlib import Path
Path('/tmp/opencode/console-login-validation/config/nginx.conf').write_text('''
events {}
http {
    include /etc/nginx/mime.types;
    default_type application/octet-stream;
    map $http_upgrade $connection_upgrade {
        default upgrade;
        '' close;
    }
    server {
        listen 443 ssl;
        server_name localhost;
        ssl_certificate /etc/nginx/tls/localhost.crt;
        ssl_certificate_key /etc/nginx/tls/localhost.key;
        location /console/ {
            root /usr/share/nginx/html;
            add_header Cache-Control "no-store";
        }
        location / {
            proxy_pass http://adx-console-login:8088;
            proxy_http_version 1.1;
            proxy_set_header Host $http_host;
            proxy_set_header X-Forwarded-Proto https;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
            proxy_read_timeout 3600s;
            proxy_buffering off;
        }
    }
}
''')
PY
```

## 5. 启动 Console 与代理

以下名称和端口应未被占用。命令需要能拉取 `nginx:stable-alpine`。

先检查端口与旧容器：

```bash
ss -ltnp 'sport = :28443'
docker ps -a --filter name=adx-console-login
```

端口检查应只有表头。当前开发环境的 8443 被 API Server 使用，18443 被
adxlet 使用，因此这里改用 28443；该端口也必须以实际检查为准。

**若已经执行过旧版步骤，不要重新生成 token。** 第 3 节更新配置后必须让 Console 重新加载。以下迁移仅适用于第 5 节 bridge 网络，且现有 Console 使用的镜像已是步骤 2 构建的镜像；核对 `docker inspect adx-console-login --format '{{.Image}}'` 和 `docker image inspect adx-agent-console:local --format '{{.Id}}'`。不同则重建 Console，不能只 restart。已接入真实上游的 host 网络配置直接使用第 7 节。

先按第 7.7 节构建前端，再重新执行第 4 节生成 Nginx 配置；静态目录由宿主机只读挂载，不能只向容器内复制文件。再执行下面的迁移步骤，跳过后面的 network create 和 Console run，只执行代理的 `docker run`：

```bash
python3 - <<'PY'
from pathlib import Path
import json
path = Path('/tmp/opencode/console-login-validation/config/config.json')
mode = path.stat().st_mode & 0o777
path.chmod(mode | 0o200)
try:
    config = json.loads(path.read_text())
    config['public_origin'] = 'https://localhost:28443'
    path.write_text(json.dumps(config, indent=2))
finally:
    path.chmod(mode)
PY
docker restart adx-console-login
if docker container inspect adx-console-login-proxy >/dev/null 2>&1; then
  docker rm -f adx-console-login-proxy
fi
```

Docker 的端口映射在创建容器时固定，修改文档或重启旧代理不会更新映射，必须重新创建代理。

首次安装先按第 7.7 节生成宿主机 `frontend/dist`，再执行以下网络创建与 Console 启动命令：

```bash
docker network create adx-console-login-net
docker run -d --name adx-console-login \
  --network adx-console-login-net \
  --restart unless-stopped \
  --read-only --cap-drop=ALL --security-opt=no-new-privileges \
  --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  --mount type=bind,src=/tmp/opencode/console-login-validation/config,dst=/etc/adx-console,readonly \
  --mount type=bind,src=/tmp/opencode/console-login-validation/secrets,dst=/run/secrets,readonly \
  adx-agent-console:local
docker run -d --name adx-console-login-proxy \
  --network adx-console-login-net \
  --restart unless-stopped \
  -p 127.0.0.1:28443:443 \
  --mount type=bind,src=/tmp/opencode/console-login-validation/config/nginx.conf,dst=/etc/nginx/nginx.conf,readonly \
  --mount type=bind,src=/tmp/opencode/console-login-validation/tls,dst=/etc/nginx/tls,readonly \
  --mount type=bind,src=/mnt/c/code/agent-dx/agent/console/frontend/dist,dst=/usr/share/nginx/html/console,readonly \
  nginx:stable-alpine
```

查看启动状态：

```bash
docker logs adx-console-login
docker logs adx-console-login-proxy
curl -k --silent --show-error -o /dev/null -w 'Console HTML %{http_code}\n' \
  https://localhost:28443/console/
```

预期 HTML 返回 200。`-k` 仅用于这里的自签名测试证书；真实部署使用受信任证书。

## 6. 在浏览器登录

1. 打开 `https://localhost:28443/console/#/login`。使用 localhost，不要改成 IP；Origin 必须与配置完全一致。
2. 对本次本机证书完成浏览器信任/例外处理；若浏览器策略禁止例外，先将该测试证书导入受信任证书库，或换用本机受信任证书。
3. 选择“平台管理员”，输入 `operator` 和步骤 3 输出的平台 token。
4. 成功后应进入 `#/tenants`，显示 `wld` 租户。顶栏真实主体应为 `operator`。
5. 浏览器开发者工具 → Network：`POST /api/console/v1/session` 应为 200，并设置 `adx_console_session` cookie。
6. 后续 `GET /api/console/v1/session` 应为 200，响应包含 `role=platform_admin`、`subject_id=operator`。
7. 点击“退出”，应发出 session DELETE，重新访问租户目录须登录。
8. 再选择“租户管理员”，首次配置使用 `wld` 和 `~/adx-wld/tenant-key` 的原文内容；保留过旧摘要时使用原 Console token。应进入本租户模板页。占位 Gateway 不可用时，模板区域报错不代表登录失败；查看实际模板先完成第 7 节。digitalmate 租户显示业务用户模块，实际查询仍需接通 Accounts 管理依赖。

若只需确认登录成功，以步骤 4～6 为准，不要以占位上游返回模板列表作为通过条件。

### 登录反馈回归验证

- 输入错误 token：保持完整登录表单，显示“账号或管理凭据不正确”，保留账号并清空凭据；可再次提交。
- 点击退出：显示完整登录页和“已安全退出，请重新登录”，不得出现空页面。
- 会话过期：关闭已打开的终端、清理业务页面，显示“登录已过期，请重新登录”。
- 登录请求尚未结束时，按钮显示验证状态并禁止重复提交；网络失败后恢复可提交状态。
- 新登录页参考现有展示站，采用全屏 46%/54% 分栏、深海蓝品牌区、薄荷绿强调色和浅色无边框表单，窄屏上下排列。本机代理直接读取挂载的 `frontend/dist`，前端修改按第 7.7 节构建后强制刷新；Rust 二进制中的嵌入资源仅在重新构建镜像后更新。

### 可选：通过 API 验证登录

下面命令通过交互读取 token，不将它直接写在命令历史中。JSON 和 cookie 临时文件仅当前用户可读，结束时删除：

```bash
python3 - <<'PY'
from pathlib import Path
import getpass
import json
path = Path('/tmp/opencode/console-login-validation/login.json')
with path.open('x') as file:
    path.chmod(0o600)
    json.dump({'login_type': 'platform_admin', 'admin_id': 'operator',
               'token': getpass.getpass('平台管理员 token: ')}, file)
PY
umask 077
curl -k -i -c /tmp/opencode/console-login-validation/cookies.txt \
  -H 'Origin: https://localhost:28443' -H 'Content-Type: application/json' \
  --data-binary @/tmp/opencode/console-login-validation/login.json \
  https://localhost:28443/api/console/v1/session
curl -k -i -b /tmp/opencode/console-login-validation/cookies.txt \
  https://localhost:28443/api/console/v1/session
curl -k -i -b /tmp/opencode/console-login-validation/cookies.txt \
  https://localhost:28443/api/console/v1/tenants
python3 - <<'PY'
from pathlib import Path
for name in ('login.json', 'cookies.txt'):
    Path('/tmp/opencode/console-login-validation', name).unlink(missing_ok=True)
PY
```

预期三个响应均为 200；租户目录包含 wld。匿名 session 请求预期 401。登录请求 Origin 错误预期 403，token 错误预期 401。

## 7. 接入北京 adx-wld 后端

本节接续前面已运行的本机 Console，将第 3 节生成的 digitalmate 租户接入真实上游。

### 7.1 地址、凭据与当前阻塞点

| 用途 | 地址/配置 |
|---|---|
| 浏览器访问 Console | `https://localhost:28443/console/#/login`，保持不变 |
| 模板、版本、Alias、Binding 管理 | `agent_api.base_url=https://adx.internal:49444/api/agent/v2` |
| 宿主机 Ingress 转发 | Docker bridge 网关 `172.18.0.1:49444` → 集群 `service/ingress:8443` |
| 独立转发容器 | `adx-console-upstream-forward`，网络别名 `adx.internal`，监听 `49444` |
| Environment 查询与显式恢复 | `apiserver.base_url=https://apiserver:49888`；`adx-console-platform-forward` 网络别名 `apiserver`，TCP 转发到 `172.18.0.1:49888` |
| 宿主机 Platform 转发 | `kubectl --kubeconfig /home/wld/beijing/openjiuwen-adx-test-kubeconfig.yaml --context externalTLSVerify -n adx-wld port-forward service/apiserver 49888:8888 --address=172.18.0.1`，需持续运行 |
| 终端 Gateway 地址 | `ingress.base_url=https://adx.internal:49444`；仅完成地址配置，未验证真实 PTY 会话 |

Environment 链路已从 Console 容器使用租户凭据和配置 CA 严格验证 TLS：
`GET /api/instances?instance_id=<demo-v2 的 current Environment ID>` 返回 200，
响应归属 `wld`、状态 `paused`。该证据不代表恢复、终端或文件全链路已通过。
`adx-console-platform-forward` 与 Console 同属 `adx-console-login-net`，使用 Node TCP relay，
监听 `49888` 并转发至 Docker 网关 `172.18.0.1:49888`；容器设置自动重启，
宿主机 kubectl 转发仍需单独恢复。不要将 `apiserver.base_url` 恢复为示例域名。

历史终端排查记录：当时运行配置没有 `terminal_profiles_file`，因此没有
`wld / digitalmate / 2` 的终端 profile；实际交互还需要镜像中存在 profile 对应的
helper、明确的 uid/gid/shell/cwd。配置端口 `49983` 的固定 Environment 请求返回
Relay CONNECT 504；诊断性查询 `50090` 返回 401，不能据此将端口直接替换为 50090。
前端已补齐单终端生命周期及可见连接错误，但真实 PTY 链路仍未通过。

后续修复：实际 runtime profile 的 Execd HTTP 端口为 50090；普通端口转发
不会注入 Execd 凭据，`/direct/<environment_id>/invoke` 才能通过 Gateway 的
Environment 授权及凭据注入，实测返回 200。Console PTY URL 已改为 direct 路由。
本地配置已挂载 `terminal-profiles.json`，目标 `wld/digitalmate/2` 使用
UID/GID 10001、附加组 `[10001]`、HOME `/home/digitalmate`、shell `/bin/sh`、
cwd 挂载 `/data/state`。helper 已通过授权 invoke 安装到当前运行环境的
`/opt/adx/terminal-helper`，这只是运行实例修复，未构建或发布包含 helper 的镜像。
Console 当前以只读 bind 挂载 `target/debug/adx-agent-console` 运行新二进制；
旧容器保留为 `adx-console-login-before-terminal`，重建操作需保留此 binary 挂载。

剩余部署阻塞：direct PTY 升级返回 101 后立即断开，节点 Execd 日志为
`Permission denied (os error 13)`。实际 Execd cwd 是 `/`，而继承的 HOME 是
`/data/state/home`；root 无法访问该共享目录。portable-pty 默认使用 HOME，
在 helper 切换到业务用户前就 spawn 失败。源码已显式采用 Execd cwd，聚焦
PTY 测试及 debug 构建通过；集群当前仍运行旧 bootstrap Execd，尚未部署修复。
不能把源码修复或 101 升级视作真实终端已连通。需要发布新 runtime bootstrap
并通过平台执行替换；不能直接修改模板不可变版本或用第二个无生命周期管理的
Execd 监听器绕过现有运行实例。
| Gateway 转发根地址、Accounts 与 Platform | 各自配置，模板目录成功不代表这些入口已接通；容器内不能直接使用宿主机 `localhost` |
| 上游鉴权 | 本环境 `tenant-key` 文件，不是个人 LiteLLM Key |
| Console 登录 | 平台管理员用独立 token；租户管理员用配置摘要对应的原文，首次示例与 tenant-key 相同，迁移时可不同 |

`wss://localhost:49443/agent/ws/ws` 和 `/agent/ws/digitalmate/ws` 是业务 Harness 通道，**不填入 Console 的 base_url**。Console 的终端走固定 Environment 的 PTY 转发，也不是这两个业务 WS。

当前代码通过顶层 `upstream_ca_file` 显式加载私有 CA，HTTP 与终端 WS 共用信任，并保留公共根和主机名校验。镜像须包含此功能；仅挂载文件或设置 `SSL_CERT_FILE` 不能代替配置字段，不能关闭上游 TLS 校验。adx-wld 的模板目录已部署；Accounts 新接口及 grant 仍需独立核对，不能据模板目录 200 推断业务用户页可用。

### 7.2 在运行 Docker Engine 的 Linux/WSL 中保持转发

每个终端都单独设置 kubeconfig；终端之间不共享之前的 `export`。当前 Docker 网络网关为 `172.18.0.1`，先核对：

```bash
docker network inspect adx-console-login-net --format '{{json .IPAM.Config}}'
ss -ltnp 'sport = :49444'
```

若网关不同，下面的 `--address` 和第 7.4 节转发目标必须一起替换。已有同一转发则复用；否则在独立终端保持以下命令运行。此转发不会随 Console 容器自动重启，断开后需要重新启动。

```bash
# 终端 A：已有同一转发则跳过
export KUBECONFIG=/home/wld/beijing/openjiuwen-adx-test-kubeconfig.yaml
kubectl --kubeconfig "$KUBECONFIG" --context externalTLSVerify \
  -n adx-wld port-forward service/ingress 49444:8443 --address=172.18.0.1
```

```bash
# 终端 B：仅诊断宿主机到 Platform 的独立 HTTPS 端口
export KUBECONFIG=/home/wld/beijing/openjiuwen-adx-test-kubeconfig.yaml
kubectl --kubeconfig "$KUBECONFIG" --context externalTLSVerify \
  -n adx-wld port-forward service/apiserver 58888:8888 --address=127.0.0.1
```

终端 C 中先确认文件与上游连接（假定凭据已放在 `~/adx-wld`）：

```bash
test -s "$HOME/adx-wld/ca.pem" && test -s "$HOME/adx-wld/tenant-key"
curl --silent --show-error --fail-with-body --noproxy '*' \
  --connect-timeout 5 --max-time 20 \
  --cacert "$HOME/adx-wld/ca.pem" \
  -H "Authorization: Bearer $(cat "$HOME/adx-wld/tenant-key")" \
   --connect-to adx.internal:49444:172.18.0.1:49444 \
   https://adx.internal:49444/api/agent/v2/templates/jiuwen/aliases/stable
curl --silent --show-error --noproxy '*' --max-time 10 \
  --cacert "$HOME/adx-wld/ca.pem" -o /dev/null -w 'Platform HTTP %{http_code}\n' \
  https://localhost:58888/api/environment/v1/environments
```

第一个请求预期返回 Alias JSON；第二个匿名请求预期 401，只证明宿主机网络/TLS 通畅，不代表 bridge 容器可达或租户已获得 Platform 操作权限。不要用 `http://localhost:58888`，该服务实测为 HTTPS。当前模板修复未接通 Console 的 Platform、终端或 Accounts 链路。

### 7.3 修改本机 Console 配置，保留原登录 token

以下脚本只改临时验证目录，保留平台管理员、原租户登录 token 摘要及 session secret；运行前确保第 3 节已执行。上游文件换成本环境 Key，但 Console 登录 token 不自动跟着改变。脚本允许单租户旧配置迁移到实际租户 `wld`，执行前备份；多租户配置必须逐项核对，不能套用。

```bash
python3 - <<'PY'
from pathlib import Path
import json
import shutil

root = Path('/tmp/opencode/console-login-validation')
path = root / 'config/config.json'
config = json.loads(path.read_text())
tenant = config['tenants'][0]
assert len(config['tenants']) == 1, 'Only single-tenant validation configs are supported'
backup = path.with_name('config.before-adx-wld.json')
if not backup.exists():
    shutil.copy2(path, backup)
config['listen'] = '0.0.0.0:8088'
config['public_origin'] = 'https://localhost:28443'
config['agent_api']['base_url'] = 'https://adx.internal:49444/api/agent/v2'
config['upstream_ca_file'] = '/run/secrets/adx-wld-ca.pem'
# 保留 Accounts、Ingress 和 Platform 配置；本节只接通模板管理。
tenant.update(tenant_id='wld', display_name='北京 adx-wld',
              upstream_token_file='/run/secrets/adx-wld-upstream')
source = Path.home() / 'adx-wld'
for name in ('tenant-key', 'ca.pem'):
    if not (source / name).is_file() or not (source / name).read_bytes().strip():
        raise SystemExit(f'Required credential file missing or empty: {source / name}')
for src, dst in [('tenant-key', 'adx-wld-upstream'), ('ca.pem', 'adx-wld-ca.pem')]:
    target = root / 'secrets' / dst
    if target.exists():
        target.chmod(0o600)
    target.write_bytes((source / src).read_bytes())
    target.chmod(0o444)  # 与第 3 节临时 UID 65532 挂载约定一致
mode = path.stat().st_mode & 0o777
path.chmod(mode | 0o200)
try:
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2))
finally:
    path.chmod(mode)
PY
```

CA 文件在容器中为 `/run/secrets/adx-wld-ca.pem`，由 `upstream_ca_file` 在启动时加载。先执行第 3 节迁移旧 business 字段，再执行本节网络地址更新。业务查询依赖已发布且显式授权的 Accounts API。

### 7.4 独立转发容器与稳定网络名称

Console、Nginx 和转发容器均使用 `adx-console-login-net`。转发容器通过网络别名 `adx.internal` 提供 TCP 透传；该名称匹配当前 Ingress 证书 SAN，TLS 仍由 Console 严格验证。不要使用 `--network container:adx-console-login`：Console 重启或重建后，共享旧网络空间的转发容器可能仍显示运行中，但新 Console 访问 loopback 会被拒绝。

先按第 7.3 节更新配置。已有 bridge Console 只需重启；旧 host 网络 Console 或更新镜像时，按第 7.7 节重建后端。随后创建独立转发容器（替换旧转发容器，不更改登录凭据）：

```bash
if docker container inspect adx-console-upstream-forward >/dev/null 2>&1; then
  docker rm -f adx-console-upstream-forward
fi
docker run -d --name adx-console-upstream-forward \
  --network adx-console-login-net --network-alias adx.internal \
  --restart unless-stopped \
  node:22-alpine node -e '
    const net = require("net");
    net.createServer(client => {
      const upstream = net.connect(49444, "172.18.0.1");
      client.pipe(upstream);
      upstream.pipe(client);
      client.on("error", () => upstream.destroy());
      upstream.on("error", () => client.destroy());
    }).listen(49444, "0.0.0.0");
  '
docker restart adx-console-login
docker exec adx-console-login-proxy nginx -s reload
```

后端重启不再要求重启转发容器。Nginx reload 用于更新后端容器名称解析；第 7.2 节宿主机 kubectl 转发仍必须持续运行。此步骤不向宿主机发布转发容器端口。

### 7.5 接入验收

1. 打开 `https://localhost:28443/console/#/login`，平台管理员仍用 `operator` 和原平台 token；租户管理员用 `wld` 和原租户 Console token。
2. 租户目录应显示 `wld`。登录本身不验证上游连通性。
3. 模板列表应实际显示 `jiuwen`、`orbit`、`digitalmate`，分别打开版本与 Alias。Network 中 `GET /api/console/v1/tenants/wld/templates` 必须为 200，不能仅以 session 200 判断接入成功。
4. 查询已有 Binding 和固定 Environment 状态；Platform 返回 401/403 时另查其授权，不将管理 API 租户 Key 的权限等同于所有 Platform 权限。
5. 文件与终端还需部署映射/只读挂载及 profile/helper；DigitalMate 业务用户页还需已发布的 Accounts 管理接口及对应 grant，见 [DEPLOYMENT.md](DEPLOYMENT.md)、[TERMINAL.md](TERMINAL.md)、[BUSINESS.md](BUSINESS.md)。

验证应分别记录 Console 登录、模板管理、Platform、Accounts 结果；旧环境的历史成功不能作为 adx-wld 证据。浏览器交互、文件和终端需单独验收，API 登录成功不等于这些能力已通过。

### 7.6 执行记录（2026-11-16）

早期 host 网络验证记录（后续已被下面的 bridge 修复替换，不作为当前启动方式）：

- 使用本机已有 `adx-agent-console:local` 镜像（ID 前缀 `d7b96909ade0`），重新创建 host 网络 Console 和 Nginx；本次未重新执行第 2 节构建。
- 修正实际租户 `wld`、namespace `adx-wld`、Gateway 端口 `49443` 后，`GET /console/` 返回 200。
- 使用保留摘要对应的原租户 Console token：登录、会话、租户目录、模板目录、Jiuwen 版本/Alias、Binding 查询均返回 200；模板包含 `digitalmate`、`jiuwen`、`orbit`。
- 错误 token 返回 401，错误 Origin 返回 403；退出返回 204，随后会话查询返回 401。新环境 tenant-key 不能替代旧摘要对应的 Console token，实测替代后返回 401。
- 宿主机通过本环境 CA 严格校验 TLS：Gateway 模板目录和 stable Alias 均返回 200；临时启动 Platform `58888` 转发后，匿名请求返回 401，随后停止该临时转发。实际使用 Platform 功能需持续运行转发并另验授权。
- Gateway `GET /api/accounts/v1/admin/users` 返回 404 `route not found`，业务用户查询尚不可验收，需先发布 Accounts 管理入口并配置 grant。
- 未执行浏览器点击、平台管理员原 token 登录、会话自然过期、文件/PTY 或完整业务用户链路；这些仍需独立验证。API 验证未输出 token 或 cookie。

后续修复与复验（当前本机配置）：

- 将 Console 与代理恢复为 bridge 网络；Nginx 监听容器 `443`，宿主机映射 `127.0.0.1:28443`，API 代理目标为 `adx-console-login:8088`。
- 前端通过宿主机 `frontend/dist` 只读挂载；修复容器重建丢失静态文件导致的 404，并补充 Nginx MIME 配置，HTML/CSS/JS 分别实测为 `text/html`、`text/css`、`application/javascript`。
- 模板上游改为 `https://adx.internal:49444/api/agent/v2`，使用独立转发容器与 `adx-wld-ca.pem`；不再共享后端网络空间。实际重启 Console 后，从后端容器内严格验证 TLS 的模板请求仍为 200，返回 `digitalmate`、`jiuwen`、`orbit` 共 3 项。
- 前端 25 项测试、Vite 构建及文档检查通过；28443 提供的静态文件与构建产物逐字节一致。未执行浏览器视觉验收，未把模板查询结果作为 Platform、Accounts、文件或终端的通过证据。

### 7.7 重新编译、重启与重建

仅修改前端，在仓库根目录运行：

```bash
docker run --rm \
  -v /mnt/c/code/agent-dx/agent/console/frontend:/app -w /app \
  node:22-alpine sh -c 'npm test && npm run build'
```

Nginx 直接读取挂载的 `frontend/dist`，无需重启容器，浏览器 Ctrl+F5 即可。首次创建代理前也必须先生成该目录。不要只用 `docker cp` 把文件复制进容器，否则重建后再次丢失。

只修改配置或需要普通重启：

```bash
docker restart adx-console-login
docker exec adx-console-login-proxy nginx -t
docker exec adx-console-login-proxy nginx -s reload
```

后端重启会清除内存登录会话，需要重新登录；不会改变配置中保存的登录摘要。独立转发容器无需随之重启。

修改 Rust 后端时，重新构建镜像后重建 Console；`docker restart` 不会使用同名标签的新镜像：

```bash
docker build -f build/images/Dockerfile.console -t adx-agent-console:local .
# 仅在上面的构建成功后执行以下命令
docker rm -f adx-console-login
docker run -d --name adx-console-login \
  --network adx-console-login-net --restart unless-stopped \
  --read-only --cap-drop=ALL --security-opt=no-new-privileges \
  --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  --mount type=bind,src=/tmp/opencode/console-login-validation/config,dst=/etc/adx-console,readonly \
  --mount type=bind,src=/tmp/opencode/console-login-validation/secrets,dst=/run/secrets,readonly \
  adx-agent-console:local
docker exec adx-console-login-proxy nginx -s reload
```

修改代理挂载、网络或端口映射时，重新执行第 4 节生成配置，再删除并按第 5 节的代理 `docker run` 重建；仅 reload/restart 不会更新容器挂载或端口映射。第 5 节静态目录挂载必须保留。

每次重启/重建后检查静态资源：

```bash
curl -k -sS -o /dev/null -w 'HTML %{http_code} %{content_type}\n' https://localhost:28443/console/
curl -k -sS -o /dev/null -w 'CSS %{http_code} %{content_type}\n' https://localhost:28443/console/console.css
curl -k -sS -o /dev/null -w 'JS %{http_code} %{content_type}\n' https://localhost:28443/console/console.js
```

预期全部 200，类型依次为 `text/html`、`text/css`、`application/javascript`。随后从 Console 容器内验证模板上游，不输出 token：

```bash
docker exec -i adx-console-login python3 - <<'PY'
import json
from pathlib import Path
import ssl
import urllib.request

config = json.loads(Path('/etc/adx-console/config.json').read_text())
tenant = next(t for t in config['tenants'] if t['tenant_id'] == 'wld')
token = Path(tenant['upstream_token_file']).read_text().strip()
context = ssl.create_default_context(cafile=config['upstream_ca_file'])
request = urllib.request.Request(config['agent_api']['base_url'] + '/templates',
                                 headers={'Authorization': 'Bearer ' + token})
with urllib.request.urlopen(request, context=context, timeout=15) as response:
    result = json.load(response)
    print('Template HTTP', response.status, 'total', result['total'])
    print('Templates:', [item['name'] for item in result['templates']])
PY
```

这只验证容器到上游的网络、TLS 和租户鉴权；浏览器还需重新登录并确认 Console 模板 API 返回 200。

## 8. 常见故障定位

| 现象 | 检查方法 |
|---|---|
| 容器启动即退出 | `docker logs adx-console-login`；确认配置存在，session secret 至少 32 字节，UID 65532 可遍历目录并读取挂载文件 |
| 登录返回 403 origin_forbidden | 浏览器地址是否严格为 `https://localhost:28443`；public_origin 不含 `/console/`、不带末尾 `/` |
| 登录返回 401 invalid_credentials | 确认登录类型、ID、原始 token；摘要必须按原始 UTF-8 token 计算，不包含多余换行 |
| POST 登录 200，随后 session 401 | 检查 HTTPS、cookie 是否被浏览器接受、是否混用 localhost/IP、Console 是否重启；当前 session 注册表为单进程内存 |
| 登录后模板区域失败 | 检查真实 Gateway 地址、DNS、TLS 和租户上游 token；模板读取不使用管理员登录 token |
| 代理返回 502 | 检查同一 bridge 网络、Console 监听 0.0.0.0:8088、proxy_pass 为 adx-console-login:8088；后端重建后 reload Nginx |
| /console/ 返回 404 | 检查 frontend/dist 已构建并只读挂载到 /usr/share/nginx/html/console；容器重建不会保留 docker cp 的文件 |
| 页面无样式或空白，资源却为 200 | 检查 CSS/JS 类型；Nginx 必须 include /etc/nginx/mime.types，不能以 text/plain 返回模块脚本 |
| 重启后模板又加载失败 | 检查 49444 的 kubectl 转发、独立转发容器及 adx.internal 解析；不能使用共享旧后端网络空间的 --network container: 模式 |
| 新 tenant-key 登录仍为 401 | 更新保留了旧租户登录摘要；使用原 Console token，而非新上游 Key |
| 业务用户页失败但模板正常 | 独立检查 /api/accounts/v1/admin/users 的路由及 grant；404 不是模板接口故障 |
| 页面仍是旧代码 | 本机静态挂载模式重新构建 frontend/dist 后 Ctrl+F5；后端修改则构建镜像并重建 Console |

## 9. 停止验证容器

```bash
docker rm -f adx-console-login-proxy adx-console-login adx-console-upstream-forward
docker network rm adx-console-login-net
```

临时配置、密钥和证书保留在 `/tmp/opencode/console-login-validation`，验证结束后按本机文件清理方式删除。不要把该目录加入版本控制。
