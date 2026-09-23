# ADX 用户命令行

`adx` 使用 Rust，通过公开 Gateway 接口访问 Agent 资源。当前实现管理命令和 SSH 交互终端；HTTP CLI 仍待实施，HTTP/WS 的 URN 入口已由 Gateway 提供。

```sh
cargo build --locked -p adx-cli --bin adx
cargo install --locked --path agent/cli
```

管理命令的租户 API Key 由平台管理员通过 `/api/admin/v1/keys` 签发，CLI 不签发密钥、不自行指定租户，也不连接 Redis、Master 或 Activator。租户身份由 Gateway 验证凭据后确定。

```sh
export ADX_SERVER_ADDRESS=https://gateway.example.com:8443
# ADX_TOKEN 由部署环境注入，或使用 --token-file 指向管理员提供的租户密钥文件。
adx --token-file /run/secrets/adx-token template publish -f template.json
adx --token-file /run/secrets/adx-token template get assistant --version 1
adx --token-file /run/secrets/adx-token env list --template assistant --version 1 --page-size 50
adx --token-file /run/secrets/adx-token env get demo --template assistant --version 1
adx --token-file /run/secrets/adx-token env delete demo --template assistant --version 1
```

`template.json` 使用 [Agent Template](../README.md) 的 `name/version/image/isolation_runtime/entrypoint/resources/service` 配置。发布的模板版本不可变；查询和列举 Environment 不激活 Sandbox，删除会调用服务端删除流程。指定 ID 的 Environment 创建由HTTP/WS/SSH 访问流量或 resolve 请求触发，CLI 不提供显式创建命令。

`env list` 每次仅请求一页，返回 `environments` 和 `next_page_token`。后续页传 `--page-token`；默认页大小 50，上限 100。同一 token 只能用于同一租户、模板和版本。列表反映 ADX 元数据，不表示实时 Sandbox 健康状态；并发更新时不保证快照。

## 配置

参数优先于环境变量，环境变量优先于显式指定的 JSON 配置文件；没有配置时 endpoint 为 `https://localhost:8443`，管理请求超时为 60 秒。

| 参数 | 环境变量 | 配置文件字段 |
| --- | --- | --- |
| `--config` | `ADX_CONFIG` | — |
| `--endpoint` | `ADX_SERVER_ADDRESS` | `endpoint` |
| `--token-file` | `ADX_TOKEN`（密钥内容） | `token_file` |
| `--ca` | `ADX_CA_CERT` | `ca` |
| `--timeout-seconds` | `ADX_TIMEOUT_SECONDS` | `timeout_seconds` |
| `--allow-http` / `--allow-http=false` | `ADX_ALLOW_HTTP` | `allow_http` |

凭据优先级为显式 `--token-file`、`ADX_TOKEN`、配置文件 `token_file`。配置不保存明文密钥；文件路径相对当前工作目录。地址必须是 origin，不包含业务 path、query 或用户信息。默认校验证书，测试明文服务须显式使用 `--allow-http`，不提供跳过证书验证选项。

```json
{
  "endpoint": "https://gateway.example.com:8443",
  "token_file": "/run/secrets/adx-token",
  "ca": "/etc/adx/public-ca.pem",
  "timeout_seconds": 60
}
```

管理命令的正常结果打印到 stdout，默认使用格式化 JSON；`--output json` 输出单行 JSON，不附加说明文字。诊断写入 stderr；成功退出码为 0，失败为 1，Ctrl-C 为 130。客户端不自动重放请求，也不跟随重定向；写请求发送后中断可能已经提交，应通过 get/list 回查原身份。Ctrl-C 不保证取消已经提交的服务端操作。

## SSH 交互终端

SSH 使用系统 `ssh` 和用户公钥认证，不要求 `ADX_TOKEN`。Gateway 管理员将公钥映射到租户；客户端需提前通过可信渠道配置 Gateway 主机的 known_hosts 记录，CLI 强制校验主机身份。

```sh
export ADX_SSH_ADDRESS=gateway.example.com:2222
adx ssh --template assistant --version 1 -i ~/.ssh/id_ed25519
adx ssh --template assistant --version 1 --env demo --port 22
```

可用 `--gateway` 覆盖 `ADX_SSH_ADDRESS` 或配置文件 `ssh_address`；身份文件优先级为 `-i/--identity`、`ADX_SSH_IDENTITY`、配置文件 `ssh_identity`。没有指定身份文件时由 OpenSSH 使用用户自身配置或 ssh-agent。地址为 host:port，端口省略时为 22；`--port` 是 Template 中的后端 SSH service 端口。

省略 `--env` 时由 Gateway 生成 ID，在 shell 开始时打印 `Environment ID` 和 `Environment URN`；正常终端内容和这两行提示都在 stdout，错误诊断走 stderr。该 ID 可用于后续 `--env`、查询和删除；提示不代表 Sandbox 已就绪，失败后仍应回查同一身份。

当前冷启动可能先返回创建结果未知，或在平台报告 Running 后短暂遇到路由尚未发布。收到 ID 后连接失败时，使用 `adx env get` 查询，并在再次连接时显式传入 `--env <返回的ID>`；不要再次省略 `--env`，否则会生成另一个 Environment。CLI 不自动重试 SSH 连接。

仅提供交互式 shell，stdin 必须是终端；不接收远程命令，不提供 SFTP、inline 或 port-forward 命令。CLI 构造路由用户名后直接启动 OpenSSH，保留终端输入、窗口调整和后端退出码，不先发送 HTTP resolve，也不做连接复用或自动重连。`--output json` 和 HTTP 凭据/超时参数只用于管理命令，不改变 SSH 终端。

## 验证范围

`make agent-test` 包含 CLI 测试。CLI 组件用例使用本地 HTTP 服务核对路由编码、分页参数、凭据、JSON 输出和错误处理；这些用例不代表真实 Platform 端到端验收。

SSH 参数用例覆盖显式/自动 Environment、身份文件路径及 IPv6 地址。Gateway 组件另用原生 OpenSSH 验证终端 stdout 的 ID/URN 与退出码。

2026-09-22 另行完成容器内真实 Platform、Redis、RRT 和用户镜像验证：CLI 模板发布/读取、Environment 三页查询和跨 Gateway 查询/删除、已有 Environment SSH、自动生成 ID 后同 ID 重连均通过，终端输出与后端退出码正确。首次冷启动仍可能遇到平台就绪或路由发布延迟，需要按上文使用原 ID 重试；该结果不是多机或 Kubernetes 验收。完整范围和限制见 [Agent 验证说明](../README.md#验证)。
