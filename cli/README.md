# ar 命令行

openYuanrong Agent Runtime 的命令行工具。Agent 本质上就是函数,`ar` 把函数的注册、调用包装成对底层 FaaS HTTP 接口的调用。

- `ar deploy` —— 通过 meta_service 注册一个 agent(函数)。
- `ar exec` —— 调用 agent(函数),并以 SSE 流式输出返回结果;未传 `--args` 时进入交互模式。


## 安装

先构建 whl 再安装:

```bash
python setup.py bdist_wheel
pip install dist/openyuanrong_agentruntime-*.whl
```

安装后即可使用 `ar` 命令,`ar -h` 查看帮助,`ar --version` 查看版本。

## 使用

### JWT 鉴权

`ar` 支持通过全局参数或环境变量提供 JWT,并通过 `X-Auth` 请求头发送:

```bash
ar --jwt-token <JWT> exec --agent <AGENT> --server <FRONTEND_ADDR>
```

```bash
export YR_JWT_TOKEN=<JWT>
ar exec --agent <AGENT> --server <FRONTEND_ADDR>
```

`--jwt-token` 是全局参数,需要放在 `deploy` 或 `exec` 子命令之前。SessionCtx 查询、历史查询、Fork 和 Delete 接口要求 Developer JWT。使用 `-v` 时日志中的 JWT 会显示为 `<redacted>`。

### ar deploy —— 注册 agent

```bash
ar deploy -s <函数定义> --server <META_SERVICE_ADDR>
```

| 参数 | 必选 | 说明 |
|------|------|------|
| `-s, --spec` | 是 | 函数定义,可以是一段 inline JSON 字符串,也可以是 JSON 文件路径(自动识别);非法 JSON、或文件不存在均会报错 |
| `--server` | 是 | meta_service 地址,格式为 `host:port`,例如 `127.0.0.1:31182`(默认 http,无需加 `http://` 前缀);格式不合法会报错 |

说明:

- `-s/--spec` 会做格式校验:若值是已存在的文件则按文件读取并解析 JSON;否则按 inline JSON 解析。两者都不满足(既非合法 JSON,也不是存在的文件路径)时报错退出(退出码 2)。
- `--server` 必须是 `host:port` 形式(缺端口或端口非法会报错)。
- 函数定义中若未设置 `enableSessionCtx` 字段,会自动注入默认值 `true`;若已显式设置(`true` 或 `false`),则以用户设置为准。
- 注册成功后会打印公开 agent 名称,格式为 `0@namespace@funcname`,可直接用于 `ar exec --agent`。

示例:

```bash
# 文件方式
ar deploy -s ./agent.json --server 127.0.0.1:31182

# inline JSON 方式
ar deploy -s '{"name":"0@faaspy@demo","runtime":"python3.11","handler":"demo.handler"}' \
          --server 127.0.0.1:31182
```

### ar exec —— 调用 agent(流式)

```bash
ar exec --agent <AGENT> --server <FRONTEND_ADDR> [可选参数]
```

| 参数 | 必选 | 默认 | 说明 |
|------|------|------|------|
| `--agent` | 是 | — | 要调用的 agent,格式为 `0@namespace@funcname[:version]`;该值会原样用于 Frontend URL |
| `--server` | 是 | — | frontend 地址,格式为 `host:port`,例如 `127.0.0.1:31180`(默认 http,无需加 `http://` 前缀) |
| `--session-ctx` | 否 | 无 | agent 会话上下文;传入才会带 `X-Session-Context` 请求头,交互模式会自动生成默认值 |
| `--session-id` | 否 | 无 | 实例会话 ID;一次性调用需显式传入，交互模式未传入时自动生成 |
| `--session-ttl` | 否 | 90 | 实例会话 TTL;交互模式默认 600 秒，命令行显式传入时需同时传入 `--session-id` |
| `--concurrency` | 否 | 1 | 实例会话并发数;命令行显式传入时需同时传入 `--session-id` |
| `--args` | 否 | 无 | handler 入参,JSON 字符串;不传则进入交互模式 |

说明:

- 只有 `--agent` 和 `--server` 必选,其余均可选。
- `--session-ttl` / `--concurrency` 必须配合 `--session-id` 使用;若只传了它们而没传 `--session-id`,`ar` 会报错并直接退出(退出码 2),**不发送任何请求**。
- 传入 `--args` 时执行一次性调用,请求体原样使用该 JSON 字符串。
- 未传 `--args` 时进入交互模式;每轮用户输入会自动包装为 `{"message":"用户输入"}` 后发起一次调用。
- 交互模式下若未传 `--session-ctx`,会自动生成一个会话上下文,并在每次调用中携带同一个 `X-Session-Context` 请求头;若已传入,则使用用户提供的值。
- 交互模式下若未传 `--session-id`,会自动生成一个 InstanceSession ID。同一 SessionCtx 的每次普通消息都会携带同一个 `X-Instance-Session`;未指定 `--session-ttl` 时使用 600 秒。
- 通过 `/sessions`、`/fork` 或 `/new` 切换 SessionCtx 后,CLI 会使用原 SessionCtx 的 InstanceSession ID 发送 `sessionTTL` 为 0 的释放调用,并为新 SessionCtx 生成新的 InstanceSession ID。
- 至少发起过一次普通消息后,`/quit` 或输入结束会额外发送一条 `sessionTTL` 为 0、body 为 `{}` 的调用,使该 InstanceSession 立即过期。
- 交互模式输入 `/quit` 退出。
- 返回结果为 SSE 流,`ar` 会边接收边持续输出,直到服务端发送结束标记。

#### 交互 SessionCtx 管理(Linux)

交互模式下可以使用以下命令管理当前 Agent 的 SessionCtx:

| 命令 | 说明 |
|------|------|
| `/sessions` | 查询当前 Agent 的 SessionCtx。Linux TTY 下可用上下方向键选择,Enter 切换,Esc 或 `q` 取消。 |
| `/history` | 查询当前 SessionCtx 最近的 Turn 输入、输出和状态。 |
| `/fork <turn-id> <new-session-ctx-id>` | 从已完成 Turn 创建指定的新 SessionCtx,成功后自动切换。 |
| `/delete <session-ctx-id>` | 删除非当前 SessionCtx。删除当前会话前，需先通过 `/new` 或 `/sessions` 切换。 |
| `/new [session-ctx-id]` | 仅切换 CLI 当前 SessionCtx;首条普通消息才会创建服务端会话。 |

Linux TTY 的交互输入支持斜杠命令补全：输入 `/` 或命令前缀会显示候选，使用上下方向键选择，按 `Tab` 或 `Enter` 将候选填入输入行；填入后再次按 `Enter` 执行。拼写接近但未前缀匹配的命令会显示最多三个相近候选。

`SessionCtx ID` 与 `Turn ID` 最长为 63 个字符。`/sessions` 默认显示当前 Agent 最近更新的前 50 个会话;非 TTY 环境仅打印列表,可使用 `/new <session-ctx-id>` 切换。

`/fork` 的目标 SessionCtx ID 必须显式提供,并且不能与当前 SessionCtx ID 相同。请求超时后重试时应继续使用相同的源 SessionCtx、Turn ID 和目标 SessionCtx ID。

示例:

```console
$ ar exec --agent 0@default@demo --server 127.0.0.1:31180 --session-ctx research-main
[research-main] > /history
[research-main] > /fork turn-0001 research-alt
[research-alt] > 忽略之前的结论,改为检查依赖安全问题
[research-alt] > /delete research-main
```

示例:

```bash
# 最简调用
ar exec --agent <AGENT> --server 127.0.0.1:31180

# 一次性调用
ar exec --agent <AGENT> --server 127.0.0.1:31180 --args '{"message":"你好"}'

# 带会话上下文与入参
ar exec --agent <AGENT> --server 127.0.0.1:31180 \
        --session-ctx ctx1 --session-id id1 --session-ttl 90 --concurrency 1 \
        --args '{"param1":"你好"}'
```

## 日志与排查

- `ar` 的日志只输出到控制台,不落盘到日志文件。
- 加 `-v / --verbose` 开启 DEBUG 级日志,会在请求发送前打印请求详情(method、url、headers、body),方便定位问题:

  ```bash
  ar -v exec --agent <AGENT> --server 127.0.0.1:31180
  ```

- DEBUG 日志会将 `X-Auth` 的值替换为 `<redacted>`,避免 JWT 明文输出。
- 普通日志走 stderr,流式数据走 stdout,互不干扰。需要把日志存盘时自行重定向:

  ```bash
  ar exec ... 2> ar.log
  ```

## 退出码

| 退出码 | 含义 |
|--------|------|
| `0` | 成功 |
| `1` | 服务端失败(HTTP 非 2xx,或响应 `code != 0`) |
| `2` | 参数错误(JSON 非法、文件不存在、缺少必选参数) |
| `3` | 网络错误(连不上、超时) |

## 测试

测试代码位于仓库根目录的 `tests/cli/`。在**仓库根目录**执行(`pytest.ini` 已把 `cli/` 加入路径):

```bash
python -m pytest -q
```
