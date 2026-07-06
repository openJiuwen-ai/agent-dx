# ar 命令行

openYuanrong Agent Runtime 的命令行工具。Agent 本质上就是函数,`ar` 把函数的注册、调用包装成对底层 FaaS HTTP 接口的调用。

- `ar deploy` —— 通过 meta_service 注册一个 agent(函数)。
- `ar exec` —— 调用 agent(函数),并以 SSE 流式输出返回结果;未传 `--args` 时进入交互模式。

详细设计见 [`../docs/ar-cli-design.md`](../docs/ar-cli-design.md)。

## 安装

在本 `cli/` 目录下执行:

```bash
pip install .
```

或先构建 whl 再安装:

```bash
python setup.py bdist_wheel
pip install dist/openyuanrong_agentruntime-*.whl
```

安装后即可使用 `ar` 命令,`ar -h` 查看帮助,`ar --version` 查看版本。

## 使用

### ar deploy —— 注册 agent

```bash
ar deploy -s <函数定义> --server <META_SERVICE_ADDR>
```

| 参数 | 必选 | 说明 |
|------|------|------|
| `-s, --spec` | 是 | 函数定义,可以是一段 inline JSON 字符串,也可以是 JSON 文件路径(自动识别) |
| `--server` | 是 | meta_service 地址,格式为 `host:port`,例如 `127.0.0.1:31182`(默认 http,无需加 `http://` 前缀) |

说明:

- 函数定义中若未设置 `enableSessionCtx` 字段,会自动注入默认值 `true`;若已显式设置(`true` 或 `false`),则以用户设置为准。
- 注册成功后会打印 `functionVersionUrn`,可直接用于 `ar exec --agent`。

示例:

```bash
# 文件方式
ar deploy -s ./agent.json --server 127.0.0.1:31182

# inline JSON 方式
ar deploy -s '{"name":"0@svc@demo","runtime":"python3.11","handler":"demo.handler"}' \
          --server 127.0.0.1:31182
```

### ar exec —— 调用 agent(流式)

```bash
ar exec --agent <FUNCTION_VERSION_URN> --server <FRONTEND_ADDR> [可选参数]
```

| 参数 | 必选 | 默认 | 说明 |
|------|------|------|------|
| `--agent` | 是 | — | 要调用的 agent 的 functionVersionUrn |
| `--server` | 是 | — | frontend 地址,格式为 `host:port`,例如 `127.0.0.1:31180`(默认 http,无需加 `http://` 前缀) |
| `--session-ctx` | 否 | 无 | agent 会话上下文;传入才会带 `X-Session-Context` 请求头,交互模式会自动生成默认值 |
| `--session-id` | 否 | 无 | 实例会话 id;传入才会带 `X-Instance-Session` 请求头,交互模式会自动生成默认值 |
| `--session-ttl` | 否 | 90 | 实例会话 TTL;仅在传了 `--session-id` 时生效,交互模式默认 600 |
| `--concurrency` | 否 | 1 | 实例会话并发数;仅在传了 `--session-id` 时生效,交互模式随默认 `session-id` 生效 |
| `--args` | 否 | 无 | handler 入参,JSON 字符串;不传则进入交互模式 |

说明:

- 只有 `--agent` 和 `--server` 必选,其余均可选。
- 传入 `--args` 时执行一次性调用,请求体原样使用该 JSON 字符串。
- 未传 `--args` 时进入交互模式;每轮用户输入会自动包装为 `{"message":"用户输入"}` 后发起一次调用。
- 交互模式下若未传 `--session-ctx`,会自动生成一个会话上下文,并在每次调用中携带同一个 `X-Session-Context` 请求头;若已传入,则使用用户提供的值。
- 交互模式下若未传 `--session-id`,会自动生成一个实例会话 id,每次调用携带同一个 `X-Instance-Session` 请求头。
- 交互模式默认 `sessionTTL` 为 600 秒;若已传入 `--session-id` 或 `--session-ttl`,则使用用户提供的值。
- 交互模式退出时会使用同一个 `session-id` 再发送一次 `sessionTTL` 为 0 的请求,用于释放实例会话。
- 交互模式输入 `/exit` 或 `/quit` 退出。
- 返回结果为 SSE 流,`ar` 会边接收边持续输出,直到服务端发送结束标记。

示例:

```bash
# 最简调用
ar exec --agent <URN> --server 127.0.0.1:31180

# 一次性调用
ar exec --agent <URN> --server 127.0.0.1:31180 --args '{"message":"你好"}'

# 带会话上下文与入参
ar exec --agent <URN> --server 127.0.0.1:31180 \
        --session-ctx ctx1 --session-id id1 --session-ttl 90 --concurrency 1 \
        --args '{"param1":"你好"}'
```

## 日志与排查

- `ar` 的日志只输出到控制台,不落盘到日志文件。
- 加 `-v / --verbose` 开启 DEBUG 级日志,会在请求发送前打印请求详情(method、url、headers、body),方便定位问题:

  ```bash
  ar -v exec --agent <URN> --server 127.0.0.1:31180
  ```

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
