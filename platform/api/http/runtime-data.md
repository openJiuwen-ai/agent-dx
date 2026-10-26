# Execd 用户数据 HTTP 契约

机器可读定义为 [data-plane.yaml](../openapi/data-plane.yaml)。用户通过 Ingress
访问 `POST /direct/{environmentId}/invoke`，请求为 `action`、`args` 和可选 `requestId`。
`environmentId` 按 URL 路径段编码，Ingress 只解码一次；例如名称中的 `+` 可编码为 `%2B`。
后续业务路径保持原始编码，解码后的 Environment ID 不允许控制字符或路径分隔符。
Ingress 校验 API Key 与归属，Relay 核对运行时绑定，再转发给 Execd 的 `/invoke`。
运行时协作与 Unix checkpoint 使用独立的 [控制契约](runtime-control.md)。

每种 action 在 OpenAPI 中有独立请求变体；`x-adx-result-schema` 指向对应响应。
响应正文不重复 action，因此 HTTP 200 的 `InvokeResult` 使用 `anyOf`，客户端按
已发送 action 解码。运行时业务响应不使用 Environment 生命周期 envelope。
表中列出主要名称；OpenAPI 同时列出实际支持的 `process.run`、`file.info`、
`shell.init` 等公共别名。旧 `cmd_*`、`fs_*`、`bash_*` 名称不作为公开契约。

| Action | args | 结果 |
|---|---|---|
| `process.exec` | `cmd` 或 `command`，可选 `cwd`、`envs`、`timeout` | 同步 `stdout`、`stderr`、`exit_code`、可选 `exit_signal` |
| `process.start` | `command_id`、`command`，可选 `cwd`、`envs`、`want_stdin`、`timeout` | command identity、PID、状态；启动失败可返回失败记录 |
| `process.get` | `command_id` 或兼容 `pid` | 当前命令记录、输出与终止原因 |
| `process.poll` | 命令身份、可选 `wait_timeout` | 等待期间退出或到期后的当前记录 |
| `process.wait` | 命令身份、可选 `timeout` | 最终记录，或进程仍运行的 `WAIT_TIMEOUT` |
| `process.kill` | 命令身份 | `status`、`killed`；不存在或已退出为 `killed=false` |
| `process.list` | `{}` | `processes` 命令记录数组 |
| `process.capabilities` | `{}` | `protocol_version`、`capabilities` |
| `process.send_stdin` | 命令身份、文本 `data`、可选 `eof` | `error`、`error_code`；需要启动时开启 stdin |
| `entrypoint.poll` | 可选 `wait_timeout` | 继承入口的运行／退出／错误状态 |
| `file.read` | `path`、可选 `binary` | `data`、`error`；二进制为十六进制字符串 |
| `file.write` | `path`、`data`、可选 `binary` | 文件名称、路径、类型、大小、`error` |
| `file.write_chunk` | `path`、十六进制 `data`、可选 `offset` | `bytes_written`、`offset`、`error`；offset=0 创建或截断 |
| `file.read_chunk` | `path`、可选 `offset`、`limit` | 十六进制 `data`、`bytes_read`、`eof`、`error` |
| `file.list` | `path`、可选 `depth` | `entries`、`error`；EntryInfo 含权限、大小、修改时间 |
| `file.search` | `path`，可选 `pattern`、`exclude_patterns`、`depth`、`max_results`、`max_entries`、`timeout_ms` | 按路径排序的 `entries`、`scanned_entries`、`error`；失败无部分结果 |
| `file.exists` | `path` | `exists` |
| `file.remove` | `path` | `error`；目录递归删除 |
| `file.rename` | `old_path`、`new_path` | 新路径的 EntryInfo 与 `error` |
| `file.make_dir` | `path` | `created`、`error`；创建缺失的父目录，已存在时 created=false |
| `file.stat` | `path` | EntryInfo 与 `error` |
| `shell.create` | `session_id`、可选 `shell` | `error`；创建持久 PTY shell，默认 `/bin/bash` |
| `shell.run` | `session_id`、`command`、兼容 `timeout` | 提交结果 `error`；之后 poll 读取执行结果 |
| `shell.poll` | `session_id`、兼容 `wait_timeout` | `running`／`done`／`error`；完成时有输出与 exit_code |
| `shell.close` | `session_id` | `error`；关闭 shell，重复关闭成功 |

## 命令、cwd 与超时

构造器 cwd 的初始化发生在 Execd Ready 前，见
[创建契约](../../../gateway/apiserver/docs/environment-lifecycle-api.md#constructor-working-directory)。
单次命令的 cwd 覆盖要求目录已存在，不会自动创建。

SDK `commands.run` 的默认／长等待路径和后台命令使用 `process.start`。
缺失 cwd 在这条路径产生 `status=FAILED`、`error_code=SPAWN_FAILED`、
`exit_code=null` 及 `error_message`；错误不会终止 Execd。短超时同步执行走
`process.exec`，其启动失败用 `exit_code=-1` 和 stderr 表示。两种原语的响应不能混用。

`process.exec.timeout` 是执行预算，省略无限等待，零立即到期。
`process.start.timeout` 省略或零不设置执行截止；正值到期终止进程组。
`process.wait.timeout` 只约束本次等待，不杀进程；到期仍返回 HTTP 200：

```json
{"status":"running","error_code":"WAIT_TIMEOUT","error_message":"command wait timed out","error":null}
```

`shell.run.timeout` 是 SDK 发送的兼容参数，后端不执行该截止；SDK 约束自己的
poll 等待。`shell.poll.wait_timeout` 当前不使后端阻塞。cwd 和环境变量由 SDK
编码到 shell 命令中，持久 shell 会保留其状态；PTY 输出可能含回显与协议标记。
`X-ADX-Processing-Timeout-Ms` 是 Ingress 的整条 HTTP 处理截止，和命令执行／等待
预算分开；流式响应已开始时截止会结束流，不可能再返回新的 JSON 错误正文。

## 身份、重试与错误

`file.search` 在运行时匹配文件名或完整路径，区分大小写，支持 `*`、`?` 和字符类；
`*` 也匹配路径分隔符。排除模式只过滤结果，不剪枝目录遍历；不会跟随子目录符号链接。
根路径不存在或不是目录时返回空结果。路径最多 4096 UTF-8 字节；模式最多 1024 UTF-8
字节，排除模式最多 64 个。默认 depth=64、max_results=1000、max_entries=100000、
timeout_ms=10000；上限依次为 64、10000、1000000、30000，均须为正整数。
超出数量或响应大小限制返回 HTTP 429 `RESOURCE_EXHAUSTED`，搜索超时返回
HTTP 408 `DEADLINE_EXCEEDED`；错误响应的 entries 为空，不返回部分匹配结果。

请求身份优先级为 `X-ADX-Request-ID`、`X-Request-Id`、正文 `requestId`。写操作重试
应保留同一身份和完全相同的参数；不得将一个身份用于不同操作。有限的内存重放缓存
不提供跨 Execd 重启的持久幂等保证。需要恢复命令结果时使用稳定 `command_id`；
PID 可以在运行时被复用，不能作为长期恢复身份。

| 结果 | HTTP／业务表现 |
|---|---|
| 正常结果、命令非零退出、等待超时 | HTTP 200；读取 action 对应的状态及错误字段 |
| 操作参数或文件／shell 操作错误 | HTTP 400，错误正文保留 action 字段 |
| 稳定命令不存在 | HTTP 404、`COMMAND_NOT_FOUND` |
| 稳定命令结果过期 | HTTP 410、`COMMAND_EXPIRED` |
| 同 command_id 不同启动参数 | HTTP 409、`COMMAND_CONFLICT` |
| 命令注册表资源耗尽 | HTTP 429、`RESOURCE_EXHAUSTED` |
| 缺失能力 | HTTP 501、`UNSUPPORTED_FEATURE` |
| Ingress 恢复仍在等待 | HTTP 503、`ENV_RESUMING`；仅符合网关标记与 requestForwarded=false 才可安全重试 |

信号 9 或退出码 137 单独不能证明 OOM。Execd 不采集 OOM 计数；Runtime 的 OOM 原因由
sandboxd 传至 Environment failure，SDK 按 [系统错误契约](error-contract.md#oom-原因与-sdk)
分类。HTTP 502/503 也不能直接推断 Runtime OOM。

文件 invoke 的二进制编码是十六进制；`/upload`、`/download` 的流式正文是原始字节。
大文件优先使用流式传输。PTY 与命令 watch 是 WebSocket 协议，其连接与帧定义见 OpenAPI
扩展；它们不使用 invoke 的普通 JSON 请求／响应模型。
