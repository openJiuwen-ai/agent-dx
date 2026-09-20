**中文** | [English](README.md)

# agent-dx-executor

面向自定义镜像 Agent 实例的平台 FaaS 代码包。它通过标准元戎 FaaS
initializer 启动 Executor HTTP Server 和用户进程。该 wheel 预装在 Agent
基础镜像中，不是面向用户的 SDK。

Executor 不依赖 agent-dx SDK。沙箱能力由 `yr.agentexecutor.sandbox` 子包
提供——它是 openYuanRong Runtime `yr.sandbox` SDK 的移植副本，复用镜像自带
`yr` 包的 native RPC（`@yr.instance`/`yr.get`）创建并操控远程子沙箱容器；
因此 `yr` 不写入包依赖，但镜像仍必须包含兼容的 openYuanRong Python
Runtime——Runtime 也会将 Executor 的入口作为普通 FaaS 代码包加载。打包依赖
为 `aiohttp`、`websockets`、`httpx`、`multidict`（沙箱隧道组件需要）。

Executor 需安装到元戎 Runtime 使用的 Python 环境中。它属于镜像基础设施，不占用
函数的 `DELEGATE_DOWNLOAD`；该字段仍可用于传入可选的用户进程代码包。例如可在
构建镜像时执行：

```text
python -m pip install --no-deps agent_dx_executor-<version>-py3-none-any.whl
```

该包支持 Python 3.9 及以上版本，不依赖特定 Python 次版本。该 wheel 的包根目录会安装到
Python 的 `platlib`，从而在 `purelib` 和 `platlib` 分别映射到 `lib` 和 `lib64` 的
系统上，使 `yr.agentexecutor` 与包含原生库的 openYuanRong Runtime 位于同一目录。
目前主要运行时目标为 Python 3.9 和 Python 3.11。

FaaS 入口如下：

```text
initializer: yr.agentexecutor.handler.initialize
handler:     yr.agentexecutor.handler.handle
pre-stop:    yr.agentexecutor.handler.pre_stop
```

## 沙箱 API

Executor 启动时创建 `SandboxManager` 并注入 HTTP Server，默认监听
`0.0.0.0:18093`（可用环境变量 `YR_AGENT_EXECUTOR_HOST`/`YR_AGENT_EXECUTOR_PORT`
覆盖），但仅接受回环来源的请求。同机进程通过回环 HTTP 按需创建、操控和销毁
**远程子沙箱容器**——操作对象是独立的子容器，不是函数实例容器本身。沙箱不预建：
调用方先 `POST /v1/sandbox/sandboxes` 创建，拿到 `instance_id` 后把它放进后续
请求的 URL 路径；多个沙箱按 id 隔离，各自独立生命周期。

### 通用约定

- **调用地址**：沙箱 API 只服务同机进程，必须用回环地址访问
  `http://127.0.0.1:{port}/...`。请求来源 IP 不属于回环段（IPv4 `127.0.0.0/8`、
  IPv6 `::1` 及 IPv4-mapped 回环地址）时返回 403——即使调用方与 Executor 同机，
  使用机器 IP 访问也会被拒绝：

  ```json
  {"message": "sandbox API is loopback-only"}
  ```

- **资源式 URL**：路径为 `/v1/sandbox/sandboxes[/{id}[/{action}]]`，按 HTTP 语义
  选方法——纯读 GET、创建/动作 POST、覆写 PUT、删除 DELETE。
- **请求格式**：`create`/`execute` 的请求体为 JSON，必须带
  `Content-Type: application/json`（否则 400），body 用 `Content-Length` 或
  chunked 传输均可；`files/write` 的 body 为文件原始内容（非 JSON），必须带
  `Content-Length`（缺失返回 400）。
- **尺寸上限**：沙箱 API 的请求（JSON body 与 `files/write` 的原始 body）与
  响应各有独立的 512 MiB 默认上限，超限均返回 413，不受 Frontend 文件接口
  `max_file_size` 影响。Base64 传输的二进制实际大小约为上限的四分之三。
- **并发**：HTTP Server 全局最多同时处理 64 个请求（含所有端点），超限返回 503
  并关闭连接：

  ```json
  {"message":"executor is busy"}
  ```

- **trace_id**：所有端点接受可选 `trace_id`（`create`/`execute` 放 JSON body，
  其余端点放 URL query），透传到整条调用链便于日志排查。1-128 个字符，仅允许
  `A-Z a-z 0-9 _ - . :`（覆盖 UUID、hex、点分名）；不传时由后端自行生成。
  注意格式非法时报 500 而非 400（校验发生在底层调用）。
- **状态码**：

  | 状态码 | 触发条件 |
  |---|---|
  | 200 | 成功（含命令执行失败等业务性结果，见各端点说明） |
  | 400 | 参数缺失、类型/取值非法、`Content-Type` 不符等请求错误 |
  | 403 | 请求来源非回环地址 |
  | 404 | URL 形状未注册，或路径中的 `{id}` 对应的沙箱不存在（`DELETE` 对不存在的 `{id}` 返回幂等 200） |
  | 405 | 路径存在但 HTTP 方法未注册，响应带 `Allow` 头（如对 `.../{id}/execute` 发 GET 返回 `Allow: POST`）；`{id}` 不存在时优先返回 404 |
  | 413 | 请求体或响应体超过 512 MiB 上限 |
  | 500 | 底层 RPC/IO 异常，`message` 为带操作前缀的底层真实原因（如 `execute failed: rpc timeout`） |
  | 503 | 并发超过 64 |

所有错误响应的 body 均为 `{"message": "..."}` 形式的 JSON。

### 1. 创建沙箱：`POST /v1/sandbox/sandboxes`

```http
POST /v1/sandbox/sandboxes HTTP/1.1
Content-Type: application/json

{
  "image": "python:3.12-slim",
  "cpu": 1000,
  "memory": 2048,
  "sandbox_type": "supervisor",
  "ports": ["tcp:8080", "9090"],
  "upstream": "127.0.0.1:8000",
  "working_dir": "/sandbox",
  "env": {"PATH": "/usr/bin"},
  "idle_timeout": 600
}
```

| 参数             | 类型                      | 必填 | 默认值            | 说明                                                                                                    |
|-----------------|---------------------------|------|------------------|---------------------------------------------------------------------------------------------------------|
| `image`         | string                    | 否   | 无                | docker 镜像名，`sandbox_type="docker"` 时生效                                                              |
| `cpu`           | int                       | 否   | 无                | CPU 资源，millicores                                                                                      |
| `memory`        | int                       | 否   | 无                | 内存，MB                                                                                                  |
| `sandbox_type`  | string                    | 否   | `""`              | 沙箱执行器类型：`""`/`"supervisor"`/`"docker"`                                                              |
| `ports`         | array\<string\>           | 否   | 无                | 端口转发列表，元素为 `"proto:port"`（如 `"tcp:8080"`）或 `"port"`                                             |
| `upstream`      | string                    | 否   | 无                | 反向隧道 upstream，本机服务地址 `host:port`，如 `"127.0.0.1:8000"`                                             |
| `working_dir`   | string                    | 否   | 沙箱内临时目录      | 沙箱内工作目录                                                                                              |
| `env`           | object\<string,string\>   | 否   | 继承沙箱运行环境    | 环境变量；指定时整体生效（不与默认环境合并）                                                                    |
| `idle_timeout`  | int                       | 否   | `300`             | 空闲超时（秒），必须大于 0                                                                                  |
| `user`          | string                    | 否   | 平台默认 `agentos` | 容器 run-as 用户，透传为 Docker 的 `Config.User`；支持 `"1000"`、`"1000:1000"`、`"root"` 等 Docker 接受的格式；仅 `sandbox_type="docker"` 时生效 |
| `trace_id`      | string                    | 否   | `""`              | 链路追踪 ID，1-128 字符，仅允许 `A-Z a-z 0-9 _ - . :`；不传由后端生成                                          |

**成功响应（200）**——create 为同步阻塞语义，返回 200 即子沙箱已就绪（创建过程
内含就绪验证），无需轮询状态：

```json
{
  "instance_id": "34420000-0000-4000-886a-980025037460"
}
```

`instance_id` 为平台实例 ID（UUID 形态），后续所有操作把它填进 URL 路径的 `{id}` 段。

**失败响应**：

- 400——参数类型/取值非法，如 `ports` 非字符串数组、`env` 键值非字符串、
  `cpu`/`memory`/`idle_timeout` 非整数、`idle_timeout <= 0`、`trace_id` 非字符串：

  ```json
  {"message": "idle_timeout must be greater than zero"}
  ```

- 500——创建失败，`message` 为 `create sandbox failed: ` 加底层原因。注意
  `ports` 元素的格式（`proto:port`）在创建阶段才校验，非法格式（如
  `"tcp:8080:9"`、`"tcp:abc"`）也走 500 而非 400：

  ```json
  {"message": "create sandbox failed: rpc timeout"}
  ```

### 2. 删除沙箱：`DELETE /v1/sandbox/sandboxes/{id}`

```http
DELETE /v1/sandbox/sandboxes/34420000-0000-4000-886a-980025037460 HTTP/1.1
```

| 参数         | 类型 | 必填 | 默认值 | 说明                                        |
|-------------|------|------|--------|---------------------------------------------|
| `{id}`（路径） | string | 是 | — | 沙箱实例 ID，create 返回的 `instance_id`     |

无请求 body。

**成功响应（200）**：

```json
{
  "success": true
}
```

**幂等删除（200）**——`{id}` 不存在时不发底层 terminate 请求，仍返回 200，
重复删除安全：

```json
{
  "success": false,
  "message": "sandbox not found"
}
```

**失败响应（500）**——仅当底层 terminate RPC 抛异常（例如平台侧实例已被
回收）时返回，`message` 为底层原始原因：

```json
{
  "message": "instance not found"
}
```

### 3. 执行命令：`POST /v1/sandbox/sandboxes/{id}/execute`

```http
POST /v1/sandbox/sandboxes/34420000-0000-4000-886a-980025037460/execute HTTP/1.1
Content-Type: application/json

{
  "command": ["ls", "-la"],
  "working_dir": "/tmp",
  "env": {"PATH": "/usr/bin"},
  "timeout": 30
}
```

| 参数          | 类型                       | 必填 | 默认值         | 说明                                                                 |
|---------------|----------------------------|------|----------------|----------------------------------------------------------------------|
| `{id}`（路径） | string                     | 是   | —              | 沙箱实例 ID，create 返回的 `instance_id`                               |
| `command`     | string \| array\<string\>  | 是   | —              | 命令。字符串经沙箱内 `/bin/sh -c` 执行；数组直接执行、不经 shell          |
| `working_dir` | string                     | 否   | 沙箱工作目录    | 本次命令的工作目录                                                     |
| `env`         | object\<string,string\>    | 否   | 继承沙箱环境变量 | 命令环境变量；指定时**整体替换**沙箱默认环境（不合并）                     |
| `timeout`     | number                     | 否   | 不限时         | 超时秒数，必须大于 0                                                    |
| `trace_id`    | string                     | 否   | 后端自行生成    | 链路追踪 ID，1-128 字符，仅允许 `A-Z a-z 0-9 _ - . :`                    |

**成功响应（200）**：

```json
{
  "returncode": 0,
  "stdout": "total 0\ndrwxr-xr-x  2 root root  40 Aug 31 12:00 .\n",
  "stderr": ""
}
```

**命令执行失败不是 HTTP 错误**——命令非零退出、超时、非法命令均在 200 响应中
以 `returncode` 表达：

命令超时（200，命令进程被终止）：

```json
{
  "returncode": -1,
  "stdout": "",
  "stderr": "Command timed out after 30 seconds"
}
```

非法命令（200，如空数组、数组含非字符串元素，命令不执行）：

```json
{
  "returncode": -1,
  "stdout": "",
  "stderr": "Error: cmd list cannot be empty"
}
```

**失败响应**：

- 400——缺 `command`，或 `working_dir`/`env`/`timeout`/`trace_id` 类型非法、
  `timeout <= 0`：

  ```json
  {"message": "command is required"}
  ```

- 404——`{id}` 对应的沙箱不存在：

  ```json
  {"message": "sandbox 34420000-0000-4000-886a-980025037460 not found"}
  ```

- 500——底层 RPC 异常：

  ```json
  {"message": "execute failed: rpc timeout"}
  ```

### 4. 读文件：`GET /v1/sandbox/sandboxes/{id}/files/read`

```http
GET /v1/sandbox/sandboxes/34420000-0000-4000-886a-980025037460/files/read?path=/sandbox/data.txt&mode=r HTTP/1.1
```

| 参数          | 类型  | 必填 | 默认值       | 说明                                                  |
|---------------|-------|------|--------------|-------------------------------------------------------|
| `{id}`（路径） | string | 是  | —            | 沙箱实例 ID，create 返回的 `instance_id`                |
| `path`        | query | 是   | —            | 沙箱内文件绝对路径                                      |
| `mode`        | query | 否   | `rb`         | 打开模式：`rb`（二进制）/ `r`（文本）                     |
| `trace_id`    | query | 否   | 后端自行生成  | 链路追踪 ID，1-128 字符，仅允许 `A-Z a-z 0-9 _ - . :`     |

**成功响应（200），文本模式**——`mode=r` 时 `content` 为文件文本，
`content_encoding` 为 `text`：

```json
{
  "path": "/sandbox/data.txt",
  "mode": "r",
  "content": "hello sandbox",
  "content_encoding": "text"
}
```

**成功响应（200），二进制模式**——`mode=rb`（默认）时 `content` 为文件内容的
Base64 编码，`content_encoding` 为 `base64`：

```json
{
  "path": "/sandbox/payload.bin",
  "mode": "rb",
  "content": "AAECAw==",
  "content_encoding": "base64"
}
```

读取通过 RPC 调用沙箱内原生 Python `open`，不依赖沙箱内的 shell 工具，
最小镜像（无 tar/cat/sh）也可用。

**失败响应**：

- 400——缺 `path`：

  ```json
  {"message": "path is required"}
  ```

- 404——`{id}` 对应的沙箱不存在：

  ```json
  {"message": "sandbox 34420000-0000-4000-886a-980025037460 not found"}
  ```

- 500——沙箱内文件不存在或不可读：

  ```json
  {"message": "read file failed: [Errno 2] No such file or directory: '/sandbox/data.txt'"}
  ```

### 5. 写文件：`PUT /v1/sandbox/sandboxes/{id}/files/write`

`path`/`mode` 走 URL query，文件内容为请求 body（原始内容，非 JSON）。

文本写入（`mode=w`，body 为原文本，须为 UTF-8 编码）：

```http
PUT /v1/sandbox/sandboxes/34420000-0000-4000-886a-980025037460/files/write?path=/sandbox/output.txt&mode=w HTTP/1.1
Content-Type: text/plain

hello sandbox
```

二进制写入（`mode=wb`，body 为 Base64 编码字符串）：

```http
PUT /v1/sandbox/sandboxes/34420000-0000-4000-886a-980025037460/files/write?path=/sandbox/payload.bin&mode=wb HTTP/1.1
Content-Type: text/plain

AAECAw==
```

| 参数          | 类型                  | 必填 | 默认值       | 说明                                                              |
|---------------|-----------------------|------|--------------|-------------------------------------------------------------------|
| `{id}`（路径） | string                | 是   | —            | 沙箱实例 ID，create 返回的 `instance_id`                            |
| `path`        | query                 | 是   | —            | 沙箱内文件绝对路径，父目录自动创建                                   |
| `mode`        | query                 | 否   | `wb`         | `wb`（写二进制）/ `w`（写文本）/ `a`（追加文本）/ `ab`（追加二进制）    |
| body          | string                | 是   | —            | 文件内容。`mode` 含 `b` 时 body 须为 Base64 字符串，否则为 UTF-8 文本 |
| `trace_id`    | query                 | 否   | 后端自行生成  | 链路追踪 ID，1-128 字符，仅允许 `A-Z a-z 0-9 _ - . :`                 |

body 编码方式由 `mode` 自动决定，调用方无需额外声明。注意：空 body 会被拒绝
（400），因此无法通过本接口写入空文件。

**成功响应（200）**：

```json
{
  "success": true,
  "path": "/sandbox/output.txt"
}
```

**失败响应**：

- 400——缺 `path`、body 为空、缺 `Content-Length`、二进制模式 body 非法
  Base64、文本模式 body 非 UTF-8：

  ```json
  {"message": "invalid base64 content: Incorrect padding"}
  ```

- 404——`{id}` 对应的沙箱不存在：

  ```json
  {"message": "sandbox 34420000-0000-4000-886a-980025037460 not found"}
  ```

- 500——沙箱内写入失败（如权限不足）：

  ```json
  {"message": "write file failed: [Errno 13] Permission denied: '/sandbox/output.txt'"}
  ```

### 6. 列目录：`GET /v1/sandbox/sandboxes/{id}/files/list`

```http
GET /v1/sandbox/sandboxes/34420000-0000-4000-886a-980025037460/files/list?path=/sandbox&recursive=true&max_depth=2&include_files=true&include_dirs=true HTTP/1.1
```

| 参数              | 类型          | 必填 | 默认值      | 说明                                                                  |
|-------------------|---------------|------|-------------|-----------------------------------------------------------------------|
| `{id}`（路径）     | string        | 是   | —           | 沙箱实例 ID，create 返回的 `instance_id`                                 |
| `path`            | query         | 是   | —           | 沙箱内目录绝对路径                                                      |
| `recursive`       | query（bool） | 否   | `false`     | 是否递归列举子目录；仅接受 `true`/`false`（大小写不敏感）                  |
| `include_files`   | query（bool） | 否   | `true`      | 结果是否包含文件                                                        |
| `include_dirs`    | query（bool） | 否   | `true`      | 结果是否包含目录                                                        |
| `max_depth`       | query（int）  | 否   | `20`        | 最大递归深度，`>=0`；仅在 `recursive=true` 时生效，传 `0` 等同未指定       |
| `trace_id`        | query         | 否   | 后端自行生成 | 链路追踪 ID，1-128 字符，仅允许 `A-Z a-z 0-9 _ - . :`                     |

**成功响应（200）**：

```json
{
  "items": [
    {
      "name": "data.txt",
      "path": "/sandbox/data.txt",
      "size": 12,
      "is_directory": false,
      "modified_time": "2026-08-31T12:00:00.123456",
      "type": ".txt"
    },
    {
      "name": "logs",
      "path": "/sandbox/logs",
      "size": 0,
      "is_directory": true,
      "modified_time": "2026-08-31T11:30:00.456789",
      "type": null
    }
  ]
}
```

`items` 内每个条目的字段：

- `name`：文件/目录名（不含路径）。
- `path`：沙箱内绝对路径。
- `size`：字节数；目录恒为 `0`。
- `is_directory`：是否为目录。
- `modified_time`：最后修改时间，沙箱本地时间、微秒精度、不带时区
  （格式 `YYYY-MM-DDTHH:MM:SS.ffffff`）。
- `type`：文件扩展名（含点，如 `.txt`）；目录或无扩展名的文件为 `null`。

结果最多返回 10000 条，达到上限后静默截断；无法读取的子目录会被跳过。

**失败响应**：

- 400——缺 `path`、`max_depth` 非整数或为负、布尔参数非 `true`/`false`：

  ```json
  {"message": "recursive must be a boolean ('true' or 'false')"}
  ```

- 404——`{id}` 对应的沙箱不存在：

  ```json
  {"message": "sandbox 34420000-0000-4000-886a-980025037460 not found"}
  ```

- 500——沙箱内目录不存在：

  ```json
  {"message": "list files failed: path not found: /sandbox"}
  ```

### 7. 搜索文件：`GET /v1/sandbox/sandboxes/{id}/files/search`

```http
GET /v1/sandbox/sandboxes/34420000-0000-4000-886a-980025037460/files/search?path=/sandbox&pattern=*.txt&exclude_patterns=ignore*,*.bak HTTP/1.1
```

| 参数                 | 类型           | 必填 | 默认值       | 说明                                                                   |
|----------------------|----------------|------|--------------|------------------------------------------------------------------------|
| `{id}`（路径）        | string         | 是   | —            | 沙箱实例 ID，create 返回的 `instance_id`                                  |
| `path`               | query          | 是   | —            | 搜索根目录绝对路径                                                       |
| `pattern`            | query          | 是   | —            | glob 匹配模式（如 `*.txt`），对**文件名**匹配；只返回文件，不返回目录       |
| `exclude_patterns`   | query（array） | 否   | 无           | 排除模式列表，逗号分隔（如 `ignore*,*.bak`），同样对文件名匹配              |
| `trace_id`           | query          | 否   | 后端自行生成 | 链路追踪 ID，1-128 字符，仅允许 `A-Z a-z 0-9 _ - . :`                      |

搜索在 `path` 下递归进行，匹配的文件按条目返回。

**成功响应（200）**：

```json
{
  "items": [
    {
      "name": "data.txt",
      "path": "/sandbox/data.txt",
      "size": 12,
      "is_directory": false,
      "modified_time": "2026-08-31T12:00:00.123456",
      "type": ".txt"
    }
  ]
}
```

`items` 内每个条目的字段：

- `name`：文件名（不含路径）。
- `path`：沙箱内绝对路径。
- `size`：字节数。
- `is_directory`：是否为目录（本接口恒为 `false`）。
- `modified_time`：最后修改时间，沙箱本地时间、微秒精度、不带时区
  （格式 `YYYY-MM-DDTHH:MM:SS.ffffff`）。
- `type`：文件扩展名（含点，如 `.txt`）；无扩展名的文件为 `null`。

**无匹配（200）**——没有文件命中，或搜索根目录不存在（不报错），均返回空
列表；无法读取的子目录会被跳过：

```json
{
  "items": []
}
```

**失败响应**：

- 400——缺 `path` 或 `pattern`：

  ```json
  {"message": "pattern is required"}
  ```

- 404——`{id}` 对应的沙箱不存在：

  ```json
  {"message": "sandbox 34420000-0000-4000-886a-980025037460 not found"}
  ```

- 500——底层 RPC 异常：

  ```json
  {"message": "search files failed: rpc timeout"}
  ```

### 生命周期与回收

- `instance_id` 即平台实例 ID（UUID 形态）；functionsystem 内部维护实例到
  物理沙箱的映射，调用方无需感知物理沙箱 ID。
- create 同步阻塞：返回 200 即就绪，失败 500，无需轮询。
- 多实例：可同时 create 多个沙箱，按 id 各自操作、互相隔离，删除一个不影响
  其他。
- 兜底销毁：FaaS `pre_stop` 触发 Executor `stop()` 时，按「停 HTTP Server →
  按 grace 停用户进程 → `terminate_all()` 尽力销毁全部子沙箱」的顺序收尾；
  启动失败路径同样兜底销毁已创建的沙箱，避免远程容器泄漏。
- 沙箱 API 与 `/v1/files/upload`、`/v1/files/download`、`/v1/files/list`
  管理面文件接口相互独立：后者是 Frontend 与函数实例容器之间的文件传输通道，
  沙箱 handler 不复用其 `FileHandler`，尺寸上限也各自独立。

## 用户进程

Executor 从 `YR_RUNTIME_BOOTSTRAP_CMD` 读取用户进程命令，并保持原 Python
Runtime 实现的尽力启动语义：最多处理 64 个 argv 数组；格式错误的条目和单个命令的
启动失败会记录日志并跳过，不影响其他命令。

用户进程的 stdin 连接到 `/dev/null`。stdout 和 stderr 合并追加到
`${GLOG_log_dir}/${YR_RUNTIME_ID}.std`（每 runtime 一个文件，全部命令共用）；未设置
`GLOG_log_dir` 时默认使用 `/home/snuser/log/`，日志文件无法打开时两个输出流均回退到
`/dev/null`。缺失或不安全的 runtime ID 会替换为路径安全的文件名。

FaaS 的 `PRE_STOP_TIMEOUT` 限制关闭总时长。默认情况下，Executor 为强制终止和最终
清理预留两秒，其余时间作为子进程收到 SIGTERM 后的优雅退出时间。
