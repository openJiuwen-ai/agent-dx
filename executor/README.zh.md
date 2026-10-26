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

执行器启动时创建 `SandboxManager` 并注入 HTTP Server,后者默认监听
`0.0.0.0:18093`(可用环境变量 `YR_AGENT_EXECUTOR_HOST`/
`YR_AGENT_EXECUTOR_PORT` 覆盖),但仅接受回环来源的请求。同机进程通过回环
HTTP 创建、操作与销毁**远端子沙箱容器**——目标是独立的子容器,不是函数实例
容器本身。沙箱不预创建:调用方先 `POST /api/environment/v1/environments` 创建一个,
拿到环境 id 后放入后续请求的 URL 路径;多个沙箱按 id 隔离,各自独立生命周期。

本 API 遵循平台 yaml 契约(管理面 `sandbox.yaml` + 数据面 `data-plane.yaml`),
由两族端点组成:

- **管理面**——`POST /api/environment/v1/environments`(创建)与
  `DELETE /api/environment/v1/environments/{environmentId}`(删除)。成功响应为
  Envelope 形态(`data` 为 Base64 编码的 JSON);失败响应为 ErrorEnvelope
  形态。
- **数据面**——`POST /direct/{environmentId}/invoke`,单一动作分发端点,携带
  原生动作 JSON;action 级失败走 HTTP 200、错误在 body。

旧前缀 `/v1/sandbox/sandboxes` **不再注册**:任何请求一律返回 404
`{"message": "endpoint not found"}`。

### 通用约定

- **基础地址**:沙箱 API 仅服务同机进程,必须通过回环地址
  `http://127.0.0.1:{port}/...` 访问。来源 IP 不在回环范围(IPv4
  `127.0.0.0/8`、IPv6 `::1` 及 IPv4 映射回环地址)内的请求一律 403 拒绝
  ——即使同机调用方使用本机 IP 也会被拒:

  ```json
  {"message": "sandbox API is loopback-only"}
  ```

- **X-Request-Id(管理面)**:可选的请求标识(幂等键:重试须保持同一 id 且
  同一 body)。携带时在成功与错误响应的 `X-Request-Id` 响应头中回显;缺省
  时服务端生成 UUID 并同样回显。生效的 id 也会以 `requestId` 出现在
  Envelope 内。
- **X-Trace-ID**(私有约定,契约外):两族端点均可选的链路追踪 id,适配层
  读取后传给底层 RPC,并复用于 agent 进出日志对。旧的 `trace_id` body 字段
  返回 400(`trace_id moved to X-Trace-ID header`)。
- **请求格式**:请求体为 JSON,必须携带 `Content-Type: application/json`
  (否则 400);body 可用 `Content-Length` 或 chunked 传输编码。非合法 JSON
  或非 JSON 对象一律 400。
- **尺寸上限**:沙箱 API 的请求与响应各自独立默认上限 512 MiB,超出返回
  413;该上限与前端文件 API 的 `max_file_size` 相互独立。二进制内容以 hex
  编码传输(见各文件 action),有效负载为上限的一半。
- **并发**:HTTP Server 全局(所有端点合计)最多同时处理 64 个请求,超限
  返回 503 并断开连接:

  ```json
  {"message":"executor is busy"}
  ```

- **管理面错误模型**:管理面所有非 200 响应为 ErrorEnvelope,携带符号码、
  重试提示与结果语义,生效请求 id 同时出现在响应头与 body:

  ```json
  {"code": 400, "message": "cpu must be an integer", "data": null,
   "error": {"code": "invalid", "retry": "never",
             "outcome": "not_started", "requestId": "req-create-013"}}
  ```

  | HTTP | error.code | retry | outcome | 触发条件 |
  |---|---|---|---|---|
  | 400 | `invalid` | `never` | `not_started` | 参数类型/取值错误、Content-Type 错误、JSON 非法 |
  | 404 | `not_found` | `never` | `terminal` | 沙箱 id 未知、子资源未注册 |
  | 405 | `method_not_allowed` | `never` | `not_started` | 路径存在但方法未注册,响应携带 `Allow` 头 |
  | 413 | `too_large` | `never` | `not_started` | 请求或响应超尺寸上限 |
  | 500 | `internal` | `after_backoff` | `unknown` | 底层 RPC/IO 失败 |

- **数据面错误模型**:传输与参数类错误(400 未知 action 或参数非法、404
  实例或路径形状未知、405 带 `Allow`、413、500)返回 `{"error": "..."}`
  形态。action 级失败——分发已完成但操作本身失败(缺文件、RPC 失败等)——
  走 HTTP 200,错误在动作结果 body 内(见各 action)。

### 1. 创建环境:`POST /api/environment/v1/environments`

```http
POST /api/environment/v1/environments HTTP/1.1
Content-Type: application/json
X-Request-Id: req-create-013
X-Trace-ID: trace-abc

{
  "image": "python:3.12-slim",
  "runtime": "supervisor",
  "cpu": 2000,
  "memory": 4096,
  "env": {"PATH": "/usr/bin"},
  "ports": ["tcp:8080"],
  "idle_timeout": 600,
  "extra_config": {"working_dir": "/sandbox"}
}
```

请求头:`X-Request-Id`(可选,生成/回显)、`X-Trace-ID`(可选,私有追踪)。

请求体字段(适配层消费的 13 个契约字段):

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `name` | string | 否 | 无 | 沙箱名称;实例以该名称创建并寻址 |
| `image` | string | 否 | 无 | 镜像名;仅 `runtime="docker"` 时生效 |
| `runtime` | string | 否 | `""` | 运行时/执行器类型,直通底层执行器选择;空串走底层默认执行器 |
| `rootfs` | string 或 object | 否 | 无 | 根文件系统:字符串原样透传;对象 `{type, imageurl, ...}` 由适配层组装为 rootfs JSON(mounts 并入)。与 `image` 互斥 |
| `cpu` | int | 否 | 无 | CPU 资源(millicores) |
| `memory` | int | 否 | 无 | 内存(MiB) |
| `env` | object\<string,string\> | 否 | 继承沙箱运行时 | 环境变量;指定后整体生效(不与默认环境合并) |
| `ports` | array\<string\> | 否 | 无 | 端口转发列表;元素为 `"proto:port"`(如 `"tcp:8080"`)或 `"port"` |
| `idleTimeoutSeconds` | int | 否 | `0` | 空闲观察窗(秒),0 关闭空闲回收。`idle_timeout` 为兼容别名 |
| `mounts` | array\<object\> | 否 | 无 | 挂载声明;原样并入 rootfs JSON 的 `mounts` 键透传后端。须搭配对象形态 `rootfs` 或 `image`(字符串 `rootfs` 不接受 mounts) |
| `snapshotId` | string | 否 | 无 | 从快照创建:分流到恢复链路(`Sandbox(checkpoint_id=...)`),其余参数忽略 |
| `tunnel` | object | 否 | 无 | `{enabled, proxyPort}`:`proxyPort` 接底层代理端口;`enabled=true` 须搭配 `extra_config.upstream`(否则 400);`enabled=false` 显式关断 upstream 隐含的隧道 |
| `extra_config` | object | 否 | 无 | 运行时自定义键通道:`upstream`/`working_dir`/`user` 三键提取映射底层字段,剩余键整体透传 |

执行器类型由 body 顶层的 `runtime` 字段承载。私有字段
`upstream`/`working_dir`/`user` 位于 `extra_config` 内,`trace_id` 位于
`X-Trace-ID` 头:顶层携带这些字段、或在 `extra_config` 内携带
`sandbox_type`,均返回 400 并提示正确位置。

**成功(200)**——创建同步阻塞:200 即子沙箱就绪(创建含就绪验证),无需
轮询。响应头回显 `X-Request-Id`;`data` 为 Base64 编码的 JSON:

```json
{"code": 200, "message": "ok",
 "data": "eyJzYW5kYm94SWQiOiJzYngtMTIzIi..."}
```

`data` 明文:

```json
{"environmentId": "34420000-0000-4000-886a-980025037460",
 "status": "running", "requestId": "req-create-013"}
```

`environmentId` 即底层 create 返回的平台实例 id;`status` 恒为
`"running"`,`requestId` 为本次生效的请求 id。后续所有操作将该 id 放入
URL 路径。

**失败响应**为 ErrorEnvelope 形态,例如:

- 400——参数类型/取值非法(`cpu must be an integer`、
  `idle_timeout must be non-negative`、`rootfs and image are mutually
  exclusive`、`tunnel.enabled=true requires extra_config.upstream` 等)。
- 500——创建失败;`message` 为 `create sandbox failed: ` 加底层原因
  (`Failed to initialize sandbox instance: ...`)。注意 `ports` 元素格式
  (`proto:port`)仅在创建时校验,非法元素同样表现为 500。

### 2. 删除环境:`DELETE /api/environment/v1/environments/{environmentId}`

```http
DELETE /api/environment/v1/environments/34420000-0000-4000-886a-980025037460 HTTP/1.1
X-Request-Id: req-del-007
```

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `{environmentId}`(路径) | string | 是 | 环境 id,即 create 返回的 `environmentId` |
| `X-Request-Id`(头) | string | 否 | 同创建,生成/回显 |

无请求体。

**成功(200)**——`data` 明文为 `{"success": true}`:

```json
{"code": 200, "message": "ok", "data": "eyJzdWNjZXNzIjp0cnVlfQ=="}
```

**沙箱不存在(404)**——删除未知或已删除的沙箱返回 404 + ErrorEnvelope
(旧接口为 200 + `success:false`,已变更):

```json
{"code": 404, "message": "sandbox sbx-123 not found", "data": null,
 "error": {"code": "not_found", "retry": "never", "outcome": "terminal",
           "requestId": "req-del-007"}}
```

### 3. 运行时动作分发:`POST /direct/{environmentId}/invoke`

数据面单一端点分发受支持的动作。响应为原生动作 JSON(无 Envelope);
action 级失败走 HTTP 200、错误在 body。

公共参数:

| 参数 | 位置 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `{environmentId}` | path | 是 | — | 环境实例 id |
| `X-ADX-Request-ID` | header | 否 | 见 `requestId` | 请求标识(幂等键);优先级:头 > body.requestId > 服务端生成 UUID |
| `X-Trace-ID` | header | 否 | `""` | 链路追踪(私有约定,契约外) |
| `action` | body | 是 | — | 6 个受支持动作之一:`process.exec`、`process.run`、`file.read`、`file.write`、`file.list`、`file.search`;契约其余 action(含分块 `file.read_chunk`/`file.write_chunk`)一律返回 400 |
| `args` | body | 否 | `{}` | 各动作参数(见下) |
| `requestId` | body | 否 | 服务端生成 UUID | 请求标识的 body 形式 |

**action `process.exec` / `process.run`**——命令执行(两个名字同义):

| args | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `cmd` | string 或 array\<string\> | 与 `command` 二选一(`cmd` 优先) | 无 | 命令;字符串经沙箱内 `/bin/sh -c` 执行,数组不经 shell 直接执行(契约外扩展) |
| `command` | string 或 array\<string\> | 否 | 无 | `cmd` 的并列字段(契约默认空串) |
| `cwd` | string | 否 | 无 | 工作目录(`working_dir` 并列同收);须已存在 |
| `envs` | object\<string,string\> | 否 | 无 | 环境变量(`env` 并列同收);指定后整体替换默认环境 |
| `timeout` | number,≥0 | 否 | 不限 | 超时秒数;0 为立即超时(契约语义) |

```json
{"action": "process.exec", "args": {"cmd": "ls -la", "cwd": "/ws", "timeout": 300},
 "requestId": "invoke-001"}
```

响应:`{"stdout": "...", "stderr": "...", "exit_code": 0}`——注意字段为
`exit_code`(旧接口 `returncode` 已改名)。RPC 失败仍返回 200:
`{"stdout": "", "stderr": "execute failed: ...", "exit_code": -1}`。

**action `file.read`**——整读:

| args | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `path` | string | 是 | — | 文件绝对路径 |
| `binary` | bool | 否 | `false` | `true` 时 `data` 为 **hex** 编码;`false` 为 UTF-8 文本 |

响应:`{"data": "...", "error": null}`;action 级失败(如缺文件):
`{"data": null, "error": "read file failed: ..."}`(仍 HTTP 200)。

**action `file.write`**——整写(自动创建父目录):

| args | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `path` | string | 是 | — | 目标路径 |
| `data` | string | 是 | — | 内容(可为空串——写空文件);`binary=true` 时为 **hex** |
| `binary` | bool | 否 | `false` | 编码选择,同 `file.read` |
| `append` | bool | 否 | `false` | 追加而非截断(契约外扩展);与 `binary` 组合映射底层 `a`/`ab` |

响应:`{"name": "a.txt", "path": "/ws/a.txt", "type": "file", "size": 5,
"error": null}`。

**action `file.list`**——目录列表:

| args | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `path` | string | 是 | — | 目录路径 |
| `depth` | int | 否 | `1`(clamp 1–64) | 递归深度(返回相对深度 ≤ N 的条目) |

响应条目对齐 execd 结构——`type` 为类别(`file`/`dir`)、`modified_time`
为 unix 秒、`permissions` 恒 `null`(py 底层无权限数据;execd 实现返回
八进制权限串,属两实现的已知差异):

```json
{"entries": [{"name": "a.py", "path": "/ws/a.py", "type": "file",
              "size": 1024, "permissions": null,
              "modified_time": 1695441203.5}], "error": null}
```

**action `file.search`**——递归文件搜索:

| args | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `path` | string | 是 | — | 搜索根目录;根不存在或非目录返回空结果 |
| `pattern` | string | 否 | `*` | 大小写敏感的文件名或完整路径 glob,`*` 也匹配路径分隔符 |
| `exclude_patterns` | array\<string\> | 否 | `[]` | 排除 glob,只过滤结果、不剪枝遍历 |
| `depth` | int | 否 | `64`(clamp 1–64) | 递归深度上限,底层遍历按深度剪枝 |
| `max_results` | int | 否 | `1000`(clamp 1–10000) | 结果条数上限,超出返回 HTTP 429 |
| `max_entries` | int | 否 | `100000`(clamp 1–1000000) | 校验接受;py 底层不执行扫描上限,不触发 429 |
| `timeout_ms` | int | 否 | `10000`(clamp 1–30000) | 校验接受;py 底层无搜索超时,不触发 408 |

响应:`{"entries": [...], "scanned_entries": <int>, "error": null}`,条目为
`file.list` 形态、按 path 排序,`scanned_entries` 为遍历遇到的条目数(文件
与目录,不含根,受 `depth` 约束;子目录符号链接计入扫描但不跟随)。
action 级失败(仍 200):`{"entries": [],
"error_code": "FILESYSTEM_ERROR", "error": "search files failed: ..."}`;
超出 `max_results` 返回 HTTP 429,体为
`{"entries": [], "error_code": "RESOURCE_EXHAUSTED", "error": ...}`。

### 生命周期与回收

- `environmentId` 即平台实例 id(UUID 形态);functionsystem 内部维护实例
  到物理沙箱的映射,调用方无需感知物理沙箱 id。
- 命名创建:设置 `name` 时,实例以运行时命名空间(`{ns}-{name}`)创建并
  寻址;适配层钉住 namespace,保证 create 侧注册与 invoke 侧寻址一致。
- 创建同步阻塞:200 即就绪,500 即失败,无需轮询。
- 多实例:可同时创建多个沙箱,按 id 独立操作与隔离;删除互不影响。
- 删除为终态:同一 id 再次删除返回 404。
- 尽力回收:FaaS `pre_stop` 触发执行器 `stop()` 时,按"停 HTTP Server →
  宽限停用户进程 → `terminate_all()` 尽力销毁全部子沙箱"的顺序收尾;
  启动失败路径同样回收已创建的沙箱,避免远端容器泄漏。
- 沙箱 API 与 `/v1/files/upload`、`/v1/files/download`、`/v1/files/list`
  管理面文件端点相互独立:后者是前端与函数实例容器之间的文件传输通道,
  沙箱 handler 不复用其 `FileHandler`,尺寸上限也相互独立。
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
