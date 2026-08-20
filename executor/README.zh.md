**中文** | [English](README.md)

# agent-dx-executor

面向自定义镜像 Agent 实例的平台 FaaS 代码包。它通过标准元戎 FaaS
initializer 启动 Executor HTTP Server 和用户进程。该 wheel 预装在 Agent
基础镜像中，不是面向用户的 SDK。

Executor 不依赖 agent-dx SDK 或 `yr` 包。镜像仍需包含兼容的 openYuanRong
Python Runtime，因为 Runtime 会将 Executor 的入口作为普通 FaaS 代码包加载。

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

Executor 启动时会在当前函数实例容器中初始化一个进程内 `SandboxInstance`，供它拉起的
Agent 进程通过 loopback HTTP 调用。该 API 不创建远程 `@yr.instance` actor，也不依赖
`yr` 包：

```text
POST /v1/sandbox/execute
POST /v1/sandbox/read_file
POST /v1/sandbox/write_file
POST /v1/sandbox/list_files
POST /v1/sandbox/search_files
```

请求和响应均为 `application/json`。`execute` 接收 `command`，以及可选的
`working_dir`、`env`、`timeout`；同时接受 `cwd`、`environment`、
`timeout_seconds` 别名。文件读写沿用 `path` 和 `mode` 语义，内容通过 `content`
传递；二进制内容必须声明 `content_encoding: "base64"`，文本使用 `"text"`。
`list_files` 和 `search_files` 的结果统一返回在 `items` 字段中。

`execute` 返回命令的 `returncode`、`stdout` 和 `stderr`；未指定 `timeout` 时默认
300 秒。命令在独立进程组中启动，超时后整个进程组会被终止，结果返回
`returncode: -1`。HTTP Server 最多同时处理 64 个请求，超过上限时返回 503。

Sandbox JSON 请求体和响应体分别使用独立的 512 MiB 默认上限，不受 Frontend 文件
接口的 `max_file_size` 影响。二进制文件以 Base64 放入 JSON，因此请求中的实际二进制
内容上限约为 JSON 上限的四分之三，并需扣除其他 JSON 字段占用的空间。

这组 Sandbox API 与现有的 `/v1/files/upload`、`/v1/files/download`、
`/v1/files/list` 相互独立。后者仍是 Frontend 与当前函数实例容器之间的管理面文件传输
接口，Sandbox handler 不会复用其 `FileHandler`。

Executor 从 `YR_RUNTIME_BOOTSTRAP_CMD` 读取用户进程命令，并保持原 Python
Runtime 实现的尽力启动语义：最多处理 64 个 argv 数组；格式错误的条目和单个命令的
启动失败会记录日志并跳过，不影响其他命令。

用户进程的 stdin 连接到 `/dev/null`。stdout 和 stderr 合并追加到
`${GLOG_log_dir}/bootstrap_cmd_<index>.log`；未设置 `GLOG_log_dir` 时默认使用
`/home/snuser/log/`，日志文件无法打开时两个输出流均回退到 `/dev/null`。

FaaS 的 `PRE_STOP_TIMEOUT` 限制关闭总时长。默认情况下，Executor 为强制终止和最终
清理预留两秒，其余时间作为子进程收到 SIGTERM 后的优雅退出时间。
