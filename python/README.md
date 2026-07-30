# openYuanrong Agent Runtime Python SDK

本软件包提供面向用户的 Agent API 和固定的 FaaS 启动入口。它独立于 `ar` CLI
构建，仅通过 openYuanrong 提供的公共 `yr.datasystem` Python SDK 访问
DataSystem。

## 编写 Agent

在函数代码包的根目录创建 `agent.py`：

```python
from yuanrong.agentruntime import AgentExecutor, Complete, InputRequired


class Agent(AgentExecutor):
    async def init(self, session_context):
        self.history = await session_context.event_log.get()

    async def execute(self, ctx):
        message = ctx.input.message
        if message == {"action": "confirm"}:
            return Complete({"accepted": True})

        await ctx.output.write({"progress": "waiting"})
        await ctx.session_context.event_log.append(
            ctx,
            "agent.confirmation.requested",
            {"input": message},
        )
        return InputRequired({"question": "Continue?"})
```

模块和类名固定为 `agent.Agent`。`init()` 和 `execute()` 都是异步方法，
`execute()` 必须显式返回 `Complete(value)` 或 `InputRequired(value)`。

## 函数配置

函数使用 SDK 提供的启动入口，用户无需自行实现 FaaS handler：

```json
{
  "runtime": "python3.11",
  "handler": "yuanrong.agentruntime.bootstrap.handler",
  "extendedHandler": {
    "initializer": "yuanrong.agentruntime.bootstrap.initialize"
  },
  "extendedTimeout": {
    "initializer": 60
  },
  "enableSessionCtx": true
}
```

当前 FaaS 加载器会校验函数 `codePath` 下的入口模块。请将 SDK wheel 安装到
待打包的代码目录中，例如：

```bash
pip install --target <codePath> openyuanrong-agentruntime-sdk.whl
```

也可以通过函数层提供 SDK，但必须确保
`yuanrong/agentruntime/bootstrap.py` 包含在函数代码包中。

调度器必须开启 `enableSessionCtx`，才能绑定并注入 `YR_SESSION_CTX_ID`。本 SDK
不使用旧的 `enableAgentSession` 开关。调用函数时必须使用 SSE；继续已有会话时，
需要传入相同的 Session Context ID。SDK 会通过 `FunctionContext` 校验流式调用要求。

EventLog 写入复用 libruntime 使用的集群级
`YR_DATASYSTEM_DEFAULT_WRITE_MODE` 配置，默认值为 `NONE_L2_CACHE`。

初始化器会捕获并缓存启动错误。`agent.Agent` 定义错误、DataSystem 初始化失败或用户
`init()` 执行失败，都不会直接导致 FaaS 实例启动失败；后续调用会返回已缓存且稳定的
错误信息。

## Event 与 Turn 行为

- 每次被接受的调用都会写入 `input.message`。
- `ctx.output.write(value)` 会先持久化 `output.message`，再写入 SSE。
- `InputRequired(value)` 会持久化 `turn.input_required`，并保持当前 Turn 打开。
- `Complete(value)` 会持久化 `turn.completed`，并关闭当前 Turn。
- 执行发生异常时会持久化 `turn.failed`；下一次调用将创建新的 Turn。
- 最终返回值和 SSE EOF 由 FaaS runtime 追加，不由本 SDK 写入。

## 首个版本的限制

当前版本依赖调度器单一所有权和进程内串行执行。暂不实现请求 ID、重试去重、取消、
writer epoch、多写者 fencing、EventLog watermark、非流式调用和 SSE 重放。
Event 与 Turn key 按序号连续读取，遇到的第一个缺失 key 将被视为数据结尾。
