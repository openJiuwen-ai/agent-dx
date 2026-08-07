# AGENTS.md

## Overall Goal
你正在 `/opt/openyuanrong/yuanrong-agentruntime` 这个子项目中协作开发。该项目是 Agent Distributed Executor（agent-dx）的独立 Python CLI 包，提供 `adx` 命令，把 agent 的注册、调用封装为对底层 openYuanrong FaaS HTTP 接口的访问。

默认先理解当前 CLI 行为、测试约束和上级工作区约定，再做最小侵入式修改。不要把本仓的改动混入上级 `yuanrong/` 主集成仓或其他 submodule。

## Repository Layout

- `README.md`: 子项目简介。
- `cli/`: Python 包源码与打包配置。
- `cli/README.md`: `adx` 命令安装、使用、退出码和测试说明。
- `cli/setup.py`: `agent-dx-cli` 包定义，console entry point 为 `adx=ar_cli.main:main`。
- `cli/ar_cli/`: CLI 实现。
- `tests/cli/`: CLI 单元测试。
- `pytest.ini`: 测试配置，已将 `cli/` 加入 `pythonpath`。

## Core Modules

### CLI entrypoint
- `cli/ar_cli/main.py`: `click` 根命令、全局 `-v/--verbose`、`--version` 和子命令注册。
- `cli/ar_cli/__init__.py`: 包版本号来源，`setup.py` 会读取这里的 `__version__`。

### Commands
- `cli/ar_cli/commands/deploy.py`: `adx deploy`，通过 meta_service 注册 agent/function。
- `cli/ar_cli/commands/exec.py`: `adx exec`，调用 frontend 并流式输出 SSE 响应。
- `cli/ar_cli/commands/__init__.py`: 已启用命令列表；新增命令时需要在这里显式注册。

### Shared logic
- `cli/ar_cli/api/`: HTTP client、函数调用、SessionCtx 和 SSE 协议封装。
- `cli/ar_cli/interactive/`: 交互循环、SessionCtx 生命周期、查询、渲染和本地状态管理。
- `cli/ar_cli/utils.py`: 地址归一化、JSON/spec 解析、日志配置等通用工具。
- `cli/ar_cli/errors.py`: CLI 错误与退出码承载。
- `cli/ar_cli/const.py`: 常量定义。

## Runtime Contracts

- `adx deploy` 的 `--server` 指向 meta_service，地址格式为 `host:port`，未显式传 scheme 时默认使用 `http://`。
- `adx deploy -s/--spec` 支持 inline JSON 字符串或 JSON 文件路径，spec 必须解析为 JSON object。
- `adx deploy` 在 spec 未包含 `enableSessionCtx` 时默认注入 `true`；若用户显式设置 `true` 或 `false`，必须保留原值。
- `adx exec` 的 `--server` 指向 frontend，地址格式同样为 `host:port`。
- `adx exec` 只有在传入 `--session-ctx` 时才发送 `X-Agent-Session`。
- `adx exec` 只有在传入 `--session-id` 时才发送 `X-Instance-Session`，并附带实例 session TTL/concurrency 默认值。
- `adx exec --args` 必须是合法 JSON 字符串；不传时进入交互模式，每轮输入包装为 `{"message":"..."}`。
- `adx exec` 交互模式下若用户未传 `--session-ctx`，会自动生成一个并在每次 POST 中复用。
- 普通日志走 stderr，流式数据走 stdout；不要把 debug 日志混入 stdout。
- 退出码约定：`0` 成功，`1` 服务端失败，`2` 参数错误，`3` 网络错误。

## Build, Run, And Test

### Install locally
在 `cli/` 目录下安装：

```bash
pip install .
```

或构建 wheel：

```bash
python setup.py bdist_wheel
pip install dist/agent_dx_cli-*.whl
```

### Run CLI from source
在仓库根目录可依赖 `pytest.ini` 的 `pythonpath = cli` 运行测试；手动从源码运行时确保 `cli/` 在 `PYTHONPATH` 中，或先安装包。

### Tests
在仓库根目录执行：

```bash
python -m pytest -q
```

新增或修改命令行为时，优先补充 `tests/cli/` 下的单元测试。涉及 HTTP 行为时优先 mock client/response，不依赖真实 openYuanrong 服务。

## Working Rules

- 修改前先确认影响的是命令参数、HTTP 请求协议、SSE 解析、session header，还是打包入口。
- 保持 CLI 对外参数、stdout/stderr、退出码兼容；任何行为变化都需要同步更新 `cli/README.md` 和测试。
- 新增命令时使用 `click` 现有风格，并在 `cli/ar_cli/commands/__init__.py` 注册。
- 对 JSON/spec 处理优先复用 `utils.py` 中已有解析逻辑，避免重复实现。
- 对请求地址处理优先复用 `normalize_addr`，避免在命令模块中拼接 URL 细节。
- 对调用和 session header 处理优先复用 `api/` 下的协议封装，不要在命令模块中分散规则。
- 修改 SSE 或 SessionCtx 行为时同时检查 `tests/cli/test_sse.py`、`tests/cli/test_interactive.py` 和 `tests/cli/test_session_context_client.py`。
- 这个仓库是上级 `/opt/openyuanrong` 工作区的一部分，但不是 `yuanrong/` 主集成仓；不要在本子项目任务中修改 `../yuanrong/`、`../yuanrong/functionsystem/`、`../yuanrong/datasystem/` 或 `../yuanrong/frontend/`，除非用户明确要求。

## Commit Rules

- Commit 信息沿用上级工作区约定，使用 PR 类型前缀，格式如 `fix: xxxxx`。
- Commit 正文说明具体修改内容和修改原因。
- 提交时使用 `git commit -s`，确保包含 `Signed-off-by`。
