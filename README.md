# openYuanrong Agent Runtime

openYuanrong Agent Runtime 是 openYuanrong 的 agent 运行时接入仓库，用于承载
agent 注册、调用、会话管理等开发者工具。仓库当前提供 Python CLI `ar`，
后续也会在这里扩展 SDK 等接入能力。

## Current Status

当前已实现 CLI 能力：

- 通过 meta_service 注册 agent/function。
- 通过 frontend 调用 agent/function，并以 SSE 方式流式输出执行结果。
- 支持 agent session 和 instance session 相关请求头。
- 支持一次性调用和交互式调用。

CLI 的安装、命令参数、示例、退出码和测试说明见 [cli/README.md](cli/README.md)。

## Quick Start

安装 CLI：

```bash
cd cli
pip install .
```

注册 agent/function：

```bash
ar deploy -s ./agent.json --server 127.0.0.1:31182
```

调用 agent/function：

```bash
ar exec --agent <AGENT> --server 127.0.0.1:31180 --args '{"message":"你好"}'
```

更多安装方式、参数说明和交互模式用法见 [cli/README.md](cli/README.md)。

## Repository Layout

```text
cli/                 Python CLI 包源码与打包配置
cli/ar_cli/          ar 命令实现
tests/cli/           CLI 单元测试
pytest.ini           测试配置
```

## License

Apache-2.0
