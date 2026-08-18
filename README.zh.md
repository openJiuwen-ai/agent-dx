**中文** | [English](README.md)

# Agent Distributed Executor（agent-dx）

## 简介

Agent Distributed Executor（简称 agent-dx）是面向 Agent 的分布式执行底座，用于承载 Agent 注册、调用、会话管理等开发者工具。仓库当前提供 Python CLI `adx`、独立构建的 agent-dx Python SDK，以及用于自定义镜像 Agent 实例的平台 Executor。

### 关键能力

当前 CLI 能力包括：

- 通过 openYuanrong meta_service 组件注册 agent/function。
- 通过 openYuanrong frontend 组件调用 agent/function，并以 SSE 方式流式输出执行结果。
- 支持 agent session 和 instance session 相关请求头。
- 支持一次性调用和交互式调用。

CLI 的安装、命令参数、示例、退出码和测试说明见 [cli/README.md](cli/README.md)。
agent-dx SDK 的编程模型、固定 Bootstrap 和部署配置见 [python/README.md](python/README.md)。
平台托管的自定义镜像 Agent Executor 见 [executor/README.zh.md](executor/README.zh.md)。

## 入门

- 安装：`pip install https://openyuanrong.obs.cn-southwest-2.myhuaweicloud.com/release/0.9.0/openeuler/{x86_64 or aarch64}/agent_dx_cli-0.9.0-py3-none-any.whl`。
- 依赖：需要先安装并部署 openYuanrong 支持函数服务能力，可参考 [openYuanrong 安装部署](https://docs.openyuanrong.org/zh-cn/latest/deploy/index.html)文档。

### CLI 工具

参考 openYuanrong [函数服务](https://docs.openyuanrong.org/zh-cn/latest/multi_language_function_programming_interface/development_guide/function_service/index.html)开发指南完成 Agent 开发，使用 CLI 工具 `adx` 注册：

```bash
adx deploy -s ./agent.json --server {meta_service_endpoint}
```
agent.json 示例：

```json
{
    "name":"0@ai@agent",
    "runtime":"python3.9",
    "handler":"agent.handler",
    "kind":"faas",
    "cpu":1000,
    "memory":1024,
    "timeout":600,
    "storageType":"local",
    "codePath":"/your/agent/code/absolute/path"
}
```

调用 agent：

```bash
adx exec --agent <agent_name> --server {frontend_endpoint} --args '{"message":"你好"}'
```

更多安装方式、参数说明和交互模式用法见 [cli/README.md](cli/README.md)。

### 项目目录结构

```text
cli/                 Python CLI 包源码与打包配置
cli/ar_cli/          adx 命令实现
python/              agent-dx Python SDK 独立包
executor/            平台 Agent Executor 独立包
tests/cli/           CLI 单元测试
tests/python/        agent-dx SDK 单元与集成测试
tests/executor/      Agent Executor 单元测试
pytest.ini           测试配置
```

CLI、SDK 和 Executor 是三个独立发布包，共享仓库根目录 `VERSION` 中的版本号。

## 贡献

欢迎开发者参与 agent-dx 的建设。你可以通过以下方式贡献：

- 提交 Bug、功能建议或使用问题：[Issues](https://gitcode.com/openJiuwen/agent-dx/issues)
- 提交代码、文档或示例：[Pull Requests](https://gitcode.com/openJiuwen/agent-dx/pulls)

## 许可证

[Apache License 2.0](./LICENSE)

本产品仅作为流程编排工具，不包含 AI 模型能力；用户在连接 AI 模型用于特定业务场景时，需自行承担欧盟 AI 法案等相关合规义务。
