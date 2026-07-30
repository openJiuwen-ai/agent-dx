**中文** | [English](README_EN.md)

## 简介

openYuanrong Agent Runtime 是 openYuanrong 的 agent 运行时接入仓库，用于承载 agent 注册、调用、会话管理等开发者工具。仓库当前提供 Python CLI `ar` 和独立构建的 Python AgentRuntimeSDK。

### 关键能力

当前 CLI 能力包括：

- 通过 openYuanrong meta_service 组件注册 agent/function。
- 通过 openYuanrong frontend 组件调用 agent/function，并以 SSE 方式流式输出执行结果。
- 支持 agent session 和 instance session 相关请求头。
- 支持一次性调用和交互式调用。

CLI 的安装、命令参数、示例、退出码和测试说明见 [cli/README.md](cli/README.md)。
AgentRuntimeSDK 的编程模型、固定 Bootstrap 和部署配置见 [python/README.md](python/README.md)。

## 入门

- 安装：`pip install https://openyuanrong.obs.cn-southwest-2.myhuaweicloud.com/release/0.9.0/openeuler/{x86_64 or aarch64}/openyuanrong_agentruntime-0.9.0-py3-none-any.whl`。
- 依赖：需要先安装并部署 openYuanrong 支持函数服务能力，可参考 [openYuanrong 安装部署](https://docs.openyuanrong.org/zh-cn/latest/deploy/index.html)文档。

### CLI 工具

参考 openYuanrong [函数服务](https://docs.openyuanrong.org/zh-cn/latest/multi_language_function_programming_interface/development_guide/function_service/index.html)开发指南完成 Agent 开发，使用 CLI 工具 ar 注册：

```bash
ar deploy -s ./agent.json --server {meta_service_endpoint}
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
ar exec --agent <agent_name> --server {frontend_endpoint} --args '{"message":"你好"}'
```

更多安装方式、参数说明和交互模式用法见 [cli/README.md](cli/README.md)。

### 项目目录结构

```text
cli/                 Python CLI 包源码与打包配置
cli/ar_cli/          ar 命令实现
python/              Python AgentRuntimeSDK 独立包
tests/cli/           CLI 单元测试
tests/python/        AgentRuntimeSDK 单元与集成测试
pytest.ini           测试配置
```

CLI 和 SDK 是两个独立发布包，共享仓库根目录 `VERSION` 中的版本号。

## 贡献

我们欢迎您做各种形式的贡献，请参阅我们的[贡献者指南](https://docs.openyuanrong.org/zh-cn/latest/contributor_guide/index.html)。

## 许可证

[Apache License 2.0](./LICENSE)
