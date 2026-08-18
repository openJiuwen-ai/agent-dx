[中文](README.zh.md) | **English**

# Agent Distributed Executor (agent-dx)

## Introduction

Agent Distributed Executor (agent-dx) is a distributed execution substrate for agents, providing developer tools for agent registration, invocation, and session management. The repository currently offers the Python CLI `adx`, a separately built agent-dx Python SDK, and a platform-owned Executor for custom-image Agent instances.

### Key Capabilities

Current CLI capabilities include:

- Register agents/functions via the openYuanrong meta_service component.
- Invoke agents/functions via the openYuanrong frontend component, with SSE streaming output.
- Support for agent session and instance session request headers.
- Support for one-shot invocation and interactive invocation.

For CLI installation, command parameters, examples, exit codes, and testing details, see [cli/README.md](cli/README.md).
For the agent-dx SDK programming model, fixed bootstrap, and deployment configuration, see [python/README.md](python/README.md).
For the platform-owned custom-image Agent executor, see [executor/README.md](executor/README.md).

## Getting Started

- Install: `pip install https://openyuanrong.obs.cn-southwest-2.myhuaweicloud.com/release/0.9.0/openeuler/{x86_64 or aarch64}/agent_dx_cli-0.9.0-py3-none-any.whl`.
- Prerequisites: You need to have openYuanrong deployed with function service capability. See the [openYuanrong Deployment](https://docs.openyuanrong.org/en/latest/deploy/index.html) documentation.

### CLI Tool

Follow the openYuanrong [Function Service](https://docs.openyuanrong.org/en/latest/multi_language_function_programming_interface/development_guide/function_service/index.html) development guide to build your agent, then use the `adx` CLI to register it:

```bash
adx deploy -s ./agent.json --server {meta_service_endpoint}
```

Example `agent.json`:

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

Invoke an agent:

```bash
adx exec --agent <agent_name> --server {frontend_endpoint} --args '{"message":"hello"}'
```

For more installation methods, parameter details, and interactive mode usage, see [cli/README.md](cli/README.md).

### Project Structure

```text
cli/                 Python CLI package source and packaging config
cli/ar_cli/          adx command implementation
python/              Independent agent-dx Python SDK package
executor/            Independent platform Agent Executor package
tests/cli/           CLI unit tests
tests/python/        agent-dx SDK unit and integration tests
tests/executor/      Agent Executor unit tests
pytest.ini           Test configuration
```

The CLI, SDK, and Executor are separate distributions that share the version
in the repository-level `VERSION` file.

## Contributing

We welcome developers to contribute to agent-dx. You can contribute in the following ways:

- Submit bugs, feature requests, or usage issues: [Issues](https://github.com/openJiuwen-ai/agent-dx/issues)
- Submit code, documentation, or examples: [Pull Requests](https://github.com/openJiuwen-ai/agent-dx/pulls)

## License

[Apache License 2.0](./LICENSE)

This product serves solely as a workflow orchestration tool and does not embed any AI model capabilities. When users integrate AI models for specific business scenarios, they shall bear full responsibility for compliance obligations under the EU AI Act and other relevant regulatory frameworks.
