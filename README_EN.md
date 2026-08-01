[中文](README.md) | **English**

## Introduction

openYuanrong Agent Runtime is the agent runtime access repository for openYuanrong, providing developer tools for agent registration, invocation, and session management. The repository currently offers the Python CLI `ar` and a separately built Python AgentRuntimeSDK.

### Key Capabilities

Current CLI capabilities include:

- Register agents/functions via the openYuanrong meta_service component.
- Invoke agents/functions via the openYuanrong frontend component, with SSE streaming output.
- Support for agent session and instance session request headers.
- Support for one-shot invocation and interactive invocation.

For CLI installation, command parameters, examples, exit codes, and testing details, see [cli/README.md](cli/README.md).
For the AgentRuntimeSDK programming model, fixed bootstrap, and deployment configuration, see [python/README.md](python/README.md).

## Getting Started

- Install: `pip install https://openyuanrong.obs.cn-southwest-2.myhuaweicloud.com/release/0.9.0/openeuler/{x86_64 or aarch64}/openyuanrong_agentruntime-0.9.0-py3-none-any.whl`.
- Prerequisites: You need to have openYuanrong deployed with function service capability. See the [openYuanrong Deployment](https://docs.openyuanrong.org/en/latest/deploy/index.html) documentation.

### CLI Tool

Follow the openYuanrong [Function Service](https://docs.openyuanrong.org/en/latest/multi_language_function_programming_interface/development_guide/function_service/index.html) development guide to build your agent, then use the `ar` CLI to register it:

```bash
ar deploy -s ./agent.json --server {meta_service_endpoint}
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
ar exec --agent <agent_name> --server {frontend_endpoint} --args '{"message":"hello"}'
```

For more installation methods, parameter details, and interactive mode usage, see [cli/README.md](cli/README.md).

### Project Structure

```text
cli/                 Python CLI package source and packaging config
cli/ar_cli/          ar command implementation
python/              Independent Python AgentRuntimeSDK package
tests/cli/           CLI unit tests
tests/python/        AgentRuntimeSDK unit and integration tests
pytest.ini           Test configuration
```

The CLI and SDK are separate distributions that share the version in the
repository-level `VERSION` file.

## Contributing

We welcome contributions of all kinds. Please refer to our [Contributor Guide](https://docs.openyuanrong.org/en/latest/contributor_guide/index.html).

## License

[Apache License 2.0](./LICENSE)
