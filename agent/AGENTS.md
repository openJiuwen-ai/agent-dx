# Agent product development

This subtree contains the existing CLI, SDK, and executor. Preserve public package names, CLI arguments, stdout/stderr, exit codes, SSE and session-header behavior during migration. The current implementation still has the original FaaS backend; replacing it with Sandbox SDK calls is a separate implementation step.

Paths are relative to this subtree: `cli/`, `sdk/python/`, `executor/`, and `tests/`. Run tests from the repository root with `python -m pytest -q`.

## Runtime Contracts

- `adx deploy` 的 `--server` 指向 meta_service，地址格式为 `host:port`，未显式传 scheme 时默认使用 `http://`。
- `adx deploy -s/--spec` 支持 inline JSON 字符串或 JSON 文件路径，spec 必须解析为 JSON object。
- `adx deploy` 在 spec 未包含 `enableSessionCtx` 时默认注入 `true`；若用户显式设置 `true` 或 `false`，必须保留原值。
- `adx exec` 的 `--server` 指向 frontend，地址格式同样为 `host:port`。
- `adx exec` 只有在传入 `--session-ctx` 时才发送 `X-Session-Context`。
- `adx exec` 只有在传入 `--session-id` 时才发送 `X-Instance-Session`，并附带实例 session TTL/concurrency 默认值。
- `adx exec --args` 必须是合法 JSON 字符串；不传时进入交互模式，每轮输入包装为 `{"message":"..."}`。
- `adx exec` 交互模式下若用户未传 `--session-ctx`，会自动生成一个并在每次 POST 中复用。
- 普通日志走 stderr，流式数据走 stdout；不要把 debug 日志混入 stdout。
- 退出码约定：`0` 成功，`1` 服务端失败，`2` 参数错误，`3` 网络错误。
