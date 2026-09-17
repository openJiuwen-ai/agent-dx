# 本地运行环境与 RRT PID 1 验证

工作树 `fix/atomic-instance-claim`，基线 `d467c2cfc90c182111b209241843270b809c4b8c` 加本轮未提交修改。使用原生 Linux ARM64 release 制品；manifest 明确记录 `dirty: true`。正式 K8s 与 Firecracker 本轮未运行。

## 行为

统一配置 `runtime_environment` 下发到 API Server 与 Node Manager，并随 InstanceSpec 持久化。无自定义镜像时使用本地 EROFS；有自定义镜像时只读挂载相同环境到 `/__adx`。启动命令直接运行静态 RRT。详见 [部署配置](../deployment/runtime-environment.md)。

RRT 作为 PID 1 时，在创建 Tokio 与任何导出线程前 fork 服务子进程。父进程负责等待孤儿和转发信号，服务子进程保留命令与 PTY 的等待所有权；非 PID 1 保持原启动路径。

## 证据

证据目录为工作树下 `out/ci/runtime-environment/`。

| 检查 | 结果 | 日志 |
|---|---|---|
| 契约 RED | 新模型不存在时按预期编译失败 | `red.log` |
| PID 1 RED | 旧 RRT 遗留 30 个僵尸进程 | `pid1-red.log` |
| Rust 组件回归 | 284 passed，45 ignored | `green-2.log` |
| 严格 Clippy | 全目标通过 | `clippy.log` |
| 记录往返 | JSON 持久化和 protobuf 保留运行环境；2 passed | `protocol-final.log` |
| SDK | 237 passed、64 subtests passed | `sdk-tests.log` |
| 验收驱动与打包 | 59 + 2 项通过 | `driver.log`、`package-tests.log` |
| 真实 PID namespace | 30 个孤儿、0 僵尸；命令退出码 7；SIGTERM 返回 143 | `pid1-green-2.log` |
| 真实双节点 E2E | sdk/auth/capacity/placement/local-first/node-failure/restart/stop 八组通过；无遗漏、无清理错误 | `run-1/result.json`、`build-e2e-3.log` |

SDK 组额外验证默认环境、仅覆盖 runtime、自定义镜像三条路径。自定义镜像内没有 `/usr/local/bin/rrt-runtime`，由挂载提供 `/__adx/usr/local/bin/rrt-runtime`，执行命令并删除成功。后端为仓库固定 sandboxd PR #56、runc；运行 ID 为 `adx-e2e-6315b6b24b0c`。

首次独立 PID 1 GREEN 验证因测试脚本在 HTTP 就绪后过早结束 PID 发现而失败；修正测试竞态后使用同一二进制通过，原日志 `pid1-green.log` 保留。首次构建受 apt 下载失败影响，第二次构建因容器未挂载 worktree 的 Git 公共目录而打包失败，分别保留 `build-e2e-1.log` 和 `build-e2e-2.log`。

## 制品

`package/manifest.json` 记录全部文件摘要，其中：

- 静态 RRT：`744b224323fdf8e66ca6b87e50d3755f77aca26d2bd52f444ded2c4abad7b130`
- 本地 EROFS：`9ba41987493f7e738192378c0b4b03f91ca5a6957ebb24fad9423cf47f2587c8`

## 复跑 PID 1 验证

在具备 PID/mount namespace 权限的独立 Linux 测试环境运行：

```sh
python3 build/ci/rrt_pid1.py /absolute/package/runtime/rrt-runtime
```

该检查启动隔离 PID namespace，不操作节点上其他 RRT。其结果与完整 SDK 端到端验收分别记录。
