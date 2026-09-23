# Environment 与组件命名调整验证

日期：2026-09-23。基于 `community/refactor` 提交 `c66a0240e79be5e3c417901ac05bbd70ba4ad2ea`，在独立 `refactor/environment-naming` worktree 验证未提交修改。命名与配置契约见 [组件命名与抽象](../architecture/naming.md)。

## 修改范围

进程统一为 `adx-coordinator`、`adxlet`、`adx-apiserver`、`adx-ingress`、`adx-relay`、`adx-execd`。平台 Capsule 更名 Environment，部署 rootfs/bootstrap 模型更名 RuntimeProfile。目录、内部协议、Redis/SQLite 字段、部署角色、发布清单、环境变量、指标、SDK 内部 Execd 连接配置和测试驱动同步调整。

公开 Sandbox 类、包名和生命周期 HTTP 路径保持现有定义。Agent Environment 产品状态与平台 Environment 生命周期分别管理。API Server + Ingress、adxlet + Relay 的默认合进程和可选拆分能力保留。

本次使用新 namespace 和临时数据库进行验证；不提供旧持久化格式读取或迁移流程。

## 已执行验证

环境为 macOS arm64、Rust 1.95.0、Python 3.12；Rust 使用仓库共享缓存，编译并发度 2。真实 Redis 使用本机 arm64 `redis-server`，每个用例自行启动临时 Unix Socket 实例，不连接部署中的 Redis。

| 检查 | 结果 | 证据（相对 worktree） |
|---|---|---|
| Rust workspace，all-features，测试串行 | 通过；需要外部条件的 ignored 用例按下述专项执行 | `out/ci/environment-naming/workspace-tests-clean-contract.log` |
| Redis storage，包括 AOF、原子归属与恢复 | 27 项通过 | `out/ci/environment-naming/real-redis/` |
| Coordinator/adxlet/Gateway RPC、真实 API Server 进程、mTLS | 21 项通过 | `out/ci/environment-naming/api-control-final/` |
| SDK ↔ Execd 真实 Socket、TLS command watch | 通过 | `out/ci/environment-naming/interop/` |
| Sandbox SDK | 246 项、64 subtests 通过 | `out/ci/environment-naming/sdk-final.log` |
| 部署、CI、缓存、发布、E2E 驱动与管理客户端测试 | 155 项、3 subtests 通过，1 项跳过 | `out/ci/environment-naming/harness-final.log` |
| SDK wheel + sdist 构建 | 通过 | `out/ci/environment-naming/sdk-package/` |
| Shell / Python 源码语法 | 36 个 Shell、160 个 Python 文件通过 | `out/ci/environment-naming/script-syntax.log` |
| fmt、workspace Clippy `-D warnings`、文档链接与生成页、diff 空白检查 | 通过 | `out/ci/environment-naming/` 中对应 `*-final.log`、`diff-check.log` |

## Linux standalone 端到端回归

本次在独立 Lima VM `adx-fc` 验证：Ubuntu 24.04、Linux `6.8.0-124-generic`、aarch64。使用本次未提交源码重新构建 GNU release 组件、静态 musl Execd、内置 EROFS 镜像及独立 SDK wheel，通过发布包部署两个 Docker 逻辑节点。新进程命名、Environment 协议/存储和默认合进程模式均进入实际创建、执行与清理链路。

sandboxd 使用缓存工具镜像中的未修改产物：revision `efc201531d7e2e9d69505da151eb66084b61eebf`，Go buildinfo 为 `vcs.modified=false`，SHA256 `f1df80fba119e31a5499daecdc40f42b4d0011118be4da72c13c0f1748676cfb`。执行后端为真实 runc；本轮没有修改或重编 sandboxd。

| 用例组 | 结果与主要覆盖 |
|---|---|
| sdk | 通过；内置/runtime-only/自定义镜像、跨两个节点创建、命令、文件及删除清理 |
| data-plane | 通过；资源查询、命令幂等/冲突/超时、Shell、PTY、文件、端口访问、reverse tunnel |
| lifecycle | 通过；detach/reattach、close、上下文删除、空闲回收 |
| auth | 通过；无效密钥、租户隔离、管理员密钥管理和吊销 |
| capacity | 通过；资源耗尽排队与释放后恢复调度 |
| placement | 通过；亲和/反亲和、节点偏好与指定节点 |
| local-first | 通过；入口轮转、同名并发收敛、规格冲突、本地 claim 与清理 |
| node-failure | 通过；心跳失联判失效、迟到节点清理旧 backend |
| restart | 通过；adxlet 重启对账，backend ID 保持且原实例仍可执行 |
| stop | 通过；产品停机物理清理，独立 sandboxd 仍可访问 |

最终 run ID：`adx-e2e-73af3a596745`。十组全部通过，34 项细分功能断言；JUnit 共 **41 项**（34 项细分用例、6 项组级用例、1 项清理），失败与跳过均为零。业务组总耗时约 126 秒。测试容器、网络和专属 cgroup 已清理，`cleanup_errors` 为空。

证据根目录：`out/ci/environment-naming/linux-e2e/`。

- `standalone-r3/result.json`、`standalone-r3/junit.xml`：最终结论与每项结果。
- `standalone-r3.log`、`standalone-r3/`：部署、SDK、组件和 sandboxd 日志。
- `identity/package.json`、`identity/backend.json`、`identity/bundle-r3.json`：release 文件哈希、backend 和测试镜像身份。
- `source-files.json`、`harness-overlay.json`：打包源码与后续测试驱动修正的哈希。
- `harness-regression-final.log`：E2E 驱动相关测试 115 项、3 subtests 通过。
- `standalone-r1/`、`standalone-r2/`：原始失败保留，未计入通过结果。

## 发现与修复

重命名后首次完整回归暴露固定长度假设：隧道 ID 前缀必须保持 4 字节；RPC 流测试读取长度必须跟随 payload。前者已改为 4 字节 `ADEX` 标识，后者按 payload 实际长度分配读取缓冲区；完整隧道测试和真实 RPC 重跑通过。

两线程 workspace 测试另出现一次 Execd HTTP 测试的临时端口占用（`Address already in use`）。串行完整 workspace 通过。本次没有改变生产监听和重试机制，也没有把该次失败计为通过；原日志保留在 `workspace-tests-final.log`。

Linux 回归另修复两个测试问题：Docker 私有 cgroup namespace 中 sandboxd 启用控制器返回 `EBUSY`，增加显式 `--cgroupns host` 选项、按进程 membership 读取容器限额，以及运行级 cgroup 清理；数据面用例把节点正常状态误写为 `1`，按公开 OpenAPI 契约修正为 `0`。先保留失败证据，再修复并重跑完整十组。资源控制没有关闭。

## 验证边界

本次新增的是单台 Linux VM 内两个 Docker 逻辑节点的真实 sandboxd/runc E2E，不代表跨 VM、Kubernetes 或 Firecracker 验收。pause/resume、快照、S3 和 GPU/NPU 等依赖特定后端的场景未在本轮重跑。已生成本地 Linux release 包，但未触发 Buildkite、发布产物、提交或推送分支。历史部署报告保留其原始版本与名称，不能替代本次改名后的验证证据。
