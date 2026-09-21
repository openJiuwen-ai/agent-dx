# 统一进程部署与发布包

实际安装和配置步骤见 [单机进程部署](../deployment/standalone.md)。本文保留 CLI 实现与早期验收记录。

`platform/deployment` 中的 Rust `adxctl` 承载统一部署配置和轻量 supervisor；它不属于某个平面，配置和托管 Master/API Server、Edge/Node Proxy/Node Manager。Node Manager 仍负责本机 Instance 停止清理。统一包同时携带控制面和数据面。本文描述现有进程托管、停止契约和出包；末尾保留早期组件测试。完整 SDK 基础 K8s 已通过 [Buildkite #21](2026-09-17-observability-k8s.md)。

## 模块

| 模块 | 职责 |
|---|---|
| `platform/deployment/src/cli.rs` | 类型化子命令和参数；`start` 是 `run` 的兼容别名 |
| `platform/deployment/src/config.rs` | 部署 schema、控制面/数据面角色与公共参数校验，生成组件配置、Redis AOF 配置和 Node 管理 socket 地址 |
| `platform/deployment/src/supervisor.rs` | 单部署文件锁、受保护的控制 UDS、子进程组、日志、有限次数重启、状态查询和停止顺序 |
| `platform/node-manager/src/admin.rs` | 本机已管理 Instance 的停止清理；保持 Node Manager 的生命周期所有权 |
| `node.proto / NodeAdminService` | 仅在本机受保护 UDS 服务的 Drain RPC，不挂到 Node TCP 服务 |
| `build/release/build.sh` | 原生 release 构建、SDK wheel 构建、统一包组装 |
| `build/release/package.py` | 输入制品完整性检查、Redis 版本检查、清单与 SHA256、已展开目录复核 |
| `build/ci/process_smoke.py` | 从统一包启动真实 Redis／Master，注入进程退出、检查新 epoch、确认干净停止 |

## 运行入口

```sh
adxctl config init
adxctl validate
adxctl render --output /tmp/adx-generated
adxctl run
# 另一个终端
adxctl status
adxctl stop
```

`start` 与 `run` 相同，前台运行 supervisor，适合直接作为服务或 Pod 的入口进程。状态查询反映子进程 PID、重启次数和失败标记，**不等同于平台业务就绪**。同一 `state_dir` 只允许一个 supervisor，命令经当前进程的 UDS 执行，不读取 PID 文件后盲目 kill。

默认配置路径为 `/etc/adx/deployment.yaml`；可通过全局 `-c/--config` 或 `ADX_DEPLOYMENT_CONFIG` 修改。`config init` 默认生成带本地托管 Redis 的 standalone 配置，并支持 `standalone-external-redis`、`master`、`node` 和 `edge-api` profile。统一配置示例位于 `build/config/examples/`。`services` 选择本机角色；同一包可以部署 Master 主机或工作节点。每个服务的原有细节放在 `config` 和 `env` 中；公共 Redis／namespace 和管理 socket 由 CLI 注入。当前校验覆盖部署结构、公共字段、Redis 和 socket 等约束；TLS 文件、资源观测及其他组件细节仍由对应服务执行最终校验。

配置目录权限为 0700、生成文件和管理 socket 为 0600。日志在 `state_dir/logs/<service-id>.log`；状态响应不返回环境变量或配置正文。统一部署的 `logging` 可启用 Supervisor 输出接管、大小/时间滚动、gzip 压缩及历史保留；见[日志配置与故障契约](log-rotation.md)。

## 进程和停止契约

默认启动顺序为 Redis → Master → Node Manager（含内嵌 Proxy）→ API Server（含内嵌 Edge）。显式分进程时，独立 Node Proxy 在 Node Manager 前启动，独立 Edge 在 API Server 后启动。该顺序只安排进程拉起；组件通过已有发现和对账握手达到就绪。异常退出按配置延迟重启，超过本次 supervisor 生命周期的预算后标记失败，其他角色保持运行；不会假装故障组件已就绪。初次 spawn 失败同样进入有限重试。

显式 `stop` 和 supervisor 收到 SIGTERM／SIGINT 都执行：

1. 调用本机各 Node Manager 的 Drain。Node 关闭新准入，等待已接收的串行操作，逐一退役绑定、删除运行时、提交 Deleted。
2. 每个删除结果必须为 `Published` 且不占资源。Master 不可用、清理失败或只进入本地降级日志时，停止不成功，保留依赖供重试。
3. 所有本机节点清理成功后，按相反顺序停止子进程。单个进程终止超时会被强制终止，并使这次停止返回失败。

Node Manager 未完成权威对账时，不能用空内存目录宣称清理完成。清理开始后保持 draining；失败可再次执行。作用域是本机已接收并管理的 Instance，不执行远端节点排空或迁移。完整 E2E 还需覆盖停止与 Master 在途分配之间的竞争。

sandboxd 始终由部署环境独立托管，角色枚举不允许 supervisor 拉起它。RRT 随包交付到 `runtime/`，须进入实际实例环境；不会被当成宿主公共服务启动。Node Proxy 和 Edge 均支持默认共进程及显式分进程，见 [Node Proxy 进程模式](node-proxy-process-modes.md)与 [Edge 进程模式](api-edge-process-modes.md)。

## Redis

不配置 `redis` 角色时，使用公共 `redis_url` 指向外部 Redis。需要托管时，在 `services` 增加：

```json
{
  "id": "redis",
  "role": "redis",
  "config": {
    "bind": "127.0.0.1",
    "port": 6379,
    "data_dir": "/var/lib/adx/redis",
    "appendfsync": "everysec"
  }
}
```

CLI 创建配置指定的数据目录，生成 `appendonly yes` 的 Redis 配置；`appendfsync` 可选 `always`、`everysec`、`no`。部署者应确保公共 `redis_url` 与实际 Redis 入口一致。当前托管 Redis 示例用于本机受保护入口；外部 Redis 的网络和访问配置由部署环境负责。包中固定 Redis 7.2.5，携带许可证和来源信息。

## 出包

```text
package/
├── bin/             adxctl、Master、Node Manager、Sandbox API、Edge、Node Proxy、forwarder、Redis
├── runtime/         rrt-runtime
├── sdk/             adx_sandbox wheel
├── etc/examples/    部署与组件配置示例
├── third_party/     Redis／sandboxd 来源与许可证
├── LICENSE
└── manifest.json    commit、dirty、target、profile、文件 SHA256
```

`build/release/build.sh` 从当前源码构建原生 release 制品，显式要求 Cargo 缓存、目标架构、Redis 二进制和新输出目录。部署阶段只使用已构建包。`package.py assemble` 也可组装显式提供的开发制品，并按实际 profile 标注；不能将 debug 包当成 Linux release 验收。

`package.py verify <directory>` 校验完整文件集合和哈希，拒绝缺失、额外文件、符号链接及内容变更。哈希用于与受信制品清单核对，本身不是签名或源码构建证明。真实 Buildkite 仍需从干净检出构建并交接这一批制品。

## 验证边界

- TDD 首先确认 `drain`／`is_draining` 接口缺失；随后测试删除提交失败、重试只补提交、未对账拒绝停止、新分配拒绝。
- supervisor 测试运行真实子进程和 UDS，检查重复启动排他、重启预算、清理失败时依赖存活、再次停止成功。
- 真实包 smoke 托管真实 Redis＋Master，检查 Redis 地址发布、Master 强制退出后的新 PID／新 epoch，以及 `adxctl stop` 后 supervisor 退出。
- API Server 通过根 Cargo workspace 编译与测试；原有 HTTPS／RPC／Redis 协作套件继续回归。

这不是两节点完整平台验收：没有启动真实 sandboxd／RRT，也没有通过安装后的公开 SDK 完成创建—命令—文件—删除。后续环境驱动器、实例镜像、资源源与 SDK 用例已接通，正式结果见本文开头。不会用本阶段的包构建或 Master smoke 代替该门禁。

## 本轮结果

基准为 `refactor/monorepo-layout` 的 `1e49d86f2123173a8f5358182ca294fab9a9b1e4` 加未提交工作树。Rust／进程验证在 macOS ARM64；Go 在 Linux ARM64 工具链容器测试并生成本机服务。统一包是明确标记的 `aarch64-apple-darwin / debug` 开发包。

| 检查 | 结果 | `out/ci/process-deployment/` 证据 |
|---|---|---|
| CLI／Node／Master／Protocol | 101 通过、0 失败、12 默认忽略 | `rust-final.log` |
| 严格 Clippy | 通过 | `clippy-final.log` |
| Go 全包测试、vet、服务构建 | 206 通过 | `go-v2.log` |
| 包完整性测试 | 2 通过 | `package-tests-final.log` |
| Go HTTP／真实 Redis／mTLS RPC 协作 | 4 通过、0 忽略 | `frontend-final/result.json` |
| 当前 SDK wheel 构建 | 通过 | `sdk-build.log`、`sdk/` |
| 统一开发包组装与校验 | 通过 | `package-final/manifest.json` |
| 真实包 Redis／Master 重启与停止 | Master 重启 1 次，epoch 1→2，干净停止 | `smoke-final/result.json` |

默认忽略项不计通过，性能基准未重跑；原有独立 RPC 用例由带 Go HTTP 的入口实际运行。源码和制品摘要见 `source-manifest.json`。原生 release 构建脚本已提供并通过 shell 语法检查，本阶段未执行 Linux release 构建或 Buildkite。

后续的 Linux 双节点真实 SDK 验收结果见 [local-e2e.md](local-e2e.md)。该结果单独记录，不改变上表的历史验证边界。
