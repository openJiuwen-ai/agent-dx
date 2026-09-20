# ADX 端到端用例分层与 Buildkite 门禁

本文基于提交 `2e30b80e4426e5edba6af28531e267714dfb1a81` 重新整理测试分类。
UT 与 E2E 分开管理：UT 按代码模块运行；E2E 按业务闭环和部署拓扑逐级扩展。

## 1. UT 不纳入 E2E 层级

UT 包含 Rust workspace、Agent、Sandbox SDK、构建驱动器，以及使用测试进程或隔离 Redis
验证组件契约的用例。它们用于快速定位规则和接口错误，但不代表完整平台已经部署。

| UT 类别 | 当前规模／入口 | 作用 |
|---|---|---|
| Rust | 404 个静态测试定义；`cargo test --workspace --all-features` | API Server、Master、Node Manager、RRT、Gateway、CLI 和公共 crates |
| Python | 528 个静态测试定义 | Agent、Sandbox SDK、构建和测试驱动器 |
| 状态与 RPC 契约 | `storage`、`control-rpc`、`api-control`、`interop` suite | Redis/AOF、mTLS、进程 RPC 和 RRT Socket 协作 |

这些测试都应进入 Buildkite 的代码门禁，但报告为 `UT / component tests`，不使用 L0、
Standalone、多 VM 或全量部署 E2E 的通过结论。

## 2. E2E 采用递进模型

```text
L0 最小业务闭环
    ↓ 在单机真实进程和执行后端上扩展
Local Standalone
    ↓ 在独立机器和真实跨节点网络上扩展
Local Multi-VM
    ↓ 使用正式制品、镜像、编排和目标环境扩展
Full Deployment Acceptance
```

L0 是用例集合，不是一种 mock 环境。更高层必须重跑 L0，并增加该拓扑特有的用例；不能用
Standalone 通过替代多 VM，也不能用多 VM 通过替代正式 K8s／目标环境验收。

## 3. L0：最小真实端到端用例

L0 只证明最核心的用户闭环。它必须使用发布包、安装后的 Sandbox SDK、真实 Redis、真实
控制面／Gateway、真实 sandboxd 和真实 RRT；允许所有组件位于一个测试宿主。

| L0 用例 | 必须断言 |
|---|---|
| 部署就绪 | Master 可发现、Node 注册并完成对账、容量有效、路由和本机绑定同步完成 |
| API Key | 有效 Key 可访问；无效 Key 和跨租户访问被拒绝 |
| 创建 Instance | 经公共 SDK 创建，sandboxd 后端真实运行，RRT 就绪，状态和路由已提交 |
| 查询 Instance | `get/list` 返回正确租户、状态、资源和执行归属 |
| 执行命令 | 真实经过 Edge → Node Proxy → RRT，校验 stdout、stderr 和退出码 |
| 文件操作 | 二进制写入和读回一致，不能只验证 HTTP 状态码 |
| 删除 Instance | Redis 终态正确、路由撤销、资源释放、sandboxd inventory 为空 |
| 环境清理 | 测试进程／容器、临时网络和测试凭证均无残留；清理失败使整轮失败 |

当前八组驱动中的 `sdk` 提供创建、查询、命令、文件和删除主体，`auth` 提供凭证与租户隔离。
L0 不包含双节点放置、节点失联、跨节点恢复、暂停／快照、性能或长稳。

## 4. Local Standalone：单机真实部署验收

Standalone 在一台 Linux 主机或一台 Lima KVM VM 上，以进程方式运行完整 ADX。sandboxd
由部署环境独立托管。它重跑全部 L0，并验证单机部署、运行时和本机恢复能力。

| Standalone 用例 | 当前归属 |
|---|---|
| `adxctl validate/render/start/status/stop` | 验证统一配置、supervisor、进程重启预算和停机清理 |
| 发布包安装示例 | 从干净发布包和 wheel 启动，配置及制品哈希固定 |
| 运行环境 | 本地 EROFS、OCI 默认 runtime、runtime-only、自定义镜像只读挂载 RRT |
| Node Proxy 组合 | embedded 和 standalone 两种模式遵守同一绑定／路由契约 |
| 单机容量 | 资源满载不超分、等待请求在释放后继续、账本和 Metrics 一致 |
| Node Manager 重启 | 新 session 完成权威对账；已运行后端身份和 SDK 操作保持正确 |
| 日志与可观测 | Metrics、结构化日志、滚动压缩、Collector 中断恢复、Trace 链路 |
| supervisor stop | 删除本机 Instance 后退出；独立 sandboxd 仍可响应且 inventory 为空 |
| Firecracker 生命周期 | 在 KVM 主机验证暂停／恢复、可复用快照、双克隆、对象存储和残留回收 |

仓库的本地 Docker 双容器驱动仍属于 Standalone 级：它能在单宿主上模拟两个逻辑节点，
适合复现 `capacity`、`placement`、`local-first`、`node-failure` 等代码路径，但不能证明 VM
隔离、跨宿主网络或机器故障。

## 5. Local Multi-VM：本地三 VM 分布式验收

推荐拓扑是一台控制 VM 加两台 worker VM。所有 VM 使用同一份已验证发布包；控制面、Redis、
SDK 客户端和两个执行节点通过真实 VM 网络通信。它重跑 L0，并增加分布式行为用例。

| Multi-VM 用例 | 必须断言 |
|---|---|
| 三 VM 部署与发现 | 唯一机器／节点身份、自动 Shard 归属、跨 VM Redis/RPC/mTLS、双方容量和路由就绪 |
| 双 worker 放置 | 两个 worker 都实际承载 Instance；保存平台归属和 sandboxd 后端证据 |
| 容量与调度 | 两节点满载、排队、释放唤醒；Pack/Spread 配置；亲和／反亲和和节点偏好 |
| Local-first | API Server 入口轮转、本机准入、同 ID 并发收敛、冲突规格拒绝、中心 fallback 不重复计账 |
| 跨节点数据链路 | Edge → 对应 Node Proxy → RRT 的命令、文件和路由结果正确 |
| worker 失联 | 心跳过期后实例失效、路由撤销、健康 worker 继续服务；返回节点先清理旧后端再开放准入 |
| worker 进程重启 | 新 node session、实例对账、旧 session fencing，未失效 Instance 仍可查询和执行 |
| 控制面重启 | Redis 权威状态恢复、API Server／Edge 重新全量同步、旧 epoch 不能继续写入 |
| 跨节点 checkpoint | 仅在 VM 均有 KVM 时验证共享 checkpoint、同 Instance ID 新 generation、旧节点清理 |
| 有序停机 | 先 worker、后控制面；删除结果提交成功，三台 VM 无后端和路由残留 |

三台 VM 即使位于同一台 Mac 上，也只能证明 guest 网络和进程隔离；不能作为物理宿主故障证据。
现有 `gateway/tests/local_3vm_*` 主要覆盖数据面和性能，完整控制面三 VM 自动化仍需补齐。

## 6. Full Deployment Acceptance：全量实际部署验证

全量验收使用正式 Buildkite 构建的 release、SDK wheel、不可变 Node/RRT 镜像和固定外部依赖，
部署到目标 Kubernetes／准生产环境。运行节点不编译代码，也不借用开发机文件。

| 全量验收用例组 | 内容 |
|---|---|
| 制品交接 | clean commit、release SHA256、SDK 版本、sandboxd revision、镜像 digest 和目标架构一致 |
| 实际部署 | 独立 namespace、Secret、Service、Pod、镜像拉取、权限和目标 kubeconfig；等待业务就绪而非仅 Pod Ready |
| L0 | 完整重跑 API Key、创建、查询、命令、文件、删除和物理清理 |
| `capacity` | 两节点资源满载／排队／释放；Master 与 Node Metrics 账本一致 |
| `placement` | 双节点亲和 OR、反亲和、节点约束、有序／加权偏好和实际归属 |
| `local-first` | 入口轮转、原子 claim、相同 ID 收敛、冲突拒绝和中心 fallback |
| `node-failure` | 心跳失效、实例／路由撤销、健康节点可用、恢复节点清理后重新准入 |
| `restart` | Node Manager 重启和权威对账；必要时增加 Master/API/Edge 重启 |
| `stop` | supervisor 清理、sandboxd 独立托管、日志滚动压缩和最终后端清空 |
| 可观测验收 | 实例数量和资源指标、Gateway 指标、结构化日志、Collector 恢复、完整 Trace 父子链 |
| 环境清理 | JUnit 无失败／跳过，`missing_checks=[]`、`cleanup_errors=[]`，namespace 删除完成 |

当前 Buildkite 基础 K8s 已执行八组：`sdk`、`auth`、`capacity`、`placement`、`local-first`、
`node-failure`、`restart`、`stop`。它验证正式制品和跨 Pod 链路，但历史运行的两个 Pod 位于
同一物理 worker，因此跨物理宿主网络和宿主故障仍属于全量验收缺口。

Firecracker checkpoint 使用独立 KVM profile；GPU/NPU 使用具备真实设备的 worker profile。
缺少对应环境时应报告未执行，不能通过 mock 或 skip 把全量结果置为绿色。

## 7. 用例归属矩阵

`✓` 表示该层必须执行，`扩展` 表示在该层增加更强断言。

| 用例 | L0 | Standalone | Multi-VM | Full Deployment |
|---|:---:|:---:|:---:|:---:|
| API Key、创建、查询、命令、文件、删除 | ✓ | ✓ | ✓ | ✓ |
| 发布包／CLI／进程托管 |  | ✓ | ✓ | ✓ |
| EROFS／OCI／自定义镜像 |  | ✓ | 按部署配置 | ✓ |
| 容量排队与释放 |  | ✓ | 扩展为双节点 | ✓ |
| Node Manager 重启对账 |  | ✓ | 扩展为独立 worker | ✓ |
| 双节点放置／亲和／偏好 |  | 逻辑节点复现 | ✓ | ✓ |
| Local-first／原子归属 |  | 逻辑节点复现 | ✓ | ✓ |
| 节点失联／返回清理 |  | 进程级复现 | ✓ | ✓ |
| Master/API/Edge 重启同步 |  | 可验证 | ✓ | ✓ |
| Metrics／日志／Trace |  | ✓ | 跨节点扩展 | ✓ |
| FC 暂停／快照／克隆 |  | KVM Standalone | 可选 KVM | 独立 KVM profile |
| 跨节点 checkpoint |  |  | KVM Multi-VM | 独立 KVM profile |
| 物理宿主故障／网络隔离 |  |  | 仅不同物理宿主时 | ✓ |
| 真实 GPU/NPU |  |  | 有硬件时 | 独立设备 profile |

## 8. 当前自动化状态

| 层级 | 当前状态 | 主要缺口 |
|---|---|---|
| L0 | 已具备；由现有 `sdk` 和 `auth` 场景覆盖主体 | 需要从八组驱动中提供独立的 L0 选择入口和单独结果 |
| Standalone | 已有本地 Docker 八组、安装示例、运行环境和 Lima FC 18 项，但分散在多个入口 | 需要一个统一 Standalone 结果汇总，明确普通 Linux 与 KVM profile |
| Multi-VM | 部分具备；`gateway/tests/local_3vm_*` 覆盖数据面，FC transfer 驱动覆盖单宿主网络命名空间归属转移 | 缺完整控制面三 VM 部署器，以及发现、调度、故障、恢复和清理的统一验收 |
| Full Deployment | 基础 K8s 八组及可观测验收已具备 | 历史运行未跨物理 worker；Master/API/Edge 重启、K8s FC、真实 GPU/NPU 仍未纳入基础结果 |

因此当前可以直接形成 Buildkite 门禁的是 UT、L0 和基础 K8s 八组。Standalone 可作为独立
Linux/KVM profile；完整 Multi-VM 在补齐控制面自动化前不能标记为全量通过。

## 9. Buildkite 门禁建议

Buildkite 保留 UT 和 E2E 两条清晰的结果线：

1. **UT gate**：Rust、Agent、Sandbox SDK、驱动器、真实 Redis/mTLS/RRT 组件契约；任何失败阻断。
2. **L0 E2E gate**：每个提交使用真实发布包部署完整最小平台，执行 L0 全部用例；不能用 mock 后端。
3. **Full deployment gate**：主分支、合入候选和发布候选执行当前八组 K8s 用例、可观测断言及清理。
4. **Conditional profiles**：checkpoint/snapshot 变更触发 KVM；设备调度变更触发 GPU/NPU worker。
5. **Nightly / release**：本地多 VM、跨物理宿主、性能、压力、长稳和重复故障注入。

如果流水线资源允许，当前八组 K8s 场景本身约两分钟，可继续对每个提交执行全量；主要耗时来自
编译、镜像和部署。即使拆出较快的 L0，也必须保留主分支／发布前的全量实际部署门禁。
