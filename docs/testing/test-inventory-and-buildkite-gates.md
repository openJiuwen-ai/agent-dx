# ADX 测试分层与 Buildkite 门禁

本文盘点提交 `88a01de9380419f3f132cf6db025c344808ec2a7` 的测试资产，并给出
Buildkite 分层门禁。静态计数按源码中的 Rust `#[test]` / `#[tokio::test]` 和 Python
`test_*` 定义统计；参数化、子用例和运行时生成会使实际执行数不同，因此计数用于描述规模，
最终结果仍以测试运行器和 JUnit 为准。

## 1. 分层原则

| 级别 | 目标 | 运行环境 | 是否阻断合入 |
|---|---|---|---|
| L0 源码与构建契约 | 尽早发现格式、静态检查、脚本语法和制品交接错误 | 普通 Linux builder | 是 |
| L1 单元／纯契约 | 验证单模块规则，不启动真实 Redis、sandboxd 或集群 | Linux builder，可并行 | 是 |
| L2 组件协作 | 验证真实 Redis、mTLS、进程、Socket/RRT 协作 | Linux builder + 本机依赖 | 是 |
| L3 本地平台 E2E | 用完整发布包和真实 sandboxd 快速复现公共 SDK 链路 | Linux Docker 双节点 | 开发复现，不和 K8s 重复阻断 |
| L4 Kubernetes 冒烟 | 验证本次不可变制品、部署、公共 SDK、故障和清理 | 独立 K8s namespace | 是，正式端到端门槛 |
| L5 专项／长稳 | 验证 KVM、跨宿主、加速卡、性能、压力和长稳 | 专用 worker／3 VM | 条件触发、夜间或发布门槛 |

`L0–L2` 解决“哪一条规则坏了”；`L4` 解决“本次制品部署后是否真的能工作”。本地
Docker 和 Kubernetes 复用同一套八组业务场景，Buildkite 不需要再串行运行一遍 L3。

## 2. 当前测试资产

### 2.1 Rust：404 个静态测试定义

| 功能区域 | 数量 | 主要覆盖 |
|---|---:|---|
| API Server | 14 | HTTP 契约、鉴权上下文、实例目录全量／增量、revision 断档、epoch 重置、节点入口轮转 |
| Master / ShardScheduler | 97 | 调度、Filter/Score、资源和设备、亲和／反亲和、公平队列、优化安全、恢复、快照、原子 claim、真实 RPC |
| Node Manager | 93 | 生命周期串行化、准入账本、sandboxd 适配、SQLite 降级日志、对账、路由、就绪、暂停恢复、对象 checkpoint |
| `adxctl` / supervisor | 22 | 配置校验、进程启动重启、日志滚动压缩、停机清理 |
| 公共 crates | 30 | Instance／恢复身份、协议转换、调度框架、发现和可观测上下文 |
| RRT | 62 | HTTP 控制接口、命令／文件／PTY、启动、checkpoint 协作和 Python 值转换 |
| Gateway | 86 | Edge、Node Proxy、路由订阅、转发、连接、指标和进程组合 |

其中 `master/tests/storage.rs`、`master/tests/storage/claims.rs` 和部分
`master/tests/rpc.rs` 依赖隔离 Redis、mTLS 或真实进程，由 L2 suite 显式运行；普通
`cargo test --workspace` 中被标记为 ignored 的用例不能当作已经覆盖。

### 2.2 Python：528 个静态测试定义

| 功能区域 | 数量 | 主要覆盖 |
|---|---:|---|
| Agent | 232 | CLI、session、executor、文件、进程、SSE、存储和 Agent 接口 |
| Sandbox SDK | 223 | 公共类型、生命周期、超时／重试、命令监听、文件、PTY、隧道、快照和重启策略 |
| 构建与测试基础设施 | 73 | CI runner、E2E 驱动、K8s/FC 清理、制品校验、日志输出、Trace 校验、发布包 |

Agent 测试属于 Agent 层回归，不替代 Sandbox 平台 E2E。SDK 目录内的独立 `sdk_e2e.py`、
`e2e_rrt_direct.py` 等脚本也不能因普通 pytest 通过而视为已执行，只有对应 suite 或 E2E
显式调用才形成证据。

### 2.3 真实依赖与完整平台用例

| suite／入口 | 级别 | 当前覆盖 |
|---|---|---|
| `build/ci/run.py storage` | L2 | Redis CAS、AOF 重启、代次 fencing、凭证、暂停恢复状态、atomic claim 和快照引用 |
| `build/ci/run.py control-rpc` | L2 | Master／Node RPC、mTLS、心跳失效、状态提交、路由发布、快照和恢复协作 |
| `build/ci/run.py api-control` | L2 | 真实 Rust HTTPS API Server → Master／Node RPC → Redis；实例目录与本地优先创建 |
| `build/ci/run.py interop` | L2 | SDK → 真实 RRT Socket、命令监听和 TLS 协作 |
| `build/e2e/run.py` | L3 | 本地 Docker 双节点完整发布包和真实 sandboxd；开发复现 |
| `build/e2e/kubernetes/run.py` | L4 | K8s 双 Pod、不可变镜像、公共 SDK 八组场景、诊断和 namespace 清理 |
| `build/e2e/firecracker/*` | L5 | 暂停／恢复、可复用快照、跨节点转移和故障；需要 KVM 与固定 FC kit |
| `master/tests/benchmark.rs`、Gateway perf 脚本 | L5 | 调度、连接和规模性能；不是功能正确性门槛 |

## 3. Buildkite 应保留的八组基础冒烟

八组场景必须全部进入正式 K8s 门槛。Buildkite #30 的场景执行合计约 98 秒，部署和制品
准备才是主要耗时；删除场景节省很少，却会失去关键链路证据。

| 场景 | 功能级别 | 必须断言 | 门槛结论 |
|---|---|---|---|
| `sdk` | 核心 P0 | OCI 默认环境、runtime-only、自定义镜像只读 RRT bootstrap；创建、查询、真实命令／文件、删除和物理回收 | 必选 |
| `auth` | 安全 P0 | 无效 Key、跨租户隔离、管理员创建／吊销租户 Key、缓存到期生效 | 必选 |
| `capacity` | 资源 P0 | 满载不超分、请求排队、释放后继续；Master/Node 实例数和 CPU／内存／磁盘账本一致 | 必选 |
| `placement` | 调度 P1 | 双节点亲和 OR、反亲和、node ID 约束、有序／加权偏好、实际归属和清理 | 必选 |
| `local-first` | 创建 P0 | API Server 入口轮转、本地准入、同 ID 并发收敛、冲突规格拒绝、中心 fallback 不重复计账 | 必选 |
| `node-failure` | 故障 P0 | 心跳失效、实例和路由撤销、健康节点不受影响、节点返回后先对账清理再准入 | 必选 |
| `restart` | 恢复 P0 | Node Manager 新 session、原 backend ID 对账、实例仍可查询和执行 | 必选 |
| `stop` | 运维 P0 | supervisor 删除本机实例、sandboxd 独立存活、日志滚动压缩、Collector 恢复和最终清理 | 必选 |

Metrics 在 `capacity` 中检查，结构化日志与滚动压缩在 `stop` 中检查，Trace 在整轮调用结束后
核对完整创建链路和父子关系。这些不是另起一套长场景，适合继续留在基础门槛。

K8s 通过还必须满足：八组无缺失、JUnit 无失败／跳过、`cleanup_errors=[]`、后端实例目录为空、
namespace 删除完成、release commit 和镜像 digest 与本次提交一致。Pod Ready 或 SDK 单个请求
成功不能单独形成通过结论。

## 4. 推荐的 Buildkite DAG

```text
                 ┌─ quality-and-driver (L0)
本次 clean commit ├─ rust-unit-contracts (L1) ─┐
                 ├─ python-contracts (L1)      ├─ stateful-and-interop (L2)
                 └─ package-contracts (L1) ────┘
                                                   ↓
             platform-build → platform-images → kubernetes-smoke-8 (L4)
                                                   ├─ conditional firecracker (L5)
                                                   └─ nightly / release profiles (L5)
```

### 4.1 每次提交的阻断步骤

1. `quality-and-driver`
   - `cargo fmt --all -- --check`
   - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
   - `python -m unittest discover -s build/e2e/tests -v`
   - `python -m unittest discover -s build/ci/tests -v`
   - 发布包和 E2E Python 入口语法／交接契约。
2. `rust-unit-contracts`
   - `cargo test --locked --workspace --all-features`；普通 ignored 用例不在此冒充通过。
3. `python-contracts`
   - Agent 232 项与 Sandbox SDK 223 项离线回归，分别输出 JUnit。
4. `package-contracts`
   - `python -m unittest discover -s build/release/tests -v`
   - `python -m unittest discover -s build/dev/tests -v`
   - 四个 Python 包的 wheel/sdist 构建和发布清单校验。
5. `stateful-and-interop`
   - 独立 Redis 依次执行 `storage`、`control-rpc`、`api-control`；执行 `interop`。
   - 每个 suite 使用独立 namespace、证书和证据目录，不能共用上轮 Redis 状态。
6. 现有 `platform-build`、`platform-images`、`platform-e2e`
   - 保留 clean commit、SHA256、digest 交接；L4 执行全部八组场景。

这些步骤应并行编译可共享缓存，但测试状态不共享。任何步骤失败均阻断合入；取消、结果文件
缺失或用例未执行也按失败处理。

### 4.2 条件门槛

| Profile | 触发条件 | 原因 |
|---|---|---|
| Firecracker checkpoint | 修改 checkpoint、snapshot、恢复、sandboxd/FC 接口，或显式 `ADX_E2E_CHECKPOINT=1` | 需要 KVM；不能由 runc 基础门槛替代 |
| 独立进程安装示例 | release 候选、部署配置／CLI／打包变更 | 验证统一包在非 K8s 环境可安装、启动和清理 |
| 3 VM 跨宿主 | release 候选、路由／节点故障／发现变更 | 两 Pod 同宿主不能证明宿主故障和真实网络隔离 |
| GPU/NPU | 设备调度变更且有对应硬件 worker | 模拟设备只证明规则，不能证明驱动和整卡使用 |

### 4.3 夜间或发布前，不作为普通 PR 冒烟

- 调度性能基线、Gateway keepalive/规模测试和大并发创建。
- 多租户持续入队、公平性和资源抖动压力；正确性单测仍属于 L1 必选。
- 1–24 小时 mixed-load、日志容量、Collector 中断和进程反复重启。
- FC 双克隆网络调查、跨节点共享 checkpoint、对象存储故障注入。
- Agent 服务经 Sandbox SDK 的跨层验收。平台基础门槛不应为此启动 Agent 服务。

## 5. 当前流水线差距

当前 `platform-build` 只执行 62 个 E2E 驱动器静态契约，然后编译 release、SDK wheel 和
sandboxd 后端；它没有运行完整 Rust workspace、Agent、Sandbox SDK、真实 Redis/mTLS
协作和 RRT interop。后续应先增加 L0–L2 独立步骤，再保留现有三阶段制品与 K8s 流程。

当前基础 K8s 八组已经适合作为正式门槛，无需缩减。Firecracker、跨物理宿主、真实
GPU/NPU、性能和长稳必须单列结果，不能因基础流水线通过而宣称这些能力已覆盖。

源码中仍保留 topology spread 的规则测试，但当前基础 E2E 未把它作为产品承诺。该规则可随
Rust workspace 低成本回归；是否继续作为对外能力，需要另行决定，不能用单测存在反推产品范围。
