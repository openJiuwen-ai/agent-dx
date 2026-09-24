# ADX 端到端用例分层与 Buildkite 门禁

UT 与 E2E 分开管理：UT 按代码模块运行；E2E 按业务闭环和部署拓扑逐级扩展。

跨层级的错误、Deadline、sandboxd、adxlet 和节点故障契约见[系统可靠性门禁](system-reliability-gates.md)。这些场景按所需拓扑分别进入 Standalone、Multi-VM 和 Full Deployment，不能只用组件测试宣称完成。

## 1. UT 不纳入 E2E 层级

UT 包含 Rust workspace、Agent、Sandbox SDK、构建驱动器，以及使用测试进程或隔离 Redis
验证组件契约的用例。它们用于快速定位规则和接口错误，但不代表完整平台已经部署。

| UT 类别 | 当前规模／入口 | 作用 |
|---|---|---|
| Rust | 404 个静态测试定义；`cargo test --workspace --all-features` | API Server、Coordinator、adxlet、Execd、Gateway、CLI 和公共 crates |
| Python | 528 个静态测试定义 | Agent、Sandbox SDK、构建和测试驱动器 |
| 状态与 RPC 契约 | `storage`、`control-rpc`、`api-control`、`interop` suite | Redis/AOF、mTLS、进程 RPC 和 Execd Socket 协作 |

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
控制面／Gateway、真实 sandboxd 和真实 Execd；允许所有组件位于一个测试宿主。

| L0 用例 | 必须断言 |
|---|---|
| 部署就绪 | Coordinator 可发现、Node 注册并完成对账、容量有效、路由和本机绑定同步完成 |
| API Key | 有效 Key 可访问；无效 Key 和跨租户访问被拒绝 |
| 创建 Environment | 经公共 SDK 创建，sandboxd 后端真实运行，Execd 就绪，状态和路由已提交 |
| 查询 Environment | `get/list` 返回正确租户、状态、资源和执行归属 |
| 执行命令 | 真实经过 Ingress → Relay → Execd，校验 stdout、stderr 和退出码 |
| 文件操作 | 二进制写入和读回一致，不能只验证 HTTP 状态码 |
| 删除 Environment | Redis 终态正确、路由撤销、资源释放、sandboxd inventory 为空 |
| 环境清理 | 测试进程／容器、临时网络和测试凭证均无残留；清理失败使整轮失败 |

当前十一组驱动中的 `sdk` 提供最小创建、查询、命令、文件和删除主体；`data-plane` 独立覆盖
资源发现、按 ID 重连、可恢复后台命令、文件目录复制、PTY 和端口转发；`lifecycle` 覆盖
detached 重连／显式删除与空闲回收；`auth` 提供凭证与租户隔离。
L0 不包含双节点放置、节点失联、跨节点恢复、暂停／快照、性能或长稳。

十一组是部署和清理边界，不是功能用例总数。SDK 的 66 个公开操作、构造参数、错误类型以及
逐项 E2E 状态见 [Sandbox SDK 公开能力与端到端覆盖](sdk-e2e-coverage.md)。`sdk`、
`data-plane`、`lifecycle` 和 `placement` 会把稳定子用例写入 JUnit，而不再用一个组名掩盖
组内覆盖数量。

## 4. Local Standalone：单机真实部署验收

Standalone 在一台 Linux 主机或一台 Lima KVM VM 上，以进程方式运行完整 ADX。sandboxd
由部署环境独立托管。它重跑全部 L0，并验证单机部署、运行时和本机恢复能力。

| Standalone 用例 | 当前归属 |
|---|---|
| `adxctl validate/render/start/status/stop` | 验证统一配置、supervisor、进程重启预算和停机清理 |
| 发布包安装示例 | 从干净发布包和 wheel 启动，配置及制品哈希固定 |
| 运行环境 | 本地 EROFS、OCI 默认 runtime、runtime-only、自定义镜像只读挂载 Execd |
| Relay 组合 | embedded 和 standalone 两种模式遵守同一绑定／路由契约 |
| 单机容量 | 资源满载不超分、等待请求在释放后继续、账本和 Metrics 一致 |
| adxlet 重启 | 新 session 完成权威对账；已运行后端身份和 SDK 操作保持正确 |
| 日志与可观测 | Metrics、结构化日志、滚动压缩、Collector 中断恢复、Trace 链路 |
| supervisor stop | 删除本机 Environment 后退出；独立 sandboxd 仍可响应且 inventory 为空 |
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
| 双 worker 放置 | 两个 worker 都实际承载 Environment；保存平台归属和 sandboxd 后端证据 |
| 容量与调度 | 两节点满载、排队、释放唤醒；Pack/Spread 配置；亲和／反亲和和节点偏好 |
| Local-first | API Server 入口轮转、本机准入、同 ID 并发收敛、冲突规格拒绝、中心 fallback 不重复计账 |
| 跨节点数据链路 | Ingress → 对应 Relay → Execd 的命令、文件和路由结果正确 |
| worker 失联 | 心跳过期后实例失效、路由撤销、健康 worker 继续服务；返回节点先清理旧后端再开放准入 |
| worker 进程重启 | 新 node session、实例对账、旧 session fencing，未失效 Environment 仍可查询和执行 |
| 控制面重启 | Redis 权威状态恢复、API Server 内嵌 Ingress 重新全量同步、旧 epoch 不能继续写入 |
| 跨节点 checkpoint | 仅在 VM 均有 KVM 时验证共享 checkpoint、同 Environment ID 新 generation、旧节点清理 |
| 有序停机 | 先 worker、后控制面；删除结果提交成功，三台 VM 无后端和路由残留 |

三台 VM 即使位于同一台 Mac 上，也只能证明 guest 网络和进程隔离；不能作为物理宿主故障证据。
现有 `gateway/tests/local_3vm_*` 主要覆盖数据面和性能，完整控制面三 VM 自动化仍需补齐。

## 6. Full Deployment Acceptance：全量实际部署验证

全量验收使用正式 Buildkite 构建的 release、SDK wheel、不可变 Node/Execd 镜像和固定外部依赖，
部署到目标 Kubernetes／准生产环境。运行节点不编译代码，也不借用开发机文件。

| 全量验收用例组 | 内容 |
|---|---|
| 制品交接 | clean commit、release SHA256、SDK 版本、sandboxd revision、镜像 digest 和目标架构一致 |
| 实际部署 | 独立 namespace、Secret、Service、Pod、镜像拉取、权限和目标 kubeconfig；等待业务就绪而非仅 Pod Ready |
| L0 | 完整重跑 API Key、创建、查询、命令、文件、删除和物理清理 |
| `data-plane` | 资源发现、重连、命令 stdin／查询／终止、文件 CRUD／目录复制、PTY 和鉴权端口转发 |
| `lifecycle` | detached 句柄释放后重连、显式删除，以及空闲超时自动回收 |
| `capacity` | 两节点资源满载／排队／释放；Coordinator 与 Node Metrics 账本一致 |
| `placement` | 双节点亲和 OR、反亲和、节点约束、有序／加权偏好和实际归属 |
| `local-first` | 入口轮转、原子 claim、相同 ID 收敛、冲突拒绝和中心 fallback |
| `node-failure` | 心跳失效、实例／路由撤销、健康节点可用、恢复节点清理后重新准入 |
| `sandboxd-restart` | 向独立 sandboxd 注入 `SIGKILL`，要求运行中后端 ID、文件内容和命令能力保持 |
| `restart` | adxlet 重启和权威对账；必要时增加 Coordinator/API/Ingress 重启 |
| `stop` | 各节点独立创建运行实例、转发端口与采集探针，再验证 supervisor 清理、日志滚动压缩和最终后端清空 |
| 可观测验收 | 实例数量和资源指标、Gateway 指标、结构化日志、Collector 恢复、完整 Trace 父子链 |
| 环境清理 | JUnit 无失败／跳过，`missing_checks=[]`、`cleanup_errors=[]`，namespace 删除完成 |

当前 `k8s-basic` 要求 `sdk`、`auth`、`capacity`、`placement`、`local-first` 五组；
`full` 再加入 `data-plane`、`lifecycle`、`node-failure`、`sandboxd-restart`、`restart`、`stop`。
[Buildkite Full #17](https://buildkite.com/agent-dx/agent-dx-full-test/builds/17) 在两个不同物理
worker 上通过全部十一组（用例累计 185.431 秒；JUnit 45 项，失败和跳过均为 0，
`cleanup_errors=[]`），已形成完整 Full 门禁证据。Full #13 定向 `stop` 和 Full #16
定向 `restart` 用于此前失败的定位，不能单独替代 Full 门禁。当前 K8s 两节点只报告 `runc`，
验证了不支持的 runtime 不分配；异构 runtime 的正向亲和还需单独环境验收。

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
| adxlet 重启对账 |  | ✓ | 扩展为独立 worker | ✓ |
| 双节点放置／亲和／偏好 |  | 逻辑节点复现 | ✓ | ✓ |
| Local-first／原子归属 |  | 逻辑节点复现 | ✓ | ✓ |
| 节点失联／返回清理 |  | 进程级复现 | ✓ | ✓ |
| Coordinator/API/Ingress 重启同步 |  | 可验证 | ✓ | ✓ |
| Metrics／日志／Trace |  | ✓ | 跨节点扩展 | ✓ |
| FC 暂停／快照／克隆 |  | KVM Standalone | 可选 KVM | 独立 KVM profile |
| 跨节点 checkpoint |  |  | KVM Multi-VM | 独立 KVM profile |
| 物理宿主故障／网络隔离 |  |  | 仅不同物理宿主时 | ✓ |
| 真实 GPU/NPU |  |  | 有硬件时 | 独立设备 profile |

## 8. 当前自动化状态

| 层级 | 当前状态 | 主要缺口 |
|---|---|---|
| L0 | 独立 `--profile l0` 执行 `l0 + auth`，输出 `required_checks`、逐项 JSON 和 JUnit；Buildkite 基础包 #87 已通过 | 后续提交仍需持续执行门禁 |
| Standalone | `--profile standalone` 统一本地 Docker 十一组；安装示例和 Lima FC 各自有严格结果契约 | 需增加汇总清单，把普通 Linux、安装示例和按需 KVM 结果关联到同一 revision |
| Multi-VM | 三 VM inventory 与结果契约、公开 SDK 放置/数据链路、容量队列、Pack/Spread、local-first、worker 与控制节点故障及有序停机定向用例已提供；`suite.py` 为跨配置分批执行提供共享三小时预算和失败汇总 | 尚缺生成配置、分发制品及完整 `contract.REQUIRED` 结果的三 VM 部署器；节点偏好、旧 session fencing、独立 Ingress 与 KVM checkpoint 仍有专项缺口；未进行真实三 VM 验收 |
| Full Deployment | K8s 已支持 `l0`、五组 `k8s-basic` 和十一组 `full`；`full` 强制两个 Pod 位于不同物理 worker | Full #17 在不同 worker 同次通过十一组、JUnit 45 项和资源清理；异构 runtime、K8s FC 和真实 GPU/NPU 仍需独立环境 |

因此当前可以直接形成 Buildkite 门禁的是 UT、L0、基础 K8s 五组和 Full 十一组。`full` 的同宿主假绿已被
驱动拒绝；Full #17 已在双 worker 同次通过十一组。Standalone 可作为独立 Linux/KVM
profile；完整 Multi-VM 在补齐部署器前不能标记为通过。

## 9. 新增用例规划

以下 ID 用于进度页、Buildkite annotation、`result.json` 和缺口追踪。`已实现` 表示已有可执行
断言；`计划` 表示契约已确定但尚无通过证据。新增用例必须保存调用结果、平台状态和物理后端
三类证据，不能只保存客户端成功响应。

### 9.1 L0

| ID | 状态 | 用例与通过条件 |
|---|---|---|
| `L0-01` | 已实现 | 部署业务就绪：Coordinator、节点容量、路由和本机绑定均可用 |
| `L0-02` | 已实现 | 有效／无效 API Key、跨租户拒绝、管理员密钥管理及缓存过期 |
| `L0-03` | 已实现 | 公共 SDK 创建和查询，真实 sandboxd/Execd 后端运行 |
| `L0-04` | 已实现 | 命令 stdout、stderr、退出码及二进制文件往返 |
| `L0-05` | 已实现 | 显式删除后 Redis 终态、路由、资源及 sandboxd inventory 全部清理 |
| `L0-06` | 定向用例已实现，待实测 | `create-response-cut` 用真实 TLS 代理在首个 Running final 已生成后切断下游，要求 SDK 同 Request ID／名称重试、Redis generation 和 sandboxd backend 不变，并完成命令与清理。结构化 unknown 与正常 EOF 无 final 已有 SDK 契约测试；完整场景尚未纳入每次提交的 L0 门禁 |

### 9.2 Local Standalone

| ID | 状态 | 用例与通过条件 |
|---|---|---|
| `ST-01` | 已实现 | 发布包 verify、配置 validate/render、五角色启动和 stop 清理 |
| `ST-02` | 已实现 | EROFS、OCI、runtime-only、自定义镜像挂载内置 Execd |
| `ST-03` | 已实现 | 容量用尽、内存队列、释放唤醒和资源 Metrics 一致 |
| `ST-04` | 已实现 | adxlet 新 session 对账，已运行后端身份不变 |
| `ST-05` | 已实现 | 日志滚动压缩、Metrics、Trace、Collector 中断恢复 |
| `ST-FC-01` | 已实现 | KVM 暂停／恢复、可复用快照、克隆和制品清理 |
| `ST-06` | 定向用例已实现，待实测 | `sqlite-fallback` 暂停 Coordinator、保持 Redis 可读，验证空闲实例本地删除写入 SQLite pending、Redis 暂时保留旧 Running 结果；心跳期限内恢复后要求 pending 清空、Redis 收敛为 Deleted，另一个存活实例保留原 backend 并继续执行 SDK 命令。节点自身重启且 Coordinator 仍不可用的组合故障仍需独立 E2E |
| `ST-07` | 客户端退出已验证，活动请求用例待实测 | `standalone`／`full` 覆盖无活动实例空闲删除；[Full #22](https://buildkite.com/agent-dx/agent-dx-full-test/builds/22) 验证独立 SDK 客户端进程退出、120 秒后台命令仍运行时由 6 秒空闲策略先行删除（子项 13.491 秒）。定向 `idle-active` 用例已加入：前台请求跨越空闲阈值仍保持运行，结束后空闲删除并释放资源；尚未在真实部署执行 |
| `ST-08` | 定向用例已实现，待实测 | `runtime-exit` 通过真实 sandboxd 删除运行中 backend：默认 Never 进入 Failed 且无新执行；配置两次重启时每次产生新 runtime identity、继续提供命令能力，第三次退出后达到重试上限并释放资源。退避精确时序已有组件测试，正式进程用例待新包运行 |
| `ST-09` | K8s 定向验证中 | Coordinator 与 API Server（含 Ingress）分别重启后的 epoch、全量目录和路由重同步；独立 Ingress 使用分进程 fixture，单机部署仍需验证 |
| `ST-10` | 分进程用例已实现，待实测 | 定向 `relay-standalone` 为两个节点分别启动独立 `adx-relay`，核对进程 PID、实际可执行文件与健康端点，再复用 embedded 模式通过的数据面 SDK 命令、文件、端口转发和反向隧道用例；新发布包上的真实结果待验证 |
| `ST-11` | daemon 重启已验证，采集过期定向用例待实测 | [Full #17](https://buildkite.com/agent-dx/agent-dx-full-test/builds/17) 对两节点独立 sandboxd 注入 `SIGKILL` 并重启，核对运行中 backend ID 不变、公开 SDK 可查询及继续执行命令；`sandboxd-restart` 用例 5.065 秒通过。新增 `resource-stale` 定向用例：暂停节点资源采集直到样本过期，要求保持已运行 backend 和节点 session、关闭新准入，恢复后重新开放并通过公开 SDK 创建和清理；正式部署尚未运行 |
| `ST-12` | 定向用例已实现，待实测 | `reconcile-crash` 使 node2 心跳过期并产生旧 backend；暂挂其 runc init，在 sandboxd Delete 发出 TERM、节点保持关闭准入时杀掉 Adxlet。随后恢复 init，要求新 Adxlet 换 session、从权威目录清理旧 backend 后恢复准入；node1 原实例继续执行，最终释放资源。真实 standalone/full 尚未运行 |

### 9.3 Local Multi-VM

| ID | 状态 | 用例与通过条件 |
|---|---|---|
| `MV-01` | 契约已固化 | 三个唯一 machine ID，控制节点和两个 worker 完成跨 VM mTLS/Redis/RPC 就绪 |
| `MV-02` | SDK 子集已有，未实机执行 | 实例实际落到两个 worker，保存 `adx-inspect` 的持久化 assignment 与各自 sandboxd inventory |
| `MV-03` | 容量/队列及 Pack/Spread 定向用例已有，未实机执行 | 双 worker 总容量、排队和释放唤醒；在两个独立 central 配置中分别验证 Pack 同节点、Spread 分节点。节点偏好评分仍待补 |
| `MV-04` | 定向用例已有，未实机执行 | Local-first 入口轮转、原子归属、冲突拒绝、真实本地 claim 证据及中心 fallback 不重复计账 |
| `MV-05` | SDK 子集已有，未实机执行 | Ingress 经目标 Relay/Execd 的跨 VM 命令与文件路径 |
| `MV-06` | 故障子集已有，未实机执行 | worker 心跳过期使实例失效并撤路由；返回 worker 清理旧后端、换会话后再准入，健康 worker 继续执行 |
| `MV-07` | 快速重启子集已有，未实机执行 | worker 进程重启、原 backend/归属代数保持和新 session 对账；旧 session 写入隔离仍待协议级探针 |
| `MV-08` | 默认共进程定向用例已有，未实机执行 | Coordinator/API Server（含嵌入式 Ingress）及托管 Redis 逐个重启；双 worker 原归属、generation、后端、文件与公开 SDK 路由保持可用。独立 Ingress 进程仍待单独用例 |
| `MV-FC-01` | 条件计划 | 两个 KVM worker 间共享 checkpoint 恢复，同 ID 新 generation 且旧节点清理 |
| `MV-09` | 定向用例已有，未实机执行 | 专用环境明确确认后，worker-2、worker-1、控制节点依次停止；各 worker 后端为空、持久化归属删除、旧路由不可达，剩余 worker 在停机前仍可服务 |

### 9.4 Full Deployment

| ID | 状态 | 用例与通过条件 |
|---|---|---|
| `FD-01` | 已实现 | clean commit、发布包 SHA256、SDK、sandboxd revision 和镜像 digest 一致 |
| `FD-02` | 已实现 | 独立 namespace 的 L0 全量重跑 |
| `FD-03` | 已验证 | K8s 基础五组与完整十一组均输出逐项 JUnit、日志、事件和清理证据；Full #17 同次通过十一组 |
| `FD-04` | 已实现 | Metrics、日志、滚动压缩、Collector 重启与 Trace 父子关系 |
| `FD-05` | 已验证 | `full` profile 的两个 Pod 必须落在不同物理 worker；Full #6 分别运行于 `10.244.128.124` 和 `10.244.128.160` |
| `FD-06` | 同 Pod 重启已验证；跨 Pod 用例已实现、待实测 | [Full #18](https://buildkite.com/agent-dx/agent-dx-full-test/builds/18) 定向运行 `redis-restart`：`SIGKILL` 托管 Redis 后由 supervisor 重启，AOF 开启，双节点实例归属和代次、后端 ID、文件及命令保持一致，删除后资源释放。新增 K8s 专用 `redis-pod-restart`：Redis 独立 Pod 与 PVC，两个 worker 继续运行；替换 Redis Pod 后核对新 Pod UID、同一 PVC UID、AOF、归属／代次、后端 ID、公开 SDK 文件与命令及最终资源释放。该用例尚无真实集群结果 |
| `FD-07` | Coordinator／API Server 已验证，Ingress 待新包复验 | [Full #19](https://buildkite.com/agent-dx/agent-dx-full-test/builds/19) 通过 Coordinator 重启，[Full #20](https://buildkite.com/agent-dx/agent-dx-full-test/builds/20) 通过 API Server 重启；均核对原归属／后端和公开 SDK 文件、命令。[Full #21](https://buildkite.com/agent-dx/agent-dx-full-test/builds/21) 因默认共进程 fixture 无独立 Ingress PID，在故障注入前失败；[Full #24](https://buildkite.com/agent-dx/agent-dx-full-test/builds/24) 改用分进程 fixture 后发现复用的旧产品包未携带 `adx-ingress`，启动阶段失败，故障注入未执行。发布包和构建配方现已补入分进程二进制，待新产物复验；持续网络分区仍待独立用例 |
| `FD-08` | 定向用例已实现，待实测 | `network-partition` 在 node2 Pod／容器网络命名空间阻断到 Coordinator 的 TCP 流量，要求防火墙计数非零、心跳失效及旧实例撤路由；隔离期 node1 继续执行和创建，解除后 node2 清理旧后端再准入。正式双物理 worker 尚未运行该用例 |
| `FD-09` | 已验证 | [Full #23](https://buildkite.com/agent-dx/agent-dx-full-test/builds/23) 的 `data-plane` 定向用例通过 Host 子域名端口转发：`<instance-id>-18081.example.test` 携带鉴权后到达实例的嵌套路径，缺少 Token 被拒绝；用例 28.859 秒，清理错误为 0 |
| `FD-10` | 用例已实现，异构环境未验证 | 定向 `runtime-affinity` 要求两个节点真实上报不同的 sandboxd runtime inventory；通过公开 SDK 请求 `runsc` 且不指定节点，核对唯一支持节点上的归属、命令执行和资源释放。当前 runc-only fixture 无法使该用例通过，不能将负向拒绝测试充当正向亲和证据 |
| `FD-FC-01` | 条件计划 | KVM worker 的 Firecracker pause/resume/snapshot profile |
| `FD-XPU-01` | 条件计划 | 真实 GPU/NPU 整卡发现、过滤、分配、释放和故障清理 |
| `FD-SOAK-01` | Nightly | 创建／执行／删除循环及反复节点故障，持续 1–24 小时无资源增长 |

推荐执行频率：每次提交运行 `L0-01..05`；主分支运行 `ST-01..05` 和 `FD-01..04`；具备双物理
worker 时把 `FD-05` 设为合入门槛；Multi-VM、FC、XPU 和长稳按 nightly、相关路径变更及发布候选触发。

## 10. Buildkite 门禁建议

Buildkite 保留 UT 和 E2E 两条清晰的结果线：

1. **UT gate**：Rust、Agent、Sandbox SDK、驱动器、真实 Redis/mTLS/Execd 组件契约；任何失败阻断。
2. **L0 E2E gate**：每个提交使用真实发布包部署完整最小平台，执行 L0 全部用例；不能用 mock 后端。
3. **Full deployment gate**：主分支、合入候选和发布候选执行十一组 K8s 用例、可观测断言及清理。
4. **Conditional profiles**：checkpoint/snapshot 变更触发 KVM；设备调度变更触发 GPU/NPU worker。
5. **Nightly / release**：本地多 VM、跨物理宿主、性能、压力、长稳和重复故障注入。

默认每次提交执行五组 `k8s-basic`。完整十一组保留在主分支、合入候选和发布前：本地实测中
`node-failure` 约 33 秒，`lifecycle`、`data-plane`、`stop` 各约 11–14 秒；其中
`node-failure`、`lifecycle`、`stop` 包含固定等待、故障注入或完整停机。分层只减少基础门禁
时间，不降低完整实际部署验收范围。
