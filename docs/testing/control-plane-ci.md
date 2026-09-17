# 管控面重构：本地测试与 Buildkite

Rust API Server 重写前的正式验证：[Buildkite #21 基础 Kubernetes、Metrics、日志与 Trace 验收](2026-09-17-observability-k8s.md)。FC 按当前决策继续本地验收。

2026-09-15。开发验证在本地执行，Buildkite 使用完整系统的端到端验收入口。统一包、两节点环境驱动器和流水线配置已落地：`build/e2e/prepare.py` 构建验收制品，`build/e2e/kubernetes/run.py` 在目标 Kubernetes 集群部署、验收、收集并清理。真实 sandboxd、本次包内 RRT 和安装后的 SDK 均参与执行。流水线复用现有 default/linux/amd64 队列、builder/packager/deployer、目标 kubeconfig 挂载与 SWR Secret，见 [Buildkite 说明](../../.buildkite/README.md)。本地通过不等于远端 Buildkite 已通过。

## 本地开发验证

`python3 build/ci/run.py <suite>` 用于重构过程中的快速反馈。Makefile 保持语言原生构建入口，运行器负责超时、退出码和证据记录。

| suite | 实际执行 | 证据 |
|---|---|---|
| harness | 本地执行器测试 | unittest 日志 |
| rust | Cargo workspace all-features 测试；包含 RRT 子进程测试 | 工具版本、Cargo 日志 |
| storage | 独立真实 Redis：提交、代次校验、AOF 崩溃重启、调度状态恢复 | Redis 版本/摘要、测试与进程日志；设置 ADX_TEST_REDIS_SERVER |
| control-rpc | Master／Node RPC、StateSink、真实 mTLS 与 Redis 协作 | 临时测试证书、进程日志和结果；运行时为测试实现 |
| api-control | 真实 Rust HTTPS 进程 → Rust Master／Node RPC → Redis，另验证 Master 进程启动重启 | 生产 API 二进制摘要、TLS／进程日志；运行时为测试实现 |
| api-server | Rust API Server 契约与缓存测试 | Cargo 测试日志 |
| agent | 原 Agent 测试集 | 日志、JUnit XML |
| sandbox-sdk | 完整离线 SDK 测试集 | 日志、JUnit XML |
| interop | SDK → 真实 RRT → 测试 HTTP upstream 的 Socket 互操作 | 构建与进程测试日志 |
| package | 四个 Python 包生成 wheel 和 sdist | 8 个制品及 SHA256 |

Socket 测试只覆盖部分数据链路；Agent 测试包含外部运行时桩。上述结果属于本地组件/协作验证，完整平台验收遵循下面的 E2E 契约。

## 本地运行

工具链基线：Rust 版本遵循根 `rust-toolchain.toml`；外部 sandboxd 构建使用锁定 Go 工具链。Python 测试环境固定在 `build/ci/requirements.lock`，仅约束测试/构建环境，不修改 SDK 对用户的依赖范围。

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r build/ci/requirements.lock

# cache 目录由本机提供；不同平台、工具链、release/UT 使用独立 namespace。
export CARGO_TARGET_DIR=/your/cache/ut/linux-amd64-rust/target
export GOCACHE=/your/cache/ut/linux-amd64-go/build
export GOMODCACHE=/your/cache/go-mod
export PIP_CACHE_DIR=/your/cache/pip

.venv/bin/python build/ci/run.py harness
.venv/bin/python build/ci/run.py rust --jobs 2
.venv/bin/python build/ci/run.py api-server --jobs 2
.venv/bin/python build/ci/run.py agent
.venv/bin/python build/ci/run.py sandbox-sdk
.venv/bin/python build/ci/run.py interop --jobs 2
.venv/bin/python build/ci/run.py package
```

也可使用 `make ci SUITE=api-server PYTHON=.venv/bin/python JOBS=2`。`--list` 打印将执行的命令，不运行测试；它仍会检查前置配置，`api-control` 需要通过 `ADX_TEST_API_SERVER` 指向已构建、可执行的 Rust API Server 二进制。各轮结果默认写入 `out/ci/<suite>/<UTC时间>-<随机ID>/`；指定 `--output` 时必须使用新目录，重试不会覆盖上次失败证据。

每个结果包含 commit、本地 dirty 标记、平台、Python/工具版本日志、命令与退出码、耗时、配置的缓存位置和制品校验和。Buildkite 环境要求入口执行前工作树干净。本地 dirty 运行允许，但不能当作该 commit 的正式 CI 结果。

运行器对每条命令设定超时；失败停止后续命令并保留真实退出码。超时或取消会终止本轮子进程组，保存日志和 result.json。强制 SIGKILL、宿主掉电等无法执行收尾的情况仍由 Buildkite job 状态裁决，不能将缺失结果当作通过。

## Buildkite 端到端验收契约

```text
本次提交 → 构建统一部署包及 SDK wheel → 制品校验
                                    ↓
                        部署独立 namespace 内的两节点完整平台
                                    ↓
                       就绪检查 → 公共 SDK E2E
                                    ↓
                       结果与诊断收集 → 环境清理
```

### 1. 构建及制品交接

从本次提交生成 Master、Node Manager、Sandbox API、Edge、Node Proxy、RRT、部署工具与 Sandbox SDK；统一包清单记录 commit、各组件版本、外部依赖版本与 SHA256。运行阶段下载并验证这一批制品，部署阶段不重新编译或从开发机借用程序。四个 Python 包构建成功只是构建检查的一部分。

`build/images/Dockerfile.ci` 保留为构建环境配方，发布后按 digest 引用。运行环境按 sandboxd 的真实运行要求配置；Buildkite 以 Kubernetes Pod 承载测试进程，Pod 内使用统一 supervisor；通过 kubectl 清单部署，不增加产品 Kubernetes 控制器。

### 2. 部署完整平台

- 平台组件：Redis、单 Master（Global 与内嵌 Shard）、Sandbox API、Edge、两个 Node Manager 和 Node Proxy。
- 执行后端：测试环境独立托管 sandboxd，锁定 PR #56 提交 `efc201531d7e2e9d69505da151eb66084b61eebf`（见 `third_party/sandboxd/source.json`）；使用本次构建的 RRT 准备真实实例环境。
- 客户端：干净 Python 环境安装本次构建的 SDK wheel，通过对外入口操作；禁止用源码 PYTHONPATH 代替安装包验收。
- 用例资源：本轮独立的租户/API Key、实例、端口与目录。快照阶段接入实际的对象存储测试后端；GPU/NPU 在对应环境与能力实现后扩展。

运行器等待组件与节点就绪，读取运行时实际地址和实例归属。固定 sleep 结束、TCP 端口打开、某个健康端点返回 200，都不能单独作为完整平台就绪的判断。

### 3. 公共 SDK 执行用例

基础门禁包含七组用例：SDK 创建／命令／文件／删除、API Key 与租户隔离、资源不足与释放后重新调度、双节点放置约束、心跳超时与恢复清理、Node Manager 进程重启、supervisor 停机清理。放置组检查实例亲和 OR、实例反亲和、加权与有序节点偏好、每个 OR 分支的 node_id 约束及反向实例反亲和，并比对实际节点归属。业务操作使用公共 SDK；管理查询、只读状态检查和受控故障注入用于验证内部结果，不替代真实调用链。

容量组新增Metrics核对：实际抓取Master和两个Node Manager，验证满载实例数量/分配量、排队请求及删除后的释放，保存原始指标证据。

通过条件同时覆盖：用户可见结果、真实执行结果、持久化状态、路由和资源回收。例如创建必须能实际执行命令，删除必须确认旧实例及路由退出服务。清理后的残留检查也是门禁的一部分。

暂停/恢复、快照与跨节点 checkpoint 恢复当前使用本地 Firecracker 验收，后续具备 KVM worker 与对应架构 runtime kit 时再启用独立 FC profile。基础流程使用两个逻辑节点；同宿主 Pod 的进程故障注入不代表已覆盖跨宿主网络分区或宿主机故障。

### 4. 结果、清理与最终状态

部署失败、就绪超时、测试失败都进入诊断和清理流程。收集 JUnit、结构化结果、各组件日志、制品清单及本轮必要的实例/路由/资源状态。日志不打印完整环境变量或凭证。

清理仅处理本轮创建的进程、容器、实例和目录。保留最初失败原因；清理失败也必须使整轮不通过。取消和宿主故障由运行环境的资源标记及残留回收机制处理。结果缺失、用例未执行、仅构建成功，均不产生 E2E 通过结论。

### 接入状态

`.buildkite/README.md` 保存端到端流程约定。基础流水线有三个独立步骤：`platform-build` 构建与交接发布包，`platform-images` 发布固定 digest 的节点／RRT 镜像，`platform-e2e` 部署 Kubernetes 并执行七组用例。运行阶段只使用这批制品，验证 commit、架构及 SHA256；七组场景和环境清理全部成功后才通过。当前正式验收及制品身份见本文顶部记录。

本地组件测试继续用于每一步的测试驱动开发；同一套完整 E2E 驱动器也应支持在具备环境的本地机器上复现 Buildkite 失败。

## 新管控面的测试驱动实施顺序

每个功能先写本地规则/协作测试，再定义公共 SDK 的 E2E 验收用例；补实现使测试通过后，按约定环境纳入验收门禁。以下保留实施顺序，各阶段当前完成情况与剩余项见[阶段路线图](control-plane-roadmap.md)。未执行的用例不以 skip 计为通过。

### 第一条纵向链路：创建、命令、删除

```text
Sandbox SDK → Rust API Server → Master / Global → Shard → Node Manager → sandboxd
                       现有实例操作 ────────────────────────┘
命令与文件：SDK → Edge → Node Proxy → RRT
状态提交：Node Manager → Master → Redis；Master → Edge 发布路由
```

先实现纯 Instance 状态与资源类型、Node Manager 串行控制器及 RuntimeBackend 边界。紧接着接入最小 RPC、Redis 提交、内嵌 Shard、Sandbox API 后端，打通完整链路。Global 保留轮转职责，Shard 实际选择节点，Node Manager 本机复核并拥有生命周期状态机。

首批必过的真实功能用例：

| 用例 | 验收断言 |
|---|---|
| 节点注册 | 自动分域，资源上报后可调度 |
| API Key | 无效凭证被拒绝，跨租户读写被拒绝 |
| 创建 | sandboxd 实例运行、RRT 就绪、本机绑定已应用、状态与路由已提交 |
| 命令 | 实际经过数据链路，输出和退出码一致 |
| 文件 | 上传后下载的二进制内容一致 |
| 删除 | 实际实例退出、旧绑定失效、资源释放、重复删除符合 API 契约 |
| 资源不足 | 不超分配；资源释放后等待请求可继续 |

第一阶段使用 CPU、Linux amd64、两个逻辑节点。RRT 与 Node Manager 的就绪协作、Node Proxy 绑定确认、结果提交完成条件需要与第一批测试同时明确；不等所有组件完成后才定义。

### 后续功能批次

| 批次 | 用例与组件 |
|---|---|
| 生命周期 | Node Manager 的暂停恢复、空闲删除、异常退出重试；删除排在暂停之后；对象制品上传失败的本地恢复回滚 |
| 状态持久化 | 正常经 Master 写 Redis；故障降级日志；节点重启而 Master 不可用时只观察，等待权威对账 |
| 快照 | 本地/对象存储接口、可复用快照、引用及延迟删除、缓存、恢复点过期 |
| 故障恢复 | Master 重启、路由重新同步、节点接管；有效共享 checkpoint 可跨节点恢复；local-only 或缺失 checkpoint 明确失败 |
| 调度完善 | 租户轮转、优先级/FIFO、亲和/反亲和、整卡分配、压力和维护开关 |
| 进程组合 | Node Manager / Node Proxy 共进程和分进程的同一业务契约；分别验证故障行为 |

故障用例使用受控时钟、明确注入点和有期限的条件等待。心跳超时用于故障判定，但网络分区中的旧进程是否还能执行，必须单独写明隔离机制或部署假设；“相同 ID 不会并行执行”不能只检查 Master 状态表。

环境驱动器统一负责部署、探测、收集和清理：每轮唯一标识、独立目录与端口，只清理本轮启动的 PID/容器和测试实例，不按进程名批量 kill。普通功能两节点可共宿主；真实节点故障与网络分区必须使用可独立隔离的环境。sandboxd 由测试环境独立托管，产品 supervisor 不托管它。

Agent 业务通过 Sandbox SDK 验收；平台基础流水线无需启动 Agent 服务。Agent 对新 Sandbox SDK 的真实适配完成后，增加单独跨层测试，不以当前路由透传测试代替业务闭环。

## 早期组件验证记录（历史批次）

基于 `1e49d86` 加当前 CI 工作树修改，通过同一 runner 验证：本地执行器 7 项；Rust 199 项；Go 107 个顶层测试（含子测试 200 项）及 vet；Agent 234 项通过、1 项跳过；Sandbox SDK 233 项通过；真实 SDK/RRT Socket 互操作 11 项；四包 8 个 wheel/sdist，SHA256 已复核。

Go 使用 Linux arm64 容器，其余使用 macOS arm64。完整平台尚未部署，以上结果不包含 Instance 生命周期 E2E。

首次接入互操作脚本时，未知长度 POST 返回 502。已定位到旧测试上游只读取 Content-Length，无法消费 V2 流式请求的 chunked 编码。仅修正测试夹具，新增已知长度 buffered 请求断言，复测 11 项通过；产品数据面实现未修改。失败与成功记录分别保存在 `out/ci/interop/` 的独立运行目录。

Master 存储阶段的契约与运行方法见 [Redis 持久化与恢复](master-storage.md)。`storage` 属于真实依赖的组件集成验证，不代表完整平台 E2E。

组件日志采集验收复用现有 Edge/Node Proxy 指标端点，并通过真实 OpenTelemetry Collector 接收结构化组件日志。stop 组包含后端 503、文件滚动与 Collector 重启，控制台输出 `[METRICS PASS]` / `[COLLECTION PASS]`；产物含 `gateway-metrics-node*.json`、`collection-node*.json`、`collected-logs.jsonl` 和 `collector-process.log`。部署及保证边界见 `docs/testing/log-collection.md`。Trace 已纳入采集验收，输出 `[TRACE PASS]` 并保存 `traces-node*.json` 和 `collected-traces.jsonl`；正式结果见 [Buildkite #21](2026-09-17-observability-k8s.md)。
